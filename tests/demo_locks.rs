//! The locks demo's harness: the crash-reincarnation story end to end.
//!
//! A cluster of three lock nodes on an in-memory transport, one
//! single-threaded loop per node. A client takes locks, a chosen node
//! fakes its death at a named commit (slot 5, the observation the story
//! starts with), the crash performs no disk sync and no fence write, the
//! reborn identity distrusts its stale state file by the fence's verdict,
//! rejoins through the announcement and the leader's forced sequence, and
//! the observation that closes the story returns the same lease bytes
//! from every node, survivors and reborn alike.
//!
//! No wall-clock timeouts anywhere: every wait is a bounded, deterministic
//! loop over the transport, and the lease clock is the loop's logical
//! tick.

#![cfg(feature = "demo-locks")]

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde_json::json;

use uvrr::demo::{
    BootFence, ClientId, CommitHook, DemoDisk, DemoNode, FileBootFence, FileStateStore, NoOpHook,
    Response, StateLoad, StateStore, Transport,
};
use uvrr::ids::{CrashCounter, Era, NodeId, Operation, OperationId, Slot, SystemId};
use uvrr::message::Message;
use uvrr::progress::Status;
use uvrr::replica::ViewChangeKnobs;
use uvrr::wire::{Tag, Unpack};

/// The suspicion budget in ticks: a node fences into the next view after
/// more than three ticks of primary silence (S4). Logical ticks, never a
/// wall clock.
const TIMEOUT: u64 = 3;

/// The lease window, in committed slots: long enough to survive the
/// rejoin's choreography, short enough that a bounded run of committed
/// observations expires it.
const LEASE_WINDOW: u64 = 16;

fn n(id: u32) -> NodeId {
    NodeId::new(
        SystemId::new((id + 1) as u16).expect("test system ids are small and non-zero"),
        CrashCounter::new(1).expect("one is non-zero"),
    )
}

fn op_id(lsb: u64) -> OperationId {
    OperationId { msb: 0, lsb }
}

/// The demo's in-memory transport: one shared bus, explicit queues, no
/// loss and no reordering. The log records every datagram's route, so
/// the announcement's tag is assertable without touching the nodes.
#[derive(Clone)]
struct Bus {
    queue: Rc<RefCell<VecDeque<Envelope>>>,
    log: Rc<RefCell<Vec<(NodeId, NodeId, Tag)>>>,
}

struct Envelope {
    from: NodeId,
    to: NodeId,
    message: Message,
}

impl Bus {
    fn new() -> Self {
        Self {
            queue: Rc::new(RefCell::new(VecDeque::new())),
            log: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn sent(&self, from: NodeId, tag: Tag) -> bool {
        self.log
            .borrow()
            .iter()
            .any(|(sent_from, _, sent_tag)| *sent_from == from && *sent_tag == tag)
    }
}

/// One node's end of the bus.
struct MemTransport {
    own: NodeId,
    bus: Bus,
}

impl Transport for MemTransport {
    fn receive(&mut self) -> Option<(NodeId, Message)> {
        let mut queue = self.bus.queue.borrow_mut();
        let position = queue.iter().position(|envelope| envelope.to == self.own)?;
        let envelope = queue.remove(position)?;
        Some((envelope.from, envelope.message))
    }

    fn send(&mut self, to: NodeId, era: Era, message: &Message) {
        // The bus has no partition and no era routing: the copy is queued
        // as the core released it, the era noted for the record only.
        let _ = era;
        let mut queue = self.bus.queue.borrow_mut();
        queue.push_back(Envelope {
            from: self.own,
            to,
            message: message.clone(),
        });
        self.bus
            .log
            .borrow_mut()
            .push((self.own, to, message.header.tag));
    }
}

/// The test's own commit hook: the trigger the crash decision hangs on.
/// It returns nothing and carries no verdict; the driver reads the flag
/// between steps, and the machinery never does.
struct CrashAtSlot {
    fired: Arc<AtomicBool>,
}

impl CommitHook for CrashAtSlot {
    fn on_commit(&mut self, slot: Slot) {
        if slot == Slot(5) {
            self.fired.store(true, Ordering::SeqCst);
        }
    }
}

static ROOT_SEQ: AtomicU64 = AtomicU64::new(0);

/// One unique temporary root per cluster, so a parallel test target never
/// shares another's files.
fn temp_root() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the system clock is after the epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "uvrr-demo-locks-{}-{}-{}",
        std::process::id(),
        nanos,
        ROOT_SEQ.fetch_add(1, Ordering::SeqCst),
    ))
}

/// The demo cluster: the live nodes, the bus, and the disks a crash has
/// recorded. The glue the harness drives; every protocol decision lives
/// in the demo module, every assertion reads its public surface.
struct Cluster {
    bus: Bus,
    nodes: Vec<Option<DemoNode>>,
    disks: Vec<Option<DemoDisk>>,
    slot_of: BTreeMap<NodeId, usize>,
    knobs: ViewChangeKnobs,
    crash_fired: Arc<AtomicBool>,
    root: PathBuf,
    clock: u64,
}

impl Cluster {
    /// The genesis nodes, each a first life over its own directory: one
    /// fence file, one state file, one end of the bus.
    fn provision(size: usize) -> Self {
        let bus = Bus::new();
        let genesis: Vec<NodeId> = (0..size as u32).map(n).collect();
        let root = temp_root();
        let crash_fired = Arc::new(AtomicBool::new(false));
        let mut nodes = Vec::with_capacity(size);
        let mut disks = Vec::with_capacity(size);
        let mut slot_of = BTreeMap::new();
        for (index, &id) in genesis.iter().enumerate() {
            let dir = root.join(format!("node{index}"));
            std::fs::create_dir_all(&dir).expect("the node directory creates");
            let transport = MemTransport {
                own: id,
                bus: bus.clone(),
            };
            let hooks: Box<dyn CommitHook> = if id == n(size as u32 - 1) {
                Box::new(CrashAtSlot {
                    fired: Arc::clone(&crash_fired),
                })
            } else {
                Box::new(NoOpHook)
            };
            let node = DemoNode::first(
                id,
                genesis.clone(),
                Box::new(FileBootFence::new(&dir.join("fence"))),
                Box::new(FileStateStore::new(&dir.join("locks"))),
                Box::new(transport),
                hooks,
                Cluster::knobs(),
            )
            .expect("the genesis order provisions");
            slot_of.insert(id, index);
            nodes.push(Some(node));
            disks.push(None);
        }
        Self {
            bus,
            nodes,
            disks,
            slot_of,
            knobs: Cluster::knobs(),
            crash_fired,
            root,
            clock: 0,
        }
    }

    fn knobs() -> ViewChangeKnobs {
        ViewChangeKnobs {
            primary_timeout: TIMEOUT,
            view_change_budget: usize::MAX,
        }
    }

    fn index_of(&self, id: NodeId) -> usize {
        *self
            .slot_of
            .get(&id)
            .unwrap_or_else(|| panic!("{id:?} is not in the cluster"))
    }

    /// The live node, for reads.
    fn node(&self, id: NodeId) -> &DemoNode {
        self.nodes[self.index_of(id)]
            .as_ref()
            .unwrap_or_else(|| panic!("{id:?} is down"))
    }

    /// The live node, for writes.
    fn node_mut(&mut self, id: NodeId) -> &mut DemoNode {
        let index = self.index_of(id);
        self.nodes[index]
            .as_mut()
            .unwrap_or_else(|| panic!("{id:?} is down"))
    }

    fn is_up(&self, id: NodeId) -> bool {
        // By identity, never by slot: a crashed identity's slot holds its
        // reborn successor now, and datagrams addressed to the dead one
        // are addressed to nobody.
        self.nodes[self.index_of(id)]
            .as_ref()
            .is_some_and(|node| node.own() == id)
    }

    fn live_ids(&self) -> Vec<NodeId> {
        // One identity per live slot: the identity the live node runs
        // under, never a superseded predecessor.
        self.slot_of
            .iter()
            .filter(|&(id, index)| {
                self.nodes[*index]
                    .as_ref()
                    .is_some_and(|node| node.own() == *id)
            })
            .map(|(&id, _)| id)
            .collect()
    }

    /// One bounded, deterministic wait: every live node drains the bus,
    /// in index order, repeatedly, until a full pass delivers nothing
    /// and nothing live-addressed is pending. No wall clock anywhere.
    fn quiesce(&mut self) {
        loop {
            let mut delivered = 0;
            for index in 0..self.nodes.len() {
                if self.nodes[index].is_some() {
                    let outcomes = self.nodes[index].as_mut().expect("checked above").run();
                    for outcome in &outcomes {
                        if let Err(refusal) = &outcome.result {
                            panic!(
                                "n={:?} refused a delivery: {refusal:?}",
                                self.nodes[index].as_ref().expect("checked above").own()
                            );
                        }
                    }
                    delivered += outcomes.len();
                }
            }
            let pending_to_live = self
                .bus
                .queue
                .borrow()
                .iter()
                .any(|envelope| self.is_up(envelope.to));
            if delivered == 0 && !pending_to_live {
                break;
            }
        }
    }

    /// Advances the run's one logical clock by one and settles. The
    /// clock is the host's observation (S4): one clock for the whole
    /// run, read by every live node, a reborn life included.
    fn tick_all(&mut self) {
        self.clock += 1;
        let at = self.clock;
        for id in self.live_ids() {
            self.node_mut(id)
                .tick(at)
                .expect("a tick never refuses a live node");
        }
        self.quiesce();
    }

    /// The standard bootstrap: the genesis primary promotes itself and
    /// the backups adopt the initial view from the promotion's commit
    /// announcement.
    fn bootstrap(&mut self) {
        self.tick_all();
        for id in self.live_ids() {
            assert_eq!(self.status(id), Status::Normal, "{id:?} boots normal");
        }
    }

    /// Proposes one lock operation to a node and settles the cluster.
    fn propose(&mut self, to: NodeId, id: OperationId, payload: &[u8]) -> u64 {
        let revision = self
            .node_mut(to)
            .propose(Operation {
                id,
                payload: payload.into(),
            })
            .unwrap_or_else(|refusal| panic!("the proposal refused: {refusal:?}"));
        self.quiesce();
        revision
    }

    /// Announces a reincarnation pair and settles the cluster.
    fn announce(&mut self, id: NodeId, old: NodeId) -> u64 {
        let revision = self
            .node_mut(id)
            .announce(old)
            .unwrap_or_else(|refusal| panic!("the announcement refused: {refusal:?}"));
        self.quiesce();
        revision
    }

    /// Crashes a node: its volatile state dies, the disk is recorded,
    /// and nothing is written.
    fn crash(&mut self, id: NodeId) {
        let index = self.index_of(id);
        let node = self.nodes[index]
            .take()
            .unwrap_or_else(|| panic!("{id:?} is already down"));
        self.disks[index] = Some(node.record_disk());
    }

    /// Reincarnates a crashed node over its recorded disk: the fence's
    /// verdict, the bump, the announcement. Returns the bumped identity.
    fn reincarnate(&mut self, old: NodeId) -> NodeId {
        let index = self.index_of(old);
        let dir = self.root.join(format!("node{index}"));
        let disk = self.disks[index]
            .take()
            .unwrap_or_else(|| panic!("{old:?} has no recorded disk; crash it first"));
        let transport = MemTransport {
            own: old.next_life().expect("the bump never exhausts in tests"),
            bus: self.bus.clone(),
        };
        let node = DemoNode::reincarnate(
            old,
            disk,
            Box::new(FileBootFence::new(&dir.join("fence"))),
            Box::new(FileStateStore::new(&dir.join("locks"))),
            Box::new(transport),
            Box::new(NoOpHook),
            self.knobs,
        )
        .unwrap_or_else(|refusal| panic!("the reincarnation refused: {refusal:?}"));
        let new = node.own();
        assert_ne!(new, old, "the reincarnation always changes the identity");
        // The bumped identity occupies the old life's slot from here on;
        // the superseded identity's datagrams address nobody.
        self.slot_of.insert(new, index);
        self.nodes[index] = Some(node);
        new
    }

    /// Drives the view change the fence machinery targets: a bounded
    /// tick sweep on every live node, then the settle. Every live member
    /// of the target view installs it.
    fn drive_view_change(&mut self) {
        for _ in 0..=TIMEOUT {
            self.tick_all();
        }
        self.quiesce();
    }

    fn status(&self, id: NodeId) -> Status {
        self.node(id).status()
    }

    fn era(&self, id: NodeId) -> Era {
        self.node(id).era()
    }

    fn committed(&self, id: NodeId) -> Slot {
        self.node(id).committed()
    }

    fn membership(&self, id: NodeId) -> Vec<(NodeId, u32)> {
        self.node(id).membership()
    }

    fn applied_response(&self, id: NodeId, operation: OperationId) -> Vec<u8> {
        self.node(id)
            .applied()
            .iter()
            .find(|applied| applied.operation_id == operation)
            .unwrap_or_else(|| panic!("{id:?} never applied {operation:?}"))
            .response
            .clone()
    }

    fn applied_slot(&self, id: NodeId, operation: OperationId) -> Slot {
        self.node(id)
            .applied()
            .iter()
            .find(|applied| applied.operation_id == operation)
            .expect("the node applied the operation")
            .slot
    }

    /// The fence word a node's directory carries, read straight off the
    /// disk the node does not own.
    fn fence_word(&self, id: NodeId) -> Option<String> {
        let dir = self.root.join(format!("node{}", self.index_of(id)));
        std::fs::read_to_string(dir.join("fence"))
            .ok()
            .map(|text| text.trim().to_owned())
    }

    /// Whether a node's directory carries a state file.
    fn has_state_file(&self, id: NodeId) -> bool {
        let dir = self.root.join(format!("node{}", self.index_of(id)));
        dir.join("locks.state").exists()
    }

    /// A live primary to propose to, named by a live member.
    fn a_live_primary(&self) -> NodeId {
        self.live_ids()
            .iter()
            .copied()
            .filter_map(|id| self.node(id).primary())
            .find(|primary| self.is_up(*primary))
            .expect("a live member names a live primary")
    }
}

/// One grant: a set, proposed to a live primary and settled.
fn client_grant(cluster: &mut Cluster, client: ClientId, lock_id: u64, id: u64) -> Response {
    let (operation, payload) = (
        op_id(id),
        serde_json::to_vec(&json!({
            "op": "set",
            "client": client,
            "lock_id": lock_id,
            "window": LEASE_WINDOW,
        }))
        .expect("the request encodes"),
    );
    let primary = cluster.a_live_primary();
    cluster.propose(primary, operation, &payload);
    let bytes = cluster.applied_response(n(0), operation);
    serde_json::from_slice(&bytes).expect("the reply decodes")
}

/// One observation: a get, proposed to a live primary and settled.
fn client_observation(cluster: &mut Cluster, lock_id: u64, id: u64) -> OperationId {
    let (operation, payload) = (
        op_id(id),
        serde_json::to_vec(&json!({ "op": "get", "lock_id": lock_id }))
            .expect("the request encodes"),
    );
    let primary = cluster.a_live_primary();
    cluster.propose(primary, operation, &payload);
    operation
}

#[test]
fn the_join_gossip_fans_out_to_the_configurations_members() {
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    cluster
        .node_mut(n(1))
        .gossip()
        .expect("the gossip datagram composes");
    assert!(
        cluster.bus.sent(n(1), Tag::GossipRequest),
        "the joiner half went to the wire"
    );
    cluster.quiesce();
    // The gossip is answered, and the cluster is unharmed by it.
    assert!(
        cluster
            .bus
            .queue
            .borrow()
            .iter()
            .all(|envelope| envelope.to != n(1)),
        "the answer was delivered, none left standing for the joiner"
    );
    for id in cluster.live_ids() {
        assert_eq!(cluster.status(id), Status::Normal, "{id:?} stays normal");
    }
}

#[test]
fn the_graceful_stop_flushes_state_and_lands_the_clean_word() {
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    let client_a = ClientId { msb: 2, lsb: 1 };
    let first = client_grant(&mut cluster, client_a, 1, 1);
    let Response::Set {
        granted: true,
        lease: Some(granted),
        ..
    } = &first
    else {
        panic!("the grant stands: {first:?}");
    };
    // A backup stops: the halt begins, the table flushes eagerly in the
    // foreground, the clean word lands last.
    cluster.node_mut(n(2)).stop().expect("the stop flushes");
    assert_eq!(
        cluster.fence_word(n(2)).as_deref(),
        Some("flushed"),
        "the clean word is the stop's last write"
    );
    assert!(
        cluster.has_state_file(n(2)),
        "the eager flush wrote the file"
    );
    // The file the stop wrote loads back exactly: the clean path's
    // loadable file, the law's one grant of trust.
    let dir = cluster.root.join(format!("node{}", cluster.index_of(n(2))));
    let store = FileStateStore::new(&dir.join("locks"));
    let loaded = store.load().expect("the load never refuses");
    let snapshot = loaded.expect("the flushed snapshot is there");
    assert_eq!(snapshot.locks.len(), 1, "the table is the one lease");
    let lease = snapshot.locks.get(&1).expect("lock 1 is stored");
    assert_eq!(lease, granted, "the flushed record is the granted record");
}

#[test]
fn the_fence_words_round_trip_and_a_torn_file_reads_unproven() {
    let dir = temp_root();
    std::fs::create_dir_all(&dir).expect("the fence directory creates");
    let path = dir.join("fence");
    let mut fence = FileBootFence::new(&path);
    assert_eq!(
        fence.read().expect("the read never refuses"),
        None,
        "no fence was ever written: the first life"
    );
    for word in [
        uvrr::demo::FenceWord::Stopping,
        uvrr::demo::FenceWord::Stopped,
        uvrr::demo::FenceWord::Flushed,
        uvrr::demo::FenceWord::Stopping,
    ] {
        fence.write(word).expect("the forced write lands");
        assert_eq!(
            fence.read().expect("the read never refuses"),
            Some(word),
            "the word reads back as it was written"
        );
    }
    std::fs::write(&path, b"not-a-word").expect("the torn file writes");
    assert_eq!(
        fence.read().expect("the read never refuses"),
        Some(uvrr::demo::FenceWord::Stopping),
        "a word the fence did not write reads as the unproven one"
    );
}

#[test]
fn the_state_store_round_trips_and_refuses_rot() {
    let dir = temp_root();
    std::fs::create_dir_all(&dir).expect("the state directory creates");
    let mut store = FileStateStore::new(&dir.join("locks"));
    assert_eq!(
        store.load().expect("the load never refuses"),
        None,
        "an absent file is the cold-start answer"
    );
    let snapshot = uvrr::demo::StateSnapshot::default();
    store.flush(&snapshot).expect("the eager flush lands");
    let loaded = store.load().expect("the load never refuses");
    assert_eq!(loaded, Some(snapshot), "the round trip is exact");

    let path = dir.join("locks.state");
    let good = std::fs::read(&path).expect("the framed file reads");
    // A torn tail: the length prefix disagrees with the bytes present.
    std::fs::write(&path, &good[..good.len() - 3]).expect("the torn tail writes");
    assert_eq!(
        store.load().expect("the load never refuses"),
        None,
        "a torn tail is a refusal, never a half-decoded table"
    );
    // A rotted payload byte: the checksum refuses it.
    let mut rotted = good.clone();
    let last_payload = rotted.len() - 5;
    rotted[last_payload] ^= 0xFF;
    std::fs::write(&path, &rotted).expect("the rotted file writes");
    assert_eq!(
        store.load().expect("the load never refuses"),
        None,
        "a rotted checksum is a refusal"
    );
    // A version this build does not know.
    let mut foreign = good;
    foreign[0] = 99;
    std::fs::write(&path, &foreign).expect("the foreign file writes");
    assert_eq!(
        store.load().expect("the load never refuses"),
        None,
        "an unknown version byte is a refusal"
    );
}

#[test]
fn the_gossip_datagram_is_wire_valid() {
    let frame = uvrr::demo::gossip_datagram(2, 7);
    let message = Message::unpack_from(&frame).expect("the frame decodes back");
    assert_eq!(message.header.tag, Tag::GossipRequest);
    assert_eq!(message.header.view.era, Era(2));
    assert_eq!(message.header.view.view.0, 7);
    assert_eq!(message.header.slot, Slot::NONE);
}

/// The crash-reincarnation story, end to end.
#[test]
fn a_crash_at_a_named_commit_reincarnates_and_the_locks_survive() {
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();

    let client_a = ClientId { msb: 1, lsb: 1 };
    let client_b = ClientId { msb: 1, lsb: 2 };
    let client_c = ClientId { msb: 1, lsb: 3 };

    // Slot 3: the first grant.
    let first = client_grant(&mut cluster, client_a, 1, 1);
    let Response::Set {
        granted: true,
        lease: Some(granted),
        ..
    } = &first
    else {
        panic!("the first grant stands: {first:?}");
    };
    assert_eq!(granted.holder, client_a);
    assert_eq!(granted.lease_id, 1);

    // Slot 4: a second grant on a second lock.
    let second = client_grant(&mut cluster, client_b, 2, 2);
    let Response::Set {
        granted: true,
        lease: Some(_),
        ..
    } = &second
    else {
        panic!("the second grant stands: {second:?}");
    };

    // Slot 5: the observation. Every node applies it, and the chosen
    // node fakes its death at exactly this commit: the hook fired, and
    // the crash will write nothing.
    let observation = client_observation(&mut cluster, 1, 3);
    assert!(
        cluster.crash_fired.load(Ordering::SeqCst),
        "the hook fired at slot 5 on the chosen node"
    );
    for id in cluster.live_ids() {
        assert_eq!(cluster.applied_slot(id, observation), Slot(5));
        let bytes = cluster.applied_response(id, observation);
        let reply: Response = serde_json::from_slice(&bytes).expect("the observation decodes");
        let Response::Get {
            lease: Some(lease), ..
        } = &reply
        else {
            panic!("the lock stands before the crash: {reply:?}");
        };
        assert_eq!(lease, granted, "the lease is the same record everywhere");
    }

    // The crash is a crash: no state file was ever written, and the
    // fence still reads the anchor the first boot wrote.
    cluster.crash(n(2));
    assert!(!cluster.has_state_file(n(2)), "a crash never flushes state");
    assert_eq!(
        cluster.fence_word(n(2)).as_deref(),
        Some("stopping"),
        "the fence still reads the boot anchor"
    );

    // The ledger remembers what the dead identity never answered: the
    // leader's traffic to the crashed node stands un-acked, and the
    // relay set for the ballot carries it, byte-identical.
    let leader_view = cluster.node(n(0));
    let ballot = (leader_view.era().0, 0);
    assert!(
        cluster.node(n(0)).unacknowledged() > 0,
        "the dead node's traffic stands un-acked"
    );
    let relayed = cluster.node(n(0)).relay(ballot.0, ballot.1);
    assert!(
        relayed.iter().any(|datagram| datagram.to == n(2).0),
        "the relay set carries the dead node's traffic"
    );

    // The survivors keep committing: one more grant the crashed node
    // never heard.
    let _third = client_grant(&mut cluster, client_c, 3, 4);
    assert_eq!(cluster.committed(n(0)), Slot(6));
    assert_eq!(cluster.committed(n(1)), Slot(6));

    // The rebirth: the fence's verdict, the bump, the announcement.
    let bumped = cluster.reincarnate(n(2));
    assert_eq!(Some(bumped), n(2).next_life(), "the pair is one past");
    assert!(
        cluster.bus.sent(bumped, Tag::Reincarnation),
        "the announcement went to the wire"
    );
    cluster.quiesce();

    // The leader's ack and the first forced batch: era 2 committed, the
    // old identity at weight 0, the new identity joined at 0, and the
    // missed range pushed: the reborn node is current without a fetch.
    for id in [n(0), n(1)] {
        assert_eq!(cluster.era(id), Era(2), "{id:?} folded the first batch");
    }
    assert_eq!(
        cluster.membership(n(0)),
        vec![(n(0), 1), (n(1), 1), (bumped, 0), (n(2), 0)],
        "the first forced batch's membership"
    );
    assert_eq!(
        cluster.committed(bumped),
        cluster.committed(n(0)),
        "the reborn node synced to the committed frontier"
    );
    assert!(cluster.committed(n(0)) >= Slot(6), "the missed slot landed");
    assert_eq!(
        cluster.node(bumped).state_load(),
        StateLoad::Distrusted,
        "a crashed boot distrusts the state file, whatever sits in it"
    );

    // The next forced batch waits for the view change into era 2, then
    // the re-announce, then the promotion era: the old identity evicted,
    // the new identity at weight 1.
    cluster.drive_view_change();
    cluster.announce(bumped, n(2));
    for id in [n(0), n(1)] {
        assert_eq!(cluster.era(id), Era(3), "{id:?} folded the promotion era");
    }
    assert_eq!(
        cluster.membership(n(0)),
        vec![(n(0), 1), (n(1), 1), (bumped, 1)],
        "the rejoin is complete"
    );

    // The reborn node seats: a member at voting weight, ticking the
    // full protocol.
    cluster.drive_view_change();
    assert_eq!(
        cluster.status(bumped),
        Status::Normal,
        "the reincarnation completed"
    );

    // The closing observation: the lock the story started with, read
    // through the replication path. The survivors answer identically,
    // and the lease is the record it always was: the locks are intact.
    let closing = client_observation(&mut cluster, 1, 5);
    for id in [n(0), n(1)] {
        let bytes = cluster.applied_response(id, closing);
        let reply: Response = serde_json::from_slice(&bytes).expect("the closing reply decodes");
        let Response::Get {
            lease: Some(lease), ..
        } = &reply
        else {
            panic!("the lock survived the crash and the rejoin at {id:?}: {reply:?}");
        };
        assert_eq!(lease, granted, "the lease is the record it always was");
    }
    assert_eq!(
        cluster.applied_response(n(0), closing),
        cluster.applied_response(n(1), closing),
        "the survivors answered identically"
    );

    // The reborn node is caught up on the protocol, and its answer is
    // the least environment's honest one: the law distrusted its state
    // file and nothing re-delivers what it had already applied, so its
    // table is empty where the survivors hold the lease.
    assert_eq!(cluster.committed(bumped), cluster.committed(n(0)));
    let reborn_bytes = cluster.applied_response(bumped, closing);
    let reborn_reply: Response =
        serde_json::from_slice(&reborn_bytes).expect("the reborn reply decodes");
    let Response::Get { lease: None, .. } = &reborn_reply else {
        panic!("the reborn table is the law's empty one: {reborn_reply:?}");
    };

    // The lease clock is the replicated log's own progress: a bounded run
    // of committed observations past the expiry retires the lease,
    // identically on the survivors.
    while cluster.committed(n(0)).0 <= 3 + LEASE_WINDOW {
        let filler = op_id(cluster.committed(n(0)).0 + 100);
        let payload =
            serde_json::to_vec(&json!({ "op": "get", "lock_id": 1 })).expect("the request encodes");
        let primary = cluster.a_live_primary();
        cluster.propose(primary, filler, &payload);
    }
    let expired = client_observation(&mut cluster, 1, 6);
    for id in [n(0), n(1)] {
        let bytes = cluster.applied_response(id, expired);
        let reply: Response = serde_json::from_slice(&bytes).expect("the expired reply decodes");
        let Response::Get { lease: None, .. } = &reply else {
            panic!("the lease expired on the replicated clock at {id:?}: {reply:?}");
        };
    }
}
