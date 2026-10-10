//! The locks demo's harness: the crash-reincarnation story end to end,
//! and the seams under it.
//!
//! Three lock nodes on an in-memory transport, one single-threaded loop
//! per node. A client takes locks, a chosen node fakes its death at a
//! named commit, the crash writes nothing, the reborn identity distrusts
//! its stale state file by the fence's verdict, rejoins, and the closing
//! observation returns the same lease bytes from every node. The seams
//! are proved beside it, each through its own public surface.

#![cfg(feature = "demo-locks")]

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde_json::{Value, json};

use uvrr::demo::{
    Applied, BootFence, BootRefusal, ClientId, CommitHook, DemoDisk, DemoNode, FenceWord,
    FileBootFence, FileStateStore, Lease, NoOpHook, Response, Service, StateLoad, StateSnapshot,
    StateStore, Transport,
};
use uvrr::ids::{CrashCounter, Era, NodeId, Operation, OperationId, Slot, SystemId};
use uvrr::message::Message;
use uvrr::progress::Status;
use uvrr::replica::ViewChangeKnobs;
use uvrr::wire::{Tag, Unpack};

/// The suspicion budget in ticks: a node fences into the next view after
/// more than three ticks of primary silence (S4). Logical, never a clock.
const TIMEOUT: u64 = 3;

/// The lease window in committed slots: long enough to survive the
/// rejoin's choreography, short enough that a bounded run expires it.
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

/// The in-memory transport: one shared bus, explicit queues, no loss and
/// no reordering, with a route log that makes a fan-out assertable.
#[derive(Clone)]
struct Bus {
    queue: Rc<RefCell<VecDeque<Envelope>>>,
    log: Rc<RefCell<HashSet<(NodeId, NodeId, Tag)>>>,
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
            log: Rc::new(RefCell::new(HashSet::new())),
        }
    }

    /// Whether one node put one tag on the wire at all.
    fn sent(&self, from: NodeId, tag: Tag) -> bool {
        self.log
            .borrow()
            .iter()
            .any(|&(f, _, t)| f == from && t == tag)
    }

    /// Whether one node put one tag on the wire to one peer, so a
    /// broadcast is assertable peer by peer.
    fn sent_to(&self, from: NodeId, to: NodeId, tag: Tag) -> bool {
        self.log.borrow().contains(&(from, to, tag))
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
        self.bus.queue.borrow_mut().push_back(Envelope {
            from: self.own,
            to,
            message: message.clone(),
        });
        self.bus
            .log
            .borrow_mut()
            .insert((self.own, to, message.header.tag));
    }
}

/// The commit hook the crash decision hangs on: no verdict, and only the
/// driver ever reads its flag.
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

/// A fresh temporary directory: the process id plus a counter is unique.
fn temp_dir() -> PathBuf {
    let unique = ROOT_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("uvrr-demo-locks-{}-{unique}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("the test directory creates");
    dir
}

/// One node's directory, created. Idempotent, so a reader of a crashed
/// node's files asks for it the way a writer does.
fn node_dir(root: &Path, index: usize) -> PathBuf {
    let dir = root.join(format!("node{index}"));
    std::fs::create_dir_all(&dir).expect("the node directory creates");
    dir
}

/// The demo cluster: the live nodes, the bus, and the disks a crash has
/// recorded. Every protocol decision lives in the demo module; every
/// assertion here reads its public surface.
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
    /// The genesis nodes, each a first life over its own directory.
    fn provision(size: usize) -> Self {
        let bus = Bus::new();
        let genesis: Vec<NodeId> = (0..size as u32).map(n).collect();
        let root = temp_dir();
        let crash_fired = Arc::new(AtomicBool::new(false));
        let mut nodes = Vec::with_capacity(size);
        let mut disks = Vec::with_capacity(size);
        let mut slot_of = BTreeMap::new();
        for (index, &id) in genesis.iter().enumerate() {
            let bus = bus.clone();
            let dir = node_dir(&root, index);
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
                Box::new(MemTransport { own: id, bus }),
                hooks,
                Self::knobs(),
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
            knobs: Self::knobs(),
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

    /// The directory one node's files live in.
    fn dir_of(&self, id: NodeId) -> PathBuf {
        node_dir(&self.root, self.index_of(id))
    }

    /// Up by identity, never by slot: a crashed identity's slot holds its
    /// reborn successor, and datagrams to the dead one address nobody.
    fn is_up(&self, id: NodeId) -> bool {
        self.nodes[self.index_of(id)]
            .as_ref()
            .is_some_and(|node| node.own() == id)
    }

    /// One identity per live slot, never a superseded predecessor.
    fn live_ids(&self) -> Vec<NodeId> {
        self.slot_of
            .iter()
            .filter(|&(id, _)| self.is_up(*id))
            .map(|(&id, _)| id)
            .collect()
    }

    /// One bounded, deterministic wait: every live node drains the bus
    /// until a full pass delivers nothing and nothing live is pending.
    fn quiesce(&mut self) {
        loop {
            let mut delivered = 0;
            for node in self.nodes.iter_mut().flatten() {
                delivered += node.run().len();
            }
            let bus = self.bus.clone();
            let pending = bus.queue.borrow().iter().any(|e| self.is_up(e.to));
            if delivered == 0 && !pending {
                break;
            }
        }
    }

    /// Advances the run's one logical clock by one and settles. The clock
    /// is the host's observation (S4), one for the whole run.
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

    /// The genesis primary promotes itself; the backups adopt its view.
    fn bootstrap(&mut self) {
        self.tick_all();
        for id in self.live_ids() {
            assert_eq!(self.status(id), Status::Normal, "{id:?} boots normal");
        }
    }

    /// Proposes one operation and settles the cluster.
    fn propose(&mut self, to: NodeId, id: OperationId, payload: &[u8]) {
        self.node_mut(to)
            .propose(Operation {
                id,
                payload: payload.into(),
            })
            .unwrap_or_else(|refusal| panic!("the proposal refused: {refusal:?}"));
        self.quiesce();
    }

    /// Crashes a node: its volatile state dies and nothing is written.
    fn crash(&mut self, id: NodeId) {
        let index = self.index_of(id);
        let node = self.nodes[index]
            .take()
            .unwrap_or_else(|| panic!("{id:?} is already down"));
        self.disks[index] = Some(node.record_disk());
    }

    /// Reincarnates a crashed node over its recorded disk. Returns the
    /// bumped identity.
    fn reincarnate(&mut self, old: NodeId) -> NodeId {
        let index = self.index_of(old);
        let dir = node_dir(&self.root, index);
        let disk = self.disks[index]
            .take()
            .unwrap_or_else(|| panic!("{old:?} has no recorded disk; crash it first"));
        let new = old.next_life().expect("the bump never exhausts in tests");
        let bus = self.bus.clone();
        let node = DemoNode::reincarnate(
            old,
            disk,
            Box::new(FileBootFence::new(&dir.join("fence"))),
            Box::new(FileStateStore::new(&dir.join("locks"))),
            Box::new(MemTransport { own: new, bus }),
            Box::new(NoOpHook),
            self.knobs,
        )
        .unwrap_or_else(|refusal| panic!("the reincarnation refused: {refusal:?}"));
        assert_eq!(node.own(), new, "the bump is taken up");
        // The bumped identity occupies the old life's slot from here on; the
        // superseded identity's datagrams address nobody.
        self.slot_of.insert(new, index);
        self.nodes[index] = Some(node);
        self.quiesce();
        new
    }

    /// A bounded tick sweep every live member of the target view installs.
    fn drive_view_change(&mut self) {
        for _ in 0..=TIMEOUT {
            self.tick_all();
        }
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

    /// The one applied commit a node recorded for an operation.
    fn applied(&self, id: NodeId, operation: OperationId) -> &Applied {
        self.node(id)
            .applied()
            .iter()
            .find(|applied| applied.operation_id == operation)
            .unwrap_or_else(|| panic!("{id:?} never applied {operation:?}"))
    }

    fn applied_response(&self, id: NodeId, operation: OperationId) -> Vec<u8> {
        self.applied(id, operation).response.clone()
    }

    /// The fence word on disk, read off the medium the node does not own.
    fn fence_word(&self, id: NodeId) -> Option<String> {
        std::fs::read_to_string(self.dir_of(id).join("fence"))
            .ok()
            .map(|text| text.trim().to_owned())
    }

    fn has_state_file(&self, id: NodeId) -> bool {
        self.dir_of(id).join("locks.state").exists()
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

/// The reply a node executed for `operation`, decoded.
fn applied_reply(cluster: &Cluster, id: NodeId, operation: OperationId) -> Response {
    let bytes = cluster.applied_response(id, operation);
    serde_json::from_slice(&bytes).expect("the reply decodes")
}

/// The lease a `Get` reply reports, or the panic naming the node.
fn live_lease(reply: &Response, at: NodeId) -> Lease {
    let Response::Get {
        lease: Some(lease), ..
    } = reply
    else {
        panic!("the lock stands at {at:?}: {reply:?}");
    };
    lease.clone()
}

/// The grant flag and the record a `Set` reply carries.
fn set_reply(reply: &Response) -> (bool, Option<Lease>) {
    let Response::Set { granted, lease, .. } = reply else {
        panic!("not a set reply: {reply:?}");
    };
    (*granted, lease.clone())
}

/// The release flag and the record a `Release` reply carries.
fn release_reply(reply: &Response) -> (bool, Option<Lease>) {
    let Response::Release {
        released, lease, ..
    } = reply
    else {
        panic!("not a release reply: {reply:?}");
    };
    (*released, lease.clone())
}

/// The break flag and the keeper a `Break` reply carries.
fn break_reply(reply: &Response) -> (bool, Option<Lease>) {
    let Response::Break { broken, lease, .. } = reply else {
        panic!("not a break reply: {reply:?}");
    };
    (*broken, lease.clone())
}

/// The lease a `Set` reply granted to the client it names, or the panic.
fn granted_lease(reply: &Response, client: ClientId) -> Lease {
    let Response::Set {
        granted: true,
        lease: Some(lease),
        ..
    } = reply
    else {
        panic!("the grant stands for {client:?}: {reply:?}");
    };
    assert_eq!(lease.holder, client, "the grant is the requesting client's");
    lease.clone()
}

/// The client vocabulary as request bodies: a client names a window and
/// never an expiry, so a renewal is a wider `Set`.
fn set_body(client: ClientId, lock_id: u64, window: u64) -> Value {
    json!({ "op": "set", "client": client, "lock_id": lock_id, "window": window })
}

fn release_body(client: ClientId, lock_id: u64, lease_id: u64) -> Value {
    json!({ "op": "release", "client": client, "lock_id": lock_id, "lease_id": lease_id })
}
fn break_body(lock_id: u64) -> Value {
    json!({ "op": "break", "lock_id": lock_id })
}
fn get_body(lock_id: u64) -> Value {
    json!({ "op": "get", "lock_id": lock_id })
}

/// One way of rotting a framed state file: why, and the mutation.
type Rot = (&'static str, fn(&mut Vec<u8>));

/// One grant, proposed to a live primary and settled. The reply comes off
/// that primary, so the proposer is proved to have applied its request.
fn client_grant(cluster: &mut Cluster, client: ClientId, lock_id: u64, id: u64) -> Response {
    let operation = op_id(id);
    let primary = cluster.a_live_primary();
    let payload = serde_json::to_vec(&set_body(client, lock_id, LEASE_WINDOW)).expect("encodes");
    cluster.propose(primary, operation, &payload);
    applied_reply(cluster, primary, operation)
}

/// One observation, settled. The identity comes back because every later
/// assertion reads the reply off each node by it.
fn client_observation(cluster: &mut Cluster, lock_id: u64, id: u64) -> OperationId {
    let operation = op_id(id);
    let primary = cluster.a_live_primary();
    let payload = serde_json::to_vec(&get_body(lock_id)).expect("encodes");
    cluster.propose(primary, operation, &payload);
    operation
}

#[test]
fn the_join_gossip_is_wire_valid_and_fans_out_to_every_member() {
    // The core's own frame at the node's current view, no frontier named.
    let frame = uvrr::demo::gossip_datagram(2, 7);
    let packed = Message::unpack_from(&frame).expect("the frame decodes back");
    assert_eq!(packed.header.tag, Tag::GossipRequest);
    assert_eq!(packed.header.view.era, Era(2));
    assert_eq!(packed.header.view.view.0, 7);
    assert_eq!(packed.header.slot, Slot::NONE);

    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    cluster.node_mut(n(1)).gossip().expect("composes");
    // The fan-out is the claim: every other member, nobody outside it.
    for peer in [n(0), n(2)] {
        let reached = cluster.bus.sent_to(n(1), peer, Tag::GossipRequest);
        assert!(reached, "{peer:?} got it");
    }
    cluster.quiesce();
    // The gossip is answered, and the cluster is unharmed by it.
    let answered = cluster.bus.queue.borrow().iter().all(|e| e.to != n(1));
    assert!(answered, "the answer landed");
    for id in cluster.live_ids() {
        assert_eq!(cluster.status(id), Status::Normal, "{id:?} stays normal");
    }
}

#[test]
fn the_graceful_stop_flushes_state_and_lands_the_clean_word() {
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    let client = ClientId { msb: 2, lsb: 1 };
    let granted = granted_lease(&client_grant(&mut cluster, client, 1, 1), client);
    // A backup stops: halt, eager flush in the foreground, clean word.
    cluster.node_mut(n(2)).stop().expect("the stop flushes");
    let word = cluster.fence_word(n(2));
    assert_eq!(word.as_deref(), Some("flushed"), "the stop's last write");
    assert!(cluster.has_state_file(n(2)), "the eager flush wrote it");
    // The file the stop wrote loads back exactly: the law's one grant.
    let dir = cluster.dir_of(n(2));
    let store = FileStateStore::new(&dir.join("locks"));
    let snapshot = store.load().expect("no refusal").expect("it is there");
    let one = BTreeMap::from([(1, granted)]);
    assert_eq!(snapshot.locks, one, "the flushed table is one lease");
    // The clean word is the stop's alone: a boot that reads it refuses.
    let bus = cluster.bus.clone();
    let boot = DemoNode::reincarnate(
        n(2),
        cluster.node(n(2)).record_disk(),
        Box::new(FileBootFence::new(&dir.join("fence"))),
        Box::new(FileStateStore::new(&dir.join("locks"))),
        Box::new(MemTransport { own: n(2), bus }),
        Box::new(NoOpHook),
        cluster.knobs,
    );
    let refusal = boot.err();
    let clean = matches!(refusal, Some(BootRefusal::NotCrashed));
    assert!(clean, "{refusal:?}");
}

/// The lock table's whole state machine, and the reply each transition
/// must produce.
#[test]
fn the_lock_tables_transitions_reply_as_the_vocabulary_states() {
    let holder = ClientId { msb: 7, lsb: 1 };
    let stranger = ClientId { msb: 7, lsb: 2 };
    let mut table = Service::default();
    // One committed operation at the slot it applies at, and its reply.
    let mut at = |clock: u64, body: Value| -> Response {
        let payload = serde_json::to_vec(&body).expect("encodes");
        let bytes = table.execute(clock, &payload).expect("the table executes");
        serde_json::from_slice(&bytes).expect("the reply decodes")
    };
    // The keeper a break leaves behind: bumped, nil holder, never live.
    let keeper = |g: u64| Lease {
        lease_id: g,
        holder: ClientId::nil(),
        expiry: 0,
        window: 0,
    };

    // Generation one, stamped clock plus window on the replicated clock.
    let first = at(3, set_body(holder, 1, LEASE_WINDOW));
    let granted = granted_lease(&first, holder);
    assert_eq!(granted.lease_id, 1, "the first grant is generation one");
    assert_eq!(granted.expiry, 3 + LEASE_WINDOW, "clock plus window");
    assert_eq!(granted.window, LEASE_WINDOW, "the window asked");
    let Response::Set { executed_at, .. } = &first else {
        panic!("a set request answered with a set reply")
    };
    assert_eq!(*executed_at, 3, "the reply names its own clock");

    // A stranger's grant is refused, and the incumbent comes back.
    let refused = at(5, set_body(stranger, 1, LEASE_WINDOW));
    let held = (false, Some(granted.clone()));
    assert_eq!(set_reply(&refused), held, "refused, incumbent returned");

    // The holder's own renewal is the same lease extended.
    let body = set_body(holder, 1, LEASE_WINDOW + 8);
    let renewed = granted_lease(&at(7, body), holder);
    assert_eq!(renewed.lease_id, granted.lease_id, "the generation stands");
    assert!(renewed.expiry > granted.expiry, "the expiry moved");

    // Neither a stale generation nor another client retires anything.
    let keeps = (false, Some(renewed.clone()));
    let stale = at(9, release_body(holder, 1, renewed.lease_id + 99));
    assert_eq!(release_reply(&stale), keeps, "nothing was retired");
    let wrong = at(11, release_body(stranger, 1, renewed.lease_id));
    assert_eq!(release_reply(&wrong), keeps, "nor by another client");

    // The holder's own generation retires it, and a free table is no
    // failure either: there was nothing to retire.
    let mine = at(13, release_body(holder, 1, renewed.lease_id));
    let none = (true, None);
    assert_eq!(release_reply(&mine), none, "the holder retires it");
    let free = at(15, release_body(holder, 1, renewed.lease_id));
    let none = (true, None);
    assert_eq!(release_reply(&free), none, "a free lock is no failure");
    let vacant = at(17, break_body(1));
    assert_eq!(break_reply(&vacant), (false, None), "and nothing to break");

    // The break is unconditional, and bumps the keeper it finds again.
    let body = set_body(holder, 1, LEASE_WINDOW);
    let held_lease = granted_lease(&at(19, body.clone()), holder);
    let (broke, kept) = break_reply(&at(21, break_body(1)));
    assert!(broke, "a record stood to break");
    let one = keeper(held_lease.lease_id + 1);
    assert_eq!(kept, Some(one), "the bumped keeper is what stands");
    let live = Response::Get {
        lock_id: 1,
        lease: None,
        executed_at: 22,
    };
    assert_eq!(at(22, get_body(1)), live, "the keeper is never live");
    let (_, again) = break_reply(&at(23, break_body(1)));
    let two = keeper(held_lease.lease_id + 2);
    assert_eq!(again, Some(two), "the next break bumps the keeper again");

    // A grant over a keeper bumps once more, as a release does.
    let taken = granted_lease(&at(24, body), holder);
    let bumped = held_lease.lease_id + 3;
    assert_eq!(taken.lease_id, bumped, "the keeper was bumped once more");
}

#[test]
fn the_fence_words_round_trip_and_a_torn_file_reads_unproven() {
    let path = temp_dir().join("fence");
    let mut fence = FileBootFence::new(&path);
    let read = |fence: &FileBootFence| fence.read().expect("the read never refuses");
    assert_eq!(read(&fence), None, "no fence yet: the first life");
    for word in [
        FenceWord::Stopping,
        FenceWord::Stopped,
        FenceWord::Flushed,
        FenceWord::Stopping,
    ] {
        fence.write(word).expect("the forced write lands");
        assert_eq!(read(&fence), Some(word), "the word reads back as written");
    }
    // Content the fence did not write reads as the anchor, never clean.
    let torn: [&[u8]; 4] = [b"not-a-word", b"", b"FLUSHED", b"flushed\n\nflushed"];
    for bytes in torn {
        std::fs::write(&path, bytes).expect("the torn file writes");
        assert_eq!(read(&fence), Some(FenceWord::Stopping), "{bytes:?}");
    }
    // A file that is not text at all is a classification, never an error.
    std::fs::write(&path, [0xFF, 0xFE]).expect("the binary file writes");
    assert_eq!(read(&fence), Some(FenceWord::Stopping), "not text either");
}

#[test]
fn the_state_store_round_trips_rotates_and_refuses() {
    let dir = temp_dir();
    let path = dir.join("locks.state");
    let mut store = FileStateStore::new(&dir.join("locks"));
    let load = |store: &FileStateStore| store.load().expect("the load never refuses");
    assert_eq!(load(&store), None, "an absent file is the cold start");
    let empty = StateSnapshot::default();
    store.flush(&empty).expect("the eager flush lands");
    assert_eq!(load(&store), Some(empty), "the round trip is exact");

    let good = std::fs::read(&path).expect("the framed file reads");
    // Every content problem is a refusal, never an `Err`.
    let rot: [Rot; 4] = [
        ("a torn tail", |b| b.truncate(b.len() - 3)),
        ("a rotted payload byte", |b| {
            *b.iter_mut().nth_back(4).unwrap() ^= 0xFF
        }),
        ("a rotted checksum byte", |b| *b.last_mut().unwrap() ^= 0xFF),
        ("an unknown version", |b| b[0] = 99),
    ];
    for (reason, corrupt) in rot {
        let mut bytes = good.clone();
        corrupt(&mut bytes);
        std::fs::write(&path, &bytes).expect("the rotted file writes");
        assert_eq!(load(&store), None, "{reason} is a refusal");
    }

    // Rotation replaces the rotted file whole. An append would leave the
    // length prefix disagreeing with the bytes present, which the reader
    // refuses, so a load that succeeds IS the proof of replacement.
    let lease = Lease {
        lease_id: 4,
        holder: ClientId { msb: 3, lsb: 1 },
        expiry: 90,
        window: LEASE_WINDOW,
    };
    let rotated = StateSnapshot {
        locks: BTreeMap::from([(1, lease)]),
    };
    store.flush(&rotated).expect("the rotation lands");
    assert_eq!(load(&store), Some(rotated), "it replaced the rotted one");
}

#[test]
fn a_crash_at_a_named_commit_reincarnates_and_the_locks_survive() {
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    let client_a = ClientId { msb: 1, lsb: 1 };
    let client_b = ClientId { msb: 1, lsb: 2 };
    let client_c = ClientId { msb: 1, lsb: 3 };

    // Slot 3: the first grant.
    let granted = granted_lease(&client_grant(&mut cluster, client_a, 1, 1), client_a);
    assert_eq!(granted.lease_id, 1, "the first grant is generation one");

    // Slot 4: a second grant, on a second lock.
    let second = client_grant(&mut cluster, client_b, 2, 2);
    let (granted_it, _) = set_reply(&second);
    assert!(granted_it, "the second grant stands");

    // Slot 5: every node applies it, and the chosen node dies here.
    let observation = client_observation(&mut cluster, 1, 3);
    let fired = cluster.crash_fired.load(Ordering::SeqCst);
    assert!(fired, "the hook fired at slot 5");
    for id in cluster.live_ids() {
        let slot = cluster.applied(id, observation).slot;
        assert_eq!(slot, Slot(5), "{id:?} applied it at five");
        let reply = applied_reply(&cluster, id, observation);
        assert_eq!(live_lease(&reply, id), granted, "one record everywhere");
    }

    // No state file was written, and the fence still reads the anchor.
    cluster.crash(n(2));
    assert!(!cluster.has_state_file(n(2)), "a crash never flushes state");
    let anchor = cluster.fence_word(n(2));
    assert_eq!(anchor.as_deref(), Some("stopping"), "the anchor stands");

    // The ledger holds what the dead identity never answered.
    let leader = cluster.node(n(0));
    let unacked = leader.unacknowledged() > 0;
    assert!(unacked, "the dead node's traffic stands");
    let relayed = leader.relay(leader.era().0, 0);
    let carried = relayed.iter().any(|d| d.to == n(2).0);
    assert!(carried, "the relay set carries it");

    // The survivors keep committing: one grant the dead node never heard.
    let _third = client_grant(&mut cluster, client_c, 3, 4);
    assert_eq!(cluster.committed(n(0)), Slot(6));
    assert_eq!(cluster.committed(n(1)), Slot(6));

    // The rebirth: the fence's verdict, the bump, the announcement.
    let bumped = cluster.reincarnate(n(2));
    assert_eq!(Some(bumped), n(2).next_life(), "the pair is one past");
    let announced = cluster.bus.sent(bumped, Tag::Reincarnation);
    assert!(announced, "the announcement went out");

    // The first forced batch: era 2, the old identity at weight 0, the new
    // one joined at 0, and the missed range pushed, so the reborn node is
    // current without a fetch.
    let batch = vec![(n(0), 1), (n(1), 1), (bumped, 0), (n(2), 0)];
    for id in [n(0), n(1)] {
        assert_eq!(cluster.era(id), Era(2), "{id:?} folded the first batch");
        assert_eq!(cluster.membership(id), batch, "{id:?} holds that batch");
    }
    let synced = cluster.committed(bumped) == cluster.committed(n(0));
    assert!(synced, "the reborn node synced to the frontier");
    assert!(cluster.committed(n(0)) >= Slot(6), "the missed slot landed");
    let load = cluster.node(bumped).state_load();
    assert_eq!(load, StateLoad::Distrusted, "the state file is distrusted");

    // Then the view change, the re-announce and the promotion era: the old
    // identity evicted, the new one at weight 1.
    cluster.drive_view_change();
    let pair = cluster.node_mut(bumped).announce(n(2));
    assert!(pair.is_ok(), "the pair announces: {pair:?}");
    cluster.quiesce();
    let promoted = vec![(n(0), 1), (n(1), 1), (bumped, 1)];
    for id in [n(0), n(1)] {
        assert_eq!(cluster.era(id), Era(3), "{id:?} folded the promotion");
        assert_eq!(cluster.membership(id), promoted, "{id:?} and its rejoin");
    }

    // The reborn node seats, ticking the full protocol.
    cluster.drive_view_change();
    let seated = cluster.status(bumped) == Status::Normal;
    assert!(seated, "the reincarnation completed");

    // The closing observation, read through the replication path: the
    // survivors answer identically and the lease is intact.
    let closing = client_observation(&mut cluster, 1, 5);
    for id in [n(0), n(1)] {
        let reply = applied_reply(&cluster, id, closing);
        assert_eq!(live_lease(&reply, id), granted, "it survived at {id:?}");
    }
    let left = cluster.applied_response(n(0), closing);
    let right = cluster.applied_response(n(1), closing);
    assert_eq!(left, right, "the survivors answered identically");

    // The reborn node's answer is the least environment's honest one: the
    // law distrusted its state file and nothing re-delivers what it had
    // applied, so its table is empty where the survivors hold the lease.
    let caught_up = cluster.committed(bumped) == cluster.committed(n(0));
    assert!(caught_up, "the reborn node is caught up");
    let reborn = applied_reply(&cluster, bumped, closing);
    let empty = matches!(&reborn, Response::Get { lease: None, .. });
    assert!(empty, "the reborn table is the law's empty one: {reborn:?}");

    // The lease clock is the log's own progress: past the expiry, gone.
    while cluster.committed(n(0)).0 <= 3 + LEASE_WINDOW {
        let filler = cluster.committed(n(0)).0 + 100;
        client_observation(&mut cluster, 1, filler);
    }
    let expired = client_observation(&mut cluster, 1, 6);
    for id in [n(0), n(1)] {
        let reply = applied_reply(&cluster, id, expired);
        let gone = matches!(&reply, Response::Get { lease: None, .. });
        assert!(gone, "expired on the replicated clock: {reply:?}");
    }
}
