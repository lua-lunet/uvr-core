//! Invariant reproductions for the locks demo, driven through the demo's
//! public surface only.
//!
//! Each test names a behaviour the demo's own documentation states, drives
//! it through the public surface the landed `demo_locks` harness uses
//! (propose an operation, read the applied reply), and asserts the stated
//! behaviour. Nothing here reaches into private state.

#![cfg(feature = "demo-locks")]

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use uvrr::demo::{
    ClientId, DemoNode, FileBootFence, FileStateStore, NoOpHook, Request, Response, Service,
    Transport,
};
use uvrr::ids::{CrashCounter, Era, NodeId, Operation, OperationId, Slot, SystemId};
use uvrr::message::Message;
use uvrr::progress::Status;
use uvrr::replica::ViewChangeKnobs;

/// The suspicion budget in ticks, in logical ticks, never a wall clock.
const TIMEOUT: u64 = 3;

fn n(id: u32) -> NodeId {
    NodeId::new(
        SystemId::new((id + 1) as u16).expect("test system ids are small and non-zero"),
        CrashCounter::new(1).expect("one is non-zero"),
    )
}

/// A client identity through the public mint, which refuses the nil pair:
/// every client here is one a real client could have named.
fn client(msb: u64, lsb: u64) -> ClientId {
    ClientId::new(msb, lsb).expect("a test client is not the nil identity")
}

#[derive(Clone)]
struct Bus {
    queue: Rc<RefCell<VecDeque<Envelope>>>,
    chaos: Rc<RefCell<bool>>,
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
            chaos: Rc::new(RefCell::new(false)),
        }
    }

    /// Whether the bus misbehaves on the wire: every datagram is queued
    /// twice and every node drains its own queue newest first.
    fn set_chaos(&self, on: bool) {
        *self.chaos.borrow_mut() = on;
    }

    fn chaotic(&self) -> bool {
        *self.chaos.borrow()
    }
}

/// One node's end of the in-memory bus: no loss, no reordering, the
/// seam the demo's loop drains.
struct MemTransport {
    own: NodeId,
    bus: Bus,
}

impl Transport for MemTransport {
    fn receive(&mut self) -> Option<(NodeId, Message)> {
        let mut queue = self.bus.queue.borrow_mut();
        if self.bus.chaotic() {
            let position = queue.iter().rposition(|envelope| envelope.to == self.own)?;
            let envelope = queue.remove(position)?;
            return Some((envelope.from, envelope.message));
        }
        let position = queue.iter().position(|envelope| envelope.to == self.own)?;
        let envelope = queue.remove(position)?;
        Some((envelope.from, envelope.message))
    }

    fn send(&mut self, to: NodeId, _era: Era, message: &Message) {
        let copies = if self.bus.chaotic() { 2 } else { 1 };
        let mut queue = self.bus.queue.borrow_mut();
        for _ in 0..copies {
            queue.push_back(Envelope {
                from: self.own,
                to,
                message: message.clone(),
            });
        }
    }
}

/// The cluster the harness drives: the live nodes and the bus between
/// them. Every protocol decision lives in the demo module; every
/// assertion reads its public surface.
struct Cluster {
    bus: Bus,
    nodes: Vec<Option<DemoNode>>,
    slot_of: BTreeMap<NodeId, usize>,
    clock: u64,
}

impl Cluster {
    fn provision(size: usize) -> Self {
        let bus = Bus::new();
        let genesis: Vec<NodeId> = (0..size as u32).map(n).collect();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the system clock is after the epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "uvrr-demo-invariants-{}-{nanos}",
            std::process::id()
        ));
        let mut nodes = Vec::with_capacity(size);
        let mut slot_of = BTreeMap::new();
        for (index, &id) in genesis.iter().enumerate() {
            let dir = root.join(format!("node{index}"));
            std::fs::create_dir_all(&dir).expect("the node directory creates");
            let node = DemoNode::first(
                id,
                genesis.clone(),
                Box::new(FileBootFence::new(&dir.join("fence"))),
                Box::new(FileStateStore::new(&dir.join("locks"))),
                Box::new(MemTransport {
                    own: id,
                    bus: bus.clone(),
                }),
                Box::new(NoOpHook),
                Cluster::knobs(),
            )
            .expect("the genesis order provisions");
            slot_of.insert(id, index);
            nodes.push(Some(node));
        }
        Self {
            bus,
            nodes,
            slot_of,
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

    fn node(&self, id: NodeId) -> &DemoNode {
        self.nodes[self.index_of(id)]
            .as_ref()
            .unwrap_or_else(|| panic!("{id:?} is down"))
    }

    fn node_mut(&mut self, id: NodeId) -> &mut DemoNode {
        let index = self.index_of(id);
        self.nodes[index]
            .as_mut()
            .unwrap_or_else(|| panic!("{id:?} is down"))
    }

    fn live_ids(&self) -> Vec<NodeId> {
        self.slot_of.keys().copied().collect()
    }

    /// One bounded, deterministic settle: every live node drains the bus
    /// until a full pass delivers nothing and nothing live-addressed is
    /// pending. No wall clock anywhere.
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
            let pending_to_live = self.bus.queue.borrow().len();
            if delivered == 0 && pending_to_live == 0 {
                break;
            }
        }
    }

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

    fn bootstrap(&mut self) {
        self.tick_all();
        for id in self.live_ids() {
            assert_eq!(
                self.node(id).status(),
                Status::Normal,
                "{id:?} boots normal"
            );
        }
    }

    fn status(&self, id: NodeId) -> Status {
        self.node(id).status()
    }

    fn committed(&self, id: NodeId) -> Slot {
        self.node(id).committed()
    }

    /// One live primary to propose to.
    fn a_live_primary(&self) -> NodeId {
        self.live_ids()
            .iter()
            .copied()
            .filter_map(|id| self.node(id).primary())
            .next()
            .expect("a live member names a live primary")
    }

    /// Proposes one lock request to a live primary and settles. Nothing is
    /// read back: the caller's business.
    fn send_request(&mut self, request: &Request, lsb: u64) -> OperationId {
        let operation = OperationId { msb: 0, lsb };
        let payload = serde_json::to_vec(request).expect("the request encodes");
        let primary = self.a_live_primary();
        self.node_mut(primary)
            .propose(Operation {
                id: operation,
                payload: payload.into(),
            })
            .unwrap_or_else(|refusal| panic!("the proposal refused: {refusal:?}"));
        self.quiesce();
        operation
    }

    /// Proposes one lock request to a live primary and settles, then
    /// reads the reply every live node applied. The whole public surface:
    /// the request goes in as a payload, the answer comes out as the
    /// applied reply bytes.
    fn lock_request(&mut self, request: &Request, lsb: u64) -> Vec<Response> {
        let operation = self.send_request(request, lsb);
        self.live_ids()
            .iter()
            .map(|&id| {
                let bytes = self
                    .node(id)
                    .applied()
                    .iter()
                    .find(|applied| applied.operation_id == operation)
                    .unwrap_or_else(|| panic!("{id:?} never applied {operation:?}"))
                    .response
                    .clone();
                serde_json::from_slice(&bytes).expect("the reply decodes")
            })
            .collect()
    }
}

/// The one lease a `Set` granted, taken from every node's identical
/// reply. Every replica answering the same record is the premise, so a
/// divergence in what they answer is the finding.
fn granted_lease(replies: &[Response]) -> uvrr::demo::Lease {
    let mut leases = Vec::new();
    for reply in replies {
        match reply {
            Response::Set {
                granted: true,
                lease: Some(lease),
                ..
            } => leases.push(lease.clone()),
            other => panic!("every node granted the lock: {other:?}"),
        }
    }
    assert!(
        leases.windows(2).all(|pair| pair[0] == pair[1]),
        "every replica answers the same record: {leases:?}"
    );
    leases[0].clone()
}

/// The locks demo states that a lease's generation is "bumped on every
/// holder change and on a break, so a release naming an older generation
/// retires nothing". The generation is therefore a property of the lock,
/// not of the record currently standing, and a grant that follows a
/// release must not answer with a generation already spent on this lock.
///
/// A granted, a released, a re-granted to a second client: three
/// committed operations through the demo's own request path.
#[test]
fn a_lease_generation_is_never_reused_after_the_holder_releases_the_lock() {
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    let first_holder = client(7, 1);
    let second_holder = client(7, 2);

    // A takes lock 1. The first grant on the lock answers generation 1.
    let first = granted_lease(&cluster.lock_request(
        &Request::Set {
            client: first_holder,
            lock_id: 1,
            window: 32,
        },
        1,
    ));
    assert_eq!(
        first.lease_id, 1,
        "the first grant on the lock is generation 1"
    );

    // A releases it, naming the generation A holds. The release stands.
    let released = cluster.lock_request(
        &Request::Release {
            client: first_holder,
            lock_id: 1,
            lease_id: first.lease_id,
        },
        2,
    );
    for reply in &released {
        let Response::Release { released: true, .. } = reply else {
            panic!("A held the lease and the release stands: {reply:?}");
        };
    }

    // B takes lock 1. The holder changed from A to B, so the generation
    // is bumped on this holder change.
    let second = granted_lease(&cluster.lock_request(
        &Request::Set {
            client: second_holder,
            lock_id: 1,
            window: 32,
        },
        3,
    ));
    assert_eq!(second.holder, second_holder, "B holds the lock");
    assert_eq!(
        second.lease_id, 2,
        "the generation is bumped on every holder change, so a release \
         naming generation 1 cannot retire B's lease"
    );

    // Which is the law the bump exists for: A's late release, still
    // naming generation 1, retires nothing and reports that it retired
    // nothing. The generation it names is spent, so it is not a lease.
    let stale = cluster.lock_request(
        &Request::Release {
            client: first_holder,
            lock_id: 1,
            lease_id: first.lease_id,
        },
        4,
    );
    for reply in &stale {
        let Response::Release {
            released, lease, ..
        } = reply
        else {
            panic!("the reply is a release: {reply:?}");
        };
        assert!(
            !*released,
            "a stale release retires nothing and says so: {reply:?}"
        );
        assert_eq!(
            *lease,
            Some(second.clone()),
            "and the incumbent comes back: {reply:?}"
        );
    }

    // And B's lease is exactly where it was.
    for reply in &cluster.lock_request(&Request::Get { lock_id: 1 }, 5) {
        let Response::Get { lease, .. } = reply else {
            panic!("the reply is a read: {reply:?}");
        };
        assert_eq!(
            *lease,
            Some(second.clone()),
            "the stale release left B's lease standing: {reply:?}"
        );
    }
}

/// The locks demo states of `Response::Release` that it answers "whether
/// the named lease was retired". A release of a lock no client was ever
/// granted retires nothing, so the answer reports that nothing was
/// retired.
#[test]
fn a_release_of_a_lock_no_client_holds_reports_that_nothing_was_retired() {
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    let stranger = client(9, 1);

    // No grant was ever proposed for lock 7, so no lease stands on it.
    let released = cluster.lock_request(
        &Request::Release {
            client: stranger,
            lock_id: 7,
            lease_id: 1,
        },
        1,
    );
    for reply in &released {
        let Response::Release {
            lock_id,
            released: retired,
            lease,
            ..
        } = reply
        else {
            panic!("the reply is a release: {reply:?}");
        };
        assert_eq!(*lock_id, 7, "the release names the lock asked about");
        assert!(
            !*retired,
            "no lease stood on lock 7, so nothing was retired: {reply:?}"
        );
        assert_eq!(*lease, None, "nothing stands and nothing stood");
    }

    // The no-op is idempotent: the second release finds the same absence,
    // retires nothing, and reports the same truth.
    for reply in &cluster.lock_request(
        &Request::Release {
            client: stranger,
            lock_id: 7,
            lease_id: 1,
        },
        2,
    ) {
        assert!(
            matches!(
                reply,
                Response::Release {
                    released: false,
                    lease: None,
                    ..
                }
            ),
            "repeating the no-op changes nothing and still reports it: {reply:?}"
        );
    }

    // And it spent nothing: the lock's first grant is still the first
    // generation, because a release that retired nothing was not a
    // holder change.
    let first = granted_lease(&cluster.lock_request(
        &Request::Set {
            client: stranger,
            lock_id: 7,
            window: 32,
        },
        3,
    ));
    assert_eq!(first.lease_id, 1, "the no-op release spent no generation");
}

/// The locks demo documents `ClientId::nil` as "the cleared keeper
/// record's holder: never live". Outside the crate that claim is now
/// unrepresentability rather than a check at the point of use, and the
/// proof is the type's: the public mint refuses the all-zero pair, the
/// wire decode refuses a request that carries it, and the lock the
/// refused request named is there for the first real client at the lock's
/// first generation.
#[test]
fn the_nil_client_identity_is_never_granted_a_live_lease() {
    // The mint: the all-zero pair is not a client identity, and the mint is
    // the only public way to name one.
    assert!(
        ClientId::new(0, 0).is_none(),
        "the nil identity is the keeper record's placeholder and is never a client"
    );

    // The wire: the request that carries the nil identity does not decode,
    // so it never becomes a request at all. The table is untouched by the
    // refusal, which the next operation's own generation shows.
    let mut table = Service::default();
    let nil = br#"{"op":"set","client":{"msb":0,"lsb":0},"lock_id":3,"window":32}"#;
    let refused = table.execute(3, nil);
    assert!(
        refused.is_err(),
        "the nil identity cannot arrive on the wire: {refused:?}"
    );
    let real = br#"{"op":"set","client":{"msb":7,"lsb":1},"lock_id":3,"window":32}"#;
    let bytes = table.execute(3, real).expect("a lawful client decodes");
    let Response::Set {
        granted,
        lease: Some(lease),
        ..
    } = serde_json::from_slice(&bytes).expect("the reply decodes")
    else {
        panic!("a granted reply carries the lease: {bytes:?}");
    };
    assert!(granted, "the lawful client is granted: {bytes:?}");
    assert_eq!(
        lease.lease_id, 1,
        "the refused request spent nothing on the lock"
    );

    // And the committed path agrees: every replica grants the real client
    // the lock's first generation, and no replica reports a lease for the
    // nil identity, because none of them can be sent one.
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    let holder = client(7, 1);
    let taken = granted_lease(&cluster.lock_request(
        &Request::Set {
            client: holder,
            lock_id: 3,
            window: 32,
        },
        1,
    ));
    assert_eq!(taken.holder, holder, "the real client takes it");
    assert_eq!(taken.lease_id, 1, "and it is the lock's first generation");
}

/// A transport that duplicates every datagram and delivers each node's
/// queue newest first: the demo's own seam, asked for the two faults a
/// datagram network really has. Every replica's applied history must
/// settle to the same slots, the same operations, and the same reply
/// bytes: the lock table's answer is a function of the replicated log,
/// so two replicas of one cluster never disagree about it.
#[test]
fn a_duplicated_and_reordered_datagram_stream_commits_one_identical_history() {
    let mut cluster = Cluster::provision(3);
    cluster.bus.set_chaos(true);
    cluster.bootstrap();
    let holder = client(7, 1);
    let second = client(7, 2);

    cluster.send_request(
        &Request::Set {
            client: holder,
            lock_id: 1,
            window: 32,
        },
        1,
    );
    cluster.send_request(
        &Request::Set {
            client: second,
            lock_id: 2,
            window: 32,
        },
        2,
    );

    // A tick sweep: the suspicion budget's currency, spent on a healthy
    // cluster, is a view change the primaries time out into.
    for _ in 0..=TIMEOUT {
        cluster.tick_all();
    }

    cluster.send_request(
        &Request::Set {
            client: second,
            lock_id: 3,
            window: 32,
        },
        3,
    );

    // Settling. A replica whose slot-5 commit arrives ahead of its
    // prepare holds a frontier behind the primary's until the suspicion
    // budget is spent and the leader's evidence reaches it; the demo has
    // no timers, so the sweep is the harness's own recovery idiom, the
    // one the landed harness drives a view change with. Convergence is
    // asserted after it, never before it.
    for _ in 0..2 {
        for _ in 0..=TIMEOUT {
            cluster.tick_all();
        }
    }

    let history = |id: NodeId| -> Vec<(Slot, OperationId, Vec<u8>)> {
        cluster
            .node(id)
            .applied()
            .iter()
            .map(|applied| (applied.slot, applied.operation_id, applied.response.clone()))
            .collect()
    };
    let first = history(n(0));
    for id in cluster.live_ids() {
        assert_eq!(
            history(id),
            first,
            "{id:?} applied the same history as n(0): the replicated log decides"
        );
        assert_eq!(
            cluster.committed(id),
            cluster.committed(n(0)),
            "{id:?} sits at the same committed frontier"
        );
        assert_eq!(
            cluster.status(id),
            Status::Normal,
            "{id:?} settled into a working view"
        );
    }
    assert!(!first.is_empty(), "the run committed something");
}
