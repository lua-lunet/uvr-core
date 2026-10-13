//! The locks demo over the real transport: PAXE-encrypted datagrams on real
//! loopback UDP sockets, and the crash-reincarnation story end to end.
//!
//! Three lock nodes over [`PaxeTransport`], one single-threaded loop per
//! node. A client takes locks, a chosen node fakes its death at a named
//! commit, the crash writes nothing, the reborn identity distrusts its stale
//! state file by the fence's verdict, rejoins over the encrypted link, and
//! the closing observation returns the same lease bytes from every survivor.
//! Beside it, one datagram is taken off the wire and examined as what it
//! is: a sealed frame, which the holder of the link key opens and nobody
//! else opens at all.
//!
//! The transport under the demo is the same [`Transport`] seam the in-memory
//! harness drives, so the protocol is identical on both sides of it and
//! this file asserts the story the harness asserts. What is new is what is
//! under the seam: a socket, a cipher, per-link keys, and an addressing
//! space of its own.
//!
//! No wall clock decides anything here. A settle is a bounded count of
//! rounds, and a lost datagram is a failed test, not a slower one: loopback
//! UDP to a bound socket does not drop. The durations in this file bound
//! socket reads so that a defect fails the test instead of hanging it;
//! neither can make it pass.

#![cfg(feature = "demo-locks")]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use uvrr::demo::{
    Applied, ClientId, CommitHook, DemoDisk, DemoNode, FileBootFence, FileStateStore, Lease,
    NoOpHook, PaxeConfig, PaxePeer, PaxeRefusal, PaxeTransport, Response, StateLoad, Transport,
};
use uvrr::ids::{CrashCounter, Era, NodeId, Operation, OperationId, Slot, SystemId};
use uvrr::message::Message;
use uvrr::progress::Status;
use uvrr::replica::ViewChangeKnobs;
use uvrr::wire::{Pack, Unpack};

/// The suspicion budget in ticks: a node fences into the next view after
/// more than three ticks of primary silence (S4). Logical, never a clock.
const TIMEOUT: u64 = 3;

/// The lease window in committed slots: long enough to survive the rejoin's
/// choreography, short enough that a bounded run expires it.
const LEASE_WINDOW: u64 = 16;

/// The channel every frame in this run carries.
const CHANNEL: u32 = 0x00C0_FFEE;

/// The key epoch every frame in this run carries.
const EPOCH: u8 = 0;

/// PAXE's standard-mode frame overhead in bytes: the 9-byte authenticated
/// prefix, the 12-byte nonce and the 16-byte tag, over the core's packed
/// message (PAXE.md "Standard Frame"). A standard frame is exactly the
/// payload plus this, which is what makes the datagram on the wire a
/// readable shape rather than an opaque length.
const FRAME_OVERHEAD: usize = 37;

/// The bound on one settle, in rounds of drains. A count, never a duration:
/// convergence is a property of the traffic, and a cluster that has not
/// converged by the bound has lost a datagram.
const ROUNDS: usize = 64;

/// The bounded I/O-readiness read a quiet round performs, so a datagram
/// still in flight when the round's drains ran is picked up rather than
/// assumed absent. It bounds a socket read and nothing else: no protocol
/// input is derived from it, and the core's clock is the driver's tick.
const READY: Duration = Duration::from_millis(50);

/// The largest possible UDP datagram, and so the buffer this file reads
/// into. A datagram cannot exceed it, so nothing read here is truncated.
const MAX_DATAGRAM: usize = 65_507;

fn n(id: u32) -> NodeId {
    NodeId::new(
        SystemId::new((id + 1) as u16).expect("test system ids are small and non-zero"),
        CrashCounter::new(1).expect("one is non-zero"),
    )
}

fn op_id(lsb: u64) -> OperationId {
    OperationId { msb: 0, lsb }
}

/// A client identity through the public mint, which refuses the nil pair.
fn client(msb: u64, lsb: u64) -> ClientId {
    ClientId::new(msb, lsb).expect("a test client is not the nil identity")
}

/// The key one link shares, named by the two endpoints' paxe identities in
/// ascending order. One distinct 32-byte material per link: a reincarnated
/// identity is a new link with a new key, exactly as its new socket and its
/// new paxe identity are new. Thirty-two bytes is paxe's AES-256-GCM key
/// length, and the field this is assigned to is that length, so a codec
/// that changed its key size would not compile here.
fn link(one: u16, other: u16) -> [u8; 32] {
    let (low, high) = if one <= other {
        (one, other)
    } else {
        (other, one)
    };
    let mut material = [0u8; 32];
    material[..4].copy_from_slice(&[low as u8, high as u8, 0x5E, 0xED]);
    material
}

/// A socket on the loopback interface, bound to a port the host chose.
fn bind() -> UdpSocket {
    UdpSocket::bind("127.0.0.1:0").expect("a loopback socket binds")
}

/// The address a bound socket holds.
fn address(socket: &UdpSocket) -> SocketAddr {
    socket.local_addr().expect("a bound socket has an address")
}

/// One socket read, bounded so a defect fails this test instead of hanging
/// the suite. The bound is a hang guard on the test's own I/O, never a
/// condition for the assertions that follow: on loopback the datagram is
/// already waiting by the time the send returned.
fn read_datagram(socket: &UdpSocket) -> Vec<u8> {
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("the read bound is set");
    let mut buffer = vec![0u8; MAX_DATAGRAM];
    let (length, _source) = socket
        .recv_from(&mut buffer)
        .expect("a datagram sent to a bound loopback socket arrives");
    buffer.truncate(length);
    buffer
}

/// One datagram off a transport, waited for rather than raced: a loopback
/// datagram is delivered by the kernel, and a datagram this test has just
/// sent may not have landed by the time the next statement runs. The bound
/// is the hang guard on that wait, and it can only make this test fail.
fn deliver(transport: &mut PaxeTransport) -> Option<(NodeId, Message)> {
    for _ in 0..ROUNDS {
        if transport.ready() {
            return transport.receive();
        }
        if transport
            .wait(READY)
            .expect("the readiness read never refuses")
        {
            return transport.receive();
        }
    }
    None
}

/// Whether `needle` appears anywhere inside `haystack`.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// One node's end of the real wire. The transport is shared with the driver
/// so the driver can ask what is waiting on a socket without owning the
/// node; both trait methods are the transport's own, called through the
/// borrow, so the node runs the production transport and the driver watches
/// the same socket.
struct Wire(Rc<RefCell<PaxeTransport>>);

impl Transport for Wire {
    fn receive(&mut self) -> Option<(NodeId, Message)> {
        self.0.borrow_mut().receive()
    }

    fn send(&mut self, to: NodeId, era: Era, message: &Message) {
        self.0.borrow_mut().send(to, era, message);
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
    let dir = std::env::temp_dir().join(format!("uvrr-demo-paxe-{}-{unique}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("the test directory creates");
    dir
}

/// One node's directory, created.
fn node_dir(root: &Path, index: usize) -> PathBuf {
    let dir = root.join(format!("node{index}"));
    std::fs::create_dir_all(&dir).expect("the node directory creates");
    dir
}

/// The demo cluster over real sockets: the live nodes, the transports the
/// driver watches, the socket held for the identity a crash will supersede,
/// and the disks a crash has recorded. Every protocol decision lives in the
/// demo module; every assertion here reads its public surface.
struct Cluster {
    nodes: Vec<Option<DemoNode>>,
    wires: Vec<Option<Rc<RefCell<PaxeTransport>>>>,
    /// The address each genesis slot holds, and the address the incarnation
    /// a reincarnation will take.
    addresses: Vec<SocketAddr>,
    /// The socket for the incarnation a reincarnation will mount: bound
    /// before the cluster is provisioned, because a configuration names
    /// its peers' addresses and nothing here discovers one.
    reborn_socket: UdpSocket,
    /// The paxe identity the incarnation will hold: one past every genesis
    /// identity, so the reborn link is a link of its own.
    reborn_paxe: u16,
    reborn_identity: NodeId,
    disks: Vec<Option<DemoDisk>>,
    slot_of: BTreeMap<NodeId, usize>,
    knobs: ViewChangeKnobs,
    crash_fired: Arc<AtomicBool>,
    root: PathBuf,
    clock: u64,
}

/// Whether this host can run PAXE at all, and if not, the refusal it names.
///
/// The codec's own law is that a startup-check failure is fatal for PAXE: the
/// host refuses to enable it and surfaces the cause verbatim, never
/// degrading to a weaker cipher. Some distro libsodium builds ship without the
/// hardware AES-256-GCM path (Ubuntu 24.04 and Debian trixie arm64 are the
/// named ones), and a host like that must REFUSE. So the sealed proofs below
/// run where the cipher is available, and where it is not, the refusal itself
/// is what gets proved, with its cause named rather than swallowed.
fn paxe_refusal() -> Option<String> {
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").expect("the probe socket binds");
    let shape = PaxeConfig {
        own: n(0),
        paxe_id: 1,
        channel: CHANNEL,
        epoch: EPOCH,
        peers: Vec::new(),
    };
    PaxeTransport::mount(probe, shape)
        .err()
        .map(|e| e.to_string())
}

/// Proves the refusal on a host that cannot run PAXE, and reports whether the
/// caller should run the sealed proofs instead.
///
/// A skip is not a pass. Where the host's libsodium has no hardware
/// AES-256-GCM path, the transport's refusal is the claim under test, and it
/// is asserted here with its cause intact: a refusal that names the distro
/// build that caused it is a working refusal. Returns true when the sealed
/// proofs should run.
#[must_use]
fn host_can_seal(reason: &str) -> bool {
    match paxe_refusal() {
        None => true,
        Some(cause) => {
            assert!(
                cause.contains("AES-256-GCM") || cause.contains("AesGcmUnavailable"),
                "the transport refused for a cause the sealed proofs do not \
                 expect, so this is a defect and not a host limitation: {cause}"
            );
            println!("skipping {reason}: {cause}");
            false
        }
    }
}

impl Cluster {
    /// The genesis nodes, each a first life over its own socket, and the
    /// socket a reincarnation will take when it comes.
    ///
    /// Every identity in the eventual cluster, including the one that does
    /// not exist yet, is in every peer's configuration from the first
    /// moment: peers are configured by address, and a configuration that
    /// grew a peer when the peer appeared would be discovery by another
    /// name.
    fn provision(size: usize) -> Self {
        let genesis: Vec<NodeId> = (0..size as u32).map(n).collect();
        // The incarnation the crash will produce: one past the paxe
        // identity of the slot the crash hook hangs on, which is the last
        // genesis slot, and one past that slot's own identity.
        let reborn_paxe = size as u16 + 1;
        let reborn_identity = n(size as u32 - 1)
            .next_life()
            .expect("the bump never exhausts");
        let sockets: Vec<UdpSocket> = (0..=size).map(|_| bind()).collect();
        let addresses: Vec<SocketAddr> = sockets.iter().map(address).collect();
        let reborn_socket = sockets[size].try_clone().expect("the socket duplicates");
        let root = temp_dir();
        let crash_fired = Arc::new(AtomicBool::new(false));
        let mut nodes = Vec::with_capacity(size);
        let mut wires = Vec::with_capacity(size);
        let mut disks = Vec::with_capacity(size);
        let mut slot_of = BTreeMap::new();
        for (index, &id) in genesis.iter().enumerate() {
            let mut peers: Vec<PaxePeer> = (0..size as u32)
                .filter(|&other| other != index as u32)
                .map(|other| PaxePeer {
                    node: n(other),
                    paxe_id: other as u16 + 1,
                    address: addresses[other as usize],
                    link: link(index as u16 + 1, other as u16 + 1),
                })
                .collect();
            peers.push(PaxePeer {
                node: reborn_identity,
                paxe_id: reborn_paxe,
                address: addresses[size],
                link: link(index as u16 + 1, reborn_paxe),
            });
            let wire = Rc::new(RefCell::new(
                PaxeTransport::mount(
                    sockets[index].try_clone().expect("the socket duplicates"),
                    PaxeConfig {
                        own: id,
                        paxe_id: index as u16 + 1,
                        channel: CHANNEL,
                        epoch: EPOCH,
                        peers,
                    },
                )
                .expect("the transport mounts"),
            ));
            let hooks: Box<dyn CommitHook> = if index == size - 1 {
                Box::new(CrashAtSlot {
                    fired: Arc::clone(&crash_fired),
                })
            } else {
                Box::new(NoOpHook)
            };
            let dir = node_dir(&root, index);
            let node = DemoNode::first(
                id,
                genesis.clone(),
                Box::new(FileBootFence::new(&dir.join("fence"))),
                Box::new(FileStateStore::new(&dir.join("locks"))),
                Box::new(Wire(Rc::clone(&wire))),
                hooks,
                Self::knobs(),
            )
            .expect("the genesis order provisions");
            slot_of.insert(id, index);
            nodes.push(Some(node));
            wires.push(Some(wire));
            disks.push(None);
        }
        Self {
            nodes,
            wires,
            addresses,
            reborn_socket,
            reborn_paxe,
            reborn_identity,
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

    /// The transport behind a live slot.
    fn wire(&self, index: usize) -> &Rc<RefCell<PaxeTransport>> {
        self.wires[index]
            .as_ref()
            .unwrap_or_else(|| panic!("slot {index} holds no live transport"))
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

    /// One bounded, deterministic settle: rounds of drains until a full
    /// round delivers nothing, no socket holds a datagram, and a bounded
    /// readiness read finds none either. Every live node drains every round,
    /// so traffic that lands on a node whose turn has already passed is
    /// picked up by the next round rather than missed.
    fn settle(&mut self) {
        for _ in 0..ROUNDS {
            let mut delivered = 0;
            for node in self.nodes.iter_mut().flatten() {
                delivered += node.run().len();
            }
            if delivered > 0 || self.any_ready() {
                continue;
            }
            // The run's one wait, on the first live socket. It is an
            // I/O-readiness read: the core is fed nothing from it.
            if self.wait_any() {
                continue;
            }
            // A round can strand a datagram whose destination had already
            // drained, so every socket is asked once more before the
            // cluster is called quiet.
            if self.any_ready() {
                continue;
            }
            return;
        }
        panic!("the cluster did not settle in {ROUNDS} rounds: a datagram was lost");
    }

    /// Whether any live socket holds a datagram. A readiness probe buffers
    /// what it finds, so nothing is consumed that the core would not see.
    fn any_ready(&mut self) -> bool {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.is_some())
            .any(|(index, _)| self.wire(index).borrow_mut().ready())
    }

    /// The bounded readiness read on the first live socket.
    fn wait_any(&mut self) -> bool {
        let Some(index) = self.nodes.iter().position(Option::is_some) else {
            return false;
        };
        self.wire(index)
            .borrow_mut()
            .wait(READY)
            .expect("the readiness read never refuses")
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
        self.settle();
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
        self.settle();
    }

    /// Crashes a node: its volatile state dies and nothing is written. Its
    /// socket dies with it, so nothing answers at that address again.
    fn crash(&mut self, id: NodeId) {
        let index = self.index_of(id);
        let node = self.nodes[index]
            .take()
            .unwrap_or_else(|| panic!("{id:?} is already down"));
        self.wires[index] = None;
        self.disks[index] = Some(node.record_disk());
    }

    /// Reincarnates a crashed node over its recorded disk, onto the socket
    /// and the link key the configuration has held for the identity that
    /// does not exist yet. Returns the bumped identity.
    fn reincarnate(&mut self, old: NodeId) -> NodeId {
        let index = self.index_of(old);
        let dir = node_dir(&self.root, index);
        let disk = self.disks[index]
            .take()
            .unwrap_or_else(|| panic!("{old:?} has no recorded disk; crash it first"));
        let new = old.next_life().expect("the bump never exhausts in tests");
        assert_eq!(
            new, self.reborn_identity,
            "the configured incarnation is this one"
        );
        // The reborn node's peers are the survivors, each under the link key
        // the reborn identity shares with it: a new identity is a new link,
        // and the dead identity's key is not its successor's.
        let peers: Vec<PaxePeer> = (0..self.nodes.len() as u32)
            .filter(|&other| other != index as u32)
            .map(|other| PaxePeer {
                node: n(other),
                paxe_id: other as u16 + 1,
                address: self.addresses[other as usize],
                link: link(self.reborn_paxe, other as u16 + 1),
            })
            .collect();
        let wire = Rc::new(RefCell::new(
            PaxeTransport::mount(
                self.reborn_socket
                    .try_clone()
                    .expect("the socket duplicates"),
                PaxeConfig {
                    own: new,
                    paxe_id: self.reborn_paxe,
                    channel: CHANNEL,
                    epoch: EPOCH,
                    peers,
                },
            )
            .expect("the incarnation mounts on its own socket"),
        ));
        let node = DemoNode::reincarnate(
            old,
            disk,
            Box::new(FileBootFence::new(&dir.join("fence"))),
            Box::new(FileStateStore::new(&dir.join("locks"))),
            Box::new(Wire(Rc::clone(&wire))),
            Box::new(NoOpHook),
            self.knobs,
        )
        .unwrap_or_else(|refusal| panic!("the reincarnation refused: {refusal:?}"));
        assert_eq!(node.own(), new, "the bump is taken up");
        // The bumped identity occupies the old life's slot from here on; the
        // superseded identity's datagrams address nobody.
        self.slot_of.insert(new, index);
        self.nodes[index] = Some(node);
        self.wires[index] = Some(wire);
        self.settle();
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

fn get_body(lock_id: u64) -> Value {
    json!({ "op": "get", "lock_id": lock_id })
}

/// One grant, proposed to a live primary and settled.
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
fn a_crash_at_a_named_commit_reincarnates_over_the_encrypted_link_and_the_locks_survive() {
    if !host_can_seal("the crash story over the encrypted link") {
        return;
    }
    let mut cluster = Cluster::provision(3);
    cluster.bootstrap();
    let client_a = client(1, 1);
    let client_b = client(1, 2);
    let client_c = client(1, 3);

    // Every node's whole world is the three identities it can exchange
    // traffic with, and nothing it discovered since.
    for id in cluster.live_ids() {
        let wire = cluster.wire(cluster.index_of(id));
        assert_eq!(wire.borrow().own(), id, "{id:?} is who it says it is");
        assert_eq!(
            wire.borrow().peers(),
            3,
            "{id:?} has three configured peers"
        );
    }

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

    // The datagrams that carried it were frames, not requests: every node
    // put sealed traffic on the wire and nothing it could not open came
    // back off it.
    for id in cluster.live_ids() {
        let wire = cluster.wire(cluster.index_of(id)).borrow();
        assert!(wire.sent() > 0, "{id:?} sealed traffic onto the wire");
        assert_eq!(wire.dropped(), 0, "{id:?} dropped no traffic it could open");
        assert_eq!(
            wire.unaddressed(),
            0,
            "{id:?} addressed everything it released"
        );
        assert_eq!(wire.unsealed(), 0, "{id:?} sealed everything it released");
        assert_eq!(wire.io_failures(), 0, "{id:?} had no socket failure");
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

    // The rebirth: the fence's verdict, the bump, the announcement, sent
    // onto the incarnation's own socket under its own link keys.
    let reborn_slot = cluster.index_of(n(2));
    let bumped = cluster.reincarnate(n(2));
    assert_eq!(Some(bumped), n(2).next_life(), "the pair is one past");
    let released = cluster.wire(reborn_slot).borrow().sent();
    assert!(released > 0, "the announcement went onto the wire");

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
    cluster.settle();
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

#[test]
fn a_datagram_on_the_wire_is_a_sealed_frame_that_only_its_link_key_opens() {
    if !host_can_seal("the sealed frame on the wire") {
        return;
    }
    // The tap stands on the wire for a peer the sender is configured to
    // believe in, so what it reads is a datagram the sender really sent.
    let tap = bind();
    let tap_address = address(&tap);
    let sender_socket = bind();
    let sender_address = address(&sender_socket);
    let mut sender = PaxeTransport::mount(
        sender_socket,
        PaxeConfig {
            own: n(0),
            paxe_id: 1,
            channel: CHANNEL,
            epoch: EPOCH,
            peers: vec![PaxePeer {
                node: n(1),
                paxe_id: 2,
                address: tap_address,
                link: link(1, 2),
            }],
        },
    )
    .expect("the transport mounts");

    // A message of the core's own, sealed and sent as one datagram.
    let plaintext = uvrr::demo::gossip_datagram(2, 7);
    let message = Message::unpack_from(&plaintext).expect("the core's frame decodes");
    sender.send(n(1), Era(2), &message);
    assert_eq!(sender.sent(), 1, "one datagram left");
    assert_eq!(sender.unaddressed(), 0, "the destination was configured");
    assert_eq!(sender.unsealed(), 0, "the destination's key was installed");
    assert_eq!(sender.io_failures(), 0, "the socket took it");

    let captured = read_datagram(&tap);

    // The datagram is the core's message under a fixed 37-byte overhead:
    // the frame's shape, byte-exact.
    assert_eq!(
        captured.len(),
        message.packed_len() + FRAME_OVERHEAD,
        "a standard frame is the payload plus the prefix, nonce and tag"
    );
    // The request itself is not on the wire: the plaintext appears nowhere
    // in the datagram, and the datagram is not a message.
    assert!(
        !contains(&captured, &plaintext),
        "the plaintext is not in the datagram"
    );
    assert!(
        Message::unpack_from(&captured).is_err(),
        "the datagram is not a request"
    );

    // The holder of this link's key opens it, and it is the same message:
    // the frame on the wire is the core's own body, recovered.
    let receiver_socket = bind();
    let receiver_address = address(&receiver_socket);
    let mut receiver = PaxeTransport::mount(
        receiver_socket,
        PaxeConfig {
            own: n(1),
            paxe_id: 2,
            channel: CHANNEL,
            epoch: EPOCH,
            peers: vec![PaxePeer {
                node: n(0),
                paxe_id: 1,
                address: sender_address,
                link: link(1, 2),
            }],
        },
    )
    .expect("the link's other end mounts");
    tap.send_to(&captured, receiver_address)
        .expect("the datagram is offered to the other end");
    let (from, arrived) = deliver(&mut receiver).expect("the right key opens the frame");
    assert_eq!(from, n(0), "the sender is the frame's authenticated source");
    assert_eq!(arrived, message, "the frame carries the core's own message");
    assert_eq!(receiver.dropped(), 0, "nothing was dropped");
    assert!(!receiver.ready(), "nothing else is waiting");

    // A stranger holding a different link's key opens nothing at all: the
    // datagram reaches its socket, and no message ever leaves it.
    let stranger_socket = bind();
    let stranger_address = address(&stranger_socket);
    let mut stranger = PaxeTransport::mount(
        stranger_socket,
        PaxeConfig {
            own: n(1),
            paxe_id: 2,
            channel: CHANNEL,
            epoch: EPOCH,
            peers: vec![PaxePeer {
                node: n(0),
                paxe_id: 1,
                address: sender_address,
                link: link(1, 3),
            }],
        },
    )
    .expect("a stranger holding another link's key mounts");
    tap.send_to(&captured, stranger_address)
        .expect("the datagram is offered to the stranger");
    assert!(
        deliver(&mut stranger).is_none(),
        "the wrong key opens no message and yields no noise"
    );
    assert_eq!(
        stranger.dropped(),
        1,
        "the frame was refused, not guessed at"
    );
    assert_eq!(stranger.sent(), 0, "and nothing came back out");

    // A destination the configuration does not name is not encrypted and not
    // sent: the release is counted, never quietly dropped.
    sender.send(n(2), Era(2), &message);
    assert_eq!(sender.unaddressed(), 1, "no route, no datagram");
    assert_eq!(sender.sent(), 1, "and nothing was sealed for it either");
}

#[test]
fn a_transport_refuses_an_environment_or_configuration_it_cannot_serve() {
    let socket = bind();

    // A host that cannot run PAXE at all refuses EVERY configuration, the
    // unusable ones included, because the codec's startup contract runs
    // before a configuration is interpreted: an operator on such a host is
    // told the environment is the problem, not sent hunting a configuration
    // that would have been refused anyway. The precedence is the claim.
    if let Some(cause) = paxe_refusal() {
        let refused = PaxeTransport::mount(
            socket.try_clone().expect("the socket duplicates"),
            PaxeConfig {
                own: n(0),
                paxe_id: 1,
                channel: CHANNEL,
                epoch: 32,
                peers: Vec::new(),
            },
        )
        .err()
        .map(|e| e.to_string())
        .expect("a host without the cipher cannot mount any transport");
        assert!(
            refused.contains("libsodium"),
            "the startup contract answers before the configuration is read, \
             so the unusable epoch is not what is reported: {refused}"
        );
        println!("the environment refusal answers first here: {cause}");
        return;
    }

    // An epoch the 5-bit wire field cannot hold is refused at the mount, not
    // masked onto the wire.
    let refused = PaxeTransport::mount(
        socket.try_clone().expect("the socket duplicates"),
        PaxeConfig {
            own: n(0),
            paxe_id: 1,
            channel: CHANNEL,
            epoch: 32,
            peers: Vec::new(),
        },
    )
    .err();
    assert!(
        matches!(&refused, Some(PaxeRefusal::Epoch(32))),
        "{refused:?}"
    );

    // So is a peer listed twice under one paxe identity: two addresses
    // claiming one identity is a configuration that cannot be routed.
    let address_one = address(&socket);
    let other = bind();
    let address_two = address(&other);
    let twice = PaxeTransport::mount(
        socket.try_clone().expect("the socket duplicates"),
        PaxeConfig {
            own: n(0),
            paxe_id: 1,
            channel: CHANNEL,
            epoch: EPOCH,
            peers: vec![
                PaxePeer {
                    node: n(1),
                    paxe_id: 2,
                    address: address_one,
                    link: link(1, 2),
                },
                PaxePeer {
                    node: n(2),
                    paxe_id: 2,
                    address: address_two,
                    link: link(1, 2),
                },
            ],
        },
    )
    .err();
    let collision = matches!(&twice, Some(PaxeRefusal::Peer(_)));
    assert!(collision, "one paxe identity, two peers: {twice:?}");

    // And a peer that is this node is refused, because a node's frames are
    // sealed from the store's own identity and it holds no link to itself.
    let itself = PaxeTransport::mount(
        other.try_clone().expect("the socket duplicates"),
        PaxeConfig {
            own: n(0),
            paxe_id: 1,
            channel: CHANNEL,
            epoch: EPOCH,
            peers: vec![PaxePeer {
                node: n(0),
                paxe_id: 2,
                address: address_one,
                link: link(1, 2),
            }],
        },
    )
    .err();
    let looping = matches!(&itself, Some(PaxeRefusal::Peer(_)));
    assert!(looping, "a node cannot be its own peer: {itself:?}");
}
