//! The real transport: PAXE-encrypted datagrams on a real UDP socket.
//!
//! This is the [`Transport`] seam with something under it. Every other way
//! the demo moves a message is a harness: a bus in memory, driven by the
//! test, proving the protocol. This one owns a [`std::net::UdpSocket`],
//! packs each released [`Message`] with the core's own [`Pack`], seals the
//! bytes into a PAXE frame, and sends them as one datagram. On the way in
//! it opens each datagram and hands the core a message. The socket, the
//! codec, and nothing else is here: no timers, no discovery, no threads.
//!
//! ## The frame
//!
//! One datagram is one PAXE standard-mode frame, whose plaintext is the
//! core's packed message and whose overhead is PAXE's fixed 37 bytes: a
//! 9-byte authenticated prefix (source, destination, channel, flags), a
//! 12-byte nonce and a 16-byte tag. The frame carries no length field and
//! no fragment: the payload length is the datagram length minus that
//! overhead, and a message that cannot fit a datagram is refused and
//! counted, never split and never truncated. The message the core sees is
//! byte-for-byte the message the core released.
//!
//! ## Key selection, and what this module does not do
//!
//! Keys are per link and they are configuration. This module has NO key
//! management: it never derives, exchanges, rotates, retires or stores key
//! material, and it has no idea how a link key came to exist. An operator
//! provisions them beside the peers, and a configuration without a key for a
//! peer simply cannot exchange traffic with it. The store holds the material
//! in paxe's guarded, locked, reliably-zeroed allocations; this module only
//! ever hands paxe a borrowed slice of it.
//!
//! Peers are configured by address and never discovered: there is no
//! membership gossip here and no address resolution beyond what the
//! configuration states. Sending selects the key by the DESTINATION's paxe
//! identity, receiving selects it by the frame's SOURCE, which is paxe's
//! rule and this module's only routing fact.
//!
//! ## Attribution is authenticated, the socket address is not
//!
//! A received datagram's sender is the identity in the frame's authenticated
//! `fromId`, resolved through this node's configured peers. The datagram's
//! UDP source address is read from the socket and then discarded: an
//! address is the first thing on the wire to be forged, and a frame that
//! opens under the link key is proof the sender holds that link's material
//! regardless of where the datagram came from. A frame naming a peer this
//! node has no entry for, or one that does not open, is dropped and counted
//! rather than attributed.
//!
//! ## Time
//!
//! The loop's only wait is the socket read. [`PaxeTransport::wait`] bounds
//! how long that read blocks and nothing else: it is an I/O-READINESS wait,
//! never protocol time, and no duration from it ever reaches the core. The
//! core's clock is the driver's logical tick and the replicated log's own
//! progress, exactly as it is on the in-memory transport; a node here has no
//! timer of its own and this module owns none.
//!
//! ## Threads
//!
//! One node, one thread. The keystore this module owns is neither `Send` nor
//! `Sync` by construction (guarded key material, no `unsafe impl` anywhere
//! to override it), so a transport cannot cross a thread boundary and the
//! compiler says so rather than a comment.

use std::collections::BTreeMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use paxe::keystore::{Epoch, KeyStore, KeystoreError};
use paxe::sodium::{KEYBYTES, SodiumError};
use paxe::standard::{self, MAX_PAYLOAD, MAX_UDP_DATAGRAM};

use crate::demo::node::Transport;
use crate::ids::{Era, NodeId};
use crate::message::Message;
use crate::wire::{Pack, Unpack};

/// One configured peer: the core identity it is, the paxe identity frames to
/// and from it are addressed under, the address its datagrams go to, and
/// the key this link shares with it.
///
/// No `Debug`: this type holds key material, and a derived `Debug` is a log
/// line away from disclosing it.
pub struct PaxePeer {
    /// The core identity. The [`Transport`] seam addresses peers by this and
    /// by nothing else.
    pub node: NodeId,
    /// The identity paxe addresses frames to this peer under, as the frame's
    /// `toId` on send and its `fromId` on receive. It is a separate space
    /// from [`PaxeConfig::paxe_id`]'s because one core identity may outlive
    /// several paxe identities, and a reincarnated identity is a new link
    /// with a new key.
    pub paxe_id: u16,
    /// Where this peer's datagrams are sent. The only routing this module
    /// does: there is no discovery and no resolution.
    pub address: SocketAddr,
    /// The key shared with this peer. Provisioned, never generated here.
    pub link: [u8; KEYBYTES],
}

/// One node's transport configuration: who this node is, which paxe
/// identity it seals under, the channel and epoch every frame carries, and
/// the peers it can exchange traffic with.
///
/// One channel and one epoch for the whole configuration, both of them
/// stated rather than derived. The channel is a static label for this
/// cluster's traffic; the epoch is paxe's key-rotation counter, which this
/// module exposes as a single configured value and never rotates.
///
/// No `Debug`: a configuration holds link keys (see [`PaxePeer`]).
pub struct PaxeConfig {
    /// This node's core identity.
    pub own: NodeId,
    /// The identity this node seals every frame from, as the frame's
    /// `fromId`. paxe takes it from its keystore, so no caller here can seal
    /// a frame claiming a source it does not hold.
    pub paxe_id: u16,
    /// The channel every frame this node seals carries.
    pub channel: u32,
    /// The key epoch every frame this node seals carries, 0 to 31. It is
    /// the epoch the configured link keys are installed under.
    pub epoch: u8,
    /// Every peer this node can exchange traffic with, each with its own
    /// address and link key.
    pub peers: Vec<PaxePeer>,
}

/// Why the transport refused to mount. A refusal is data: it names the
/// configuration or the environment that made the transport impossible, and
/// it happens before any datagram is exchanged, so a refusal never leaves a
/// half-configured node on the wire.
#[derive(Debug)]
pub enum PaxeRefusal {
    /// libsodium's startup contract failed: it did not initialise, its ABI
    /// sizes disagree with the codec's, or the hardware AES-256-GCM path is
    /// unavailable. Paxe requires every host to run this at startup and to
    /// treat it as fatal for the transport rather than discovering it at
    /// the first packet.
    Sodium(SodiumError),
    /// The configured epoch is not representable in the 5-bit wire field.
    Epoch(u8),
    /// The keystore refused a configured link key: a guarded allocation or
    /// its lock failed, which in practice is the OS refusing more locked
    /// memory.
    Keystore(KeystoreError),
    /// The configuration is not one this transport can route: a peer
    /// repeating an identity, or two peers sharing one paxe identity.
    Peer(String),
}

/// The refusal as an operator reads it: the cause verbatim, never a summary
/// of it. The codec's own law is that a startup-check failure is fatal for
/// the transport and its reason is surfaced whole, because the reason names
/// what would satisfy it.
impl std::fmt::Display for PaxeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sodium(e) => write!(f, "libsodium refused the transport: {e}"),
            Self::Epoch(epoch) => {
                write!(
                    f,
                    "epoch {epoch} is not representable in the 5-bit wire field"
                )
            }
            Self::Keystore(e) => write!(f, "the keystore refused a configured link key: {e}"),
            Self::Peer(why) => write!(f, "the configuration cannot be routed: {why}"),
        }
    }
}

/// One node's end of the real wire: a bound socket, paxe's keystore for its
/// links, and the counters an operator reads when a link is not carrying
/// what it should.
pub struct PaxeTransport {
    socket: UdpSocket,
    store: KeyStore,
    channel: u32,
    epoch: Epoch,
    own: NodeId,
    /// Peers by core identity: the routing table for a release, and the key
    /// source for the seal it performs.
    routes: BTreeMap<NodeId, PaxePeer>,
    /// Configured peers by paxe identity: the reverse table that resolves an
    /// authenticated `fromId` to the core identity the core must attribute
    /// the datagram to.
    identities: BTreeMap<u16, NodeId>,
    /// The sealed frame scratch, one whole datagram, reused by every release.
    frame: Vec<u8>,
    /// The opened plaintext scratch, the largest payload a datagram can
    /// carry. One byte short of it would turn every legitimate frame into a
    /// rejection, so it is sized from the codec's own payload ceiling.
    plaintext: Vec<u8>,
    /// The received datagram scratch, one whole datagram. A datagram larger
    /// than a UDP datagram cannot exist, so nothing is ever truncated into
    /// it.
    received: Vec<u8>,
    /// The length of the datagram waiting in `received`, or `None` when the
    /// socket holds nothing this node has taken. One datagram of look-ahead:
    /// the readiness and wait reads park their datagram here and the next
    /// `receive` returns it, so neither can consume traffic the core never
    /// sees.
    held: Option<usize>,
    sent: u64,
    dropped: u64,
    unaddressed: u64,
    unsealed: u64,
    io_failures: u64,
}

impl PaxeTransport {
    /// Mounts the transport on an already-bound socket.
    ///
    /// The socket is bound by the caller because a configuration names its
    /// peers' addresses, and those are only knowable once every socket has
    /// been bound: the host resolves the addresses, then mounts.
    ///
    /// Paxe runs its startup contract here, before the first datagram, so a
    /// broken environment is refused at mount rather than discovered on the
    /// wire. Every configured link key is installed under its peer and the
    /// configured epoch, and a key that cannot be installed fails the mount
    /// instead of producing a node that cannot seal to a configured peer.
    ///
    /// # Errors
    ///
    /// [`PaxeRefusal`] as the environment or the configuration refuses. The
    /// node is not constructed and no datagram has moved.
    pub fn mount(socket: UdpSocket, config: PaxeConfig) -> Result<Self, PaxeRefusal> {
        paxe::startup_check().map_err(PaxeRefusal::Sodium)?;
        let epoch = Epoch::new(config.epoch).map_err(|_| PaxeRefusal::Epoch(config.epoch))?;
        let mut store = KeyStore::new(config.paxe_id).map_err(PaxeRefusal::Keystore)?;
        let mut routes = BTreeMap::new();
        let mut identities = BTreeMap::new();
        for peer in config.peers {
            if peer.node == config.own {
                return Err(PaxeRefusal::Peer(format!(
                    "{:?} is configured as its own peer",
                    peer.node
                )));
            }
            if routes.contains_key(&peer.node) {
                return Err(PaxeRefusal::Peer(format!(
                    "{:?} is configured twice",
                    peer.node
                )));
            }
            if identities.contains_key(&peer.paxe_id) {
                return Err(PaxeRefusal::Peer(format!(
                    "paxe identity {} is configured twice",
                    peer.paxe_id
                )));
            }
            store
                .install(peer.paxe_id, epoch, &peer.link)
                .map_err(PaxeRefusal::Keystore)?;
            identities.insert(peer.paxe_id, peer.node);
            routes.insert(peer.node, peer);
        }
        Ok(Self {
            socket,
            store,
            channel: config.channel,
            epoch,
            own: config.own,
            routes,
            identities,
            frame: vec![0u8; MAX_UDP_DATAGRAM],
            plaintext: vec![0u8; MAX_PAYLOAD],
            received: vec![0u8; MAX_UDP_DATAGRAM],
            held: None,
            sent: 0,
            dropped: 0,
            unaddressed: 0,
            unsealed: 0,
            io_failures: 0,
        })
    }

    /// This node's core identity, as the configuration stated it.
    #[must_use]
    pub fn own(&self) -> NodeId {
        self.own
    }

    /// How many peers are configured. The size of the transport's whole
    /// world: it exchanges traffic with these and nobody else.
    #[must_use]
    pub fn peers(&self) -> usize {
        self.routes.len()
    }

    /// The run loop's only wait: one blocking read, bounded by
    /// `ready_after`. Returns whether a datagram was taken.
    ///
    /// An I/O-READINESS wait. The duration bounds how long the socket read
    /// blocks before reporting that nothing is waiting; it is not a protocol
    /// timeout, it is not a lease, and no core input is derived from it. A
    /// real loop calls this between passes and does its own work when it
    /// reports nothing.
    ///
    /// # Errors
    ///
    /// The underlying socket error when the read failed outright. A read
    /// that found nothing is `Ok(false)`, never an error: an idle link is
    /// not a fault.
    pub fn wait(&mut self, ready_after: Duration) -> io::Result<bool> {
        if self.held.is_some() {
            return Ok(true);
        }
        self.socket.set_read_timeout(Some(ready_after))?;
        let read = self.socket.recv_from(&mut self.received);
        self.socket.set_read_timeout(None)?;
        match read {
            Ok((length, _source)) => {
                self.held = Some(length);
                Ok(true)
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Whether a datagram is waiting for this node. A datagram found here is
    /// buffered and returned by the next [`Transport::receive`], so the
    /// readiness probe never consumes traffic the core would not see.
    ///
    /// A host loop asks this where it needs to know whether a pass has work
    /// without blocking; a host that has nothing else to do asks
    /// [`PaxeTransport::wait`] instead.
    pub fn ready(&mut self) -> bool {
        if self.held.is_some() {
            return true;
        }
        match read_datagram(&self.socket, &mut self.received) {
            Ok(Some(length)) => {
                self.held = Some(length);
                true
            }
            // A socket error is neither readiness nor a datagram. The probe
            // reports nothing, and the error surfaces as an I/O failure the
            // next time the socket is read.
            _ => false,
        }
    }

    /// Datagrams this node sealed and put on the wire. The positive counter the
    /// others are read against: this one at zero with the rest at zero is a
    /// silent link, which no refusal count would say.
    #[must_use]
    pub fn sent(&self) -> u64 {
        self.sent
    }

    /// Inbound datagrams the core never saw: a frame paxe refused to open,
    /// a frame naming a peer this node has no entry for, and a frame that
    /// opened and did not decode into a message. One counter because the
    /// core's position is the same in every case and the operator reads it
    /// as "this link is carrying traffic this node cannot use".
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Released datagrams this node could not put on the wire because no
    /// peer is configured for the destination. A dead identity, or a
    /// destination outside this node's configuration.
    #[must_use]
    pub fn unaddressed(&self) -> u64 {
        self.unaddressed
    }

    /// Released datagrams paxe refused to seal, or that are too large for a
    /// datagram. A configuration fault: no key is installed for the
    /// destination under this node's epoch, or the packed message does not
    /// fit the datagram ceiling. Never a truncation and never a split.
    #[must_use]
    pub fn unsealed(&self) -> u64 {
        self.unsealed
    }

    /// Socket reads and writes the operating system refused.
    #[must_use]
    pub fn io_failures(&self) -> u64 {
        self.io_failures
    }
}

impl Transport for PaxeTransport {
    /// The next datagram that opens under a link key this node holds, or
    /// `None` when the socket holds nothing usable. A frame that does not
    /// open is counted and skipped inside this call, so one forgery cannot
    /// end a pass that still holds real traffic.
    fn receive(&mut self) -> Option<(NodeId, Message)> {
        loop {
            if self.held.is_none() {
                match read_datagram(&self.socket, &mut self.received) {
                    Ok(Some(length)) => self.held = Some(length),
                    Ok(None) => return None,
                    Err(_) => {
                        self.io_failures += 1;
                        return None;
                    }
                }
            }
            // Both lengths are established by construction: `held` is a
            // length `recv_from` returned for this buffer, and the payload
            // length paxe returns is bounded by the plaintext scratch.
            let length = self.held.unwrap_or(0);
            let opened = standard::open(&self.store, &self.received[..length], &mut self.plaintext);
            self.held = None;
            let message = match opened {
                Ok((header, _flags, payload)) => match self.identities.get(&header.from_id) {
                    Some(&from) => Message::unpack_from(&self.plaintext[..payload])
                        .ok()
                        .map(|message| (from, message)),
                    None => None,
                },
                Err(_) => None,
            };
            match message {
                Some(attributed) => return Some(attributed),
                None => self.dropped += 1,
            }
        }
    }

    /// Packs the message, seals it for the destination's link, and sends it
    /// as one datagram.
    ///
    /// The era is not consulted: paxe authenticates the frame's own
    /// addressing and the core is the authority on the era a ballot was
    /// authorised at, so a stale frame is the core's rejection and never
    /// this module's.
    fn send(&mut self, to: NodeId, _era: Era, message: &Message) {
        let Some(peer) = self.routes.get(&to) else {
            self.unaddressed += 1;
            return;
        };
        let length = message.packed_len();
        let mut packed = vec![0u8; length];
        if message.pack_into(&mut packed).is_err() {
            self.unsealed += 1;
            return;
        }
        let sealed = standard::seal(
            &self.store,
            peer.paxe_id,
            self.channel,
            self.epoch,
            &packed,
            &mut self.frame,
        );
        match sealed {
            // `frame` is a whole datagram and paxe has already refused a
            // payload that does not fit one, so the slice is in bounds by
            // construction rather than by an arithmetic check here.
            Ok(frame_length) => {
                if self
                    .socket
                    .send_to(&self.frame[..frame_length], peer.address)
                    .is_err()
                {
                    self.io_failures += 1;
                } else {
                    self.sent += 1;
                }
            }
            Err(_) => self.unsealed += 1,
        }
    }
}

/// One read with no wait: the datagram taken, or nothing waiting.
///
/// The socket's resting state is blocking with no read timeout, which is
/// what a host loop wants between passes. A readiness probe is the one read
/// that must not block, so the socket is put in non-blocking mode for
/// exactly this call and returned to blocking after it. Nothing is lost in
/// the change of mode: a datagram already queued on the socket is still
/// there when the socket is put back, and one that has not arrived yet is
/// reported as absent rather than as a fault.
fn read_datagram(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<Option<usize>> {
    socket.set_nonblocking(true)?;
    let read = socket.recv_from(buffer);
    socket.set_nonblocking(false)?;
    match read {
        Ok((length, _source)) => Ok(Some(length)),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error),
    }
}
