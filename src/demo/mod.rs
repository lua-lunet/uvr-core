//! The locks demo (`demo` feature): the least environment the advisory
//! locks machinery runs in.
//!
//! This is the scaffold: a cluster of lock nodes on an in-memory
//! transport, one thread per node and one loop per thread, a lock table
//! as the whole application, and crash-reincarnation run end to end. The
//! machinery itself — the CAS lock table and the seams it is written
//! against ([`StateStore`], [`BootFence`], [`CommitHook`]) — lives in the
//! core locks module (`crate::locks`), re-exported below so the demo's
//! surface reads as one place. What the demo itself carries:
//!
//! - the disk seam ([`Disk`]): every byte of the demo's own file access,
//!   the one place `std::fs` is named, and the demo's [`StdDisk`];
//! - the plain-file store and fence ([`FileStateStore`],
//!   [`FileBootFence`]): the least implementations of the core's
//!   contracts, over the disk seam; an industrial host mounts its
//!   storage engine and its superblock in their places and changes
//!   nothing above the traits;
//! - the single-threaded loop ([`DemoNode`]): drain the transport, feed
//!   the core, take the effects, emit. The transport is a seam, so a
//!   harness drives the loop in memory; the seam's real transport is
//!   [`PaxeTransport`], the one place a socket and a cipher are mounted,
//!   and the loop's only wait there is the socket read;
//! - the resend ledger: the host half that replays exactly the datagrams
//!   the core released, byte-identical. The demo has no timers, so the
//!   harness drives every relay deterministically;
//! - the join gossip's datagram: the host obligation a node outside the
//!   cluster owes the wire.
//!
//! What the demo deliberately does not carry: timers and timeout
//! machinery, an industrial WAL, snapshots, an AOF, key management of any
//! kind, any external dependency beyond the `serde` lane the lock
//! table's payload codec rides and the paxe lane its real transport
//! seals with. Nothing here reads a wall clock: the lease clock is the
//! replicated log's own progress, the slot the operation applies at, the
//! one clock every replica already agrees on.
//!
//! Two seams are the whole distance between this scaffold and an
//! industrial deployment: mount the heavy store and the industrial fence
//! in place of the plain files, and the machinery above them is
//! unchanged. The third, the real transport, is mounted here, so the
//! distance is two.

pub mod disk;
pub mod fence;
pub mod node;
pub mod paxe_transport;
pub mod rejoin;
pub mod relay;
pub mod state;

pub use crate::locks::{
    BootFence, ClientId, CommitHook, FenceWord, Lease, NoOpHook, Request, Response, Service,
    StateSnapshot, StateStore, lazy_load,
};
pub use disk::{Disk, DiskFile, StdDisk, std_disk};
pub use fence::FileBootFence;
pub use node::{
    Applied, BootRefusal, DemoDisk, DemoNode, DemoRefusal, RunOutcome, StateLoad, Transport,
};
pub use paxe_transport::{PaxeConfig, PaxePeer, PaxeRefusal, PaxeTransport};
pub use rejoin::gossip_datagram;
pub use relay::{Datagram, Ledger};
pub use state::FileStateStore;
