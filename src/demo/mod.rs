//! The locks demo (`demo-locks` feature): the advisory locks over uVRR
//! with the least environment.
//!
//! This is the paper's scaffold: a cluster of lock nodes on an in-memory
//! transport, one thread per node and one loop per thread, a lock table as
//! the whole application, and crash-reincarnation run end to end. What the
//! demo carries:
//!
//! - the contract seam the machinery is written against: [`Disk`] (every
//!   byte of disk access), [`StateStore`] (the lock table's persistence,
//!   under the eager-flush lazy-load law), [`CommitHook`] (a trigger at
//!   each applied commit);
//! - the CAS lock table itself: hold, renew, release, break, over one
//!   client table;
//! - the [`BootFence`]: a single small file carrying a single status word,
//!   one forced write per transition, read back at boot, the disk trusted.
//!   The demo's implementation is the trivial plain file; an industrial
//!   host mounts its superblock here and changes nothing above the trait;
//! - the single-threaded loop ([`DemoNode`]): drain the transport, feed
//!   the core, take the effects, emit. The transport is a seam, so a
//!   harness drives the loop in memory; the loop's intended real transport
//!   is PAXE, the sans-I/O codec a real loop calls;
//! - the resend ledger: the host half that replays exactly the datagrams
//!   the core released, byte-identical. The demo has no timers, so the
//!   harness drives every relay deterministically;
//! - the join gossip's datagram: the host obligation a node outside the
//!   cluster owes the wire.
//!
//! What the demo deliberately does not carry: timers and timeout
//! machinery, an industrial WAL, snapshots, an AOF, any external
//! dependency beyond the `serde` lane the lock table's payload codec
//! rides. Nothing here reads a wall clock: the lease clock is the
//! replicated log's own progress, the slot the operation applies at, the
//! one clock every replica already agrees on.
//!
//! The three seams are the whole distance between this scaffold and an
//! industrial deployment: mount the heavy disk, the industrial fence and
//! the real transport, and the machinery above them is unchanged.

pub mod disk;
pub mod fence;
pub mod locks;
pub mod node;
pub mod rejoin;
pub mod relay;
pub mod spi;
pub mod state;

pub use disk::{Disk, DiskFile, StdDisk, std_disk};
pub use fence::{BootFence, FenceWord, FileBootFence};
pub use locks::{ClientId, Lease, Request, Response, Service, StateSnapshot};
pub use node::{
    Applied, BootRefusal, DemoDisk, DemoNode, DemoRefusal, RunOutcome, StateLoad, Transport,
};
pub use rejoin::gossip_datagram;
pub use relay::{Datagram, Ledger};
pub use spi::{CommitHook, NoOpHook};
pub use state::{FileStateStore, StateStore, lazy_load};
