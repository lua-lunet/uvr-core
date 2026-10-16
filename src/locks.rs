//! The advisory locks machinery: the CAS lock table, and the seams it is
//! written against.
//!
//! This module is the lock service a consumer mounts. What it carries:
//!
//! - the CAS lock table itself: hold, renew, release, break, over one
//!   client table ([`Service`]), with the generation watermark that never
//!   hands a generation out twice, the truthful release, the
//!   nil-unrepresentable [`ClientId`], and the clock-parametric
//!   [`Service::execute`];
//! - the persistence contract ([`StateStore`]) and the boot fence
//!   ([`BootFence`], with its [`FenceWord`]): the seams an industrial host
//!   implements over its own storage engine and mounts in place of the
//!   demo's files, changing nothing above them;
//! - the commit trigger ([`CommitHook`]): the seam a host mounts for
//!   amortised maintenance and logging;
//! - [`lazy_load`]: the state seam's half of the flush-eager load-lazy
//!   law, with its guard stated once.
//!
//! The machinery is pure: no file, no socket, no clock read, no timer.
//! The lease clock is the replicated log's own progress, the slot the
//! operation applies at, the one clock every replica already agrees on.
//! The bytes in and out of [`Service::execute`] are the payload codec's
//! (`serde_json`, the `serde` lane this module rides); every byte of disk
//! access lives behind [`StateStore`] and [`BootFence`] at the host.
//!
//! The expiry window is the ephemeral lease of the fast-reads design
//! ([VR-2012] §6.3/§6.3.1, "Fast
//! Reads" and "Reads at the Primary", `docs/references.md`): the primary processes
//! reads unilaterally only while it holds valid leases from f other
//! replicas, and a new view starts only after the leases at f + 1
//! participants in the view-change protocol expire, which ensures the
//! new view starts after the old primary has stopped replying to read
//! requests, assuming clock rates are loosely synchronised. This table
//! carries that lease core under another name: a lock's expiry window
//! is an ephemeral lease taken on the replicated clock, and the
//! leader's fast-read lease is one application of the same arithmetic.
//!
//! The arithmetic is the cluster owner's, stated here once so it is
//! read before the timeouts are chosen:
//!
//! - the leader lease must outlast the reader lease plus the clock
//!   drift safety margin: an old primary that answers reads must hold
//!   its lease only while the new view cannot yet have started;
//! - for stale data to be served at all, the isolated old leader must
//!   still be answering while a new leader is elected AND makes
//!   progress: the `StartViewChange` and `DoViewChange` exchanges are
//!   quorum message rounds, and the rounds themselves are a safety
//!   margin beyond the lease expiry;
//! - a leader lease is not renewed while the cluster is idle with no
//!   reads: renewal rides traffic, and an idle cluster is not churned;
//! - a lease is therefore correct only under a stated set of timeouts
//!   with a stated clock drift, and both belong to the configuration,
//!   never to this table.
//!
//! The residual hazard is a dirty read during a leader crash: a
//! writer with one outstanding request, reading its own write over
//! the internet, may very rarely read stale because the lease was
//! fractionally too long. That hazard is indistinguishable from two
//! racing clients whose race merely appears reordered — the write did
//! happen first in the cluster, and the read missed it — so whether it
//! is noise or an outage is almost entirely the application's call:
//! whether the leases are set correctly, and whether a stale read is
//! catastrophic, are the application's questions, answered before the
//! timeouts are chosen.
//!
//! The demo (`demo` feature) carries this module's scaffold: the
//! plain-file store and fence over the demo's own disk seam, the
//! single-threaded node loop, the resend ledger, and the sealed transport.

use std::collections::BTreeMap;
use std::io;

use serde::de::{Deserializer, Error as _};
use serde::{Deserialize, Serialize};

/// A lock client's identity: 128 bits the client assigns, opaque to the
/// machinery. The table compares identities and nothing more.
///
/// The nil identity is unrepresentable outside this crate, by construction
/// rather than by a check at the point of use: the halves are private, the
/// lawful mint refuses the all-zero pair, and the door a request's client
/// comes through refuses it as well. The nil identity exists here alone,
/// as the keeper record's holder, and a keeper record is never live.
///
/// ```
/// use uvrr::locks::ClientId;
///
/// assert!(ClientId::new(0, 0).is_none(), "the nil identity is not a client");
/// assert!(ClientId::new(7, 1).is_some(), "a client is a pair the client named");
/// ```
///
/// Neither the nil identity nor its halves are nameable from out here, so
/// the doctest below is the compile-time half of the claim the mint states:
///
/// ```compile_fail
/// use uvrr::locks::ClientId;
///
/// let nil = ClientId::nil();
/// let parts = ClientId { msb: 0, lsb: 0 };
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId {
    msb: u64,
    lsb: u64,
}

impl ClientId {
    /// The lawful mint: the 128 bits the client chose, or none when the pair
    /// is the nil identity. Zero is refused, not defaulted, so the nil
    /// identity is a value this crate constructs and a client cannot name.
    #[must_use]
    pub const fn new(msb: u64, lsb: u64) -> Option<Self> {
        if msb == 0 && lsb == 0 {
            return None;
        }
        Some(Self { msb, lsb })
    }

    /// The nil identity, the cleared keeper record's holder: never live, and
    /// reachable only inside this crate. It is also the only identity a
    /// reply can carry that no client holds, which is how an observer reads
    /// a keeper record without being told.
    pub(crate) const fn nil() -> Self {
        Self { msb: 0, lsb: 0 }
    }
}

/// The identity as a request carries it: the two halves, off the wire,
/// strictly. A request's client is the one place the nil identity could
/// arrive from outside, so the refusal is at this door.
#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
struct ClientBits {
    msb: u64,
    lsb: u64,
}

/// The door a request's `client` field comes through: the wire's own mint,
/// which refuses the nil pair before the request exists. Nothing downstream
/// checks again, because after this there is nothing to check.
fn named_client<'de, D>(deserializer: D) -> Result<ClientId, D::Error>
where
    D: Deserializer<'de>,
{
    let bits = ClientBits::deserialize(deserializer)?;
    ClientId::new(bits.msb, bits.lsb)
        .ok_or_else(|| D::Error::custom("the nil client identity is not a client"))
}

/// The table's stored lease record and the reply's lease shape. Every
/// response carries the whole record (or none), so an observer classifies
/// the lock's state without diffing successive replies.
///
/// The record's `expiry` is an ephemeral lease on the replicated clock:
/// the fast-reads arithmetic of the module's documentation ([VR-2012]
/// §6.3.1, `docs/references.md`) is the owner's to settle before the timeouts
/// are chosen — the leader lease outlasting the reader lease plus the
/// clock drift margin, the quorum view-change rounds as a safety margin
/// beyond the expiry, no renewal while the cluster is idle, and the
/// application's own judgement of whether a rare stale read during a
/// leader crash is noise or an outage.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// The lease's generation. Bumped on every holder change and on a
    /// break, so a release naming an older generation retires nothing.
    /// The lock's generation watermark outlives the record, so a grant
    /// over a lock whose record was removed never answers a generation
    /// the lock has already spent.
    pub lease_id: u64,
    /// The holder the lease was granted to, or the keeper record's nil
    /// placeholder, which no client holds and which is never live.
    pub holder: ClientId,
    /// The clock-stamped expiry: `clock + window` at the holder-changing
    /// set, on the replicated clock. Requests never carry it.
    pub expiry: u64,
    /// The granted window, in committed slots: the log's own progress the
    /// lease lives for, echoed so the reply is self-describing.
    pub window: u64,
}

/// What the client asks. The shape decodes strictly: an unknown field or
/// an unknown operation is a refusal before the table moves.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    /// Read the live lease, or none.
    Get {
        /// The lock to read.
        lock_id: u64,
    },
    /// Ask for a grant or a same-holder renewal: the client names the
    /// window, never the expiry.
    Set {
        /// The requesting client. The nil identity is not one a request
        /// can carry: this field is minted through the door that refuses
        /// it.
        #[serde(deserialize_with = "named_client")]
        client: ClientId,
        /// The lock to take.
        lock_id: u64,
        /// The requested window, in committed slots. The grant condition
        /// is `window > 0`.
        window: u64,
    },
    /// Retire the lease the client holds, named by its generation.
    Release {
        /// The releasing client, through the same door: a release names a
        /// holder, and the nil identity holds nothing.
        #[serde(deserialize_with = "named_client")]
        client: ClientId,
        /// The lock to release.
        lock_id: u64,
        /// The generation the client holds.
        lease_id: u64,
    },
    /// The unconditional force-release, which names no holder.
    Break {
        /// The lock to break.
        lock_id: u64,
    },
}

/// What the table answered. Every field is carried in every reply (`lease`
/// as `null` where no record stands), so replies are self-describing.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Response {
    /// The read's answer.
    Get {
        /// The lock read.
        lock_id: u64,
        /// The live lease, or none.
        lease: Option<Lease>,
        /// The replicated clock the reply executed at: the slot the
        /// operation applied at, the same at every replica.
        executed_at: u64,
    },
    /// The grant's answer.
    Set {
        /// The lock named.
        lock_id: u64,
        /// Whether the grant or renewal stood.
        granted: bool,
        /// The stored lease on a grant, the live incumbent otherwise.
        lease: Option<Lease>,
        /// The replicated clock the reply executed at.
        executed_at: u64,
    },
    /// The release's answer.
    Release {
        /// The lock named.
        lock_id: u64,
        /// Whether the named lease was retired. `false` is the truth on
        /// every path that retires nothing: no live record stood, the
        /// holder is not this client, or the named generation is stale. A
        /// release that retires nothing is a no-op that says so, never a
        /// refusal and never an error.
        released: bool,
        /// The live incumbent when the release retired nothing, and none
        /// when it retired the lease or when nothing stood.
        lease: Option<Lease>,
        /// The replicated clock the reply executed at.
        executed_at: u64,
    },
    /// The break's answer.
    Break {
        /// The lock named.
        lock_id: u64,
        /// Whether a record stood to break. `false` reports that the lock
        /// was already free, with no record to break.
        broken: bool,
        /// On a break, the post-break keeper record: the bumped
        /// generation, the nil holder, and `expiry: 0`, never live.
        lease: Option<Lease>,
        /// The replicated clock the reply executed at.
        executed_at: u64,
    },
}

/// The table's whole durable state as one serialisable value: every lock
/// id's stored record, and every lock id's generation watermark. The state
/// seam flushes and loads this payload.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct StateSnapshot {
    /// The table itself, ordered so the encoded bytes are a function of
    /// the state and never of the map's insertion history.
    pub locks: BTreeMap<u64, Lease>,
    /// Every lock's generation watermark: the highest generation the lock
    /// has ever handed out or reserved, which outlives the record a
    /// release removes. A snapshot without the watermark is a decode
    /// refusal, never a defaulted empty one: a resumed table that read it
    /// as absent would hand out a generation twice.
    pub generations: BTreeMap<u64, u64>,
}

/// The CAS lock table: one client table, and the lock service's whole
/// state. Clone and default because a node constructs an empty table and
/// installs a loaded snapshot wholesale.
#[derive(Clone, Default)]
pub struct Service {
    locks: BTreeMap<u64, Lease>,
    generations: BTreeMap<u64, u64>,
}

impl Service {
    /// Executes one committed operation against the table.
    ///
    /// `clock` is the replicated clock, the slot the operation
    /// applies at: the stamp the lease's expiry and every reply's
    /// `executed_at` carry. A rebuilt table re-executes the same
    /// operations at the same slots, so the stamps it produces are the
    /// survivors' stamps. `payload` is the committed operation's opaque
    /// bytes, decoded here and nowhere else.
    ///
    /// # The state machine
    ///
    /// One table serves every client, keyed by lock id: a client has no
    /// table of its own and the machinery keeps nothing about it but the
    /// records it stands behind. Four transitions, each legal over the
    /// stored record exactly as stated:
    ///
    /// | request | legal when | the stored record afterwards |
    /// |---|---|---|
    /// | `Get` | always | unchanged |
    /// | `Set` | no live record stands, or the live record's holder is this client, and the window is positive | the granted record: a renewal keeps the live record's generation, otherwise the lock's next generation, one past its watermark |
    /// | `Release` | the live record's holder and generation are both this client | removed; the lock's watermark stands |
    /// | `Break` | a stored record stands | the keeper: generation bumped, nil holder, `expiry: 0`, never live |
    ///
    /// A refused `Set` is not an error: the reply carries `granted:
    /// false` beside the live incumbent, so a client reads the lock's
    /// state out of the reply it already has and never has to ask twice.
    /// A `Set` over a keeper record bumps once more, so a break costs the
    /// next holder a generation exactly as a release does.
    ///
    /// # Errors
    ///
    /// A payload that does not decode into a [`Request`], which includes a
    /// request naming the nil client identity: the identity is not
    /// constructible on the wire either, so such a request never reaches
    /// the table. The reply bytes are returned on success, and the reply
    /// itself never fails to encode.
    pub fn execute(&mut self, clock: u64, payload: &[u8]) -> Result<Vec<u8>, serde_json::Error> {
        let request: Request = serde_json::from_slice(payload)?;
        let response = match request {
            Request::Get { lock_id } => Response::Get {
                lock_id,
                lease: self.live(clock, lock_id),
                executed_at: clock,
            },
            Request::Set {
                client,
                lock_id,
                window,
            } => {
                let held = self.live(clock, lock_id);
                let granted =
                    held.as_ref().is_none_or(|lease| lease.holder == client) && window > 0;
                if granted {
                    let lease = self.grant(clock, lock_id, client, window, held);
                    Response::Set {
                        lock_id,
                        granted: true,
                        lease: Some(lease),
                        executed_at: clock,
                    }
                } else {
                    Response::Set {
                        lock_id,
                        granted: false,
                        lease: held,
                        executed_at: clock,
                    }
                }
            }
            Request::Release {
                client,
                lock_id,
                lease_id,
            } => {
                let held = self.live(clock, lock_id);
                // The compare-and-swap: a live record whose holder and
                // generation are this client, and nothing else. With no
                // live record there is nothing to compare against, so the
                // swap does not apply and the reply reports the truth.
                let released = held
                    .as_ref()
                    .is_some_and(|lease| lease.holder == client && lease.lease_id == lease_id);
                if released {
                    self.locks.remove(&lock_id);
                }
                Response::Release {
                    lock_id,
                    released,
                    lease: if released { None } else { held },
                    executed_at: clock,
                }
            }
            Request::Break { lock_id } => match self.locks.remove(&lock_id) {
                Some(broken) => {
                    let keeper = Lease {
                        lease_id: broken.lease_id.saturating_add(1),
                        holder: ClientId::nil(),
                        expiry: 0,
                        window: 0,
                    };
                    // The bump is spent the moment it is reserved: the
                    // watermark carries it, so the next holder of this
                    // lock pays for it even if the keeper is later
                    // removed.
                    self.generations.insert(lock_id, keeper.lease_id);
                    self.locks.insert(lock_id, keeper.clone());
                    Response::Break {
                        lock_id,
                        broken: true,
                        lease: Some(keeper),
                        executed_at: clock,
                    }
                }
                None => Response::Break {
                    lock_id,
                    broken: false,
                    lease: None,
                    executed_at: clock,
                },
            },
        };
        serde_json::to_vec(&response)
    }

    /// The grant or same-holder renewal, as the stored record. A renewal
    /// keeps the incumbent's generation, the same lease extended. A grant
    /// over no live record takes the lock's next generation: one past the
    /// watermark, which the record a release removes never carried.
    fn grant(
        &mut self,
        clock: u64,
        lock_id: u64,
        client: ClientId,
        window: u64,
        held: Option<Lease>,
    ) -> Lease {
        let spent = self.generations.get(&lock_id).copied().unwrap_or(0);
        let lease_id = match &held {
            Some(incumbent) => incumbent.lease_id,
            None => spent.saturating_add(1),
        };
        let lease = Lease {
            lease_id,
            holder: client,
            expiry: clock.saturating_add(window),
            window,
        };
        self.generations.insert(lock_id, lease_id);
        self.locks.insert(lock_id, lease.clone());
        lease
    }

    /// The live lease at `lock_id`, or none: a stored record whose expiry
    /// sits at or before the clock is not live.
    fn live(&self, clock: u64, lock_id: u64) -> Option<Lease> {
        self.locks
            .get(&lock_id)
            .cloned()
            .filter(|lease| lease.expiry > clock)
    }

    /// Whether the table holds nothing at all: no record and no watermark.
    /// The lazy-load seam's first clause: a table with anything in it was
    /// materialised by the regular path (or by an earlier load), so a load
    /// now would overwrite state the node already holds, the watermark
    /// included.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.locks.is_empty() && self.generations.is_empty()
    }

    /// The table's whole state as one serialisable value, captured at the
    /// moment the caller asks. The state seam's flush payload.
    #[must_use]
    pub fn snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            locks: self.locks.clone(),
            generations: self.generations.clone(),
        }
    }

    /// Install a loaded snapshot as the table's whole contents.
    ///
    /// Wholesale, not a merge: the loaded value is the pre-stop table, so
    /// a merge against whatever the node holds would fabricate records
    /// the stop never wrote. The seam calls this on the lazy-load path
    /// only, against a table it has just established is empty.
    ///
    /// The watermarks install with the records: a resumed table that
    /// dropped them would hand out a generation the lock had already
    /// spent.
    pub fn restore(&mut self, snapshot: StateSnapshot) {
        self.locks = snapshot.locks;
        self.generations = snapshot.generations;
    }
}

/// The lock table's persistence contract: flush the whole table on the
/// shutdown path, load it back on the regular path.
///
/// The law, restated on the trait itself so every implementation inherits
/// it:
///
/// - **FLUSH is EAGER**: on the shutdown path the whole lock-table state
///   is written in the foreground before the stop completes. A stop that
///   cannot flush is a FAILED stop: it surfaces the failure, it never
///   silently skips.
/// - **LOAD is LAZY**: the regular path never loads eagerly at boot. The
///   table starts empty and state materialises only on demand.
/// - **A CRASHED boot DISTRUSTS the state file**: load returns nothing
///   and the node rebuilds from the replica stream. Only a clean-stop
///   file is loadable.
///
/// `flush` takes `&mut self` because a store may hold a staging
/// position; `load` takes `&self` because loading must never mutate the
/// store: the snapshot it hands back is the caller's to apply or drop.
pub trait StateStore: Send + Sync {
    /// Write the whole lock-table state, durably, before returning. The
    /// eager half of the law: the caller is the shutdown path, in the
    /// foreground, between the stop's first fence write and the final
    /// one. An `Err` fails the stop; the caller surfaces it and never
    /// skips it.
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()>;

    /// Read the state back, or report that there is none to read.
    ///
    /// `Ok(None)` is every "there is nothing trustworthy here" answer:
    /// no file, a version this build does not know, a length that does
    /// not match the bytes present, a failed checksum, or a payload that
    /// does not decode. Never an `Err` for a content problem: a fallback
    /// that blocks the boot is an outage, and this is a fallback. The
    /// crashed-boot guard lives at the call site, never here: a store
    /// that is handed a load has already been cleared for it.
    fn load(&self) -> io::Result<Option<StateSnapshot>>;
}

/// THE LAZY LOAD, with its guard, as one function: the seam's half of the
/// law, kept beside the trait so the guard is stated once and can be
/// driven without a node.
///
/// The guard has two clauses and both are refusals:
///
/// - **A CRASHED boot distrusts the file.** `clean_boot` is the fence's
///   verdict: only a clean stop, the one whose final fence write landed
///   over the completed flush, left a file worth reading. A crashed boot
///   reads nothing and rebuilds from the replica stream, even when a
///   perfectly readable file is sitting there.
/// - **A populated table is never overwritten.** `table_empty` says the
///   caller has established the table holds nothing; anything in it came
///   from the regular path or an earlier load, and a load now would
///   overwrite live state.
///
/// The one-shot clause, at most one load per node life, belongs to the
/// caller, because it is per-life state and this function is not.
///
/// Every refusal is `Ok(None)`: the same answer a missing file gives.
/// Nothing here can fail a boot.
pub fn lazy_load(
    store: &dyn StateStore,
    clean_boot: bool,
    table_empty: bool,
) -> io::Result<Option<StateSnapshot>> {
    if !clean_boot {
        return Ok(None);
    }
    if !table_empty {
        return Ok(None);
    }
    store.load()
}

/// The fence's status word, the one value the single file carries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FenceWord {
    /// The boot anchor: this life's writes are unproven.
    Stopping,
    /// The halt has begun; the flush is unproven under it.
    Stopped,
    /// The flush was forced over the halt; the clean word. Only a stop
    /// that completed its flush writes it, so only a stop can make a
    /// boot clean.
    Flushed,
}

impl FenceWord {
    /// The word as the file carries it: one word, one line.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            FenceWord::Stopping => "stopping",
            FenceWord::Stopped => "stopped",
            FenceWord::Flushed => "flushed",
        }
    }

    /// The word a line reads as, or none for anything the fence did not
    /// write.
    #[must_use]
    pub fn of_str(word: &str) -> Option<Self> {
        match word {
            "stopping" => Some(FenceWord::Stopping),
            "stopped" => Some(FenceWord::Stopped),
            "flushed" => Some(FenceWord::Flushed),
            _ => None,
        }
    }
}

/// The boot fence's contract: write the status word, one forced write;
/// read it back at boot; trust the disk.
///
/// `write` takes `&mut self` because a write is a transition of the
/// fence's own state; `read` takes `&self` because reading is the boot's
/// verdict and must never mutate the fence.
pub trait BootFence: Send + Sync {
    /// One forced write of `word`: staged temp file, fsync, rename,
    /// parent fsync. The fence is one small file and the write is one
    /// transition; there is nothing to batch and nothing to amortise.
    ///
    /// # Errors
    ///
    /// The disk refused. A stop that cannot advance its fence word is a
    /// failed stop, and a boot that cannot write its anchor refuses to
    /// start; the caller surfaces the error in both cases.
    fn write(&mut self, word: FenceWord) -> io::Result<()>;

    /// The word at boot, or `None` when no fence was ever written (the
    /// first life). A file the fence did not write reads as
    /// [`FenceWord::Stopping`], the unproven word: the fence classifies
    /// the past conservatively and never invents the clean word.
    ///
    /// The clean word is the stop's to write and never the boot's. A boot
    /// that reads [`FenceWord::Flushed`] refuses to start on it, and a
    /// boot that reads anything else has no ground on which to promote
    /// itself by writing it: the word vouches for a flush the boot did
    /// not perform, and a fence that let a life buy its own clean verdict
    /// would make the third word worthless. Every boot's own durable act
    /// is the [`FenceWord::Stopping`] anchor, which says nothing and
    /// proves nothing, which is exactly what a boot may claim.
    ///
    /// # Errors
    ///
    /// The read itself failed; only a disk error is an `Err`. Content
    /// problems are classifications, never errors.
    fn read(&self) -> io::Result<Option<FenceWord>>;
}

use crate::ids::Slot;

/// A trigger fired at each applied commit.
///
/// Called synchronously on the apply path, once per applied commit,
/// between the boot's one lazy state load and the state machine's
/// execution: a hook sees the commit after the node's state has been
/// settled for it and before the table has moved. A hook therefore
/// observes commits in the order the replica committed them and never
/// asynchronously with respect to them, and a hook that kills the process
/// kills it at a commit the state file has not yet been told about.
///
/// The method returns nothing, and that is the whole contract. The
/// callback carries no verdict: a hook cannot fail a commit, cannot
/// refuse one and cannot tell the machinery anything, because the
/// machinery has no use for the answer. Whatever a hook does, crash the
/// process at a named commit, flush every N commits, log the commit, is
/// the hook's own affair, and the machinery's ignorance is what keeps the
/// seam a seam.
///
/// `&mut self` because a hook may hold a position across commits: an
/// amortised flush counts them, and a counter is state. `Send + Sync`
/// because a hook is mounted once and lives as long as the node does.
pub trait CommitHook: Send + Sync {
    /// One applied commit. `slot` is the slot the entry committed at, so
    /// a hook can name the commit it acts on.
    fn on_commit(&mut self, slot: Slot);
}

/// The hook that does nothing: the default wiring.
///
/// Every node built without an explicit hook holds this, so the default
/// path pays one dynamic dispatch per commit and nothing else. A consumer
/// mounting its own hook replaces it wholesale.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoOpHook;

impl CommitHook for NoOpHook {
    fn on_commit(&mut self, _slot: Slot) {}
}
