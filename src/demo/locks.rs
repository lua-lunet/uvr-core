//! The CAS lock table: the demo's whole application.
//!
//! One table, keyed by lock id, holds the lease records. A [`Request`]
//! arrives as the opaque payload of a committed operation; [`Service`]
//! executes it against the table and answers with a [`Response`]. The
//! table is the only state the lock service keeps, so its serialisable
//! view ([`StateSnapshot`]) is its state, the payload the state seam
//! flushes and loads.
//!
//! The grant rule is CAS: a lock is granted when no live lease stands or
//! the live lease already belongs to the requesting client, and the
//! requested window is positive. The client names a window and can never
//! name an absolute expiry; the executing node stamps
//! `expiry = clock + window` on the demo's replicated clock, so no client
//! clock enters the lease's semantics. The replicated clock is the slot
//! the operation applies at: the demo has no timers, and the log's own
//! progress is the one clock every replica already agrees on, which is
//! what makes a rebuilt table bit-identical to the survivors'.
//!
//! A release names the holder and the lease id, so it is a compare-and-swap
//! on (lock id, generation, holder) and a stale release can never retire a
//! newer lease. A release that retires nothing reports that it retired
//! nothing: with no live record standing there is nothing to compare
//! against, so the swap does not apply, and the reply says so. It is an
//! idempotent no-op, never a refusal and never an error.
//!
//! The generation belongs to the lock, not to the record standing for it: a
//! release removes the record, and the lock's generation watermark outlives
//! it, so no generation is ever handed out twice on the same lock. The
//! watermark is part of the table's state and rides the snapshot, so a
//! reloaded or rebuilt table is exactly as monotone as the one it resumes.
//!
//! The break is the unconditional force-release: whatever record stands is
//! replaced by the bumped, cleared keeper record, and the bump spends a
//! generation, so a break costs the next holder exactly what a release
//! costs.

use std::collections::BTreeMap;

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
/// use uvrr::demo::ClientId;
///
/// assert!(ClientId::new(0, 0).is_none(), "the nil identity is not a client");
/// assert!(ClientId::new(7, 1).is_some(), "a client is a pair the client named");
/// ```
///
/// Neither the nil identity nor its halves are nameable from out here, so
/// the doctest below is the compile-time half of the claim the mint states:
///
/// ```compile_fail
/// use uvrr::demo::ClientId;
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
    /// set, on the demo's replicated clock. Requests never carry it.
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
    /// `clock` is the demo's replicated clock, the slot the operation
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
