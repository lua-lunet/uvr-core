//! The host's relay ledger: the memory of un-acknowledged traffic.
//!
//! The core releases a message once and never re-releases it: a repeated
//! send is a relay of what the core already put on the wire, never a
//! message the host composes. This module is that host half, and nothing
//! else: the ledger remembers every datagram the node released that the
//! protocol gives a response to, and drops the ones a response answered.
//! The relay is a transport replay of those exact bytes to the same
//! peers; the ledger never mints a message.
//!
//! The demo has no timers, so nothing here decides when a relay is due.
//! The harness drives relays deterministically: it asks the ledger for
//! the relay set and replays it. The ledger is protocol, not a timer.

use crate::wire::Tag;

/// One datagram the core released, kept for the relay: the peer it was
/// addressed to, the ballot that authorised it, the slot its header
/// carries, the tag, and the exact bytes the network carried. The relay
/// re-sends these and nothing else, so a relayed datagram is
/// byte-identical to the datagram it repeats.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Datagram {
    /// The member the datagram was addressed to.
    pub to: u32,
    /// The era the header was authorised at.
    pub era: u32,
    /// The view the header was authorised at.
    pub view: u32,
    /// The slot the header speaks at.
    pub slot: u64,
    /// The header's tag, the core's own table's value.
    pub tag: Tag,
    /// The datagram, exactly as the core released it.
    pub bytes: Vec<u8>,
}

/// The tags whose repeats the relay carries, and the evidence each one is
/// answered by. The four classes are the resend policy's: phase 1
/// ([`Tag::Prepare`]), phase 2 ([`Tag::Commit`]), the ordinary fence
/// ([`Tag::StartViewChange`]) and the primary's evidence request
/// ([`Tag::DoViewChange`]). Everything else the core releases is either
/// already final (a [`Tag::StartView`] the cluster has installed) or
/// answered by the core's own re-release, and relaying it would put a
/// stale ballot on the wire for no gain.
fn relayable(tag: Tag) -> bool {
    matches!(
        tag,
        Tag::Prepare | Tag::Commit | Tag::StartViewChange | Tag::DoViewChange
    )
}

/// Whether one inbound datagram is the response that retires a ledger
/// entry. The evidence is the protocol's own, never an inference from
/// silence:
///
/// - a `Prepare` is answered by its recipient's `PrepareOk` at the same
///   slot or later (a replica acknowledges a slot only once it holds
///   every slot before it);
/// - a `Commit` carries no acknowledgement of its own, and the
///   recipient's `PrepareOk` at a LATER slot is the proof it applied
///   through this one;
/// - a `StartViewChange` fence vote is answered by the recipient's own
///   fence vote, which the receiver counts once however often it
///   repeats;
/// - `DoViewChange` evidence is answered by the designated primary
///   installing a view of this era, which retires the attempt whatever
///   became of the evidence itself.
///
/// A response from a different peer, or at a ballot the entry was not
/// authorised at, answers nothing.
fn answers(entry: &Datagram, from: u32, era: u32, view: u32, slot: u64, tag: Tag) -> bool {
    if entry.to != from || entry.era != era {
        return false;
    }
    match entry.tag {
        Tag::Prepare => tag == Tag::PrepareOk && view == entry.view && slot >= entry.slot,
        Tag::Commit => tag == Tag::PrepareOk && view == entry.view && slot > entry.slot,
        Tag::StartViewChange => tag == Tag::StartViewChange && view == entry.view,
        Tag::DoViewChange => tag == Tag::StartView && view >= entry.view,
        _ => false,
    }
}

/// The ledger of un-acknowledged traffic: every relayable datagram this
/// node released and no response has retired, bounded so a long
/// partition cannot grow it without limit. The bound drops the OLDEST
/// entries: the relay covers the recent window, and a datagram nobody
/// answered after that many successors is traffic the cluster has moved
/// past.
pub struct Ledger {
    unacked: Vec<Datagram>,
    bound: usize,
    /// How many entries the bound has dropped, for the operator's line.
    dropped: u64,
}

/// The default bound: a window far wider than any quorum response a
/// deterministic relay loop needs, so a healthy cluster never reaches it.
const DEFAULT_BOUND: usize = 256;

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

impl Ledger {
    /// An empty ledger with the default bound.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bound(DEFAULT_BOUND)
    }

    /// An empty ledger that keeps at most `bound` entries.
    #[must_use]
    pub fn with_bound(bound: usize) -> Self {
        Self {
            unacked: Vec::new(),
            bound: bound.max(1),
            dropped: 0,
        }
    }

    /// Remember one datagram the node released. A tag outside the four
    /// relayable classes is not the relay's business and is not kept. A
    /// repeat of a datagram already standing (the core released the same
    /// peer, ballot, slot and tag again) replaces it rather than doubling
    /// it, so one outstanding request is one relay.
    pub fn record(&mut self, datagram: Datagram) {
        if !relayable(datagram.tag) {
            return;
        }
        if let Some(standing) = self.unacked.iter_mut().find(|entry| {
            entry.to == datagram.to
                && entry.era == datagram.era
                && entry.view == datagram.view
                && entry.slot == datagram.slot
                && entry.tag == datagram.tag
        }) {
            *standing = datagram;
            return;
        }
        if self.unacked.len() >= self.bound {
            self.unacked.remove(0);
            self.dropped += 1;
        }
        self.unacked.push(datagram);
    }

    /// One inbound datagram arrived: retire every entry its tag answers.
    pub fn answered(&mut self, from: u32, era: u32, view: u32, slot: u64, tag: Tag) {
        if !relayable(tag) && !matches!(tag, Tag::PrepareOk | Tag::StartView) {
            return;
        }
        self.unacked
            .retain(|entry| !answers(entry, from, era, view, slot, tag));
    }

    /// The relay set for the ballot this node is in: every un-answered
    /// datagram authorised at exactly `(era, view)`, in send order. An
    /// entry from an earlier ballot is dead: the attempt it belonged to
    /// is over, and a receiver would discard the repeat, so it is never
    /// relayed.
    #[must_use]
    pub fn relay(&self, era: u32, view: u32) -> Vec<Datagram> {
        self.unacked
            .iter()
            .filter(|entry| entry.era == era && entry.view == view)
            .cloned()
            .collect()
    }

    /// How many datagrams are standing un-answered.
    #[must_use]
    pub fn unacknowledged(&self) -> usize {
        self.unacked.len()
    }

    /// How many entries the bound has dropped since boot.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}
