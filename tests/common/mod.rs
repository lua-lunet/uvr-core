//! The shared harness vocabulary: one definition of each helper the
//! protocol test binaries say the same thing with.
//!
//! Every `tests/*.rs` file is its own crate, so each binary that speaks
//! these names carried its own copy of them. A binary includes this module
//! by path and imports the names it speaks:
//!
//! ```text
//! #[path = "common/mod.rs"]
//! mod common;
//!
//! use common::{current_view, n, op_id, snap, status_of, view};
//! ```
//!
//! What belongs here is a helper that is the same helper everywhere it
//! appears: one behaviour, one body, one name. A helper whose behaviour
//! differs between binaries is not the same helper and stays local, as do
//! the binaries whose harness is their own over a public surface rather
//! than over this vocabulary.
//!
//! # Partial use, and the dead-code lint
//!
//! A binary compiles this module whole whether it speaks three of these
//! names or all of them, so a binary that speaks three sees the rest as
//! unused, and the warnings are errors. The lint is allowed for this
//! module alone, together with the unused imports that partial use also
//! leaves behind. Nothing stronger is used: a per-entry allow would have
//! to name every entry and would say nothing the module-level line does
//! not.
//!
//! The compiler cannot catch a helper that no binary calls, because the
//! module is compiled into every binary that includes it, and a binary
//! that does not call one of its entries is not a failing binary. What
//! holds the line is that every name here is spoken by the suite: an
//! entry with no caller is an entry no test reaches, and this file is the
//! only place one can be added, so a diff that adds one shows it. The
//! honest limit of that is worth stating: `cargo clippy --all-targets
//! --all-features` compiles every binary against this module and catches
//! an entry that stops type-checking, but nothing mechanical in this
//! repository catches an entry that is simply never called.

#![allow(dead_code, unused_imports)]

use uvrr::configuration::Configuration;
use uvrr::conformance::harness::{Harness, StepOutcome};
use uvrr::effects::Effect;
use uvrr::ids::{Ballot, CrashCounter, Era, NodeId, OperationId, Slot, SystemId, View};
use uvrr::journal::{LogEntry, Payload};
use uvrr::message::Message;
use uvrr::progress::{ProgressSnapshot, Status};
use uvrr::replica::ViewChangeKnobs;

/// Node id shorthand (the harness's own pattern).
pub const fn n(id: u32) -> NodeId {
    NodeId::new(
        SystemId::new((id + 1) as u16).expect("test system ids are small and non-zero"),
        CrashCounter::new(1).expect("one is non-zero"),
    )
}

/// An operation identity for the scripts: the host assigns it, the core
/// carries it opaque (B2).
pub fn op_id(lsb: u64) -> OperationId {
    OperationId { msb: 0, lsb }
}

/// A view in era 1, the era every node here bootstraps into.
pub fn view(number: u32) -> Ballot {
    Ballot {
        era: Era(1),
        view: View(number),
    }
}

/// The snapshot of a live node (tests never snapshot a crashed one).
pub fn snap(h: &Harness, id: NodeId) -> ProgressSnapshot {
    h.snapshot(id).expect("the node is live")
}

/// The node's status, decoded from the observation word.
pub fn status_of(h: &Harness, id: NodeId) -> Status {
    Status::from_word(snap(h, id).status).expect("the word is a status")
}

/// The node's current view as a [`uvrr::ids::Ballot`].
pub fn current_view(h: &Harness, id: NodeId) -> Ballot {
    let snapshot = snap(h, id);
    Ballot {
        era: Era(snapshot.era),
        view: View(snapshot.view),
    }
}

/// Bootstraps the cluster: the genesis primary promotes itself and both
/// backups adopt view (1, 0) from the promotion's
/// [`uvrr::wire::Tag::Commit`] announcement.
pub fn bootstrap(h: &mut Harness) {
    h.tick_all();
    h.deliver_all();
    h.assert_safety();
}

/// A three-node cluster whose view-change machinery is live, at the
/// caller's timeout. The node count is the one these scripts share; the
/// timeout is the one each of them names for itself.
pub fn three_node_cluster(timeout: u64) -> Harness {
    Harness::with_knobs(
        3,
        ViewChangeKnobs {
            primary_timeout: timeout,
            view_change_budget: usize::MAX,
        },
    )
}

/// The messages one step released, as `(to, message)` pairs.
pub fn released(outcome: &StepOutcome) -> Vec<(NodeId, Message)> {
    match outcome {
        StepOutcome::Published { effects, .. } => effects
            .iter()
            .filter_map(|e| match e {
                Effect::Send { to, message, .. } => Some((*to, message.clone())),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The relation of one numeric field against the receiver's own value.
#[derive(Clone, Copy)]
pub enum Rel {
    Less,
    Equal,
    Greater,
}

pub const REL_ALL: [Rel; 3] = [Rel::Less, Rel::Equal, Rel::Greater];

/// The field's value under `rel` against `base`.
pub fn rel(r: Rel, base: u64) -> u64 {
    match r {
        Rel::Less => base - 1,
        Rel::Equal => base,
        Rel::Greater => base + 1,
    }
}

/// The view-change timeout the scripts share: a node suspects its primary
/// after more than three ticks of silence (S4).
pub const TIMEOUT: u64 = 3;

/// The three members of the cluster the rejoin scripts rehearse.
pub const ALL: [NodeId; 3] = [n(0), n(1), n(2)];

/// The era the node's committed history has established.
pub fn current_era(h: &Harness, id: NodeId) -> Era {
    h.era_table(id).expect("the node is live").current().era
}

/// The primary of a view, as the observer's own era table names it.
pub fn primary_of(h: &Harness, observer: NodeId, view: Ballot) -> Option<NodeId> {
    h.era_table(observer)?
        .record(view.era)
        .and_then(|record| record.config.primary(view.view))
}

/// The member weights of the node's current configuration, in order.
pub fn current_weights(h: &Harness, id: NodeId) -> Vec<u64> {
    h.era_table(id)
        .expect("the node is live")
        .current()
        .config
        .order()
        .iter()
        .map(|member| u64::from(member.weight.0))
        .collect()
}

/// The member identities of the node's current configuration, in order.
pub fn current_order(h: &Harness, id: NodeId) -> Vec<NodeId> {
    h.era_table(id)
        .expect("the node is live")
        .current()
        .config
        .order()
        .iter()
        .map(|member| member.node)
        .collect()
}

/// The view-change target the fence machinery itself chooses for `node`:
/// the next view in the era the committed history has established
/// (§8.7.8). The primary of that view is whoever the position names.
pub fn fence_target(h: &Harness, node: NodeId) -> Ballot {
    let current = current_view(h, node);
    Ballot {
        era: current_era(h, node),
        view: View(current.view.0 + 1),
    }
}

/// Drives the view change the fence machinery targets, asserting every
/// node in `live` installs it.
pub fn drive_view_change(h: &mut Harness, live: &[NodeId]) -> Ballot {
    let target = fence_target(h, live[0]);
    let driver = live
        .iter()
        .copied()
        .find(|&id| status_of(h, id) == Status::Normal)
        .expect("a live Normal member drives the fence");
    for _ in 0..=TIMEOUT {
        h.tick(driver);
    }
    h.deliver_all();
    for &id in live {
        assert_eq!(status_of(h, id), Status::Normal, "{id:?} installs");
        assert_eq!(current_view(h, id), target, "{id:?} is in the target");
    }
    h.assert_safety();
    target
}

/// Ticks the node into view change and asserts it installed `target`.
pub fn tick_into_view_change(h: &mut Harness, id: NodeId, target: Ballot) {
    for _ in 0..=TIMEOUT {
        h.tick(id);
    }
    assert_eq!(status_of(h, id), Status::ViewChange);
    assert_eq!(current_view(h, id), target);
}

/// A ballot in era 2, the era a pivot reconfiguration establishes.
pub fn era2_view(number: u32) -> Ballot {
    Ballot {
        era: Era(2),
        view: View(number),
    }
}

/// The genesis view: era 1, the initial view.
pub fn genesis_view() -> Ballot {
    Ballot {
        era: Era(1),
        view: View::INITIAL,
    }
}

/// The next view in `era` whose configuration selects `leader`.
pub fn view_selecting(h: &Harness, leader: NodeId, era: Era) -> Ballot {
    let snapshot = h.snapshot(leader).expect("live");
    let table = h.era_table(leader).expect("live");
    let record = table
        .record(era)
        .unwrap_or_else(|| panic!("era {era:?} is established"));
    for index in snapshot.view + 1.. {
        let view = View(index);
        if record.config.primary(view) == Some(leader) {
            return Ballot { era, view };
        }
    }
    unreachable!("a view selecting the leader is representable")
}

/// A three-node cluster at view 1, every node Normal: the baseline the
/// exhaustive case drivers assemble their scenarios on.
#[rustfmt::skip]
pub fn assembled() -> Harness {
    let knobs = ViewChangeKnobs { primary_timeout: TIMEOUT, view_change_budget: usize::MAX };
    let mut h = Harness::with_knobs(3, knobs);
    h.tick_all();
    h.deliver_all();
    for _ in 0..=TIMEOUT { h.tick(n(1)); }
    h.deliver_all();
    h
}

/// The timeout knobs the rejoin scripts share, at [`TIMEOUT`].
pub fn knobs() -> uvrr::replica::ViewChangeKnobs {
    uvrr::replica::ViewChangeKnobs {
        primary_timeout: TIMEOUT,
        view_change_budget: usize::MAX,
    }
}

/// The operation payload a committed entry carries.
pub fn payload_of(entry: &LogEntry) -> &[u8] {
    let Payload::Operation { payload, .. } = &entry.payload else {
        panic!("expected an operation payload at {:?}", entry.slot);
    };
    payload
}

/// The apply effect one step released, if any.
pub fn released_apply(outcome: &StepOutcome) -> Option<(Slot, OperationId)> {
    match outcome {
        StepOutcome::Published { effects, .. } => effects.iter().find_map(|e| match e {
            Effect::Apply {
                slot, operation_id, ..
            } => Some((*slot, *operation_id)),
            _ => None,
        }),
        _ => None,
    }
}

/// Commits one operation through the primary and applies the effects at
/// every node.
pub fn commit_one(h: &mut Harness, primary: NodeId, lsb: u64, payload: &[u8]) {
    h.propose(primary, op_id(lsb), payload);
    h.deliver_all();
    for id in [n(0), n(1), n(2)] {
        h.execute_apply_effects(id);
    }
}

/// The payloads the node has applied, in order.
pub fn applied_payloads(h: &Harness, id: NodeId) -> Vec<Vec<u8>> {
    h.applied(id)
        .iter()
        .map(|(_, payload)| payload.to_vec())
        .collect()
}

/// The configuration's (node, weight) shape, in order.
pub fn shape(config: &Configuration) -> Vec<(NodeId, u32)> {
    config
        .order()
        .iter()
        .map(|member| (member.node, member.weight.0))
        .collect()
}

/// The configuration's member weights, in order.
pub fn weights(config: &Configuration) -> Vec<u64> {
    config
        .order()
        .iter()
        .map(|member| u64::from(member.weight.0))
        .collect()
}
