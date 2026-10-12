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

use uvrr::conformance::harness::{Harness, StepOutcome};
use uvrr::effects::Effect;
use uvrr::ids::{Ballot, CrashCounter, Era, NodeId, OperationId, SystemId, View};
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
