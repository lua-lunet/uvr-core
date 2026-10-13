//! Exhaustive per-message properties: the [`uvrr::wire::Tag::PrepareOk`] the serving primary
//! counts (§4).
//!
//! One message over the cross product of the dimensions the handler branches
//! on: the sender's membership, the acknowledgement's view against the
//! primary's current view, and the acknowledged slot against the outstanding
//! proposal. Exhaustive by construction under the host obligations: the
//! identity law (universally unique and never recycled, so a minted foreign
//! sender is outside the configuration for the life of the test), the
//! boot-gate marker states (the primary was never halted, so it holds its
//! assembled [`uvrr::progress::Status::Normal`] life), and the quorum gate fixed at construction (Q1).
//! The learner-discard dimension is not enumerated: the assembled
//! configuration holds no weight-zero member, and the learner rule is the
//! reincarnation suite's.

#[path = "harness/mod.rs"]
mod harness;

#[path = "common/mod.rs"]
mod common;

use common::{REL_ALL, Rel, TIMEOUT, n, rel, released, released_apply, view};

use harness::{Harness, StepOutcome, mint_id, mint_pair};
use uvrr::ids::{OperationId, Slot};
use uvrr::message::{Body, Message};
use uvrr::observe::Diagnostic;
use uvrr::replica::ViewChangeKnobs;
use uvrr::wire::{Header, Tag};

const PAYLOAD: &[u8] = b"ordered";

/// Who the acknowledgement claims to come from.
#[rustfmt::skip]
#[derive(Clone, Copy)]
enum Sender { Voter, Foreign }

const SENDER_ALL: [Sender; 2] = [Sender::Voter, Sender::Foreign];

/// What the handler does with the case, named by an exhaustive match.
#[rustfmt::skip]
enum Route { UnknownSender, ViewMismatch, SlotNotOutstanding, Commit }

#[rustfmt::skip]
fn route(sender: Sender, v: Rel, s: Rel) -> Route {
    match sender {
        Sender::Foreign => Route::UnknownSender,
        Sender::Voter => match v {
            Rel::Less | Rel::Greater => Route::ViewMismatch,
            Rel::Equal => match s {
                Rel::Equal => Route::Commit,
                _ => Route::SlotNotOutstanding,
            },
        },
    }
}

/// A three-node cluster at view 1, whose primary is the node the change
/// designated: one outstanding proposal at slot 3, whose only [`uvrr::wire::Tag::Prepare`] was
/// accepted by the backup that votes here.
#[rustfmt::skip]
fn assembled(op: OperationId) -> Harness {
    let knobs = ViewChangeKnobs { primary_timeout: TIMEOUT, view_change_budget: usize::MAX };
    let mut h = Harness::with_knobs(3, knobs);
    h.tick_all();
    h.deliver_all();
    for _ in 0..=TIMEOUT { h.tick(n(1)); }
    h.deliver_all();
    h.propose(n(1), op, PAYLOAD);
    h.deliver_to_matching(n(0), Tag::Prepare, Slot(3));
    h
}

#[rustfmt::skip]
fn run_case(sender: Sender, v: Rel, s: Rel) {
    let (system, crash) = mint_pair();
    let op = OperationId { msb: (u64::from(system.get()) << 16) | u64::from(crash.get()), lsb: 1 };
    assert!(op.msb != 0 && op.lsb != 0, "the mint never draws a zero half");
    let mut h = assembled(op);
    let receiver = n(1);
    let header_view = view(rel(v, 1) as u32);
    let slot = Slot(rel(s, 3));
    let from = match sender {
        Sender::Voter => n(0),
        Sender::Foreign => mint_id(),
    };
    let header = Header { tag: Tag::PrepareOk, view: header_view, slot };
    let outcome = h.inject(from, receiver, Message { header, body: Body::PrepareOk {} });
    let diagnostic = h.diagnostic(receiver);
    let after = h.snapshot(receiver).expect("the receiver is live");
    assert!(h.fault_of(receiver).is_none() && h.check_safety().is_ok());
    let sent = released(&outcome);
    let published = matches!(outcome, StepOutcome::Published { .. });
    let named = |d: &Option<Diagnostic>| match sender {
        Sender::Foreign => matches!(d, Some(Diagnostic::UnknownSender { sender: x }) if *x == from),
        Sender::Voter => matches!(d, Some(Diagnostic::ViewMismatch { got, .. }) if *got == header_view)
            || matches!(d, Some(Diagnostic::SlotNotOutstanding { slot: x, sender: y })
                if *x == slot && *y == from),
    };
    match route(sender, v, s) {
        Route::UnknownSender | Route::ViewMismatch | Route::SlotNotOutstanding => {
            assert!(published && sent.is_empty(), "{sent:?}");
            assert!(named(&diagnostic), "{diagnostic:?}");
            assert_eq!(after.committed, 2, "the vote was never counted");
        }
        Route::Commit => {
            assert!(published);
            assert_eq!(after.committed, 3, "the vote completed the quorum");
            assert!(sent.len() == 2, "the Apply leads and the commit broadcasts: {sent:?}");
            assert!(
                matches!(released_apply(&outcome), Some((Slot(3), id)) if id == op),
                "the Apply carries the minted identity: {outcome:?}"
            );
            assert!(sent.iter().all(|(_, m)| m.header.tag == Tag::Commit
                && matches!(&m.body, Body::Commit { committed: Slot(x) } if *x == 3)),
                "{sent:?}");
        }
    }
}

#[test]
fn exhaustive_prepare_ok_at_the_primary() {
    for sender in SENDER_ALL {
        for v in REL_ALL {
            for s in REL_ALL {
                run_case(sender, v, s);
            }
        }
    }
}
