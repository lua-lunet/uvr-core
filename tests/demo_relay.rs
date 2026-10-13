//! The resend ledger's one obligation, through its public surface.
//!
//! The core releases a message once and never re-releases it. What the
//! host owes the wire is the replay of exactly those bytes to exactly the
//! peer that never answered, and nothing else: the ledger never mints a
//! message, and the demo has no timers, so nothing here decides when a
//! relay is due. Each test states the obligation in one sentence.

#![cfg(feature = "demo-locks")]

use uvrr::demo::relay::{Datagram, Ledger};
use uvrr::wire::Tag;

fn prepare(to: u32, slot: u64) -> Datagram {
    Datagram {
        to,
        era: 4,
        view: 42,
        slot,
        tag: Tag::Prepare,
        bytes: vec![Tag::Prepare as u8, slot as u8],
    }
}

fn commit(to: u32, slot: u64) -> Datagram {
    Datagram {
        to,
        era: 4,
        view: 42,
        slot,
        tag: Tag::Commit,
        bytes: vec![Tag::Commit as u8, slot as u8],
    }
}

#[test]
fn the_relay_returns_every_unanswered_datagram_byte_for_byte() {
    let mut ledger = Ledger::new();
    ledger.record(prepare(2, 10));
    ledger.record(commit(2, 11));
    ledger.record(prepare(3, 10));
    assert_eq!(ledger.unacknowledged(), 3);
    let relayed = ledger.relay(4, 42);
    assert_eq!(relayed.len(), 3, "nothing was answered: {relayed:?}");
    assert_eq!(relayed[0], prepare(2, 10));
    assert_eq!(relayed[0].bytes, prepare(2, 10).bytes);
}

#[test]
fn a_prepare_ok_from_one_peer_answers_only_that_peers_entry() {
    let mut ledger = Ledger::new();
    ledger.record(prepare(2, 10));
    ledger.record(prepare(3, 10));
    ledger.answered(2, 4, 42, 10, Tag::PrepareOk);
    let relayed = ledger.relay(4, 42);
    assert_eq!(relayed.len(), 1);
    assert_eq!(relayed[0].to, 3, "the other peer's prepare is unanswered");
}

#[test]
fn a_later_prepare_ok_answers_the_commit_below_it() {
    let mut ledger = Ledger::new();
    ledger.record(commit(2, 10));
    ledger.answered(2, 4, 42, 10, Tag::PrepareOk);
    assert_eq!(
        ledger.unacknowledged(),
        1,
        "an ack at the commit's own slot does not prove the commit was applied"
    );
    ledger.answered(2, 4, 42, 11, Tag::PrepareOk);
    assert_eq!(ledger.unacknowledged(), 0);
}

#[test]
fn an_older_ballots_entry_is_never_relayed() {
    let mut ledger = Ledger::new();
    ledger.record(prepare(2, 10));
    assert!(
        ledger.relay(4, 43).is_empty(),
        "the view advanced: the attempt is dead"
    );
    assert_eq!(ledger.unacknowledged(), 1, "and it is still counted");
}

#[test]
fn a_tag_outside_the_four_classes_is_not_the_relays_business() {
    let mut ledger = Ledger::new();
    ledger.record(Datagram {
        to: 2,
        era: 4,
        view: 42,
        slot: 10,
        tag: Tag::Reincarnation,
        bytes: vec![Tag::Reincarnation as u8],
    });
    assert_eq!(ledger.unacknowledged(), 0);
}

#[test]
fn the_bound_drops_the_oldest_and_counts_it() {
    let mut ledger = Ledger::with_bound(3);
    for slot in 1..=6 {
        ledger.record(prepare(2, slot));
    }
    assert_eq!(ledger.unacknowledged(), 3);
    assert_eq!(ledger.dropped(), 3);
    let relayed = ledger.relay(4, 42);
    assert_eq!(relayed[0].slot, 4, "the oldest went first");
    assert_eq!(relayed[2].slot, 6);
}

#[test]
fn a_released_repeat_is_one_relay_not_two() {
    let mut ledger = Ledger::new();
    ledger.record(prepare(2, 10));
    ledger.record(prepare(2, 10));
    assert_eq!(ledger.unacknowledged(), 1);
}
