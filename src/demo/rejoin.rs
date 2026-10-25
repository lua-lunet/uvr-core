//! The rejoin gossip's joiner half: the host obligation a node outside
//! the cluster owes the wire.
//!
//! Rejoining is a gossip protocol outside the main UVR protocol: a node
//! outside the cluster never assumes the cluster will come to it. It
//! gossips its wish to join to every node it knows about, on its own
//! resend discipline, and the cluster streams it until it is promoted.
//! The demo has no timers, so the harness drives the gossip's repeats;
//! this module is the datagram itself, and nothing else.
//!
//! Why the send is the host's: the core emits no `GossipRequest`. A
//! joining node's tick drives only an already-open fetch, and the fetch
//! opens only through paths a fenced fresh boot never reaches. The core
//! HANDLES the message on receive: every node that hears it records the
//! sender as a gossip witness, and the leader answers with the missed
//! range above the sender's frontier plus a fresh commit.
//!
//! Why the datagram carries the node's CURRENT view: the leader's answer
//! echoes the request's view, and the echo is what qualifies the push at
//! the boot fence. The entry ticket names the view the node already
//! holds, never one it has merely heard of. The frontiers ride the body;
//! a fresh boot names none, so the leader's push re-sends the whole
//! history and the install path dedups what the node already holds.

use crate::ids::{Ballot, Era, Slot, View};
use crate::message::{Body, Message};
use crate::wire::{Header, Pack, Tag};

/// The join-gossip datagram: the core's own `Body::GossipRequest` wire
/// frame, packed with the core's own `Pack` at the node's current
/// `(era, view)`. The header slot is the `Absent` sentinel; the body
/// carries the frontiers the host can name, and a fresh boot names none.
#[must_use]
pub fn gossip_datagram(era: u32, view: u32) -> Vec<u8> {
    let message = Message {
        header: Header {
            tag: Tag::GossipRequest,
            view: Ballot {
                era: Era(era),
                view: View(view),
            },
            slot: Slot::NONE,
        },
        body: Body::GossipRequest {
            prepared: Slot::NONE,
            committed: Slot::NONE,
        },
    };
    let mut frame = vec![0u8; message.packed_len()];
    let written = message
        .pack_into(&mut frame)
        .expect("the core packs its own body");
    debug_assert_eq!(written, frame.len());
    frame
}
