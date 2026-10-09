//! The commit hook: a trigger at each applied commit.
//!
//! [`CommitHook::on_commit`] is a trigger, not a decision point. It
//! returns nothing because there is nothing for the machinery to do with
//! an answer: a commit has happened, the machinery applies it, and what
//! happens around that commit is the consumer's affair. The experimental
//! harness kills a node at a named commit, by whatever means it chooses,
//! so a crash is exercised at a chosen point instead of after a timeout.
//! An industrial consumer uses the same callback for amortised
//! maintenance and for logging. The wiring this tree ships by default is
//! [`NoOpHook`].

use crate::ids::Slot;

/// A trigger fired at each applied commit.
///
/// Called synchronously on the apply path, once per applied commit,
/// immediately before the state machine is asked to execute against the
/// lock table, so a hook observes commits in the order the replica
/// committed them and never asynchronously with respect to them.
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
