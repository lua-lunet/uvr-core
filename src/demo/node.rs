//! The single-threaded loop: one node, one thread, no timers.
//!
//! The loop's whole shape is the sentence it is built from: drain the
//! transport, feed the core, take the effects, emit. The core is the
//! sans-I/O replica; the loop is everything the core refuses to be, and
//! nothing more. It owns a transport seam, a boot fence, a state store,
//! a commit hook, a lock table and a resend ledger, and it routes the
//! core's effects to them in release order.
//!
//! Nothing here reads a wall clock. The driver's logical tick, advanced
//! only by the driver, is the suspicion budget's currency; the lease
//! clock is the replicated log's own progress, the slot the operation
//! applies at, the one clock every replica already agrees on. Every wait
//! in a demo run is a bounded, deterministic loop over the transport.
//!
//! The loop's real transport is [`crate::demo::PaxeTransport`]: a
//! [`std::net::UdpSocket`] and paxe's datagram codec, one wait on the
//! socket read, per-link keys that are configuration rather than anything
//! this crate manages. The [`Transport`] seam is what the loop is written
//! against, and the in-memory harness on the other side of it is how the
//! protocol is proved; both are the same seam with something under them.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

use crate::configuration::EraTable;
use crate::demo::fence::{BootFence, FenceWord};
use crate::demo::locks::Service as LockService;
use crate::demo::relay::{Datagram, Ledger};
use crate::demo::spi::CommitHook;
use crate::demo::state::{StateStore, lazy_load};
use crate::effects::{Effect, Stability};
use crate::ids::{Era, NodeId, Operation, OperationId, Slot, Tick};
use crate::journal::{Journal, JournalView, RangeOutcome, SegmentedLog};
use crate::lifecycle::Bumped;
use crate::message::Message;
use crate::progress::Status;
use crate::quorum::WeightedMajority;
use crate::replica::{
    Input, LifecycleRefusal, PersistedProgress, PlanRefusal, PublishOutcome, PublishRefusal,
    Replica, TimedInput, ViewChangeKnobs,
};
use crate::wire::{Pack, Unpack};

/// The demo's transport seam: what the loop drains and what it emits.
///
/// `from` is the transport-attributed sender; authentication is the
/// transport's job, the core consumes the attribution and never derives
/// identity from a payload. A real transport mounts here: the demo's
/// in-memory queues on one side, PAXE over real sockets on the other.
pub trait Transport {
    /// The next datagram addressed to this node, or `None` when the
    /// transport is drained. The loop's pass ends on the first `None`.
    fn receive(&mut self) -> Option<(NodeId, Message)>;

    /// Hand one datagram to the transport. `era` is the era authorising
    /// this copy, the transport-visible routing fact the core carries
    /// separately from the message.
    fn send(&mut self, to: NodeId, era: Era, message: &Message);
}

/// Why a step refused. Refusals are data, never failures: a refusal says
/// which precondition failed, and the driver decides what it means.
#[derive(Debug)]
pub enum DemoRefusal {
    /// The core's planner refused the input.
    Plan(PlanRefusal),
    /// The core's publisher refused the planned transition.
    Publish(PublishRefusal),
    /// The state store refused on the shutdown path or the lazy-load
    /// path.
    State(io::Error),
    /// A committed payload did not decode into a lock request. The demo's
    /// client composes its own payloads, so this is a contract breach
    /// above the boundary, surfaced rather than absorbed.
    Application(String),
    /// An effect the volatile demo never releases: the demo runs
    /// [`Stability::Volatile`], so nothing parks, and it owns no admin
    /// ingress.
    Effect(&'static str),
}

/// The demo's durable record, as a crash takes it: the accepted history,
/// the published progress record, and the configuration history. A crash
/// writes nothing: this record is assembled from the live node's durable
/// halves exactly as they were, no sync, no fence write.
#[derive(Debug)]
pub struct DemoDisk {
    /// The journal's retained window.
    pub journal: SegmentedLog,
    /// The durable half of the progress record.
    pub persisted: PersistedProgress,
    /// The configuration history.
    pub config: Arc<EraTable>,
}

/// What the boot's one lazy state load decided. The verdict the demo
/// records so a test can assert the law's clauses by name.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StateLoad {
    /// No load has been attempted yet this life.
    Unrequested,
    /// A clean boot loaded a snapshot and installed it.
    Loaded,
    /// A clean boot found no state file: the ordinary cold start.
    Absent,
    /// A crashed boot distrusted the file and read nothing.
    Distrusted,
}

/// One applied commit, as the demo records it for its consumer: the
/// slot, the operation's identity, and the lock table's reply bytes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Applied {
    /// The applied slot.
    pub slot: Slot,
    /// The identity the proposing host assigned, carried opaque.
    pub operation_id: OperationId,
    /// The reply bytes the lock table executed to.
    pub response: Vec<u8>,
}

/// What one drained datagram did.
#[derive(Debug)]
pub struct RunOutcome {
    /// The transport-attributed sender.
    pub from: NodeId,
    /// The published revision, or the refusal.
    pub result: Result<u64, DemoRefusal>,
}

/// Why the graceful stop refused. A stop that cannot flush is a failed
/// stop, and a failed stop is surfaced, never skipped.
#[derive(Debug)]
pub struct StopRefusal {
    /// The fence word or the flush that failed, and why.
    pub reason: String,
    /// The underlying error, when the disk produced one.
    pub error: Option<io::Error>,
}

/// Why the boot refused. The fence's verdict chooses the path; every
/// refusal is named.
#[derive(Debug)]
pub enum BootRefusal {
    /// The core refused the construction.
    Lifecycle(LifecycleRefusal),
    /// The fence refused the boot anchor write.
    Fence(io::Error),
    /// A read fence says the previous life stopped cleanly.
    NotCrashed,
    /// The identity space is spent.
    Exhausted(NodeId),
    /// The boot's announcement step refused.
    Step(Box<DemoRefusal>),
}

/// One demo node: the core, the seams, the lock table, the ledger, and
/// the single-threaded loop that ties them.
pub struct DemoNode {
    own: NodeId,
    replica: Replica<SegmentedLog, WeightedMajority>,
    hooks: Box<dyn CommitHook>,
    fence: Box<dyn BootFence>,
    state: Box<dyn StateStore>,
    transport: Box<dyn Transport>,
    ledger: Ledger,
    locks: LockService,
    tick: Tick,
    clean_boot: bool,
    state_touched: bool,
    state_load: StateLoad,
    applied: Vec<Applied>,
    pending: VecDeque<(Slot, OperationId, Box<[u8]>)>,
}

impl DemoNode {
    /// The first life: construct the genesis history, fence the anchor.
    ///
    /// The fence anchor is the boot's one durable act: any later boot
    /// that does not read the clean word reads a crash. The fence
    /// refuses, the node refuses to start.
    ///
    /// # Errors
    ///
    /// [`BootRefusal::Lifecycle`] when the core refuses the genesis
    /// construction; [`BootRefusal::Fence`] when the anchor write
    /// failed.
    pub fn first(
        own: NodeId,
        genesis_order: Vec<NodeId>,
        mut fence: Box<dyn BootFence>,
        state: Box<dyn StateStore>,
        transport: Box<dyn Transport>,
        hooks: Box<dyn CommitHook>,
        knobs: ViewChangeKnobs,
    ) -> Result<Self, BootRefusal> {
        let replica = Replica::provision(
            own,
            genesis_order,
            WeightedMajority,
            SegmentedLog::new(),
            Stability::Volatile,
            knobs,
        )
        .map_err(BootRefusal::Lifecycle)?;
        fence
            .write(FenceWord::Stopping)
            .map_err(BootRefusal::Fence)?;
        Ok(Self::assemble(
            replica, fence, state, transport, hooks, false,
        ))
    }

    /// The reincarnation of a crashed life: the fence's verdict, the
    /// bump, the core's crashed-life constructor, and the announcement
    /// the pair commits the node to.
    ///
    /// The fence is read first and trusted: only the clean word refuses
    /// here (a clean start is downstream's, it needs the industrial
    /// store and the boot gate's vouching), everything else classifies
    /// the previous life as crashed. The replacement identity is one
    /// past the superseded one, and the pair is the commitment: the
    /// announcement is fed as the reborn node's first step, before the
    /// driver sees the node at all.
    ///
    /// # Errors
    ///
    /// [`BootRefusal::NotCrashed`] on a clean word,
    /// [`BootRefusal::Exhausted`] at the bump's bound,
    /// [`BootRefusal::Fence`] when the read failed,
    /// [`BootRefusal::Lifecycle`] when the core refused the crashed-life
    /// construction, [`BootRefusal::Step`] when the announcement
    /// refused.
    pub fn reincarnate(
        old: NodeId,
        disk: DemoDisk,
        fence: Box<dyn BootFence>,
        state: Box<dyn StateStore>,
        transport: Box<dyn Transport>,
        hooks: Box<dyn CommitHook>,
        knobs: ViewChangeKnobs,
    ) -> Result<Self, BootRefusal> {
        match fence.read().map_err(BootRefusal::Fence)? {
            Some(FenceWord::Flushed) => return Err(BootRefusal::NotCrashed),
            Some(FenceWord::Stopping) | Some(FenceWord::Stopped) | None => {}
        }
        let new = old.next_life().ok_or(BootRefusal::Exhausted(old))?;
        let replica = Replica::reincarnate(
            Bumped { old, new },
            new,
            WeightedMajority,
            disk.journal,
            disk.persisted,
            disk.config,
            Stability::Volatile,
            knobs,
        )
        .map_err(BootRefusal::Lifecycle)?;
        let mut node = Self::assemble(replica, fence, state, transport, hooks, false);
        node.announce(old)
            .map_err(|refusal| BootRefusal::Step(Box::new(refusal)))?;
        Ok(node)
    }

    fn assemble(
        replica: Replica<SegmentedLog, WeightedMajority>,
        fence: Box<dyn BootFence>,
        state: Box<dyn StateStore>,
        transport: Box<dyn Transport>,
        hooks: Box<dyn CommitHook>,
        clean_boot: bool,
    ) -> Self {
        Self {
            own: replica.own(),
            replica,
            hooks,
            fence,
            state,
            transport,
            ledger: Ledger::new(),
            locks: LockService::default(),
            tick: Tick(0),
            clean_boot,
            state_touched: false,
            state_load: StateLoad::Unrequested,
            applied: Vec::new(),
            pending: VecDeque::new(),
        }
    }

    /// This node's identity.
    #[must_use]
    pub fn own(&self) -> NodeId {
        self.own
    }

    /// The node's published status.
    #[must_use]
    pub fn status(&self) -> Status {
        self.replica.progress().status()
    }

    /// The node's committed frontier.
    #[must_use]
    pub fn committed(&self) -> Slot {
        self.replica.progress().committed()
    }

    /// The primary of the node's current view under its current
    /// configuration, or none when the view names no member.
    #[must_use]
    pub fn primary(&self) -> Option<NodeId> {
        self.replica
            .progress()
            .config()
            .current()
            .config
            .primary(self.replica.progress().current().view)
    }

    /// The node's current configuration's membership: identity and
    /// voting weight, in order.
    #[must_use]
    pub fn membership(&self) -> Vec<(NodeId, u32)> {
        self.replica
            .progress()
            .config()
            .current()
            .config
            .order()
            .iter()
            .map(|member| (member.node, member.weight.0))
            .collect()
    }

    /// The current configuration's era.
    #[must_use]
    pub fn era(&self) -> Era {
        self.replica.progress().config().current().era
    }

    /// The node's applied commits, in apply order.
    #[must_use]
    pub fn applied(&self) -> &[Applied] {
        &self.applied
    }

    /// What the boot's lazy state load decided.
    #[must_use]
    pub fn state_load(&self) -> StateLoad {
        self.state_load
    }

    /// The node's durable record, as a crash would take it. No sync, no
    /// fence write: the record is assembled from the durable halves
    /// exactly as they stand.
    ///
    /// The record carries the applied frontier as the node claimed it,
    /// and with it the law's sharpest consequence for the least
    /// environment: the demo application has no durability of its own,
    /// the crashed boot distrusts its state file, and the replica stream
    /// re-delivers only what the node had not yet applied. A reborn
    /// life's lock table is therefore empty where its past life had
    /// incorporated state: the survivors hold the locks, the reborn node
    /// is caught up on the protocol, and the industrial host rebuilds
    /// the application state behind the same seam, from its WAL and
    /// checkpoint, where the demo has none.
    #[must_use]
    pub fn record_disk(&self) -> DemoDisk {
        let view = self.replica.journal().view();
        let mut journal = SegmentedLog::new();
        if let Some(frontier) = view.accepted() {
            let (base, _) = view.retained();
            let mut entries = Vec::new();
            match view.copy_out(base, frontier, &mut entries) {
                RangeOutcome::Complete => {}
                other => panic!("the retained window is whole by construction: {other:?}"),
            }
            journal
                .install_suffix(base, &entries)
                .expect("a whole retained window installs on a pristine log");
        }
        DemoDisk {
            journal,
            persisted: PersistedProgress::from(self.replica.progress()),
            config: Arc::clone(self.replica.progress().config()),
        }
    }

    /// Proposes one operation for ordering. The identity is the host's
    /// to assign; the core never deduplicates on it.
    ///
    /// # Errors
    ///
    /// [`DemoRefusal`] as the step refuses.
    pub fn propose(&mut self, operation: Operation) -> Result<u64, DemoRefusal> {
        self.step(Input::Propose { operation })
    }

    /// Feeds the tick at the driver's logical time. The clock is the
    /// host's observation sampled at dispatch (S4), one clock for the
    /// whole run, never a per-node counter a crash could lose: a reborn
    /// life resumes the driver's time. The tick is the suspicion
    /// budget's currency.
    ///
    /// # Errors
    ///
    /// [`DemoRefusal`] as the step refuses.
    pub fn tick(&mut self, at: u64) -> Result<u64, DemoRefusal> {
        self.tick = Tick(self.tick.0.max(at));
        self.step(Input::Tick)
    }

    /// Announces the reincarnation pair to the configuration: the wire
    /// phase the pair commits the node to. The stable leader acts on it;
    /// the members that are not the leader drop it by name.
    ///
    /// # Errors
    ///
    /// [`DemoRefusal`] as the step refuses.
    pub fn announce(&mut self, old: NodeId) -> Result<u64, DemoRefusal> {
        self.step(Input::Reincarnate { old })
    }

    /// Fires the join gossip: the core's own `GossipRequest` frame at the
    /// node's current view, addressed to every member of the node's
    /// current configuration. The joiner half the host owes the wire;
    /// the demo has no timers, so the harness decides when it repeats.
    ///
    /// # Errors
    ///
    /// [`DemoRefusal::Effect`] when a frame does not decode back; the
    /// core packs its own body, so this names a contract breach rather
    /// than a traffic condition.
    pub fn gossip(&mut self) -> Result<(), DemoRefusal> {
        let current = self.replica.progress().current();
        let frame = crate::demo::rejoin::gossip_datagram(current.era.0, current.view.0);
        let message = Message::unpack_from(&frame)
            .map_err(|_| DemoRefusal::Effect("the gossip frame did not decode back"))?;
        let era = self.era();
        for (member, _) in self.membership() {
            if member != self.own {
                self.transport.send(member, era, &message);
            }
        }
        Ok(())
    }

    /// The relay set for a ballot: every un-answered datagram this node
    /// released that the protocol gives a response to, authorised at
    /// exactly `(era, view)`, byte-identical to what the core released.
    /// The harness decides when to replay it.
    #[must_use]
    pub fn relay(&self, era: u32, view: u32) -> Vec<Datagram> {
        self.ledger.relay(era, view)
    }

    /// How many datagrams stand un-answered in the ledger.
    #[must_use]
    pub fn unacknowledged(&self) -> usize {
        self.ledger.unacknowledged()
    }

    /// One pass of the loop: drain the transport, feed the core until it
    /// is dry. The effects each input releases are taken and emitted
    /// inside the pass, so a pass ends with the transport dry and the
    /// node's applies walked.
    ///
    /// The outcomes come back in arrival order, one per delivered
    /// datagram, so a refusal is attributable to the message that caused
    /// it. The pass reads no clock and blocks on nothing: the only thing
    /// that ends it is the transport reporting itself drained.
    pub fn run(&mut self) -> Vec<RunOutcome> {
        let mut outcomes = Vec::new();
        while let Some((from, message)) = self.transport.receive() {
            let header = &message.header;
            self.ledger.answered(
                from.0,
                header.view.era.0,
                header.view.view.0,
                header.slot.0,
                header.tag,
            );
            let result = self.step(Input::Peer { from, message });
            outcomes.push(RunOutcome { from, result });
        }
        outcomes
    }

    /// The graceful stop: the halt begins, the whole lock-table state is
    /// flushed in the foreground, the clean word lands. A stop that
    /// cannot flush is a FAILED stop and is surfaced, never skipped.
    ///
    /// # Errors
    ///
    /// [`StopRefusal`] when the fence or the flush failed; the node does
    /// not report itself cleanly stopped in that case.
    pub fn stop(&mut self) -> Result<(), StopRefusal> {
        if let Err(error) = self.fence.write(FenceWord::Stopped) {
            return Err(StopRefusal {
                reason: "the halt's fence word did not land".to_owned(),
                error: Some(error),
            });
        }
        if let Err(error) = self.state.flush(&self.locks.snapshot()) {
            return Err(StopRefusal {
                reason: "the eager state flush failed".to_owned(),
                error: Some(error),
            });
        }
        if let Err(error) = self.fence.write(FenceWord::Flushed) {
            return Err(StopRefusal {
                reason: "the clean word did not land".to_owned(),
                error: Some(error),
            });
        }
        Ok(())
    }

    /// One step: plan, publish, take the effects, walk the applies the
    /// transition released. Each apply's completion is fed back through
    /// the same machinery, so a commit announcement's cascade settles
    /// inside one step.
    ///
    /// # Errors
    ///
    /// [`DemoRefusal`] as the machinery refuses; the first refusal ends
    /// the step and is surfaced, never absorbed.
    pub fn step(&mut self, input: Input) -> Result<u64, DemoRefusal> {
        let revision = self.drive(input)?;
        while let Some((slot, operation_id, payload)) = self.pending.pop_front() {
            self.walk_apply(slot, operation_id, payload)?;
        }
        Ok(revision)
    }

    /// One plan/publish pass and the routing of the released effects.
    /// Apply effects are parked for the step's walk; everything else is
    /// taken here.
    fn drive(&mut self, input: Input) -> Result<u64, DemoRefusal> {
        let accepted_before = self.replica.progress().accepted();
        let timed = TimedInput {
            at: self.tick,
            event: input,
        };
        let planned = self
            .replica
            .plan(&timed, &self.replica.journal().view())
            .map_err(DemoRefusal::Plan)?;
        let (revision, effects) = match self
            .replica
            .publish(planned)
            .map_err(DemoRefusal::Publish)?
        {
            PublishOutcome::Published { revision, effects } => {
                // Opportunistic reclamation: an append is the lazy
                // moment, and the published checkpoint is the sole
                // authorisation; with none, this is a no-op.
                if self.replica.progress().accepted() > accepted_before {
                    self.replica.reclaim_journal();
                }
                (revision, effects)
            }
            PublishOutcome::Parked { .. } => {
                return Err(DemoRefusal::Effect(
                    "a transition parked under the volatile demo's stability",
                ));
            }
        };
        for effect in effects {
            match effect {
                Effect::Send { to, era, message } => {
                    let mut bytes = vec![0u8; message.packed_len()];
                    message
                        .pack_into(&mut bytes)
                        .expect("the core packs its own body");
                    let header = &message.header;
                    self.ledger.record(Datagram {
                        to: to.0,
                        era: header.view.era.0,
                        view: header.view.view.0,
                        slot: header.slot.0,
                        tag: header.tag,
                        bytes,
                    });
                    self.transport.send(to, era, &message);
                }
                Effect::Apply {
                    slot,
                    operation_id,
                    payload,
                } => self.pending.push_back((slot, operation_id, payload)),
                Effect::Persist(_) => {
                    return Err(DemoRefusal::Effect(
                        "a persistence intent released under the volatile demo's stability",
                    ));
                }
                Effect::AdminResponse { .. } => {
                    return Err(DemoRefusal::Effect(
                        "an admin verdict released with no admin ingress",
                    ));
                }
            }
        }
        Ok(revision)
    }

    /// One apply: the state materialises first (the lazy load's one
    /// shot), the hook fires, the lock table executes, the completion is
    /// fed back.
    fn walk_apply(
        &mut self,
        slot: Slot,
        operation_id: OperationId,
        payload: Box<[u8]>,
    ) -> Result<(), DemoRefusal> {
        self.materialise_state()?;
        self.hooks.on_commit(slot);
        // The replicated clock: the slot the operation applies at. The
        // stamps are a function of the replicated log, never of a wall
        // clock, so every replica's reply bytes at a slot agree.
        let response = self
            .locks
            .execute(slot.0, &payload)
            .map_err(|error| DemoRefusal::Application(error.to_string()))?;
        self.applied.push(Applied {
            slot,
            operation_id,
            response,
        });
        self.drive(Input::Applied { slot })?;
        Ok(())
    }

    /// The lazy load's one shot, behind the fence verdict. The table
    /// starts empty; state materialises only on the first apply, and a
    /// crashed boot distrusts the file no matter what sits in it. The
    /// demo's only boots are first lives and crashed rebirths, so the
    /// load never runs here: the clean arm is the seam's contract, and
    /// the industrial host's clean start is the boot that flips it.
    fn materialise_state(&mut self) -> Result<(), DemoRefusal> {
        if self.state_touched {
            return Ok(());
        }
        self.state_touched = true;
        let loaded = lazy_load(self.state.as_ref(), self.clean_boot, self.locks.is_empty())
            .map_err(DemoRefusal::State)?;
        self.state_load = if !self.clean_boot {
            StateLoad::Distrusted
        } else if let Some(snapshot) = loaded {
            self.locks.restore(snapshot);
            StateLoad::Loaded
        } else {
            StateLoad::Absent
        };
        Ok(())
    }
}
