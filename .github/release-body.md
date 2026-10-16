# The ephemeral-lease core moves into the core

The pivot's whole arc, landed: the advisory locks machinery is core
surface now, not a demo.

## The feature set since the pivot

- **The CAS lock table is core surface** (`locks` feature): hold,
  renew, release, break, over one client table, with the generation
  watermark that never hands a generation out twice on a lock, the
  truthful release (one that retires nothing says so), the
  nil-unrepresentable `ClientId`, and the clock-parametric `execute`.
  The expiry window is the ephemeral lease of the fast-reads design
  ([VR-2012] §6.3/§6.3.1,
  docs/references.md): the primary
  answers reads unilaterally only while it holds valid leases from f
  other replicas, and a new view starts only after the leases at f + 1
  participants expire. The lease arithmetic the cluster owner owns —
  the leader lease outlasting the reader lease plus the clock drift
  margin, the `StartViewChange`/`DoViewChange` quorum rounds as a
  safety margin beyond the expiry, no renewal while the cluster is
  idle, and the application's own judgement of whether a rare stale
  read during a leader crash is noise or an outage — is documented on
  the machinery itself (`src/locks.rs`).
- **The SPI the machinery is written against is core surface**:
  `StateStore` (flush eager, load lazy, a crashed boot distrusts the
  state file), `BootFence` with its `FenceWord` (one file, one status
  word, one forced write per transition), `CommitHook` (a trigger at
  each applied commit), and `lazy_load` with its guard stated once. A
  consumer implements the store and the fence over its own storage
  engine — a heavyweight superblock and WAL, a double-write, a custom
  flush — mounts its own hook and its own timeouts, and changes
  nothing above the traits.
- **The demo is scaffold, and only scaffold** (`demo` feature, renamed
  from `demo-locks`): the plain-file store and fence over the demo's
  own disk seam (the one place `std::fs` is named), the
  single-threaded node loop, the resend ledger, the join gossip, and
  the PAXE-sealed UDP transport, pinned by revision.
- **The sans-I/O law is kept**: zero default dependencies; `locks`
  rides the existing optional `serde` lane; `demo` adds the optional
  pinned paxe-core. No timers, no WAL, no snapshots, no superblock:
  heavy IO is the host's to mount behind the traits.
- **The release flow verifies the version**: a tag publishes only when
  the manifest the tag points at declares the tag's version; a
  forgotten bump blocks the release instead of shipping a lie. The
  release note itself is a reviewed file in the repository
  (`.github/release-body.md`), carried by the tag that earns it.
