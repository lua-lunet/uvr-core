# Unbounded Viewstamped Replication Revisited (UVR)

A **sans-io** [Viewstamped Replication Revisited](https://dspace.mit.edu/server/api/core/bitstreams/9f8c52b3-ea46-4fde-9dc9-354ed6d9c7d9/content)
core library in Rust, with unbounded cluster reconfigurations, that replaces
Crash-Recover behaviour with a low-latency Crash-Stop-Reincarnation. It has a C ABI
for LuaJIT FFI and a demo Maelstrom node for checking it.

This crate offers strong consistency during non-stop cluster reconfigurations
without disk flushes. This is achieved by porting to Viewstamped Replication
Revisited the leader casting vote technique from David Turner's technical
report on unbounded pipelining in dynamically reconfigurable clusters
(Tracsis, 2016–17, tessanddave.com).

This Rust crate exposes a C ABI for FFI. It scales down to offer a lightweight
and embeddable strong consistency model. With a small amount of data, such as
leader leases or advisory locks, it removes the need to run something like
ZooKeeper or etcd.

Sans-io means the whole protocol is a state machine: you hand a replica an input
and drain the outputs it produced. No sockets, no threads, no async runtime, no
callbacks. The host owns transport, timers, and durability.

```rust
use uvrr::effects::Stability;
use uvrr::ids::{NodeId, Operation, OperationId, Tick};
use uvrr::journal::{Journal, SegmentedLog};
use uvrr::quorum::WeightedMajority;
use uvrr::replica::{Input, Replica, TimedInput, ViewChangeKnobs};

let mut replica = Replica::provision(
    NodeId(0),
    vec![NodeId(0), NodeId(1), NodeId(2)],
    WeightedMajority,
    SegmentedLog::new(),
    Stability::Volatile,
    ViewChangeKnobs { primary_timeout: 0, view_change_budget: usize::MAX },
)
.expect("provision");

// The genesis primary promotes itself on the first tick.
let view = replica.journal().view();
let planned = replica
    .plan(&TimedInput { at: Tick(1), event: Input::Tick }, &view)
    .expect("plan tick");
replica.publish(planned).expect("publish tick");

// Propose a host operation for ordering; the core carries it opaque.
let proposal = TimedInput {
    at: Tick(2),
    event: Input::Propose {
        operation: Operation {
            id: OperationId { msb: 0, lsb: 1 },
            payload: b"hello".to_vec().into_boxed_slice(),
        },
    },
};
let view = replica.journal().view();
let planned = replica.plan(&proposal, &view).expect("plan propose");
let _outcome = replica.publish(planned).expect("publish propose"); // PublishOutcome::Published { revision, effects }
```

To build and test without running the crash testing and network partitioning simply:

```text
cargo test
```

The demo binary in this repo implements a trivial key-value store protocol so that the Maelstrom test harness may simulate network partitions and crashes. The Makefile runs the harness as a git submodule locally or in Docker.

## Why this exists

The concept of an external strong consistency service is one that has very sharp edges. It splits responsibility for quality, legitimacy, and performance across two product teams. Every client connected to the core is part of the full distributed system and must experience consistency. When there are silos of responsibility on the critical path then often no-ones hold themselves accountable the removal of every single source of outages. This repo aims to embed strong consistency with predictable leader rotation, viewstamped replication, and other things that will soon seem obviously desirable features in the second quarter of the twenty-first century. 

In the second quarter of the twenty-first century, clusters that run across three data centres, that have a uniform configuration, where there is a single stable leader, that automatically fails over in a round-robin manner, with a client api that is HTTP without transactions, where clients connect via a cloud-based sticky load balancer, is the a very compelling architecture: 

- Stable storage capacity is near infinite at extremely low costs. Business applications can simple first post to a storage solution to get back an URL and use the URL as a transparent handle to the data. Your client API simply needs to hide the fact that the get/put of the bytes goes to a storage solution, whereas all the real logic goes to your strongly consistent cluster that works with the URLs. 
- Stable storage is a solved problem on-prem or on a VPS or on your laptop with open-source MinIO and others. There are manny S3-like endpoint on-prem, or in private data centre, or on EU sovereign cloud providers, else via on a global public cloud provider. Your author worked at a global enterprise that used a major disk array vendor for on-prem NAS and SAN. That enterprise disk manufacturer's enterprise storage solution has an S3 API endpoint that was the vendor's own webapp writing to the vendor's disk arrays on-prem. In the EU, cloud providers like Scaleway in France run the commercial version of the open-source min.io system that is written in Go. The S3 protocol, which does not support transactions, has been the de facto standard for file storage for many years now. 
- All good sovereign or regional cloud providers have three independent client resilience zones per region. Using a three-node configuration is normal.
- While global business-to-consumer (B2C) dominates what is fashionable for resume-driven engineering, in reality, government-to-consumer for regional governments and most countries worldwide can easily fit legitimate books-and-records data in a cost-effective three-node cluster. [How Netflix Uses Java – 2025](https://www.youtube.com/watch?v=XpunFFS-n8I) Edition by Paul Bakker of the Netflix Java Platform team, recorded at JavaOne 2025, and his 2023 paper [How Netflix Really Uses Java](https://www.infoq.com/presentations/netflix-java/) indicate that even a hyperscale company may have hundreds or thousands of internal applications. 
- Servers with hundreds of gigs of RAM and async write-behind to have hundreds of gigs of storage are possible for mid-sized businesses. Yet in the first quarter of the twenty-first century, on an internal enterprise cloud training course, students were tasked with spinning up a normal VM using cloud tools. Your author noticed there was no enforced policy on the type of VM that could be launched on the public cloud subscription paid for by his employer, so he launched a server with 128 cores, 1G of disk and 265G of RAM, printed off the details of CPU/RAM/Disk, then deleted it. When you can first offload data to S3 to have fixed-size data records that are URLs so that you can then have a three-node cluster that can scale to far beyond the wildest ambitions of most systems. You can start with three very small VMs and scale up to a beyond your wildest dreams in terms of the number of records as long as you can easily reconfigure the live server to upgrade the VMs dynamically when the service is online. 

Those previous statements are in effect a manifesto for dynamically reconfigurable Viewstamped Replication Revisited. 

If you are curious to see if embedding strong consistency directly into your application reduces the complexity, costs and latencies of your system, then try this crate.

## Maelstrom Tire-Kicking

The demo kv replication passing Maelstrom testing is not evidence of zero bugs. Yet it is a demonstration of an absence of shallow bugs and that the library has some resilience to network partitions, crashes, or combinations of both. If you build a system on top of this library, the bugs may be any  combination of all the code. You should consider writing custom Maelstrom logic to validate your entire system. 

`make e2e` does a docker build to run the end-to-end Maelstrom test suite.

`make tla` builds a self-contained TLC image and exhaustively checks the
finite [TLA+ correspondence model](formal/README.md) for normal operation,
view change, and the fenced crash-stop event. The image embeds the model: this path
uses classic Docker commands and requires neither BuildKit nor a volume mount.

`cargo test` runs the protocol-path tests, targeted
regressions, and a deterministic seeded multi-replica cluster harness (K=3..7,
loss / reorder / duplication / partition / crash-restart, safety asserted
after *every* single step), plus proptest companions. The default feature set
never boots the Maelstrom harness: the library, not the harness, is the
deliverable. Run the full lane with the harness before any push:

```shell
cargo test --features maelstrom
```

### The locks demo lane

The `locks` feature carries the advisory-locks machinery at the core surface, and the `demo` feature carries the scaffold and its real
socket transport, which seals every datagram with the
[PAXE](https://github.com/lua-lunet/paxe-core) codec. The codec links libsodium statically and requires a build of it with
the hardware AES-256-GCM path; without one it refuses to run, by design,
naming the cause rather than falling back to a weaker cypher. That makes
the build machine's sodium part of this feature's requirements:

- **macOS**: `brew install libsodium` (1.0.22 carries the path).
- **Linux**: `SODIUM_PREFIX=/tmp/libsodium bash scripts/build_libsodium.sh`,
  then `export PAXE_SODIUM_LIB_DIR=/tmp/libsodium/lib`. The distro package is
  not enough on arm64: Ubuntu 24.04 and Debian Trixie ship 1.0.18, whose
  archive has no ARM Crypto Extensions path. The script pins the tarball by
  digest and builds it static-only.

The CI lanes do exactly this, which is why the sealed-transport proofs run
on every runner rather than only where the cypher happens to exist.

### The compliance corpus

The protocol's obligations are a corpus of test cases stated as data
(`docs/uvr-host-compliance.md`), replayed in process by the reference
runner. The committed corpus is generated from the reference behaviour:
`make corpus-export` regenerates it, and the sync gate fails when the
committed bytes drift from what the reference produces.

### The pre-push hook

`make hooks` installs the repository's pre-push gate (it points git at the
committed hooks with `git config core.hooksPath .githooks`). Before anything
reaches the remote, the hook runs the full verification gate, including the
slow and expensive maelstrom lane. It is skippable with
`git push --no-verify`; please do not skip it.

In order to run the maelstrom targets you need to fetch maelstrom as a submodule with 

```shell
git submodule init
git submodule update
```

`maelstrom-lin-kv` runs the core as a [Maelstrom](https://github.com/jepsen-io/maelstrom)
node so Jepsen's Knossos checker verifies **linearizability**, strictly
stronger than [sequential consistency](https://jepsen.io/consistency/models/sequential),
under partition, process kill, and process pause. The K=5 configuration runs
under all three nemeses with `:valid? true` and no failures on recent runs.

```bash
mise install          # JDK 25 + Leiningen 2.11.2
brew install gnuplot  # only for Jepsen's plots; checkers work without it
make test             # the Rust suite
make test-all         # lin-kv under partition + kill + pause
make serve            # browse results at http://localhost:8080
```

`make help` lists the tunables (`NODES`, `TIME_LIMIT`, `RATE`, `INTERVAL`).

### Docker

For environments where installing Java, Leiningen, and Rust is difficult, use
the Docker image:

```bash
make e2e
```

Or manually:

```bash
make docker-build
docker run --rm -e NODES=5 -e TIME_LIMIT=180 -e RATE=20 -e INTERVAL=10 uvr-core-maelstrom test-all
```

See `Dockerfile.maelstrom` for the build definition.


N.B. **Cluster size and the fault budget.** Jepsen's default kill targets include
`:majority` and `:all`. At `NODES=3` (f=1) that routinely kills two of three
nodes, and a crash-fault protocol tolerating one failure then cannot make
progress *by construction*, you get `ok-count 0`, `:valid? false`, and a
vacuously true `:linearizable`. Safety still held; liveness was impossible. The
default here is `NODES=5` (f=2) for that reason. Read `ok-count` before reading
the verdict.

## Status

The core is open to extension and closed to modification that would invalidate
the invariants of the algorithm. 

The naming of everything is subject to change until the final paper is published. Naming is hard, so expect lots and lots of renaming. I probably will publish a v1.0.x simply at some point to show that the solution is seen as complete to some definition of complete where I can find some external definitions of "complete" to align to. 

## Attribution

This project uses the following open-source tools for testing and validation:

- **[Maelstrom](https://github.com/jepsen-io/maelstrom)**, a workbench for learning
  distributed systems by writing your own, created by [Kyle Kingsbury](https://jepsen.io)
  and the [Jepsen](https://jepsen.io) team. Licensed under the [Eclipse Public
  License 1.0](https://www.eclipse.org/legal/epl-v10.html).

- **[Jepsen](https://github.com/jepsen-io/jepsen)**, a framework for testing
  distributed systems, also by Kyle Kingsbury. Licensed under the [Eclipse Public
  License 1.0](https://www.eclipse.org/legal/epl-v10.html).

- **[Knossos](https://github.com/jepsen-io/knossos)**, Jepsen's linearizability
  checker. Licensed under the [Eclipse Public
  License 1.0](https://www.eclipse.org/legal/epl-v10.html).

The `maelstrom-lin-kv` binary implements a [Fly.io Distributed Systems
Challenge](https://fly.io/dist-sys/) style linearizable key-value store using this
core, allowing it to be tested against Maelstrom's `lin-kv` workload and
Knossos checker under network partitions, process kills, and pauses.

## Licence

MIT.
