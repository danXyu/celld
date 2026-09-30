# Performance testing: a plan for measuring celld end to end

Status: revision 2, 2026-09-30. Tiers 0 to 3 are implemented; see
[performance-tests.md](../performance-tests.md) for how to run them and
[What was built](#what-was-built) for where the implementation departs
from this plan. Tier 4 is not.

celld has two Criterion targets today, and both measure change export
([export-benchmarks.md](../export-benchmarks.md)). They time JSON and
Parquet encoding, SQLite capture, and the delivery and audit caches. No
benchmark covers the request path, the output gate, replication, restore,
ownership, WebSockets, the platform services, or deploys. The numbers in
[testing.md](../testing.md#a-few-numbers-we-trust) come from a fleet lab
whose tooling is not in this repository, and nothing reproduces them from a
checkout. The [celld-tck](https://github.com/ewhauser/celld-tck) suite runs
a real multi-node fleet with faults, but it checks behavior, not speed.

This plan adds performance tests in four tiers. The tiers go from pure
functions to cloud fleets, and each tier answers the questions the one below
it cannot answer. It also adds the instrumentation those tiers need, and a
cadence for running each tier in CI.

## Contents

- [Goals and non-goals](#goals-and-non-goals)
- [Principles](#principles)
- [Where the time goes](#where-the-time-goes)
- [Tier 0: instrumentation](#tier-0-instrumentation)
- [Tier 1: component benchmarks](#tier-1-component-benchmarks)
- [Tier 2: single-node scenarios](#tier-2-single-node-scenarios)
- [Tier 3: fleet scenarios](#tier-3-fleet-scenarios)
- [Tier 4: cloud qualification](#tier-4-cloud-qualification)
- [Workloads](#workloads)
- [The harness](#the-harness)
- [Cadence and gates](#cadence-and-gates)
- [Bottleneck hypotheses](#bottleneck-hypotheses)
- [Milestones](#milestones)
- [Open questions](#open-questions)
- [What was built](#what-was-built)

## Goals and non-goals

Goals:

- Each claim in "A few numbers we trust" can be reproduced by one command
  from this repository or celld-tck. The run records its conditions.
- A pull request that adds a bucket operation to a warm request, an extra
  core round trip, or an extra fsync to an append fails CI. The test counts
  the operations and does not time them.
- Nightly runs detect timing regressions on the request, durability,
  activation, and restore paths. They run on hardware where the noise is
  lower than the regression we want to catch.
- For each of these (a node, a cell, a namespace, a queue, a fleet), we know
  the throughput ceiling, the resource that sets it, and the knob that moves
  it.
- The cost of a workload can be read in bucket operations per request, per
  resident cell-hour, and per idle node-second. The
  [DynamoDB control plane](dynamodb-control-plane.md) design estimates these
  counts today; with these tests we can measure them.

Non-goals:

- Comparisons against Cloudflare. celld-tck checks compatibility; speed on a
  different network is not comparable.
- Snowflake warehouse performance, which belongs to the loader.
- Replacing the fleet lab's fault and correctness role. Performance runs
  inject faults only to measure the time to recover, not to prove safety.
  Every load run still ends with a verification sweep.

## Principles

**Count before timing.** Many regressions change a count before they change
a latency: bucket GETs per request, PUTs per acknowledged write, LISTs per
restore, fsyncs per follower batch, isolate compiles per activation, core
messages per request. These counts do not depend on the machine, so CI can
fail on them strictly. Timings only get trends and thresholds, and only on
dedicated hardware.

**Open-loop load.** The load generator sends at a fixed arrival rate and
measures each request from its scheduled start. A closed loop slows down
when the server slows down and hides tail latency (coordinated omission).
Latency is recorded in HDR histograms and reported at p50, p90, p99, p99.9,
and max, with the achieved rate beside the offered rate.

**Phases, not only totals.** A request's latency is split into admission,
route, dispatch, V8 turn, output-gate wait, and response. Most of these
timings already exist as `timing` tracing events. Tier 0 turns them into
metrics so that a harness does not need to parse debug logs.

**No speed without correctness.** Each macro run ends with a verification
sweep, like the one in the fleet lab: each cell's durable state is read
through a different node and compared. A run that loses a write fails,
however fast it was.

**Recorded conditions.** Each result records the commit, the build profile,
every `CELLD_*` variable, the host shape, the kernel, the bucket backend and
its injected latency, the node count, and a `/state` snapshot at the start
and end.

**Same profile everywhere.** Macro runs use the `lab` profile (thin LTO,
line tables for `perf`). Criterion uses the `bench` profile, as it does
today. Neither is the shipping `release` build. Tier 4 runs the release
image once for each release, to confirm that the lab profile predicts it.

## Where the time goes

This summary of the hot paths shows where the tests should look. File
references are starting points, not an exhaustive map.

| Path | Shape | Dominant cost |
| --- | --- | --- |
| Stateless fetch | `handle_ingress` ([main.rs](../../crates/celld/main.rs)) → `Pool::admit_or_wait` ([pool.rs](../../crates/celld/pool.rs)) → a V8 turn | V8 CPU; isolates capped at the core count |
| DO request, warm | a core `Request` round trip, then a turn on an isolate shared by up to 32 cells ([runtime.rs](../../crates/celld/runtime.rs)), then synchronous SQLite ([storage.rs](../../crates/celld/storage.rs)), then a core `Output` round trip ([actor.rs](../../crates/celld/actor.rs) `gate_output`) | the serial `celld-core` thread, the isolate turn lock, the gate wait |
| DO request, forwarded | a peer tunnel with h1 keep-alive, one request per connection at a time ([peer_tunnel.rs](../../crates/celld/main/peer_tunnel.rs)) | a network round trip, the tunnel pool |
| Write proof, fleet | `ship_loop` group commit → follower `append_batch` fsync ([node_log.rs](../../crates/celld/node_log.rs)); bundle PUT every 2 s | capture CPU, follower RTT and fsync |
| Write proof, bucket | `sync_loop`, one sync in flight per cell, 64 upload slots per node ([ltx_repl.rs](../../crates/celld/ltx_repl.rs)); then an ownership GET | a PUT and a GET round trip, and the slot queue |
| Cold activation | ReadOwner → ReadNodeLease → (RecoverNodeLog) → CasOwner → Restore → StartRuntime | bucket round trips, LISTs across 10 levels per epoch, GET bytes, isolate compile |
| Restore, large | paged VFS at 256 MB or more, hydration at 16 MB/s ([paged.rs](../../crates/ltx/src/paged.rs)) | the object store's throughput |
| Takeover | lease expiry (10 s TTL) → `recover_as` → restore | the TTL, recovery uploads |
| WebSocket message | a core route, a `ws_message` event, and an output barrier; the pumps are polled on the main `block_on` thread | the main thread and the core thread |
| Alarms | wake entries in the bucket, a due scan every 60 s by an elected node ([wake.rs](../../crates/celld/wake.rs)) | bucket LIST/PUT, the upload slots in a storm |
| Services | KV, D1, Queues, Workflows, and Cron are each a reserved cell with one writer; R2 goes straight to the bucket | the single writer per namespace, queue, or database |

## Tier 0: instrumentation

Most later tiers depend on this tier. Today:

- [metrics.rs](../../crates/celld/metrics.rs) exports gauges plus two
  histograms (cell CPU time and heap) over OTLP, every 60 s by default.
  It has no Prometheus endpoint.
- [bucket.rs](../../crates/celld/bucket.rs) has no counters. Nothing can
  check the claim that a warm request makes zero bucket operations.
- Latency by phase exists only as `RUST_LOG=timing=debug` events:
  `durable_wait`, `gate_write_timing`, `cell_route_timing`,
  `worker_fetch_timing`, `cell_handoff_timing`, `log_ship_round`
  (every `SyncTiming` field), `restore_plan`, and `log_append_serve`.
- The fault seams (`Bucket::with_stores`, `LtxRepl::start_with_store`,
  `LogTransport`, `LtxHost::with_filesystem`) are compiled only under
  `cfg(test)` or `celld_internal_tests`. The simulation sources that use
  them are not in this repository.

Add:

1. **Bucket operation counters and histograms.** One layer in `Bucket`,
   labeled by operation (GET, range GET, PUT, conditional PUT, LIST, HEAD,
   DELETE, multipart), key class (`cells/*/own`, `nodes/`, `fleet/`,
   `log/*/bundle`, `cells/*/ltx`, `wake/`, `deploy/`, `r2/`, `export/`,
   `telemetry/`), and outcome (ok, precondition failed, not found, 429,
   other). The LTX replica client and the R2 store report through it too.
2. **Latency histograms for each phase.** Request duration by route
   (stateless, local cell, forwarded) and by whether the request wrote.
   Admission wait. Gate wait by proof source (fleet, bucket). Activation
   duration by restore path (fresh, local rename, download, paged). Restore
   duration by phase (`RemoteRestoreTiming`). Capture duration by phase
   (`SyncTiming`). Follower append write and fsync. Isolate compile time.
   Handoff duration by phase.
3. **Counters for the fleet proof.** Acknowledgements by proof source, cells
   per ship round, degrades, evictions, and ensemble swaps.
4. **Loop lag probes.** A timer on the `celld-core` current-thread runtime
   and on the main `block_on` loop, which measures how late it fires, plus
   the depth of the core mailbox. These two threads are serial, and each
   request and WebSocket message passes through them.
5. **A snapshot endpoint.** `GET /debug/metrics` on the internal listener,
   next to `/state`, returns every instrument above as JSON (or Prometheus
   text) at the moment of the call. The harness reads it before and after a
   phase and takes the difference. The OTLP interval stays unchanged.
6. **A `perf` feature.** It follows the `export-bench` pattern: ordinary
   builds exclude it. It exposes the fault seams to benches and to the
   harness: an `ObjectStore` wrapper that adds latency, jitter, 429s, and
   errors by key class (extending `FlakyStore` in
   [export_sink/tests.rs](../../crates/celld/export_sink/tests.rs)), a
   `FileSystem` that delays fsync, and a `LogTransport` that delays or
   drops frames.

The counters in items 1 and 3 must cost little enough to stay in release
builds, because operators need them too. Item 6 never ships.

## Tier 1: component benchmarks

These are Criterion targets beside the existing ones, run with
`cargo bench`, and smoke-tested in CI with `-- --test`. They are
deterministic, need no network, and name one function each. Fixtures come
from the `perf` feature, as `export-bench` provides fixtures today.

| Target | Cases | Why |
| --- | --- | --- |
| `ltx_capture` (crate `ltx`) | `Db::sync` for 1, 16, and 256 changed pages, and with the WAL at 1× and 10× the checkpoint threshold; report the `SyncTiming` phases | Capture CPU is on every write proof |
| `ltx_codec` | LZ4 block encode and decode, CRC64, LTX encode and decode by page count | These are the inner loops of capture and restore |
| `ltx_compact` | `merge_l0_rows` and `compact_cell` for 16, 256, and 1,024 L0 files | Compaction must keep up, or restores slow down |
| `ltx_restore` | apply a snapshot plus N L0s from a local store; build the paged page map for 256 MB and 2 GB | The CPU part of restore, apart from the bucket |
| `core_events` (crate `logic`) | `on_event` throughput for Request, Output, and Route messages with 10, 10k, and 1M known cells; rebalance planning with 10 nodes × 100k cells; pressure victim selection | The core thread is serial, so its cost for each event limits the throughput of a node |
| `storage_ops` | DO KV put and get, `sql_exec`, cursor reads of 1k rows, hits and misses in the prepared-statement cache, a transaction commit in WAL mode | The synchronous SQLite work inside every turn |
| `node_log` | `log_append_encode`, follower `append_batch` on tmpfs and on disk (write, file fsync, directory fsync), bundle encode | The fleet proof's fixed cost |
| `js_turn` | isolate creation; bundle compile for `hello` versus a large bundle (`examples/opencode`); an empty turn; structured clone of 1 KB and 1 MB values; the `export_kv` decode | Cold starts depend on compile time; every request pays the turn overhead |
| `wire` | signing and verification of peer auth, tunnel framing, WebSocket frame encoding | These are per request, and per message |
| `services_pure` | queue producer grouping, the next cron occurrence, KV `list` pagination, `_headers` and `_redirects` matching | Pure work inside service cells |

A Tier 1 result shows that a function got slower. It does not show that a
user sees it. Tiers 2 and 3 answer that.

## Tier 2: single-node scenarios

A harness (see [the harness](#the-harness)) starts `celld` as a subprocess,
as [crates/celld/tests](../../crates/celld/tests) does, and deploys a fixture
Worker. It drives the node with open-loop load and reads the Tier 0
snapshots. The node uses one of three bucket backends:

- **LocalStore**, the SQLite dev store. Fast and deterministic. Use it for
  counting and for the CPU ceiling.
- **MinIO**. A real S3 protocol on the local network.
- **MinIO behind a latency proxy** (the `perf` store wrapper or toxiproxy),
  set to an S3-like profile: a PUT with p50 30 ms and p99 150 ms, a GET with
  p50 15 ms, and optional 429s. Durability numbers are useful only with this
  backend.

A single node has no followers, so every write in this tier waits for a
bucket proof. Fleet proofs are in Tier 3.

| # | Scenario | Workload and sweep | What it measures |
| --- | --- | --- | --- |
| S1 | Stateless ceiling | `hello` at increasing rates; sweep `CELLD_MAX_STATELESS_ISOLATES` and `CELLD_TOKIO_THREADS` | Peak requests per second, CPU per request, latency at 50/80/95% of peak |
| S2 | Warm read | `counter` reads across 1, 100, and 10k resident cells | p50 and p99 (target: ~1.1 ms and ~7 ms, from testing.md); **zero bucket operations per request** as a hard count |
| S3 | Warm write, bucket proof | writes of 100 B, 4 KB, and 64 KB; injected PUT latency of 0, 30, and 100 ms | Latency close to PUT + GET; one PUT and one GET per proof |
| S4 | Write coalescing | 1, 8, and 64 concurrent writers on one cell | PUTs per acknowledged write fall as concurrency rises (the shared upload claim) |
| S5 | Upload slots | writes spread over 100, 1k, and 4k cells at the same moment | Time until all are acknowledged, with 64 slots; checks the 4k-cell alarm storm (~9 s) noted in [durability.rs](../../crates/logic/durability.rs) |
| S6 | Hot cell | one cell at its peak rate; then that cell with 31 busy neighbours in its isolate | The peak for one cell; the cost of sharing the isolate; rejections at `CELLD_MAX_CELL_REQUESTS` |
| S7 | Core saturation | small DO calls to 10k cells at increasing rates | The rate at which core mailbox lag grows; which comes first, the core thread or the V8 isolates |
| S8 | Cold activation | a working set of 10× `CELLD_MAX_RESIDENT_CELLS`, uniform and Zipfian; database sizes of 4 KB, 1 MB, 64 MB, and 300 MB (paged); L0 chains of 1, 64, and 256 | Activations per second, latency by restore path, bucket operations per activation, wait time for the pool maintenance lock during `grow()` |
| S9 | Memory | fill with idle cells, then with hibernated cells, then with isolates, up to `CELLD_MAX_RSS_MB` | RSS for each; pressure shedding rate; latency while shedding |
| S10 | WebSockets | 1k, 10k, and 50k idle sockets; echo at increasing rates; a chat room fanning out to 1k members; the auto-response path; a message to a hibernated cell | Memory per socket, messages per second before the main thread saturates, wake latency |
| S11 | Alarms | 10k alarms due at one instant; alarms at a steady rate | Firing delay against the scheduled time; bucket operations per alarm |
| S12 | Services | see [services](#services-in-tier-2) | Per service |
| S13 | Deploy | a new generation under S2 load; bundles of 100 KB, 5 MB, and 20 MB; 10k resident cells at `CELLD_DEPLOY_MAX_AGE_S` | Build time, the error and latency spike during cutover, memory while two generations coexist, the reconnect storm after close code 1012 |
| S14 | Overhead switches | S2 and S3 with change export on and off; `CELLD_OTEL` unset, `1`, and pointing at a slow OTLP collector | Added p99 and CPU for each feature; telemetry drops |
| S15 | Soak | a mixed workload for 24 h at 50% of peak | Growth of RSS, file descriptors, the local cache, the L0 count, and bucket objects; p99 drift |

### Services in Tier 2

| Service | Shape | Measure |
| --- | --- | --- |
| KV | one namespace is one cell (`SHARDS = 1`); values over 1 MiB go to the bucket | reads per second on one namespace (the ceiling); put and get at 1 MiB, just over 1 MiB, and 25 MiB; `list` pagination |
| Queues | one queue is one cell; producer calls grouped for 4 ms; at most 256 producer calls in flight ([queue_batching.rs](../../crates/celld/queue_batching.rs)) | `send()` latency and rejections against producer concurrency; delay from send to handler against batch settings; tail latency with many busy queues; retry and dead-letter cost |
| D1 | one database is one cell | `run()` × N against `batch(N)`; writes per second on one database; cost of large result sets |
| R2 | direct to the bucket; multipart over 8 MiB; `list` makes 16 HEADs in parallel ([r2_store.rs](../../crates/celld/r2_store.rs)) | throughput by object size; range reads; memory with parts out of order; `list` on large prefixes |
| Workflows | `run()` replays from the top on each wake | step latency at 10, 100, and 1,000 steps (replay grows with the step count); instances created per second; wake after hibernation |
| Cron | one cell per script; handlers run serially | firing delay; many expressions; many scripts |
| Static assets | index loaded at boot; bodies cached on disk, LRU of 512 MiB ([assets.rs](../../crates/celld/assets.rs)) | cold and warm latency; a working set larger than the cache; 404 storms against the 5 s pointer re-read |
| Dynamic Workers | one isolate per loaded Worker; at most 256 per process | `load()` against memoized `get()`; memory per isolate; behavior near the cap |
| Facets | one SQLite file and one LTX stream for each facet | call latency against a separate DO; write cost with 1, 10, and 100 facets per root |
| Containers | Docker Engine API; image loaded from the bucket on first use per node ([container.rs](../../crates/celld/container.rs)) | cold start with and without the image; proxy latency; containers per node before shedding |

## Tier 3: fleet scenarios

Fleet behavior needs several nodes, a shared bucket, and a network that can
be made slow. celld-tck already starts a Docker Compose fleet on MinIO and
can kill, pause, and discard nodes (`FleetControls.ts`, `Multinode.ts`,
`Qualification*.ts`). Its qualification context keeps a ledger of
acknowledged writes and audits it. The fleet scenarios should build on that,
as a new `perf` suite. They add:

- `tc netem` on the fleet network for delay, jitter, and loss between nodes;
- toxiproxy in front of MinIO for bucket latency and 429s;
- cgroup I/O limits, or the `perf` `FileSystem` wrapper, for one slow disk;
- the Tier 2 load generator as a container, which reads the Tier 0 snapshot
  from every node.

| # | Scenario | Sweep | What it measures |
| --- | --- | --- | --- |
| F1 | Fleet proof | 2, 3, and 5 nodes; `CELLD_LOG_PIPELINE`; `CELLD_LOG_TRANSPORT` http and stream; `CELLD_LOG_WINDOW` | Acknowledgement latency by proof source (target: ~25 ms against ~600 ms for a bucket proof, from testing.md); follower fsync p99; cells per ship round |
| F2 | Gray follower | one follower with a slow fsync; then a slow network; then with 3% loss | Acknowledgement p99 during the fault; time to evict; swaps. Checks the claim that a slow follower never makes a write slower than a bucket proof |
| F3 | Forwarding | all requests sent to a node that owns no cells | The latency a forward adds; tunnels per peer pair (the tunnel is not multiplexed); how often `RemoteCache` answers |
| F4 | Scale-out | 1 → 2 → 4 → 8 → 16 nodes on a sharded workload; add one node under load | Throughput for each node added; time for rebalance to converge; cells moved per second; placement fairness |
| F5 | Takeover | SIGKILL an owner of 1k and 10k cells; the same with its local disk deleted; sweep `CELLD_TTL_MS` | Time from the kill to the first served request, for each cell, split into lease expiry, recovery, and restore; the burst of bucket operations. Checks the lab result that ten nodes recovered from losing two in ~11 s at the tail |
| F6 | Drain and upgrade | drain a node with 1k and 10k resident cells; a rolling restart with a clean reload | Handoff time by phase (`cell_handoff_timing`); errors during the drain; restart time against the resident count |
| F7 | Fleet deploy | move the deploy pointer under load | Time until every node serves the new version, with polling (30 s) and with a nudge |
| F8 | Bucket throttling | 429 on 10% and 50% of requests; a bucket p99 of 1 s | Throughput falls, and bucket requests per second do not rise (no amplification); fleet mode against bucket mode |
| F9 | Cost at rest | an idle fleet of 3, 10, and 30 nodes holding 0, 10k, and 100k cells | Bucket operations per second by key class: node lease CAS every ~3.3 s, capacity sample and rebalance every 5 s, waker scan every 60 s. A deterministic count to compare with the DynamoDB design's estimates |
| F10 | Contended activation | 500 claimants activate the same cells | Activation latency under CAS races (the fence itself is already tested) |
| F11 | Hot namespace | KV reads on one namespace from 1, 3, and 10 nodes | Proves the single-owner ceiling: throughput should stay flat as nodes are added |

## Tier 4: cloud qualification

This is the fleet lab of testing.md, put into version control. The Tier 3
suite runs against a real S3, GCS, Azure, or R2 bucket, on VMs of the lab
shape (4 vCPU, 8 GB), at the scale the documentation claims (10 nodes,
10k resident cells, 20k WebSockets). It runs before each release, on the
release image. Its results replace the numbers in testing.md, with their
conditions. It also reports cost: bucket operations and egress per million
requests, at each provider's list price.

## Workloads

Purpose-built fixtures cover what the examples do not. One Worker takes
its parameters from the query string:

- `noop`: returns at once. It measures the turn overhead.
- `read?cell=`: reads a key.
- `write?cell=&bytes=`: writes a value and returns its checksum.
- `sql?cell=&rows=`: inserts or reads a number of rows.
- `ws-echo` and `ws-room?members=`: echo and fan-out.
- `alarm?cell=&in=`: sets an alarm, and records the time it fired.
- `rpc?depth=`: a chain of DO RPC calls.
- `blob?cell=&kb=`: a cell with a large database, for restore.

Each cell keeps a running checksum of its writes, as the lab's cells do, so
the verification sweep can check every cell cheaply.

The examples cover the services and the heavy bundles: `counter`, `wsecho`,
`router`, `rpc`, `alarm`, `kv`, `d1`, `r2`, `queues`, `workflow`, `cron`,
`static-assets`, `dynamic-worker-tails`, `facets`, `container`, and
`opencode` and `pi` for large bundles.

The keyspace generator supports uniform, Zipfian (s = 0.99), and shifting
working sets (the lab's rotation across tens of thousands of cells).

## The harness

The harness is a new workspace crate, `crates/perf`, with one binary,
`celld-perf`. It:

1. starts N `celld` nodes as subprocesses with the `lab` profile, or
   attaches to nodes that are already running (Tier 3 and 4);
2. deploys a fixture with `celld deploy`;
3. warms up, then runs each phase of a scenario at a fixed rate, ramping
   between rates;
4. records client latency in HDR histograms, and takes `/debug/metrics` and
   `/state` from every node before and after each phase;
5. samples RSS, CPU, file descriptors, and threads from `/proc` every
   second, and can run `perf record` around a phase to produce a flame graph
   (the `lab` profile keeps line tables for this);
6. runs the verification sweep;
7. writes one JSON result to `target/perf/<run-id>/`: the scenario, its
   parameters, the environment, the histograms, the counters, and the
   verdicts.

The load generator is written in Rust on hyper and tokio, in the same
crate. It needs precise open-loop timing, WebSockets with 50k connections,
and HDR output. It also avoids adding a JavaScript or Go toolchain to the
build. Scenarios are declared in TOML files under `crates/perf/scenarios`,
so celld-tck can run the same scenario against its own fleet.

`celld-perf compare A B` compares two sets of results. A timing counts as a
regression when the bootstrap confidence intervals of at least five
repeated runs do not overlap, and the change is larger than the scenario's
threshold. A count counts as a regression when it differs at all.

## Cadence and gates

| When | Runs on | What | Gate |
| --- | --- | --- | --- |
| Every PR | GitHub runners | Criterion `-- --test` for every target; count tests (below); a 30 s Tier 2 smoke of S2, S3, S8, and S10 on LocalStore that checks errors and counts, not time | Fails the PR |
| Nightly | a dedicated machine: fixed CPU frequency, no other load, local NVMe | all of Tier 1 against the last baseline; Tier 2 S1–S14 with MinIO and injected latency; Tier 3 F1, F3, F5, and F9 on Compose | Opens an issue when a regression repeats on a second run |
| Weekly | the same machine | S15 (24 h soak); all of Tier 3 | Opens an issue |
| Each release | cloud VMs | Tier 4 | A release note, and an update to testing.md |

The count tests, as ordinary `#[test]` functions in the harness crate, are:

- a warm read makes 0 bucket operations;
- a bucket-proof write makes 1 PUT and 1 GET;
- 64 concurrent writes to one cell make fewer than 64 PUTs;
- a DO request makes exactly 2 core round trips (route and gate);
- a cold activation of a cell with one epoch makes at most the expected
  number of LISTs, GETs, and conditional PUTs;
- an idle node stays within its budget of bucket operations per minute;
- an activation into an existing isolate compiles no bundle.

GitHub's shared runners are not used for timing, as
[export-benchmarks.md](../export-benchmarks.md#ci-smoke-checks) already
says.

## Bottleneck hypotheses

The code suggests these limits. Each gets a scenario built to saturate it,
and a metric that shows whether it is the limit.

| Hypothesis | Where | Scenario | Evidence |
| --- | --- | --- | --- |
| The `celld-core` thread limits DO throughput, because each request and each WebSocket message needs two round trips | [main.rs](../../crates/celld/main.rs) core runtime; [actor.rs](../../crates/celld/actor.rs) | S7, S10 | Core lag grows while V8 CPU is still free |
| The main `block_on` loop limits WebSockets, DO calls, and service calls, because it polls their futures itself | main.rs event loop | S10, S7 | Main loop lag; one core at 100% |
| Neighbouring hot cells in one isolate slow each other down | `MAX_CELLS_PER_ISOLATE = 32`, `TurnScheduler` in [pool.rs](../../crates/celld/pool.rs) | S6 | p99 of one cell against its neighbours' load |
| Isolate growth stalls placement, because `grow()` compiles while holding the maintenance write lock | pool.rs | S8 | Lock wait during bursts of activations |
| Forwarded requests need many tunnels, because a tunnel carries one request at a time | [peer_tunnel.rs](../../crates/celld/main/peer_tunnel.rs) | F3 | Tunnels per peer; forward p99 against the rate |
| Many cells writing at once wait for the 64 upload slots in bucket mode | `SYNC_CONCURRENCY` in [ltx_repl.rs](../../crates/celld/ltx_repl.rs) | S5, S11 | Time waiting for a slot |
| Restore time grows with the epoch chain, at 10 LISTs per epoch | [epochs.rs](../../crates/ltx/src/client/epochs.rs) | S8, F5 | LISTs per activation against the chain length |
| One KV namespace, queue, or D1 database cannot scale out | reserved cells with one writer | F11, S12 | Flat throughput as nodes are added |
| Workflow replay makes a long workflow quadratic | workflow harness | S12 | Step latency against the step count |
| Global mutexes contend: `RuntimeManager.cells`, `WsRegistry`, `loader_registry`, the LTX cell map | runtime.rs, [ws_registry.rs](../../crates/celld/ws_registry.rs), js.rs, ltx_repl.rs | S7, S10 | Off-CPU time in `perf` or `tokio-console` |

The first profile of each scenario should confirm or remove the hypothesis
before anyone tunes it.

## Milestones

1. **Instrumentation.** Bucket counters and histograms, histograms for each
   phase, loop lag probes, `/debug/metrics`, and the `perf` feature with the
   store, filesystem, and transport wrappers.
2. **Counts and components.** The count tests in CI; the Tier 1 targets for
   `ltx`, `core_events`, `storage_ops`, and `node_log`.
3. **Single node.** `celld-perf`, the fixtures, S1–S8 nightly on a dedicated
   machine. First deliverable: reproduce the warm-request numbers from
   testing.md, from a checkout.
4. **Breadth.** S9–S15 and the rest of Tier 1.
5. **Fleet.** The `perf` suite in celld-tck with netem and toxiproxy: F1,
   F5, and F9 first, then the rest.
6. **Cloud.** Tier 4 before each release; testing.md cites its results.

Each milestone is useful alone. Milestone 2 catches most regressions in
cost even if the later ones never happen.

## Open questions

- **Where does the fleet suite live?** celld-tck has the fleet controls, the
  faults, and the ledger audit, but it is TypeScript and pinned to a commit.
  The alternative is a Compose fleet driven by `celld-perf` in this
  repository. This plan prefers celld-tck, with scenarios shared as TOML.
- **Which machine runs nightly?** A self-hosted runner gives stable numbers.
  A cloud VM for each run is simpler but noisier, and needs more repeats.
- **How are metrics exposed?** A JSON snapshot on the internal listener is
  the smallest change. A Prometheus endpoint would help operators too, but
  it is a public interface and a separate decision.
- **Can LocalStore host a local fleet?** Several nodes on one SQLite store
  would give a fast multi-node loop without Docker, but
  [limitations.md](../limitations.md) says the store is for development
  only. A `perf`-only exception needs a decision.
- **Are the lab's numbers still true?** Several were measured before
  `celld-ltx` replaced the external replicator. Milestone 3 may show that
  testing.md needs a correction before it shows a regression.

## What was built

Revision 2 implements milestones 1 to 4, and the fleet scenarios of
milestone 5 in this repository. Where the implementation departs from the
plan above:

- **Metrics exposure.** It is a JSON snapshot, `GET /debug/metrics` on the
  internal listener. There is no Prometheus endpoint. The instruments are
  static arrays in `crates/celld/perf_stats.rs`, always on. The bucket
  counters come from one wrapper installed where each store is built
  (`perf_store.rs`), so no call site changed. Paged-restore page faults
  sign their own requests and are not counted.
- **Fault seams.** The `perf` feature exposes a slow or throttling bucket
  (`CELLD_PERF_BUCKET_FAULTS`) and a slow fsync
  (`CELLD_PERF_FSYNC_DELAY_US`) to a node started as a subprocess, and not
  only to in-process tests. An ordinary build refuses both variables.
- **Network faults.** The harness, not celld, injects them. A scenario with
  `"network": true` routes each node's peer and bucket traffic through TCP
  proxies. `net` steps set delay, jitter, bandwidth, resets and partitions
  on each direction of a link, and they can change during a phase. This
  replaces the `LogTransport` injector and `tc netem` of the plan. It needs
  no root and runs on macOS, and it covers every peer protocol and the
  bucket, not only the log. The scenarios are N1 to N5.
- **Count tests.** They are integration tests in `crates/celld`, where the
  test can start the `celld` binary it was built with. They are not in the
  harness crate. The core-round-trip gate counts `core.requests` and
  `core.outputs`, one each per DO request. `core.messages` is three per
  request, because the activity-finished notice is a message too.
- **Scenario files.** They are JSON, not TOML, so the harness adds no
  dependency. Variants, per-node environment, timed steps during a phase
  (kill, freeze, start, redeploy), and a per-second timeline were added so
  that the fleet scenarios fit the same format.
- **Where the fleet scenarios live.** The fleet scenarios (F1 to F9, F11)
  run from this repository, on the `s3` backend against MinIO. They do not
  run in celld-tck. The harness starts every node on one machine. F10
  (contended activation) was not built.
- **Service time.** Each phase reports service time (from send) beside
  latency (from schedule). On macOS the generator's timer can fire a
  millisecond late, and this separates the generator's lateness from the
  node's.
- **CI.** Pull requests run the count gates, every Criterion target in test
  mode, and `celld-perf run smoke`. `.github/workflows/perf.yml` runs
  nightly and weekly, on a runner named by the `PERF_RUNNER` variable. It
  compares each run with the last nightly on main, and opens an issue on a
  regression.

The first runs found one defect. `Effect::StartRuntime` runs
`RuntimeManager::start_cell` in a future that the core thread polls, and
`Worker::own_cell` opens the cell's SQLite database inside it: the open,
the schema, and WAL writes. A burst of new cells therefore occupies the
thread that renews the node lease. In a macOS `sample` of a lab-build node
receiving 150 first activations per second, 2,064 of 2,200 core-thread
samples were in that open. `loop.core_lag_us` reached p99 18 s. At 300 per
second the node missed its renewal and fenced itself. `S16-activation-rate`
reproduces it. The other scenarios activate their setup cells 16 at a
time, so they measure what they are for.

The other findings of the first runs were:

- **Residency churn has a ceiling.** With `CELLD_MAX_RESIDENT_CELLS=200`
  and S3-like bucket latency, a lone node sustained about 10 evict-and-restore
  activations a second (restore p50 303 ms). At 25 a second requests waited
  about 15 s for capacity and failed with `CapacityExhausted` (S8).
- **The dev store is not a bucket.** On the dev store, 200 writes a second
  over 1,000 cells took p50 540 ms. On MinIO, with the same injected
  latency, they took p50 96 ms. Both made two bucket requests per write.
  The dev store fsyncs every object under one SQLite writer.
- **Capture fsyncs add up per round.** In a three-node fleet on macOS, a
  ship round's captures fsync each cell's L0 file. At 300 writes a second a
  round carried about 28 cells, and its fsyncs summed to p50 418 ms against a
  capture of 336 ms: nearly serial. The fleet proof went from p50 76 ms at 100
  writes a second to 1.9 s at 300. macOS fsync is slow, so confirm this on
  Linux before acting on it.
- **The takeover works.** A killed owner's cells answered "owner
  unreachable" for about the lease TTL (10 s), then served again, with no
  acknowledged write lost (F5).

The network scenarios (N1 to N5, three nodes on MinIO, macOS) found:

- **A silent partition costs far more than a refused one.** Node 1, cut off
  from its peers and the bucket while it kept running, fenced itself in
  about 7 s either way. With the links blackholed, requests held on it
  waited up to 35 s and 365 writes failed with `NodeFenced`. With the links
  refused, 2 writes failed and the worst request took 1 s (N4).
- **A peer partition stalls requests for its whole length.** Node 2 lost
  its peers but kept the bucket and its lease. Requests for its cells
  through node 0 neither failed nor met the operation deadline: they waited
  for the 30 s partition to heal, up to 37 s. Node 0 opened 1,727 new
  connections to node 2 meanwhile (N2).
- **A node cut off from the bucket fences itself**, here in 6.3 s. Its cells
  moved, 24 writes failed with `NodeFenced`, and none was lost (N3).
- **Tunnels churn.** Over 70 s, node 0 opened 191 connections to one peer
  where the log links held one. Each tunnel carries one request at a time.
- **No scenario lost an acknowledged write.**

The runs also point at costs to look at next:

- the pressure victim scan clones every candidate's cell id (about 200 ns
  per resident cell);
- rebalance selection clones and sorts every dormant id, even for a budget
  of one;
- `request_authorized` removes and reinserts each cell in the core's cell
  map on every request, which is most of the growth from 10 to 1M known
  cells in `core_request`.

A debug build's core thread falls behind its lease renewals at 1,000
requests per second, and the node fences itself.

