# Performance tests

celld measures its performance in three layers. Each layer answers what the
one below it cannot, and each has its own command.

| Layer | What it measures | Where | Command |
| --- | --- | --- | --- |
| Count gates | Operations per request: bucket requests, core messages, uploads, Worker loads | `crates/celld/tests/perf_counts.rs` | `cargo test -p celld --test perf_counts` |
| Component benchmarks | One function's time: capture, compaction, restore, the core's event step, storage, follower append, V8 turns | Criterion targets in `crates/*/benches` | `cargo bench -p <crate> --bench <target>` |
| Scenarios | A node or a fleet under open-loop load: latency, throughput, CPU, memory, bucket traffic, recovery time | `crates/perf` (`celld-perf`) | `celld-perf run <scenario>` |

The design and its reasoning are in
[design/performance-testing.md](design/performance-testing.md). The export
pipeline has its own Criterion targets, described in
[export-benchmarks.md](export-benchmarks.md).

## Contents

- [What a node counts](#what-a-node-counts)
- [Count gates](#count-gates)
- [Component benchmarks](#component-benchmarks)
- [Scenarios](#scenarios)
- [Injected faults](#injected-faults)
- [Writing a scenario](#writing-a-scenario)
- [Results and comparison](#results-and-comparison)
- [CI and cadence](#ci-and-cadence)
- [Reading the numbers](#reading-the-numbers)
- [Not yet covered](#not-yet-covered)

## What a node counts

Every node keeps counters and latency histograms for its hot paths, and
serves them on its internal listener:

```sh
curl -s http://127.0.0.1:8081/debug/metrics | jq .
```

The instruments are always on. Each observation is a relaxed atomic add on
a fixed static array, so a node takes no lock and allocates nothing to
record one. A histogram is log-linear, with 16 buckets per power of two,
so a value lands in a bucket at most 6.25% wider than itself. The snapshot
carries each histogram's raw buckets. The difference of two snapshots is
therefore itself a histogram, and a harness reads percentiles over any
window by subtracting ([`perf_stats.rs`](../crates/celld/perf_stats.rs)).

| Instrument | Meaning |
| --- | --- |
| `request.stateless_us`, `request.stateless_queue_us` | A stateless Worker fetch, and its wait for an isolate |
| `request.cell_route_us` | The core's route for a cell that was not resident: ownership reads, activation, restore |
| `websocket.route_us` | One WebSocket message's route to its cell |
| `gate.wait_us` | One output gate, from submission to the core's answer |
| `durability.proof_fleet_us`, `durability.proof_bucket_us` | A durability wait, by which proof released it |
| `activation.{fresh,local,download,paged}_us` | An activation, by how it found its database |
| `isolate.startup_us`, `isolate.worker_load_us` | A cell isolate's startup; one V8 isolate plus bundle compile |
| `log.ship_round_us`, `log.ship_round_cells`, `log.ship_round_bytes` | One fleet ship round |
| `capture.total_us`, `capture.encode_us`, `capture.fsync_us` | The capture inside a ship round |
| `log.follower_write_us`, `log.follower_fsync_us` | A follower's batch write, and its file and directory fsyncs |
| `log.bundle_flush_us` | One node bundle upload |
| `handoff.total_us` | One cell handoff between nodes |
| `loop.core_lag_us`, `loop.main_lag_us` | How late the core thread and the main loop ran a timer (see [below](#reading-the-numbers)) |
| `core.messages`, `core.requests`, `core.outputs` | Messages the core thread handled; route decisions; output gates |

Every object-store request goes through one counting wrapper that is
installed where the store is built ([`perf_store.rs`](../crates/celld/perf_store.rs)).
The snapshot's `bucket.requests` counts requests by **op** (`get`,
`get_range`, `head`, `put`, `put_create`, `put_update`, `multipart`, `list`,
`list_delimited`, `list_paginated`, `delete`, `copy`), by **class** of key,
and by **outcome** (`ok`, `not_found`, `precondition`, `throttled`,
`error`). A key's class comes from the first segment of its path that names
a known root:

| Class | Keys |
| --- | --- |
| `cell_owner` | `cells/<cell>/own.json` |
| `cell_data` | everything else under `cells/`: LTX files, facets, snapshots |
| `nodes`, `fleet`, `drain`, `probe` | node leases, capacity samples and fleet singletons, the drain token, storage probes |
| `log_bundle`, `log` | node log bundles; recovery claims and tails |
| `wake`, `deploy`, `r2`, `kv`, `export`, `telemetry`, `other` | the rest |

`bucket.bytes` records payload bytes by op and class. `bucket.latency_us`
records latency by op; a GET's latency ends when its headers arrive.

## Count gates

`crates/celld/tests/perf_counts.rs` starts a `celld dev` node on the bench
fixture. It sends requests one at a time and asserts what the node counted:

- a warm read makes no request for its cell's keys, one route decision, and
  one output gate;
- a lone node proves each write with at most one upload and exactly one
  ownership read;
- 64 concurrent writes to one cell share their uploads;
- a cell activated into an existing isolate compiles nothing;
- an evicted cell comes back from its local database: one ownership read
  and one claim, and no listing;
- an idle node stays within 5 bucket requests per second, and a resident
  cell costs nothing while it waits.

They run in `cargo test`, so they gate every pull request. A count does not
depend on the machine, so a test fails when a change adds a request or a
round trip, not when a runner is slow. When a change makes a count smaller,
update the bound in the same change.

## Component benchmarks

| Target | Command | Groups |
| --- | --- | --- |
| `ltx` | `cargo bench -p celld-ltx --bench ltx` | `ltx_capture` (`Db::sync` for 1, 16, 256 pages), `ltx_codec` (LZ4, CRC64, LTX encode and decode), `ltx_compact` (L0 merges, replica compaction), `ltx_restore` (image apply, file-replica restore, paged page map) |
| `core_events` | `cargo bench -p celld-logic --bench core_events` | `core_request` (local, remote, read and write output gates, with 10, 10k and 1M known cells), `rebalance`, `pressure` |
| `perf_components` | `cargo bench -p celld --features perf --bench perf_components` | `storage_ops` (DO KV, SQL, cursors, statement cache, commit), `node_log` (follower append with real fsync, append and bundle encode), `js_turn` (Worker load, empty fetch turn, structured clone), `wire` (peer signing and verification), `services_pure` (queue batch policy, cron, asset routing) |
| `export_format`, `export_pipeline` | see [export-benchmarks.md](export-benchmarks.md) | export |

Every case checks its fixture before timing. Baselines work as in any
Criterion target:

```sh
cargo bench -p celld-logic --bench core_events -- --save-baseline before
# change something
cargo bench -p celld-logic --bench core_events -- --baseline before
```

## Scenarios

`celld-perf` starts nodes as subprocesses and deploys a fixture Worker. It
drives the nodes with open-loop load and reads each node's
`/debug/metrics` before and after every phase. It ends with a verification
sweep and writes one JSON result.

Build a node in the `lab` profile (optimized, with line tables for
`perf`), with the `perf` feature for injected faults:

```sh
cargo build --profile lab -p celld -p celld-perf --features celld/perf
target/lab/celld-perf list
target/lab/celld-perf run S2               # one scenario, by id or name
target/lab/celld-perf run all --repeat 3   # every scenario the backend supports
```

A debug build answers too slowly to measure. Under load its core thread
falls far enough behind that the node misses its lease renewal and fences
itself.

### Backends

**`dev`** (the default) runs one `celld dev` node on the local SQLite store,
in a copy of the fixture project. It needs nothing else. Use it for counts,
CPU ceilings, and single-node paths.

**`s3`** deploys to an S3-compatible bucket and starts N ordinary nodes on
it, each with its own listeners and local state. This is the fleet path,
and the only one with fleet proofs. Every run writes under its own prefix
of the bucket.

```sh
docker run -d -p 127.0.0.1:9000:9000 \
  -e MINIO_ROOT_USER=perf -e MINIO_ROOT_PASSWORD=perf-disposable-password \
  <a MinIO image> server /data          # then create the bucket `perf`
AWS_ACCESS_KEY_ID=perf AWS_SECRET_ACCESS_KEY=perf-disposable-password \
  target/lab/celld-perf run F1 --backend s3 --bucket s3://perf --endpoint http://127.0.0.1:9000
```

MinIO's public images are no longer published. The
[Performance workflow](../.github/workflows/perf.yml) downloads its
pinned, checksummed release binaries, as celld-tck does.

### Options

| Option | Effect |
| --- | --- |
| `--env NAME=VALUE` | Node environment, over the scenario's; for example sweep `CELLD_TOKIO_THREADS` |
| `--repeat N` | Run each scenario N times; `compare` needs three per side to call a timing change significant |
| `--quick` | Every phase at a fifth of its length (at least 1 s), for a fast check |
| `--enforce-timing` | Fail the run when a timing check fails |
| `--keep` | Keep each run's project copy and node state |
| `--out DIR` | Results directory (default `target/perf`) |

A run exits 1 when a count check fails, or when the verification sweep
finds a lost or unreadable write.

### The scenario catalog

| Scenario | Backend | What it answers |
| --- | --- | --- |
| `smoke` | any | Every main path at low rates, count checks only (CI) |
| `S1-stateless-ceiling` | any | Peak stateless requests per second; CPU per request |
| `S2-warm-read` | any | Warm read latency (testing.md's p50 ~1.1 ms, p99 ~7 ms); no bucket request, one route, one gate |
| `S3-write-bucket-proof` | any | A lone node's write latency at local, S3-like, and slow bucket latency; one upload and one ownership read per write |
| `S4-write-coalescing` | any | Uploads per write as one cell's write rate rises |
| `S5-upload-slots` | any | Many cells committing at once against the 64 upload slots |
| `S6-hot-cell` | any | One cell's ceiling, alone and with busy isolate neighbours |
| `S7-core-saturation` | any | Whether the core thread or V8 saturates first on small DO calls |
| `S8-cold-activation` | any | Restores from the bucket after a restart that deleted local state; LISTs per activation |
| `S8-cold-activation-large` | any, heavy | 64 MiB and 300 MiB (paged) cells restored |
| `S9-memory` | any | RSS with 10k resident cells, then after evicting them all |
| `S10-websockets` | any | 2,000 sockets: echo, auto-response pings (no core), writes, broadcast |
| `S11-alarms` | any | 2,000 alarms due at once; how late they fire |
| `S12-services`, `S12-workflow-steps` | any | KV, D1, R2, Queues (delivery lag), Workflows (completion time against step count) |
| `S13-deploy-under-load` | s3 | A deploy and reload under load: the cutover in the timeline |
| `S14-overheads` | any | The warm paths with telemetry and change export on |
| `S15-soak` | any, heavy | 24 hours of shifting mixed traffic: growth and drift |
| `S16-activation-rate` | any | Fresh cells activated at 50, 150 and 300 per second: how activation load reaches the core thread and the node lease (fails today; see [design](design/performance-testing.md#what-was-built)) |
| `F1-fleet-proof` | s3 | Fleet proof latency (testing.md's ~25 ms) on 2, 3, 5 nodes, both log transports |
| `F2-gray-follower` | s3 | One follower with a 50 ms fsync: eviction, and no write slower than a bucket proof |
| `F3-forwarding` | s3 | Requests forwarded through the peer tunnel |
| `F4-scale-out` | s3 | Throughput on 1, 2, 4 nodes |
| `F5-takeover` | s3 | An owner killed mid-load, then with its disk lost: unavailability in the timeline, no acknowledged write lost |
| `F6-drain` | s3 | A drain (SIGTERM handoff) under load |
| `F7-frozen-owner` | s3 | An owner frozen past its lease, then thawed |
| `F8-bucket-throttle` | s3 | 10% and 50% of bucket requests answered 429: no amplification |
| `F9-idle-cost`, `F9-idle-cost-resident` | s3 | Bucket requests an idle fleet makes on its own, with and without 10k resident cells |
| `F11-hot-namespace` | s3 | One KV namespace does not scale out |

`all` skips the heavy scenarios; name them to run them.

## Injected faults

A `perf` build honors two variables. An ordinary build refuses to start
with either one set, so a benchmark cannot run at full speed by mistake.

`CELLD_PERF_BUCKET_FAULTS` makes the bucket slower or throttled. It is a
comma-separated list:

| Item | Effect |
| --- | --- |
| `read=MS`, `write=MS`, `list=MS`, `all=MS` | A fixed delay before each request of that kind (`read` is GET and HEAD; `write` is PUT, multipart, copy, delete) |
| `tail=MS` | Plus an exponentially distributed delay with this mean |
| `throttle=F` | A fraction F of requests fails with a 429 before reaching the bucket |
| `class=A+B` | Only for keys of these classes |

The scenarios use `read=15,write=30,list=20,tail=10` as an S3-like profile.
The faults sit under the request counters, so an injected delay shows as
that request's latency, and an injected 429 as a throttled request.

`CELLD_PERF_FSYNC_DELAY_US` adds a delay to every fsync of the node's own
filesystem: LTX files, follower batches, and their directories. SQLite's
own syncs do not pass through it. A scenario's `node_env` sets it on one
node to make a gray follower.

## Writing a scenario

A scenario is a JSON file in `crates/perf/scenarios`. Fixtures are in
`crates/perf/fixtures`:

- `bench`: `/noop`, `/cpu?iters=`, `/do/<op>?cell=` with ops `noop`, `read`,
  `write?bytes=`, `sql?rows=`, `sqlread`, `blob?kb=`, `alarm?in=`,
  `alarmstat`, `rpc?depth=`, `state`, plus hibernatable WebSockets on
  `/ws?cell=`;
- `services`: `/kv/*`, `/d1/*`, `/r2/*`, `/queue/send`, `/wf/create`,
  `/stats?kind=queue|workflow`.

```json
{
  "name": "S2-warm-read",
  "nodes": 1,
  "env": {"CELLD_MAX_RESIDENT_CELLS": "5000"},
  "setup": [
    {"step": "touch", "request": {"path": "/do/write", "query": {"bytes": "100"}, "counts_write": true},
     "cells": {"count": 1000, "prefix": "warm"}}
  ],
  "phases": [
    {"name": "reads", "duration_s": 20, "rates": [1000, 4000],
     "load": [{"request": {"path": "/do/read"}, "cells": {"count": 1000, "prefix": "warm", "distribution": "zipf"}}],
     "checks": [
       {"metric": "bucket:class=cell_owner+cell_data", "per_ok": true, "max": 0},
       {"metric": "client:p99_us", "max": 7000, "timing": true}
     ]}
  ]
}
```

- **Scenario fields:** `name`, `description`, `fixture`, `nodes`, `env`,
  `node_env` (by node index), `backends`, `heavy`, `variants` (each a
  `name` with `env` and `nodes` overrides; the scenario runs once per
  variant), `setup`, `phases`, `after`, `verify`.
- **Steps** (`setup`, a phase's `before`, a phase's `during` with `at_s`,
  and `after`):
  - `touch`: one request to every cell;
  - `connect`: open WebSockets;
  - `sleep`;
  - `restart` (optionally `wipe_local`);
  - `evict_all`;
  - `signal` (`KILL`, `TERM`, `STOP`, `CONT` to one node);
  - `start` (one node, optionally `wipe_local`);
  - `redeploy` (s3 backend, optionally `reload`);
  - `collect`: one request per cell, summing and maxing the numeric fields
    of the answers.
- **Phases:** `rate` or `rates` (one phase per rate), `duration_s`,
  `arrival` (`uniform` or `poisson`), `warmup`, `load`, `checks`,
  `max_inflight`, `timeout_ms`.
- **Loads:** each is weighted, and either an HTTP `request` (with optional
  `cells` and a pinned `node`) or a WebSocket `message` (`echo`, `write`,
  `broadcast`, `ping`) on the sockets a `connect` step opened.
- **Cells:** `count` and `prefix`, and a `distribution`: `uniform`, `zipf`
  (`zipf_s`), or `shifting` (a `window` that moves `shift_per_s` cells per
  second).
- **Checks:** a `metric`, optionally divided by successful requests
  (`per_ok`), by another metric (`per`), or by the phase's seconds
  (`per_second`), and bounded by `min` and `max`. A check marked `timing`
  fails a run only with `--enforce-timing`. Metrics:
  - `bucket[:class=…,op=…,outcome=…]`;
  - `counter:LABEL`;
  - `hist_count:LABEL`, `hist_p50:LABEL`, `hist_p99:LABEL`;
  - `client:{ok,errors,shed,error_rate,achieved_rate,p50_us,p99_us,p999_us}`;
  - `node:{cpu_cores,rss_bytes_max}`.

## Results and comparison

Each run writes `target/perf/<run-id>/result.json` and prints a summary.
For each phase the result holds:

- the offered rate, the achieved rate, and error counts by kind;
- **latency** from each request's scheduled start, which is what a client
  waits, and **service time** from the moment each request was sent;
- a per-second **timeline** of successes, errors, and the slowest success;
- for each node, and merged: counter deltas, histogram deltas, and bucket
  request deltas; CPU cores used and peak RSS;
- each check's value and verdict, and the steps run before and during the
  phase.

The run also records the verification sweep and each node's end state. It
records the celld version, the commit, the host, and the environment.

The generator is open-loop. It sends at the offered rate whatever the node
does, so queueing shows in latency rather than slowing the generator
(coordinated omission). When latency and service time differ, the
generator ran behind its schedule. `schedule_lag_max_us` says by how much.

```sh
celld-perf compare base/result.json new/result.json [--threshold 0.05]
celld-perf summary result.json
```

`compare` matches scenarios and phases by name, and fails on two kinds of
change:

- **a count regression:** bucket requests or core messages per successful
  request grew by more than the threshold, and by more than 0.05 in
  absolute terms;
- **a timing regression:** the 95% bootstrap interval of the change in
  throughput or client p50 or p99 lies entirely past the threshold. This
  needs at least three repeats on each side. With fewer, a large change is
  only reported as possible.

## CI and cadence

| When | Where | What | Gate |
| --- | --- | --- | --- |
| Every pull request | `ci.yml` | The count gates, the component and export benchmarks in Criterion test mode (each case once), and `celld-perf run smoke` on the debug build | Fails the pull request |
| Nightly | `perf.yml` | Every component benchmark against the last nightly baseline; `celld-perf run all --repeat 3` on the dev backend; F1, F3, F5 and F9 on MinIO; `compare` against the last successful nightly | Opens or updates an issue |
| Weekly | `perf.yml` | Every scenario, including the heavy ones and the 24-hour soak | Opens or updates an issue |

`perf.yml` runs on a GitHub runner unless the repository variable
`PERF_RUNNER` names a dedicated one, for example
`["self-hosted","perf"]`. A shared runner's counts and failures are sound,
but its timings are too noisy to call regressions, and it stops a job after
six hours, which is before the soak ends. For a dedicated machine, use
Linux on bare metal: a fixed CPU frequency, SMT and turbo off if you can,
local NVMe, and nothing else running.

## Reading the numbers

- **The platform.** macOS coalesces timers and parks idle cores deeply.
  A lightly loaded node there shows 1 to 3 ms of latency that the same node
  under load, or on Linux, does not. Compare numbers from the same machine
  only.
- **Loop lag.** `loop.core_lag_us` and `loop.main_lag_us` include the
  timer's own granularity, about 1 ms, and more on macOS. Read them against
  an idle baseline from the same machine. A rise under load means the
  thread is busy.
- **The bucket.** LocalStore and MinIO answer in well under a millisecond.
  Durability numbers are meaningful only with injected S3-like latency, or
  against a real bucket.
- **Cold routes.** A node's first request for a cell another node owns
  reads that cell's ownership record. A short phase that spreads requests
  over many nodes and cells is mostly first requests. Longer phases
  amortize them.
- **Paged restore.** Pages faulted in during a paged restore are fetched by
  a client that signs its own requests, outside the counting wrapper, so
  `bucket.requests` does not include them.
- **The dev store.** The `dev` backend's SQLite store fsyncs every object
  under one writer, so a lone dev node proves only a few hundred writes a
  second, far fewer than it would against MinIO. Its listings also scan the
  whole store on the calling thread. Measure durability (S3, S5) on the
  `s3` backend.
- **fsync on macOS.** A Mac's fsync is a full flush (`F_FULLFSYNC`) of
  10–15 ms, so capture and follower fsyncs, and with them fleet proofs,
  are an order of magnitude slower than on Linux with NVMe.
- **Ports on one host.** A local fleet at a few thousand writes a second
  can exhaust the ephemeral ports (macOS: `Can't assign requested address`).
  Its nodes then lose the bucket and fence themselves. Keep local rates
  lower, or widen the port range.

## Not yet covered

- **Cloud qualification.** This is the scenarios against a real S3, GCS,
  Azure, or R2 bucket on VMs of the lab shape (10 nodes of 4 vCPU and
  8 GB), before each release. Run the `s3` backend against that bucket with
  its endpoint. The harness starts every node on the machine it runs on,
  so a multi-host fleet needs nodes started by hand or by an operator.
- **Network faults between nodes.** Use `tc netem` on Linux around the
  `s3` backend: delay, jitter, and loss on the loopback or a bridge. There
  is no in-process network injector for the peer log transport.
- **Contended activation** (500 claimants for the same cells) needs more
  nodes than one machine runs well. celld-tck exercises its safety, but
  not its latency.
- **celld-tck.** The fleet scenarios live here, not in celld-tck. The
  scenario files are JSON, so the TCK can run the same files against its
  own Compose fleet.
