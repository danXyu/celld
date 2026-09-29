# Export performance benchmarks

Two [Criterion](https://bheisler.github.io/criterion.rs/book/) targets measure
export CPU and local storage costs. They use deterministic fixtures and check
round trips, captured row counts, or consumer state before timing.

```sh
# Format and consumer only; does not build the node/V8.
cargo bench -p celld-export-format --bench export_format --locked

# SQLite capture, Parquet, delivery state and bucket audit cache.
cargo bench -p celld --features export-bench --bench export_pipeline --locked
```

The second command builds the node dependencies, including V8, and can take
several minutes on a clean checkout. The `export-bench` feature exposes internal
fixtures only for this target; ordinary node builds do not include them.

## Coverage and timing boundaries

| Group | Cases | Timed work and throughput unit |
| --- | --- | --- |
| `export_json` | 1, 128 and 1,024 rows; mixed SQLite values | JSON serialization/deserialization; encoded bytes |
| `export_fragments` | 1,024 rows with 128-byte or 4-KiB text payloads, 64-KiB record cap | Split and reassemble separately; original encoded bytes |
| `export_consumer` | 128/1,024 commits, 1/32 streams, duplicate delivery, optional complete repair snapshot | Ingest and state derivation separately; delivered records, including duplicates and metadata |
| `capture` | 1/64/512 rows, 128-byte/4-KiB blobs; 64 oversized 32-KiB rows | Plain SQLite write, write plus checkpoint, checkpoint alone; updated rows |
| `parquet` | 64/1,024 mixed-value records over 32 streams | Production Parquet encoding/decoding; records |
| `delivery_cache` | 64 historical/64 active streams, 4,096/64, 1,024/1,024 | Production cache lookups, position updates and spill within a transaction; 64 acknowledgements per iteration |
| `audit_cache` | 16/128 Parquet objects, one stream and 64 commits per object | Cold/warm index refresh and stream summaries; objects. One-cell state derivation; delivered records for that cell |

Capture fixtures update a fixed-size in-memory table. Capture sessions and schema
caches are warmed first. Checkpoint-only timing excludes the preceding write;
every write is immediately followed by a checkpoint. The oversize case verifies
that capture returns a bulk marker instead of row afterimages. Compare plain
writes with writes plus checkpoint to estimate capture overhead for these cases.
This does not include LTX publication or a durable SQLite commit to disk.

Input construction and cloning for ownership-consuming APIs are outside the timed
region using Criterion's
[`iter_batched` with `PerIteration`](https://docs.rs/criterion/0.8.2/criterion/enum.BatchSize.html).
Output destruction is outside that region for batched cases; ordinary `iter`
cases include it. Database creation, history seeding, and initial Parquet uploads
are excluded. Cold audit refresh includes index schema creation, local object
reads, decoding, indexing and connection close, but excludes temporary-file
creation/deletion. Warm refresh includes opening and closing the existing index
and listing unchanged objects. Audit operations include the Tokio `block_on`
boundary. They use the development SQLite bucket on local disk; this measures no
remote S3 request latency.

Delivery fixtures keep their historical and active stream sets fixed across
iterations, exercising the production hot-cache/spill implementation. They do
not run the full sink acknowledgement channel or exporter scheduling loop.
Consumer fixtures retain at most 128 current keys per stream while varying
history length. No measured loop accumulates new streams or rows indefinitely.

## Baselines and comparison

The default settings are 30 samples, one second of warm-up and three seconds of
measurement per case. Criterion writes reports under `target/criterion` (or
`$CARGO_TARGET_DIR/criterion`). To compare two revisions, run the same target on
each revision with the same target directory:

```sh
# Before the change:
cargo bench -p celld-export-format --bench export_format --locked -- --save-baseline before
# After the change:
cargo bench -p celld-export-format --bench export_format --locked -- --baseline before

# Restrict a pipeline run to one group:
cargo bench -p celld --features export-bench --bench export_pipeline --locked -- delivery_cache
```

Use the same machine, toolchain, feature set, power state and build profile, with
other heavy work stopped. The workspace bench profile inherits the release
optimization level but uses thin LTO and 16 codegen units to keep benchmark links
practical. Shipping release uses fat LTO and one codegen unit; these are not
identical builds. The pipeline target uses the node's jemalloc allocator; the
standalone format target uses the platform default allocator. Compare revisions
within a target, rather than treating cross-target timings as interchangeable.

These benchmarks measure elapsed time and throughput, not peak memory, network
backpressure, end-to-end durability latency or Snowflake warehouse performance.
Use integration/fault tests and workload profiling for those questions. A new
baseline alone does not demonstrate a performance improvement.

## CI smoke checks

CI compiles the targets with Clippy and executes each case once in Criterion test
mode, including fixture assertions. Debug-profile runs keep CI build costs down;
CI does not reject PRs based on timing noise from shared runners.

```sh
cargo test -p celld-export-format --bench export_format --locked -- --test
cargo test -p celld --features export-bench --bench export_pipeline --locked -- --test
```

See Criterion's [command-line options](https://bheisler.github.io/criterion.rs/book/user_guide/command_line_options.html)
for filters, saved baselines and shorter exploratory runs.
