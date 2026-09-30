// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Tier 1 component benchmarks (docs/design/performance-testing.md): the
//! synchronous storage work of a turn, the fleet log's fixed cost, isolate
//! load and turn overhead, peer request authentication, and the pure parts
//! of the service cells.
//!
//! Every case names one production function and checks its result before
//! timing. Nothing here needs a network; the storage and log cases write to a
//! temporary directory on the local disk.
#![allow(clippy::disallowed_methods)] // Offline benchmark, outside Actor execution.

use celld::js::Worker;
use celld::node_log::{decode_append, encode_append};
use celld::peer_auth::PeerAuth;
use celld::perf_bench::{
    append_req, confirmed, init_v8, large_worker, queue_policy, sql_run, worker_config,
    AssetFixture, CloneFixture, FollowerFixture, StorageFixture, TurnFixture, HELLO_WORKER,
};
use celld::storage;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rusqlite::types::Value;
use std::cell::RefCell;
use std::hint::black_box;
use std::sync::OnceLock;
use std::time::Duration;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The one runtime of the process. The node's execution domain binds to the
/// first runtime that reaches it, so every case shares this one.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

/// Rows in the table the read cases scan.
const ROWS: i64 = 1_000;
/// Distinct keys the KV and upsert cases cycle through, so the tables stay
/// the same size however many iterations run.
const KEYS: usize = 1_024;

/// `ctx.storage` KV and `ctx.storage.sql` on one cell database.
///
/// The database is a real file in WAL mode at `synchronous=NORMAL`, as every
/// cell's is, so a commit appends to the WAL without an fsync. No replicator
/// is attached, so SQLite's default automatic checkpoint (every 1,000 WAL
/// pages) runs and fsyncs, and its cost is spread across the write cases
/// that trigger it. No write here waits for LTX capture or a proof.
fn storage_ops(c: &mut Criterion) {
    let _fixture = StorageFixture::new();
    let scope = StorageFixture::SCOPE;
    storage::sql_exec(
        scope,
        "CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT NOT NULL); \
         CREATE TABLE writes(id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
        &[],
    )
    .unwrap();
    let text = Value::Text("x".repeat(100));
    storage::transaction_control(scope, "start", false, "").unwrap();
    for id in 0..ROWS {
        let run = sql_run(
            scope,
            "INSERT INTO items(id, v) VALUES (?1, ?2)",
            &[Value::Integer(id), text.clone()],
        );
        assert_eq!(run.rows_written, 1);
    }
    storage::transaction_control(scope, "commit", false, "").unwrap();
    let keys: Vec<String> = (0..KEYS).map(|n| format!("key-{n:04}")).collect();
    // A 256-byte stored row: a V8 wire-format one-byte string of 251 bytes
    // (version 15 header, tag, varint length), as `put()` stores one.
    let mut value = vec![0xff, 0x0f, b'"', 0xfb, 0x01];
    value.extend(std::iter::repeat_n(b'v', 251));
    assert_eq!(value.len(), 256);
    for key in &keys {
        storage::put_serialized(scope, key, &value).unwrap();
    }
    match storage::get_stored(scope, &keys[7]).unwrap() {
        Some(storage::StoredValue::V8(stored)) => assert_eq!(stored, value),
        _ => panic!("the KV fixture did not read back its value"),
    }

    let mut group = c.benchmark_group("storage_ops");
    group.throughput(Throughput::Elements(1));
    let mut n = 0;
    group.bench_function("kv_put_256b", |b| {
        b.iter(|| {
            n = (n + 1) % KEYS;
            storage::put_serialized(scope, &keys[n], &value).unwrap();
        });
    });
    group.bench_function("kv_get_256b", |b| {
        b.iter(|| {
            n = (n + 1) % KEYS;
            black_box(storage::get_stored(scope, &keys[n]).unwrap())
        });
    });

    // `sql.exec()` of an insert. It is an upsert over a fixed id range, so
    // the table does not grow with the iteration count.
    const UPSERT: &str = "INSERT INTO writes(id, v) VALUES (?1, ?2) \
                          ON CONFLICT(id) DO UPDATE SET v = excluded.v";
    const SELECT_ONE: &str = "SELECT v FROM items WHERE id = ?1";
    let binds: Vec<[Value; 2]> = (0..KEYS as i64)
        .map(|id| [Value::Integer(id), text.clone()])
        .collect();
    assert_eq!(sql_run(scope, UPSERT, &binds[0]).rows_written, 1);
    group.bench_function("sql_exec_insert", |b| {
        b.iter(|| {
            n = (n + 1) % KEYS;
            black_box(sql_run(scope, UPSERT, &binds[n]))
        });
    });
    let first = sql_run(scope, SELECT_ONE, &[Value::Integer(1)]);
    let again = sql_run(scope, SELECT_ONE, &[Value::Integer(1)]);
    assert_eq!((first.rows, again.rows), (1, 1));
    assert!(
        again.reused,
        "a repeated query must hit the statement cache"
    );
    let ids: Vec<[Value; 1]> = (0..ROWS).map(|id| [Value::Integer(id)]).collect();
    group.bench_function("sql_exec_select_one", |b| {
        b.iter(|| {
            n = (n + 1) % ids.len();
            black_box(sql_run(scope, SELECT_ONE, &ids[n]))
        });
    });

    // A query text the cell has not run before: prepared, then admitted to
    // the statement cache, which evicts to stay inside its byte budget.
    let miss = |serial: u64| format!("SELECT v FROM items WHERE id = ?1 AND {serial} = {serial}");
    let fresh = sql_run(scope, &miss(0), &ids[3]);
    assert!(fresh.rows == 1 && !fresh.reused);
    group.bench_function("statement_cache_hit", |b| {
        b.iter(|| black_box(sql_run(scope, SELECT_ONE, &ids[3])));
    });
    let mut serial = 0;
    group.bench_function("statement_cache_miss", |b| {
        b.iter_batched(
            || {
                serial += 1;
                miss(serial)
            },
            |query| black_box(sql_run(scope, &query, &ids[3])),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(ROWS as u64));
    const SCAN: &str = "SELECT id, v FROM items ORDER BY id";
    assert_eq!(sql_run(scope, SCAN, &[]).rows, ROWS as usize);
    group.bench_function("cursor_1000_rows", |b| {
        b.iter(|| black_box(sql_run(scope, SCAN, &[])));
    });

    // Only the COMMIT is timed; BEGIN IMMEDIATE and the 16 upserts inside
    // the transaction are set up first.
    group.throughput(Throughput::Elements(1));
    group.bench_function("transaction_commit_16_writes", |b| {
        b.iter_batched(
            || {
                storage::transaction_control(scope, "start", false, "").unwrap();
                for _ in 0..16 {
                    n = (n + 1) % KEYS;
                    sql_run(scope, UPSERT, &binds[n]);
                }
            },
            |()| storage::transaction_control(scope, "commit", false, "").unwrap(),
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// The fixed cost of a fleet write proof on each side of the wire.
///
/// `follower_append_batch` runs `FollowerStore::append_batch` against a real
/// directory: it encodes the batch file, writes it, fsyncs the file and the
/// leader directory, then removes the previous batch below the truncate
/// watermark. Those are real disk fsyncs (on macOS, `F_FULLFSYNC`), so the
/// result is a property of the disk under the temporary directory; point
/// `TMPDIR` at a RAM disk to take the device out of it.
fn node_log(c: &mut Criterion) {
    let _enter = runtime().enter();
    const ENTRY_BYTES: usize = 4096;
    let mut group = c.benchmark_group("node_log");
    for frames in [1_usize, 64] {
        let fixture = RefCell::new(FollowerFixture::new(ENTRY_BYTES));
        let (batch, last) = fixture.borrow_mut().batch(frames);
        let answers = runtime().block_on(fixture.borrow().store().append_batch(batch));
        assert_eq!(answers.len(), frames);
        assert!(confirmed(&answers, last));
        group.throughput(Throughput::Elements(frames as u64));
        group.bench_function(BenchmarkId::new("follower_append_batch", frames), |b| {
            b.iter_batched(
                || fixture.borrow_mut().batch(frames),
                |(batch, last)| {
                    let answers = runtime().block_on(fixture.borrow().store().append_batch(batch));
                    (answers, last)
                },
                BatchSize::PerIteration,
            );
        });
        let (batch, last) = fixture.borrow_mut().batch(frames);
        assert!(confirmed(
            &runtime().block_on(fixture.borrow().store().append_batch(batch)),
            last
        ));
    }

    // The leader's `post_append_to` body, the `log_append_encode` region.
    let payload: Vec<u8> = (0..ENTRY_BYTES).map(|n| n as u8).collect();
    for entries in [1_u64, 64] {
        let mut req = append_req("bench-leader/g1", 1, 0, 1, &payload);
        for seq in 2..=entries {
            req.entries
                .extend(append_req("bench-leader/g1", 1, 0, seq, &payload).entries);
        }
        let body = encode_append(&req);
        let decoded = decode_append(&body).unwrap();
        assert_eq!(decoded.entries.len() as u64, entries);
        assert_eq!(encode_append(&decoded), body);
        group.throughput(Throughput::Bytes(body.len() as u64));
        group.bench_function(BenchmarkId::new("leader_append_encode", entries), |b| {
            b.iter(|| black_box(encode_append(&req)));
        });
    }

    // One node flush: 64 captured L0 segments into one bucket object.
    let segments: Vec<celld_ltx::bundle::BundleEntry> = (0..64_u64)
        .map(|n| celld_ltx::bundle::BundleEntry {
            cell: format!("Bench:{n}"),
            cell_epoch: 1,
            txid: n + 1,
            bytes: payload.clone(),
        })
        .collect();
    let bundle = celld_ltx::bundle::encode(&segments).unwrap();
    let rows = celld_ltx::bundle::decode_rows(&bundle).unwrap();
    assert_eq!(rows.len(), segments.len());
    assert_eq!(
        celld_ltx::bundle::slice(&bundle, &rows[63]).unwrap(),
        &segments[63].bytes[..]
    );
    group.throughput(Throughput::Bytes(bundle.len() as u64));
    group.bench_function("bundle_encode/64", |b| {
        b.iter(|| black_box(celld_ltx::bundle::encode(&segments).unwrap()));
    });
    group.finish();
}

/// Isolate load, turn overhead and the storage clone codec.
///
/// `worker_load` is `Worker::load_config`: a new isolate, the runtime
/// prelude and harness, then compiling and evaluating the Worker module. The
/// hello case is mostly the fixed isolate cost; the synthetic case adds the
/// compile and top-level evaluation of about 3 MB of JavaScript. Dropping the
/// isolate is outside the timing. The synthetic case takes about 50 ms, so it
/// runs 10 samples to fit the measurement time.
fn js_turn(c: &mut Criterion) {
    let _enter = runtime().enter();
    let mut group = c.benchmark_group("js_turn");
    group.throughput(Throughput::Elements(1));
    let large = large_worker(3 * 1024 * 1024);
    for (name, src, samples) in [
        ("hello", HELLO_WORKER.to_string(), 30),
        ("synthetic_3mb", large, 10),
    ] {
        group.sample_size(samples);
        let config = worker_config(src);
        init_v8();
        drop(Worker::load_config(config.clone()).unwrap());
        group.bench_function(BenchmarkId::new("worker_load", name), |b| {
            b.iter_batched(
                || config.clone(),
                |config| Worker::load_config(config).unwrap(),
                BatchSize::PerIteration,
            );
        });
    }

    group.sample_size(30);

    // A fetch whose handler answers at once: request object, handler call,
    // Response conversion and the end of the event, with no host op.
    let mut turn = TurnFixture::new(HELLO_WORKER);
    assert_eq!(turn.fetch(), (200, 2));
    group.bench_function("empty_fetch_turn", |b| {
        b.iter(|| black_box(turn.fetch()));
    });

    // Each value encodes to within 10% of its nominal size.
    for (name, nominal, expression) in [
        (
            "1kb",
            1_024,
            "({ id: 42, name: 'user-42', email: 'user42@example.com', active: true, \
             roles: ['admin', 'editor'], score: 1234.5, \
             tags: Array.from({ length: 8 }, (_, i) => 'tag-' + i), bio: 'x'.repeat(760) })",
        ),
        (
            "1mb",
            1_048_576,
            "Array.from({ length: 1024 }, (_, i) => ({ id: i, name: 'user-' + i, \
             email: 'user' + i + '@example.com', active: i % 2 === 0, roles: ['reader'], \
             score: i * 1.5, tags: ['a', 'b', 'c'], bio: 'y'.repeat(900) }))",
        ),
    ] {
        let fixture = RefCell::new(CloneFixture::new(expression));
        let encoded = fixture.borrow().encoded_len();
        assert!(
            encoded.abs_diff(nominal) * 10 <= nominal,
            "{name} encodes to {encoded} bytes"
        );
        group.throughput(Throughput::Bytes(encoded as u64));
        group.bench_function(BenchmarkId::new("clone_serialize", name), |b| {
            b.iter(|| black_box(fixture.borrow_mut().serialize()));
        });
        let bytes = fixture.borrow().encoded();
        assert!(fixture.borrow_mut().deserialize(bytes));
        group.bench_function(BenchmarkId::new("clone_deserialize", name), |b| {
            b.iter_batched(
                || fixture.borrow().encoded(),
                |bytes| black_box(fixture.borrow_mut().deserialize(bytes)),
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Per-request peer authentication: HMAC-SHA256 over the canonical request
/// and a SHA-256 of the body on both sides, plus the nonce and the replay
/// cache on the verifier.
///
/// WebSocket frames are encoded by `fastwebsockets` in the binary's
/// `main/websocket.rs`, and the peer tunnel's framing is in
/// `main/peer_tunnel.rs`; neither has an encoder in the library, so neither
/// is here.
fn wire(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire");
    let key = [7_u8; 32];
    let signer = PeerAuth::new(key, "node-a").unwrap();
    let method = axum::http::Method::POST;
    let path = "/peer/log/append";
    for bytes in [256_usize, 65_536] {
        let body: Vec<u8> = (0..bytes).map(|n| n as u8).collect();
        let verifier = RefCell::new(PeerAuth::new(key, "node-b").unwrap());
        let headers = signer
            .signed_headers(method.as_str(), path, &body, "node-b")
            .unwrap();
        verifier
            .borrow()
            .verify(&method, path, &headers, &body, "node-b")
            .unwrap();
        assert!(verifier
            .borrow()
            .verify(&method, path, &headers, &body, "node-b")
            .is_err());
        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_function(BenchmarkId::new("peer_sign", bytes), |b| {
            b.iter(|| {
                black_box(
                    signer
                        .signed_headers(method.as_str(), path, &body, "node-b")
                        .unwrap(),
                )
            });
        });
        // Each verification needs a fresh nonce, so signing is set up
        // outside the timing. The verifier is replaced every 65,536 checks
        // to keep its replay cache near the size a node holds for one
        // clock window rather than letting it grow for the whole run.
        let mut checks = 0_u32;
        group.bench_function(BenchmarkId::new("peer_verify", bytes), |b| {
            b.iter_batched(
                || {
                    checks += 1;
                    if checks.is_multiple_of(65_536) {
                        *verifier.borrow_mut() = PeerAuth::new(key, "node-b").unwrap();
                    }
                    signer
                        .signed_headers(method.as_str(), path, &body, "node-b")
                        .unwrap()
                },
                |headers| {
                    verifier
                        .borrow()
                        .verify(&method, path, &headers, &body, "node-b")
                        .unwrap()
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// The Rust-side pure work of the service cells.
///
/// Queue producer grouping and KV `list` pagination run in the service
/// cells' JavaScript (the harness and `js/services/kv.js`), not in a Rust
/// function, so they belong to the Tier 2 service scenarios. The Queue case
/// here is the batch-selection policy the Queue cell asks the host for on
/// every dispatch.
fn services_pure(c: &mut Criterion) {
    let _enter = runtime().enter();
    let mut group = c.benchmark_group("services_pure");
    group.throughput(Throughput::Elements(1));

    let now = 1_790_000_000_000_i64;
    let rows: Vec<serde_json::Value> = (0..100)
        .map(|seq| {
            serde_json::json!({
                "seq": seq,
                "visibleAt": now - 1_000 + seq,
                "leaseGeneration": "0",
                "leasedUntil": null,
                "purgeOnSettle": false,
            })
        })
        .collect();
    let request = serde_json::json!({
        "op": "batch",
        "now": now,
        "maxBatchSize": 10,
        "rows": rows,
    });
    let plan = queue_policy(&request);
    assert_eq!(plan["leases"].as_array().unwrap().len(), 10);
    group.bench_function("queue_batch_policy/100_rows", |b| {
        b.iter(|| black_box(queue_policy(&request)));
    });

    for (name, expression) in [
        ("every_5_minutes", "*/5 * * * *"),
        ("weekdays_0930", "30 9 * * MON-FRI"),
        ("leap_day", "0 0 29 2 *"),
    ] {
        let cron = celld_logic::cron::parse(expression).unwrap();
        let next = celld_logic::cron::next_after(&cron, now).unwrap();
        assert!(next > now && celld_logic::cron::matches(&cron, next));
        group.bench_function(BenchmarkId::new("cron_next", name), |b| {
            b.iter(|| black_box(celld_logic::cron::next_after(&cron, black_box(now))));
        });
    }

    // HEAD requests, so no asset body is read: path decoding, the
    // `_redirects` scan, the index lookup and the `_headers` rules.
    let assets = runtime().block_on(AssetFixture::new(1_000, 200, 50, 50));
    let mut request_headers = axum::http::HeaderMap::new();
    request_headers.insert("host", "acme.example.com".parse().unwrap());
    let respond = |path: &'static str| {
        runtime()
            .block_on(
                assets
                    .resolver
                    .ingress_response(path, None, true, &request_headers),
            )
            .unwrap()
            .expect("the asset layer answers")
    };
    let served = respond("/assets/app-500.js");
    assert_eq!(served.status(), 200);
    assert_eq!(
        served.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    assert_eq!(served.headers()["x-zone"], "example.com");
    group.bench_function("assets/asset_through_250_redirects_50_headers", |b| {
        b.iter(|| black_box(respond("/assets/app-500.js")));
    });
    let redirected = respond("/blog-49/2026/hello");
    assert_eq!(redirected.status(), 301);
    assert_eq!(redirected.headers()["location"], "/posts/2026/hello");
    group.bench_function("assets/redirect_on_rule_250", |b| {
        b.iter(|| black_box(respond("/blog-49/2026/hello")));
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = storage_ops, node_log, js_turn, wire, services_pure
}
criterion_main!(benches);
