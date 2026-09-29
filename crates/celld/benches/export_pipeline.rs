//! Benchmarks the production capture, encoding, delivery cache and audit paths.
use celld::export_audit::{BucketConsumer, ConsumerView};
use celld::export_bench::{AuditFixture, CaptureFixture};
use celld::export_live::bench::DeliveryCache;
use celld::export_sink::{decode_records, encode_records};
use celld_export_format::Position;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::cell::RefCell;
use std::hint::black_box;
use std::time::Duration;

#[path = "../../export-format/benches/support/mod.rs"]
mod support;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn capture(c: &mut Criterion) {
    let mut group = c.benchmark_group("capture");
    for (rows, payload) in [(1, 128), (64, 128), (512, 128), (64, 4096)] {
        let id = format!("{rows}_rows_{payload}_bytes");
        group.throughput(Throughput::Elements(rows as u64));
        let plain = CaptureFixture::new(rows, payload, 8 * 1024 * 1024, false);
        group.bench_function(BenchmarkId::new("write_plain", &id), |b| {
            b.iter(|| plain.write());
        });
        let mut enabled = CaptureFixture::new(rows, payload, 8 * 1024 * 1024, true);
        enabled.write();
        let batch = enabled.checkpoint();
        assert_eq!(batch.rows(), rows);
        assert_eq!(batch.bulk(), 0);
        group.bench_function(BenchmarkId::new("write_and_checkpoint", &id), |b| {
            b.iter(|| {
                enabled.write();
                black_box(enabled.checkpoint())
            });
        });
        let enabled = RefCell::new(enabled);
        group.bench_function(BenchmarkId::new("checkpoint", &id), |b| {
            b.iter_batched(
                || enabled.borrow().write(),
                |()| black_box(enabled.borrow_mut().checkpoint()),
                BatchSize::PerIteration,
            );
        });
    }
    let mut large = CaptureFixture::new(64, 32768, 65536, true);
    large.write();
    let batch = large.checkpoint();
    assert_eq!(batch.rows(), 0);
    assert_eq!(batch.bulk(), 1);
    group.throughput(Throughput::Elements(64));
    group.bench_function("write_and_checkpoint/oversize_64_rows_32768_bytes", |b| {
        b.iter(|| {
            large.write();
            black_box(large.checkpoint())
        });
    });
    group.finish();
}

fn parquet(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet");
    for count in [64, 1024] {
        let records: Vec<_> = (0..count)
            .map(|i| {
                let mut record = support::rows_record(1, 128);
                record.envelope = support::envelope(i % 32, (i / 32 + 1) as u64);
                record
            })
            .collect();
        let encoded = encode_records(records.clone()).unwrap();
        assert_eq!(decode_records(encoded.clone()).unwrap(), records);
        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(BenchmarkId::new("encode", count), |b| {
            b.iter_batched(
                || records.clone(),
                |records| black_box(encode_records(records).unwrap()),
                BatchSize::PerIteration,
            );
        });
        group.bench_function(BenchmarkId::new("decode", count), |b| {
            b.iter_batched(
                || encoded.clone(),
                |bytes| black_box(decode_records(bytes).unwrap()),
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn delivery(c: &mut Criterion) {
    let mut group = c.benchmark_group("delivery_cache");
    group.throughput(Throughput::Elements(64));
    for (history, active) in [(64, 64), (4096, 64), (1024, 1024)] {
        let mut cache = DeliveryCache::new(history, active);
        group.bench_function(format!("{history}_historical_{active}_active"), |b| {
            b.iter(|| black_box(cache.advance(64)));
        });
    }
    group.finish();
}

fn audit(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("audit_cache");
    let records = support::history(64, 1, false);
    support::check_history(&records, 64, 1, false);
    for objects in [16, 128] {
        let fixture = runtime.block_on(AuditFixture::new(objects, &records));
        let warm_path = fixture.directory.path().join("warm.sqlite");
        let load = |path: &std::path::Path| {
            runtime
                .block_on(BucketConsumer::load_cached(
                    fixture.bucket.clone(),
                    Some(path),
                    "benchmark",
                    None,
                ))
                .unwrap()
        };
        let consumer = load(&warm_path);
        let streams = runtime.block_on(consumer.streams()).unwrap();
        assert_eq!(streams.len(), objects);
        let at = Position::new(1, 64, 64);
        let state = runtime
            .block_on(consumer.state_at(&streams[0].id, at))
            .unwrap()
            .unwrap();
        assert_eq!(state.certified_head(), Some(at));
        assert_eq!(state.table("items").unwrap().rows.len(), 64);
        group.throughput(Throughput::Elements(objects as u64));
        group.bench_function(BenchmarkId::new("cold_refresh", objects), |b| {
            b.iter_batched(
                || tempfile::NamedTempFile::new_in(fixture.directory.path()).unwrap(),
                |file| {
                    let consumer = load(file.path());
                    // Drop the SQLite connection before its backing file.
                    black_box(&consumer);
                    drop(consumer);
                    file
                },
                BatchSize::PerIteration,
            );
        });
        group.bench_function(BenchmarkId::new("warm_refresh", objects), |b| {
            b.iter(|| black_box(load(&warm_path)));
        });
        group.bench_function(BenchmarkId::new("stream_summaries", objects), |b| {
            b.iter(|| black_box(runtime.block_on(consumer.streams()).unwrap()));
        });
        group.throughput(Throughput::Elements(records.len() as u64));
        group.bench_function(BenchmarkId::new("one_cell_state", objects), |b| {
            b.iter(|| {
                black_box(
                    runtime
                        .block_on(consumer.state_at(&streams[0].id, at))
                        .unwrap(),
                )
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = capture, parquet, delivery, audit
}
criterion_main!(benches);
