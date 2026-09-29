use celld_export_format::{split, Consumer, Reassembler, Record, Split};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::time::Duration;
mod support;

fn json(c: &mut Criterion) {
    let mut g = c.benchmark_group("export_json");
    for rows in [1, 128, 1024] {
        let record = support::rows_record(rows, 128);
        let bytes = record.to_json();
        assert_eq!(Record::from_json(&bytes).unwrap(), record);
        g.throughput(Throughput::Bytes(bytes.len() as u64));
        g.bench_with_input(BenchmarkId::new("encode", rows), &record, |b, r| {
            b.iter(|| black_box(r).to_json())
        });
        g.bench_with_input(BenchmarkId::new("decode", rows), &bytes, |b, bytes| {
            b.iter(|| Record::from_json(black_box(bytes)).unwrap())
        });
    }
    g.finish();
}

fn fragments(c: &mut Criterion) {
    let mut g = c.benchmark_group("export_fragments");
    for payload in [128, 4096] {
        let record = support::rows_record(1024, payload);
        let Split::Fragments(fragments) = split(record.clone(), 64 * 1024) else {
            panic!("must fragment, not bulk")
        };
        assert!(fragments.len() > 1);
        let reassemble = |fragments: Vec<Record>| {
            let mut join = Reassembler::new();
            let mut whole = None;
            for fragment in fragments {
                if let Some(r) = join.push(fragment).unwrap() {
                    whole = Some(r);
                }
            }
            whole.expect("complete record")
        };
        assert_eq!(reassemble(fragments.clone()), record);
        assert!(fragments.iter().all(|r| r.to_json().len() <= 64 * 1024));
        g.throughput(Throughput::Bytes(record.to_json().len() as u64));
        g.bench_function(BenchmarkId::new("split_64k", payload), |b| {
            b.iter_batched(
                || record.clone(),
                |r| split(black_box(r), 64 * 1024),
                BatchSize::PerIteration,
            )
        });
        g.bench_function(BenchmarkId::new("reassemble", payload), |b| {
            b.iter_batched(
                || fragments.clone(),
                |rs| reassemble(black_box(rs)),
                BatchSize::PerIteration,
            )
        });
    }
    g.finish();
}

fn replay(c: &mut Criterion) {
    let mut g = c.benchmark_group("export_consumer");
    for (commits, streams, repair) in [
        (128, 1, false),
        (1024, 1, false),
        (1024, 32, false),
        (1024, 32, true),
    ] {
        let records = support::history(commits, streams, repair);
        support::check_history(&records, commits, streams, repair);
        let consumer = support::consumer(&records);
        let case = format!("{commits}_commits_{streams}_streams_repair_{repair}");
        g.throughput(Throughput::Elements(records.len() as u64));
        g.bench_function(BenchmarkId::new("ingest", &case), |b| {
            b.iter_batched(
                || records.clone(),
                |rs| {
                    let mut consumer = Consumer::new();
                    consumer.ingest_all(black_box(rs)).unwrap();
                    consumer
                },
                BatchSize::PerIteration,
            )
        });
        g.bench_function(BenchmarkId::new("derive", &case), |b| {
            b.iter(|| black_box(&consumer).state())
        });
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = json, fragments, replay
}
criterion_main!(benches);
