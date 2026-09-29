// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::bucket::StorageBackend;
use async_trait::async_trait;
use celld_export_format::BulkBody;
use celld_export_format::ColumnDef;
use celld_export_format::Consumer;
use celld_export_format::DeletedBody;
use celld_export_format::GapBody;
use celld_export_format::LinkBody;
use celld_export_format::LinkMode;
use celld_export_format::Op;
use celld_export_format::RecoveredBody;
use celld_export_format::RowChange;
use celld_export_format::RowsBody;
use celld_export_format::SchemaBody;
use celld_export_format::SnapshotBody;
use celld_export_format::SnapshotEndBody;
use celld_export_format::SnapshotScope;
use celld_export_format::TableGen;
use celld_export_format::TableRows;
use celld_export_format::Value;
use celld_export_format::WatermarkBody;
use futures_util::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::GetOptions;
use object_store::GetResult;
use object_store::ListResult;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutMultipartOptions;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;
use std::sync::atomic::AtomicU32;

/// 2026-09-29T12:34:56Z.
const NOW_US: i64 = 1_790_685_296_000_000;
const DAY_US: i64 = 86_400 * 1_000_000;

fn stream(cell: &str) -> StreamId {
    StreamId {
        script: "app".into(),
        class: "Counter".into(),
        cell: cell.into(),
        facet: None,
        incarnation: 7,
    }
}

fn envelope(cell: &str, txid: u64) -> Envelope {
    Envelope {
        stream: stream(cell),
        cell_name: None,
        position: Position::new(1, txid, txid),
        committed_at: 1_790_685_296_000 + txid as i64,
        node: "node-1".into(),
        origin: Origin::Live,
        fragment: 1,
        fragments: 1,
    }
}

fn table_rows(rows: Vec<RowChange>) -> TableRows {
    TableRows {
        table: "items".into(),
        generation: 1,
        columns: vec!["id".into(), "name".into(), "score".into(), "data".into()],
        key_columns: vec!["id".into()],
        rows,
    }
}

fn insert(id: i64, name: &str) -> RowChange {
    RowChange(
        Op::Insert,
        vec![Value::Integer(id)],
        vec![
            Value::Integer(id),
            Value::Text(name.into()),
            Value::Real(0.5),
            Value::Blob(vec![0, 1, 2]),
        ],
    )
}

fn schema_body() -> Body {
    Body::Schema(SchemaBody {
        table: "items".into(),
        generation: 1,
        sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB)".into(),
        columns: ["id", "name", "score", "data"]
            .iter()
            .enumerate()
            .map(|(i, name)| ColumnDef {
                name: (*name).into(),
                decl_type: ["INTEGER", "TEXT", "REAL", "BLOB"][i].into(),
                pk: u32::from(i == 0),
                not_null: false,
                generated: false,
            })
            .collect(),
        dropped: false,
        renamed_from: None,
        unsupported: false,
    })
}

fn rows_record(cell: &str, txid: u64, rows: Vec<RowChange>) -> Record {
    Record {
        envelope: envelope(cell, txid),
        body: Body::Rows(RowsBody {
            data: table_rows(rows),
        }),
    }
}

/// One record of every kind, with the envelope's optional fields and
/// extreme values exercised.
fn every_kind() -> Vec<Record> {
    let position = Position::new(3, 40, 41);
    let bodies = vec![
        Body::Rows(RowsBody {
            data: table_rows(vec![
                insert(1, "one"),
                RowChange(Op::Delete, vec![Value::Integer(2)], vec![Value::Null; 4]),
            ]),
        }),
        Body::Snapshot(SnapshotBody {
            snapshot_id: "snap-1".into(),
            data: table_rows(vec![insert(3, "three")]),
        }),
        Body::SnapshotEnd(SnapshotEndBody {
            snapshot_id: "snap-1".into(),
            scope: SnapshotScope::Tables,
            tables: vec![TableGen {
                table: "items".into(),
                generation: 1,
            }],
            records: 1,
        }),
        schema_body(),
        Body::Link(LinkBody {
            start_txid: 1,
            prev_epoch: Some(2),
            prev_txid: Some(99),
            mode: LinkMode::Clone,
        }),
        Body::Recovered(RecoveredBody {
            session: "s".into(),
            head: position,
        }),
        Body::Deleted(DeletedBody {
            facet: Some("child".into()),
            incarnation: Some(u64::MAX),
            subtree: true,
        }),
        Body::Watermark(WatermarkBody {
            from: None,
            through: position,
            commits: 2,
            records: 5,
        }),
        Body::Bulk(BulkBody {
            tables: vec![TableGen {
                table: "items".into(),
                generation: 2,
            }],
        }),
        Body::Gap(GapBody {
            from: Position::new(3, 1, 1),
            to: position,
            reason: "queue_overflow".into(),
        }),
    ];
    assert_eq!(bodies.len(), Kind::ALL.len());
    bodies
        .into_iter()
        .enumerate()
        .map(|(i, body)| {
            let mut envelope = envelope("cell-a", 40);
            envelope.position = position;
            if i % 2 == 0 {
                envelope.cell_name = Some("counter-a".into());
                envelope.stream.facet = Some("root/child".into());
            }
            envelope.stream.incarnation = u64::MAX - i as u64;
            envelope.position.txid = u64::MAX;
            envelope.origin = [Origin::Live, Origin::Snapshot, Origin::Repair][i % 3];
            envelope.fragment = 2;
            envelope.fragments = u32::MAX;
            Record { envelope, body }
        })
        .collect()
}

#[test]
fn records_of_every_kind_round_trip_through_parquet() {
    let records = every_kind();
    let bytes = encode_records(records.clone()).unwrap();
    assert_eq!(decode_records(bytes).unwrap(), records);
}

#[test]
fn columns_are_the_envelope_fields_and_the_body() {
    use parquet::file::reader::FileReader as _;
    use parquet::file::reader::SerializedFileReader;

    let bytes = encode_records(vec![rows_record("c", 1, vec![insert(1, "a")])]).unwrap();
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
    let schema = reader.metadata().file_metadata().schema_descr_ptr();
    let names: Vec<&str> = schema.columns().iter().map(|c| c.name()).collect();
    assert_eq!(
        names,
        [
            "kind",
            "script",
            "class",
            "cell",
            "cell_name",
            "facet",
            "incarnation",
            "epoch",
            "txid",
            "commit",
            "committed_at",
            "node",
            "origin",
            "fragment",
            "fragments",
            "body",
        ]
    );
    let row_group = reader.metadata().row_group(0);
    let cell = names.iter().position(|n| *n == "cell").unwrap();
    assert!(row_group.column(cell).bloom_filter_offset().is_some());
}

#[test]
fn body_is_the_kind_specific_json_with_its_kind() {
    let record = rows_record("c", 1, vec![insert(1, "a")]);
    let row = Row::new(record).unwrap();
    let body: serde_json::Value = serde_json::from_str(&row.body).unwrap();
    assert_eq!(body["kind"], "rows");
    assert_eq!(body["table"], "items");
    assert!(body.get("cell").is_none(), "envelope stays out of the body");
}

#[test]
fn decode_rejects_a_body_whose_kind_disagrees_with_the_column() {
    let mut row = Row::new(rows_record("c", 1, vec![])).unwrap();
    row.kind = Kind::Gap;
    let bytes = encode_rows(&[row]).unwrap();
    let error = decode_records(bytes).unwrap_err();
    assert!(error.to_string().contains("disagrees"), "{error:#}");
}

/// An in-memory bucket. Not the development store: that runs its I/O on
/// the process's first runtime, which a `#[tokio::test]` does not outlive.
fn memory_bucket() -> Bucket {
    FlakyStore::bucket(0).1
}

/// Settings that never flush on their own, so a test decides when.
fn manual() -> BucketSinkConfig {
    BucketSinkConfig {
        flush: Duration::from_secs(3600),
        flush_bytes: u64::MAX,
        retention: Retention::None,
        put_attempts: 3,
        retry_backoff: Duration::from_millis(1),
    }
}

fn submitted(seqs: std::ops::Range<u64>) -> Vec<SinkRecord> {
    seqs.map(|seq| SinkRecord {
        seq,
        record: rows_record("cell-a", seq, vec![insert(seq as i64, "x")]),
    })
    .collect()
}

async fn next(outcomes: &mut mpsc::UnboundedReceiver<Outcome>) -> Outcome {
    tokio::time::timeout(Duration::from_secs(10), outcomes.recv())
        .await
        .expect("an outcome in time")
        .expect("the sink is running")
}

async fn read_object(bucket: &Bucket, key: &str) -> Vec<Record> {
    let (bytes, _) = bucket.get(key).await.unwrap().expect("object written");
    decode_records(bytes.to_vec()).unwrap()
}

fn object_of(outcome: &Outcome) -> Arc<str> {
    match &outcome.results[0].1 {
        Delivery::Acknowledged { object } => object.clone(),
        other => panic!("expected an acknowledgement, got {other:?}"),
    }
}

#[tokio::test]
async fn flush_writes_one_object_and_acknowledges_every_record_in_order() {
    let bucket = memory_bucket();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sink = BucketSink::start(bucket.clone(), "node-1".into(), manual(), tx);
    let records = submitted(0..3);
    sink.submit(records.clone()).unwrap();
    sink.submit(submitted(3..5)).unwrap();
    assert!(sink.buffered_bytes() > 0);
    sink.flush();

    let outcome = next(&mut outcomes).await;
    assert_eq!(outcome.sink, "bucket");
    let object = object_of(&outcome);
    assert!(
        object.starts_with("export/changes/node-1/"),
        "unexpected key {object}"
    );
    assert!(object.ends_with(".parquet"));
    let expected: Vec<(u64, Delivery)> = (0..5)
        .map(|seq| {
            (
                seq,
                Delivery::Acknowledged {
                    object: object.clone(),
                },
            )
        })
        .collect();
    assert_eq!(outcome.results, expected);
    assert_eq!(sink.buffered_bytes(), 0);

    let written = read_object(&bucket, &object).await;
    let all: Vec<Record> = submitted(0..5).into_iter().map(|r| r.record).collect();
    assert_eq!(written, all);
    let (_, schema) = bucket
        .head_with_meta(&object, "celld-schema")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.as_deref(), Some(SCHEMA_VERSION));
    let (_, retention) = bucket
        .head_with_meta(&object, "celld-retention")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retention.as_deref(), Some("none"));
    assert_eq!(bucket.list(CHANGES_PREFIX).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_flush_with_nothing_buffered_writes_nothing() {
    let bucket = memory_bucket();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sink = BucketSink::start(bucket.clone(), "node-1".into(), manual(), tx);
    sink.flush();
    sink.submit(Vec::new()).unwrap();
    sink.close().await;
    assert!(outcomes.recv().await.is_none());
    assert!(bucket.list(CHANGES_PREFIX).await.unwrap().is_empty());
}

#[tokio::test]
async fn the_byte_threshold_flushes_early() {
    let bucket = memory_bucket();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let config = BucketSinkConfig {
        flush_bytes: 1,
        ..manual()
    };
    let sink = BucketSink::start(bucket.clone(), "node-1".into(), config, tx);
    sink.submit(submitted(0..1)).unwrap();
    sink.submit(submitted(1..2)).unwrap();
    let first = next(&mut outcomes).await;
    let second = next(&mut outcomes).await;
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.results[0].0, 0);
    assert_eq!(second.results[0].0, 1);
    assert_ne!(object_of(&first), object_of(&second));
    assert_eq!(bucket.list(CHANGES_PREFIX).await.unwrap().len(), 2);
}

#[tokio::test]
async fn the_interval_flushes_without_being_asked() {
    let bucket = memory_bucket();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let config = BucketSinkConfig {
        flush: Duration::from_millis(20),
        ..manual()
    };
    let sink = BucketSink::start(bucket.clone(), "node-1".into(), config, tx);
    sink.submit(submitted(0..2)).unwrap();
    let outcome = next(&mut outcomes).await;
    assert_eq!(outcome.results.len(), 2);
    assert!(outcome.results.iter().all(|(_, d)| d.is_acknowledged()));
}

#[tokio::test]
async fn close_writes_what_is_buffered_then_refuses_records() {
    let bucket = memory_bucket();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sink = BucketSink::start(bucket.clone(), "node-1".into(), manual(), tx);
    sink.submit(submitted(0..2)).unwrap();
    sink.close().await;
    let outcome = next(&mut outcomes).await;
    assert_eq!(outcome.results.len(), 2);
    assert!(outcome.results.iter().all(|(_, d)| d.is_acknowledged()));
    assert_eq!(sink.submit(submitted(2..3)), Err(Closed));
    assert_eq!(sink.buffered_bytes(), 0);
    // A second close finds the sink already stopped.
    sink.close().await;
}

#[tokio::test]
async fn records_submitted_after_close_is_called_are_refused() {
    let bucket = memory_bucket();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sink = BucketSink::start(bucket, "node-1".into(), manual(), tx);
    sink.submit(submitted(0..1)).unwrap();
    let closing = sink.close();
    assert_eq!(sink.submit(submitted(1..2)), Err(Closed));
    closing.await;
    let outcome = outcomes
        .try_recv()
        .expect("outcome sent before close resolved");
    assert_eq!(
        outcome
            .results
            .iter()
            .map(|(seq, _)| *seq)
            .collect::<Vec<_>>(),
        [0]
    );
    assert!(outcomes.try_recv().is_err());
}

#[tokio::test]
async fn every_close_waits_for_the_final_write() {
    let (_store, bucket) = FlakyStore::bucket(1);
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let config = BucketSinkConfig {
        retry_backoff: Duration::from_millis(50),
        ..manual()
    };
    let sink = BucketSink::start(bucket, "node-1".into(), config, tx);
    sink.submit(submitted(0..1)).unwrap();
    let first = sink.close();
    sink.close().await;
    let outcome = outcomes
        .try_recv()
        .expect("the second close resolved after the outcome");
    assert!(outcome.results[0].1.is_acknowledged());
    first.await;
    // A close after the sink has stopped resolves at once.
    sink.close().await;
}

#[tokio::test]
async fn one_sink_is_usable_as_a_trait_object() {
    let bucket = memory_bucket();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sinks: Vec<Box<dyn ExportSink>> = vec![Box::new(BucketSink::start(
        bucket,
        "node-1".into(),
        manual(),
        tx,
    ))];
    for sink in &sinks {
        sink.submit(submitted(0..1)).unwrap();
        sink.flush();
    }
    assert_eq!(next(&mut outcomes).await.sink, sinks[0].name());
}

/// An in-memory store whose next `fail_puts` puts fail.
#[derive(Debug)]
struct FlakyStore {
    inner: InMemory,
    fail_puts: AtomicU32,
    puts: AtomicU32,
}

impl FlakyStore {
    fn bucket(fail_puts: u32) -> (Arc<FlakyStore>, Bucket) {
        let store = Arc::new(FlakyStore {
            inner: InMemory::new(),
            fail_puts: AtomicU32::new(fail_puts),
            puts: AtomicU32::new(0),
        });
        let bucket = Bucket::with_stores(
            store.clone(),
            store.clone(),
            StorageBackend::S3,
            "flaky".into(),
            String::new(),
        );
        (store, bucket)
    }
}

impl std::fmt::Display for FlakyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FlakyStore")
    }
}

#[async_trait]
impl ObjectStore for FlakyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let failing = self
            .fail_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if failing {
            return Err(object_store::Error::Generic {
                store: "FlakyStore",
                source: "injected put failure".into(),
            });
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[tokio::test]
async fn a_failed_put_is_retried_before_acknowledging() {
    let (store, bucket) = FlakyStore::bucket(2);
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sink = BucketSink::start(bucket.clone(), "node-1".into(), manual(), tx);
    sink.submit(submitted(0..2)).unwrap();
    sink.flush();
    let outcome = next(&mut outcomes).await;
    assert!(outcome.results.iter().all(|(_, d)| d.is_acknowledged()));
    assert_eq!(store.puts.load(Ordering::SeqCst), 3);
    let written = read_object(&bucket, &object_of(&outcome)).await;
    assert_eq!(written.len(), 2);
}

#[tokio::test]
async fn records_are_dropped_after_the_last_attempt_and_the_sink_carries_on() {
    let (store, bucket) = FlakyStore::bucket(3);
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sink = BucketSink::start(bucket.clone(), "node-1".into(), manual(), tx);
    sink.submit(submitted(0..2)).unwrap();
    sink.flush();
    let outcome = next(&mut outcomes).await;
    assert_eq!(
        outcome
            .results
            .iter()
            .map(|(seq, _)| *seq)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    for (_, delivery) in &outcome.results {
        let Delivery::Dropped { reason } = delivery else {
            panic!("expected a drop, got {delivery:?}");
        };
        assert!(reason.contains("after 3 attempts"), "{reason}");
    }
    assert_eq!(store.puts.load(Ordering::SeqCst), 3);
    assert_eq!(sink.buffered_bytes(), 0);
    assert!(bucket.list(CHANGES_PREFIX).await.unwrap().is_empty());

    // The next batch is independent of the dropped one.
    sink.submit(submitted(2..3)).unwrap();
    sink.flush();
    let outcome = next(&mut outcomes).await;
    assert_eq!(outcome.results[0].0, 2);
    assert!(outcome.results[0].1.is_acknowledged());
}

#[tokio::test]
async fn records_submitted_during_a_retry_wait_keep_their_order() {
    let (_store, bucket) = FlakyStore::bucket(1);
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let config = BucketSinkConfig {
        retry_backoff: Duration::from_millis(100),
        ..manual()
    };
    let sink = BucketSink::start(bucket, "node-1".into(), config, tx);
    sink.submit(submitted(0..1)).unwrap();
    sink.flush();
    sink.submit(submitted(1..2)).unwrap();
    sink.flush();
    let first = next(&mut outcomes).await;
    let second = next(&mut outcomes).await;
    let seqs: Vec<u64> = first
        .results
        .iter()
        .chain(&second.results)
        .map(|(seq, _)| *seq)
        .collect();
    assert_eq!(seqs, [0, 1]);
}

#[test]
fn retention_sweeps_the_export_layout_only() {
    let cutoff = parquet_batch::cutoff_date(NOW_US, 30);
    let old = parquet_batch::object_key(CHANGES_PREFIX, "n", NOW_US - 40 * DAY_US);
    let fresh = parquet_batch::object_key(CHANGES_PREFIX, "n", NOW_US);
    let telemetry = parquet_batch::object_key("telemetry/traces", "n", NOW_US - 40 * DAY_US);
    assert!(parquet_batch::expired(&old, CHANGES_PREFIX, cutoff));
    assert!(!parquet_batch::expired(&fresh, CHANGES_PREFIX, cutoff));
    assert!(!parquet_batch::expired(&telemetry, CHANGES_PREFIX, cutoff));
}

#[test]
fn retention_defaults_to_none() {
    let config = BucketSinkConfig::default();
    assert_eq!(config.retention, Retention::None);
    assert_eq!(config.flush, Duration::from_secs(10));
    assert_eq!(config.flush_bytes, 8 * 1024 * 1024);
    assert_eq!(Retention::Days(30).label(), "30d");
}

#[tokio::test]
async fn the_reference_consumer_applies_what_the_sink_wrote() {
    let bucket = memory_bucket();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sink = BucketSink::start(bucket.clone(), "node-1".into(), manual(), tx);
    let schema = Record {
        envelope: envelope("cell-a", 1),
        body: schema_body(),
    };
    let records = [
        schema,
        rows_record("cell-a", 1, vec![insert(1, "one"), insert(2, "two")]),
        rows_record(
            "cell-a",
            2,
            vec![RowChange(
                Op::Delete,
                vec![Value::Integer(1)],
                insert(1, "one").2,
            )],
        ),
    ];
    sink.submit(
        records
            .iter()
            .cloned()
            .enumerate()
            .map(|(seq, record)| SinkRecord {
                seq: seq as u64,
                record,
            })
            .collect(),
    )
    .unwrap();
    sink.flush();
    let object = object_of(&next(&mut outcomes).await);

    let mut consumer = Consumer::new();
    consumer
        .ingest_all(read_object(&bucket, &object).await)
        .unwrap();
    let state = consumer.stream(&stream("cell-a")).expect("stream applied");
    let items = state.table("items").expect("table applied");
    assert_eq!(
        items.rows.keys().cloned().collect::<Vec<_>>(),
        [vec![Value::Integer(2)]]
    );
}
