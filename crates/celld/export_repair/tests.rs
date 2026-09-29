use super::*;
use crate::bucket::StorageBackend;
use crate::export_sink::{decode_records, BucketSink, BucketSinkConfig};
use celld_export_format::consumer::Consumer;
use celld_export_format::{Kind, Op, RowsBody, Value, WatermarkBody};
use celld_ltx::client::object_store::{ObjectStoreClient, ObjectStoreConfig};
use celld_ltx::client::ReplicaClient as _;
use celld_ltx::{ltx, TXID};
use object_store::memory::InMemory;
use std::time::Duration;

const SCOPE: &str = "Cart:one";
const SCRIPT: &str = "shop";

fn bucket(name: &str) -> Bucket {
    let store = Arc::new(InMemory::new());
    Bucket::with_stores(
        store.clone(),
        store,
        StorageBackend::S3,
        name.into(),
        "fleet/".into(),
    )
}

/// The cell's tables at one point: a keyed table, a rowid-only table, a
/// `WITHOUT ROWID` table with a composite key, a denied table, and the
/// internal tables capture never exports.
const SCHEMA: &str = "
    PRAGMA journal_mode=WAL;
    CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL, img BLOB,
                        label TEXT GENERATED ALWAYS AS (upper(name)) VIRTUAL);
    CREATE TABLE notes (body TEXT);
    CREATE TABLE pairs (a TEXT, b INTEGER, v, PRIMARY KEY (b, a)) WITHOUT ROWID;
    CREATE TABLE secrets (k TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE _cf_METADATA (scope TEXT PRIMARY KEY, actor_name TEXT);
    CREATE TABLE __queue_messages (id INTEGER PRIMARY KEY);
    INSERT INTO _cf_METADATA VALUES ('Cart:one', 'one');
    INSERT INTO secrets VALUES ('pin', '1234');
";

/// A whole-database image as an LTX file over `min..=max`.
fn image(min: u64, max: u64, sql: &str) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(sql).unwrap();
    drop(db);
    let bytes = std::fs::read(&path).unwrap();
    let page_size = u32::from(u16::from_be_bytes([bytes[16], bytes[17]]));
    let pages: Vec<_> = bytes
        .chunks_exact(page_size as usize)
        .enumerate()
        .map(|(i, p)| (i as u32 + 1, p.to_vec()))
        .collect();
    let checksum = pages.iter().fold(celld_ltx::CHECKSUM_FLAG, |sum, (n, p)| {
        sum ^ (ltx::checksum_page(*n, p) & !celld_ltx::CHECKSUM_FLAG)
    });
    let header = ltx::Header {
        version: ltx::VERSION,
        page_size,
        commit: pages.len() as u32,
        min_txid: TXID(min),
        max_txid: TXID(max),
        pre_apply_checksum: if min == 1 {
            0
        } else {
            celld_ltx::CHECKSUM_FLAG | 1
        },
        ..Default::default()
    };
    ltx::encode_file(&header, &pages, checksum).unwrap()
}

async fn put(bucket: &Bucket, scope: &str, epoch: u64, min: u64, max: u64, sql: &str) {
    let config = ObjectStoreConfig {
        path: format!("{}cells/{scope}/ltx/e{epoch}", bucket.prefix),
        ..Default::default()
    };
    ObjectStoreClient::with_store(config, bucket.store.clone())
        .write_ltx_file(0, TXID(min), TXID(max), &image(min, max, sql))
        .await
        .unwrap();
}

fn settings(max_record_bytes: usize) -> Settings {
    Settings {
        node: "repair-test".to_string(),
        max_record_bytes,
        denied_tables: [("Cart".to_string(), "secrets".to_string())].into(),
        concurrency: 2,
        buffer_bytes: 1 << 20,
    }
}

fn job(target: Target) -> Job {
    Job {
        stream: root_stream(SCRIPT, SCOPE).unwrap(),
        target,
        reasons: ["gap".to_string()].into(),
        pin_incarnation: false,
    }
}

/// Run `jobs` into a fresh destination bucket and return the reports and
/// every record written, decoded from the Parquet objects.
async fn snapshot(
    source: &Bucket,
    jobs: Vec<Job>,
    settings: &Settings,
) -> (Vec<Report>, Vec<Record>) {
    let destination = bucket("export");
    let (tx, rx) = mpsc::unbounded_channel();
    let sink = BucketSink::start(
        destination.clone(),
        settings.node.clone(),
        BucketSinkConfig {
            flush: Duration::from_millis(50),
            flush_bytes: 64 << 10,
            ..BucketSinkConfig::default()
        },
        tx,
    );
    let reports = run(source, Arc::new(sink), rx, jobs, settings, None, |_| {}).await;
    let mut records = Vec::new();
    for object in destination.list("export/changes/").await.unwrap() {
        let key = object.location.to_string();
        assert!(
            key.starts_with(&format!("export/changes/{}/", settings.node)),
            "{key}"
        );
        let (bytes, _) = destination.get(&key).await.unwrap().unwrap();
        records.extend(decode_records(bytes.to_vec()).unwrap());
    }
    (reports, records)
}

/// The cell at txid 4: the rows the snapshot must carry.
fn state_at_4() -> String {
    format!(
        "{SCHEMA}
        INSERT INTO items (id, name, price, img) VALUES (1, 'apple', 1.5, x'00ff'), (2, 'pear', NULL, NULL);
        INSERT INTO notes (rowid, body) VALUES (7, 'hello'), (9, NULL);
        INSERT INTO pairs VALUES ('x', 1, 'one'), ('y', 1, 2.5), ('x', 2, NULL);"
    )
}

async fn source() -> Bucket {
    let source = bucket("fleet");
    put(&source, SCOPE, 3, 1, 2, SCHEMA).await;
    put(&source, SCOPE, 3, 3, 4, &state_at_4()).await;
    source
}

fn table(
    state: &celld_export_format::consumer::StreamState,
    name: &str,
) -> Vec<(Vec<Value>, Vec<Value>)> {
    state
        .table(name)
        .map(|t| t.rows.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

fn live(
    position: Position,
    table: &str,
    key_columns: &[&str],
    columns: &[&str],
    rows: Vec<RowChange>,
) -> Record {
    Record {
        envelope: Envelope {
            stream: root_stream(SCRIPT, SCOPE).unwrap(),
            cell_name: None,
            position,
            committed_at: 0,
            node: "n1".to_string(),
            origin: Origin::Live,
            fragment: 1,
            fragments: 1,
        },
        body: Body::Rows(RowsBody {
            data: TableRows {
                table: table.to_string(),
                generation: FIRST_GENERATION,
                columns: columns.iter().map(|c| c.to_string()).collect(),
                key_columns: key_columns.iter().map(|c| c.to_string()).collect(),
                rows,
            },
        }),
    }
}

fn int(i: i64) -> Value {
    Value::Integer(i)
}

fn text(s: &str) -> Value {
    Value::Text(s.to_string())
}

#[tokio::test]
async fn a_repair_replaces_the_consumers_state_with_the_restored_image() {
    let source = source().await;
    let (reports, records) = snapshot(&source, vec![job(Target::Head)], &settings(1 << 20)).await;
    let report = &reports[0];
    assert_eq!(report.status, Status::Written, "{report:?}");
    assert_eq!(report.reached, Some(Position::new(3, 4, REPAIR_COMMIT)));
    assert_eq!(report.bucket_head, report.reached);
    assert!(report.covers_target);
    assert_eq!((report.tables, report.rows), (3, 7));

    // Every record is at the reached position, from repair.
    for r in &records {
        assert_eq!(r.position(), Position::new(3, 4, REPAIR_COMMIT));
        assert_eq!(r.envelope.origin, Origin::Repair);
        assert_eq!(r.envelope.node, "repair-test");
        assert_eq!(r.stream(), &root_stream(SCRIPT, SCOPE).unwrap());
        assert_eq!(r.envelope.cell_name.as_deref(), Some("one"));
    }
    // Schemas first, the end last.
    let kinds: Vec<_> = records.iter().map(Record::kind).collect();
    assert_eq!(
        kinds,
        [
            Kind::Schema,
            Kind::Schema,
            Kind::Schema,
            Kind::Snapshot,
            Kind::Snapshot,
            Kind::Snapshot,
            Kind::SnapshotEnd
        ]
    );
    let Body::Schema(items) = &records[0].body else {
        panic!()
    };
    assert_eq!(items.table, "items");
    assert!(items.sql.starts_with("CREATE TABLE items"));
    let label = items.columns.iter().find(|c| c.name == "label").unwrap();
    assert!(label.generated);
    assert_eq!(items.columns.iter().find(|c| c.name == "id").unwrap().pk, 1);

    // The consumer had live rows the image no longer holds, and rows past
    // the snapshot that must survive it.
    let mut consumer = Consumer::new();
    consumer
        .ingest_all([
            live(
                Position::new(3, 2, 1),
                "items",
                &["id"],
                &["id", "name", "price", "img"],
                vec![RowChange(
                    Op::Insert,
                    vec![int(99)],
                    vec![int(99), text("ghost"), Value::Null, Value::Null],
                )],
            ),
            // The last commit of the snapshot's own txid is superseded too.
            live(
                Position::new(3, 4, 12),
                "notes",
                &["_rowid_"],
                &["body"],
                vec![RowChange(Op::Insert, vec![int(50)], vec![text("phantom")])],
            ),
            live(
                Position::new(3, 5, 13),
                "items",
                &["id"],
                &["id", "name", "price", "img"],
                vec![RowChange(
                    Op::Update,
                    vec![int(2)],
                    vec![int(2), text("pear"), Value::Real(3.0), Value::Null],
                )],
            ),
        ])
        .unwrap();
    consumer.ingest_all(records).unwrap();
    let state = consumer
        .stream(&root_stream(SCRIPT, SCOPE).unwrap())
        .unwrap();
    assert!(state.gaps.is_empty() && state.uncertain.is_empty());
    assert_eq!(
        table(&state, "items"),
        vec![
            (
                vec![int(1)],
                vec![
                    int(1),
                    text("apple"),
                    Value::Real(1.5),
                    Value::Blob(vec![0, 255])
                ]
            ),
            (
                vec![int(2)],
                vec![int(2), text("pear"), Value::Real(3.0), Value::Null]
            ),
        ]
    );
    assert_eq!(
        table(&state, "notes"),
        vec![
            (vec![int(7)], vec![text("hello")]),
            (vec![int(9)], vec![Value::Null]),
        ]
    );
    let pairs = state.table("pairs").unwrap();
    assert_eq!(pairs.key_columns, ["b", "a"]);
    assert_eq!(pairs.rows.len(), 3);
    assert_eq!(
        pairs.rows[&vec![int(1), text("y")]],
        vec![text("y"), int(1), Value::Real(2.5)]
    );
    // Denied and internal tables are neither snapshotted nor named.
    for name in ["secrets", "_cf_METADATA", "__queue_messages"] {
        assert!(state.table(name).is_none(), "{name}");
    }
}

#[tokio::test]
async fn a_snapshot_covers_a_gap_up_to_the_position_it_reached() {
    let source = source().await;
    let (reports, records) = snapshot(
        &source,
        vec![job(Target::AtOrAfter(export_restore::Position {
            epoch: 3,
            txid: 3,
        }))],
        &settings(1 << 20),
    )
    .await;
    // Txid 3 sits inside the range 3..=4, so the first cut at or after it
    // is 4.
    assert_eq!(reports[0].reached, Some(Position::new(3, 4, REPAIR_COMMIT)));
    assert!(reports[0].covers_target);

    let gap = Record {
        envelope: Envelope {
            stream: root_stream(SCRIPT, SCOPE).unwrap(),
            cell_name: None,
            position: Position::new(3, 3, 5),
            committed_at: 0,
            node: "n1".into(),
            origin: Origin::Live,
            fragment: 1,
            fragments: 1,
        },
        body: Body::Gap(celld_export_format::GapBody {
            from: Position::new(3, 2, 0),
            to: Position::new(3, 3, u64::MAX),
            reason: "unmatched".into(),
        }),
    };
    let mut consumer = Consumer::new();
    consumer.ingest(gap).unwrap();
    let stream = root_stream(SCRIPT, SCOPE).unwrap();
    assert_eq!(consumer.stream(&stream).unwrap().gaps.len(), 1);
    consumer.ingest_all(records).unwrap();
    assert!(consumer.stream(&stream).unwrap().gaps.is_empty());
}

#[tokio::test]
async fn a_target_the_bucket_does_not_hold_yet_is_snapshotted_at_the_head_and_reported_short() {
    let source = source().await;
    let (reports, records) = snapshot(
        &source,
        vec![job(Target::AtOrAfter(export_restore::Position {
            epoch: 3,
            txid: 9,
        }))],
        &settings(1 << 20),
    )
    .await;
    let report = &reports[0];
    assert_eq!(report.status, Status::Written);
    assert_eq!(report.reached, Some(Position::new(3, 4, REPAIR_COMMIT)));
    assert!(!report.covers_target);
    assert_eq!(report.target, "e3:9");
    assert!(records
        .iter()
        .all(|r| r.position() == Position::new(3, 4, REPAIR_COMMIT)));
}

#[tokio::test]
async fn large_tables_split_into_fragments_that_reassemble() {
    let source = bucket("fleet");
    let mut sql = format!("{SCHEMA} INSERT INTO notes (rowid, body) VALUES ");
    sql.push_str(
        &(1..=300)
            .map(|i| format!("({i}, '{}')", "n".repeat(40)))
            .collect::<Vec<_>>()
            .join(","),
    );
    sql.push_str(&format!(
        "; INSERT INTO items (id, name) VALUES (1, '{}');",
        "x".repeat(5000)
    ));
    put(&source, SCOPE, 1, 1, 1, &sql).await;
    let (reports, records) = snapshot(&source, vec![job(Target::Head)], &settings(2048)).await;
    let report = &reports[0];
    assert_eq!(report.status, Status::Written, "{report:?}");
    assert_eq!(report.rows, 301);
    // The long name cannot fit a record alone; it rides alone, oversized.
    assert_eq!(report.oversized_rows, 1);
    let notes: Vec<_> = records
        .iter()
        .filter(|r| matches!(&r.body, Body::Snapshot(s) if s.data.table == "notes"))
        .collect();
    assert!(notes.len() > 5, "{}", notes.len());
    for (i, r) in notes.iter().enumerate() {
        assert_eq!(r.envelope.fragment as usize, i + 1);
        assert_eq!(r.envelope.fragments as usize, notes.len());
        assert!(r.to_json().len() <= 2048, "{}", r.to_json().len());
    }
    assert_eq!(report.records as usize, records.len());

    let mut consumer = Consumer::new();
    consumer.ingest_all(records).unwrap();
    assert_eq!(consumer.incomplete(), 0);
    let state = consumer
        .stream(&root_stream(SCRIPT, SCOPE).unwrap())
        .unwrap();
    assert_eq!(state.table("notes").unwrap().rows.len(), 300);
    assert_eq!(state.table("items").unwrap().rows.len(), 1);
}

#[tokio::test]
async fn an_empty_table_is_still_named_and_emptied() {
    let source = bucket("fleet");
    put(&source, SCOPE, 1, 1, 1, SCHEMA).await;
    let (reports, records) = snapshot(&source, vec![job(Target::Head)], &settings(1 << 20)).await;
    assert_eq!((reports[0].tables, reports[0].rows), (3, 0));
    let mut consumer = Consumer::new();
    consumer
        .ingest(live(
            Position::new(1, 1, 1),
            "pairs",
            &["b", "a"],
            &["a", "b", "v"],
            vec![RowChange(
                Op::Insert,
                vec![int(1), text("q")],
                vec![text("q"), int(1), Value::Null],
            )],
        ))
        .unwrap();
    consumer.ingest_all(records).unwrap();
    let state = consumer
        .stream(&root_stream(SCRIPT, SCOPE).unwrap())
        .unwrap();
    assert!(state.tables.is_empty(), "{:?}", state.tables);
}

#[tokio::test]
async fn repair_records_do_not_disturb_watermark_certification() {
    let source = source().await;
    let (_, records) = snapshot(&source, vec![job(Target::Head)], &settings(1 << 20)).await;
    let stream = root_stream(SCRIPT, SCOPE).unwrap();
    let commit = live(
        Position::new(3, 1, 1),
        "notes",
        &["_rowid_"],
        &["body"],
        vec![RowChange(Op::Insert, vec![int(1)], vec![text("a")])],
    );
    let mut watermark = commit.clone();
    watermark.envelope.position = Position::new(3, 1, 1);
    watermark.body = Body::Watermark(WatermarkBody {
        from: None,
        through: Position::new(3, 1, 1),
        commits: 1,
        records: 1,
    });
    let mut consumer = Consumer::new();
    consumer.ingest_all([commit, watermark]).unwrap();
    consumer.ingest_all(records).unwrap();
    assert_eq!(
        consumer.stream(&stream).unwrap().certified_head(),
        Some(Position::new(3, 1, 1))
    );
}

#[tokio::test]
async fn jobs_run_concurrently_and_report_in_order_and_failures_stay_per_stream() {
    let source = source().await;
    put(&source, "Cart:two", 1, 1, 1, &state_at_4()).await;
    let mut facet = job(Target::Head);
    facet.stream.facet = Some("child".into());
    let missing = Job {
        stream: root_stream(SCRIPT, "Cart:nothing").unwrap(),
        target: Target::Head,
        reasons: ["backfill".into()].into(),
        pin_incarnation: false,
    };
    let two = Job {
        stream: root_stream(SCRIPT, "Cart:two").unwrap(),
        target: Target::Head,
        reasons: ["backfill".into()].into(),
        pin_incarnation: false,
    };
    let (reports, records) = snapshot(
        &source,
        vec![job(Target::Head), missing, facet, two],
        &settings(1 << 20),
    )
    .await;
    let statuses: Vec<_> = reports
        .iter()
        .map(|r| (r.cell.as_str(), r.status))
        .collect();
    assert_eq!(
        statuses,
        [
            ("Cart:one", Status::Written),
            ("Cart:nothing", Status::Failed),
            ("Cart:one", Status::Skipped),
            ("Cart:two", Status::Written),
        ]
    );
    assert!(reports[1]
        .error
        .as_deref()
        .unwrap()
        .contains("nothing in the bucket"));
    let mut consumer = Consumer::new();
    consumer.ingest_all(records).unwrap();
    let state = consumer.state();
    assert_eq!(state.len(), 2);
    for s in state.values() {
        assert_eq!(s.table("items").unwrap().rows.len(), 2);
    }
}

#[tokio::test]
async fn a_dropped_record_fails_its_stream() {
    struct Refusing(mpsc::UnboundedSender<Outcome>);
    impl ExportSink for Refusing {
        fn name(&self) -> &'static str {
            "refusing"
        }
        fn submit(&self, records: Vec<SinkRecord>) -> Result<(), crate::export_sink::Closed> {
            let results = records
                .iter()
                .map(|r| {
                    (
                        r.seq,
                        Delivery::Dropped {
                            reason: "full".into(),
                        },
                    )
                })
                .collect();
            let _ = self.0.send(Outcome {
                sink: "refusing",
                results,
            });
            Ok(())
        }
        fn flush(&self) {}
        fn buffered_bytes(&self) -> u64 {
            0
        }
        fn close(&self) -> futures_util::future::BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }
    let source = source().await;
    let (tx, rx) = mpsc::unbounded_channel();
    let reports = run(
        &source,
        Arc::new(Refusing(tx)),
        rx,
        vec![job(Target::Head)],
        &settings(1 << 20),
        None,
        |_| {},
    )
    .await;
    assert_eq!(reports[0].status, Status::Failed);
    assert!(reports[0].error.as_deref().unwrap().contains("dropped"));
}

#[test]
fn the_gaps_list_parses_snowflake_unloads_and_groups_by_stream() {
    let text = r#"
{"SCRIPT":"shop","CLASS":"Cart","CELL":"Cart:one","FACET":"","INCARNATION":0,"GAP_KIND":"gap","BOUND_EPOCH":3,"BOUND_TXID":"7","REASON":"unmatched"}
{"script":"shop","class":"Cart","cell":"Cart:one","facet":"","incarnation":0,"gap_kind":"link","bound_epoch":4,"bound_txid":2}
{"script":"shop","class":"Cart","cell":"Cart:two","facet":null,"incarnation":0,"gap_kind":"bulk","bound_epoch":null,"bound_txid":null,"table_name":"items"}
{"script":"shop","class":"Cart","cell":"Cart:two","facet":"","incarnation":0,"gap_kind":"gap","bound_epoch":1,"bound_txid":1}
{"script":"shop","class":"Cart","cell":"Cart:one","facet":"a/b","incarnation":5,"gap_kind":"recovered","bound_epoch":2,"bound_txid":9}
"#;
    let rows = parse_gaps(text).unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].stream, root_stream("shop", "Cart:one").unwrap());
    assert_eq!(
        rows[0].bound,
        Some(export_restore::Position { epoch: 3, txid: 7 })
    );
    assert_eq!(rows[2].bound, None);

    let jobs = jobs_from_gaps(&rows, false);
    assert_eq!(jobs.len(), 3);
    // The highest bound across a stream's rows, by epoch first.
    assert_eq!(
        jobs[0].target,
        Target::AtOrAfter(export_restore::Position { epoch: 4, txid: 2 })
    );
    assert_eq!(
        jobs[0].reasons,
        ["gap".to_string(), "link".to_string()].into()
    );
    // A facet stream is its own job.
    assert_eq!(jobs[1].stream.facet.as_deref(), Some("a/b"));
    // A bulk row has no bound, so only the head covers it.
    assert_eq!(jobs[2].stream.cell, "Cart:two");
    assert_eq!(jobs[2].target, Target::Head);

    // Backfill takes every stream at the head.
    assert!(jobs_from_gaps(&rows, true)
        .iter()
        .all(|j| j.target == Target::Head));
}

#[test]
fn a_bad_gaps_row_names_its_line() {
    let error = parse_gaps("\n{\"script\":\"s\",\"class\":\"A\",\"cell\":\"B:x\",\"incarnation\":0,\"gap_kind\":\"gap\"}")
        .unwrap_err();
    assert!(format!("{error:#}").contains("line 2"), "{error:#}");
    assert!(format!("{error:#}").contains("not of class"), "{error:#}");
    assert!(
        parse_gaps("{\"script\":\"s\",\"class\":\"A\",\"cell\":\"A:x\",\"gap_kind\":\"gap\"}")
            .is_err()
    );
}

#[tokio::test]
async fn the_snapshot_goes_to_the_incarnation_the_image_records() {
    let with = |incarnation: &str| {
        format!(
            "{SCHEMA}
            ALTER TABLE _cf_METADATA ADD COLUMN incarnation INTEGER;
            UPDATE _cf_METADATA SET incarnation = {incarnation};
            INSERT INTO notes VALUES ('n');"
        )
    };
    let source = bucket("fleet");
    put(&source, SCOPE, 7, 1, 1, &with("7")).await;
    put(
        &source,
        "Cart:new",
        1,
        1,
        1,
        &with("NULL").replace("Cart:one", "Cart:new"),
    )
    .await;

    let mut pinned = job(Target::Head);
    pinned.pin_incarnation = true;
    let mut matching = pinned.clone();
    matching.stream.incarnation = 7;
    let fresh = Job {
        stream: root_stream(SCRIPT, "Cart:new").unwrap(),
        target: Target::Head,
        reasons: ["backfill".into()].into(),
        pin_incarnation: false,
    };
    let (reports, records) = snapshot(
        &source,
        vec![job(Target::Head), pinned, matching, fresh],
        &settings(1 << 20),
    )
    .await;
    // Unpinned, the image decides; pinned, it must agree.
    assert_eq!(reports[0].status, Status::Written);
    assert_eq!(reports[0].incarnation, 7);
    assert_eq!(reports[1].status, Status::Skipped);
    assert!(reports[1]
        .error
        .as_deref()
        .unwrap()
        .contains("incarnation 7"));
    assert_eq!(reports[2].status, Status::Written);
    // A cell that never opened with export on has no stream to write to.
    assert_eq!(reports[3].status, Status::Skipped, "{:?}", reports[3]);
    assert!(reports[3]
        .error
        .as_deref()
        .unwrap()
        .contains("no stream yet"));

    assert!(records.iter().all(|r| r.stream().incarnation == 7
        && r.stream().cell == SCOPE
        && r.envelope.cell_name.as_deref() == Some("one")));
    let mut consumer = Consumer::new();
    consumer.ingest_all(records).unwrap();
    let mut stream = root_stream(SCRIPT, SCOPE).unwrap();
    stream.incarnation = 7;
    assert_eq!(
        consumer
            .stream(&stream)
            .unwrap()
            .table("notes")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn an_image_without_metadata_takes_the_default_identity() {
    let db = Connection::open_in_memory().unwrap();
    assert_eq!(
        image_identity(&db, SCOPE).unwrap(),
        ImageIdentity::default()
    );
    let job = job(Target::Head);
    assert_eq!(
        resolve_identity(&job, &ImageIdentity::default()).unwrap(),
        job.stream
    );
    // Even an unpinned job with another default lands on the legacy stream.
    let mut other = job.clone();
    other.stream.incarnation = 7;
    assert_eq!(
        resolve_identity(&other, &ImageIdentity::default())
            .unwrap()
            .incarnation,
        ROOT_INCARNATION
    );
}

#[tokio::test]
async fn a_pinned_incarnation_never_takes_a_legacy_image() {
    let source = source().await;
    let mut pinned = job(Target::Head);
    pinned.pin_incarnation = true;
    pinned.stream.incarnation = 7;
    let (reports, records) = snapshot(&source, vec![pinned], &settings(1 << 20)).await;
    assert_eq!(reports[0].status, Status::Skipped, "{:?}", reports[0]);
    assert!(reports[0]
        .error
        .as_deref()
        .unwrap()
        .contains("incarnation 0"));
    assert!(records.is_empty());
}

#[tokio::test]
async fn classes_never_exported_are_skipped_whatever_named_them() {
    let source = bucket("fleet");
    for scope in ["__Workflow.shop:one", "__Queue:q"] {
        put(
            &source,
            scope,
            1,
            1,
            1,
            "CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB); INSERT INTO _cf_KV VALUES ('k', x'01');",
        )
        .await;
    }
    let jobs = ["__Workflow.shop:one", "__Queue:q"]
        .into_iter()
        .map(|scope| Job {
            stream: root_stream(SCRIPT, scope).unwrap(),
            target: Target::Head,
            reasons: ["gap".into()].into(),
            pin_incarnation: true,
        })
        .collect();
    let (reports, records) = snapshot(&source, jobs, &settings(1 << 20)).await;
    for report in &reports {
        assert_eq!(report.status, Status::Skipped, "{report:?}");
        assert!(report.error.as_deref().unwrap().contains("never exported"));
    }
    assert!(records.is_empty());
}

#[tokio::test]
async fn a_pace_spaces_bucket_reads_across_jobs() {
    assert!(Pace::per_second(0).is_none());
    let pace = Pace::per_second(50).unwrap();
    let started = tokio::time::Instant::now();
    for _ in 0..6 {
        pace.wait().await;
    }
    // Five intervals of 20 ms after the first free slot.
    assert!(started.elapsed() >= Duration::from_millis(100));

    let source = source().await;
    let destination = bucket("export");
    let (tx, rx) = mpsc::unbounded_channel();
    let sink = BucketSink::start(destination, "paced".into(), BucketSinkConfig::default(), tx);
    let reports = run(
        &source,
        Arc::new(sink),
        rx,
        vec![job(Target::Head)],
        &settings(1 << 20),
        Pace::per_second(1000),
        |_| {},
    )
    .await;
    assert_eq!(reports[0].status, Status::Written);
}
