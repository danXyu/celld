//! Fixtures for `benches/export_pipeline.rs`, absent from ordinary builds.
#![allow(clippy::disallowed_methods)] // Offline benchmark, outside Actor execution.
use crate::bucket::Bucket;
use crate::storage::export_capture::{Capture, CapturedCommit, Checkpoint, DirtyList, Settings};
use celld_export_format::Record;
use rusqlite::Connection;

pub struct CaptureFixture {
    // Drop the session before its SQLite handle, including on a panic.
    capture: Option<Capture>,
    connection: Connection,
    dirty: DirtyList,
}

pub struct Batch(CapturedCommit);
impl Batch {
    pub fn rows(&self) -> usize {
        self.0.tables.iter().map(|t| t.rows.len()).sum()
    }
    pub fn bulk(&self) -> usize {
        self.0.bulk.len()
    }
}

impl CaptureFixture {
    pub fn new(rows: usize, payload: usize, budget: u64, enabled: bool) -> Self {
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, payload BLOB, revision INTEGER NOT NULL);").unwrap();
        let tx = connection.transaction().unwrap();
        {
            let mut insert = tx.prepare("INSERT INTO items VALUES (?1, ?2, 0)").unwrap();
            let bytes: Vec<u8> = (0..payload).map(|n| n as u8).collect();
            for id in 0..rows {
                insert.execute(rusqlite::params![id, bytes]).unwrap();
            }
        }
        tx.commit().unwrap();
        let dirty = DirtyList::default();
        let capture = enabled.then(|| {
            Capture::install(
                &connection,
                "Items:bench",
                Settings {
                    max_tx_bytes: budget,
                },
                Default::default(),
                dirty.clone(),
            )
            .unwrap()
        });
        let mut fixture = Self {
            capture,
            connection,
            dirty,
        };
        // Warm the catalog and schema cache before measuring steady-state writes.
        if enabled {
            fixture.write();
            drop(fixture.checkpoint());
        }
        fixture
    }

    pub fn write(&self) {
        self.connection
            .execute_batch("BEGIN; UPDATE items SET revision = revision + 1; COMMIT;")
            .unwrap();
    }

    pub fn checkpoint(&mut self) -> Batch {
        self.dirty.borrow_mut().clear();
        match self
            .capture
            .as_mut()
            .expect("capture enabled")
            .checkpoint(&self.connection, 1_790_000_000_000)
        {
            Checkpoint::Pulled(batch) => Batch(batch),
            other => panic!("expected a captured update, got {other:?}"),
        }
    }
}

pub struct AuditFixture {
    pub bucket: Bucket,
    pub directory: tempfile::TempDir,
}
impl AuditFixture {
    pub async fn new(objects: usize, records: &[Record]) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&directory.path().join("bucket.sqlite")).unwrap();
        for i in 0..objects {
            let mut records = records.to_vec();
            // Each object is a separate stream; objects must not deduplicate away.
            for r in &mut records {
                r.envelope.stream.cell = format!("Items:{i}");
            }
            bucket
                .put(
                    &format!("export/changes/bench/{i:06}.parquet"),
                    crate::export_sink::encode_records(records).unwrap(),
                )
                .await
                .unwrap();
        }
        Self { bucket, directory }
    }
}
