//! Disk-backed, incremental index of bucket export objects. A listing is still
//! necessary to find late uploads, replacements and retention deletes; unchanged
//! objects need no GET or Parquet decode. Only one cell's records enter the
//! reference consumer at a time.
use std::path::Path;
use std::sync::Mutex;

use anyhow::{ensure, Context as _};
use celld_export_format::{Body, Record};
use futures_util::TryStreamExt as _;
use rusqlite::{params, Connection, OptionalExtension as _};

use super::{Bucket, Tombstone, CHANGES_PREFIX};

// Fail explicitly rather than exhaust operator memory on an exceptionally hot
// cell. This bounds encoded history; the reference consumer has further overhead.
pub(super) const MAX_CELL_HISTORY: usize = 64 * 1024 * 1024;

pub(super) struct Cache {
    pub db: Mutex<Connection>,
}

impl Cache {
    pub fn open(path: Option<&Path>, identity: &str) -> anyhow::Result<Self> {
        let db = match path {
            Some(path) => {
                let mut options = std::fs::OpenOptions::new();
                options.read(true).write(true).create(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt as _;
                    options.mode(0o600);
                }
                options.open(path)?;
                Connection::open(path)?
            }
            None => Connection::open("")?,
        };
        db.execute_batch("PRAGMA cache_size=-4096; PRAGMA foreign_keys=ON; PRAGMA secure_delete=ON;
            CREATE TABLE IF NOT EXISTS scope (identity TEXT NOT NULL, tombstones TEXT);
            CREATE TABLE IF NOT EXISTS objects (key TEXT PRIMARY KEY, version TEXT NOT NULL, seen INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS records (
                object TEXT NOT NULL REFERENCES objects(key) ON DELETE CASCADE,
                cell TEXT NOT NULL, recovered INTEGER NOT NULL, data BLOB NOT NULL);
            CREATE INDEX IF NOT EXISTS records_cell ON records(cell, object);
            CREATE INDEX IF NOT EXISTS records_object ON records(object);
            CREATE INDEX IF NOT EXISTS records_recovered ON records(recovered);")?;
        let prior: Option<String> = db
            .query_row("SELECT identity FROM scope", [], |r| r.get(0))
            .optional()?;
        if let Some(prior) = prior {
            ensure!(
                prior == identity,
                "export cache belongs to a different bucket; choose another --cache path"
            );
        } else {
            db.execute("INSERT INTO scope (identity) VALUES (?1)", [identity])?;
        }
        Ok(Self { db: Mutex::new(db) })
    }

    pub async fn refresh(&self, bucket: &Bucket, tombstones: &[Tombstone]) -> anyhow::Result<()> {
        // This connection is not shared across audit processes. SQLite's write
        // lock prevents concurrent refreshes of the same cache from interleaving.
        // The transaction is held across the listing, without holding a Mutex.
        self.db
            .lock()
            .unwrap()
            .execute_batch("BEGIN IMMEDIATE; UPDATE objects SET seen=0;")?;
        let result = async {
            let mut encoded = tombstones
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()?;
            encoded.sort();
            let fingerprint = serde_json::to_string(&encoded)?;
            {
                let db = self.db.lock().unwrap();
                let previous: Option<String> =
                    db.query_row("SELECT tombstones FROM scope", [], |r| r.get(0))?;
                if previous.as_deref() != Some(&fingerprint) {
                    // Erasure applies to the local index too. Clearing a tombstone
                    // must re-import records that an earlier refresh filtered out.
                    db.execute_batch("DELETE FROM objects;")?;
                    db.execute("UPDATE scope SET tombstones=?1", [&fingerprint])?;
                }
            }
            self.import(bucket, tombstones).await
        }
        .await;
        let db = self.db.lock().unwrap();
        match result {
            Ok(()) => db.execute_batch("DELETE FROM objects WHERE seen=0; COMMIT;")?,
            Err(error) => {
                db.execute_batch("ROLLBACK;")?;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn import(&self, bucket: &Bucket, tombstones: &[Tombstone]) -> anyhow::Result<()> {
        let prefix = object_store::path::Path::from(format!("{}{CHANGES_PREFIX}", bucket.prefix));
        let mut objects = bucket.store.list(Some(&prefix));
        while let Some(object) = objects.try_next().await? {
            let key = object
                .location
                .as_ref()
                .strip_prefix(&bucket.prefix)
                .context("export object outside bucket prefix")?;
            if !key.ends_with(".parquet") {
                continue;
            }
            let version = serde_json::to_string(&(
                &object.e_tag,
                &object.version,
                object.size,
                object.last_modified.timestamp_millis(),
            ))?;
            let unchanged = (object.e_tag.is_some() || object.version.is_some()) && {
                let db = self.db.lock().unwrap();
                db.execute(
                    "UPDATE objects SET seen=1 WHERE key=?1 AND version=?2",
                    params![key, version],
                )? > 0
            };
            if unchanged {
                continue;
            }
            let Some((bytes, _)) = bucket.get(key).await? else {
                continue;
            };
            let records = crate::export_sink::decode_records(bytes.to_vec())
                .with_context(|| format!("decode export object {key}"))?;
            let db = self.db.lock().unwrap();
            db.execute("DELETE FROM objects WHERE key=?1", [key])?;
            db.execute(
                "INSERT INTO objects VALUES (?1, ?2, 1)",
                params![key, version],
            )?;
            insert(
                &db,
                key,
                records
                    .into_iter()
                    .filter(|r| !tombstones.iter().any(|t| t.matches(r.stream()))),
            )?;
        }
        Ok(())
    }

    pub fn insert_records(&self, records: Vec<Record>) -> anyhow::Result<()> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        tx.execute("INSERT INTO objects VALUES ('test', '', 1)", [])?;
        insert(&tx, "test", records)?;
        tx.commit()?;
        Ok(())
    }

    pub fn cells(&self, only: Option<&str>) -> anyhow::Result<Vec<String>> {
        let db = self.db.lock().unwrap();
        if let Some(cell) = only {
            let exists: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM records WHERE cell=?1)",
                [cell],
                |r| r.get(0),
            )?;
            return Ok(if exists {
                vec![cell.to_string()]
            } else {
                vec![]
            });
        }
        let mut stmt = db.prepare("SELECT DISTINCT cell FROM records ORDER BY cell")?;
        let cells = stmt
            .query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(cells)
    }

    pub fn records(
        &self,
        cell: &str,
        tombstones: &[Tombstone],
        max_history: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let db = self.db.lock().unwrap();
        let mut stmt =
            db.prepare("SELECT data FROM records WHERE cell=?1 ORDER BY object, rowid")?;
        let mut rows = stmt.query([cell])?;
        let mut total = 0;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let bytes = row.get_ref(0)?.as_blob()?;
            total += bytes.len();
            ensure!(total <= max_history,
                "export history for {cell} exceeds the {max_history}-byte audit limit; raise --max-cell-history with sufficient memory, or reduce retained history after repair");
            let record: Record = serde_json::from_slice(bytes)?;
            if !tombstones.iter().any(|t| t.matches(record.stream())) {
                records.push(record);
            }
        }
        Ok(records)
    }
}

fn insert(
    db: &Connection,
    key: &str,
    records: impl IntoIterator<Item = Record>,
) -> anyhow::Result<()> {
    let mut stmt = db.prepare_cached("INSERT INTO records VALUES (?1, ?2, ?3, ?4)")?;
    for record in records {
        stmt.execute(params![
            key,
            record.stream().cell,
            matches!(record.body, Body::Recovered(_)),
            record.to_json()
        ])?;
    }
    Ok(())
}
