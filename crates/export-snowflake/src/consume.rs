//! Batching consumed records to [`Land`].
//!
//! The loader reads messages, each one record's JSON, from the blob-stream
//! topic, adds them to a [`Batch`], and lands the batch once it is full or
//! old enough: through Snowpipe Streaming ([`crate::streaming`]), whose
//! elastic channel acknowledges each append once the rows are durable in
//! Snowflake, and does not order them. Only after every row of the batch is
//! acknowledged does the loader commit the offsets the batch covered, so a crash replays at most the
//! batch, and a replayed record is a duplicate every reader drops. Arrival
//! order means nothing to the tables: completeness comes from positions and
//! watermarks.
//!
//! A message that is not a record is never committed past on its own: the
//! caller stops there, after landing what came before it, so the message is
//! read again once the loader can decode it (a record from a newer celld
//! needs a newer loader). Nothing downstream would notice the record
//! missing: `EXPORT_GAPS` lists explicit gaps, not watermark counts that fall
//! short. An operator who has looked at a message and decided to drop it
//! names it with [`Batch::skip`].

use std::collections::BTreeMap;

use celld_export_format::Record;

use crate::loader::{LoadError, WarehouseError};
use crate::LandingRow;

/// Somewhere to land rows in `EXPORT_LANDING`. `Ok` means every row is
/// durable there; after an error, any of them may be, and landing them all
/// again is harmless.
pub trait Land {
    fn land(&mut self, rows: &[LandingRow]) -> Result<(), WarehouseError>;
}

/// When a batch is full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub records: usize,
    /// Encoded record bytes.
    pub bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            records: 10_000,
            bytes: 8 << 20,
        }
    }
}

/// Where a message was read, as a landed row's `source` says:
/// `blob-stream/<partition>/<offset>`.
pub fn message_source(partition: u32, offset: u64) -> String {
    format!("blob-stream/{partition}/{offset}")
}

/// A message that did not decode as a record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Undecodable {
    /// Where it was read, as a landed row's `source` would say.
    pub source: String,
    pub error: String,
}

/// Records waiting to land, and the highest offset they cover in each
/// virtual partition.
#[derive(Debug, Default)]
pub struct Batch {
    rows: Vec<LandingRow>,
    bytes: usize,
    offsets: BTreeMap<u32, u64>,
    /// Appended to every row's source.
    tag: String,
}

impl Batch {
    /// A batch whose rows' sources all end with `tag`, so the rows one run
    /// landed can be counted in `EXPORT_LANDING`.
    pub fn tagged(tag: impl Into<String>) -> Batch {
        Batch {
            tag: tag.into(),
            ..Batch::default()
        }
    }

    /// Add the message at `offset` in virtual partition `partition`. A
    /// message that is not a record is not added, and its offset is not
    /// covered.
    pub fn push_message(
        &mut self,
        partition: u32,
        offset: u64,
        payload: &[u8],
    ) -> Result<(), Undecodable> {
        let source = message_source(partition, offset);
        let record = Record::from_json(payload).map_err(|e| Undecodable {
            source: source.clone(),
            error: e.to_string(),
        })?;
        self.cover(partition, offset);
        self.push_record(&record, source, payload.len());
        Ok(())
    }

    /// Cover the message at `offset` without landing anything: one an
    /// operator chose to drop.
    pub fn skip(&mut self, partition: u32, offset: u64) {
        self.cover(partition, offset);
    }

    fn cover(&mut self, partition: u32, offset: u64) {
        let highest = self.offsets.entry(partition).or_insert(offset);
        *highest = (*highest).max(offset);
    }

    /// Add one line of JSON records, as `celld export inspect` prints them:
    /// a record, with the object it was read from as `object`, which
    /// becomes the row's source. Without one, the source is `fallback`.
    pub fn push_json_line(&mut self, line: &str, fallback: &str) -> Result<(), Undecodable> {
        let fail = |source: &str, error: String| Undecodable {
            source: source.to_string(),
            error,
        };
        let mut value: serde_json::Value =
            serde_json::from_str(line).map_err(|e| fail(fallback, e.to_string()))?;
        let source = match value.as_object_mut().and_then(|m| m.remove("object")) {
            Some(serde_json::Value::String(object)) => object,
            _ => fallback.to_string(),
        };
        let bytes = serde_json::to_vec(&value).map_err(|e| fail(&source, e.to_string()))?;
        let record = Record::from_json(&bytes).map_err(|e| fail(&source, e.to_string()))?;
        self.push_record(&record, source, bytes.len());
        Ok(())
    }

    fn push_record(&mut self, record: &Record, mut source: String, bytes: usize) {
        source.push_str(&self.tag);
        self.bytes += bytes + source.len();
        self.rows.push(LandingRow::from_record(record, source));
    }

    /// Records waiting to land.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Nothing to land and no offset to commit.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.offsets.is_empty()
    }

    pub fn is_full(&self, limits: &Limits) -> bool {
        self.rows.len() >= limits.records || self.bytes >= limits.bytes
    }

    /// Land the batch. On success the batch is empty again and the offsets
    /// it covered, highest per partition, are returned for the caller to
    /// commit. On failure nothing changes, so the same batch can be landed
    /// again.
    pub fn land(&mut self, to: &mut impl Land) -> Result<BTreeMap<u32, u64>, LoadError> {
        if !self.rows.is_empty() {
            to.land(&self.rows).map_err(|source| LoadError::Warehouse {
                statement: "land".to_string(),
                source,
            })?;
        }
        self.rows.clear();
        self.bytes = 0;
        Ok(std::mem::take(&mut self.offsets))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celld_export_format::{Body, Envelope, Origin, Position, StreamId, WatermarkBody};

    #[derive(Default)]
    struct Fake {
        landed: Vec<Vec<LandingRow>>,
        fail: bool,
    }

    impl Land for Fake {
        fn land(&mut self, rows: &[LandingRow]) -> Result<(), WarehouseError> {
            if self.fail {
                return Err(WarehouseError::other("unavailable"));
            }
            self.landed.push(rows.to_vec());
            Ok(())
        }
    }

    fn record(txid: u64) -> Record {
        Record {
            envelope: Envelope {
                stream: StreamId {
                    script: "app".into(),
                    class: "Room".into(),
                    cell: "r1".into(),
                    facet: None,
                    incarnation: 1,
                },
                cell_name: None,
                position: Position::new(1, txid, 1),
                committed_at: 1_790_000_000_000,
                node: "node-a".into(),
                origin: Origin::Live,
                fragment: 1,
                fragments: 1,
            },
            body: Body::Watermark(WatermarkBody {
                from: None,
                through: Position::new(1, txid, 1),
                commits: 1,
                records: 1,
            }),
        }
    }

    #[test]
    fn offsets_are_returned_only_once_the_batch_lands() {
        let mut batch = Batch::default();
        let mut l = Fake::default();
        batch.push_message(3, 10, &record(1).to_json()).unwrap();
        batch.push_message(3, 12, &record(2).to_json()).unwrap();
        // Offsets need not arrive in order across calls; the highest wins.
        batch.push_message(3, 11, &record(3).to_json()).unwrap();
        batch.push_message(5, 7, &record(4).to_json()).unwrap();
        assert_eq!(batch.len(), 4);

        l.fail = true;
        assert!(batch.land(&mut l).is_err());
        assert_eq!(batch.len(), 4, "a failed landing keeps the batch");

        l.fail = false;
        let offsets = batch.land(&mut l).unwrap();
        assert_eq!(offsets, BTreeMap::from([(3, 12), (5, 7)]));
        assert!(batch.is_empty());
        let landed = &l.landed[0];
        assert_eq!(landed.len(), 4);
        assert_eq!(landed[0].source, "blob-stream/3/10");
        assert_eq!(landed[3].to_record().unwrap(), record(4));
    }

    #[test]
    fn an_undecodable_message_is_not_covered_unless_skipped() {
        let mut batch = Batch::default();
        let err = batch.push_message(2, 40, b"not json").unwrap_err();
        assert_eq!(err.source, "blob-stream/2/40");
        assert!(batch.is_empty(), "nothing to land and no offset to commit");
        batch.skip(2, 40);
        let mut l = Fake::default();
        assert_eq!(batch.land(&mut l).unwrap(), BTreeMap::from([(2, 40)]));
        assert!(l.landed.is_empty(), "nothing to insert");
    }

    #[test]
    fn a_batch_is_full_at_either_limit() {
        let limits = Limits {
            records: 2,
            bytes: 1 << 20,
        };
        let mut batch = Batch::default();
        batch.push_message(0, 1, &record(1).to_json()).unwrap();
        assert!(!batch.is_full(&limits));
        batch.push_message(0, 2, &record(2).to_json()).unwrap();
        assert!(batch.is_full(&limits));
        let small = Limits {
            records: 100,
            bytes: 10,
        };
        let mut batch = Batch::default();
        batch.push_message(0, 1, &record(1).to_json()).unwrap();
        assert!(batch.is_full(&small));
    }

    #[test]
    fn json_lines_take_their_source_from_inspects_object() {
        let mut batch = Batch::tagged(" (ingest 1)");
        let mut line = serde_json::to_value(record(1)).unwrap();
        line["object"] = "export/changes/node-a/2026/09/29/02/1-a.parquet".into();
        batch.push_json_line(&line.to_string(), "stdin:1").unwrap();
        batch
            .push_json_line(&String::from_utf8(record(2).to_json()).unwrap(), "stdin:2")
            .unwrap();
        assert!(batch.push_json_line("{}", "stdin:3").is_err());
        let mut l = Fake::default();
        batch.land(&mut l).unwrap();
        let landed = &l.landed[0];
        assert_eq!(
            landed[0].source,
            "export/changes/node-a/2026/09/29/02/1-a.parquet (ingest 1)"
        );
        assert_eq!(landed[0].to_record().unwrap(), record(1));
        assert_eq!(landed[1].source, "stdin:2 (ingest 1)");
    }
}
