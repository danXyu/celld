//! `celld export verify` (`docs/design/change-export.md#snapshots-and-repair`).
//!
//! Restores a stream from the bucket at its head, read-only, and compares
//! every exported table with the consumer's state. The consumer's state is
//! taken from its records at or below the restored position only
//! ([`ConsumerView::state_at`]), so live changes past the head cannot show
//! up as drift. A stream whose certified position has not reached the head
//! yet, and that no snapshot at or past the head covers, is reported as
//! behind and not compared: the reconciler owns that difference.
//!
//! What is compared is what the capture exports: tables that pass the
//! capture's filter and the operator's deny list, by name, against the
//! newest open generation the consumer holds; rows by the same key the
//! export uses (the declared primary key, or the rowid); and every visible
//! column by name. `_cf_KV` is skipped because its export is decoded, not a
//! copy of the table, and tables a `bulk` record left uncertain are skipped
//! until a snapshot covers them.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context as _;
use celld_export_format::{Position, StreamId, StreamState, Value};
use rusqlite::Connection;
use serde::Serialize;

use super::tombstone::scope_of;
use super::{ConsumerView, StreamSummary};
use crate::bucket::Bucket;
use crate::export_restore::{self, Target};

/// How many differences one table reports in detail.
const MAX_DETAILS: usize = 20;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffKind {
    /// In the cell, not in the consumer.
    Missing,
    /// In the consumer, not in the cell.
    Extra,
    /// In both, with different values.
    Changed,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Diff {
    pub table: String,
    pub kind: DiffKind,
    /// The row key, or empty for a whole table.
    pub key: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    /// Every compared table matches.
    Match { tables: usize, rows: usize },
    /// The consumer differs from the cell.
    Drift {
        tables: usize,
        rows: usize,
        /// How many differences in all; `diffs` holds the first few of each
        /// table.
        total: usize,
        diffs: Vec<Diff>,
    },
    /// The consumer has not certified the restored position yet.
    Behind { certified: Option<Position> },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Verdict {
    pub stream: StreamId,
    pub scope: String,
    /// The position restored, the bucket's head.
    pub epoch: u64,
    pub txid: u64,
    #[serde(flatten)]
    pub outcome: Outcome,
    /// Tables left out, with why.
    pub skipped: Vec<(String, String)>,
}

impl Verdict {
    pub fn drifted(&self) -> bool {
        matches!(self.outcome, Outcome::Drift { .. })
    }
}

/// Choose up to `sample` streams at random: roots and facets alike, never a
/// stream a `deleted` record removed.
pub fn sample(streams: &[StreamSummary], sample: usize, seed: u64) -> Vec<StreamSummary> {
    let mut pool: Vec<&StreamSummary> = streams
        .iter()
        .filter(|s| s.deleted_at.is_none() && !s.certified.is_empty())
        .collect();
    // xorshift: a sample needs spread, not a cryptographic source.
    let mut state = seed | 1;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for i in (1..pool.len()).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        pool.swap(i, j);
    }
    pool.into_iter().take(sample).cloned().collect()
}

/// Verify one stream. `denies` is the operator's table deny list,
/// `(class, table)`.
pub async fn verify(
    bucket: &Bucket,
    consumer: &dyn ConsumerView,
    stream: &StreamSummary,
    denies: impl Fn(&str, &str) -> bool,
) -> anyhow::Result<Verdict> {
    let scope = scope_of(&stream.id.cell, stream.id.facet.as_deref());
    let restored = export_restore::restore(
        bucket,
        &export_restore::Stream::parse(&scope)?,
        Target::Head,
    )
    .await
    .with_context(|| format!("restore {scope} at its head"))?;
    let at = Position::new(restored.position.epoch, restored.position.txid, u64::MAX);
    let mut verdict = Verdict {
        stream: stream.id.clone(),
        scope,
        epoch: at.epoch,
        txid: at.txid,
        outcome: Outcome::Behind {
            certified: stream.certified.get(&at.epoch).copied(),
        },
        skipped: Vec::new(),
    };
    let reached = stream
        .certified
        .get(&at.epoch)
        .is_some_and(|c| c.txid >= at.txid);
    if !reached && !stream.covered(at.epoch, at.txid) {
        return Ok(verdict);
    }
    let state = consumer.state_at(&stream.id, at).await?.unwrap_or_default();
    let db = restored.open()?;
    let (outcome, skipped) = compare(&db, &state, |table| denies(&stream.id.class, table))?;
    verdict.outcome = outcome;
    verdict.skipped = skipped;
    Ok(verdict)
}

/// Compare a restored image with a consumer state.
pub fn compare(
    db: &Connection,
    state: &StreamState,
    denied: impl Fn(&str) -> bool,
) -> anyhow::Result<(Outcome, Vec<(String, String)>)> {
    let mut skipped = Vec::new();
    let mut compared = BTreeSet::new();
    let mut diffs = Vec::new();
    let mut total = 0;
    let mut rows = 0;
    let uncertain: BTreeSet<&str> = state.uncertain.iter().map(|t| t.table.as_str()).collect();

    for table in tables(db)? {
        if !crate::storage::export_capture::exported_table(&table) {
            continue;
        }
        if denied(&table) {
            skipped.push((table, "denied by CELLD_EXPORT_TABLES".into()));
            continue;
        }
        if table == "_cf_KV" {
            skipped.push((table, "exported through the KV decoder".into()));
            continue;
        }
        if uncertain.contains(table.as_str()) {
            skipped.push((table, "a bulk record left it uncertain".into()));
            continue;
        }
        compared.insert(table.clone());
        let cell = read_table(db, &table)?;
        rows += cell.len();
        let held: BTreeMap<Vec<Value>, BTreeMap<&str, &Value>> = state
            .table(&table)
            .map(|t| {
                t.rows
                    .iter()
                    .map(|(k, row)| {
                        let named = t
                            .columns
                            .iter()
                            .map(String::as_str)
                            .zip(row.iter())
                            .collect();
                        (k.clone(), named)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut shown = 0;
        let mut push = |kind: DiffKind, key: &[Value]| {
            total += 1;
            if shown < MAX_DETAILS {
                shown += 1;
                diffs.push(Diff {
                    table: table.clone(),
                    kind,
                    key: key.to_vec(),
                });
            }
        };
        for (key, row) in &cell {
            match held.get(key) {
                None => push(DiffKind::Missing, key),
                Some(theirs) => {
                    let ours: BTreeMap<&str, &Value> =
                        row.iter().map(|(c, v)| (c.as_str(), v)).collect();
                    if !same_row(&ours, theirs) {
                        push(DiffKind::Changed, key);
                    }
                }
            }
        }
        for key in held.keys().filter(|k| !cell.contains_key(*k)) {
            push(DiffKind::Extra, key);
        }
    }
    for (tg, t) in &state.tables {
        if !compared.contains(&tg.table)
            && !skipped.iter().any(|(s, _)| *s == tg.table)
            && !t.rows.is_empty()
        {
            total += 1;
            diffs.push(Diff {
                table: tg.table.clone(),
                kind: DiffKind::Extra,
                key: Vec::new(),
            });
        }
    }
    let tables = compared.len();
    let outcome = if total == 0 {
        Outcome::Match { tables, rows }
    } else {
        Outcome::Drift {
            tables,
            rows,
            total,
            diffs,
        }
    };
    Ok((outcome, skipped))
}

/// Two rows are the same when every column either holds has the same value.
/// A column one side lacks counts as NULL, which is what an added column
/// reads as in rows written before it.
fn same_row(ours: &BTreeMap<&str, &Value>, theirs: &BTreeMap<&str, &Value>) -> bool {
    let columns: BTreeSet<&str> = ours.keys().chain(theirs.keys()).copied().collect();
    columns.into_iter().all(|c| {
        let a = ours.get(c).copied().unwrap_or(&Value::Null);
        let b = theirs.get(c).copied().unwrap_or(&Value::Null);
        a == b
    })
}

/// Ordinary tables of `main`: no views, virtual tables or their shadows.
fn tables(db: &Connection) -> anyhow::Result<Vec<String>> {
    let mut statement = db.prepare(
        "SELECT name FROM pragma_table_list WHERE schema = 'main' AND type = 'table' ORDER BY name",
    )?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(names)
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Rows by export key, each with its visible columns by name.
type Rows = BTreeMap<Vec<Value>, Vec<(String, Value)>>;

/// Every row of `table`, keyed the way the export keys it, with its visible
/// columns by name.
fn read_table(db: &Connection, table: &str) -> anyhow::Result<Rows> {
    let mut info = db.prepare(&format!("PRAGMA main.table_xinfo({})", quote(table)))?;
    let mut columns = Vec::new();
    let mut all = BTreeSet::new();
    let mut key: Vec<(i64, usize)> = Vec::new();
    let mut rows = info.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let pk: i64 = row.get(5)?;
        let hidden: i64 = row.get(6)?;
        all.insert(name.to_ascii_lowercase());
        if hidden != 0 {
            continue;
        }
        if pk > 0 {
            key.push((pk, columns.len()));
        }
        columns.push(name);
    }
    key.sort();
    let key: Vec<usize> = key.into_iter().map(|(_, i)| i).collect();
    let rowid = if key.is_empty() {
        Some(
            ["rowid", "_rowid_", "oid"]
                .into_iter()
                .find(|name| !all.contains(*name))
                .with_context(|| format!("table {table} shadows every rowid alias"))?,
        )
    } else {
        None
    };
    let mut select: Vec<String> = columns.iter().map(|c| quote(c)).collect();
    if let Some(rowid) = rowid {
        select.push(rowid.to_string());
    }
    let mut statement = db.prepare(&format!(
        "SELECT {} FROM main.{}",
        select.join(", "),
        quote(table)
    ))?;
    let mut out = BTreeMap::new();
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let values: Vec<Value> = (0..select.len())
            .map(|i| Ok(value(row.get_ref(i)?)))
            .collect::<anyhow::Result<_>>()?;
        let k = match rowid {
            Some(_) => vec![values[columns.len()].clone()],
            None => key.iter().map(|&i| values[i].clone()).collect(),
        };
        let named = columns
            .iter()
            .cloned()
            .zip(values.into_iter().take(columns.len()))
            .collect();
        out.insert(k, named);
    }
    Ok(out)
}

fn value(v: rusqlite::types::ValueRef<'_>) -> Value {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(r) => Value::Real(r),
        ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    }
}
