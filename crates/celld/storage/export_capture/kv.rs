// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The export shape of the key-value tables (docs/design/change-export.md,
//! "Special tables").
//!
//! - `_cf_KV`, the Durable Object key-value API, is exported as table `kv`
//!   with columns `key` and `value`. The key is `k` alone: `scope` is the
//!   run's scope, which a facet rewrites on every open, so it is not part of
//!   the logical key, and rows of any other scope are not the cell's. The
//!   value is JSON text ([`crate::export_kv`] for V8 bytes, legacy JSON text
//!   as it is). A value that does not decode is exported as the stored bytes,
//!   a `{"$blob": …}` in the record.
//! - `__kv`, a KV namespace's table, keeps its columns and gains
//!   `blob_key`: the bucket object that holds a value too large to store
//!   inline (`kv/blobs-v2/<cell>/e<epoch>/<digest>`), or `NULL`. The blob
//!   itself is not exported.

use celld_export_format::{RowChange, TableRows, Value};
use celld_logic::kv::BlobRef;

/// The Durable Object key-value table in SQLite.
pub(crate) const KV_SOURCE: &str = "_cf_KV";
/// The name `_cf_KV` is exported under.
pub(crate) const KV_TABLE: &str = "kv";
/// A KV namespace cell's table.
pub(crate) const NAMESPACE_TABLE: &str = "__kv";
/// The column `__kv` gains.
pub(crate) const BLOB_KEY_COLUMN: &str = "blob_key";

/// Decodes V8-serialized values to JSON text, `None` where a value does not
/// decode.
pub(crate) type Decoder = fn(Vec<Vec<u8>>) -> Vec<Option<String>>;

/// The name `table` is exported under.
pub(crate) fn exported_name(table: &str) -> &str {
    if table == KV_SOURCE {
        KV_TABLE
    } else {
        table
    }
}

/// Reshape the rows of a key-value table for `scope`; other tables pass
/// through. `None` when nothing of the cell's is left.
pub(crate) fn reshape(
    rows: TableRows,
    scope: &str,
    decode: Decoder,
) -> anyhow::Result<Option<TableRows>> {
    match rows.table.as_str() {
        KV_SOURCE => kv(rows, scope, decode),
        NAMESPACE_TABLE => Ok(Some(namespace(rows, scope))),
        _ => Ok(Some(rows)),
    }
}

fn column(rows: &TableRows, name: &str) -> anyhow::Result<usize> {
    rows.columns
        .iter()
        .position(|column| column == name)
        .ok_or_else(|| anyhow::anyhow!("{} has no column {name}", rows.table))
}

fn kv(rows: TableRows, scope: &str, decode: Decoder) -> anyhow::Result<Option<TableRows>> {
    let scope_at = column(&rows, "scope")?;
    let key_at = column(&rows, "k")?;
    let value_at = column(&rows, "v")?;
    anyhow::ensure!(
        rows.key_columns == ["scope", "k"],
        "{KV_SOURCE} is keyed by {:?}",
        rows.key_columns
    );
    let scope = Value::Text(scope.to_string());
    let mut out = Vec::new();
    let mut undecoded = Vec::new();
    for RowChange(op, _, row) in rows.rows {
        if row[scope_at] != scope {
            continue;
        }
        let key = row[key_at].clone();
        let value = match &row[value_at] {
            Value::Blob(bytes) => {
                undecoded.push((out.len(), bytes.clone()));
                Value::Null
            }
            value => legacy(value),
        };
        out.push(RowChange(op, vec![key.clone()], vec![key, value]));
    }
    if !undecoded.is_empty() {
        let decoded = decode(undecoded.iter().map(|(_, bytes)| bytes.clone()).collect());
        for ((at, bytes), json) in undecoded.into_iter().zip(decoded) {
            out[at].2[1] = match json {
                Some(json) => Value::Text(json),
                None => Value::Blob(bytes),
            };
        }
    }
    if out.is_empty() {
        return Ok(None);
    }
    Ok(Some(TableRows {
        table: KV_TABLE.to_string(),
        generation: rows.generation,
        columns: vec!["key".to_string(), "value".to_string()],
        key_columns: vec!["key".to_string()],
        rows: out,
    }))
}

/// A value that is not V8 bytes. The storage API reads text as legacy JSON,
/// and any other SQL value as the JSON of itself.
fn legacy(value: &Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Integer(i) => Value::Text(i.to_string()),
        Value::Real(r) => match serde_json::Number::from_f64(*r) {
            Some(number) => Value::Text(number.to_string()),
            None => Value::Blob(r.to_string().into_bytes()),
        },
        Value::Text(text) => match serde_json::from_str::<serde_json::Value>(text) {
            Ok(json) => Value::Text(json.to_string()),
            Err(_) => Value::Blob(text.clone().into_bytes()),
        },
        Value::Blob(bytes) => Value::Blob(bytes.clone()),
    }
}

fn namespace(mut rows: TableRows, scope: &str) -> TableRows {
    let Ok(blob_at) = column(&rows, "blob_id") else {
        // The first release's table, which stored every value inline.
        return rows;
    };
    rows.columns.push(BLOB_KEY_COLUMN.to_string());
    for RowChange(_, _, row) in &mut rows.rows {
        let key = match &row[blob_at] {
            Value::Text(reference) => BlobRef::parse(reference)
                .map(|reference| Value::Text(reference.object_key(scope)))
                .unwrap_or(Value::Null),
            _ => Value::Null,
        };
        row.push(key);
    }
    rows
}
