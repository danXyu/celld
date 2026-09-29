//! Erasure tombstones (`docs/design/change-export.md#erasure`).
//!
//! A tombstone is a bucket object under `export/tombstones/` and a row in
//! the consumer's `EXPORT_TOMBSTONES`. It has the same fields and the same
//! matching rule as that table: a stream is erased when script, class, cell
//! and facet are equal and the tombstone's incarnation is absent or equal.
//! An absent incarnation erases every incarnation of the scope, which is
//! what erasing a Durable Object by name means. Clearing a tombstone, so a
//! stream recreated under the same scope exports again, is an explicit
//! operator action that sets `cleared_at_ms`; the object stays as the
//! record of the erasure.

use anyhow::Context as _;
use celld_export_format::StreamId;
use serde::{Deserialize, Serialize};

use crate::bucket::Bucket;

pub const TOMBSTONES_PREFIX: &str = "export/tombstones";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub script: String,
    pub class: String,
    /// The root cell scope.
    pub cell: String,
    /// The facet path, `None` for the root.
    #[serde(default)]
    pub facet: Option<String>,
    /// `None` erases every incarnation.
    #[serde(default)]
    pub incarnation: Option<u64>,
    pub erased_at_ms: i64,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub cleared_at_ms: Option<i64>,
}

impl Tombstone {
    pub fn is_active(&self) -> bool {
        self.cleared_at_ms.is_none()
    }

    /// Whether this tombstone erases `stream`: the loader's rule.
    pub fn matches(&self, stream: &StreamId) -> bool {
        self.is_active()
            && self.script == stream.script
            && self.class == stream.class
            && self.cell == stream.cell
            && self.facet == stream.facet
            && self.incarnation.is_none_or(|i| i == stream.incarnation)
    }

    /// Whether this tombstone erases the bucket scope `scope`. The bucket
    /// names neither script nor incarnation, so any active tombstone for the
    /// same cell and facet covers the scope: the reconciler, repair and
    /// backfill skip it rather than resurrect data an erasure removed.
    pub fn covers_scope(&self, scope: &str) -> bool {
        self.is_active() && scope_of(&self.cell, self.facet.as_deref()) == scope
    }

    /// `export/tombstones/<cell>/<script>/<root | f.<facet>>/<all | incarnation>.json`,
    /// each part escaped, so all tombstones of a cell list under one prefix.
    pub fn key(&self) -> String {
        let facet = match &self.facet {
            None => "root".to_string(),
            Some(f) => format!("f.{}", escape(f)),
        };
        let incarnation = self
            .incarnation
            .map_or_else(|| "all".to_string(), |i| i.to_string());
        format!(
            "{}/{}/{facet}/{incarnation}.json",
            cell_prefix(&self.cell),
            escape_script(&self.script),
        )
    }
}

/// The bucket scope of a stream: the root cell, or its facet's own LTX
/// scope under `/facets/`.
pub fn scope_of(cell: &str, facet: Option<&str>) -> String {
    match facet {
        None => cell.to_string(),
        Some(path) => {
            let names: Vec<String> = path.split('/').map(str::to_string).collect();
            crate::engine_api::facet_cell(cell, &names)
        }
    }
}

fn cell_prefix(cell: &str) -> String {
    format!("{TOMBSTONES_PREFIX}/{}", escape(cell))
}

/// An empty script (a stream from before scripts were stamped) still needs
/// a path segment.
fn escape_script(script: &str) -> String {
    if script.is_empty() {
        "-".to_string()
    } else {
        escape(script)
    }
}

/// Percent-escape everything outside the cell scope alphabet, so a path
/// segment never contains `/` and never decodes to two different values.
fn escape(part: &str) -> String {
    let mut out = String::with_capacity(part.len());
    for b in part.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'$') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Write the tombstone object. Rewriting it (to clear it) replaces it.
pub async fn put(bucket: &Bucket, tombstone: &Tombstone) -> anyhow::Result<String> {
    let key = tombstone.key();
    bucket
        .put(&key, serde_json::to_vec_pretty(tombstone)?)
        .await
        .with_context(|| format!("write tombstone {key}"))?;
    Ok(key)
}

/// Every tombstone in the bucket, cleared ones included.
pub async fn load(bucket: &Bucket) -> anyhow::Result<Vec<Tombstone>> {
    load_under(bucket, TOMBSTONES_PREFIX).await
}

async fn load_under(bucket: &Bucket, prefix: &str) -> anyhow::Result<Vec<Tombstone>> {
    let mut out = Vec::new();
    for object in bucket.list(prefix).await? {
        let key = object.location.as_ref();
        if !key.ends_with(".json") {
            continue;
        }
        let Some((bytes, _)) = bucket.get(key).await? else {
            continue;
        };
        out.push(serde_json::from_slice(&bytes).with_context(|| format!("read tombstone {key}"))?);
    }
    Ok(out)
}

/// Whether an active tombstone erases `stream`. For repair and backfill,
/// which must skip tombstoned streams. Lists one cell's tombstones only.
pub async fn is_tombstoned(bucket: &Bucket, stream: &StreamId) -> anyhow::Result<bool> {
    let tombstones = load_under(bucket, &cell_prefix(&stream.cell)).await?;
    Ok(tombstones.iter().any(|t| t.matches(stream)))
}

/// Whether an active tombstone covers the bucket scope `scope` of `cell`.
pub async fn is_scope_tombstoned(bucket: &Bucket, cell: &str, scope: &str) -> anyhow::Result<bool> {
    let tombstones = load_under(bucket, &cell_prefix(cell)).await?;
    Ok(tombstones.iter().any(|t| t.covers_scope(scope)))
}
