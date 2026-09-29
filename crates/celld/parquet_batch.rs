// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Parquet batches are written by shell tasks outside the execution boundary.
#![allow(clippy::disallowed_methods)]

//! Parquet batches in the bucket: the encoder, the object layout, the put
//! and the retention sweep that telemetry and change export share.
//!
//! A batch is one Parquet object with one row group, keyed
//! `<prefix>/<node>/<yyyy>/<mm>/<dd>/<hh>/<unix_us>-<rand>.parquet` by the
//! time it was flushed. The caller owns its schema, its prefix, its object
//! metadata and what a failed put means: [`put`] returns the error, so a
//! sink that must acknowledge delivery can, and one that sheds can log it.

use crate::bucket::Bucket;
use anyhow::bail;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::data_type::ByteArray;
use parquet::data_type::DataType;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedColumnWriter;
use parquet::file::writer::SerializedFileWriter;
use parquet::file::writer::SerializedRowGroupWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::ColumnPath;
use parquet::schema::types::SchemaDescriptor;
use std::sync::Arc;

/// The provider page and the S3 bulk-delete limit are both 1,000. Keeping
/// them equal lets a sweep delete one page without retaining a second key
/// collection or splitting one provider page across storage requests.
const SWEEP_PAGE_SIZE: usize = 1_000;

/// Encode one row group under `message_type`, ZSTD-compressed, with a bloom
/// filter on each of `bloom_filter_columns`. `write` fills every column of
/// the schema, in schema order, through [`Columns`].
///
/// The native column-writer API rather than the arrow one: the schemas are
/// flat primitives, and skipping the `arrow` feature keeps the whole arrow
/// crate stack out of the binary.
pub fn encode(
    message_type: &str,
    bloom_filter_columns: &[&str],
    write: impl FnOnce(&mut Columns<'_>) -> anyhow::Result<()>,
) -> anyhow::Result<Vec<u8>> {
    let schema = Arc::new(parse_message_type(message_type)?);
    let total = SchemaDescriptor::new(schema.clone()).num_columns();
    let mut properties =
        WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default()));
    for column in bloom_filter_columns {
        properties = properties.set_column_bloom_filter_enabled(ColumnPath::from(*column), true);
    }
    let mut writer = SerializedFileWriter::new(Vec::new(), schema, Arc::new(properties.build()))?;
    {
        let mut columns = Columns {
            group: writer.next_row_group()?,
            written: 0,
            total,
        };
        write(&mut columns)?;
        let Columns { group, written, .. } = columns;
        if written != total {
            bail!("parquet batch wrote {written} of {total} schema columns");
        }
        group.close()?;
    }
    Ok(writer.into_inner()?)
}

/// The open row group. Each call consumes the next schema column.
pub struct Columns<'a> {
    group: SerializedRowGroupWriter<'a, Vec<u8>>,
    written: usize,
    total: usize,
}

impl Columns<'_> {
    /// A `required` column: one value per row.
    pub fn required<T: DataType>(&mut self, values: &[T::T]) -> anyhow::Result<()> {
        let mut column = self.next()?;
        column.typed::<T>().write_batch(values, None, None)?;
        column.close()?;
        Ok(())
    }

    /// An `optional` column: one option per row. Definition levels mark
    /// presence (1 present, 0 null) and only the present values are
    /// packed, as the format requires.
    pub fn optional<T: DataType>(
        &mut self,
        options: impl IntoIterator<Item = Option<T::T>>,
    ) -> anyhow::Result<()> {
        let options: Vec<Option<T::T>> = options.into_iter().collect();
        let levels: Vec<i16> = options.iter().map(|value| value.is_some() as i16).collect();
        let values: Vec<T::T> = options.into_iter().flatten().collect();
        let mut column = self.next()?;
        column
            .typed::<T>()
            .write_batch(&values, Some(&levels), None)?;
        column.close()?;
        Ok(())
    }

    fn next(&mut self) -> anyhow::Result<SerializedColumnWriter<'_>> {
        let Some(column) = self.group.next_column()? else {
            bail!(
                "parquet batch wrote more than its {} schema columns",
                self.total
            );
        };
        self.written += 1;
        Ok(column)
    }
}

/// A `STRING` column value.
pub fn text(value: &str) -> ByteArray {
    ByteArray::from(value.as_bytes().to_vec())
}

/// `<prefix>/<node>/<yyyy/mm/dd/hh>/<flush_us>-<rand>.parquet`.
/// Partitioned by arrival at the flush, which is what retention prunes
/// by; event timestamps stay exact inside the file.
pub fn object_key(prefix: &str, node: &str, unix_us: i64) -> String {
    let seconds = unix_us / 1_000_000;
    let (y, m, d) = civil_from_days(seconds.div_euclid(86_400));
    let hour = seconds.rem_euclid(86_400) / 3_600;
    let tag: u32 = rand::random();
    format!("{prefix}/{node}/{y:04}/{m:02}/{d:02}/{hour:02}/{unix_us}-{tag:08x}.parquet")
}

/// A put that did not land, with the key it was for.
#[derive(Debug)]
pub struct PutError {
    pub key: String,
    pub error: anyhow::Error,
}

impl std::fmt::Display for PutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for PutError {}

/// Write one encoded batch under `prefix` for `node`, keyed by `unix_us`,
/// carrying `meta` as object metadata. Returns the key written. The batch
/// has not landed until this returns `Ok`; what a failure costs is the
/// caller's decision.
pub async fn put(
    bucket: &Bucket,
    prefix: &str,
    node: &str,
    unix_us: i64,
    bytes: Vec<u8>,
    meta: &[(&'static str, &str)],
) -> Result<String, PutError> {
    let key = object_key(prefix, node, unix_us);
    match bucket.put_with_meta(&key, bytes, meta).await {
        Ok(()) => Ok(key),
        Err(error) => Err(PutError { key, error }),
    }
}

/// One complete retention pass over `prefixes`, deleting objects whose day
/// partition is before `cutoff`. Prefix deletes over the day-partitioned
/// layout are idempotent and safe to race between nodes. A page failure ends
/// only that prefix's pass, while deletions completed from its earlier pages
/// remain effective.
pub async fn sweep_once(bucket: &Bucket, prefixes: &[&str], cutoff: (i64, u32, u32)) -> u64 {
    let mut deleted = 0u64;
    for &prefix in prefixes {
        let mut page_token = None;
        loop {
            let page = match bucket
                .objects_page(prefix, page_token, SWEEP_PAGE_SIZE)
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(%error, prefix, "retention sweep could not list");
                    break;
                }
            };
            let next_page = page.page_token;
            // Retain only the keys this page proves are expired. Fresh and
            // unparseable descriptions are released before deletion starts.
            let expired_keys: Vec<String> = page
                .objects
                .into_iter()
                .filter_map(|object| {
                    let key = object.location.as_ref();
                    expired(key, prefix, cutoff).then(|| key.to_string())
                })
                .collect();
            if !expired_keys.is_empty() {
                deleted += bucket.delete_many(&expired_keys).await.len() as u64;
            }
            match next_page {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }
    }
    deleted
}

/// The newest civil date old enough to delete: strictly before
/// `retention_days` whole days ago.
pub fn cutoff_date(now_unix_us: i64, retention_days: u32) -> (i64, u32, u32) {
    let today = (now_unix_us / 1_000_000).div_euclid(86_400);
    civil_from_days(today - retention_days as i64)
}

/// Whether an object key's day partition is older than the cutoff. Keys
/// that do not parse as `<prefix>/<node>/<yyyy>/<mm>/<dd>/...` are never
/// touched: the sweep deletes only what the layout proves is a batch with a
/// date.
pub fn expired(key: &str, prefix: &str, cutoff: (i64, u32, u32)) -> bool {
    let Some(rest) = key
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('/'))
    else {
        return false;
    };
    let mut parts = rest.split('/');
    let _node = parts.next();
    let (Some(y), Some(m), Some(d)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    let (Ok(y), Ok(m), Ok(d)) = (y.parse::<i64>(), m.parse::<u32>(), d.parse::<u32>()) else {
        return false;
    };
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return false;
    }
    (y, m, d) < cutoff
}

/// Days since the Unix epoch to a civil date (Howard Hinnant's
/// `civil_from_days`, public domain construction).
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests;
