// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use parquet::data_type::BoolType;
use parquet::data_type::ByteArrayType;
use parquet::data_type::Int64Type;
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use parquet::record::Field;

const MESSAGE_TYPE: &str = "
message batch_test {
  required binary id (STRING);
  required int64 at;
  optional binary note (STRING);
  optional boolean flag;
}";

/// 2026-09-29T12:34:56Z.
const NOW_US: i64 = 1_790_685_296_000_000;

fn encode_rows(rows: &[(&str, i64, Option<&str>, Option<bool>)]) -> Vec<u8> {
    encode(MESSAGE_TYPE, &["id"], |columns| {
        columns.required::<ByteArrayType>(&rows.iter().map(|r| text(r.0)).collect::<Vec<_>>())?;
        columns.required::<Int64Type>(&rows.iter().map(|r| r.1).collect::<Vec<_>>())?;
        columns.optional::<ByteArrayType>(rows.iter().map(|r| r.2.map(text)))?;
        columns.optional::<BoolType>(rows.iter().map(|r| r.3))?;
        Ok(())
    })
    .unwrap()
}

fn read_rows(bytes: Vec<u8>) -> Vec<Vec<(String, Field)>> {
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
    reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            row.unwrap()
                .get_column_iter()
                .map(|(name, field)| (name.clone(), field.clone()))
                .collect()
        })
        .collect()
}

#[test]
fn encode_round_trips_required_and_optional_columns() {
    let bytes = encode_rows(&[
        ("a", 1, Some("first"), None),
        ("b", -2, None, Some(true)),
        ("c", 3, Some(""), Some(false)),
    ]);
    let rows = read_rows(bytes);
    assert_eq!(rows.len(), 3);
    let row = |i: usize| rows[i].iter().map(|(_, f)| f.clone()).collect::<Vec<_>>();
    assert_eq!(
        row(0),
        [
            Field::Str("a".into()),
            Field::Long(1),
            Field::Str("first".into()),
            Field::Null,
        ]
    );
    assert_eq!(
        row(1),
        [
            Field::Str("b".into()),
            Field::Long(-2),
            Field::Null,
            Field::Bool(true),
        ]
    );
    assert_eq!(
        row(2),
        [
            Field::Str("c".into()),
            Field::Long(3),
            Field::Str("".into()),
            Field::Bool(false),
        ]
    );
    let names: Vec<_> = rows[0].iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, ["id", "at", "note", "flag"]);
}

#[test]
fn encode_writes_an_empty_batch() {
    let rows = read_rows(encode_rows(&[]));
    assert!(rows.is_empty());
}

#[test]
fn encode_compresses_and_blooms_only_the_named_columns() {
    let bytes = encode_rows(&[("a", 1, None, None)]);
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
    let group = reader.metadata().row_group(0);
    for (index, column) in group.columns().iter().enumerate() {
        assert_eq!(
            column.compression(),
            Compression::ZSTD(ZstdLevel::default()),
            "column {index}"
        );
        assert_eq!(
            column.bloom_filter_offset().is_some(),
            index == 0,
            "bloom filter on column {index}"
        );
    }
}

#[test]
fn encode_rejects_a_batch_that_skips_columns() {
    let error = encode(MESSAGE_TYPE, &[], |columns| {
        columns.required::<ByteArrayType>(&[text("a")])?;
        columns.required::<Int64Type>(&[1])?;
        Ok(())
    })
    .unwrap_err();
    assert!(error.to_string().contains("wrote 2 of 4"), "{error}");
}

#[test]
fn encode_rejects_a_batch_with_extra_columns() {
    let error = encode(MESSAGE_TYPE, &[], |columns| {
        columns.required::<ByteArrayType>(&[text("a")])?;
        columns.required::<Int64Type>(&[1])?;
        columns.optional::<ByteArrayType>([None])?;
        columns.optional::<BoolType>([None])?;
        columns.optional::<BoolType>([None])?;
        Ok(())
    })
    .unwrap_err();
    assert!(error.to_string().contains("more than its 4"), "{error}");
}

#[test]
fn encode_surfaces_the_closure_error() {
    let error = encode(MESSAGE_TYPE, &[], |_| anyhow::bail!("row source failed")).unwrap_err();
    assert_eq!(error.to_string(), "row source failed");
}

#[test]
fn encode_rejects_a_bad_schema() {
    assert!(encode("message {", &[], |_| Ok(())).is_err());
}

#[test]
fn object_key_partitions_by_flush_hour() {
    let key = object_key("export/changes", "node-1", NOW_US);
    let prefix = format!("export/changes/node-1/2026/09/29/12/{NOW_US}-");
    assert!(key.starts_with(&prefix), "{key}");
    let tag = key[prefix.len()..].strip_suffix(".parquet").unwrap();
    assert_eq!(tag.len(), 8);
    assert!(tag.bytes().all(|b| b.is_ascii_hexdigit()), "{key}");
}

#[test]
fn object_key_handles_the_epoch() {
    assert!(object_key("p", "n", 0).starts_with("p/n/1970/01/01/00/0-"));
}

#[test]
fn civil_from_days_matches_known_dates() {
    assert_eq!(civil_from_days(0), (1970, 1, 1));
    assert_eq!(civil_from_days(-1), (1969, 12, 31));
    assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    assert_eq!(civil_from_days(20_725), (2026, 9, 29));
}

#[test]
fn cutoff_date_is_retention_days_before_today() {
    assert_eq!(cutoff_date(NOW_US, 0), (2026, 9, 29));
    assert_eq!(cutoff_date(NOW_US, 30), (2026, 8, 30));
}

#[test]
fn expired_reads_only_its_own_layout() {
    let cutoff = (2026, 8, 30);
    assert!(expired("p/n/2026/08/29/23/1-a.parquet", "p", cutoff));
    assert!(!expired("p/n/2026/08/30/00/1-a.parquet", "p", cutoff));
    // Another prefix, a prefix that only shares a stem, and keys the layout
    // does not prove are dated batches are never touched.
    assert!(!expired("q/n/2020/01/01/00/1-a.parquet", "p", cutoff));
    assert!(!expired("pp/n/2020/01/01/00/1-a.parquet", "p", cutoff));
    assert!(!expired("p/n/2020/13/01/00/1-a.parquet", "p", cutoff));
    assert!(!expired("p/n/2020/01/00/00/1-a.parquet", "p", cutoff));
    assert!(!expired("p/n/2020/01", "p", cutoff));
    assert!(!expired("p/n/year/01/01/00/1-a.parquet", "p", cutoff));
    assert!(!expired("p", "p", cutoff));
}

/// The dev store does its I/O through `asyncrt::blocking`, whose process
/// domain binds to the first runtime that touches it. A per-test runtime
/// would leave later tests on a dropped one, so every dev-store test runs
/// on this process-lifetime runtime.
fn run(future: impl std::future::Future<Output = ()>) {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    });
    crate::asyncrt::set_host_handle(runtime.handle().clone());
    runtime.block_on(future);
}

fn dev_bucket() -> (tempfile::TempDir, Bucket) {
    let dir = tempfile::tempdir().unwrap();
    let bucket = Bucket::open_dev(&dir.path().join("bucket.sqlite")).unwrap();
    (dir, bucket)
}

#[test]
fn put_returns_the_key_it_wrote() {
    run(async {
        let (_dir, bucket) = dev_bucket();
        let bytes = encode_rows(&[("a", 1, None, None)]);
        let key = put(
            &bucket,
            "export/changes",
            "node-1",
            NOW_US,
            bytes.clone(),
            &[("celld-schema", "v1")],
        )
        .await
        .unwrap();
        assert!(
            key.starts_with("export/changes/node-1/2026/09/29/12/"),
            "{key}"
        );
        let (stored, _) = bucket.get(&key).await.unwrap().expect("object written");
        assert_eq!(stored.as_ref(), bytes.as_slice());
        let (_, schema) = bucket
            .head_with_meta(&key, "celld-schema")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(schema.as_deref(), Some("v1"));
    });
}

#[test]
fn sweep_once_deletes_only_expired_keys_under_its_prefixes() {
    run(async {
        let (_dir, bucket) = dev_bucket();
        let day = 86_400 * 1_000_000;
        let old = NOW_US - 40 * day;
        let mut keep = Vec::new();
        for prefix in ["a/one", "a/two", "b"] {
            put(&bucket, prefix, "n", old, vec![1], &[]).await.unwrap();
            keep.push(
                put(&bucket, prefix, "n", NOW_US, vec![2], &[])
                    .await
                    .unwrap(),
            );
        }
        let undated = "a/one/n/not-a-date.parquet".to_string();
        bucket.put(&undated, vec![3]).await.unwrap();
        keep.push(undated);
        let b_old = bucket.list("b").await.unwrap();
        assert_eq!(b_old.len(), 2);

        let deleted = sweep_once(&bucket, &["a/one", "a/two"], cutoff_date(NOW_US, 30)).await;
        assert_eq!(deleted, 2);

        let mut left: Vec<String> = Vec::new();
        for prefix in ["a", "b"] {
            for object in bucket.list(prefix).await.unwrap() {
                left.push(object.location.to_string());
            }
        }
        left.sort();
        // The unswept prefix keeps its expired object.
        let mut expected: Vec<String> = keep;
        expected.extend(
            b_old
                .into_iter()
                .map(|object| object.location.to_string())
                .filter(|key| key.contains("/2026/08/")),
        );
        expected.sort();
        assert_eq!(left, expected);
    });
}

#[test]
fn sweep_once_with_no_prefixes_deletes_nothing() {
    run(async {
        let (_dir, bucket) = dev_bucket();
        put(&bucket, "a", "n", 0, vec![1], &[]).await.unwrap();
        assert_eq!(sweep_once(&bucket, &[], cutoff_date(NOW_US, 0)).await, 0);
        assert_eq!(bucket.list("a").await.unwrap().len(), 1);
    });
}
