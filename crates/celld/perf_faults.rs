// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A thread sleep is the fault: a slow disk blocks the fsyncing thread.
#![allow(clippy::disallowed_methods)]

//! Injected faults for performance runs: a slow or throttling bucket, and
//! a slow disk. Only a `perf` build has them.
//!
//! A local bucket (MinIO, or the dev store) answers in well under a
//! millisecond, so a benchmark against it measures a node that never waits
//! for storage. These wrappers restore the wait. They sit under the request
//! counters, so an injected delay is counted as the request's latency and an
//! injected 429 as a throttled request, exactly as a slow bucket would be.
//!
//! `CELLD_PERF_BUCKET_FAULTS` is a comma-separated list:
//!
//! - `read=MS`, `write=MS`, `list=MS`, `all=MS`: a fixed delay before each
//!   request of that kind (`read` is GET and HEAD; `write` is PUT,
//!   multipart, copy, and delete);
//! - `tail=MS`: plus an exponentially distributed delay with this mean, so
//!   the latency has a tail as a real bucket's does;
//! - `throttle=F`: a fraction F (0 to 1) of requests fail with a 429 before
//!   they reach the bucket;
//! - `class=A+B`: apply only to keys of these classes
//!   ([`crate::perf_stats::KeyClass`] labels, e.g. `cell_data+cell_owner`).
//!
//! `CELLD_PERF_FSYNC_DELAY_US` adds a fixed delay to every fsync the node's
//! own filesystem makes: LTX files, follower log batches, and their
//! directories. SQLite's own syncs do not pass through it.

use crate::perf_stats::{self, KeyClass};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::StreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rand::Rng as _;
use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

pub const BUCKET_FAULTS: &str = "CELLD_PERF_BUCKET_FAULTS";
pub const FSYNC_DELAY: &str = "CELLD_PERF_FSYNC_DELAY_US";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Read,
    Write,
    List,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BucketFaults {
    read_ms: u64,
    write_ms: u64,
    list_ms: u64,
    tail_ms: f64,
    throttle: f64,
    /// Empty applies to every class.
    classes: Vec<KeyClass>,
}

impl BucketFaults {
    pub fn parse(spec: &str) -> anyhow::Result<Self> {
        let mut faults = Self::default();
        for part in spec
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            let (name, value) = part
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("{BUCKET_FAULTS}: {part:?} is not NAME=VALUE"))?;
            let millis = || -> anyhow::Result<u64> {
                value
                    .trim_end_matches("ms")
                    .parse()
                    .map_err(|_| anyhow::anyhow!("{BUCKET_FAULTS}: {name} takes milliseconds"))
            };
            match name {
                "read" => faults.read_ms = millis()?,
                "write" => faults.write_ms = millis()?,
                "list" => faults.list_ms = millis()?,
                "all" => {
                    let all = millis()?;
                    faults.read_ms = all;
                    faults.write_ms = all;
                    faults.list_ms = all;
                }
                "tail" => faults.tail_ms = millis()? as f64,
                "throttle" => {
                    let fraction: f64 = value.parse().map_err(|_| {
                        anyhow::anyhow!("{BUCKET_FAULTS}: throttle takes a fraction")
                    })?;
                    anyhow::ensure!(
                        (0.0..=1.0).contains(&fraction),
                        "{BUCKET_FAULTS}: throttle must be between 0 and 1"
                    );
                    faults.throttle = fraction;
                }
                "class" => {
                    for label in value.split('+') {
                        let class = KeyClass::ALL
                            .iter()
                            .find(|class| class.label() == label)
                            .ok_or_else(|| {
                                anyhow::anyhow!("{BUCKET_FAULTS}: unknown key class {label:?}")
                            })?;
                        faults.classes.push(*class);
                    }
                }
                _ => anyhow::bail!("{BUCKET_FAULTS}: unknown fault {name:?}"),
            }
        }
        Ok(faults)
    }

    fn applies(&self, key: &str) -> bool {
        self.classes.is_empty() || self.classes.contains(&perf_stats::key_class(key))
    }

    /// Wait out the injected delay, then fail the request if it is throttled.
    async fn before(&self, kind: Kind, key: &str) -> object_store::Result<()> {
        if !self.applies(key) {
            return Ok(());
        }
        let base = match kind {
            Kind::Read => self.read_ms,
            Kind::Write => self.write_ms,
            Kind::List => self.list_ms,
        } as f64;
        let (tail, throttled) = {
            let mut rng = crate::asyncrt::rng("perf_faults");
            let tail = if self.tail_ms > 0.0 {
                // Exponential with mean `tail_ms`, by inversion.
                let uniform: f64 = rng.gen_range(f64::EPSILON..1.0);
                -self.tail_ms * uniform.ln()
            } else {
                0.0
            };
            (tail, self.throttle > 0.0 && rng.gen_bool(self.throttle))
        };
        let delay = base + tail;
        if delay > 0.0 {
            crate::asyncrt::sleep(Duration::from_secs_f64(delay / 1_000.0)).await;
        }
        if throttled {
            return Err(object_store::Error::Generic {
                store: "PerfFaults",
                source: "injected 429 Too Many Requests (SlowDown)".into(),
            });
        }
        Ok(())
    }
}

/// The bucket faults this process was started with, parsed once.
pub fn bucket_faults() -> Option<&'static BucketFaults> {
    static FAULTS: OnceLock<Option<BucketFaults>> = OnceLock::new();
    FAULTS
        .get_or_init(|| {
            let spec = std::env::var(BUCKET_FAULTS).ok()?;
            // `env_vars::validate` already refused a bad value at startup.
            BucketFaults::parse(&spec).ok()
        })
        .as_ref()
}

/// Wrap `inner` with this process's bucket faults, if it has any.
pub fn inject(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    match bucket_faults() {
        Some(faults) => Arc::new(FaultyStore {
            inner,
            faults: faults.clone(),
        }),
        None => inner,
    }
}

pub fn inject_paginated(inner: Arc<dyn PaginatedListStore>) -> Arc<dyn PaginatedListStore> {
    match bucket_faults() {
        Some(faults) => Arc::new(FaultyPaginated {
            inner,
            faults: faults.clone(),
        }),
        None => inner,
    }
}

/// Validate the fault variables; called from `env_vars::validate`.
pub fn validate() -> anyhow::Result<()> {
    if let Ok(spec) = std::env::var(BUCKET_FAULTS) {
        BucketFaults::parse(&spec)?;
    }
    fsync_delay()?;
    Ok(())
}

fn fsync_delay() -> anyhow::Result<Option<Duration>> {
    Ok(crate::env_vars::optional::<u64>(FSYNC_DELAY)?.map(Duration::from_micros))
}

#[derive(Debug)]
struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    faults: BucketFaults,
}

impl std::fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PerfFaults({})", self.inner)
    }
}

fn prefix_key(prefix: Option<&Path>) -> String {
    prefix.map_or_else(String::new, |prefix| prefix.to_string())
}

/// A listing whose first item waits out the delay; a throttled listing
/// yields one error.
fn delayed_listing(
    faults: BucketFaults,
    key: String,
    inner: BoxStream<'static, object_store::Result<ObjectMeta>>,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    // `once` yields one gate, so the inner listing is taken exactly once.
    let mut inner = Some(inner);
    futures_util::stream::once(async move { faults.before(Kind::List, &key).await })
        .flat_map(move |gate| match (gate, inner.take()) {
            (Ok(()), Some(inner)) => inner,
            (Ok(()), None) => futures_util::stream::empty().boxed(),
            (Err(error), _) => futures_util::stream::once(async move { Err(error) }).boxed(),
        })
        .boxed()
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        self.faults.before(Kind::Write, location.as_ref()).await?;
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.faults.before(Kind::Write, location.as_ref()).await?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.faults.before(Kind::Write, location.as_ref()).await?;
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.faults.before(Kind::Write, location.as_ref()).await?;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.faults.before(Kind::Read, location.as_ref()).await?;
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.faults.before(Kind::Read, location.as_ref()).await?;
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        self.faults.before(Kind::Read, location.as_ref()).await?;
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.faults.before(Kind::Read, location.as_ref()).await?;
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.faults.before(Kind::Read, location.as_ref()).await?;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.faults.before(Kind::Write, location.as_ref()).await?;
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        delayed_listing(
            self.faults.clone(),
            prefix_key(prefix),
            self.inner.list(prefix),
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        delayed_listing(
            self.faults.clone(),
            prefix_key(prefix),
            self.inner.list_with_offset(prefix, offset),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.faults.before(Kind::List, &prefix_key(prefix)).await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.faults.before(Kind::Write, to.as_ref()).await?;
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.faults.before(Kind::Write, to.as_ref()).await?;
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.faults.before(Kind::Write, to.as_ref()).await?;
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.faults.before(Kind::Write, to.as_ref()).await?;
        self.inner.rename_if_not_exists(from, to).await
    }
}

struct FaultyPaginated {
    inner: Arc<dyn PaginatedListStore>,
    faults: BucketFaults,
}

#[async_trait]
impl PaginatedListStore for FaultyPaginated {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        self.faults.before(Kind::List, prefix.unwrap_or("")).await?;
        self.inner.list_paginated(prefix, options).await
    }
}

/// Wrap the node filesystem so every fsync takes at least the configured
/// delay, if `CELLD_PERF_FSYNC_DELAY_US` is set.
pub fn filesystem(inner: Arc<dyn celld_ltx::FileSystem>) -> Arc<dyn celld_ltx::FileSystem> {
    match fsync_delay() {
        Ok(Some(delay)) if !delay.is_zero() => Arc::new(SlowFsync { inner, delay }),
        _ => inner,
    }
}

struct SlowFsync {
    inner: Arc<dyn celld_ltx::FileSystem>,
    delay: Duration,
}

struct SlowFile {
    inner: celld_ltx::HostFile,
    delay: Duration,
}

impl celld_ltx::HostFileIo for SlowFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        self.inner.read_exact_at(offset, len)
    }

    fn sync_all(&mut self) -> std::io::Result<()> {
        std::thread::sleep(self.delay);
        self.inner.sync_all()
    }

    fn file_len(&mut self) -> std::io::Result<u64> {
        self.inner.file_len()
    }
}

impl SlowFsync {
    fn slow(&self, file: celld_ltx::HostFile) -> celld_ltx::HostFile {
        celld_ltx::HostFile::from_io(SlowFile {
            inner: file,
            delay: self.delay,
        })
    }
}

impl celld_ltx::FileSystem for SlowFsync {
    fn temporary_file(
        &self,
        directory: Option<&std::path::Path>,
    ) -> std::io::Result<celld_ltx::HostFile> {
        self.inner
            .temporary_file(directory)
            .map(|file| self.slow(file))
    }

    fn read(&self, path: &std::path::Path) -> std::io::Result<Vec<u8>> {
        self.inner.read(path)
    }

    fn read_dir(&self, path: &std::path::Path) -> std::io::Result<Vec<celld_ltx::HostDirEntry>> {
        self.inner.read_dir(path)
    }

    fn metadata(&self, path: &std::path::Path) -> std::io::Result<celld_ltx::HostMetadata> {
        self.inner.metadata(path)
    }

    fn create(&self, path: &std::path::Path) -> std::io::Result<celld_ltx::HostFile> {
        self.inner.create(path).map(|file| self.slow(file))
    }

    fn open(&self, path: &std::path::Path) -> std::io::Result<celld_ltx::HostFile> {
        self.inner.open(path).map(|file| self.slow(file))
    }

    fn write(&self, path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write(path, bytes)
    }

    fn sync_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        std::thread::sleep(self.delay);
        self.inner.sync_all(path)
    }

    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        self.inner.rename(from, to)
    }

    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.inner.create_dir_all(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fault_spec_parses() {
        let faults = BucketFaults::parse(
            "read=15, write=30ms,list=20,tail=40,throttle=0.05,class=cell_data+nodes",
        )
        .unwrap();
        assert_eq!(faults.read_ms, 15);
        assert_eq!(faults.write_ms, 30);
        assert_eq!(faults.list_ms, 20);
        assert_eq!(faults.tail_ms, 40.0);
        assert_eq!(faults.throttle, 0.05);
        assert_eq!(faults.classes, vec![KeyClass::CellData, KeyClass::Nodes]);
        assert!(faults.applies("cells/a/ltx/x"));
        assert!(!faults.applies("cells/a/own.json"));
        assert_eq!(BucketFaults::parse("all=5").unwrap().list_ms, 5);
        assert!(BucketFaults::parse("throttle=2").is_err());
        assert!(BucketFaults::parse("read").is_err());
        assert!(BucketFaults::parse("class=nope").is_err());
        assert!(BucketFaults::parse("speed=1").is_err());
    }
}
