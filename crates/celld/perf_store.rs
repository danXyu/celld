// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! An object store that counts every request it forwards.
//!
//! Every store a node builds is wrapped once, where it is built
//! ([`counted`], [`counted_paginated`]), so each bucket request is recorded
//! in [`crate::perf_stats`] by operation, key class, and outcome, with its
//! latency and payload bytes. Callers are unchanged: each method forwards to
//! the same method of the inner store, including the ones a backend
//! overrides (bulk delete, offset listing), so the requests on the wire are
//! the ones the unwrapped store would send.
//!
//! A GET's latency ends when its headers arrive, and its bytes are the range
//! it returned; the body streams afterwards. A listing is one call however
//! many pages the backend fetches for it.

use crate::perf_stats::{self, BucketOp, KeyClass, Outcome};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::StreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::ops::Range;
use std::sync::Arc;

/// Wrap a store a node builds: a `perf` build's injected faults, if the
/// process has any, under the request counters.
pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    #[cfg(feature = "perf")]
    let inner = crate::perf_faults::inject(inner);
    counted(inner)
}

/// [`wrap`] for a paginated listing client.
pub fn wrap_paginated(inner: Arc<dyn PaginatedListStore>) -> Arc<dyn PaginatedListStore> {
    #[cfg(feature = "perf")]
    let inner = crate::perf_faults::inject_paginated(inner);
    counted_paginated(inner)
}

/// Wrap `inner` so its requests are counted.
pub fn counted(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(CountedStore { inner })
}

/// Wrap a paginated listing client so its requests are counted.
pub fn counted_paginated(inner: Arc<dyn PaginatedListStore>) -> Arc<dyn PaginatedListStore> {
    Arc::new(CountedPaginated { inner })
}

#[derive(Debug)]
struct CountedStore {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for CountedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counted({})", self.inner)
    }
}

/// Time one request and record it against `key`.
async fn measure<T>(
    op: BucketOp,
    key: &str,
    request: impl std::future::Future<Output = object_store::Result<T>>,
    bytes: impl FnOnce(&T) -> u64,
) -> object_store::Result<T> {
    let started = crate::asyncrt::mono_us();
    let result = request.await;
    let elapsed = crate::asyncrt::mono_us().saturating_sub(started);
    let moved = result.as_ref().map_or(0, bytes);
    perf_stats::bucket_request(
        op,
        perf_stats::key_class(key),
        perf_stats::outcome_of(&result),
        moved,
        elapsed,
    );
    result
}

fn put_op(options: &PutOptions) -> BucketOp {
    match options.mode {
        PutMode::Overwrite => BucketOp::Put,
        PutMode::Create => BucketOp::PutCreate,
        PutMode::Update(_) => BucketOp::PutUpdate,
    }
}

fn prefix_key(prefix: Option<&Path>) -> &str {
    prefix.map_or("", Path::as_ref)
}

/// Count a listing when its stream ends, with every object it yielded.
fn counted_listing(
    op: BucketOp,
    class: KeyClass,
    stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    struct Listing {
        op: BucketOp,
        class: KeyClass,
        started: u64,
        outcome: Outcome,
        recorded: bool,
    }
    impl Listing {
        fn finish(&mut self) {
            if !self.recorded {
                self.recorded = true;
                let elapsed = crate::asyncrt::mono_us().saturating_sub(self.started);
                perf_stats::bucket_request(self.op, self.class, self.outcome, 0, elapsed);
            }
        }
    }
    impl Drop for Listing {
        fn drop(&mut self) {
            self.finish();
        }
    }
    let mut listing = Listing {
        op,
        class,
        started: crate::asyncrt::mono_us(),
        outcome: Outcome::Ok,
        recorded: false,
    };
    stream
        .map(move |item| {
            if let Err(error) = &item {
                listing.outcome = perf_stats::outcome_of::<()>(&Err(clone_error(error)));
                listing.finish();
            }
            item
        })
        .boxed()
}

/// A stand-in carrying the classification-relevant shape of `error`;
/// `object_store::Error` is not `Clone`.
fn clone_error(error: &object_store::Error) -> object_store::Error {
    match error {
        object_store::Error::NotFound { path, .. } => object_store::Error::NotFound {
            path: path.clone(),
            source: error.to_string().into(),
        },
        object_store::Error::Precondition { path, .. } => object_store::Error::Precondition {
            path: path.clone(),
            source: error.to_string().into(),
        },
        _ => object_store::Error::Generic {
            store: "Counted",
            source: error.to_string().into(),
        },
    }
}

#[async_trait]
impl ObjectStore for CountedStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        let bytes = payload.content_length() as u64;
        measure(
            BucketOp::Put,
            location.as_ref(),
            self.inner.put(location, payload),
            |_| bytes,
        )
        .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let bytes = payload.content_length() as u64;
        measure(
            put_op(&options),
            location.as_ref(),
            self.inner.put_opts(location, payload, options),
            |_| bytes,
        )
        .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        measure(
            BucketOp::Multipart,
            location.as_ref(),
            self.inner.put_multipart(location),
            |_| 0,
        )
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        measure(
            BucketOp::Multipart,
            location.as_ref(),
            self.inner.put_multipart_opts(location, options),
            |_| 0,
        )
        .await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        measure(
            BucketOp::Get,
            location.as_ref(),
            self.inner.get(location),
            |result| result.range.end.saturating_sub(result.range.start),
        )
        .await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let op = if options.head {
            BucketOp::Head
        } else if options.range.is_some() {
            BucketOp::GetRange
        } else {
            BucketOp::Get
        };
        let head = options.head;
        measure(
            op,
            location.as_ref(),
            self.inner.get_opts(location, options),
            |result| {
                if head {
                    0
                } else {
                    result.range.end.saturating_sub(result.range.start)
                }
            },
        )
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        measure(
            BucketOp::GetRange,
            location.as_ref(),
            self.inner.get_range(location, range),
            |bytes| bytes.len() as u64,
        )
        .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        measure(
            BucketOp::GetRange,
            location.as_ref(),
            self.inner.get_ranges(location, ranges),
            |parts| parts.iter().map(|part| part.len() as u64).sum(),
        )
        .await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        measure(
            BucketOp::Head,
            location.as_ref(),
            self.inner.head(location),
            |_| 0,
        )
        .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        measure(
            BucketOp::Delete,
            location.as_ref(),
            self.inner.delete(location),
            |_| 0,
        )
        .await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        // A backend batches these into bulk requests; each key deleted is
        // one count, so the tally matches the per-key path.
        self.inner
            .delete_stream(locations)
            .map(|result| {
                let (class, outcome) = match &result {
                    Ok(path) => (perf_stats::key_class(path.as_ref()), Outcome::Ok),
                    Err(error) => (
                        KeyClass::Other,
                        perf_stats::outcome_of::<()>(&Err(clone_error(error))),
                    ),
                };
                perf_stats::bucket_request(BucketOp::Delete, class, outcome, 0, 0);
                result
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        counted_listing(
            BucketOp::List,
            perf_stats::key_class(prefix_key(prefix)),
            self.inner.list(prefix),
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        counted_listing(
            BucketOp::List,
            perf_stats::key_class(prefix_key(prefix)),
            self.inner.list_with_offset(prefix, offset),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        measure(
            BucketOp::ListDelimited,
            prefix_key(prefix),
            self.inner.list_with_delimiter(prefix),
            |_| 0,
        )
        .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        measure(
            BucketOp::Copy,
            to.as_ref(),
            self.inner.copy(from, to),
            |_| 0,
        )
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        measure(
            BucketOp::Copy,
            to.as_ref(),
            self.inner.rename(from, to),
            |_| 0,
        )
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        measure(
            BucketOp::Copy,
            to.as_ref(),
            self.inner.copy_if_not_exists(from, to),
            |_| 0,
        )
        .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        measure(
            BucketOp::Copy,
            to.as_ref(),
            self.inner.rename_if_not_exists(from, to),
            |_| 0,
        )
        .await
    }
}

struct CountedPaginated {
    inner: Arc<dyn PaginatedListStore>,
}

#[async_trait]
impl PaginatedListStore for CountedPaginated {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        measure(
            BucketOp::ListPaginated,
            prefix.unwrap_or(""),
            self.inner.list_paginated(prefix, options),
            |_| 0,
        )
        .await
    }
}
