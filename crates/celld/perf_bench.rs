// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Fixtures for `benches/perf_components.rs`, absent from ordinary builds.
//!
//! Each fixture sets up the state a production function expects, reaching
//! crate-private entry points where there is no public one. None of them
//! changes what the node does.
#![allow(clippy::disallowed_methods)] // Offline benchmark, outside Actor execution.

use crate::assets::AssetResolver;
use crate::bucket::Bucket;
use crate::js::{Compat, Worker, WorkerConfig, WorkerConfigOptions};
use crate::node_log::{AppendBatch, AppendReq, AppendResp, Entry, FollowerState, FollowerStore};
use crate::protocol::{AssetConfig, AssetEntry, AssetIndex, AssetManifestRef};
use crate::storage;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// One cell database opened the way an activation opens it: the production
/// `storage::open` (WAL, `synchronous=NORMAL`, the SQL authorizer and the
/// statement-cache budget) on a file in a temporary directory, reached
/// through the same per-isolate `Cells` a turn installs.
///
/// The fixture keeps its `Cells` installed on the creating thread for its
/// whole life, so it must stay on that thread, as an isolate's cells do.
pub struct StorageFixture {
    installed: Option<storage::Installed>,
    // Boxed so the installed pointer stays valid when the fixture moves.
    _cells: Box<storage::Cells>,
    _directory: tempfile::TempDir,
}

impl StorageFixture {
    /// The scope every storage case uses.
    pub const SCOPE: &'static str = "Bench:storage";

    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let cells = Box::new(storage::Cells::default());
        let installed = cells.install();
        let path = directory.path().join("cell.sqlite");
        storage::open(Self::SCOPE, path.to_str().unwrap()).unwrap();
        Self {
            installed: Some(installed),
            _cells: cells,
            _directory: directory,
        }
    }
}

impl Default for StorageFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for StorageFixture {
    fn drop(&mut self) {
        // Close while the cells are still installed, then restore whatever
        // was installed before; the cells and the file go after that.
        storage::close(Self::SCOPE);
        drop(self.installed.take());
    }
}

/// What one drained `ctx.storage.sql.exec(...)` call produced.
pub struct SqlRun {
    pub rows: usize,
    pub rows_written: u64,
    /// Whether the statement came from the cell's statement cache.
    pub reused: bool,
}

/// Run `query` through the op pair a turn's `sql.exec()` uses
/// (`sql_cursor_start_values`, then `sql_cursor_next` until done) and drain
/// every row. The rows are copied out of SQLite as they are for V8; the
/// conversion to JavaScript values is not included.
pub fn sql_run(scope: &str, query: &str, binds: &[rusqlite::types::Value]) -> SqlRun {
    let (cursor, _columns, first, mut rows_written, reused) =
        storage::sql_cursor_start_values(scope, query, binds).unwrap();
    let mut rows = 0;
    if first.is_some() {
        rows += 1;
        loop {
            match storage::sql_cursor_next(cursor).unwrap() {
                (Some(_), _) => rows += 1,
                (None, written) => {
                    rows_written = written;
                    break;
                }
            }
        }
    }
    SqlRun {
        rows,
        rows_written,
        reused,
    }
}

/// A follower's store of one leader fragment, on a real directory.
///
/// The fragment is adopted up front with `FollowerStore::persist`, which is
/// what a successful adoption leaves behind, so no bucket record is needed.
/// Every batch it builds is contiguous with the last one and carries a
/// truncate watermark just below its first entry, as a leader whose earlier
/// entries reached the bucket sends. The follower then deletes the previous
/// batch file, so the directory stays one file deep however long a run is.
pub struct FollowerFixture {
    store: FollowerStore,
    next_seq: u64,
    payload: Vec<u8>,
    _directory: tempfile::TempDir,
}

impl FollowerFixture {
    const LEADER: &'static str = "bench-leader/g1";
    const EPOCH: u64 = 1;

    /// Must be called inside a Tokio runtime: the store takes the node's
    /// filesystem from the process execution domain.
    pub fn new(entry_bytes: usize) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = FollowerStore::new(directory.path(), None, "bench-follower");
        store
            .persist(
                Self::LEADER,
                FollowerState {
                    fragment_epoch: Self::EPOCH,
                    ..FollowerState::default()
                },
            )
            .unwrap();
        Self {
            store,
            next_seq: 1,
            payload: (0..entry_bytes).map(|n| n as u8).collect(),
            _directory: directory,
        }
    }

    /// The next `frames` frames of one entry each, contiguous with the last
    /// batch.
    pub fn batch(&mut self, frames: usize) -> (AppendBatch, u64) {
        let truncate_to = self.next_seq - 1;
        let mut frames = (0..frames).map(|_| {
            let seq = self.next_seq;
            self.next_seq += 1;
            append_req(Self::LEADER, Self::EPOCH, truncate_to, seq, &self.payload)
        });
        let mut batch = AppendBatch::new(frames.next().unwrap());
        for frame in frames {
            assert!(batch.try_push(frame).is_ok());
        }
        (batch, self.next_seq - 1)
    }

    pub fn store(&self) -> &FollowerStore {
        &self.store
    }
}

/// One leader frame carrying one entry.
pub fn append_req(leader: &str, epoch: u64, truncate_to: u64, seq: u64, bytes: &[u8]) -> AppendReq {
    AppendReq {
        leader: leader.to_string(),
        epoch,
        truncate_to,
        entries: vec![Entry {
            seq,
            cell: format!("Bench:{}", seq % 64),
            cell_epoch: 1,
            txid: seq,
            bytes: bytes.to_vec(),
        }],
    }
}

/// Whether every answer confirmed the batch through `last`.
pub fn confirmed(answers: &[AppendResp], last: u64) -> bool {
    answers.iter().all(|answer| answer.ok && answer.end == last)
}

/// A Worker whose default export answers `ok` at once.
pub const HELLO_WORKER: &str = "export default { fetch() { return new Response(\"ok\"); } };\n";

/// A synthetic module of about `target_bytes`, standing in for a large
/// bundled Worker (no example in the repository ships a built bundle).
///
/// It mixes top-level data that module evaluation must build with functions
/// that V8 only pre-parses, which is how a bundler's output looks to the
/// compiler. Each block is distinct, so no cache can share work between them.
pub fn large_worker(target_bytes: usize) -> String {
    let mut source = String::with_capacity(target_bytes + 4096);
    let mut block = 0_usize;
    while source.len() < target_bytes {
        source.push_str(&format!(
            "const table_{block} = {{ id: {block}, name: \"entry-{block}\", \
             tags: [\"alpha-{block}\", \"beta-{block}\"], weight: {block}.5 }};\n\
             function handler_{block}(input, options) {{\n\
             \x20 let acc = (input | 0) + table_{block}.id;\n\
             \x20 for (let k = 0; k < (options?.rounds ?? 4); k++) {{\n\
             \x20   acc = (acc * 31 + k) % 1000003;\n\
             \x20 }}\n\
             \x20 return `handler-{block}:${{acc}}:${{table_{block}.name}}`;\n\
             }}\n\
             class Model_{block} {{\n\
             \x20 constructor(value) {{ this.value = value; }}\n\
             \x20 render() {{ return handler_{block}(this.value, {{ rounds: 2 }}); }}\n\
             }}\n"
        ));
        block += 1;
    }
    source.push_str(&format!(
        "const models = [new Model_0(1), new Model_{last}(2)];\n\
         export default {{ fetch() {{ return new Response(models[0].render()); }} }};\n",
        last = block - 1
    ));
    source
}

/// The configuration a deployment builds for a plain module Worker.
pub fn worker_config(src: String) -> Arc<WorkerConfig> {
    Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src,
        script_name: "bench".to_string(),
        do_classes: Vec::new(),
        bindings: Vec::new(),
        r2_bindings: Vec::new(),
        d1_bindings: Vec::new(),
        kv_bindings: Vec::new(),
        queue_bindings: Vec::new(),
        queue_consumers: Vec::new(),
        workflow_bindings: Vec::new(),
        vars: Vec::new(),
        node: "bench-node".to_string(),
        modules: Vec::new(),
        compat: Compat::default(),
    }))
}

/// Start V8 once for the process, as the node does.
pub fn init_v8() {
    crate::js::Engine::init();
}

/// A loaded Worker driven one fetch at a time through `Worker::turn_begin`,
/// the entry the runtime's stateless drive calls. Only a handler that
/// answers inside its first turn fits: the fixture checks that the turn left
/// nothing in flight rather than running the runtime's wake loop.
pub struct TurnFixture {
    worker: Worker,
}

impl TurnFixture {
    pub fn new(src: &str) -> Self {
        init_v8();
        Self {
            worker: Worker::load_config(worker_config(src.to_string())).unwrap(),
        }
    }

    /// One GET, its first turn and the end of the event. Returns the
    /// response status and body length.
    pub fn fetch(&mut self) -> (u16, usize) {
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Fetch {
            queued_at: std::time::Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url: "http://bench.local/".to_string(),
            method: "GET".to_string(),
            body: crate::js::RequestBody::Bytes(bytes::Bytes::new()),
            headers: Vec::new(),
            request_id: None,
            tail_report: None,
            reply,
        };
        let (entry, ops) = self.worker.turn_begin(job, None);
        assert!(ops.is_empty(), "the handler started host work");
        if let Some(mut entry) = entry {
            assert!(entry.finished(), "the handler did not finish in one turn");
            entry.finish_tail_report();
            entry.abandon();
        }
        let response = answer
            .try_recv()
            .expect("the turn sent its answer")
            .expect("the handler succeeded");
        (response.status, response.body.len())
    }
}

/// The storage structured-clone codec (`serialize_storage_value` and
/// `deserialize_storage_value`, which `put`, `get` and RPC arguments use) on
/// one value, in a bare context of its own isolate.
pub struct CloneFixture {
    // Declared before the isolate: handles must go before it does.
    context: v8::Global<v8::Context>,
    value: v8::Global<v8::Value>,
    encoded: Vec<u8>,
    isolate: v8::OwnedIsolate,
}

impl CloneFixture {
    /// Evaluate `expression` once and keep the value it produces.
    pub fn new(expression: &str) -> Self {
        init_v8();
        let mut isolate = v8::Isolate::new(v8::CreateParams::default());
        let (context, value) = {
            v8::scope!(let hs, &mut isolate);
            let context = v8::Context::new(hs, Default::default());
            let scope = &mut v8::ContextScope::new(hs, context);
            let source = v8::String::new(scope, expression).unwrap();
            let value = v8::Script::compile(scope, source, None)
                .and_then(|script| script.run(scope))
                .expect("the clone fixture expression evaluates");
            (
                v8::Global::new(scope, context),
                v8::Global::new(scope, value),
            )
        };
        let mut fixture = Self {
            context,
            value,
            encoded: Vec::new(),
            isolate,
        };
        fixture.encoded = fixture.serialize();
        // A round trip must reproduce the value before either direction is
        // worth timing.
        let (original, decoded) = fixture.round_trip();
        assert_eq!(original, decoded);
        fixture
    }

    pub fn encoded_len(&self) -> usize {
        self.encoded.len()
    }

    /// A copy of the encoded value, as a storage read hands one over.
    pub fn encoded(&self) -> Vec<u8> {
        self.encoded.clone()
    }

    pub fn serialize(&mut self) -> Vec<u8> {
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, &self.context);
        let scope = &mut v8::ContextScope::new(hs, context);
        let value = v8::Local::new(scope, &self.value);
        crate::js::storage_ops::serialize_storage_value(scope, value)
            .expect("the fixture value clones")
    }

    /// Decode `encoded`; returns whether an object came back.
    pub fn deserialize(&mut self, encoded: Vec<u8>) -> bool {
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, &self.context);
        let scope = &mut v8::ContextScope::new(hs, context);
        crate::js::storage_ops::deserialize_storage_value(scope, storage::StoredValue::V8(encoded))
            .is_some_and(|value| value.is_object())
    }

    /// The original value and its decoded copy as JSON text. V8 may choose
    /// another array layout for the copy (dense for sparse), so the bytes of
    /// a second encoding can differ while the value is the same.
    fn round_trip(&mut self) -> (String, String) {
        v8::scope!(let hs, &mut self.isolate);
        let context = v8::Local::new(hs, &self.context);
        let scope = &mut v8::ContextScope::new(hs, context);
        let decoded = crate::js::storage_ops::deserialize_storage_value(
            scope,
            storage::StoredValue::V8(self.encoded.clone()),
        )
        .expect("the encoded value decodes");
        let original = v8::Local::new(scope, &self.value);
        let json = |value: v8::Local<v8::Value>| {
            v8::json::stringify(scope, value)
                .expect("the fixture value is JSON")
                .to_rust_string_lossy(scope)
        };
        (json(original), json(decoded))
    }
}

/// The Queue cell's policy seam (`queue_policy::run`), which the queue
/// harness calls through a synchronous op with a JSON request.
pub fn queue_policy(request: &serde_json::Value) -> serde_json::Value {
    crate::queue_policy::run(request).unwrap()
}

/// A static-asset deployment with `_headers` and `_redirects` rules, loaded
/// through `AssetResolver::load` from a development bucket.
pub struct AssetFixture {
    pub resolver: AssetResolver,
    _bucket: Bucket,
    _directory: tempfile::TempDir,
}

impl AssetFixture {
    /// `assets` files under `/assets/`, `static_redirects` literal and
    /// `dynamic_redirects` placeholder redirect rules, and `header_rules`
    /// `_headers` rules of which three match an asset on a `*.example.com`
    /// host.
    pub async fn new(
        assets: usize,
        static_redirects: usize,
        dynamic_redirects: usize,
        header_rules: usize,
    ) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&directory.path().join("bucket.sqlite")).unwrap();
        let mut index = AssetIndex {
            schema_version: 1,
            entries: Default::default(),
            config: AssetConfig::default(),
        };
        for i in 0..assets {
            index.entries.insert(
                format!("/assets/app-{i}.js"),
                AssetEntry {
                    sha256: format!("{i:064x}"),
                    bytes: 1024,
                    content_type: Some("text/javascript".to_string()),
                },
            );
        }
        let mut redirects = String::new();
        for i in 0..static_redirects {
            redirects.push_str(&format!("/old/page-{i} /new/page-{i} 301\n"));
        }
        for i in 0..dynamic_redirects {
            redirects.push_str(&format!("/blog-{i}/:year/:slug /posts/:year/:slug 301\n"));
        }
        let mut headers = String::from(
            "/assets/*\n  Cache-Control: public, max-age=31536000, immutable\n\
             /*\n  X-Frame-Options: DENY\n  ! X-Powered-By\n",
        );
        for i in 0..header_rules.saturating_sub(3) {
            headers.push_str(&format!("/page-{i}/*\n  X-Page: {i}\n"));
        }
        headers.push_str("https://*.example.com/*\n  X-Zone: example.com\n");
        index.config.redirects = Some(redirects);
        index.config.headers = Some(headers);
        let body = serde_json::to_vec(&index).unwrap();
        let reference = AssetManifestRef {
            index: "assets.json".to_string(),
            sha256: format!("{:x}", Sha256::digest(&body)),
            file_count: assets as u32,
            total_bytes: 1024 * assets as u64,
        };
        bucket.put("deploy/bench/assets.json", body).await.unwrap();
        let resolver = AssetResolver::load(&bucket, "deploy/bench", &reference, false, None)
            .await
            .unwrap();
        Self {
            resolver,
            _bucket: bucket,
            _directory: directory,
        }
    }
}
