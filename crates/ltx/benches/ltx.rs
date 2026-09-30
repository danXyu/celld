// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Fixtures live in temporary directories outside any node filesystem, and the
// file replica and bench harness use the ambient filesystem and clock.
#![allow(clippy::disallowed_methods)]

//! Benchmarks capture, the LTX codec, compaction and restore on local files.
//!
//! Fixtures are deterministic 4 KiB pages. Each case checks its fixture or
//! output before timing. Setup that a case must repeat, such as the write that
//! a capture reads or the output a compaction publishes, runs outside the
//! timed region through `iter_batched` with `PerIteration`.

use celld_ltx::compactor::Compactor;
use celld_ltx::internal::lz4_block::Compressor;
use celld_ltx::ltx::{self, Crc64, FileInfo, Header, HEADER_FLAG_NO_CHECKSUM};
use celld_ltx::paged::build_page_map;
use celld_ltx::replica::{calc_restore_plan, restore_timed_with_download_slots};
use celld_ltx::replica_compactor::ReplicaCompactor;
use celld_ltx::{ltx_file_path, Db, FileReplicaClient, ReplicaClient, TXID};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rusqlite::{params, Connection};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::hint::black_box;
use std::io::{BufReader, BufWriter, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;

const PAGE_SIZE: u32 = 4096;
/// Pages in the database the compaction and restore fixtures change.
const DB_PAGES: u32 = 1024;
/// Pages each L0 fixture changes: page 1 and three scattered pages.
const L0_PAGES: usize = 4;
/// The download ceiling celld gives restores (`ltx_repl.rs`).
const RESTORE_SLOTS: usize = 64;

/// A deterministic xorshift generator, so every run builds the same bytes.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// A 4 KiB page shaped like a SQLite b-tree leaf: a header, an index of cell
/// offsets, text-like records with repeated structure, and free space. It
/// compresses about as well as ordinary row data does.
fn page(pgno: u32, version: u64) -> Vec<u8> {
    let mut rng = Rng(u64::from(pgno).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version ^ 0xDEAD_BEEF);
    let mut page = vec![0u8; PAGE_SIZE as usize];
    page[0] = 0x0D;
    page[1..5].copy_from_slice(&pgno.to_be_bytes());
    page[5..13].copy_from_slice(&version.to_be_bytes());
    for slot in 0..32 {
        let offset = 16 + slot * 2;
        page[offset..offset + 2].copy_from_slice(&(1024 + slot as u16 * 96).to_be_bytes());
    }
    const WORDS: [&[u8]; 8] = [
        b"user",
        b"session",
        b"count",
        b"2026-09-29",
        b"active",
        b"cell",
        b"value",
        b"null",
    ];
    let mut at = 1024;
    while at < 3968 {
        let word = WORDS[(rng.next() % 8) as usize];
        let end = (at + word.len()).min(3968);
        page[at..end].copy_from_slice(&word[..end - at]);
        at = end;
        if at < 3968 {
            page[at] = (rng.next() % 64) as u8;
            at += 1;
        }
    }
    page
}

fn header(commit: u32, min_txid: u64, max_txid: u64) -> Header {
    Header {
        version: ltx::VERSION,
        // Capture writes checksum-free L0 files, and compaction preserves the
        // flag (`db.rs`, `replica_compactor.rs`).
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size: PAGE_SIZE,
        commit,
        min_txid: TXID(min_txid),
        max_txid: TXID(max_txid),
        timestamp: 1_790_000_000_000 + min_txid as i64,
        ..Header::default()
    }
}

/// The capture format: one L0 file covering `txid`.
fn l0(txid: u64, commit: u32, pages: &[(u32, Vec<u8>)]) -> Vec<u8> {
    ltx::encode_file(&header(commit, txid, txid), pages, 0).expect("encode L0")
}

/// A snapshot of `DB_PAGES` pages at txid 1, followed by `count` L0 files at
/// txids 2.. that each change `L0_PAGES` scattered pages.
fn chain(count: usize) -> Vec<Vec<u8>> {
    let snapshot: Vec<_> = (1..=DB_PAGES).map(|pgno| (pgno, page(pgno, 0))).collect();
    let mut files = vec![l0(1, DB_PAGES, &snapshot)];
    let mut rng = Rng(0x5EED ^ count as u64);
    for index in 0..count {
        let txid = index as u64 + 2;
        let mut pgnos = BTreeSet::from([1]);
        while pgnos.len() < L0_PAGES {
            pgnos.insert(1 + (rng.next() % u64::from(DB_PAGES)) as u32);
        }
        let pages: Vec<_> = pgnos
            .into_iter()
            .map(|pgno| (pgno, page(pgno, txid)))
            .collect();
        files.push(l0(txid, DB_PAGES, &pages));
    }
    files
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Writes `files` (txids 1..) to level 0 of a file replica rooted at `root`.
fn publish(runtime: &Runtime, root: &Path, files: &[Vec<u8>]) -> FileReplicaClient {
    let client = FileReplicaClient::new(root.to_string_lossy());
    runtime.block_on(async {
        for (index, bytes) in files.iter().enumerate() {
            let txid = TXID(index as u64 + 1);
            client
                .write_ltx_file(0, txid, txid, bytes)
                .await
                .expect("publish L0");
        }
    });
    client
}

// ── ltx_capture ─────────────────────────────────────────────────────────────

/// Rows sized so that each fills one 4 KiB table leaf.
const ROW_BYTES: usize = 3000;
const ROWS: usize = 4096;

struct CaptureFixture {
    _dir: tempfile::TempDir,
    db: Db,
    writer: Connection,
    version: u64,
    rows: usize,
}

impl CaptureFixture {
    fn new(rows: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cell.db");
        let mut db = Db::open(&path).expect("open db");
        let writer = Connection::open(&path).expect("open writer");
        writer
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB NOT NULL)")
            .expect("create table");
        let tx = writer.unchecked_transaction().expect("begin");
        for id in 0..ROWS {
            tx.execute(
                "INSERT INTO t (id, v) VALUES (?1, ?2)",
                params![id as i64, row(id, 0)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
        // The first sync snapshots; every timed sync after it is incremental.
        db.sync().expect("snapshot sync");
        let mut fixture = Self {
            _dir: dir,
            db,
            writer,
            version: 0,
            rows,
        };
        // Warm the capture path and check its output before timing.
        let before = fixture.db.pos().expect("pos").txid;
        fixture.write();
        fixture.db.sync().expect("sync");
        let timing = fixture.db.last_sync_timing();
        assert!(!timing.snapshot, "an incremental write must not snapshot");
        let txid = fixture.db.pos().expect("pos").txid;
        assert_eq!(txid.0, before.0 + 1);
        let bytes = fixture.db.read_ltx_file(0, txid, txid).expect("read L0");
        let decoded = ltx::decode_file(&bytes).expect("decode L0");
        assert!(
            decoded.pgnos.len() >= rows,
            "{} pages captured for {rows} rows",
            decoded.pgnos.len()
        );
        fixture
    }

    /// Updates `rows` rows spread across the table, one leaf page each.
    fn write(&mut self) {
        self.version += 1;
        let stride = ROWS / self.rows;
        let tx = self.writer.unchecked_transaction().expect("begin");
        {
            let mut update = tx
                .prepare_cached("UPDATE t SET v = ?1 WHERE id = ?2")
                .expect("prepare");
            for index in 0..self.rows {
                let id = (index * stride + self.version as usize) % ROWS;
                update
                    .execute(params![row(id, self.version), id as i64])
                    .expect("update");
            }
        }
        tx.commit().expect("commit");
    }
}

fn row(id: usize, version: u64) -> Vec<u8> {
    let mut bytes = page(id as u32 + 1, version);
    bytes.truncate(ROW_BYTES);
    bytes
}

/// Mean `SyncTiming` phases over the measured syncs, in microseconds.
#[derive(Default)]
struct PhaseTotals {
    syncs: u64,
    snapshots: u64,
    verify: u64,
    wal_read: u64,
    map_collect: u64,
    ltx_encode: u64,
    file_write: u64,
    fsync: u64,
    checkpoint: u64,
    checkpoints: u64,
}

impl PhaseTotals {
    fn add(&mut self, timing: celld_ltx::db::SyncTiming) {
        self.syncs += 1;
        self.snapshots += u64::from(timing.snapshot);
        self.verify += timing.verify_us;
        self.wal_read += timing.wal_read_us;
        self.map_collect += timing.map_collect_us;
        self.ltx_encode += timing.ltx_encode_us;
        self.file_write += timing.file_write_us;
        self.fsync += timing.fsync_us;
        self.checkpoint += timing.checkpoint_us;
        self.checkpoints += timing.checkpoint_runs;
    }

    fn report(&self, case: &str) {
        if self.syncs == 0 {
            return;
        }
        let mean = |total: u64| total as f64 / self.syncs as f64;
        eprintln!(
            "{case}: {} syncs, mean µs: verify {:.1}, wal_read {:.1}, map_collect {:.1}, \
             ltx_encode {:.1}, file_write {:.1}, fsync {:.1}, checkpoint {:.1} \
             ({} checkpoints, {} snapshots)",
            self.syncs,
            mean(self.verify),
            mean(self.wal_read),
            mean(self.map_collect),
            mean(self.ltx_encode),
            mean(self.file_write),
            mean(self.fsync),
            mean(self.checkpoint),
            self.checkpoints,
            self.snapshots,
        );
    }
}

/// `Db::sync` after a transaction that changed 1, 16 or 256 table leaves.
///
/// The write runs untimed before each sync. The sync includes the capture
/// fsync and, when the WAL reaches the default threshold of 1,000 pages, the
/// passive checkpoint, as in production. The phase means printed after each
/// case come from `Db::last_sync_timing`.
///
/// There is no case with the WAL held at 10× the checkpoint threshold: with
/// checkpoints disabled every timed sync grows the WAL, so the case would not
/// measure a fixed size.
fn ltx_capture(c: &mut Criterion) {
    let mut group = c.benchmark_group("ltx_capture");
    for rows in [1, 16, 256] {
        let fixture = RefCell::new(CaptureFixture::new(rows));
        let totals = RefCell::new(PhaseTotals::default());
        let pending = RefCell::new(false);
        group.throughput(Throughput::Bytes(rows as u64 * u64::from(PAGE_SIZE)));
        group.bench_function(BenchmarkId::new("sync", format!("{rows}_pages")), |b| {
            b.iter_batched(
                || {
                    let mut fixture = fixture.borrow_mut();
                    if std::mem::take(&mut *pending.borrow_mut()) {
                        totals.borrow_mut().add(fixture.db.last_sync_timing());
                    }
                    fixture.write();
                },
                |()| {
                    fixture.borrow_mut().db.sync().expect("sync");
                    *pending.borrow_mut() = true;
                },
                BatchSize::PerIteration,
            )
        });
        totals
            .borrow()
            .report(&format!("ltx_capture/sync/{rows}_pages"));
    }
    group.finish();
}

// ── ltx_codec ───────────────────────────────────────────────────────────────

/// The inner loops of capture and restore: LZ4 blocks, CRC64, and whole LTX
/// files by page count. `legacy` is the LZ4-frame encoding capture writes;
/// `block` is the v0.5.2 sized-block encoding compaction writes.
fn ltx_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("ltx_codec");

    let data = page(7, 1);
    let mut compressor = Compressor::default();
    let compressed = compressor.compress(&data);
    let mut decoded = vec![0u8; data.len()];
    assert_eq!(
        lz4_flex::block::decompress_into(&compressed, &mut decoded).expect("decompress"),
        data.len()
    );
    assert_eq!(decoded, data);
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("lz4_block_encode/4KiB", |b| {
        b.iter(|| compressor.compress(black_box(&data)))
    });
    group.bench_function("lz4_block_decode/4KiB", |b| {
        b.iter(|| lz4_flex::block::decompress_into(black_box(&compressed), &mut decoded))
    });

    for size in [4096usize, 1 << 20] {
        let bytes: Vec<u8> = (0..size / PAGE_SIZE as usize)
            .flat_map(|index| page(index as u32 + 1, 3))
            .collect();
        group.throughput(Throughput::Bytes(size as u64));
        let label = if size == 4096 { "4KiB" } else { "1MiB" };
        group.bench_function(BenchmarkId::new("crc64", label), |b| {
            b.iter(|| {
                let mut hash = Crc64::new();
                hash.update(black_box(&bytes));
                hash.sum64()
            })
        });
    }

    for pages in [1u32, 64, 1024] {
        let input: Vec<_> = (1..=pages).map(|pgno| (pgno, page(pgno, 2))).collect();
        let header = header(pages, 2, 2);
        let legacy = ltx::encode_file(&header, &input, 0).expect("encode legacy");
        let block = ltx::encode_file_v0_5_2(&header, &input, 0).expect("encode block");
        assert_eq!(
            ltx::decode_file_pages(&legacy).expect("decode legacy"),
            input
        );
        assert_eq!(ltx::decode_file_pages(&block).expect("decode block"), input);
        group.throughput(Throughput::Bytes(u64::from(pages * PAGE_SIZE)));
        group.bench_function(BenchmarkId::new("encode_legacy", pages), |b| {
            b.iter(|| ltx::encode_file(&header, black_box(&input), 0).unwrap())
        });
        group.bench_function(BenchmarkId::new("encode_block", pages), |b| {
            b.iter(|| ltx::encode_file_v0_5_2(&header, black_box(&input), 0).unwrap())
        });
        group.bench_function(BenchmarkId::new("decode_legacy", pages), |b| {
            b.iter(|| ltx::decode_file_pages(black_box(&legacy)).unwrap())
        });
        group.bench_function(BenchmarkId::new("decode_block", pages), |b| {
            b.iter(|| ltx::decode_file_pages(black_box(&block)).unwrap())
        });
    }
    group.finish();
}

// ── ltx_compact ─────────────────────────────────────────────────────────────

/// Folding 16, 256 and 1,024 L0 files into one.
///
/// `merge` is the in-memory `Compactor` over encoded L0 bytes, which is what
/// `LtxRepl::merge_l0_rows` runs for a recovered tail. `replica` is
/// `ReplicaCompactor::compact(1)` over a file replica, which is what
/// `compact_cell` runs: list both levels, read every source, merge, and write
/// and fsync the L1 object. The untimed setup deletes the previous L1 object
/// so each round has the same work.
fn ltx_compact(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("ltx_compact");
    for count in [16, 256, 1024] {
        let files = chain(count);
        let sources = &files[1..];
        let merge = |sources: &[Vec<u8>]| {
            let readers: Vec<_> = sources
                .iter()
                .map(|bytes| Cursor::new(bytes.as_slice()))
                .collect();
            let mut compactor = Compactor::new(Vec::new(), readers);
            compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
            compactor.compact().expect("compact");
            compactor.into_writer()
        };
        let merged = ltx::decode_file(&merge(sources)).expect("decode merge");
        assert_eq!(
            (merged.header.min_txid, merged.header.max_txid),
            (TXID(2), TXID(count as u64 + 1))
        );
        let changed: BTreeSet<u32> = sources
            .iter()
            .flat_map(|bytes| ltx::decode_file(bytes).unwrap().pgnos)
            .collect();
        assert_eq!(merged.pgnos, changed.into_iter().collect::<Vec<_>>());

        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(BenchmarkId::new("merge", count), |b| {
            b.iter(|| merge(black_box(sources)))
        });

        let dir = tempfile::tempdir().expect("tempdir");
        let client = publish(&runtime, dir.path(), &files);
        let output = PathBuf::from(ltx_file_path(
            &dir.path().to_string_lossy(),
            1,
            TXID(2),
            TXID(count as u64 + 1),
        ));
        let compact = || {
            runtime.block_on(async {
                ReplicaCompactor::new(&client)
                    .with_verification(true)
                    .with_base(TXID(2))
                    .compact(1)
                    .await
                    .expect("compact")
            })
        };
        let first = compact().expect("one L1 object");
        assert_eq!(first.input_files, count);
        assert_eq!(first.info.max_txid, TXID(count as u64 + 1));
        assert!(compact().is_none(), "a covered level has no work");
        group.bench_function(BenchmarkId::new("replica", count), |b| {
            b.iter_batched(
                || std::fs::remove_file(&output).expect("remove L1"),
                |()| compact().expect("one L1 object"),
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── ltx_restore ─────────────────────────────────────────────────────────────

/// A 1,024-page snapshot plus 16, 256 or 1,024 L0 files, and paged page maps
/// for 256 MiB and 2 GiB databases.
///
/// `apply` merges in-memory files into the database image, the CPU part of a
/// restore. `file_replica` is the whole restore from a file replica: the plan
/// listing, the reads with celld's download ceiling, the apply, and the
/// write, fsync and rename of the output; the untimed setup removes the
/// previous output. `page_map` builds the paged-restore page map from each
/// planned object's page index without reading its frames.
fn ltx_restore(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("ltx_restore");
    for count in [16, 256, 1024] {
        let files = chain(count);
        let image = celld_ltx::internal::replica::build_database_image(&files).expect("apply");
        assert_eq!(image.len(), (DB_PAGES * PAGE_SIZE) as usize);
        group.throughput(Throughput::Elements(files.len() as u64));
        group.bench_function(BenchmarkId::new("apply", count), |b| {
            b.iter(|| {
                celld_ltx::internal::replica::build_database_image(black_box(&files)).unwrap()
            })
        });

        let dir = tempfile::tempdir().expect("tempdir");
        let client = publish(&runtime, &dir.path().join("replica"), &files);
        let output = dir.path().join("restored.db");
        let restore = || {
            runtime.block_on(restore_timed_with_download_slots(
                &client,
                &output,
                TXID(0),
                Arc::new(Semaphore::new(RESTORE_SLOTS)),
            ))
        };
        let stats = restore().expect("restore");
        assert_eq!(stats.plan.objects, files.len());
        assert_eq!(std::fs::read(&output).expect("read output"), image);
        group.bench_function(BenchmarkId::new("file_replica", count), |b| {
            b.iter_batched(
                || std::fs::remove_file(&output).expect("remove output"),
                |()| restore().expect("restore"),
                BatchSize::PerIteration,
            )
        });
    }

    // `cargo test` runs this once, unoptimized, as CI's smoke check; the
    // 2 GiB fixture is for `cargo bench` only.
    let sizes: &[(&str, u32)] = if cfg!(debug_assertions) {
        &[("256MiB", 1 << 16)]
    } else {
        &[("256MiB", 1 << 16), ("2GiB", 1 << 19)]
    };
    for &(label, pages) in sizes {
        let dir = tempfile::tempdir().expect("tempdir");
        let (client, plan) = paged_fixture(&runtime, dir.path(), pages);
        let map = runtime
            .block_on(build_page_map(&client, &plan))
            .expect("page map");
        assert_eq!(map.commit, pages);
        // Every page but SQLite's lock page is mapped.
        let lock = u32::from(pages > ltx::lock_pgno(PAGE_SIZE));
        assert_eq!(map.pages.len(), (pages - lock) as usize);
        group.throughput(Throughput::Elements(u64::from(pages)));
        group.bench_function(BenchmarkId::new("page_map", label), |b| {
            b.iter(|| runtime.block_on(build_page_map(&client, &plan)).unwrap())
        });
    }
    group.finish();
}

/// Pages per scratch file when the paged fixture streams a large snapshot
/// through the compactor, so no fixture holds the database in memory.
const CHUNK_PAGES: u32 = 4096;
/// L0 files planned on top of the paged snapshot.
const PAGED_L0: u64 = 16;

/// A snapshot-level object holding `pages` pages at txids 1..=chunks, with
/// `PAGED_L0` L0 files above it, in a file replica. Returns the client and
/// its restore plan.
fn paged_fixture(runtime: &Runtime, root: &Path, pages: u32) -> (FileReplicaClient, Vec<FileInfo>) {
    let lock = ltx::lock_pgno(PAGE_SIZE);
    let scratch = root.join("scratch");
    std::fs::create_dir_all(&scratch).expect("scratch dir");
    let chunks = pages.div_ceil(CHUNK_PAGES);
    let mut paths = Vec::new();
    for chunk in 0..chunks {
        let first = chunk * CHUNK_PAGES + 1;
        let last = ((chunk + 1) * CHUNK_PAGES).min(pages);
        let input: Vec<_> = (first..=last)
            .filter(|pgno| *pgno != lock)
            .map(|pgno| (pgno, page(pgno, 0)))
            .collect();
        let txid = u64::from(chunk) + 1;
        let bytes =
            ltx::encode_file_v0_5_2(&header(pages, txid, txid), &input, 0).expect("encode chunk");
        let path = scratch.join(format!("{chunk}.ltx"));
        std::fs::write(&path, bytes).expect("write chunk");
        paths.push(path);
    }

    let root_str = root.join("replica").to_string_lossy().into_owned();
    let snapshot_path = PathBuf::from(ltx_file_path(
        &root_str,
        celld_ltx::compaction_level::SNAPSHOT_LEVEL as u32,
        TXID(1),
        TXID(u64::from(chunks)),
    ));
    std::fs::create_dir_all(snapshot_path.parent().unwrap()).expect("snapshot dir");
    let readers: Vec<_> = paths
        .iter()
        .map(|path| BufReader::new(std::fs::File::open(path).expect("open chunk")))
        .collect();
    let writer = BufWriter::new(std::fs::File::create(&snapshot_path).expect("create snapshot"));
    let mut compactor = Compactor::new(writer, readers);
    compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
    compactor.compact().expect("compact snapshot");
    compactor.into_writer().flush().expect("flush snapshot");
    std::fs::remove_dir_all(&scratch).expect("remove scratch");

    let client = FileReplicaClient::new(root_str);
    let mut rng = Rng(0xFACE);
    runtime.block_on(async {
        for index in 0..PAGED_L0 {
            let txid = u64::from(chunks) + 1 + index;
            let mut pgnos = BTreeSet::from([1]);
            while pgnos.len() < L0_PAGES {
                let pgno = 1 + (rng.next() % u64::from(pages)) as u32;
                if pgno != lock {
                    pgnos.insert(pgno);
                }
            }
            let input: Vec<_> = pgnos
                .into_iter()
                .map(|pgno| (pgno, page(pgno, txid)))
                .collect();
            client
                .write_ltx_file(0, TXID(txid), TXID(txid), &l0(txid, pages, &input))
                .await
                .expect("publish L0");
        }
    });
    let plan = runtime
        .block_on(calc_restore_plan(&client, TXID(0)))
        .expect("restore plan");
    assert_eq!(plan.len(), 1 + PAGED_L0 as usize);
    (client, plan)
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = ltx_capture, ltx_codec, ltx_compact, ltx_restore
}
criterion_main!(benches);
