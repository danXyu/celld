//! The capture observer reports every L0 file the capture loop writes, with
//! the WAL range each one covers, and does nothing when none is installed.

use celld_ltx::{CaptureKind, CapturedFile, CheckpointMode, Db, TXID};
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

const WAL_HEADER_SIZE: i64 = 32;
const WAL_FRAME_HEADER_SIZE: i64 = 24;

fn open(dir: &Path) -> (Db, Connection, Arc<Mutex<Vec<CapturedFile>>>) {
    let path = dir.join("cell.db");
    let db = Db::open(&path).expect("open db");
    let writer = Connection::open(&path).expect("open writer");
    writer
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create table");
    (db, writer, Arc::new(Mutex::new(Vec::new())))
}

fn observe(db: &mut Db, seen: &Arc<Mutex<Vec<CapturedFile>>>) {
    let seen = Arc::clone(seen);
    db.set_capture_observer(move |file: &CapturedFile| seen.lock().unwrap().push(file.clone()));
}

fn insert(writer: &Connection, n: usize) {
    for _ in 0..n {
        writer
            .execute("INSERT INTO t (v) VALUES (?1)", ["x".repeat(3000)])
            .expect("insert");
    }
}

fn wal_frames(db: &Db, page_size: u32) -> u64 {
    let len = celld_ltx::LtxHost::default()
        .metadata(&db.wal_path())
        .expect("stat wal")
        .len as i64;
    ((len - WAL_HEADER_SIZE) / (i64::from(page_size) + WAL_FRAME_HEADER_SIZE)) as u64
}

/// The header of the L0 file at `txid`, as written to disk.
fn l0_header(db: &Db, txid: TXID) -> celld_ltx::ltx::Header {
    let bytes = db
        .read_ltx_file(0, txid, txid)
        .unwrap_or_else(|e| panic!("read L0 {txid}: {e}"));
    celld_ltx::ltx::Header::parse(&bytes).expect("parse header")
}

/// Every reported file matches the header of the file on disk, and TXIDs
/// are reported in order without holes.
fn assert_matches_disk(db: &Db, seen: &[CapturedFile]) {
    for pair in seen.windows(2) {
        assert_eq!(pair[1].txid.0, pair[0].txid.0 + 1, "{seen:#?}");
    }
    for file in seen {
        let header = l0_header(db, file.txid);
        assert_eq!(file.page_size, header.page_size);
        assert_eq!(file.commit, header.commit);
        let wal = file.wal.expect("captures carry a WAL range");
        assert_eq!(
            (wal.salt1, wal.salt2, wal.offset, wal.size),
            (
                header.wal_salt1,
                header.wal_salt2,
                header.wal_offset,
                header.wal_size
            ),
            "txid {}",
            file.txid
        );
    }
}

#[test]
fn reports_each_capture_with_its_wal_range() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, writer, seen) = open(dir.path());
    observe(&mut db, &seen);

    insert(&writer, 3);
    db.sync().expect("first sync");
    insert(&writer, 2);
    db.sync().expect("second sync");
    // Nothing new: no file, no report.
    db.sync().expect("idle sync");

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:#?}");
    let (first, second) = (&seen[0], &seen[1]);
    assert_eq!(first.kind, CaptureKind::Sync);
    assert!(first.full_image, "the first capture is a snapshot");
    assert_eq!(second.kind, CaptureKind::Sync);
    assert!(!second.full_image);

    let page_size = first.page_size;
    let (a, b) = (first.wal.unwrap(), second.wal.unwrap());
    assert_eq!((a.salt1, a.salt2), (b.salt1, b.salt2));
    assert_eq!(a.frames_before(page_size), 0);
    assert_eq!(b.frames_before(page_size), a.frames_after(page_size));
    assert_eq!(b.frames_after(page_size), wal_frames(&db, page_size));
    assert!(b.frames_after(page_size) > a.frames_after(page_size));
    assert_matches_disk(&db, &seen);
}

#[test]
fn reports_the_files_a_checkpoint_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, writer, seen) = open(dir.path());
    observe(&mut db, &seen);

    insert(&writer, 4);
    db.sync().expect("sync");
    insert(&writer, 4);
    db.checkpoint(CheckpointMode::Passive).expect("passive");
    insert(&writer, 4);
    db.checkpoint(CheckpointMode::Truncate).expect("truncate");
    insert(&writer, 1);
    db.sync().expect("sync after truncate");

    let seen = seen.lock().unwrap().clone();
    let kinds: Vec<_> = seen.iter().map(|f| f.kind).collect();
    assert!(kinds.contains(&CaptureKind::CheckpointLead), "{kinds:?}");
    assert!(kinds.contains(&CaptureKind::PostRestart), "{kinds:?}");
    // A truncate always ends in a boundary image of the new WAL.
    let boundary = seen
        .iter()
        .rev()
        .find(|f| f.kind == CaptureKind::PostRestart)
        .unwrap();
    assert!(boundary.full_image);
    assert_eq!(boundary.wal.unwrap().offset, WAL_HEADER_SIZE);
    assert_eq!(seen.last().unwrap().kind, CaptureKind::Sync);
    assert_matches_disk(&db, &seen);
}

#[test]
fn reports_the_passive_barrier_capture() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, writer, seen) = open(dir.path());
    observe(&mut db, &seen);
    insert(&writer, 2);
    db.sync().expect("sync");

    // A commit that lands after the lead capture and before the writer
    // barrier is sealed by the capture under the barrier.
    let path = dir.path().join("cell.db");
    celld_ltx::internal::db::checkpoint_passive_with_unlocked_hook(
        &mut db,
        Box::new(move || insert(&Connection::open(&path).unwrap(), 1)),
    )
    .expect("passive");

    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.iter().any(|f| f.kind == CaptureKind::PassiveBarrier),
        "{seen:#?}"
    );
    assert_matches_disk(&db, &seen);
}

#[test]
fn reports_a_seed_without_a_wal_range() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, writer, seen) = open(dir.path());
    insert(&writer, 1);
    let commit: u32 = writer
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    observe(&mut db, &seen);

    db.seed_continuation(TXID(7), commit).expect("seed");
    insert(&writer, 1);
    db.sync().expect("sync after seed");

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:#?}");
    assert_eq!(seen[0].kind, CaptureKind::Seed);
    assert_eq!(seen[0].txid, TXID(7));
    assert_eq!(seen[0].commit, commit);
    assert_eq!(seen[0].wal, None);
    assert_eq!(seen[1].txid, TXID(8));
    assert_eq!(seen[1].kind, CaptureKind::Sync);
    assert!(seen[1].wal.is_some());
}

#[test]
fn removing_the_observer_stops_reports() {
    let dir = tempfile::tempdir().unwrap();
    let (mut db, writer, seen) = open(dir.path());
    observe(&mut db, &seen);
    insert(&writer, 1);
    db.sync().expect("sync");
    assert!(db.take_capture_observer().is_some());
    insert(&writer, 1);
    db.sync().expect("sync without observer");
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(db.take_capture_observer().is_none());
}
