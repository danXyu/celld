// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Pins the SQLite session behaviour that change export relies on
//! (docs/design/change-export.md). The bundled SQLite is built with
//! SQLITE_ENABLE_SESSION through rusqlite's `session` feature. rusqlite's own
//! `Session` wrapper hides its handle, so it cannot set
//! SQLITE_SESSION_OBJCONFIG_ROWID; these tests drive the C API directly.

use std::ffi::{c_int, c_void, CStr};
use std::ptr;

use rusqlite::{ffi, Connection};

/// One change as the session module reports it.
#[derive(Debug, PartialEq, Eq)]
struct Change {
    table: String,
    op: Op,
    /// Column 0 of the before-image (delete, update) or after-image (insert).
    first: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
enum Op {
    Insert,
    Update,
    Delete,
}

struct RawSession {
    session: *mut ffi::sqlite3_session,
}

impl RawSession {
    /// A session on `main` attached to every table, with the rowid option
    /// so that tables without a declared PRIMARY KEY are tracked by rowid.
    fn new(conn: &Connection, rowid: bool) -> Self {
        let mut session = ptr::null_mut();
        unsafe {
            let rc = ffi::sqlite3session_create(conn.handle(), c"main".as_ptr(), &mut session);
            assert_eq!(rc, ffi::SQLITE_OK, "sqlite3session_create");
            // Must be set before the first attach.
            let mut on: c_int = rowid.into();
            let rc = ffi::sqlite3session_object_config(
                session,
                ffi::SQLITE_SESSION_OBJCONFIG_ROWID,
                (&mut on as *mut c_int).cast::<c_void>(),
            );
            assert_eq!(rc, ffi::SQLITE_OK, "SQLITE_SESSION_OBJCONFIG_ROWID");
            assert_eq!(on, c_int::from(rowid), "rowid option not applied");
            let rc = ffi::sqlite3session_attach(session, ptr::null());
            assert_eq!(rc, ffi::SQLITE_OK, "sqlite3session_attach");
        }
        Self { session }
    }

    fn changes(&self) -> Vec<Change> {
        let mut size: c_int = 0;
        let mut buffer: *mut c_void = ptr::null_mut();
        let mut out = Vec::new();
        unsafe {
            let rc = ffi::sqlite3session_changeset(self.session, &mut size, &mut buffer);
            assert_eq!(rc, ffi::SQLITE_OK, "sqlite3session_changeset");
            let mut iter = ptr::null_mut();
            let rc = ffi::sqlite3changeset_start(&mut iter, size, buffer);
            assert_eq!(rc, ffi::SQLITE_OK, "sqlite3changeset_start");
            while ffi::sqlite3changeset_next(iter) == ffi::SQLITE_ROW {
                let mut table = ptr::null();
                let mut columns: c_int = 0;
                let mut code: c_int = 0;
                let mut indirect: c_int = 0;
                let rc = ffi::sqlite3changeset_op(
                    iter,
                    &mut table,
                    &mut columns,
                    &mut code,
                    &mut indirect,
                );
                assert_eq!(rc, ffi::SQLITE_OK, "sqlite3changeset_op");
                let op = match code {
                    ffi::SQLITE_INSERT => Op::Insert,
                    ffi::SQLITE_UPDATE => Op::Update,
                    ffi::SQLITE_DELETE => Op::Delete,
                    other => panic!("unexpected changeset op {other}"),
                };
                let mut value = ptr::null_mut();
                let rc = if op == Op::Insert {
                    ffi::sqlite3changeset_new(iter, 0, &mut value)
                } else {
                    ffi::sqlite3changeset_old(iter, 0, &mut value)
                };
                assert_eq!(rc, ffi::SQLITE_OK, "column 0 value");
                let first = (!value.is_null()
                    && ffi::sqlite3_value_type(value) == ffi::SQLITE_INTEGER)
                    .then(|| ffi::sqlite3_value_int64(value));
                out.push(Change {
                    table: CStr::from_ptr(table).to_str().unwrap().to_owned(),
                    op,
                    first,
                });
            }
            assert_eq!(ffi::sqlite3changeset_finalize(iter), ffi::SQLITE_OK);
            ffi::sqlite3_free(buffer);
        }
        out
    }
}

impl Drop for RawSession {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3session_delete(self.session) };
    }
}

fn change(table: &str, op: Op, first: i64) -> Change {
    Change {
        table: table.to_owned(),
        op,
        first: Some(first),
    }
}

/// Three table shapes: a declared key, rowid only, and WITHOUT ROWID.
fn open() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE keyed (id TEXT PRIMARY KEY, v INTEGER);
         CREATE TABLE plain (v INTEGER);
         CREATE TABLE norowid (id INTEGER PRIMARY KEY, v INTEGER) WITHOUT ROWID;",
    )
    .unwrap();
    conn
}

#[test]
fn bundled_sqlite_has_session_and_preupdate() {
    let conn = Connection::open_in_memory().unwrap();
    let options: Vec<String> = conn
        .prepare("PRAGMA compile_options")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(options.iter().any(|o| o == "ENABLE_SESSION"), "{options:?}");
    assert!(
        options.iter().any(|o| o == "ENABLE_PREUPDATE_HOOK"),
        "{options:?}"
    );
    // SQLITE_SESSION_OBJCONFIG_ROWID arrived in 3.40.0.
    assert!(
        rusqlite::version_number() >= 3_040_000,
        "{}",
        rusqlite::version()
    );
}

#[test]
fn captures_every_table_shape_with_rowid_option() {
    let conn = open();
    let session = RawSession::new(&conn, true);
    conn.execute_batch(
        "INSERT INTO keyed VALUES ('a', 1);
         INSERT INTO plain VALUES (2);
         INSERT INTO norowid VALUES (3, 3);",
    )
    .unwrap();
    // A rowid-only table reports the rowid as an extra leading column.
    assert_eq!(
        session.changes(),
        vec![
            Change {
                table: "keyed".to_owned(),
                op: Op::Insert,
                first: None,
            },
            change("plain", Op::Insert, 1),
            change("norowid", Op::Insert, 3),
        ]
    );
}

#[test]
fn rowid_only_table_is_invisible_without_rowid_option() {
    let conn = open();
    let session = RawSession::new(&conn, false);
    conn.execute_batch("INSERT INTO plain VALUES (2); INSERT INTO norowid VALUES (3, 3);")
        .unwrap();
    assert_eq!(session.changes(), vec![change("norowid", Op::Insert, 3)]);
}

#[test]
fn insert_then_delete_nets_to_nothing() {
    let conn = open();
    let session = RawSession::new(&conn, true);
    conn.execute_batch(
        "BEGIN;
         INSERT INTO keyed VALUES ('a', 1);
         INSERT INTO plain VALUES (2);
         INSERT INTO norowid VALUES (3, 3);
         DELETE FROM keyed; DELETE FROM plain; DELETE FROM norowid;
         COMMIT;",
    )
    .unwrap();
    assert_eq!(session.changes(), vec![]);
}

#[test]
fn rollback_to_drops_reverted_rows() {
    let conn = open();
    conn.execute_batch("INSERT INTO norowid VALUES (1, 1);")
        .unwrap();
    let session = RawSession::new(&conn, true);
    conn.execute_batch(
        "BEGIN;
         INSERT INTO norowid VALUES (2, 2);
         SAVEPOINT s;
         INSERT INTO norowid VALUES (3, 3);
         INSERT INTO plain VALUES (4);
         UPDATE norowid SET v = 10 WHERE id = 1;
         ROLLBACK TO s;
         RELEASE s;
         COMMIT;",
    )
    .unwrap();
    assert_eq!(session.changes(), vec![change("norowid", Op::Insert, 2)]);
}

#[test]
fn rowid_change_on_keyed_table_is_no_change() {
    let conn = open();
    conn.execute_batch("INSERT INTO keyed VALUES ('a', 1);")
        .unwrap();
    let session = RawSession::new(&conn, true);
    conn.execute_batch("UPDATE keyed SET rowid = 100 WHERE id = 'a';")
        .unwrap();
    assert_eq!(
        conn.query_row("SELECT rowid FROM keyed", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        100
    );
    assert_eq!(session.changes(), vec![]);
}

#[test]
fn rowid_change_on_rowid_only_table_is_delete_and_insert() {
    let conn = open();
    conn.execute_batch("INSERT INTO plain VALUES (7);").unwrap();
    let session = RawSession::new(&conn, true);
    conn.execute_batch("UPDATE plain SET rowid = 100;").unwrap();
    let mut changes = session.changes();
    changes.sort_by_key(|c| c.first);
    assert_eq!(
        changes,
        vec![
            change("plain", Op::Delete, 1),
            change("plain", Op::Insert, 100)
        ]
    );
}
