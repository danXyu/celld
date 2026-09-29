// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use celld_export_format::{
    Body, BulkBody, Consumer, Envelope, Origin, Position, Record, RowsBody, SnapshotBody,
    SnapshotEndBody, SnapshotScope, StreamId,
};
use proptest::prelude::*;

const SCOPE: &str = "cell-a";

fn settings() -> Settings {
    Settings {
        max_tx_bytes: 1 << 20,
    }
}

struct Fixture {
    connection: Connection,
    capture: Capture,
    queue: DirtyList,
    /// `DROP TABLE`s and `ALTER TABLE`s the authorizer saw, as `storage`
    /// collects them.
    drops: Arc<Mutex<Vec<String>>>,
}

impl Fixture {
    fn new(schema: &str) -> Self {
        Self::with_settings(schema, settings())
    }

    fn with_settings(schema: &str, settings: Settings) -> Self {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(schema).unwrap();
        Self::install(connection, settings)
    }

    /// Capture on `connection`, as a cell open does.
    fn install(connection: Connection, settings: Settings) -> Self {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let seen = drops.clone();
        connection.authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
            use rusqlite::hooks::AuthAction;
            if let AuthAction::DropTable { table_name }
            | AuthAction::AlterTable { table_name, .. } = context.action
            {
                seen.lock().unwrap().push(table_name.to_string());
            }
            rusqlite::hooks::Authorization::Allow
        }));
        let queue = DirtyList::default();
        let capture = Capture::install(
            &connection,
            SCOPE,
            settings,
            Default::default(),
            queue.clone(),
        )
        .unwrap();
        Self {
            connection,
            capture,
            queue,
            drops,
        }
    }

    /// Drop the capture and install a new one on the same connection: the
    /// cell's next residency.
    fn reinstall(self) -> Self {
        let Self {
            connection,
            capture,
            ..
        } = self;
        drop(capture);
        Self::install(connection, settings())
    }

    fn run(&self, sql: &str) {
        self.connection.execute_batch(sql).unwrap();
    }

    fn checkpoint(&mut self) -> Checkpoint {
        let drops = std::mem::take(&mut *self.drops.lock().unwrap());
        self.capture.hint_dropped(&drops);
        self.capture.checkpoint(&self.connection, 1_790_000_000_000)
    }

    fn pull(&mut self) -> CapturedCommit {
        match self.checkpoint() {
            Checkpoint::Pulled(commit) => commit,
            other => panic!("expected a commit, got {other:?}"),
        }
    }

    fn queued(&self) -> Vec<String> {
        std::mem::take(&mut *self.queue.borrow_mut())
    }
}

const SHAPES: &str = "
    CREATE TABLE keyed (id TEXT PRIMARY KEY, v INTEGER);
    CREATE TABLE plain (v INTEGER, w TEXT);
    CREATE TABLE norowid (a INTEGER, b TEXT, v BLOB, PRIMARY KEY (b, a)) WITHOUT ROWID;
";

fn int(i: i64) -> Value {
    Value::Integer(i)
}

fn text(t: &str) -> Value {
    Value::Text(t.to_string())
}

fn table<'a>(commit: &'a CapturedCommit, name: &str) -> &'a TableRows {
    commit
        .tables
        .iter()
        .find(|t| t.table == name)
        .unwrap_or_else(|| panic!("no rows for {name} in {commit:?}"))
}

#[test]
fn exported_table_names() {
    for name in ["users", "_cf_KV", "__kv", "__queue", "cf_x", "_cfx"] {
        assert!(exported_table(name), "{name}");
    }
    for name in [
        "sqlite_sequence",
        "SQLITE_stat1",
        "_litestream_seq",
        "_litestream_lock",
        "_cf_METADATA",
        "_cf_ALARM",
        "_cf_WAKE",
        "_CF_kv",
        "__queue_messages",
        "__queue_meta",
        "__queue_stats",
        "__queue_transfer_receipts",
        "__kv_meta",
    ] {
        assert!(!exported_table(name), "{name}");
    }
}

#[test]
fn inserts_carry_whole_rows_and_keys_for_every_table_shape() {
    let mut f = Fixture::new(SHAPES);
    f.run(
        "INSERT INTO keyed VALUES ('a', 1);
         INSERT INTO plain VALUES (2, 'two');
         INSERT INTO norowid VALUES (3, 'three', x'ff');",
    );
    let commit = f.pull();
    assert_eq!(commit.seq, 1);
    assert!(commit.bulk.is_empty());
    assert_eq!(
        commit
            .tables
            .iter()
            .map(|t| t.table.as_str())
            .collect::<Vec<_>>(),
        ["keyed", "plain", "norowid"]
    );

    let keyed = table(&commit, "keyed");
    assert_eq!(keyed.generation, FIRST_GENERATION);
    assert_eq!(keyed.columns, ["id", "v"]);
    assert_eq!(keyed.key_columns, ["id"]);
    assert_eq!(
        keyed.rows,
        [RowChange(
            Op::Insert,
            vec![text("a")],
            vec![text("a"), int(1)]
        )]
    );

    let plain = table(&commit, "plain");
    assert_eq!(plain.columns, ["v", "w"]);
    assert_eq!(plain.key_columns, [ROWID_KEY_COLUMN]);
    assert_eq!(
        plain.rows,
        [RowChange(
            Op::Insert,
            vec![int(1)],
            vec![int(2), text("two")]
        )]
    );

    // The key follows the declared key order, not column order.
    let norowid = table(&commit, "norowid");
    assert_eq!(norowid.key_columns, ["b", "a"]);
    assert_eq!(
        norowid.rows,
        [RowChange(
            Op::Insert,
            vec![text("three"), int(3)],
            vec![int(3), text("three"), Value::Blob(vec![0xff])]
        )]
    );
}

#[test]
fn updates_read_the_after_image_and_deletes_carry_the_before_image() {
    let mut f = Fixture::new(SHAPES);
    f.run(
        "INSERT INTO keyed VALUES ('a', 1), ('b', 2);
         INSERT INTO plain VALUES (10, 'x');",
    );
    f.pull();
    f.run(
        "UPDATE keyed SET v = 5 WHERE id = 'a';
         DELETE FROM keyed WHERE id = 'b';
         UPDATE plain SET w = 'y';",
    );
    let commit = f.pull();
    assert_eq!(commit.seq, 2);
    let mut keyed = table(&commit, "keyed").rows.clone();
    keyed.sort_by(|a, b| a.key().cmp(b.key()));
    assert_eq!(
        keyed,
        [
            // The whole row, though only `v` changed.
            RowChange(Op::Update, vec![text("a")], vec![text("a"), int(5)]),
            RowChange(Op::Delete, vec![text("b")], vec![text("b"), int(2)]),
        ]
    );
    assert_eq!(
        table(&commit, "plain").rows,
        [RowChange(
            Op::Update,
            vec![int(1)],
            vec![int(10), text("y")]
        )]
    );
}

#[test]
fn a_key_change_is_a_delete_and_an_insert() {
    let mut f = Fixture::new(SHAPES);
    f.run("INSERT INTO keyed VALUES ('a', 1);");
    f.pull();
    f.run("UPDATE keyed SET id = 'z' WHERE id = 'a';");
    let mut rows = table(&f.pull(), "keyed").rows.clone();
    rows.sort_by_key(|r| r.op());
    assert_eq!(
        rows,
        [
            RowChange(Op::Insert, vec![text("z")], vec![text("z"), int(1)]),
            RowChange(Op::Delete, vec![text("a")], vec![text("a"), int(1)]),
        ]
    );
}

#[test]
fn rowid_change_on_keyed_table_is_no_change() {
    let mut f = Fixture::new(SHAPES);
    f.run("INSERT INTO keyed VALUES ('a', 1);");
    f.pull();
    f.run("UPDATE keyed SET rowid = 100 WHERE id = 'a';");
    assert_eq!(f.checkpoint(), Checkpoint::Clean);
}

#[test]
fn rowid_change_on_rowid_only_table_is_delete_and_insert() {
    let mut f = Fixture::new(SHAPES);
    f.run("INSERT INTO plain VALUES (7, 'x');");
    f.pull();
    f.run("UPDATE plain SET rowid = 100;");
    let mut rows = table(&f.pull(), "plain").rows.clone();
    rows.sort_by_key(|r| r.op());
    assert_eq!(
        rows,
        [
            RowChange(Op::Insert, vec![int(100)], vec![int(7), text("x")]),
            RowChange(Op::Delete, vec![int(1)], vec![int(7), text("x")]),
        ]
    );
}

#[test]
fn insert_and_delete_in_one_host_call_nets_to_nothing() {
    let mut f = Fixture::new(SHAPES);
    f.run(
        "INSERT INTO keyed VALUES ('a', 1); DELETE FROM keyed;
         BEGIN; INSERT INTO plain VALUES (1, 'x'); DELETE FROM plain; COMMIT;",
    );
    assert_eq!(f.checkpoint(), Checkpoint::Clean);
}

#[test]
fn rollback_and_rollback_to_drop_reverted_rows() {
    let mut f = Fixture::new(SHAPES);
    f.run("BEGIN; INSERT INTO keyed VALUES ('gone', 1); ROLLBACK;");
    assert_eq!(f.checkpoint(), Checkpoint::Clean);
    f.run(
        "BEGIN;
         INSERT INTO keyed VALUES ('kept', 1);
         SAVEPOINT s;
         INSERT INTO keyed VALUES ('reverted', 2);
         INSERT INTO plain VALUES (3, 'reverted');
         ROLLBACK TO s;
         RELEASE s;
         COMMIT;",
    );
    let commit = f.pull();
    assert_eq!(commit.tables.len(), 1);
    assert_eq!(
        table(&commit, "keyed").rows,
        [RowChange(
            Op::Insert,
            vec![text("kept")],
            vec![text("kept"), int(1)]
        )]
    );
}

#[test]
fn an_open_explicit_transaction_defers_the_pull() {
    let mut f = Fixture::new(SHAPES);
    f.run("BEGIN; INSERT INTO keyed VALUES ('a', 1);");
    assert_eq!(f.checkpoint(), Checkpoint::Deferred);
    assert!(f.capture.needs_visit());
    f.run("INSERT INTO keyed VALUES ('b', 2); COMMIT;");
    assert_eq!(table(&f.pull(), "keyed").rows.len(), 2);
    assert!(!f.capture.needs_visit());
}

/// The design's probe: autocommit reports on while a `RETURNING` cursor holds
/// its write transaction open, and a pull there would read uncommitted rows.
#[test]
fn an_open_returning_cursor_refuses_the_pull() {
    let mut f = Fixture::new(SHAPES);
    // SAFETY: the fixture owns the live connection.
    let database = unsafe { f.connection.handle() };
    let mut statement = ptr::null_mut();
    // SAFETY: a live connection; the statement is finalized below.
    unsafe {
        let sql = c"INSERT INTO keyed VALUES ('a', 1), ('b', 2) RETURNING id";
        let rc =
            ffi::sqlite3_prepare_v2(database, sql.as_ptr(), -1, &mut statement, ptr::null_mut());
        assert_eq!(rc, ffi::SQLITE_OK);
        assert_eq!(ffi::sqlite3_step(statement), ffi::SQLITE_ROW);
    }
    assert!(f.connection.is_autocommit());
    assert_eq!(f.checkpoint(), Checkpoint::Deferred);
    // SAFETY: the statement prepared above.
    unsafe {
        while ffi::sqlite3_step(statement) == ffi::SQLITE_ROW {}
        ffi::sqlite3_finalize(statement);
    }
    assert_eq!(table(&f.pull(), "keyed").rows.len(), 2);
}

/// A lazy read cursor can stay open across awaits indefinitely, so it must
/// not block export the way the design's "no busy statement" rule would.
#[test]
fn an_open_read_cursor_does_not_block_the_pull() {
    let mut f = Fixture::new(SHAPES);
    f.run("INSERT INTO keyed VALUES ('a', 1), ('b', 2);");
    f.pull();
    // SAFETY: the fixture owns the live connection.
    let database = unsafe { f.connection.handle() };
    let mut statement = ptr::null_mut();
    // SAFETY: a live connection; the statement is finalized below.
    unsafe {
        let sql = c"SELECT id FROM keyed";
        let rc =
            ffi::sqlite3_prepare_v2(database, sql.as_ptr(), -1, &mut statement, ptr::null_mut());
        assert_eq!(rc, ffi::SQLITE_OK);
        assert_eq!(ffi::sqlite3_step(statement), ffi::SQLITE_ROW);
    }
    f.run("INSERT INTO plain VALUES (1, 'x');");
    assert_eq!(table(&f.pull(), "plain").rows.len(), 1);
    // SAFETY: the statement prepared above.
    unsafe { ffi::sqlite3_finalize(statement) };
}

#[test]
fn excluded_tables_are_not_captured() {
    let mut f = Fixture::new(
        "CREATE TABLE _cf_METADATA (scope TEXT PRIMARY KEY, actor_name TEXT);
         CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
         CREATE TABLE __queue_messages (id INTEGER PRIMARY KEY, body BLOB);
         CREATE TABLE __kv_meta (k TEXT PRIMARY KEY, v TEXT);
         CREATE TABLE counters (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER);",
    );
    f.run(
        "INSERT INTO _cf_METADATA VALUES ('s', 'n');
         INSERT INTO _litestream_seq VALUES (1, 1);
         INSERT INTO __queue_messages VALUES (1, x'00');
         INSERT INTO __kv_meta VALUES ('k', 'v');",
    );
    // Every commit is visited, since only a visit sees schema changes, and
    // the visit finds nothing to export.
    assert_eq!(f.queued(), [SCOPE]);
    assert_eq!(f.checkpoint(), Checkpoint::Clean);
    assert!(!f.capture.needs_visit());
    // AUTOINCREMENT writes `sqlite_sequence` beside the row.
    f.run("INSERT INTO counters(n) VALUES (1);");
    assert_eq!(f.queued(), [SCOPE]);
    let commit = f.pull();
    assert_eq!(
        commit
            .tables
            .iter()
            .map(|t| t.table.as_str())
            .collect::<Vec<_>>(),
        ["counters"]
    );
}

#[test]
fn the_kv_table_is_exported() {
    let mut f =
        Fixture::new("CREATE TABLE _cf_KV (scope TEXT, k TEXT, v BLOB, PRIMARY KEY (scope, k));");
    f.run("INSERT INTO _cf_KV VALUES ('s', 'key', x'0f22');");
    let kv = f.pull();
    let kv = table(&kv, "_cf_KV");
    assert_eq!(kv.key_columns, ["scope", "k"]);
    assert_eq!(
        kv.rows,
        [RowChange(
            Op::Insert,
            vec![text("s"), text("key")],
            vec![text("s"), text("key"), Value::Blob(vec![0x0f, 0x22])]
        )]
    );
}

/// The key-value tables are described in the shape they are exported in:
/// `_cf_KV` as `kv (key, value)`, and `__kv` with its `blob_key`.
#[test]
fn key_value_tables_are_described_as_exported() {
    let mut f = Fixture::new(
        "CREATE TABLE _cf_KV (scope TEXT, k TEXT, v BLOB, PRIMARY KEY (scope, k));
         CREATE TABLE __kv (key TEXT PRIMARY KEY, value BLOB, metadata TEXT, expiration INTEGER);",
    );
    f.run(
        "INSERT INTO _cf_KV VALUES ('s', 'key', x'0f22');
         INSERT INTO __kv VALUES ('a', x'00', NULL, NULL);",
    );
    let commit = f.pull();
    let kv = schema(&commit, "kv");
    assert_eq!(kv.generation, 1);
    let columns: Vec<_> = kv
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.decl_type.as_str(), c.pk))
        .collect();
    assert_eq!(columns, [("key", "TEXT", 1), ("value", "", 0)]);
    let namespace: Vec<_> = schema(&commit, "__kv")
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(
        namespace,
        ["key", "value", "metadata", "expiration", "blob_key"]
    );
    // deleteAll drops and recreates `_cf_KV`: the drop names `kv` too.
    f.run(
        "DROP TABLE _cf_KV;
         CREATE TABLE _cf_KV (scope TEXT, k TEXT, v BLOB, PRIMARY KEY (scope, k));",
    );
    let commit = f.pull();
    let kv: Vec<_> = commit
        .schemas
        .iter()
        .map(|s| (s.table.as_str(), s.generation))
        .collect();
    assert_eq!(kv, [("kv", 2)]);
}

#[test]
fn virtual_tables_and_their_shadow_tables_are_excluded() {
    let mut f = Fixture::new(
        "CREATE VIRTUAL TABLE docs USING fts5(body);
         CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);",
    );
    f.run("INSERT INTO docs VALUES ('hello'); INSERT INTO notes VALUES (1, 'n');");
    let commit = f.pull();
    assert_eq!(
        commit
            .tables
            .iter()
            .map(|t| t.table.as_str())
            .collect::<Vec<_>>(),
        ["notes"]
    );
    // Created mid-session: its shadow writes are filtered at the pull, and a
    // schema record names it unsupported.
    f.run(
        "CREATE VIRTUAL TABLE later USING fts5(body);
         INSERT INTO later VALUES ('x');",
    );
    let commit = f.pull();
    assert!(commit.tables.is_empty() && commit.bulk.is_empty() && commit.snapshot.is_none());
    assert_eq!(commit.schemas.len(), 1, "{commit:?}");
    let later = &commit.schemas[0];
    assert!(later.unsupported && later.table == "later" && later.generation == 1);
    assert_eq!(later.columns.len(), 1);
}

/// A table with a generated column fails a session's whole changeset with
/// `SQLITE_SCHEMA`, so the filter leaves it untracked and its writes become
/// `bulk` while the other tables of the commit keep their rows.
#[test]
fn a_table_with_generated_columns_is_bulk() {
    let mut f = Fixture::new(
        "CREATE TABLE g (id INTEGER PRIMARY KEY, a INTEGER,
            twice INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL);
         CREATE TABLE s (id INTEGER PRIMARY KEY, a INTEGER,
            thrice INTEGER AS (a * 3) STORED);
         CREATE TABLE n (id INTEGER PRIMARY KEY, a INTEGER);",
    );
    f.run(
        "INSERT INTO g(id, a) VALUES (1, 5);
         INSERT INTO n VALUES (1, 5);
         INSERT INTO s(id, a) VALUES (1, 5);",
    );
    let commit = f.pull();
    assert_eq!(table(&commit, "n").rows.len(), 1);
    let bulk: Vec<_> = commit.bulk.iter().map(|t| t.table.as_str()).collect();
    assert_eq!(bulk, ["g", "s"]);
    // Alone, it still marks the cell dirty and produces a commit.
    f.run("UPDATE g SET a = 6;");
    assert_eq!(f.queued(), [SCOPE, SCOPE]);
    let commit = f.pull();
    assert!(commit.tables.is_empty());
    assert_eq!(commit.bulk.len(), 1);
}

#[test]
fn quoted_names_round_trip() {
    let mut f =
        Fixture::new(r#"CREATE TABLE "we""ird" ("co""l" TEXT PRIMARY KEY, "select" INTEGER);"#);
    f.run(r#"INSERT INTO "we""ird" VALUES ('k', 1); UPDATE "we""ird" SET "select" = 2;"#);
    let commit = f.pull();
    let t = table(&commit, "we\"ird");
    assert_eq!(t.columns, ["co\"l", "select"]);
    assert_eq!(t.rows[0].row(), [text("k"), int(2)]);
}

#[test]
fn a_rowid_only_table_with_a_rowid_column_uses_another_alias() {
    let mut f = Fixture::new("CREATE TABLE r (rowid TEXT, v INTEGER);");
    f.run("INSERT INTO r VALUES ('not the rowid', 1);");
    let commit = f.pull();
    assert_eq!(
        table(&commit, "r").rows,
        [RowChange(
            Op::Insert,
            vec![int(1)],
            vec![text("not the rowid"), int(1)]
        )]
    );
}

#[test]
fn the_filter_queues_a_cell_once_per_pull() {
    let mut f = Fixture::new(SHAPES);
    f.run("INSERT INTO keyed VALUES ('a', 1); INSERT INTO plain VALUES (1, 'x');");
    f.run("INSERT INTO keyed VALUES ('b', 2);");
    assert_eq!(f.queued(), [SCOPE]);
    f.pull();
    f.run("INSERT INTO keyed VALUES ('c', 3);");
    assert_eq!(f.queued(), [SCOPE]);
}

#[test]
fn a_transaction_over_budget_becomes_bulk_for_every_exported_table() {
    let mut f = Fixture::with_settings(
        &format!("{SHAPES} CREATE TABLE _cf_ALARM (scope TEXT PRIMARY KEY);"),
        Settings { max_tx_bytes: 4096 },
    );
    f.run("BEGIN;");
    for i in 0..200 {
        f.run(&format!("INSERT INTO keyed VALUES ('{i:0>40}', {i});"));
        // The check point after each op enforces the budget.
        if i % 10 == 0 {
            assert_eq!(f.checkpoint(), Checkpoint::Deferred);
        }
    }
    f.run("COMMIT;");
    let commit = f.pull();
    assert!(commit.tables.is_empty());
    let bulk: Vec<_> = commit.bulk.iter().map(|t| t.table.as_str()).collect();
    assert_eq!(bulk, ["keyed", "norowid", "plain"]);
    // Tracking resumes with the next transaction.
    f.run("INSERT INTO plain VALUES (1, 'x');");
    let commit = f.pull();
    assert!(commit.bulk.is_empty());
    assert_eq!(table(&commit, "plain").rows.len(), 1);
}

fn schema<'a>(commit: &'a CapturedCommit, name: &str) -> &'a SchemaBody {
    commit
        .schemas
        .iter()
        .find(|s| s.table == name)
        .unwrap_or_else(|| panic!("no schema for {name} in {commit:?}"))
}

fn snapshotted<'a>(commit: &'a CapturedCommit, name: &str) -> &'a TableRows {
    commit
        .snapshot
        .as_ref()
        .and_then(|s| s.tables.iter().find(|t| t.table == name))
        .unwrap_or_else(|| panic!("no snapshot of {name} in {commit:?}"))
}

fn stored(f: &Fixture) -> Vec<(String, i64, Option<String>)> {
    let mut statement = f
        .connection
        .prepare("SELECT name, generation, schema_sql FROM _cf_EXPORT ORDER BY name")
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// A cell's first capture starts every table at generation one without a
/// record, stores the generations, and describes each generation once, with
/// its first rows.
#[test]
fn first_rows_of_a_generation_carry_its_definition() {
    let mut f = Fixture::new(SHAPES);
    assert_eq!(f.checkpoint(), Checkpoint::Clean);
    assert_eq!(
        stored(&f)
            .into_iter()
            .map(|(name, generation, _)| (name, generation))
            .collect::<Vec<_>>(),
        [
            ("keyed".to_string(), 1),
            ("norowid".to_string(), 1),
            ("plain".to_string(), 1)
        ]
    );
    f.run("INSERT INTO keyed VALUES ('a', 1);");
    let commit = f.pull();
    let keyed = schema(&commit, "keyed");
    assert_eq!(keyed.generation, 1);
    assert_eq!(
        keyed.sql,
        "CREATE TABLE keyed (id TEXT PRIMARY KEY, v INTEGER)"
    );
    assert_eq!(
        keyed.columns,
        [
            ColumnDef {
                name: "id".into(),
                decl_type: "TEXT".into(),
                pk: 1,
                not_null: false,
                generated: false,
            },
            ColumnDef {
                name: "v".into(),
                decl_type: "INTEGER".into(),
                pk: 0,
                not_null: false,
                generated: false,
            }
        ]
    );
    assert_eq!(commit.schemas.len(), 1);
    f.run("INSERT INTO keyed VALUES ('b', 2);");
    assert!(f.pull().schemas.is_empty());
}

/// A create opens generation one with a schema record and a snapshot of
/// what the table holds at the commit; later commits carry its rows.
#[test]
fn a_created_table_opens_its_first_generation() {
    let mut f = Fixture::new(SHAPES);
    f.run(
        "INSERT INTO keyed VALUES ('a', 1);
         CREATE TABLE fresh (id INTEGER PRIMARY KEY, v TEXT);
         INSERT INTO fresh VALUES (1, 'a');",
    );
    let commit = f.pull();
    let fresh = schema(&commit, "fresh");
    assert_eq!((fresh.generation, fresh.dropped), (1, false));
    assert_eq!(fresh.renamed_from, None);
    assert_eq!(
        snapshotted(&commit, "fresh").rows,
        [RowChange(Op::Insert, vec![int(1)], vec![int(1), text("a")])]
    );
    assert_eq!(commit.tables.len(), 1);
    assert_eq!(table(&commit, "keyed").rows.len(), 1);
    // A create alone, with no rows, is reported with an empty snapshot.
    f.run("CREATE TABLE empty (id INTEGER PRIMARY KEY);");
    let commit = f.pull();
    assert_eq!(schema(&commit, "empty").generation, 1);
    assert!(snapshotted(&commit, "empty").rows.is_empty());
    assert!(commit.tables.is_empty());
    f.run("INSERT INTO fresh VALUES (2, 'b');");
    assert_eq!(table(&f.pull(), "fresh").rows.len(), 1);
}

/// A table created, written and renamed between two pulls: the session
/// tracked its rows under the first name, which never reached the schema.
#[test]
fn a_table_created_and_renamed_in_one_interval_keeps_its_rows() {
    let mut f = Fixture::new(SHAPES);
    f.run(
        "CREATE TABLE first (id INTEGER PRIMARY KEY, v TEXT);
         INSERT INTO first VALUES (1, 'a');
         ALTER TABLE first RENAME TO second;",
    );
    let commit = f.pull();
    assert_eq!(schema(&commit, "second").generation, 1);
    assert_eq!(snapshotted(&commit, "second").rows.len(), 1);
    assert!(commit.bulk.is_empty());
}

/// An added column changes every row's logical value without a row event,
/// so it opens a new generation and snapshots the table under it.
#[test]
fn an_added_column_opens_a_generation_with_a_snapshot() {
    let mut f = Fixture::new(SHAPES);
    f.run("INSERT INTO plain VALUES (1, 'x'); INSERT INTO keyed VALUES ('a', 1);");
    f.pull();
    f.run(
        "INSERT INTO keyed VALUES ('b', 2);
         ALTER TABLE plain ADD COLUMN extra INTEGER DEFAULT 9;
         INSERT INTO plain VALUES (2, 'y', 3);",
    );
    let commit = f.pull();
    let plain = schema(&commit, "plain");
    assert_eq!(plain.generation, 2);
    assert!(
        plain.sql.contains("extra INTEGER DEFAULT 9"),
        "{}",
        plain.sql
    );
    assert_eq!(plain.columns.len(), 3);
    let snapshot = snapshotted(&commit, "plain");
    assert_eq!(snapshot.generation, 2);
    assert_eq!(snapshot.columns, ["v", "w", "extra"]);
    let rows: Vec<_> = snapshot.rows.iter().map(|r| r.row().to_vec()).collect();
    assert_eq!(
        rows,
        [
            vec![int(1), text("x"), int(9)],
            vec![int(2), text("y"), int(3)]
        ]
    );
    assert!(snapshot.rows.iter().all(|r| r.op() == Op::Insert));
    // The untouched table keeps its generation and its rows.
    assert_eq!(table(&commit, "keyed").generation, 1);
    assert!(commit.tables.iter().all(|t| t.table != "plain"));
    assert_eq!(stored(&f)[2], ("plain".into(), 2, Some(plain.sql.clone())));
}

/// A drop closes the generation. A table written and then dropped in one
/// interval fails the changeset as a whole; the remaining tables are
/// snapshotted rather than all becoming `bulk`.
#[test]
fn a_dropped_table_closes_its_generation() {
    let mut f = Fixture::new(SHAPES);
    f.run(
        "INSERT INTO keyed VALUES ('a', 1);
         INSERT INTO plain VALUES (1, 'x');
         DROP TABLE plain;",
    );
    let commit = f.pull();
    let plain = schema(&commit, "plain");
    assert!(plain.dropped);
    assert_eq!(plain.generation, 1);
    assert!(commit.bulk.is_empty(), "{commit:?}");
    let keyed = snapshotted(&commit, "keyed");
    assert_eq!((keyed.generation, keyed.rows.len()), (1, 1));
    // The next commit is captured normally.
    f.run("INSERT INTO keyed VALUES ('b', 2);");
    assert_eq!(table(&f.pull(), "keyed").rows.len(), 1);
    // A drop alone, with nothing written, is reported too.
    f.run("DROP TABLE norowid;");
    let commit = f.pull();
    assert!(schema(&commit, "norowid").dropped);
    assert_eq!(stored(&f)[1], ("norowid".into(), 1, None));
}

/// Dropping a column from a table the session recorded stops the session
/// for the rest of the interval, so a table written after it was never seen.
/// Every table is snapshotted instead.
#[test]
fn a_dropped_column_stops_the_session_and_every_table_is_snapshotted() {
    let mut f = Fixture::new(SHAPES);
    f.run("ALTER TABLE plain ADD COLUMN extra INTEGER;");
    f.pull();
    f.run(
        "BEGIN;
         INSERT INTO plain VALUES (1, 'x', 2);
         ALTER TABLE plain DROP COLUMN extra;
         INSERT INTO keyed VALUES ('a', 1);
         COMMIT;",
    );
    let commit = f.pull();
    assert_eq!(schema(&commit, "plain").generation, 3);
    assert_eq!(snapshotted(&commit, "plain").rows.len(), 1);
    assert_eq!(snapshotted(&commit, "keyed").rows.len(), 1);
    assert!(snapshotted(&commit, "norowid").rows.is_empty());
    assert!(commit.tables.is_empty() && commit.bulk.is_empty());
}

/// A recreated table continues from the old generation, so rows of the old
/// one cannot resurrect.
#[test]
fn a_recreated_table_is_a_new_generation() {
    let mut f = Fixture::new(SHAPES);
    f.run("INSERT INTO plain VALUES (1, 'x'); DROP TABLE plain;");
    f.pull();
    f.run("CREATE TABLE plain (v INTEGER, w TEXT); INSERT INTO plain VALUES (2, 'y');");
    let commit = f.pull();
    assert_eq!(schema(&commit, "plain").generation, 2);
    assert_eq!(snapshotted(&commit, "plain").rows.len(), 1);

    // Dropped and recreated with the same definition between two pulls:
    // `sqlite_schema` looks the same, and only the dropped hint tells.
    f.run(
        "INSERT INTO plain VALUES (3, 'z');
         DROP TABLE plain;
         CREATE TABLE plain (v INTEGER, w TEXT);
         INSERT INTO plain VALUES (4, 'w');",
    );
    let commit = f.pull();
    assert_eq!(schema(&commit, "plain").generation, 3);
    let plain = snapshotted(&commit, "plain");
    assert_eq!(plain.generation, 3);
    assert_eq!(plain.rows.len(), 1);
    assert_eq!(plain.rows[0].row(), [int(4), text("w")]);
}

/// A rename keeps the root page: the new name opens a generation with
/// `renamed_from`, is snapshotted, and the old name gets no record of its
/// own.
#[test]
fn a_rename_opens_a_generation_renamed_from_the_old_name() {
    let mut f = Fixture::new(SHAPES);
    f.run("INSERT INTO keyed VALUES ('a', 1);");
    f.pull();
    f.run("ALTER TABLE keyed RENAME TO renamed; INSERT INTO renamed VALUES ('b', 2);");
    let commit = f.pull();
    assert_eq!(commit.schemas.len(), 1, "{commit:?}");
    let renamed = schema(&commit, "renamed");
    assert_eq!(renamed.generation, 1);
    assert_eq!(renamed.renamed_from.as_deref(), Some("keyed"));
    assert_eq!(snapshotted(&commit, "renamed").rows.len(), 2);
    assert!(commit.tables.is_empty() && commit.bulk.is_empty());
    // Back to the old name: that name continues from its old generation.
    f.run("ALTER TABLE renamed RENAME TO keyed;");
    let commit = f.pull();
    let keyed = schema(&commit, "keyed");
    assert_eq!(keyed.generation, 2);
    assert_eq!(keyed.renamed_from.as_deref(), Some("renamed"));
}

/// Generations live in the cell. A later residency continues from them, and
/// a change made while nothing captured the cell is reported by its first
/// pull, with a snapshot since no session saw the rows move.
#[test]
fn generations_survive_a_new_residency() {
    let mut f = Fixture::new(SHAPES);
    f.run("DROP TABLE plain;");
    f.pull();
    let mut f = f.reinstall();
    f.run("CREATE TABLE plain (v INTEGER, w TEXT);");
    assert_eq!(schema(&f.pull(), "plain").generation, 2);
    // Unobserved: the capture is gone while the table changes.
    drop(f.capture);
    f.connection
        .execute_batch(
            "INSERT INTO keyed VALUES ('a', 1);
             ALTER TABLE keyed ADD COLUMN extra TEXT;
             CREATE TABLE later (id INTEGER PRIMARY KEY);
             INSERT INTO later VALUES (1);",
        )
        .unwrap();
    let mut f = Fixture::install(f.connection, settings());
    assert!(f.capture.needs_visit());
    let commit = f.pull();
    assert_eq!(schema(&commit, "keyed").generation, 2);
    assert_eq!(snapshotted(&commit, "keyed").rows.len(), 1);
    assert_eq!(schema(&commit, "later").generation, 1);
    assert_eq!(snapshotted(&commit, "later").rows.len(), 1);
    assert!(commit.tables.is_empty());
}

/// A table too large for the transaction budget is `bulk` under its new
/// generation instead of snapshotted.
#[test]
fn a_large_table_is_bulk_instead_of_snapshotted() {
    let mut f = Fixture::with_settings(SHAPES, Settings { max_tx_bytes: 2048 });
    for i in 0..100 {
        f.run(&format!("INSERT INTO plain VALUES ({i}, '{i:0>20}');"));
        f.pull();
    }
    f.run("ALTER TABLE plain ADD COLUMN extra INTEGER;");
    let commit = f.pull();
    assert_eq!(schema(&commit, "plain").generation, 2);
    assert_eq!(
        commit.bulk,
        [TableGen {
            table: "plain".into(),
            generation: 2
        }]
    );
    assert!(commit.snapshot.as_ref().is_none_or(|s| s.tables.is_empty()));
}

/// A table the operator denies is not described either.
#[test]
fn denied_tables_have_no_generations() {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(SHAPES).unwrap();
    let queue = DirtyList::default();
    let mut capture = Capture::install(
        &connection,
        SCOPE,
        settings(),
        HashSet::from(["plain".to_string()]),
        queue,
    )
    .unwrap();
    connection
        .execute_batch("DROP TABLE plain; CREATE TABLE plain2 (v INTEGER);")
        .unwrap();
    let Checkpoint::Pulled(commit) = capture.checkpoint(&connection, 0) else {
        panic!("expected a commit");
    };
    let names: Vec<_> = commit.schemas.iter().map(|s| s.table.as_str()).collect();
    assert_eq!(names, ["plain2"]);
}

/// Records built from captured commits, applied by the reference consumer,
/// hold exactly the rows the database holds.
fn assert_consumer_matches(f: &Fixture, consumer: &Consumer) {
    let state = consumer.stream(&stream()).unwrap_or_default();
    for name in ["keyed", "plain", "norowid"] {
        let mut statement = f
            .connection
            .prepare(&match name {
                "keyed" => "SELECT id, id, v FROM keyed".to_string(),
                "plain" => "SELECT rowid, v, w FROM plain".to_string(),
                _ => "SELECT b, a, a, b, v FROM norowid".to_string(),
            })
            .unwrap();
        let key_width = if name == "norowid" { 2 } else { 1 };
        let expected: std::collections::BTreeMap<Vec<Value>, Vec<Value>> = statement
            .query_map([], |row| {
                let width = row.as_ref().column_count();
                let values: Vec<Value> = (0..width)
                    .map(|i| from_row(row.get_ref(i).unwrap()))
                    .collect();
                Ok((values[..key_width].to_vec(), values[key_width..].to_vec()))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let actual = state
            .table(name)
            .map(|t| t.rows.clone())
            .unwrap_or_default();
        assert_eq!(actual, expected, "{name}");
    }
}

fn stream() -> StreamId {
    StreamId {
        script: "app".into(),
        class: "Room".into(),
        cell: SCOPE.into(),
        facet: None,
        incarnation: 1,
    }
}

/// The records release builds from a commit, in its order, without
/// fragmenting.
fn records(commit: &CapturedCommit) -> Vec<Record> {
    let envelope = Envelope {
        stream: stream(),
        cell_name: None,
        position: Position::new(1, commit.seq, commit.seq),
        committed_at: commit.committed_at,
        node: "node-a".into(),
        origin: Origin::Live,
        fragment: 1,
        fragments: 1,
    };
    let record = |body| Record {
        envelope: envelope.clone(),
        body,
    };
    let mut out: Vec<Record> = commit
        .schemas
        .iter()
        .map(|s| record(Body::Schema(s.clone())))
        .collect();
    if let Some(snapshot) = &commit.snapshot {
        for table in &snapshot.tables {
            out.push(record(Body::Snapshot(SnapshotBody {
                snapshot_id: snapshot.id.clone(),
                data: table.clone(),
            })));
        }
        out.push(record(Body::SnapshotEnd(SnapshotEndBody {
            snapshot_id: snapshot.id.clone(),
            scope: SnapshotScope::Tables,
            tables: snapshot.tables.iter().map(TableRows::table_gen).collect(),
            records: snapshot.tables.len() as u64,
        })));
    }
    for rows in &commit.tables {
        out.push(record(Body::Rows(RowsBody { data: rows.clone() })));
    }
    if !commit.bulk.is_empty() {
        out.push(record(Body::Bulk(BulkBody {
            tables: commit.bulk.clone(),
        })));
    }
    out
}

fn ingest(consumer: &mut Consumer, commit: &CapturedCommit) {
    assert!(commit.bulk.is_empty(), "{commit:?}");
    consumer.ingest_all(records(commit)).unwrap();
}

#[derive(Clone, Debug)]
enum Step {
    Upsert { table: u8, key: u8, value: i64 },
    Delete { table: u8, key: u8 },
    Rekey { table: u8, from: u8, to: u8 },
    Savepoint,
    RollbackTo,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (0..3u8, 0..6u8, any::<i64>()).prop_map(|(table, key, value)| Step::Upsert { table, key, value }),
        2 => (0..3u8, 0..6u8).prop_map(|(table, key)| Step::Delete { table, key }),
        1 => (0..3u8, 0..6u8, 0..6u8).prop_map(|(table, from, to)| Step::Rekey { table, from, to }),
        1 => Just(Step::Savepoint),
        1 => Just(Step::RollbackTo),
    ]
}

fn sql(step: &Step) -> String {
    let name = |table: &u8| ["keyed", "plain", "norowid"][usize::from(*table)];
    match step {
        Step::Upsert { table: 0, key, value } => format!(
            "INSERT INTO keyed VALUES ('k{key}', {value}) ON CONFLICT(id) DO UPDATE SET v = excluded.v;"
        ),
        Step::Upsert { table: 1, key, value } => format!(
            "INSERT INTO plain(rowid, v, w) VALUES ({key}, {value}, 'w{key}') \
             ON CONFLICT(rowid) DO UPDATE SET v = excluded.v;"
        ),
        Step::Upsert { key, value, .. } => format!(
            "INSERT INTO norowid VALUES ({key}, 'b{key}', x'{:02x}') \
             ON CONFLICT(b, a) DO UPDATE SET v = excluded.v;",
            value.unsigned_abs() % 256
        ),
        Step::Delete { table: 0, key } => format!("DELETE FROM keyed WHERE id = 'k{key}';"),
        Step::Delete { table: 1, key } => format!("DELETE FROM plain WHERE rowid = {key};"),
        Step::Delete { key, .. } => format!("DELETE FROM norowid WHERE a = {key};"),
        Step::Rekey { table: 0, from, to } => {
            format!("UPDATE OR REPLACE keyed SET id = 'k{to}' WHERE id = 'k{from}';")
        }
        Step::Rekey { table, from, to } if *table == 1 => format!(
            "UPDATE OR REPLACE {} SET rowid = {to} WHERE rowid = {from};",
            name(table)
        ),
        Step::Rekey { from, to, .. } => format!(
            "UPDATE OR REPLACE norowid SET a = {to}, b = 'b{to}' WHERE a = {from};"
        ),
        Step::Savepoint => "SAVEPOINT s;".into(),
        Step::RollbackTo => "ROLLBACK TO s;".into(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Random transactions with savepoints and key changes: after every
    /// commit, the reference consumer's state equals the database.
    #[test]
    fn the_reference_consumer_converges(
        transactions in prop::collection::vec(prop::collection::vec(step(), 1..12), 1..10)
    ) {
        let mut f = Fixture::new(SHAPES);
        let mut consumer = Consumer::new();
        for transaction in &transactions {
            f.run("BEGIN;");
            let mut savepoint = false;
            for step in transaction {
                match step {
                    Step::RollbackTo if !savepoint => continue,
                    Step::Savepoint => savepoint = true,
                    _ => {}
                }
                f.run(&sql(step));
            }
            f.run("COMMIT;");
            if let Checkpoint::Pulled(commit) = f.checkpoint() {
                ingest(&mut consumer, &commit);
            }
            assert_consumer_matches(&f, &consumer);
        }
    }
}

/// A table in the schema-change property test: how it is keyed, and the
/// columns added since it was created.
#[derive(Clone, Debug)]
struct Model {
    shape: u8,
    added: Vec<u32>,
}

#[derive(Clone, Debug)]
enum DdlStep {
    Create { table: u8, shape: u8 },
    Drop { table: u8 },
    Recreate { table: u8 },
    Rename { from: u8, to: u8 },
    AddColumn { table: u8 },
    DropColumn { table: u8 },
    Upsert { table: u8, key: u8, value: u8 },
    Delete { table: u8, key: u8 },
}

fn ddl_step() -> impl Strategy<Value = DdlStep> {
    prop_oneof![
        2 => (0..3u8, 0..3u8).prop_map(|(table, shape)| DdlStep::Create { table, shape }),
        1 => (0..3u8).prop_map(|table| DdlStep::Drop { table }),
        1 => (0..3u8).prop_map(|table| DdlStep::Recreate { table }),
        1 => (0..3u8, 0..3u8).prop_map(|(from, to)| DdlStep::Rename { from, to }),
        1 => (0..3u8).prop_map(|table| DdlStep::AddColumn { table }),
        1 => (0..3u8).prop_map(|table| DdlStep::DropColumn { table }),
        6 => (0..3u8, 0..5u8, any::<u8>()).prop_map(|(table, key, value)| DdlStep::Upsert { table, key, value }),
        2 => (0..3u8, 0..5u8).prop_map(|(table, key)| DdlStep::Delete { table, key }),
    ]
}

fn create_sql(name: &str, shape: u8) -> String {
    match shape {
        0 => format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, v TEXT)"),
        1 => format!("CREATE TABLE {name} (id INTEGER, v TEXT)"),
        _ => format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID"),
    }
}

/// The SQL for `step` against the tables in `model`, applying it to the
/// model; `None` when the step does not apply.
fn ddl_sql(
    step: &DdlStep,
    model: &mut BTreeMap<String, Model>,
    next_column: &mut u32,
) -> Option<String> {
    let name = |t: &u8| format!("t{t}");
    match step {
        DdlStep::Create { table, shape } => {
            let n = name(table);
            if model.contains_key(&n) {
                return None;
            }
            model.insert(
                n.clone(),
                Model {
                    shape: *shape,
                    added: Vec::new(),
                },
            );
            Some(create_sql(&n, *shape))
        }
        DdlStep::Drop { table } => {
            let n = name(table);
            model.remove(&n)?;
            Some(format!("DROP TABLE {n}"))
        }
        DdlStep::Recreate { table } => {
            let n = name(table);
            let m = model.get_mut(&n)?;
            m.added.clear();
            Some(format!("DROP TABLE {n}; {}", create_sql(&n, m.shape)))
        }
        DdlStep::Rename { from, to } => {
            let (f, t) = (name(from), name(to));
            if model.contains_key(&t) {
                return None;
            }
            let m = model.remove(&f)?;
            model.insert(t.clone(), m);
            Some(format!("ALTER TABLE {f} RENAME TO {t}"))
        }
        DdlStep::AddColumn { table } => {
            let n = name(table);
            let m = model.get_mut(&n)?;
            *next_column += 1;
            m.added.push(*next_column);
            Some(format!(
                "ALTER TABLE {n} ADD COLUMN c{next_column} INTEGER DEFAULT {next_column}"
            ))
        }
        DdlStep::DropColumn { table } => {
            let n = name(table);
            let column = model.get_mut(&n)?.added.pop()?;
            Some(format!("ALTER TABLE {n} DROP COLUMN c{column}"))
        }
        DdlStep::Upsert { table, key, value } => {
            let n = name(table);
            let m = model.get(&n)?;
            Some(if m.shape == 1 {
                format!(
                    "INSERT INTO {n}(rowid, id, v) VALUES ({key}, {key}, 'v{value}') \
                     ON CONFLICT(rowid) DO UPDATE SET v = excluded.v"
                )
            } else {
                format!(
                    "INSERT INTO {n}(id, v) VALUES ({key}, 'v{value}') \
                     ON CONFLICT(id) DO UPDATE SET v = excluded.v"
                )
            })
        }
        DdlStep::Delete { table, key } => {
            let n = name(table);
            model.get(&n)?;
            Some(format!("DELETE FROM {n} WHERE id = {key}"))
        }
    }
}

/// Every exported table's rows as the reference consumer should hold them,
/// against what it holds.
fn assert_consumer_matches_schema(f: &Fixture, consumer: &Consumer) {
    let state = consumer.stream(&stream()).unwrap_or_default();
    assert!(state.uncertain.is_empty(), "{:?}", state.uncertain);
    let mut expected: BTreeMap<String, BTreeMap<Vec<Value>, Vec<Value>>> = BTreeMap::new();
    let names: Vec<String> = f
        .connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE 't%'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    for name in names {
        let shape = read_shape(&f.connection, &name).unwrap();
        let mut statement = f.connection.prepare(&scan_sql(&name, &shape)).unwrap();
        let rows: BTreeMap<Vec<Value>, Vec<Value>> = statement
            .query_map([], |row| {
                let width = row.as_ref().column_count();
                let values: Vec<Value> = (0..width)
                    .map(|i| from_row(row.get_ref(i).unwrap()))
                    .collect();
                // Every table here is keyed by `id` or by the rowid, which
                // leads the scan of a rowid-only table.
                Ok(if shape.rowid_only() {
                    (values[..1].to_vec(), values[1..].to_vec())
                } else {
                    (values[..1].to_vec(), values)
                })
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        if !rows.is_empty() {
            expected.insert(name, rows);
        }
    }
    let actual: BTreeMap<String, BTreeMap<Vec<Value>, Vec<Value>>> = state
        .tables
        .iter()
        .map(|(tg, t)| (tg.table.clone(), t.rows.clone()))
        .collect();
    // One open generation per name at most.
    assert_eq!(
        actual.len(),
        state.tables.len(),
        "{:?}",
        state.tables.keys()
    );
    assert_eq!(actual, expected);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// Random transactions mixing row changes with creates, drops, drop and
    /// recreate under one name, renames and column changes, some rolled
    /// back: after every commit, the reference consumer holds exactly the
    /// rows of every table, under one open generation each, and rows of a
    /// dropped or replaced generation never come back.
    #[test]
    fn the_reference_consumer_converges_across_schema_changes(
        transactions in prop::collection::vec(
            (prop::collection::vec(ddl_step(), 1..10), prop::bool::weighted(0.15)),
            1..10,
        )
    ) {
        let mut f = Fixture::new("CREATE TABLE t0 (id INTEGER PRIMARY KEY, v TEXT);");
        let mut model = BTreeMap::from([(
            "t0".to_string(),
            Model { shape: 0, added: Vec::new() },
        )]);
        let mut next_column = 0;
        let mut consumer = Consumer::new();
        for (steps, rollback) in &transactions {
            let before = model.clone();
            f.run("BEGIN;");
            for step in steps {
                if let Some(sql) = ddl_sql(step, &mut model, &mut next_column) {
                    f.run(&sql);
                }
            }
            if *rollback {
                f.run("ROLLBACK;");
                model = before;
            } else {
                f.run("COMMIT;");
            }
            if let Checkpoint::Pulled(commit) = f.checkpoint() {
                ingest(&mut consumer, &commit);
            }
            assert_consumer_matches_schema(&f, &consumer);
        }
    }
}

/// The capture as `storage` runs it: installed on open under the cell
/// connection's authorizer and limits, pulled at check points, and dropped
/// before the connection on close.
mod on_the_cell_connection {
    use super::*;
    use crate::storage;

    const CELL: &str = "cell-on-connection";

    struct Opened {
        cells: Box<storage::Cells>,
        installed: Option<storage::Installed>,
    }

    impl Drop for Opened {
        fn drop(&mut self) {
            storage::close(CELL);
            self.installed.take();
            let _ = &self.cells;
        }
    }

    fn open(settings: Option<Settings>) -> Opened {
        let cells = Box::new(storage::Cells::default());
        cells.set_export_capture(settings);
        let installed = Some(cells.install());
        storage::open(CELL, ":memory:").unwrap();
        Opened { cells, installed }
    }

    fn exec(sql: &str) {
        storage::sql_exec(CELL, sql, &[]).unwrap();
    }

    /// Run schema statements and pull the commit that creates the tables,
    /// so later commits carry rows rather than the creating snapshot.
    fn create(sql: &str) {
        exec(sql);
        storage::export_checkpoint();
        let events = events();
        assert!(
            matches!(events.as_slice(), [CaptureEvent::Commit(c), CaptureEvent::CaughtUp] if c.snapshot.is_some()),
            "{events:?}"
        );
    }

    fn events() -> Vec<CaptureEvent> {
        storage::take_capture_events(CELL)
    }

    /// The commits among the events. Every commit is followed by `CaughtUp`,
    /// since a pull only happens at a safe point.
    fn take() -> Vec<CapturedCommit> {
        let events = events();
        let mut commits = Vec::new();
        let mut iter = events.iter().peekable();
        while let Some(event) = iter.next() {
            if let CaptureEvent::Commit(commit) = event {
                assert_eq!(iter.peek(), Some(&&CaptureEvent::CaughtUp), "{events:?}");
                commits.push(commit.clone());
            }
        }
        commits
    }

    #[test]
    fn off_by_default() {
        let _cell = open(None);
        exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        exec("INSERT INTO t VALUES (1, 'a')");
        storage::export_checkpoint();
        assert!(take().is_empty());
    }

    #[test]
    fn user_sql_and_kv_writes_are_captured_at_check_points() {
        let _cell = open(Some(settings()));
        exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, g INTEGER AS (id * 2))");
        create("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
        // Nothing is pulled until a check point.
        exec("INSERT INTO u VALUES (1, 'a')");
        assert!(take().is_empty());
        storage::export_checkpoint();
        let commits = take();
        assert_eq!(commits.len(), 1);
        assert_eq!(
            table(&commits[0], "u").rows,
            [RowChange(Op::Insert, vec![int(1)], vec![int(1), text("a")])]
        );

        // Writes under the user-SQL authorizer to a generated-column table.
        exec("INSERT INTO t(id, v) VALUES (1, 'a')");
        storage::put_many_serialized(CELL, &[("k".into(), vec![1, 2])]).unwrap();
        storage::export_checkpoint();
        let commits = take();
        assert_eq!(commits.len(), 1);
        let kv = table(&commits[0], "_cf_KV");
        assert_eq!(kv.rows[0].key()[1], text("k"));
        assert_eq!(
            commits[0].bulk,
            [TableGen {
                table: "t".into(),
                generation: FIRST_GENERATION
            }]
        );
    }

    #[test]
    fn caught_up_is_reported_only_with_nothing_unpulled() {
        let _cell = open(Some(settings()));
        exec("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
        // The create is not pulled yet.
        storage::export_report_caught_up(CELL);
        assert!(events().is_empty());
        storage::export_checkpoint();
        assert_eq!(take().len(), 1);
        storage::export_report_caught_up(CELL);
        assert_eq!(events(), [CaptureEvent::CaughtUp]);
        exec("INSERT INTO u VALUES (1, 'a')");
        storage::export_report_caught_up(CELL);
        assert!(events().is_empty());
        storage::transaction_control(CELL, "start", false, "cells_tx_1").unwrap();
        exec("INSERT INTO u VALUES (2, 'b')");
        // Not at a safe point: deferred, and nothing reported.
        storage::export_checkpoint();
        storage::export_report_caught_up(CELL);
        assert!(events().is_empty());
        storage::transaction_control(CELL, "commit", false, "cells_tx_1").unwrap();
        storage::export_checkpoint();
        let events = events();
        assert!(
            matches!(events.as_slice(), [CaptureEvent::Commit(c), CaptureEvent::CaughtUp] if c.tables[0].rows.len() == 2)
        );
    }

    #[test]
    fn engine_control_tables_are_not_captured() {
        let _cell = open(Some(settings()));
        storage::set_actor_name(CELL, "room-1").unwrap();
        storage::export_checkpoint();
        assert!(take().is_empty());
    }

    #[test]
    fn an_open_transaction_is_pulled_after_it_commits() {
        let _cell = open(Some(settings()));
        create("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
        storage::transaction_control(CELL, "start", false, "cells_tx_1").unwrap();
        exec("INSERT INTO u VALUES (1, 'a')");
        storage::export_checkpoint();
        assert!(take().is_empty());
        storage::transaction_control(CELL, "commit", false, "cells_tx_1").unwrap();
        storage::export_checkpoint();
        assert_eq!(take().len(), 1);
    }

    #[test]
    fn an_open_returning_cursor_defers_the_pull() {
        let _cell = open(Some(settings()));
        create("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
        let (cursor, _, first, _, done) = storage::sql_cursor_start(
            CELL,
            "INSERT INTO u VALUES (1, 'a'), (2, 'b') RETURNING id",
            &[],
        )
        .unwrap();
        assert!(first.is_some() && !done);
        storage::export_checkpoint();
        assert!(take().is_empty());
        while storage::sql_cursor_next(cursor).unwrap().0.is_some() {}
        storage::export_checkpoint();
        let commits = take();
        assert_eq!(commits.len(), 1);
        assert_eq!(table(&commits[0], "u").rows.len(), 2);
    }

    #[test]
    fn close_pulls_the_last_writes() {
        let _cell = open(Some(settings()));
        create("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
        exec("INSERT INTO u VALUES (1, 'a')");
        storage::close(CELL);
        let commits = take();
        assert_eq!(commits.len(), 1);
        assert_eq!(table(&commits[0], "u").rows.len(), 1);
    }
}
