// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;

use celld_export_format::{Body, Consumer, Envelope, Origin, Position, Record, RowsBody, StreamId};
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
}

impl Fixture {
    fn new(schema: &str) -> Self {
        Self::with_settings(schema, settings())
    }

    fn with_settings(schema: &str, settings: Settings) -> Self {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(schema).unwrap();
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
        }
    }

    fn run(&self, sql: &str) {
        self.connection.execute_batch(sql).unwrap();
    }

    fn checkpoint(&mut self) -> Checkpoint {
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
fn excluded_tables_are_neither_captured_nor_marked_dirty() {
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
    assert!(f.queued().is_empty());
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
    // Created mid-session: its shadow writes are filtered at the pull.
    f.run(
        "CREATE VIRTUAL TABLE later USING fts5(body);
         INSERT INTO later VALUES ('x');",
    );
    assert_eq!(f.checkpoint(), Checkpoint::Clean);
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

#[test]
fn an_added_column_is_carried() {
    let mut f = Fixture::new(SHAPES);
    f.run(
        "INSERT INTO plain VALUES (1, 'x');
         ALTER TABLE plain ADD COLUMN extra INTEGER DEFAULT 9;
         INSERT INTO plain VALUES (2, 'y', 3);",
    );
    let commit = f.pull();
    let plain = table(&commit, "plain");
    assert_eq!(plain.columns, ["v", "w", "extra"]);
    let rows: Vec<_> = plain.rows.iter().map(|r| r.row().to_vec()).collect();
    assert_eq!(
        rows,
        [
            vec![int(1), text("x"), int(9)],
            vec![int(2), text("y"), int(3)]
        ]
    );
}

/// DDL is tracked by table generations, which are not here yet. Until then a
/// table dropped under the session fails the changeset, and the commit
/// conservatively names every remaining exported table as `bulk`.
#[test]
fn a_dropped_table_falls_back_to_bulk() {
    let mut f = Fixture::new(SHAPES);
    f.run(
        "INSERT INTO keyed VALUES ('a', 1);
         INSERT INTO plain VALUES (1, 'x');
         DROP TABLE plain;",
    );
    let commit = f.pull();
    let covered: Vec<_> = commit
        .tables
        .iter()
        .map(|t| t.table.as_str())
        .chain(commit.bulk.iter().map(|t| t.table.as_str()))
        .collect();
    assert!(covered.contains(&"keyed"), "{commit:?}");
    // The next commit is captured normally.
    f.run("INSERT INTO keyed VALUES ('b', 2);");
    assert_eq!(table(&f.pull(), "keyed").rows.len(), 1);
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

fn ingest(consumer: &mut Consumer, commit: &CapturedCommit) {
    assert!(commit.bulk.is_empty(), "{commit:?}");
    for rows in &commit.tables {
        let record = Record {
            envelope: Envelope {
                stream: stream(),
                cell_name: None,
                position: Position::new(1, commit.seq, commit.seq),
                committed_at: commit.committed_at,
                node: "node-a".into(),
                origin: Origin::Live,
                fragment: 1,
                fragments: 1,
            },
            body: Body::Rows(RowsBody { data: rows.clone() }),
        };
        consumer.ingest(record).unwrap();
    }
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
        exec("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
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
        exec("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
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
        exec("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
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
        exec("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)");
        exec("INSERT INTO u VALUES (1, 'a')");
        storage::close(CELL);
        let commits = take();
        assert_eq!(commits.len(), 1);
        assert_eq!(table(&commits[0], "u").rows.len(), 1);
    }
}
