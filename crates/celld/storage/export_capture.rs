// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Change-export capture on a cell's connection (docs/design/change-export.md,
//! "Capture").
//!
//! One SQLite session per open cell records the net row changes of its
//! exported tables. At a safe point the changeset is pulled, the session is
//! recreated, and the changes are materialized into whole-row
//! [`TableRows`] in the export record format. A commit that is too large to
//! track, or that cannot be materialized, becomes `bulk` for the tables it may
//! have touched instead.
//!
//! rusqlite's `Session` borrows the connection and hides its handle, so it can
//! neither live beside the connection in `OpenCell` nor set
//! `SQLITE_SESSION_OBJCONFIG_ROWID`. [`Capture`] drives the C API directly.
//!
//! What this module does not do yet: positions (the WAL stamp and the LTX
//! label arrive with the live path), table generations (every table is at
//! [`FIRST_GENERATION`] until DDL tracking lands), and the `kv` mapping of
//! `_cf_KV`, which is exported as the raw table here.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;
use std::rc::Rc;

use celld_export_format::{Op, RowChange, TableGen, TableRows, Value, ROWID_KEY_COLUMN};
use rusqlite::{ffi, Connection};

/// The generation every table is exported at until DDL tracking assigns real
/// ones.
pub(crate) const FIRST_GENERATION: u64 = 1;

/// How capture behaves on every cell of one isolate.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Settings {
    /// `CELLD_EXPORT_MAX_TX_BYTES`: the session memory a transaction may use
    /// before tracking stops and the commit becomes `bulk`.
    pub max_tx_bytes: u64,
}

/// The scopes whose sessions recorded a change since their last pull. The
/// session's table filter appends a scope the first time the session sees a
/// write to an exported table, so a check point visits only cells that wrote.
pub(crate) type DirtyList = Rc<RefCell<Vec<String>>>;

/// One pulled commit, before release gives it a position.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CapturedCommit {
    /// The commit's sequence among this capture's commits, from one.
    pub seq: u64,
    /// Milliseconds since the Unix epoch at the pull.
    pub committed_at: i64,
    /// One entry per table with materialized row changes, in the order the
    /// changeset first named them.
    pub tables: Vec<TableRows>,
    /// Tables whose changes the commit does not carry. A consumer's copy of
    /// them is unknown until a snapshot covers them.
    pub bulk: Vec<TableGen>,
}

/// What capture reports for one cell, in the order it happened. The live
/// path feeds these, together with the capture observer's file reports,
/// through the one FIFO release matches on.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CaptureEvent {
    /// A pulled commit, in commit order.
    Commit(CapturedCommit),
    /// The cell is at a safe point and every commit it made has been pulled:
    /// no commit is still on its way from this cell. Release relies on this
    /// to settle captures that hold no exported commit.
    CaughtUp,
}

/// What a check point did for one cell.
#[derive(Debug, PartialEq)]
pub(crate) enum Checkpoint {
    /// Not at a safe point; the cell stays dirty and is visited again.
    Deferred,
    /// At a safe point, and the writes netted to nothing.
    Clean,
    Pulled(CapturedCommit),
}

/// Whether `table` is exported at all. Virtual tables and their shadow tables
/// are excluded separately, through `PRAGMA table_list`.
pub(crate) fn exported_table(table: &str) -> bool {
    let prefixed = |prefix: &str| {
        table
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    };
    if prefixed("sqlite_") || prefixed("_litestream_") {
        return false;
    }
    if prefixed("_cf_") {
        return table == "_cf_KV";
    }
    !(table.starts_with("__queue_") || table == "__kv_meta")
}

/// The safe point: every transaction the host call ran has committed or
/// rolled back. An `INSERT … RETURNING` cursor still open holds its implicit
/// write transaction with autocommit reported on, so the transaction state is
/// the test, not autocommit alone. An open read cursor does not block a pull:
/// it can stay open across awaits indefinitely and holds nothing to export.
pub(crate) fn safe_point(connection: &Connection) -> bool {
    // SAFETY: the handle belongs to the live connection.
    connection.is_autocommit()
        && unsafe { ffi::sqlite3_txn_state(connection.handle(), c"main".as_ptr()) }
            != ffi::SQLITE_TXN_WRITE
}

/// What the table filter needs. Boxed so its address survives moves of the
/// [`Capture`] that owns it, since the session holds a pointer to it.
struct FilterState {
    scope: String,
    /// Virtual tables and their shadow tables, as of the last refresh.
    excluded: RefCell<HashSet<String>>,
    /// `WITHOUT ROWID` tables, as of the last refresh.
    without_rowid: RefCell<HashSet<String>>,
    /// What the filter learned per table since the last refresh.
    traits: RefCell<HashMap<String, Traits>>,
    /// Tables with generated columns the session saw a write to. They are
    /// exported as `bulk`.
    untracked: RefCell<Vec<String>>,
    /// Tables whose declared key can hold `NULL` that the session saw a write
    /// to, with whether a row with a `NULL` key existed before that write.
    nullable_touched: RefCell<Vec<(String, bool)>>,
    dirty: Cell<bool>,
    queue: DirtyList,
    database: *mut ffi::sqlite3,
}

/// What the filter needs to know about a table the session sees.
#[derive(Clone, Debug, Default)]
struct Traits {
    /// The session cannot track a table with a generated column: its
    /// changeset fails as a whole with `SQLITE_SCHEMA`.
    generated: bool,
    /// The declared key columns that can hold `NULL`. A rowid table's
    /// declared key accepts `NULL` unless the column is `NOT NULL` (or is the
    /// rowid alias, whose probe is then free), and the session silently
    /// skips a row whose key holds `NULL`.
    nullable_key: Vec<String>,
    /// Their column numbers, for the pre-update values.
    nullable_key_columns: Vec<c_int>,
}

impl FilterState {
    fn traits(&self, table: &str) -> Traits {
        if let Some(known) = self.traits.borrow().get(table) {
            return known.clone();
        }
        let without_rowid = self.without_rowid.borrow().contains(table);
        // SAFETY: the connection is live while its session is, and SQLite
        // runs its own table-info query at this point too.
        let traits = unsafe { read_traits(self.database, table, without_rowid) }
            // A table that cannot be inspected is treated as untrackable.
            .unwrap_or(Traits {
                generated: true,
                ..Traits::default()
            });
        self.traits
            .borrow_mut()
            .insert(table.to_string(), traits.clone());
        traits
    }

    fn mark_dirty(&self) {
        if !self.dirty.replace(true) {
            self.queue.borrow_mut().push(self.scope.clone());
        }
    }
}

/// Run `sql` and hand each row to `row`. `None` when it fails.
///
/// # Safety
///
/// `database` is a live connection.
unsafe fn raw_query(
    database: *mut ffi::sqlite3,
    sql: &str,
    mut row: impl FnMut(*mut ffi::sqlite3_stmt),
) -> Option<()> {
    let sql = std::ffi::CString::new(sql).ok()?;
    let mut statement = ptr::null_mut();
    let rc = unsafe {
        ffi::sqlite3_prepare_v2(database, sql.as_ptr(), -1, &mut statement, ptr::null_mut())
    };
    if rc != ffi::SQLITE_OK {
        return None;
    }
    let result = loop {
        match unsafe { ffi::sqlite3_step(statement) } {
            ffi::SQLITE_ROW => row(statement),
            ffi::SQLITE_DONE => break Some(()),
            _ => break None,
        }
    };
    unsafe { ffi::sqlite3_finalize(statement) };
    result
}

/// Read `table`'s [`Traits`] with `PRAGMA table_xinfo`.
///
/// # Safety
///
/// `database` is a live connection.
unsafe fn read_traits(
    database: *mut ffi::sqlite3,
    table: &str,
    without_rowid: bool,
) -> Option<Traits> {
    let mut traits = Traits::default();
    let sql = format!("PRAGMA main.table_xinfo({})", quote(table));
    unsafe {
        raw_query(database, &sql, |statement| {
            // `hidden` is 2 for a virtual and 3 for a stored generated column.
            traits.generated |= ffi::sqlite3_column_int(statement, 6) >= 2;
            let not_null = ffi::sqlite3_column_int(statement, 3) != 0;
            let pk = ffi::sqlite3_column_int(statement, 5) > 0;
            // `WITHOUT ROWID` enforces `NOT NULL` on its key.
            if pk && !not_null && !without_rowid {
                let name = ffi::sqlite3_column_text(statement, 1);
                if !name.is_null() {
                    let name = CStr::from_ptr(name.cast()).to_string_lossy().into_owned();
                    traits.nullable_key.push(name);
                    traits
                        .nullable_key_columns
                        .push(ffi::sqlite3_column_int(statement, 0));
                }
            }
        })?;
    }
    Some(traits)
}

/// The query that says whether `table` holds a row whose key has a `NULL`.
fn null_key_sql(table: &str, columns: &[String]) -> String {
    let predicate = columns
        .iter()
        .map(|c| format!("{} IS NULL", quote(c)))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "SELECT EXISTS (SELECT 1 FROM main.{} WHERE {predicate})",
        quote(table)
    )
}

/// Whether the row the current pre-update callback is about to update or
/// delete has a `NULL` in `columns`. False for an insert, which has no old row.
///
/// # Safety
///
/// Called from inside a pre-update callback on `database`, which the session
/// filter is.
unsafe fn old_row_has_null_key(database: *mut ffi::sqlite3, columns: &[c_int]) -> bool {
    columns.iter().any(|&column| {
        let mut value = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3_preupdate_old(database, column, &mut value) };
        rc == ffi::SQLITE_OK
            && !value.is_null()
            && unsafe { ffi::sqlite3_value_type(value) } == ffi::SQLITE_NULL
    })
}

/// # Safety
///
/// `database` is a live connection.
unsafe fn has_null_key(
    database: *mut ffi::sqlite3,
    table: &str,
    columns: &[String],
) -> Option<bool> {
    let mut found = false;
    unsafe {
        raw_query(database, &null_key_sql(table, columns), |statement| {
            found = ffi::sqlite3_column_int(statement, 0) != 0;
        })?;
    }
    Some(found)
}

/// SQLite calls this the first time a session sees a change to `table`.
///
/// # Safety
///
/// `context` is the `FilterState` the owning `Capture` registered, which
/// outlives the session.
unsafe extern "C" fn table_filter(context: *mut c_void, table: *const c_char) -> c_int {
    let state = unsafe { &*context.cast::<FilterState>() };
    let table = unsafe { CStr::from_ptr(table) }.to_string_lossy();
    if !exported_table(&table) || state.excluded.borrow().contains(table.as_ref()) {
        return 0;
    }
    state.mark_dirty();
    let traits = state.traits(&table);
    if traits.generated {
        let mut untracked = state.untracked.borrow_mut();
        if !untracked.iter().any(|t| t == table.as_ref()) {
            untracked.push(table.into_owned());
        }
        return 0;
    }
    if !traits.nullable_key.is_empty() {
        // The filter runs inside the pre-update callback of the first change
        // the session sees to the table. SQLite may already have changed the
        // key index for that row, so the probe covers every other row and
        // the row itself is read from its pre-update values.
        // SAFETY: as for `traits`, and this is a pre-update callback.
        let had_null = unsafe {
            has_null_key(state.database, &table, &traits.nullable_key).unwrap_or(true)
                || old_row_has_null_key(state.database, &traits.nullable_key_columns)
        };
        state
            .nullable_touched
            .borrow_mut()
            .push((table.into_owned(), had_null));
    }
    1
}

/// A table's columns as the session records them, and how to key it.
#[derive(Clone, Debug)]
struct Shape {
    /// Ordinary columns in declared order: the columns the changeset carries
    /// after any leading rowid. Generated and hidden columns are not in a
    /// changeset and are not exported.
    columns: Vec<String>,
    /// Indices into `columns` of the declared primary key, in key order.
    /// Empty for a rowid-only table.
    key: Vec<usize>,
    /// The name that addresses the rowid of a rowid-only table.
    rowid: Option<&'static str>,
}

impl Shape {
    fn rowid_only(&self) -> bool {
        self.key.is_empty()
    }

    /// The column count a changeset reports for the table.
    fn changeset_columns(&self) -> usize {
        self.columns.len() + usize::from(self.rowid_only())
    }

    fn key_columns(&self) -> Vec<String> {
        if self.rowid_only() {
            vec![ROWID_KEY_COLUMN.to_string()]
        } else {
            self.key.iter().map(|&i| self.columns[i].clone()).collect()
        }
    }
}

/// One session on one cell connection.
///
/// It must be dropped before the connection closes: the session holds the
/// connection's handle. `OpenCell` drops it first.
pub(crate) struct Capture {
    session: *mut ffi::sqlite3_session,
    database: *mut ffi::sqlite3,
    filter: Box<FilterState>,
    settings: Settings,
    /// Tracking stopped for the current transaction because it exceeded
    /// `max_tx_bytes`.
    overflowed: bool,
    seq: u64,
    shapes: HashMap<String, Shape>,
    /// The schema cookie `shapes` and the excluded set were read at.
    schema_version: Option<i64>,
}

impl Capture {
    /// Start capturing on `connection`. Changes made before this are not
    /// captured.
    pub(crate) fn install(
        connection: &Connection,
        scope: &str,
        settings: Settings,
        queue: DirtyList,
    ) -> anyhow::Result<Self> {
        let filter = Box::new(FilterState {
            scope: scope.to_string(),
            excluded: RefCell::new(HashSet::new()),
            without_rowid: RefCell::new(HashSet::new()),
            traits: RefCell::new(HashMap::new()),
            untracked: RefCell::new(Vec::new()),
            nullable_touched: RefCell::new(Vec::new()),
            dirty: Cell::new(false),
            queue,
            // SAFETY: as for `database` below.
            database: unsafe { connection.handle() },
        });
        let mut capture = Self {
            session: ptr::null_mut(),
            // SAFETY: the handle outlives the capture; `OpenCell` drops the
            // capture before the connection.
            database: unsafe { connection.handle() },
            filter,
            settings,
            overflowed: false,
            seq: 0,
            shapes: HashMap::new(),
            schema_version: None,
        };
        capture.refresh_schema(connection)?;
        capture.start_session()?;
        Ok(capture)
    }

    fn start_session(&mut self) -> anyhow::Result<()> {
        debug_assert!(self.session.is_null());
        let mut session = ptr::null_mut();
        // SAFETY: `database` is the live connection's handle, and `filter` is
        // boxed and outlives the session, which `end_session` deletes.
        unsafe {
            let rc = ffi::sqlite3session_create(self.database, c"main".as_ptr(), &mut session);
            anyhow::ensure!(rc == ffi::SQLITE_OK, "sqlite3session_create failed: {rc}");
            self.session = session;
            // Must be set before the first attach, so tables without a
            // declared key are tracked by rowid.
            let mut on: c_int = 1;
            let rc = ffi::sqlite3session_object_config(
                session,
                ffi::SQLITE_SESSION_OBJCONFIG_ROWID,
                (&mut on as *mut c_int).cast::<c_void>(),
            );
            anyhow::ensure!(
                rc == ffi::SQLITE_OK && on == 1,
                "SQLITE_SESSION_OBJCONFIG_ROWID failed: {rc}"
            );
            ffi::sqlite3session_table_filter(
                session,
                Some(table_filter),
                (&*self.filter as *const FilterState)
                    .cast_mut()
                    .cast::<c_void>(),
            );
            let rc = ffi::sqlite3session_attach(session, ptr::null());
            anyhow::ensure!(rc == ffi::SQLITE_OK, "sqlite3session_attach failed: {rc}");
        }
        self.filter.dirty.set(false);
        self.overflowed = false;
        Ok(())
    }

    fn end_session(&mut self) {
        if !self.session.is_null() {
            // SAFETY: created by `start_session` and not yet deleted.
            unsafe { ffi::sqlite3session_delete(self.session) };
            self.session = ptr::null_mut();
        }
    }

    /// Visit the cell at a check point. At a safe point the session is pulled
    /// and recreated; otherwise only the tracking budget is enforced.
    pub(crate) fn checkpoint(&mut self, connection: &Connection, now_ms: i64) -> Checkpoint {
        // SAFETY: only compared.
        debug_assert_eq!(unsafe { connection.handle() }, self.database);
        if !safe_point(connection) {
            self.enforce_budget();
            return Checkpoint::Deferred;
        }
        let commit = if self.session.is_null() {
            // The last restart failed, so writes since then went untracked.
            if let Err(error) = self.restart(connection) {
                tracing::error!(scope = %self.filter.scope, %error, "export capture: restart session");
                return Checkpoint::Deferred;
            }
            self.bulk_everything(connection, now_ms)
        } else {
            let commit = self.pull(connection, now_ms);
            if let Err(error) = self.restart(connection) {
                tracing::error!(scope = %self.filter.scope, %error, "export capture: restart session");
            }
            commit
        };
        match commit {
            Some(commit) => Checkpoint::Pulled(commit),
            None => Checkpoint::Clean,
        }
    }

    /// Whether a check point must visit the cell again: it has unpulled
    /// writes, or it lost its session.
    pub(crate) fn needs_visit(&self) -> bool {
        self.session.is_null() || self.filter.dirty.get()
    }

    /// Whether every commit the connection made has been pulled.
    pub(crate) fn caught_up(&self, connection: &Connection) -> bool {
        !self.needs_visit() && safe_point(connection)
    }

    fn restart(&mut self, connection: &Connection) -> anyhow::Result<()> {
        self.end_session();
        // Every write so far is pulled or accounted for as bulk.
        self.filter.dirty.set(false);
        self.filter.untracked.borrow_mut().clear();
        self.filter.nullable_touched.borrow_mut().clear();
        self.refresh_schema(connection)?;
        self.start_session()
    }

    fn memory_used(&self) -> u64 {
        if self.session.is_null() {
            return 0;
        }
        // SAFETY: a live session.
        u64::try_from(unsafe { ffi::sqlite3session_memory_used(self.session) }).unwrap_or(0)
    }

    /// Stop tracking for the rest of the transaction once it exceeds the
    /// budget. One statement can overshoot by the rows it touches.
    fn enforce_budget(&mut self) {
        if !self.overflowed && self.memory_used() > self.settings.max_tx_bytes {
            // SAFETY: a live session (a non-zero memory use implies one).
            unsafe { ffi::sqlite3session_enable(self.session, 0) };
            self.overflowed = true;
        }
    }

    /// Pull the session at a safe point. `None` when nothing exported changed.
    fn pull(&mut self, connection: &Connection, now_ms: i64) -> Option<CapturedCommit> {
        self.enforce_budget();
        if self.overflowed {
            return self.bulk_everything(connection, now_ms);
        }
        let untracked = self.untracked_tables(connection);
        // SAFETY: a live session.
        if unsafe { ffi::sqlite3session_isempty(self.session) } != 0 {
            return (!untracked.is_empty()).then(|| self.commit(now_ms, Vec::new(), untracked));
        }
        if let Err(error) = self.refresh_schema(connection) {
            tracing::error!(scope = %self.filter.scope, %error, "export capture: read schema");
            return self.bulk_everything(connection, now_ms);
        }
        let changes = match self.changeset() {
            Ok(changes) => changes,
            Err(error) => {
                tracing::error!(scope = %self.filter.scope, %error, "export capture: changeset");
                return self.bulk_everything(connection, now_ms);
            }
        };
        let (mut tables, mut bulk) = self.materialize(connection, changes);
        // A table that is bulk carries no rows: they would be partial.
        tables.retain(|rows| !untracked.iter().any(|t| t.table == rows.table));
        bulk.retain(|table| !untracked.contains(table));
        bulk.extend(untracked);
        if tables.is_empty() && bulk.is_empty() {
            return None;
        }
        Some(self.commit(now_ms, tables, bulk))
    }

    /// The tables the session saw writes to but could not track: tables with
    /// generated columns, and tables that held a row with a `NULL` key before
    /// or after the session's writes. A change to such a row is invisible to
    /// the session, so the table is exported as `bulk`.
    fn untracked_tables(&self, connection: &Connection) -> Vec<TableGen> {
        let mut tables: Vec<String> = self.filter.untracked.borrow().clone();
        let traits = self.filter.traits.borrow();
        for (table, had_null) in self.filter.nullable_touched.borrow().iter() {
            let columns = traits
                .get(table)
                .map(|t| t.nullable_key.clone())
                .unwrap_or_default();
            let has_null = *had_null
                || columns.is_empty()
                || connection
                    .query_row(&null_key_sql(table, &columns), [], |row| {
                        row.get::<_, bool>(0)
                    })
                    .unwrap_or(true);
            if has_null && !tables.contains(table) {
                tables.push(table.clone());
            }
        }
        tables
            .into_iter()
            .map(|table| TableGen {
                table,
                generation: FIRST_GENERATION,
            })
            .collect()
    }

    fn commit(
        &mut self,
        now_ms: i64,
        tables: Vec<TableRows>,
        bulk: Vec<TableGen>,
    ) -> CapturedCommit {
        self.seq += 1;
        CapturedCommit {
            seq: self.seq,
            committed_at: now_ms,
            tables,
            bulk,
        }
    }

    /// A commit that names every exported table as `bulk`: the conservative
    /// answer when the tables a transaction touched are not known.
    fn bulk_everything(&mut self, connection: &Connection, now_ms: i64) -> Option<CapturedCommit> {
        match self.exported_tables(connection) {
            Ok(tables) => {
                let bulk = tables
                    .into_iter()
                    .map(|table| TableGen {
                        table,
                        generation: FIRST_GENERATION,
                    })
                    .collect();
                Some(self.commit(now_ms, Vec::new(), bulk))
            }
            Err(error) => {
                // Nothing to name. Release reports the stream's gap.
                tracing::error!(scope = %self.filter.scope, %error, "export capture: list tables");
                None
            }
        }
    }

    fn exported_tables(&self, connection: &Connection) -> anyhow::Result<Vec<String>> {
        let excluded = self.filter.excluded.borrow();
        let mut statement = connection.prepare("PRAGMA table_list")?;
        let mut rows = statement.query([])?;
        let mut tables = Vec::new();
        while let Some(row) = rows.next()? {
            let (schema, name, kind): (String, String, String) =
                (row.get(0)?, row.get(1)?, row.get(2)?);
            if schema == "main"
                && kind == "table"
                && exported_table(&name)
                && !excluded.contains(&name)
            {
                tables.push(name);
            }
        }
        tables.sort();
        Ok(tables)
    }

    /// Re-read the excluded set and drop cached shapes when the schema cookie
    /// moved.
    fn refresh_schema(&mut self, connection: &Connection) -> anyhow::Result<()> {
        let version: i64 = connection.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        if self.schema_version == Some(version) {
            return Ok(());
        }
        let mut excluded = HashSet::new();
        let mut without_rowid = HashSet::new();
        let mut statement = connection.prepare("PRAGMA table_list")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let (schema, name, kind, wr): (String, String, String, i64) =
                (row.get(0)?, row.get(1)?, row.get(2)?, row.get(4)?);
            if schema != "main" {
                continue;
            }
            if kind == "virtual" || kind == "shadow" {
                excluded.insert(name);
            } else if wr != 0 {
                without_rowid.insert(name);
            }
        }
        *self.filter.excluded.borrow_mut() = excluded;
        *self.filter.without_rowid.borrow_mut() = without_rowid;
        self.filter.traits.borrow_mut().clear();
        self.shapes.clear();
        self.schema_version = Some(version);
        Ok(())
    }

    fn shape(&mut self, connection: &Connection, table: &str) -> anyhow::Result<&Shape> {
        if !self.shapes.contains_key(table) {
            let shape = read_shape(connection, table)?;
            self.shapes.insert(table.to_string(), shape);
        }
        Ok(&self.shapes[table])
    }

    /// Take the session's changeset. The session is left as it was; the
    /// caller recreates it.
    fn changeset(&self) -> anyhow::Result<Vec<Change>> {
        let mut size: c_int = 0;
        let mut buffer: *mut c_void = ptr::null_mut();
        // SAFETY: a live session; the buffer is freed below.
        let rc = unsafe { ffi::sqlite3session_changeset(self.session, &mut size, &mut buffer) };
        anyhow::ensure!(
            rc == ffi::SQLITE_OK,
            "sqlite3session_changeset failed: {rc}"
        );
        // SAFETY: `buffer` holds `size` bytes of changeset from SQLite.
        let changes = unsafe { decode_changeset(size, buffer) };
        // SAFETY: allocated by SQLite, or null when the changeset is empty.
        unsafe { ffi::sqlite3_free(buffer) };
        changes
    }

    /// Turn raw changes into whole-row records. A table whose changes cannot
    /// be materialized becomes `bulk`.
    fn materialize(
        &mut self,
        connection: &Connection,
        changes: Vec<Change>,
    ) -> (Vec<TableRows>, Vec<TableGen>) {
        let mut order: Vec<String> = Vec::new();
        let mut by_table: HashMap<String, Vec<Change>> = HashMap::new();
        {
            let excluded = self.filter.excluded.borrow();
            for change in changes {
                // The excluded set may have grown since the filter saw the
                // table, when a virtual table was created mid-session.
                if excluded.contains(&change.table) {
                    continue;
                }
                if !by_table.contains_key(&change.table) {
                    order.push(change.table.clone());
                }
                by_table
                    .entry(change.table.clone())
                    .or_default()
                    .push(change);
            }
        }
        let mut tables = Vec::new();
        let mut bulk = Vec::new();
        for table in order {
            let changes = by_table.remove(&table).unwrap_or_default();
            match self.materialize_table(connection, &table, changes) {
                Ok(rows) => tables.push(rows),
                Err(error) => {
                    tracing::warn!(
                        scope = %self.filter.scope, table, %error,
                        "export capture: table exported as bulk"
                    );
                    bulk.push(TableGen {
                        table,
                        generation: FIRST_GENERATION,
                    });
                }
            }
        }
        (tables, bulk)
    }

    fn materialize_table(
        &mut self,
        connection: &Connection,
        table: &str,
        changes: Vec<Change>,
    ) -> anyhow::Result<TableRows> {
        let shape = self.shape(connection, table)?.clone();
        let width = shape.changeset_columns();
        let offset = usize::from(shape.rowid_only());
        let mut lookup = connection.prepare(&lookup_sql(table, &shape)?)?;
        let mut rows = Vec::with_capacity(changes.len());
        for change in changes {
            // A mismatch means the table's schema changed under the session.
            anyhow::ensure!(
                change.old.len() == width && change.new.len() == width,
                "changeset has {} columns, the table has {width}",
                change.old.len()
            );
            // An insert's key is in its new values; an update's and a
            // delete's are in the old ones.
            let keyed = if change.op == Op::Insert {
                &change.new
            } else {
                &change.old
            };
            let key: Vec<Value> = if shape.rowid_only() {
                vec![present(&keyed[0])?]
            } else {
                shape
                    .key
                    .iter()
                    .map(|&i| present(&keyed[i + offset]))
                    .collect::<anyhow::Result<_>>()?
            };
            let row = if change.op == Op::Delete {
                // The session stores a deleted row's full pre-image.
                change.old[offset..]
                    .iter()
                    .map(present)
                    .collect::<anyhow::Result<_>>()?
            } else {
                // A key change reaches the changeset as a delete and an
                // insert, so an update's row is still found by its old key.
                read_row(&mut lookup, &key)?
            };
            rows.push(RowChange(change.op, key, row));
        }
        Ok(TableRows {
            table: table.to_string(),
            generation: FIRST_GENERATION,
            columns: shape.columns.clone(),
            key_columns: shape.key_columns(),
            rows,
        })
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.end_session();
    }
}

/// One changeset entry. `old` and `new` hold one slot per changeset column;
/// a slot is `None` where the changeset carries no value (an insert's old
/// values, a delete's new values, an update's unchanged columns).
#[derive(Debug)]
struct Change {
    table: String,
    op: Op,
    old: Vec<Option<Value>>,
    new: Vec<Option<Value>>,
}

fn present(value: &Option<Value>) -> anyhow::Result<Value> {
    value
        .clone()
        .ok_or_else(|| anyhow::anyhow!("changeset lacks a key or pre-image value"))
}

/// # Safety
///
/// `buffer` must hold `size` bytes of changeset.
unsafe fn decode_changeset(size: c_int, buffer: *mut c_void) -> anyhow::Result<Vec<Change>> {
    let mut changes = Vec::new();
    if size == 0 {
        return Ok(changes);
    }
    let mut iter = ptr::null_mut();
    let rc = unsafe { ffi::sqlite3changeset_start(&mut iter, size, buffer) };
    anyhow::ensure!(rc == ffi::SQLITE_OK, "sqlite3changeset_start failed: {rc}");
    let result = (|| loop {
        let rc = unsafe { ffi::sqlite3changeset_next(iter) };
        if rc == ffi::SQLITE_DONE {
            return Ok(());
        }
        anyhow::ensure!(rc == ffi::SQLITE_ROW, "sqlite3changeset_next failed: {rc}");
        let mut table = ptr::null();
        let mut columns: c_int = 0;
        let mut code: c_int = 0;
        let mut indirect: c_int = 0;
        let rc = unsafe {
            ffi::sqlite3changeset_op(iter, &mut table, &mut columns, &mut code, &mut indirect)
        };
        anyhow::ensure!(rc == ffi::SQLITE_OK, "sqlite3changeset_op failed: {rc}");
        let op = match code {
            ffi::SQLITE_INSERT => Op::Insert,
            ffi::SQLITE_UPDATE => Op::Update,
            ffi::SQLITE_DELETE => Op::Delete,
            other => anyhow::bail!("unexpected changeset op {other}"),
        };
        let columns = usize::try_from(columns)?;
        let mut old = vec![None; columns];
        let mut new = vec![None; columns];
        for i in 0..columns {
            let column = c_int::try_from(i)?;
            if op != Op::Insert {
                let mut value = ptr::null_mut();
                let rc = unsafe { ffi::sqlite3changeset_old(iter, column, &mut value) };
                anyhow::ensure!(rc == ffi::SQLITE_OK, "sqlite3changeset_old failed: {rc}");
                old[i] = unsafe { from_sqlite(value) };
            }
            if op != Op::Delete {
                let mut value = ptr::null_mut();
                let rc = unsafe { ffi::sqlite3changeset_new(iter, column, &mut value) };
                anyhow::ensure!(rc == ffi::SQLITE_OK, "sqlite3changeset_new failed: {rc}");
                new[i] = unsafe { from_sqlite(value) };
            }
        }
        changes.push(Change {
            table: unsafe { CStr::from_ptr(table) }
                .to_string_lossy()
                .into_owned(),
            op,
            old,
            new,
        });
    })();
    let rc = unsafe { ffi::sqlite3changeset_finalize(iter) };
    result?;
    anyhow::ensure!(
        rc == ffi::SQLITE_OK,
        "sqlite3changeset_finalize failed: {rc}"
    );
    Ok(changes)
}

/// # Safety
///
/// `value` is null or a value the changeset iterator currently owns.
unsafe fn from_sqlite(value: *mut ffi::sqlite3_value) -> Option<Value> {
    if value.is_null() {
        return None;
    }
    Some(unsafe {
        match ffi::sqlite3_value_type(value) {
            ffi::SQLITE_INTEGER => Value::Integer(ffi::sqlite3_value_int64(value)),
            ffi::SQLITE_FLOAT => Value::Real(ffi::sqlite3_value_double(value)),
            ffi::SQLITE_TEXT => {
                let text = ffi::sqlite3_value_text(value);
                let len = usize::try_from(ffi::sqlite3_value_bytes(value)).unwrap_or(0);
                let bytes = if text.is_null() {
                    &[][..]
                } else {
                    std::slice::from_raw_parts(text, len)
                };
                Value::Text(String::from_utf8_lossy(bytes).into_owned())
            }
            ffi::SQLITE_BLOB => {
                let blob = ffi::sqlite3_value_blob(value);
                let len = usize::try_from(ffi::sqlite3_value_bytes(value)).unwrap_or(0);
                if blob.is_null() {
                    Value::Blob(Vec::new())
                } else {
                    Value::Blob(std::slice::from_raw_parts(blob.cast::<u8>(), len).to_vec())
                }
            }
            _ => Value::Null,
        }
    })
}

fn from_row(value: rusqlite::types::ValueRef<'_>) -> Value {
    use rusqlite::types::ValueRef;
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(r) => Value::Real(r),
        ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    }
}

fn to_sql(value: &Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as Sql;
    match value {
        Value::Null => Sql::Null,
        Value::Integer(i) => Sql::Integer(*i),
        Value::Real(r) => Sql::Real(*r),
        Value::Text(t) => Sql::Text(t.clone()),
        Value::Blob(b) => Sql::Blob(b.clone()),
    }
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn read_shape(connection: &Connection, table: &str) -> anyhow::Result<Shape> {
    let mut statement =
        connection.prepare(&format!("PRAGMA main.table_xinfo({})", quote(table)))?;
    let mut rows = statement.query([])?;
    let mut columns = Vec::new();
    let mut all = HashSet::new();
    let mut key: Vec<(i64, usize)> = Vec::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let pk: i64 = row.get(5)?;
        let hidden: i64 = row.get(6)?;
        all.insert(name.to_ascii_lowercase());
        if hidden != 0 {
            continue;
        }
        if pk > 0 {
            key.push((pk, columns.len()));
        }
        columns.push(name);
    }
    anyhow::ensure!(!columns.is_empty(), "table {table} does not exist");
    key.sort();
    let key: Vec<usize> = key.into_iter().map(|(_, i)| i).collect();
    let rowid = if key.is_empty() {
        let name = ["rowid", "_rowid_", "oid"]
            .into_iter()
            .find(|name| !all.contains(*name))
            .ok_or_else(|| anyhow::anyhow!("table {table} shadows every rowid alias"))?;
        Some(name)
    } else {
        None
    };
    Ok(Shape {
        columns,
        key,
        rowid,
    })
}

fn lookup_sql(table: &str, shape: &Shape) -> anyhow::Result<String> {
    let columns: Vec<String> = shape.columns.iter().map(|c| quote(c)).collect();
    let predicate = match shape.rowid {
        Some(rowid) => format!("{rowid} = ?1"),
        None => shape
            .key
            .iter()
            .enumerate()
            .map(|(n, &i)| format!("{} = ?{}", quote(&shape.columns[i]), n + 1))
            .collect::<Vec<_>>()
            .join(" AND "),
    };
    Ok(format!(
        "SELECT {} FROM main.{} WHERE {predicate}",
        columns.join(", "),
        quote(table)
    ))
}

fn read_row(lookup: &mut rusqlite::Statement<'_>, key: &[Value]) -> anyhow::Result<Vec<Value>> {
    let params: Vec<rusqlite::types::Value> = key.iter().map(to_sql).collect();
    let mut rows = lookup.query(rusqlite::params_from_iter(params))?;
    let row = rows
        .next()?
        .ok_or_else(|| anyhow::anyhow!("changed row is missing after its commit"))?;
    let width = row.as_ref().column_count();
    (0..width).map(|i| Ok(from_row(row.get_ref(i)?))).collect()
}

#[cfg(test)]
mod tests;
