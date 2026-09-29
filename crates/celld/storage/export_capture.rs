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
//! A WAL hook on the same connection stamps each commit with where its last
//! frame landed ([`WalStamp`]), which release matches against the LTX files
//! the capture loop reports. Installing the hook replaces SQLite's default
//! autocheckpoint, so the hook runs the same passive checkpoint itself.
//!
//! Schema changes are found by comparing `sqlite_schema` with what the cell
//! last exported, at the same safe point, not through the authorizer (plan
//! C9): `deleteAll` drops tables with the authorizer off, and the authorizer
//! reports neither the kind of an `ALTER TABLE` nor a rename's new name. Each
//! exported table has a generation that starts at one and moves on every
//! change to its definition, and a drop and recreate under one name is a new
//! generation. Generations persist in the cell's own [`GENERATIONS_TABLE`],
//! written right after the change is seen, so they move and restore with the
//! cell (plan C10). A table whose generation opened in a commit is exported
//! as an inline snapshot rather than rows, or as `bulk` when it is larger than
//! `CELLD_EXPORT_MAX_TX_BYTES`. See "DDL and table generations" in the design.
//!
//! What this module does not do yet: the `kv` mapping of `_cf_KV` rows, which
//! are exported as the raw table here. Schema records already describe the
//! key-value tables in their exported shape ([`exported_schema`]).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;
use std::rc::Rc;

use celld_export_format::{
    ColumnDef, Op, RowChange, SchemaBody, TableGen, TableRows, Value, ROWID_KEY_COLUMN,
};
use rusqlite::{ffi, Connection};

/// The generation a table name starts at.
pub(crate) const FIRST_GENERATION: u64 = 1;

/// Where a cell keeps its table generations: one row per table name the
/// exporter has seen, with the name's newest generation, the definition and
/// root page it was exported under, or `NULL` once no table has the name.
/// The `_cf_` prefix keeps it out of application SQL and out of the export,
/// and `deleteAll` leaves it in place, so a table recreated afterwards
/// continues from the old generation instead of reusing it.
pub(crate) const GENERATIONS_TABLE: &str = "_cf_EXPORT";

/// The [`GENERATIONS_TABLE`] row that holds, as its generation, the schema
/// cookie the generations were last compared at. No table can have this
/// name, and a later residency that finds the cookie moved knows the schema
/// changed while nothing watched it.
const COOKIE_ROW: &str = "sqlite_schema";

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
    /// `schema` records at this commit, in order: generations it closed, then
    /// the ones it opened, then the definition of every other generation this
    /// capture exports for the first time.
    pub schemas: Vec<SchemaBody>,
    /// Tables exported whole at this commit instead of as rows: every table
    /// whose generation opened here, and every table when the changeset
    /// could not be pulled across a schema change.
    pub snapshot: Option<InlineSnapshot>,
}

/// A snapshot taken at the commit, of the listed table generations only.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InlineSnapshot {
    pub id: String,
    /// One entry per table with every row as an insert, including empty
    /// tables, which the snapshot also covers.
    pub tables: Vec<TableRows>,
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

/// SQLite's default `wal_autocheckpoint`. The cell connection never sets
/// one, so this is what it ran before the export hook replaced the default.
pub(crate) const AUTOCHECKPOINT_FRAMES: c_int = 1000;

/// Where a commit's last frame landed in the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WalStamp {
    /// The WAL generation named by its header salts, and the number of
    /// frames in it through the commit (the WAL hook's count).
    At { salt1: u32, salt2: u32, frames: u64 },
    /// The hook could not read the generation its commit landed in: the WAL
    /// was restarted or truncated between the commit and the read. Only the
    /// capture loop does that, and only after it captured every frame, so
    /// the file holding the commit was already reported.
    Unplaced,
}

/// What the WAL hook needs. Boxed for the same reason as [`FilterState`].
struct WalState {
    /// The newest commit's stamp since the last pull.
    last: Cell<Option<WalStamp>>,
    /// The capture's filter, to queue the cell on a commit (see
    /// [`commit_hook`]).
    filter: *const FilterState,
    /// Set while capture writes [`GENERATIONS_TABLE`] itself, which needs no
    /// visit of its own.
    quiet: Cell<bool>,
}

/// SQLite calls this after each commit on the connection, with the number
/// of frames now in the WAL.
///
/// # Safety
///
/// `context` is the `WalState` the owning `Capture` registered, which
/// outlives the hook: `Capture` removes the hook when it drops.
unsafe extern "C" fn wal_hook(
    context: *mut c_void,
    database: *mut ffi::sqlite3,
    name: *const c_char,
    frames: c_int,
) -> c_int {
    let state = unsafe { &*context.cast::<WalState>() };
    if unsafe { CStr::from_ptr(name) }.to_bytes() == b"main" {
        // Read on this thread before any later statement runs, so the header
        // is the one the commit wrote into unless the capture loop restarted
        // the WAL in between; the frame's own salts catch that.
        let stamp = unsafe { read_stamp(database, u64::try_from(frames).unwrap_or(0)) };
        state.last.set(Some(stamp.unwrap_or(WalStamp::Unplaced)));
    }
    // The default hook's passive checkpoint, which this hook replaced.
    if frames >= AUTOCHECKPOINT_FRAMES {
        unsafe { ffi::sqlite3_wal_checkpoint(database, name) };
    }
    ffi::SQLITE_OK
}

/// SQLite calls this as each transaction on the connection commits. Every
/// commit is visited, not only those the session saw: a schema change
/// reaches no session, and a cell is caught up only once its schema has been
/// compared. A commit that then fails costs one visit that finds nothing.
///
/// # Safety
///
/// `context` is the `WalState` the owning `Capture` registered, which
/// outlives the hook: `Capture` removes the hook when it drops.
unsafe extern "C" fn commit_hook(context: *mut c_void) -> c_int {
    let state = unsafe { &*context.cast::<WalState>() };
    if !state.quiet.get() {
        // SAFETY: the filter is boxed beside `wal` in the same `Capture`.
        unsafe { &*state.filter }.mark_dirty();
    }
    // Zero lets the commit proceed.
    0
}

/// Read the WAL header's salts and check that frame `frames` is a commit
/// frame of that generation. `None` when the WAL no longer holds it.
///
/// # Safety
///
/// `database` is a live connection in WAL mode.
unsafe fn read_stamp(database: *mut ffi::sqlite3, frames: u64) -> Option<WalStamp> {
    if frames == 0 {
        return None;
    }
    let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
    // The pager's own handle on the WAL, through whatever VFS opened it.
    let rc = unsafe {
        ffi::sqlite3_file_control(
            database,
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_JOURNAL_POINTER,
            (&mut file as *mut *mut ffi::sqlite3_file).cast::<c_void>(),
        )
    };
    if rc != ffi::SQLITE_OK || file.is_null() {
        return None;
    }
    let methods = unsafe { (*file).pMethods };
    if methods.is_null() {
        return None;
    }
    let read = unsafe { (*methods).xRead }?;
    let mut header = [0u8; 32];
    let rc = unsafe { read(file, header.as_mut_ptr().cast(), 32, 0) };
    if rc != ffi::SQLITE_OK {
        return None;
    }
    let word = |bytes: &[u8], at: usize| u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
    let page_size = u64::from(word(&header, 8));
    let (salt1, salt2) = (word(&header, 16), word(&header, 20));
    let offset = 32 + (frames - 1) * (24 + page_size);
    let mut frame = [0u8; 24];
    let rc = unsafe {
        read(
            file,
            frame.as_mut_ptr().cast(),
            24,
            i64::try_from(offset).ok()?,
        )
    };
    // A commit frame records the database size after the commit; a frame of
    // a later generation carries that generation's salts.
    (rc == ffi::SQLITE_OK
        && page_size > 0
        && word(&frame, 4) != 0
        && word(&frame, 8) == salt1
        && word(&frame, 12) == salt2)
        .then_some(WalStamp::At {
            salt1,
            salt2,
            frames,
        })
}

/// What the table filter needs. Boxed so its address survives moves of the
/// [`Capture`] that owns it, since the session holds a pointer to it.
struct FilterState {
    scope: String,
    /// Virtual tables and their shadow tables, as of the last refresh.
    excluded: RefCell<HashSet<String>>,
    /// Tables the operator's `CELLD_EXPORT_TABLES` denies for this class.
    denied: HashSet<String>,
    /// Per table seen since the last refresh, whether it has generated
    /// columns. The session cannot track such a table: its changeset fails
    /// as a whole with `SQLITE_SCHEMA`.
    generated: RefCell<HashMap<String, bool>>,
    /// Tables with generated columns the transaction wrote. They are
    /// exported as `bulk`.
    untracked: RefCell<Vec<String>>,
    /// Tables the session tracks changes to since it started. When the
    /// changeset fails, these are snapshotted first.
    touched: RefCell<Vec<String>>,
    dirty: Cell<bool>,
    queue: DirtyList,
    database: *mut ffi::sqlite3,
}

impl FilterState {
    fn has_generated_columns(&self, table: &str) -> bool {
        if let Some(&known) = self.generated.borrow().get(table) {
            return known;
        }
        // SAFETY: the connection is live while its session is, and SQLite
        // runs its own table-info query at this point too.
        let found = unsafe { generated_columns(self.database, table) };
        // A table that cannot be inspected is treated as untrackable.
        let found = found.unwrap_or(true);
        self.generated.borrow_mut().insert(table.to_string(), found);
        found
    }

    fn mark_dirty(&self) {
        if !self.dirty.replace(true) {
            self.queue.borrow_mut().push(self.scope.clone());
        }
    }
}

/// Whether `table` has a generated column, read with `PRAGMA table_xinfo`.
///
/// # Safety
///
/// `database` is a live connection.
unsafe fn generated_columns(database: *mut ffi::sqlite3, table: &str) -> Option<bool> {
    let sql = std::ffi::CString::new(format!("PRAGMA main.table_xinfo({})", quote(table))).ok()?;
    let mut statement = ptr::null_mut();
    let rc = unsafe {
        ffi::sqlite3_prepare_v2(database, sql.as_ptr(), -1, &mut statement, ptr::null_mut())
    };
    if rc != ffi::SQLITE_OK {
        return None;
    }
    let mut found = false;
    let result = loop {
        match unsafe { ffi::sqlite3_step(statement) } {
            // `hidden` is 2 for a virtual and 3 for a stored generated column.
            ffi::SQLITE_ROW => found |= unsafe { ffi::sqlite3_column_int(statement, 6) } >= 2,
            ffi::SQLITE_DONE => break Some(found),
            _ => break None,
        }
    };
    unsafe { ffi::sqlite3_finalize(statement) };
    result
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
    if !exported_table(&table)
        || state.denied.contains(table.as_ref())
        || state.excluded.borrow().contains(table.as_ref())
    {
        return 0;
    }
    state.mark_dirty();
    if state.has_generated_columns(&table) {
        let mut untracked = state.untracked.borrow_mut();
        if !untracked.iter().any(|t| t == table.as_ref()) {
            untracked.push(table.into_owned());
        }
        return 0;
    }
    state.touched.borrow_mut().push(table.into_owned());
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

/// One table name's generation, as the cell last exported it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Tracked {
    generation: u64,
    /// The `sqlite_schema.sql` the generation was exported under; `None` once
    /// no table has the name.
    sql: Option<String>,
    /// The table's root page, which a rename keeps and a recreate changes.
    /// Zero when not known.
    rootpage: i64,
    /// A virtual table, which the export names but does not cover.
    virtual_table: bool,
}

/// An exported table as `sqlite_schema` has it now.
struct LiveTable {
    sql: String,
    rootpage: i64,
    virtual_table: bool,
}

/// A schema record the next pulled commit carries, and whether the table it
/// opens is snapshotted with it.
#[derive(Clone, Debug)]
struct SchemaChange {
    body: SchemaBody,
    snapshot: bool,
}

/// What a schema comparison compares against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Since {
    /// Nothing: the cell has never exported. Its tables start at their first
    /// generation with no record, since there is no earlier state to
    /// supersede, and are described with their first rows.
    Never,
    /// The generations the cell stored, or the previous comparison.
    Before,
}

/// One session on one cell connection.
///
/// It must be dropped before the connection closes: the session holds the
/// connection's handle. `OpenCell` drops it first.
pub(crate) struct Capture {
    session: *mut ffi::sqlite3_session,
    database: *mut ffi::sqlite3,
    filter: Box<FilterState>,
    wal: Box<WalState>,
    settings: Settings,
    /// Tracking stopped for the current transaction because it exceeded
    /// `max_tx_bytes`.
    overflowed: bool,
    seq: u64,
    shapes: HashMap<String, Shape>,
    /// The schema cookie `shapes` and the excluded set were read at.
    schema_version: Option<i64>,
    /// Every table name this cell has exported, with its generation.
    generations: BTreeMap<String, Tracked>,
    /// The schema cookie `generations` was last compared at.
    compared_version: Option<i64>,
    /// Whether the last comparison found the schema changed, whether or not
    /// any generation moved: an added column dropped again leaves every
    /// definition as it was, but the changeset no longer matches the table.
    schema_moved: bool,
    /// Whether [`GENERATIONS_TABLE`] exists in the cell.
    stored: bool,
    /// Schema changes found outside a pull (at install), for the next commit.
    pending: Vec<SchemaChange>,
    /// Table generations whose `schema` record this capture already emitted.
    announced: HashSet<TableGen>,
    /// Tables a `DROP TABLE` or rename may have removed since the last
    /// comparison. A table replaced by one with the same definition between
    /// two safe points can leave `sqlite_schema` as it was, so only this says
    /// it is new.
    hinted: HashSet<String>,
}

impl Capture {
    /// Start capturing on `connection`. Changes made before this are not
    /// captured. Table generations are read from the cell and brought up to
    /// date with its schema; a change made while nothing captured the cell
    /// is reported by the first pull.
    pub(crate) fn install(
        connection: &Connection,
        scope: &str,
        settings: Settings,
        denied: HashSet<String>,
        queue: DirtyList,
    ) -> anyhow::Result<Self> {
        let filter = Box::new(FilterState {
            scope: scope.to_string(),
            excluded: RefCell::new(HashSet::new()),
            denied,
            generated: RefCell::new(HashMap::new()),
            untracked: RefCell::new(Vec::new()),
            touched: RefCell::new(Vec::new()),
            dirty: Cell::new(false),
            queue,
            // SAFETY: as for `database` below.
            database: unsafe { connection.handle() },
        });
        let wal = Box::new(WalState {
            last: Cell::new(None),
            filter: &*filter,
            quiet: Cell::new(false),
        });
        let mut capture = Self {
            session: ptr::null_mut(),
            // SAFETY: the handle outlives the capture; `OpenCell` drops the
            // capture before the connection.
            database: unsafe { connection.handle() },
            filter,
            wal,
            settings,
            overflowed: false,
            seq: 0,
            shapes: HashMap::new(),
            schema_version: None,
            generations: BTreeMap::new(),
            compared_version: None,
            schema_moved: false,
            stored: false,
            pending: Vec::new(),
            announced: HashSet::new(),
            hinted: HashSet::new(),
        };
        capture.refresh_schema(connection)?;
        capture.start_session()?;
        // SAFETY: the connection is live, and `wal` is boxed and outlives the
        // hook, which `Drop` removes.
        unsafe {
            let context = (&*capture.wal as *const WalState)
                .cast_mut()
                .cast::<c_void>();
            ffi::sqlite3_wal_hook(capture.database, Some(wal_hook), context);
            ffi::sqlite3_commit_hook(capture.database, Some(commit_hook), context);
        }
        // After the hook, so a write of the generations is stamped for the
        // commit that reports it.
        capture.load_generations(connection)?;
        let since = if capture.generations.is_empty() {
            Since::Never
        } else {
            Since::Before
        };
        match capture.compare_schema(connection, since) {
            Ok(pending) => capture.pending = pending,
            // Nothing is released for the changes until they are stored; the
            // first pull compares again.
            Err(error) => {
                tracing::warn!(scope, %error, "export capture: compare schema at install");
                capture.filter.mark_dirty();
            }
        }
        if !capture.pending.is_empty() {
            capture.filter.mark_dirty();
        }
        Ok(capture)
    }

    /// Where the newest commit since the last call landed, and forget it.
    /// `None` when no commit ran since.
    pub(crate) fn take_wal_stamp(&self) -> Option<WalStamp> {
        self.wal.last.take()
    }

    /// `tables` may have been dropped or renamed away on this connection since
    /// the last pull: a `DROP TABLE` or `ALTER TABLE` naming them was
    /// prepared on the cell thread. A table that exists under such a name
    /// with the same definition at the next schema change is then taken to
    /// be a new one. A table that was not replaced only costs a snapshot.
    pub(crate) fn hint_dropped<'a>(&mut self, tables: impl IntoIterator<Item = &'a String>) {
        self.hinted.extend(tables.into_iter().cloned());
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
            let changes = self
                .take_schema_changes(connection)
                .unwrap_or_else(|known| known);
            self.bulk_everything(connection, now_ms, changes)
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
    /// writes or schema changes, or it lost its session.
    pub(crate) fn needs_visit(&self) -> bool {
        self.session.is_null() || self.filter.dirty.get() || !self.pending.is_empty()
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
        self.filter.touched.borrow_mut().clear();
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

    /// The schema changes the next commit reports: those found at install
    /// and those since the last comparison. `Err` holds the ones already
    /// known when the schema cannot be read; which tables exist is then
    /// unknown, and the commit must be conservative.
    fn take_schema_changes(
        &mut self,
        connection: &Connection,
    ) -> Result<Vec<SchemaChange>, Vec<SchemaChange>> {
        let compared = self.compare_schema(connection, Since::Before);
        let mut all = std::mem::take(&mut self.pending);
        match compared {
            Ok(changes) => {
                all.extend(changes);
                Ok(all)
            }
            Err(error) => {
                tracing::error!(scope = %self.filter.scope, %error, "export capture: compare schema");
                Err(all)
            }
        }
    }

    /// The generation `table` is exported at now.
    fn generation(&self, table: &str) -> u64 {
        self.generations
            .get(table)
            .map_or(FIRST_GENERATION, |tracked| tracked.generation)
    }

    fn table_gen(&self, table: &str) -> TableGen {
        TableGen {
            table: table.to_string(),
            generation: self.generation(table),
        }
    }

    /// Whether `table` is an exported table that exists now.
    fn live(&self, table: &str) -> bool {
        self.generations
            .get(table)
            .is_some_and(|tracked| tracked.sql.is_some())
    }

    /// Whether the commit exports `table`'s changes as rows: it exists, and
    /// it is not exported whole.
    fn carries_rows(&self, table: &str, whole: &[String]) -> bool {
        self.live(table) && !whole.iter().any(|t| t == table)
    }

    /// Pull the session at a safe point. `None` when nothing exported changed.
    fn pull(&mut self, connection: &Connection, now_ms: i64) -> Option<CapturedCommit> {
        self.enforce_budget();
        let changes = match self.take_schema_changes(connection) {
            Ok(changes) if !self.overflowed => changes,
            Ok(changes) | Err(changes) => return self.bulk_everything(connection, now_ms, changes),
        };
        // A table whose generation opened here is exported whole; the rows of
        // a table that no longer exists are moot.
        let mut whole: Vec<String> = Vec::new();
        for change in &changes {
            let table = &change.body.table;
            if change.snapshot && self.live(table) && !whole.contains(table) {
                whole.push(table.clone());
            }
        }
        let untracked: Vec<TableGen> = self
            .filter
            .untracked
            .borrow()
            .iter()
            .filter(|table| self.carries_rows(table, &whole))
            .map(|table| self.table_gen(table))
            .collect();
        let mut tables = Vec::new();
        let mut bulk = untracked;
        // SAFETY: a live session.
        if unsafe { ffi::sqlite3session_isempty(self.session) } == 0 {
            if let Err(error) = self.refresh_schema(connection) {
                tracing::error!(scope = %self.filter.scope, %error, "export capture: read schema");
                return self.bulk_everything(connection, now_ms, changes);
            }
            match self.changeset() {
                Ok(rows) => {
                    let rows = rows
                        .into_iter()
                        .filter(|change| self.carries_rows(&change.table, &whole))
                        .collect();
                    let (materialized, failed) = self.materialize(connection, rows);
                    tables = materialized;
                    for table in failed {
                        // Changes recorded under an earlier shape of the
                        // table: its current rows say what they became.
                        if self.schema_moved {
                            whole.push(table);
                        } else {
                            tracing::warn!(
                                scope = %self.filter.scope, table,
                                "export capture: table exported as bulk"
                            );
                            bulk.push(self.table_gen(&table));
                        }
                    }
                }
                // A table dropped or renamed after the session recorded
                // changes to it fails the changeset as a whole, and so does
                // one created in a transaction that rolled back. A column
                // dropped from a table the session recorded stops the
                // session outright, so tables written after it were never
                // seen. Every table is snapshotted, those the session saw
                // first, within the snapshot budget.
                Err(error) => {
                    tracing::debug!(
                        scope = %self.filter.scope, %error,
                        "export capture: changeset failed; snapshotting"
                    );
                    let mut candidates = self.filter.touched.borrow().clone();
                    match self.exported_tables(connection) {
                        Ok(tables) => candidates.extend(tables),
                        Err(error) => {
                            tracing::error!(scope = %self.filter.scope, %error, "export capture: list tables");
                            return self.bulk_everything(connection, now_ms, changes);
                        }
                    }
                    for table in candidates {
                        if self.carries_rows(&table, &whole) {
                            whole.push(table);
                        }
                    }
                    bulk.retain(|tg| !whole.contains(&tg.table));
                }
            }
        }
        let snapshot = if whole.is_empty() {
            None
        } else {
            if let Err(error) = self.refresh_schema(connection) {
                tracing::error!(scope = %self.filter.scope, %error, "export capture: read schema");
                return self.bulk_everything(connection, now_ms, changes);
            }
            let mut taken = Vec::new();
            let mut budget = self.settings.max_tx_bytes;
            for table in &whole {
                match self.snapshot_table(connection, table, &mut budget) {
                    Ok(Some(rows)) => taken.push(rows),
                    Ok(None) => bulk.push(self.table_gen(table)),
                    Err(error) => {
                        tracing::warn!(
                            scope = %self.filter.scope, table, %error,
                            "export capture: snapshot exported as bulk"
                        );
                        bulk.push(self.table_gen(table));
                    }
                }
            }
            Some(InlineSnapshot {
                id: format!("ddl-{now_ms}-{}", self.seq + 1),
                tables: taken,
            })
        };
        if tables.is_empty() && bulk.is_empty() && snapshot.is_none() && changes.is_empty() {
            return None;
        }
        Some(self.commit(connection, now_ms, changes, tables, bulk, snapshot))
    }

    fn commit(
        &mut self,
        connection: &Connection,
        now_ms: i64,
        changes: Vec<SchemaChange>,
        tables: Vec<TableRows>,
        bulk: Vec<TableGen>,
        snapshot: Option<InlineSnapshot>,
    ) -> CapturedCommit {
        self.seq += 1;
        let mut schemas: Vec<SchemaBody> = changes.into_iter().map(|change| change.body).collect();
        // Every generation this commit carries has its definition in the
        // stream: announced once per capture, and again with each snapshot.
        let snapshotted = snapshot.iter().flat_map(|s| s.tables.iter());
        let carried: Vec<(TableGen, bool)> = snapshotted
            .map(|t| (t.table_gen(), true))
            .chain(tables.iter().map(|t| (t.table_gen(), false)))
            .chain(bulk.iter().map(|tg| (tg.clone(), false)))
            .collect();
        for (tg, always) in carried {
            let present = schemas
                .iter()
                .any(|s| !s.dropped && s.table == tg.table && s.generation == tg.generation);
            if present || (!always && self.announced.contains(&tg)) {
                continue;
            }
            schemas.push(self.schema_body(connection, &tg.table));
        }
        for schema in &schemas {
            self.announced.insert(TableGen {
                table: schema.table.clone(),
                generation: schema.generation,
            });
        }
        let schemas = schemas.into_iter().map(exported_schema).collect();
        CapturedCommit {
            seq: self.seq,
            committed_at: now_ms,
            tables,
            bulk,
            schemas,
            snapshot,
        }
    }

    /// A commit that names every exported table as `bulk`: the conservative
    /// answer when the tables a transaction touched are not known.
    fn bulk_everything(
        &mut self,
        connection: &Connection,
        now_ms: i64,
        changes: Vec<SchemaChange>,
    ) -> Option<CapturedCommit> {
        match self.exported_tables(connection) {
            Ok(tables) => {
                let bulk = tables.iter().map(|table| self.table_gen(table)).collect();
                Some(self.commit(connection, now_ms, changes, Vec::new(), bulk, None))
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
                && !self.filter.denied.contains(&name)
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
        let version = schema_cookie(connection)?;
        if self.schema_version == Some(version) {
            return Ok(());
        }
        let mut excluded = HashSet::new();
        let mut statement = connection.prepare("PRAGMA table_list")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let (schema, name, kind): (String, String, String) =
                (row.get(0)?, row.get(1)?, row.get(2)?);
            if schema == "main" && (kind == "virtual" || kind == "shadow") {
                excluded.insert(name);
            }
        }
        *self.filter.excluded.borrow_mut() = excluded;
        self.filter.generated.borrow_mut().clear();
        self.shapes.clear();
        self.schema_version = Some(version);
        Ok(())
    }

    /// Read the generations the cell stored.
    fn load_generations(&mut self, connection: &Connection) -> anyhow::Result<()> {
        self.stored = connection
            .query_row(
                "SELECT 1 FROM main.sqlite_schema WHERE type = 'table' AND name = ?1",
                [GENERATIONS_TABLE],
                |_| Ok(()),
            )
            .map(|()| true)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(false),
                error => Err(error),
            })?;
        if !self.stored {
            return Ok(());
        }
        let mut statement = connection.prepare(&format!(
            "SELECT name, generation, schema_sql, rootpage FROM main.{GENERATIONS_TABLE}"
        ))?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })?;
        let mut cookie = None;
        for row in rows {
            let (name, generation, sql, rootpage) = row?;
            if name == COOKIE_ROW {
                cookie = Some(generation);
                continue;
            }
            let virtual_table = sql.as_deref().is_some_and(is_virtual_sql);
            self.generations.insert(
                name,
                Tracked {
                    generation: u64::try_from(generation).unwrap_or(FIRST_GENERATION),
                    sql,
                    rootpage: rootpage.unwrap_or(0),
                    virtual_table,
                },
            );
        }
        // The schema changed since the generations were stored, while no
        // capture watched: a table may have been dropped and recreated with
        // the same definition, even on the same root page. Every table is
        // then taken to be new, which costs a snapshot each.
        if !self.generations.is_empty() && cookie != Some(schema_cookie(connection)?) {
            self.hinted.extend(self.generations.keys().cloned());
        }
        Ok(())
    }

    /// The exported tables `sqlite_schema` holds now, virtual tables included
    /// and their shadow tables not.
    fn live_tables(&self, connection: &Connection) -> anyhow::Result<BTreeMap<String, LiveTable>> {
        let mut shadow = HashSet::new();
        let mut statement = connection.prepare("PRAGMA table_list")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let (schema, name, kind): (String, String, String) =
                (row.get(0)?, row.get(1)?, row.get(2)?);
            if schema == "main" && kind == "shadow" {
                shadow.insert(name);
            }
        }
        let mut statement = connection
            .prepare("SELECT name, sql, rootpage FROM main.sqlite_schema WHERE type = 'table'")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?;
        let mut live = BTreeMap::new();
        for row in rows {
            let (name, sql, rootpage) = row?;
            if !exported_table(&name)
                || self.filter.denied.contains(&name)
                || shadow.contains(&name)
            {
                continue;
            }
            let sql = sql.unwrap_or_default();
            let virtual_table = is_virtual_sql(&sql);
            live.insert(
                name,
                LiveTable {
                    sql,
                    rootpage: rootpage.unwrap_or(0),
                    virtual_table,
                },
            );
        }
        Ok(live)
    }

    /// Compare the schema with the generations and bring them up to date,
    /// persisting what moved. Returns the `schema` records that describe the
    /// change.
    ///
    /// Every generation that opens is snapshotted. After an alteration, a
    /// rename, or a drop and recreate under one name, its rows reached the
    /// session under the old definition or not at all. Even a new name's
    /// rows may have: a table created, written, and renamed in one interval
    /// was tracked under its first name. The snapshot of a table just
    /// created holds the rows its rows records would have held.
    fn compare_schema(
        &mut self,
        connection: &Connection,
        since: Since,
    ) -> anyhow::Result<Vec<SchemaChange>> {
        let version = schema_cookie(connection)?;
        self.schema_moved = self.compared_version != Some(version);
        if !self.schema_moved {
            // Nothing was dropped if nothing changed.
            self.hinted.clear();
            return Ok(Vec::new());
        }
        let live = self.live_tables(connection)?;
        // A generation is released only once stored: a later residency must
        // never reuse one a consumer has seen. If the store fails, this
        // comparison is undone and the next pull repeats it.
        let before = (self.generations.clone(), self.hinted.clone());
        let hinted = std::mem::take(&mut self.hinted);
        let vanished: Vec<String> = self
            .generations
            .iter()
            .filter(|(name, tracked)| tracked.sql.is_some() && !live.contains_key(*name))
            .map(|(name, _)| name.clone())
            .collect();
        let opened: Vec<&String> = live
            .iter()
            .filter(|(name, table)| match self.generations.get(*name) {
                Some(tracked @ Tracked { sql: Some(sql), .. }) => {
                    // A new root page is a new b-tree: the table was dropped
                    // and recreated. Nothing else moves one, since VACUUM is
                    // denied and cells do not use auto-vacuum.
                    let moved = !tracked.virtual_table
                        && tracked.rootpage > 0
                        && tracked.rootpage != table.rootpage;
                    *sql != table.sql || hinted.contains(*name) || moved
                }
                _ => true,
            })
            .map(|(name, _)| name)
            .collect();
        // A rename keeps the root page. A drop and create in one interval can
        // reuse it too, and is then reported as a rename: the consumer's
        // result is the same, since the new generation is snapshotted.
        let mut renamed_from: HashMap<String, String> = HashMap::new();
        for name in &opened {
            let table = &live[*name];
            if table.virtual_table || table.rootpage <= 0 {
                continue;
            }
            if let Some(from) = vanished.iter().find(|from| {
                let tracked = &self.generations[*from];
                !tracked.virtual_table
                    && tracked.rootpage == table.rootpage
                    && !renamed_from.values().any(|used| used == *from)
            }) {
                renamed_from.insert((*name).clone(), from.clone());
            }
        }

        let mut changes = Vec::new();
        let mut moved: Vec<String> = Vec::new();
        for name in &vanished {
            let tracked = self.generations.get_mut(name).expect("tracked");
            let sql = tracked.sql.take().unwrap_or_default();
            moved.push(name.clone());
            if renamed_from.values().any(|from| from == name) {
                // Closed by the new name's `renamed_from`.
                continue;
            }
            changes.push(SchemaChange {
                body: SchemaBody {
                    table: name.clone(),
                    generation: tracked.generation,
                    sql,
                    columns: Vec::new(),
                    dropped: true,
                    renamed_from: None,
                    unsupported: tracked.virtual_table,
                },
                snapshot: false,
            });
        }
        let opened: Vec<String> = opened.into_iter().cloned().collect();
        for name in &opened {
            let table = &live[name];
            let first_seen = !self.generations.contains_key(name);
            let generation = self
                .generations
                .get(name)
                .map_or(FIRST_GENERATION, |tracked| tracked.generation + 1);
            self.generations.insert(
                name.clone(),
                Tracked {
                    generation,
                    sql: Some(table.sql.clone()),
                    rootpage: table.rootpage,
                    virtual_table: table.virtual_table,
                },
            );
            moved.push(name.clone());
            if since == Since::Never && first_seen {
                continue;
            }
            let mut body = self.schema_body(connection, name);
            body.renamed_from = renamed_from.get(name).cloned();
            changes.push(SchemaChange {
                body,
                snapshot: !table.virtual_table,
            });
        }
        for (name, table) in &live {
            if let Some(tracked) = self.generations.get_mut(name) {
                tracked.rootpage = table.rootpage;
            }
        }
        match self.store_generations(connection, &moved) {
            // The store read the cookie after any change of its own.
            Ok(cookie) => self.compared_version = Some(cookie),
            Err(error) => {
                (self.generations, self.hinted) = before;
                return Err(error.context("store generations"));
            }
        }
        Ok(changes)
    }

    /// Write the generations of `names` to [`GENERATIONS_TABLE`], with the
    /// schema cookie they were compared at. Returns that cookie. Nothing is
    /// written when neither the generations nor the stored cookie would
    /// change, as for a cell that has never had an exported table.
    fn store_generations(
        &mut self,
        connection: &Connection,
        names: &[String],
    ) -> anyhow::Result<i64> {
        use rusqlite::types::Value as Sql;
        if names.is_empty() && !self.stored {
            return Ok(schema_cookie(connection)?);
        }
        self.wal.quiet.set(true);
        let result = (|| -> anyhow::Result<i64> {
            if !self.stored {
                connection.execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS main.{GENERATIONS_TABLE} (
                        name TEXT PRIMARY KEY,
                        generation INTEGER NOT NULL,
                        schema_sql TEXT,
                        rootpage INTEGER
                    ) WITHOUT ROWID"
                ))?;
                self.stored = true;
            }
            // Read after the create, which moves it; the inserts do not.
            let cookie = schema_cookie(connection)?;
            let mut rows: Vec<[Sql; 4]> = names
                .iter()
                .map(|name| {
                    let tracked = &self.generations[name];
                    [
                        Sql::Text(name.clone()),
                        Sql::Integer(i64::try_from(tracked.generation).unwrap_or(i64::MAX)),
                        tracked.sql.clone().map_or(Sql::Null, Sql::Text),
                        Sql::Integer(tracked.rootpage),
                    ]
                })
                .collect();
            rows.push([
                Sql::Text(COOKIE_ROW.to_string()),
                Sql::Integer(cookie),
                Sql::Null,
                Sql::Null,
            ]);
            // SQLITE_LIMIT_VARIABLE_NUMBER on the cell connection is 100.
            // A failure part way leaves some generations stored ahead of
            // what was released, which only skips numbers.
            for chunk in rows.chunks(24) {
                let tuples: Vec<String> = (0..chunk.len())
                    .map(|i| {
                        let n = i * 4;
                        format!("(?{}, ?{}, ?{}, ?{})", n + 1, n + 2, n + 3, n + 4)
                    })
                    .collect();
                connection.execute(
                    &format!(
                        "INSERT INTO main.{GENERATIONS_TABLE} (name, generation, schema_sql, rootpage)
                         VALUES {} ON CONFLICT(name) DO UPDATE SET
                         generation = excluded.generation, schema_sql = excluded.schema_sql,
                         rootpage = excluded.rootpage",
                        tuples.join(", ")
                    ),
                    rusqlite::params_from_iter(chunk.iter().flatten()),
                )?;
            }
            Ok(cookie)
        })();
        self.wal.quiet.set(false);
        result
    }

    /// The `schema` record for `table` at its current generation. The
    /// columns are left empty when the table cannot be inspected, which a
    /// virtual table whose module is not loaded cannot; `sql` still says
    /// what the table is.
    fn schema_body(&self, connection: &Connection, table: &str) -> SchemaBody {
        let tracked = self.generations.get(table);
        let virtual_table = tracked.is_some_and(|t| t.virtual_table);
        let columns = read_columns(connection, table).unwrap_or_else(|error| {
            tracing::warn!(scope = %self.filter.scope, table, %error, "export capture: read columns");
            Vec::new()
        });
        SchemaBody {
            table: table.to_string(),
            generation: self.generation(table),
            sql: tracked.and_then(|t| t.sql.clone()).unwrap_or_default(),
            columns,
            dropped: false,
            renamed_from: None,
            unsupported: virtual_table,
        }
    }

    /// Every row of `table` as an insert, or `None` when its encoding exceeds
    /// what is left of the commit's snapshot `budget`, which it draws from.
    fn snapshot_table(
        &mut self,
        connection: &Connection,
        table: &str,
        budget: &mut u64,
    ) -> anyhow::Result<Option<TableRows>> {
        let shape = self.shape(connection, table)?.clone();
        let mut statement = connection.prepare(&scan_sql(table, &shape))?;
        let mut rows = statement.query([])?;
        // A rowid-only table's scan leads with the rowid, its key.
        let key_width = usize::from(shape.rowid_only());
        let mut changes = Vec::new();
        let mut bytes: u64 = 0;
        while let Some(row) = rows.next()? {
            let width = row.as_ref().column_count();
            let values: Vec<Value> = (0..width)
                .map(|i| Ok(from_row(row.get_ref(i)?)))
                .collect::<anyhow::Result<_>>()?;
            bytes += values.iter().map(encoded_size).sum::<u64>() + 8;
            if bytes > *budget {
                return Ok(None);
            }
            let (key, row) = values.split_at(key_width);
            let key = if shape.rowid_only() {
                key.to_vec()
            } else {
                shape.key.iter().map(|&i| row[i].clone()).collect()
            };
            changes.push(RowChange(Op::Insert, key, row.to_vec()));
        }
        *budget -= bytes;
        Ok(Some(TableRows {
            table: table.to_string(),
            generation: self.generation(table),
            columns: shape.columns.clone(),
            key_columns: shape.key_columns(),
            rows: changes,
        }))
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

    /// Turn raw changes into whole-row records. Also returns the tables
    /// whose changes could not be materialized.
    fn materialize(
        &mut self,
        connection: &Connection,
        changes: Vec<Change>,
    ) -> (Vec<TableRows>, Vec<String>) {
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
                    tracing::debug!(
                        scope = %self.filter.scope, table, %error,
                        "export capture: rows not materialized"
                    );
                    bulk.push(table);
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
            generation: self.generation(table),
            columns: shape.columns.clone(),
            key_columns: shape.key_columns(),
            rows,
        })
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.end_session();
        // SAFETY: the connection outlives the capture (`OpenCell` drops the
        // capture first). Put the default autocheckpoint back, which also
        // removes the WAL hook, and remove the commit hook; both point at
        // `wal`.
        unsafe {
            ffi::sqlite3_wal_autocheckpoint(self.database, AUTOCHECKPOINT_FRAMES);
            ffi::sqlite3_commit_hook(self.database, None, ptr::null_mut());
        }
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

/// The key-value tables are exported in another shape than they are stored
/// (plan piece 12): `_cf_KV` as `kv` with columns `key` and `value`, the key
/// being `k` alone and the value decoded JSON with no declared type, and
/// `__kv` with a `blob_key` column naming a large value's bucket object. A
/// schema record describes the exported shape.
fn exported_schema(mut schema: SchemaBody) -> SchemaBody {
    let column = |name: &str, decl_type: &str, pk: u32| ColumnDef {
        name: name.to_string(),
        decl_type: decl_type.to_string(),
        pk,
        not_null: pk > 0,
        generated: false,
    };
    match schema.table.as_str() {
        "_cf_KV" => {
            schema.table = "kv".to_string();
            if !schema.columns.is_empty() {
                schema.columns = vec![column("key", "TEXT", 1), column("value", "", 0)];
            }
        }
        "__kv" if !schema.columns.is_empty() => {
            schema.columns.push(column("blob_key", "TEXT", 0));
        }
        _ => {}
    }
    schema
}

/// Every row of a table, led by the rowid for a rowid-only table.
fn scan_sql(table: &str, shape: &Shape) -> String {
    let columns = shape
        .rowid
        .map(str::to_string)
        .into_iter()
        .chain(shape.columns.iter().map(|c| quote(c)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {columns} FROM main.{}", quote(table))
}

/// The schema cookie, which every schema change on the connection moves.
fn schema_cookie(connection: &Connection) -> rusqlite::Result<i64> {
    connection
        .prepare_cached("PRAGMA schema_version")?
        .query_row([], |row| row.get(0))
}

fn is_virtual_sql(sql: &str) -> bool {
    let mut words = sql.split_ascii_whitespace();
    words
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("create"))
        && words
            .next()
            .is_some_and(|w| w.eq_ignore_ascii_case("virtual"))
}

/// A table's columns as `PRAGMA table_xinfo` reports them, without the
/// hidden columns of a virtual table.
fn read_columns(connection: &Connection, table: &str) -> anyhow::Result<Vec<ColumnDef>> {
    let mut statement =
        connection.prepare(&format!("PRAGMA main.table_xinfo({})", quote(table)))?;
    let mut rows = statement.query([])?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next()? {
        let hidden: i64 = row.get(6)?;
        if hidden == 1 {
            continue;
        }
        columns.push(ColumnDef {
            name: row.get(1)?,
            decl_type: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            pk: u32::try_from(row.get::<_, i64>(5)?).unwrap_or(0),
            not_null: row.get::<_, i64>(3)? != 0,
            generated: hidden >= 2,
        });
    }
    anyhow::ensure!(!columns.is_empty(), "table {table} does not exist");
    Ok(columns)
}

/// Roughly what a value adds to a record's JSON, for the snapshot budget.
fn encoded_size(value: &Value) -> u64 {
    let bytes = match value {
        Value::Null => 4,
        Value::Integer(_) | Value::Real(_) => 20,
        Value::Text(t) => t.len() + 2,
        // Base64 in `{"$blob": "…"}`.
        Value::Blob(b) => b.len().div_ceil(3) * 4 + 12,
    };
    bytes as u64 + 1
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
