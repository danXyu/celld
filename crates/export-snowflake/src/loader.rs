//! The loader's Snowflake side: deploy the export's objects, keep one Dynamic
//! Table per `(script, class, table)` in step with the schema union, load
//! repair and backfill files, erase streams, and read what verify and the
//! reconciler need.
//!
//! Everything here goes through [`Warehouse`], one statement at a time, so
//! the same code runs against Snowflake (the `sql-api` feature's client) and
//! against the emulator in `sqltest/`.
//!
//! Records reach `EXPORT_LANDING` by the pipe, from the files the bucket sink
//! writes. Consuming blob-stream (group `snowflake`) and writing batches to
//! the stage waits on the blob-stream sink; when it lands, it hands the
//! batch's files to [`Loader::load_prefix`] or leaves them to the pipe.

use std::collections::BTreeMap;

use celld_export_format::SchemaBody;

use crate::{literal, statement, task_body, Deployment, DynamicTable, RenderError, LOAD_SQL};

/// A statement's result as the SQL API returns it: every value as text,
/// NULL as `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rows {
    pub columns: Vec<String>,
    pub data: Vec<Vec<Option<String>>>,
}

impl Rows {
    /// The value of `column` (matched ignoring case) in row `row`.
    pub fn get(&self, row: usize, column: &str) -> Option<&str> {
        let i = self
            .columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(column))?;
        self.data.get(row)?.get(i)?.as_deref()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// A failed statement, with Snowflake's error code and SQL state when it
/// gave them.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}{}", code.as_deref().map(|c| format!(" (code {c})")).unwrap_or_default())]
pub struct WarehouseError {
    pub code: Option<String>,
    pub sql_state: Option<String>,
    pub message: String,
}

impl WarehouseError {
    pub fn other(message: impl Into<String>) -> Self {
        WarehouseError {
            code: None,
            sql_state: None,
            message: message.into(),
        }
    }
}

/// Somewhere to run one Snowflake statement.
pub trait Warehouse {
    fn execute(&mut self, sql: &str) -> Result<Rows, WarehouseError> {
        self.execute_bound(sql, &[])
    }

    /// Run `sql` with each `?` bound, in order, to a value of `binds`: a
    /// string, number, boolean or null. The reconciler's statements
    /// (`celld export`, C17) take their values this way.
    fn execute_bound(
        &mut self,
        sql: &str,
        binds: &[serde_json::Value],
    ) -> Result<Rows, WarehouseError>;
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("{statement}: {source}")]
    Warehouse {
        statement: String,
        #[source]
        source: WarehouseError,
    },
    #[error(transparent)]
    Render(#[from] RenderError),
    #[error("{0:?} is not a stage path of letters, digits and _ - . = /")]
    StagePath(String),
    #[error("a schema record in CELL_META does not decode: {0}")]
    Schema(serde_json::Error),
}

/// `schema` records by `(script, class, table)`.
pub type Schemas = BTreeMap<(String, String, String), Vec<SchemaBody>>;

/// What the loader needs besides a warehouse.
#[derive(Clone, Debug)]
pub struct LoaderConfig {
    pub deployment: Deployment,
    /// The Dynamic Tables' target lag, such as `1 minute`.
    pub target_lag: String,
    /// The first part of every Dynamic Table's name.
    pub dynamic_table_prefix: String,
}

/// What [`Loader::deploy`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeployReport {
    pub statements: usize,
    /// The queue the bucket's event notifications must go to for the pipe to
    /// auto-ingest (`SHOW PIPES`'s `notification_channel`).
    pub notification_channel: Option<String>,
    pub dynamic_tables: SyncReport,
}

/// What [`Loader::sync_dynamic_tables`] did, by Dynamic Table name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub created: Vec<String>,
    pub replaced: Vec<String>,
    pub unchanged: Vec<String>,
    /// Tables left without a Dynamic Table, and why: a table that was only
    /// ever dropped, or a column named like an export column.
    pub skipped: Vec<(String, String)>,
    /// Dynamic Tables whose statement failed; the next sync retries them.
    pub failed: Vec<(String, String)>,
}

/// A stream to erase. `incarnation: None` erases every incarnation.
#[derive(Clone, Debug)]
pub struct Erasure {
    pub script: String,
    pub class: String,
    pub cell: String,
    /// `None` for the root cell.
    pub facet: Option<String>,
    pub incarnation: Option<u64>,
    pub reason: Option<String>,
}

pub struct Loader<W> {
    pub warehouse: W,
    pub config: LoaderConfig,
}

impl<W: Warehouse> Loader<W> {
    pub fn new(warehouse: W, config: LoaderConfig) -> Self {
        Loader { warehouse, config }
    }

    fn run(&mut self, name: &str, sql: &str) -> Result<Rows, LoadError> {
        self.warehouse
            .execute(sql)
            .map_err(|source| LoadError::Warehouse {
                statement: name.to_string(),
                source,
            })
    }

    /// Create every object that does not exist yet, resume the tasks, and
    /// create the Dynamic Tables. Safe to run again: the objects are created
    /// `IF NOT EXISTS`, so a deployed object whose SQL changed must be
    /// dropped by hand first (see the README).
    pub fn deploy(&mut self) -> Result<DeployReport, LoadError> {
        let statements = self.config.deployment.statements()?;
        for s in &statements {
            self.run(&s.name, &s.sql)?;
        }
        let pipes = self.run("show pipes", "SHOW PIPES LIKE 'EXPORT_PIPE'")?;
        let notification_channel = pipes.get(0, "notification_channel").map(str::to_string);
        let dynamic_tables = self.sync_dynamic_tables()?;
        Ok(DeployReport {
            statements: statements.len(),
            notification_channel,
            dynamic_tables,
        })
    }

    /// Every `(script, class, table)` with its `schema` records, from all
    /// the streams that have ever exported one.
    pub fn schemas(&mut self) -> Result<Schemas, LoadError> {
        let rows = self.run(
            "read schemas",
            "SELECT DISTINCT script, class, TO_JSON(body) AS body \
             FROM CELL_META WHERE kind = 'schema'",
        )?;
        let mut out: BTreeMap<_, Vec<SchemaBody>> = BTreeMap::new();
        for r in 0..rows.len() {
            let (Some(script), Some(class), Some(body)) = (
                rows.get(r, "script"),
                rows.get(r, "class"),
                rows.get(r, "body"),
            ) else {
                continue;
            };
            let body: SchemaBody = serde_json::from_str(body).map_err(LoadError::Schema)?;
            let bodies = out
                .entry((script.to_string(), class.to_string(), body.table.clone()))
                .or_default();
            if !bodies.contains(&body) {
                bodies.push(body);
            }
        }
        // Rows arrive in no particular order, and the projection keeps
        // input order among equal generations, so sort to render the same
        // statement from the same union every time.
        for bodies in out.values_mut() {
            bodies.sort_by_cached_key(|b| {
                (
                    b.generation,
                    serde_json::to_string(b).expect("a schema body encodes"),
                )
            });
        }
        Ok(out)
    }

    /// Render each table's Dynamic Table from its schema union and create
    /// or replace the ones whose statement changed since the last sync.
    pub fn sync_dynamic_tables(&mut self) -> Result<SyncReport, LoadError> {
        let schemas = self.schemas()?;
        let existing = self.run(
            "read dynamic tables",
            "SELECT name, sql FROM EXPORT_DYNAMIC_TABLES",
        )?;
        let deployed: BTreeMap<String, String> = (0..existing.len())
            .filter_map(|r| {
                Some((
                    existing.get(r, "name")?.to_string(),
                    existing.get(r, "sql")?.to_string(),
                ))
            })
            .collect();
        let mut report = SyncReport::default();
        for ((script, class, table), bodies) in schemas {
            let dt = DynamicTable {
                name: dynamic_table_name(
                    &self.config.dynamic_table_prefix,
                    &script,
                    &class,
                    &table,
                ),
                target_lag: self.config.target_lag.clone(),
                warehouse: self.config.deployment.warehouse.clone(),
                script,
                class,
                table,
            };
            let sql = match dt.render(&bodies) {
                Ok(sql) => sql,
                Err(e @ (RenderError::NoSchema { .. } | RenderError::ReservedColumn(_))) => {
                    report.skipped.push((dt.name, e.to_string()));
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            let previous = deployed.get(&dt.name);
            if previous == Some(&sql) {
                report.unchanged.push(dt.name);
                continue;
            }
            if let Err(e) = self.warehouse.execute(&sql) {
                report.failed.push((dt.name, e.to_string()));
                continue;
            }
            // Recorded after the Dynamic Table exists: a crash between the
            // two only makes the next sync replace it once more.
            let name = literal(&dt.name);
            self.run(
                "record dynamic table",
                &format!("DELETE FROM EXPORT_DYNAMIC_TABLES WHERE name = {name}"),
            )?;
            self.run(
                "record dynamic table",
                &format!(
                    "INSERT INTO EXPORT_DYNAMIC_TABLES \
                     (name, script, class, table_name, sql, created_at) \
                     SELECT {name}, {}, {}, {}, {}, CURRENT_TIMESTAMP()",
                    literal(&dt.script),
                    literal(&dt.class),
                    literal(&dt.table),
                    literal(&sql)
                ),
            )?;
            if previous.is_some() {
                report.replaced.push(dt.name);
            } else {
                report.created.push(dt.name);
            }
        }
        Ok(report)
    }

    /// Copy the stage files under `prefix` (relative to the stage URL) into
    /// `EXPORT_LANDING`, then route them into the tables by running the
    /// route task's body, which returns once they are routed. This is how snapshot, repair and
    /// backfill files load when the pipe does not see them. Files COPY has
    /// already loaded are skipped. Returns COPY's per-file result.
    pub fn load_prefix(&mut self, prefix: &str) -> Result<Rows, LoadError> {
        let copy = copy_from_prefix(prefix)?;
        let loaded = self.run("copy_into_landing", &copy)?;
        self.run("route", &task_body("export_route_task")?)?;
        Ok(loaded)
    }

    /// Tombstone a stream, unless it already has an open tombstone for the
    /// same incarnations, and delete its rows by running the erase task's
    /// body, which returns once they are deleted. Routing stops taking its
    /// records at once; the views hide it at once. The rows stay in time
    /// travel for a day.
    pub fn erase(&mut self, e: &Erasure) -> Result<(), LoadError> {
        let incarnation = e
            .incarnation
            .map_or_else(|| "NULL".to_string(), |i| i.to_string());
        let reason = e
            .reason
            .as_deref()
            .map_or_else(|| "NULL".to_string(), literal);
        let (script, class, cell) = (literal(&e.script), literal(&e.class), literal(&e.cell));
        let facet = literal(e.facet.as_deref().unwrap_or(""));
        self.run(
            "tombstone",
            &format!(
                "INSERT INTO EXPORT_TOMBSTONES \
                 (script, class, cell, facet, incarnation, erased_at, reason) \
                 SELECT {script}, {class}, {cell}, {facet}, {incarnation}, CURRENT_TIMESTAMP(), {reason} \
                 WHERE NOT EXISTS (SELECT 1 FROM EXPORT_TOMBSTONES t \
                 WHERE t.cleared_at IS NULL AND t.script = {script} AND t.class = {class} \
                 AND t.cell = {cell} AND t.facet = {facet} \
                 AND t.incarnation IS NOT DISTINCT FROM {incarnation})"
            ),
        )?;
        self.run("erase", &task_body("export_erase_task")?)?;
        Ok(())
    }

    /// Any statement, with `?` binds: the read side for `verify` and the
    /// reconciler, and anything an operator needs.
    pub fn query(&mut self, sql: &str, binds: &[serde_json::Value]) -> Result<Rows, LoadError> {
        self.warehouse
            .execute_bound(sql, binds)
            .map_err(|source| LoadError::Warehouse {
                statement: "query".to_string(),
                source,
            })
    }

    /// What the repair driver works from: `EXPORT_GAPS`.
    pub fn gaps(&mut self) -> Result<Rows, LoadError> {
        self.run("gaps", "SELECT * FROM EXPORT_GAPS")
    }

    /// The positions the loader certifies, per stream and epoch.
    pub fn certified(&mut self) -> Result<Rows, LoadError> {
        self.run("certified", "SELECT * FROM CELL_CERTIFIED")
    }
}

/// `copy_into_landing` reading only the stage files under `prefix`.
pub fn copy_from_prefix(prefix: &str) -> Result<String, LoadError> {
    let ok = !prefix.is_empty()
        && !prefix.starts_with('/')
        && !prefix.split('/').any(|p| p == "..")
        && prefix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_-.=/".contains(c));
    if !ok {
        return Err(LoadError::StagePath(prefix.to_string()));
    }
    let copy = statement(LOAD_SQL, "copy_into_landing")?;
    Ok(copy.sql.replace(
        "FROM @EXPORT_STAGE",
        &format!("FROM @EXPORT_STAGE/{prefix}"),
    ))
}

/// The Dynamic Table for `(script, class, table)`: `PREFIX_SCRIPT_CLASS_TABLE`
/// in upper case with anything but letters, digits and `_` replaced by `_`,
/// and a hash of the three exact names, so names that fold together still
/// differ and a name never depends on what else exists.
pub fn dynamic_table_name(prefix: &str, script: &str, class: &str, table: &str) -> String {
    let part = |s: &str| -> String {
        s.chars()
            .take(48)
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect()
    };
    // FNV-1a, 64-bit: stable across releases and platforms.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in [script, class, table].join("\0").bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let name = format!(
        "{}_{}_{}_{}_{:08X}",
        part(prefix),
        part(script),
        part(class),
        part(table),
        h >> 32
    );
    name
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifier;
    use celld_export_format::ColumnDef;

    /// Records every statement and answers from canned results.
    #[derive(Default)]
    struct Fake {
        log: Vec<String>,
        schemas: Vec<(String, String, String)>,
        dynamic_tables: Vec<(String, String)>,
        fail_containing: Option<String>,
    }

    impl Warehouse for Fake {
        fn execute_bound(
            &mut self,
            sql: &str,
            _binds: &[serde_json::Value],
        ) -> Result<Rows, WarehouseError> {
            self.log.push(sql.to_string());
            if let Some(f) = &self.fail_containing {
                if sql.contains(f.as_str()) {
                    return Err(WarehouseError::other("boom"));
                }
            }
            let rows = |columns: &[&str], data: Vec<Vec<Option<String>>>| Rows {
                columns: columns.iter().map(|c| c.to_string()).collect(),
                data,
            };
            Ok(if sql.starts_with("SHOW PIPES") {
                rows(
                    &["name", "notification_channel"],
                    vec![vec![
                        Some("EXPORT_PIPE".into()),
                        Some("arn:aws:sqs:q".into()),
                    ]],
                )
            } else if sql.contains("FROM CELL_META WHERE kind = 'schema'") {
                rows(
                    &["SCRIPT", "CLASS", "BODY"],
                    self.schemas
                        .iter()
                        .map(|(s, c, b)| vec![Some(s.clone()), Some(c.clone()), Some(b.clone())])
                        .collect(),
                )
            } else if sql.starts_with("SELECT name, sql FROM EXPORT_DYNAMIC_TABLES") {
                rows(
                    &["NAME", "SQL"],
                    self.dynamic_tables
                        .iter()
                        .map(|(n, s)| vec![Some(n.clone()), Some(s.clone())])
                        .collect(),
                )
            } else {
                Rows::default()
            })
        }
    }

    fn schema(table: &str, generation: u64, columns: &[(&str, &str)]) -> String {
        serde_json::to_string(&SchemaBody {
            table: table.into(),
            generation,
            sql: String::new(),
            columns: columns
                .iter()
                .map(|(n, t)| ColumnDef {
                    name: (*n).into(),
                    decl_type: (*t).into(),
                    pk: 0,
                    not_null: false,
                    generated: false,
                })
                .collect(),
            dropped: false,
            renamed_from: None,
            unsupported: false,
        })
        .unwrap()
    }

    fn config() -> LoaderConfig {
        LoaderConfig {
            deployment: Deployment {
                stage_url: "s3://b/export/changes/".into(),
                storage_integration: "S3_INT".into(),
                warehouse: "WH".into(),
            },
            target_lag: "1 minute".into(),
            dynamic_table_prefix: "CF".into(),
        }
    }

    #[test]
    fn deploy_creates_everything_resumes_the_tasks_and_reports_the_queue() {
        let mut l = Loader::new(Fake::default(), config());
        let report = l.deploy().unwrap();
        let log = &l.warehouse.log;
        let n = config().deployment.statements().unwrap().len();
        assert_eq!(report.statements, n);
        assert!(log[..n].contains(&"ALTER TASK EXPORT_ROUTE RESUME".to_string()));
        assert!(log[..n].contains(&"ALTER TASK EXPORT_ERASE RESUME".to_string()));
        assert!(log[n].starts_with("SHOW PIPES"));
        assert_eq!(
            report.notification_channel.as_deref(),
            Some("arn:aws:sqs:q")
        );
        assert!(log
            .iter()
            .any(|s| s.contains("CREATE TABLE IF NOT EXISTS EXPORT_DYNAMIC_TABLES")));
    }

    #[test]
    fn sync_creates_then_leaves_alone_then_replaces_on_a_new_column() {
        let mut fake = Fake {
            schemas: vec![
                (
                    "app".into(),
                    "Room".into(),
                    schema("items", 1, &[("id", "INTEGER")]),
                ),
                // The same schema from another cell folds into one.
                (
                    "app".into(),
                    "Room".into(),
                    schema("items", 1, &[("id", "INTEGER")]),
                ),
                (
                    "app".into(),
                    "Room".into(),
                    schema("_cf", 1, &[("_CF_KEY", "")]),
                ),
            ],
            ..Fake::default()
        };
        let mut l = Loader::new(std::mem::take(&mut fake), config());
        let r = l.sync_dynamic_tables().unwrap();
        let items = dynamic_table_name("CF", "app", "Room", "items");
        assert_eq!(r.created, vec![items.clone()]);
        assert_eq!(r.skipped.len(), 1);
        let created = l
            .warehouse
            .log
            .iter()
            .find(|s| s.starts_with("CREATE OR REPLACE DYNAMIC TABLE"))
            .unwrap()
            .clone();
        assert!(created.contains(&items));
        assert!(l
            .warehouse
            .log
            .iter()
            .any(|s| s.starts_with("INSERT INTO EXPORT_DYNAMIC_TABLES")));

        l.warehouse.dynamic_tables = vec![(items.clone(), created)];
        l.warehouse.log.clear();
        let r = l.sync_dynamic_tables().unwrap();
        assert_eq!(r.unchanged, vec![items.clone()]);
        assert!(!l.warehouse.log.iter().any(|s| s.contains("DYNAMIC TABLE")));

        l.warehouse.schemas.push((
            "app".into(),
            "Room".into(),
            schema("items", 2, &[("id", "INTEGER"), ("name", "TEXT")]),
        ));
        let r = l.sync_dynamic_tables().unwrap();
        assert_eq!(r.replaced, vec![items]);
    }

    #[test]
    fn the_order_schemas_arrive_in_does_not_replace_a_table() {
        let a = (
            "app".to_string(),
            "Room".to_string(),
            schema("items", 1, &[("id", "INTEGER"), ("a", "TEXT")]),
        );
        let b = (
            "app".to_string(),
            "Room".to_string(),
            schema("items", 1, &[("id", "INTEGER"), ("b", "REAL")]),
        );
        let fake = Fake {
            schemas: vec![a.clone(), b.clone()],
            ..Fake::default()
        };
        let mut l = Loader::new(fake, config());
        assert_eq!(l.sync_dynamic_tables().unwrap().created.len(), 1);
        let name = dynamic_table_name("CF", "app", "Room", "items");
        let created = l
            .warehouse
            .log
            .iter()
            .find(|s| s.starts_with("CREATE OR REPLACE DYNAMIC TABLE"))
            .unwrap()
            .clone();
        l.warehouse.dynamic_tables = vec![(name.clone(), created)];
        l.warehouse.schemas = vec![b, a];
        assert_eq!(l.sync_dynamic_tables().unwrap().unchanged, vec![name]);
    }

    #[test]
    fn a_failed_dynamic_table_is_reported_and_not_recorded() {
        let fake = Fake {
            schemas: vec![(
                "app".into(),
                "Room".into(),
                schema("items", 1, &[("id", "INTEGER")]),
            )],
            fail_containing: Some("CREATE OR REPLACE DYNAMIC TABLE".into()),
            ..Fake::default()
        };
        let mut l = Loader::new(fake, config());
        let r = l.sync_dynamic_tables().unwrap();
        assert_eq!(r.failed.len(), 1);
        assert!(!l
            .warehouse
            .log
            .iter()
            .any(|s| s.starts_with("INSERT INTO EXPORT_DYNAMIC_TABLES")));
    }

    #[test]
    fn load_prefix_reads_only_the_prefix_and_routes() {
        let mut l = Loader::new(Fake::default(), config());
        l.load_prefix("repair/2026/09/29").unwrap();
        assert!(l.warehouse.log[0].contains("FROM @EXPORT_STAGE/repair/2026/09/29\n"));
        assert_eq!(l.warehouse.log[1], task_body("export_route_task").unwrap());
        for bad in ["", "/abs", "a/../b", "a b", "x'; DROP", "a\\b"] {
            assert!(
                matches!(copy_from_prefix(bad), Err(LoadError::StagePath(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn erase_quotes_everything_and_runs_the_erase_task() {
        let mut l = Loader::new(Fake::default(), config());
        l.erase(&Erasure {
            script: "app".into(),
            class: "Room".into(),
            cell: "it's".into(),
            facet: None,
            incarnation: None,
            reason: Some("gdpr".into()),
        })
        .unwrap();
        let sql = &l.warehouse.log[0];
        assert!(sql.contains("'it''s'"));
        assert!(sql.contains("t.facet = ''"));
        assert!(sql.contains("IS NOT DISTINCT FROM NULL"));
        assert_eq!(l.warehouse.log[1], task_body("export_erase_task").unwrap());
    }

    #[test]
    fn dynamic_table_names_are_identifiers_and_distinct() {
        let a = dynamic_table_name("CF", "app", "Room", "my-table");
        let b = dynamic_table_name("CF", "app", "Room", "my_table");
        let c = dynamic_table_name("CF", "APP", "Room", "my_table");
        assert!(a.starts_with("CF_APP_ROOM_MY_TABLE_"));
        assert!(identifier("t", &a).is_ok());
        assert_ne!(a, b);
        assert_ne!(b, c);
        let long = dynamic_table_name("CF", &"s".repeat(300), "ü", "t");
        assert!(identifier("t", &long).is_ok(), "{long}");
        assert_eq!(a, dynamic_table_name("CF", "app", "Room", "my-table"));
    }
}
