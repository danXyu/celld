//! The change export's Snowflake side: tables, loading, derived views, and
//! the Dynamic Table template.
//!
//! See `docs/design/change-export.md`, "The Snowflake loader" and "Erasure".
//! The SQL lives in `sql/` as plain Snowflake SQL, one `-- statement: name`
//! header per statement, so it can be read and run by hand:
//!
//! - `tables.sql`: `EXPORT_LANDING`, `CELL_CHANGES`, `CELL_META`,
//!   `EXPORT_TOMBSTONES`, `EXPORT_RECONCILER_FINDINGS`, and the loader's
//!   `EXPORT_DYNAMIC_TABLES`;
//! - `load.sql`: the Snowpipe Streaming pipe that lands records in
//!   `EXPORT_LANDING` ([`LandingRow`] is its row), and the tasks that route
//!   landed records past the tombstones into the two tables and erase
//!   tombstoned streams;
//! - `views.sql`: `CELL_STREAMS`, `CELL_CHANGES_CURRENT`, `CELL_META_CURRENT`,
//!   `CELL_SNAPSHOTS`, `CELL_GENERATIONS`, `CELL_CERTIFIED`, `EXPORT_GAPS`:
//!   the reference consumer's precedence rules in SQL;
//! - `dynamic_table.sql`: one Dynamic Table per `(script, class, table)`,
//!   which [`DynamicTable::render`] fills in from the table's `schema`
//!   records.
//!
//! [`loader`] deploys and drives these objects through a [`Warehouse`]: one
//! statement at a time, so it runs the same against Snowflake and against the
//! emulator. [`consume`] batches records to land. The `sql-api` feature
//! adds [`sql_api::SqlApi`], a Warehouse on Snowflake's SQL API,
//! [`streaming::Streaming`], which lands batches through Snowpipe Streaming,
//! and the `celld-export-loader` binary; the `blob-stream` feature adds
//! [`blob_stream`], which feeds them from the export topic.

// celld's rule against tokio::select! is for its execution boundary; the
// loader's consumer loop runs outside it, on the host's runtime. Clippy only
// honours this lint's allow at the crate root.
#![cfg_attr(feature = "blob-stream", allow(clippy::disallowed_macros))]

#[cfg(feature = "blob-stream")]
pub mod blob_stream;
pub mod consume;
mod dynamic_table;
mod landing;
pub mod loader;
#[cfg(feature = "sql-api")]
pub mod sql_api;
#[cfg(feature = "sql-api")]
pub mod streaming;

pub use dynamic_table::{affinity, ColumnType, DynamicTable, ProjectedColumn};
pub use landing::{LandingRow, LANDING_COLUMNS};
pub use loader::{Loader, LoaderConfig, Rows, Warehouse, WarehouseError};

pub const TABLES_SQL: &str = include_str!("../sql/tables.sql");
pub const LOAD_SQL: &str = include_str!("../sql/load.sql");
pub const VIEWS_SQL: &str = include_str!("../sql/views.sql");
pub const DYNAMIC_TABLE_SQL: &str = include_str!("../sql/dynamic_table.sql");

/// The Snowpipe Streaming pipe records land through (`load.sql`).
pub const LANDING_PIPE: &str = "EXPORT_LANDING_PIPE";

/// One named statement from a SQL file, without its leading comment lines
/// or its closing `;`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Statement {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    #[error("{what} {value:?} is not a plain Snowflake identifier")]
    Identifier { what: &'static str, value: String },
    #[error("{0:?} is not a Snowflake interval such as '1 minute'")]
    TargetLag(String),
    #[error("no statement named {0:?}")]
    NoStatement(String),
    #[error("statement {statement:?} leaves {placeholder} unfilled")]
    Unfilled {
        statement: String,
        placeholder: String,
    },
    #[error("table {table:?} has no schema records")]
    NoSchema { table: String },
    #[error("column {0:?} collides with an export column")]
    ReservedColumn(String),
}

/// Split a SQL file into its named statements, in file order. Text before
/// the first header is the file's commentary and is not a statement.
pub fn statements(file: &str) -> Vec<Statement> {
    const HEADER: &str = "-- statement:";
    let mut out: Vec<Statement> = Vec::new();
    let mut current: Option<(String, Vec<&str>)> = None;
    let finish = |current: Option<(String, Vec<&str>)>, out: &mut Vec<Statement>| {
        if let Some((name, lines)) = current {
            let body: Vec<&str> = lines
                .into_iter()
                .skip_while(|l| l.trim_start().starts_with("--") || l.trim().is_empty())
                .collect();
            let sql = body.join("\n");
            let sql = sql.trim_end().trim_end_matches(';').trim_end().to_string();
            out.push(Statement { name, sql });
        }
    };
    for line in file.lines() {
        if let Some(name) = line.strip_prefix(HEADER) {
            finish(current.take(), &mut out);
            current = Some((name.trim().to_string(), Vec::new()));
        } else if let Some((_, lines)) = current.as_mut() {
            lines.push(line);
        }
    }
    finish(current, &mut out);
    out
}

/// Where the export's objects are deployed. The loader creates them with
/// [`Deployment::statements`], in order, in the export schema.
#[derive(Clone, Debug)]
pub struct Deployment {
    /// The warehouse the route and erase tasks run on.
    pub warehouse: String,
}

impl Deployment {
    /// Every statement that sets the export up: tables, loading, views. The
    /// statements that only exist to be inlined into a task
    /// (`route_changes`, `route_meta`, `expire_landing`, `erase_tombstoned`,
    /// `erase_tombstoned_meta`) are left out; the tasks run them.
    pub fn statements(&self) -> Result<Vec<Statement>, RenderError> {
        identifier("warehouse", &self.warehouse)?;
        let load = statements(LOAD_SQL);
        let mut vars: Vec<(String, String)> = vec![("WAREHOUSE".into(), self.warehouse.clone())];
        vars.extend(
            load.iter()
                .map(|s| (s.name.to_ascii_uppercase(), s.sql.clone())),
        );
        const INLINED: [&str; 5] = [
            "route_changes",
            "route_meta",
            "expire_landing",
            "erase_tombstoned",
            "erase_tombstoned_meta",
        ];
        let mut out = statements(TABLES_SQL);
        for s in load {
            if INLINED.contains(&s.name.as_str()) {
                continue;
            }
            let sql = fill(&s.name, &s.sql, &vars)?;
            out.push(Statement { name: s.name, sql });
        }
        out.extend(statements(VIEWS_SQL));
        Ok(out)
    }
}

/// A task's body: the `EXECUTE IMMEDIATE` block the task named by
/// `statement` in `load.sql` runs, with the statements it inlines filled
/// in. Run on its own, it does the task's work synchronously, since
/// `EXECUTE TASK` only schedules a run.
pub fn task_body(statement_name: &str) -> Result<String, RenderError> {
    let task = statement(LOAD_SQL, statement_name)?;
    let start = task
        .sql
        .find("EXECUTE IMMEDIATE")
        .ok_or_else(|| RenderError::NoStatement(format!("{statement_name} body")))?;
    let vars: Vec<(String, String)> = statements(LOAD_SQL)
        .into_iter()
        .map(|s| (s.name.to_ascii_uppercase(), s.sql))
        .collect();
    fill(statement_name, &task.sql[start..], &vars)
}

/// The statement named `name` in `file`.
pub fn statement(file: &str, name: &str) -> Result<Statement, RenderError> {
    statements(file)
        .into_iter()
        .find(|s| s.name == name)
        .ok_or_else(|| RenderError::NoStatement(name.to_string()))
}

/// Replace each `{{NAME}}` in `sql` by its value in one pass, so a value is
/// never scanned for placeholders. Every placeholder must have a value.
pub(crate) fn fill(
    statement: &str,
    sql: &str,
    vars: &[(String, String)],
) -> Result<String, RenderError> {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let end = rest[start..].find("}}").map(|e| start + e + 2);
        let token = &rest[start..end.unwrap_or(rest.len())];
        let name = token.trim_start_matches("{{").trim_end_matches("}}");
        match (end, vars.iter().find(|(n, _)| n == name)) {
            (Some(end), Some((_, value))) => {
                out.push_str(value);
                rest = &rest[end..];
            }
            _ => {
                return Err(RenderError::Unfilled {
                    statement: statement.to_string(),
                    placeholder: token.to_string(),
                })
            }
        }
    }
    out.push_str(rest);
    Ok(out)
}

/// A plain identifier, which Snowflake resolves case-insensitively.
pub(crate) fn identifier(what: &'static str, value: &str) -> Result<(), RenderError> {
    let mut chars = value.chars();
    let ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        && value.len() <= 255;
    if ok {
        Ok(())
    } else {
        Err(RenderError::Identifier {
            what,
            value: value.to_string(),
        })
    }
}

/// The inside of a single-quoted Snowflake string literal: a quote doubled,
/// and a backslash escaped, since Snowflake reads backslash escapes there.
pub(crate) fn escape_literal_body(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

/// A single-quoted Snowflake string literal.
pub fn literal(s: &str) -> String {
    format!("'{}'", escape_literal_body(s))
}

/// A double-quoted Snowflake identifier, which keeps its case.
pub fn quoted_identifier(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_file_splits_into_named_statements() {
        let names = |f| {
            statements(f)
                .into_iter()
                .map(|s| s.name)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(TABLES_SQL),
            [
                "export_landing",
                "cell_changes",
                "cell_meta",
                "export_tombstones",
                "export_reconciler_findings",
                "export_dynamic_tables"
            ]
        );
        assert_eq!(
            names(VIEWS_SQL),
            [
                "cell_streams",
                "cell_changes_current",
                "cell_meta_current",
                "cell_snapshots",
                "cell_generations",
                "cell_certified",
                "export_gaps"
            ]
        );
        assert_eq!(names(DYNAMIC_TABLE_SQL), ["dynamic_table"]);
        for file in [TABLES_SQL, LOAD_SQL, VIEWS_SQL, DYNAMIC_TABLE_SQL] {
            for s in statements(file) {
                assert!(!s.sql.starts_with("--"), "{}", s.name);
                assert!(!s.sql.ends_with(';'), "{}", s.name);
                assert!(!s.sql.contains("-- statement:"), "{}", s.name);
            }
        }
    }

    #[test]
    fn deployment_fills_every_placeholder() {
        let d = Deployment {
            warehouse: "EXPORT_WH".into(),
        };
        let all = d.statements().unwrap();
        for s in &all {
            assert!(!s.sql.contains("{{"), "{}: {}", s.name, s.sql);
        }
        let pos = |n: &str| all.iter().position(|s| s.name == n).unwrap();
        let task = all.iter().find(|s| s.name == "export_route_task").unwrap();
        let route = statement(LOAD_SQL, "route_changes").unwrap();
        assert!(task.sql.contains(&route.sql));
        assert!(task.sql.contains("WAREHOUSE = EXPORT_WH"));
        // Tasks start suspended; setup resumes each after creating it.
        for (task, resume) in [
            ("export_route_task", "resume_route_task"),
            ("export_erase_task", "resume_erase_task"),
        ] {
            assert!(pos(task) < pos(resume));
        }
        assert_eq!(
            all.iter()
                .find(|s| s.name == "resume_route_task")
                .unwrap()
                .sql,
            "ALTER TASK EXPORT_ROUTE RESUME"
        );
        // Records land through the pipe, into the table the route task reads.
        let pipe = all
            .iter()
            .find(|s| s.name == "export_landing_pipe")
            .unwrap();
        assert!(pipe.sql.starts_with(&format!(
            "CREATE PIPE IF NOT EXISTS {LANDING_PIPE} AS\nCOPY INTO EXPORT_LANDING ("
        )));
        assert!(pipe
            .sql
            .contains("FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))"));
        assert!(pos("export_landing") < pos("export_landing_pipe"));
        // Every landing column comes from the row field of the same name.
        for c in LANDING_COLUMNS {
            assert!(pipe.sql.contains(&format!("$1:{c}::")), "{c}");
        }
        // Tables first, then loading, then views that read both.
        assert!(pos("cell_changes") < pos("export_route_task"));
        assert!(pos("export_route_task") < pos("cell_streams"));
    }

    #[test]
    fn task_bodies_are_the_deployed_tasks_bodies() {
        let d = Deployment {
            warehouse: "W".into(),
        };
        let all = d.statements().unwrap();
        for (task, inlined) in [
            ("export_route_task", "route_changes"),
            ("export_erase_task", "erase_tombstoned"),
        ] {
            let body = task_body(task).unwrap();
            assert!(body.starts_with("EXECUTE IMMEDIATE $$"), "{body}");
            assert!(body.contains(&statement(LOAD_SQL, inlined).unwrap().sql));
            let deployed = all.iter().find(|s| s.name == task).unwrap();
            assert!(deployed.sql.ends_with(&body));
        }
    }

    #[test]
    fn deployment_rejects_what_would_inject() {
        let d = Deployment {
            warehouse: "X; DROP TABLE CELL_CHANGES".into(),
        };
        assert!(matches!(
            d.statements(),
            Err(RenderError::Identifier { .. })
        ));
        assert_eq!(literal("it's\\"), "'it''s\\\\'");
        assert_eq!(quoted_identifier("a\"b"), "\"a\"\"b\"");
    }
}
