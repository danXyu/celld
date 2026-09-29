//! The Dynamic Table for one `(script, class, table)`, with a typed
//! projection generated from the union of the table's `schema` records.

use celld_export_format::SchemaBody;

use crate::{
    escape_literal_body, fill, identifier, literal, quoted_identifier, statement, RenderError,
    DYNAMIC_TABLE_SQL,
};

/// The Snowflake type a projected column takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnType {
    /// SQLite INTEGER affinity: `NUMBER(38, 0)`.
    Integer,
    /// REAL affinity: `FLOAT`, with `{"$real": "inf"}` decoded.
    Real,
    /// TEXT affinity: `STRING`.
    Text,
    /// A declared `BLOB`: `BINARY`, from `{"$blob": ...}`.
    Blob,
    /// NUMERIC affinity, no declared type, or generations that disagree: the
    /// exported value as a `VARIANT`.
    Variant,
}

/// SQLite's affinity rules (datatype3.html, 3.1) mapped to a column type. A
/// column with no declared type holds anything, and NUMERIC affinity keeps
/// integers and reals alike, so both stay `VARIANT`.
pub fn affinity(decl_type: &str) -> ColumnType {
    let t = decl_type.to_ascii_uppercase();
    if t.contains("INT") {
        ColumnType::Integer
    } else if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        ColumnType::Text
    } else if t.contains("BLOB") {
        ColumnType::Blob
    } else if t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") {
        ColumnType::Real
    } else {
        ColumnType::Variant
    }
}

/// One typed column of the projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectedColumn {
    pub name: String,
    pub ty: ColumnType,
}

/// Export columns every Dynamic Table carries ahead of the table's own.
const EXPORT_COLUMNS: [&str; 15] = [
    "_CF_SCRIPT",
    "_CF_CLASS",
    "_CF_CELL",
    "_CF_FACET",
    "_CF_INCARNATION",
    "_CF_CELL_NAME",
    "_CF_GENERATION",
    "_CF_EPOCH",
    "_CF_TXID",
    "_CF_COMMIT",
    "_CF_COMMITTED_AT",
    "_CF_ORIGIN",
    "_CF_KEY",
    "_CF_COLUMNS",
    "_CF_ROW",
];

/// One Dynamic Table.
#[derive(Clone, Debug)]
pub struct DynamicTable {
    /// The Dynamic Table's name, a plain identifier.
    pub name: String,
    /// A Snowflake interval such as `1 minute`, or `DOWNSTREAM`.
    pub target_lag: String,
    pub warehouse: String,
    pub script: String,
    pub class: String,
    pub table: String,
}

impl DynamicTable {
    /// The typed projection: every column any generation of the table had,
    /// in the order the generations first list them, typed by affinity, and
    /// `VARIANT` where two generations disagree. Generations are taken in
    /// order so that a column keeps its place as the table evolves. `schema`
    /// records of other tables and `dropped` ones are ignored.
    pub fn projection(&self, schemas: &[SchemaBody]) -> Vec<ProjectedColumn> {
        let mut defs: Vec<&SchemaBody> = schemas
            .iter()
            .filter(|s| s.table == self.table && !s.dropped && !s.unsupported)
            .collect();
        defs.sort_by_key(|s| s.generation);
        let mut out: Vec<ProjectedColumn> = Vec::new();
        for def in defs {
            for c in &def.columns {
                let ty = affinity(&c.decl_type);
                match out.iter_mut().find(|p| p.name == c.name) {
                    Some(p) if p.ty != ty => p.ty = ColumnType::Variant,
                    Some(_) => {}
                    None => out.push(ProjectedColumn {
                        name: c.name.clone(),
                        ty,
                    }),
                }
            }
        }
        out
    }

    /// The `CREATE OR REPLACE DYNAMIC TABLE` statement.
    pub fn render(&self, schemas: &[SchemaBody]) -> Result<String, RenderError> {
        identifier("dynamic table name", &self.name)?;
        identifier("warehouse", &self.warehouse)?;
        let lag_ok = self.target_lag.eq_ignore_ascii_case("DOWNSTREAM")
            || self.target_lag.split_once(' ').is_some_and(|(n, unit)| {
                n.parse::<u32>().is_ok_and(|n| n > 0)
                    && [
                        "second", "seconds", "minute", "minutes", "hour", "hours", "day", "days",
                    ]
                    .contains(&unit.to_ascii_lowercase().as_str())
            });
        if !lag_ok {
            return Err(RenderError::TargetLag(self.target_lag.clone()));
        }
        let columns = self.projection(schemas);
        if columns.is_empty() {
            return Err(RenderError::NoSchema {
                table: self.table.clone(),
            });
        }
        if let Some(c) = columns
            .iter()
            .find(|c| EXPORT_COLUMNS.contains(&c.name.to_ascii_uppercase().as_str()))
        {
            return Err(RenderError::ReservedColumn(c.name.clone()));
        }
        let projection: String = columns
            .iter()
            .map(|c| {
                format!(
                    ",\n    {} AS {}",
                    typed(&value_of(&c.name), c.ty),
                    quoted_identifier(&c.name)
                )
            })
            .collect();
        let template = statement(DYNAMIC_TABLE_SQL, "dynamic_table")?;
        let vars = [
            ("NAME".to_string(), self.name.clone()),
            (
                "TARGET_LAG".to_string(),
                escape_literal_body(&self.target_lag),
            ),
            ("WAREHOUSE".to_string(), self.warehouse.clone()),
            ("SCRIPT".to_string(), literal(&self.script)),
            ("CLASS".to_string(), literal(&self.class)),
            ("TABLE".to_string(), literal(&self.table)),
            ("COLUMNS".to_string(), projection),
        ];
        fill(&template.name, &template.sql, &vars)
    }
}

/// The exported value of `column` in `latest.image`, found by name in the
/// record's own column list, NULL when that generation lacks it.
fn value_of(column: &str) -> String {
    format!(
        "image[ARRAY_POSITION({}::VARIANT, columns)]",
        literal(column)
    )
}

/// `v` as a column of type `ty`. A value that does not fit is NULL.
fn typed(v: &str, ty: ColumnType) -> String {
    // The scalar's text, NULL for a JSON null, a blob, or an infinite real.
    let text = format!(
        "IFF({v} IS NULL OR IS_NULL_VALUE({v}) OR {v}:\"$blob\" IS NOT NULL \
         OR {v}:\"$real\" IS NOT NULL, NULL, TO_VARCHAR({v}))"
    );
    match ty {
        ColumnType::Integer => format!("TRY_TO_NUMBER({text})"),
        ColumnType::Real => {
            format!("COALESCE(TRY_TO_DOUBLE({v}:\"$real\"::STRING), TRY_TO_DOUBLE({text}))")
        }
        ColumnType::Text => text,
        ColumnType::Blob => format!("TRY_BASE64_DECODE_BINARY({v}:\"$blob\"::STRING)"),
        ColumnType::Variant => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celld_export_format::ColumnDef;

    fn col(name: &str, ty: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            decl_type: ty.into(),
            pk: 0,
            not_null: false,
            generated: false,
        }
    }

    fn schema(table: &str, generation: u64, columns: Vec<ColumnDef>) -> SchemaBody {
        SchemaBody {
            table: table.into(),
            generation,
            sql: String::new(),
            columns,
            dropped: false,
            renamed_from: None,
            unsupported: false,
        }
    }

    fn dt() -> DynamicTable {
        DynamicTable {
            name: "ORDERS_T".into(),
            target_lag: "1 minute".into(),
            warehouse: "EXPORT_WH".into(),
            script: "shop".into(),
            class: "Cart".into(),
            table: "orders".into(),
        }
    }

    #[test]
    fn affinity_follows_sqlite() {
        assert_eq!(affinity("INTEGER"), ColumnType::Integer);
        assert_eq!(affinity("bigint"), ColumnType::Integer);
        assert_eq!(affinity("VARCHAR(10)"), ColumnType::Text);
        assert_eq!(affinity("CHARINT"), ColumnType::Integer);
        assert_eq!(affinity("blob"), ColumnType::Blob);
        assert_eq!(affinity(""), ColumnType::Variant);
        assert_eq!(affinity("DOUBLE PRECISION"), ColumnType::Real);
        assert_eq!(affinity("DECIMAL(10,2)"), ColumnType::Variant);
        assert_eq!(affinity("ANY"), ColumnType::Variant);
    }

    #[test]
    fn projection_unions_generations_and_widens_conflicts() {
        let schemas = vec![
            schema(
                "orders",
                2,
                vec![
                    col("id", "INTEGER"),
                    col("total", "TEXT"),
                    col("note", "TEXT"),
                ],
            ),
            schema(
                "orders",
                1,
                vec![col("id", "INTEGER"), col("total", "REAL")],
            ),
            schema("other", 1, vec![col("x", "INTEGER")]),
        ];
        let p = dt().projection(&schemas);
        assert_eq!(
            p,
            vec![
                ProjectedColumn {
                    name: "id".into(),
                    ty: ColumnType::Integer
                },
                ProjectedColumn {
                    name: "total".into(),
                    ty: ColumnType::Variant
                },
                ProjectedColumn {
                    name: "note".into(),
                    ty: ColumnType::Text
                },
            ]
        );
    }

    #[test]
    fn render_fills_the_template() {
        let schemas = vec![schema(
            "orders",
            1,
            vec![col("id", "INTEGER"), col("{{NAME}}", "")],
        )];
        let sql = dt().render(&schemas).unwrap();
        assert!(sql.starts_with("CREATE OR REPLACE DYNAMIC TABLE ORDERS_T"));
        assert!(sql.contains("TARGET_LAG = '1 minute'"));
        assert!(sql.contains("table_name = 'orders'"));
        assert!(sql.contains("AS \"id\""));
        assert!(sql.contains("AS \"{{NAME}}\""));
        assert!(!sql.contains("{{COLUMNS}}"));
    }

    #[test]
    fn render_refuses_bad_input() {
        let ok = vec![schema("orders", 1, vec![col("id", "INTEGER")])];
        let mut d = dt();
        d.target_lag = "1 minute'; DROP TABLE X; --".into();
        assert!(matches!(d.render(&ok), Err(RenderError::TargetLag(_))));
        let mut d = dt();
        d.name = "a b".into();
        assert!(matches!(d.render(&ok), Err(RenderError::Identifier { .. })));
        assert!(matches!(
            dt().render(&[]),
            Err(RenderError::NoSchema { .. })
        ));
        let reserved = vec![schema("orders", 1, vec![col("_cf_key", "")])];
        assert!(matches!(
            dt().render(&reserved),
            Err(RenderError::ReservedColumn(_))
        ));
    }
}
