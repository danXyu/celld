#![allow(dead_code)]

use celld_export_format::*;

pub fn stream() -> StreamId {
    StreamId {
        script: "app".into(),
        class: "Chat".into(),
        cell: "c0ffee".into(),
        facet: None,
        incarnation: 1,
    }
}

pub fn facet(path: &str, incarnation: u64) -> StreamId {
    StreamId {
        facet: Some(path.into()),
        incarnation,
        ..stream()
    }
}

pub fn pos(txid: u64, commit: u64) -> Position {
    Position::new(1, txid, commit)
}

pub fn record(stream: &StreamId, at: Position, origin: Origin, body: Body) -> Record {
    Record {
        envelope: Envelope {
            stream: stream.clone(),
            cell_name: Some("room-1".into()),
            position: at,
            committed_at: 1_790_000_000_000,
            node: "node-a".into(),
            origin,
            fragment: 1,
            fragments: 1,
        },
        body,
    }
}

pub fn table_rows(table: &str, generation: u64, rows: Vec<RowChange>) -> TableRows {
    TableRows {
        table: table.into(),
        generation,
        columns: vec!["id".into(), "body".into()],
        key_columns: vec!["id".into()],
        rows,
    }
}

pub fn put(id: i64, body: &str) -> RowChange {
    RowChange(
        Op::Insert,
        vec![Value::Integer(id)],
        vec![Value::Integer(id), Value::Text(body.into())],
    )
}

pub fn upd(id: i64, body: &str) -> RowChange {
    RowChange(
        Op::Update,
        vec![Value::Integer(id)],
        vec![Value::Integer(id), Value::Text(body.into())],
    )
}

pub fn del(id: i64, body: &str) -> RowChange {
    RowChange(
        Op::Delete,
        vec![Value::Integer(id)],
        vec![Value::Integer(id), Value::Text(body.into())],
    )
}

pub fn rows(at: Position, table: &str, generation: u64, changes: Vec<RowChange>) -> Record {
    record(
        &stream(),
        at,
        Origin::Live,
        Body::Rows(RowsBody {
            data: table_rows(table, generation, changes),
        }),
    )
}

pub fn snapshot(
    at: Position,
    id: &str,
    table: &str,
    generation: u64,
    changes: Vec<RowChange>,
) -> Record {
    record(
        &stream(),
        at,
        Origin::Repair,
        Body::Snapshot(SnapshotBody {
            snapshot_id: id.into(),
            data: table_rows(table, generation, changes),
        }),
    )
}

pub fn snapshot_end(
    at: Position,
    id: &str,
    scope: SnapshotScope,
    tables: &[(&str, u64)],
    records: u64,
) -> Record {
    record(
        &stream(),
        at,
        Origin::Repair,
        Body::SnapshotEnd(SnapshotEndBody {
            snapshot_id: id.into(),
            scope,
            tables: tables
                .iter()
                .map(|(t, g)| TableGen {
                    table: (*t).into(),
                    generation: *g,
                })
                .collect(),
            records,
        }),
    )
}

pub fn schema(table: &str, generation: u64) -> SchemaBody {
    SchemaBody {
        table: table.into(),
        generation,
        sql: format!("CREATE TABLE {table}(id INTEGER PRIMARY KEY, body TEXT)"),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                decl_type: "INTEGER".into(),
                pk: 1,
                not_null: false,
                generated: false,
            },
            ColumnDef {
                name: "body".into(),
                decl_type: "TEXT".into(),
                pk: 0,
                not_null: false,
                generated: false,
            },
        ],
        dropped: false,
        renamed_from: None,
        unsupported: false,
    }
}

pub fn live(at: Position, body: Body) -> Record {
    record(&stream(), at, Origin::Live, body)
}

pub fn rows_of(state: &StreamState, table: &str) -> Vec<(i64, String)> {
    let Some(t) = state.table(table) else {
        return Vec::new();
    };
    t.rows
        .values()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Integer(i), Value::Text(s)) => (*i, s.clone()),
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}
