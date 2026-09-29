//! Deterministic fixtures shared by the format and node benchmarks.
#![allow(dead_code)] // Each target uses only the fixtures it benchmarks.
use celld_export_format::*;

pub fn envelope(cell: usize, commit: u64) -> Envelope {
    Envelope {
        stream: StreamId {
            script: "bench".into(),
            class: "Items".into(),
            cell: format!("Items:{cell}"),
            facet: None,
            incarnation: 1,
        },
        cell_name: None,
        position: Position::new(1, commit, commit),
        committed_at: 1_790_000_000_000,
        node: "bench-node".into(),
        origin: Origin::Live,
        fragment: 1,
        fragments: 1,
    }
}

pub fn row(key: usize, payload: usize) -> RowChange {
    RowChange(
        Op::Insert,
        vec![Value::Integer(key as i64)],
        vec![
            Value::Integer(key as i64),
            Value::Text(format!("row-{key}\n{}", "x".repeat(payload))),
            Value::Blob((0..32).map(|n| (n + key) as u8).collect()),
            Value::Real(1.25),
            Value::Null,
        ],
    )
}

pub fn data(rows: Vec<RowChange>) -> TableRows {
    TableRows {
        table: "items".into(),
        generation: 1,
        columns: ["id", "text", "blob", "price", "optional"]
            .map(str::to_string)
            .to_vec(),
        key_columns: vec!["id".into()],
        rows,
    }
}

pub fn rows_record(rows: usize, payload: usize) -> Record {
    Record {
        envelope: envelope(0, 1),
        body: Body::Rows(RowsBody {
            data: data((0..rows).map(|key| row(key, payload)).collect()),
        }),
    }
}

/// Upserts into a fixed 128-key set per stream, with a watermark per commit,
/// duplicate deliveries, and optionally a complete authoritative repair.
pub fn history(commits: usize, streams: usize, repair: bool) -> Vec<Record> {
    assert!(streams > 0 && commits.is_multiple_of(streams));
    let mut records = Vec::new();
    for i in 0..commits {
        let cell = i % streams;
        let commit = (i / streams + 1) as u64;
        let mut r = rows_record(1, 128);
        r.envelope = envelope(cell, commit);
        let Body::Rows(body) = &mut r.body else {
            unreachable!()
        };
        body.data.rows = vec![row((commit as usize - 1) % 128, 128)];
        if commit > 128 {
            body.data.rows[0].0 = Op::Update;
        }
        records.push(r.clone());
        if i % 5 == 0 {
            records.push(r);
        }
        records.push(Record {
            envelope: envelope(cell, commit),
            body: Body::Watermark(WatermarkBody {
                from: (commit > 1).then(|| Position::new(1, commit - 1, commit - 1)),
                through: Position::new(1, commit, commit),
                commits: 1,
                records: 1,
            }),
        });
    }
    if repair {
        for cell in 0..streams {
            let mut env = envelope(cell, (commits / streams + 1) as u64);
            env.origin = Origin::Repair;
            let table = data(
                (0..(commits / streams).min(128))
                    .map(|key| row(key, 128))
                    .collect(),
            );
            let tg = table.table_gen();
            records.push(Record {
                envelope: env.clone(),
                body: Body::Snapshot(SnapshotBody {
                    snapshot_id: "repair".into(),
                    data: table,
                }),
            });
            records.push(Record {
                envelope: env,
                body: Body::SnapshotEnd(SnapshotEndBody {
                    snapshot_id: "repair".into(),
                    scope: SnapshotScope::Stream,
                    tables: vec![tg],
                    records: 1,
                }),
            });
        }
    }
    records
}

pub fn consumer(records: &[Record]) -> Consumer {
    let mut consumer = Consumer::new();
    consumer.ingest_all(records.iter().cloned()).unwrap();
    assert_eq!(consumer.incomplete(), 0);
    consumer
}

pub fn check_history(records: &[Record], commits: usize, streams: usize, repair: bool) {
    let state = consumer(records).state();
    assert_eq!(state.len(), streams);
    for state in state.values() {
        assert!(state.gaps.is_empty());
        assert_eq!(
            state.certified_head(),
            Some(Position::new(
                1,
                (commits / streams) as u64,
                (commits / streams) as u64
            ))
        );
        let table = state.table("items").unwrap();
        assert_eq!(table.rows.len(), (commits / streams).min(128));
        assert_eq!(state.snapshot_at.is_some(), repair);
        for (key, actual) in &table.rows {
            let Value::Integer(id) = key[0] else {
                panic!("integer key")
            };
            assert_eq!(*actual, row(id as usize, 128).2);
        }
    }
}
