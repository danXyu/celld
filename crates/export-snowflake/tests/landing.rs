use celld_export_format::*;
use celld_export_snowflake::{LandingRow, LANDING_COLUMNS};

fn record(facet: Option<&str>, body: Body) -> Record {
    Record {
        envelope: Envelope {
            stream: StreamId {
                script: "app".into(),
                class: "Room".into(),
                cell: "r1".into(),
                facet: facet.map(Into::into),
                incarnation: u64::MAX,
            },
            cell_name: None,
            position: Position::new(u64::MAX, 7, 3),
            committed_at: 1_790_000_000_123,
            node: "node-a".into(),
            origin: Origin::Repair,
            fragment: 2,
            fragments: 3,
        },
        body,
    }
}

#[test]
fn a_landing_row_holds_the_whole_record() {
    let bodies = [
        Body::Rows(RowsBody {
            data: TableRows {
                table: "t".into(),
                generation: 2,
                columns: vec!["k".into(), "v".into()],
                key_columns: vec![ROWID_KEY_COLUMN.into()],
                rows: vec![RowChange(
                    Op::Update,
                    vec![Value::Integer(1)],
                    vec![Value::Real(f64::INFINITY), Value::Blob(vec![1, 2])],
                )],
            },
        }),
        Body::Watermark(WatermarkBody {
            from: None,
            through: Position::new(1, 2, 3),
            commits: 1,
            records: 4,
        }),
        Body::Deleted(DeletedBody {
            facet: Some("f".into()),
            incarnation: Some(9),
            subtree: true,
            through_incarnation: Some(12),
        }),
    ];
    for body in bodies {
        for facet in [None, Some("f/g")] {
            let r = record(facet, body.clone());
            let row = LandingRow::from_record(&r, "blob-stream/3/17");
            assert_eq!(row.to_record().unwrap(), r);
            // The body carries only kind-specific fields.
            let body: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(&row.body).unwrap();
            for c in LANDING_COLUMNS.iter().filter(|c| **c != "body") {
                assert!(!body.contains_key(*c), "{c} in body");
            }
            // The row's JSON field names are the landing columns.
            let j = serde_json::to_value(&row).unwrap();
            let names: Vec<&str> = j.as_object().unwrap().keys().map(|k| k.as_str()).collect();
            let mut want = LANDING_COLUMNS.to_vec();
            want.sort();
            let mut got = names.clone();
            got.sort();
            assert_eq!(got, want);
        }
    }
}
