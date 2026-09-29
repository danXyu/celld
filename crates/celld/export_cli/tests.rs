use super::*;
use crate::bucket::StorageBackend;
use celld_export_format::{Body, Envelope, GapBody};
use object_store::memory::InMemory;

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

fn bucket() -> Bucket {
    let store = Arc::new(InMemory::new());
    Bucket::with_stores(
        store.clone(),
        store,
        StorageBackend::S3,
        "test".into(),
        "fleet/".into(),
    )
}

#[test]
fn repair_takes_one_stream_or_a_gaps_list() {
    let options = snapshot_options(
        Mode::Repair,
        args(&[
            "--stream", "Cart:one", "--at", "e3:17", "--bucket", "s3://b",
        ]),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        options.source,
        Source::Stream {
            scope: "Cart:one".into(),
            at: Some(export_restore::Position { epoch: 3, txid: 17 }),
        }
    );
    assert_eq!(options.fleet.bucket.as_deref(), Some("s3://b"));
    assert_eq!(options.node, DEFAULT_NODE);
    assert_eq!(
        (options.concurrency, options.rate),
        (DEFAULT_CONCURRENCY, DEFAULT_RATE)
    );

    let options = snapshot_options(
        Mode::Repair,
        args(&[
            "--gaps",
            "gaps.jsonl",
            "--class",
            "Cart",
            "--node",
            "ops-1",
            "--dry-run",
        ]),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        options.source,
        Source::Gaps {
            path: "gaps.jsonl".into(),
            class: Some("Cart".into())
        }
    );
    assert!(options.dry_run);
    assert_eq!(options.node, "ops-1");

    for bad in [
        &[][..],
        &["--stream", "Cart:one", "--gaps", "g"],
        &["--gaps", "g", "--at", "1:2"],
        &["--class", "Cart"],
        &["--stream", "../x"],
        &["--stream", "Cart:one", "--node", "a/b"],
        &["--stream", "Cart:one", "--concurrency", "0"],
        &["--stream", "Cart:one", "--after", "Cart:a"],
        &["--stream", "Cart:one", "--frobnicate"],
        &["--stream", "__Workflow.shop:one"],
        &["--stream", "__Queue:q"],
    ] {
        assert!(
            snapshot_options(Mode::Repair, args(bad)).is_err(),
            "{bad:?} was accepted"
        );
    }
    assert!(snapshot_options(Mode::Repair, args(&["--help"]))
        .unwrap()
        .is_none());
}

#[test]
fn backfill_takes_a_class_or_a_gaps_list_and_refuses_classes_never_exported() {
    let options = snapshot_options(
        Mode::Backfill,
        args(&[
            "--class",
            "Cart",
            "--after",
            "Cart:b",
            "--rate",
            "0",
            "--concurrency",
            "16",
        ]),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        options.source,
        Source::Class {
            class: "Cart".into(),
            after: Some("Cart:b".into())
        }
    );
    assert_eq!((options.concurrency, options.rate), (16, 0));
    assert!(snapshot_options(Mode::Backfill, args(&["--gaps", "g"])).is_ok());

    for bad in [
        &["--stream", "Cart:one"][..],
        &["--class", "Cart:one"],
        &["--class", "__Queue"],
        &["--class", "__Workflow.shop"],
        &[],
    ] {
        assert!(
            snapshot_options(Mode::Backfill, args(bad)).is_err(),
            "{bad:?} was accepted"
        );
    }
}

#[test]
fn positions_parse_as_reports_print_them() {
    assert_eq!(
        parse_position("e3:17").unwrap(),
        export_restore::Position { epoch: 3, txid: 17 }
    );
    assert_eq!(
        parse_position("4:1").unwrap(),
        export_restore::Position { epoch: 4, txid: 1 }
    );
    let printed = export_restore::Position { epoch: 9, txid: 2 }.to_string();
    assert_eq!(parse_position(&printed).unwrap().txid, 2);
    for bad in ["3", "e:1", "3:x", ""] {
        assert!(parse_position(bad).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn backfill_plans_every_cell_of_the_class_for_the_current_script() {
    // The class walk pages through the store's paginated listing, which the
    // local store serves.
    let dir = tempfile::tempdir().unwrap();
    let bucket = Bucket::open_dev(&dir.path().join("objects.sqlite3")).unwrap();
    for scope in ["Cart:a", "Cart:b", "Cart:c", "Carton:z", "Other:q"] {
        bucket
            .put(&format!("cells/{scope}/ltx/e1/0000.ltx"), vec![1])
            .await
            .unwrap();
    }
    bucket
        .put(
            "deploy/current.json",
            br#"{"script_name":"shop","version":"v1","prefix":"deploy/shop/v1","rollout":{"percent":100}}"#.to_vec(),
        )
        .await
        .unwrap();
    let options = snapshot_options(
        Mode::Backfill,
        args(&["--class", "Cart", "--after", "Cart:a"]),
    )
    .unwrap()
    .unwrap();
    let jobs = plan(&options, &bucket).await.unwrap();
    let cells: Vec<_> = jobs.iter().map(|j| j.stream.cell.as_str()).collect();
    assert_eq!(cells, ["Cart:b", "Cart:c"]);
    for job in &jobs {
        assert_eq!(job.stream.script, "shop");
        assert_eq!(job.stream.class, "Cart");
        assert_eq!(job.target, Target::Head);
    }

    // --script wins over the pointer.
    let options = snapshot_options(
        Mode::Repair,
        args(&["--stream", "Cart:a", "--script", "other"]),
    )
    .unwrap()
    .unwrap();
    let jobs = plan(&options, &bucket).await.unwrap();
    assert_eq!(jobs[0].stream.script, "other");
    assert_eq!(jobs[0].target, Target::Head);

    // Without either, the command says what to pass.
    let empty = self::bucket();
    let options = snapshot_options(Mode::Repair, args(&["--stream", "Cart:a"]))
        .unwrap()
        .unwrap();
    let error = plan(&options, &empty).await.unwrap_err();
    assert!(format!("{error:#}").contains("--script"), "{error:#}");
}

#[tokio::test]
async fn a_gaps_file_plans_repair_through_its_bounds_and_backfill_at_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gaps.jsonl");
    std::fs::write(
        &path,
        concat!(
            r#"{"SCRIPT":"shop","CLASS":"Cart","CELL":"Cart:a","FACET":"","INCARNATION":0,"GAP_KIND":"gap","BOUND_EPOCH":2,"BOUND_TXID":5}"#,
            "\n",
            r#"{"SCRIPT":"shop","CLASS":"Book","CELL":"Book:x","FACET":"","INCARNATION":0,"GAP_KIND":"bulk"}"#,
            "\n"
        ),
    )
    .unwrap();
    let path = path.to_str().unwrap();
    let bucket = bucket();

    let options = snapshot_options(Mode::Repair, args(&["--gaps", path]))
        .unwrap()
        .unwrap();
    let jobs = plan(&options, &bucket).await.unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].stream.cell, "Book:x");
    assert_eq!(jobs[0].target, Target::Head);
    assert_eq!(
        jobs[1].target,
        Target::AtOrAfter(export_restore::Position { epoch: 2, txid: 5 })
    );

    let options = snapshot_options(Mode::Repair, args(&["--gaps", path, "--class", "Cart"]))
        .unwrap()
        .unwrap();
    assert_eq!(plan(&options, &bucket).await.unwrap().len(), 1);

    let options = snapshot_options(Mode::Backfill, args(&["--gaps", path]))
        .unwrap()
        .unwrap();
    assert!(plan(&options, &bucket)
        .await
        .unwrap()
        .iter()
        .all(|j| j.target == Target::Head));
}

fn record(cell: &str, kind_gap: bool, txid: u64, origin: Origin) -> Record {
    Record {
        envelope: Envelope {
            stream: crate::export_repair::root_stream("shop", cell).unwrap(),
            cell_name: None,
            position: Position::new(1, txid, 1),
            committed_at: 0,
            node: "n1".into(),
            origin,
            fragment: 1,
            fragments: 1,
        },
        body: if kind_gap {
            Body::Gap(GapBody {
                from: Position::new(1, 0, 0),
                to: Position::new(1, txid, 1),
                reason: "test".into(),
            })
        } else {
            Body::Deleted(celld_export_format::DeletedBody {
                facet: None,
                incarnation: None,
                subtree: false,
                through_incarnation: None,
            })
        },
    }
}

#[test]
fn inspect_options_select_objects_and_filter_records() {
    let options = inspect_options(args(&[
        "--node",
        "n1",
        "--hour",
        "2026/09/29/02",
        "--kind",
        "gap",
        "--origin",
        "live",
        "--cell",
        "Cart:a",
        "--objects",
        "5",
    ]))
    .unwrap()
    .unwrap();
    assert_eq!(inspect_prefix(&options), "export/changes/n1/2026/09/29/02/");
    assert_eq!(options.objects, 5);
    assert!(options
        .filter
        .keeps(&record("Cart:a", true, 1, Origin::Live)));
    assert!(!options
        .filter
        .keeps(&record("Cart:b", true, 1, Origin::Live)));
    assert!(!options
        .filter
        .keeps(&record("Cart:a", false, 1, Origin::Live)));
    assert!(!options
        .filter
        .keeps(&record("Cart:a", true, 1, Origin::Repair)));
    assert_eq!(
        inspect_prefix(&inspect_options(vec![]).unwrap().unwrap()),
        "export/changes/"
    );

    for bad in [
        &["--hour", "2026/09/29"][..],
        &["--node", "n1", "--hour", "2026/09"],
        &["--node", "../x"],
        &["--kind", "rowz"],
        &["--origin", "elsewhere"],
        &["--file", "a.parquet", "--node", "n1"],
        &["--objects", "0"],
    ] {
        assert!(inspect_options(args(bad)).is_err(), "{bad:?} was accepted");
    }
}

#[tokio::test]
async fn inspect_lists_bounded_pages_and_decodes_records() {
    let bucket = bucket();
    let keys = [
        "export/changes/n1/2026/09/29/01/1-a.parquet",
        "export/changes/n1/2026/09/29/02/2-b.parquet",
        "export/changes/n1/2026/09/29/02/3-c.parquet",
        "export/changes/n2/2026/09/29/02/4-d.parquet",
    ];
    for (i, key) in keys.iter().enumerate() {
        let body = crate::export_sink::encode_records(vec![
            record("Cart:a", true, i as u64 + 1, Origin::Live),
            record("Cart:b", false, i as u64 + 1, Origin::Live),
        ])
        .unwrap();
        bucket.put(key, body).await.unwrap();
    }
    bucket
        .put("export/changes/n1/not-an-object.txt", vec![0])
        .await
        .unwrap();

    let mut options = inspect_options(args(&["--node", "n1", "--objects", "2"]))
        .unwrap()
        .unwrap();
    let (page, more) = list_objects(&bucket, &options).await.unwrap();
    assert_eq!(page, keys[..2]);
    assert!(more);
    options.after = Some(page[1].clone());
    let (page, more) = list_objects(&bucket, &options).await.unwrap();
    assert_eq!(page, keys[2..3]);
    assert!(!more);

    let options = inspect_options(args(&["--node", "n1", "--hour", "2026/09/29/02"]))
        .unwrap()
        .unwrap();
    let (page, _) = list_objects(&bucket, &options).await.unwrap();
    assert_eq!(page, keys[1..3]);

    // Every record, tagged with its object, reads back as a record.
    let mut out = Vec::new();
    let mut summary = Summary::default();
    let (bytes, _) = bucket.get(keys[0]).await.unwrap().unwrap();
    let records = crate::export_sink::decode_records(bytes.to_vec()).unwrap();
    emit(&mut out, &options, &mut summary, keys[0], records.clone()).unwrap();
    let lines: Vec<serde_json::Value> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["object"], keys[0]);
    assert_eq!(
        Record::from_json(lines[1].to_string().as_bytes()).unwrap(),
        records[1]
    );

    // The summary counts per stream instead.
    let options = inspect_options(args(&["--summary", "--cell", "Cart:a"]))
        .unwrap()
        .unwrap();
    let mut out = Vec::new();
    for key in keys {
        let (bytes, _) = bucket.get(key).await.unwrap().unwrap();
        let records = crate::export_sink::decode_records(bytes.to_vec()).unwrap();
        emit(&mut out, &options, &mut summary, key, records).unwrap();
    }
    assert!(out.is_empty());
    let a = &summary.streams[&crate::export_repair::root_stream("shop", "Cart:a").unwrap()];
    // One from the earlier non-summary pass is not counted; four here.
    assert_eq!(a.records, 4);
    assert_eq!(a.kinds["gap"], 4);
    assert_eq!(a.first, Some(Position::new(1, 1, 1)));
    assert_eq!(a.last, Some(Position::new(1, 4, 1)));
    assert_eq!(summary.streams.len(), 1);
}
