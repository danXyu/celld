use super::*;
use crate::bucket::StorageBackend;
use celld_ltx::ltx;
use object_store::memory::InMemory;

const SCOPE: &str = "Cart:one";

fn stream() -> Stream {
    Stream::cell(SCOPE).unwrap()
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

/// A whole WAL-mode database whose one row is `marker`, as an LTX file over
/// `min..=max`. Every file is a full image, so a restore holds exactly the
/// marker of the last file its plan applied.
fn image(min: u64, max: u64, marker: i64) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t (v INTEGER);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (?1)", [marker]).unwrap();
    drop(db);
    let bytes = std::fs::read(&path).unwrap();
    let page_size = u32::from(u16::from_be_bytes([bytes[16], bytes[17]]));
    let pages: Vec<_> = bytes
        .chunks_exact(page_size as usize)
        .enumerate()
        .map(|(i, p)| (i as u32 + 1, p.to_vec()))
        .collect();
    let checksum = pages.iter().fold(celld_ltx::CHECKSUM_FLAG, |sum, (n, p)| {
        sum ^ (ltx::checksum_page(*n, p) & !celld_ltx::CHECKSUM_FLAG)
    });
    let header = ltx::Header {
        version: ltx::VERSION,
        page_size,
        commit: pages.len() as u32,
        min_txid: TXID(min),
        max_txid: TXID(max),
        pre_apply_checksum: if min == 1 {
            0
        } else {
            celld_ltx::CHECKSUM_FLAG | 1
        },
        ..Default::default()
    };
    ltx::encode_file(&header, &pages, checksum).unwrap()
}

async fn put(bucket: &Bucket, epoch: u64, level: i32, min: u64, max: u64) {
    put_to(bucket, SCOPE, epoch, level, min, max).await;
}

async fn put_to(bucket: &Bucket, stream: &str, epoch: u64, level: i32, min: u64, max: u64) {
    let config = ObjectStoreConfig {
        path: format!("{}cells/{stream}/ltx/e{epoch}", bucket.prefix),
        ..Default::default()
    };
    ObjectStoreClient::with_store(config, bucket.store.clone())
        .write_ltx_file(level, TXID(min), TXID(max), &image(min, max, max as i64))
        .await
        .unwrap();
}

/// Epoch 3 opens with txid 1 and folds 3..=5 into one range; epoch 4 is a
/// paged continuation from 7.
async fn paged_chain() -> Bucket {
    let bucket = bucket();
    for (min, max) in [(1, 1), (2, 2), (3, 5), (6, 6)] {
        put(&bucket, 3, 0, min, max).await;
    }
    for (min, max) in [(7, 7), (8, 9)] {
        put(&bucket, 4, 0, min, max).await;
    }
    bucket
}

fn at(epoch: u64, txid: u64) -> Position {
    Position { epoch, txid }
}

async fn marker(bucket: &Bucket, target: Target) -> (Position, Position, i64) {
    let restored = restore(bucket, &stream(), target).await.unwrap();
    let v = restored
        .open()
        .unwrap()
        .query_row("SELECT v FROM t", [], |r| r.get(0))
        .unwrap();
    (restored.position, restored.bucket_head, v)
}

#[tokio::test]
async fn head_is_the_newest_cut_across_a_paged_continuation() {
    let bucket = paged_chain().await;
    assert_eq!(marker(&bucket, Target::Head).await, (at(4, 9), at(4, 9), 9));
}

#[tokio::test]
async fn at_or_after_lands_on_the_first_cut_not_below() {
    let bucket = paged_chain().await;
    for (target, cut) in [
        (at(3, 1), at(3, 1)),
        (at(3, 2), at(3, 2)),
        // Inside the folded range: the range's end is the first cut.
        (at(3, 3), at(3, 5)),
        (at(3, 4), at(3, 5)),
        (at(3, 6), at(3, 6)),
        // Past epoch 3's last commit: the continuation's first cut.
        (at(3, 7), at(4, 7)),
        (at(4, 8), at(4, 9)),
        // An older epoch than the chain holds: its first cut is after it.
        (at(2, 100), at(3, 1)),
    ] {
        assert_eq!(
            marker(&bucket, Target::AtOrAfter(target)).await,
            (cut, at(4, 9), cut.txid as i64),
            "target {target}"
        );
    }
}

#[tokio::test]
async fn past_the_bucket_head_fails_and_names_it() {
    let bucket = paged_chain().await;
    for target in [at(4, 10), at(5, 1)] {
        let error = restore(&bucket, &stream(), Target::AtOrAfter(target))
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("bucket head e4:9"), "{error}");
    }
}

#[tokio::test]
async fn a_clone_epoch_restarts_txids_and_is_after_everything_before_it() {
    let bucket = paged_chain().await;
    put(&bucket, 5, 0, 1, 1).await;
    put(&bucket, 5, 0, 2, 2).await;
    assert_eq!(
        marker(&bucket, Target::AtOrAfter(at(3, 4))).await,
        (at(5, 1), at(5, 2), 1)
    );
    assert_eq!(marker(&bucket, Target::Head).await.0, at(5, 2));
}

#[tokio::test]
async fn nothing_is_written_to_the_bucket_or_the_image() {
    let bucket = paged_chain().await;
    let before = bucket.list("").await.unwrap();
    let restored = restore(&bucket, &stream(), Target::AtOrAfter(at(3, 4)))
        .await
        .unwrap();
    let db = restored.open().unwrap();
    assert!(db.execute("INSERT INTO t VALUES (0)", []).is_err());
    assert!(db.execute_batch("CREATE TABLE u (x)").is_err());
    drop(db);
    let after = bucket.list("").await.unwrap();
    let keys = |list: &[object_store::ObjectMeta]| {
        list.iter()
            .map(|m| (m.location.to_string(), m.e_tag.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&before), keys(&after));

    let client = ReadOnly(
        ObjectStoreClient::with_store(
            ObjectStoreConfig {
                path: format!("{}cells/{SCOPE}/ltx/e3", bucket.prefix),
                ..Default::default()
            },
            bucket.store.clone(),
        ),
        None,
    );
    assert!(client
        .write_ltx_file(0, TXID(7), TXID(7), &image(7, 7, 7))
        .await
        .is_err());
    let files = client.ltx_files(0, TXID(0)).await.unwrap();
    assert!(client.delete_ltx_files(&files).await.is_err());
    assert!(client.delete_all().await.is_err());
    assert_eq!(keys(&bucket.list("").await.unwrap()), keys(&before));
}

#[tokio::test]
async fn a_cell_with_nothing_in_the_bucket_is_an_error() {
    assert!(restore(&bucket(), &stream(), Target::Head).await.is_err());
}

#[tokio::test]
async fn a_nested_facet_restores_from_its_own_stream() {
    let bucket = paged_chain().await;
    let names = ["cart".to_string(), "lines".to_string()];
    let facet = Stream::facet(SCOPE, &names).unwrap();
    assert_eq!(facet.as_str(), crate::engine_api::facet_cell(SCOPE, &names));
    assert_eq!(Stream::parse(facet.as_str()).unwrap(), facet);
    put_to(&bucket, facet.as_str(), 2, 0, 1, 40).await;
    put_to(&bucket, facet.as_str(), 2, 0, 41, 42).await;
    let restored = restore(&bucket, &facet, Target::AtOrAfter(at(2, 2)))
        .await
        .unwrap();
    let v: i64 = restored
        .open()
        .unwrap()
        .query_row("SELECT v FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        (restored.position, restored.bucket_head, v),
        (at(2, 40), at(2, 42), 40)
    );
    // The root's own stream is untouched by its facet's objects.
    assert_eq!(marker(&bucket, Target::Head).await.0, at(4, 9));
}

#[test]
fn a_stream_is_a_valid_root_and_facet_hashes_only() {
    let hash = "0123456789abcdef0123456789abcdef";
    for ok in [
        SCOPE.to_string(),
        format!("{SCOPE}/facets/{hash}"),
        format!("{SCOPE}/facets/{hash}/facets/{hash}"),
    ] {
        assert!(Stream::parse(&ok).is_ok(), "{ok}");
    }
    for bad in [
        String::new(),
        "not a scope".to_string(),
        format!("../{SCOPE}"),
        format!("{SCOPE}/facets/"),
        format!("{SCOPE}/facets/{}", &hash[1..]),
        format!("{SCOPE}/facets/{}", hash.to_uppercase()),
        format!("{SCOPE}/facets/{hash}/"),
        format!("{SCOPE}/facets/{hash}/facets/../{hash}"),
        format!("{SCOPE}/ltx/e1"),
    ] {
        assert!(Stream::parse(&bad).is_err(), "{bad}");
    }
    assert!(Stream::facet("../x", &["a".into()]).is_err());
}

#[test]
fn a_restore_path_is_escaped_for_the_uri() {
    assert_eq!(
        uri_path(Path::new("/tmp/a?b#c%d/db")).unwrap(),
        "/tmp/a%3fb%23c%25d/db"
    );
}

/// The planner's cuts over listings alone, without restoring anything.
mod planner {
    use super::*;
    use celld_ltx::replica::{
        calc_restore_plan, calc_restore_plan_at_or_after, cuts_of, restorable_cuts, SNAPSHOT_LEVEL,
    };

    /// A listing-only replica: the planner never opens a file here.
    struct Listing(Vec<FileInfo>);

    #[async_trait]
    impl ReplicaClient for Listing {
        async fn ltx_files(&self, level: i32, seek: TXID) -> LtxResult<Vec<FileInfo>> {
            let mut files: Vec<FileInfo> = self
                .0
                .iter()
                .filter(|f| f.level == level && f.min_txid >= seek)
                .cloned()
                .collect();
            files.sort_by_key(|f| (f.min_txid, f.max_txid));
            Ok(files)
        }
        async fn open_ltx_file(&self, _: i32, _: TXID, _: TXID) -> LtxResult<Vec<u8>> {
            unreachable!("planning only lists")
        }
        async fn write_ltx_file(&self, _: i32, _: TXID, _: TXID, _: &[u8]) -> LtxResult<FileInfo> {
            unreachable!("planning never writes")
        }
        async fn delete_ltx_files(&self, _: &[FileInfo]) -> LtxResult<()> {
            unreachable!("planning never deletes")
        }
        async fn delete_all(&self) -> LtxResult<()> {
            unreachable!("planning never deletes")
        }
    }

    fn file(level: i32, min: u64, max: u64) -> FileInfo {
        FileInfo {
            level,
            min_txid: TXID(min),
            max_txid: TXID(max),
            size: 4096,
            ..Default::default()
        }
    }

    fn end(plan: &[FileInfo]) -> TXID {
        plan.iter().map(|f| f.max_txid).max().unwrap_or_default()
    }

    fn txids(cuts: &[TXID]) -> Vec<u64> {
        cuts.iter().map(|t| t.0).collect()
    }

    /// L0 singles 1..=5 with an L1 over them, then a merged recovery tail
    /// 6..=9 and a single 10: nothing inside the tail is a cut.
    fn merged_tail() -> Vec<FileInfo> {
        let mut files: Vec<_> = (1..=5).map(|t| file(0, t, t)).collect();
        files.push(file(1, 1, 5));
        files.push(file(0, 6, 9));
        files.push(file(0, 10, 10));
        files
    }

    #[test]
    fn a_merged_range_hides_its_inner_txids() {
        assert_eq!(txids(&cuts_of(&merged_tail())), [1, 2, 3, 4, 5, 9, 10]);
    }

    #[test]
    fn a_snapshot_anchors_and_overlap_still_extends() {
        let files = vec![
            file(SNAPSHOT_LEVEL, 1, 7),
            file(0, 5, 8),
            file(1, 8, 12),
            file(0, 13, 13),
        ];
        assert_eq!(txids(&cuts_of(&files)), [7, 8, 12, 13]);
    }

    #[test]
    fn nothing_past_a_hole_is_a_cut() {
        // 2..=2 needs a cut at 1, which the 1..=3 range hides.
        let files = vec![file(0, 1, 3), file(0, 5, 6), file(0, 2, 2)];
        assert_eq!(txids(&cuts_of(&files)), [3]);
    }

    #[test]
    fn a_chain_without_its_start_has_no_cut() {
        assert!(cuts_of(&[file(0, 2, 4), file(0, 5, 5)]).is_empty());
    }

    #[tokio::test]
    async fn the_planner_ends_exactly_at_every_cut() {
        for files in [
            merged_tail(),
            vec![
                file(SNAPSHOT_LEVEL, 1, 7),
                file(0, 5, 8),
                file(1, 8, 12),
                file(0, 13, 13),
            ],
        ] {
            let client = Listing(files);
            let cuts = restorable_cuts(&client).await.unwrap();
            for cut in &cuts {
                let plan = calc_restore_plan(&client, *cut).await.unwrap();
                assert_eq!(end(&plan), *cut);
            }
            let head = calc_restore_plan(&client, TXID(0)).await.unwrap();
            assert_eq!(Some(&end(&head)), cuts.last());
        }
    }

    #[tokio::test]
    async fn at_or_after_takes_the_first_cut_not_below() {
        let client = Listing(merged_tail());
        for (target, cut) in [(1, 1), (5, 5), (6, 9), (8, 9), (9, 9), (10, 10)] {
            let (got, plan) = calc_restore_plan_at_or_after(&client, TXID(target))
                .await
                .unwrap();
            assert_eq!(got, TXID(cut), "target {target}");
            assert_eq!(end(&plan), TXID(cut));
        }
        assert!(matches!(
            calc_restore_plan_at_or_after(&client, TXID(11)).await,
            Err(LtxError::TxNotAvailable)
        ));
        // The planner alone has no plan inside the tail.
        assert!(calc_restore_plan(&client, TXID(7)).await.is_err());
    }
}
