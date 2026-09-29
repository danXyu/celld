// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This harness owns real child processes and host deadlines outside celld's
// injected execution boundary.
#![allow(clippy::disallowed_methods)]

//! Change export end to end: a `celld dev` node with `CELLD_EXPORT=1` writes
//! rows through a Durable Object, the bucket sink puts Parquet objects into
//! the node's local bucket, and the reference consumer applied to those
//! objects must hold exactly what the cell holds after a restart restores it
//! from the same bucket. The node's watermarks must certify every commit,
//! including across enough commits to pass the WAL's autocheckpoint.

use celld_export_format::{Body, Consumer, LinkMode, Position, Record, StreamState, Value};
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CONFIG: &str = r#"{
  "name": "exported",
  "main": "index.js",
  "no_bundle": true,
  "compatibility_date": "2026-01-01",
  "durable_objects": { "bindings": [{ "name": "ITEMS", "class_name": "Items" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Items"] }]
}"#;

const WORKER: &str = r#"
export class Items {
  constructor(state) {
    this.storage = state.storage;
    this.sql = state.storage.sql;
    this.sql.exec("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT, qty REAL)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS notes (body TEXT)");
  }
  async alarm() {}
  async fetch(request) {
    const url = new URL(request.url);
    const op = url.searchParams.get("op");
    const id = Number(url.searchParams.get("id"));
    if (op === "ddl") {
      // Schema changes, including deleteAll()'s drops, which run with the
      // SQL authorizer off.
      const step = url.searchParams.get("step");
      if (step === "create") {
        this.sql.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        this.sql.exec("INSERT INTO t VALUES (1, 'one'), (2, 'two')");
      } else if (step === "alter") {
        this.sql.exec("ALTER TABLE t ADD COLUMN w INTEGER DEFAULT 5");
      } else if (step === "wipe") {
        await this.storage.deleteAll();
        return Response.json({});
      } else if (step === "again") {
        this.sql.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        this.sql.exec("INSERT INTO t VALUES (3, 'three')");
      }
      return Response.json({ t: this.sql.exec("SELECT * FROM t ORDER BY id").toArray() });
    }
    if (op === "put") {
      this.sql.exec(
        "INSERT INTO items(id, name, qty) VALUES(?, ?, ?) " +
          "ON CONFLICT(id) DO UPDATE SET name=excluded.name, qty=excluded.qty",
        id, url.searchParams.get("name"), Number(url.searchParams.get("qty")));
    } else if (op === "delete") {
      this.sql.exec("DELETE FROM items WHERE id = ?", id);
    } else if (op === "many") {
      // One commit per statement, enough frames to pass the WAL's
      // autocheckpoint and the capture loop's restarts.
      for (let i = 0; i < 1500; i++) {
        this.sql.exec("INSERT INTO items(id, name, qty) VALUES(?, ?, ?)", 1000 + i, "row " + i, i);
      }
      this.sql.exec("DELETE FROM items WHERE id >= 1000 AND id % 3 = 0");
    } else if (op === "alarm") {
      // A write to no exported table: its txid carries no record.
      await this.storage.setAlarm(Date.now() + 3600 * 1000);
    } else if (op === "batch") {
      // Several statements in one transaction: one commit.
      this.storage.transactionSync(() => {
        for (let i = 0; i < 20; i++) {
          this.sql.exec("INSERT INTO items(id, name, qty) VALUES(?, ?, ?)", 100 + i, "bulk " + i, i / 4);
        }
        this.sql.exec("DELETE FROM items WHERE id = 105");
        this.sql.exec("INSERT INTO notes(body) VALUES('rowid table')");
      });
    }
    return Response.json({
      items: this.sql.exec("SELECT id, name, qty FROM items ORDER BY id").toArray(),
      notes: this.sql.exec("SELECT rowid AS id, body FROM notes ORDER BY rowid").toArray(),
    });
  }
}
export default {
  async fetch(request, env) {
    const name = new URL(request.url).searchParams.get("cell") ?? "default";
    return env.ITEMS.get(env.ITEMS.idFromName(name)).fetch(request);
  },
};
"#;

struct Dev {
    child: Child,
    url: String,
    log: PathBuf,
}

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Dev {
    async fn start(client: &reqwest::Client, project: &Path, run: usize) -> Dev {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let log = project.join(format!("dev-{run}.log"));
        let out = std::fs::File::create(&log).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("CELLD_") {
                command.env_remove(name);
            }
        }
        let child = command
            .args(["dev", "--no-watch", "--logs", "--port", &port.to_string()])
            .current_dir(project)
            .env("RUST_LOG", "info")
            .env("CELLD_EXPORT", "1")
            .env("CELLD_EXPORT_FLUSH_MS", "200")
            .env("CELLD_SHUTDOWN_TOTAL_MS", "1000")
            .stdin(Stdio::null())
            .stdout(Stdio::from(out.try_clone().unwrap()))
            .stderr(Stdio::from(out))
            .spawn()
            .unwrap();
        let mut dev = Dev {
            child,
            url: format!("http://127.0.0.1:{port}"),
            log,
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if client
                .get(format!("{}/?op=list&cell=probe", dev.url))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return dev;
            }
            if dev.child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                panic!("celld dev did not start:\n{}", dev.log_text());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    async fn call(&self, client: &reqwest::Client, query: &str) -> serde_json::Value {
        let response = client
            .get(format!("{}/?{query}", self.url))
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{query}: {}",
            response.status()
        );
        response.json().await.unwrap()
    }
}

/// Every export record the bucket sink has written to the dev store.
fn exported(project: &Path) -> Vec<celld_export_format::Record> {
    let store = project.join(".celld/dev/objects.sqlite3");
    let Ok(connection) =
        rusqlite::Connection::open_with_flags(&store, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
    else {
        return Vec::new();
    };
    let Ok(mut statement) = connection
        .prepare("SELECT body FROM objects WHERE key LIKE 'export/changes/%' ORDER BY key")
    else {
        return Vec::new();
    };
    let bodies: Vec<Vec<u8>> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    bodies
        .into_iter()
        .flat_map(|body| celld::export_sink::decode_records(body).unwrap())
        .collect()
}

/// A table as `(key, row)`, with every number as a real so that SQLite's
/// integer-valued reals compare equal to JavaScript's numbers.
type Rows = BTreeMap<String, Vec<serde_json::Value>>;

fn normalize(value: &serde_json::Value) -> serde_json::Value {
    match value.as_f64() {
        Some(number) => serde_json::json!(number),
        None => value.clone(),
    }
}

fn cell_rows(answer: &serde_json::Value, table: &str, columns: &[&str]) -> Rows {
    answer[table]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["id"].to_string(),
                columns
                    .iter()
                    .map(|column| normalize(&row[*column]))
                    .collect(),
            )
        })
        .collect()
}

fn exported_rows(state: &StreamState, table: &str) -> Rows {
    let Some(table) = state.table(table) else {
        return Rows::new();
    };
    table
        .rows
        .iter()
        .map(|(key, row)| {
            let json = |value: &Value| normalize(&serde_json::to_value(value).unwrap());
            let key = match &key[..] {
                [Value::Integer(id)] => id.to_string(),
                other => panic!("unexpected key {other:?}"),
            };
            (key, row.iter().map(json).collect())
        })
        .collect()
}

/// The consumer's view of the stream of the cell named `cell_id`.
fn stream_state(
    records: &[celld_export_format::Record],
    cell_id: &str,
) -> Option<(StreamState, Position)> {
    let mut consumer = Consumer::new();
    consumer.ingest_all(records.iter().cloned()).unwrap();
    let (stream, state) = consumer
        .state()
        .into_iter()
        .find(|(stream, _)| stream.cell == cell_id)?;
    let newest = records
        .iter()
        .filter(|r| r.envelope.stream == stream && !matches!(r.body, Body::Watermark(_)))
        .map(|r| r.envelope.position)
        .max()?;
    Some((state, newest))
}

/// The `link` records of the cell `cell_id`, in position order.
fn links(records: &[Record], cell_id: &str) -> Vec<Record> {
    let mut links: Vec<Record> = records
        .iter()
        .filter(|r| r.envelope.stream.cell == cell_id && matches!(r.body, Body::Link(_)))
        .cloned()
        .collect();
    links.sort_by_key(|r| r.envelope.position);
    links.dedup_by_key(|r| r.envelope.position);
    links
}

/// Checks the first residency of a stream: it opens with a fresh link, its
/// incarnation is the epoch it began in, and its rows carry the cell's name.
fn assert_opens_fresh(records: &[Record], cell_id: &str, name: &str) -> u64 {
    let links = links(records, cell_id);
    let [link] = &links[..] else {
        panic!("one link for {cell_id}: {links:#?}");
    };
    let Body::Link(body) = &link.body else {
        unreachable!()
    };
    assert_eq!(body.mode, LinkMode::Fresh);
    assert_eq!((body.prev_epoch, body.prev_txid), (None, None));
    let epoch = link.envelope.position.epoch;
    assert_eq!(
        link.envelope.position,
        Position::new(epoch, body.start_txid, 0)
    );
    assert_eq!(link.envelope.stream.incarnation, epoch);
    let first = records
        .iter()
        .filter(|r| r.envelope.stream.cell == cell_id)
        .map(|r| r.envelope.position)
        .min();
    assert_eq!(first, Some(link.envelope.position), "the link comes first");
    for record in records.iter().filter(|r| r.envelope.stream.cell == cell_id) {
        assert_eq!(record.envelope.stream.incarnation, epoch);
        if matches!(record.body, Body::Rows(_)) {
            assert_eq!(record.envelope.cell_name.as_deref(), Some(name));
        }
    }
    epoch
}

#[tokio::test(flavor = "multi_thread")]
async fn exported_rows_match_the_restored_cell() {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let project = tempfile::tempdir().unwrap();
    std::fs::write(project.path().join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(project.path().join("index.js"), WORKER).unwrap();

    let dev = Dev::start(&client, project.path(), 1).await;
    for query in [
        "cell=a&op=put&id=1&name=apple&qty=2.5",
        "cell=a&op=put&id=2&name=pear&qty=1",
        "cell=b&op=put&id=1&name=fig&qty=7",
        "cell=a&op=put&id=1&name=apple&qty=3",
        "cell=a&op=batch",
        "cell=a&op=many",
        "cell=a&op=delete&id=2",
        "cell=b&op=put&id=2&name=kiwi&qty=0.5",
        "cell=b&op=delete&id=1",
        "cell=b&op=alarm",
        "cell=c&op=ddl&step=create",
        "cell=c&op=ddl&step=alter",
        "cell=c&op=ddl&step=wipe",
        "cell=c&op=ddl&step=again",
    ] {
        dev.call(&client, query).await;
    }
    let live_a = dev.call(&client, "cell=a&op=list").await;
    let live_b = dev.call(&client, "cell=b&op=list").await;
    let log = dev.log_text();
    // The cell ids, from the node's activation log.
    let cells: Vec<String> = log
        .split("scope=")
        .skip(1)
        .filter_map(|rest| rest.split_whitespace().next())
        .filter(|scope| scope.starts_with("Items:"))
        .map(str::to_string)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    // Wait for every commit to be written and certified by a watermark.
    let deadline = Instant::now() + Duration::from_secs(30);
    let records = loop {
        let records = exported(project.path());
        let expected = |live: &serde_json::Value| {
            (
                cell_rows(live, "items", &["id", "name", "qty"]),
                cell_rows(live, "notes", &["body"]),
            )
        };
        let done = [&live_a, &live_b].iter().all(|live| {
            let want = expected(live);
            cells.iter().any(|cell| {
                stream_state(&records, cell).is_some_and(|(state, newest)| {
                    (
                        exported_rows(&state, "items"),
                        exported_rows(&state, "notes"),
                    ) == want
                        && state.certified_head().is_some_and(|head| head >= newest)
                        && state.gaps.is_empty()
                        && state.uncertain.is_empty()
                })
            })
        });
        // Cell c: `t` was created, altered, dropped by deleteAll() along
        // with the constructor's tables, and created again. Only the third
        // generation's row is live; nothing of the first two resurrects.
        let recreated = cells.iter().any(|cell| {
            stream_state(&records, cell).is_some_and(|(state, newest)| {
                let open: Vec<(&str, u64)> = state
                    .tables
                    .keys()
                    .map(|tg| (tg.table.as_str(), tg.generation))
                    .collect();
                open == [("t", 3)]
                    && exported_rows(&state, "t").len() == 1
                    && exported_rows(&state, "t")["3"]
                        == [serde_json::json!(3.0), serde_json::json!("three")]
                    && state.certified_head() == Some(newest)
                    && state.gaps.is_empty()
                    && state.uncertain.is_empty()
            })
        });
        if done && recreated {
            break records;
        }
        assert!(
            Instant::now() < deadline,
            "the export did not converge on the cells' rows; {} records:\n{:#?}\nlog:\n{}",
            records.len(),
            records,
            dev.log_text()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // Cell b's last write set an alarm, a table the export skips. Its TXID
    // carries no record, yet the watermarks must pass it, or the link of
    // b's next residency would name a predecessor the consumer never
    // certified.
    let want_b = cell_rows(&live_b, "items", &["id", "name", "qty"]);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let records = exported(project.path());
        let passed = cells.iter().any(|cell| {
            stream_state(&records, cell).is_some_and(|(state, newest)| {
                exported_rows(&state, "items") == want_b
                    && state
                        .certified_head()
                        .is_some_and(|head| head.txid > newest.txid)
            })
        });
        if passed {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the alarm's TXID was never certified:\n{}",
            dev.log_text()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(dev);
    let first_epochs: Vec<(String, u64)> = [("a", &live_a), ("b", &live_b)]
        .iter()
        .map(|(name, live)| {
            let want = cell_rows(live, "items", &["id", "name", "qty"]);
            let cell = cells
                .iter()
                .find(|cell| {
                    stream_state(&records, cell)
                        .is_some_and(|(state, _)| exported_rows(&state, "items") == want)
                })
                .unwrap();
            (cell.clone(), assert_opens_fresh(&records, cell, name))
        })
        .collect();

    // Restore both cells from the bucket and compare what they hold now
    // with what the export says they hold.
    let dev = Dev::start(&client, project.path(), 2).await;
    for (name, live) in [("a", &live_a), ("b", &live_b)] {
        let restored = dev.call(&client, &format!("cell={name}&op=list")).await;
        assert_eq!(&restored, live, "cell {name} restored differently");
        let matched = cells
            .iter()
            .filter_map(|cell| stream_state(&records, cell))
            .any(|(state, _)| {
                exported_rows(&state, "items")
                    == cell_rows(&restored, "items", &["id", "name", "qty"])
                    && exported_rows(&state, "notes") == cell_rows(&restored, "notes", &["body"])
            });
        assert!(
            matched,
            "no exported stream holds cell {name}'s restored rows"
        );
    }
    // Every commit carries a position the stream orders by, and the batch
    // was one commit: its 20 inserts, one delete and one insert into a
    // rowid table share one position.
    let batch: Vec<&celld_export_format::Record> = records
        .iter()
        .filter(|r| {
            matches!(&r.body, Body::Rows(rows) if rows.data.rows.iter().any(|row| {
                matches!(row.key(), [Value::Integer(id)] if (100..120).contains(id))
            }))
        })
        .collect();
    assert_eq!(batch.len(), 1, "the batch is one rows record for items");
    // The row inserted and deleted inside the transaction nets to nothing.
    let Body::Rows(rows) = &batch[0].body else {
        unreachable!()
    };
    assert_eq!(rows.data.rows.len(), 19);
    let notes = records
        .iter()
        .find(|r| matches!(&r.body, Body::Rows(rows) if rows.data.table == "notes"))
        .expect("the rowid table is exported");
    assert_eq!(notes.envelope.position, batch[0].envelope.position);

    // The restart's residencies link back to the first: same stream
    // identity, a later epoch, and a predecessor position the consumer has
    // certified, so the stream has no gap across the restart.
    let live_a = dev
        .call(&client, "cell=a&op=put&id=3&name=plum&qty=4")
        .await;
    let live_b = dev
        .call(&client, "cell=b&op=put&id=3&name=date&qty=9")
        .await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let records = exported(project.path());
        let settled = [&live_a, &live_b]
            .iter()
            .zip(&first_epochs)
            .all(|(live, (cell, _))| {
                stream_state(&records, cell).is_some_and(|(state, newest)| {
                    exported_rows(&state, "items")
                        == cell_rows(live, "items", &["id", "name", "qty"])
                        && state.certified_head().is_some_and(|head| head >= newest)
                })
            });
        if settled {
            for (cell, first) in &first_epochs {
                let links = links(&records, cell);
                let [_, link] = &links[..] else {
                    panic!("two links for {cell}: {links:#?}");
                };
                let Body::Link(body) = &link.body else {
                    unreachable!()
                };
                assert!(link.envelope.position.epoch > *first, "{link:#?}");
                assert_eq!(link.envelope.stream.incarnation, *first);
                assert_ne!(body.mode, LinkMode::Fresh, "{link:#?}");
                assert_eq!(body.prev_epoch, Some(*first), "{link:#?}");
                assert!(body.prev_txid.is_some(), "{link:#?}");
                let (state, _) = stream_state(&records, cell).unwrap();
                assert!(state.gaps.is_empty(), "{cell}: {:#?}", state.gaps);
            }
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the restarted residencies did not export:\n{:#?}\nlog:\n{}",
            records
                .iter()
                .filter(|r| !matches!(r.body, Body::Rows(_)))
                .collect::<Vec<_>>(),
            dev.log_text()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
