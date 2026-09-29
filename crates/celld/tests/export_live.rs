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

use celld_export_format::{Body, Consumer, Position, StreamState, Value};
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
  async fetch(request) {
    const url = new URL(request.url);
    const op = url.searchParams.get("op");
    const id = Number(url.searchParams.get("id"));
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
                        && state.certified_head() == Some(newest)
                        && state.gaps.is_empty()
                        && state.uncertain.is_empty()
                })
            })
        });
        if done {
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
    drop(dev);

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
}
