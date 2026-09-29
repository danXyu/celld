// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This harness owns real child processes and host deadlines outside celld's
// injected execution boundary.
#![allow(clippy::disallowed_methods)]

//! The Durable Object key-value API, exported: a `celld dev` node with
//! `CELLD_EXPORT=1` writes values through `ctx.storage`, and the reference
//! consumer applied to what the bucket sink wrote must hold table `kv` with
//! each live key and its value decoded to JSON.

use celld_export_format::{Consumer, Value};
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
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
  }
  async fetch(request) {
    const op = new URL(request.url).searchParams.get("op");
    if (op === "write") {
      await this.storage.put("profile", { name: "Ada", tags: ["x"], born: new Date(Date.UTC(1815, 11, 10)) });
      await this.storage.put({ counter: 1, doomed: "soon", big: 2n ** 70n });
      await this.storage.put("counter", 2);
      await this.storage.delete("doomed");
      this.storage.kv.put("sync", new Map([["a", 1]]));
    }
    return Response.json([...(await this.storage.list()).keys()]);
  }
}
export default {
  async fetch(request, env) {
    return env.ITEMS.get(env.ITEMS.idFromName("one")).fetch(request);
  },
};
"#;

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

/// The `kv` table of every exported stream, key to parsed JSON value.
fn kv_tables(records: &[celld_export_format::Record]) -> Vec<BTreeMap<String, serde_json::Value>> {
    let mut consumer = Consumer::new();
    consumer.ingest_all(records.iter().cloned()).unwrap();
    consumer
        .state()
        .values()
        .filter_map(|state| state.table("kv"))
        .map(|table| {
            assert_eq!(table.columns, ["key", "value"]);
            assert_eq!(table.key_columns, ["key"]);
            table
                .rows
                .values()
                .map(|row| match &row[..] {
                    [Value::Text(key), Value::Text(json)] => {
                        (key.clone(), serde_json::from_str(json).unwrap())
                    }
                    other => panic!("unexpected kv row {other:?}"),
                })
                .collect()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_object_values_are_exported_as_json() {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let project = tempfile::tempdir().unwrap();
    std::fs::write(project.path().join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(project.path().join("index.js"), WORKER).unwrap();

    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let log = project.path().join("dev.log");
    let out = std::fs::File::create(&log).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("CELLD_") {
            command.env_remove(name);
        }
    }
    let mut child = command
        .args(["dev", "--no-watch", "--logs", "--port", &port.to_string()])
        .current_dir(project.path())
        .env("CELLD_EXPORT", "1")
        .env("CELLD_EXPORT_FLUSH_MS", "200")
        .env("CELLD_SHUTDOWN_TOTAL_MS", "1000")
        .stdin(Stdio::null())
        .stdout(Stdio::from(out.try_clone().unwrap()))
        .stderr(Stdio::from(out))
        .spawn()
        .unwrap();
    let log_text = || std::fs::read_to_string(&log).unwrap_or_default();
    let call = |op: &str| {
        client
            .get(format!("http://127.0.0.1:{port}/?op={op}"))
            .send()
    };

    let deadline = Instant::now() + Duration::from_secs(60);
    while !call("list")
        .await
        .is_ok_and(|response| response.status().is_success())
    {
        if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
            let _ = child.kill();
            panic!("celld dev did not start:\n{}", log_text());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let keys: Vec<String> = call("write").await.unwrap().json().await.unwrap();
    assert_eq!(keys, ["big", "counter", "profile", "sync"]);

    let want: BTreeMap<String, serde_json::Value> = [
        ("big", serde_json::json!({"$bigint": "1180591620717411303424"})),
        ("counter", serde_json::json!(2)),
        (
            "profile",
            serde_json::json!({"name": "Ada", "tags": ["x"], "born": {"$date": "1815-12-10T00:00:00.000Z"}}),
        ),
        ("sync", serde_json::json!({"$map": [["a", 1]]})),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value))
    .collect();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let records = exported(project.path());
        let tables = kv_tables(&records);
        if tables.contains(&want) {
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!(
                "the export did not converge on the cell's key-value state; kv tables: {tables:#?}\nlog:\n{}",
                log_text()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
}
