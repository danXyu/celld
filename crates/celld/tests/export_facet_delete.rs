// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This harness owns real child processes and host deadlines outside celld's
// injected execution boundary.
#![allow(clippy::disallowed_methods)]

//! Facet `deleted` records end to end: a `celld dev` node with
//! `CELLD_EXPORT=1` runs a Durable Object that opens a facet with a nested
//! facet, deletes it, and opens both again. The root's stream must carry one
//! `deleted` record for the facet's subtree whose incarnation bound lies
//! above both facets' first incarnations and below both recreated ones, so a
//! consumer that removes the streams at or below the bound keeps the
//! recreated facets, the nested one included.

use celld_export_format::{Body, Record};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CONFIG: &str = r#"{
  "name": "facets",
  "main": "index.js",
  "no_bundle": true,
  "compatibility_date": "2026-01-01",
  "durable_objects": { "bindings": [{ "name": "ROOT", "class_name": "Root" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Root"] }]
}"#;

const WORKER: &str = r#"
import { DurableObject } from "cloudflare:workers";

export class Child extends DurableObject {
  async fetch(request) {
    this.ctx.storage.kv.put("n", (this.ctx.storage.kv.get("n") ?? 0) + 1);
    if (new URL(request.url).searchParams.get("nest") === "1") {
      const nested = this.ctx.facets.get("nested", () => ({ class: this.ctx.exports.Child }));
      await (await nested.fetch("http://facet/")).text();
    }
    return new Response("ok");
  }
}

export class Root extends DurableObject {
  async fetch(request) {
    const op = new URL(request.url).searchParams.get("op");
    const sql = this.ctx.storage.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS log (id INTEGER PRIMARY KEY, op TEXT)");
    sql.exec("INSERT INTO log(op) VALUES(?)", op);
    if (op === "open") {
      const child = this.ctx.facets.get("child", () => ({ class: this.ctx.exports.Child }));
      await (await child.fetch("http://facet/?nest=1")).text();
    } else if (op === "delete") {
      this.ctx.facets.delete("child");
    }
    return new Response("ok");
  }
}

export default {
  fetch(request, env) {
    return env.ROOT.getByName("only").fetch(request);
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
    async fn start(client: &reqwest::Client, project: &Path) -> Dev {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let log = project.join("dev.log");
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
                .get(format!("{}/?op=probe", dev.url))
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

    async fn call(&self, client: &reqwest::Client, op: &str) {
        let response = client
            .get(format!("{}/?op={op}", self.url))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        assert!(
            status.is_success(),
            "{op}: {status} {body}\n{}",
            self.log_text()
        );
    }
}

/// Every export record the bucket sink has written to the dev store.
fn exported(project: &Path) -> Vec<Record> {
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

/// The incarnation stamped in each facet database on disk, by the facet's
/// depth: 1 for the child, 2 for the nested facet.
fn incarnations(project: &Path) -> Vec<(usize, u64)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.file_name().is_some_and(|name| name == "db.sqlite") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&project.join(".celld"), &mut files);
    let mut found = Vec::new();
    for file in files {
        let depth = file.to_string_lossy().matches("/facets/").count();
        if depth == 0 {
            continue;
        }
        let connection = rusqlite::Connection::open(&file).unwrap();
        let incarnation: Option<i64> = connection
            .query_row("SELECT incarnation FROM _cf_METADATA", [], |row| row.get(0))
            .unwrap();
        found.push((
            depth,
            incarnation.expect("a facet has an incarnation") as u64,
        ));
    }
    found.sort();
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_facet_is_bounded_below_its_recreation() {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let project = tempfile::tempdir().unwrap();
    std::fs::write(project.path().join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(project.path().join("index.js"), WORKER).unwrap();

    let dev = Dev::start(&client, project.path()).await;
    dev.call(&client, "open").await;
    let before = incarnations(project.path());
    assert_eq!(
        before.iter().map(|(depth, _)| *depth).collect::<Vec<_>>(),
        [1, 2],
        "the child and its nested facet are on disk: {before:?}\n{}",
        dev.log_text()
    );
    dev.call(&client, "delete").await;
    // The next op waits for the delete, then recreates both facets.
    dev.call(&client, "open").await;
    let after = incarnations(project.path());
    assert_eq!(after.len(), 2, "{after:?}");

    let deadline = Instant::now() + Duration::from_secs(30);
    let deleted = loop {
        let records = exported(project.path());
        let deletes: Vec<Record> = records
            .into_iter()
            .filter(|r| matches!(r.body, Body::Deleted(_)))
            .collect();
        if !deletes.is_empty() {
            break deletes;
        }
        assert!(
            Instant::now() < deadline,
            "no deleted record was exported:\n{}",
            dev.log_text()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(deleted.len(), 1, "{deleted:#?}");
    let record = &deleted[0];
    let Body::Deleted(body) = &record.body else {
        unreachable!()
    };
    assert_eq!(
        record.envelope.stream.facet, None,
        "it rides the root's stream"
    );
    assert!(record.envelope.stream.cell.starts_with("Root:"));
    let path = body.facet.as_deref().expect("it names the facet");
    assert!(
        path.starts_with("facets/") && !path[7..].contains('/'),
        "the child's path below its root: {path}"
    );
    assert!(body.subtree);
    assert_eq!(body.incarnation, None);
    let through = body.through_incarnation.expect("it carries its bound");
    for (depth, incarnation) in &before {
        assert!(*incarnation <= through, "facet at depth {depth} is removed");
    }
    for (depth, incarnation) in &after {
        assert!(
            *incarnation > through,
            "recreated facet at depth {depth} survives"
        );
    }
    // It follows the root's own commits: the log row the delete op wrote
    // sorts before it, the one the next open wrote after it.
    let records = exported(project.path());
    let root_rows: Vec<_> = records
        .iter()
        .filter(|r| r.envelope.stream == record.envelope.stream)
        .filter_map(|r| match &r.body {
            Body::Rows(rows) if rows.data.table == "log" => Some(r.envelope.position),
            _ => None,
        })
        .collect();
    assert!(root_rows.iter().any(|p| *p < record.envelope.position));
}
