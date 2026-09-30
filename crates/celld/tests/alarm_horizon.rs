// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This harness owns real child processes and host deadlines outside celld's
// injected execution boundary.
#![allow(clippy::disallowed_methods)]

//! Alarms past any timer horizon, end to end: a `celld dev` node runs Durable
//! Objects that arm alarms ten years ahead and at 9999-12-31. tokio-util's
//! `DelayQueue` panicked on such a deadline, aborting the node, and the stored
//! alarm aborted it again at every start. The node must keep serving, fire
//! near alarms beside the far ones, and restart with the far ones unchanged.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CONFIG: &str = r#"{
  "name": "alarms",
  "main": "index.js",
  "no_bundle": true,
  "compatibility_date": "2026-01-01",
  "durable_objects": { "bindings": [{ "name": "CLOCK", "class_name": "Clock" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Clock"] }]
}"#;

const WORKER: &str = r#"
import { DurableObject } from "cloudflare:workers";

const TEN_YEARS_MS = 10 * 365 * 24 * 60 * 60 * 1000;

export class Clock extends DurableObject {
  async fetch(request) {
    const op = new URL(request.url).searchParams.get("op");
    const storage = this.ctx.storage;
    if (op === "far") await storage.setAlarm(Date.now() + TEN_YEARS_MS);
    if (op === "max") await storage.setAlarm(Date.parse("9999-12-31T23:59:59.999Z"));
    if (op === "near") await storage.setAlarm(Date.now() + 1000);
    return Response.json({
      alarm: await storage.getAlarm(),
      fired: storage.kv.get("fired") ?? 0,
    });
  }

  async alarm() {
    this.ctx.storage.kv.put("fired", (this.ctx.storage.kv.get("fired") ?? 0) + 1);
  }
}

export default {
  fetch(request, env) {
    const name = new URL(request.url).searchParams.get("name") ?? "far";
    return env.CLOCK.getByName(name).fetch(request);
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

#[derive(serde::Deserialize)]
struct Reading {
    alarm: Option<f64>,
    fired: u64,
}

impl Dev {
    async fn start(client: &reqwest::Client, project: &Path) -> Dev {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let log = project.join("dev.log");
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("CELLD_") {
                command.env_remove(name);
            }
        }
        let child = command
            .args(["dev", "--no-watch", "--logs", "--port", &port.to_string()])
            .current_dir(project)
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
                .get(format!("{}/?name=probe", dev.url))
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

    async fn call(&mut self, client: &reqwest::Client, name: &str, op: &str) -> Reading {
        let response = client
            .get(format!("{}/?name={name}&op={op}", self.url))
            .send()
            .await;
        let alive = self.child.try_wait().unwrap().is_none();
        let response = response.unwrap_or_else(|error| {
            panic!(
                "{name} {op}: {error} (node alive: {alive})\n{}",
                self.log_text()
            )
        });
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        assert!(
            status.is_success(),
            "{name} {op}: {status} {body}\n{}",
            self.log_text()
        );
        serde_json::from_str(&body).unwrap()
    }
}

async fn burst(client: &reqwest::Client, url: &str, name: &str, op: &str, count: usize) {
    let requests = (0..count).map(|_| client.get(format!("{url}/?name={name}&op={op}")).send());
    for response in futures_util::future::join_all(requests).await {
        assert!(response.unwrap().status().is_success(), "{name} {op}");
    }
}

impl Dev {
    async fn await_fired(&mut self, client: &reqwest::Client, name: &str, fired: u64) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let reading = self.call(client, name, "read").await;
            assert!(
                reading.fired <= fired,
                "{name} fired more than {fired} times"
            );
            if reading.fired == fired {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{name} never fired:\n{}",
                self.log_text()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_years_ahead_neither_aborts_the_node_nor_its_restart() {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let project = tempfile::tempdir().unwrap();
    std::fs::write(project.path().join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(project.path().join("index.js"), WORKER).unwrap();

    let mut dev = Dev::start(&client, project.path()).await;
    let far = dev.call(&client, "far", "far").await.alarm;
    assert!(far.is_some(), "the ten-year alarm is armed");
    let max = dev.call(&client, "max", "max").await.alarm;
    assert_eq!(
        max,
        Some(253_402_300_799_999.0),
        "the 9999-12-31 alarm is armed"
    );

    // Each re-arm displaces the last; the near alarm that finally replaces
    // them fires exactly once.
    burst(&client, &dev.url, "churn", "far", 64).await;
    dev.call(&client, "churn", "near").await;
    dev.call(&client, "near", "near").await;
    dev.await_fired(&client, "churn", 1).await;
    dev.await_fired(&client, "near", 1).await;
    assert_eq!(dev.call(&client, "churn", "read").await.alarm, None);

    assert_eq!(dev.call(&client, "far", "read").await.alarm, far);
    assert_eq!(dev.call(&client, "max", "read").await.alarm, max);
    assert!(
        dev.child.try_wait().unwrap().is_none(),
        "the node is still running:\n{}",
        dev.log_text()
    );

    // The stored far alarms are armed again at start.
    drop(dev);
    let mut dev = Dev::start(&client, project.path()).await;
    for (name, alarm) in [("far", far), ("max", max)] {
        let after = dev.call(&client, name, "read").await;
        assert_eq!(after.alarm, alarm, "{name} survives a restart");
        assert_eq!(after.fired, 0, "{name} has not fired");
    }
    assert_eq!(dev.call(&client, "churn", "read").await.fired, 1);
    dev.call(&client, "near", "near").await;
    dev.await_fired(&client, "near", 2).await;
}
