// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This harness owns real child processes and host deadlines outside celld's
// injected execution boundary.
#![allow(clippy::disallowed_methods)]

use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Node {
    child: Child,
    directory: tempfile::TempDir,
    url: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Node {
    async fn start(client: &reqwest::Client) -> Self {
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let log = std::fs::File::create(directory.path().join("node.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        // Parent shell fleet configuration must not attach this disposable node
        // to a real bucket or select another listener.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("CELLD_") {
                command.env_remove(name);
            }
        }
        let child = command
            .args([
                "--no-control-plane",
                "--listen",
                "127.0.0.1:0",
                "--internal-listen",
                &address.to_string(),
            ])
            .env("CELLD_TEST_DATA_DIR", directory.path())
            .env("CELLD_SHUTDOWN_TOTAL_MS", "1000")
            .current_dir(directory.path())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let mut node = Self {
            child,
            directory,
            url: format!("http://{address}"),
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(response) = client.get(format!("{}/state", node.url)).send().await {
                if response.status().is_success() {
                    return node;
                }
            }
            if node.child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                panic!(
                    "node failed to become ready: {}",
                    std::fs::read_to_string(node.directory.path().join("node.log")).unwrap()
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

#[tokio::test]
async fn retired_modes_refuse_without_stopping_and_ordinary_shutdown_still_exits() {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let mut generations = Vec::new();
    for shutdown in ["/shutdown", "/shutdown?handoff=preserve"] {
        let mut node = Node::start(&client).await;
        let state: serde_json::Value = client
            .get(format!("{}/state", node.url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(state.get("node_load").is_some());
        assert!(state.get("node_log").is_none());
        assert_eq!(state["shutdown"]["schema_version"], 1);
        assert_eq!(
            state["shutdown"]["capabilities"]["strict_disk_removal"],
            false
        );
        let generation = state["shutdown"]["runtime_generation"].as_str().unwrap();
        assert!(!generation.is_empty());
        generations.push(generation.to_owned());
        for query in [
            "mode=remove-disk",
            "mode=remove-disk&handoff=preserve",
            "mode=unknown",
            "mode",
        ] {
            let response = client
                .post(format!("{}/shutdown?{query}", node.url))
                .json(&serde_json::json!({"operation_id":"old-client", "expected_generation":generation}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            assert!(node.child.try_wait().unwrap().is_none());
            let state: serde_json::Value = client
                .get(format!("{}/state", node.url))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(state["shutdown"]["runtime_generation"], generation);
        }
        let response = client
            .post(format!("{}{shutdown}", node.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = node.child.try_wait().unwrap() {
                assert!(status.success(), "shutdown failed: {status}");
                break;
            }
            assert!(Instant::now() < deadline, "ordinary shutdown did not exit");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    assert_ne!(generations[0], generations[1]);
}
