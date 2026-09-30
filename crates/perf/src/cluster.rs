// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The nodes under test, as subprocesses.
//!
//! Two backends:
//!
//! - `dev`: one `celld dev` in a copy of the fixture project, on the local
//!   SQLite store. Quick and self-contained; counting and CPU ceilings.
//! - `s3`: `celld deploy` to an S3-compatible bucket (MinIO, a real bucket),
//!   then N ordinary nodes on it, each with its own listeners and local
//!   state. This is the fleet path, and the only one with fleet proofs.
//!
//! Every node starts with no inherited `CELLD_*` variable, so the host's
//! configuration cannot leak into a measurement, then takes the scenario's
//! environment and the command line's `--env` overrides.

use anyhow::{anyhow, bail, Context as _};
use serde_json::Value;
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::{Child, Command};

use crate::netem::{Endpoint, Network};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Backend {
    Dev,
    S3 {
        endpoint: Option<String>,
        /// `s3://NAME[/PREFIX]`; each run adds its own prefix below it.
        bucket: String,
        region: String,
    },
}

impl Backend {
    pub fn name(&self) -> &'static str {
        match self {
            Backend::Dev => "dev",
            Backend::S3 { .. } => "s3",
        }
    }
}

pub struct Options {
    pub celld: PathBuf,
    pub backend: Backend,
    /// The run's working directory: project copy, node state, logs.
    pub work: PathBuf,
    pub fixture: PathBuf,
    pub nodes: usize,
    pub env: BTreeMap<String, String>,
    /// Per-node environment over `env`, by node index.
    pub node_env: BTreeMap<usize, BTreeMap<String, String>>,
    /// Unique per run, so runs never share bucket state.
    pub run_id: String,
    /// Put every node's peer and bucket traffic through [`Network`]'s
    /// proxies (s3 backend only).
    pub network: bool,
}

pub struct Node {
    pub index: usize,
    pub public: String,
    pub internal: String,
    child: Option<Child>,
    /// The process that serves: the node itself, or `celld dev`'s child.
    pub pid: Option<u32>,
    pub log: PathBuf,
    state: PathBuf,
    public_port: u16,
    internal_port: u16,
    /// With a network: the proxy peers reach this node through, which the
    /// node advertises, and the node's own proxy to the bucket.
    peer_proxy_port: Option<u16>,
    bucket_proxy_port: Option<u16>,
}

pub struct Cluster {
    options: Options,
    pub nodes: Vec<Node>,
    project: PathBuf,
    bucket: Option<String>,
    http: reqwest::Client,
    network: Option<Arc<Network>>,
    proxies: Vec<tokio::task::JoinHandle<()>>,
}

fn cluster_network(options: &Options) -> anyhow::Result<bool> {
    if options.network && !matches!(options.backend, Backend::S3 { .. }) {
        bail!("network faults need the s3 backend: a dev node has no peers and no bucket link");
    }
    Ok(options.network)
}

/// The bucket endpoint's socket address, for a proxy to forward to. Only a
/// plain-HTTP endpoint can be proxied this way.
async fn bucket_address(backend: &Backend) -> anyhow::Result<std::net::SocketAddr> {
    let Backend::S3 {
        endpoint: Some(endpoint),
        ..
    } = backend
    else {
        bail!("network faults need an explicit --endpoint for the bucket");
    };
    let authority = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| anyhow!("network faults need an http:// endpoint, not {endpoint}"))?
        .trim_end_matches('/');
    tokio::net::lookup_host(authority)
        .await?
        .next()
        .ok_or_else(|| anyhow!("{authority} resolves to nothing"))
}

fn free_port() -> anyhow::Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

fn copy_dir(from: &Path, to: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

impl Cluster {
    pub async fn start(options: Options) -> anyhow::Result<Cluster> {
        std::fs::create_dir_all(&options.work)?;
        let project = options.work.join("project");
        copy_dir(&options.fixture, &project)
            .with_context(|| format!("copy fixture {}", options.fixture.display()))?;
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(500))
            .timeout(Duration::from_secs(10))
            .build()?;
        let bucket = match &options.backend {
            Backend::Dev => {
                if options.nodes != 1 {
                    bail!(
                        "the dev backend runs one node; this scenario needs {}",
                        options.nodes
                    );
                }
                None
            }
            Backend::S3 { bucket, .. } => Some(format!(
                "{}/perf-{}",
                bucket.trim_end_matches('/'),
                options.run_id
            )),
        };
        let network = if cluster_network(&options)? {
            Some(Network::new(
                (0..options.nodes)
                    .map(|index| format!("perf-{index}"))
                    .collect(),
            ))
        } else {
            None
        };
        let mut cluster = Cluster {
            nodes: Vec::new(),
            project,
            bucket,
            http,
            options,
            network,
            proxies: Vec::new(),
        };
        if matches!(cluster.options.backend, Backend::S3 { .. }) {
            cluster.deploy().await?;
        }
        for index in 0..cluster.options.nodes {
            let node = Node {
                index,
                public: String::new(),
                internal: String::new(),
                child: None,
                pid: None,
                log: cluster.options.work.join(format!("node-{index}.log")),
                state: cluster.options.work.join(format!("node-{index}")),
                public_port: free_port()?,
                internal_port: free_port()?,
                peer_proxy_port: None,
                bucket_proxy_port: None,
            };
            cluster.nodes.push(node);
        }
        if let Some(network) = cluster.network.clone() {
            let bucket = bucket_address(&cluster.options.backend).await?;
            for node in &mut cluster.nodes {
                let peers = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                node.peer_proxy_port = Some(peers.local_addr()?.port());
                let target = format!("127.0.0.1:{}", node.internal_port).parse()?;
                cluster.proxies.push(network.serve(
                    peers,
                    target,
                    Endpoint::Node(node.index),
                    None,
                ));
                let objects = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                node.bucket_proxy_port = Some(objects.local_addr()?.port());
                cluster.proxies.push(network.serve(
                    objects,
                    bucket,
                    Endpoint::Bucket,
                    Some(Endpoint::Node(node.index)),
                ));
            }
        }
        for index in 0..cluster.nodes.len() {
            cluster.launch(index).await?;
        }
        for index in 0..cluster.nodes.len() {
            cluster.wait_ready(index).await?;
        }
        Ok(cluster)
    }

    fn base_command(&self) -> Command {
        let mut command = Command::new(&self.options.celld);
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("CELLD_") {
                command.env_remove(name);
            }
        }
        // Stop quickly between phases; a scenario can override it.
        command.env("CELLD_SHUTDOWN_TOTAL_MS", "5000");
        command.env("RUST_LOG", "info");
        for (name, value) in &self.options.env {
            command.env(name, value);
        }
        command.stdin(Stdio::null()).kill_on_drop(true);
        command
    }

    /// The bucket arguments; `through` replaces the endpoint with a node's
    /// bucket proxy.
    fn s3_args(&self, through: Option<u16>) -> Vec<String> {
        let Backend::S3 {
            endpoint, region, ..
        } = &self.options.backend
        else {
            return Vec::new();
        };
        let proxied = through.map(|port| format!("http://127.0.0.1:{port}"));
        let endpoint = proxied.as_ref().or(endpoint.as_ref());
        let mut args = vec![
            "--bucket".to_string(),
            self.bucket.clone().unwrap_or_default(),
            "--region".to_string(),
            region.clone(),
        ];
        if let Some(endpoint) = endpoint {
            args.push("--endpoint".into());
            args.push(endpoint.clone());
        }
        args
    }

    async fn deploy(&self) -> anyhow::Result<()> {
        let mut command = self.base_command();
        command
            .arg("deploy")
            .arg(&self.project)
            .args(self.s3_args(None))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = command.output().await.context("run celld deploy")?;
        if !output.status.success() {
            bail!(
                "celld deploy failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    async fn launch(&mut self, index: usize) -> anyhow::Result<()> {
        let mut command = self.base_command();
        for (name, value) in self.options.node_env.get(&index).into_iter().flatten() {
            command.env(name, value);
        }
        let node = &self.nodes[index];
        match &self.options.backend {
            Backend::Dev => {
                command
                    .args(["dev", "--no-watch", "--logs", "--port"])
                    .arg(node.public_port.to_string())
                    .current_dir(&self.project);
            }
            Backend::S3 { .. } => {
                std::fs::create_dir_all(&node.state)?;
                let internal = format!("127.0.0.1:{}", node.internal_port);
                // Peers dial the advertised address: the node's proxy, with
                // a network, or its listener.
                let advertise = format!(
                    "127.0.0.1:{}",
                    node.peer_proxy_port.unwrap_or(node.internal_port)
                );
                command
                    .arg("--no-control-plane")
                    .args(self.s3_args(node.bucket_proxy_port))
                    .args(["--listen", &format!("127.0.0.1:{}", node.public_port)])
                    .args(["--internal-listen", &internal])
                    .args(["--advertise", &advertise])
                    .env("CELLD_WATCH", &node.state)
                    .env("CELLD_NODE", format!("perf-{index}"));
            }
        }
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().context("start celld")?;
        let log = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&node.log)
            .await?;
        let log = std::sync::Arc::new(tokio::sync::Mutex::new(log));
        let (internal_tx, internal_rx) = tokio::sync::oneshot::channel();
        // `celld dev` forwards its node's lines to either stream, so both
        // are searched for the internal listener's address.
        let internal_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(internal_tx)));
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        fn pump(
            stream: impl tokio::io::AsyncRead + Unpin + Send + 'static,
            log: std::sync::Arc<tokio::sync::Mutex<tokio::fs::File>>,
            internal_tx: std::sync::Arc<
                std::sync::Mutex<Option<tokio::sync::oneshot::Sender<String>>>,
            >,
        ) {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stream).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(address) = line
                        .split("celld internal listening on ")
                        .nth(1)
                        .and_then(|rest| rest.split_whitespace().next())
                    {
                        if let Some(sender) = internal_tx.lock().unwrap().take() {
                            let _ = sender.send(address.to_string());
                        }
                    }
                    let mut log = log.lock().await;
                    let _ = log.write_all(line.as_bytes()).await;
                    let _ = log.write_all(b"\n").await;
                }
            });
        }
        pump(stdout, log.clone(), internal_tx.clone());
        pump(stderr, log, internal_tx);
        let supervisor = child.id();
        let node = &mut self.nodes[index];
        node.public = format!("http://127.0.0.1:{}", node.public_port);
        node.child = Some(child);
        let internal = tokio::time::timeout(Duration::from_secs(60), internal_rx)
            .await
            .map_err(|_| {
                anyhow!(
                    "node {index} never reported its internal listener; see {}",
                    node.log.display()
                )
            })?
            .map_err(|_| {
                anyhow!(
                    "node {index} exited during startup; see {}",
                    node.log.display()
                )
            })?;
        node.internal = format!("http://{internal}");
        node.pid = match self.options.backend {
            // `celld dev` starts other children too (a deploy, a bundler);
            // its node is the one started with `--no-control-plane`.
            Backend::Dev => supervisor.and_then(|pid| {
                crate::sysstat::children(pid, "no-control-plane")
                    .into_iter()
                    .next()
            }),
            Backend::S3 { .. } => supervisor,
        };
        Ok(())
    }

    async fn wait_ready(&mut self, index: usize) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(90);
        let url = format!("{}/.well-known/celld/health", self.nodes[index].public);
        loop {
            if let Ok(response) = self.http.get(&url).send().await {
                if response.status().is_success() {
                    return Ok(());
                }
            }
            if let Some(child) = self.nodes[index].child.as_mut() {
                if let Some(status) = child.try_wait()? {
                    bail!(
                        "node {index} exited with {status}; see {}",
                        self.nodes[index].log.display()
                    );
                }
            }
            if Instant::now() >= deadline {
                bail!(
                    "node {index} was not ready within 90 s; see {}",
                    self.nodes[index].log.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop every node with SIGTERM, then SIGKILL after 30 s.
    pub async fn stop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                if let Some(pid) = child.id() {
                    let _ = std::process::Command::new("kill")
                        .args(["-TERM", &pid.to_string()])
                        .status();
                }
                if tokio::time::timeout(Duration::from_secs(30), child.wait())
                    .await
                    .is_err()
                {
                    let _ = child.kill().await;
                }
            }
            node.pid = None;
        }
    }

    /// Send `signal` to one node. `KILL` and `TERM` also reap it.
    pub async fn signal(&mut self, index: usize, signal: &str) -> anyhow::Result<()> {
        let node = self
            .nodes
            .get_mut(index)
            .ok_or_else(|| anyhow!("no node {index}"))?;
        let pid = node
            .pid
            .ok_or_else(|| anyhow!("node {index} is not running"))?;
        let status = std::process::Command::new("kill")
            .args([&format!("-{signal}"), &pid.to_string()])
            .status()?;
        if !status.success() {
            bail!("kill -{signal} {pid} failed");
        }
        if matches!(signal, "KILL" | "TERM") {
            if let Some(mut child) = node.child.take() {
                let _ = tokio::time::timeout(Duration::from_secs(30), child.wait()).await;
            }
            node.pid = None;
        }
        Ok(())
    }

    /// Start one stopped node again on its own ports.
    pub async fn start_node(&mut self, index: usize, wipe_local: bool) -> anyhow::Result<()> {
        anyhow::ensure!(index < self.nodes.len(), "no node {index}");
        anyhow::ensure!(
            self.nodes[index].pid.is_none(),
            "node {index} is still running"
        );
        if wipe_local && self.nodes[index].state.exists() {
            std::fs::remove_dir_all(&self.nodes[index].state)?;
        }
        self.launch(index).await?;
        self.wait_ready(index).await
    }

    /// Deploy the fixture as a new version, and optionally make every node
    /// load it now (`POST /reload`).
    pub async fn redeploy(&mut self, reload: bool) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(self.options.backend, Backend::S3 { .. }),
            "redeploy needs the s3 backend; `celld dev` restarts its node to deploy"
        );
        // A new comment is a new bundle, so a new version.
        let main = self.project.join("index.js");
        let mut source = std::fs::read_to_string(&main)?;
        source.push_str(&format!(
            "\n// celld-perf redeploy {}\n",
            crate::load::now_us()
        ));
        std::fs::write(&main, source)?;
        self.deploy().await?;
        if reload {
            for node in &self.nodes {
                if node.pid.is_some() {
                    self.http
                        .post(format!("{}/reload", node.internal))
                        .send()
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn restart(&mut self, wipe_local: bool) -> anyhow::Result<()> {
        self.stop().await;
        if wipe_local {
            match self.options.backend {
                Backend::Dev => {
                    let runtime = self.project.join(".celld/dev/runtime");
                    if runtime.exists() {
                        std::fs::remove_dir_all(&runtime)?;
                    }
                }
                Backend::S3 { .. } => {
                    for node in &self.nodes {
                        if node.state.exists() {
                            std::fs::remove_dir_all(&node.state)?;
                        }
                    }
                }
            }
        }
        for index in 0..self.nodes.len() {
            self.launch(index).await?;
        }
        for index in 0..self.nodes.len() {
            self.wait_ready(index).await?;
        }
        Ok(())
    }

    pub async fn metrics(&self, node: &Node) -> anyhow::Result<Value> {
        self.internal_json(node, "/debug/metrics").await
    }

    pub async fn state(&self, node: &Node) -> anyhow::Result<Value> {
        self.internal_json(node, "/state").await
    }

    async fn internal_json(&self, node: &Node, path: &str) -> anyhow::Result<Value> {
        let response = self
            .http
            .get(format!("{}{path}", node.internal))
            .send()
            .await
            .with_context(|| format!("GET {path} on node {}", node.index))?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            bail!(
                "GET {path} on node {} answered {status}: {text}",
                node.index
            );
        }
        Ok(serde_json::from_str(&text)?)
    }

    /// Ask every node to evict each cell it holds resident. Returns how
    /// many evictions were asked for.
    pub async fn evict_all(&self) -> anyhow::Result<usize> {
        let mut asked = 0;
        for node in &self.nodes {
            let state = self.state(node).await?;
            let residents: Vec<String> = state["residents"]
                .as_array()
                .map(|cells| {
                    cells
                        .iter()
                        .filter_map(|cell| cell.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            for cell in residents {
                let _ = self
                    .http
                    .post(format!("{}/evict/{cell}", node.internal))
                    .send()
                    .await?;
                asked += 1;
            }
        }
        Ok(asked)
    }

    /// Fail if a node the harness did not stop has exited, naming its
    /// exit status and, when it fenced itself, the fence.
    pub fn ensure_alive(&mut self, during: &str) -> anyhow::Result<()> {
        for node in &mut self.nodes {
            let Some(child) = node.child.as_mut() else {
                continue;
            };
            if let Some(status) = child.try_wait()? {
                node.child = None;
                node.pid = None;
                let log = std::fs::read_to_string(&node.log).unwrap_or_default();
                let fence = log
                    .lines()
                    .rev()
                    .find(|line| line.contains("SELF-FENCE"))
                    .map(|line| format!("; it fenced itself: {line}"))
                    .unwrap_or_default();
                bail!(
                    "node {} exited during {during} with {status}{fence}; see {}",
                    node.index,
                    node.log.display()
                );
            }
        }
        Ok(())
    }

    /// The fleet's network, when the scenario asked for one.
    pub fn network(&self) -> Option<&Arc<Network>> {
        self.network.as_ref()
    }

    /// Wait up to `timeout` for a node to exit on its own (a node cut off
    /// from the bucket must fence itself). Returns the seconds waited and
    /// its exit status, and leaves the node stopped for a later `start`.
    pub async fn await_exit(
        &mut self,
        index: usize,
        timeout: Duration,
    ) -> anyhow::Result<(f64, String)> {
        let node = self
            .nodes
            .get_mut(index)
            .ok_or_else(|| anyhow!("no node {index}"))?;
        let mut child = node
            .child
            .take()
            .ok_or_else(|| anyhow!("node {index} is not running"))?;
        let started = Instant::now();
        match tokio::time::timeout(timeout, child.wait()).await {
            Ok(status) => {
                node.pid = None;
                Ok((started.elapsed().as_secs_f64(), status?.to_string()))
            }
            Err(_) => {
                node.child = Some(child);
                bail!(
                    "node {index} was still running after {} s",
                    timeout.as_secs_f64()
                )
            }
        }
    }

    /// The node's pid, if it runs.
    pub fn running(&self, index: usize) -> bool {
        self.nodes.get(index).is_some_and(|node| node.pid.is_some())
    }

    pub fn publics(&self) -> Vec<String> {
        self.nodes.iter().map(|node| node.public.clone()).collect()
    }

    pub fn backend(&self) -> &Backend {
        &self.options.backend
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for proxy in &self.proxies {
            proxy.abort();
        }
        // `kill_on_drop` covers the children; `celld dev`'s own node is a
        // grandchild, so stop it by pid too.
        for node in &self.nodes {
            if let Some(pid) = node.pid {
                let _ = std::process::Command::new("kill")
                    .args(["-KILL", &pid.to_string()])
                    .status();
            }
        }
    }
}
