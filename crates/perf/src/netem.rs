// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Network faults between the nodes of a fleet, and between each node and
//! the bucket.
//!
//! With `"network": true` the harness starts a TCP proxy in front of every
//! node's internal listener and has each node advertise the proxy instead.
//! It also gives each node a proxy of its own in front of the bucket
//! endpoint. Every byte between two nodes, and between a node and the
//! bucket, then passes through [`Network`]. Rules set by the `net` step can
//! delay it, limit its bandwidth, reset new connections, or partition the
//! link, and they can change at any moment of a phase. The harness's own
//! requests to a node's internal listener go direct, so measuring a fault
//! never passes through it.
//!
//! A rule names a direction: `from` the endpoint that sends the bytes `to`
//! the endpoint that receives them. The request half of a connection is
//! `(initiator, acceptor)` and the response half `(acceptor, initiator)`,
//! so a one-way partition leaves the other way open, as a failed route
//! does. The proxy learns which node opened a peer connection from the
//! first request's `x-cells-peer-source` header, which every signed peer
//! request carries. A bucket proxy serves one node, so it knows.
//!
//! Faults:
//!
//! - `delay_ms`, `jitter_ms`: each chunk waits the delay plus a uniform
//!   jitter, without reordering, so a symmetric delay of `d` adds `2d` to a
//!   round trip;
//! - `kbps`: a bandwidth limit per connection and direction;
//! - `reset`: the chance a new connection is closed as soon as it opens;
//! - `partition`: `blackhole` holds every byte, and a new connection hangs,
//!   until the rule is lifted, as a lost route does; `reject` closes open
//!   connections and refuses new ones, as a dead host does.
//!
//! This is TCP-level emulation: it cannot drop a single packet, so packet
//! loss shows as latency (with `jitter_ms`) or as resets.

use anyhow::{anyhow, bail};
use rand::Rng;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

/// One end of a link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Endpoint {
    Node(usize),
    Bucket,
    /// Anything that is not a node: the harness, a stray client.
    Client,
}

impl Endpoint {
    pub fn label(self) -> String {
        match self {
            Endpoint::Node(index) => format!("node:{index}"),
            Endpoint::Bucket => "bucket".into(),
            Endpoint::Client => "client".into(),
        }
    }
}

/// Which endpoints a rule applies to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selector {
    Node(usize),
    /// Every node.
    Nodes,
    Bucket,
    Client,
    Any,
}

impl Selector {
    pub fn parse(text: &str) -> anyhow::Result<Selector> {
        Ok(match text {
            "nodes" => Selector::Nodes,
            "bucket" => Selector::Bucket,
            "client" => Selector::Client,
            "any" => Selector::Any,
            _ => match text.strip_prefix("node:") {
                Some(index) => Selector::Node(
                    index
                        .parse()
                        .map_err(|_| anyhow!("bad node index in {text:?}"))?,
                ),
                None => bail!("unknown endpoint {text:?}: node:N, nodes, bucket, client, or any"),
            },
        })
    }

    fn matches(&self, endpoint: Endpoint) -> bool {
        match (self, endpoint) {
            (Selector::Any, _) => true,
            (Selector::Nodes, Endpoint::Node(_)) => true,
            (Selector::Node(want), Endpoint::Node(index)) => *want == index,
            (Selector::Bucket, Endpoint::Bucket) => true,
            (Selector::Client, Endpoint::Client) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Partition {
    Blackhole,
    Reject,
}

impl Partition {
    pub fn parse(text: &str) -> anyhow::Result<Partition> {
        match text {
            "blackhole" => Ok(Partition::Blackhole),
            "reject" => Ok(Partition::Reject),
            _ => bail!("partition is blackhole or reject, not {text:?}"),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Fault {
    pub delay_ms: f64,
    pub jitter_ms: f64,
    pub kbps: Option<f64>,
    pub reset: f64,
    pub partition: Option<Partition>,
}

impl Fault {
    /// Two rules on one direction: delays add, the tighter bandwidth and
    /// the likelier reset win, and a reject outranks a blackhole.
    fn combine(&mut self, other: &Fault) {
        self.delay_ms += other.delay_ms;
        self.jitter_ms += other.jitter_ms;
        self.kbps = match (self.kbps, other.kbps) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.reset = self.reset.max(other.reset);
        self.partition = self.partition.max(other.partition);
    }

    fn to_json(&self) -> Value {
        json!({
            "delay_ms": self.delay_ms,
            "jitter_ms": self.jitter_ms,
            "kbps": self.kbps,
            "reset": self.reset,
            "partition": self.partition.map(|partition| match partition {
                Partition::Blackhole => "blackhole",
                Partition::Reject => "reject",
            }),
        })
    }
}

struct Rule {
    from: Selector,
    to: Selector,
    fault: Fault,
}

#[derive(Default, Clone, Copy)]
struct LinkStats {
    connections: u64,
    resets: u64,
    bytes: u64,
}

pub struct Network {
    rules: RwLock<Vec<Rule>>,
    /// Bumped on every rule change, so held bytes can re-check their rule.
    changed: watch::Sender<u64>,
    links: Mutex<BTreeMap<(Endpoint, Endpoint), LinkStats>>,
    /// Node names by index, to read a peer connection's source.
    names: Vec<String>,
}

impl Network {
    pub fn new(names: Vec<String>) -> Arc<Network> {
        Arc::new(Network {
            rules: RwLock::default(),
            changed: watch::channel(0).0,
            links: Mutex::default(),
            names,
        })
    }

    /// Set the fault for `from` → `to`, replacing a rule on the same pair.
    /// With `both`, the reverse direction gets the same fault.
    pub fn set(&self, from: Selector, to: Selector, fault: Fault, both: bool) {
        {
            let mut rules = self.rules.write().unwrap();
            let mut place = |from: Selector, to: Selector| {
                rules.retain(|rule| !(rule.from == from && rule.to == to));
                rules.push(Rule {
                    from,
                    to,
                    fault: fault.clone(),
                });
            };
            if both {
                place(to.clone(), from.clone());
            }
            place(from, to);
        }
        self.changed.send_modify(|generation| *generation += 1);
    }

    /// Remove every rule.
    pub fn clear(&self) {
        self.rules.write().unwrap().clear();
        self.changed.send_modify(|generation| *generation += 1);
    }

    /// The combined fault on one direction now.
    pub fn fault(&self, from: Endpoint, to: Endpoint) -> Fault {
        let mut fault = Fault::default();
        for rule in self.rules.read().unwrap().iter() {
            if rule.from.matches(from) && rule.to.matches(to) {
                fault.combine(&rule.fault);
            }
        }
        fault
    }

    /// The rules in force, for a result.
    pub fn rules_json(&self) -> Value {
        Value::Array(
            self.rules
                .read()
                .unwrap()
                .iter()
                .map(|rule| {
                    json!({
                        "from": format!("{:?}", rule.from).to_lowercase(),
                        "to": format!("{:?}", rule.to).to_lowercase(),
                        "fault": rule.fault.to_json(),
                    })
                })
                .collect(),
        )
    }

    /// Per-link counters so far: connections opened by `from` to `to`,
    /// connections the proxy reset, and bytes sent `from` → `to`.
    pub fn stats(&self) -> BTreeMap<String, Value> {
        self.links
            .lock()
            .unwrap()
            .iter()
            .map(|((from, to), stats)| {
                (
                    format!("{}>{}", from.label(), to.label()),
                    json!({
                        "connections": stats.connections,
                        "resets": stats.resets,
                        "bytes": stats.bytes,
                    }),
                )
            })
            .collect()
    }

    fn count(&self, from: Endpoint, to: Endpoint, update: impl FnOnce(&mut LinkStats)) {
        update(self.links.lock().unwrap().entry((from, to)).or_default());
    }

    fn endpoint_named(&self, name: &str) -> Endpoint {
        self.names
            .iter()
            .position(|known| known == name)
            .map_or(Endpoint::Client, Endpoint::Node)
    }

    /// Wait while `from` → `to` is blackholed. Returns false if it became
    /// a reject instead.
    async fn wait_open(&self, from: Endpoint, to: Endpoint) -> bool {
        let mut changed = self.changed.subscribe();
        loop {
            match self.fault(from, to).partition {
                None => return true,
                Some(Partition::Reject) => return false,
                Some(Partition::Blackhole) => {
                    if changed.changed().await.is_err() {
                        return false;
                    }
                }
            }
        }
    }

    /// Serve `listener`, forwarding every connection to `target`, which is
    /// the endpoint `to`. `from` is fixed for a bucket proxy; for a node's
    /// proxy it is read from each connection's first request.
    pub fn serve(
        self: &Arc<Self>,
        listener: TcpListener,
        target: SocketAddr,
        to: Endpoint,
        from: Option<Endpoint>,
    ) -> tokio::task::JoinHandle<()> {
        let network = self.clone();
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let network = network.clone();
                tokio::spawn(async move {
                    let _ = network.connection(inbound, target, to, from).await;
                });
            }
        })
    }

    async fn connection(
        self: Arc<Self>,
        mut inbound: TcpStream,
        target: SocketAddr,
        to: Endpoint,
        from: Option<Endpoint>,
    ) -> std::io::Result<()> {
        inbound.set_nodelay(true)?;
        let (from, head) = match from {
            Some(from) => (from, Vec::new()),
            None => {
                let head = read_head(&mut inbound).await;
                (
                    self.endpoint_named(&peer_source(&head).unwrap_or_default()),
                    head,
                )
            }
        };
        let fault = self.fault(from, to);
        let reset = fault.reset > 0.0 && rand::thread_rng().gen_bool(fault.reset.min(1.0));
        if reset || fault.partition == Some(Partition::Reject) {
            self.count(from, to, |stats| stats.resets += 1);
            return Ok(());
        }
        // A new connection into a blackhole hangs until the route returns.
        if !self.wait_open(from, to).await {
            self.count(from, to, |stats| stats.resets += 1);
            return Ok(());
        }
        let outbound = TcpStream::connect(target).await?;
        outbound.set_nodelay(true)?;
        self.count(from, to, |stats| stats.connections += 1);
        let (inbound_read, inbound_write) = inbound.into_split();
        let (outbound_read, outbound_write) = outbound.into_split();
        let (closed, _) = watch::channel(false);
        let up = self
            .clone()
            .pump(inbound_read, outbound_write, from, to, head, closed.clone());
        let down = self
            .clone()
            .pump(outbound_read, inbound_write, to, from, Vec::new(), closed);
        let _ = tokio::join!(up, down);
        Ok(())
    }

    /// Carry one direction of a connection, `from` → `to`, applying the
    /// fault in force as each chunk arrives and again as it leaves.
    async fn pump(
        self: Arc<Self>,
        mut reader: tokio::net::tcp::OwnedReadHalf,
        mut writer: tokio::net::tcp::OwnedWriteHalf,
        from: Endpoint,
        to: Endpoint,
        head: Vec<u8>,
        closed: watch::Sender<bool>,
    ) {
        let (chunks, mut ready) = mpsc::channel::<(Instant, Vec<u8>)>(64);
        let network = self.clone();
        let mut stop = closed.subscribe();
        let read = async move {
            let mut last = Instant::now();
            let mut schedule = |bytes: Vec<u8>| {
                let fault = network.fault(from, to);
                let jitter = if fault.jitter_ms > 0.0 {
                    rand::thread_rng().gen_range(0.0..fault.jitter_ms)
                } else {
                    0.0
                };
                let mut at = Instant::now()
                    + Duration::from_secs_f64((fault.delay_ms + jitter).max(0.0) / 1000.0);
                if let Some(kbps) = fault.kbps.filter(|kbps| *kbps > 0.0) {
                    let serialize = bytes.len() as f64 * 8.0 / (kbps * 1000.0);
                    at = at.max(last + Duration::from_secs_f64(serialize));
                }
                // Never reorder: a chunk leaves no earlier than the last.
                at = at.max(last);
                last = at;
                (at, bytes)
            };
            if !head.is_empty() && chunks.send(schedule(head)).await.is_err() {
                return;
            }
            let mut buffer = vec![0u8; 64 * 1024];
            loop {
                let read = tokio::select! {
                    read = reader.read(&mut buffer) => read,
                    _ = stop.changed() => return,
                };
                match read {
                    Ok(0) | Err(_) => return,
                    Ok(count) => {
                        if chunks
                            .send(schedule(buffer[..count].to_vec()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        };
        let network = self.clone();
        let write = async move {
            while let Some((at, bytes)) = ready.recv().await {
                if !network.wait_open(from, to).await {
                    break;
                }
                tokio::time::sleep_until(at).await;
                if writer.write_all(&bytes).await.is_err() {
                    break;
                }
                network.count(from, to, |stats| stats.bytes += bytes.len() as u64);
            }
            let _ = writer.shutdown().await;
        };
        tokio::join!(read, write);
        // One direction ending (a reject, an error, a close) ends the other.
        let _ = closed.send(true);
    }
}

/// The start of a connection, up to the end of its first request's
/// headers: at most 16 KiB, and whatever arrived within two seconds.
async fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    let mut buffer = [0u8; 4096];
    let deadline = Instant::now() + Duration::from_secs(2);
    while head.len() < 16 * 1024 && !head.windows(4).any(|window| window == b"\r\n\r\n") {
        match tokio::time::timeout_at(deadline, stream.read(&mut buffer)).await {
            Ok(Ok(count)) if count > 0 => head.extend_from_slice(&buffer[..count]),
            _ => break,
        }
    }
    head
}

/// The `x-cells-peer-source` header of a request head.
fn peer_source(head: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(head);
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case("x-cells-peer-source")
            .then(|| value.trim().to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_select_and_combine() {
        let network = Network::new(vec!["perf-0".into(), "perf-1".into()]);
        network.set(
            Selector::Nodes,
            Selector::Nodes,
            Fault {
                delay_ms: 5.0,
                ..Fault::default()
            },
            false,
        );
        network.set(
            Selector::Node(1),
            Selector::Any,
            Fault {
                delay_ms: 10.0,
                partition: Some(Partition::Blackhole),
                ..Fault::default()
            },
            true,
        );
        let fault = network.fault(Endpoint::Node(1), Endpoint::Node(0));
        assert_eq!(fault.delay_ms, 15.0);
        assert_eq!(fault.partition, Some(Partition::Blackhole));
        // `both` made the reverse rule too.
        assert_eq!(
            network.fault(Endpoint::Bucket, Endpoint::Node(1)).partition,
            Some(Partition::Blackhole)
        );
        assert_eq!(
            network.fault(Endpoint::Node(0), Endpoint::Bucket),
            Fault::default()
        );
        network.clear();
        assert_eq!(
            network.fault(Endpoint::Node(1), Endpoint::Node(0)),
            Fault::default()
        );
        assert_eq!(Selector::parse("node:3").unwrap(), Selector::Node(3));
        assert!(Selector::parse("host:3").is_err());
    }

    #[test]
    fn a_peer_request_names_its_source() {
        let head = b"GET /peer/tunnel HTTP/1.1\r\nhost: x\r\nX-Cells-Peer-Source: perf-1\r\n\r\n";
        assert_eq!(peer_source(head).as_deref(), Some("perf-1"));
        assert_eq!(peer_source(b"GET / HTTP/1.1\r\n\r\n"), None);
        let network = Network::new(vec!["perf-0".into(), "perf-1".into()]);
        assert_eq!(network.endpoint_named("perf-1"), Endpoint::Node(1));
        assert_eq!(network.endpoint_named("other"), Endpoint::Client);
    }

    async fn echo_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buffer = [0u8; 1024];
                    while let Ok(count) = stream.read(&mut buffer).await {
                        if count == 0 || stream.write_all(&buffer[..count]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        address
    }

    async fn round_trip(address: SocketAddr) -> std::io::Result<Duration> {
        let mut stream = TcpStream::connect(address).await?;
        let started = Instant::now();
        stream.write_all(b"ping").await?;
        let mut buffer = [0u8; 4];
        stream.read_exact(&mut buffer).await?;
        Ok(started.elapsed())
    }

    #[tokio::test]
    async fn the_proxy_delays_holds_and_refuses() {
        let target = echo_server().await;
        let network = Network::new(vec!["perf-0".into()]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        network.serve(listener, target, Endpoint::Node(0), Some(Endpoint::Client));

        let clean = round_trip(proxy).await.unwrap();
        assert!(clean < Duration::from_millis(50), "{clean:?}");

        network.set(
            Selector::Client,
            Selector::Node(0),
            Fault {
                delay_ms: 40.0,
                ..Fault::default()
            },
            true,
        );
        let delayed = round_trip(proxy).await.unwrap();
        assert!(delayed >= Duration::from_millis(80), "{delayed:?}");

        // A blackhole holds the exchange until it is lifted.
        network.set(
            Selector::Client,
            Selector::Node(0),
            Fault {
                partition: Some(Partition::Blackhole),
                ..Fault::default()
            },
            true,
        );
        let held = tokio::spawn(round_trip(proxy));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!held.is_finished());
        network.clear();
        let waited = held.await.unwrap().unwrap();
        assert!(waited >= Duration::from_millis(250), "{waited:?}");

        // A reject closes a new connection at once.
        network.set(
            Selector::Client,
            Selector::Node(0),
            Fault {
                partition: Some(Partition::Reject),
                ..Fault::default()
            },
            false,
        );
        assert!(round_trip(proxy).await.is_err());
        let stats = network.stats();
        assert_eq!(stats["client>node:0"]["resets"], 1);
        assert_eq!(stats["client>node:0"]["connections"], 3);
    }
}
