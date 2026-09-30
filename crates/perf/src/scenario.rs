// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The scenario file format. A scenario is JSON (see `crates/perf/scenarios`
//! and docs/performance-tests.md): the nodes to start, setup steps, timed
//! phases of open-loop load, and checks on what the nodes counted.

use anyhow::{bail, Context as _};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Short id, e.g. `S2-warm-read`.
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// A directory under `crates/perf/fixtures`.
    #[serde(default = "default_fixture")]
    pub fixture: String,
    /// Nodes to start. The `dev` backend runs exactly one.
    #[serde(default = "one")]
    pub nodes: usize,
    /// Extra environment for every node (`CELLD_*` tunables, perf faults).
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Environment for one node, by index, over `env`: one slow follower.
    #[serde(default)]
    pub node_env: BTreeMap<usize, BTreeMap<String, String>>,
    /// Run the whole scenario once per variant, each named
    /// `<name>[<variant>]`, with the variant's overrides.
    #[serde(default)]
    pub variants: Vec<Variant>,
    /// Long or large: `all` skips it unless asked by name.
    #[serde(default)]
    pub heavy: bool,
    /// Route peer and bucket traffic through the harness's proxies, so the
    /// `net` step can fault it (s3 backend).
    #[serde(default)]
    pub network: bool,
    /// Backends this scenario is meaningful on; empty means any.
    #[serde(default)]
    pub backends: Vec<String>,
    #[serde(default)]
    pub setup: Vec<Step>,
    pub phases: Vec<Phase>,
    /// Steps after the last phase, before the verification sweep; their
    /// results (a `collect`, say) are part of the scenario's result.
    #[serde(default)]
    pub after: Vec<Step>,
    /// Run the verification sweep after the last phase.
    #[serde(default = "yes")]
    pub verify: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Variant {
    pub name: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub nodes: Option<usize>,
}

impl Scenario {
    /// One scenario per variant, or this one alone.
    pub fn expand_variants(&self) -> Vec<Scenario> {
        if self.variants.is_empty() {
            return vec![self.clone()];
        }
        self.variants
            .iter()
            .map(|variant| {
                let mut scenario = self.clone();
                scenario.name = format!("{}[{}]", self.name, variant.name);
                scenario.env.extend(variant.env.clone());
                if let Some(nodes) = variant.nodes {
                    scenario.nodes = nodes;
                }
                scenario.variants.clear();
                scenario
            })
            .collect()
    }
}

fn default_fixture() -> String {
    "bench".into()
}

fn one() -> usize {
    1
}

fn yes() -> bool {
    true
}

/// A step between phases or before the first. Steps are not timed.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum Step {
    /// Send a signal to one node: `KILL` or `TERM` stops it (a later
    /// `start` brings it back), `STOP` freezes it and `CONT` thaws it.
    Signal {
        node: usize,
        #[serde(default = "default_signal")]
        signal: String,
    },
    /// Start a node that a `signal` stopped, on its old ports.
    /// `wipe_local` deletes its local state first (a disk loss).
    Start {
        node: usize,
        #[serde(default)]
        wipe_local: bool,
    },
    /// Deploy the fixture again as a new version (s3 backend). With
    /// `reload`, ask every node to pick it up now instead of at its poll.
    Redeploy {
        #[serde(default)]
        reload: bool,
    },
    /// Fault the network (needs `"network": true`): the traffic `from` one
    /// endpoint `to` another (`node:N`, `nodes`, `bucket`, `client`,
    /// `any`), and with `both` the reverse too. It replaces an earlier rule
    /// on the same pair; a rule with no fault lifts it.
    Net {
        from: String,
        to: String,
        #[serde(default)]
        both: bool,
        #[serde(default)]
        delay_ms: f64,
        #[serde(default)]
        jitter_ms: f64,
        #[serde(default)]
        kbps: Option<f64>,
        #[serde(default)]
        reset: f64,
        /// `blackhole` or `reject`.
        #[serde(default)]
        partition: Option<String>,
    },
    /// Lift every network fault.
    NetClear {},
    /// Wait for a node to exit on its own, as a node cut off from the
    /// bucket must; fails if it is still running after `timeout_s`.
    AwaitExit {
        node: usize,
        #[serde(default = "default_exit_timeout")]
        timeout_s: f64,
    },
    /// Send one request to every cell of `cells`, `concurrency` at a time.
    Touch {
        request: Request,
        cells: Cells,
        #[serde(default = "default_touch_concurrency")]
        concurrency: usize,
    },
    Sleep {
        ms: u64,
    },
    /// Open WebSockets and keep them for later phases.
    Connect {
        cells: Cells,
        /// Sockets per cell.
        #[serde(default = "one")]
        per_cell: usize,
        /// New connections per second.
        #[serde(default = "default_connect_rate")]
        rate: f64,
    },
    /// Stop every node and start it again. `wipe_local` also deletes the
    /// nodes' local state, so every cell restores from the bucket.
    Restart {
        #[serde(default)]
        wipe_local: bool,
    },
    /// Ask each node to evict every cell it holds resident
    /// (`/state` residents, then `POST /evict/<cell>`).
    EvictAll {},
    /// Send `request` once (or once per cell) and report the sum and the
    /// max of every numeric field in the JSON answers.
    Collect {
        request: Request,
        #[serde(default)]
        cells: Option<Cells>,
        #[serde(default = "default_touch_concurrency")]
        concurrency: usize,
    },
}

fn default_exit_timeout() -> f64 {
    60.0
}

fn default_signal() -> String {
    "KILL".into()
}

fn default_touch_concurrency() -> usize {
    64
}

fn default_connect_rate() -> f64 {
    500.0
}

/// A step at `at_s` seconds into a phase.
#[derive(Clone, Debug, Deserialize)]
pub struct Timed {
    pub at_s: f64,
    #[serde(flatten)]
    pub step: Step,
}

/// One timed phase at one offered rate.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Phase {
    pub name: String,
    /// Steps before this phase (after the previous one).
    #[serde(default)]
    pub before: Vec<Step>,
    /// Steps while this phase runs, each at its offset: a node killed
    /// mid-load, a deploy under load.
    #[serde(default)]
    pub during: Vec<Timed>,
    pub duration_s: f64,
    /// Offered requests (or messages) per second. With `rates`, one phase
    /// runs at each rate, named `<name>@<rate>`.
    #[serde(default)]
    pub rate: Option<f64>,
    #[serde(default)]
    pub rates: Vec<f64>,
    #[serde(default)]
    pub arrival: Arrival,
    /// A warm-up phase runs but is not reported or checked.
    #[serde(default)]
    pub warmup: bool,
    pub load: Vec<Load>,
    #[serde(default)]
    pub checks: Vec<Check>,
    /// Requests allowed in flight before the generator counts the rest as
    /// shed by the client.
    #[serde(default = "default_max_inflight")]
    pub max_inflight: usize,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_max_inflight() -> usize {
    20_000
}

fn default_timeout_ms() -> u64 {
    30_000
}

impl Phase {
    /// The phases this entry runs, one per rate.
    pub fn expand(&self) -> anyhow::Result<Vec<(String, f64)>> {
        match (self.rate, self.rates.is_empty()) {
            (Some(rate), true) => Ok(vec![(self.name.clone(), rate)]),
            (None, false) => Ok(self
                .rates
                .iter()
                .map(|rate| (format!("{}@{rate}", self.name), *rate))
                .collect()),
            _ => bail!("phase {}: give exactly one of rate or rates", self.name),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Arrival {
    /// Evenly spaced.
    #[default]
    Uniform,
    /// Exponential gaps: bursts, as independent clients produce.
    Poisson,
}

/// One weighted part of a phase's load. (`deny_unknown_fields` does not
/// combine with `flatten`, so a misspelled field here is ignored.)
#[derive(Clone, Debug, Deserialize)]
pub struct Load {
    #[serde(default = "one_f")]
    pub weight: f64,
    /// A label for this part's latency in the result; defaults to the path.
    #[serde(default)]
    pub label: Option<String>,
    #[serde(flatten)]
    pub target: Target,
}

fn one_f() -> f64 {
    1.0
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum Target {
    Http {
        request: Request,
        #[serde(default)]
        cells: Option<Cells>,
        /// Send every request to this node instead of a random one.
        #[serde(default)]
        node: Option<usize>,
    },
    /// One message on a socket opened by an earlier `connect` step.
    WebSocket { message: WsMessage },
}

impl Load {
    pub fn label(&self) -> String {
        if let Some(label) = &self.label {
            return label.clone();
        }
        match &self.target {
            Target::Http { request, .. } => request.path.clone(),
            Target::WebSocket { message } => format!("ws:{message:?}").to_lowercase(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub path: String,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    /// This request increments the cell's write count on success, so the
    /// verification sweep counts it.
    #[serde(default)]
    pub counts_write: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WsMessage {
    /// Echoed by the cell without a write.
    Echo,
    /// Written, then echoed.
    Write,
    /// Sent to every socket on the cell.
    Broadcast,
    /// Answered by the node's auto-response, without waking the cell.
    Ping,
}

/// A set of cell names and how requests pick among them.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cells {
    pub count: usize,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default)]
    pub distribution: Distribution,
    /// Zipf exponent.
    #[serde(default = "default_zipf")]
    pub zipf_s: f64,
    /// For `shifting`: the window of cells in use at once, and how many
    /// cells per second it advances.
    #[serde(default)]
    pub window: Option<usize>,
    #[serde(default)]
    pub shift_per_s: f64,
}

fn default_prefix() -> String {
    "cell".into()
}

fn default_zipf() -> f64 {
    0.99
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Distribution {
    #[default]
    Uniform,
    Zipf,
    /// A uniform window that slides through the cells.
    Shifting,
}

/// A check on one phase's results.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Check {
    /// What to measure; see [`crate::check`] for the forms.
    pub metric: String,
    /// Divide by the phase's successful requests.
    #[serde(default)]
    pub per_ok: bool,
    /// Divide by another metric of the same phase instead, e.g. bucket
    /// LISTs per `hist_count:activation.download_us`.
    #[serde(default)]
    pub per: Option<String>,
    /// Divide by the phase's length in seconds.
    #[serde(default)]
    pub per_second: bool,
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default)]
    pub max: Option<f64>,
    /// A timing check. It is reported, and fails the run only with
    /// `--enforce-timing`.
    #[serde(default)]
    pub timing: bool,
}

impl Scenario {
    pub fn load(path: &Path) -> anyhow::Result<Scenario> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read scenario {}", path.display()))?;
        let scenario: Scenario = serde_json::from_str(&text)
            .with_context(|| format!("parse scenario {}", path.display()))?;
        scenario.validate()?;
        Ok(scenario)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.nodes == 0 {
            bail!("{}: nodes must be at least 1", self.name);
        }
        for phase in &self.phases {
            phase.expand()?;
            if phase.load.is_empty() {
                bail!("{}: phase {} has no load", self.name, phase.name);
            }
            if phase.load.iter().any(|load| load.weight <= 0.0) {
                bail!("{}: phase {} has a weight <= 0", self.name, phase.name);
            }
        }
        Ok(())
    }
}
