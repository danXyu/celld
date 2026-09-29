//! `celld export reconcile | verify | erase`.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _};
use celld_export_format::StreamId;
use serde_json::json;

use super::inventory::Inventory;
use super::reconcile::{reconcile, Options};
use super::tombstone::{self, Tombstone};
use super::verify::{self, Verdict};
use super::{emit, BucketConsumer, ConsumerView};
use crate::bucket::Bucket;
use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output, Record};
use crate::export::Config;
use crate::note;

/// How old a difference must be before the reconciler counts it.
const DEFAULT_SETTLE: Duration = Duration::from_secs(60 * 60);
const DEFAULT_SAMPLE: usize = 10;

const HELP: &str = "celld export reconcile | verify | erase

  reconcile           Compare every cell's head in the bucket with what the
                      consumer certified; record and export the differences
    --settle DUR      Only count differences older than this (default 1h)
    --dry-run         Report only; write no records and no findings
    --schedule        Run every CELLD_EXPORT_RECONCILE (default 24h), forever

  verify              Restore streams at their head and compare every table
    --sample N        How many streams to pick at random (default 10)
    --cell SCOPE      Verify this cell instead of a sample
    --facet PATH      With --cell: verify this facet of it

  erase               Tombstone a stream so no path exports it again
    --cell SCOPE      The root cell scope, Class:id (required)
    --script NAME     The script; default: every script the consumer holds
                      for the cell
    --facet PATH      Erase only this facet; default: the root and every
                      facet the consumer holds for it
    --incarnation N   Erase only this incarnation; default: every one
    --reason TEXT     Recorded with the tombstone
    --clear           Clear the tombstones instead, so a stream recreated
                      under the same scope exports again

  --export-bucket B   Where the export writes, if not the fleet bucket
                      (or CELLD_EXPORT_BUCKET)
  --json              One JSON object per line
";

struct Command {
    fleet: FleetFlags,
    export_bucket: Option<String>,
    json: bool,
    settle: Duration,
    dry_run: bool,
    schedule: bool,
    sample: usize,
    cell: Option<String>,
    facet: Option<String>,
    script: Option<String>,
    incarnation: Option<u64>,
    reason: Option<String>,
    clear: bool,
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut command = Command {
            fleet: FleetFlags::default(),
            export_bucket: None,
            json: false,
            settle: DEFAULT_SETTLE,
            dry_run: false,
            schedule: false,
            sample: DEFAULT_SAMPLE,
            cell: None,
            facet: None,
            script: None,
            incarnation: None,
            reason: None,
            clear: false,
        };
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} needs a value"))
            };
            if command.fleet.consume(&argument, &mut value)? {
                continue;
            }
            match argument.as_str() {
                "--help" | "-h" => return Ok(None),
                "--export-bucket" => command.export_bucket = Some(value("--export-bucket")?),
                "--json" => command.json = true,
                "--settle" => {
                    command.settle = crate::export::parse_interval("--settle", &value("--settle")?)?
                }
                "--dry-run" => command.dry_run = true,
                "--schedule" => command.schedule = true,
                "--sample" => {
                    command.sample = value("--sample")?
                        .parse()
                        .context("--sample takes a count")?
                }
                "--cell" => command.cell = Some(value("--cell")?),
                "--facet" => command.facet = Some(value("--facet")?),
                "--script" => command.script = Some(value("--script")?),
                "--incarnation" => {
                    command.incarnation = Some(
                        value("--incarnation")?
                            .parse()
                            .context("--incarnation takes a number")?,
                    )
                }
                "--reason" => command.reason = Some(value("--reason")?),
                "--clear" => command.clear = true,
                other => bail!("unknown `celld export` flag: {other}"),
            }
        }
        if let Some(cell) = &command.cell {
            anyhow::ensure!(
                celld_logic::cell::valid_cell_scope(cell),
                "invalid cell scope {cell:?}"
            );
        }
        Ok(Some(command))
    }

    fn format(&self) -> Format {
        if self.json {
            Format::Json
        } else {
            Format::Text
        }
    }

    /// The fleet bucket, and the bucket the export writes to.
    async fn buckets(&self, name: &str) -> anyhow::Result<(Bucket, Bucket)> {
        let storage = self.fleet.clone().resolve(name)?;
        let fleet = storage.open().await?;
        let over = self
            .export_bucket
            .clone()
            .or_else(|| crate::env_vars::value("CELLD_EXPORT_BUCKET").ok().flatten());
        let export = match over {
            Some(bucket) => {
                crate::cli_options::Storage {
                    bucket,
                    endpoint: storage.endpoint.clone(),
                    region: storage.region.clone(),
                }
                .open()
                .await?
            }
            None => storage.open().await?,
        };
        Ok((fleet, export))
    }
}

/// The export settings as a node would read them, whether or not this
/// process has `CELLD_EXPORT` set.
fn config() -> anyhow::Result<Config> {
    Config::from_lookup(|name| {
        if name == "CELLD_EXPORT" {
            Ok(Some("1".into()))
        } else {
            crate::env_vars::value(name)
        }
    })?
    .context("export settings")
}

pub async fn run(command: &str, arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(parsed) = Command::parse(arguments)? else {
        return Output::new(Format::Text).help(HELP);
    };
    match command {
        "reconcile" => run_reconcile(parsed).await,
        "verify" => run_verify(parsed).await,
        "erase" => run_erase(parsed).await,
        "help" | "--help" | "-h" => Output::new(Format::Text).help(HELP),
        other => bail!("unknown `celld export` subcommand: {other}\n\n{HELP}{FLEET_HELP}"),
    }
}

struct Row(serde_json::Value, String);

impl Record for Row {
    fn json(&self) -> serde_json::Value {
        self.0.clone()
    }
    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.1)
    }
}

async fn run_reconcile(command: Command) -> anyhow::Result<()> {
    let config = config()?;
    let (fleet, export) = command.buckets("celld export reconcile").await?;
    loop {
        reconcile_once(&command, &config, &fleet, &export).await?;
        if !command.schedule {
            return Ok(());
        }
        note!("next reconcile in {:?}", config.reconcile);
        crate::asyncrt::sleep(config.reconcile).await;
    }
}

async fn reconcile_once(
    command: &Command,
    config: &Config,
    fleet: &Bucket,
    export: &Bucket,
) -> anyhow::Result<()> {
    let mut inventory = Inventory::new();
    for prefix in ["cells", "log"] {
        for object in fleet.list(prefix).await? {
            inventory.add(
                object.location.as_ref(),
                object.last_modified.timestamp_millis(),
            );
        }
    }
    let heads = inventory.heads().await;
    let tombstones = tombstone::load(export).await?;
    let consumer = BucketConsumer::load(export.clone()).await?;
    let streams = consumer.streams().await?;
    let recovered = consumer.recovered().await?;
    let broken = inventory.broken(&heads);
    let result = reconcile(
        &heads,
        &broken,
        inventory.losses(),
        &streams,
        &recovered,
        &tombstones,
        |class| config.exports_class(class),
        Options {
            now_ms: crate::asyncrt::wall_ms(),
            settle_ms: command.settle.as_millis() as i64,
        },
    );
    let mut out = Output::new(command.format());
    for finding in &result.findings {
        let text = format!(
            "{:<16} {} {}",
            finding.kind.as_str(),
            finding.scope,
            finding.detail
        );
        out.row(&Row(serde_json::to_value(finding)?, text))?;
    }
    out.finish()?;
    for session in &result.recovered_short {
        note!(
            "recovery of {} sent {} recovered record(s); the consumer holds {}",
            session.session,
            session.expected,
            session.held
        );
    }
    note!(
        "reconciled {} cell(s): {} finding(s), {} tombstoned, {} not exported, {} too recent",
        result.checked,
        result.findings.len(),
        result.tombstoned,
        result.not_exported,
        result.unsettled
    );
    if command.dry_run {
        return Ok(());
    }
    if let Some(key) = emit(export, result.records).await? {
        note!("wrote the reconciler's records to {key}");
    }
    consumer.record_findings(&result.findings).await
}

async fn run_verify(command: Command) -> anyhow::Result<()> {
    let config = config()?;
    let (fleet, export) = command.buckets("celld export verify").await?;
    let consumer = BucketConsumer::load(export.clone()).await?;
    let streams = consumer.streams().await?;
    let chosen = match &command.cell {
        Some(cell) => {
            let found: Vec<_> = streams
                .iter()
                .filter(|s| s.id.cell == *cell && s.id.facet == command.facet)
                .cloned()
                .collect();
            anyhow::ensure!(
                !found.is_empty(),
                "the consumer holds no stream for {cell}{}",
                command
                    .facet
                    .as_deref()
                    .map(|f| format!(" facet {f:?}"))
                    .unwrap_or_default()
            );
            found
        }
        None => verify::sample(&streams, command.sample, crate::asyncrt::wall_ms() as u64),
    };
    let mut out = Output::new(command.format());
    let mut drifted = 0;
    for stream in &chosen {
        let verdict = verify::verify(&fleet, &consumer, stream, |class, table| {
            config.denies_table(class, table)
        })
        .await?;
        drifted += usize::from(verdict.drifted());
        out.row(&Row(
            serde_json::to_value(&verdict)?,
            verdict_text(&verdict),
        ))?;
    }
    out.finish()?;
    note!("verified {} stream(s), {drifted} drifted", chosen.len());
    if drifted > 0 {
        bail!("{drifted} stream(s) differ from the cell");
    }
    Ok(())
}

fn verdict_text(v: &Verdict) -> String {
    let at = format!("e{}:{}", v.epoch, v.txid);
    match &v.outcome {
        verify::Outcome::Match { tables, rows } => {
            format!(
                "match   {} at {at}: {tables} table(s), {rows} row(s)",
                v.scope
            )
        }
        verify::Outcome::Drift { total, diffs, .. } => {
            let shown: Vec<String> = diffs
                .iter()
                .map(|d| format!("{:?} {} {:?}", d.kind, d.table, d.key))
                .collect();
            format!(
                "drift   {} at {at}: {total} difference(s): {}",
                v.scope,
                shown.join("; ")
            )
        }
        verify::Outcome::Behind { certified } => format!(
            "behind  {} at {at}: consumer certified {}",
            v.scope,
            certified.map_or("nothing".into(), |c| format!("e{}:{}", c.epoch, c.txid))
        ),
    }
}

async fn run_erase(command: Command) -> anyhow::Result<()> {
    let cell = command
        .cell
        .clone()
        .context("celld export erase needs --cell SCOPE")?;
    let (_, export) = command.buckets("celld export erase").await?;
    let class = super::inventory::class_of(&cell).to_string();
    let now = crate::asyncrt::wall_ms();

    let existing = tombstone::load(&export).await?;
    let targets: Vec<Tombstone> = if command.clear {
        existing
            .into_iter()
            .filter(|t| t.is_active() && t.cell == cell)
            .filter(|t| command.script.as_ref().is_none_or(|s| *s == t.script))
            .filter(|t| command.facet.is_none() || t.facet == command.facet)
            .filter(|t| command.incarnation.is_none() || t.incarnation == command.incarnation)
            .map(|t| Tombstone {
                cleared_at_ms: Some(now),
                ..t
            })
            .collect()
    } else {
        let consumer = BucketConsumer::load(export.clone()).await?;
        let held: Vec<StreamId> = consumer
            .streams()
            .await?
            .into_iter()
            .map(|s| s.id)
            .filter(|s| s.cell == cell)
            .collect();
        erase_targets(
            &cell,
            &class,
            Selection {
                script: command.script.as_deref(),
                facet: command.facet.as_deref(),
                incarnation: command.incarnation,
                reason: command.reason.as_deref(),
            },
            &held,
            now,
        )?
    };
    anyhow::ensure!(
        !targets.is_empty(),
        "nothing to {}",
        if command.clear { "clear" } else { "erase" }
    );
    // The bucket first: every path that reads it stops exporting the stream
    // before the consumer's copy goes.
    let consumer = BucketConsumer::from_records(export.clone(), Vec::new(), &[])?;
    let mut out = Output::new(command.format());
    for t in &targets {
        let key = tombstone::put(&export, t).await?;
        consumer.tombstone(t).await?;
        let text = format!(
            "{} {} script={:?} facet={:?} incarnation={}",
            if command.clear { "cleared" } else { "erased " },
            t.cell,
            t.script,
            t.facet,
            t.incarnation.map_or("all".into(), |i| i.to_string())
        );
        out.row(&Row(json!({ "key": key, "tombstone": t }), text))?;
    }
    out.finish()
}

/// What an erase names.
#[derive(Clone, Copy, Default)]
pub(crate) struct Selection<'a> {
    pub script: Option<&'a str>,
    pub facet: Option<&'a str>,
    pub incarnation: Option<u64>,
    pub reason: Option<&'a str>,
}

/// The tombstones one erase writes: for each script, the named facet, or the
/// root and every facet the consumer holds for the cell.
pub(crate) fn erase_targets(
    cell: &str,
    class: &str,
    command: Selection<'_>,
    held: &[StreamId],
    now: i64,
) -> anyhow::Result<Vec<Tombstone>> {
    let scripts: BTreeSet<String> = match command.script {
        Some(script) => BTreeSet::from([script.to_string()]),
        None => held.iter().map(|s| s.script.clone()).collect(),
    };
    anyhow::ensure!(
        !scripts.is_empty(),
        "the consumer holds no stream for {cell}; name the script with --script"
    );
    let mut out = Vec::new();
    for script in scripts {
        let facets: BTreeSet<Option<String>> = match command.facet {
            Some(facet) => BTreeSet::from([Some(facet.to_string())]),
            None => std::iter::once(None)
                .chain(
                    held.iter()
                        .filter(|s| s.script == script && s.facet.is_some())
                        .map(|s| s.facet.clone()),
                )
                .collect(),
        };
        for facet in facets {
            out.push(Tombstone {
                script: script.clone(),
                class: class.to_string(),
                cell: cell.to_string(),
                facet,
                incarnation: command.incarnation,
                erased_at_ms: now,
                reason: command.reason.map(str::to_string),
                cleared_at_ms: None,
            });
        }
    }
    Ok(out)
}
