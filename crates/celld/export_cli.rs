// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The export CLI runs offline against the bucket, outside the execution
// boundary.
#![allow(clippy::disallowed_methods)]

//! `celld export repair | backfill | inspect`
//! (`docs/design/change-export.md`, "Snapshots and repair" and "The fleet
//! bucket").
//!
//! `repair` and `backfill` restore streams read-only from the fleet bucket
//! and write their snapshots through the bucket sink, under
//! `export/changes/<node>/` of the export bucket, where the loader's
//! `COPY INTO` picks them up like any other export object. Both take the
//! consumer's `EXPORT_GAPS` view unloaded as JSON lines with `--gaps`, so
//! gaps the live path reported, links and recovered heads beyond what was
//! certified, tables a `bulk` record left unknown, and reconciler findings
//! all feed one list. Each prints one JSON report per stream saying the
//! position its snapshot reached; see [`crate::export_repair`].
//!
//! `inspect` prints the records of bucket-sink objects as JSON lines, from
//! the bucket or from downloaded files.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::sync::Arc;

use anyhow::{bail, ensure, Context as _};
use celld_export_format::{Kind, Origin, Position, Record, StreamId};
use futures_util::StreamExt as _;
use object_store::path::Path as ObjectPath;
use serde::Serialize;

use crate::bucket::Bucket;
use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::export_repair::{self, Job, Report, Settings, Status};
use crate::export_restore::{self, Pace, Target};
use crate::export_sink::{BucketSink, BucketSinkConfig, CHANGES_PREFIX};
use crate::note;

/// The `node` of a snapshot this command writes, unless `--node` names one.
const DEFAULT_NODE: &str = "export-cli";
const DEFAULT_CONCURRENCY: usize = 4;
const DEFAULT_RATE: u32 = 100;
const DEFAULT_OBJECTS: usize = 100;

pub fn help_text() -> String {
    format!(
        r#"Repair, backfill, inspect, reconcile, verify, and erase the change export.

USAGE:
  celld export repair --stream SCOPE [--at EPOCH:TXID] [OPTIONS]
  celld export repair --gaps FILE [--class CLASS] [OPTIONS]
  celld export backfill --class CLASS [--after SCOPE] [OPTIONS]
  celld export backfill --gaps FILE [OPTIONS]
  celld export inspect [--node NODE] [--hour YYYY/MM/DD[/HH]] [FILTERS]
  celld export inspect --file PATH [--file PATH]... [FILTERS]
  celld export reconcile | verify | erase [FLAGS]

Run `celld export reconcile|verify|erase --help` for those commands' flags.

repair restores a stream read-only from the bucket at the first position
at or after --at (the bucket's newest cut without it) and writes a snapshot
there that replaces the stream's state in the consumer. The bucket's newest
cut is not the fleet's: when the bucket does not hold the position yet,
repair snapshots what it does hold and reports covers_target=false. With
--gaps it repairs every stream in an EXPORT_GAPS unload, one JSON object
per line, through the highest position its rows name.

backfill snapshots streams at the bucket's newest cut: every cell of a
class, or every stream an EXPORT_GAPS unload names.

Both print one JSON report per stream and exit non-zero when any failed.

OPTIONS:
{FLEET_HELP}
  --export-bucket NAME  Where snapshots are written (or CELLD_EXPORT_BUCKET;
                        default: the fleet bucket)
  --script NAME         The stream's script (default: the fleet's current
                        deployment). Ignored with --gaps, whose rows name it
  --node NAME           The node recorded on snapshots, and their object
                        prefix (default: {DEFAULT_NODE})
  --concurrency N       Streams restored at once (default: {DEFAULT_CONCURRENCY})
  --rate N              Bucket reads per second across all streams, 0 for
                        no limit (default: {DEFAULT_RATE})
  --dry-run             Print the streams and targets without restoring

INSPECT OPTIONS:
  --node NODE           Only objects this node wrote
  --hour YYYY/MM/DD[/HH]
                        Only objects of this day or hour (needs --node)
  --after KEY           Resume after this object key
  --objects N           Objects to read (default: {DEFAULT_OBJECTS})
  --file PATH           Read a downloaded object instead of the bucket
  --cell SCOPE          Only records of this cell
  --kind KIND           Only records of this kind
  --origin ORIGIN       Only records of this origin: live, snapshot, repair
  --summary             One line per stream instead of every record

CELLD_EXPORT_MAX_RECORD_BYTES and CELLD_EXPORT_TABLES apply as on a node.
  -h, --help            Show this help"#
    )
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let mut arguments = arguments.into_iter();
    match arguments.next().as_deref() {
        Some("repair") => run_snapshots(Mode::Repair, arguments.collect()).await,
        Some("backfill") => run_snapshots(Mode::Backfill, arguments.collect()).await,
        Some("inspect") => run_inspect(arguments.collect()).await,
        Some(command @ ("reconcile" | "verify" | "erase")) => {
            crate::export_audit::cli::run(command, arguments.collect()).await
        }
        None | Some("-h") | Some("--help") | Some("help") => {
            crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&help_text())
        }
        Some(other) => bail!(
            "unknown export command: {other}; celld export takes repair, backfill, inspect, reconcile, verify, or erase"
        ),
    }
}

// ---- repair and backfill ----------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Repair,
    Backfill,
}

impl Mode {
    fn command(self) -> &'static str {
        match self {
            Mode::Repair => "celld export repair",
            Mode::Backfill => "celld export backfill",
        }
    }
}

/// Which streams a run covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    /// One root cell.
    Stream {
        scope: String,
        at: Option<export_restore::Position>,
    },
    /// An `EXPORT_GAPS` unload, optionally only one class of it.
    Gaps { path: String, class: Option<String> },
    /// Every cell of a class in the bucket.
    Class {
        class: String,
        after: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotOptions {
    pub(crate) mode: Mode,
    pub(crate) fleet: FleetFlags,
    pub(crate) export_bucket: Option<String>,
    pub(crate) source: Source,
    pub(crate) script: Option<String>,
    pub(crate) node: String,
    pub(crate) concurrency: usize,
    pub(crate) rate: u32,
    pub(crate) dry_run: bool,
}

pub(crate) fn snapshot_options(
    mode: Mode,
    arguments: Vec<String>,
) -> anyhow::Result<Option<SnapshotOptions>> {
    let command = mode.command();
    let mut fleet = FleetFlags::default();
    let (mut export_bucket, mut stream, mut at, mut gaps, mut class, mut after) =
        (None, None, None, None, None, None);
    let (mut script, mut node, mut dry_run) = (None, DEFAULT_NODE.to_string(), false);
    let (mut concurrency, mut rate) = (DEFAULT_CONCURRENCY, DEFAULT_RATE);
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        let mut value = |option: &str| {
            arguments
                .next()
                .ok_or_else(|| anyhow::anyhow!("{option} requires a value"))
        };
        match argument.as_str() {
            "-h" | "--help" => return Ok(None),
            "--export-bucket" => export_bucket = Some(value("--export-bucket")?),
            "--stream" if mode == Mode::Repair => stream = Some(value("--stream")?),
            "--at" if mode == Mode::Repair => at = Some(parse_position(&value("--at")?)?),
            "--gaps" => gaps = Some(value("--gaps")?),
            "--class" => class = Some(value("--class")?),
            "--after" if mode == Mode::Backfill => after = Some(value("--after")?),
            "--script" => script = Some(value("--script")?),
            "--node" => node = value("--node")?,
            "--concurrency" => {
                concurrency = value("--concurrency")?
                    .parse()
                    .ok()
                    .filter(|n| *n > 0)
                    .context("--concurrency takes a positive number")?
            }
            "--rate" => {
                rate = value("--rate")?
                    .parse()
                    .context("--rate takes a number of reads per second")?
            }
            "--dry-run" => dry_run = true,
            other => {
                if fleet.consume(other, &mut value)? {
                    continue;
                }
                bail!("unknown option for {command}: {other}; run `{command} --help` for usage");
            }
        }
    }
    ensure!(
        node.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            && !node.is_empty()
            && !node.starts_with('.'),
        "--node must use ASCII letters, digits, and `- _ .`, not {node:?}"
    );
    if let Some(class) = class.as_deref() {
        ensure!(
            !class.contains(':') && celld_logic::cell::valid_cell_scope(class),
            "a class must use ASCII letters, digits, and `_ - . $`, not {class:?}"
        );
        ensure!(
            !crate::export::is_never_exported(class),
            "class {class} is never exported"
        );
    }
    if let Some(after) = after.as_deref() {
        ensure!(
            celld_logic::cell::valid_cell_scope(after),
            "--after takes a cell scope this command printed, not {after:?}"
        );
    }
    let source = match (stream, gaps, class) {
        (Some(scope), None, None) => {
            ensure!(
                celld_logic::cell::valid_cell_scope(&scope),
                "--stream takes a cell scope, not {scope:?}"
            );
            ensure!(
                !crate::export::is_never_exported(export_repair::class_of(&scope)),
                "class {} is never exported",
                export_repair::class_of(&scope)
            );
            ensure!(
                !scope.contains("/facets/"),
                "facet streams are not exported yet"
            );
            Source::Stream { scope, at }
        }
        (None, Some(path), class) => {
            ensure!(
                at.is_none(),
                "--at goes with --stream; --gaps names its own positions"
            );
            ensure!(after.is_none(), "--after goes with --class, not --gaps");
            Source::Gaps { path, class }
        }
        (None, None, Some(class)) if mode == Mode::Backfill => Source::Class { class, after },
        _ => match mode {
            Mode::Repair => bail!("{command} takes --stream SCOPE or --gaps FILE"),
            Mode::Backfill => bail!("{command} takes --class CLASS or --gaps FILE"),
        },
    };
    Ok(Some(SnapshotOptions {
        mode,
        fleet,
        export_bucket,
        source,
        script,
        node,
        concurrency,
        rate,
        dry_run,
    }))
}

/// `EPOCH:TXID`, as a report prints it (`e3:17`) or bare (`3:17`).
pub(crate) fn parse_position(text: &str) -> anyhow::Result<export_restore::Position> {
    let bare = text.strip_prefix('e').unwrap_or(text);
    let (epoch, txid) = bare
        .split_once(':')
        .with_context(|| format!("a position is EPOCH:TXID, not {text:?}"))?;
    Ok(export_restore::Position {
        epoch: epoch
            .parse()
            .with_context(|| format!("bad epoch in {text:?}"))?,
        txid: txid
            .parse()
            .with_context(|| format!("bad txid in {text:?}"))?,
    })
}

/// The export settings a node would use, whether or not `CELLD_EXPORT` is
/// on in this shell.
fn export_config() -> anyhow::Result<crate::export::Config> {
    let config = crate::export::Config::from_lookup(|name| {
        if name == "CELLD_EXPORT" {
            Ok(Some("1".to_string()))
        } else {
            crate::env_vars::value(name)
        }
    })?;
    config.context("export settings did not load")
}

async fn run_snapshots(mode: Mode, arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = snapshot_options(mode, arguments)? else {
        return crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&help_text());
    };
    let config = export_config()?;
    let storage = options.fleet.clone().resolve(mode.command())?;
    let source = storage.open().await?;

    let jobs = plan(&options, &source).await?;
    if jobs.is_empty() {
        note!("no streams to snapshot");
        return Ok(());
    }
    if options.dry_run {
        let mut out = std::io::stdout().lock();
        for job in &jobs {
            writeln!(out, "{}", serde_json::to_string(&planned(job))?)?;
        }
        note!("{} streams; nothing restored (--dry-run)", jobs.len());
        return Ok(());
    }

    let export_bucket = options
        .export_bucket
        .clone()
        .or_else(|| config.bucket_override.clone());
    let destination = match export_bucket {
        Some(bucket) => {
            let bucket =
                crate::fleet::bucket_client(&bucket, storage.endpoint.as_deref(), &storage.region)?;
            crate::fleet::validate_bucket(&bucket).await?;
            bucket
        }
        None => crate::fleet::bucket_client(
            &storage.bucket,
            storage.endpoint.as_deref(),
            &storage.region,
        )?,
    };
    let (outcomes_tx, outcomes) = tokio::sync::mpsc::unbounded_channel();
    let sink = BucketSink::start(
        destination,
        options.node.clone(),
        BucketSinkConfig {
            flush: config.flush,
            flush_bytes: config.flush_bytes as u64,
            ..BucketSinkConfig::default()
        },
        outcomes_tx,
    );
    let settings = Settings {
        node: options.node.clone(),
        max_record_bytes: config.max_record_bytes,
        denied_tables: config.denied_tables.clone(),
        concurrency: options.concurrency,
        buffer_bytes: (config.flush_bytes as u64).saturating_mul(2),
    };
    let total = jobs.len();
    let reports = export_repair::run(
        &source,
        Arc::new(sink),
        outcomes,
        jobs,
        &settings,
        Pace::per_second(options.rate),
        |report| {
            if let Ok(line) = serde_json::to_string(report) {
                let _ = writeln!(std::io::stdout(), "{line}");
            }
        },
    )
    .await;
    summarize(total, &reports)
}

/// The jobs of a run, in the order they will be reported.
async fn plan(options: &SnapshotOptions, bucket: &Bucket) -> anyhow::Result<Vec<Job>> {
    let script = || async {
        match options.script.clone() {
            Some(script) => Ok(script),
            None => current_script(bucket).await,
        }
    };
    Ok(match &options.source {
        Source::Stream { scope, at } => vec![Job {
            stream: export_repair::root_stream(&script().await?, scope)?,
            target: at.map_or(Target::Head, Target::AtOrAfter),
            reasons: ["operator".to_string()].into(),
            pin_incarnation: false,
        }],
        Source::Gaps { path, class } => {
            let text =
                std::fs::read_to_string(path).with_context(|| format!("read gaps list {path}"))?;
            let rows: Vec<_> = export_repair::parse_gaps(&text)?
                .into_iter()
                .filter(|row| class.as_ref().is_none_or(|c| *c == row.stream.class))
                .collect();
            export_repair::jobs_from_gaps(&rows, options.mode == Mode::Backfill)
        }
        Source::Class { class, after } => {
            let script = script().await?;
            list_class(bucket, class, after.as_deref())
                .await?
                .into_iter()
                .map(|scope| {
                    Ok(Job {
                        stream: export_repair::root_stream(&script, &scope)?,
                        target: Target::Head,
                        reasons: ["backfill".to_string()].into(),
                        pin_incarnation: false,
                    })
                })
                .collect::<anyhow::Result<_>>()?
        }
    })
}

/// The script of the fleet's current deployment.
async fn current_script(bucket: &Bucket) -> anyhow::Result<String> {
    let pointer = crate::fleet::read_current_pointer(bucket)
        .await
        .context("find the fleet's script; pass --script")?;
    pointer
        .script_name
        .context("the fleet's deployment pointer names no script; pass --script")
}

/// Every cell scope of `class` in the bucket, in key order, after `after`.
async fn list_class(
    bucket: &Bucket,
    class: &str,
    after: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    let prefix = format!("cells/{class}:");
    let start_after = after.map(|scope| format!("cells/{scope}"));
    let mut token = None;
    let mut scopes = Vec::new();
    loop {
        let page = bucket
            .common_prefixes_page(&prefix, start_after.as_deref(), token, 1000)
            .await
            .with_context(|| format!("list cells of {class}"))?;
        scopes.extend(crate::cell_cli::cell_scopes_from_prefixes(
            page.prefixes,
            Some(class),
            after,
        ));
        match page.page_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    Ok(scopes)
}

#[derive(Serialize)]
struct Planned<'a> {
    script: &'a str,
    class: &'a str,
    cell: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    facet: Option<&'a str>,
    incarnation: u64,
    reasons: Vec<&'a str>,
    target: String,
}

fn planned(job: &Job) -> Planned<'_> {
    Planned {
        script: &job.stream.script,
        class: &job.stream.class,
        cell: &job.stream.cell,
        facet: job.stream.facet.as_deref(),
        incarnation: job.stream.incarnation,
        reasons: job.reasons.iter().map(String::as_str).collect(),
        target: match job.target {
            Target::Head => "head".to_string(),
            Target::AtOrAfter(p) => p.to_string(),
        },
    }
}

fn summarize(total: usize, reports: &[Report]) -> anyhow::Result<()> {
    let count = |status| reports.iter().filter(|r| r.status == status).count();
    let (written, skipped, failed) = (
        count(Status::Written),
        count(Status::Skipped),
        count(Status::Failed),
    );
    let short = reports
        .iter()
        .filter(|r| r.status == Status::Written && !r.covers_target)
        .count();
    note!(
        "{total} streams: {written} written, {skipped} skipped, {failed} failed; \
         {short} written short of their target, which the next flush or reconcile covers"
    );
    ensure!(failed == 0, "{failed} of {total} snapshots failed");
    Ok(())
}

// ---- inspect -------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct InspectOptions {
    pub(crate) fleet: FleetFlags,
    pub(crate) export_bucket: Option<String>,
    pub(crate) node: Option<String>,
    pub(crate) hour: Option<String>,
    pub(crate) after: Option<String>,
    pub(crate) objects: usize,
    pub(crate) files: Vec<String>,
    pub(crate) filter: Filter,
    pub(crate) summary: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Filter {
    pub(crate) cell: Option<String>,
    pub(crate) kind: Option<Kind>,
    pub(crate) origin: Option<Origin>,
}

impl Filter {
    pub(crate) fn keeps(&self, record: &Record) -> bool {
        self.cell
            .as_ref()
            .is_none_or(|c| *c == record.stream().cell)
            && self.kind.is_none_or(|k| k == record.kind())
            && self.origin.is_none_or(|o| o == record.envelope.origin)
    }
}

pub(crate) fn inspect_options(arguments: Vec<String>) -> anyhow::Result<Option<InspectOptions>> {
    let mut options = InspectOptions {
        objects: DEFAULT_OBJECTS,
        ..Default::default()
    };
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        let mut value = |option: &str| {
            arguments
                .next()
                .ok_or_else(|| anyhow::anyhow!("{option} requires a value"))
        };
        match argument.as_str() {
            "-h" | "--help" => return Ok(None),
            "--export-bucket" => options.export_bucket = Some(value("--export-bucket")?),
            "--node" => options.node = Some(value("--node")?),
            "--hour" => options.hour = Some(value("--hour")?),
            "--after" => options.after = Some(value("--after")?),
            "--objects" => {
                options.objects = value("--objects")?
                    .parse()
                    .ok()
                    .filter(|n| *n > 0)
                    .context("--objects takes a positive number")?
            }
            "--file" => options.files.push(value("--file")?),
            "--cell" => options.filter.cell = Some(value("--cell")?),
            "--kind" => {
                let kind = value("--kind")?;
                options.filter.kind = Some(
                    serde_json::from_value(serde_json::Value::String(kind.clone()))
                        .with_context(|| format!("unknown record kind {kind:?}"))?,
                )
            }
            "--origin" => {
                let origin = value("--origin")?;
                options.filter.origin = Some(
                    serde_json::from_value(serde_json::Value::String(origin.clone()))
                        .with_context(|| format!("unknown origin {origin:?}"))?,
                )
            }
            "--summary" => options.summary = true,
            other => {
                if options.fleet.consume(other, &mut value)? {
                    continue;
                }
                bail!("unknown option for celld export inspect: {other}; run `celld export inspect --help` for usage");
            }
        }
    }
    let segment = |s: &str| {
        !s.is_empty()
            && !s.starts_with('.')
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    };
    if let Some(node) = options.node.as_deref() {
        ensure!(segment(node), "--node takes a node name, not {node:?}");
    }
    if let Some(hour) = options.hour.as_deref() {
        ensure!(options.node.is_some(), "--hour needs --node");
        let parts: Vec<&str> = hour.split('/').collect();
        ensure!(
            (3..=4).contains(&parts.len())
                && parts
                    .iter()
                    .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit())),
            "--hour takes YYYY/MM/DD or YYYY/MM/DD/HH, not {hour:?}"
        );
    }
    if !options.files.is_empty() {
        ensure!(
            options.node.is_none() && options.hour.is_none() && options.after.is_none(),
            "--file reads local objects; --node, --hour and --after select bucket objects"
        );
    }
    Ok(Some(options))
}

/// The bucket key prefix `inspect` lists, relative to the bucket's prefix.
pub(crate) fn inspect_prefix(options: &InspectOptions) -> String {
    match (&options.node, &options.hour) {
        (Some(node), Some(hour)) => format!("{CHANGES_PREFIX}/{node}/{hour}/"),
        (Some(node), None) => format!("{CHANGES_PREFIX}/{node}/"),
        _ => format!("{CHANGES_PREFIX}/"),
    }
}

async fn run_inspect(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = inspect_options(arguments)? else {
        return crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&help_text());
    };
    let mut out = std::io::stdout();
    let mut summary = Summary::default();
    if !options.files.is_empty() {
        for path in &options.files {
            let bytes = std::fs::read(path).with_context(|| format!("read {path}"))?;
            let records = crate::export_sink::decode_records(bytes)
                .with_context(|| format!("decode {path}"))?;
            emit(&mut out, &options, &mut summary, path, records)?;
        }
    } else {
        let bucket = match options
            .export_bucket
            .clone()
            .or_else(|| crate::env_vars::value("CELLD_EXPORT_BUCKET").ok().flatten())
        {
            Some(bucket) => {
                let storage = options.fleet.clone().with_environment();
                crate::fleet::bucket_client(
                    &bucket,
                    storage.endpoint.as_deref(),
                    storage.region.as_deref().unwrap_or("us-east-1"),
                )?
            }
            None => {
                options
                    .fleet
                    .clone()
                    .resolve("celld export inspect")?
                    .open()
                    .await?
            }
        };
        let (keys, more) = list_objects(&bucket, &options).await?;
        for key in &keys {
            let (bytes, _) = bucket
                .get(key)
                .await?
                .with_context(|| format!("{key} disappeared while listing"))?;
            let records = crate::export_sink::decode_records(bytes.to_vec())
                .with_context(|| format!("decode {key}"))?;
            emit(&mut out, &options, &mut summary, key, records)?;
        }
        if let (true, Some(last)) = (more, keys.last()) {
            note!("more objects follow; resume with --after {last}");
        }
    }
    if options.summary {
        for (stream, line) in summary.streams {
            writeln!(
                out,
                "{}",
                serde_json::to_string(&SummaryLine {
                    stream: &stream,
                    line
                })?
            )?;
        }
    }
    Ok(())
}

/// Up to `options.objects` bucket-sink object keys in key order, and whether
/// more follow.
pub(crate) async fn list_objects(
    bucket: &Bucket,
    options: &InspectOptions,
) -> anyhow::Result<(Vec<String>, bool)> {
    let prefix = inspect_prefix(options);
    let path = ObjectPath::from(format!("{}{}", bucket.prefix, prefix.trim_end_matches('/')));
    let mut listing = match &options.after {
        Some(after) => bucket.store.list_with_offset(
            Some(&path),
            &ObjectPath::from(format!("{}{after}", bucket.prefix)),
        ),
        None => bucket.store.list(Some(&path)),
    };
    let mut keys = Vec::new();
    while let Some(meta) = listing.next().await {
        let meta = meta.with_context(|| format!("list {prefix}"))?;
        let key = meta.location.as_ref();
        let key = key.strip_prefix(bucket.prefix.as_str()).unwrap_or(key);
        if !key.ends_with(".parquet") {
            continue;
        }
        if keys.len() == options.objects {
            return Ok((keys, true));
        }
        keys.push(key.to_string());
    }
    Ok((keys, false))
}

fn emit(
    out: &mut impl std::io::Write,
    options: &InspectOptions,
    summary: &mut Summary,
    object: &str,
    records: Vec<Record>,
) -> anyhow::Result<()> {
    for record in records.into_iter().filter(|r| options.filter.keeps(r)) {
        if options.summary {
            summary.add(&record);
            continue;
        }
        let mut value = serde_json::to_value(&record)?;
        if let Some(map) = value.as_object_mut() {
            map.insert("object".to_string(), object.into());
        }
        writeln!(out, "{value}")?;
    }
    Ok(())
}

/// Per stream: record counts by kind and the span of positions seen.
#[derive(Default)]
pub(crate) struct Summary {
    pub(crate) streams: BTreeMap<StreamId, StreamSummary>,
}

#[derive(Default, Serialize)]
pub(crate) struct StreamSummary {
    pub(crate) records: u64,
    pub(crate) kinds: BTreeMap<String, u64>,
    pub(crate) first: Option<Position>,
    pub(crate) last: Option<Position>,
}

#[derive(Serialize)]
struct SummaryLine<'a> {
    #[serde(flatten)]
    stream: &'a StreamId,
    #[serde(flatten)]
    line: StreamSummary,
}

impl Summary {
    pub(crate) fn add(&mut self, record: &Record) {
        let line = self.streams.entry(record.stream().clone()).or_default();
        line.records += 1;
        let kind = serde_json::to_value(record.kind())
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        *line.kinds.entry(kind).or_default() += 1;
        let p = record.position();
        line.first = Some(line.first.map_or(p, |f| f.min(p)));
        line.last = Some(line.last.map_or(p, |l| l.max(p)));
    }
}

#[cfg(test)]
mod tests;
