//! `celld-export-loader`: deploys the change export's Snowflake objects,
//! feeds them from the export topic (blob-stream or Kafka), and keeps them
//! in step. See this
//! crate's README for the settings and for what each command does.

// celld's rule against tokio::select! is for its execution boundary; `run`
// waits on the host's signals outside it.
#![cfg_attr(
    any(feature = "blob-stream", feature = "kafka"),
    allow(clippy::disallowed_macros)
)]

use std::io::Write as _;
use std::process::ExitCode;
use std::time::Duration;

use celld_export_snowflake::consume::{Batch, Limits};
use celld_export_snowflake::loader::{DeployReport, Erasure, SyncReport};
use celld_export_snowflake::sql_api::{Clock, Connection, KeyPair, SqlApi};
use celld_export_snowflake::streaming::Streaming;
use celld_export_snowflake::{Deployment, Loader, LoaderConfig, Rows};

const USAGE: &str = "\
usage: celld-export-loader COMMAND

  deploy                 create what is missing, resume the tasks, sync the Dynamic Tables
  sync                   create or replace the Dynamic Tables whose schema changed
  run [SECONDS]          deploy, then land the export topic (EXPORT_SOURCE) through Snowpipe
                         Streaming and sync the Dynamic Tables every SECONDS (default 60)
                         until stopped
  ingest FILE            land the records in FILE (JSON lines, as `celld export inspect`
                         prints them; - for stdin) through Snowpipe Streaming, and route them
  erase SCRIPT CLASS CELL [--facet PATH] [--incarnation N] [--reason TEXT]
                         tombstone a stream and delete its rows
  query SQL [BIND...]    run SQL with each ? bound to a JSON value, and print the rows
  gaps                   print EXPORT_GAPS
  certified              print CELL_CERTIFIED

settings (environment):
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_FILE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE        required
  SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, SNOWFLAKE_ROLE, SNOWFLAKE_URL  optional
  EXPORT_TARGET_LAG (default '1 minute'), EXPORT_DYNAMIC_TABLE_PREFIX (default CF)
  EXPORT_SOURCE                  run: the topic's transport, blob-stream (default) or kafka
  EXPORT_BLOB_STREAM_CONFIG      run, blob-stream: consumer config (.yaml or .json)
  EXPORT_KAFKA_BROKERS           run, kafka: bootstrap servers, host:port comma-separated
  EXPORT_KAFKA_TOPIC             run, kafka: the topic (default celld-changes)
  EXPORT_KAFKA_PROPERTIES        run, kafka: a file of librdkafka consumer properties (name=value)
  EXPORT_MEMBER_ID               run: this loader's stable member id (default $HOSTNAME)
  EXPORT_GROUP                   run: consumer group (default snowflake)
  EXPORT_SKIP                    run: messages to drop, as SOURCE/PARTITION/OFFSET (blob-stream/3/17,
                                 kafka/0/42), comma-separated
  EXPORT_BATCH_RECORDS (default 10000), EXPORT_BATCH_BYTES (default 8388608),
  EXPORT_BATCH_MS (default 5000) when a batch lands
  EXPORT_VISIBLE_SECONDS (default 300) ingest: how long to wait for landed rows to be queryable
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("celld-export-loader: {e}");
            ExitCode::FAILURE
        }
    }
}

type Error = Box<dyn std::error::Error>;

fn env(name: &str) -> Result<String, Error> {
    std::env::var(name).map_err(|_| format!("{name} is not set").into())
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[allow(clippy::disallowed_methods)] // The loader is a host tool; its clock and key file are the host's.
fn credentials() -> Result<(Connection, KeyPair, Clock), Error> {
    let pem = std::fs::read_to_string(env("SNOWFLAKE_PRIVATE_KEY_FILE")?)?;
    let passphrase = std::env::var("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE").ok();
    let key = KeyPair::from_pem(&pem, passphrase.as_deref())?;
    let connection = Connection {
        account: env("SNOWFLAKE_ACCOUNT")?,
        user: env("SNOWFLAKE_USER")?,
        role: std::env::var("SNOWFLAKE_ROLE").ok(),
        database: env("SNOWFLAKE_DATABASE")?,
        schema: env("SNOWFLAKE_SCHEMA")?,
        warehouse: env("SNOWFLAKE_WAREHOUSE")?,
        url: std::env::var("SNOWFLAKE_URL").ok(),
        statement_timeout: 600,
    };
    let clock: Clock = Box::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    });
    Ok((connection, key, clock))
}

/// Where batches land: Snowpipe Streaming, as the same user.
fn streaming() -> Result<Streaming, Error> {
    let (connection, key, clock) = credentials()?;
    Ok(Streaming::new(&connection, key, clock))
}

fn env_number<T: std::str::FromStr>(name: &str, default: T) -> Result<T, Error> {
    match std::env::var(name) {
        Ok(v) => v
            .parse()
            .map_err(|_| format!("{name} must be a number, not {v:?}").into()),
        Err(_) => Ok(default),
    }
}

fn limits() -> Result<Limits, Error> {
    let d = Limits::default();
    Ok(Limits {
        records: env_number("EXPORT_BATCH_RECORDS", d.records)?.max(1),
        bytes: env_number("EXPORT_BATCH_BYTES", d.bytes)?.max(1),
    })
}

fn loader() -> Result<Loader<SqlApi>, Error> {
    let config = LoaderConfig {
        deployment: Deployment {
            warehouse: env("SNOWFLAKE_WAREHOUSE")?,
        },
        target_lag: env_or("EXPORT_TARGET_LAG", "1 minute"),
        dynamic_table_prefix: env_or("EXPORT_DYNAMIC_TABLE_PREFIX", "CF"),
    };
    let (connection, key, clock) = credentials()?;
    Ok(Loader::new(SqlApi::new(connection, key, clock), config))
}

fn out(line: &str) -> Result<(), Error> {
    writeln!(std::io::stdout().lock(), "{line}")?;
    Ok(())
}

fn print_deploy(r: &DeployReport) -> Result<(), Error> {
    out(&format!(
        "deployed {} statements; tasks resumed",
        r.statements
    ))?;
    print_sync(&r.dynamic_tables)
}

fn print_sync(r: &SyncReport) -> Result<(), Error> {
    for (what, names) in [
        ("created", &r.created),
        ("replaced", &r.replaced),
        ("unchanged", &r.unchanged),
    ] {
        for n in names {
            out(&format!("{what} {n}"))?;
        }
    }
    for (n, why) in &r.skipped {
        out(&format!("skipped {n}: {why}"))?;
    }
    for (n, why) in &r.failed {
        out(&format!("failed {n}: {why}"))?;
    }
    Ok(())
}

/// Tab-separated, with a header line; NULL as an empty field.
fn print_rows(rows: &Rows) -> Result<(), Error> {
    out(&rows.columns.join("\t"))?;
    for r in &rows.data {
        let fields: Vec<&str> = r.iter().map(|v| v.as_deref().unwrap_or("")).collect();
        out(&fields.join("\t"))?;
    }
    Ok(())
}

fn run(args: &[String]) -> Result<(), Error> {
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["deploy"] => print_deploy(&loader()?.deploy()?),
        ["sync"] => print_sync(&loader()?.sync_dynamic_tables()?),
        ["run", rest @ ..] => {
            let every = match rest {
                [] => 60,
                [s] => s.parse::<u64>().map_err(|_| USAGE)?,
                _ => return Err(USAGE.into()),
            };
            consume(Duration::from_secs(every.max(1)))
        }
        ["ingest", file] => ingest(file),
        ["erase", script, class, cell, rest @ ..] => {
            let mut e = Erasure {
                script: script.to_string(),
                class: class.to_string(),
                cell: cell.to_string(),
                facet: None,
                incarnation: None,
                reason: None,
            };
            let mut rest = rest.iter();
            while let Some(flag) = rest.next() {
                let value = rest.next().ok_or(USAGE)?;
                match *flag {
                    "--facet" => e.facet = Some(value.to_string()),
                    "--incarnation" => e.incarnation = Some(value.parse().map_err(|_| USAGE)?),
                    "--reason" => e.reason = Some(value.to_string()),
                    _ => return Err(USAGE.into()),
                }
            }
            loader()?.erase(&e)?;
            out("erased")
        }
        ["query", sql, binds @ ..] => {
            let binds = binds
                .iter()
                .map(|b| serde_json::from_str(b))
                .collect::<Result<Vec<serde_json::Value>, _>>()?;
            print_rows(&loader()?.query(sql, &binds)?)
        }
        ["gaps"] => print_rows(&loader()?.gaps()?),
        ["certified"] => print_rows(&loader()?.certified()?),
        _ => Err(USAGE.into()),
    }
}

/// Land the JSON-lines records in `file` in batches through Snowpipe
/// Streaming, wait until queries see them all, then route them. A line
/// that is not a record is reported and fails the command once the rest
/// have landed.
///
/// Snowpipe Streaming acknowledges rows once they are durable, which can
/// be before a query sees them, so routing straight away could route
/// nothing. Every row's source ends with a tag unique to this run, and the
/// command counts the tagged rows in `EXPORT_LANDING` until all are there
/// or `EXPORT_VISIBLE_SECONDS` (default 300) pass.
#[allow(clippy::disallowed_methods)] // The file and stdin are the host's.
fn ingest(file: &str) -> Result<(), Error> {
    use std::io::BufRead as _;
    let reader: Box<dyn std::io::BufRead> = if file == "-" {
        Box::new(std::io::stdin().lock())
    } else {
        Box::new(std::io::BufReader::new(std::fs::File::open(file)?))
    };
    let limits = limits()?;
    let mut l = loader()?;
    let mut to = streaming()?;
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    // No `%` or `_`: `visible` matches it with LIKE.
    let tag = format!(" (ingest {started}-{})", std::process::id());
    let mut batch = Batch::tagged(&tag);
    let (mut landed, mut bad) = (0, 0);
    for (n, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Err(u) = batch.push_json_line(&line, &format!("{file}:{}", n + 1)) {
            eprintln!(
                "celld-export-loader: {}: not a record: {}",
                u.source, u.error
            );
            bad += 1;
        }
        if batch.is_full(&limits) {
            landed += batch.len();
            batch.land(&mut to)?;
        }
    }
    landed += batch.len();
    batch.land(&mut to)?;
    let timeout = Duration::from_secs(env_number("EXPORT_VISIBLE_SECONDS", 300)?);
    let deadline = std::time::Instant::now() + timeout;
    let mut pause = Duration::from_millis(100);
    let mut visible = l.visible(&tag)?;
    while visible < landed as u64 && std::time::Instant::now() < deadline {
        std::thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_secs(2));
        visible = l.visible(&tag)?;
    }
    l.route()?;
    if visible < landed as u64 {
        return Err(format!(
            "landed {landed} records, but only {visible} were visible after {timeout:?} and \
             were routed; the route task routes the rest when they appear"
        )
        .into());
    }
    out(&format!("landed and routed {landed} records"))?;
    if bad > 0 {
        return Err(format!("{bad} lines were not records").into());
    }
    Ok(())
}

/// The transport `run` reads: `EXPORT_SOURCE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    BlobStream,
    Kafka,
}

fn transport() -> Result<Transport, Error> {
    match std::env::var("EXPORT_SOURCE").as_deref() {
        Err(_) | Ok("blob-stream") => Ok(Transport::BlobStream),
        Ok("kafka") => Ok(Transport::Kafka),
        Ok(other) => {
            Err(format!("EXPORT_SOURCE must be blob-stream or kafka, not {other:?}").into())
        }
    }
}

/// `run`: deploy, then consume the topic until SIGINT or SIGTERM.
#[cfg(any(feature = "blob-stream", feature = "kafka"))]
#[allow(clippy::disallowed_methods)] // The host's runtime and signals.
fn consume(sync_every: Duration) -> Result<(), Error> {
    use celld_export_snowflake::source::{Event, Settings};

    let transport = transport()?;
    let member = std::env::var("EXPORT_MEMBER_ID")
        .or_else(|_| std::env::var("HOSTNAME"))
        .ok();
    let group = std::env::var("EXPORT_GROUP").ok();
    let settings = Settings {
        limits: limits()?,
        linger: Duration::from_millis(env_number("EXPORT_BATCH_MS", 5000)?),
        sync_every,
        skip: std::env::var("EXPORT_SKIP")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        ..Settings::default()
    };
    // Read the consumer's settings before deploying, so a bad one fails
    // first.
    let source = Consumer::configure(transport, group.as_deref(), member.as_deref())?;
    let mut l = loader()?;
    let mut to = streaming()?;
    print_deploy(&l.deploy()?)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let stop = tokio_util::sync::CancellationToken::new();
        let on_signal = stop.clone();
        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install the SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            on_signal.cancel();
        });
        let report = |event: Event<'_>| {
            let line = match event {
                Event::Landed { .. } | Event::Synced(_) => None,
                Event::Skipped(source) => Some(format!("{source}: skipped (EXPORT_SKIP)")),
                Event::LandFailed { error, retry } => Some(format!(
                    "landing a batch failed, retrying in {retry:?}: {error}"
                )),
                Event::CommitFailed(e) => Some(format!("{e:#}")),
                Event::Fenced(p) => Some(format!(
                    "partitions {p:?} were fenced; another member may read their last batch again"
                )),
                Event::Revoked(p) => Some(format!("partitions {p:?} revoked")),
                Event::ReadFailed(e) => Some(format!("{e:#}")),
                Event::SyncFailed(e) => Some(format!("sync: {e}")),
            };
            if let Event::Synced(r) = event {
                if r.created.len() + r.replaced.len() + r.skipped.len() + r.failed.len() > 0 {
                    let _ = print_sync(r);
                }
            }
            if let Some(line) = line {
                eprintln!("celld-export-loader: {line}");
            }
        };
        let result = match source {
            #[cfg(feature = "blob-stream")]
            Consumer::BlobStream(config) => {
                let iterator = celld_export_snowflake::blob_stream::connect(config).await?;
                eprintln!("celld-export-loader: consuming the blob-stream export topic");
                celld_export_snowflake::blob_stream::run(
                    iterator, &mut l, &mut to, &settings, stop, report,
                )
                .await
            }
            #[cfg(feature = "kafka")]
            Consumer::Kafka(config) => {
                eprintln!(
                    "celld-export-loader: consuming the Kafka export topic {:?}",
                    config.topic
                );
                let source = celld_export_snowflake::kafka::KafkaSource::new(config);
                celld_export_snowflake::source::run(
                    source, &mut l, &mut to, &settings, stop, report,
                )
                .await
            }
        };
        result.map_err(|e| format!("{e:#}").into())
    })
}

/// The consumer `run` reads through, configured.
#[cfg(any(feature = "blob-stream", feature = "kafka"))]
enum Consumer {
    #[cfg(feature = "blob-stream")]
    BlobStream(blob_stream_proto::protos::blobstream::v1::config::ConsumerIteratorBootstrapConfig),
    #[cfg(feature = "kafka")]
    Kafka(celld_export_snowflake::kafka::Settings),
}

#[cfg(any(feature = "blob-stream", feature = "kafka"))]
impl Consumer {
    fn configure(
        transport: Transport,
        group: Option<&str>,
        member: Option<&str>,
    ) -> Result<Consumer, Error> {
        match transport {
            #[cfg(feature = "blob-stream")]
            Transport::BlobStream => {
                let path = env("EXPORT_BLOB_STREAM_CONFIG")?;
                Ok(Consumer::BlobStream(
                    celld_export_snowflake::blob_stream::bootstrap_config(
                        path.as_ref(),
                        group,
                        member,
                    )?,
                ))
            }
            #[cfg(feature = "kafka")]
            Transport::Kafka => {
                let properties = std::env::var("EXPORT_KAFKA_PROPERTIES").ok();
                Ok(Consumer::Kafka(
                    celld_export_snowflake::kafka::Settings::new(
                        &env("EXPORT_KAFKA_BROKERS")?,
                        std::env::var("EXPORT_KAFKA_TOPIC").ok().as_deref(),
                        group,
                        member,
                        properties.as_deref().map(std::path::Path::new),
                    )?,
                ))
            }
            #[allow(unreachable_patterns)]
            other => Err(missing_feature(other)),
        }
    }
}

/// Why this build cannot read `transport`.
fn missing_feature(transport: Transport) -> Error {
    let (feature, what) = match transport {
        Transport::BlobStream => ("blob-stream", "the blob-stream topic"),
        Transport::Kafka => ("kafka", "a Kafka topic"),
    };
    format!(
        "run consumes {what}, which needs a celld-export-loader built with the {feature} \
         feature (cargo build -p celld-export-snowflake --features sql-api,{feature} --bin \
         celld-export-loader); ingest lands record files without it"
    )
    .into()
}

#[cfg(not(any(feature = "blob-stream", feature = "kafka")))]
fn consume(_sync_every: Duration) -> Result<(), Error> {
    Err(missing_feature(transport()?))
}
