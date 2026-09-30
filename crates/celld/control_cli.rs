// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Operator commands read the process environment for their fleet settings.
#![allow(clippy::disallowed_methods)]

//! `celld control` — choose and inspect where a fleet keeps its
//! coordination records.

use std::borrow::Cow;

use anyhow::{bail, Context};
use serde_json::{json, Value};

use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output, Record};
use crate::control::{Backend, Settings};

struct Reply(Value);

impl Record for Reply {
    fn json(&self) -> Value {
        self.0.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(
            serde_json::to_string_pretty(&self.0)
                .expect("a serde_json::Value always serializes as JSON"),
        )
    }
}

enum Verb {
    Init {
        table: Option<String>,
        table_region: Option<String>,
        create: bool,
    },
    Show,
}

struct Command {
    verb: Verb,
    fleet: FleetFlags,
    json: bool,
}

fn help_text() -> String {
    format!(
        "Choose and inspect where a fleet keeps its coordination records.

USAGE:
  celld control init --table NAME --bucket s3://NAME[/PREFIX] [OPTIONS]
  celld control show --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]

`init` creates the DynamoDB table if it is absent (on-demand capacity,
deletion protection, point-in-time recovery), claims it for this fleet, and
records the choice in fleet/control.json. Run it before the fleet's first
node starts; a fleet with live node leases in the bucket is refused.

`show` prints the fleet's choice and, for a table, its health.

OPTIONS:
  --table NAME          The DynamoDB table (or CELLD_CONTROL=dynamodb://NAME)
  --table-region REGION The table's region (or CELLD_CONTROL_REGION; default:
                        the bucket's region)
  --no-create           Adopt an existing table instead of creating it
  --json                Print one JSON object instead of text
{FLEET_HELP}
"
    )
}

fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Command>> {
    let mut arguments = arguments.into_iter();
    let verb = match arguments.next().as_deref() {
        None | Some("-h") | Some("--help") | Some("help") => return Ok(None),
        Some("init") => Verb::Init {
            table: None,
            table_region: None,
            create: true,
        },
        Some("show") => Verb::Show,
        Some(other) => bail!("unknown celld control command {other:?}; see celld control --help"),
    };
    let mut command = Command {
        verb,
        fleet: FleetFlags::default(),
        json: false,
    };
    let rest: Vec<String> = arguments.collect();
    let mut index = 0;
    while index < rest.len() {
        let argument = rest[index].as_str();
        let mut value = |name: &str| -> anyhow::Result<String> {
            index += 1;
            rest.get(index)
                .cloned()
                .with_context(|| format!("{name} needs a value"))
        };
        if command.fleet.consume(argument, &mut value)? {
            index += 1;
            continue;
        }
        match (argument, &mut command.verb) {
            ("--json", _) => command.json = true,
            ("-h" | "--help", _) => return Ok(None),
            ("--table", Verb::Init { table, .. }) => *table = Some(value("--table")?),
            ("--table-region", Verb::Init { table_region, .. }) => {
                *table_region = Some(value("--table-region")?)
            }
            ("--no-create", Verb::Init { create, .. }) => *create = false,
            (other, _) => bail!("unknown option {other:?}; see celld control --help"),
        }
        index += 1;
    }
    Ok(Some(command))
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = parse(arguments)? else {
        return Output::new(Format::Text).help(&help_text());
    };
    let mut out = Output::new(if command.json {
        Format::Json
    } else {
        Format::Text
    });
    match command.verb {
        Verb::Init {
            table,
            table_region,
            create,
        } => {
            let mut settings = Settings::from_env()?;
            if let Some(table) = table {
                settings.backend = Some(Backend::parse(&format!("dynamodb://{table}"))?);
            }
            if table_region.is_some() {
                settings.region = table_region;
            }
            let storage = command.fleet.resolve("celld control init")?;
            let bucket = crate::fleet::bucket_client(
                &storage.bucket,
                storage.endpoint.as_deref(),
                &storage.region,
            )?;
            crate::fleet::validate_bucket(&bucket).await?;
            let resolved = crate::control::init(&bucket, &settings, create).await?;
            out.row(&Reply(json!({
                "backend": resolved.backend.to_string(),
                "region": resolved.region,
                "fleet": resolved.fleet,
            })))?;
        }
        Verb::Show => {
            let storage = command.fleet.resolve("celld control show")?;
            let bucket = crate::fleet::bucket_client(
                &storage.bucket,
                storage.endpoint.as_deref(),
                &storage.region,
            )?;
            crate::fleet::validate_bucket(&bucket).await?;
            out.row(&Reply(Value::Object(crate::control::show(&bucket).await?)))?;
        }
    }
    out.finish()
}
