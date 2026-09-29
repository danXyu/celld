//! `celld-export-loader`: deploys the change export's Snowflake objects and
//! keeps them in step. See this crate's README for the settings and for what
//! each command does.

use std::io::Write as _;
use std::process::ExitCode;
use std::time::Duration;

use celld_export_snowflake::loader::{DeployReport, Erasure, SyncReport};
use celld_export_snowflake::sql_api::{Connection, KeyPair, SqlApi};
use celld_export_snowflake::{Deployment, Loader, LoaderConfig, Rows};

const USAGE: &str = "\
usage: celld-export-loader COMMAND

  deploy                 create what is missing, resume the tasks, sync the Dynamic Tables
  sync                   create or replace the Dynamic Tables whose schema changed
  run [SECONDS]          deploy, then sync every SECONDS (default 60) until killed
  load PREFIX            COPY the stage files under PREFIX and route them now
  erase SCRIPT CLASS CELL [--facet PATH] [--incarnation N] [--reason TEXT]
                         tombstone a stream and delete its rows
  query SQL [BIND...]    run SQL with each ? bound to a JSON value, and print the rows
  gaps                   print EXPORT_GAPS
  certified              print CELL_CERTIFIED

settings (environment):
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_FILE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE        required
  SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, SNOWFLAKE_ROLE, SNOWFLAKE_URL  optional
  EXPORT_STAGE_URL, EXPORT_STORAGE_INTEGRATION                     required by deploy and run
  EXPORT_TARGET_LAG (default '1 minute'), EXPORT_DYNAMIC_TABLE_PREFIX (default CF)
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
fn connect() -> Result<SqlApi, Error> {
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
    let clock = Box::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    });
    Ok(SqlApi::new(connection, key, clock))
}

fn loader(needs_stage: bool) -> Result<Loader<SqlApi>, Error> {
    let (stage_url, storage_integration) = if needs_stage {
        (env("EXPORT_STAGE_URL")?, env("EXPORT_STORAGE_INTEGRATION")?)
    } else {
        (String::new(), String::new())
    };
    let config = LoaderConfig {
        deployment: Deployment {
            stage_url,
            storage_integration,
            warehouse: env("SNOWFLAKE_WAREHOUSE")?,
        },
        target_lag: env_or("EXPORT_TARGET_LAG", "1 minute"),
        dynamic_table_prefix: env_or("EXPORT_DYNAMIC_TABLE_PREFIX", "CF"),
    };
    Ok(Loader::new(connect()?, config))
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
    match &r.notification_channel {
        Some(q) => out(&format!(
            "point the bucket's object-created notifications for the stage prefix at {q}"
        ))?,
        None => out("SHOW PIPES did not report EXPORT_PIPE's notification channel")?,
    }
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
        ["deploy"] => print_deploy(&loader(true)?.deploy()?),
        ["sync"] => print_sync(&loader(false)?.sync_dynamic_tables()?),
        ["run", rest @ ..] => {
            let every = match rest {
                [] => 60,
                [s] => s.parse::<u64>().map_err(|_| USAGE)?,
                _ => return Err(USAGE.into()),
            };
            let mut l = loader(true)?;
            print_deploy(&l.deploy()?)?;
            loop {
                std::thread::sleep(Duration::from_secs(every));
                // A failed sync is retried on the next round; the objects it
                // did not touch keep running.
                match l.sync_dynamic_tables() {
                    Ok(r)
                        if r == SyncReport {
                            unchanged: r.unchanged.clone(),
                            ..SyncReport::default()
                        } => {}
                    Ok(r) => print_sync(&r)?,
                    Err(e) => eprintln!("celld-export-loader: sync: {e}"),
                }
            }
        }
        ["load", prefix] => print_rows(&loader(false)?.load_prefix(prefix)?),
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
            loader(false)?.erase(&e)?;
            out("erased")
        }
        ["query", sql, binds @ ..] => {
            let binds = binds
                .iter()
                .map(|b| serde_json::from_str(b))
                .collect::<Result<Vec<serde_json::Value>, _>>()?;
            print_rows(&loader(false)?.query(sql, &binds)?)
        }
        ["gaps"] => print_rows(&loader(false)?.gaps()?),
        ["certified"] => print_rows(&loader(false)?.certified()?),
        _ => Err(USAGE.into()),
    }
}
