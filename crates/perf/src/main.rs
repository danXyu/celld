// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The harness is a client of celld, outside its execution boundary: it
// uses the real clock, filesystem, and runtime, and prints its reports.
#![allow(clippy::disallowed_methods, clippy::disallowed_macros)]

//! `celld-perf`: run performance scenarios against celld nodes, and compare
//! results. See docs/performance-tests.md.

mod check;
mod cluster;
mod hist;
mod keys;
mod load;
mod netem;
mod report;
mod run;
mod scenario;
mod sysstat;

use anyhow::{anyhow, bail, Context as _};
use cluster::Backend;
use scenario::Scenario;
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const USAGE: &str = "celld-perf — performance scenarios for celld

USAGE:
  celld-perf run [OPTIONS] SCENARIO...
  celld-perf compare BASE.json NEW.json [--threshold FRACTION]
  celld-perf summary RESULT.json
  celld-perf list

A SCENARIO is a JSON file, or the name (or leading id, like S2) of one in
crates/perf/scenarios; `all` runs every scenario the backend supports.

RUN OPTIONS:
  --celld PATH        The celld binary (default: beside celld-perf)
  --backend dev|s3    dev: one `celld dev` node on the local store (default)
                      s3: `celld deploy` and N nodes on an S3-compatible bucket
  --bucket s3://NAME[/PREFIX]
                      The s3 backend's bucket; each run adds a prefix below it
  --endpoint URL      The s3 backend's endpoint (e.g. MinIO)
  --region REGION     The s3 backend's region (default: us-east-1)
  --env NAME=VALUE    Node environment, over the scenario's; repeatable
  --repeat N          Run each scenario N times (default: 1)
  --quick             Shorten every phase to a fifth (at least 1 s)
  --enforce-timing    Fail the run when a timing check fails
  --out DIR           Results directory (default: target/perf)
  --keep              Keep each run's working directory

A run exits 1 when a count check or the verification sweep fails.
compare exits 1 on a count regression, or a timing regression that is
significant across at least three repeats on each side.
";

fn main() -> anyhow::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = arguments.first() else {
        print!("{USAGE}");
        return Ok(());
    };
    let rest = &arguments[1..];
    match command.as_str() {
        "run" => {
            raise_file_limit();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let code = runtime.block_on(run_command(rest))?;
            std::process::exit(code);
        }
        "compare" => compare_command(rest),
        "summary" => {
            let path = rest
                .first()
                .ok_or_else(|| anyhow!("summary needs a result file"))?;
            let result = read_json(Path::new(path))?;
            print!("{}", report::summary(&result));
            Ok(())
        }
        "list" => {
            for (path, scenario) in all_scenarios()? {
                println!(
                    "{:<28} nodes={} variants={} heavy={:<5} backends={:<8} {}",
                    scenario.name,
                    scenario.nodes,
                    scenario.variants.len().max(1),
                    scenario.heavy,
                    if scenario.backends.is_empty() {
                        "any".to_string()
                    } else {
                        scenario.backends.join(",")
                    },
                    path.file_name().unwrap_or_default().to_string_lossy()
                );
            }
            Ok(())
        }
        "-h" | "--help" | "help" => {
            print!("{USAGE}");
            Ok(())
        }
        other => bail!("unknown command {other}; run `celld-perf --help`"),
    }
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_json(path: &Path) -> anyhow::Result<serde_json::Value> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(serde_json::from_str(&text)?)
}

fn all_scenarios() -> anyhow::Result<Vec<(PathBuf, Scenario)>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(manifest_dir().join("scenarios"))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    paths.sort();
    paths
        .into_iter()
        .map(|path| Scenario::load(&path).map(|scenario| (path, scenario)))
        .collect()
}

/// A path, a scenario name, a leading id (`S2`), or `all`; each expanded
/// into its variants.
fn resolve(argument: &str, backend: &Backend) -> anyhow::Result<Vec<Scenario>> {
    Ok(resolve_files(argument, backend)?
        .iter()
        .flat_map(Scenario::expand_variants)
        .collect())
}

fn resolve_files(argument: &str, backend: &Backend) -> anyhow::Result<Vec<Scenario>> {
    let path = Path::new(argument);
    if path
        .extension()
        .is_some_and(|extension| extension == "json")
        && path.exists()
    {
        return Ok(vec![Scenario::load(path)?]);
    }
    let supports = |scenario: &Scenario| {
        scenario.backends.is_empty() || scenario.backends.iter().any(|name| name == backend.name())
    };
    let all = all_scenarios()?;
    if argument == "all" {
        return Ok(all
            .into_iter()
            .map(|(_, scenario)| scenario)
            .filter(|scenario| supports(scenario) && !scenario.heavy)
            .collect());
    }
    let all: Vec<Scenario> = all.into_iter().map(|(_, scenario)| scenario).collect();
    // An exact name is one scenario; an id (`S8`) is every scenario under it.
    let exact: Vec<Scenario> = all
        .iter()
        .filter(|scenario| scenario.name == argument)
        .cloned()
        .collect();
    let matches: Vec<Scenario> = if exact.is_empty() {
        all.into_iter()
            .filter(|scenario| scenario.name.starts_with(&format!("{argument}-")))
            .collect()
    } else {
        exact
    };
    match matches.len() {
        0 => bail!("no scenario named {argument}; run `celld-perf list`"),
        _ => Ok(matches),
    }
}

fn default_celld() -> PathBuf {
    let sibling = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("celld")));
    match sibling {
        Some(path) if path.exists() => path,
        _ => PathBuf::from("celld"),
    }
}

async fn run_command(arguments: &[String]) -> anyhow::Result<i32> {
    let mut celld = default_celld();
    let mut backend_name = "dev".to_string();
    let mut bucket = None;
    let mut endpoint = None;
    let mut region = "us-east-1".to_string();
    let mut env = BTreeMap::new();
    let mut repeat = 1usize;
    let mut quick = false;
    let mut enforce_timing = false;
    let mut out = manifest_dir().join("../../target/perf");
    let mut keep = false;
    let mut names = Vec::new();
    let mut arguments = arguments.iter();
    while let Some(argument) = arguments.next() {
        let mut value = |name: &str| {
            arguments
                .next()
                .cloned()
                .ok_or_else(|| anyhow!("{name} needs a value"))
        };
        match argument.as_str() {
            "--celld" => celld = PathBuf::from(value("--celld")?),
            "--backend" => backend_name = value("--backend")?,
            "--bucket" => bucket = Some(value("--bucket")?),
            "--endpoint" => endpoint = Some(value("--endpoint")?),
            "--region" => region = value("--region")?,
            "--env" => {
                let pair = value("--env")?;
                let (name, setting) = pair
                    .split_once('=')
                    .ok_or_else(|| anyhow!("--env takes NAME=VALUE, not {pair}"))?;
                env.insert(name.to_string(), setting.to_string());
            }
            "--repeat" => repeat = value("--repeat")?.parse()?,
            "--quick" => quick = true,
            "--enforce-timing" => enforce_timing = true,
            "--out" => out = PathBuf::from(value("--out")?),
            "--keep" => keep = true,
            flag if flag.starts_with("--") => bail!("unknown option {flag}"),
            name => names.push(name.to_string()),
        }
    }
    let backend = match backend_name.as_str() {
        "dev" => Backend::Dev,
        "s3" => Backend::S3 {
            bucket: bucket.ok_or_else(|| anyhow!("--backend s3 needs --bucket"))?,
            endpoint,
            region,
        },
        other => bail!("unknown backend {other}"),
    };
    if names.is_empty() {
        bail!("name at least one scenario; run `celld-perf list`");
    }
    let mut scenarios = Vec::new();
    for name in &names {
        scenarios.extend(resolve(name, &backend)?);
    }
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let run_id = format!("{stamp}-{}", std::process::id());
    let run_dir = out.join(&run_id);
    std::fs::create_dir_all(&run_dir)?;
    let version = std::process::Command::new(&celld)
        .arg("--version")
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .with_context(|| format!("run {} --version", celld.display()))?;
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(manifest_dir())
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string());
    let mut runs = Vec::new();
    let mut failed = false;
    for scenario in &scenarios {
        for attempt in 0..repeat {
            eprintln!(
                "celld-perf: {} (repeat {}/{repeat}) on {}",
                scenario.name,
                attempt + 1,
                backend.name()
            );
            let work = run_dir.join(format!("{}-{attempt}", scenario.name));
            let options = run::RunOptions {
                celld: celld.clone(),
                backend: backend.clone(),
                env: env.clone(),
                fixtures: manifest_dir().join("fixtures"),
                work: work.clone(),
                // It becomes a bucket prefix, which takes only [A-Za-z0-9-_./].
                run_id: format!("{run_id}-{}-{attempt}", scenario.name.to_lowercase())
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '-' {
                            c
                        } else {
                            '-'
                        }
                    })
                    .collect(),
                duration_scale: if quick { 0.2 } else { 1.0 },
                enforce_timing,
            };
            match run::run(scenario, &options).await {
                Ok(outcome) => {
                    failed |= outcome.failed;
                    let mut result = outcome.result;
                    result["repeat"] = json!(attempt);
                    runs.push(result);
                }
                Err(error) => {
                    failed = true;
                    eprintln!("celld-perf: {} failed: {error:#}", scenario.name);
                    runs.push(json!({
                        "name": scenario.name,
                        "repeat": attempt,
                        "failed": true,
                        "error": format!("{error:#}"),
                        "phases": [],
                    }));
                }
            }
            if !keep {
                // Keep the node logs; drop the project copy and node state.
                let _ = std::fs::remove_dir_all(work.join("project"));
                for index in 0..scenario.nodes {
                    let _ = std::fs::remove_dir_all(work.join(format!("node-{index}")));
                }
            }
        }
    }
    let result = json!({
        "schema": "celld-perf.result.v1",
        "run_id": run_id,
        "started_unix_s": stamp,
        "celld": {"path": celld, "version": version},
        "commit": commit,
        "backend": backend.name(),
        "env": env,
        "host": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "cpus": std::thread::available_parallelism().map(usize::from).unwrap_or(0),
        },
        "quick": quick,
        "runs": runs,
    });
    let path = run_dir.join("result.json");
    std::fs::write(&path, serde_json::to_string_pretty(&result)?)?;
    let mut stdout = std::io::stdout().lock();
    write!(stdout, "{}", report::summary(&result))?;
    writeln!(stdout, "\nresult: {}", path.display())?;
    Ok(if failed { 1 } else { 0 })
}

fn compare_command(arguments: &[String]) -> anyhow::Result<()> {
    let mut threshold = 0.05;
    let mut files = Vec::new();
    let mut arguments = arguments.iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--threshold" => {
                threshold = arguments
                    .next()
                    .ok_or_else(|| anyhow!("--threshold needs a value"))?
                    .parse()?;
            }
            file => files.push(PathBuf::from(file)),
        }
    }
    let [base, new] = files.as_slice() else {
        bail!("compare needs BASE.json and NEW.json");
    };
    let (text, regressed) = report::compare(&read_json(base)?, &read_json(new)?, threshold);
    print!("{text}");
    if regressed {
        std::process::exit(1);
    }
    Ok(())
}

/// Tens of thousands of sockets need as many descriptors.
fn raise_file_limit() {
    // SAFETY: getrlimit and setrlimit only read and write the struct.
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            // macOS refuses a soft limit above OPEN_MAX even when the hard
            // limit is unlimited.
            let want = limit.rlim_max.min(1 << 20);
            for candidate in [want, 1 << 18, 1 << 16, 10_240] {
                limit.rlim_cur = candidate.min(limit.rlim_max);
                if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == 0 {
                    break;
                }
            }
        }
    }
}
