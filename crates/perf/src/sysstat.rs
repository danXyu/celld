// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Process samples: resident memory and cumulative CPU time, by pid.
//!
//! Linux reads `/proc`; other Unix systems ask `ps`, whose `time` column
//! carries centiseconds on macOS. CPU is cumulative, so a phase's usage is
//! the difference of two samples over the phase's length.

use std::process::Command;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Sample {
    pub rss_bytes: u64,
    pub cpu_seconds: f64,
    pub threads: u64,
    /// When the sample was taken, in seconds on the harness clock.
    pub at_s: f64,
}

fn now_s() -> f64 {
    crate::load::now_us() as f64 / 1e6
}

#[cfg(target_os = "linux")]
pub fn sample(pid: u32) -> Option<Sample> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let field = |name: &str| -> Option<u64> {
        status
            .lines()
            .find(|line| line.starts_with(name))?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()
    };
    let rss_bytes = field("VmRSS:")? * 1024;
    let threads = field("Threads:").unwrap_or(0);
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the parenthesized command name; utime and stime are
    // the 14th and 15th fields overall.
    let rest = &stat[stat.rfind(')')? + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let ticks: u64 = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    // SAFETY: sysconf has no preconditions.
    let hz = match unsafe { libc::sysconf(libc::_SC_CLK_TCK) } {
        value if value > 0 => value as f64,
        _ => 100.0,
    };
    Some(Sample {
        rss_bytes,
        cpu_seconds: ticks as f64 / hz,
        threads,
        at_s: now_s(),
    })
}

#[cfg(not(target_os = "linux"))]
pub fn sample(pid: u32) -> Option<Sample> {
    let output = Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let mut fields = text.split_whitespace();
    let rss_kib: u64 = fields.next()?.parse().ok()?;
    let cpu_seconds = parse_cpu_time(fields.next()?)?;
    Some(Sample {
        rss_bytes: rss_kib * 1024,
        cpu_seconds,
        threads: 0,
        at_s: now_s(),
    })
}

/// `ps` time: `[[dd-]hh:]mm:ss[.cc]`.
#[cfg_attr(target_os = "linux", allow(dead_code))]
pub fn parse_cpu_time(text: &str) -> Option<f64> {
    let (days, clock) = match text.split_once('-') {
        Some((days, clock)) => (days.parse::<f64>().ok()?, clock),
        None => (0.0, text),
    };
    let mut seconds = 0.0;
    for part in clock.split(':') {
        seconds = seconds * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(days * 86_400.0 + seconds)
}

/// The direct children of `pid` whose command line contains `pattern`,
/// for finding the node under `celld dev` among its other children.
pub fn children(pid: u32, pattern: &str) -> Vec<u32> {
    Command::new("pgrep")
        .args(["-P", &pid.to_string(), "-f", pattern])
        .output()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .split_whitespace()
                .filter_map(|pid| pid.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_time_parses_every_form() {
        assert_eq!(parse_cpu_time("0:01.25"), Some(1.25));
        assert_eq!(parse_cpu_time("01:02:03"), Some(3723.0));
        assert_eq!(parse_cpu_time("1-00:00:01"), Some(86_401.0));
        assert_eq!(parse_cpu_time("x"), None);
    }

    #[test]
    fn this_process_samples() {
        let sample = sample(std::process::id()).expect("sample self");
        assert!(sample.rss_bytes > 0);
    }
}
