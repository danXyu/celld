// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Log-linear histograms with the same layout as the node's
//! (`crates/celld/perf_stats.rs`): 16 buckets per power of two, so a client
//! histogram and a node's can be read, subtracted, and merged alike.

use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};

const SUB_BITS: u32 = 4;
const SUB: u64 = 1 << SUB_BITS;
pub const BUCKETS: usize = (64 - SUB_BITS as usize + 1) * SUB as usize;

pub fn index(value: u64) -> usize {
    if value < SUB {
        return value as usize;
    }
    let exponent = 63 - value.leading_zeros();
    let sub = (value >> (exponent - SUB_BITS)) & (SUB - 1);
    ((u64::from(exponent - SUB_BITS) + 1) * SUB + sub) as usize
}

pub fn lower_bound(index: usize) -> u64 {
    let index = index as u64;
    if index < SUB {
        return index;
    }
    (SUB + index % SUB) << (index / SUB - 1)
}

pub fn upper_bound(index: usize) -> u64 {
    if index + 1 >= BUCKETS {
        return u64::MAX;
    }
    lower_bound(index + 1) - 1
}

/// A histogram many tasks record into at once.
pub struct Recorder {
    counts: Vec<AtomicU64>,
    sum: AtomicU64,
    max: AtomicU64,
}

impl Default for Recorder {
    fn default() -> Self {
        Self {
            counts: (0..BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }
}

impl Recorder {
    pub fn record(&self, value: u64) {
        self.counts[index(value)].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Hist {
        Hist {
            counts: self
                .counts
                .iter()
                .map(|count| count.load(Ordering::Relaxed))
                .collect(),
            sum: self.sum.load(Ordering::Relaxed),
            max: self.max.load(Ordering::Relaxed),
        }
    }
}

/// A finished histogram.
#[derive(Clone, Debug, PartialEq)]
pub struct Hist {
    pub counts: Vec<u64>,
    pub sum: u64,
    pub max: u64,
}

impl Default for Hist {
    fn default() -> Self {
        Self {
            counts: vec![0; BUCKETS],
            sum: 0,
            max: 0,
        }
    }
}

impl Hist {
    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// The midpoint of the bucket holding quantile `q`.
    pub fn quantile(&self, q: f64) -> u64 {
        let total = self.count();
        if total == 0 {
            return 0;
        }
        let rank = ((q.clamp(0.0, 1.0) * total as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (index, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                let low = lower_bound(index);
                return low + (upper_bound(index) - low) / 2;
            }
        }
        self.max
    }

    pub fn mean(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            0.0
        } else {
            self.sum as f64 / count as f64
        }
    }

    pub fn merge(&mut self, other: &Hist) {
        for (mine, theirs) in self.counts.iter_mut().zip(&other.counts) {
            *mine += theirs;
        }
        self.sum += other.sum;
        self.max = self.max.max(other.max);
    }

    /// `self - earlier`, for two snapshots of one growing histogram. The
    /// max cannot be subtracted, so it is the later snapshot's.
    pub fn since(&self, earlier: &Hist) -> Hist {
        Hist {
            counts: self
                .counts
                .iter()
                .zip(&earlier.counts)
                .map(|(now, then)| now.saturating_sub(*then))
                .collect(),
            sum: self.sum.saturating_sub(earlier.sum),
            max: self.max,
        }
    }

    /// Read the node's JSON form: `{count, sum, max, buckets: [[i, n]]}`.
    pub fn from_node_json(value: &Value) -> Hist {
        let mut hist = Hist::default();
        if let Some(buckets) = value["buckets"].as_array() {
            for pair in buckets {
                let index = pair[0].as_u64().unwrap_or(0) as usize;
                if index < BUCKETS {
                    hist.counts[index] = pair[1].as_u64().unwrap_or(0);
                }
            }
        }
        hist.sum = value["sum"].as_u64().unwrap_or(0);
        hist.max = value["max"].as_u64().unwrap_or(0);
        hist
    }

    /// The summary a result carries, with raw buckets so results can be
    /// merged and compared later.
    pub fn to_json(&self) -> Value {
        let buckets: Vec<Value> = self
            .counts
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .map(|(index, count)| json!([index, count]))
            .collect();
        json!({
            "count": self.count(),
            "mean": self.mean().round(),
            "p50": self.quantile(0.50),
            "p90": self.quantile(0.90),
            "p99": self.quantile(0.99),
            "p999": self.quantile(0.999),
            "max": self.max,
            "sum": self.sum,
            "buckets": buckets,
        })
    }

    /// Read [`Self::to_json`] back.
    pub fn from_json(value: &Value) -> Hist {
        Self::from_node_json(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_contiguous() {
        let mut previous = None;
        for index in 0..BUCKETS {
            assert_eq!(super::index(lower_bound(index)), index);
            assert_eq!(super::index(upper_bound(index)), index);
            if let Some(previous) = previous {
                assert_eq!(lower_bound(index), previous + 1);
            }
            previous = Some(upper_bound(index));
        }
    }

    #[test]
    fn since_subtracts_and_quantiles_follow() {
        let recorder = Recorder::default();
        for value in 1..=100 {
            recorder.record(value);
        }
        let earlier = recorder.snapshot();
        for _ in 0..100 {
            recorder.record(10_000);
        }
        let delta = recorder.snapshot().since(&earlier);
        assert_eq!(delta.count(), 100);
        let p50 = delta.quantile(0.5);
        assert!((9_700..=10_300).contains(&p50), "{p50}");
        let round_trip = Hist::from_json(&delta.to_json());
        assert_eq!(round_trip.counts, delta.counts);
    }
}
