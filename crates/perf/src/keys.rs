// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Cell names, and how a request picks one.

use crate::scenario::{Cells, Distribution};
use rand::Rng;
use std::time::Instant;

/// A sampler over one [`Cells`] set.
pub struct Keyspace {
    cells: Cells,
    /// For `zipf`: the cumulative probability of each rank.
    cdf: Vec<f64>,
    started: Instant,
}

impl Keyspace {
    pub fn new(cells: &Cells) -> Keyspace {
        let cdf = if cells.distribution == Distribution::Zipf {
            let mut total = 0.0;
            let mut cdf: Vec<f64> = (1..=cells.count)
                .map(|rank| {
                    total += 1.0 / (rank as f64).powf(cells.zipf_s);
                    total
                })
                .collect();
            for value in &mut cdf {
                *value /= total;
            }
            cdf
        } else {
            Vec::new()
        };
        Keyspace {
            cells: cells.clone(),
            cdf,
            started: Instant::now(),
        }
    }

    pub fn name(&self, index: usize) -> String {
        format!("{}-{index}", self.cells.prefix)
    }

    pub fn names(&self) -> impl Iterator<Item = String> + '_ {
        (0..self.cells.count).map(|index| self.name(index))
    }

    /// The index of the next cell a request uses.
    pub fn sample(&self, rng: &mut impl Rng) -> usize {
        let count = self.cells.count.max(1);
        match self.cells.distribution {
            Distribution::Uniform => rng.gen_range(0..count),
            Distribution::Zipf => {
                let point: f64 = rng.gen();
                self.cdf.partition_point(|&p| p < point).min(count - 1)
            }
            Distribution::Shifting => {
                let window = self.cells.window.unwrap_or(count).clamp(1, count);
                let offset =
                    (self.started.elapsed().as_secs_f64() * self.cells.shift_per_s) as usize;
                (offset + rng.gen_range(0..window)) % count
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells(distribution: Distribution) -> Cells {
        Cells {
            count: 1000,
            prefix: "c".into(),
            distribution,
            zipf_s: 0.99,
            window: Some(10),
            shift_per_s: 0.0,
        }
    }

    #[test]
    fn zipf_favours_low_ranks() {
        let keys = Keyspace::new(&cells(Distribution::Zipf));
        let mut rng = rand::thread_rng();
        let hits = (0..10_000).filter(|_| keys.sample(&mut rng) < 10).count();
        // The ten hottest of 1,000 cells carry about 39% of the load at s=0.99.
        assert!((3_000..5_000).contains(&hits), "{hits}");
    }

    #[test]
    fn shifting_stays_in_its_window() {
        let keys = Keyspace::new(&cells(Distribution::Shifting));
        let mut rng = rand::thread_rng();
        assert!((0..1_000).all(|_| keys.sample(&mut rng) < 10));
        assert_eq!(keys.name(7), "c-7");
    }
}
