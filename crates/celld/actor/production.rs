// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The production selector for the Actor execution domain.

use super::*;
use futures_util::StreamExt as _;

/// How often the core samples its own timer lateness
/// (`loop.core_lag_us` in `/debug/metrics`).
const LAG_PROBE_MS: u64 = 20;

impl Actor {
    /// Runs the production start-select-step loop.
    ///
    /// This selector stays unbiased because production permits every
    /// ready-input order. Direct callers can use [`Actor::start`] and
    /// [`Actor::step`] without this raw Tokio select.
    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Message>) {
        let mut effects = FuturesUnordered::new();
        let mut timers = Timers::default();
        let mut wake: Option<(u64, crate::asyncrt::Sleep)> = None;
        let mut out = StepOutput::default();
        self.start(&mut out);
        drain_step_output(&mut out, &mut effects, &mut timers);
        // How late this thread runs a timer is how long a message can wait
        // in its mailbox. It observes and decides nothing.
        let mut lag_due_ms = crate::asyncrt::mono_ms().saturating_add(LAG_PROBE_MS);
        loop {
            let next_ms = timers.next_deadline_ms();
            if wake.as_ref().map(|(at_ms, _)| *at_ms) != next_ms {
                wake = next_ms.map(|at_ms| (at_ms, crate::asyncrt::sleep_until(at_ms)));
            }
            crate::asyncrt::select! {
                message = rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    self.step(ActorInput::Message(message), &mut out);
                }
                Some(completed) = effects.next(), if !effects.is_empty() => {
                    self.step(ActorInput::Completed(completed), &mut out);
                }
                () = until(&mut wake) => {
                    // Tokio ends a sleep past what `Instant` holds after about
                    // 30 years; dropping it re-arms for the time that remains.
                    wake = None;
                    if let Some(timer) = timers.pop_due(crate::asyncrt::mono_ms()) {
                        self.step(ActorInput::TimerFired(timer), &mut out);
                    }
                }
                _ = crate::asyncrt::sleep_until(lag_due_ms) => {
                    let now_us = crate::asyncrt::mono_us();
                    crate::perf_stats::record(
                        crate::perf_stats::Hist::CoreLag,
                        now_us.saturating_sub(lag_due_ms.saturating_mul(1_000)),
                    );
                    lag_due_ms = (now_us / 1_000).saturating_add(LAG_PROBE_MS);
                }
            }
            drain_step_output(&mut out, &mut effects, &mut timers);
        }
    }
}

async fn until(wake: &mut Option<(u64, crate::asyncrt::Sleep)>) {
    match wake {
        Some((at_ms, _)) if *at_ms <= crate::asyncrt::mono_ms() => {}
        Some((_, sleep)) => sleep.as_mut().await,
        None => std::future::pending().await,
    }
}
