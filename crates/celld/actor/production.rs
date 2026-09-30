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
        let mut delays = DelayQueue::new();
        let mut timer_slots = TimerSlots::<delay_queue::Key>::default();
        let mut out = StepOutput::default();
        self.start(&mut out);
        drain_step_output(&mut out, &mut effects, &mut delays, &mut timer_slots);
        // How late this thread runs a timer is how long a message can wait
        // in its mailbox. It observes and decides nothing.
        let mut lag_due_ms = crate::asyncrt::mono_ms().saturating_add(LAG_PROBE_MS);
        loop {
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
                Some(expired) = delays.next(), if !delays.is_empty() => {
                    let arm = expired.into_inner();
                    if arm.at_mono_ms > crate::asyncrt::mono_ms() {
                        // Only an arm longer than MAX_QUEUED_DELAY comes out
                        // before its deadline; queue it again for the rest.
                        if let Some(key) = timer_slots.armed_value_mut(&arm.slot, arm.ordinal) {
                            *key = delays.insert(arm.clone(), queued_delay(&arm));
                        }
                    } else if timer_slots.fire(&arm.slot, arm.ordinal).is_some() {
                        self.step(ActorInput::TimerFired(arm.timer), &mut out);
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
            drain_step_output(&mut out, &mut effects, &mut delays, &mut timer_slots);
        }
    }
}
