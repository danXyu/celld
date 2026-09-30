// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Benchmarks the decision core's cost per event.
//!
//! The core runs on one thread, so its cost for each event bounds the event
//! rate of a node. Every fixture is built through `on_event` by a shell that
//! answers each storage and runtime effect at once, so the timed state is one
//! the production executor can reach. Fixture construction is not timed.
//! The bench profile has no debug assertions, so `on_event` skips the
//! whole-state `validate` walk that debug builds run after every event.

use celld_logic::isolate::HeapId;
use celld_logic::pressure::{Load, PressureConfig};
use celld_logic::rebalance;
use celld_logic::{
    on_event, CapacityPeer, CasOutcome, Channel, Config, Effect, Event, Failure, NodeLeaseRecord,
    OwnerRecord, OwnershipOnEvict, Phase, ProofSource, RestoreOutcome, Route, State,
};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::hint::black_box;
use std::time::Duration;

const NODE: &str = "node";
const PEER: &str = "peer";
const PEER_ADDR: &str = "10.0.0.2:8080";
/// Placements spread cells over this many V8 heaps, so the pressure walk
/// groups them the way a loaded node's cells are grouped.
const HEAPS: u64 = 64;

fn config(max_resident: usize, ownership_on_evict: OwnershipOnEvict) -> Config {
    Config {
        max_resident,
        max_activations: 64,
        max_evictions: 1,
        max_releases: 1,
        max_outbound_websockets: 1,
        ownership_on_evict,
        require_node_lease: false,
        peer_protocol: 1,
        operation_deadline_ms: None,
        owner_log_recovery_backoff_ms: 0,
        owner_log_recovery_attempts: 1,
        alarm_resident_ms: 0,
        idle_evict_ms: None,
        pressure: PressureConfig {
            high_bytes: Some(GIB),
            rss_hard_bytes: None,
        },
    }
}

const GIB: u64 = 1 << 30;

fn cell_name(index: usize) -> String {
    format!("bench:{index:08}")
}

/// A shell whose storage and runtime answer every effect at once.
///
/// Cells in `remote` are owned by a live peer; every other cell is unowned
/// and this node claims it.
struct Shell {
    state: State,
    next_request: u64,
    remote: fn(&str) -> bool,
}

impl Shell {
    fn new(config: Config, remote: fn(&str) -> bool) -> Self {
        Self {
            state: State::new(NODE, config),
            next_request: 1,
            remote,
        }
    }

    fn request_id(&mut self) -> u64 {
        let id = self.next_request;
        self.next_request += 1;
        id
    }

    /// Feeds `event`, answers every effect the shell can settle at once, and
    /// returns the effects addressed to a caller.
    fn drive(&mut self, event: Event) -> Vec<Effect> {
        let mut queue = VecDeque::from([event]);
        let mut left = Vec::new();
        while let Some(event) = queue.pop_front() {
            for effect in on_event(&mut self.state, event) {
                match effect {
                    Effect::ReadOwner { op, cell } => {
                        let record = (self.remote)(&cell).then(|| OwnerRecord {
                            node: Some(PEER.to_string()),
                            epoch: 1,
                            etag: "peer-etag".to_string(),
                        });
                        queue.push_back(Event::OwnerRead {
                            op,
                            now_ms: 0,
                            result: Ok(record),
                        });
                    }
                    Effect::ReadNodeLease { op, .. } => queue.push_back(Event::NodeLeaseRead {
                        op,
                        now_ms: 0,
                        result: Ok(Some(NodeLeaseRecord {
                            node: PEER.to_string(),
                            addr: PEER_ADDR.to_string(),
                            expires_ms: u64::MAX / 4,
                            peer_protocol: 1,
                            generation: "peer-generation".to_string(),
                            log_state: None,
                            etag: "peer-lease".to_string(),
                        })),
                    }),
                    Effect::CasOwner { op, .. } => queue.push_back(Event::OwnerCasCompleted {
                        op,
                        result: Ok(CasOutcome::Applied),
                    }),
                    Effect::Restore { op, .. } => queue.push_back(Event::RestoreCompleted {
                        op,
                        result: Ok(RestoreOutcome {
                            restored: false,
                            alarm: None,
                        }),
                    }),
                    Effect::StartRuntime { op, .. } => {
                        // Uneven heap sizes, so the pressure walk's
                        // emptiest-heap ordering has something to decide.
                        let heap = (op * op) % HEAPS;
                        queue.push_back(Event::RuntimeStarted {
                            op,
                            isolate: Some(HeapId::new(heap)),
                            generation: 0,
                            result: Ok(()),
                        })
                    }
                    Effect::Publish { op, .. } => {
                        queue.push_back(Event::Published { op, result: Ok(()) })
                    }
                    Effect::EnsureDurable { op, .. } => {
                        queue.push_back(Event::DurabilityChecked { op, result: Ok(()) })
                    }
                    Effect::StopRuntime { op, .. } => queue.push_back(Event::RuntimeStopped { op }),
                    Effect::ScheduleTimer { .. } | Effect::ReconcileWakeEntry { .. } => {}
                    other => left.push(other),
                }
            }
        }
        left
    }

    /// Routes one request to `cell` and finishes its activity, leaving the
    /// cell idle. Returns the route.
    fn touch(&mut self, cell: &str) -> Route {
        let request = self.request_id();
        let effects = self.drive(Event::Request {
            request,
            cell: cell.to_string(),
        });
        let route = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Complete {
                    request: done,
                    result: Ok(route),
                } if *done == request => Some(route.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("request to {cell} did not complete: {effects:?}"));
        if route == Route::Local {
            self.drive(Event::ActivityFinished { request });
        }
        route
    }
}

/// Even-numbered cells are resident here; odd-numbered cells are owned by a
/// live peer and cached as remote routes.
fn odd_is_remote(cell: &str) -> bool {
    cell.as_bytes().last().is_some_and(|digit| digit % 2 == 1)
}

fn never_remote(_: &str) -> bool {
    false
}

/// `known` cells: half resident here, half routed to a peer.
fn mixed_fixture(known: usize) -> (Shell, Vec<String>, Vec<String>) {
    let mut shell = Shell::new(
        config(known.div_ceil(2) + 1, OwnershipOnEvict::Release),
        odd_is_remote,
    );
    let (mut local, mut remote) = (Vec::new(), Vec::new());
    for index in 0..known {
        let name = cell_name(index);
        match shell.touch(&name) {
            Route::Local => local.push(name),
            Route::Remote { .. } => remote.push(name),
        }
    }
    assert_eq!(shell.state.owned_cells(), local.len());
    assert_eq!(local.len() + remote.len(), known);
    for name in local.iter().take(4) {
        assert!(matches!(
            shell.state.phase(name),
            Some(Phase::Resident { .. })
        ));
        assert!(shell.state.is_hibernatable(name));
    }
    for name in remote.iter().take(4) {
        assert!(matches!(
            shell.state.phase(name),
            Some(Phase::Remote { .. })
        ));
    }
    (shell, local, remote)
}

/// A deterministic walk over `len` cells that does not visit neighbours in
/// order, so large tables pay for their cache misses.
struct Stride {
    at: usize,
    len: usize,
}

impl Stride {
    fn new(len: usize) -> Self {
        Self { at: 0, len }
    }

    fn next(&mut self) -> usize {
        // 7919 is prime, so the walk visits every cell before it repeats
        // unless `len` is a multiple of it.
        self.at = (self.at + 7919) % self.len;
        self.at
    }
}

fn expect_release(effects: &[Effect], request: u64) {
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Release { request: r, result: Ok(()), .. } if *r == request
        )),
        "{effects:?}"
    );
}

/// Warm request routing and the output gate against 10, 10k and 1M known
/// cells, half resident here and half cached as routes to a peer. A resident
/// request is `Request` then `ActivityFinished`; the remote route is one
/// `Request` answered from the cached peer lease; a read output releases at
/// once; a write output opens a barrier that a fleet proof settles. Each
/// element is one request; the cell and request id are made untimed.
/// `cargo test` runs each case once, unoptimized, as CI's smoke check. The
/// largest fixtures take minutes to build there, so the smoke check uses
/// smaller ones; `cargo bench` builds optimized and uses every size.
const SMOKE: bool = cfg!(debug_assertions);

fn core_request(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_request");
    group.throughput(Throughput::Elements(1));
    let sizes: &[usize] = if SMOKE {
        &[10, 1_000]
    } else {
        &[10, 10_000, 1_000_000]
    };
    for &known in sizes {
        let (shell, local, remote) = mixed_fixture(known);
        let shell = RefCell::new(shell);

        // Correctness before timing: one of each cycle settles as expected.
        {
            let mut shell = shell.borrow_mut();
            assert_eq!(shell.touch(&local[0]), Route::Local);
            assert!(matches!(shell.touch(&remote[0]), Route::Remote { .. }));
            let request = shell.request_id();
            shell.drive(Event::Request {
                request,
                cell: local[0].clone(),
            });
            let effects = shell.drive(Event::Output {
                request,
                channel: Channel::Response,
                position: Some(1),
                observed: None,
                epoch: None,
            });
            let [Effect::AwaitDurable { op, .. }] = effects[..] else {
                panic!("a write opens one barrier: {effects:?}");
            };
            let effects = shell.drive(Event::DurableReached {
                op,
                result: Ok(1),
                source: ProofSource::Fleet,
            });
            expect_release(&effects, request);
            shell.drive(Event::ActivityFinished { request });
        }

        let mut stride = Stride::new(local.len());
        group.bench_function(BenchmarkId::new("local", known), |b| {
            b.iter_batched(
                || {
                    let mut shell = shell.borrow_mut();
                    (shell.request_id(), local[stride.next()].clone())
                },
                |(request, cell)| {
                    let state = &mut shell.borrow_mut().state;
                    black_box(on_event(state, Event::Request { request, cell }));
                    black_box(on_event(state, Event::ActivityFinished { request }));
                },
                BatchSize::SmallInput,
            )
        });

        let mut stride = Stride::new(remote.len());
        group.bench_function(BenchmarkId::new("remote", known), |b| {
            b.iter_batched(
                || {
                    let mut shell = shell.borrow_mut();
                    (shell.request_id(), remote[stride.next()].clone())
                },
                |(request, cell)| {
                    let state = &mut shell.borrow_mut().state;
                    black_box(on_event(state, Event::Request { request, cell }))
                },
                BatchSize::SmallInput,
            )
        });

        let mut stride = Stride::new(local.len());
        group.bench_function(BenchmarkId::new("output_read", known), |b| {
            b.iter_batched(
                || {
                    let mut shell = shell.borrow_mut();
                    (shell.request_id(), local[stride.next()].clone())
                },
                |(request, cell)| {
                    let state = &mut shell.borrow_mut().state;
                    black_box(on_event(state, Event::Request { request, cell }));
                    black_box(on_event(
                        state,
                        Event::Output {
                            request,
                            channel: Channel::Response,
                            position: None,
                            observed: None,
                            epoch: None,
                        },
                    ));
                    black_box(on_event(state, Event::ActivityFinished { request }));
                },
                BatchSize::SmallInput,
            )
        });

        let mut stride = Stride::new(local.len());
        let mut position = 1;
        group.bench_function(BenchmarkId::new("output_write", known), |b| {
            b.iter_batched(
                || {
                    let mut shell = shell.borrow_mut();
                    position += 1;
                    (shell.request_id(), local[stride.next()].clone(), position)
                },
                |(request, cell, position)| {
                    let state = &mut shell.borrow_mut().state;
                    black_box(on_event(state, Event::Request { request, cell }));
                    let effects = on_event(
                        state,
                        Event::Output {
                            request,
                            channel: Channel::Response,
                            position: Some(position),
                            observed: None,
                            epoch: None,
                        },
                    );
                    let Some(Effect::AwaitDurable { op, .. }) = effects.first() else {
                        unreachable!("checked before timing")
                    };
                    black_box(on_event(
                        state,
                        Event::DurableReached {
                            op: *op,
                            result: Ok(position),
                            source: ProofSource::Fleet,
                        },
                    ));
                    black_box(on_event(state, Event::ActivityFinished { request }));
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

/// Rebalance planning for a fleet of 10 nodes with 100k cells on this node.
///
/// `plan` is the fleet arithmetic the executor runs on each capacity sample.
/// `select` is `Event::Rebalance` choosing one dormant cell to give away from
/// 100k dormant cells. The untimed setup fails the previous release
/// ambiguously, which returns that cell to `Dormant`, so every timed event
/// walks and sorts the same table and releases exactly one cell.
fn rebalance(c: &mut Criterion) {
    const NODES: usize = 10;
    const CELLS: usize = 100_000;
    // The fixture's surplus depends on 100k owned cells per peer, and an
    // unoptimized build takes tens of minutes to make them; CI smoke-tests
    // this target with `cargo bench -- --test`, which is optimized.
    if SMOKE {
        return;
    }
    let mut group = c.benchmark_group("rebalance");

    let peers: Vec<CapacityPeer> = (0..NODES)
        .map(|index| CapacityPeer {
            node: if index == 0 {
                NODE.to_string()
            } else {
                format!("peer-{index}")
            },
            addr: format!("10.0.0.{index}:8080"),
            expires_ms: 60_000,
            peer_protocol: 1,
            sampled_ms: 1_000,
            // This node holds the most; the rest are spread below the mean.
            owned_cells: Some(if index == 0 {
                CELLS
            } else {
                CELLS / 2 + index * 1_000
            }),
            placement_weight: Some(1),
            bucket_format: None,
            resident_cells: 0,
            host_websockets: 0,
            rss_bytes: 0,
            in_use_bytes: None,
            pressured: false,
            memory_headroom: Some(true),
            restoring: 0,
            paced_handoff: true,
            rebalance_paused: false,
            draining: false,
        })
        .collect();
    let surplus = rebalance::surplus(&peers, NODE, 2_000, 10_000, 0, 64);
    assert_eq!(surplus, Some(64));
    assert_eq!(
        rebalance::receivers(&peers, NODE, 2_000, 10_000).len(),
        NODES - 1
    );
    group.throughput(Throughput::Elements(NODES as u64));
    group.bench_function(BenchmarkId::new("plan", format!("{NODES}_nodes")), |b| {
        b.iter(|| {
            let peers = black_box(&peers);
            (
                rebalance::surplus(peers, NODE, 2_000, 10_000, 0, 64),
                rebalance::receivers(peers, NODE, 2_000, 10_000),
            )
        })
    });

    let mut shell = Shell::new(config(CELLS + 1, OwnershipOnEvict::Sticky), never_remote);
    for index in 0..CELLS {
        let name = cell_name(index);
        assert_eq!(shell.touch(&name), Route::Local);
        let left = shell.drive(Event::Evict { cell: name.clone() });
        assert!(left.is_empty(), "{left:?}");
        assert!(matches!(
            shell.state.phase(&name),
            Some(Phase::Dormant { .. })
        ));
    }
    assert_eq!(shell.state.owned_cells(), CELLS);
    let release = |effects: Vec<Effect>| match effects[..] {
        [Effect::ReleaseOwner { op, .. }] => op,
        _ => panic!("one dormant cell is released: {effects:?}"),
    };
    let pending = RefCell::new(Some(release(shell.drive(Event::Rebalance { cells: 1 }))));
    let shell = RefCell::new(shell);
    group.throughput(Throughput::Elements(CELLS as u64));
    group.bench_function(
        BenchmarkId::new("select", format!("{CELLS}_dormant")),
        |b| {
            b.iter_batched(
                || {
                    if let Some(op) = pending.borrow_mut().take() {
                        let left = shell.borrow_mut().drive(Event::OwnerReleased {
                            op,
                            result: Err(Failure::Ambiguous),
                        });
                        assert!(left.is_empty(), "{left:?}");
                    }
                },
                |()| {
                    let effects =
                        on_event(&mut shell.borrow_mut().state, Event::Rebalance { cells: 1 });
                    if let [Effect::ReleaseOwner { op, .. }] = effects[..] {
                        *pending.borrow_mut() = Some(op);
                    }
                    effects
                },
                BatchSize::PerIteration,
            )
        },
    );
    group.finish();
}

/// Pressure victim selection over 10k idle resident cells spread across
/// heaps. The timed event is a load sample over the memory ceiling: it
/// latches shedding and nominates one victim, which scans every cell. The
/// untimed setup clears the latch with a low sample and rescues the victim
/// with a request, so each sample starts from the same unlatched node.
fn pressure(c: &mut Criterion) {
    const CELLS: usize = if SMOKE { 1_000 } else { 10_000 };
    let mut group = c.benchmark_group("pressure");
    let mut shell = Shell::new(config(CELLS + 1, OwnershipOnEvict::Release), never_remote);
    for index in 0..CELLS {
        assert_eq!(shell.touch(&cell_name(index)), Route::Local);
    }
    let load = |in_use_bytes: u64| Load {
        resident_cells: CELLS,
        rss_bytes: in_use_bytes,
        in_use_bytes,
        cgroup_working_set_bytes: None,
        cgroup_current_bytes: None,
        container_reserved_bytes: 0,
    };
    let victim = |effects: &[Effect]| match effects {
        [Effect::EnsureDurable { cell, .. }] => cell.clone(),
        _ => panic!("one victim is nominated: {effects:?}"),
    };

    let mut now_mono_ms = 1;
    let first = on_event(
        &mut shell.state,
        Event::LoadSampled {
            load: load(2 * GIB),
            now_mono_ms,
        },
    );
    assert!(shell.state.shedding());
    let pending = RefCell::new(Some(victim(&first)));
    let shell = RefCell::new(shell);
    group.throughput(Throughput::Elements(CELLS as u64));
    group.bench_function(
        BenchmarkId::new("victim", format!("{CELLS}_resident")),
        |b| {
            b.iter_batched(
                || {
                    now_mono_ms += 1;
                    let mut shell = shell.borrow_mut();
                    let left = shell.drive(Event::LoadSampled {
                        load: load(GIB / 2),
                        now_mono_ms,
                    });
                    assert!(left.is_empty(), "{left:?}");
                    assert!(!shell.state.shedding());
                    if let Some(cell) = pending.borrow_mut().take() {
                        assert_eq!(shell.touch(&cell), Route::Local);
                    }
                    now_mono_ms
                },
                |now_mono_ms| {
                    let effects = on_event(
                        &mut shell.borrow_mut().state,
                        Event::LoadSampled {
                            load: load(2 * GIB),
                            now_mono_ms,
                        },
                    );
                    if let [Effect::EnsureDurable { cell, .. }] = &effects[..] {
                        *pending.borrow_mut() = Some(cell.clone());
                    }
                    effects
                },
                BatchSize::PerIteration,
            )
        },
    );
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = core_request, rebalance, pressure
}
criterion_main!(benches);
