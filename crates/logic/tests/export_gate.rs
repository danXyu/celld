//! The change exporter's barrier on the output gate, driven through
//! `on_event` with a shell that answers every activation effect at once.

use celld_logic::{
    on_event, CasOutcome, Channel, Config, Effect, Event, Failure, Phase, ProofSource,
    RequestError, RestoreOutcome, State,
};

const CELL: &str = "cell";

fn config() -> Config {
    Config {
        max_resident: 8,
        max_activations: 8,
        max_evictions: 1,
        max_releases: 1,
        max_outbound_websockets: 1,
        ownership_on_evict: Default::default(),
        require_node_lease: false,
        peer_protocol: 1,
        operation_deadline_ms: None,
        owner_log_recovery_backoff_ms: 0,
        owner_log_recovery_attempts: 1,
        alarm_resident_ms: 0,
        idle_evict_ms: None,
        pressure: Default::default(),
    }
}

/// Feed `event`, answer every activation effect with success, and return the
/// effects nothing answered.
fn drive(state: &mut State, event: Event) -> Vec<Effect> {
    let mut queue = vec![event];
    let mut left = Vec::new();
    while let Some(event) = queue.pop() {
        let effects = on_event(state, event);
        state.validate().expect("the core stays consistent");
        for effect in effects {
            match effect {
                Effect::ReadOwner { op, .. } => queue.push(Event::OwnerRead {
                    op,
                    now_ms: 0,
                    result: Ok(None),
                }),
                Effect::CasOwner { op, .. } => queue.push(Event::OwnerCasCompleted {
                    op,
                    result: Ok(CasOutcome::Applied),
                }),
                Effect::Restore { op, .. } => queue.push(Event::RestoreCompleted {
                    op,
                    result: Ok(RestoreOutcome {
                        restored: false,
                        alarm: None,
                    }),
                }),
                Effect::StartRuntime { op, .. } => queue.push(Event::RuntimeStarted {
                    op,
                    isolate: None,
                    generation: 0,
                    result: Ok(()),
                }),
                Effect::Publish { op, .. } => queue.push(Event::Published { op, result: Ok(()) }),
                Effect::ScheduleTimer { .. } | Effect::ReconcileWakeEntry { .. } => {}
                other => left.push(other),
            }
        }
    }
    left
}

fn resident_epoch(state: &State) -> Option<u64> {
    match state.phase(CELL) {
        Some(Phase::Resident { epoch }) => Some(*epoch),
        _ => None,
    }
}

/// A node with `CELL` resident at its epoch, and no request on it.
fn resident() -> (State, u64) {
    let mut state = State::new("node", config());
    let effects = drive(
        &mut state,
        Event::Request {
            request: 1,
            cell: CELL.to_string(),
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                request: 1,
                result: Ok(_)
            }
        )),
        "{effects:?}"
    );
    drive(&mut state, Event::ActivityFinished { request: 1 });
    let epoch = resident_epoch(&state).expect("the request left the cell resident");
    (state, epoch)
}

fn ticket(state: &mut State, epoch: u64, position: u64, ticket: u64) -> Vec<Effect> {
    drive(
        state,
        Event::ExportTicket {
            cell: CELL.to_string(),
            epoch,
            position,
            ticket,
        },
    )
}

fn await_op(effects: &[Effect]) -> u64 {
    match effects {
        [Effect::AwaitDurable { op, .. }] => *op,
        other => panic!("expected one proof, got {other:?}"),
    }
}

#[test]
fn a_fleet_proof_settles_the_ticket() {
    let (mut state, epoch) = resident();
    let op = await_op(&ticket(&mut state, epoch, 5, 7));
    let effects = drive(
        &mut state,
        Event::DurableReached {
            op,
            result: Ok(5),
            source: ProofSource::Fleet,
        },
    );
    assert_eq!(
        effects,
        [Effect::ExportProven {
            cell: CELL.to_string(),
            epoch,
            ticket: 7,
            result: Ok(()),
        }]
    );
}

#[test]
fn a_bucket_proof_waits_for_the_ownership_read() {
    let (mut state, epoch) = resident();
    let op = await_op(&ticket(&mut state, epoch, 5, 7));
    let effects = drive(
        &mut state,
        Event::DurableReached {
            op,
            result: Ok(6),
            source: ProofSource::Bucket,
        },
    );
    assert!(
        matches!(effects[..], [Effect::VerifyOwnership { .. }]),
        "{effects:?}"
    );
    let effects = drive(
        &mut state,
        Event::OwnershipVerified {
            op,
            result: Err(Failure::Definite),
        },
    );
    assert_eq!(
        effects,
        [Effect::ExportProven {
            cell: CELL.to_string(),
            epoch,
            ticket: 7,
            result: Err(RequestError::DurabilityUnproven),
        }]
    );
    // The exporter's failed proof leaves the cell serving.
    assert_eq!(resident_epoch(&state), Some(epoch));
}

#[test]
fn a_short_proof_fails_the_ticket_without_resetting_the_cell() {
    let (mut state, epoch) = resident();
    let op = await_op(&ticket(&mut state, epoch, 5, 7));
    let effects = drive(
        &mut state,
        Event::DurableReached {
            op,
            result: Ok(4),
            source: ProofSource::Fleet,
        },
    );
    assert!(matches!(
        effects[..],
        [Effect::ExportProven {
            ticket: 7,
            result: Err(RequestError::DurabilityUnproven),
            ..
        }]
    ));
    assert_eq!(resident_epoch(&state), Some(epoch));
}

#[test]
fn a_ticket_for_another_epoch_or_a_fenced_node_is_refused() {
    let (mut state, epoch) = resident();
    assert!(matches!(
        ticket(&mut state, epoch + 1, 5, 7)[..],
        [Effect::ExportProven {
            result: Err(RequestError::DurabilityUnproven),
            ..
        }]
    ));
    let op = await_op(&ticket(&mut state, epoch, 5, 8));
    let effects = drive(&mut state, Event::NodeFenced);
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::ExportProven {
                ticket: 8,
                result: Err(RequestError::NodeFenced),
                ..
            }
        )),
        "{effects:?}"
    );
    // A proof that lands after the fence is ignored.
    let late = drive(
        &mut state,
        Event::DurableReached {
            op,
            result: Ok(5),
            source: ProofSource::Fleet,
        },
    );
    assert!(late.is_empty(), "{late:?}");
    assert!(matches!(
        ticket(&mut state, epoch, 5, 9)[..],
        [Effect::ExportProven {
            result: Err(RequestError::NodeFenced),
            ..
        }]
    ));
}

#[test]
fn a_reader_does_not_wait_on_the_exporter() {
    let (mut state, epoch) = resident();
    await_op(&ticket(&mut state, epoch, 5, 7));
    drive(
        &mut state,
        Event::Request {
            request: 2,
            cell: CELL.to_string(),
        },
    );
    let effects = drive(
        &mut state,
        Event::Output {
            request: 2,
            channel: Channel::Response,
            position: None,
            observed: None,
            epoch: None,
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Release {
                request: 2,
                result: Ok(()),
                ..
            }
        )),
        "{effects:?}"
    );
}
