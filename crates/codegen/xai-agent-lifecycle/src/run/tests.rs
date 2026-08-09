use super::*;
use std::sync::{Arc, Barrier, Mutex};

fn command(value: &str) -> CommandId {
    CommandId::new(value).unwrap()
}

fn run_id() -> RunId {
    RunId::new("run_test").unwrap()
}

fn session() -> SessionRef {
    SessionRef::new("session_test").unwrap()
}

fn budget() -> ResourceVector {
    ResourceVector::default()
        .iterations(20)
        .agent_calls(20)
        .agent_concurrency(4)
        .active_ms(1_000_000)
        .wall_ms(1_000_000)
        .tokens(1_000_000)
        .cost_micros(10_000_000)
        .artifact_bytes(10_000_000)
}

fn capabilities() -> CapabilityPolicy {
    let values = [
        "session.turn".to_owned(),
        "workflow.execute".to_owned(),
        "agent.spawn".to_owned(),
        "gate.execute".to_owned(),
        "artifact.write".to_owned(),
        "external.test".to_owned(),
    ];
    CapabilityPolicy::new(values.clone(), values.clone(), values)
}

fn create_request() -> CreateRunRequest {
    CreateRunRequest::new(
        command("create"),
        session(),
        GoalSpec::new("finish the durable task"),
        RunDriverSpec::AutonomousTurnLoop {
            session: session(),
            strategy_revision: 0,
        },
        capabilities(),
        budget(),
    )
    .run_id(run_id())
}

fn artifact(label: Option<&str>, workspace: Option<&str>) -> ArtifactRef {
    let mut reference =
        ArtifactRef::new("a".repeat(64), "application/json", 10, "test", "run_test");
    if let Some(label) = label {
        reference = reference.evidence_labels([label.to_owned()]);
    }
    if let Some(workspace) = workspace {
        reference = reference.workspace_digest(workspace);
    }
    reference
}

fn controller() -> (tempfile::TempDir, RunController<LocalRunStore>) {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalRunStore::new(directory.path()).unwrap();
    (directory, RunController::open(store).unwrap())
}

fn create(controller: &mut RunController<LocalRunStore>) -> RunCommandResult {
    controller.create_run(create_request(), 1).unwrap()
}

fn begin_iteration(
    controller: &mut RunController<LocalRunStore>,
    run: &RunEnvelope,
) -> CommandOutput<IterationHandle> {
    let context = IterationContextManifest::new(
        run.run.revision,
        run.run.current_strategy_revision,
        run.run.verifier_policy_digest.clone(),
        "model-v1",
        "workspace-v1",
    );
    controller
        .begin_iteration(
            MutationRequest::new(
                run.run.id.clone(),
                run.run.revision,
                command("begin_iteration"),
                BeginIteration::new(context),
            ),
            2,
        )
        .unwrap()
}

#[test]
fn golden_v1_round_trips_and_future_values_fail_closed() {
    let fixture = include_str!("fixtures/run-envelope-v1.json");
    let expected: serde_json::Value = serde_json::from_str(fixture).unwrap();
    let envelope: RunEnvelope = serde_json::from_str(fixture).unwrap();
    envelope.validate().unwrap();
    assert_eq!(serde_json::to_value(&envelope).unwrap(), expected);

    let mut future_status = expected.clone();
    future_status["run"]["status"] = serde_json::json!("future_running_state");
    let envelope: RunEnvelope = serde_json::from_value(future_status).unwrap();
    assert_eq!(envelope.run.status, RunStatus::RecoveryRequired);

    let mut future_driver = expected.clone();
    future_driver["run"]["driver"] = serde_json::json!({"type":"future_driver"});
    assert!(serde_json::from_value::<RunEnvelope>(future_driver).is_err());

    let mut future_schema = expected;
    future_schema["schema_version"] = serde_json::json!(RUN_SCHEMA_VERSION + 1);
    assert!(serde_json::from_value::<RunEnvelope>(future_schema).is_err());

    assert_eq!(
        serde_json::from_value::<RunLifecycle>(serde_json::json!({
            "state":"future_lifecycle",
            "detail":{"future":"payload"}
        }))
        .unwrap(),
        RunLifecycle::Recovering
    );

    assert_eq!(
        serde_json::from_str::<EffectClass>("\"future_class\"").unwrap(),
        EffectClass::NonRepeatable
    );
    assert_ne!(
        std::any::TypeId::of::<RunId>(),
        std::any::TypeId::of::<SessionRef>()
    );
    assert_ne!(
        std::any::TypeId::of::<RunEventCursor>(),
        std::any::TypeId::of::<RunRevision>()
    );
    assert!(serde_json::from_str::<RunId>("\"bad/id\"").is_err());
    assert!(serde_json::from_str::<RunEventKind>("\"bad/event\"").is_err());
}

#[test]
fn durable_session_receipts_are_bound_to_exact_turn_intent() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("receipt_validation").unwrap();
    let turn_id = "receipt_validation_turn";
    let prompt_digest = format!("sha256-v2:{}", "b".repeat(64));
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("prepare_receipt_validation"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::Reconcilable,
                    EffectSpec::SessionTurn {
                        session: session(),
                        turn_id: turn_id.into(),
                        prompt_digest: prompt_digest.clone(),
                        input: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    let mut malformed = prepared.snapshot.clone();
    let operation = malformed.run.operations.get_mut(&operation_id).unwrap();
    operation.state = OperationState::Acknowledged;
    operation.receipt = Some(EffectReceipt::new("fabricated"));
    assert!(malformed.validate().is_err());

    let mut cross_session = prepared.snapshot.clone();
    let operation = cross_session.run.operations.get_mut(&operation_id).unwrap();
    let EffectSpec::SessionTurn {
        session: operation_session,
        ..
    } = &mut operation.spec
    else {
        unreachable!("test operation is a Session Turn");
    };
    *operation_session = SessionRef::new("different_session").unwrap();
    operation.spec_digest = operation.spec.digest().unwrap();
    assert!(
        serde_json::from_value::<RunEnvelope>(serde_json::to_value(cross_session).unwrap())
            .is_err()
    );

    let valid = EffectReceipt::for_session_turn(
        &session(),
        turn_id,
        &prompt_digest,
        0,
        SessionTurnOutcome::End,
    );
    let epoch = malformed.run.controller_epoch;
    let operation = malformed.run.operations.get_mut(&operation_id).unwrap();
    operation.receipt = Some(valid.clone());
    operation.active_attempt = Some(OperationAttempt {
        attempt: 1,
        token: DispatchToken::new("receipt_validation_token").unwrap(),
        epoch,
    });
    operation.terminal_result_digest = Some("c".repeat(64));
    malformed.validate().unwrap();

    let mut wrong_settlement = malformed;
    wrong_settlement
        .run
        .operations
        .get_mut(&operation_id)
        .unwrap()
        .receipt
        .as_mut()
        .unwrap()
        .settlement_id = Some("sha256:wrong".into());
    assert!(wrong_settlement.validate().is_err());
    let expected = session_turn_settlement_id(
        &session(),
        turn_id,
        &prompt_digest,
        0,
        SessionTurnOutcome::End,
    );
    assert_eq!(valid.settlement_id.as_deref(), Some(expected.as_str()));
}

#[test]
fn legacy_active_and_unknown_state_require_recovery() {
    for status in ["Active", "future_state"] {
        let envelope = migrate_legacy_goal(
            &serde_json::json!({
                "goal_id":"legacy",
                "objective":"recover safely",
                "status":status
            }),
            SessionRef::new("legacy_session").unwrap(),
            1,
        )
        .unwrap();
        assert_eq!(envelope.run.status, RunStatus::RecoveryRequired);
        assert_eq!(envelope.run.stage, RunStage::Recovering);
    }
}

#[test]
fn unwired_run_drivers_fail_at_creation() {
    let (_directory, mut controller) = controller();
    let mut request = create_request();
    request.driver = RunDriverSpec::RhaiWorkflow {
        session: session(),
        workflow_name: "future_workflow".into(),
        workflow_revision: 1,
        args_digest: "a".repeat(64),
    };
    assert!(matches!(
        controller.create_run(request, 1),
        Err(RunError::Validation(message))
            if message.contains("only AutonomousTurnLoop is executable")
    ));
}

#[test]
fn duplicate_command_returns_original_receipt_without_advancing_revision() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let request = MutationRequest::new(
        run_id(),
        created.snapshot.run.revision,
        command("pause"),
        RunAction::Pause,
    );
    let first = controller.control_run(request.clone(), 2).unwrap();
    let duplicate = controller.control_run(request, 3).unwrap();
    assert!(!first.duplicate);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.receipt, first.receipt);
    assert_eq!(duplicate.snapshot.run.revision, first.snapshot.run.revision);
}

#[test]
fn sqlite_cas_allows_only_one_stale_writer() {
    let (directory, mut controller) = controller();
    let created = create(&mut controller);
    let mut first = created.snapshot.clone();
    first.run.revision = RunRevision::new(2);
    first.run.status = RunStatus::UserPaused;
    let mut second = created.snapshot.clone();
    second.run.revision = RunRevision::new(2);
    second.run.status = RunStatus::Blocked;

    let store_a = LocalRunStore::new(directory.path()).unwrap();
    let store_b = LocalRunStore::new(directory.path()).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let spawn = |store: LocalRunStore, next: RunEnvelope, barrier: Arc<Barrier>| {
        std::thread::spawn(move || {
            barrier.wait();
            store
                .commit(StoreCommit {
                    run_id: run_id(),
                    expected_revision: Some(RunRevision::new(1)),
                    next,
                    finished_iteration: None,
                })
                .unwrap()
        })
    };
    let a = spawn(store_a, first, barrier.clone());
    let b = spawn(store_b, second, barrier.clone());
    barrier.wait();
    let results = [a.join().unwrap(), b.join().unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == StoreCommitResult::Applied)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, StoreCommitResult::Conflict { .. }))
            .count(),
        1
    );
}

#[derive(Clone, Copy)]
enum FailureMode {
    Healthy,
    BeforeCommit,
    UnknownCommit,
    UnknownCommitApplied,
}

#[derive(Clone)]
struct ScriptedStore {
    state: Arc<Mutex<Option<RunEnvelope>>>,
    mode: Arc<Mutex<FailureMode>>,
}

impl ScriptedStore {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
            mode: Arc::new(Mutex::new(FailureMode::Healthy)),
        }
    }

    fn fail(&self, mode: FailureMode) {
        *self.mode.lock().unwrap() = mode;
    }
}

impl RunStore for ScriptedStore {
    fn load(&self, _run_id: &RunId) -> Result<Option<RunEnvelope>, RunError> {
        Ok(self.state.lock().unwrap().clone())
    }

    fn list(&self) -> Result<Vec<RunEnvelope>, RunError> {
        Ok(self.state.lock().unwrap().clone().into_iter().collect())
    }

    fn commit(&self, commit: StoreCommit) -> Result<StoreCommitResult, RunError> {
        let mode = *self.mode.lock().unwrap();
        match mode {
            FailureMode::BeforeCommit => {
                return Err(RunError::Storage("injected pre-commit failure".into()));
            }
            FailureMode::UnknownCommit => {
                return Ok(StoreCommitResult::CommitUnknown(
                    "injected acknowledgement loss".into(),
                ));
            }
            FailureMode::Healthy | FailureMode::UnknownCommitApplied => {}
        }
        let mut state = self.state.lock().unwrap();
        let actual = state.as_ref().map(|value| value.run.revision);
        if actual != commit.expected_revision {
            return Ok(StoreCommitResult::Conflict { actual });
        }
        *state = Some(commit.next);
        if matches!(mode, FailureMode::UnknownCommitApplied) {
            Ok(StoreCommitResult::CommitUnknown(
                "injected acknowledgement loss after commit".into(),
            ))
        } else {
            Ok(StoreCommitResult::Applied)
        }
    }
}

#[test]
fn applied_but_unacknowledged_commit_reloads_and_deduplicates_without_reapplying() {
    let store = ScriptedStore::new();
    let mut controller = RunController::open(store.clone()).unwrap();
    let created = controller.create_run(create_request(), 1).unwrap();
    let pause = MutationRequest::new(
        run_id(),
        created.snapshot.run.revision,
        command("unknown_applied_pause"),
        RunAction::Pause,
    );
    store.fail(FailureMode::UnknownCommitApplied);
    assert!(matches!(
        controller.control_run(pause.clone(), 2),
        Err(RunError::CommitUnknown(_))
    ));
    assert_eq!(
        controller.get_run(&run_id()).unwrap().run.revision,
        created.snapshot.run.revision
    );

    store.fail(FailureMode::Healthy);
    let loaded = controller.reload_run(&run_id()).unwrap().unwrap();
    assert_eq!(loaded.run.status, RunStatus::UserPaused);
    assert_eq!(loaded.run.revision, RunRevision::new(2));
    let recovery = controller
        .begin_recovery(
            MutationRequest::new(
                run_id(),
                loaded.run.revision,
                command("unknown_applied_recover"),
                (),
            ),
            3,
        )
        .unwrap();
    let duplicate = controller.control_run(pause, 4).unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(
        duplicate.snapshot.run.revision,
        recovery.snapshot.run.revision
    );
    assert_eq!(duplicate.receipt.committed_revision, RunRevision::new(2));
}

#[test]
fn cache_changes_only_after_acknowledged_commit_and_unknown_commit_fences_authority() {
    for mode in [FailureMode::BeforeCommit, FailureMode::UnknownCommit] {
        let store = ScriptedStore::new();
        let mut controller = RunController::open(store.clone()).unwrap();
        let created = controller.create_run(create_request(), 1).unwrap();
        store.fail(mode);
        let result = controller.control_run(
            MutationRequest::new(
                run_id(),
                created.snapshot.run.revision,
                command("pause_after_failure"),
                RunAction::Pause,
            ),
            2,
        );
        assert!(result.is_err());
        assert_eq!(
            controller.get_run(&run_id()).unwrap().run.revision,
            RunRevision::new(1)
        );
        if matches!(mode, FailureMode::UnknownCommit) {
            assert_eq!(
                controller
                    .control_run(
                        MutationRequest::new(
                            run_id(),
                            RunRevision::new(1),
                            command("after_unknown"),
                            RunAction::Pause,
                        ),
                        3,
                    )
                    .unwrap_err(),
                RunError::AuthorityLost
            );
            assert_eq!(
                controller
                    .begin_recovery(
                        MutationRequest::new(
                            run_id(),
                            RunRevision::new(1),
                            command("recover_without_reload"),
                            (),
                        ),
                        4,
                    )
                    .unwrap_err(),
                RunError::ReloadRequired
            );
            store.fail(FailureMode::Healthy);
            controller.reload_run(&run_id()).unwrap();
            controller
                .begin_recovery(
                    MutationRequest::new(
                        run_id(),
                        RunRevision::new(1),
                        command("recover_after_reload"),
                        (),
                    ),
                    5,
                )
                .unwrap();
        }
    }
}

#[test]
fn restart_never_replays_a_duplicate_claim_handle() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalRunStore::new(directory.path()).unwrap();
    let mut controller = RunController::open(store.clone()).unwrap();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("nonrepeatable_restart").unwrap();
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("prepare_restart"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::NonRepeatable,
                    EffectSpec::External {
                        provider: "test".into(),
                        version: "1".into(),
                        payload: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    let claim_request = MutationRequest::new(
        run_id(),
        prepared.snapshot.run.revision,
        command("claim_restart"),
        ClaimEffect::new(operation_id.clone()),
    );
    let claimed = controller.claim_effect(claim_request.clone(), 4).unwrap();
    drop(controller);

    let mut restarted = RunController::open(store).unwrap();
    assert_eq!(
        restarted.claim_effect(claim_request, 5).unwrap_err(),
        RunError::AuthorityLost
    );
    let recovery = restarted
        .begin_recovery(
            MutationRequest::new(
                run_id(),
                claimed.command.snapshot.run.revision,
                command("recover_restart_claim"),
                (),
            ),
            6,
        )
        .unwrap();
    assert_eq!(
        recovery.snapshot.run.operations[&operation_id].state,
        OperationState::Uncertain
    );
    assert!(recovery.needs.iter().any(|need| matches!(
        need,
        RecoveryNeed::EffectReconciliation {
            operation_id: candidate,
            effect_class: EffectClass::NonRepeatable,
        } if candidate == &operation_id
    )));
}

#[test]
fn recovery_abandons_prepared_effect_and_it_can_never_be_claimed_later() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalRunStore::new(directory.path()).unwrap();
    let mut controller = RunController::open(store.clone()).unwrap();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("prepared_before_crash").unwrap();
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("prepare_before_crash"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::Reconcilable,
                    EffectSpec::External {
                        provider: "test".into(),
                        version: "1".into(),
                        payload: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    drop(controller);

    let mut restarted = RunController::open(store).unwrap();
    let recovery = restarted
        .begin_recovery(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("recover_prepared"),
                (),
            ),
            4,
        )
        .unwrap();
    let finished = restarted
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                recovery.snapshot.run.revision,
                command("finish_prepared_recovery"),
                RecoveryResolution::new(true, true),
            ),
            5,
        )
        .unwrap();
    assert_eq!(
        finished.snapshot.run.operations[&operation_id].state,
        OperationState::Abandoned
    );
    let next_context = IterationContextManifest::new(
        finished.snapshot.run.revision,
        finished.snapshot.run.current_strategy_revision,
        finished.snapshot.run.verifier_policy_digest.clone(),
        "model-v1",
        "workspace-v1",
    );
    let next_iteration = restarted
        .begin_iteration(
            MutationRequest::new(
                run_id(),
                finished.snapshot.run.revision,
                command("begin_after_prepared_recovery"),
                BeginIteration::new(next_context),
            ),
            6,
        )
        .unwrap();
    assert!(matches!(
        restarted.claim_effect(
            MutationRequest::new(
                run_id(),
                next_iteration.command.snapshot.run.revision,
                command("claim_abandoned"),
                ClaimEffect::new(operation_id),
            ),
            7,
        ),
        Err(RunError::InvalidTransition(_))
    ));
}

#[test]
fn late_unknown_after_cancel_recovers_back_to_cancelled() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("late_cancel_operation").unwrap();
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("late_cancel_prepare"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::Reconcilable,
                    EffectSpec::External {
                        provider: "test".into(),
                        version: "1".into(),
                        payload: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("late_cancel_claim"),
                ClaimEffect::new(operation_id.clone()),
            ),
            4,
        )
        .unwrap();
    let cancelled = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                claimed.command.snapshot.run.revision,
                command("late_cancel"),
                RunAction::Cancel,
            ),
            5,
        )
        .unwrap();
    let late = controller
        .acknowledge_effect(
            EffectCallback::new(
                &claimed.output,
                EffectOutcome::Unknown {
                    message: "transport ended after cancellation".into(),
                },
            ),
            6,
        )
        .unwrap();
    assert_eq!(late.snapshot.run.status, RunStatus::RecoveryRequired);
    assert_eq!(
        late.snapshot.run.recovery_prior_status,
        Some(RunStatus::Cancelled)
    );
    assert!(late.snapshot.run.revision > cancelled.snapshot.run.revision);
    let reconciled = controller
        .reconcile_effect(
            MutationRequest::new(
                run_id(),
                late.snapshot.run.revision,
                command("late_cancel_not_applied"),
                ReconcileEffect::new(operation_id, ReconcileDecision::NotApplied),
            ),
            7,
        )
        .unwrap();
    let finished = controller
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                reconciled.snapshot.run.revision,
                command("late_cancel_finish"),
                RecoveryResolution::new(true, true),
            ),
            8,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::Cancelled);
}

#[test]
fn restart_fences_callbacks_and_reconciles_before_status_decision() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalRunStore::new(directory.path()).unwrap();
    let mut controller = RunController::open(store.clone()).unwrap();
    let created = create(&mut controller);
    drop(controller);

    let mut restarted = RunController::open(store).unwrap();
    assert_eq!(restarted.list_recoverable_runs().len(), 1);
    assert_eq!(
        restarted
            .control_run(
                MutationRequest::new(
                    run_id(),
                    created.snapshot.run.revision,
                    command("unsafe_pause"),
                    RunAction::Pause,
                ),
                2,
            )
            .unwrap_err(),
        RunError::AuthorityLost
    );
    let plan = restarted
        .begin_recovery(
            MutationRequest::new(
                run_id(),
                created.snapshot.run.revision,
                command("recover"),
                (),
            ),
            3,
        )
        .unwrap();
    assert_eq!(plan.snapshot.run.status, RunStatus::RecoveryRequired);
    assert_eq!(plan.snapshot.run.stage, RunStage::Recovering);
    assert_eq!(plan.snapshot.run.controller_epoch, ControllerEpoch::new(2));
    let finished = restarted
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                plan.snapshot.run.revision,
                command("finish_recovery"),
                RecoveryResolution::new(false, false),
            ),
            4,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::UserPaused);
}

#[test]
fn recovery_preserves_every_waiting_status_until_explicit_resume() {
    let cases = [
        (WaitingReason::User, RunStatus::UserPaused),
        (WaitingReason::Backoff, RunStatus::BackOffPaused),
        (WaitingReason::NoProgress, RunStatus::NoProgressPaused),
        (WaitingReason::Infrastructure, RunStatus::InfraPaused),
        (WaitingReason::Blocked, RunStatus::Blocked),
        (WaitingReason::BudgetExhausted, RunStatus::BudgetLimited),
    ];
    for (index, (reason, expected_status)) in cases.into_iter().enumerate() {
        let (directory, mut controller) = controller();
        let created = create(&mut controller);
        let paused = controller
            .control_run(
                MutationRequest::new(
                    run_id(),
                    created.snapshot.run.revision,
                    command(&format!("wait_{index}")),
                    RunAction::PauseFor { reason },
                ),
                2,
            )
            .unwrap();
        assert_eq!(paused.snapshot.run.status, expected_status);
        drop(controller);

        let store = LocalRunStore::new(directory.path()).unwrap();
        let mut restarted = RunController::open(store).unwrap();
        let recovery = restarted
            .begin_recovery(
                MutationRequest::new(
                    run_id(),
                    paused.snapshot.run.revision,
                    command(&format!("recover_wait_{index}")),
                    (),
                ),
                3,
            )
            .unwrap();
        assert_eq!(
            recovery.snapshot.run.recovery_prior_status,
            Some(expected_status)
        );
        let finished = restarted
            .finish_recovery(
                MutationRequest::new(
                    run_id(),
                    recovery.snapshot.run.revision,
                    command(&format!("finish_wait_{index}")),
                    RecoveryResolution::new(true, false),
                ),
                4,
            )
            .unwrap();
        assert_eq!(finished.snapshot.run.status, expected_status);
        assert_eq!(
            finished.snapshot.run.lifecycle(),
            RunLifecycle::Waiting(reason)
        );
    }
}

#[test]
fn late_unknown_after_pause_recovers_back_to_paused() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("late_pause_operation").unwrap();
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("late_pause_prepare"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::Reconcilable,
                    EffectSpec::External {
                        provider: "test".into(),
                        version: "1".into(),
                        payload: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("late_pause_claim"),
                ClaimEffect::new(operation_id.clone()),
            ),
            4,
        )
        .unwrap();
    let paused = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                claimed.command.snapshot.run.revision,
                command("late_pause"),
                RunAction::Pause,
            ),
            5,
        )
        .unwrap();
    let late = controller
        .acknowledge_effect(
            EffectCallback::new(
                &claimed.output,
                EffectOutcome::Unknown {
                    message: "transport ended after pause".into(),
                },
            ),
            6,
        )
        .unwrap();
    assert_eq!(late.snapshot.run.status, RunStatus::RecoveryRequired);
    assert_eq!(
        late.snapshot.run.recovery_prior_status,
        Some(RunStatus::UserPaused)
    );
    assert!(late.snapshot.run.revision > paused.snapshot.run.revision);
    let reconciled = controller
        .reconcile_effect(
            MutationRequest::new(
                run_id(),
                late.snapshot.run.revision,
                command("late_pause_not_applied"),
                ReconcileEffect::new(operation_id, ReconcileDecision::NotApplied),
            ),
            7,
        )
        .unwrap();
    let finished = controller
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                reconciled.snapshot.run.revision,
                command("late_pause_finish"),
                RecoveryResolution::new(true, true),
            ),
            8,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::UserPaused);
    assert_eq!(
        finished.snapshot.run.lifecycle(),
        RunLifecycle::Waiting(WaitingReason::User)
    );
}

#[test]
fn effect_must_be_committed_and_claimed_and_callbacks_are_fenced_and_deduplicated() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("op_effect").unwrap();
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("prepare"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::Idempotent,
                    EffectSpec::External {
                        provider: "test".into(),
                        version: "1".into(),
                        payload: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    assert_eq!(
        prepared.snapshot.run.operations[&operation_id].state,
        OperationState::Prepared
    );
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("claim"),
                ClaimEffect::new(operation_id.clone()),
            ),
            4,
        )
        .unwrap();
    let callback = EffectCallback::new(
        &claimed.output,
        EffectOutcome::Applied {
            receipt: EffectReceipt::new("receipt-1"),
        },
    );
    let mut stale = callback.clone();
    stale.token = DispatchToken::new("dispatch_stale").unwrap();
    assert_eq!(
        controller.acknowledge_effect(stale, 5).unwrap_err(),
        RunError::StaleCallback
    );
    let mut stale_epoch = callback.clone();
    stale_epoch.epoch = ControllerEpoch::new(0);
    assert_eq!(
        controller.acknowledge_effect(stale_epoch, 5).unwrap_err(),
        RunError::StaleEpoch
    );
    let first = controller.acknowledge_effect(callback.clone(), 5).unwrap();
    let duplicate = controller.acknowledge_effect(callback, 6).unwrap();
    assert!(!first.duplicate);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.snapshot.run.revision, first.snapshot.run.revision);
}

#[test]
fn cancelled_run_reconciles_inflight_effect_before_restoring_terminal_status() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalRunStore::new(directory.path()).unwrap();
    let mut controller = RunController::open(store.clone()).unwrap();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("op_cancelled").unwrap();
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("prepare_cancelled"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::Reconcilable,
                    EffectSpec::External {
                        provider: "test".into(),
                        version: "1".into(),
                        payload: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("claim_cancelled"),
                ClaimEffect::new(operation_id.clone()),
            ),
            4,
        )
        .unwrap();
    let cancelled = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                claimed.command.snapshot.run.revision,
                command("cancel_inflight"),
                RunAction::Cancel,
            ),
            5,
        )
        .unwrap();
    drop(controller);

    let mut restarted = RunController::open(store).unwrap();
    assert_eq!(restarted.list_recoverable_runs().len(), 1);
    let plan = restarted
        .begin_recovery(
            MutationRequest::new(
                run_id(),
                cancelled.snapshot.run.revision,
                command("recover_cancelled"),
                (),
            ),
            6,
        )
        .unwrap();
    assert_eq!(plan.snapshot.run.status, RunStatus::RecoveryRequired);
    assert_eq!(
        plan.snapshot.run.recovery_prior_status,
        Some(RunStatus::Cancelled)
    );
    let reconciled = restarted
        .reconcile_effect(
            MutationRequest::new(
                run_id(),
                plan.snapshot.run.revision,
                command("reconcile_cancelled"),
                ReconcileEffect::new(
                    operation_id,
                    ReconcileDecision::Applied {
                        receipt: EffectReceipt::new("cancelled-effect-receipt"),
                    },
                ),
            ),
            7,
        )
        .unwrap();
    let finished = restarted
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                reconciled.snapshot.run.revision,
                command("finish_cancelled"),
                RecoveryResolution::new(false, true)
                    .recovered_usage(ResourceVector::default().iterations(1).agent_calls(1)),
            ),
            8,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::Cancelled);
    assert!(finished.snapshot.run.active_iteration.is_none());
    assert_eq!(finished.snapshot.run.usage.iterations, 1);
}

#[test]
fn uncertain_non_repeatable_effect_requires_recovery() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("op_nonrepeatable").unwrap();
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("prepare_nonrepeatable"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::NonRepeatable,
                    EffectSpec::External {
                        provider: "test".into(),
                        version: "1".into(),
                        payload: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("claim_nonrepeatable"),
                ClaimEffect::new(operation_id.clone()),
            ),
            4,
        )
        .unwrap();
    let result = controller
        .acknowledge_effect(
            EffectCallback::new(
                &claimed.output,
                EffectOutcome::Unknown {
                    message: "connection ended after send".into(),
                },
            ),
            5,
        )
        .unwrap();
    assert_eq!(result.snapshot.run.status, RunStatus::RecoveryRequired);
    assert_eq!(
        result.snapshot.run.operations[&operation_id].state,
        OperationState::Uncertain
    );
}

#[test]
fn completion_is_atomic_at_verified_iteration_boundary() {
    let (_directory, mut controller) = controller();
    let request = CreateRunRequest::new(
        command("create"),
        session(),
        GoalSpec::new("verified finish")
            .acceptance_criteria(["tests pass".to_owned()])
            .required_evidence(["test-report".to_owned()]),
        RunDriverSpec::AutonomousTurnLoop {
            session: session(),
            strategy_revision: 0,
        },
        capabilities(),
        budget(),
    )
    .run_id(run_id())
    .required_gates(["tests".to_owned()]);
    let created = controller.create_run(request, 1).unwrap();
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let finished = controller
        .finish_iteration(
            FinishIteration::new(
                &iteration.output,
                true,
                "verified",
                GoalVerdict::Achieved,
                ResourceVector::default().iterations(1),
            )
            .evidence([artifact(Some("test-report"), Some("workspace-v1"))])
            .gates([("tests".to_owned(), true)]),
            3,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::Active);
    let complete = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                finished.snapshot.run.revision,
                command("complete"),
                RunAction::TryComplete,
            ),
            4,
        )
        .unwrap();
    assert_eq!(complete.snapshot.run.status, RunStatus::Complete);
}

#[test]
fn child_budget_is_reserved_once_and_settled_once() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let reservation = ResourceVector::default()
        .agent_calls(3)
        .agent_concurrency(1)
        .tokens(100);
    let admitted = controller
        .admit_child(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("admit_child"),
                AdmitChild::new(
                    ChildId::new("child_1").unwrap(),
                    iteration.output.iteration_id,
                    reservation.clone(),
                    "isolated-worktree",
                    ChildCompletionPolicy::MustSucceed,
                ),
            ),
            3,
        )
        .unwrap();
    assert_eq!(admitted.command.snapshot.run.child_reserved, reservation);
    let started = controller
        .child_callback(
            ChildCallback::new(
                run_id(),
                admitted.command.snapshot.run.controller_epoch,
                &admitted.output,
                ChildState::Started,
            ),
            4,
        )
        .unwrap();
    let child = started.snapshot.run.children[&admitted.output.id].clone();
    let completed = controller
        .child_callback(
            ChildCallback::new(
                run_id(),
                started.snapshot.run.controller_epoch,
                &child,
                ChildState::Completed,
            )
            .settlement(ResourceVector::default().agent_calls(2).tokens(80)),
            5,
        )
        .unwrap();
    assert!(completed.snapshot.run.child_reserved.is_zero());
    assert_eq!(completed.snapshot.run.usage.agent_calls, 2);
    let duplicate_child = completed.snapshot.run.children[&admitted.output.id].clone();
    let duplicate = controller
        .child_callback(
            ChildCallback::new(
                run_id(),
                completed.snapshot.run.controller_epoch,
                &duplicate_child,
                ChildState::Completed,
            )
            .settlement(ResourceVector::default().agent_calls(2).tokens(80)),
            6,
        )
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.snapshot.run.usage.agent_calls, 2);
}

#[test]
fn attach_gap_returns_snapshot_and_tombstone_blocks_resurrection() {
    let (_directory, mut controller) = controller();
    let mut snapshot = create(&mut controller).snapshot;
    for index in 0..260 {
        snapshot = controller
            .control_run(
                MutationRequest::new(
                    run_id(),
                    snapshot.run.revision,
                    command(&format!("steer_{index}")),
                    RunAction::Steer {
                        message_id: MessageId::new(format!("message_{index}")).unwrap(),
                        body: format!("steering {index}"),
                    },
                ),
                index + 2,
            )
            .unwrap()
            .snapshot;
    }
    assert!(matches!(
        controller
            .attach_run(&run_id(), RunEventCursor::new(0))
            .unwrap(),
        RunAttach::Snapshot(_)
    ));
    let cancelled = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                snapshot.run.revision,
                command("cancel"),
                RunAction::Cancel,
            ),
            300,
        )
        .unwrap();
    let tombstoned = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                cancelled.snapshot.run.revision,
                command("tombstone"),
                RunAction::Tombstone,
            ),
            301,
        )
        .unwrap();
    assert_eq!(tombstoned.snapshot.run.status, RunStatus::Tombstoned);
    assert!(
        controller
            .control_run(
                MutationRequest::new(
                    run_id(),
                    tombstoned.snapshot.run.revision,
                    command("resurrect"),
                    RunAction::Resume { budget: None },
                ),
                302,
            )
            .is_err()
    );
}

#[test]
fn workflow_revision_requires_compile_policy_dry_run_and_iteration_boundary() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let proposed = controller
        .propose_workflow(
            MutationRequest::new(
                run_id(),
                created.snapshot.run.revision,
                command("propose_workflow"),
                ProposeWorkflow::new("b".repeat(64), "run-local diagnosis"),
            ),
            2,
        )
        .unwrap();
    let revision = proposed.snapshot.run.workflow_revisions[0].revision;
    let validated = controller
        .validate_workflow(
            MutationRequest::new(
                run_id(),
                proposed.snapshot.run.revision,
                command("validate_workflow"),
                ValidateWorkflow::new(revision, true, true, true),
            ),
            3,
        )
        .unwrap();
    let applied = controller
        .apply_workflow(
            MutationRequest::new(
                run_id(),
                validated.snapshot.run.revision,
                command("apply_workflow"),
                SetWorkflowRevision::new(revision),
            ),
            4,
        )
        .unwrap();
    assert_eq!(
        applied.snapshot.run.current_workflow_revision,
        Some(revision)
    );
}
