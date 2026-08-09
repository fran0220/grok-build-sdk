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

fn create<S: RunStore>(controller: &mut RunController<S>) -> RunCommandResult {
    controller.create_run(create_request(), 1).unwrap()
}

fn begin_iteration<S: RunStore>(
    controller: &mut RunController<S>,
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
fn current_v2_fixture_round_trips_and_future_values_fail_closed() {
    // This fixture documents the current v2 wire shape; it is not historical
    // compatibility evidence. Release fixtures become immutable only after
    // their originating release has shipped.
    let fixture = include_str!("fixtures/run-envelope-current-v2.json");
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
    assert!(
        serde_json::from_value::<RunLifecycle>(serde_json::json!({
            "state":"active",
            "detail":{"malformed":true}
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<RunLifecycle>(serde_json::json!({
            "state":"recovering",
            "detail":{"malformed":true}
        }))
        .is_err()
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
fn durable_deserialize_rejects_nested_and_journal_corruption() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let begun = begin_iteration(&mut controller, &created.snapshot);

    let mut malformed_context = serde_json::to_value(&begun.command.snapshot).unwrap();
    malformed_context["run"]["active_iteration"]["context"]["model_revision"] =
        serde_json::json!("x".repeat(257));
    assert!(serde_json::from_value::<RunEnvelope>(malformed_context).is_err());

    let operation_id = OperationId::new("nested_validation").unwrap();
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                begun.command.snapshot.run.revision,
                command("prepare_nested_validation"),
                PrepareOperation::new(
                    operation_id.clone(),
                    begun.output.iteration_id,
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
    let mut malformed_spec = serde_json::to_value(&prepared.snapshot).unwrap();
    malformed_spec["run"]["operations"][operation_id.as_str()]["spec"]["payload"]["retention"] =
        serde_json::json!("x".repeat(129));
    assert!(serde_json::from_value::<RunEnvelope>(malformed_spec).is_err());

    let admitted = controller
        .admit_child(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("admit_nested_validation"),
                AdmitChild::new(
                    ChildId::new("nested_child").unwrap(),
                    begun.output.iteration_id,
                    ResourceVector::default().agent_calls(1),
                    "isolated",
                    ChildCompletionPolicy::MayFail,
                ),
            ),
            4,
        )
        .unwrap();
    let mut malformed_child = serde_json::to_value(&admitted.command.snapshot).unwrap();
    malformed_child["run"]["children"]["nested_child"]["artifacts"] = serde_json::json!([{
        "digest":"a".repeat(64),
        "media_type":"application/json",
        "size":1,
        "provenance":"test",
        "owner":"x".repeat(161),
        "retention":"run",
        "evidence_labels":[]
    }]);
    assert!(serde_json::from_value::<RunEnvelope>(malformed_child).is_err());

    let mut too_many_messages = serde_json::to_value(&created.snapshot).unwrap();
    let mailbox = too_many_messages["run"]["mailbox"].as_object_mut().unwrap();
    for sequence in 1..=(MAX_MESSAGES + 1) {
        let id = format!("message_{sequence}");
        mailbox.insert(
            id.clone(),
            serde_json::json!({
                "id":id,
                "sequence":sequence,
                "causation_id":null,
                "sender":"test",
                "trust_label":"trusted",
                "body":"bounded",
                "state":"accepted"
            }),
        );
    }
    assert!(serde_json::from_value::<RunEnvelope>(too_many_messages).is_err());

    let mut gap = created.snapshot.clone();
    gap.run.revision = RunRevision::new(3);
    gap.run.event_cursor = RunEventCursor::new(3);
    gap.events.push_back(RunEvent {
        cursor: RunEventCursor::new(3),
        revision: RunRevision::new(3),
        kind: RunEventKind::new("gap_tail").unwrap(),
        at_ms: 3,
    });
    assert!(serde_json::from_value::<RunEnvelope>(serde_json::to_value(gap).unwrap()).is_err());

    let mut truncated = created.snapshot;
    truncated.run.revision = RunRevision::new(2);
    truncated.run.event_cursor = RunEventCursor::new(2);
    assert!(
        serde_json::from_value::<RunEnvelope>(serde_json::to_value(truncated).unwrap()).is_err()
    );

    let oversized = vec![b'x'; MAX_RUN_ENVELOPE_BYTES + 2];
    assert!(RunEnvelope::from_json_slice(&oversized).is_err());
    let mut reader = std::io::Cursor::new(oversized);
    assert!(RunEnvelope::from_json_reader(&mut reader).is_err());
    assert_eq!(reader.position(), MAX_RUN_ENVELOPE_BYTES as u64 + 1);
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

    let usage = EffectUsage::measured(
        ResourceVector::default()
            .iterations(1)
            .agent_calls(1)
            .agent_concurrency(1)
            .artifact_bytes(10),
    );
    let valid = EffectReceipt::for_session_turn(
        &session(),
        turn_id,
        &prompt_digest,
        0,
        SessionTurnOutcome::End,
        usage.clone(),
        usage.clone(),
    );
    let epoch = malformed.run.controller_epoch;
    let operation = malformed.run.operations.get_mut(&operation_id).unwrap();
    operation.reservation = Some(usage.resources.clone());
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
        &usage,
    );
    assert_eq!(valid.settlement_id.as_deref(), Some(expected.as_str()));
}

#[test]
fn session_turn_applied_without_usage_is_deduplicated_and_recovery_fenced() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("missing_usage_turn").unwrap();
    let turn_id = "missing-usage-turn";
    let prompt_digest = "d".repeat(64);
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("prepare_missing_usage"),
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
    let resources = ResourceVector::default()
        .iterations(1)
        .agent_calls(1)
        .agent_concurrency(1)
        .artifact_bytes(10);
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("claim_missing_usage"),
                ClaimEffect::new(operation_id.clone()).reservation(resources.clone()),
            ),
            4,
        )
        .unwrap();
    let usage = EffectUsage::measured(resources);
    let corrected_receipt = EffectReceipt::for_session_turn(
        &session(),
        turn_id,
        &prompt_digest,
        0,
        SessionTurnOutcome::End,
        usage.clone(),
        usage,
    );
    let mut incomplete_receipt = corrected_receipt.clone();
    incomplete_receipt.actual_usage = None;
    let callback = EffectCallback::new(
        &claimed.output,
        EffectOutcome::Applied {
            receipt: incomplete_receipt.clone(),
        },
    );
    let first = controller.acknowledge_effect(callback.clone(), 5).unwrap();
    let duplicate = controller.acknowledge_effect(callback, 6).unwrap();
    assert!(!first.duplicate);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.snapshot.run.revision, first.snapshot.run.revision);
    assert_eq!(first.snapshot.run.status, RunStatus::RecoveryRequired);
    assert_eq!(
        first.snapshot.run.operations[&operation_id].state,
        OperationState::Uncertain
    );
    assert_eq!(
        first.snapshot.run.operations[&operation_id].receipt,
        Some(incomplete_receipt)
    );

    let reconciled = controller
        .reconcile_effect(
            MutationRequest::new(
                run_id(),
                first.snapshot.run.revision,
                command("reconcile_missing_usage"),
                ReconcileEffect::new(
                    operation_id,
                    ReconcileDecision::Applied {
                        receipt: corrected_receipt,
                    },
                ),
            ),
            7,
        )
        .unwrap();
    assert_eq!(reconciled.snapshot.run.status, RunStatus::RecoveryRequired);
}

#[test]
fn session_turn_budget_overrun_is_durably_fenced_for_recovery() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("overrun_turn").unwrap();
    let turn_id = "overrun-turn";
    let prompt_digest = "d".repeat(64);
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("prepare_overrun"),
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
    let reservation = ResourceVector::default()
        .iterations(1)
        .agent_calls(1)
        .agent_concurrency(1)
        .tokens(10)
        .artifact_bytes(10);
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("claim_overrun"),
                ClaimEffect::new(operation_id.clone()).reservation(reservation.clone()),
            ),
            4,
        )
        .unwrap();
    let actual = EffectUsage::measured(
        ResourceVector::default()
            .iterations(1)
            .agent_calls(1)
            .agent_concurrency(1)
            .tokens(11)
            .artifact_bytes(10),
    );
    let receipt = EffectReceipt::for_session_turn(
        &session(),
        turn_id,
        &prompt_digest,
        0,
        SessionTurnOutcome::End,
        actual.clone(),
        actual,
    );
    let acknowledged = controller
        .acknowledge_effect(
            EffectCallback::new(
                &claimed.output,
                EffectOutcome::Applied {
                    receipt: receipt.clone(),
                },
            ),
            5,
        )
        .unwrap();
    assert_eq!(
        acknowledged.snapshot.run.status,
        RunStatus::RecoveryRequired
    );
    assert_eq!(
        acknowledged.snapshot.run.operations[&operation_id].state,
        OperationState::Uncertain
    );
    assert_eq!(
        acknowledged.snapshot.run.operations[&operation_id].receipt,
        Some(receipt.clone())
    );
    assert!(matches!(
        controller.reconcile_effect(
            MutationRequest::new(
                run_id(),
                acknowledged.snapshot.run.revision,
                command("reconcile_overrun_not_applied"),
                ReconcileEffect::new(operation_id.clone(), ReconcileDecision::NotApplied),
            ),
            6,
        ),
        Err(RunError::InvalidTransition(_))
    ));
    let still_uncertain = controller.get_run(&run_id()).unwrap();
    assert_eq!(
        still_uncertain.run.revision, acknowledged.snapshot.run.revision,
        "NotApplied must not consume a revision when Applied evidence is retained"
    );
    assert_eq!(
        still_uncertain.run.operations[&operation_id].state,
        OperationState::Uncertain
    );
    assert_eq!(
        still_uncertain.run.operations[&operation_id].receipt,
        Some(receipt.clone())
    );
    assert!(matches!(
        controller.finish_recovery(
            MutationRequest::new(
                run_id(),
                still_uncertain.run.revision,
                command("finish_overrun_while_uncertain"),
                RecoveryResolution::new(true, true),
            ),
            7,
        ),
        Err(RunError::InvalidTransition(_))
    ));
    assert_eq!(
        controller
            .reconcile_effect(
                MutationRequest::new(
                    run_id(),
                    still_uncertain.run.revision,
                    command("reconcile_overrun"),
                    ReconcileEffect::new(
                        operation_id.clone(),
                        ReconcileDecision::Applied {
                            receipt: receipt.clone(),
                        },
                    ),
                ),
                8,
            )
            .unwrap_err(),
        RunError::Budget
    );
    let after_overrun = controller.get_run(&run_id()).unwrap();
    assert_eq!(after_overrun.run.status, RunStatus::RecoveryRequired);
    assert_eq!(
        after_overrun.run.revision, still_uncertain.run.revision,
        "rejected overrun evidence must remain correctable"
    );
    let corrected_usage = EffectUsage::measured(reservation);
    let corrected = controller
        .reconcile_effect(
            MutationRequest::new(
                run_id(),
                after_overrun.run.revision,
                command("reconcile_overrun_not_applied"),
                ReconcileEffect::new(
                    operation_id,
                    ReconcileDecision::Applied {
                        receipt: EffectReceipt::for_session_turn(
                            &session(),
                            turn_id,
                            &prompt_digest,
                            0,
                            SessionTurnOutcome::End,
                            corrected_usage.clone(),
                            corrected_usage,
                        ),
                    },
                ),
            ),
            9,
        )
        .unwrap();
    let finished = controller
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                corrected.snapshot.run.revision,
                command("finish_corrected_overrun"),
                RecoveryResolution::new(true, true),
            ),
            10,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::Active);
    assert_eq!(finished.snapshot.run.usage.tokens, 10);
}

#[test]
fn unknown_usage_is_allowed_only_by_an_explicitly_unbounded_dimension() {
    let (_directory, mut controller) = controller();
    let mut request = create_request();
    request.command_id = command("create_unbounded_usage");
    request.run_id = Some(RunId::new("run_unknown_usage").unwrap());
    request.budget.tokens = u64::MAX;
    let run_id = request.run_id.clone().unwrap();
    let created = controller.create_run(request, 1).unwrap();
    let context = IterationContextManifest::new(
        created.snapshot.run.revision,
        created.snapshot.run.current_strategy_revision,
        created.snapshot.run.verifier_policy_digest.clone(),
        "model-v1",
        "workspace-v1",
    );
    let iteration = controller
        .begin_iteration(
            MutationRequest::new(
                run_id.clone(),
                created.snapshot.run.revision,
                command("begin_unknown_usage"),
                BeginIteration::new(context),
            ),
            2,
        )
        .unwrap();
    let operation_id = OperationId::new("unknown_usage_turn").unwrap();
    let prompt_digest = "e".repeat(64);
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id.clone(),
                iteration.command.snapshot.run.revision,
                command("prepare_unknown_usage"),
                PrepareOperation::new(
                    operation_id.clone(),
                    iteration.output.iteration_id,
                    EffectClass::Reconcilable,
                    EffectSpec::SessionTurn {
                        session: session(),
                        turn_id: "unknown-usage-turn".into(),
                        prompt_digest: prompt_digest.clone(),
                        input: artifact(None, None),
                    },
                ),
            ),
            3,
        )
        .unwrap();
    let resources = ResourceVector::default()
        .iterations(1)
        .agent_calls(1)
        .agent_concurrency(1)
        .artifact_bytes(10);
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id.clone(),
                prepared.snapshot.run.revision,
                command("claim_unknown_usage"),
                ClaimEffect::new(operation_id).reservation(resources.clone()),
            ),
            4,
        )
        .unwrap();
    let usage = EffectUsage::measured(resources.clone()).unknown([ResourceDimension::Tokens]);
    let receipt = EffectReceipt::for_session_turn(
        &session(),
        "unknown-usage-turn",
        &prompt_digest,
        0,
        SessionTurnOutcome::End,
        usage.clone(),
        usage,
    );
    let acknowledged = controller
        .acknowledge_effect(
            EffectCallback::new(&claimed.output, EffectOutcome::Applied { receipt }),
            5,
        )
        .unwrap();
    let finished = controller
        .finish_iteration(
            FinishIteration::new(
                &iteration.output,
                false,
                "usage remains explicit",
                GoalVerdict::NotAchieved,
                resources,
            ),
            6,
        )
        .unwrap();
    assert_eq!(acknowledged.snapshot.run.status, RunStatus::Active);
    assert!(
        finished
            .snapshot
            .run
            .usage_unknown
            .contains(&ResourceDimension::Tokens)
    );
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
    first.run.event_cursor = RunEventCursor::new(2);
    first.events.push_back(RunEvent {
        cursor: RunEventCursor::new(2),
        revision: RunRevision::new(2),
        kind: RunEventKind::new("cas_first").unwrap(),
        at_ms: 2,
    });
    let mut second = created.snapshot.clone();
    second.run.revision = RunRevision::new(2);
    second.run.status = RunStatus::Blocked;
    second.run.event_cursor = RunEventCursor::new(2);
    second.events.push_back(RunEvent {
        cursor: RunEventCursor::new(2),
        revision: RunRevision::new(2),
        kind: RunEventKind::new("cas_second").unwrap(),
        at_ms: 2,
    });

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

    fn replace(&self, envelope: RunEnvelope) {
        *self.state.lock().unwrap() = Some(envelope);
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
fn failed_reload_keeps_identity_and_authority_fences_until_valid_replacement() {
    let store = ScriptedStore::new();
    let mut controller = RunController::open(store.clone()).unwrap();
    let created = controller.create_run(create_request(), 1).unwrap();
    store.fail(FailureMode::UnknownCommit);
    assert!(matches!(
        controller.control_run(
            MutationRequest::new(
                run_id(),
                created.snapshot.run.revision,
                command("reload_fence_pause"),
                RunAction::Pause,
            ),
            2,
        ),
        Err(RunError::CommitUnknown(_))
    ));
    store.fail(FailureMode::Healthy);

    let mut wrong_identity = created.snapshot.clone();
    wrong_identity.run.id = RunId::new("different_run").unwrap();
    store.replace(wrong_identity);
    assert!(matches!(
        controller.reload_run(&run_id()),
        Err(RunError::Integrity(_))
    ));
    assert_eq!(
        controller
            .begin_recovery(
                MutationRequest::new(
                    run_id(),
                    created.snapshot.run.revision,
                    command("reload_wrong_id_recovery"),
                    (),
                ),
                3,
            )
            .unwrap_err(),
        RunError::ReloadRequired
    );
    assert_eq!(
        controller.get_run(&run_id()).unwrap().run.revision,
        created.snapshot.run.revision
    );

    let mut malformed = created.snapshot.clone();
    malformed.run.event_cursor = RunEventCursor::new(2);
    store.replace(malformed);
    assert!(matches!(
        controller.reload_run(&run_id()),
        Err(RunError::Integrity(_))
    ));
    assert_eq!(
        controller
            .begin_recovery(
                MutationRequest::new(
                    run_id(),
                    created.snapshot.run.revision,
                    command("reload_invalid_recovery"),
                    (),
                ),
                4,
            )
            .unwrap_err(),
        RunError::ReloadRequired
    );

    store.replace(created.snapshot.clone());
    let reloaded = controller.reload_run(&run_id()).unwrap().unwrap();
    let recovery = controller
        .begin_recovery(
            MutationRequest::new(
                run_id(),
                reloaded.run.revision,
                command("reload_valid_recovery"),
                (),
            ),
            5,
        )
        .unwrap();
    let resumed = controller
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                recovery.snapshot.run.revision,
                command("reload_valid_finish_recovery"),
                RecoveryResolution::new(true, false),
            ),
            6,
        )
        .unwrap();
    let paused = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                resumed.snapshot.run.revision,
                command("reload_valid_pause"),
                RunAction::Pause,
            ),
            7,
        )
        .unwrap();
    assert_eq!(paused.snapshot.run.status, RunStatus::UserPaused);
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
        ClaimEffect::new(operation_id.clone())
            .reservation(ResourceVector::default().agent_calls(1)),
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
                ClaimEffect::new(operation_id.clone())
                    .reservation(ResourceVector::default().agent_calls(1)),
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
fn cancelled_run_cannot_tombstone_late_applied_usage_before_recovery() {
    let (_directory, mut controller) = controller();
    let created = create(&mut controller);
    let iteration = begin_iteration(&mut controller, &created.snapshot);
    let operation_id = OperationId::new("late_applied_cancel_operation").unwrap();
    let turn_id = "late_applied_cancel_turn";
    let prompt_digest = "e".repeat(64);
    let prepared = controller
        .prepare_operation(
            MutationRequest::new(
                run_id(),
                iteration.command.snapshot.run.revision,
                command("late_applied_cancel_prepare"),
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
    let reservation = ResourceVector::default()
        .iterations(1)
        .agent_calls(1)
        .agent_concurrency(1)
        .active_ms(100)
        .wall_ms(100)
        .tokens(100)
        .cost_micros(100)
        .artifact_bytes(100);
    let claimed = controller
        .claim_effect(
            MutationRequest::new(
                run_id(),
                prepared.snapshot.run.revision,
                command("late_applied_cancel_claim"),
                ClaimEffect::new(operation_id.clone()).reservation(reservation),
            ),
            4,
        )
        .unwrap();
    let cancelled = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                claimed.command.snapshot.run.revision,
                command("late_applied_cancel"),
                RunAction::Cancel,
            ),
            5,
        )
        .unwrap();
    assert_eq!(cancelled.snapshot.run.status, RunStatus::Cancelled);
    let usage = EffectUsage::measured(
        ResourceVector::default()
            .iterations(1)
            .agent_calls(1)
            .agent_concurrency(1)
            .active_ms(10)
            .wall_ms(20)
            .tokens(30)
            .cost_micros(40)
            .artifact_bytes(10),
    );
    let late = controller
        .acknowledge_effect(
            EffectCallback::new(
                &claimed.output,
                EffectOutcome::Applied {
                    receipt: EffectReceipt::for_session_turn(
                        &session(),
                        turn_id,
                        &prompt_digest,
                        0,
                        SessionTurnOutcome::End,
                        usage.clone(),
                        usage.clone(),
                    ),
                },
            ),
            6,
        )
        .unwrap();
    assert_eq!(late.snapshot.run.status, RunStatus::Cancelled);
    assert_eq!(
        late.snapshot.run.operations[&operation_id].state,
        OperationState::Acknowledged
    );
    assert!(late.snapshot.run.active_iteration.is_some());
    assert!(late.snapshot.run.usage.is_zero());
    for (index, action) in [RunAction::ClaimTerminalReport, RunAction::Tombstone]
        .into_iter()
        .enumerate()
    {
        assert!(matches!(
            controller.control_run(
                MutationRequest::new(
                    run_id(),
                    late.snapshot.run.revision,
                    command(&format!("late_applied_terminal_guard_{index}")),
                    action,
                ),
                7 + index as u64,
            ),
            Err(RunError::InvalidTransition(_))
        ));
    }

    let recovery = controller
        .begin_recovery(
            MutationRequest::new(
                run_id(),
                late.snapshot.run.revision,
                command("late_applied_cancel_recovery"),
                (),
            ),
            9,
        )
        .unwrap();
    assert_eq!(
        recovery.snapshot.run.recovery_prior_status,
        Some(RunStatus::Cancelled)
    );
    let finished = controller
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                recovery.snapshot.run.revision,
                command("late_applied_cancel_recovery_finish"),
                RecoveryResolution::new(false, true),
            ),
            10,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::Cancelled);
    assert!(finished.snapshot.run.active_iteration.is_none());
    assert_eq!(finished.snapshot.run.usage, usage.resources);

    let reported = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                finished.snapshot.run.revision,
                command("late_applied_terminal_report"),
                RunAction::ClaimTerminalReport,
            ),
            11,
        )
        .unwrap();
    let tombstoned = controller
        .control_run(
            MutationRequest::new(
                run_id(),
                reported.snapshot.run.revision,
                command("late_applied_tombstone"),
                RunAction::Tombstone,
            ),
            12,
        )
        .unwrap();
    assert_eq!(tombstoned.snapshot.run.status, RunStatus::Tombstoned);
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
                ClaimEffect::new(operation_id.clone())
                    .reservation(ResourceVector::default().agent_calls(1)),
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
                ClaimEffect::new(operation_id.clone())
                    .reservation(ResourceVector::default().iterations(1).agent_calls(1)),
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
    let duplicate = controller.acknowledge_effect(callback.clone(), 6).unwrap();
    assert!(!first.duplicate);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.snapshot.run.revision, first.snapshot.run.revision);
    assert_eq!(first.snapshot.run.status, RunStatus::RecoveryRequired);
    assert_eq!(
        first.snapshot.run.operations[&operation_id].state,
        OperationState::Uncertain
    );
    assert!(
        first.snapshot.run.operations[&operation_id]
            .receipt
            .as_ref()
            .is_some_and(|receipt| receipt.actual_usage.is_none()),
        "an applied generic effect without usage is retained but recovery-fenced"
    );

    let mut abandoned_with_receipt = first.snapshot.clone();
    abandoned_with_receipt
        .run
        .operations
        .get_mut(&operation_id)
        .unwrap()
        .state = OperationState::Abandoned;
    assert!(abandoned_with_receipt.validate().is_err());

    assert!(matches!(
        controller.reconcile_effect(
            MutationRequest::new(
                run_id(),
                first.snapshot.run.revision,
                command("generic_applied_reconcile"),
                ReconcileEffect::new(operation_id.clone(), ReconcileDecision::NotApplied),
            ),
            7,
        ),
        Err(RunError::InvalidTransition(_))
    ));
    let still_uncertain = controller.get_run(&run_id()).unwrap();
    assert_eq!(
        still_uncertain.run.revision, first.snapshot.run.revision,
        "NotApplied must not consume a revision when Applied evidence is retained"
    );
    assert_eq!(
        still_uncertain.run.operations[&operation_id].state,
        OperationState::Uncertain
    );
    assert!(
        still_uncertain.run.operations[&operation_id]
            .receipt
            .is_some()
    );
    let duplicate_after_rejected = controller.acknowledge_effect(callback, 8).unwrap();
    assert!(duplicate_after_rejected.duplicate);
    assert_eq!(
        duplicate_after_rejected.snapshot.run.revision,
        still_uncertain.run.revision
    );
    assert!(matches!(
        controller.finish_recovery(
            MutationRequest::new(
                run_id(),
                still_uncertain.run.revision,
                command("finish_generic_while_uncertain"),
                RecoveryResolution::new(true, true),
            ),
            9,
        ),
        Err(RunError::InvalidTransition(_))
    ));

    let usage = EffectUsage::measured(ResourceVector::default().iterations(1).agent_calls(1));
    let reconciled = controller
        .reconcile_effect(
            MutationRequest::new(
                run_id(),
                still_uncertain.run.revision,
                command("generic_applied_reconcile"),
                ReconcileEffect::new(
                    operation_id,
                    ReconcileDecision::Applied {
                        receipt: EffectReceipt::new("receipt-1").actual_usage(usage.clone()),
                    },
                ),
            ),
            10,
        )
        .unwrap();
    let finished = controller
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                reconciled.snapshot.run.revision,
                command("finish_generic_with_usage"),
                RecoveryResolution::new(true, true),
            ),
            11,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::Active);
    assert_eq!(finished.snapshot.run.usage, usage.resources);
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
                ClaimEffect::new(operation_id.clone())
                    .reservation(ResourceVector::default().iterations(1).agent_calls(1)),
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
                        receipt: EffectReceipt::new("cancelled-effect-receipt").actual_usage(
                            EffectUsage::measured(
                                ResourceVector::default().iterations(1).agent_calls(1),
                            ),
                        ),
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
                RecoveryResolution::new(false, true),
            ),
            8,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::Cancelled);
    assert!(finished.snapshot.run.active_iteration.is_none());
    assert_eq!(finished.snapshot.run.usage.iterations, 1);
}

#[test]
fn uncertain_non_repeatable_effect_recovers_only_with_typed_usage() {
    let store = ScriptedStore::new();
    let mut controller = RunController::open(store.clone()).unwrap();
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
                ClaimEffect::new(operation_id.clone())
                    .reservation(ResourceVector::default().iterations(1).agent_calls(1)),
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

    let missing_usage = controller.reconcile_effect(
        MutationRequest::new(
            run_id(),
            result.snapshot.run.revision,
            command("reconcile_nonrepeatable_missing_usage"),
            ReconcileEffect::new(
                operation_id.clone(),
                ReconcileDecision::Applied {
                    receipt: EffectReceipt::new("nonrepeatable-applied"),
                },
            ),
        ),
        6,
    );
    assert!(matches!(missing_usage, Err(RunError::InvalidTransition(_))));
    let still_uncertain = controller.get_run(&run_id()).unwrap();
    assert_eq!(
        still_uncertain.run.revision, result.snapshot.run.revision,
        "rejected evidence must not consume a revision or reconciliation command"
    );
    assert_eq!(
        still_uncertain.run.operations[&operation_id].state,
        OperationState::Uncertain
    );

    let usage = EffectUsage::measured(ResourceVector::default().iterations(1).agent_calls(1));
    let reconciled = controller
        .reconcile_effect(
            MutationRequest::new(
                run_id(),
                still_uncertain.run.revision,
                command("reconcile_nonrepeatable_with_usage"),
                ReconcileEffect::new(
                    operation_id.clone(),
                    ReconcileDecision::Applied {
                        receipt: EffectReceipt::new("nonrepeatable-applied")
                            .actual_usage(usage.clone()),
                    },
                ),
            ),
            7,
        )
        .unwrap();
    assert_eq!(
        reconciled.snapshot.run.operations[&operation_id].state,
        OperationState::Reconciled
    );

    let mut malformed_reload = reconciled.snapshot.clone();
    malformed_reload
        .run
        .operations
        .get_mut(&operation_id)
        .unwrap()
        .receipt
        .as_mut()
        .unwrap()
        .actual_usage = None;
    assert!(malformed_reload.validate().is_err());

    let mut above_budget_reload = reconciled.snapshot.clone();
    above_budget_reload.run.budget.agent_calls = 0;
    assert!(above_budget_reload.validate().is_err());

    let finished = controller
        .finish_recovery(
            MutationRequest::new(
                run_id(),
                reconciled.snapshot.run.revision,
                command("finish_nonrepeatable_recovery"),
                RecoveryResolution::new(true, true),
            ),
            8,
        )
        .unwrap();
    assert_eq!(finished.snapshot.run.status, RunStatus::Active);
    assert_eq!(finished.snapshot.run.usage, usage.resources);
    assert!(finished.snapshot.run.active_iteration.is_none());

    store.replace(malformed_reload);
    drop(controller);
    assert!(matches!(
        RunController::open(store),
        Err(RunError::Integrity(_))
    ));
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
