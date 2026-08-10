use super::*;

struct SequenceVerifier {
    remaining_failures: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl run::GoalVerifier for SequenceVerifier {
    async fn verify(
        &self,
        _request: run::GoalVerificationRequest,
    ) -> Result<run::GoalVerification, run::RunError> {
        let previous = self
            .remaining_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .unwrap_or(0);
        let verdict = if previous == 0 {
            run::GoalVerdict::Achieved
        } else {
            run::GoalVerdict::NotAchieved
        };
        Ok(run::GoalVerification::new(
            verdict,
            "test-verifier",
            "deterministic test verifier",
        ))
    }
}

fn autonomous_providers(root: &std::path::Path, remaining_failures: usize) -> run::ProviderSet {
    run::ProviderSet::new(
        Arc::new(run::LocalArtifactStore::new(root, 1024 * 1024).unwrap()),
        Arc::new(run::FailClosedGateProvider),
        Arc::new(SequenceVerifier {
            remaining_failures: std::sync::atomic::AtomicUsize::new(remaining_failures),
        }),
        Arc::new(run::DenyApprovalHandler),
        Arc::new(run::NoopTelemetrySink),
    )
}

fn autonomous_run_request(
    run_id: &str,
    session: &SessionId,
    iteration_budget: u64,
) -> run::CreateRunRequest {
    let capability = "session.turn".to_owned();
    run::CreateRunRequest::new(
        run::CommandId::new(format!("create_{run_id}")).unwrap(),
        run::SessionRef::new(session.as_str()).unwrap(),
        run::GoalSpec::new("produce a verified durable result"),
        run::RunDriverSpec::AutonomousTurnLoop {
            session: run::SessionRef::new(session.as_str()).unwrap(),
            strategy_revision: 0,
        },
        run::CapabilityPolicy::new([capability.clone()], [capability.clone()], [capability]),
        run::ResourceVector::default()
            .iterations(iteration_budget)
            .agent_calls(iteration_budget)
            .agent_concurrency(1)
            .active_ms(u64::MAX)
            .wall_ms(u64::MAX)
            .tokens(u64::MAX)
            .cost_micros(u64::MAX)
            .artifact_bytes(u64::MAX),
    )
    .run_id(run::RunId::new(run_id).unwrap())
    .harness_snapshot(run::HarnessSnapshotPin::new(
        "b".repeat(64),
        "c".repeat(64),
        "test-v1",
        1,
        "sdk-test",
    ))
    .verifier_policy_digest("test-verifier")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_run_store_replaces_the_default_run_authority() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let default_store_path = config
        .session_storage
        .join("durable-runs")
        .join("runs.sqlite3");
    let host_store =
        run::LocalRunStore::new(root.path().join("host-run-authority")).expect("Host store opens");
    let (runtime, _) = Runtime::start_with_run_store(config, Arc::new(host_store.clone()))
        .await
        .expect("runtime starts with Host authority");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    let created = runtime
        .create_run(autonomous_run_request("host_store_run", &session, 1))
        .await
        .expect("Run commits to Host store");
    assert_eq!(
        run::RunStore::load(&host_store, &created.snapshot.run.id)
            .unwrap()
            .unwrap()
            .run
            .revision,
        created.snapshot.run.revision
    );
    assert!(
        !default_store_path.exists(),
        "an injected Host store must replace, not mirror, LocalRunStore"
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

async fn claim_test_session_turn(
    runtime: &Runtime,
    created: &run::RunCommandResult,
    command_prefix: &str,
    turn_id: &str,
    prompt_digest: String,
) -> (run::OperationId, run::RunEnvelope) {
    let context = run::IterationContextManifest::new(
        created.snapshot.run.revision,
        0,
        "test-verifier",
        "test-model-v1",
        "workspace-v1",
    )
    .harness_snapshot(
        created
            .snapshot
            .run
            .harness
            .active
            .as_ref()
            .unwrap()
            .digest
            .clone(),
    );
    let iteration = runtime
        .inner
        .begin_iteration(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            created.snapshot.run.revision,
            run::CommandId::new(format!("{command_prefix}_begin")).unwrap(),
            run::BeginIteration::new(context),
        ))
        .await
        .unwrap();
    let operation_id =
        run::OperationId::new(format!("{}_operation", command_prefix.replace('-', "_"))).unwrap();
    let prepared = runtime
        .inner
        .prepare_operation(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            iteration.command.snapshot.run.revision,
            run::CommandId::new(format!("{command_prefix}_prepare")).unwrap(),
            run::PrepareOperation::new(
                operation_id.clone(),
                iteration.output.iteration_id,
                run::EffectClass::Reconcilable,
                run::EffectSpec::SessionTurn {
                    session: created.snapshot.run.session.clone(),
                    turn_id: turn_id.into(),
                    prompt_digest,
                    input: run::ArtifactRef::new(
                        "a".repeat(64),
                        "text/plain",
                        1,
                        "test",
                        created.snapshot.run.id.as_str(),
                    ),
                },
            ),
        ))
        .await
        .unwrap();
    let claimed = runtime
        .inner
        .claim_effect(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            prepared.snapshot.run.revision,
            run::CommandId::new(format!("{command_prefix}_claim")).unwrap(),
            run::ClaimEffect::new(operation_id.clone()).reservation(
                run::ResourceVector::default()
                    .iterations(1)
                    .agent_calls(1)
                    .agent_concurrency(1)
                    .active_ms(created.snapshot.run.budget.active_ms)
                    .wall_ms(created.snapshot.run.budget.wall_ms)
                    .tokens(created.snapshot.run.budget.tokens)
                    .cost_micros(created.snapshot.run.budget.cost_micros)
                    .artifact_bytes(created.snapshot.run.budget.artifact_bytes),
            ),
        ))
        .await
        .unwrap();
    (operation_id, claimed.command.snapshot)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autonomous_turn_loop_runs_multiple_ledgered_turns_and_budget_is_not_success() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    server.set_response("durable progress with evidence");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, _) = Runtime::start(runtime_config(&root, server.url()))
        .await
        .expect("runtime starts");

    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");
    let created = runtime
        .create_run(autonomous_run_request("vertical_run", &session, 4))
        .await
        .expect("Run created");
    let result = runtime
        .autonomous_turn_loop(autonomous_providers(&root.path().join("artifacts"), 1))
        .activate(
            AutonomousActivation::new(
                created.snapshot.run.id.clone(),
                "test-model-v1",
                "workspace-v1",
            )
            .max_iterations(3),
        )
        .await
        .expect("autonomous loop succeeds");
    assert_eq!(
        result.snapshot.run.lifecycle(),
        run::RunLifecycle::Finished(run::FinishedOutcome::Succeeded)
    );
    assert_eq!(result.iterations_executed, 2);
    let ledger = runtime.session_ledger(&session).await.unwrap();
    assert_eq!(ledger.entries.len(), 2);
    for entry in &ledger.entries {
        let LedgerTurnState::Completed {
            settlement_id,
            usage: Some(usage),
            ..
        } = &entry.state
        else {
            panic!("autonomous Turns must persist typed usage evidence");
        };
        assert_eq!(usage.resources.tokens, 14);
        assert!(!usage.is_unknown(run::ResourceDimension::Tokens));
        assert!(usage.is_unknown(run::ResourceDimension::CostMicros));
        assert!(settlement_id.starts_with("sha256:"));
    }
    assert_eq!(result.snapshot.run.usage.tokens, 28);
    assert!(
        result
            .snapshot
            .run
            .usage_unknown
            .contains(&run::ResourceDimension::CostMicros)
    );

    let budget_session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("budget session starts");
    let budget_run = runtime
        .create_run(autonomous_run_request("budget_run", &budget_session, 1))
        .await
        .expect("budget Run created");
    let budget_result = runtime
        .autonomous_turn_loop(autonomous_providers(
            &root.path().join("budget-artifacts"),
            usize::MAX,
        ))
        .activate(
            AutonomousActivation::new(
                budget_run.snapshot.run.id.clone(),
                "test-model-v1",
                "workspace-v1",
            )
            .max_iterations(2),
        )
        .await
        .expect("budget exhaustion is a normal wait");
    assert_eq!(
        budget_result.snapshot.run.lifecycle(),
        run::RunLifecycle::Waiting(run::WaitingReason::BudgetExhausted)
    );
    assert_ne!(budget_result.snapshot.run.status, run::RunStatus::Complete);

    let finite_session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("finite-budget session starts");
    let mut finite_request = autonomous_run_request("finite_token_run", &finite_session, 1);
    finite_request.budget.tokens = 100;
    let finite_run = runtime
        .create_run(finite_request)
        .await
        .expect("finite-budget Run created");
    let requests_before = server.requests().len();
    let error = runtime
        .autonomous_turn_loop(autonomous_providers(
            &root.path().join("finite-budget-artifacts"),
            0,
        ))
        .activate(AutonomousActivation::new(
            finite_run.snapshot.run.id.clone(),
            "test-model-v1",
            "workspace-v1",
        ))
        .await
        .expect_err("unsupported finite budget must fail before dispatch");
    assert!(matches!(
        error,
        Error::DurableRun(run::RunError::Validation(_))
    ));
    assert_eq!(server.requests().len(), requests_before);
    let unchanged = runtime
        .get_run(&finite_run.snapshot.run.id)
        .await
        .unwrap()
        .unwrap();
    assert!(unchanged.run.active_iteration.is_none());
    assert!(unchanged.run.operations.is_empty());
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autonomous_restart_preserves_pause_until_explicit_resume() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let session_config_value = session_config(workspace);
    let (first, _) = Runtime::start(config.clone()).await.expect("first runtime");
    let session = first
        .create_session(session_config_value.clone())
        .await
        .expect("session starts");
    let created = first
        .create_run(autonomous_run_request("paused_restart_run", &session, 4))
        .await
        .expect("Run created");
    let paused = first
        .control_run(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            created.snapshot.run.revision,
            run::CommandId::new("pause_before_restart").unwrap(),
            run::RunAction::Pause,
        ))
        .await
        .expect("Run pauses");
    assert_eq!(
        paused.snapshot.run.lifecycle(),
        run::RunLifecycle::Waiting(run::WaitingReason::User)
    );
    first.shutdown().await.expect("first runtime stops");

    let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .resume_session(session, session_config_value)
        .await
        .expect("session resumes");
    let result = restarted
        .autonomous_turn_loop(autonomous_providers(
            &root.path().join("paused-restart-artifacts"),
            0,
        ))
        .activate(AutonomousActivation::new(
            created.snapshot.run.id,
            "test-model-v1",
            "workspace-v1",
        ))
        .await
        .expect("paused Run reconciles without resuming");
    assert_eq!(
        result.snapshot.run.lifecycle(),
        run::RunLifecycle::Waiting(run::WaitingReason::User)
    );
    assert_eq!(result.iterations_executed, 0);
    assert!(
        server.requests().is_empty(),
        "restart recovery must not reactivate a paused Run"
    );
    restarted.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autonomous_restart_reconciles_pre_dispatch_intent_without_replaying_claim() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    server.set_response("recovered once");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let session_config_value = session_config(workspace.clone());
    let (first, _) = Runtime::start(config.clone()).await.expect("first runtime");
    let session = first
        .create_session(session_config_value.clone())
        .await
        .expect("session starts");
    let created = first
        .create_run(autonomous_run_request("pre_dispatch_run", &session, 4))
        .await
        .expect("Run created");
    let context = run::IterationContextManifest::new(
        created.snapshot.run.revision,
        0,
        "test-verifier",
        "test-model-v1",
        "workspace-v1",
    )
    .harness_snapshot(
        created
            .snapshot
            .run
            .harness
            .active
            .as_ref()
            .unwrap()
            .digest
            .clone(),
    );
    let iteration = first
        .inner
        .begin_iteration(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            created.snapshot.run.revision,
            run::CommandId::new("crash_begin").unwrap(),
            run::BeginIteration::new(context),
        ))
        .await
        .unwrap();
    let operation_id = run::OperationId::new("turn_1").unwrap();
    let prepared = first
        .inner
        .prepare_operation(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            iteration.command.snapshot.run.revision,
            run::CommandId::new("crash_prepare").unwrap(),
            run::PrepareOperation::new(
                operation_id.clone(),
                iteration.output.iteration_id,
                run::EffectClass::Reconcilable,
                run::EffectSpec::SessionTurn {
                    session: created.snapshot.run.session.clone(),
                    turn_id: "pre_dispatch_run_turn_1".into(),
                    prompt_digest: "b".repeat(64),
                    input: run::ArtifactRef::new(
                        "a".repeat(64),
                        "text/plain",
                        1,
                        "test",
                        "pre_dispatch_run",
                    ),
                },
            ),
        ))
        .await
        .unwrap();
    first
        .inner
        .claim_effect(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            prepared.snapshot.run.revision,
            run::CommandId::new("crash_claim").unwrap(),
            run::ClaimEffect::new(operation_id.clone()).reservation(
                run::ResourceVector::default()
                    .iterations(1)
                    .agent_calls(1)
                    .agent_concurrency(1)
                    .active_ms(u64::MAX)
                    .wall_ms(u64::MAX)
                    .tokens(u64::MAX)
                    .cost_micros(u64::MAX)
                    .artifact_bytes(u64::MAX),
            ),
        ))
        .await
        .unwrap();
    first.shutdown().await.expect("first runtime stops");

    let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .resume_session(session.clone(), session_config_value)
        .await
        .expect("session resumes");
    let result = restarted
        .autonomous_turn_loop(autonomous_providers(
            &root.path().join("restart-artifacts"),
            0,
        ))
        .activate(AutonomousActivation::new(
            created.snapshot.run.id,
            "test-model-v1",
            "workspace-v1",
        ))
        .await
        .expect("pre-dispatch crash recovers");
    assert_eq!(
        result.snapshot.run.lifecycle(),
        run::RunLifecycle::Finished(run::FinishedOutcome::Succeeded)
    );
    assert_eq!(
        result.snapshot.run.operations[&operation_id].state,
        run::OperationState::Abandoned
    );
    assert_eq!(
        restarted
            .session_ledger(&session)
            .await
            .unwrap()
            .entries
            .len(),
        1,
        "the uncertain claim was not dispatched or replayed"
    );
    restarted.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autonomous_restart_uses_completed_ledger_evidence_without_repeating_turn() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    server.set_response("completed ledger evidence");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let session_config_value = session_config(workspace);
    let (first, _) = Runtime::start(config.clone()).await.expect("first runtime");
    let session = first
        .create_session(session_config_value.clone())
        .await
        .expect("session starts");
    let created = first
        .create_run(autonomous_run_request("completed_ledger_run", &session, 4))
        .await
        .expect("Run created");
    let turn_id = "completed_ledger_run_turn_1";
    let prompt = "durable prompt completed before Run acknowledgement";
    let prompt_digest = crate::prompt_digest(prompt);
    let (operation_id, _claimed) =
        claim_test_session_turn(&first, &created, "completed_ledger", turn_id, prompt_digest).await;
    first
        .prompt(&session, turn_id, prompt)
        .await
        .expect("SessionLedger settlement commits");
    // Simulate process loss before the Run callback is persisted.
    first.shutdown().await.expect("first runtime stops");

    let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .resume_session(session.clone(), session_config_value)
        .await
        .expect("session resumes");
    let result = restarted
        .autonomous_turn_loop(autonomous_providers(
            &root.path().join("completed-ledger-artifacts"),
            0,
        ))
        .activate(AutonomousActivation::new(
            created.snapshot.run.id,
            "test-model-v1",
            "workspace-v1",
        ))
        .await
        .expect("completed ledger recovers");
    assert_eq!(
        result.snapshot.run.operations[&operation_id].state,
        run::OperationState::Reconciled
    );
    let recovered_usage = result.snapshot.run.operations[&operation_id]
        .receipt
        .as_ref()
        .and_then(|receipt| receipt.actual_usage.as_deref())
        .expect("recovery persists typed actual usage");
    assert_eq!(recovered_usage.resources.artifact_bytes, 0);
    assert!(
        recovered_usage.is_unknown(run::ResourceDimension::ArtifactBytes),
        "SessionLedger cannot prove whether the SDK artifact committed before a crash"
    );
    assert_eq!(
        result.snapshot.run.lifecycle(),
        run::RunLifecycle::Recovering
    );
    assert!(
        result
            .recovery_needs
            .iter()
            .any(|need| matches!(need, run::RecoveryNeed::ActiveIteration { .. }))
    );
    let resolved = restarted
        .resolve_run_recovery(run::MutationRequest::new(
            result.snapshot.run.id.clone(),
            result.snapshot.run.revision,
            run::CommandId::new("resolve_completed_ledger_iteration").unwrap(),
            run::RecoveryResolution::new(true, true),
        ))
        .await
        .expect("SDK derives usage from typed ledger evidence");
    assert_eq!(resolved.snapshot.run.lifecycle(), run::RunLifecycle::Active);
    let continued = restarted
        .autonomous_turn_loop(autonomous_providers(
            &root.path().join("completed-ledger-continuation-artifacts"),
            0,
        ))
        .activate(AutonomousActivation::new(
            resolved.snapshot.run.id,
            "test-model-v1",
            "workspace-v1",
        ))
        .await
        .expect("recovered Run continues with a new iteration");
    assert_eq!(
        continued.snapshot.run.lifecycle(),
        run::RunLifecycle::Finished(run::FinishedOutcome::Succeeded)
    );
    let ledger = restarted.session_ledger(&session).await.unwrap();
    assert_eq!(ledger.entries.len(), 2);
    assert_eq!(
        ledger
            .entries
            .iter()
            .filter(|entry| entry.turn_id == turn_id)
            .count(),
        1,
        "the settled Turn identity must never be replayed"
    );
    restarted.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finite_artifact_budget_cannot_be_recovered_from_session_ledger_as_zero() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    server.set_response("completed under a finite artifact budget");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, _) = Runtime::start(runtime_config(&root, server.url()))
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    let mut request = autonomous_run_request("finite_artifact_recovery", &session, 2);
    request.budget.artifact_bytes = 1;
    let created = runtime.create_run(request).await.expect("Run created");
    let turn_id = "finite_artifact_recovery_turn";
    let prompt = "durable prompt with unknown recovered artifact usage";
    let (operation_id, claimed) = claim_test_session_turn(
        &runtime,
        &created,
        "finite_artifact_recovery",
        turn_id,
        crate::prompt_digest(prompt),
    )
    .await;
    runtime
        .prompt(&session, turn_id, prompt)
        .await
        .expect("SessionLedger completion commits");

    let error = runtime
        .reconcile_run(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            claimed.run.revision,
            run::CommandId::new("finite_artifact_reconcile").unwrap(),
            (),
        ))
        .await
        .expect_err("unknown artifact usage cannot settle a finite budget");
    assert!(matches!(error, Error::DurableRun(run::RunError::Budget)));
    let persisted = runtime
        .get_run(&created.snapshot.run.id)
        .await
        .unwrap()
        .expect("Run remains durable");
    assert_eq!(persisted.run.lifecycle(), run::RunLifecycle::Recovering);
    assert_eq!(
        persisted.run.operations[&operation_id].state,
        run::OperationState::Uncertain,
        "the applied Turn must remain fenced rather than reactivate with fabricated zero usage"
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_reconciliation_fails_closed_on_ledger_identity_conflict() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    server.set_response("existing conflicting Turn");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, _) = Runtime::start(runtime_config(&root, server.url()))
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    let conflicting_turn_id = "identity_conflict_turn";
    runtime
        .prompt(&session, conflicting_turn_id, "first prompt identity")
        .await
        .expect("existing Turn settles");
    let created = runtime
        .create_run(autonomous_run_request("ledger_conflict_run", &session, 4))
        .await
        .expect("Run created");
    let (operation_id, claimed) = claim_test_session_turn(
        &runtime,
        &created,
        "ledger_conflict",
        conflicting_turn_id,
        "b".repeat(64),
    )
    .await;
    let plan = runtime
        .reconcile_run(run::MutationRequest::new(
            created.snapshot.run.id,
            claimed.run.revision,
            run::CommandId::new("ledger_conflict_recovery").unwrap(),
            (),
        ))
        .await
        .expect("reconciliation remains explicit");
    assert_eq!(plan.snapshot.run.lifecycle(), run::RunLifecycle::Recovering);
    assert!(plan.needs.iter().any(|need| matches!(
        need,
        run::RecoveryNeed::SessionTurnLedger {
            operation_id: candidate,
            ..
        } if candidate == &operation_id
    )));
    assert_eq!(
        plan.snapshot.run.operations[&operation_id].state,
        run::OperationState::Uncertain
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discarded_ledger_entry_without_exact_rewind_receipt_stays_uncertain() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, _) = Runtime::start(runtime_config(&root, server.url()))
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    let created = runtime
        .create_run(autonomous_run_request(
            "discarded_without_rewind_run",
            &session,
            4,
        ))
        .await
        .expect("Run created");
    let turn_id = "discarded_without_rewind_turn";
    let prompt_digest = crate::prompt_digest("possibly dispatched prompt");
    let (operation_id, claimed) = claim_test_session_turn(
        &runtime,
        &created,
        "discarded_without_rewind",
        turn_id,
        prompt_digest.clone(),
    )
    .await;
    runtime
        .mark_turn_discarded(&session, turn_id, &prompt_digest, 0)
        .await
        .expect("simulate ledger-only discard without native rewind evidence");

    let plan = runtime
        .reconcile_run(run::MutationRequest::new(
            created.snapshot.run.id,
            claimed.run.revision,
            run::CommandId::new("recover_discarded_without_rewind").unwrap(),
            (),
        ))
        .await
        .expect("recovery remains explicit");
    assert_eq!(plan.snapshot.run.lifecycle(), run::RunLifecycle::Recovering);
    assert_eq!(
        plan.snapshot.run.operations[&operation_id].state,
        run::OperationState::Uncertain
    );
    assert!(plan.needs.iter().any(|need| matches!(
        need,
        run::RecoveryNeed::SessionTurnLedger {
            operation_id: candidate,
            ..
        } if candidate == &operation_id
    )));
    assert!(
        server.requests().is_empty(),
        "a ledger-only discard must never authorize replay"
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autonomous_restart_advances_finished_iteration_without_another_turn() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let session_config_value = session_config(workspace);
    let (first, _) = Runtime::start(config.clone()).await.expect("first runtime");
    let session = first
        .create_session(session_config_value.clone())
        .await
        .expect("session starts");
    let created = first
        .create_run(autonomous_run_request("finished_boundary_run", &session, 4))
        .await
        .expect("Run created");
    let context = run::IterationContextManifest::new(
        created.snapshot.run.revision,
        0,
        "test-verifier",
        "test-model-v1",
        "workspace-v1",
    )
    .harness_snapshot(
        created
            .snapshot
            .run
            .harness
            .active
            .as_ref()
            .unwrap()
            .digest
            .clone(),
    );
    let iteration = first
        .inner
        .begin_iteration(run::MutationRequest::new(
            created.snapshot.run.id.clone(),
            created.snapshot.run.revision,
            run::CommandId::new("finished_boundary_begin").unwrap(),
            run::BeginIteration::new(context),
        ))
        .await
        .unwrap();
    first
        .inner
        .finish_iteration(run::FinishIteration::new(
            &iteration.output,
            true,
            "verified before crash",
            run::GoalVerdict::Achieved,
            run::ResourceVector::default().iterations(1).agent_calls(1),
        ))
        .await
        .unwrap();
    first.shutdown().await.expect("first runtime stops");

    let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .resume_session(session, session_config_value)
        .await
        .expect("session resumes");
    let result = restarted
        .autonomous_turn_loop(autonomous_providers(
            &root.path().join("finished-boundary-artifacts"),
            0,
        ))
        .activate(AutonomousActivation::new(
            created.snapshot.run.id,
            "test-model-v1",
            "workspace-v1",
        ))
        .await
        .expect("finished boundary advances");
    assert_eq!(
        result.snapshot.run.lifecycle(),
        run::RunLifecycle::Finished(run::FinishedOutcome::Succeeded)
    );
    assert!(
        server.requests().is_empty(),
        "a durable finished iteration must advance without another model Turn"
    );
    restarted.shutdown().await.expect("runtime shuts down");
}
