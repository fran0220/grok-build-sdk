use super::model::*;
use super::store::{RunStore, StoreCommit, StoreCommitResult};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub struct RunController<S: RunStore> {
    store: S,
    runs: BTreeMap<RunId, RunEnvelope>,
    /// Loaded nonterminal Runs must acquire a new durable epoch and reconcile
    /// ledger evidence before this process is allowed to mutate them.
    authority_fenced: BTreeSet<RunId>,
    /// A CAS conflict or indeterminate commit requires a fresh durable read;
    /// recovery is not allowed to proceed from the pre-commit cache.
    reload_required: BTreeSet<RunId>,
}

impl<S: RunStore> RunController<S> {
    pub fn open(store: S) -> Result<Self, RunError> {
        let mut runs = BTreeMap::new();
        let mut authority_fenced = BTreeSet::new();
        for envelope in store.list()? {
            envelope.validate()?;
            if needs_controller_recovery(&envelope.run) {
                authority_fenced.insert(envelope.run.id.clone());
            }
            runs.insert(envelope.run.id.clone(), envelope);
        }
        Ok(Self {
            store,
            runs,
            authority_fenced,
            reload_required: BTreeSet::new(),
        })
    }

    pub fn create_run(
        &mut self,
        request: CreateRunRequest,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        let input_digest = canonical_digest(&("create_run", &request))?;
        for envelope in self.runs.values() {
            if let Some(receipt) = envelope.run.command_receipts.get(&request.command_id) {
                if receipt.input_digest != input_digest {
                    return Err(RunError::Integrity(
                        "command id was reused with different create input".into(),
                    ));
                }
                return Ok(RunCommandResult {
                    receipt: receipt.clone(),
                    snapshot: envelope.clone(),
                    duplicate: true,
                });
            }
        }
        request.goal.validate()?;
        request.capabilities.validate()?;
        if !matches!(&request.driver, RunDriverSpec::AutonomousTurnLoop { .. }) {
            return Err(RunError::Validation(
                "only AutonomousTurnLoop is executable in Run schema v1".into(),
            ));
        }
        if request
            .driver
            .session()
            .is_some_and(|session| session != &request.session)
        {
            return Err(RunError::Validation(
                "driver and Run must reference the same Session".into(),
            ));
        }
        if request.verifier_policy_digest.trim().is_empty()
            || request.verifier_policy_digest.len() > 256
        {
            return Err(RunError::Validation(
                "verifier policy digest is empty or too large".into(),
            ));
        }
        if (!request.goal.acceptance_criteria.is_empty()
            || !request.goal.required_evidence.is_empty())
            && request.required_gates.is_empty()
        {
            return Err(RunError::Validation(
                "acceptance criteria require at least one deterministic gate".into(),
            ));
        }
        if self.runs.values().any(|envelope| {
            envelope.run.session == request.session && !envelope.run.status.is_terminal()
        }) {
            return Err(RunError::Conflict {
                expected: None,
                actual: None,
            });
        }

        let run_id = request.run_id.clone().unwrap_or_else(RunId::random);
        let revision = RunRevision::new(1);
        let epoch = ControllerEpoch::new(1);
        let event_cursor = RunEventCursor::new(1);
        let receipt = CommandReceipt {
            command_id: request.command_id.clone(),
            input_digest,
            disposition: CommandDisposition::Applied,
            committed_revision: revision,
            epoch,
        };
        let (current_strategy_revision, current_workflow_revision) = match &request.driver {
            RunDriverSpec::AutonomousTurnLoop {
                strategy_revision, ..
            } => (*strategy_revision, None),
            RunDriverSpec::RhaiWorkflow {
                workflow_revision, ..
            } => (0, Some(*workflow_revision)),
            RunDriverSpec::External { .. } | RunDriverSpec::Unknown => (0, None),
        };
        let run = RunRecord {
            revision,
            controller_epoch: epoch,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            id: run_id.clone(),
            session: request.session,
            goal: request.goal,
            driver: request.driver,
            status: RunStatus::Active,
            stage: RunStage::Idle,
            capabilities: request.capabilities,
            required_gates: request.required_gates,
            verifier_policy_digest: request.verifier_policy_digest,
            budget: request.budget,
            usage: ResourceVector::default(),
            child_reserved: ResourceVector::default(),
            next_iteration_id: 1,
            active_iteration: None,
            iterations: VecDeque::new(),
            operations: BTreeMap::new(),
            children: BTreeMap::new(),
            mailbox: BTreeMap::new(),
            next_message_sequence: 1,
            steering: BTreeMap::new(),
            steering_high_water: 0,
            strategy_revisions: Vec::new(),
            current_strategy_revision,
            workflow_revisions: Vec::new(),
            current_workflow_revision,
            verdict: None,
            pending_approval: false,
            recovery_prior_status: None,
            command_receipts: BTreeMap::from([(request.command_id, receipt.clone())]),
            terminal_report_claimed: false,
            event_cursor,
        };
        let envelope = RunEnvelope {
            schema_version: RUN_SCHEMA_VERSION,
            run,
            events: VecDeque::from([RunEvent {
                cursor: event_cursor,
                revision,
                kind: internal_event_kind("run_created"),
                at_ms: now_ms,
            }]),
        };
        envelope.validate()?;
        self.commit(None, envelope.clone(), None)?;
        self.runs.insert(run_id, envelope.clone());
        Ok(RunCommandResult {
            receipt,
            snapshot: envelope,
            duplicate: false,
        })
    }

    pub fn get_run(&self, run_id: &RunId) -> Option<RunEnvelope> {
        self.runs.get(run_id).cloned()
    }

    pub fn list_runs(&self) -> Vec<RunEnvelope> {
        self.runs.values().cloned().collect()
    }

    pub fn list_recoverable_runs(&self) -> Vec<RunEnvelope> {
        self.runs
            .values()
            .filter(|envelope| {
                needs_controller_recovery(&envelope.run)
                    && (self.authority_fenced.contains(&envelope.run.id)
                        || envelope.run.status == RunStatus::RecoveryRequired
                        || envelope.run.active_iteration.is_some()
                        || envelope.run.operations.values().any(|operation| {
                            matches!(
                                operation.state,
                                OperationState::Dispatching | OperationState::Uncertain
                            )
                        })
                        || envelope
                            .run
                            .children
                            .values()
                            .any(|child| !child.state.is_terminal()))
            })
            .cloned()
            .collect()
    }

    pub fn recovery_plan(&self, run_id: &RunId) -> Result<RecoveryPlan, RunError> {
        let snapshot = self.runs.get(run_id).cloned().ok_or(RunError::NotFound)?;
        if snapshot.run.status != RunStatus::RecoveryRequired {
            return Err(RunError::InvalidTransition(
                "Run is not in durable recovery".into(),
            ));
        }
        Ok(RecoveryPlan {
            needs: recovery_needs(&snapshot.run),
            snapshot,
        })
    }

    pub fn reload_run(&mut self, run_id: &RunId) -> Result<Option<RunEnvelope>, RunError> {
        let loaded = self.store.load(run_id)?;
        self.reload_required.remove(run_id);
        if let Some(envelope) = &loaded {
            envelope.validate()?;
            if needs_controller_recovery(&envelope.run) {
                self.authority_fenced.insert(run_id.clone());
            } else {
                self.authority_fenced.remove(run_id);
            }
            self.runs.insert(run_id.clone(), envelope.clone());
        } else {
            self.runs.remove(run_id);
            self.authority_fenced.remove(run_id);
        }
        Ok(loaded)
    }

    pub fn reload_is_required(&self, run_id: &RunId) -> bool {
        self.reload_required.contains(run_id)
    }

    pub fn attach_run(&self, run_id: &RunId, after: RunEventCursor) -> Result<RunAttach, RunError> {
        let envelope = self.runs.get(run_id).ok_or(RunError::NotFound)?;
        let current = envelope.run.event_cursor;
        let first = envelope.events.front().map(|event| event.cursor);
        let contiguous = after.get() <= current.get()
            && match first {
                Some(first) => after
                    .get()
                    .checked_add(1)
                    .is_some_and(|next| next >= first.get()),
                None => after == current,
            };
        if !contiguous {
            return Ok(RunAttach::Snapshot(envelope.clone()));
        }
        Ok(RunAttach::Replay {
            run_id: run_id.clone(),
            through: current,
            events: envelope
                .events
                .iter()
                .filter(|event| event.cursor > after)
                .cloned()
                .collect(),
        })
    }

    pub fn begin_recovery(
        &mut self,
        request: MutationRequest<()>,
        now_ms: u64,
    ) -> Result<RecoveryPlan, RunError> {
        let run_id = request.run_id.clone();
        if self.reload_required.contains(&run_id) {
            return Err(RunError::ReloadRequired);
        }
        let acquiring_new_epoch = self.authority_fenced.contains(&run_id);
        let command = self.apply_command(
            request,
            "begin_recovery",
            "recovery_started",
            now_ms,
            true,
            |run| {
                if run.status == RunStatus::Tombstoned
                    || (run.status.is_terminal()
                        && run.active_iteration.is_none()
                        && !has_unsettled_work(run))
                {
                    return Err(RunError::InvalidTransition(
                        "settled terminal Run cannot enter recovery".into(),
                    ));
                }
                if !matches!(run.status, RunStatus::Active | RunStatus::RecoveryRequired) {
                    run.recovery_prior_status.get_or_insert(run.status);
                }
                run.controller_epoch = ControllerEpoch::new(
                    run.controller_epoch
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| RunError::Integrity("controller epoch overflow".into()))?,
                );
                run.status = RunStatus::RecoveryRequired;
                run.stage = RunStage::Recovering;
                for operation in run.operations.values_mut() {
                    if operation.state == OperationState::Dispatching {
                        operation.state = OperationState::Uncertain;
                    }
                }
                Ok(())
            },
        )?;
        if acquiring_new_epoch && command.duplicate {
            return Err(RunError::AuthorityLost);
        }
        self.authority_fenced.remove(&run_id);
        let needs = recovery_needs(&command.snapshot.run);
        Ok(RecoveryPlan {
            snapshot: command.snapshot,
            needs,
        })
    }

    pub fn finish_recovery(
        &mut self,
        request: MutationRequest<RecoveryResolution>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "finish_recovery",
            "recovery_finished",
            now_ms,
            false,
            |run| {
                if run.status != RunStatus::RecoveryRequired || run.stage != RunStage::Recovering {
                    return Err(RunError::InvalidTransition("Run is not reconciling".into()));
                }
                if run.operations.values().any(|operation| {
                    matches!(
                        operation.state,
                        OperationState::Dispatching | OperationState::Uncertain
                    )
                }) || run
                    .children
                    .values()
                    .any(|child| !child.state.is_terminal())
                {
                    return Err(RunError::InvalidTransition(
                        "recovery evidence remains unresolved".into(),
                    ));
                }
                if let Some(mut iteration) = run.active_iteration.take() {
                    if !request.input.abandon_active_iteration {
                        run.active_iteration = Some(iteration);
                        return Err(RunError::InvalidTransition(
                            "active iteration requires explicit reconciliation".into(),
                        ));
                    }
                    let applied = run.operations.values().any(|operation| {
                        operation.iteration_id == iteration.iteration_id
                            && matches!(
                                operation.state,
                                OperationState::Acknowledged | OperationState::Reconciled
                            )
                    });
                    if applied {
                        let usage = request.input.recovered_usage.as_ref().ok_or_else(|| {
                            RunError::InvalidTransition(
                                "applied iteration recovery requires observed usage".into(),
                            )
                        })?;
                        if usage.iterations != 1 || usage.agent_calls == 0 {
                            return Err(RunError::Validation(
                                "recovered iteration usage must charge one iteration and at least one agent call"
                                    .into(),
                            ));
                        }
                        let total = run
                            .usage
                            .add_usage(usage)
                            .and_then(|value| value.with_reservations(&run.child_reserved))
                            .ok_or(RunError::Budget)?;
                        if !total.within(&run.budget) {
                            return Err(RunError::Budget);
                        }
                        run.usage = run.usage.add_usage(usage).ok_or(RunError::Budget)?;
                    } else if request.input.recovered_usage.is_some() {
                        return Err(RunError::Validation(
                            "unused recovered iteration usage was supplied".into(),
                        ));
                    }
                    for operation in run.operations.values_mut().filter(|operation| {
                        operation.iteration_id == iteration.iteration_id
                    }) {
                        if matches!(
                            operation.state,
                            OperationState::Prepared | OperationState::FailedRetryable
                        ) {
                            operation.state = OperationState::Abandoned;
                            operation.active_attempt = None;
                            operation.terminal_result_digest = None;
                        }
                    }
                    if has_unsettled_operations(run) {
                        return Err(RunError::InvalidTransition(
                            "abandoned iteration still has unresolved effects".into(),
                        ));
                    }
                    iteration.driver_terminal_success = false;
                    iteration.summary = Some("abandoned during durable recovery".into());
                    iteration.verdict = Some(GoalVerdict::Unverifiable);
                    iteration.finished_at_ms = Some(now_ms);
                    iteration.result_digest = Some(canonical_digest(&(
                        "recovery_abandon",
                        iteration.iteration_id,
                        iteration.token.clone(),
                    ))?);
                    iteration.recovery_abandoned = true;
                    push_iteration(run, iteration);
                }
                run.status = if let Some(status) = run.recovery_prior_status.take() {
                    status
                } else if request.input.resume {
                    RunStatus::Active
                } else {
                    RunStatus::UserPaused
                };
                run.stage = RunStage::Idle;
                Ok(())
            },
        )
    }

    pub fn control_run(
        &mut self,
        request: MutationRequest<RunAction>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "control_run",
            "run_controlled",
            now_ms,
            false,
            |run| apply_control(run, &request.input, now_ms),
        )
    }

    pub fn wake_run(
        &mut self,
        request: MutationRequest<RunAction>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        if !matches!(request.input, RunAction::Resume { .. }) {
            return Err(RunError::Validation(
                "wake requires a Resume Run action".into(),
            ));
        }
        self.apply_command(
            request.clone(),
            "wake_run",
            "run_woken",
            now_ms,
            false,
            |run| apply_control(run, &request.input, now_ms),
        )
    }

    pub fn begin_iteration(
        &mut self,
        request: MutationRequest<BeginIteration>,
        now_ms: u64,
    ) -> Result<CommandOutput<IterationHandle>, RunError> {
        let run_id = request.run_id.clone();
        let command = self.apply_command(
            request.clone(),
            "begin_iteration",
            "iteration_started",
            now_ms,
            false,
            |run| {
                if run.status != RunStatus::Active || run.active_iteration.is_some() {
                    return Err(RunError::InvalidTransition(
                        "Run is paused, recovering, terminal, or already executing".into(),
                    ));
                }
                if request.input.context.goal_revision != request.expected_revision
                    || request.input.context.strategy_revision != run.current_strategy_revision
                    || request.input.context.workflow_revision != run.current_workflow_revision
                    || request.input.context.policy_digest != run.verifier_policy_digest
                {
                    return Err(RunError::Conflict {
                        expected: Some(request.expected_revision),
                        actual: Some(run.revision),
                    });
                }
                if request.input.context.artifacts.len() > MAX_LIST_ITEMS {
                    return Err(RunError::Validation(
                        "iteration context has too many artifacts".into(),
                    ));
                }
                for artifact in &request.input.context.artifacts {
                    artifact.validate()?;
                }
                let projected = run
                    .usage
                    .add_usage(&ResourceVector {
                        iterations: 1,
                        ..ResourceVector::default()
                    })
                    .and_then(|usage| usage.with_reservations(&run.child_reserved))
                    .ok_or(RunError::Budget)?;
                if !projected.within(&run.budget) {
                    return Err(RunError::Budget);
                }
                let iteration_id = IterationId::new(run.next_iteration_id);
                run.next_iteration_id = run
                    .next_iteration_id
                    .checked_add(1)
                    .ok_or_else(|| RunError::Integrity("iteration id overflow".into()))?;
                run.active_iteration = Some(IterationManifest {
                    iteration_id,
                    token: IterationToken::random(),
                    context: request.input.context.clone(),
                    started_at_ms: now_ms,
                    driver_terminal_success: false,
                    summary: None,
                    evidence: Vec::new(),
                    gates: BTreeMap::new(),
                    verdict: None,
                    finished_at_ms: None,
                    result_digest: None,
                    recovery_abandoned: false,
                });
                run.stage = RunStage::Executing;
                Ok(())
            },
        )?;
        let iteration = command
            .snapshot
            .run
            .active_iteration
            .as_ref()
            .ok_or_else(|| RunError::Integrity("iteration command lost its output".into()))?;
        Ok(CommandOutput {
            output: IterationHandle {
                run_id,
                iteration_id: iteration.iteration_id,
                token: iteration.token.clone(),
                epoch: command.snapshot.run.controller_epoch,
                committed_revision: command.snapshot.run.revision,
            },
            command,
        })
    }

    pub fn finish_iteration(
        &mut self,
        callback: FinishIteration,
        now_ms: u64,
    ) -> Result<CallbackResult, RunError> {
        self.ensure_authority(&callback.run_id)?;
        let old = self
            .runs
            .get(&callback.run_id)
            .cloned()
            .ok_or(RunError::NotFound)?;
        if old.run.controller_epoch != callback.epoch {
            return Err(RunError::StaleEpoch);
        }
        let result_digest = canonical_digest(&callback)?;
        if let Some(iteration) = old
            .run
            .iterations
            .iter()
            .find(|iteration| iteration.iteration_id == callback.iteration_id)
        {
            if iteration.token == callback.token
                && iteration.result_digest.as_deref() == Some(&result_digest)
            {
                return Ok(CallbackResult {
                    snapshot: old,
                    duplicate: true,
                });
            }
            return Err(RunError::StaleCallback);
        }
        if old.run.status != RunStatus::Active {
            return Err(RunError::InvalidTransition(
                "late iteration result cannot transition a paused or terminal Run".into(),
            ));
        }
        let active = old
            .run
            .active_iteration
            .as_ref()
            .ok_or(RunError::StaleCallback)?;
        if active.iteration_id != callback.iteration_id || active.token != callback.token {
            return Err(RunError::StaleCallback);
        }
        if callback.summary.len() > MAX_ITEM_BYTES
            || callback.evidence.len() > MAX_LIST_ITEMS
            || callback.usage.iterations != 1
        {
            return Err(RunError::Validation(
                "iteration result exceeds bounds or has invalid iteration usage".into(),
            ));
        }
        for artifact in &callback.evidence {
            artifact.validate()?;
        }
        if old.run.operations.values().any(|operation| {
            operation.iteration_id == callback.iteration_id
                && !matches!(
                    operation.state,
                    OperationState::Acknowledged | OperationState::Reconciled
                )
        }) {
            return Err(RunError::InvalidTransition(
                "iteration has unresolved committed effects".into(),
            ));
        }
        let total = old
            .run
            .usage
            .add_usage(&callback.usage)
            .and_then(|usage| usage.with_reservations(&old.run.child_reserved))
            .ok_or(RunError::Budget)?;
        if !total.within(&old.run.budget) {
            return Err(RunError::Budget);
        }
        let mut next = old.clone();
        let mut iteration = next
            .run
            .active_iteration
            .take()
            .ok_or(RunError::StaleCallback)?;
        iteration.driver_terminal_success = callback.driver_terminal_success;
        iteration.summary = Some(callback.summary);
        iteration.evidence = callback.evidence;
        iteration.gates = callback.gates;
        iteration.verdict = Some(callback.verdict);
        iteration.finished_at_ms = Some(now_ms);
        iteration.result_digest = Some(result_digest);
        next.run.usage = next
            .run
            .usage
            .add_usage(&callback.usage)
            .ok_or(RunError::Budget)?;
        next.run.verdict = Some(callback.verdict);
        next.run.stage = if callback.verdict == GoalVerdict::Achieved {
            RunStage::Verifying
        } else {
            RunStage::Refining
        };
        push_iteration(&mut next.run, iteration.clone());
        let snapshot =
            self.commit_callback(old, next, "iteration_finished", now_ms, Some(iteration))?;
        Ok(CallbackResult {
            snapshot,
            duplicate: false,
        })
    }

    pub fn prepare_operation(
        &mut self,
        request: MutationRequest<PrepareOperation>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "prepare_operation",
            "effect_committed",
            now_ms,
            false,
            |run| {
                if run.status != RunStatus::Active {
                    return Err(RunError::InvalidTransition(
                        "effects can only be prepared by an active Run".into(),
                    ));
                }
                let iteration = run
                    .active_iteration
                    .as_ref()
                    .ok_or_else(|| RunError::InvalidTransition("no active iteration".into()))?;
                if iteration.iteration_id != request.input.iteration_id {
                    return Err(RunError::StaleCallback);
                }
                validate_effect_capability(run, &request.input.spec)?;
                let digest = request.input.spec.digest()?;
                if let Some(existing) = run.operations.get(&request.input.operation_id) {
                    if existing.spec_digest == digest
                        && existing.iteration_id == request.input.iteration_id
                        && existing.effect_class == request.input.effect_class
                    {
                        return Ok(());
                    }
                    return Err(RunError::Integrity(
                        "operation id was reused for a different effect".into(),
                    ));
                }
                run.operations.insert(
                    request.input.operation_id.clone(),
                    Operation {
                        id: request.input.operation_id.clone(),
                        iteration_id: request.input.iteration_id,
                        effect_class: request.input.effect_class,
                        spec: request.input.spec.clone(),
                        spec_digest: digest,
                        state: OperationState::Prepared,
                        next_attempt: 1,
                        active_attempt: None,
                        receipt: None,
                        terminal_result_digest: None,
                    },
                );
                Ok(())
            },
        )
    }

    pub fn claim_effect(
        &mut self,
        request: MutationRequest<ClaimEffect>,
        now_ms: u64,
    ) -> Result<CommandOutput<CommittedEffect>, RunError> {
        let run_id = request.run_id.clone();
        let operation_id = request.input.operation_id.clone();
        let command = self.apply_command(
            request,
            "claim_effect",
            "effect_claimed",
            now_ms,
            false,
            |run| {
                if run.status != RunStatus::Active {
                    return Err(RunError::InvalidTransition(
                        "only active Runs may claim effects".into(),
                    ));
                }
                let operation = run
                    .operations
                    .get_mut(&operation_id)
                    .ok_or(RunError::NotFound)?;
                let active_iteration = run.active_iteration.as_ref().ok_or_else(|| {
                    RunError::InvalidTransition("effect has no active owning iteration".into())
                })?;
                if active_iteration.iteration_id != operation.iteration_id {
                    return Err(RunError::InvalidTransition(
                        "effect does not belong to the active iteration".into(),
                    ));
                }
                if !matches!(
                    operation.state,
                    OperationState::Prepared | OperationState::FailedRetryable
                ) {
                    return Err(RunError::InvalidTransition(
                        "effect is not available in the committed outbox".into(),
                    ));
                }
                if operation.effect_class == EffectClass::NonRepeatable
                    && operation.next_attempt > 1
                {
                    return Err(RunError::InvalidTransition(
                        "non-repeatable effect cannot be retried automatically".into(),
                    ));
                }
                let attempt = operation.next_attempt;
                operation.next_attempt = operation
                    .next_attempt
                    .checked_add(1)
                    .ok_or_else(|| RunError::Integrity("effect attempt overflow".into()))?;
                operation.active_attempt = Some(OperationAttempt {
                    attempt,
                    token: DispatchToken::random(),
                    epoch: run.controller_epoch,
                });
                operation.terminal_result_digest = None;
                operation.state = OperationState::Dispatching;
                Ok(())
            },
        )?;
        let operation = command
            .snapshot
            .run
            .operations
            .get(&operation_id)
            .ok_or_else(|| RunError::Integrity("effect claim lost operation".into()))?;
        let attempt = operation
            .active_attempt
            .as_ref()
            .ok_or_else(|| RunError::Integrity("effect claim lost attempt token".into()))?;
        Ok(CommandOutput {
            output: CommittedEffect {
                run_id,
                operation_id,
                iteration_id: operation.iteration_id,
                attempt: attempt.attempt,
                token: attempt.token.clone(),
                epoch: attempt.epoch,
                effect_class: operation.effect_class,
                spec: operation.spec.clone(),
            },
            command,
        })
    }

    pub fn acknowledge_effect(
        &mut self,
        callback: EffectCallback,
        now_ms: u64,
    ) -> Result<CallbackResult, RunError> {
        self.ensure_authority(&callback.run_id)?;
        let old = self
            .runs
            .get(&callback.run_id)
            .cloned()
            .ok_or(RunError::NotFound)?;
        if old.run.controller_epoch != callback.epoch {
            return Err(RunError::StaleEpoch);
        }
        let result_digest = canonical_digest(&callback.outcome)?;
        let operation = old
            .run
            .operations
            .get(&callback.operation_id)
            .ok_or(RunError::NotFound)?;
        if let (Some(accepted_attempt), Some(accepted_digest)) = (
            operation.active_attempt.as_ref(),
            operation.terminal_result_digest.as_deref(),
        ) {
            if operation.iteration_id == callback.iteration_id
                && accepted_attempt.attempt == callback.attempt
                && accepted_attempt.token == callback.token
                && accepted_attempt.epoch == callback.epoch
                && accepted_digest == result_digest
            {
                return Ok(CallbackResult {
                    snapshot: old,
                    duplicate: true,
                });
            }
            return Err(RunError::StaleCallback);
        }
        let attempt = operation
            .active_attempt
            .as_ref()
            .ok_or(RunError::StaleCallback)?;
        if operation.iteration_id != callback.iteration_id
            || attempt.attempt != callback.attempt
            || attempt.token != callback.token
            || attempt.epoch != callback.epoch
        {
            return Err(RunError::StaleCallback);
        }
        let mut next = old.clone();
        let operation = next
            .run
            .operations
            .get_mut(&callback.operation_id)
            .ok_or(RunError::NotFound)?;
        let mut requires_recovery = false;
        match callback.outcome {
            EffectOutcome::Applied { receipt } => {
                validate_effect_receipt(&operation.spec, &receipt)?;
                operation.receipt = Some(receipt);
                operation.terminal_result_digest = Some(result_digest);
                operation.state = OperationState::Acknowledged;
            }
            EffectOutcome::FailedRetryable { message } => {
                if message.len() > MAX_ITEM_BYTES {
                    return Err(RunError::Validation("effect error exceeds bounds".into()));
                }
                if operation.effect_class == EffectClass::NonRepeatable {
                    operation.state = OperationState::Uncertain;
                    requires_recovery = true;
                } else {
                    operation.state = OperationState::FailedRetryable;
                }
                operation.terminal_result_digest = Some(result_digest);
            }
            EffectOutcome::Unknown { message } => {
                if message.len() > MAX_ITEM_BYTES {
                    return Err(RunError::Validation("effect error exceeds bounds".into()));
                }
                if matches!(
                    operation.effect_class,
                    EffectClass::Replayable | EffectClass::Idempotent
                ) {
                    operation.state = OperationState::Prepared;
                } else {
                    operation.state = OperationState::Uncertain;
                    requires_recovery = true;
                }
                operation.terminal_result_digest = Some(result_digest);
            }
        }
        if requires_recovery {
            enter_recovery(&mut next.run);
        }
        let snapshot = self.commit_callback(old, next, "effect_acknowledged", now_ms, None)?;
        Ok(CallbackResult {
            snapshot,
            duplicate: false,
        })
    }

    pub fn reconcile_effect(
        &mut self,
        request: MutationRequest<ReconcileEffect>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "reconcile_effect",
            "effect_reconciled",
            now_ms,
            false,
            |run| {
                if run.status != RunStatus::RecoveryRequired {
                    return Err(RunError::InvalidTransition(
                        "effect reconciliation requires recovery state".into(),
                    ));
                }
                let operation = run
                    .operations
                    .get_mut(&request.input.operation_id)
                    .ok_or(RunError::NotFound)?;
                if operation.state != OperationState::Uncertain {
                    return Err(RunError::InvalidTransition(
                        "only uncertain effects require reconciliation".into(),
                    ));
                }
                operation.active_attempt = None;
                match &request.input.decision {
                    ReconcileDecision::Applied { receipt } => {
                        validate_effect_receipt(&operation.spec, receipt)?;
                        operation.receipt = Some(receipt.clone());
                        operation.terminal_result_digest =
                            Some(canonical_digest(&request.input.decision)?);
                        operation.state = OperationState::Reconciled;
                    }
                    ReconcileDecision::NotApplied => {
                        operation.state = OperationState::Abandoned;
                    }
                    ReconcileDecision::Unknown { .. } => {
                        operation.state = OperationState::Uncertain;
                    }
                }
                Ok(())
            },
        )
    }

    pub fn admit_child(
        &mut self,
        request: MutationRequest<AdmitChild>,
        now_ms: u64,
    ) -> Result<CommandOutput<ChildRun>, RunError> {
        let child_id = request.input.child_id.clone();
        let command = self.apply_command(
            request.clone(),
            "admit_child",
            "child_admitted",
            now_ms,
            false,
            |run| {
                if run.status != RunStatus::Active {
                    return Err(RunError::InvalidTransition(
                        "child admission requires an active Run".into(),
                    ));
                }
                let iteration = run
                    .active_iteration
                    .as_ref()
                    .ok_or_else(|| RunError::InvalidTransition("no active iteration".into()))?;
                if iteration.iteration_id != request.input.iteration_id {
                    return Err(RunError::StaleCallback);
                }
                if let Some(existing) = run.children.get(&child_id) {
                    if existing.iteration_id == request.input.iteration_id
                        && existing.reservation == request.input.reservation
                        && existing.workspace_isolation == request.input.workspace_isolation
                        && existing.completion_policy == request.input.completion_policy
                    {
                        return Ok(());
                    }
                    return Err(RunError::Integrity(
                        "child id was reused with different admission input".into(),
                    ));
                }
                if request.input.completion_policy == ChildCompletionPolicy::Unknown
                    || request.input.workspace_isolation.trim().is_empty()
                    || request.input.workspace_isolation.len() > 256
                {
                    return Err(RunError::Validation("invalid child policy".into()));
                }
                let reserved = run
                    .child_reserved
                    .add_reservation(&request.input.reservation)
                    .ok_or(RunError::Budget)?;
                let projected = run
                    .usage
                    .with_reservations(&reserved)
                    .ok_or(RunError::Budget)?;
                if !projected.within(&run.budget) {
                    return Err(RunError::Budget);
                }
                run.child_reserved = reserved;
                run.children.insert(
                    child_id.clone(),
                    ChildRun {
                        id: child_id.clone(),
                        state: ChildState::Admitted,
                        iteration_id: request.input.iteration_id,
                        callback_token: DispatchToken::random(),
                        reservation: request.input.reservation.clone(),
                        settlement: None,
                        workspace_isolation: request.input.workspace_isolation.clone(),
                        completion_policy: request.input.completion_policy,
                        artifacts: Vec::new(),
                    },
                );
                Ok(())
            },
        )?;
        let output = command
            .snapshot
            .run
            .children
            .get(&child_id)
            .cloned()
            .ok_or_else(|| RunError::Integrity("child admission lost child state".into()))?;
        Ok(CommandOutput { command, output })
    }

    pub fn child_callback(
        &mut self,
        callback: ChildCallback,
        now_ms: u64,
    ) -> Result<CallbackResult, RunError> {
        self.ensure_authority(&callback.run_id)?;
        let old = self
            .runs
            .get(&callback.run_id)
            .cloned()
            .ok_or(RunError::NotFound)?;
        if old.run.controller_epoch != callback.epoch {
            return Err(RunError::StaleEpoch);
        }
        let child = old
            .run
            .children
            .get(&callback.child_id)
            .ok_or(RunError::NotFound)?;
        if child.iteration_id != callback.iteration_id || child.callback_token != callback.token {
            return Err(RunError::StaleCallback);
        }
        if child.state.is_terminal()
            || (child.state == ChildState::Started && callback.state == ChildState::Started)
        {
            if child.state == callback.state
                && child.settlement == callback.settlement
                && child.artifacts == callback.artifacts
            {
                return Ok(CallbackResult {
                    snapshot: old,
                    duplicate: true,
                });
            }
            return Err(RunError::StaleCallback);
        }
        if callback.state == ChildState::Unknown || callback.state == ChildState::Admitted {
            return Err(RunError::InvalidTransition(
                "invalid child lifecycle transition".into(),
            ));
        }
        for artifact in &callback.artifacts {
            artifact.validate()?;
        }
        let terminal = callback.state.is_terminal();
        if terminal && callback.settlement.is_none() {
            return Err(RunError::InvalidTransition(
                "terminal child callback requires resource settlement".into(),
            ));
        }
        if !terminal && callback.settlement.is_some() {
            return Err(RunError::InvalidTransition(
                "nonterminal child cannot settle resources".into(),
            ));
        }
        let mut next = old.clone();
        let child = next
            .run
            .children
            .get_mut(&callback.child_id)
            .ok_or(RunError::NotFound)?;
        if let Some(settlement) = &callback.settlement {
            if !settlement.within(&child.reservation) {
                return Err(RunError::Budget);
            }
            let remaining = next
                .run
                .child_reserved
                .subtract_reservation(&child.reservation)
                .ok_or_else(|| RunError::Integrity("child reservation underflow".into()))?;
            let usage = next
                .run
                .usage
                .add_usage(settlement)
                .ok_or(RunError::Budget)?;
            let projected = usage
                .with_reservations(&remaining)
                .ok_or(RunError::Budget)?;
            if !projected.within(&next.run.budget) {
                return Err(RunError::Budget);
            }
            next.run.child_reserved = remaining;
            next.run.usage = usage;
            child.settlement = Some(settlement.clone());
        }
        child.state = callback.state;
        child.artifacts = callback.artifacts;
        let snapshot = self.commit_callback(old, next, "child_transitioned", now_ms, None)?;
        Ok(CallbackResult {
            snapshot,
            duplicate: false,
        })
    }

    pub fn accept_message(
        &mut self,
        request: MutationRequest<AcceptMessage>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "accept_message",
            "message_accepted",
            now_ms,
            false,
            |run| {
                if run.status.is_terminal()
                    || request.input.body.trim().is_empty()
                    || request.input.body.len() > MAX_ITEM_BYTES
                    || request.input.sender.trim().is_empty()
                    || request.input.trust_label.trim().is_empty()
                {
                    return Err(RunError::Validation("invalid mailbox message".into()));
                }
                if let Some(existing) = run.mailbox.get(&request.input.message_id) {
                    if existing.body == request.input.body
                        && existing.sender == request.input.sender
                        && existing.causation_id == request.input.causation_id
                        && existing.trust_label == request.input.trust_label
                    {
                        return Ok(());
                    }
                    return Err(RunError::Integrity(
                        "message id was reused with different content".into(),
                    ));
                }
                if run.mailbox.len() >= MAX_MESSAGES {
                    return Err(RunError::DedupCapacity);
                }
                let sequence = run.next_message_sequence;
                run.next_message_sequence = run
                    .next_message_sequence
                    .checked_add(1)
                    .ok_or_else(|| RunError::Integrity("mailbox sequence overflow".into()))?;
                run.mailbox.insert(
                    request.input.message_id.clone(),
                    MailMessage {
                        id: request.input.message_id.clone(),
                        sequence,
                        causation_id: request.input.causation_id.clone(),
                        sender: request.input.sender.clone(),
                        trust_label: request.input.trust_label.clone(),
                        body: request.input.body.clone(),
                        state: MessageState::Accepted,
                    },
                );
                Ok(())
            },
        )
    }

    pub fn transition_message(
        &mut self,
        request: MutationRequest<TransitionMessage>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "transition_message",
            "message_transitioned",
            now_ms,
            false,
            |run| {
                let message = run
                    .mailbox
                    .get_mut(&request.input.message_id)
                    .ok_or(RunError::NotFound)?;
                let valid = matches!(
                    (message.state, request.input.state),
                    (MessageState::Accepted, MessageState::Queued)
                        | (MessageState::Queued, MessageState::DeliveredToContext)
                        | (MessageState::DeliveredToContext, MessageState::Processed)
                );
                if !valid {
                    return Err(RunError::InvalidTransition(
                        "mailbox transition is not monotonic".into(),
                    ));
                }
                message.state = request.input.state;
                Ok(())
            },
        )
    }

    pub fn propose_strategy(
        &mut self,
        request: MutationRequest<ProposeStrategy>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "propose_strategy",
            "strategy_proposed",
            now_ms,
            false,
            |run| {
                ensure_refinement_boundary(run)?;
                validate_revision_input(&request.input.digest, &request.input.provenance)?;
                let revision = run
                    .strategy_revisions
                    .iter()
                    .map(|revision| revision.revision)
                    .max()
                    .unwrap_or(run.current_strategy_revision)
                    .checked_add(1)
                    .ok_or_else(|| RunError::Integrity("strategy revision overflow".into()))?;
                run.strategy_revisions.push(StrategyRevision {
                    revision,
                    digest: request.input.digest.clone(),
                    provenance: request.input.provenance.clone(),
                    applied: false,
                    promotion_proposal: request.input.promotion_proposal.clone(),
                });
                Ok(())
            },
        )
    }

    pub fn apply_strategy(
        &mut self,
        request: MutationRequest<ApplyStrategy>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "apply_strategy",
            "strategy_applied",
            now_ms,
            false,
            |run| {
                ensure_refinement_boundary(run)?;
                let target = run
                    .strategy_revisions
                    .iter()
                    .position(|revision| revision.revision == request.input.revision)
                    .ok_or(RunError::NotFound)?;
                for revision in &mut run.strategy_revisions {
                    revision.applied = false;
                }
                run.strategy_revisions[target].applied = true;
                run.current_strategy_revision = request.input.revision;
                Ok(())
            },
        )
    }

    pub fn propose_workflow(
        &mut self,
        request: MutationRequest<ProposeWorkflow>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "propose_workflow",
            "workflow_proposed",
            now_ms,
            false,
            |run| {
                ensure_refinement_boundary(run)?;
                validate_revision_input(&request.input.source_digest, &request.input.provenance)?;
                let revision = run
                    .workflow_revisions
                    .iter()
                    .map(|revision| revision.revision)
                    .max()
                    .or(run.current_workflow_revision)
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| RunError::Integrity("workflow revision overflow".into()))?;
                run.workflow_revisions.push(WorkflowRevision {
                    revision,
                    source_digest: request.input.source_digest.clone(),
                    provenance: request.input.provenance.clone(),
                    state: WorkflowRevisionState::Proposal,
                    compiled: false,
                    static_policy_valid: false,
                    dry_run_valid: false,
                    promotion_proposal: request.input.promotion_proposal.clone(),
                });
                Ok(())
            },
        )
    }

    pub fn validate_workflow(
        &mut self,
        request: MutationRequest<ValidateWorkflow>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.apply_command(
            request.clone(),
            "validate_workflow",
            "workflow_validated",
            now_ms,
            false,
            |run| {
                ensure_refinement_boundary(run)?;
                let revision = run
                    .workflow_revisions
                    .iter_mut()
                    .find(|revision| revision.revision == request.input.revision)
                    .ok_or(RunError::NotFound)?;
                if revision.state != WorkflowRevisionState::Proposal {
                    return Err(RunError::InvalidTransition(
                        "only a workflow proposal may be validated".into(),
                    ));
                }
                revision.compiled = request.input.compiled;
                revision.static_policy_valid = request.input.static_policy_valid;
                revision.dry_run_valid = request.input.dry_run_valid;
                revision.state = if revision.compiled
                    && revision.static_policy_valid
                    && revision.dry_run_valid
                {
                    WorkflowRevisionState::Validated
                } else {
                    WorkflowRevisionState::Rejected
                };
                Ok(())
            },
        )
    }

    pub fn apply_workflow(
        &mut self,
        request: MutationRequest<SetWorkflowRevision>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.set_workflow_revision(request, now_ms, false)
    }

    pub fn rollback_workflow(
        &mut self,
        request: MutationRequest<SetWorkflowRevision>,
        now_ms: u64,
    ) -> Result<RunCommandResult, RunError> {
        self.set_workflow_revision(request, now_ms, true)
    }

    fn set_workflow_revision(
        &mut self,
        request: MutationRequest<SetWorkflowRevision>,
        now_ms: u64,
        rollback: bool,
    ) -> Result<RunCommandResult, RunError> {
        let operation_name = if rollback {
            "rollback_workflow"
        } else {
            "apply_workflow"
        };
        let event_kind = if rollback {
            "workflow_rolled_back"
        } else {
            "workflow_applied"
        };
        self.apply_command(
            request.clone(),
            operation_name,
            event_kind,
            now_ms,
            false,
            |run| {
                ensure_refinement_boundary(run)?;
                if run.operations.values().any(|operation| {
                    matches!(operation.spec, EffectSpec::RhaiWorkflow { .. })
                        && !matches!(
                            operation.state,
                            OperationState::Acknowledged | OperationState::Reconciled
                        )
                }) {
                    return Err(RunError::InvalidTransition(
                        "active workflow revision is immutable".into(),
                    ));
                }
                let target = run
                    .workflow_revisions
                    .iter()
                    .position(|revision| revision.revision == request.input.revision)
                    .ok_or(RunError::NotFound)?;
                let target_state = run.workflow_revisions[target].state;
                let eligible = if rollback {
                    matches!(
                        target_state,
                        WorkflowRevisionState::Validated
                            | WorkflowRevisionState::Applied
                            | WorkflowRevisionState::RolledBack
                    )
                } else {
                    target_state == WorkflowRevisionState::Validated
                };
                if !eligible
                    || !run.workflow_revisions[target].compiled
                    || !run.workflow_revisions[target].static_policy_valid
                    || !run.workflow_revisions[target].dry_run_valid
                {
                    return Err(RunError::InvalidTransition(
                        "workflow revision has not passed compile, policy, and dry-run gates"
                            .into(),
                    ));
                }
                for revision in &mut run.workflow_revisions {
                    if revision.state == WorkflowRevisionState::Applied {
                        revision.state = WorkflowRevisionState::RolledBack;
                    }
                }
                run.workflow_revisions[target].state = WorkflowRevisionState::Applied;
                run.current_workflow_revision = Some(request.input.revision);
                Ok(())
            },
        )
    }

    fn apply_command<T, F>(
        &mut self,
        request: MutationRequest<T>,
        operation_name: &str,
        event_kind: &str,
        now_ms: u64,
        allow_fenced: bool,
        reduce: F,
    ) -> Result<RunCommandResult, RunError>
    where
        T: Serialize,
        F: FnOnce(&mut RunRecord) -> Result<(), RunError>,
    {
        let input_digest = canonical_digest(&(operation_name, &request.input))?;
        let old = self
            .runs
            .get(&request.run_id)
            .cloned()
            .ok_or(RunError::NotFound)?;
        if !allow_fenced {
            self.ensure_authority(&request.run_id)?;
        }
        if let Some(receipt) = old.run.command_receipts.get(&request.command_id) {
            if receipt.input_digest != input_digest {
                return Err(RunError::Integrity(
                    "command id was reused with different input".into(),
                ));
            }
            return Ok(RunCommandResult {
                receipt: receipt.clone(),
                snapshot: old,
                duplicate: true,
            });
        }
        if old.run.status == RunStatus::Tombstoned {
            return Err(RunError::InvalidTransition(
                "tombstoned Run rejects every mutation".into(),
            ));
        }
        if old.run.revision != request.expected_revision {
            return Err(RunError::Conflict {
                expected: Some(request.expected_revision),
                actual: Some(old.run.revision),
            });
        }
        if old.run.command_receipts.len() >= MAX_COMMAND_RECEIPTS {
            return Err(RunError::DedupCapacity);
        }
        let mut next = old.clone();
        reduce(&mut next.run)?;
        let revision = successor(old.run.revision, "Run revision")?;
        let cursor = successor_cursor(old.run.event_cursor)?;
        next.run.revision = revision;
        next.run.updated_at_ms = now_ms;
        next.run.event_cursor = cursor;
        let receipt = CommandReceipt {
            command_id: request.command_id.clone(),
            input_digest,
            disposition: CommandDisposition::Applied,
            committed_revision: revision,
            epoch: next.run.controller_epoch,
        };
        next.run
            .command_receipts
            .insert(request.command_id, receipt.clone());
        next.events.push_back(RunEvent {
            cursor,
            revision,
            kind: internal_event_kind(event_kind),
            at_ms: now_ms,
        });
        trim_events(&mut next.events);
        let snapshot = self.commit(Some(old.run.revision), next, None)?;
        Ok(RunCommandResult {
            receipt,
            snapshot,
            duplicate: false,
        })
    }

    fn commit_callback(
        &mut self,
        old: RunEnvelope,
        mut next: RunEnvelope,
        event_kind: &str,
        now_ms: u64,
        finished_iteration: Option<IterationManifest>,
    ) -> Result<RunEnvelope, RunError> {
        let revision = successor(old.run.revision, "Run revision")?;
        let cursor = successor_cursor(old.run.event_cursor)?;
        next.run.revision = revision;
        next.run.updated_at_ms = now_ms;
        next.run.event_cursor = cursor;
        next.events.push_back(RunEvent {
            cursor,
            revision,
            kind: internal_event_kind(event_kind),
            at_ms: now_ms,
        });
        trim_events(&mut next.events);
        self.commit(Some(old.run.revision), next, finished_iteration)
    }

    fn commit(
        &mut self,
        expected_revision: Option<RunRevision>,
        next: RunEnvelope,
        finished_iteration: Option<IterationManifest>,
    ) -> Result<RunEnvelope, RunError> {
        let run_id = next.run.id.clone();
        match self.store.commit(StoreCommit {
            run_id: run_id.clone(),
            expected_revision,
            next: next.clone(),
            finished_iteration,
        })? {
            StoreCommitResult::Applied => {
                self.runs.insert(run_id, next.clone());
                Ok(next)
            }
            StoreCommitResult::Conflict { actual } => {
                self.authority_fenced.insert(run_id.clone());
                self.reload_required.insert(run_id);
                Err(RunError::Conflict {
                    expected: expected_revision,
                    actual,
                })
            }
            StoreCommitResult::Tombstoned => {
                self.authority_fenced.insert(run_id);
                Err(RunError::InvalidTransition(
                    "durable tombstone rejected mutation".into(),
                ))
            }
            StoreCommitResult::CommitUnknown(message) => {
                self.authority_fenced.insert(run_id.clone());
                self.reload_required.insert(run_id);
                Err(RunError::CommitUnknown(message))
            }
        }
    }

    fn ensure_authority(&self, run_id: &RunId) -> Result<(), RunError> {
        if self.authority_fenced.contains(run_id) {
            Err(RunError::AuthorityLost)
        } else {
            Ok(())
        }
    }
}

fn apply_control(run: &mut RunRecord, action: &RunAction, _now_ms: u64) -> Result<(), RunError> {
    match action {
        RunAction::Pause => {
            if run.status != RunStatus::Active {
                return Err(RunError::InvalidTransition("Run is not active".into()));
            }
            run.status = RunStatus::UserPaused;
        }
        RunAction::PauseFor { reason } => {
            if run.status != RunStatus::Active || run.active_iteration.is_some() {
                return Err(RunError::InvalidTransition(
                    "Run can wait only at an active iteration boundary".into(),
                ));
            }
            run.status = match reason {
                WaitingReason::User => RunStatus::UserPaused,
                WaitingReason::Backoff => RunStatus::BackOffPaused,
                WaitingReason::NoProgress => RunStatus::NoProgressPaused,
                WaitingReason::Infrastructure => RunStatus::InfraPaused,
                WaitingReason::Blocked => RunStatus::Blocked,
                WaitingReason::BudgetExhausted => RunStatus::BudgetLimited,
                WaitingReason::Approval | WaitingReason::Unknown => {
                    return Err(RunError::InvalidTransition(
                        "approval and unknown waits require their dedicated transition".into(),
                    ));
                }
            };
        }
        RunAction::Resume { budget } => {
            if !matches!(
                run.status,
                RunStatus::UserPaused
                    | RunStatus::BackOffPaused
                    | RunStatus::NoProgressPaused
                    | RunStatus::InfraPaused
                    | RunStatus::Blocked
                    | RunStatus::BudgetLimited
            ) {
                return Err(RunError::InvalidTransition(
                    "Run must finish recovery or be paused before resume".into(),
                ));
            }
            if let Some(budget) = budget {
                let committed = run
                    .usage
                    .with_reservations(&run.child_reserved)
                    .ok_or(RunError::Budget)?;
                if !run.budget.within(budget) || !committed.within(budget) {
                    return Err(RunError::Budget);
                }
                run.budget = budget.clone();
            }
            let committed = run
                .usage
                .with_reservations(&run.child_reserved)
                .ok_or(RunError::Budget)?;
            if !committed.within(&run.budget) {
                return Err(RunError::Budget);
            }
            run.status = RunStatus::Active;
        }
        RunAction::Steer { message_id, body } => {
            if run.status.is_terminal() || body.trim().is_empty() || body.len() > MAX_ITEM_BYTES {
                return Err(RunError::InvalidTransition(
                    "terminal Run or invalid steering message".into(),
                ));
            }
            if let Some(existing) = run.steering.get(message_id) {
                if existing.body == *body {
                    return Ok(());
                }
                return Err(RunError::Integrity(
                    "steering message id was reused with different body".into(),
                ));
            }
            if run.steering.len() >= MAX_MESSAGES {
                return Err(RunError::DedupCapacity);
            }
            run.steering_high_water = run
                .steering_high_water
                .checked_add(1)
                .ok_or_else(|| RunError::Integrity("steering sequence overflow".into()))?;
            run.steering.insert(
                message_id.clone(),
                MailMessage {
                    id: message_id.clone(),
                    sequence: run.steering_high_water,
                    causation_id: None,
                    sender: "user".into(),
                    trust_label: "trusted".into(),
                    body: body.clone(),
                    state: MessageState::Accepted,
                },
            );
        }
        RunAction::Cancel => {
            if run.status.is_terminal() {
                return Err(RunError::InvalidTransition(
                    "Run is already terminal".into(),
                ));
            }
            run.status = RunStatus::Cancelled;
            run.stage = RunStage::Idle;
        }
        RunAction::Approve => {
            if !run.pending_approval || run.stage != RunStage::AwaitingApproval {
                return Err(RunError::InvalidTransition(
                    "Run is not awaiting approval".into(),
                ));
            }
            run.pending_approval = false;
            run.stage = RunStage::Idle;
        }
        RunAction::Reject => {
            if !run.pending_approval {
                return Err(RunError::InvalidTransition(
                    "Run is not awaiting approval".into(),
                ));
            }
            run.pending_approval = false;
            run.status = RunStatus::UserPaused;
            run.stage = RunStage::Idle;
        }
        RunAction::TryComplete => {
            validate_completion(run)?;
            run.status = RunStatus::Complete;
            run.stage = RunStage::Idle;
        }
        RunAction::ClaimTerminalReport => {
            if !run.status.is_terminal() || run.terminal_report_claimed || has_unsettled_work(run) {
                return Err(RunError::InvalidTransition(
                    "terminal report is unavailable or already claimed".into(),
                ));
            }
            run.terminal_report_claimed = true;
        }
        RunAction::Tombstone => {
            if !run.status.is_terminal() || has_unsettled_work(run) {
                return Err(RunError::InvalidTransition(
                    "only a settled terminal Run may be tombstoned".into(),
                ));
            }
            run.status = RunStatus::Tombstoned;
            run.stage = RunStage::Idle;
        }
    }
    Ok(())
}

fn validate_completion(run: &RunRecord) -> Result<(), RunError> {
    if run.status != RunStatus::Active || run.active_iteration.is_some() || run.pending_approval {
        return Err(RunError::InvalidTransition(
            "completion requires an active Run at an iteration boundary".into(),
        ));
    }
    let iteration = run.iterations.back().ok_or_else(|| {
        RunError::InvalidTransition("completion requires a finished iteration".into())
    })?;
    if !iteration.driver_terminal_success || iteration.verdict != Some(GoalVerdict::Achieved) {
        return Err(RunError::InvalidTransition(
            "driver and skeptic verifier have not accepted the goal".into(),
        ));
    }
    if !run
        .required_gates
        .iter()
        .all(|gate| iteration.gates.get(gate) == Some(&true))
    {
        return Err(RunError::InvalidTransition(
            "required deterministic gates have not passed".into(),
        ));
    }
    if !run.goal.required_evidence.iter().all(|required| {
        iteration.evidence.iter().any(|artifact| {
            artifact.evidence_labels.contains(required)
                && artifact.workspace_digest.as_deref()
                    == Some(iteration.context.workspace_revision.as_str())
        })
    }) {
        return Err(RunError::InvalidTransition(
            "required workspace-bound evidence is missing".into(),
        ));
    }
    if has_unsettled_operations(run)
        || run
            .children
            .values()
            .any(|child| match child.completion_policy {
                ChildCompletionPolicy::MustSucceed => child.state != ChildState::Completed,
                ChildCompletionPolicy::MayFail => !child.state.is_terminal(),
                ChildCompletionPolicy::Detached => false,
                ChildCompletionPolicy::Unknown => true,
            })
        || !run.usage.within(&run.budget)
    {
        return Err(RunError::InvalidTransition(
            "effects, child Runs, or budgets are not settled".into(),
        ));
    }
    Ok(())
}

fn has_unsettled_work(run: &RunRecord) -> bool {
    has_unsettled_operations(run)
        || run
            .children
            .values()
            .any(|child| !child.state.is_terminal())
}

fn enter_recovery(run: &mut RunRecord) {
    if !matches!(run.status, RunStatus::Active | RunStatus::RecoveryRequired) {
        run.recovery_prior_status.get_or_insert(run.status);
    }
    run.status = RunStatus::RecoveryRequired;
    run.stage = RunStage::Recovering;
}

fn has_unsettled_operations(run: &RunRecord) -> bool {
    run.operations.values().any(|operation| {
        !matches!(
            operation.state,
            OperationState::Acknowledged | OperationState::Reconciled | OperationState::Abandoned
        )
    })
}

fn needs_controller_recovery(run: &RunRecord) -> bool {
    run.status == RunStatus::RecoveryRequired
        || !run.status.is_terminal()
        || run.active_iteration.is_some()
        || has_unsettled_work(run)
}

fn validate_effect_capability(run: &RunRecord, spec: &EffectSpec) -> Result<(), RunError> {
    let capability = match spec {
        EffectSpec::SessionTurn { session, input, .. } => {
            if session != &run.session {
                return Err(RunError::Integrity(
                    "Session turn effect references a different Session".into(),
                ));
            }
            input.validate()?;
            "session.turn".to_owned()
        }
        EffectSpec::RhaiWorkflow {
            session,
            workflow,
            args,
        } => {
            if session != &run.session {
                return Err(RunError::Integrity(
                    "workflow effect references a different Session".into(),
                ));
            }
            workflow.validate()?;
            args.validate()?;
            "workflow.execute".to_owned()
        }
        EffectSpec::ChildAgent { request, .. } => {
            request.validate()?;
            "agent.spawn".to_owned()
        }
        EffectSpec::Gate { input, .. } => {
            input.validate()?;
            "gate.execute".to_owned()
        }
        EffectSpec::ArtifactMutation { mutation } => {
            mutation.validate()?;
            "artifact.write".to_owned()
        }
        EffectSpec::External {
            provider, payload, ..
        } => {
            payload.validate()?;
            format!("external.{provider}")
        }
        EffectSpec::Unknown => {
            return Err(RunError::Capability(
                "unknown effect type is never executable".into(),
            ));
        }
    };
    if run.capabilities.available.contains(&capability)
        && run.capabilities.ceiling.contains(&capability)
    {
        Ok(())
    } else {
        Err(RunError::Capability(format!(
            "effect requires capability {capability}"
        )))
    }
}

fn ensure_refinement_boundary(run: &RunRecord) -> Result<(), RunError> {
    if run.status != RunStatus::Active || run.active_iteration.is_some() {
        Err(RunError::InvalidTransition(
            "refinement is only applied between bounded iterations".into(),
        ))
    } else {
        Ok(())
    }
}

fn validate_revision_input(digest: &str, provenance: &str) -> Result<(), RunError> {
    if !valid_sha256(digest) || provenance.trim().is_empty() || provenance.len() > 512 {
        Err(RunError::Validation(
            "invalid refinement digest or provenance".into(),
        ))
    } else {
        Ok(())
    }
}

fn recovery_needs(run: &RunRecord) -> Vec<RecoveryNeed> {
    let mut needs = Vec::new();
    for operation in run.operations.values() {
        if operation.state != OperationState::Uncertain {
            continue;
        }
        match &operation.spec {
            EffectSpec::SessionTurn {
                session,
                turn_id,
                prompt_digest,
                ..
            } => needs.push(RecoveryNeed::SessionTurnLedger {
                operation_id: operation.id.clone(),
                session: session.clone(),
                turn_id: turn_id.clone(),
                prompt_digest: prompt_digest.clone(),
            }),
            _ => needs.push(RecoveryNeed::EffectReconciliation {
                operation_id: operation.id.clone(),
                effect_class: operation.effect_class,
            }),
        }
    }
    if let Some(iteration) = &run.active_iteration {
        needs.push(RecoveryNeed::ActiveIteration {
            iteration_id: iteration.iteration_id,
        });
    }
    needs.extend(
        run.children
            .values()
            .filter(|child| !child.state.is_terminal())
            .map(|child| RecoveryNeed::ActiveChild {
                child_id: child.id.clone(),
            }),
    );
    needs
}

fn push_iteration(run: &mut RunRecord, iteration: IterationManifest) {
    run.iterations.push_back(iteration);
    while run.iterations.len() > MAX_ITERATION_SUMMARIES {
        run.iterations.pop_front();
    }
}

fn trim_events(events: &mut VecDeque<RunEvent>) {
    while events.len() > MAX_EVENTS {
        events.pop_front();
    }
}

fn successor(value: RunRevision, description: &str) -> Result<RunRevision, RunError> {
    Ok(RunRevision::new(value.get().checked_add(1).ok_or_else(
        || RunError::Integrity(format!("{description} overflow")),
    )?))
}

fn successor_cursor(value: RunEventCursor) -> Result<RunEventCursor, RunError> {
    Ok(RunEventCursor::new(value.get().checked_add(1).ok_or_else(
        || RunError::Integrity("Run event cursor overflow".into()),
    )?))
}

fn internal_event_kind(value: &str) -> RunEventKind {
    RunEventKind::new(value).expect("internal event names are valid")
}
