// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use crate::*;

impl Runtime {
    /// Creates a durable Run. GoalSpec defines the desired outcome; the Run is
    /// the sole lifecycle authority that executes a selected bounded driver.
    pub async fn create_run(
        &self,
        request: run::CreateRunRequest,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.create_run(request).await
    }
    pub async fn get_run(&self, run_id: &run::RunId) -> Result<Option<run::RunEnvelope>, Error> {
        self.inner.get_run(run_id).await
    }
    pub async fn list_runs(&self) -> Result<Vec<run::RunEnvelope>, Error> {
        self.inner.list_runs().await
    }
    pub async fn list_recoverable_runs(&self) -> Result<Vec<run::RunEnvelope>, Error> {
        self.inner.list_recoverable_runs().await
    }
    pub async fn inspect_run_residency(
        &self,
        run_id: &run::RunId,
    ) -> Result<run::ResidencyInspection, Error> {
        self.inner.inspect_run_residency(run_id).await
    }
    pub async fn request_run_wake(
        &self,
        request: run::MutationRequest<run::WakeRequest>,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.request_run_wake(request).await
    }
    pub async fn claim_run_activation(
        &self,
        request: run::MutationRequest<run::ClaimActivation>,
    ) -> Result<run::CommandOutput<run::ActivationLease>, Error> {
        self.inner.claim_run_activation(request).await
    }
    pub async fn renew_run_activation(
        &self,
        request: run::MutationRequest<(run::ActivationFence, u64)>,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.renew_run_activation(request).await
    }
    pub async fn release_run_activation(
        &self,
        request: run::MutationRequest<run::ActivationFence>,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.release_run_activation(request).await
    }
    pub async fn control_run(
        &self,
        request: run::MutationRequest<run::RunAction>,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.control_run(request).await
    }
    pub async fn wake_run(
        &self,
        request: run::MutationRequest<run::RunAction>,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.wake_run(request).await
    }
    /// Attaches to the independent Run journal. A pruned or invalid cursor
    /// returns a full Snapshot fallback instead of a lossy gap error.
    pub async fn attach_run(
        &self,
        run_id: &run::RunId,
        after: run::RunEventCursor,
    ) -> Result<run::RunAttach, Error> {
        self.inner.attach_run(run_id, after).await
    }
    /// Admits a child to the authoritative parent Run and reserves its budget.
    pub async fn admit_run_child(
        &self,
        request: run::MutationRequest<run::AdmitChild>,
    ) -> Result<run::CommandOutput<run::ChildRun>, Error> {
        self.inner.admit_child(request).await
    }
    /// Applies a token/epoch-fenced child settlement callback.
    pub async fn settle_run_child(
        &self,
        callback: run::ChildCallback,
    ) -> Result<run::CallbackResult, Error> {
        self.inner.child_callback(callback).await
    }
    /// Idempotently accepts one message into the durable Run mailbox.
    pub async fn accept_run_message(
        &self,
        request: run::MutationRequest<run::AcceptMessage>,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.accept_run_message(request).await
    }
    /// Advances one mailbox message through its monotonic delivery states.
    pub async fn transition_run_message(
        &self,
        request: run::MutationRequest<run::TransitionMessage>,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.transition_run_message(request).await
    }
    /// Fences the previous controller epoch, enters Recovering, then resolves
    /// Session-turn operations from the existing SessionLedger and rewind
    /// receipts. Non-session effects and active child/iteration state remain
    /// explicit needs for the host; this method never guesses an outcome.
    pub async fn reconcile_run(
        &self,
        request: run::MutationRequest<()>,
    ) -> Result<run::RecoveryPlan, Error> {
        let parent_command = request.command_id.clone();
        let run_id = request.run_id.clone();
        let mut plan = self.inner.begin_run_recovery(request).await?;
        let needs = plan.needs.clone();
        for need in needs {
            let run::RecoveryNeed::SessionTurnLedger {
                operation_id,
                session,
                turn_id,
                prompt_digest,
            } = need
            else {
                continue;
            };
            let session_id = SessionId::from_stored(session.as_str());
            let ledger = self.session_ledger(&session_id).await?;
            let conflicting_identity = ledger
                .entries
                .iter()
                .any(|entry| entry.turn_id == turn_id && entry.prompt_digest != prompt_digest);
            let matching: Vec<_> = ledger
                .entries
                .iter()
                .filter(|entry| entry.turn_id == turn_id && entry.prompt_digest == prompt_digest)
                .collect();
            let decision = match matching.as_slice() {
                [entry] => match &entry.state {
                    LedgerTurnState::Completed {
                        outcome,
                        settlement_id,
                        usage,
                    } => match usage {
                        Some(session_usage) => {
                            let actual_usage = recovered_session_turn_usage(session_usage.clone())?;
                            let receipt = run::EffectReceipt::for_session_turn(
                                &session,
                                &turn_id,
                                &prompt_digest,
                                entry.runtime_prompt_index,
                                durable_turn_outcome(*outcome),
                                session_usage.clone(),
                                actual_usage,
                            );
                            if receipt.settlement_id.as_deref() == Some(settlement_id.as_str()) {
                                run::ReconcileDecision::Applied { receipt }
                            } else {
                                run::ReconcileDecision::Unknown {
                                    message: "SessionLedger settlement identity failed validation"
                                        .into(),
                                }
                            }
                        }
                        _ => run::ReconcileDecision::Unknown {
                            message: "SessionLedger completion lacks typed usage evidence".into(),
                        },
                    },
                    LedgerTurnState::Pending | LedgerTurnState::Discarded => {
                        match self.rewind_status(&session_id, operation_id.as_str()).await {
                            Ok(ConversationRewindStatus::Applied { receipt })
                                if rewind_receipt_proves_turn_not_applied(
                                    &receipt,
                                    entry,
                                    &turn_id,
                                    &prompt_digest,
                                ) =>
                            {
                                run::ReconcileDecision::NotApplied
                            }
                            Ok(ConversationRewindStatus::Pending { .. }) => {
                                run::ReconcileDecision::Unknown {
                                    message: "Session turn rewind remains pending".into(),
                                }
                            }
                            Ok(ConversationRewindStatus::Absent)
                            | Ok(ConversationRewindStatus::Applied { .. }) => {
                                run::ReconcileDecision::Unknown {
                                    message: "Session turn lacks an exact applied rewind receipt"
                                        .into(),
                                }
                            }
                            Err(error) => run::ReconcileDecision::Unknown {
                                message: format!("rewind evidence unavailable: {error}"),
                            },
                        }
                    }
                },
                [] if conflicting_identity => run::ReconcileDecision::Unknown {
                    message: "SessionLedger reused the turn id with a different prompt digest"
                        .into(),
                },
                // Runtime::prompt durably writes the Pending ledger entry before
                // native dispatch. Absence of both the exact tuple and a
                // conflicting turn id therefore proves this committed Run intent
                // was never dispatched.
                [] => run::ReconcileDecision::NotApplied,
                _ => run::ReconcileDecision::Unknown {
                    message: "SessionLedger contains conflicting turn evidence".into(),
                },
            };
            let command_id = run_reconcile_command_id(&parent_command, &operation_id)?;
            let result = self
                .inner
                .reconcile_effect(run::MutationRequest::new(
                    run_id.clone(),
                    plan.snapshot.run.revision,
                    command_id,
                    xai_agent_lifecycle::run::ReconcileEffect::new(operation_id, decision),
                ))
                .await?;
            plan.snapshot = result.snapshot;
        }
        self.inner.run_recovery_plan(&run_id).await
    }
    /// Resolves the remaining high-level recovery needs after
    /// [`Self::reconcile_run`]. Applied work requires observed usage; the SDK
    /// never invents zero-cost usage. A persisted paused/waiting/terminal state
    /// is restored even when `resume` is requested, so only a later explicit
    /// Resume command can reactivate it.
    pub async fn resolve_run_recovery(
        &self,
        request: run::MutationRequest<run::RecoveryResolution>,
    ) -> Result<run::RunCommandResult, Error> {
        self.inner.finish_run_recovery(request).await
    }
}
