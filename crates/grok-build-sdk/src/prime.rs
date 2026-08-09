//! Host-agnostic contracts for programmable Prime Agent execution.
//!
//! Implementations must persist intent in the durable Run before invoking any
//! method that can produce an external effect. These traits describe host
//! execution; they do not introduce another Run or Turn authority.

use crate::run::{ArtifactRef, OperationId, ResourceVector, RunError, RunId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const PRIME_FACADE_VERSION: u32 = 2;
pub const MAX_PROGRAM_ACTIONS: u32 = 1024;

/// Negotiated immutable harness requirements. The descriptor is content, not
/// activation state; Hosts own revision CAS, activation and rollback history.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessDescriptor {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub required_capabilities: BTreeSet<String>,
    #[serde(default)]
    pub optional_capabilities: BTreeSet<String>,
}

impl HarnessDescriptor {
    pub fn validate(&self) -> Result<(), RunError> {
        if self.name.trim().is_empty()
            || self.name.len() > 256
            || self.version.trim().is_empty()
            || self.version.len() > 128
            || self.required_capabilities.len() > 128
            || self.optional_capabilities.len() > 128
            || self
                .required_capabilities
                .iter()
                .chain(&self.optional_capabilities)
                .any(|value| value.is_empty() || value.len() > 256)
        {
            return Err(RunError::Validation("invalid Harness descriptor".into()));
        }
        Ok(())
    }
}

/// Opaque credential reference. Secret material never enters durable state.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialRef {
    pub provider: String,
    /// Stable, non-secret identity safe to bind into durable effect intent.
    pub key_id: String,
    /// Short-lived executable handle passed only to the Host driver.
    opaque_handle: String,
    pub scope: String,
    pub generation: u64,
    pub expires_at_ms: u64,
}

impl CredentialRef {
    pub fn new(
        provider: impl Into<String>,
        key_id: impl Into<String>,
        opaque_handle: impl Into<String>,
        scope: impl Into<String>,
        generation: u64,
        expires_at_ms: u64,
    ) -> Self {
        Self {
            provider: provider.into(),
            key_id: key_id.into(),
            opaque_handle: opaque_handle.into(),
            scope: scope.into(),
            generation,
            expires_at_ms,
        }
    }
    pub fn handle_identity(&self) -> &str {
        &self.opaque_handle
    }
    pub fn validate_at(&self, now_ms: u64) -> Result<(), RunError> {
        if self.provider.trim().is_empty()
            || self.key_id.trim().is_empty()
            || self.opaque_handle.trim().is_empty()
            || self.scope.trim().is_empty()
            || self.generation == 0
            || self.expires_at_ms <= now_ms
        {
            Err(RunError::Capability(
                "credential handle is invalid or expired".into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderCapability {
    pub provider: String,
    pub revision: String,
    pub max_dispatch: ResourceVector,
    pub credential: CredentialRef,
}

impl ProviderCapability {
    /// Fail closed unless every finite Run dimension has a finite, non-zero
    /// per-dispatch maximum. This makes pre-dispatch reservation enforceable.
    pub fn validate_dispatch_bound(&self, run_budget: &ResourceVector) -> Result<(), RunError> {
        let limits = [
            &self.max_dispatch.iterations,
            &self.max_dispatch.agent_calls,
            &self.max_dispatch.agent_concurrency,
            &self.max_dispatch.active_ms,
            &self.max_dispatch.wall_ms,
            &self.max_dispatch.tokens,
            &self.max_dispatch.cost_micros,
            &self.max_dispatch.artifact_bytes,
        ];
        let budgets = [
            &run_budget.iterations,
            &run_budget.agent_calls,
            &run_budget.agent_concurrency,
            &run_budget.active_ms,
            &run_budget.wall_ms,
            &run_budget.tokens,
            &run_budget.cost_micros,
            &run_budget.artifact_bytes,
        ];
        if self.provider.trim().is_empty()
            || self.revision.trim().is_empty()
            || limits
                .iter()
                .zip(budgets)
                .any(|(limit, budget)| *budget != u64::MAX && **limit == 0)
        {
            return Err(RunError::Capability(
                "provider lacks an enforceable finite dispatch bound".into(),
            ));
        }
        Ok(())
    }
}

/// Content-addressed inputs incorporated into one execution. Context, skills,
/// and compaction summaries are artifacts and therefore restart-verifiable.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramContext {
    pub manifest: ArtifactRef,
    pub provenance: String,
    pub selected_budget: u64,
    #[serde(default)]
    pub skills: Vec<crate::run::SkillDescriptorPin>,
    pub compaction: Option<crate::run::CompactionCheckpoint>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramIntent {
    pub run_id: RunId,
    pub operation_id: OperationId,
    pub program: ArtifactRef,
    pub context: ProgramContext,
    pub action_limit: u32,
    pub reservation: ResourceVector,
}

/// Public durable dispatch request. Command identities are supplied by the
/// host so retries remain idempotent across process restarts.
pub struct ProgramExecutionRequest {
    pub expected_revision: crate::run::RunRevision,
    pub iteration_id: crate::run::IterationId,
    pub prepare_command: crate::run::CommandId,
    pub claim_command: crate::run::CommandId,
    pub intent: ProgramIntent,
    pub capability: ProviderCapability,
    pub now_ms: u64,
    pub activation: Option<crate::run::ActivationFence>,
}

pub struct ProgramReconcileRequest {
    pub run_id: RunId,
    pub operation_id: OperationId,
    pub expected_revision: crate::run::RunRevision,
    pub command_id: crate::run::CommandId,
}

impl ProgramIntent {
    pub fn validate(&self) -> Result<(), RunError> {
        if self.action_limit == 0
            || self.action_limit > MAX_PROGRAM_ACTIONS
            || self.context.skills.len() > 128
            || self.context.provenance.trim().is_empty()
            || self.context.selected_budget == 0
        {
            return Err(RunError::Validation(
                "invalid bounded program intent".into(),
            ));
        }
        self.context.manifest.validate()?;
        self.program.validate()?;
        for skill in &self.context.skills {
            skill.validate()?;
        }
        if let Some(compaction) = &self.context.compaction {
            compaction.validate()?;
        }
        Ok(())
    }
}

pub fn verify_program_artifacts(
    store: &dyn crate::run::ArtifactStore,
    intent: &ProgramIntent,
) -> Result<(), RunError> {
    let refs = std::iter::once(&intent.program)
        .chain(std::iter::once(&intent.context.manifest))
        .chain(intent.context.skills.iter().map(|skill| &skill.descriptor))
        .chain(intent.context.compaction.iter().flat_map(|compaction| {
            std::iter::once(&compaction.artifact).chain(compaction.evidence.iter())
        }));
    for artifact in refs {
        verify_artifact_ref(store, artifact)?;
    }
    Ok(())
}

fn verify_artifact_ref(
    store: &dyn crate::run::ArtifactStore,
    artifact: &ArtifactRef,
) -> Result<(), RunError> {
    artifact.validate()?;
    let bytes = store.get(&artifact.digest, artifact.size)?;
    if bytes.len() as u64 != artifact.size {
        return Err(RunError::Integrity(
            "artifact size does not match durable ref".into(),
        ));
    }
    use sha2::Digest as _;
    if format!("{:x}", sha2::Sha256::digest(&bytes)) != artifact.digest {
        return Err(RunError::Integrity(
            "artifact digest does not match durable ref".into(),
        ));
    }
    Ok(())
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramHandle {
    pub id: String,
    pub generation: u64,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramReceipt {
    pub handle: ProgramHandle,
    pub operation_id: OperationId,
    pub result: ArtifactRef,
    pub checkpoint: Option<ArtifactRef>,
    pub usage: ResourceVector,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[non_exhaustive]
// Keep recovery receipts inline in this pre-1.0 public contract. Boxing only
// the completed case would impose API indirection without changing its wire
// representation or durable size bound.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgramRecovery {
    NotExecuted,
    Completed(ProgramReceipt),
    Uncertain,
}

#[async_trait::async_trait]
pub trait ProgramRuntime: Send + Sync + 'static {
    async fn execute(
        &self,
        intent: ProgramIntent,
        credential: &CredentialRef,
    ) -> Result<ProgramReceipt, RunError>;
    async fn reconcile(&self, operation: &OperationId) -> Result<ProgramRecovery, RunError>;
    async fn cancel(&self, handle: &ProgramHandle) -> Result<(), RunError>;
}

/// Optional persistent-kernel specialization. A checkpoint is evidence, not
/// authority: recovery must still reconcile the durable operation identity.
#[async_trait::async_trait]
pub trait PersistentKernelDriver: ProgramRuntime {
    async fn restore(&self, checkpoint: &ArtifactRef) -> Result<ProgramHandle, RunError>;
    async fn checkpoint(&self, handle: &ProgramHandle) -> Result<ArtifactRef, RunError>;
}

impl crate::Runtime {
    pub async fn inspect_harness(
        &self,
        run_id: &RunId,
    ) -> Result<crate::run::HarnessGovernance, crate::Error> {
        self.get_run(run_id)
            .await?
            .map(|e| e.run.harness)
            .ok_or(crate::run::RunError::NotFound.into())
    }
    pub async fn propose_harness(
        &self,
        request: crate::run::MutationRequest<crate::run::ProposeHarness>,
    ) -> Result<crate::run::RunCommandResult, crate::Error> {
        self.inner.propose_harness(request).await
    }
    pub async fn validate_harness(
        &self,
        request: crate::run::MutationRequest<crate::run::ValidateHarness>,
    ) -> Result<crate::run::RunCommandResult, crate::Error> {
        self.inner.validate_harness(request).await
    }
    pub async fn activate_harness(
        &self,
        request: crate::run::MutationRequest<crate::run::ActivateHarness>,
    ) -> Result<crate::run::RunCommandResult, crate::Error> {
        self.inner.activate_harness(request).await
    }
    pub async fn rollback_harness(
        &self,
        request: crate::run::MutationRequest<crate::run::RollbackHarness>,
    ) -> Result<crate::run::RunCommandResult, crate::Error> {
        self.inner.rollback_harness(request).await
    }

    /// Persists and claims the exact typed intent before calling host code.
    pub async fn execute_program(
        &self,
        request: ProgramExecutionRequest,
        runtime: &dyn ProgramRuntime,
        artifacts: &dyn crate::run::ArtifactStore,
    ) -> Result<ProgramReceipt, crate::Error> {
        request.intent.validate()?;
        verify_program_artifacts(artifacts, &request.intent)?;
        request.capability.credential.validate_at(request.now_ms)?;
        request
            .capability
            .validate_dispatch_bound(&request.intent.reservation)?;
        if request.capability.provider != request.capability.credential.provider
            || !request
                .intent
                .reservation
                .within(&request.capability.max_dispatch)
            || request.intent.reservation.is_zero()
        {
            return Err(crate::run::RunError::Capability(
                "program reservation exceeds finite provider dispatch bound".into(),
            )
            .into());
        }
        let spec = crate::run::EffectSpec::ProgramExecution {
            program: request.intent.program.clone(),
            context: request.intent.context.manifest.clone(),
            skills: request.intent.context.skills.clone(),
            compaction: request.intent.context.compaction.clone(),
            checkpoint: None,
            action_limit: request.intent.action_limit,
            provider: request.capability.provider.clone(),
            capability_revision: request.capability.revision.clone(),
            credential_key_id: request.capability.credential.key_id.clone(),
            credential_generation: request.capability.credential.generation,
            credential_scope: request.capability.credential.scope.clone(),
        };
        let prepared = self
            .inner
            .prepare_operation(crate::run::MutationRequest::new(
                request.intent.run_id.clone(),
                request.expected_revision,
                request.prepare_command,
                crate::run::PrepareOperation::new(
                    request.intent.operation_id.clone(),
                    request.iteration_id,
                    crate::run::EffectClass::Reconcilable,
                    spec,
                ),
            ))
            .await?;
        let mut claim = crate::run::ClaimEffect::new(request.intent.operation_id.clone())
            .reservation(request.intent.reservation.clone());
        if let Some(activation) = request.activation {
            claim = claim.activation(activation);
        }
        let claimed = self
            .inner
            .claim_effect(crate::run::MutationRequest::new(
                request.intent.run_id.clone(),
                prepared.snapshot.run.revision,
                request.claim_command,
                claim,
            ))
            .await?;
        match runtime
            .execute(request.intent, &request.capability.credential)
            .await
        {
            Ok(receipt) => {
                if receipt.operation_id != claimed.output.operation_id
                    || receipt.handle.id.trim().is_empty()
                    || receipt.handle.generation == 0
                    || !receipt.usage.within(&claimed.output.reservation)
                    || verify_artifact_ref(artifacts, &receipt.result).is_err()
                    || receipt
                        .checkpoint
                        .as_ref()
                        .is_some_and(|value| verify_artifact_ref(artifacts, value).is_err())
                {
                    self.inner
                        .acknowledge_effect(crate::run::EffectCallback::new(
                            &claimed.output,
                            crate::run::EffectOutcome::Unknown {
                                message: "runtime returned a mismatched or over-bound receipt"
                                    .into(),
                            },
                        ))
                        .await?;
                    return Err(crate::run::RunError::Integrity(
                        "runtime returned a mismatched or over-bound receipt".into(),
                    )
                    .into());
                }
                let durable = durable_program_receipt(&receipt)?;
                let acknowledged = self
                    .inner
                    .acknowledge_effect(crate::run::EffectCallback::new(
                        &claimed.output,
                        crate::run::EffectOutcome::Applied { receipt: durable },
                    ))
                    .await?;
                let state =
                    &acknowledged.snapshot.run.operations[&claimed.output.operation_id].state;
                if *state != crate::run::OperationState::Acknowledged {
                    return Err(crate::run::RunError::Integrity(
                        "program receipt was not durably acknowledged".into(),
                    )
                    .into());
                }
                Ok(receipt)
            }
            Err(error) => {
                self.inner
                    .acknowledge_effect(crate::run::EffectCallback::new(
                        &claimed.output,
                        crate::run::EffectOutcome::Unknown {
                            message: "program transport outcome unknown".into(),
                        },
                    ))
                    .await?;
                Err(error.into())
            }
        }
    }

    pub async fn reconcile_program(
        &self,
        request: ProgramReconcileRequest,
        runtime: &dyn ProgramRuntime,
        artifacts: &dyn crate::run::ArtifactStore,
    ) -> Result<crate::run::RunCommandResult, crate::Error> {
        let decision = match runtime.reconcile(&request.operation_id).await? {
            ProgramRecovery::NotExecuted => crate::run::ReconcileDecision::NotApplied,
            ProgramRecovery::Uncertain => crate::run::ReconcileDecision::Unknown {
                message: "program runtime remains uncertain".into(),
            },
            ProgramRecovery::Completed(receipt) => {
                if receipt.operation_id != request.operation_id
                    || receipt.handle.id.trim().is_empty()
                    || receipt.handle.generation == 0
                    || verify_artifact_ref(artifacts, &receipt.result).is_err()
                    || receipt
                        .checkpoint
                        .as_ref()
                        .is_some_and(|value| verify_artifact_ref(artifacts, value).is_err())
                {
                    return Err(crate::run::RunError::Integrity(
                        "reconciled program receipt or artifact mismatch".into(),
                    )
                    .into());
                }
                crate::run::ReconcileDecision::Applied {
                    receipt: durable_program_receipt(&receipt)?,
                }
            }
        };
        self.inner
            .reconcile_effect(crate::run::MutationRequest::new(
                request.run_id,
                request.expected_revision,
                request.command_id,
                crate::run::ReconcileEffect::new(request.operation_id, decision),
            ))
            .await
    }
}

fn durable_program_receipt(
    receipt: &ProgramReceipt,
) -> Result<crate::run::EffectReceipt, RunError> {
    crate::run::EffectReceipt::for_program(
        crate::run::ProgramReceiptBinding::new(
            receipt.operation_id.clone(),
            receipt.handle.id.clone(),
            receipt.handle.generation,
            receipt.result.clone(),
            receipt.checkpoint.clone(),
        ),
        crate::run::EffectUsage::measured(receipt.usage.clone()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_and_provider_bounds_fail_closed() {
        let expired = CredentialRef::new("provider", "key-id", "opaque", "program.execute", 1, 10);
        assert!(expired.validate_at(10).is_err());
        let capability = ProviderCapability {
            provider: "provider".into(),
            revision: "v1".into(),
            max_dispatch: ResourceVector::default(),
            credential: CredentialRef::new(
                "provider",
                "key-id",
                "opaque",
                "program.execute",
                2,
                20,
            ),
        };
        assert!(
            capability
                .validate_dispatch_bound(&ResourceVector::default().tokens(1))
                .is_err()
        );
    }
}
