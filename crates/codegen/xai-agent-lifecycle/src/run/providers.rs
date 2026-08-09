use super::artifact::{ArtifactStore, LocalArtifactStore};
use super::model::{ArtifactRef, GoalSpec, GoalVerdict, IterationId, RunError, RunId, SessionRef};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateRequest {
    pub run_id: RunId,
    pub iteration_id: IterationId,
    pub gate: String,
    pub input_digest: String,
    pub workspace_digest: String,
    pub evidence: Vec<ArtifactRef>,
}

impl GateRequest {
    pub fn new(
        run_id: RunId,
        iteration_id: IterationId,
        gate: impl Into<String>,
        input_digest: impl Into<String>,
        workspace_digest: impl Into<String>,
    ) -> Self {
        Self {
            run_id,
            iteration_id,
            gate: gate.into(),
            input_digest: input_digest.into(),
            workspace_digest: workspace_digest.into(),
            evidence: Vec::new(),
        }
    }

    pub fn evidence(mut self, values: impl IntoIterator<Item = ArtifactRef>) -> Self {
        self.evidence = values.into_iter().collect();
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateEvaluation {
    pub passed: bool,
    pub input_digest: String,
    pub workspace_digest: String,
    pub evidence: Vec<ArtifactRef>,
    pub reason: String,
}

impl GateEvaluation {
    pub fn new(
        passed: bool,
        input_digest: impl Into<String>,
        workspace_digest: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            passed,
            input_digest: input_digest.into(),
            workspace_digest: workspace_digest.into(),
            evidence: Vec::new(),
            reason: reason.into(),
        }
    }

    pub fn evidence(mut self, values: impl IntoIterator<Item = ArtifactRef>) -> Self {
        self.evidence = values.into_iter().collect();
        self
    }
}

#[async_trait]
pub trait GateProvider: Send + Sync + 'static {
    async fn evaluate(&self, request: GateRequest) -> Result<GateEvaluation, RunError>;
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalVerificationRequest {
    pub run_id: RunId,
    pub iteration_id: IterationId,
    pub goal: GoalSpec,
    pub driver_summary: String,
    pub workspace_digest: String,
    pub evidence: Vec<ArtifactRef>,
}

impl GoalVerificationRequest {
    pub fn new(
        run_id: RunId,
        iteration_id: IterationId,
        goal: GoalSpec,
        driver_summary: impl Into<String>,
        workspace_digest: impl Into<String>,
    ) -> Self {
        Self {
            run_id,
            iteration_id,
            goal,
            driver_summary: driver_summary.into(),
            workspace_digest: workspace_digest.into(),
            evidence: Vec::new(),
        }
    }

    pub fn evidence(mut self, values: impl IntoIterator<Item = ArtifactRef>) -> Self {
        self.evidence = values.into_iter().collect();
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalVerification {
    pub verdict: GoalVerdict,
    pub policy_digest: String,
    pub reason: String,
}

impl GoalVerification {
    pub fn new(
        verdict: GoalVerdict,
        policy_digest: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            verdict,
            policy_digest: policy_digest.into(),
            reason: reason.into(),
        }
    }
}

#[async_trait]
pub trait GoalVerifier: Send + Sync + 'static {
    async fn verify(&self, request: GoalVerificationRequest) -> Result<GoalVerification, RunError>;
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub run_id: RunId,
    pub iteration_id: Option<IterationId>,
    pub capability: String,
    pub reason: String,
    pub request_digest: String,
}

impl ApprovalRequest {
    pub fn new(
        run_id: RunId,
        capability: impl Into<String>,
        reason: impl Into<String>,
        request_digest: impl Into<String>,
    ) -> Self {
        Self {
            run_id,
            iteration_id: None,
            capability: capability.into(),
            reason: reason.into(),
            request_digest: request_digest.into(),
        }
    }

    pub fn iteration_id(mut self, value: IterationId) -> Self {
        self.iteration_id = Some(value);
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approved,
    Denied,
    #[serde(other)]
    Unknown,
}

#[async_trait]
pub trait ApprovalHandler: Send + Sync + 'static {
    async fn decide(&self, request: ApprovalRequest) -> Result<ApprovalDecision, RunError>;
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryRecord {
    pub run_id: RunId,
    pub session: SessionRef,
    pub event: String,
    pub at_ms: u64,
    pub fields: std::collections::BTreeMap<String, String>,
}

impl TelemetryRecord {
    pub fn new(run_id: RunId, session: SessionRef, event: impl Into<String>, at_ms: u64) -> Self {
        Self {
            run_id,
            session,
            event: event.into(),
            at_ms,
            fields: std::collections::BTreeMap::new(),
        }
    }

    pub fn fields(mut self, values: impl IntoIterator<Item = (String, String)>) -> Self {
        self.fields = values.into_iter().collect();
        self
    }
}

#[async_trait]
pub trait TelemetrySink: Send + Sync + 'static {
    async fn emit(&self, record: TelemetryRecord) -> Result<(), RunError>;
}

#[derive(Default)]
pub struct FailClosedGateProvider;

#[async_trait]
impl GateProvider for FailClosedGateProvider {
    async fn evaluate(&self, request: GateRequest) -> Result<GateEvaluation, RunError> {
        Ok(GateEvaluation {
            passed: false,
            input_digest: request.input_digest,
            workspace_digest: request.workspace_digest,
            evidence: Vec::new(),
            reason: "no GateProvider was configured".into(),
        })
    }
}

#[derive(Default)]
pub struct FailClosedGoalVerifier;

#[async_trait]
impl GoalVerifier for FailClosedGoalVerifier {
    async fn verify(
        &self,
        _request: GoalVerificationRequest,
    ) -> Result<GoalVerification, RunError> {
        Ok(GoalVerification {
            verdict: GoalVerdict::Unverifiable,
            policy_digest: "unconfigured".into(),
            reason: "no GoalVerifier was configured".into(),
        })
    }
}

#[derive(Default)]
pub struct DenyApprovalHandler;

#[async_trait]
impl ApprovalHandler for DenyApprovalHandler {
    async fn decide(&self, _request: ApprovalRequest) -> Result<ApprovalDecision, RunError> {
        Ok(ApprovalDecision::Denied)
    }
}

#[derive(Default)]
pub struct NoopTelemetrySink;

#[async_trait]
impl TelemetrySink for NoopTelemetrySink {
    async fn emit(&self, _record: TelemetryRecord) -> Result<(), RunError> {
        Ok(())
    }
}

/// Typed provider bundle used by embedded executors. The SDK owns policy and
/// lifecycle; a Host supplies implementations, credentials and scheduling.
#[derive(Clone)]
pub struct ProviderSet {
    pub artifacts: Arc<dyn ArtifactStore>,
    pub gates: Arc<dyn GateProvider>,
    pub verifier: Arc<dyn GoalVerifier>,
    pub approvals: Arc<dyn ApprovalHandler>,
    pub telemetry: Arc<dyn TelemetrySink>,
}

impl ProviderSet {
    pub fn new(
        artifacts: Arc<dyn ArtifactStore>,
        gates: Arc<dyn GateProvider>,
        verifier: Arc<dyn GoalVerifier>,
        approvals: Arc<dyn ApprovalHandler>,
        telemetry: Arc<dyn TelemetrySink>,
    ) -> Self {
        Self {
            artifacts,
            gates,
            verifier,
            approvals,
            telemetry,
        }
    }

    pub fn fail_closed_local(
        artifact_root: impl Into<PathBuf>,
        max_artifact_size: u64,
    ) -> Result<Self, RunError> {
        Ok(Self {
            artifacts: Arc::new(LocalArtifactStore::new(artifact_root, max_artifact_size)?),
            gates: Arc::new(FailClosedGateProvider),
            verifier: Arc::new(FailClosedGoalVerifier),
            approvals: Arc::new(DenyApprovalHandler),
            telemetry: Arc::new(NoopTelemetrySink),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_defaults_fail_closed_without_blocking_telemetry() {
        let directory = tempfile::tempdir().unwrap();
        let providers = ProviderSet::fail_closed_local(directory.path(), 1024).unwrap();
        let run_id = RunId::new("provider_test").unwrap();
        let gate = providers
            .gates
            .evaluate(GateRequest::new(
                run_id.clone(),
                IterationId::new(1),
                "tests",
                "input",
                "workspace",
            ))
            .await
            .unwrap();
        assert!(!gate.passed);
        let verification = providers
            .verifier
            .verify(GoalVerificationRequest::new(
                run_id.clone(),
                IterationId::new(1),
                GoalSpec::new("verify"),
                "summary",
                "workspace",
            ))
            .await
            .unwrap();
        assert_eq!(verification.verdict, GoalVerdict::Unverifiable);
        assert_eq!(
            providers
                .approvals
                .decide(ApprovalRequest::new(run_id, "network", "test", "digest"))
                .await
                .unwrap(),
            ApprovalDecision::Denied
        );
    }
}
