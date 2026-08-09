use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

pub const RUN_SCHEMA_VERSION: u32 = 4;
pub const MAX_RUN_ENVELOPE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_OBJECTIVE_BYTES: usize = 16 * 1024;
pub(crate) const MAX_LIST_ITEMS: usize = 128;
pub(crate) const MAX_ITEM_BYTES: usize = 8 * 1024;
pub(crate) const MAX_EVENTS: usize = 256;
pub(crate) const MAX_COMMAND_RECEIPTS: usize = 4096;
pub(crate) const MAX_MESSAGES: usize = 1024;
pub(crate) const MAX_ITERATION_SUMMARIES: usize = 128;
pub(crate) const MAX_OPERATIONS: usize = 1024;
pub(crate) const MAX_ARTIFACT_SIZE: u64 = 1 << 50;

fn validate_text(
    value: &str,
    max: usize,
    description: &str,
    allow_empty: bool,
) -> Result<(), RunError> {
    if value.len() > max || (!allow_empty && value.trim().is_empty()) {
        Err(RunError::Validation(format!("invalid {description}")))
    } else {
        Ok(())
    }
}

fn validate_text_set(
    values: &BTreeSet<String>,
    max_items: usize,
    max_item_bytes: usize,
    description: &str,
) -> Result<(), RunError> {
    if values.len() > max_items {
        return Err(RunError::Validation(format!(
            "{description} exceeds its item bound"
        )));
    }
    for value in values {
        validate_text(value, max_item_bytes, description, false)?;
    }
    Ok(())
}

fn valid_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

macro_rules! string_id {
    ($name:ident, $description:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, RunError> {
                let value = value.into();
                if !valid_identifier(&value, 160) {
                    return Err(RunError::Validation(format!("invalid {}", $description)));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                use serde::de::Error as _;
                Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
            }
        }
    };
}

macro_rules! numeric_id {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

string_id!(RunId, "run id");
string_id!(SessionRef, "session reference");
string_id!(CommandId, "command id");
string_id!(OperationId, "operation id");
string_id!(DispatchToken, "dispatch token");
string_id!(IterationToken, "iteration token");
string_id!(ChildId, "child id");
string_id!(MessageId, "message id");
string_id!(WorkerId, "worker id");
string_id!(ActivationToken, "activation token");
numeric_id!(RunRevision);
numeric_id!(ControllerEpoch);
numeric_id!(RunEventCursor);
numeric_id!(IterationId);

impl RunId {
    pub fn random() -> Self {
        Self(format!("run_{}", uuid::Uuid::new_v4().simple()))
    }
}

impl DispatchToken {
    pub(crate) fn random() -> Self {
        Self(format!("dispatch_{}", uuid::Uuid::new_v4().simple()))
    }
}

impl IterationToken {
    pub(crate) fn random() -> Self {
        Self(format!("iteration_{}", uuid::Uuid::new_v4().simple()))
    }
}

impl ActivationToken {
    pub(crate) fn random() -> Self {
        Self(format!("activation_{}", uuid::Uuid::new_v4().simple()))
    }
}

/// Host-independent causes which make a Run eligible for bounded execution.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeReason {
    Requested,
    Timer,
    Retry,
    Dependency,
    /// Synthesized durably when a Host claims an overdue deadline.
    CatchUp,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WakeIntent {
    pub reasons: BTreeSet<WakeReason>,
    pub deadline_ms: Option<u64>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationLease {
    pub worker_id: WorkerId,
    pub claim_command_id: CommandId,
    pub epoch: ControllerEpoch,
    pub token: ActivationToken,
    pub expires_at_ms: u64,
    pub reasons: BTreeSet<WakeReason>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeRequest {
    pub reason: WakeReason,
    pub deadline_ms: Option<u64>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimActivation {
    pub worker_id: WorkerId,
    pub lease_ms: u64,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationFence {
    pub worker_id: WorkerId,
    pub epoch: ControllerEpoch,
    pub token: ActivationToken,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResidencyInspection {
    pub run_id: RunId,
    pub revision: RunRevision,
    pub wake: WakeIntent,
    pub lease: Option<ActivationLease>,
    pub overdue: bool,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalSpec {
    pub objective: String,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
}

impl GoalSpec {
    pub fn new(objective: impl Into<String>) -> Self {
        Self {
            objective: objective.into(),
            acceptance_criteria: Vec::new(),
            constraints: Vec::new(),
            required_evidence: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), RunError> {
        if self.objective.trim().is_empty() || self.objective.len() > MAX_OBJECTIVE_BYTES {
            return Err(RunError::Validation(
                "objective is empty or exceeds 16 KiB".into(),
            ));
        }
        for values in [
            &self.acceptance_criteria,
            &self.constraints,
            &self.required_evidence,
        ] {
            if values.len() > MAX_LIST_ITEMS
                || values
                    .iter()
                    .any(|value| value.trim().is_empty() || value.len() > MAX_ITEM_BYTES)
            {
                return Err(RunError::Validation("goal list exceeds bounds".into()));
            }
        }
        Ok(())
    }

    pub fn acceptance_criteria(mut self, values: impl IntoIterator<Item = String>) -> Self {
        self.acceptance_criteria = values.into_iter().collect();
        self
    }

    pub fn constraints(mut self, values: impl IntoIterator<Item = String>) -> Self {
        self.constraints = values.into_iter().collect();
        self
    }

    pub fn required_evidence(mut self, values: impl IntoIterator<Item = String>) -> Self {
        self.required_evidence = values.into_iter().collect();
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalVerdict {
    Achieved,
    NotAchieved,
    #[serde(other)]
    Unverifiable,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Active,
    UserPaused,
    BackOffPaused,
    NoProgressPaused,
    InfraPaused,
    Blocked,
    BudgetLimited,
    RecoveryRequired,
    Interrupted,
    Complete,
    Failed,
    Cancelled,
    Tombstoned,
}

impl RunStatus {
    pub fn from_wire_str(value: &str) -> Self {
        match value {
            "active" | "Active" => Self::Active,
            "user_paused" | "paused" | "Paused" | "doom_loop_paused" => Self::UserPaused,
            "back_off_paused" => Self::BackOffPaused,
            "no_progress_paused" => Self::NoProgressPaused,
            "infra_paused" => Self::InfraPaused,
            "blocked" => Self::Blocked,
            "budget_limited" | "BudgetLimited" => Self::BudgetLimited,
            "recovery_required" => Self::RecoveryRequired,
            "interrupted" => Self::Interrupted,
            "complete" | "Complete" => Self::Complete,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            "tombstoned" => Self::Tombstoned,
            _ => Self::RecoveryRequired,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::UserPaused => "user_paused",
            Self::BackOffPaused => "back_off_paused",
            Self::NoProgressPaused => "no_progress_paused",
            Self::InfraPaused => "infra_paused",
            Self::Blocked => "blocked",
            Self::BudgetLimited => "budget_limited",
            Self::RecoveryRequired => "recovery_required",
            Self::Interrupted => "interrupted",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Tombstoned => "tombstoned",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Interrupted | Self::Complete | Self::Failed | Self::Cancelled | Self::Tombstoned
        )
    }

    pub fn requires_recovery(self) -> bool {
        self == Self::RecoveryRequired
    }
}

/// Stable user-facing lifecycle projection. Detailed reducer choreography
/// remains available in `RunStatus`/`RunStage`, but clients normally need only
/// to know whether work may continue, what it is waiting for, what recovery
/// evidence is required, or how it finished.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", content = "detail", rename_all = "snake_case")]
pub enum RunLifecycle {
    Active,
    Waiting(WaitingReason),
    Finished(FinishedOutcome),
    Recovering,
}

#[derive(Deserialize)]
struct RunLifecycleWire {
    state: String,
    #[serde(default)]
    detail: Option<serde_json::Value>,
}

impl<'de> Deserialize<'de> for RunLifecycle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error as _;
        let wire = RunLifecycleWire::deserialize(deserializer)?;
        match wire.state.as_str() {
            "active" if wire.detail.is_none() => Ok(Self::Active),
            "active" => Err(D::Error::custom("active lifecycle must not contain detail")),
            "waiting" => serde_json::from_value(
                wire.detail
                    .ok_or_else(|| D::Error::custom("waiting lifecycle omitted its detail"))?,
            )
            .map(Self::Waiting)
            .map_err(D::Error::custom),
            "finished" => serde_json::from_value(
                wire.detail
                    .ok_or_else(|| D::Error::custom("finished lifecycle omitted its detail"))?,
            )
            .map(Self::Finished)
            .map_err(D::Error::custom),
            "recovering" if wire.detail.is_none() => Ok(Self::Recovering),
            "recovering" => Err(D::Error::custom(
                "recovering lifecycle must not contain detail",
            )),
            _ => Ok(Self::Recovering),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitingReason {
    User,
    Backoff,
    NoProgress,
    Infrastructure,
    Blocked,
    BudgetExhausted,
    Approval,
    #[serde(other)]
    Unknown,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishedOutcome {
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
    Tombstoned,
    #[serde(other)]
    Unknown,
}

impl<'de> Deserialize<'de> for RunStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::from_wire_str(&String::deserialize(deserializer)?))
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStage {
    Idle,
    Planning,
    Preparing,
    Executing,
    Collecting,
    Verifying,
    Refining,
    AwaitingApproval,
    Recovering,
    #[serde(other)]
    Unknown,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunDriverSpec {
    AutonomousTurnLoop {
        session: SessionRef,
        strategy_revision: u64,
    },
    RhaiWorkflow {
        session: SessionRef,
        workflow_name: String,
        workflow_revision: u64,
        args_digest: String,
    },
    External {
        driver_name: String,
        driver_version: String,
    },
    #[serde(other)]
    Unknown,
}

impl RunDriverSpec {
    pub fn session(&self) -> Option<&SessionRef> {
        match self {
            Self::AutonomousTurnLoop { session, .. } | Self::RhaiWorkflow { session, .. } => {
                Some(session)
            }
            Self::External { .. } | Self::Unknown => None,
        }
    }

    fn validate(&self) -> Result<(), RunError> {
        match self {
            Self::AutonomousTurnLoop { .. } => Ok(()),
            Self::RhaiWorkflow {
                workflow_name,
                args_digest,
                ..
            } => {
                validate_text(workflow_name, 256, "workflow name", false)?;
                validate_text(args_digest, 512, "workflow args digest", false)
            }
            Self::External {
                driver_name,
                driver_version,
            } => {
                validate_text(driver_name, 256, "external driver name", false)?;
                validate_text(driver_version, 256, "external driver version", false)
            }
            Self::Unknown => Err(RunError::Integrity(
                "unknown durable Run driver requires schema migration".into(),
            )),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityPolicy {
    #[serde(default)]
    pub required: BTreeSet<String>,
    #[serde(default)]
    pub available: BTreeSet<String>,
    #[serde(default)]
    pub ceiling: BTreeSet<String>,
}

impl CapabilityPolicy {
    pub fn new(
        required: impl IntoIterator<Item = String>,
        available: impl IntoIterator<Item = String>,
        ceiling: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            required: required.into_iter().collect(),
            available: available.into_iter().collect(),
            ceiling: ceiling.into_iter().collect(),
        }
    }

    pub fn validate(&self) -> Result<(), RunError> {
        for values in [&self.required, &self.available, &self.ceiling] {
            validate_text_set(values, MAX_LIST_ITEMS, 256, "capability name")?;
        }
        if !self.required.is_subset(&self.available) || !self.required.is_subset(&self.ceiling) {
            return Err(RunError::Capability(
                "required capabilities exceed runtime availability or Run ceiling".into(),
            ));
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceVector {
    pub iterations: u64,
    pub agent_calls: u64,
    pub agent_concurrency: u64,
    pub active_ms: u64,
    pub wall_ms: u64,
    pub tokens: u64,
    pub cost_micros: u64,
    pub artifact_bytes: u64,
}

impl ResourceVector {
    pub fn iterations(mut self, value: u64) -> Self {
        self.iterations = value;
        self
    }

    pub fn agent_calls(mut self, value: u64) -> Self {
        self.agent_calls = value;
        self
    }

    pub fn agent_concurrency(mut self, value: u64) -> Self {
        self.agent_concurrency = value;
        self
    }

    pub fn active_ms(mut self, value: u64) -> Self {
        self.active_ms = value;
        self
    }

    pub fn wall_ms(mut self, value: u64) -> Self {
        self.wall_ms = value;
        self
    }

    pub fn tokens(mut self, value: u64) -> Self {
        self.tokens = value;
        self
    }

    pub fn cost_micros(mut self, value: u64) -> Self {
        self.cost_micros = value;
        self
    }

    pub fn artifact_bytes(mut self, value: u64) -> Self {
        self.artifact_bytes = value;
        self
    }

    pub fn within(&self, limit: &Self) -> bool {
        self.iterations <= limit.iterations
            && self.agent_calls <= limit.agent_calls
            && self.agent_concurrency <= limit.agent_concurrency
            && self.active_ms <= limit.active_ms
            && self.wall_ms <= limit.wall_ms
            && self.tokens <= limit.tokens
            && self.cost_micros <= limit.cost_micros
            && self.artifact_bytes <= limit.artifact_bytes
    }

    pub fn is_zero(&self) -> bool {
        self == &Self::default()
    }

    pub(crate) fn add_usage(&self, delta: &Self) -> Option<Self> {
        Some(Self {
            iterations: self.iterations.checked_add(delta.iterations)?,
            agent_calls: self.agent_calls.checked_add(delta.agent_calls)?,
            agent_concurrency: self.agent_concurrency.max(delta.agent_concurrency),
            active_ms: self.active_ms.checked_add(delta.active_ms)?,
            wall_ms: self.wall_ms.checked_add(delta.wall_ms)?,
            tokens: self.tokens.checked_add(delta.tokens)?,
            cost_micros: self.cost_micros.checked_add(delta.cost_micros)?,
            artifact_bytes: self.artifact_bytes.checked_add(delta.artifact_bytes)?,
        })
    }

    pub(crate) fn add_reservation(&self, delta: &Self) -> Option<Self> {
        Some(Self {
            iterations: self.iterations.checked_add(delta.iterations)?,
            agent_calls: self.agent_calls.checked_add(delta.agent_calls)?,
            agent_concurrency: self
                .agent_concurrency
                .checked_add(delta.agent_concurrency)?,
            active_ms: self.active_ms.checked_add(delta.active_ms)?,
            wall_ms: self.wall_ms.checked_add(delta.wall_ms)?,
            tokens: self.tokens.checked_add(delta.tokens)?,
            cost_micros: self.cost_micros.checked_add(delta.cost_micros)?,
            artifact_bytes: self.artifact_bytes.checked_add(delta.artifact_bytes)?,
        })
    }

    pub(crate) fn subtract_reservation(&self, delta: &Self) -> Option<Self> {
        Some(Self {
            iterations: self.iterations.checked_sub(delta.iterations)?,
            agent_calls: self.agent_calls.checked_sub(delta.agent_calls)?,
            agent_concurrency: self
                .agent_concurrency
                .checked_sub(delta.agent_concurrency)?,
            active_ms: self.active_ms.checked_sub(delta.active_ms)?,
            wall_ms: self.wall_ms.checked_sub(delta.wall_ms)?,
            tokens: self.tokens.checked_sub(delta.tokens)?,
            cost_micros: self.cost_micros.checked_sub(delta.cost_micros)?,
            artifact_bytes: self.artifact_bytes.checked_sub(delta.artifact_bytes)?,
        })
    }

    pub(crate) fn with_reservations(&self, reservations: &Self) -> Option<Self> {
        let mut combined = self.add_reservation(reservations)?;
        // Settled usage records the historical concurrency high-water mark;
        // only concurrently active reservations add to one another. Combining
        // the two therefore takes the maximum rather than charging history as
        // if it were still running.
        combined.agent_concurrency = self.agent_concurrency.max(reservations.agent_concurrency);
        Some(combined)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub digest: String,
    pub media_type: String,
    pub size: u64,
    pub provenance: String,
    pub owner: String,
    pub retention: String,
    #[serde(default)]
    pub evidence_labels: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_digest: Option<String>,
}

/// Exact skill content selected for durable execution.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillDescriptorPin {
    pub name: String,
    pub version: String,
    pub descriptor: ArtifactRef,
}

impl SkillDescriptorPin {
    pub fn validate(&self) -> Result<(), RunError> {
        validate_text(&self.name, 256, "skill name", false)?;
        validate_text(&self.version, 128, "skill version", false)?;
        self.descriptor.validate()
    }
}

/// Content-addressed compaction output with explicit evidence continuity.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionCheckpoint {
    pub artifact: ArtifactRef,
    pub history_start: u64,
    pub history_end: u64,
    pub cursor: u64,
    pub evidence: Vec<ArtifactRef>,
}

impl CompactionCheckpoint {
    pub fn validate(&self) -> Result<(), RunError> {
        if self.history_start > self.history_end
            || self.cursor < self.history_end
            || self.evidence.len() > MAX_LIST_ITEMS
        {
            return Err(RunError::Validation(
                "invalid compaction evidence continuity".into(),
            ));
        }
        self.artifact.validate()?;
        for evidence in &self.evidence {
            evidence.validate()?;
        }
        Ok(())
    }
}

impl ArtifactRef {
    pub fn new(
        digest: impl Into<String>,
        media_type: impl Into<String>,
        size: u64,
        provenance: impl Into<String>,
        owner: impl Into<String>,
    ) -> Self {
        Self {
            digest: digest.into(),
            media_type: media_type.into(),
            size,
            provenance: provenance.into(),
            owner: owner.into(),
            retention: "run".into(),
            evidence_labels: BTreeSet::new(),
            workspace_digest: None,
        }
    }

    pub fn retention(mut self, value: impl Into<String>) -> Self {
        self.retention = value.into();
        self
    }

    pub fn evidence_labels(mut self, values: impl IntoIterator<Item = String>) -> Self {
        self.evidence_labels = values.into_iter().collect();
        self
    }

    pub fn workspace_digest(mut self, value: impl Into<String>) -> Self {
        self.workspace_digest = Some(value.into());
        self
    }

    pub fn validate(&self) -> Result<(), RunError> {
        if !valid_sha256(&self.digest)
            || self.media_type.trim().is_empty()
            || self.owner.trim().is_empty()
            || self.size > MAX_ARTIFACT_SIZE
        {
            return Err(RunError::Validation("invalid artifact reference".into()));
        }
        validate_text(&self.media_type, 256, "artifact media type", false)?;
        validate_text(&self.provenance, 1024, "artifact provenance", false)?;
        validate_text(&self.owner, 160, "artifact owner", false)?;
        validate_text(&self.retention, 128, "artifact retention", false)?;
        validate_text_set(
            &self.evidence_labels,
            MAX_LIST_ITEMS,
            256,
            "artifact evidence label",
        )?;
        if let Some(workspace_digest) = &self.workspace_digest {
            validate_text(workspace_digest, 512, "artifact workspace digest", false)?;
        }
        Ok(())
    }
}

pub(crate) fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IterationContextManifest {
    pub goal_revision: RunRevision,
    pub strategy_revision: u64,
    pub workflow_revision: Option<u64>,
    pub policy_digest: String,
    pub model_revision: String,
    pub workspace_revision: String,
    pub history_range: (u64, u64),
    pub memory_snapshot: Option<String>,
    pub artifacts: Vec<ArtifactRef>,
    pub steering_high_water: u64,
    /// Exact immutable Harness content selected by the Run reducer.
    pub harness_snapshot_digest: String,
}

impl IterationContextManifest {
    pub fn new(
        goal_revision: RunRevision,
        strategy_revision: u64,
        policy_digest: impl Into<String>,
        model_revision: impl Into<String>,
        workspace_revision: impl Into<String>,
    ) -> Self {
        Self {
            goal_revision,
            strategy_revision,
            workflow_revision: None,
            policy_digest: policy_digest.into(),
            model_revision: model_revision.into(),
            workspace_revision: workspace_revision.into(),
            history_range: (0, 0),
            memory_snapshot: None,
            artifacts: Vec::new(),
            steering_high_water: 0,
            harness_snapshot_digest: String::new(),
        }
    }

    pub fn workflow_revision(mut self, value: u64) -> Self {
        self.workflow_revision = Some(value);
        self
    }

    pub fn history_range(mut self, start: u64, end: u64) -> Self {
        self.history_range = (start, end);
        self
    }

    pub fn memory_snapshot(mut self, value: impl Into<String>) -> Self {
        self.memory_snapshot = Some(value.into());
        self
    }

    pub fn artifacts(mut self, values: impl IntoIterator<Item = ArtifactRef>) -> Self {
        self.artifacts = values.into_iter().collect();
        self
    }

    pub fn steering_high_water(mut self, value: u64) -> Self {
        self.steering_high_water = value;
        self
    }

    pub fn harness_snapshot(mut self, digest: impl Into<String>) -> Self {
        self.harness_snapshot_digest = digest.into();
        self
    }

    pub fn validate(&self) -> Result<(), RunError> {
        validate_text(&self.policy_digest, 256, "iteration policy digest", false)?;
        validate_text(&self.model_revision, 256, "iteration model revision", false)?;
        if !valid_sha256(&self.harness_snapshot_digest) {
            return Err(RunError::Validation(
                "iteration omitted a valid Harness snapshot pin".into(),
            ));
        }
        validate_text(
            &self.workspace_revision,
            512,
            "iteration workspace revision",
            false,
        )?;
        if self.history_range.0 > self.history_range.1 || self.artifacts.len() > MAX_LIST_ITEMS {
            return Err(RunError::Validation(
                "iteration context range or artifact count is invalid".into(),
            ));
        }
        if let Some(memory_snapshot) = &self.memory_snapshot {
            validate_text(memory_snapshot, 512, "iteration memory snapshot", false)?;
        }
        for artifact in &self.artifacts {
            artifact.validate()?;
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IterationManifest {
    pub iteration_id: IterationId,
    pub token: IterationToken,
    pub context: IterationContextManifest,
    pub started_at_ms: u64,
    pub driver_terminal_success: bool,
    pub summary: Option<String>,
    pub evidence: Vec<ArtifactRef>,
    pub gates: BTreeMap<String, bool>,
    pub verdict: Option<GoalVerdict>,
    pub finished_at_ms: Option<u64>,
    pub result_digest: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub recovery_abandoned: bool,
}

impl IterationManifest {
    pub(crate) fn validate(&self) -> Result<(), RunError> {
        if self.iteration_id.get() == 0
            || self
                .summary
                .as_ref()
                .is_some_and(|value| value.len() > MAX_ITEM_BYTES)
            || self.evidence.len() > MAX_LIST_ITEMS
            || self.gates.len() > MAX_LIST_ITEMS
            || self
                .result_digest
                .as_ref()
                .is_some_and(|digest| !valid_sha256(digest))
        {
            return Err(RunError::Validation("invalid iteration manifest".into()));
        }
        self.context.validate()?;
        for artifact in &self.evidence {
            artifact.validate()?;
        }
        for gate in self.gates.keys() {
            validate_text(gate, 256, "gate name", false)?;
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    Replayable,
    Idempotent,
    Reconcilable,
    NonRepeatable,
}

impl<'de> Deserialize<'de> for EffectClass {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match String::deserialize(deserializer)?.as_str() {
            "replayable" => Self::Replayable,
            "idempotent" => Self::Idempotent,
            "reconcilable" => Self::Reconcilable,
            _ => Self::NonRepeatable,
        })
    }
}

#[non_exhaustive]
// This public durable-schema enum keeps its structured variants inline so the
// serialized contract and consumer construction remain uniform. Boxing only
// the current largest variant would add API indirection without reducing any
// persisted payload.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EffectSpec {
    SessionTurn {
        session: SessionRef,
        turn_id: String,
        prompt_digest: String,
        input: ArtifactRef,
    },
    RhaiWorkflow {
        session: SessionRef,
        workflow: ArtifactRef,
        args: ArtifactRef,
    },
    ChildAgent {
        child: ChildId,
        request: ArtifactRef,
    },
    Gate {
        name: String,
        input: ArtifactRef,
    },
    ArtifactMutation {
        mutation: ArtifactRef,
    },
    External {
        provider: String,
        version: String,
        payload: ArtifactRef,
    },
    ProgramExecution {
        program: ArtifactRef,
        context: ArtifactRef,
        skills: Vec<SkillDescriptorPin>,
        compaction: Option<CompactionCheckpoint>,
        checkpoint: Option<ArtifactRef>,
        action_limit: u32,
        provider: String,
        capability_revision: String,
        /// Non-secret Host key identity. The executable opaque credential
        /// handle is never serialized into the Run.
        credential_key_id: String,
        credential_generation: u64,
        credential_scope: String,
    },
    #[serde(other)]
    Unknown,
}

impl EffectSpec {
    pub fn digest(&self) -> Result<String, RunError> {
        canonical_digest(self)
    }

    pub fn validate(&self) -> Result<(), RunError> {
        match self {
            Self::SessionTurn {
                turn_id,
                prompt_digest,
                input,
                ..
            } => {
                validate_text(turn_id, 512, "Session Turn id", false)?;
                validate_text(prompt_digest, 160, "Session Turn prompt digest", false)?;
                input.validate()
            }
            Self::RhaiWorkflow { workflow, args, .. } => {
                workflow.validate()?;
                args.validate()
            }
            Self::ChildAgent { request, .. } => request.validate(),
            Self::Gate { name, input } => {
                validate_text(name, 256, "gate name", false)?;
                input.validate()
            }
            Self::ArtifactMutation { mutation } => mutation.validate(),
            Self::External {
                provider,
                version,
                payload,
            } => {
                validate_text(provider, 256, "effect provider", false)?;
                validate_text(version, 256, "effect provider version", false)?;
                payload.validate()
            }
            Self::ProgramExecution {
                program,
                context,
                skills,
                compaction,
                checkpoint,
                action_limit,
                provider,
                capability_revision,
                credential_key_id,
                credential_scope,
                ..
            } => {
                if *action_limit == 0 || *action_limit > 1024 || skills.len() > 128 {
                    return Err(RunError::Validation(
                        "invalid bounded program effect".into(),
                    ));
                }
                validate_text(provider, 256, "program provider", false)?;
                validate_text(
                    capability_revision,
                    256,
                    "program capability revision",
                    false,
                )?;
                validate_text(credential_key_id, 512, "credential key id", false)?;
                validate_text(credential_scope, 512, "credential scope", false)?;
                program.validate()?;
                context.validate()?;
                for skill in skills {
                    skill.validate()?;
                }
                if let Some(compaction) = compaction {
                    compaction.validate()?;
                }
                if let Some(checkpoint) = checkpoint {
                    checkpoint.validate()?;
                }
                Ok(())
            }
            Self::Unknown => Err(RunError::Integrity(
                "unknown durable Run effect requires schema migration".into(),
            )),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Prepared,
    Dispatching,
    Acknowledged,
    Reconciled,
    /// Recovery evidence proved the intent did not take effect. The containing
    /// iteration was abandoned; a future iteration may create a fresh intent.
    Abandoned,
    FailedRetryable,
    Uncertain,
}

impl<'de> Deserialize<'de> for OperationState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match String::deserialize(deserializer)?.as_str() {
            "prepared" => Self::Prepared,
            "dispatching" => Self::Dispatching,
            "acknowledged" => Self::Acknowledged,
            "reconciled" => Self::Reconciled,
            "abandoned" => Self::Abandoned,
            "failed_retryable" => Self::FailedRetryable,
            _ => Self::Uncertain,
        })
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationAttempt {
    pub attempt: u32,
    pub claim_command_id: CommandId,
    pub token: DispatchToken,
    pub epoch: ControllerEpoch,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceDimension {
    Iterations,
    AgentCalls,
    AgentConcurrency,
    ActiveMs,
    WallMs,
    Tokens,
    CostMicros,
    ArtifactBytes,
    #[serde(other)]
    Unknown,
}

#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectUsage {
    pub resources: ResourceVector,
    /// Explicitly unknown dimensions. Their numeric resource value must be
    /// zero, but zero is not interpreted as measured usage.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub unknown: BTreeSet<ResourceDimension>,
}

impl EffectUsage {
    pub fn measured(resources: ResourceVector) -> Self {
        Self {
            resources,
            unknown: BTreeSet::new(),
        }
    }

    pub fn unknown(mut self, dimensions: impl IntoIterator<Item = ResourceDimension>) -> Self {
        self.unknown.extend(dimensions);
        self
    }

    pub fn validate(&self) -> Result<(), RunError> {
        if self.unknown.contains(&ResourceDimension::Unknown)
            || self
                .unknown
                .iter()
                .any(|dimension| resource_value(&self.resources, *dimension) != 0)
        {
            return Err(RunError::Validation(
                "effect usage has an invalid unknown dimension".into(),
            ));
        }
        Ok(())
    }

    pub fn allows_settlement_with(
        &self,
        reservation: &ResourceVector,
        budget: &ResourceVector,
    ) -> bool {
        ALL_RESOURCE_DIMENSIONS.iter().all(|dimension| {
            if self.unknown.contains(dimension) {
                resource_value(budget, *dimension) == u64::MAX
            } else {
                let actual = resource_value(&self.resources, *dimension);
                actual <= resource_value(reservation, *dimension)
                    && actual <= resource_value(budget, *dimension)
            }
        })
    }

    pub fn is_unknown(&self, dimension: ResourceDimension) -> bool {
        self.unknown.contains(&dimension)
    }
}

const ALL_RESOURCE_DIMENSIONS: [ResourceDimension; 8] = [
    ResourceDimension::Iterations,
    ResourceDimension::AgentCalls,
    ResourceDimension::AgentConcurrency,
    ResourceDimension::ActiveMs,
    ResourceDimension::WallMs,
    ResourceDimension::Tokens,
    ResourceDimension::CostMicros,
    ResourceDimension::ArtifactBytes,
];

fn resource_value(resources: &ResourceVector, dimension: ResourceDimension) -> u64 {
    match dimension {
        ResourceDimension::Iterations => resources.iterations,
        ResourceDimension::AgentCalls => resources.agent_calls,
        ResourceDimension::AgentConcurrency => resources.agent_concurrency,
        ResourceDimension::ActiveMs => resources.active_ms,
        ResourceDimension::WallMs => resources.wall_ms,
        ResourceDimension::Tokens => resources.tokens,
        ResourceDimension::CostMicros => resources.cost_micros,
        ResourceDimension::ArtifactBytes => resources.artifact_bytes,
        ResourceDimension::Unknown => 0,
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectReceipt {
    pub receipt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_prompt_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<SessionTurnOutcome>,
    /// Usage bound into the SessionLedger settlement identity. This covers the
    /// native Turn only and may explicitly mark dimensions as unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_usage: Option<Box<EffectUsage>>,
    /// Complete usage known to the Run callback, including SDK-owned artifacts
    /// and post-processing. Recovery may use it only when every finite budget
    /// dimension is measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_usage: Option<Box<EffectUsage>>,
    /// Typed binding required for ProgramExecution receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program: Option<Box<ProgramReceiptBinding>>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramReceiptBinding {
    pub operation_id: OperationId,
    pub handle_id: String,
    pub handle_generation: u64,
    pub result: ArtifactRef,
    pub checkpoint: Option<ArtifactRef>,
}
impl ProgramReceiptBinding {
    pub fn new(
        operation_id: OperationId,
        handle_id: impl Into<String>,
        handle_generation: u64,
        result: ArtifactRef,
        checkpoint: Option<ArtifactRef>,
    ) -> Self {
        Self {
            operation_id,
            handle_id: handle_id.into(),
            handle_generation,
            result,
            checkpoint,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnOutcome {
    End,
    Cancelled,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    #[serde(other)]
    Unknown,
}

impl SessionTurnOutcome {
    fn ledger_name(self) -> &'static str {
        match self {
            Self::End => "End",
            Self::Cancelled => "Cancelled",
            Self::MaxTokens => "MaxTokens",
            Self::MaxTurnRequests => "MaxTurnRequests",
            Self::Refusal => "Refusal",
            Self::Unknown => "Unknown",
        }
    }
}

impl EffectReceipt {
    pub fn new(receipt_id: impl Into<String>) -> Self {
        Self {
            receipt_id: receipt_id.into(),
            settlement_id: None,
            runtime_prompt_index: None,
            outcome: None,
            session_usage: None,
            actual_usage: None,
            program: None,
        }
    }

    pub fn for_session_turn(
        session: &SessionRef,
        turn_id: &str,
        prompt_digest: &str,
        runtime_prompt_index: u64,
        outcome: SessionTurnOutcome,
        session_usage: EffectUsage,
        actual_usage: EffectUsage,
    ) -> Self {
        let settlement_id = session_turn_settlement_id(
            session,
            turn_id,
            prompt_digest,
            runtime_prompt_index,
            outcome,
            &session_usage,
        );
        let usage_digest =
            canonical_digest(&actual_usage).expect("EffectUsage serialization cannot fail");
        Self {
            receipt_id: format!("session-ledger:{settlement_id}:{usage_digest}"),
            settlement_id: Some(settlement_id),
            runtime_prompt_index: Some(runtime_prompt_index),
            outcome: Some(outcome),
            session_usage: Some(Box::new(session_usage)),
            actual_usage: Some(Box::new(actual_usage)),
            program: None,
        }
    }

    pub fn for_program(
        binding: ProgramReceiptBinding,
        actual_usage: EffectUsage,
    ) -> Result<Self, RunError> {
        actual_usage.validate()?;
        if !actual_usage.unknown.is_empty() {
            return Err(RunError::Validation(
                "program receipt requires fully measured usage".into(),
            ));
        }
        validate_program_binding(&binding)?;
        let digest = canonical_digest(&(&binding, &actual_usage))?;
        Ok(Self {
            receipt_id: format!("program:sha256:{digest}"),
            settlement_id: None,
            runtime_prompt_index: None,
            outcome: None,
            session_usage: None,
            actual_usage: Some(Box::new(actual_usage)),
            program: Some(Box::new(binding)),
        })
    }

    pub fn actual_usage(mut self, usage: EffectUsage) -> Self {
        self.actual_usage = Some(Box::new(usage));
        self
    }

    pub fn program_binding(mut self, binding: ProgramReceiptBinding) -> Self {
        self.program = Some(Box::new(binding));
        self
    }
}

pub fn session_turn_settlement_id(
    session: &SessionRef,
    turn_id: &str,
    prompt_digest: &str,
    runtime_prompt_index: u64,
    outcome: SessionTurnOutcome,
    usage: &EffectUsage,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"origin-grok-runtime.settlement.v2\0");
    let prompt_index = runtime_prompt_index.to_be_bytes();
    for field in [
        session.as_str().as_bytes(),
        turn_id.as_bytes(),
        prompt_digest.as_bytes(),
        prompt_index.as_slice(),
        outcome.ledger_name().as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    let usage = serde_json::to_vec(usage).expect("EffectUsage serialization cannot fail");
    digest.update((usage.len() as u64).to_be_bytes());
    digest.update(usage);
    format!("sha256:{:x}", digest.finalize())
}

pub(crate) fn validate_effect_receipt(
    spec: &EffectSpec,
    receipt: &EffectReceipt,
) -> Result<(), RunError> {
    validate_effect_receipt_evidence(receipt)?;
    if let Some(usage) = &receipt.session_usage {
        usage.validate()?;
    }
    if let Some(usage) = &receipt.actual_usage {
        usage.validate()?;
    }
    if let EffectSpec::SessionTurn {
        session,
        turn_id,
        prompt_digest,
        ..
    } = spec
    {
        let settlement_id = receipt.settlement_id.as_deref().ok_or_else(|| {
            RunError::Integrity(
                "Session turn receipt must reference SessionLedger settlement evidence".into(),
            )
        })?;
        let runtime_prompt_index = receipt.runtime_prompt_index.ok_or_else(|| {
            RunError::Integrity("Session turn receipt omitted runtime prompt index".into())
        })?;
        let outcome = receipt.outcome.ok_or_else(|| {
            RunError::Integrity("Session turn receipt omitted typed outcome".into())
        })?;
        let session_usage = receipt.session_usage.as_ref().ok_or_else(|| {
            RunError::Integrity("Session turn receipt omitted ledger usage evidence".into())
        })?;
        let actual_usage = receipt.actual_usage.as_ref().ok_or_else(|| {
            RunError::Integrity("Session turn receipt omitted Run usage evidence".into())
        })?;
        session_usage.validate()?;
        actual_usage.validate()?;
        if outcome == SessionTurnOutcome::Unknown {
            return Err(RunError::Integrity(
                "unknown Session turn outcome is not settlement evidence".into(),
            ));
        }
        let expected = session_turn_settlement_id(
            session,
            turn_id,
            prompt_digest,
            runtime_prompt_index,
            outcome,
            session_usage,
        );
        let usage_digest = canonical_digest(actual_usage)?;
        if settlement_id != expected
            || receipt.receipt_id != format!("session-ledger:{expected}:{usage_digest}")
        {
            return Err(RunError::Integrity(
                "Session turn receipt does not match its durable intent".into(),
            ));
        }
    }
    if let EffectSpec::ProgramExecution { .. } = spec {
        let binding = receipt.program.as_ref().ok_or_else(|| {
            RunError::Integrity(
                "program receipt omitted exact operation/handle/result binding".into(),
            )
        })?;
        validate_program_binding(binding)?;
        let actual_usage = receipt.actual_usage.as_ref().ok_or_else(|| {
            RunError::Integrity("program receipt omitted measured actual usage".into())
        })?;
        if !actual_usage.unknown.is_empty()
            || receipt.receipt_id
                != format!(
                    "program:sha256:{}",
                    canonical_digest(&(binding.as_ref(), actual_usage.as_ref()))?
                )
        {
            return Err(RunError::Integrity(
                "program receipt identity does not match its exact binding and usage".into(),
            ));
        }
    }
    Ok(())
}

fn validate_program_binding(binding: &ProgramReceiptBinding) -> Result<(), RunError> {
    validate_text(&binding.handle_id, 512, "program handle", false)?;
    if binding.handle_generation == 0 {
        return Err(RunError::Validation(
            "program handle generation must be non-zero".into(),
        ));
    }
    binding.result.validate()?;
    if let Some(checkpoint) = &binding.checkpoint {
        checkpoint.validate()?;
    }
    Ok(())
}

pub(crate) fn validate_effect_receipt_evidence(receipt: &EffectReceipt) -> Result<(), RunError> {
    if receipt.receipt_id.trim().is_empty() || receipt.receipt_id.len() > 512 {
        return Err(RunError::Validation("invalid effect receipt".into()));
    }
    if receipt
        .settlement_id
        .as_ref()
        .is_some_and(|value| value.len() > 512 || value.trim().is_empty())
    {
        return Err(RunError::Validation(
            "invalid effect settlement identity".into(),
        ));
    }
    Ok(())
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub id: OperationId,
    pub iteration_id: IterationId,
    pub effect_class: EffectClass,
    pub spec: EffectSpec,
    pub spec_digest: String,
    pub state: OperationState,
    pub next_attempt: u32,
    pub active_attempt: Option<OperationAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation: Option<ResourceVector>,
    pub receipt: Option<EffectReceipt>,
    pub terminal_result_digest: Option<String>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedEffect {
    pub run_id: RunId,
    pub operation_id: OperationId,
    pub iteration_id: IterationId,
    pub attempt: u32,
    pub token: DispatchToken,
    pub epoch: ControllerEpoch,
    pub effect_class: EffectClass,
    pub spec: EffectSpec,
    pub reservation: ResourceVector,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EffectOutcome {
    Applied { receipt: EffectReceipt },
    FailedRetryable { message: String },
    Unknown { message: String },
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectCallback {
    pub run_id: RunId,
    pub epoch: ControllerEpoch,
    pub operation_id: OperationId,
    pub iteration_id: IterationId,
    pub attempt: u32,
    pub token: DispatchToken,
    pub outcome: EffectOutcome,
}

impl EffectCallback {
    pub fn new(effect: &CommittedEffect, outcome: EffectOutcome) -> Self {
        Self {
            run_id: effect.run_id.clone(),
            epoch: effect.epoch,
            operation_id: effect.operation_id.clone(),
            iteration_id: effect.iteration_id,
            attempt: effect.attempt,
            token: effect.token.clone(),
            outcome,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReconcileDecision {
    Applied { receipt: EffectReceipt },
    NotApplied,
    Unknown { message: String },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildState {
    Admitted,
    Started,
    Completed,
    Failed,
    Cancelled,
    Tombstoned,
    #[serde(other)]
    Unknown,
}

impl ChildState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Tombstoned
        )
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildCompletionPolicy {
    MustSucceed,
    MayFail,
    Detached,
    #[serde(other)]
    Unknown,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildRun {
    pub id: ChildId,
    pub state: ChildState,
    pub iteration_id: IterationId,
    pub callback_token: DispatchToken,
    pub reservation: ResourceVector,
    pub settlement: Option<ResourceVector>,
    pub workspace_isolation: String,
    pub completion_policy: ChildCompletionPolicy,
    pub artifacts: Vec<ArtifactRef>,
}

impl ChildRun {
    fn validate(&self) -> Result<(), RunError> {
        if self.state == ChildState::Unknown
            || self.completion_policy == ChildCompletionPolicy::Unknown
            || self.artifacts.len() > MAX_LIST_ITEMS
        {
            return Err(RunError::Integrity("invalid durable child Run".into()));
        }
        validate_text(
            &self.workspace_isolation,
            256,
            "child workspace isolation",
            false,
        )?;
        for artifact in &self.artifacts {
            artifact.validate()?;
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildCallback {
    pub run_id: RunId,
    pub epoch: ControllerEpoch,
    pub iteration_id: IterationId,
    pub child_id: ChildId,
    pub token: DispatchToken,
    pub state: ChildState,
    pub settlement: Option<ResourceVector>,
    pub artifacts: Vec<ArtifactRef>,
}

impl ChildCallback {
    pub fn new(run_id: RunId, epoch: ControllerEpoch, child: &ChildRun, state: ChildState) -> Self {
        Self {
            run_id,
            epoch,
            iteration_id: child.iteration_id,
            child_id: child.id.clone(),
            token: child.callback_token.clone(),
            state,
            settlement: None,
            artifacts: Vec::new(),
        }
    }

    pub fn settlement(mut self, value: ResourceVector) -> Self {
        self.settlement = Some(value);
        self
    }

    pub fn artifacts(mut self, values: impl IntoIterator<Item = ArtifactRef>) -> Self {
        self.artifacts = values.into_iter().collect();
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageState {
    Accepted,
    Queued,
    DeliveredToContext,
    Processed,
    #[serde(other)]
    Unknown,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailMessage {
    pub id: MessageId,
    pub sequence: u64,
    pub causation_id: Option<MessageId>,
    pub sender: String,
    pub trust_label: String,
    pub body: String,
    pub state: MessageState,
}

impl MailMessage {
    fn validate(&self) -> Result<(), RunError> {
        if self.sequence == 0 || self.state == MessageState::Unknown {
            return Err(RunError::Integrity("invalid durable Run message".into()));
        }
        validate_text(&self.sender, 256, "message sender", false)?;
        validate_text(&self.trust_label, 128, "message trust label", false)?;
        validate_text(&self.body, MAX_ITEM_BYTES, "message body", false)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyRevision {
    pub revision: u64,
    pub digest: String,
    pub provenance: String,
    pub applied: bool,
    pub promotion_proposal: Option<String>,
}

impl StrategyRevision {
    fn validate(&self) -> Result<(), RunError> {
        if self.revision == 0 || !valid_sha256(&self.digest) {
            return Err(RunError::Integrity(
                "invalid durable strategy revision".into(),
            ));
        }
        validate_text(&self.provenance, 512, "strategy revision provenance", false)?;
        if let Some(proposal) = &self.promotion_proposal {
            validate_text(
                proposal,
                MAX_ITEM_BYTES,
                "strategy promotion proposal",
                true,
            )?;
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRevisionState {
    Proposal,
    Validated,
    Applied,
    Rejected,
    RolledBack,
    #[serde(other)]
    Unknown,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRevision {
    pub revision: u64,
    pub source_digest: String,
    pub provenance: String,
    pub state: WorkflowRevisionState,
    pub compiled: bool,
    pub static_policy_valid: bool,
    pub dry_run_valid: bool,
    pub promotion_proposal: Option<String>,
}

impl WorkflowRevision {
    fn validate(&self) -> Result<(), RunError> {
        if self.revision == 0
            || !valid_sha256(&self.source_digest)
            || self.state == WorkflowRevisionState::Unknown
        {
            return Err(RunError::Integrity(
                "invalid durable workflow revision".into(),
            ));
        }
        validate_text(&self.provenance, 512, "workflow revision provenance", false)?;
        if let Some(proposal) = &self.promotion_proposal {
            validate_text(
                proposal,
                MAX_ITEM_BYTES,
                "workflow promotion proposal",
                true,
            )?;
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub revision: RunRevision,
    pub controller_epoch: ControllerEpoch,
    pub wake: WakeIntent,
    pub activation_lease: Option<ActivationLease>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub id: RunId,
    pub session: SessionRef,
    pub goal: GoalSpec,
    pub driver: RunDriverSpec,
    pub status: RunStatus,
    pub stage: RunStage,
    pub capabilities: CapabilityPolicy,
    pub required_gates: BTreeSet<String>,
    pub verifier_policy_digest: String,
    pub budget: ResourceVector,
    pub usage: ResourceVector,
    /// Dimensions for which executed work had no trustworthy exact amount.
    /// Such a dimension is permitted only when its budget is explicitly
    /// unbounded (`u64::MAX`); it is never interpreted as zero-cost work.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub usage_unknown: BTreeSet<ResourceDimension>,
    pub child_reserved: ResourceVector,
    pub next_iteration_id: u64,
    pub active_iteration: Option<IterationManifest>,
    pub iterations: VecDeque<IterationManifest>,
    pub operations: BTreeMap<OperationId, Operation>,
    pub children: BTreeMap<ChildId, ChildRun>,
    pub mailbox: BTreeMap<MessageId, MailMessage>,
    pub next_message_sequence: u64,
    pub steering: BTreeMap<MessageId, MailMessage>,
    pub steering_high_water: u64,
    pub strategy_revisions: Vec<StrategyRevision>,
    pub current_strategy_revision: u64,
    pub workflow_revisions: Vec<WorkflowRevision>,
    pub current_workflow_revision: Option<u64>,
    pub harness: HarnessGovernance,
    pub verdict: Option<GoalVerdict>,
    pub pending_approval: bool,
    /// Non-active state to restore after reconciling work that was still in
    /// flight. Recovery must not turn a paused, waiting, or terminal Run back
    /// into an active Run without a later explicit Resume command.
    #[serde(default)]
    pub recovery_prior_status: Option<RunStatus>,
    pub command_receipts: BTreeMap<CommandId, CommandReceipt>,
    pub terminal_report_claimed: bool,
    pub event_cursor: RunEventCursor,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessSnapshotPin {
    pub digest: String,
    pub descriptor_digest: String,
    pub capability_revision: String,
    pub negotiated_capabilities: BTreeSet<String>,
    pub revision: u64,
    pub evidence: Vec<ArtifactRef>,
    pub provenance: String,
}

impl HarnessSnapshotPin {
    pub fn new(
        digest: impl Into<String>,
        descriptor_digest: impl Into<String>,
        capability_revision: impl Into<String>,
        revision: u64,
        provenance: impl Into<String>,
    ) -> Self {
        Self {
            digest: digest.into(),
            descriptor_digest: descriptor_digest.into(),
            capability_revision: capability_revision.into(),
            negotiated_capabilities: BTreeSet::new(),
            revision,
            evidence: Vec::new(),
            provenance: provenance.into(),
        }
    }
    pub fn validate(&self) -> Result<(), RunError> {
        if !valid_sha256(&self.digest)
            || !valid_sha256(&self.descriptor_digest)
            || self.revision == 0
        {
            return Err(RunError::Validation(
                "invalid immutable Harness snapshot pin".into(),
            ));
        }
        validate_text(
            &self.capability_revision,
            256,
            "Harness capability revision",
            false,
        )?;
        validate_text(&self.provenance, 1024, "Harness provenance", false)?;
        validate_text_set(
            &self.negotiated_capabilities,
            MAX_LIST_ITEMS,
            256,
            "Harness capability",
        )?;
        for evidence in &self.evidence {
            evidence.validate()?;
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessProposalState {
    Proposed,
    Validated,
    Active,
    Rejected,
    RolledBack,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessProposal {
    pub pin: HarnessSnapshotPin,
    pub state: HarnessProposalState,
}

#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessGovernance {
    pub active: Option<HarnessSnapshotPin>,
    pub previous: Option<HarnessSnapshotPin>,
    pub proposals: BTreeMap<String, HarnessProposal>,
}

impl RunRecord {
    pub fn lifecycle(&self) -> RunLifecycle {
        match self.status {
            RunStatus::Active => {
                if self.pending_approval || self.stage == RunStage::AwaitingApproval {
                    RunLifecycle::Waiting(WaitingReason::Approval)
                } else {
                    RunLifecycle::Active
                }
            }
            RunStatus::UserPaused => RunLifecycle::Waiting(WaitingReason::User),
            RunStatus::BackOffPaused => RunLifecycle::Waiting(WaitingReason::Backoff),
            RunStatus::NoProgressPaused => RunLifecycle::Waiting(WaitingReason::NoProgress),
            RunStatus::InfraPaused => RunLifecycle::Waiting(WaitingReason::Infrastructure),
            RunStatus::Blocked => RunLifecycle::Waiting(WaitingReason::Blocked),
            RunStatus::BudgetLimited => RunLifecycle::Waiting(WaitingReason::BudgetExhausted),
            RunStatus::RecoveryRequired => RunLifecycle::Recovering,
            RunStatus::Complete => RunLifecycle::Finished(FinishedOutcome::Succeeded),
            RunStatus::Failed => RunLifecycle::Finished(FinishedOutcome::Failed),
            RunStatus::Cancelled => RunLifecycle::Finished(FinishedOutcome::Cancelled),
            RunStatus::Interrupted => RunLifecycle::Finished(FinishedOutcome::Interrupted),
            RunStatus::Tombstoned => RunLifecycle::Finished(FinishedOutcome::Tombstoned),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RunEventKind(String);

impl RunEventKind {
    pub fn new(value: impl Into<String>) -> Result<Self, RunError> {
        let value = value.into();
        if valid_identifier(&value, 96) {
            Ok(Self(value))
        } else {
            Err(RunError::Validation("invalid Run event kind".into()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RunEventKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error as _;
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEvent {
    pub cursor: RunEventCursor,
    pub revision: RunRevision,
    pub kind: RunEventKind,
    pub at_ms: u64,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RunEnvelope {
    pub schema_version: u32,
    pub run: RunRecord,
    pub events: VecDeque<RunEvent>,
}

#[derive(Deserialize)]
struct RunEnvelopeWire {
    schema_version: u32,
    run: RunRecord,
    events: VecDeque<RunEvent>,
}

impl<'de> Deserialize<'de> for RunEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error as _;
        let envelope = Self::from_wire(RunEnvelopeWire::deserialize(deserializer)?);
        envelope.validate().map_err(D::Error::custom)?;
        Ok(envelope)
    }
}

impl RunEnvelope {
    fn from_wire(wire: RunEnvelopeWire) -> Self {
        Self {
            schema_version: wire.schema_version,
            run: wire.run,
            events: wire.events,
        }
    }

    /// Decodes the durable JSON representation after enforcing the total byte
    /// bound. Generic serde deserialization validates the schema but cannot
    /// enforce a source-byte limit; untrusted durable input must use this API
    /// or [`Self::from_json_reader`].
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, RunError> {
        if bytes.len() > MAX_RUN_ENVELOPE_BYTES {
            return Err(RunError::Storage("Run envelope exceeds size limit".into()));
        }
        serde_json::from_slice(bytes).map_err(|error| RunError::Storage(error.to_string()))
    }

    /// Reads at most one byte beyond the durable envelope limit before
    /// decoding, so a streaming source cannot force an unbounded allocation.
    pub fn from_json_reader(reader: impl std::io::Read) -> Result<Self, RunError> {
        use std::io::Read as _;

        let mut bytes = Vec::new();
        reader
            .take(MAX_RUN_ENVELOPE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| RunError::Storage(error.to_string()))?;
        Self::from_json_slice(&bytes)
    }

    pub(crate) fn validate(&self) -> Result<(), RunError> {
        if self.schema_version != RUN_SCHEMA_VERSION {
            return Err(RunError::UnsupportedSchema(self.schema_version));
        }
        self.run.goal.validate()?;
        self.run.capabilities.validate()?;
        self.run.driver.validate()?;
        let active_harness = self
            .run
            .harness
            .active
            .as_ref()
            .ok_or_else(|| RunError::Integrity("Run omitted active Harness snapshot".into()))?;
        active_harness.validate()?;
        for (digest, proposal) in &self.run.harness.proposals {
            proposal.pin.validate()?;
            if digest != &proposal.pin.digest {
                return Err(RunError::Integrity(
                    "Harness proposal identity mismatch".into(),
                ));
            }
        }
        if self.run.wake.reasons.len() > 8
            || self.run.activation_lease.as_ref().is_some_and(|lease| {
                lease.expires_at_ms == 0
                    || lease.reasons.is_empty()
                    || lease.epoch != self.run.controller_epoch
            })
            || ((self.run.status.is_terminal() || self.run.status != RunStatus::Active)
                && (self.run.activation_lease.is_some()
                    || !self.run.wake.reasons.is_empty()
                    || self.run.wake.deadline_ms.is_some()))
        {
            return Err(RunError::Integrity(
                "invalid durable Run residency state".into(),
            ));
        }
        if self.events.len() > MAX_EVENTS
            || self.run.command_receipts.len() > MAX_COMMAND_RECEIPTS
            || self.run.iterations.len() > MAX_ITERATION_SUMMARIES
            || self.run.mailbox.len() > MAX_MESSAGES
            || self.run.steering.len() > MAX_MESSAGES
            || self.run.operations.len() > MAX_OPERATIONS
            || self.run.children.len() > MAX_MESSAGES
            || self.run.required_gates.len() > MAX_LIST_ITEMS
            || self.run.strategy_revisions.len() > MAX_ITERATION_SUMMARIES
            || self.run.workflow_revisions.len() > MAX_ITERATION_SUMMARIES
        {
            return Err(RunError::Integrity(
                "durable Run collection exceeds its schema bound".into(),
            ));
        }
        validate_text_set(
            &self.run.required_gates,
            MAX_LIST_ITEMS,
            256,
            "required gate",
        )?;
        validate_text(
            &self.run.verifier_policy_digest,
            256,
            "verifier policy digest",
            false,
        )?;
        if self.run.usage_unknown.contains(&ResourceDimension::Unknown)
            || self
                .run
                .usage_unknown
                .iter()
                .any(|dimension| resource_value(&self.run.budget, *dimension) != u64::MAX)
        {
            return Err(RunError::Integrity(
                "unknown usage is not permitted by the finite Run budget".into(),
            ));
        }
        if self.run.stage == RunStage::Unknown || self.run.driver == RunDriverSpec::Unknown {
            return Err(RunError::Integrity(
                "unknown durable Run driver or stage requires schema migration".into(),
            ));
        }
        if let Some(iteration) = &self.run.active_iteration {
            iteration.validate()?;
        }
        for iteration in &self.run.iterations {
            iteration.validate()?;
            if iteration.finished_at_ms.is_none() || iteration.result_digest.is_none() {
                return Err(RunError::Integrity(
                    "archived iteration is not durably finished".into(),
                ));
            }
        }
        for (operation_id, operation) in &self.run.operations {
            if operation_id != &operation.id
                || !valid_sha256(&operation.spec_digest)
                || operation.spec_digest != operation.spec.digest()?
                || operation
                    .terminal_result_digest
                    .as_ref()
                    .is_some_and(|digest| !valid_sha256(digest))
            {
                return Err(RunError::Integrity(
                    "unknown or inconsistent durable Run operation".into(),
                ));
            }
            operation.spec.validate()?;
            if let Some(reservation) = &operation.reservation
                && reservation.is_zero()
            {
                return Err(RunError::Integrity(
                    "durable effect has an empty reservation".into(),
                ));
            }
            if matches!(
                &operation.spec,
                EffectSpec::SessionTurn { session, .. }
                    | EffectSpec::RhaiWorkflow { session, .. }
                    if session != &self.run.session
            ) {
                return Err(RunError::Integrity(
                    "durable Run operation references a different Session".into(),
                ));
            }
            if matches!(
                operation.state,
                OperationState::Dispatching
                    | OperationState::Acknowledged
                    | OperationState::Reconciled
                    | OperationState::Uncertain
            ) && operation.reservation.is_none()
            {
                return Err(RunError::Integrity(
                    "claimed durable effect omitted its reservation".into(),
                ));
            }
            if operation.state == OperationState::Abandoned && operation.receipt.is_some() {
                return Err(RunError::Integrity(
                    "abandoned effect retained Applied receipt evidence".into(),
                ));
            }
            if let Some(receipt) = &operation.receipt {
                if receipt
                    .program
                    .as_ref()
                    .is_some_and(|binding| binding.operation_id != *operation_id)
                {
                    return Err(RunError::Integrity(
                        "program receipt operation identity mismatch".into(),
                    ));
                }
                if operation.state == OperationState::Uncertain {
                    // An Applied callback may carry incomplete or unusable
                    // usage evidence. Preserve it only behind the recovery
                    // fence; settlement validation remains strict below.
                    validate_effect_receipt_evidence(receipt)?;
                } else {
                    validate_effect_receipt(&operation.spec, receipt)?;
                }
            }
            if matches!(
                operation.state,
                OperationState::Acknowledged | OperationState::Reconciled
            ) {
                let receipt = operation.receipt.as_ref().ok_or_else(|| {
                    RunError::Integrity("settled operation omitted its receipt".into())
                })?;
                validate_effect_receipt(&operation.spec, receipt)?;
                let reservation = operation.reservation.as_ref().ok_or_else(|| {
                    RunError::Integrity("settled effect omitted its reservation".into())
                })?;
                let actual = receipt.actual_usage.as_ref().ok_or_else(|| {
                    RunError::Integrity("settled effect omitted actual usage".into())
                })?;
                if !actual.allows_settlement_with(reservation, &self.run.budget) {
                    return Err(RunError::Integrity(
                        "settled effect usage exceeds reservation or finite budget".into(),
                    ));
                }
            }
            if operation.state == OperationState::Acknowledged
                && (operation.active_attempt.is_none()
                    || operation.terminal_result_digest.is_none())
            {
                return Err(RunError::Integrity(
                    "acknowledged operation omitted callback identity".into(),
                ));
            }
            if matches!(
                operation.state,
                OperationState::Prepared
                    | OperationState::Dispatching
                    | OperationState::FailedRetryable
                    | OperationState::Uncertain
            ) && self
                .run
                .active_iteration
                .as_ref()
                .is_none_or(|iteration| iteration.iteration_id != operation.iteration_id)
            {
                return Err(RunError::Integrity(
                    "unsettled operation has no active owning iteration".into(),
                ));
            }
        }
        for (child_id, child) in &self.run.children {
            if child_id != &child.id {
                return Err(RunError::Integrity(
                    "durable child map identity is inconsistent".into(),
                ));
            }
            child.validate()?;
        }
        for messages in [&self.run.mailbox, &self.run.steering] {
            for (message_id, message) in messages {
                if message_id != &message.id {
                    return Err(RunError::Integrity(
                        "durable message map identity is inconsistent".into(),
                    ));
                }
                message.validate()?;
            }
        }
        for revision in &self.run.strategy_revisions {
            revision.validate()?;
        }
        for revision in &self.run.workflow_revisions {
            revision.validate()?;
        }
        if self
            .run
            .command_receipts
            .iter()
            .any(|(command_id, receipt)| {
                command_id != &receipt.command_id
                    || !valid_sha256(&receipt.input_digest)
                    || receipt.committed_revision > self.run.revision
                    || receipt.disposition == CommandDisposition::Unknown
            })
            || self.run.recovery_prior_status.is_some_and(|status| {
                matches!(status, RunStatus::Active | RunStatus::RecoveryRequired)
            })
        {
            return Err(RunError::Integrity(
                "unknown or inconsistent durable Run child, message, or receipt state".into(),
            ));
        }
        let mut previous_cursor: Option<RunEventCursor> = None;
        let mut previous_revision: Option<RunRevision> = None;
        for event in &self.events {
            if event.cursor > self.run.event_cursor
                || previous_cursor.is_some_and(|cursor| {
                    cursor
                        .get()
                        .checked_add(1)
                        .is_none_or(|next| event.cursor.get() != next)
                })
                || previous_revision.is_some_and(|revision| {
                    revision
                        .get()
                        .checked_add(1)
                        .is_none_or(|next| event.revision.get() != next)
                })
            {
                return Err(RunError::Integrity(
                    "Run event cursor ordering is inconsistent".into(),
                ));
            }
            previous_cursor = Some(event.cursor);
            previous_revision = Some(event.revision);
        }
        let journal_tail_matches = match self.events.back() {
            Some(event) => {
                self.run.event_cursor.get() != 0
                    && event.cursor == self.run.event_cursor
                    && event.revision == self.run.revision
            }
            None => self.run.event_cursor.get() == 0,
        };
        if !journal_tail_matches {
            return Err(RunError::Integrity(
                "Run event journal is not a complete suffix".into(),
            ));
        }
        if self
            .run
            .driver
            .session()
            .is_some_and(|session| session != &self.run.session)
        {
            return Err(RunError::Integrity(
                "driver session differs from Run session".into(),
            ));
        }
        Ok(())
    }
}

/// Imports only the stable subset of the legacy shell goal snapshot. Active or
/// unknown legacy state is always fenced as `RecoveryRequired`; import never
/// resumes execution or synthesizes Turn evidence.
pub fn migrate_legacy_goal(
    value: &serde_json::Value,
    session: SessionRef,
    now_ms: u64,
) -> Result<RunEnvelope, RunError> {
    let object = value
        .as_object()
        .ok_or_else(|| RunError::Validation("legacy goal snapshot is not an object".into()))?;
    let goal = GoalSpec::new(
        object
            .get("objective")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Recovered legacy goal"),
    );
    goal.validate()?;
    let id = RunId::new(
        object
            .get("goal_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("legacy_goal"),
    )?;
    let raw_status = object
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let parsed = RunStatus::from_wire_str(raw_status);
    let status = if parsed == RunStatus::Active
        || !matches!(
            raw_status,
            "user_paused"
                | "paused"
                | "Paused"
                | "doom_loop_paused"
                | "back_off_paused"
                | "no_progress_paused"
                | "infra_paused"
                | "blocked"
                | "budget_limited"
                | "BudgetLimited"
                | "complete"
                | "Complete"
                | "failed"
                | "cancelled"
        ) {
        RunStatus::RecoveryRequired
    } else {
        parsed
    };
    let budget = ResourceVector::default().tokens(
        object
            .get("token_budget")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    );
    let usage = ResourceVector::default().tokens(
        object
            .get("tokens_used")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    );
    let verdict = object
        .get("verdict")
        .and_then(|value| serde_json::from_value(value.clone()).ok());
    let run = RunRecord {
        revision: RunRevision::new(1),
        controller_epoch: ControllerEpoch::new(1),
        wake: WakeIntent::default(),
        activation_lease: None,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        id,
        session: session.clone(),
        goal,
        driver: RunDriverSpec::AutonomousTurnLoop {
            session,
            strategy_revision: 0,
        },
        status,
        stage: if status == RunStatus::RecoveryRequired {
            RunStage::Recovering
        } else {
            RunStage::Idle
        },
        capabilities: CapabilityPolicy::default(),
        required_gates: BTreeSet::new(),
        verifier_policy_digest: "legacy-import".into(),
        budget,
        usage,
        usage_unknown: BTreeSet::new(),
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
        current_strategy_revision: 0,
        workflow_revisions: Vec::new(),
        current_workflow_revision: None,
        harness: HarnessGovernance {
            active: Some(HarnessSnapshotPin {
                digest: format!("{:064x}", 0),
                descriptor_digest: format!("{:064x}", 0),
                capability_revision: "legacy-import".into(),
                negotiated_capabilities: BTreeSet::new(),
                revision: 1,
                evidence: Vec::new(),
                provenance: "legacy-import".into(),
            }),
            previous: None,
            proposals: BTreeMap::new(),
        },
        verdict,
        pending_approval: false,
        recovery_prior_status: None,
        command_receipts: BTreeMap::new(),
        terminal_report_claimed: false,
        event_cursor: RunEventCursor::new(0),
    };
    Ok(RunEnvelope {
        schema_version: RUN_SCHEMA_VERSION,
        run,
        events: VecDeque::new(),
    })
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandDisposition {
    Applied,
    Rejected,
    #[serde(other)]
    Unknown,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub command_id: CommandId,
    pub input_digest: String,
    pub disposition: CommandDisposition,
    pub committed_revision: RunRevision,
    pub epoch: ControllerEpoch,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunCommandResult {
    pub receipt: CommandReceipt,
    pub snapshot: RunEnvelope,
    pub duplicate: bool,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandOutput<T> {
    pub command: RunCommandResult,
    pub output: T,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallbackResult {
    pub snapshot: RunEnvelope,
    pub duplicate: bool,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationRequest<T> {
    pub run_id: RunId,
    pub expected_revision: RunRevision,
    pub command_id: CommandId,
    pub input: T,
}

impl<T> MutationRequest<T> {
    pub fn new(
        run_id: RunId,
        expected_revision: RunRevision,
        command_id: CommandId,
        input: T,
    ) -> Self {
        Self {
            run_id,
            expected_revision,
            command_id,
            input,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRunRequest {
    pub run_id: Option<RunId>,
    pub command_id: CommandId,
    pub session: SessionRef,
    pub goal: GoalSpec,
    pub driver: RunDriverSpec,
    pub capabilities: CapabilityPolicy,
    pub required_gates: BTreeSet<String>,
    pub verifier_policy_digest: String,
    pub budget: ResourceVector,
    pub harness_snapshot: Option<HarnessSnapshotPin>,
}

impl CreateRunRequest {
    pub fn new(
        command_id: CommandId,
        session: SessionRef,
        goal: GoalSpec,
        driver: RunDriverSpec,
        capabilities: CapabilityPolicy,
        budget: ResourceVector,
    ) -> Self {
        Self {
            run_id: None,
            command_id,
            session,
            goal,
            driver,
            capabilities,
            required_gates: BTreeSet::new(),
            verifier_policy_digest: "default".into(),
            budget,
            harness_snapshot: None,
        }
    }

    pub fn run_id(mut self, value: RunId) -> Self {
        self.run_id = Some(value);
        self
    }

    pub fn required_gates(mut self, values: impl IntoIterator<Item = String>) -> Self {
        self.required_gates = values.into_iter().collect();
        self
    }

    pub fn verifier_policy_digest(mut self, value: impl Into<String>) -> Self {
        self.verifier_policy_digest = value.into();
        self
    }

    pub fn harness_snapshot(mut self, pin: HarnessSnapshotPin) -> Self {
        self.harness_snapshot = Some(pin);
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposeHarness {
    pub pin: HarnessSnapshotPin,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidateHarness {
    pub digest: String,
    pub accepted: bool,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivateHarness {
    pub digest: String,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackHarness {
    pub expected_active_digest: String,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunAction {
    Pause,
    PauseFor { reason: WaitingReason },
    Resume { budget: Option<ResourceVector> },
    Steer { message_id: MessageId, body: String },
    Cancel,
    Approve,
    Reject,
    TryComplete,
    ClaimTerminalReport,
    Tombstone,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginIteration {
    pub context: IterationContextManifest,
}

impl BeginIteration {
    pub fn new(context: IterationContextManifest) -> Self {
        Self { context }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IterationHandle {
    pub run_id: RunId,
    pub iteration_id: IterationId,
    pub token: IterationToken,
    pub epoch: ControllerEpoch,
    pub committed_revision: RunRevision,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishIteration {
    pub run_id: RunId,
    pub epoch: ControllerEpoch,
    pub iteration_id: IterationId,
    pub token: IterationToken,
    pub driver_terminal_success: bool,
    pub summary: String,
    pub evidence: Vec<ArtifactRef>,
    pub gates: BTreeMap<String, bool>,
    pub verdict: GoalVerdict,
    pub usage: ResourceVector,
}

impl FinishIteration {
    pub fn new(
        handle: &IterationHandle,
        driver_terminal_success: bool,
        summary: impl Into<String>,
        verdict: GoalVerdict,
        usage: ResourceVector,
    ) -> Self {
        Self {
            run_id: handle.run_id.clone(),
            epoch: handle.epoch,
            iteration_id: handle.iteration_id,
            token: handle.token.clone(),
            driver_terminal_success,
            summary: summary.into(),
            evidence: Vec::new(),
            gates: BTreeMap::new(),
            verdict,
            usage,
        }
    }

    pub fn evidence(mut self, values: impl IntoIterator<Item = ArtifactRef>) -> Self {
        self.evidence = values.into_iter().collect();
        self
    }

    pub fn gates(mut self, values: impl IntoIterator<Item = (String, bool)>) -> Self {
        self.gates = values.into_iter().collect();
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareOperation {
    pub operation_id: OperationId,
    pub iteration_id: IterationId,
    pub effect_class: EffectClass,
    pub spec: EffectSpec,
}

impl PrepareOperation {
    pub fn new(
        operation_id: OperationId,
        iteration_id: IterationId,
        effect_class: EffectClass,
        spec: EffectSpec,
    ) -> Self {
        Self {
            operation_id,
            iteration_id,
            effect_class,
            spec,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimEffect {
    pub operation_id: OperationId,
    pub reservation: ResourceVector,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<ActivationFence>,
}

impl ClaimEffect {
    pub fn new(operation_id: OperationId) -> Self {
        Self {
            operation_id,
            reservation: ResourceVector::default(),
            activation: None,
        }
    }

    pub fn reservation(mut self, reservation: ResourceVector) -> Self {
        self.reservation = reservation;
        self
    }

    pub fn activation(mut self, activation: ActivationFence) -> Self {
        self.activation = Some(activation);
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileEffect {
    pub operation_id: OperationId,
    pub decision: ReconcileDecision,
}

impl ReconcileEffect {
    pub fn new(operation_id: OperationId, decision: ReconcileDecision) -> Self {
        Self {
            operation_id,
            decision,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmitChild {
    pub child_id: ChildId,
    pub iteration_id: IterationId,
    pub reservation: ResourceVector,
    pub workspace_isolation: String,
    pub completion_policy: ChildCompletionPolicy,
}

impl AdmitChild {
    pub fn new(
        child_id: ChildId,
        iteration_id: IterationId,
        reservation: ResourceVector,
        workspace_isolation: impl Into<String>,
        completion_policy: ChildCompletionPolicy,
    ) -> Self {
        Self {
            child_id,
            iteration_id,
            reservation,
            workspace_isolation: workspace_isolation.into(),
            completion_policy,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptMessage {
    pub message_id: MessageId,
    pub causation_id: Option<MessageId>,
    pub sender: String,
    pub trust_label: String,
    pub body: String,
}

impl AcceptMessage {
    pub fn new(
        message_id: MessageId,
        sender: impl Into<String>,
        trust_label: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            message_id,
            causation_id: None,
            sender: sender.into(),
            trust_label: trust_label.into(),
            body: body.into(),
        }
    }

    pub fn causation_id(mut self, value: MessageId) -> Self {
        self.causation_id = Some(value);
        self
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionMessage {
    pub message_id: MessageId,
    pub state: MessageState,
}

impl TransitionMessage {
    pub fn new(message_id: MessageId, state: MessageState) -> Self {
        Self { message_id, state }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposeStrategy {
    pub digest: String,
    pub provenance: String,
    pub promotion_proposal: Option<String>,
}

impl ProposeStrategy {
    pub fn new(digest: impl Into<String>, provenance: impl Into<String>) -> Self {
        Self {
            digest: digest.into(),
            provenance: provenance.into(),
            promotion_proposal: None,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyStrategy {
    pub revision: u64,
}

impl ApplyStrategy {
    pub fn new(revision: u64) -> Self {
        Self { revision }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposeWorkflow {
    pub source_digest: String,
    pub provenance: String,
    pub promotion_proposal: Option<String>,
}

impl ProposeWorkflow {
    pub fn new(source_digest: impl Into<String>, provenance: impl Into<String>) -> Self {
        Self {
            source_digest: source_digest.into(),
            provenance: provenance.into(),
            promotion_proposal: None,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidateWorkflow {
    pub revision: u64,
    pub compiled: bool,
    pub static_policy_valid: bool,
    pub dry_run_valid: bool,
}

impl ValidateWorkflow {
    pub fn new(
        revision: u64,
        compiled: bool,
        static_policy_valid: bool,
        dry_run_valid: bool,
    ) -> Self {
        Self {
            revision,
            compiled,
            static_policy_valid,
            dry_run_valid,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetWorkflowRevision {
    pub revision: u64,
}

impl SetWorkflowRevision {
    pub fn new(revision: u64) -> Self {
        Self { revision }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryResolution {
    pub resume: bool,
    pub abandon_active_iteration: bool,
}

impl RecoveryResolution {
    pub fn new(resume: bool, abandon_active_iteration: bool) -> Self {
        Self {
            resume,
            abandon_active_iteration,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryNeed {
    SessionTurnLedger {
        operation_id: OperationId,
        session: SessionRef,
        turn_id: String,
        prompt_digest: String,
    },
    EffectReconciliation {
        operation_id: OperationId,
        effect_class: EffectClass,
    },
    ActiveIteration {
        iteration_id: IterationId,
    },
    ActiveChild {
        child_id: ChildId,
    },
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub snapshot: RunEnvelope,
    pub needs: Vec<RecoveryNeed>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunAttach {
    Replay {
        run_id: RunId,
        through: RunEventCursor,
        events: Vec<RunEvent>,
    },
    Snapshot(Box<RunEnvelope>),
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunError {
    Validation(String),
    NotFound,
    Conflict {
        expected: Option<RunRevision>,
        actual: Option<RunRevision>,
    },
    StaleEpoch,
    StaleCallback,
    Storage(String),
    CommitUnknown(String),
    AuthorityLost,
    ReloadRequired,
    Capability(String),
    Budget,
    InvalidTransition(String),
    Integrity(String),
    DedupCapacity,
    UnsupportedSchema(u32),
}

impl fmt::Display for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "invalid Run input: {message}"),
            Self::NotFound => formatter.write_str("Run not found"),
            Self::Conflict { expected, actual } => {
                write!(
                    formatter,
                    "Run revision conflict: expected {expected:?}, actual {actual:?}"
                )
            }
            Self::StaleEpoch => formatter.write_str("stale Run controller epoch"),
            Self::StaleCallback => formatter.write_str("stale Run callback identity"),
            Self::Storage(message) => write!(formatter, "Run storage failed: {message}"),
            Self::CommitUnknown(message) => {
                write!(formatter, "Run commit outcome is unknown: {message}")
            }
            Self::AuthorityLost => formatter.write_str("Run controller authority was lost"),
            Self::ReloadRequired => {
                formatter.write_str("Run must be reloaded from durable storage before recovery")
            }
            Self::Capability(message) => write!(formatter, "Run capability denied: {message}"),
            Self::Budget => formatter.write_str("Run budget exceeded"),
            Self::InvalidTransition(message) => {
                write!(formatter, "invalid Run transition: {message}")
            }
            Self::Integrity(message) => write!(formatter, "Run integrity check failed: {message}"),
            Self::DedupCapacity => formatter.write_str("Run durable de-dup capacity reached"),
            Self::UnsupportedSchema(version) => {
                write!(formatter, "unsupported Run schema version {version}")
            }
        }
    }
}

impl std::error::Error for RunError {}

pub(crate) fn canonical_digest<T: Serialize>(value: &T) -> Result<String, RunError> {
    let mut value =
        serde_json::to_value(value).map_err(|error| RunError::Integrity(error.to_string()))?;
    canonicalize_json(&mut value);
    let bytes =
        serde_json::to_vec(&value).map_err(|error| RunError::Integrity(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                canonicalize_json(value);
            }
            let old = std::mem::take(values);
            let mut entries: Vec<_> = old.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            values.extend(entries);
        }
        _ => {}
    }
}
