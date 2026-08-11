// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Send/Sync, fail-closed in-process façade for the bundled Grok Build fork.
//! The public API is a typed SDK; protocol adapters used by the shell remain
//! private implementation details.

mod activation;
mod artifact;
mod autonomous;
mod conversation;
mod evidence;
mod harness;
mod prime;
mod private;
mod program;
mod session_state;

pub use activation::{
    ACTIVATION_STORE_SCHEMA_MARKER, ACTIVATION_STORE_SCHEMA_VERSION, ActivationClaim,
    ActivationClaimRequest, ActivationCoordinator, ActivationDisposition, ActivationError,
    ActivationFencingToken, ActivationGrant, ActivationHandle, ActivationItemId,
    ActivationItemState, ActivationLeaseState, ActivationRenewal, ActivationSettlement,
    ActivationWake, ActivationWakeOutcome, ActivationWorkerId, LocalActivationCoordinator,
    MAX_ACTIVATION_CLAIM_BATCH, MAX_ACTIVATION_ITEM_ID_BYTES, MAX_ACTIVATION_LEASE_MS,
    MAX_ACTIVATION_PAYLOAD_BYTES, MAX_ACTIVATION_WORKER_ID_BYTES,
    run_activation_coordinator_conformance,
};
pub use artifact::{
    ARTIFACT_VAULT_SCHEMA_MARKER, ARTIFACT_VAULT_SCHEMA_VERSION, ArtifactDamage, ArtifactDigest,
    ArtifactError, ArtifactHandle, ArtifactId, ArtifactIntegrity, ArtifactLabel,
    ArtifactMaterialization, ArtifactMediaType, ArtifactObservation, ArtifactProvenance,
    ArtifactProvenanceKind, ArtifactPut, ArtifactReceipt, ArtifactRecord, ArtifactRecovery,
    ArtifactRetention, ArtifactUsage, ArtifactVault, ArtifactVaultHarness, ArtifactWrite,
    LocalArtifactVault, MAX_ARTIFACT_BYTES, MAX_ARTIFACT_LABEL_BYTES,
    MAX_ARTIFACT_MEDIA_TYPE_BYTES, MAX_ARTIFACT_OBSERVATION_INPUTS, materialize_verified_copy,
    run_artifact_vault_conformance,
};
pub use autonomous::{AutonomousActivation, AutonomousActivationResult, AutonomousTurnLoop};
pub use conversation::{
    CONVERSATION_TOOL_SERVER, ConversationAcceptance, ConversationCreate, ConversationDelegate,
    ConversationDelivery, ConversationDigest, ConversationError, ConversationId,
    ConversationIdentity, ConversationLabel, ConversationRead, ConversationSend, ConversationTitle,
    ConversationToolCall, ConversationToolDescriptor, ConversationToolOutcome, DigestFocus,
    IdempotencyKey, InputSource, MAX_CONVERSATION_DIGEST_BYTES, MAX_CONVERSATION_ID_BYTES,
    MAX_CONVERSATION_LABEL_BYTES, MAX_CONVERSATION_TITLE_BYTES, MAX_DIGEST_FOCUS_BYTES,
    MAX_IDEMPOTENCY_KEY_BYTES, MAX_PEER_MESSAGE_BYTES, MAX_PROJECT_NAME_BYTES, PeerMessage,
    ProjectName, conversation_tool_descriptors,
};
pub use evidence::{
    LocalSessionEvidenceStore, MAX_SESSION_EVIDENCE_BYTES, SESSION_EVIDENCE_SCHEMA_MARKER,
    SESSION_EVIDENCE_SCHEMA_VERSION, SessionEvidenceCommit, SessionEvidenceDocument,
    SessionEvidenceKey, SessionEvidenceKind, SessionEvidenceStore, SessionEvidenceStoreError,
    SessionEvidenceVersion, run_session_evidence_conformance,
};
pub use harness::{
    CompleteEventCursor, HARNESS_SNAPSHOT_SCHEMA_VERSION, HARNESS_STORE_SCHEMA_MARKER,
    HARNESS_STORE_SCHEMA_VERSION, HarnessContent, HarnessDigest, HarnessError, HarnessEvidenceKind,
    HarnessEvidenceRef, HarnessPut, HarnessRefinement, HarnessRefinementPatch, HarnessSnapshot,
    HarnessStore, HarnessStoreError, LocalHarnessStore, MAX_HARNESS_EVIDENCE_REFS,
    MAX_HARNESS_SNAPSHOT_BYTES, MAX_TURN_BINDING_RECORD_BYTES, MaterializedHarness, SdkProvenance,
    TURN_BINDING_RECORD_SCHEMA_VERSION, TurnBindingKey, TurnBindingReceipt, TurnBindingRecord,
    TurnBindingStatus, harness_put_reconciled, run_harness_store_conformance,
};
pub use prime::*;
pub use program::{
    ArtifactVaultOutputSink, CONFORMANCE_CREDENTIAL_HANDLE, CONFORMANCE_CREDENTIAL_VARIABLE,
    CONFORMANCE_EXIT_CODE, CONFORMANCE_FLOOD_BYTES, CONFORMANCE_SECRET, CONFORMANCE_STDERR_MARK,
    CONFORMANCE_STDOUT_MARK, CaptureRecord, CredentialHandleName, CredentialResolver,
    EnvironmentBinding, ExecutionId, ExecutionReceipt, ExecutionStatus, ExitDisposition, Liveness,
    LivenessProbe, LocalProgramRuntime, MAX_PROGRAM_ARGUMENT_BYTES, MAX_PROGRAM_ARGUMENTS,
    MAX_PROGRAM_CAPTURE_BYTES, MAX_PROGRAM_CREDENTIAL_BINDINGS, MAX_PROGRAM_DEADLINE_MS,
    MAX_PROGRAM_ENVIRONMENT_ENTRIES, MAX_PROGRAM_ENVIRONMENT_NAME_BYTES,
    MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES, MAX_PROGRAM_EXECUTION_ID_BYTES, MAX_PROGRAM_LABEL_BYTES,
    MAX_PROGRAM_PATH_BYTES, OsLivenessProbe, PROGRAM_RUNTIME_SCHEMA_MARKER,
    PROGRAM_RUNTIME_SCHEMA_VERSION, ProcessIdentity, ProgramBounds, ProgramError, ProgramLabel,
    ProgramLaunch, ProgramOutputSink, ProgramPath, ProgramRuntime, ProgramRuntimeHarness,
    ProgramScript, ProgramStream, ReconcileOutcome, ResolvedCredential,
    run_program_runtime_conformance,
};
pub use session_state::{
    ConformanceOpen, DeleteReceipt, LiveSessionDocument, LocalSessionStateStore,
    MAX_SESSION_GENERATION_BYTES, MAX_SESSION_IDENTITY_BYTES, MAX_SESSION_MANIFEST_BYTES,
    MAX_SESSION_OBJECT_BYTES, ManifestCas, ManifestVersion, ObjectPut, PreparedManifestCas,
    PreparedSessionDelete, RewindKind, SESSION_LOG_SCHEMA_MARKER, SESSION_LOG_SCHEMA_VERSION,
    SessionDelete, SessionGeneration, SessionKey, SessionManifest, SessionObject, SessionObjectId,
    SessionObjectKind, SessionSlot, SessionStateFault, SessionStateFaultHarness,
    SessionStateFaultMetrics, SessionStateLease, SessionStateStore, SessionStateStoreError,
    TARGET_TRANSCRIPT_SEGMENT_BYTES, delete_reconciled, manifest_cas_reconciled,
    object_put_reconciled, run_session_state_conformance, run_session_state_fault_conformance,
};

pub(crate) use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
pub(crate) use tokio::sync::mpsc;

/// Typed MCP 2026 client-role services. SDK users implement these traits
/// directly; no ACP service or raw shell protocol is exposed.
pub use xai_grok_mcp::servers::{
    McpElicitationService, McpHostContext, McpHostServiceError, McpHostServices, McpRootsService,
    McpSamplingService,
};

/// Authoritative data models used by the typed MCP host-service traits.
/// Transport and service-loop internals intentionally remain private.
pub mod mcp_model {
    #[allow(deprecated)]
    pub use xai_grok_mcp::rmcp::model::{
        CreateMessageRequestParams, CreateMessageResult, ElicitRequestParams, ElicitResult,
        ElicitationAction, ListRootsResult, Root,
    };
}

/// Durable Run domain and provider contracts. Run IDs, Run event cursors and
/// revisions are deliberately distinct from [`SessionId`] and session event
/// sequences; hosts cannot accidentally cross the two namespaces.
pub mod run {
    pub use xai_agent_lifecycle::run::{
        AcceptMessage, ActivateHarness, ActivationFence, ActivationLease, ActivationToken,
        AdmitChild, ApprovalDecision, ApprovalHandler, ApprovalRequest, ArtifactMetadata,
        ArtifactRef, ArtifactStore, BeginIteration, CURRENT_RUN_SCHEMA, CallbackResult,
        CapabilityPolicy, ChildCallback, ChildCompletionPolicy, ChildId, ChildRun, ClaimActivation,
        ClaimEffect, CommandId, CommandOutput, CommittedEffect, CompactionCheckpoint,
        ControllerEpoch, CreateRunRequest, DenyApprovalHandler, EffectCallback, EffectClass,
        EffectOutcome, EffectReceipt, EffectSpec, EffectUsage, FailClosedGateProvider,
        FailClosedGoalVerifier, FinishIteration, FinishedOutcome, GateEvaluation, GateProvider,
        GateRequest, GoalSpec, GoalVerdict, GoalVerification, GoalVerificationRequest,
        GoalVerifier, HarnessGovernance, HarnessProposal, HarnessProposalState, HarnessSnapshotPin,
        IterationContextManifest, IterationId, LocalArtifactStore, LocalRunStore,
        MAX_RUN_ENVELOPE_BYTES, MailMessage, MessageId, MessageState, MutationRequest,
        NoopTelemetrySink, OperationId, OperationState, PrepareOperation, PreparedIteration,
        PreparedStoreCommit, ProgramReceiptBinding, ProposeHarness, ProviderSet, RUN_SCHEMA_MARKER,
        RUN_SCHEMA_VERSION, ReconcileDecision, ReconcileEffect, RecoveryNeed, RecoveryPlan,
        RecoveryResolution, ResidencyInspection, ResourceDimension, ResourceVector,
        RollbackHarness, RunAction, RunAttach, RunCommandResult, RunDriverSpec, RunEnvelope,
        RunError, RunEvent, RunEventCursor, RunEventKind, RunId, RunLifecycle, RunRevision,
        RunSchemaMarker, RunStage, RunStatus, RunStore, SessionRef, SessionTurnOutcome,
        SkillDescriptorPin, StoreCommit, StoreCommitResult, StoreConformanceOpen, TelemetryRecord,
        TelemetrySink, TransitionMessage, ValidateHarness, WaitingReason, WakeIntent, WakeReason,
        WakeRequest, WorkerId, migrate_legacy_goal, run_artifact_store_conformance,
        run_run_store_conformance,
    };
}

mod capability;
mod config;
mod hooks;
mod mcp;
mod provenance;
mod runtime;
mod session;

pub use capability::{
    AgentServiceContribution, CapabilityBinding, CapabilityError, CapabilityKind, CapabilityLayer,
    CapabilityOrigin, CapabilityResolution, MAX_CAPABILITY_LAYER_ENTRIES,
    MAX_CAPABILITY_NAME_BYTES, MaskedCapability, SkillContribution,
};
pub use config::*;
pub use hooks::*;
pub use mcp::*;
pub use provenance::*;
pub use runtime::*;
pub use session::*;

pub(crate) use capability::{ResolvedCapabilities, resolve_capabilities};
pub(crate) use conversation::{
    conversation_tool_server, dispatch_create, dispatch_read, dispatch_send, dispatch_tool_call,
};
pub(crate) use mcp::InProcessMcpOutbound;
pub(crate) use mcp::parsing::{parse_mcp_servers, parse_task_status};
pub(crate) use provenance::canonicalize_json;
pub(crate) use session::{durable_turn_outcome, measured_session_turn_usage};

#[cfg(test)]
mod tests;
