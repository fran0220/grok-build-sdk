// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Send/Sync, fail-closed in-process façade for the bundled Grok Build fork.
//! The public API is a typed SDK; protocol adapters used by the shell remain
//! private implementation details.

mod autonomous;
mod private;

pub use autonomous::{AutonomousActivation, AutonomousActivationResult, AutonomousTurnLoop};

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::mpsc;

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
        ApprovalDecision, ApprovalHandler, ApprovalRequest, ArtifactMetadata, ArtifactRef,
        ArtifactStore, CapabilityPolicy, CommandId, ControllerEpoch, CreateRunRequest,
        DenyApprovalHandler, EffectClass, EffectReceipt, EffectUsage, FailClosedGateProvider,
        FailClosedGoalVerifier, FinishedOutcome, GateEvaluation, GateProvider, GateRequest,
        GoalSpec, GoalVerdict, GoalVerification, GoalVerificationRequest, GoalVerifier,
        IterationId, LocalArtifactStore, LocalRunStore, MAX_RUN_ENVELOPE_BYTES, MessageId,
        MutationRequest, NoopTelemetrySink, OperationId, OperationState, ProviderSet,
        RUN_SCHEMA_VERSION, ReconcileDecision, RecoveryNeed, RecoveryPlan, RecoveryResolution,
        ResourceDimension, ResourceVector, RunAction, RunAttach, RunCommandResult, RunDriverSpec,
        RunEnvelope, RunError, RunEvent, RunEventCursor, RunEventKind, RunId, RunLifecycle,
        RunRevision, RunStage, RunStatus, RunStore, SessionRef, SessionTurnOutcome, StoreCommit,
        StoreCommitResult, TelemetryRecord, TelemetrySink, WaitingReason, migrate_legacy_goal,
    };
    #[cfg(test)]
    pub use xai_agent_lifecycle::run::{
        BeginIteration, ClaimEffect, EffectSpec, FinishIteration, IterationContextManifest,
        PrepareOperation,
    };
}

/// Hook events supported by the bundled agent. This SDK type deliberately does
/// not expose the shell's hook implementation types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHookEvent {
    SessionStart,
    SessionEnd,
    Stop,
    StopFailure,
    PreToolUse,
    PostToolUse,
    PostToolUseFailure,
    PermissionDenied,
    UserPromptSubmit,
    Notification,
    SubagentStart,
    SubagentStop,
    SubagentEnd,
    PreCompact,
    PostCompact,
}
impl AgentHookEvent {
    fn registration_name(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::SessionEnd => "SessionEnd",
            Self::Stop => "Stop",
            Self::StopFailure => "StopFailure",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::PostToolUseFailure => "PostToolUseFailure",
            Self::PermissionDenied => "PermissionDenied",
            Self::UserPromptSubmit => "UserPromptSubmit",
            Self::Notification => "Notification",
            Self::SubagentStart => "SubagentStart",
            Self::SubagentStop => "SubagentStop",
            Self::SubagentEnd => "SubagentEnd",
            Self::PreCompact => "PreCompact",
            Self::PostCompact => "PostCompact",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentHookInvocation {
    pub event: AgentHookEvent,
    pub callback_id: String,
    pub session_id: String,
    pub cwd: Option<PathBuf>,
    pub workspace_root: Option<PathBuf>,
    pub timestamp: Option<String>,
    pub prompt_id: Option<String>,
    pub permission_mode: Option<String>,
    /// Present for tool events.
    pub tool_name: Option<String>,
    pub tool_use_id: Option<String>,
    pub tool_input: Option<serde_json::Value>,
    pub tool_result: Option<serde_json::Value>,
    /// Complete reverse-channel payload, including fields added by newer agents.
    pub raw: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHookDecision {
    #[default]
    Continue,
    Deny,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentHookResponse {
    pub decision: AgentHookDecision,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_message: Option<String>,
    #[serde(rename = "continue", skip_serializing_if = "Option::is_none")]
    pub continue_: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_context: Option<String>,
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("agent hook failed: {message}")]
pub struct AgentHookError {
    pub message: String,
}

#[async_trait::async_trait]
pub trait AgentHookHandler: Send + Sync + 'static {
    async fn handle(
        &self,
        invocation: AgentHookInvocation,
    ) -> Result<AgentHookResponse, AgentHookError>;
}

#[derive(Clone)]
pub struct AgentHookRegistration {
    pub callback_id: String,
    pub event: AgentHookEvent,
    pub matcher: Option<String>,
    /// Shell wire timeout in seconds. Must be finite, positive, and at most 600.
    pub timeout: Option<f64>,
    pub handler: Arc<dyn AgentHookHandler>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RuntimeProfile {
    #[default]
    Restricted,
    Desktop,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HostCapabilities {
    pub fs_read: bool,
    pub fs_write: bool,
    pub terminal: bool,
    #[serde(default)]
    pub extension_methods: Vec<String>,
    #[serde(default)]
    pub meta: serde_json::Value,
}

/// Explicit credentials and routing for an API-compatible model provider.
/// Values are never read from environment variables by the runtime. API keys
/// are intentionally omitted from both `Debug` and `Serialize`; hosts may
/// deserialize configuration but cannot accidentally export the secret bag.
#[derive(Clone, Default, PartialEq, serde::Deserialize)]
pub struct ApiProviderConfig {
    pub base_url: String,
    pub api_key: String,
    /// Optional model slug sent to this provider. Defaults to the catalog ID.
    pub model: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Query parameters appended to every inference request for this model.
    #[serde(default)]
    pub query_params: BTreeMap<String, String>,
}

/// Explicit credentials and routing for the Imagine-compatible media API.
/// Model slugs are operation-specific fields on [`MediaServiceConfig`].
#[derive(Clone, Default, PartialEq, serde::Deserialize)]
pub struct MediaProviderConfig {
    pub base_url: String,
    pub api_key: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Query parameters appended to image, edit, video-start, and video-poll
    /// requests made through this provider.
    #[serde(default)]
    pub query_params: BTreeMap<String, String>,
}

/// Optional model routing for built-in subagents and auxiliary model calls.
/// Every referenced model must exist in [`RuntimeConfig::models`], which also
/// supplies that model's backend and context-window contract.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentServiceConfig {
    #[serde(default)]
    pub subagent_models: BTreeMap<String, String>,
    pub web_search_model: Option<String>,
    pub session_summary_model: Option<String>,
    pub image_description_model: Option<String>,
    pub prompt_suggestion_model: Option<String>,
}

/// Explicit provider for Grok's native image and video generation tools.
/// A host can advertise each tool independently and route all four operations
/// to custom API-compatible model slugs.
#[derive(Clone, PartialEq, serde::Deserialize)]
pub struct MediaServiceConfig {
    pub provider: MediaProviderConfig,
    pub image_generation: bool,
    pub image_edit: bool,
    pub video_generation: bool,
    pub image_generation_model: Option<String>,
    pub image_edit_model: Option<String>,
    pub image_to_video_model: Option<String>,
    pub reference_to_video_model: Option<String>,
}

/// Host-supplied MCP transport. HTTP/SSE headers and stdio environment values
/// can contain secrets, so this type deliberately does not implement `Debug`.
#[derive(Clone, PartialEq, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum McpServerConfig {
    Stdio {
        name: String,
        command: PathBuf,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
    },
    Http {
        name: String,
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    Sse {
        name: String,
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransportKind {
    Stdio,
    Http,
    Sse,
    ManagedGateway,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpServerSource {
    Local,
    Managed,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpServerStatus {
    Ready,
    Initializing,
    SetupRequired,
    Unavailable,
    NeedsAuth,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpToolInfo {
    pub server: String,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
    #[serde(default)]
    pub meta: serde_json::Value,
}
/// Redacted MCP catalog entry. Transport credentials, URLs, commands and
/// arguments are deliberately absent.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpServerSummary {
    pub name: String,
    pub display_name: Option<String>,
    pub source: McpServerSource,
    pub transport: McpTransportKind,
    pub enabled: bool,
    pub status: Option<McpServerStatus>,
    pub auth_required: bool,
    pub setup_required: bool,
    pub tools: Vec<McpToolInfo>,
    pub negotiated: Option<McpNegotiatedCapabilities>,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpNegotiatedCapabilities {
    /// Capabilities advertised by the server for the selected protocol
    /// version. A `true` value does not imply that the SDK exposes or
    /// authorizes the corresponding server-to-client role.
    pub protocol_version: String,
    pub tools: bool,
    pub resources: bool,
    pub prompts: bool,
    pub completions: bool,
    pub logging: bool,
    pub tool_list_changed: bool,
    pub resource_list_changed: bool,
    /// The server exposes at least one notification category usable through
    /// MCP 2026 `subscriptions/listen`.
    pub subscriptions: bool,
    /// Legacy wire metadata only. The SDK does not call
    /// `resources/subscribe`; use [`Runtime::listen_mcp`].
    pub legacy_resource_subscribe: bool,
    pub prompt_list_changed: bool,
    pub tasks: bool,
    #[serde(default)]
    pub extensions: BTreeMap<String, serde_json::Value>,
    /// Lossless negotiated capability object for extension-specific settings.
    pub raw: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum McpContent {
    Text {
        text: String,
        /// Lossless original protocol block, including annotations and metadata.
        raw: serde_json::Value,
    },
    Image {
        data: String,
        mime_type: String,
        /// Lossless original protocol block, including annotations and metadata.
        raw: serde_json::Value,
    },
    Audio {
        data: String,
        mime_type: String,
        /// Lossless original protocol block, including annotations and metadata.
        raw: serde_json::Value,
    },
    EmbeddedResource {
        resource: serde_json::Value,
        /// Lossless original protocol block, including annotations and metadata.
        raw: serde_json::Value,
    },
    ResourceLink {
        uri: String,
        name: String,
        #[serde(default)]
        mime_type: Option<String>,
        /// Lossless original protocol block, including annotations and metadata.
        raw: serde_json::Value,
    },
    Unknown {
        raw: serde_json::Value,
    },
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpToolResult {
    pub content: Vec<McpContent>,
    pub structured_content: Option<serde_json::Value>,
    pub is_error: Option<bool>,
    pub meta: Option<serde_json::Value>,
}

/// Responses for one MCP multi-round-trip (MRTR) retry, keyed by the
/// server-assigned request ID. Values must be valid results for the matching
/// roots, sampling, or elicitation request.
pub type McpInputResponses = BTreeMap<String, serde_json::Value>;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpInputRequestKind {
    Sampling,
    Elicitation,
    Roots,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpInputRequest {
    pub id: String,
    pub kind: McpInputRequestKind,
    /// Lossless MCP request envelope. Unknown request methods are rejected
    /// before this value reaches the SDK.
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpInputRequired {
    pub requests: Vec<McpInputRequest>,
    /// Opaque server state. Return it unchanged in [`McpContinuation`].
    pub request_state: Option<String>,
    pub raw: serde_json::Value,
    #[serde(skip)]
    continuation_identity: Option<McpContinuationIdentity>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct McpContinuation {
    input_responses: McpInputResponses,
    request_state: Option<String>,
    identity: McpContinuationIdentity,
}

#[derive(Clone, Debug, PartialEq)]
struct McpContinuationIdentity {
    session_id: SessionId,
    server: String,
    client_id: u64,
    operation: McpOperationIdentity,
    request_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum McpOperationIdentity {
    Tool {
        name: String,
        arguments: serde_json::Value,
    },
    Prompt {
        name: String,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
    },
    Resource {
        uri: String,
    },
}

impl McpInputRequired {
    /// Builds the only continuation accepted by the SDK for this exact MRTR
    /// round. Every advertised request ID must be answered exactly once.
    pub fn respond(&self, input_responses: McpInputResponses) -> Result<McpContinuation, Error> {
        let identity = self.continuation_identity.clone().ok_or_else(|| {
            Error::Operation(
                "this MCP input requirement belongs to a Task; use update_mcp_task".into(),
            )
        })?;
        let supplied: Vec<_> = input_responses.keys().cloned().collect();
        let mut requested = identity.request_ids.clone();
        requested.sort();
        if supplied != requested {
            return Err(Error::InvalidConfig(
                "MCP continuation responses must exactly match the requested input IDs".into(),
            ));
        }
        Ok(McpContinuation {
            input_responses,
            request_state: self.request_state.clone(),
            identity,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTaskStatus {
    Working,
    InputRequired,
    Completed,
    Failed,
    Cancelled,
}

/// A Task handle is valid only for the exact session, server and MCP client
/// generation that created it. Reconnect or server replacement makes it stale.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct McpTaskHandle {
    pub session_id: SessionId,
    pub server: String,
    pub client_id: u64,
    pub task_id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpTask {
    pub handle: McpTaskHandle,
    pub status: McpTaskStatus,
    pub status_message: Option<String>,
    pub created_at: String,
    pub last_updated_at: String,
    pub ttl_ms: Option<u64>,
    pub poll_interval_ms: Option<u64>,
    pub input_required: Option<McpInputRequired>,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum McpOperationOutcome<T> {
    Complete {
        client_id: u64,
        result: T,
    },
    InputRequired {
        client_id: u64,
        input: McpInputRequired,
    },
    Task {
        handle: McpTaskHandle,
        task: McpTask,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpSubscriptionFilter {
    #[serde(default)]
    pub tools_list_changed: bool,
    #[serde(default)]
    pub prompts_list_changed: bool,
    #[serde(default)]
    pub resources_list_changed: bool,
    #[serde(default)]
    pub resource_subscriptions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum McpSubscriptionEvent {
    ToolsListChanged,
    PromptsListChanged,
    ResourcesListChanged,
    ResourceUpdated { uri: String },
    Ended(McpSubscriptionEnd),
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum McpSubscriptionEnd {
    Graceful,
    Abrupt,
    Cancelled,
    Lagged { capacity: usize },
    Error { message: String },
}

/// Bounded MCP 2026 `subscriptions/listen` stream. Streams are bound to one
/// concrete client generation and are not resumed across reconnects.
pub struct McpSubscription {
    pub session_id: SessionId,
    pub server: String,
    pub client_id: u64,
    pub acknowledged: McpSubscriptionFilter,
    events: tokio::sync::mpsc::Receiver<serde_json::Value>,
    terminal: tokio::sync::oneshot::Receiver<serde_json::Value>,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
    pending_end: Option<McpSubscriptionEnd>,
    ended: bool,
}

impl McpSubscription {
    pub async fn next(&mut self) -> Result<Option<McpSubscriptionEvent>, Error> {
        if let Some(end) = self.pending_end.take() {
            self.ended = true;
            return Ok(Some(McpSubscriptionEvent::Ended(end)));
        }
        if self.ended {
            return Ok(None);
        }
        match self.terminal.try_recv() {
            Ok(terminal) => {
                self.ended = true;
                self.cancel.take();
                return parse_mcp_subscription_end(Some(terminal));
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                self.ended = true;
                self.cancel.take();
                return parse_mcp_subscription_end(None);
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
        }
        let value = match self.events.try_recv() {
            Ok(value) => value,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                self.ended = true;
                self.cancel.take();
                return parse_mcp_subscription_end((&mut self.terminal).await.ok());
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                tokio::select! {
                    biased;
                    terminal = &mut self.terminal => {
                        self.ended = true;
                        self.cancel.take();
                        return parse_mcp_subscription_end(terminal.ok());
                    }
                    event = self.events.recv() => {
                        let Some(event) = event else {
                            self.ended = true;
                            return Ok(Some(McpSubscriptionEvent::Ended(
                                McpSubscriptionEnd::Abrupt,
                            )));
                        };
                        event
                    }
                }
            }
        };
        match value["type"].as_str() {
            Some("notification") => {
                let notification = value.get("notification").ok_or_else(|| {
                    Error::Operation("MCP subscription event omitted payload".into())
                })?;
                match notification["method"].as_str() {
                    Some("notifications/tools/list_changed") => {
                        Ok(Some(McpSubscriptionEvent::ToolsListChanged))
                    }
                    Some("notifications/prompts/list_changed") => {
                        Ok(Some(McpSubscriptionEvent::PromptsListChanged))
                    }
                    Some("notifications/resources/list_changed") => {
                        Ok(Some(McpSubscriptionEvent::ResourcesListChanged))
                    }
                    Some("notifications/resources/updated") => {
                        let uri = notification["params"]["uri"]
                            .as_str()
                            .ok_or_else(|| {
                                Error::Operation(
                                    "MCP resource update omitted its resource URI".into(),
                                )
                            })?
                            .to_owned();
                        Ok(Some(McpSubscriptionEvent::ResourceUpdated { uri }))
                    }
                    Some(method) => Err(Error::Operation(format!(
                        "unsupported MCP subscription notification '{method}'"
                    ))),
                    None => Err(Error::Operation(
                        "MCP subscription notification omitted its method".into(),
                    )),
                }
            }
            _ => Err(Error::Operation("invalid MCP subscription event".into())),
        }
    }

    pub fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
            self.pending_end = Some(McpSubscriptionEnd::Cancelled);
        }
    }
}

fn parse_mcp_subscription_end(
    value: Option<serde_json::Value>,
) -> Result<Option<McpSubscriptionEvent>, Error> {
    let Some(value) = value else {
        return Ok(Some(McpSubscriptionEvent::Ended(
            McpSubscriptionEnd::Abrupt,
        )));
    };
    let end = match value["reason"].as_str() {
        Some("graceful") => McpSubscriptionEnd::Graceful,
        Some("abrupt") => McpSubscriptionEnd::Abrupt,
        Some("cancelled") => McpSubscriptionEnd::Cancelled,
        Some("lagged") => McpSubscriptionEnd::Lagged {
            capacity: value["capacity"]
                .as_u64()
                .and_then(|capacity| usize::try_from(capacity).ok())
                .ok_or_else(|| Error::Operation("invalid MCP subscription capacity".into()))?,
        },
        Some("error") => McpSubscriptionEnd::Error {
            message: value["message"]
                .as_str()
                .ok_or_else(|| Error::Operation("invalid MCP subscription error".into()))?
                .to_owned(),
        },
        _ => {
            return Err(Error::Operation(
                "invalid MCP subscription terminal event".into(),
            ));
        }
    };
    Ok(Some(McpSubscriptionEvent::Ended(end)))
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpReadResourceContent {
    pub uri: Option<String>,
    pub mime_type: Option<String>,
    pub text: Option<String>,
    pub blob: Option<String>,
    pub meta: Option<serde_json::Value>,
    /// Lossless original protocol block for future fields and content variants.
    pub raw: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpReadResourceResult {
    pub contents: Vec<McpReadResourceContent>,
}
/// A server primitive with stable identity fields and its lossless MCP payload.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpResourceInfo {
    pub server: String,
    pub uri: Option<String>,
    pub name: Option<String>,
    pub raw: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpResourceTemplateInfo {
    pub server: String,
    pub uri_template: Option<String>,
    pub name: Option<String>,
    pub raw: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpResources {
    pub resources: Vec<McpResourceInfo>,
    pub templates: Vec<McpResourceTemplateInfo>,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpPromptInfo {
    pub server: String,
    pub name: String,
    pub description: Option<String>,
    pub raw: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpPromptResult {
    pub raw: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpCompletionResult {
    pub values: Vec<String>,
    pub total: Option<u64>,
    pub has_more: Option<bool>,
    pub raw: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpAuthStatus {
    pub server_name: String,
    pub status: McpAuthenticationState,
    pub error: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum McpAuthenticationState {
    Authenticated,
    NeedsAuth,
    SetupRequired,
    Failed,
    Unknown(String),
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpServerReplacementReceipt {
    pub names: Vec<String>,
    pub count: usize,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpServerStatusEvent {
    pub name: String,
    pub source: McpServerSource,
    pub status: McpServerStatus,
    pub reason: McpServerStatusReason,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpTaskStatusEvent {
    pub handle: McpTaskHandle,
    pub status: McpTaskStatus,
    pub status_message: Option<String>,
    pub last_updated_at: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpServerStatusReason {
    TransportClosed,
    HandshakeFailed,
    ConfigAdded,
    ConfigRemoved,
    ConfigChanged,
    Disabled,
    AuthExpired,
    Initialized,
    RestartSucceeded,
    RestartFailed,
    ManagedTokenRefreshed,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpToolsChangedEvent {
    pub server_name: Option<String>,
    pub tools: Vec<McpToolInfo>,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpInitializationProgress {
    pub connected: u32,
    pub total: u32,
}

fn parse_mcp_authentication_state(status: &str) -> McpAuthenticationState {
    match status {
        "authenticated" => McpAuthenticationState::Authenticated,
        "needs_auth" => McpAuthenticationState::NeedsAuth,
        "setup_required" => McpAuthenticationState::SetupRequired,
        "failed" => McpAuthenticationState::Failed,
        other => McpAuthenticationState::Unknown(other.to_owned()),
    }
}

fn parse_mcp_servers(value: &serde_json::Value) -> Result<Vec<McpServerSummary>, Error> {
    let entries = value
        .get("servers")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::Operation("invalid MCP catalog response".into()))?;
    Ok(entries
        .iter()
        .map(|v| {
            let name = v["name"].as_str().unwrap_or_default().to_owned();
            let session = &v["session"];
            let tools = session["tools"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|t| McpToolInfo {
                    server: name.clone(),
                    name: t["name"].as_str().unwrap_or_default().into(),
                    display_name: t["displayName"].as_str().map(Into::into),
                    description: t["description"].as_str().map(Into::into),
                    enabled: t["enabled"].as_bool().unwrap_or(true),
                    meta: t.get("_meta").cloned().unwrap_or(serde_json::Value::Null),
                })
                .collect();
            McpServerSummary {
                name,
                display_name: v["displayName"].as_str().map(Into::into),
                source: match v["source"].as_str() {
                    Some("local") => McpServerSource::Local,
                    Some("managed") => McpServerSource::Managed,
                    _ => McpServerSource::Unknown,
                },
                transport: match v["type"].as_str() {
                    Some("stdio") => McpTransportKind::Stdio,
                    Some("http") => McpTransportKind::Http,
                    Some("sse") => McpTransportKind::Sse,
                    Some("managedGateway") => McpTransportKind::ManagedGateway,
                    _ => McpTransportKind::Unknown,
                },
                enabled: session["enabled"].as_bool().unwrap_or(false),
                status: session["status"].as_str().map(|s| match s {
                    "ready" => McpServerStatus::Ready,
                    "initializing" => McpServerStatus::Initializing,
                    "setuprequired" | "setup_required" => McpServerStatus::SetupRequired,
                    "unavailable" => McpServerStatus::Unavailable,
                    "needsauth" | "needs_auth" => McpServerStatus::NeedsAuth,
                    _ => McpServerStatus::Unknown,
                }),
                auth_required: session["authRequired"].as_bool().unwrap_or(false),
                setup_required: session["setupRequired"].as_bool().unwrap_or(false),
                tools,
                negotiated: session.get("negotiated").and_then(|negotiated| {
                    let protocol_version = negotiated["protocolVersion"].as_str()?.to_owned();
                    let capabilities = negotiated.get("capabilities")?.clone();
                    let extensions: BTreeMap<String, serde_json::Value> =
                        capabilities["extensions"]
                            .as_object()
                            .map(|values| {
                                values
                                    .iter()
                                    .map(|(name, value)| (name.clone(), value.clone()))
                                    .collect()
                            })
                            .unwrap_or_default();
                    Some(McpNegotiatedCapabilities {
                        protocol_version,
                        tools: capabilities.get("tools").is_some_and(|v| !v.is_null()),
                        resources: capabilities.get("resources").is_some_and(|v| !v.is_null()),
                        prompts: capabilities.get("prompts").is_some_and(|v| !v.is_null()),
                        completions: capabilities
                            .get("completions")
                            .is_some_and(|v| !v.is_null()),
                        logging: capabilities.get("logging").is_some_and(|v| !v.is_null()),
                        tool_list_changed: capabilities["tools"]["listChanged"]
                            .as_bool()
                            .unwrap_or(false),
                        resource_list_changed: capabilities["resources"]["listChanged"]
                            .as_bool()
                            .unwrap_or(false),
                        subscriptions: capabilities["tools"]["listChanged"]
                            .as_bool()
                            .unwrap_or(false)
                            || capabilities["prompts"]["listChanged"]
                                .as_bool()
                                .unwrap_or(false)
                            || capabilities["resources"]["listChanged"]
                                .as_bool()
                                .unwrap_or(false)
                            || capabilities["resources"]["subscribe"]
                                .as_bool()
                                .unwrap_or(false),
                        legacy_resource_subscribe: capabilities["resources"]["subscribe"]
                            .as_bool()
                            .unwrap_or(false),
                        prompt_list_changed: capabilities["prompts"]["listChanged"]
                            .as_bool()
                            .unwrap_or(false),
                        tasks: extensions.contains_key("io.modelcontextprotocol/tasks"),
                        extensions,
                        raw: capabilities,
                    })
                }),
            }
        })
        .collect())
}
fn parse_tool_result(v: serde_json::Value) -> Result<McpToolResult, Error> {
    let blocks = v["content"]
        .as_array()
        .ok_or_else(|| Error::Operation("invalid MCP call response".into()))?;
    let content = blocks
        .iter()
        .cloned()
        .map(|raw| match raw["type"].as_str() {
            Some("text") => McpContent::Text {
                text: raw["text"].as_str().unwrap_or_default().into(),
                raw,
            },
            Some("image") => McpContent::Image {
                data: raw["data"].as_str().unwrap_or_default().into(),
                mime_type: raw["mimeType"].as_str().unwrap_or_default().into(),
                raw,
            },
            Some("audio") => McpContent::Audio {
                data: raw["data"].as_str().unwrap_or_default().into(),
                mime_type: raw["mimeType"].as_str().unwrap_or_default().into(),
                raw,
            },
            Some("resource") => McpContent::EmbeddedResource {
                resource: raw["resource"].clone(),
                raw,
            },
            Some("resource_link") | Some("resourceLink") => McpContent::ResourceLink {
                uri: raw["uri"].as_str().unwrap_or_default().into(),
                name: raw["name"].as_str().unwrap_or_default().into(),
                mime_type: raw["mimeType"].as_str().map(Into::into),
                raw,
            },
            _ => McpContent::Unknown { raw },
        })
        .collect();
    Ok(McpToolResult {
        content,
        structured_content: v.get("structuredContent").cloned(),
        is_error: v["isError"].as_bool(),
        meta: v.get("_meta").cloned(),
    })
}
fn parse_resource_result(v: serde_json::Value) -> Result<McpReadResourceResult, Error> {
    let blocks = v["contents"]
        .as_array()
        .ok_or_else(|| Error::Operation("invalid MCP resource response".into()))?;
    Ok(McpReadResourceResult {
        contents: blocks
            .iter()
            .map(|x| McpReadResourceContent {
                uri: x["uri"].as_str().map(Into::into),
                mime_type: x["mimeType"].as_str().map(Into::into),
                text: x["text"].as_str().map(Into::into),
                blob: x["blob"].as_str().map(Into::into),
                meta: x.get("_meta").cloned(),
                raw: x.clone(),
            })
            .collect(),
    })
}

fn parse_input_required(v: serde_json::Value) -> Result<McpInputRequired, Error> {
    let requests = v
        .get("inputRequests")
        .and_then(serde_json::Value::as_object)
        .map(|requests| {
            requests
                .iter()
                .map(|(id, request)| {
                    let kind = match request.get("method").and_then(serde_json::Value::as_str) {
                        Some("sampling/createMessage") => McpInputRequestKind::Sampling,
                        Some("elicitation/create") => McpInputRequestKind::Elicitation,
                        Some("roots/list") => McpInputRequestKind::Roots,
                        Some(method) => {
                            return Err(Error::Operation(format!(
                                "unsupported MCP input request method '{method}'"
                            )));
                        }
                        None => {
                            return Err(Error::Operation(
                                "MCP input request omitted its method".into(),
                            ));
                        }
                    };
                    Ok(McpInputRequest {
                        id: id.clone(),
                        kind,
                        raw: request.clone(),
                    })
                })
                .collect::<Result<Vec<_>, Error>>()
        })
        .transpose()?
        .unwrap_or_default();
    let request_state = v
        .get("requestState")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    if requests.is_empty() && request_state.is_none() {
        return Err(Error::Operation(
            "invalid MCP input_required response: no requests or request state".into(),
        ));
    }
    Ok(McpInputRequired {
        requests,
        request_state,
        raw: v,
        continuation_identity: None,
    })
}

fn parse_task_status(value: &serde_json::Value) -> Result<McpTaskStatus, Error> {
    match value.as_str() {
        Some("working") => Ok(McpTaskStatus::Working),
        Some("input_required") => Ok(McpTaskStatus::InputRequired),
        Some("completed") => Ok(McpTaskStatus::Completed),
        Some("failed") => Ok(McpTaskStatus::Failed),
        Some("cancelled") => Ok(McpTaskStatus::Cancelled),
        _ => Err(Error::Operation("invalid MCP Task status".into())),
    }
}

fn parse_task(
    session_id: &SessionId,
    server: &str,
    client_id: u64,
    raw: serde_json::Value,
) -> Result<McpTask, Error> {
    let task_id = raw
        .get("taskId")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Operation("MCP Task omitted taskId".into()))?
        .to_owned();
    let status = parse_task_status(&raw["status"])?;
    let input_required = if status == McpTaskStatus::InputRequired {
        Some(parse_input_required(serde_json::json!({
            "resultType": "input_required",
            "inputRequests": raw.get("inputRequests").cloned().unwrap_or_default(),
        }))?)
    } else {
        None
    };
    Ok(McpTask {
        handle: McpTaskHandle {
            session_id: session_id.clone(),
            server: server.to_owned(),
            client_id,
            task_id,
        },
        status,
        status_message: raw["statusMessage"].as_str().map(str::to_owned),
        created_at: raw["createdAt"].as_str().unwrap_or_default().to_owned(),
        last_updated_at: raw["lastUpdatedAt"].as_str().unwrap_or_default().to_owned(),
        ttl_ms: raw["ttl"].as_u64().or_else(|| raw["ttlMs"].as_u64()),
        poll_interval_ms: raw["pollInterval"]
            .as_u64()
            .or_else(|| raw["pollIntervalMs"].as_u64()),
        input_required,
        result: raw.get("result").cloned(),
        error: raw.get("error").cloned(),
        raw,
    })
}

fn parse_mcp_operation_outcome<T>(
    session_id: &SessionId,
    server: &str,
    value: serde_json::Value,
    operation: McpOperationIdentity,
    parse_complete: impl FnOnce(serde_json::Value) -> Result<T, Error>,
) -> Result<McpOperationOutcome<T>, Error> {
    let client_id = value["clientId"]
        .as_u64()
        .ok_or_else(|| Error::Operation("MCP operation omitted client generation".into()))?;
    let result = value
        .get("result")
        .cloned()
        .ok_or_else(|| Error::Operation("MCP operation omitted result".into()))?;
    match value["outcome"].as_str() {
        Some("complete") => Ok(McpOperationOutcome::Complete {
            client_id,
            result: parse_complete(result)?,
        }),
        Some("input_required") => {
            let mut input = parse_input_required(result)?;
            input.continuation_identity = Some(McpContinuationIdentity {
                session_id: session_id.clone(),
                server: server.to_owned(),
                client_id,
                operation,
                request_ids: input
                    .requests
                    .iter()
                    .map(|request| request.id.clone())
                    .collect(),
            });
            Ok(McpOperationOutcome::InputRequired { client_id, input })
        }
        Some("task") => {
            let task = parse_task(session_id, server, client_id, result)?;
            Ok(McpOperationOutcome::Task {
                handle: task.handle.clone(),
                task,
            })
        }
        _ => Err(Error::Operation("unsupported MCP operation outcome".into())),
    }
}

fn validate_mcp_continuation(
    continuation: Option<McpContinuation>,
    session_id: &SessionId,
    server: &str,
    operation: &McpOperationIdentity,
) -> Result<(Option<McpInputResponses>, Option<String>, Option<u64>), Error> {
    let Some(continuation) = continuation else {
        return Ok((None, None, None));
    };
    if continuation.identity.session_id != *session_id
        || continuation.identity.server != server
        || continuation.identity.operation != *operation
    {
        return Err(Error::InvalidConfig(
            "MCP continuation does not belong to this session, server, or operation".into(),
        ));
    }
    Ok((
        Some(continuation.input_responses),
        continuation.request_state,
        Some(continuation.identity.client_id),
    ))
}

fn parse_subagent_snapshot(value: serde_json::Value) -> Result<SubagentSnapshot, Error> {
    let required = |name: &str| {
        value[name]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::Operation(format!("subagent snapshot omitted {name}")))
    };
    Ok(SubagentSnapshot {
        subagent_id: required("subagentId")?,
        parent_session_id: required("parentSessionId")?,
        child_session_id: required("childSessionId")?,
        subagent_type: required("subagentType")?,
        description: required("description")?,
        started_at_epoch_ms: value["startedAtEpochMs"].as_u64().unwrap_or_default(),
        duration_ms: value["durationMs"].as_u64().unwrap_or_default(),
        status: value["status"].as_str().unwrap_or("running").to_owned(),
        turn_count: value["turnCount"].as_u64().and_then(|v| v.try_into().ok()),
        tool_call_count: value["toolCallCount"]
            .as_u64()
            .and_then(|v| v.try_into().ok()),
        tokens_used: value["tokensUsed"].as_u64(),
        context_window_tokens: value["contextWindowTokens"].as_u64(),
        context_usage_pct: value["contextUsagePct"]
            .as_u64()
            .and_then(|v| v.try_into().ok()),
        tools_used: value["toolsUsed"].as_array().map(|tools| {
            tools
                .iter()
                .filter_map(|tool| tool.as_str().map(str::to_owned))
                .collect()
        }),
        error_count: value["errorCount"].as_u64().and_then(|v| v.try_into().ok()),
        output: value["output"].as_str().map(str::to_owned),
        tool_calls: value["toolCalls"].as_u64().and_then(|v| v.try_into().ok()),
        turns: value["turns"].as_u64().and_then(|v| v.try_into().ok()),
        worktree_path: value["worktreePath"].as_str().map(PathBuf::from),
        failure_error: value["failureError"].as_str().map(str::to_owned),
        cancel_reason: value["cancelReason"].as_str().map(str::to_owned),
        fork_context_source: value["forkContextSource"].as_str().map(str::to_owned),
        fork_parent_prompt_id: value["forkParentPromptId"].as_str().map(str::to_owned),
        resumed_from: value["resumedFrom"].as_str().map(str::to_owned),
        raw: value,
    })
}

/// Complete explicit external-service selection for an embedded runtime.
/// Empty/default values preserve the legacy `RuntimeConfig.endpoint` and
/// `RuntimeConfig.api_key` provider for every model.
#[derive(Clone, Default, PartialEq, serde::Deserialize)]
pub struct RuntimeServices {
    #[serde(default)]
    pub model_providers: BTreeMap<String, ApiProviderConfig>,
    #[serde(default)]
    pub agents: AgentServiceConfig,
    pub media: Option<MediaServiceConfig>,
    #[serde(default)]
    pub mcp_servers: Vec<McpServerConfig>,
}

#[derive(Clone)]
pub struct RuntimeOptions {
    pub profile: RuntimeProfile,
    pub client_identifier: String,
    pub yolo_mode: bool,
    pub host_capabilities: HostCapabilities,
    pub event_journal_capacity: usize,
    pub skill_paths: Vec<PathBuf>,
    pub plugin_paths: Vec<PathBuf>,
    pub services: RuntimeServices,
    pub host: Option<Arc<dyn HostDelegate>>,
    pub tool_permission_handler: Option<Arc<dyn ToolPermissionHandler>>,
    pub mcp_host_services: xai_grok_mcp::servers::McpHostServices,
    pub in_process_mcp_servers: Vec<InProcessMcpServer>,
    pub agent_hooks: Vec<AgentHookRegistration>,
}
impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            profile: RuntimeProfile::Restricted,
            client_identifier: "grok-build-sdk".into(),
            yolo_mode: true,
            host_capabilities: HostCapabilities::default(),
            event_journal_capacity: 4096,
            skill_paths: Vec::new(),
            plugin_paths: Vec::new(),
            services: RuntimeServices::default(),
            host: None,
            tool_permission_handler: None,
            mcp_host_services: xai_grok_mcp::servers::McpHostServices::default(),
            in_process_mcp_servers: Vec::new(),
            agent_hooks: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HostRequest {
    pub method: String,
    pub params: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HostNotification {
    pub method: String,
    pub params: serde_json::Value,
}
#[derive(Clone, Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("host protocol error {code}: {message}")]
pub struct HostError {
    pub code: i32,
    pub message: String,
    #[serde(default)]
    pub data: serde_json::Value,
}
#[async_trait::async_trait]
pub trait HostDelegate: Send + Sync + 'static {
    async fn request(&self, request: HostRequest) -> Result<serde_json::Value, HostError>;
    async fn notification(&self, _notification: HostNotification) -> Result<(), HostError> {
        Ok(())
    }
}

/// A typed, concurrency-safe policy for agent tool permission requests.
/// It is routed only by the Desktop profile; returning an error never grants permission.
#[async_trait::async_trait]
pub trait ToolPermissionHandler: Send + Sync + 'static {
    async fn request_permission(
        &self,
        request: ToolPermissionRequest,
    ) -> Result<ToolPermissionDecision, ToolPermissionError>;
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolPermissionRequest {
    pub session_id: String,
    pub tool_call: ToolCallSummary,
    pub options: Vec<ToolPermissionOption>,
    /// Lossless representation of the agent request received by this SDK version.
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolCallSummary {
    pub id: String,
    pub title: Option<String>,
    pub kind: Option<ToolKind>,
    pub status: Option<ToolCallStatus>,
    pub raw_input: Option<serde_json::Value>,
    pub raw_output: Option<serde_json::Value>,
    pub raw: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    Read,
    Edit,
    Delete,
    Move,
    Search,
    Execute,
    Think,
    Fetch,
    SwitchMode,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Other,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolPermissionOption {
    pub id: String,
    pub name: String,
    pub kind: ToolPermissionOptionKind,
    /// Original wire spelling, retained for forward compatibility.
    pub raw_kind: String,
    pub meta: Option<serde_json::Value>,
    pub raw: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPermissionOptionKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolPermissionDecision {
    Cancelled,
    Selected(String),
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("tool permission policy error: {message}")]
pub struct ToolPermissionError {
    pub message: String,
    pub data: serde_json::Value,
}

/// A concurrency-safe SDK-owned in-process MCP endpoint. The shell performs
/// MCP initialization and capability negotiation; this losslessly transports
/// individual JSON-RPC messages.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InProcessMcpContext {
    /// Monotonic identity of the owning SDK runtime within this process.
    pub runtime_instance_id: u64,
    pub session_id: SessionId,
    /// Monotonic incarnation of this logical session within the runtime.
    pub session_instance_id: u64,
    pub server_name: String,
    /// Opaque registration identifier selected in [`InProcessMcpServer::new`].
    pub registration_id: String,
}

#[async_trait::async_trait]
pub(crate) trait InProcessMcpOutbound: Send + Sync + 'static {
    async fn send(&self, message: serde_json::Value) -> Result<(), HostError>;
}

/// Identity-bound server→client notification peer for an SDK-owned MCP
/// server. The peer is bounded and becomes stale when its session actor is
/// unloaded or replaced.
#[derive(Clone)]
pub struct InProcessMcpPeer {
    outbound: Arc<dyn InProcessMcpOutbound>,
}

impl InProcessMcpPeer {
    pub(crate) fn new(outbound: Arc<dyn InProcessMcpOutbound>) -> Self {
        Self { outbound }
    }

    /// Send a protocol or extension notification. Requests are intentionally
    /// not accepted: MCP 2026 roots/sampling/elicitation use MRTR input
    /// requests rather than direct server→client requests.
    pub async fn notify(
        &self,
        method: impl Into<String>,
        params: serde_json::Value,
    ) -> Result<(), HostError> {
        let method = method.into();
        if method.trim().is_empty() {
            return Err(HostError {
                code: -32600,
                message: "MCP notification method must not be empty".into(),
                data: serde_json::Value::Null,
            });
        }
        self.outbound
            .send(serde_json::json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }))
            .await
    }
}

#[async_trait::async_trait]
pub trait InProcessMcpHandler: Send + Sync + 'static {
    async fn handle(&self, message: serde_json::Value) -> Result<serde_json::Value, HostError>;

    /// Handles one message with the immutable session identity attached by the
    /// session actor. Override this when one handler serves multiple sessions and
    /// needs per-session authorization or state; the compatibility default calls
    /// [`Self::handle`].
    async fn handle_for_session(
        &self,
        _session_id: &SessionId,
        message: serde_json::Value,
    ) -> Result<serde_json::Value, HostError> {
        self.handle(message).await
    }

    /// Full identity-aware handler. Override this for per-registration or
    /// per-session-instance authorization. The compatibility default delegates
    /// to [`Self::handle_for_session`].
    async fn handle_with_context(
        &self,
        context: &InProcessMcpContext,
        message: serde_json::Value,
    ) -> Result<serde_json::Value, HostError> {
        self.handle_for_session(&context.session_id, message).await
    }

    /// Called once when the full-duplex process-local transport is attached.
    /// Retain the peer to emit MCP 2026 subscription acknowledgements and
    /// notifications while requests are being handled.
    async fn connected(
        &self,
        _context: &InProcessMcpContext,
        _peer: InProcessMcpPeer,
    ) -> Result<(), HostError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct InProcessMcpServer {
    pub name: String,
    pub server_id: String,
    pub handler: Arc<dyn InProcessMcpHandler>,
}
impl InProcessMcpServer {
    pub fn new(
        name: impl Into<String>,
        server_id: impl Into<String>,
        handler: Arc<dyn InProcessMcpHandler>,
    ) -> Self {
        Self {
            name: name.into(),
            server_id: server_id.into(),
            handler,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExtensionRequest {
    pub method: String,
    pub params: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExtensionResponse {
    pub result: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExtensionNotification {
    pub method: String,
    pub params: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CapabilityDescriptor {
    pub namespace: String,
    pub enabled: bool,
    pub disabled_reason: Option<String>,
    pub effect_class: String,
    pub host_requirement: Option<String>,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeCapabilities {
    pub profile: RuntimeProfile,
    pub host: HostCapabilities,
    pub features: Vec<CapabilityDescriptor>,
}

/// Current host-owned model catalog. This is available in both runtime
/// profiles and never consults Grok login, disk cache, or a remote catalog.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ModelCatalog {
    pub current_model_id: String,
    pub available_models: Vec<AvailableModel>,
    /// Forward-compatible catalog metadata from the ACP contract.
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

/// One selectable model, including the upstream capability metadata used for
/// context-window, agent-harness, and reasoning-effort discovery.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AvailableModel {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Prompt {
    pub blocks: Vec<PromptBlock>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PromptBlock {
    Text {
        text: String,
    },
    Image {
        data: String,
        mime_type: String,
        uri: Option<String>,
    },
    Audio {
        data: String,
        mime_type: String,
    },
    ResourceLink {
        uri: String,
        name: String,
        mime_type: Option<String>,
    },
    EmbeddedTextResource {
        uri: String,
        text: String,
        mime_type: Option<String>,
    },
    EmbeddedBlobResource {
        uri: String,
        blob: String,
        mime_type: Option<String>,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ModelSpec {
    pub id: String,
    pub context_window: u64,
    pub api_backend: ApiBackend,
    pub supports_reasoning: bool,
    pub default_reasoning: Option<String>,
    pub reasoning_options: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiBackend {
    #[default]
    ChatCompletions,
    Responses,
}

#[derive(Clone)]
pub struct RuntimeConfig {
    pub endpoint: String,
    pub api_key: String,
    pub grok_home: PathBuf,
    pub session_storage: PathBuf,
    pub models: Vec<ModelSpec>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SessionConfig {
    pub cwd: PathBuf,
    pub model: String,
    pub reasoning: Option<String>,
    /// Replaces the agent's default system prompt for this session.
    #[serde(default)]
    pub system_prompt: Option<String>,
    /// Host rules appended to the system prompt inside `<human_rules>`.
    #[serde(default)]
    pub rules: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentCommand {
    pub name: String,
    pub description: String,
    pub input_hint: Option<String>,
    /// Lossless agent command metadata for future command kinds.
    pub meta: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InterjectionReceipt {
    pub interjection_id: Option<String>,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ForkSessionRequest {
    pub source_cwd: PathBuf,
    pub new_cwd: PathBuf,
    pub new_session_id: Option<String>,
    pub new_model_id: Option<String>,
    pub target_prompt_index: Option<usize>,
    pub session_kind: Option<String>,
    pub source_workspace_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkSessionReceipt {
    pub new_session_id: SessionId,
    pub chat_messages_copied: usize,
    pub updates_copied: usize,
    pub plan_state_copied: bool,
    pub new_cwd: PathBuf,
    pub parent_session_id: SessionId,
    pub new_model_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowInfo {
    pub name: String,
    pub description: String,
    pub when_to_use: Option<String>,
    pub source: String,
    pub path: Option<PathBuf>,
    /// Lossless workflow entry for forward-compatible fields.
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentSnapshot {
    pub subagent_id: String,
    pub parent_session_id: String,
    pub child_session_id: String,
    pub subagent_type: String,
    pub description: String,
    pub started_at_epoch_ms: u64,
    pub duration_ms: u64,
    pub status: String,
    pub turn_count: Option<u32>,
    pub tool_call_count: Option<u32>,
    pub tokens_used: Option<u64>,
    pub context_window_tokens: Option<u64>,
    pub context_usage_pct: Option<u8>,
    pub tools_used: Option<Vec<String>>,
    pub error_count: Option<u32>,
    pub output: Option<String>,
    pub tool_calls: Option<u32>,
    pub turns: Option<u32>,
    pub worktree_path: Option<PathBuf>,
    pub failure_error: Option<String>,
    pub cancel_reason: Option<String>,
    pub fork_context_source: Option<String>,
    pub fork_parent_prompt_id: Option<String>,
    pub resumed_from: Option<String>,
    /// Lossless snapshot for fields added by newer agents.
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SubagentCancelOutcome {
    Cancelled,
    AlreadyFinished {
        status: String,
    },
    NotFound,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentCancelReceipt {
    pub subagent_id: String,
    pub cancelled: bool,
    pub outcome: Option<SubagentCancelOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SessionId(String);
impl SessionId {
    const RUNTIME_EVENTS: &'static str = "__origin_runtime__";

    /// Restores an opaque Grok session identifier persisted by the host.
    /// Validation still occurs inside `load_session`; this constructor never
    /// interprets the identifier or exposes Grok protocol types.
    pub fn from_stored(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Reserved journal identity for extension notifications that are not
    /// associated with a session.
    pub fn runtime_events() -> Self {
        Self(Self::RUNTIME_EVENTS.into())
    }
}

/// SDK-owned scheduler request. `task_id == None` creates; otherwise only
/// `interval` and/or `prompt` are updated and the existing phase is retained.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledTaskRequest {
    pub task_id: Option<String>,
    pub interval: Option<String>,
    pub prompt: Option<String>,
    #[serde(default = "sdk_default_true")]
    pub recurring: bool,
    pub durable: Option<bool>,
    pub foreground: Option<bool>,
    #[serde(default)]
    pub fire_immediately: bool,
}
fn sdk_default_true() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledTaskSummary {
    pub id: String,
    pub interval_seconds: u64,
    pub prompt: String,
    pub recurring: bool,
    pub durable: bool,
    pub foreground: bool,
    pub created_at: String,
    pub last_fired_at: Option<String>,
    pub expires_at: Option<String>,
    pub last_subagent: Option<String>,
    pub next_fire_at: String,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ScheduledTaskReceipt {
    pub task: ScheduledTaskSummary,
    pub updated: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteScheduledTaskReceipt {
    pub task_id: String,
    pub deleted: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub session_id: SessionId,
    pub sequence: u64,
    pub turn_id: Option<String>,
    pub timestamp_ms: u64,
    pub replay: bool,
    pub update: EventUpdate,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EventUpdate {
    SessionStarted,
    UserText(String),
    AssistantText(String),
    ThoughtText(String),
    ToolStart(ToolEvent),
    ToolUpdate(ToolEvent),
    Plan {
        summary: String,
    },
    AvailableCommands(Vec<RuntimeCommand>),
    ModeChanged(String),
    ConfigOptions(Vec<RuntimeConfigOption>),
    SessionInfo {
        title: Option<String>,
    },
    McpServerStatus(McpServerStatusEvent),
    McpTaskStatus(McpTaskStatusEvent),
    McpToolsChanged(McpToolsChangedEvent),
    McpInitializationProgress(McpInitializationProgress),
    /// Redacted catalog update. Transport targets, headers, environment and
    /// setup values are intentionally omitted.
    McpServersChanged(Vec<McpServerSummary>),
    Unknown {
        tag: String,
        /// Lossless JSON representation of the agent update that this version of
        /// the SDK does not yet model as a typed variant.
        payload: serde_json::Value,
        /// Original JSON encoding retained for hosts that need byte-for-byte
        /// forwarding or deferred decoding.
        raw: String,
    },
    TurnFinished(TurnOutcome),
    SessionClosed,
    Extension {
        method: String,
        payload: serde_json::Value,
        raw: String,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeCommand {
    pub name: String,
    pub description: String,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeConfigOption {
    pub id: String,
    pub name: String,
    pub category: Option<String>,
    pub value: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolEvent {
    pub id: String,
    pub title: String,
    pub kind: String,
    pub status: String,
    pub raw_input: Option<String>,
    pub raw_output: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TurnOutcome {
    End,
    Cancelled,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptReceipt {
    pub outcome: TurnOutcome,
    /// Every event through this per-session sequence is retained and queryable
    /// before `prompt` returns.
    pub final_sequence: u64,
    /// Position on the native session's active conversation timeline. Rewinds
    /// retain prompts below their target and later Turns may reuse a discarded
    /// position; callers pair this with the exact prompt digest from the ledger.
    pub runtime_prompt_index: u64,
    /// Stable receipt from the fork-owned durable Turn ledger.
    pub settlement_id: String,
    /// Provider-derived usage bound into `settlement_id`. Unknown dimensions
    /// remain explicit and are never interpreted as zero.
    pub usage: run::EffectUsage,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum LedgerTurnState {
    Pending,
    Completed {
        outcome: TurnOutcome,
        settlement_id: String,
        /// `None` is accepted only for ledgers written before usage-bound
        /// settlements. Such an entry cannot recover an SDK-owned Run effect.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<run::EffectUsage>,
    },
    Discarded,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionLedgerEntry {
    pub turn_id: String,
    pub prompt_digest: String,
    pub runtime_prompt_index: u64,
    pub state: LedgerTurnState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionLedger {
    pub entries: Vec<SessionLedgerEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConversationRewindReceipt {
    pub operation_id: String,
    pub session_id: String,
    pub target_prompt_index: u64,
    pub target_turn_id: String,
    pub target_prompt_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_prompt_digest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum ConversationRewindStatus {
    Absent,
    Pending {
        operation_id: String,
        session_id: String,
        target_prompt_index: u64,
        target_turn_id: String,
        target_prompt_digest: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_prompt_digest: Option<String>,
    },
    Applied {
        receipt: ConversationRewindReceipt,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewindPoint {
    pub prompt_index: u64,
    /// SDK-owned canonical digest of the exact user prompt at this native
    /// timeline position. Hosts use it to reject stale checkpoints after a
    /// rewind reuses a prompt index on a new branch.
    pub prompt_digest: Option<String>,
    pub created_at: String,
    pub file_snapshots: u64,
    pub has_file_changes: bool,
    pub prompt_preview: Option<String>,
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid runtime configuration: {0}")]
    InvalidConfig(String),
    #[error("runtime operation failed: {0}")]
    Operation(String),
    #[error("runtime has shut down")]
    Shutdown,
    #[error("protocol error in {method}: {code}: {message}")]
    Protocol {
        method: String,
        code: i32,
        message: String,
        data: serde_json::Value,
        retryable: bool,
    },
    #[error("host error in {method}: {source}")]
    Host { method: String, source: HostError },
    #[error("event journal gap: requested {requested}, oldest {oldest_available}, newest {newest}")]
    EventGap {
        requested: u64,
        oldest_available: u64,
        newest: u64,
    },
    #[error(transparent)]
    DurableRun(#[from] run::RunError),
}

fn run_reconcile_command_id(
    parent: &run::CommandId,
    operation: &run::OperationId,
) -> Result<run::CommandId, Error> {
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(format!("{}\0{}", parent.as_str(), operation.as_str()));
    run::CommandId::new(format!("reconcile_{digest:x}")).map_err(|error| {
        Error::Operation(format!(
            "could not derive durable reconciliation command: {error}"
        ))
    })
}

fn durable_turn_outcome(outcome: TurnOutcome) -> run::SessionTurnOutcome {
    match outcome {
        TurnOutcome::End => run::SessionTurnOutcome::End,
        TurnOutcome::Cancelled => run::SessionTurnOutcome::Cancelled,
        TurnOutcome::MaxTokens => run::SessionTurnOutcome::MaxTokens,
        TurnOutcome::MaxTurnRequests => run::SessionTurnOutcome::MaxTurnRequests,
        TurnOutcome::Refusal => run::SessionTurnOutcome::Refusal,
    }
}

fn rewind_receipt_proves_turn_not_applied(
    receipt: &ConversationRewindReceipt,
    entry: &SessionLedgerEntry,
    turn_id: &str,
    prompt_digest: &str,
) -> bool {
    receipt.target_prompt_index == entry.runtime_prompt_index
        && receipt.target_turn_id == turn_id
        && receipt.target_prompt_digest == prompt_digest
        && receipt.recovery_turn_id.as_deref() == Some(turn_id)
        && receipt.recovery_prompt_digest.as_deref() == Some(prompt_digest)
}

#[derive(Clone)]
pub struct Runtime {
    inner: private::Runtime,
}
impl Runtime {
    pub fn builder(config: RuntimeConfig) -> RuntimeBuilder {
        RuntimeBuilder {
            config,
            options: RuntimeOptions::default(),
            run_store: None,
        }
    }
    pub async fn start(
        config: RuntimeConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start(config, RuntimeOptions::default())
            .await
            .map(|(inner, events)| (Self { inner }, events))
    }
    /// Starts the SDK with one Host-provided Run authority. The supplied store
    /// replaces (rather than mirrors) the standalone SQLite store, so snapshot,
    /// journal, outbox and command receipts still have exactly one writer.
    pub async fn start_with_run_store(
        config: RuntimeConfig,
        store: Arc<dyn run::RunStore>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start_with_run_store(config, RuntimeOptions::default(), Some(store))
            .await
            .map(|(inner, events)| (Self { inner }, events))
    }
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.inner.capabilities()
    }
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
    /// Returns the live fixed model catalog through Grok Build's
    /// `x.ai/models/list` contract. Unlike generic extension requests, this
    /// typed, read-only operation is also available in Restricted runtimes.
    pub async fn list_models(&self) -> Result<ModelCatalog, Error> {
        self.inner.list_models().await
    }
    pub async fn extension_request(
        &self,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        self.inner.extension_request(request).await
    }
    pub async fn extension_notification(&self, request: ExtensionRequest) -> Result<(), Error> {
        self.inner
            .extension_notification(ExtensionNotification {
                method: request.method,
                params: request.params,
            })
            .await
    }
    pub async fn notify_extension(&self, notification: ExtensionNotification) -> Result<(), Error> {
        self.inner.extension_notification(notification).await
    }
    async fn raw_ext(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        Ok(self
            .extension_request(ExtensionRequest {
                method: method.to_owned(),
                params,
            })
            .await?
            .result)
    }
    async fn typed_ext(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let envelope = self
            .extension_request(ExtensionRequest {
                method: method.into(),
                params,
            })
            .await?
            .result;
        if let Some(error) = envelope.get("error").filter(|value| !value.is_null()) {
            let detail = error
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| error.to_string());
            return Err(Error::Operation(format!(
                "extension '{method}' failed: {detail}"
            )));
        }
        envelope
            .get("result")
            .cloned()
            .filter(|value| !value.is_null())
            .ok_or_else(|| Error::Operation(format!("extension '{method}' returned no result")))
    }
    async fn mcp_ext(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        self.typed_ext(method, params).await
    }
    /// Creates or updates a recurring task in an explicit session.
    pub async fn upsert_scheduled_task(
        &self,
        id: &SessionId,
        request: &ScheduledTaskRequest,
    ) -> Result<ScheduledTaskReceipt, Error> {
        let mut params =
            serde_json::to_value(request).map_err(|e| Error::Operation(e.to_string()))?;
        params["sessionId"] = serde_json::Value::String(id.as_str().to_owned());
        serde_json::from_value(self.typed_ext("x.ai/scheduler/create", params).await?)
            .map_err(|e| Error::Operation(format!("invalid scheduler create response: {e}")))
    }
    /// Lists tasks owned by an explicit session.
    pub async fn list_scheduled_tasks(
        &self,
        id: &SessionId,
    ) -> Result<Vec<ScheduledTaskSummary>, Error> {
        let value = self
            .typed_ext(
                "x.ai/scheduler/list",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await?;
        serde_json::from_value(value.get("tasks").cloned().unwrap_or_default())
            .map_err(|e| Error::Operation(format!("invalid scheduler list response: {e}")))
    }
    pub async fn delete_scheduled_task(
        &self,
        id: &SessionId,
        task_id: &str,
    ) -> Result<DeleteScheduledTaskReceipt, Error> {
        serde_json::from_value(
            self.typed_ext(
                "x.ai/scheduler/delete",
                serde_json::json!({"sessionId": id.as_str(), "taskId": task_id}),
            )
            .await?,
        )
        .map_err(|e| Error::Operation(format!("invalid scheduler delete response: {e}")))
    }
    /// Discovers built-ins, skills (including `implement`) and workflows
    /// available to this live session.
    pub async fn list_agent_commands(&self, id: &SessionId) -> Result<Vec<AgentCommand>, Error> {
        let value = self
            .raw_ext(
                "x.ai/commands/list",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await?;
        let commands = value["commands"]
            .as_array()
            .ok_or_else(|| Error::Operation("invalid commands/list response".into()))?;
        Ok(commands
            .iter()
            .filter_map(|command| {
                Some(AgentCommand {
                    name: command["name"].as_str()?.to_owned(),
                    description: command["description"].as_str()?.to_owned(),
                    input_hint: command["input"]["hint"].as_str().map(str::to_owned),
                    meta: command.get("_meta").cloned(),
                })
            })
            .collect())
    }
    /// Executes a discovered built-in, skill or workflow command as a normal
    /// agent turn. The command is allowlisted against the live catalog first.
    /// Use [`Runtime::upsert_scheduled_task`] for `/loop` when no model-based
    /// interpretation of the schedule is desired.
    pub async fn execute_agent_command(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        name: &str,
        arguments: Option<&str>,
    ) -> Result<PromptReceipt, Error> {
        if name.is_empty()
            || name.starts_with('/')
            || name.chars().any(|ch| ch.is_whitespace() || ch.is_control())
        {
            return Err(Error::InvalidConfig(
                "agent command name must be non-empty and contain no slash prefix or whitespace"
                    .into(),
            ));
        }
        let commands = self.list_agent_commands(id).await?;
        if !commands.iter().any(|command| command.name == name) {
            return Err(Error::Operation(format!(
                "agent command '{name}' is not available in this session"
            )));
        }
        let prompt = match arguments.filter(|value| !value.is_empty()) {
            Some(arguments) => format!("/{name} {arguments}"),
            None => format!("/{name}"),
        };
        self.prompt(id, turn_id, prompt).await
    }
    /// Queues a steering/follow-up instruction into the currently running turn.
    pub async fn interject(
        &self,
        id: &SessionId,
        text: impl Into<String>,
        interjection_id: Option<String>,
    ) -> Result<InterjectionReceipt, Error> {
        let status = self
            .typed_ext(
                "x.ai/interject",
                serde_json::json!({
                    "sessionId": id.as_str(),
                    "text": text.into(),
                    "interjectionId": interjection_id
                }),
            )
            .await?;
        Ok(InterjectionReceipt {
            interjection_id,
            status: status["status"].as_str().unwrap_or("unknown").to_owned(),
        })
    }
    /// Copies a persisted conversation branch. The returned child is not
    /// resident until the host calls [`Runtime::load_session`].
    pub async fn fork_session(
        &self,
        source: &SessionId,
        request: &ForkSessionRequest,
    ) -> Result<ForkSessionReceipt, Error> {
        let value = self
            .raw_ext(
                "x.ai/session/fork",
                serde_json::json!({
                    "sourceSessionId": source.as_str(),
                    "sourceCwd": request.source_cwd,
                    "newCwd": request.new_cwd,
                    "newSessionId": request.new_session_id,
                    "newModelId": request.new_model_id,
                    "targetPromptIndex": request.target_prompt_index,
                    "sessionKind": request.session_kind,
                    "sourceWorkspaceDir": request.source_workspace_dir
                }),
            )
            .await?;
        serde_json::from_value(value)
            .map_err(|e| Error::Operation(format!("invalid session/fork response: {e}")))
    }
    /// Lists launchable workflows for this live session.
    pub async fn list_workflows(&self, id: &SessionId) -> Result<Vec<WorkflowInfo>, Error> {
        let value = self
            .typed_ext(
                "x.ai/workflows/list",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await?;
        Ok(value["workflows"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|workflow| {
                Some(WorkflowInfo {
                    name: workflow["name"].as_str()?.to_owned(),
                    description: workflow["description"].as_str()?.to_owned(),
                    when_to_use: workflow["whenToUse"].as_str().map(str::to_owned),
                    source: workflow["source"].as_str().unwrap_or("unknown").to_owned(),
                    path: workflow["path"].as_str().map(PathBuf::from),
                    raw: workflow.clone(),
                })
            })
            .collect())
    }
    /// Lists subagents currently running under this root session.
    pub async fn list_running_subagents(
        &self,
        id: &SessionId,
    ) -> Result<Vec<SubagentSnapshot>, Error> {
        let value = self
            .typed_ext(
                "x.ai/subagent/list_running",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await?;
        value["subagents"]
            .as_array()
            .into_iter()
            .flatten()
            .cloned()
            .map(parse_subagent_snapshot)
            .collect()
    }
    /// Gets a live or terminal subagent snapshot, optionally waiting for it to finish.
    pub async fn get_subagent(
        &self,
        subagent_id: &str,
        block: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Option<SubagentSnapshot>, Error> {
        let value = self
            .typed_ext(
                "x.ai/subagent/get",
                serde_json::json!({
                    "subagentId": subagent_id,
                    "block": block,
                    "timeoutMs": timeout_ms
                }),
            )
            .await?;
        value
            .get("snapshot")
            .filter(|snapshot| !snapshot.is_null())
            .cloned()
            .map(parse_subagent_snapshot)
            .transpose()
    }
    /// Cancels a subagent by its globally unique subagent id.
    pub async fn cancel_subagent(&self, subagent_id: &str) -> Result<SubagentCancelReceipt, Error> {
        serde_json::from_value(
            self.typed_ext(
                "x.ai/subagent/cancel",
                serde_json::json!({"subagentId": subagent_id}),
            )
            .await?,
        )
        .map_err(|e| Error::Operation(format!("invalid subagent/cancel response: {e}")))
    }
    /// Returns a redacted catalog for the explicit session. Restricted runtimes fail closed.
    pub async fn list_mcp_servers(
        &self,
        id: &SessionId,
        refresh: bool,
    ) -> Result<Vec<McpServerSummary>, Error> {
        let value = self
            .mcp_ext(
                "x.ai/mcp/list",
                serde_json::json!({
                    "sessionId": id.as_str(),
                    "cache": !refresh,
                    "requireSession": true
                }),
            )
            .await?;
        parse_mcp_servers(&value)
    }
    /// Lists tools currently visible in an explicit session, optionally filtered by server.
    pub async fn list_mcp_tools(
        &self,
        id: &SessionId,
        server: Option<&str>,
    ) -> Result<Vec<McpToolInfo>, Error> {
        let mut tools: Vec<_> = self
            .list_mcp_servers(id, false)
            .await?
            .into_iter()
            .flat_map(|s| s.tools)
            .collect();
        if let Some(server) = server {
            tools.retain(|t| t.server == server);
        }
        Ok(tools)
    }
    /// Calls one MCP tool through the session actor.
    pub async fn call_mcp_tool(
        &self,
        id: &SessionId,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<McpToolResult, Error> {
        let v = self.mcp_ext("x.ai/mcp/call", serde_json::json!({"sessionId":id.as_str(),"server":server,"tool":tool,"arguments":arguments})).await?;
        parse_tool_result(v)
    }
    /// Checks liveness of one initialized MCP server using the protocol ping.
    pub async fn ping_mcp(&self, id: &SessionId, server: &str) -> Result<(), Error> {
        self.inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::Ping,
            )
            .await
            .map(drop)
    }

    /// Notifies a server that the host root set changed. This fails unless a
    /// roots service was installed with `list_changed` authorization.
    pub async fn notify_mcp_roots_list_changed(
        &self,
        id: &SessionId,
        server: &str,
    ) -> Result<(), Error> {
        self.inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::NotifyRootsListChanged,
            )
            .await
            .map(drop)
    }
    /// Executes exactly one MCP 2026 tools/call round. Unlike
    /// [`Runtime::call_mcp_tool`], this preserves MRTR and Task outcomes for
    /// explicit orchestration by the host.
    pub async fn call_mcp_tool_once(
        &self,
        id: &SessionId,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
        continuation: Option<McpContinuation>,
    ) -> Result<McpOperationOutcome<McpToolResult>, Error> {
        let operation = McpOperationIdentity::Tool {
            name: tool.to_owned(),
            arguments: arguments.clone(),
        };
        let (input_responses, request_state, expected_client_id) =
            validate_mcp_continuation(continuation, id, server, &operation)?;
        let value = self
            .inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::CallToolOnce {
                    tool_name: tool.to_owned(),
                    arguments,
                    input_responses,
                    request_state,
                    expected_client_id,
                },
            )
            .await?;
        parse_mcp_operation_outcome(id, server, value, operation, parse_tool_result)
    }
    /// Reads an MCP resource through the session actor.
    pub async fn read_mcp_resource(
        &self,
        id: &SessionId,
        server: &str,
        uri: &str,
    ) -> Result<McpReadResourceResult, Error> {
        let v = self
            .mcp_ext(
                "x.ai/mcp/read_resource",
                serde_json::json!({"sessionId":id.as_str(),"server":server,"uri":uri}),
            )
            .await?;
        parse_resource_result(v)
    }
    /// Executes exactly one MCP 2026 resources/read round.
    pub async fn read_mcp_resource_once(
        &self,
        id: &SessionId,
        server: &str,
        uri: &str,
        continuation: Option<McpContinuation>,
    ) -> Result<McpOperationOutcome<McpReadResourceResult>, Error> {
        let operation = McpOperationIdentity::Resource {
            uri: uri.to_owned(),
        };
        let (input_responses, request_state, expected_client_id) =
            validate_mcp_continuation(continuation, id, server, &operation)?;
        let value = self
            .inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::ReadResourceOnce {
                    uri: uri.to_owned(),
                    input_responses,
                    request_state,
                    expected_client_id,
                },
            )
            .await?;
        parse_mcp_operation_outcome(id, server, value, operation, parse_resource_result)
    }
    /// Lists resources and URI templates from one live session server.
    pub async fn list_mcp_resources(
        &self,
        id: &SessionId,
        server: &str,
    ) -> Result<McpResources, Error> {
        let value = self
            .mcp_ext(
                "x.ai/mcp/resources/list",
                serde_json::json!({"sessionId": id.as_str(), "server": server}),
            )
            .await?;
        let resources = value["resources"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|raw| McpResourceInfo {
                server: server.into(),
                uri: raw["uri"].as_str().map(Into::into),
                name: raw["name"].as_str().map(Into::into),
                raw: raw.clone(),
            })
            .collect();
        let templates = value["resourceTemplates"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|raw| McpResourceTemplateInfo {
                server: server.into(),
                uri_template: raw["uriTemplate"].as_str().map(Into::into),
                name: raw["name"].as_str().map(Into::into),
                raw: raw.clone(),
            })
            .collect();
        Ok(McpResources {
            resources,
            templates,
        })
    }
    pub async fn list_mcp_prompts(
        &self,
        id: &SessionId,
        server: &str,
    ) -> Result<Vec<McpPromptInfo>, Error> {
        let value = self
            .mcp_ext(
                "x.ai/mcp/prompts/list",
                serde_json::json!({"sessionId": id.as_str(), "server": server}),
            )
            .await?;
        Ok(value["prompts"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|raw| McpPromptInfo {
                server: server.into(),
                name: raw["name"].as_str().unwrap_or_default().into(),
                description: raw["description"].as_str().map(Into::into),
                raw: raw.clone(),
            })
            .collect())
    }
    pub async fn get_mcp_prompt(
        &self,
        id: &SessionId,
        server: &str,
        name: &str,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<McpPromptResult, Error> {
        let raw = self.mcp_ext("x.ai/mcp/prompts/get", serde_json::json!({"sessionId": id.as_str(), "server": server, "name": name, "arguments": arguments})).await?;
        Ok(McpPromptResult { raw })
    }
    /// Executes exactly one MCP 2026 prompts/get round.
    pub async fn get_mcp_prompt_once(
        &self,
        id: &SessionId,
        server: &str,
        name: &str,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
        continuation: Option<McpContinuation>,
    ) -> Result<McpOperationOutcome<McpPromptResult>, Error> {
        let operation = McpOperationIdentity::Prompt {
            name: name.to_owned(),
            arguments: arguments.clone(),
        };
        let (input_responses, request_state, expected_client_id) =
            validate_mcp_continuation(continuation, id, server, &operation)?;
        let value = self
            .inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::GetPromptOnce {
                    name: name.to_owned(),
                    arguments,
                    input_responses,
                    request_state,
                    expected_client_id,
                },
            )
            .await?;
        parse_mcp_operation_outcome(id, server, value, operation, |raw| {
            Ok(McpPromptResult { raw })
        })
    }

    /// Reads the latest state for a generation-bound MCP Task.
    pub async fn get_mcp_task(&self, handle: &McpTaskHandle) -> Result<McpTask, Error> {
        let value = self
            .inner
            .mcp_modern(
                &handle.session_id,
                handle.server.clone(),
                xai_grok_shell::extensions::mcp::McpModernOperation::GetTask {
                    client_id: handle.client_id,
                    task_id: handle.task_id.clone(),
                },
            )
            .await?;
        let client_id = value["clientId"].as_u64().ok_or_else(|| {
            Error::Operation("MCP Task response omitted client generation".into())
        })?;
        let raw = value
            .get("result")
            .cloned()
            .ok_or_else(|| Error::Operation("MCP Task response omitted result".into()))?;
        parse_task(&handle.session_id, &handle.server, client_id, raw)
    }

    /// Supplies responses to an input_required MCP Task. Task state is read
    /// separately with [`Runtime::get_mcp_task`].
    pub async fn update_mcp_task(
        &self,
        handle: &McpTaskHandle,
        input_responses: McpInputResponses,
    ) -> Result<(), Error> {
        self.inner
            .mcp_modern(
                &handle.session_id,
                handle.server.clone(),
                xai_grok_shell::extensions::mcp::McpModernOperation::UpdateTask {
                    client_id: handle.client_id,
                    task_id: handle.task_id.clone(),
                    input_responses,
                },
            )
            .await
            .map(drop)
    }

    /// Requests cancellation of a generation-bound MCP Task.
    pub async fn cancel_mcp_task(&self, handle: &McpTaskHandle) -> Result<(), Error> {
        self.inner
            .mcp_modern(
                &handle.session_id,
                handle.server.clone(),
                xai_grok_shell::extensions::mcp::McpModernOperation::CancelTask {
                    client_id: handle.client_id,
                    task_id: handle.task_id.clone(),
                },
            )
            .await
            .map(drop)
    }
    /// Opens a bounded MCP 2026 `subscriptions/listen` stream and waits for
    /// the server's acknowledgement. Dropping the returned stream cancels the
    /// listen request; reconnects require a new call.
    pub async fn listen_mcp(
        &self,
        id: &SessionId,
        server: &str,
        filter: McpSubscriptionFilter,
        capacity: usize,
    ) -> Result<McpSubscription, Error> {
        let capacity = std::num::NonZeroUsize::new(capacity)
            .filter(|capacity| capacity.get() <= 4096)
            .ok_or_else(|| {
                Error::InvalidConfig("MCP subscription capacity must be in 1..=4096".into())
            })?;
        let bridge = self
            .inner
            .mcp_subscribe(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernSubscriptionFilter {
                    tools_list_changed: filter.tools_list_changed,
                    prompts_list_changed: filter.prompts_list_changed,
                    resources_list_changed: filter.resources_list_changed,
                    resource_subscriptions: filter.resource_subscriptions,
                },
                capacity,
            )
            .await?;
        let acknowledged = serde_json::from_value(bridge.acknowledged)
            .map_err(|error| Error::Operation(format!("invalid MCP subscription ack: {error}")))?;
        Ok(McpSubscription {
            session_id: id.clone(),
            server: server.to_owned(),
            client_id: bridge.client_id,
            acknowledged,
            events: bridge.events,
            terminal: bridge.terminal,
            cancel: Some(bridge.cancel),
            pending_end: None,
            ended: false,
        })
    }
    /// Completes either a `prompt` name argument or `resource` URI-template argument.
    pub async fn complete_mcp_argument(
        &self,
        id: &SessionId,
        server: &str,
        reference: &str,
        target: &str,
        argument: &str,
        value: &str,
        context: Option<BTreeMap<String, String>>,
    ) -> Result<McpCompletionResult, Error> {
        if !matches!(reference, "prompt" | "resource") {
            return Err(Error::InvalidConfig(
                "MCP completion reference must be 'prompt' or 'resource'".into(),
            ));
        }
        let raw = self.mcp_ext("x.ai/mcp/complete", serde_json::json!({"sessionId": id.as_str(), "server": server, "reference": reference, "target": target, "argument": argument, "value": value, "context": context})).await?;
        Ok(McpCompletionResult {
            values: raw["values"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(Into::into))
                .collect(),
            total: raw["total"].as_u64(),
            has_more: raw["hasMore"].as_bool(),
            raw,
        })
    }
    /// Returns per-server authentication state for this session.
    pub async fn mcp_auth_status(&self, id: &SessionId) -> Result<Vec<McpAuthStatus>, Error> {
        let v = self
            .mcp_ext(
                "x.ai/mcp/auth_status",
                serde_json::json!({"session_id":id.as_str()}),
            )
            .await?;
        Ok(v.get("servers")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .map(|x| McpAuthStatus {
                server_name: x["server_name"].as_str().unwrap_or_default().into(),
                status: parse_mcp_authentication_state(x["status"].as_str().unwrap_or("unknown")),
                error: None,
            })
            .collect())
    }
    /// Starts authentication and returns its immediate/deferred typed status.
    pub async fn start_mcp_auth(
        &self,
        id: &SessionId,
        server: &str,
    ) -> Result<McpAuthStatus, Error> {
        let v = self
            .mcp_ext(
                "x.ai/mcp/auth_trigger",
                serde_json::json!({"session_id":id.as_str(),"server_name":server}),
            )
            .await?;
        Ok(McpAuthStatus {
            server_name: server.into(),
            status: parse_mcp_authentication_state(v["status"].as_str().unwrap_or("unknown")),
            error: v["error"].as_str().map(Into::into),
        })
    }
    /// Changes server state only in this session; no preference file is mutated.
    pub async fn set_mcp_server_enabled(
        &self,
        id: &SessionId,
        server: &str,
        enabled: bool,
    ) -> Result<(), Error> {
        self.mcp_ext("x.ai/mcp/toggle",serde_json::json!({"session_id":id.as_str(),"server_name":server,"enabled":enabled,"session_local":true})).await.map(drop)
    }
    /// Changes tool state only in this session.
    pub async fn set_mcp_tool_enabled(
        &self,
        id: &SessionId,
        server: &str,
        tool: &str,
        enabled: bool,
    ) -> Result<(), Error> {
        self.mcp_ext("x.ai/mcp/toggle_tool",serde_json::json!({"session_id":id.as_str(),"server_name":server,"tool_name":tool,"enabled":enabled,"session_local":true})).await.map(drop)
    }
    /// Atomically replaces the session's client-provided MCP server set. The
    /// receipt contains names only and never echoes transport credentials.
    pub async fn replace_mcp_servers(
        &self,
        id: &SessionId,
        servers: Vec<McpServerConfig>,
    ) -> Result<McpServerReplacementReceipt, Error> {
        let names = servers
            .iter()
            .map(|s| match s {
                McpServerConfig::Stdio { name, .. }
                | McpServerConfig::Http { name, .. }
                | McpServerConfig::Sse { name, .. } => name.clone(),
            })
            .collect::<Vec<_>>();
        self.inner.replace_mcp_servers(id, servers).await?;
        Ok(McpServerReplacementReceipt {
            count: names.len(),
            names,
        })
    }
    pub async fn prompt_content(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        prompt: Prompt,
    ) -> Result<PromptReceipt, Error> {
        self.inner.prompt_content(id, turn_id.into(), prompt).await
    }
    pub async fn prompt_blocks(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        blocks: Vec<PromptBlock>,
    ) -> Result<PromptReceipt, Error> {
        self.prompt_content(
            id,
            turn_id,
            Prompt {
                blocks,
                metadata: serde_json::Value::Null,
            },
        )
        .await
    }
    pub async fn set_mode(&self, id: &SessionId, mode: impl Into<String>) -> Result<(), Error> {
        self.inner.set_mode(id, mode.into()).await
    }
    pub async fn list_sessions(&self) -> Result<serde_json::Value, Error> {
        self.inner.list_sessions().await
    }
    pub async fn close_session(&self, id: SessionId) -> Result<(), Error> {
        self.inner.close_session(id).await
    }
    pub async fn create_session(&self, config: SessionConfig) -> Result<SessionId, Error> {
        self.inner.create_session(config).await
    }
    pub async fn load_session(&self, id: SessionId, config: SessionConfig) -> Result<(), Error> {
        self.inner.load_session(id, config).await
    }
    /// Resumes a durable session without replaying its historical updates.
    /// Use [`Self::load_session`] when the host needs history replay to rebuild
    /// a fresh event journal.
    pub async fn resume_session(&self, id: SessionId, config: SessionConfig) -> Result<(), Error> {
        self.inner.resume_session(id, config).await
    }
    pub async fn prompt(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<PromptReceipt, Error> {
        self.inner.prompt(id, turn_id.into(), text.into()).await
    }
    /// Returns retained events whose sequence is strictly greater than
    /// `after_sequence`. Unknown or currently unloaded sessions fail closed.
    pub async fn events_after(
        &self,
        id: &SessionId,
        after_sequence: u64,
    ) -> Result<Vec<Event>, Error> {
        self.inner.events_after(id, after_sequence).await
    }
    pub async fn cancel(&self, id: &SessionId) -> Result<(), Error> {
        self.inner.cancel(id).await
    }
    pub async fn session_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        self.inner.session_ledger(id).await
    }
    pub async fn mark_turn_discarded(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        prompt_digest: impl Into<String>,
        runtime_prompt_index: u64,
    ) -> Result<(), Error> {
        self.inner
            .mark_turn_discarded(
                id,
                turn_id.into(),
                prompt_digest.into(),
                runtime_prompt_index,
            )
            .await
    }
    /// Changes only the SDK sampling route for an existing conversation.
    /// This never rebuilds the harness or rewrites the system prompt.
    pub async fn set_route(
        &self,
        id: &SessionId,
        model: impl Into<String>,
        reasoning: Option<String>,
    ) -> Result<(), Error> {
        self.inner.set_route(id, model.into(), reasoning).await
    }
    pub async fn rewind_points(&self, id: &SessionId) -> Result<Vec<RewindPoint>, Error> {
        self.inner.rewind_points(id).await
    }
    pub async fn rewind_conversation(
        &self,
        id: &SessionId,
        operation_id: impl Into<String>,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.inner
            .rewind_conversation(id, operation_id.into(), target_prompt_index)
            .await
    }
    /// Removes the exact product-unsettled tail Turn after an SDK host
    /// restart. The native Turn may still be pending or may have completed
    /// before the host durably recorded its settlement; unlike a user rewind,
    /// this requires the full ledger identity.
    pub async fn rewind_unsettled_turn(
        &self,
        id: &SessionId,
        operation_id: impl Into<String>,
        turn_id: impl Into<String>,
        prompt_digest: impl Into<String>,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.inner
            .rewind_unsettled_turn(
                id,
                operation_id.into(),
                turn_id.into(),
                prompt_digest.into(),
                target_prompt_index,
            )
            .await
    }
    pub async fn rewind_status(
        &self,
        id: &SessionId,
        operation_id: &str,
    ) -> Result<ConversationRewindStatus, Error> {
        self.inner.rewind_status(id, operation_id).await
    }
    pub async fn unload_session(&self, id: SessionId) -> Result<(), Error> {
        self.inner.unload_session(id).await
    }
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.inner.shutdown().await
    }
}

pub(crate) fn measured_session_turn_usage(
    mut usage: run::EffectUsage,
    artifact_bytes: u64,
) -> Result<run::EffectUsage, Error> {
    usage.resources.artifact_bytes = artifact_bytes;
    usage.unknown.remove(&run::ResourceDimension::ArtifactBytes);
    usage.validate()?;
    Ok(usage)
}

fn recovered_session_turn_usage(mut usage: run::EffectUsage) -> Result<run::EffectUsage, Error> {
    // SessionLedger proves native Turn usage, but cannot prove whether the
    // SDK output artifact was persisted immediately before a crash. Keep the
    // whole dimension explicitly unknown instead of fabricating input-only
    // accounting.
    usage.resources.artifact_bytes = 0;
    usage.unknown.insert(run::ResourceDimension::ArtifactBytes);
    usage.validate()?;
    Ok(usage)
}

pub struct RuntimeBuilder {
    config: RuntimeConfig,
    options: RuntimeOptions,
    run_store: Option<Arc<dyn run::RunStore>>,
}
impl RuntimeBuilder {
    pub fn profile(mut self, value: RuntimeProfile) -> Self {
        self.options.profile = value;
        if value == RuntimeProfile::Desktop {
            self.options.yolo_mode = false;
        }
        self
    }
    pub fn client_identifier(mut self, value: impl Into<String>) -> Self {
        self.options.client_identifier = value.into();
        self
    }
    pub fn yolo_mode(mut self, value: bool) -> Self {
        self.options.yolo_mode = value;
        self
    }
    pub fn host_capabilities(mut self, value: HostCapabilities) -> Self {
        self.options.host_capabilities = value;
        self
    }
    pub fn event_journal_capacity(mut self, value: usize) -> Self {
        self.options.event_journal_capacity = value;
        self
    }
    /// Adds explicit skill roots. Restricted mode ignores them; Desktop mode
    /// loads them without requiring ambient process configuration.
    pub fn skill_paths(mut self, value: impl IntoIterator<Item = PathBuf>) -> Self {
        self.options.skill_paths = value.into_iter().collect();
        self
    }
    /// Adds explicit plugin roots. Restricted mode ignores them; Desktop mode
    /// resolves them using the normal plugin security checks.
    pub fn plugin_paths(mut self, value: impl IntoIterator<Item = PathBuf>) -> Self {
        self.options.plugin_paths = value.into_iter().collect();
        self
    }
    /// Supplies all custom model, subagent, auxiliary, image, and video API
    /// routing without consulting ambient process configuration.
    pub fn services(mut self, value: RuntimeServices) -> Self {
        self.options.services = value;
        self
    }
    /// Overrides one catalog model's provider. This is a convenience for
    /// hosts that do not need the rest of [`RuntimeServices`].
    pub fn model_provider(
        mut self,
        model_id: impl Into<String>,
        provider: ApiProviderConfig,
    ) -> Self {
        self.options
            .services
            .model_providers
            .insert(model_id.into(), provider);
        self
    }
    pub fn agent_services(mut self, value: AgentServiceConfig) -> Self {
        self.options.services.agents = value;
        self
    }
    pub fn media_service(mut self, value: MediaServiceConfig) -> Self {
        self.options.services.media = Some(value);
        self
    }
    pub fn mcp_servers(mut self, value: impl IntoIterator<Item = McpServerConfig>) -> Self {
        self.options.services.mcp_servers = value.into_iter().collect();
        self
    }
    /// Registers SDK-owned MCP servers for direct in-process dispatch. These
    /// are advertised and routable only in the Desktop profile.
    pub fn in_process_mcp_servers(
        mut self,
        value: impl IntoIterator<Item = InProcessMcpServer>,
    ) -> Self {
        self.options.in_process_mcp_servers = value.into_iter().collect();
        self
    }
    /// Installs typed roots, sampling, and elicitation services used to
    /// fulfill MCP 2026 MRTR input requests. Capability advertisement is
    /// derived from the installed services and cannot be enabled separately.
    pub fn mcp_host_services(mut self, value: McpHostServices) -> Self {
        self.options.mcp_host_services = value;
        self
    }
    /// Registers typed reverse-channel hooks. Hooks are enabled only by the
    /// Desktop profile; Restricted never advertises or routes them.
    pub fn agent_hooks(mut self, value: impl IntoIterator<Item = AgentHookRegistration>) -> Self {
        self.options.agent_hooks = value.into_iter().collect();
        self
    }
    pub fn host_delegate(mut self, value: Arc<dyn HostDelegate>) -> Self {
        self.options.host = Some(value);
        self
    }
    /// Installs the typed tool policy used ahead of `HostDelegate` in Desktop mode.
    pub fn tool_permission_handler(mut self, value: Arc<dyn ToolPermissionHandler>) -> Self {
        self.options.tool_permission_handler = Some(value);
        self
    }
    /// Replaces the standalone SQLite Run store with the Host's single
    /// acknowledged Run authority. This is not an event mirror or write-through
    /// cache: the SDK reducer commits only to this store.
    pub fn run_store(mut self, value: Arc<dyn run::RunStore>) -> Self {
        self.run_store = Some(value);
        self
    }
    pub async fn start(self) -> Result<(Runtime, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start_with_run_store(self.config, self.options, self.run_store)
            .await
            .map(|(inner, events)| (Runtime { inner }, events))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceProvenance {
    pub upstream_release: &'static str,
    pub fork_commit: &'static str,
    pub upstream_source_rev: &'static str,
    pub facade_version: &'static str,
    pub dirty: bool,
}
pub fn source_provenance() -> SourceProvenance {
    SourceProvenance {
        upstream_release: "1.0.0",
        fork_commit: env!("GROK_BUILD_SDK_COMMIT"),
        upstream_source_rev: include_str!("../../../SOURCE_REV").trim(),
        facade_version: env!("CARGO_PKG_VERSION"),
        dirty: match env!("GROK_BUILD_SDK_DIRTY") {
            "true" => true,
            "false" => false,
            _ => panic!("build script emitted an invalid dirty marker"),
        },
    }
}

pub fn prompt_digest(text: &str) -> String {
    xai_grok_shell::origin_runtime::prompt_digest(text)
}
pub fn prompt_digest_content(prompt: &Prompt) -> Result<String, Error> {
    use sha2::Digest as _;
    let mut value = serde_json::to_value(prompt).map_err(|e| Error::Operation(e.to_string()))?;
    canonicalize_json(&mut value);
    let canonical = serde_json::to_vec(&value).map_err(|e| Error::Operation(e.to_string()))?;
    let mut digest = sha2::Sha256::new();
    // Compatibility identifier: persisted ledgers created before the public
    // crate rename must keep the same digest and rewind identity.
    digest.update(b"origin-grok-runtime.prompt.v2\0");
    digest.update(canonical);
    Ok(format!("sha256-v2:{:x}", digest.finalize()))
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        serde_json::Value::Object(object) => {
            for value in object.values_mut() {
                canonicalize_json(value);
            }
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            object.extend(entries);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use tempfile::TempDir;
    use xai_grok_test_support::{
        InferenceEndpoint, InferenceRequestMatcher, MockInferenceServer, ScriptedResponse, SseEvent,
    };

    #[test]
    fn typed_mcp_content_preserves_known_and_unknown_protocol_blocks() {
        let result = parse_tool_result(serde_json::json!({
            "content": [
                {
                    "type": "text",
                    "text": "hello",
                    "annotations": {"audience": ["user"]},
                    "_meta": {"trace": 7}
                },
                {"type": "futureBlock", "payload": {"answer": 42}}
            ],
            "structuredContent": {"ok": true},
            "isError": false,
            "_meta": {"requestId": "req-1"}
        }))
        .expect("typed MCP result");

        assert!(matches!(
            &result.content[0],
            McpContent::Text { text, raw }
                if text == "hello"
                    && raw["annotations"]["audience"][0] == "user"
                    && raw["_meta"]["trace"] == 7
        ));
        assert!(matches!(
            &result.content[1],
            McpContent::Unknown { raw } if raw["payload"]["answer"] == 42
        ));
        assert_eq!(result.structured_content.unwrap()["ok"], true);
        assert_eq!(result.meta.unwrap()["requestId"], "req-1");
    }

    #[test]
    fn typed_mcp_catalog_is_allowlist_redacted() {
        let servers = parse_mcp_servers(&serde_json::json!({
            "servers": [{
                "name": "fixture",
                "source": "local",
                "type": "stdio",
                "command": "/secret/command",
                "args": ["--token", "argument-secret"],
                "env": [{"name": "TOKEN", "value": "environment-secret"}],
                "setupValues": {"token": "setup-secret"},
                "session": {
                    "enabled": true,
                    "status": "ready",
                    "tools": [{"name": "echo", "enabled": true}]
                }
            }]
        }))
        .expect("catalog parses");

        let json = serde_json::to_string(&servers).expect("summary serializes");
        for secret in [
            "/secret/command",
            "argument-secret",
            "environment-secret",
            "setup-secret",
        ] {
            assert!(!json.contains(secret), "redacted catalog leaked {secret}");
        }
        assert_eq!(servers[0].transport, McpTransportKind::Stdio);
        assert_eq!(servers[0].tools[0].name, "echo");
    }

    fn test_subscription(
        values: impl IntoIterator<Item = serde_json::Value>,
        terminal_value: Option<serde_json::Value>,
    ) -> McpSubscription {
        let (tx, events) = tokio::sync::mpsc::channel(1);
        for value in values {
            tx.try_send(value).expect("fixture event fits");
        }
        drop(tx);
        let (terminal_tx, terminal) = tokio::sync::oneshot::channel();
        if let Some(terminal_value) = terminal_value {
            terminal_tx
                .send(terminal_value)
                .expect("fixture terminal receiver is open");
        } else {
            // Model a live producer so queued notification fixtures are parsed
            // before any synthetic terminal closure.
            std::mem::forget(terminal_tx);
        }
        let (cancel, _cancelled) = tokio::sync::oneshot::channel();
        McpSubscription {
            session_id: SessionId("subscription-test".into()),
            server: "fixture".into(),
            client_id: 7,
            acknowledged: McpSubscriptionFilter::default(),
            events,
            terminal,
            cancel: Some(cancel),
            pending_end: None,
            ended: false,
        }
    }

    #[tokio::test]
    async fn modern_mcp_subscription_decodes_all_terminal_states() {
        let fixtures = [
            (
                serde_json::json!({
                    "reason":"graceful",
                    "result":{"resultType":"complete"}
                }),
                McpSubscriptionEnd::Graceful,
            ),
            (
                serde_json::json!({"reason":"abrupt"}),
                McpSubscriptionEnd::Abrupt,
            ),
            (
                serde_json::json!({"reason":"cancelled"}),
                McpSubscriptionEnd::Cancelled,
            ),
            (
                serde_json::json!({"reason":"lagged","capacity":17}),
                McpSubscriptionEnd::Lagged { capacity: 17 },
            ),
            (
                serde_json::json!({"reason":"error","message":"closed"}),
                McpSubscriptionEnd::Error {
                    message: "closed".into(),
                },
            ),
        ];
        for (raw, expected) in fixtures {
            let mut subscription = test_subscription([], Some(raw));
            assert_eq!(
                subscription.next().await.expect("terminal event"),
                Some(McpSubscriptionEvent::Ended(expected))
            );
            assert!(subscription.cancel.is_none());
        }
    }

    #[tokio::test]
    async fn modern_mcp_subscription_rejects_unknown_or_malformed_notifications() {
        for notification in [
            serde_json::json!({"method":"notifications/future"}),
            serde_json::json!({"method":"notifications/resources/updated","params":{}}),
            serde_json::json!({"params":{}}),
        ] {
            let mut subscription = test_subscription(
                [serde_json::json!({
                    "type":"notification",
                    "notification":notification
                })],
                None,
            );
            assert!(
                subscription.next().await.is_err(),
                "unknown or malformed subscription notifications must fail closed"
            );
        }
    }

    #[tokio::test]
    async fn modern_mcp_subscription_cancel_is_not_blocked_by_a_full_event_queue() {
        let mut subscription = test_subscription(
            [serde_json::json!({
                "type":"notification",
                "notification":{"method":"notifications/tools/list_changed"}
            })],
            None,
        );
        subscription.cancel();
        let event =
            tokio::time::timeout(std::time::Duration::from_millis(100), subscription.next())
                .await
                .expect("cancellation must not wait for event queue capacity")
                .expect("valid terminal event");
        assert_eq!(
            event,
            Some(McpSubscriptionEvent::Ended(McpSubscriptionEnd::Cancelled))
        );
    }

    #[test]
    fn modern_mcp_mrtr_and_task_parsers_reject_unknown_protocol_variants() {
        assert!(
            parse_input_required(serde_json::json!({
                "resultType":"input_required",
                "inputRequests":{"future":{"method":"future/input"}}
            }))
            .is_err()
        );
        assert!(
            parse_task(
                &SessionId("task-test".into()),
                "fixture",
                1,
                serde_json::json!({
                    "taskId":"future-task",
                    "status":"future_status",
                    "createdAt":"2026-08-09T00:00:00Z",
                    "lastUpdatedAt":"2026-08-09T00:00:00Z"
                })
            )
            .is_err()
        );
    }

    #[test]
    fn modern_mcp_continuations_are_bound_to_the_exact_origin() {
        let session = SessionId("continuation-session".into());
        let operation = McpOperationIdentity::Tool {
            name: "tool-a".into(),
            arguments: serde_json::json!({"value": 1}),
        };
        let outcome = parse_mcp_operation_outcome(
            &session,
            "server-a",
            serde_json::json!({
                "clientId": 41,
                "outcome": "input_required",
                "result": {
                    "resultType": "input_required",
                    "inputRequests": {"request-1": {"method": "roots/list"}},
                    "requestState": "opaque-state"
                }
            }),
            operation.clone(),
            parse_tool_result,
        )
        .expect("input requirement parses");
        let McpOperationOutcome::InputRequired { input, .. } = outcome else {
            panic!("expected input requirement");
        };
        assert!(input.respond(BTreeMap::new()).is_err());
        let continuation = input
            .respond(BTreeMap::from([(
                "request-1".into(),
                serde_json::json!({"roots": []}),
            )]))
            .expect("exact response IDs are accepted");

        let (responses, request_state, generation) =
            validate_mcp_continuation(Some(continuation.clone()), &session, "server-a", &operation)
                .expect("matching origin is accepted");
        assert_eq!(responses.expect("responses").len(), 1);
        assert_eq!(request_state.as_deref(), Some("opaque-state"));
        assert_eq!(generation, Some(41));

        for (other_session, other_server, other_operation) in [
            (
                SessionId("other-session".into()),
                "server-a",
                operation.clone(),
            ),
            (session.clone(), "server-b", operation.clone()),
            (
                session.clone(),
                "server-a",
                McpOperationIdentity::Tool {
                    name: "tool-b".into(),
                    arguments: serde_json::json!({"value": 1}),
                },
            ),
            (
                session.clone(),
                "server-a",
                McpOperationIdentity::Prompt {
                    name: "tool-a".into(),
                    arguments: None,
                },
            ),
        ] {
            assert!(
                validate_mcp_continuation(
                    Some(continuation.clone()),
                    &other_session,
                    other_server,
                    &other_operation,
                )
                .is_err(),
                "cross-origin continuation must fail closed"
            );
        }
    }

    #[derive(Clone, Debug)]
    struct MediaRequest {
        path: String,
        authorization: Option<String>,
        provider_header: Option<String>,
        body: serde_json::Value,
    }

    struct MediaMock {
        addr: std::net::SocketAddr,
        requests: Arc<Mutex<Vec<MediaRequest>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl MediaMock {
        async fn start() -> Self {
            use axum::{
                Json, Router,
                extract::OriginalUri,
                http::HeaderMap,
                routing::{get, post},
            };

            let requests = Arc::new(Mutex::new(Vec::new()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind media mock");
            let addr = listener.local_addr().expect("media mock address");
            let image_requests = requests.clone();
            let edit_requests = requests.clone();
            let video_requests = requests.clone();
            let video_poll_requests = requests.clone();
            let video_url = format!("http://{addr}/v1/video.mp4");
            let router = Router::new()
                .route(
                    "/v1/images/generations",
                    post(
                        move |OriginalUri(uri): OriginalUri,
                              headers: HeaderMap,
                              Json(body): Json<serde_json::Value>| {
                            let requests = image_requests.clone();
                            async move {
                                record_media_request(
                                    &requests,
                                    uri.path_and_query().expect("image request path").as_str(),
                                    &headers,
                                    body,
                                );
                                Json(serde_json::json!({"data":[{"b64_json":"aGVsbG8="}]}))
                            }
                        },
                    ),
                )
                .route(
                    "/v1/images/edits",
                    post(
                        move |OriginalUri(uri): OriginalUri,
                              headers: HeaderMap,
                              Json(body): Json<serde_json::Value>| {
                            let requests = edit_requests.clone();
                            async move {
                                record_media_request(
                                    &requests,
                                    uri.path_and_query()
                                        .expect("image edit request path")
                                        .as_str(),
                                    &headers,
                                    body,
                                );
                                Json(serde_json::json!({"data":[{"b64_json":"aGVsbG8="}]}))
                            }
                        },
                    ),
                )
                .route(
                    "/v1/videos/generations",
                    post(
                        move |OriginalUri(uri): OriginalUri,
                              headers: HeaderMap,
                              Json(body): Json<serde_json::Value>| {
                            let requests = video_requests.clone();
                            async move {
                                record_media_request(
                                    &requests,
                                    uri.path_and_query().expect("video request path").as_str(),
                                    &headers,
                                    body,
                                );
                                Json(serde_json::json!({"request_id":"video-1"}))
                            }
                        },
                    ),
                )
                .route(
                    "/v1/videos/{id}",
                    get(move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                        let video_url = video_url.clone();
                        let requests = video_poll_requests.clone();
                        async move {
                            record_media_request(
                                &requests,
                                uri.path_and_query()
                                    .expect("video poll request path")
                                    .as_str(),
                                &headers,
                                serde_json::Value::Null,
                            );
                            Json(serde_json::json!({
                                "status":"done",
                                "video":{"url":video_url}
                            }))
                        }
                    }),
                )
                .route(
                    "/v1/video.mp4",
                    get(|| async { ([("content-type", "video/mp4")], "mock-video") }),
                );
            let task = tokio::spawn(async move {
                axum::serve(listener, router)
                    .await
                    .expect("media mock serves");
            });
            Self {
                addr,
                requests,
                task,
            }
        }

        fn url(&self) -> String {
            format!("http://{}/v1", self.addr)
        }

        fn requests(&self) -> Vec<MediaRequest> {
            self.requests.lock().expect("media requests").clone()
        }
    }

    fn record_media_request(
        requests: &Mutex<Vec<MediaRequest>>,
        path: &str,
        headers: &axum::http::HeaderMap,
        body: serde_json::Value,
    ) {
        requests.lock().expect("media requests").push(MediaRequest {
            path: path.into(),
            authorization: headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            provider_header: headers
                .get("x-origin-provider")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            body,
        });
    }

    impl Drop for MediaMock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    struct McpHttpMock {
        url: String,
        tools_listed: Arc<AtomicBool>,
        headers: Arc<Mutex<Vec<(String, String)>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl McpHttpMock {
        async fn start() -> Self {
            use axum::{
                Json, Router,
                extract::State,
                http::{HeaderMap, StatusCode},
                response::{IntoResponse, Response},
                routing::post,
            };

            #[derive(Clone)]
            struct McpState {
                tools_listed: Arc<AtomicBool>,
                headers: Arc<Mutex<Vec<(String, String)>>>,
            }

            async fn handle(
                State(state): State<McpState>,
                headers: HeaderMap,
                Json(request): Json<serde_json::Value>,
            ) -> Response {
                match request["method"].as_str() {
                    Some("server/discover") => (
                        [("mcp-session-id", "origin-runtime-http-test")],
                        Json(serde_json::json!({
                            "jsonrpc":"2.0",
                            "id":request["id"],
                            "result":{
                                "resultType":"complete",
                                "supportedVersions":["2026-07-28"],
                                "capabilities":{"tools":{}},
                                "ttlMs":0,
                                "cacheScope":"private",
                                "_meta":{"io.modelcontextprotocol/serverInfo":{
                                    "name":"origin-runtime-http-test",
                                    "version":"1"
                                }}
                            }
                        })),
                    )
                        .into_response(),
                    Some("tools/list") => {
                        let authorization = headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned();
                        let provider = headers
                            .get("x-origin-mcp")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned();
                        state
                            .headers
                            .lock()
                            .expect("MCP HTTP headers")
                            .push((authorization, provider));
                        state.tools_listed.store(true, Ordering::Release);
                        Json(serde_json::json!({
                            "jsonrpc":"2.0",
                            "id":request["id"],
                            "result":{"tools":[{
                                "name":"echo",
                                "description":"echo",
                                "inputSchema":{"type":"object"}
                            }]}
                        }))
                        .into_response()
                    }
                    _ => StatusCode::ACCEPTED.into_response(),
                }
            }

            let tools_listed = Arc::new(AtomicBool::new(false));
            let headers = Arc::new(Mutex::new(Vec::new()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind MCP HTTP mock");
            let addr = listener.local_addr().expect("MCP HTTP mock address");
            let router = Router::new()
                .route("/mcp", post(handle))
                .with_state(McpState {
                    tools_listed: tools_listed.clone(),
                    headers: headers.clone(),
                });
            let task = tokio::spawn(async move {
                axum::serve(listener, router)
                    .await
                    .expect("MCP HTTP mock serves");
            });
            Self {
                url: format!("http://{addr}/mcp"),
                tools_listed,
                headers,
                task,
            }
        }

        async fn wait_for_tools(&self) {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !self.tools_listed.load(Ordering::Acquire) {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("MCP HTTP discovery and tools/list complete");
        }

        fn headers(&self) -> Vec<(String, String)> {
            self.headers.lock().expect("MCP HTTP headers").clone()
        }
    }

    impl Drop for McpHttpMock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[derive(Default)]
    struct RecordingHost {
        allow: AtomicBool,
        slow_terminal_wait: AtomicBool,
        requests: Mutex<Vec<HostRequest>>,
        notifications: Mutex<Vec<HostNotification>>,
    }

    impl RecordingHost {
        fn approving() -> Self {
            Self {
                allow: AtomicBool::new(true),
                ..Self::default()
            }
        }

        fn request_methods(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("requests lock")
                .iter()
                .map(|request| request.method.clone())
                .collect()
        }

        fn notifications(&self) -> Vec<HostNotification> {
            self.notifications
                .lock()
                .expect("notifications lock")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl HostDelegate for RecordingHost {
        async fn request(&self, request: HostRequest) -> Result<serde_json::Value, HostError> {
            self.requests
                .lock()
                .expect("requests lock")
                .push(request.clone());
            match request.method.as_str() {
                "session/request_permission" => {
                    let wanted = if self.allow.load(Ordering::Acquire) {
                        "allow_once"
                    } else {
                        "reject_once"
                    };
                    let option_id = request.params["options"]
                        .as_array()
                        .and_then(|options| options.iter().find(|option| option["kind"] == wanted))
                        .and_then(|option| option["optionId"].as_str())
                        .ok_or_else(|| HostError {
                            code: -32602,
                            message: format!("permission request omitted {wanted}"),
                            data: request.params.clone(),
                        })?;
                    Ok(serde_json::json!({
                        "outcome": {"outcome":"selected", "optionId":option_id}
                    }))
                }
                "fs/read_text_file" => {
                    let path = request.params["path"].as_str().ok_or_else(|| HostError {
                        code: -32602,
                        message: "missing path".into(),
                        data: request.params.clone(),
                    })?;
                    let content = std::fs::read_to_string(path).map_err(|error| HostError {
                        code: -32000,
                        message: error.to_string(),
                        data: serde_json::json!({"path":path}),
                    })?;
                    Ok(serde_json::json!({"content":content}))
                }
                "fs/write_text_file" => {
                    let path = request.params["path"].as_str().ok_or_else(|| HostError {
                        code: -32602,
                        message: "missing path".into(),
                        data: request.params.clone(),
                    })?;
                    let content = request.params["content"]
                        .as_str()
                        .ok_or_else(|| HostError {
                            code: -32602,
                            message: "missing content".into(),
                            data: request.params.clone(),
                        })?;
                    std::fs::write(path, content).map_err(|error| HostError {
                        code: -32000,
                        message: error.to_string(),
                        data: serde_json::json!({"path":path}),
                    })?;
                    Ok(serde_json::json!({}))
                }
                "terminal/create" => Ok(serde_json::json!({"terminalId":"host-terminal-1"})),
                "terminal/wait_for_exit" => {
                    if self.slow_terminal_wait.load(Ordering::Acquire) {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    Ok(serde_json::json!({"exitCode":0}))
                }
                "terminal/output" => Ok(serde_json::json!({
                    "output":"terminal-from-host\n",
                    "truncated":false,
                    "exitStatus":{"exitCode":0}
                })),
                "terminal/kill" | "terminal/release" => Ok(serde_json::json!({})),
                "x.ai/folder_trust/request" => Ok(serde_json::json!({"outcome":"reject"})),
                method => Err(HostError {
                    code: -32601,
                    message: format!("unsupported host method: {method}"),
                    data: request.params,
                }),
            }
        }

        async fn notification(&self, notification: HostNotification) -> Result<(), HostError> {
            self.notifications
                .lock()
                .expect("notifications lock")
                .push(notification);
            Ok(())
        }
    }

    fn runtime_config(root: &TempDir, endpoint: String) -> RuntimeConfig {
        RuntimeConfig {
            endpoint,
            api_key: "test-key".into(),
            grok_home: root.path().join("grok"),
            session_storage: root.path().join("sessions"),
            models: vec![ModelSpec {
                id: "test-model".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: false,
                default_reasoning: None,
                reasoning_options: Vec::new(),
            }],
        }
    }

    fn session_config(cwd: PathBuf) -> SessionConfig {
        SessionConfig {
            cwd,
            model: "test-model".into(),
            reasoning: None,
            system_prompt: None,
            rules: None,
        }
    }

    fn provider(
        base_url: String,
        api_key: &str,
        model: &str,
        header_value: &str,
    ) -> ApiProviderConfig {
        ApiProviderConfig {
            base_url,
            api_key: api_key.into(),
            model: Some(model.into()),
            headers: BTreeMap::from([("x-origin-provider".into(), header_value.into())]),
            query_params: BTreeMap::new(),
        }
    }

    fn media_provider(base_url: String, api_key: &str, header_value: &str) -> MediaProviderConfig {
        MediaProviderConfig {
            base_url,
            api_key: api_key.into(),
            headers: BTreeMap::from([("x-origin-provider".into(), header_value.into())]),
            query_params: BTreeMap::from([("tenant".into(), "media".into())]),
        }
    }

    fn request_with_user_marker(server: &MockInferenceServer, marker: &str) -> serde_json::Value {
        server
            .requests()
            .into_iter()
            .filter(|entry| {
                entry.path.contains("chat/completions") || entry.path.contains("responses")
            })
            .filter_map(|entry| entry.body)
            .find(|body| {
                body.get("tools")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|tools| !tools.is_empty())
                    && body.get("tool_choice").is_none()
                    && body
                        .get("messages")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|message| message.get("content"))
                        .any(|content| content.as_str().is_some_and(|text| text.contains(marker)))
            })
            .expect("foreground inference request with marker")
    }

    fn message_prefix_is_unchanged(earlier: &serde_json::Value, later: &serde_json::Value) -> bool {
        let earlier = earlier
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .expect("earlier chat messages");
        let later = later
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .expect("later chat messages");
        later.starts_with(earlier)
    }

    fn chat_tool_call(call_id: &str, name: &str, arguments: &str) -> ScriptedResponse {
        let tool_calls = vec![serde_json::json!({
            "index": 0,
            "id": call_id,
            "type": "function",
            "function": { "name": name, "arguments": arguments }
        })];
        ScriptedResponse::sse(vec![
            SseEvent::data(
                serde_json::json!({
                    "id": "chatcmpl-origin-tool",
                    "object": "chat.completion.chunk",
                    "created": 1234567890,
                    "model": "test-model",
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": null,
                            "tool_calls": tool_calls
                        },
                        "finish_reason": null
                    }]
                })
                .to_string(),
            ),
            SseEvent::data(
                serde_json::json!({
                    "id": "chatcmpl-origin-tool",
                    "object": "chat.completion.chunk",
                    "created": 1234567890,
                    "model": "test-model",
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "tool_calls"
                    }],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 20,
                        "total_tokens": 30
                    }
                })
                .to_string(),
            ),
            SseEvent::data("[DONE]"),
        ])
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_model_providers_route_endpoint_auth_headers_and_wire_model() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let fast = MockInferenceServer::start().await.expect("fast provider");
        let deep = MockInferenceServer::start().await.expect("deep provider");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, String::new());
        config.api_key.clear();
        config.models.push(ModelSpec {
            id: "deep-model".into(),
            context_window: 65_536,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        });

        let mut fast_provider = provider(fast.url(), "fast-secret", "provider-fast", "fast");
        fast_provider
            .query_params
            .insert("tenant".into(), "fast".into());
        let mut deep_provider = provider(deep.url(), "deep-secret", "provider-deep", "deep");
        deep_provider
            .query_params
            .insert("tenant".into(), "deep".into());
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .model_provider("test-model", fast_provider)
            .model_provider("deep-model", deep_provider)
            .start()
            .await
            .expect("runtime starts solely from explicit providers");
        let fast_session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("fast session");
        runtime
            .prompt(&fast_session, "fast-turn", "provider fast marker")
            .await
            .expect("fast provider turn");
        let deep_session = runtime
            .create_session(SessionConfig {
                cwd: workspace,
                model: "deep-model".into(),
                reasoning: None,
                system_prompt: None,
                rules: None,
            })
            .await
            .expect("deep session");
        runtime
            .prompt(&deep_session, "deep-turn", "provider deep marker")
            .await
            .expect("deep provider turn");

        let fast_body = request_with_user_marker(&fast, "provider fast marker");
        let fast_request = fast
            .requests()
            .into_iter()
            .find(|request| request.body.as_ref() == Some(&fast_body))
            .expect("fast provider foreground request");
        assert_eq!(
            fast_request.authorization.as_deref(),
            Some("Bearer fast-secret")
        );
        assert_eq!(fast_request.header("x-origin-provider"), Some("fast"));
        assert_eq!(fast_request.path, "/v1/chat/completions?tenant=fast");
        assert_eq!(fast_body["model"], "provider-fast");
        let deep_body = request_with_user_marker(&deep, "provider deep marker");
        let deep_request = deep
            .requests()
            .into_iter()
            .find(|request| request.body.as_ref() == Some(&deep_body))
            .expect("deep provider foreground request");
        assert_eq!(
            deep_request.authorization.as_deref(),
            Some("Bearer deep-secret")
        );
        assert_eq!(deep_request.header("x-origin-provider"), Some("deep"));
        assert_eq!(deep_request.path, "/v1/chat/completions?tenant=deep");
        assert_eq!(deep_body["model"], "provider-deep");
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[test]
    fn desktop_explicit_provider_isolated_from_hostile_ambient_credentials() {
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "tests::desktop_explicit_provider_isolated_from_hostile_ambient_credentials_child",
                "--exact",
                "--nocapture",
            ])
            .env("ORIGIN_AMBIENT_CREDENTIAL_CHILD", "1")
            .env("XAI_API_KEY", "ambient-secret-must-not-leak")
            .env("GROK_DEPLOYMENT_KEY", "ambient-deployment-must-not-leak")
            .env("GROK_XAI_API_BASE_URL", "http://127.0.0.1:9/ambient")
            .output()
            .expect("run isolated credential child");
        assert!(
            output.status.success(),
            "isolated credential child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_explicit_provider_isolated_from_hostile_ambient_credentials_child() {
        if std::env::var_os("ORIGIN_AMBIENT_CREDENTIAL_CHILD").is_none() {
            return;
        }
        let _ = rustls::crypto::ring::default_provider().install_default();
        let explicit = MockInferenceServer::start()
            .await
            .expect("explicit provider");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, String::new());
        config.api_key.clear();
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .model_provider(
                "test-model",
                provider(
                    explicit.url(),
                    "explicit-secret",
                    "explicit-wire",
                    "explicit",
                ),
            )
            .start()
            .await
            .expect("runtime starts without ambient provider state");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt(
                &session,
                "ambient-isolation-turn",
                "ambient isolation marker",
            )
            .await
            .expect("explicit provider turn");

        let body = request_with_user_marker(&explicit, "ambient isolation marker");
        let request = explicit
            .requests()
            .into_iter()
            .find(|request| request.body.as_ref() == Some(&body))
            .expect("explicit foreground request");
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer explicit-secret")
        );
        assert_eq!(body["model"], "explicit-wire");
        assert!(
            !body["tools"].to_string().contains("web_search"),
            "an omitted auxiliary search role must remain disabled"
        );
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auxiliary_session_summary_uses_its_catalog_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let main = MockInferenceServer::start().await.expect("main provider");
        let utility = MockInferenceServer::start()
            .await
            .expect("utility provider");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, String::new());
        config.api_key.clear();
        config.models.push(ModelSpec {
            id: "utility-model".into(),
            context_window: 32_768,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        });
        let agent_services = AgentServiceConfig {
            session_summary_model: Some("utility-model".into()),
            ..AgentServiceConfig::default()
        };
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .model_provider(
                "test-model",
                provider(main.url(), "main-secret", "main-wire", "main"),
            )
            .model_provider(
                "utility-model",
                provider(utility.url(), "utility-secret", "utility-wire", "utility"),
            )
            .agent_services(agent_services)
            .start()
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt(&session, "summary-turn", "summarize provider routing")
            .await
            .expect("turn succeeds");

        let summary_wait = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !utility
                .requests()
                .iter()
                .any(|request| request.path == "/v1/chat/completions")
            {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await;
        if summary_wait.is_err() {
            let main_models = main
                .requests()
                .into_iter()
                .filter_map(|request| request.body)
                .filter_map(|body| body.get("model").cloned())
                .collect::<Vec<_>>();
            panic!("summary provider was not called; main request models: {main_models:?}");
        }
        let summary = utility
            .requests()
            .into_iter()
            .find(|request| request.path == "/v1/chat/completions")
            .expect("summary provider request");
        assert_eq!(
            summary.authorization.as_deref(),
            Some("Bearer utility-secret")
        );
        assert_eq!(summary.header("x-origin-provider"), Some("utility"));
        assert_eq!(summary.body.as_ref().unwrap()["model"], "utility-wire");
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auxiliary_image_description_uses_its_catalog_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let main = MockInferenceServer::start().await.expect("main provider");
        let vision = MockInferenceServer::start().await.expect("vision provider");
        vision.set_response("a blue rectangle");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, String::new());
        config.api_key.clear();
        config.models.push(ModelSpec {
            id: "vision-model".into(),
            context_window: 32_768,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        });
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .model_provider(
                "test-model",
                provider(main.url(), "main-secret", "main-wire", "main"),
            )
            .model_provider(
                "vision-model",
                provider(vision.url(), "vision-secret", "vision-wire", "vision"),
            )
            .agent_services(AgentServiceConfig {
                image_description_model: Some("vision-model".into()),
                ..AgentServiceConfig::default()
            })
            .start()
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt_blocks(
                &session,
                "vision-turn",
                vec![
                    PromptBlock::Text {
                        text: "describe image provider marker".into(),
                    },
                    PromptBlock::Image {
                        data: "iVBORw0KGgoAAAANSUhEUgAAACAAAAAQCAIAAAD4YuoOAAAAHUlEQVR42mPQqDhBU8QwasGoBaMWjFowasFQsAAAxdvQH+YmXBQAAAAASUVORK5CYII=".into(),
                        mime_type: "image/png".into(),
                        uri: None,
                    },
                ],
            )
            .await
            .expect("image prompt succeeds");

        let request = vision
            .requests()
            .into_iter()
            .find(|request| request.path == "/v1/chat/completions")
            .expect("vision provider request");
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer vision-secret")
        );
        assert_eq!(request.header("x-origin-provider"), Some("vision"));
        assert_eq!(request.body.as_ref().unwrap()["model"], "vision-wire");
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auxiliary_prompt_suggestion_uses_its_catalog_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let main = MockInferenceServer::start().await.expect("main provider");
        let suggestion = MockInferenceServer::start()
            .await
            .expect("suggestion provider");
        suggestion.set_response("continue");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, String::new());
        config.api_key.clear();
        config.models.push(ModelSpec {
            id: "suggestion-model".into(),
            context_window: 32_768,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        });
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .model_provider(
                "test-model",
                provider(main.url(), "main-secret", "main-wire", "main"),
            )
            .model_provider(
                "suggestion-model",
                provider(
                    suggestion.url(),
                    "suggestion-secret",
                    "suggestion-wire",
                    "suggestion",
                ),
            )
            .agent_services(AgentServiceConfig {
                prompt_suggestion_model: Some("suggestion-model".into()),
                ..AgentServiceConfig::default()
            })
            .start()
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt(
                &session,
                "suggestion-seed",
                "finish the task then suggest the next step",
            )
            .await
            .expect("seed turn succeeds");
        let response = runtime
            .extension_request(ExtensionRequest {
                method: "x.ai/suggestPrompt".into(),
                params: serde_json::json!({
                    "sessionId": session.as_str(),
                    "generation": 7
                }),
            })
            .await
            .expect("suggestion extension succeeds");

        assert_eq!(response.result["generation"], 7);
        assert_eq!(response.result["suggestion"], "continue");
        let request = suggestion
            .requests()
            .into_iter()
            .find(|request| request.path == "/v1/chat/completions")
            .expect("suggestion provider request");
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer suggestion-secret")
        );
        assert_eq!(request.header("x-origin-provider"), Some("suggestion"));
        assert_eq!(request.body.as_ref().unwrap()["model"], "suggestion-wire");
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auxiliary_web_search_uses_its_catalog_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let main = MockInferenceServer::start().await.expect("main provider");
        let search = MockInferenceServer::start().await.expect("search provider");
        let search_response = ScriptedResponse::json(
            200,
            serde_json::json!({
                "id": "resp_search",
                "object": "response",
                "created_at": 1234567890,
                "status": "completed",
                "model": "search-wire",
                "output": [{
                    "type": "message",
                    "id": "msg_search",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "current search result",
                        "annotations": []
                    }]
                }]
            }),
        );
        let search_call = search.expect_response(
            "custom web search request",
            InferenceRequestMatcher::auxiliary(InferenceEndpoint::Responses),
            search_response,
        );
        let tool_call = main.expect_response(
            "invoke web search tool",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "search-web",
                "web_search",
                r#"{"query":"current rust release","allowed_domains":["rust-lang.org"]}"#,
            ),
        );
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, String::new());
        config.api_key.clear();
        config.models.push(ModelSpec {
            id: "search-model".into(),
            context_window: 32_768,
            api_backend: ApiBackend::Responses,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        });
        let mut search_provider = provider(search.url(), "search-secret", "search-wire", "search");
        search_provider
            .query_params
            .insert("tenant".into(), "search".into());
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .model_provider(
                "test-model",
                provider(main.url(), "main-secret", "main-wire", "main"),
            )
            .model_provider("search-model", search_provider)
            .agent_services(AgentServiceConfig {
                web_search_model: Some("search-model".into()),
                ..AgentServiceConfig::default()
            })
            .start()
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt(&session, "search-turn", "search the current rust release")
            .await
            .expect("web search turn succeeds");
        tool_call.assert_satisfied();
        search_call.assert_satisfied();

        let request = search
            .requests()
            .into_iter()
            .find(|request| request.path == "/v1/responses?tenant=search")
            .expect("search provider request");
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer search-secret")
        );
        assert_eq!(request.header("x-origin-provider"), Some("search"));
        assert_eq!(request.body.as_ref().unwrap()["model"], "search-wire");
        assert_eq!(
            request.body.as_ref().unwrap()["input"],
            "current rust release"
        );
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_subagent_model_uses_its_configured_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let parent = MockInferenceServer::start().await.expect("parent provider");
        let child = MockInferenceServer::start().await.expect("child provider");
        let subagent_call = parent.expect_response(
            "spawn configured subagent",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "spawn-child",
                "spawn_subagent",
                r#"{"description":"provider routing","prompt":"answer from child","subagent_type":"general-purpose","background":false}"#,
            ),
        );
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, String::new());
        config.api_key.clear();
        config.models.push(ModelSpec {
            id: "child-model".into(),
            context_window: 65_536,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        });
        let mut agent_services = AgentServiceConfig::default();
        agent_services
            .subagent_models
            .insert("general-purpose".into(), "child-model".into());
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .model_provider(
                "test-model",
                provider(parent.url(), "parent-secret", "parent-wire", "parent"),
            )
            .model_provider(
                "child-model",
                provider(child.url(), "child-secret", "child-wire", "child"),
            )
            .agent_services(agent_services)
            .start()
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt(&session, "subagent-turn", "delegate this request")
            .await
            .expect("subagent turn succeeds");
        subagent_call.assert_satisfied();

        let child_request = child
            .requests()
            .into_iter()
            .find(|request| request.path == "/v1/chat/completions")
            .unwrap_or_else(|| {
                let parent_bodies = parent
                    .requests()
                    .into_iter()
                    .filter_map(|request| request.body)
                    .collect::<Vec<_>>();
                panic!("child provider received no inference; parent bodies: {parent_bodies:#?}")
            });
        assert_eq!(
            child_request.authorization.as_deref(),
            Some("Bearer child-secret")
        );
        assert_eq!(child_request.header("x-origin-provider"), Some("child"));
        assert_eq!(child_request.body.as_ref().unwrap()["model"], "child-wire");
        runtime.shutdown().await.expect("runtime shuts down");
    }

    struct InProcessFixture {
        contexts: Arc<std::sync::Mutex<Vec<InProcessMcpContext>>>,
    }
    #[async_trait::async_trait]
    impl InProcessMcpHandler for InProcessFixture {
        async fn handle(&self, message: serde_json::Value) -> Result<serde_json::Value, HostError> {
            let id = message.get("id").cloned();
            let result = match message["method"].as_str() {
                Some("server/discover") => serde_json::json!({
                    "resultType":"complete",
                    "supportedVersions":["2026-07-28"],
                    "capabilities":{"tools":{},"resources":{},"prompts":{},"completions":{}},
                    "ttlMs":0,
                    "cacheScope":"private",
                    "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"sdk-fixture","version":"1"}}
                }),
                Some("tools/list") => {
                    serde_json::json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]})
                }
                Some("tools/call") => {
                    serde_json::json!({"content":[{"type":"text","text":"in-process ok"}],"isError":false})
                }
                Some("resources/list") => {
                    serde_json::json!({"resources":[{"uri":"fixture://one","name":"one"}]})
                }
                Some("resources/templates/list") => {
                    serde_json::json!({"resourceTemplates":[{"uriTemplate":"fixture://{id}","name":"by id"}]})
                }
                Some("prompts/list") => {
                    serde_json::json!({"prompts":[{"name":"welcome","description":"Welcome prompt","arguments":[{"name":"who"}]}]})
                }
                Some("prompts/get") => {
                    serde_json::json!({"description":"rendered","messages":[{"role":"user","content":{"type":"text","text":format!("hello {}", message["params"]["arguments"]["who"].as_str().unwrap_or("world"))}}]})
                }
                Some("completion/complete") => {
                    serde_json::json!({"completion":{"values":["alice","alex"],"total":2,"hasMore":false}})
                }
                _ => {
                    return Ok(
                        serde_json::json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Method not found"}}),
                    );
                }
            };
            Ok(serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}))
        }

        async fn handle_with_context(
            &self,
            context: &InProcessMcpContext,
            message: serde_json::Value,
        ) -> Result<serde_json::Value, HostError> {
            self.contexts.lock().unwrap().push(context.clone());
            self.handle(message).await
        }
    }

    struct LiveGatewayMcpFixture {
        called: Arc<AtomicBool>,
    }
    #[async_trait::async_trait]
    impl InProcessMcpHandler for LiveGatewayMcpFixture {
        async fn handle(&self, message: serde_json::Value) -> Result<serde_json::Value, HostError> {
            let id = message.get("id").cloned();
            let result = match message["method"].as_str() {
                Some("server/discover") => serde_json::json!({
                    "resultType":"complete",
                    "supportedVersions":["2026-07-28"],
                    "capabilities":{"tools":{}},
                    "ttlMs":0,
                    "cacheScope":"private",
                    "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"live-sdk-fixture","version":"1"}}
                }),
                Some("tools/list") => serde_json::json!({
                    "tools":[{
                        "name":"gateway_probe",
                        "description":"Required verification tool. Call it with code LIVE_GATEWAY_E2E.",
                        "inputSchema":{
                            "type":"object",
                            "properties":{"code":{"type":"string"}},
                            "required":["code"]
                        }
                    }]
                }),
                Some("tools/call") => {
                    if message["params"]["arguments"]["code"] == "LIVE_GATEWAY_E2E" {
                        self.called.store(true, Ordering::Release);
                    }
                    serde_json::json!({
                        "content":[{"type":"text","text":"LIVE_MCP_TOOL_OK"}],
                        "structuredContent":{"verified":true},
                        "isError":false
                    })
                }
                _ => {
                    return Ok(serde_json::json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "error":{"code":-32601,"message":"Method not found"}
                    }));
                }
            };
            Ok(serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}))
        }
    }

    struct ModernMcpFixture {
        peer: std::sync::Mutex<Option<InProcessMcpPeer>>,
        task_status: std::sync::atomic::AtomicU8,
        subscription_cancelled: tokio::sync::Notify,
        subscription_cancellations: std::sync::atomic::AtomicU8,
        subscription_auto_complete: AtomicBool,
    }

    impl ModernMcpFixture {
        fn task(&self) -> serde_json::Value {
            match self.task_status.load(Ordering::Acquire) {
                0 => serde_json::json!({
                    "resultType": "complete",
                    "taskId": "fixture-task",
                    "status": "input_required",
                    "statusMessage": "needs roots",
                    "createdAt": "2026-08-09T00:00:00Z",
                    "lastUpdatedAt": "2026-08-09T00:00:01Z",
                    "ttlMs": 60000,
                    "pollIntervalMs": 10,
                    "inputRequests": {
                        "roots": {"method": "roots/list"}
                    }
                }),
                1 => serde_json::json!({
                    "resultType": "complete",
                    "taskId": "fixture-task",
                    "status": "completed",
                    "statusMessage": "done",
                    "createdAt": "2026-08-09T00:00:00Z",
                    "lastUpdatedAt": "2026-08-09T00:00:02Z",
                    "ttlMs": 60000,
                    "result": {
                        "resultType": "complete",
                        "content": [{"type":"text","text":"task complete"}],
                        "isError": false
                    }
                }),
                _ => serde_json::json!({
                    "resultType": "complete",
                    "taskId": "fixture-task",
                    "status": "cancelled",
                    "createdAt": "2026-08-09T00:00:00Z",
                    "lastUpdatedAt": "2026-08-09T00:00:03Z",
                    "ttlMs": 60000
                }),
            }
        }

        async fn notify_task(&self) {
            let peer = self.peer.lock().unwrap().clone();
            if let Some(peer) = peer {
                let _ = peer.notify("notifications/tasks", self.task()).await;
            }
        }
    }

    #[async_trait::async_trait]
    impl InProcessMcpHandler for ModernMcpFixture {
        async fn handle(&self, message: serde_json::Value) -> Result<serde_json::Value, HostError> {
            let id = message.get("id").cloned();
            if id.is_none() {
                if message["method"] == "notifications/cancelled" {
                    self.subscription_cancellations
                        .fetch_add(1, Ordering::AcqRel);
                    self.subscription_cancelled.notify_one();
                }
                return Ok(serde_json::Value::Null);
            }
            let result = match message["method"].as_str() {
                Some("server/discover") => serde_json::json!({
                    "resultType":"complete",
                    "supportedVersions":["2026-07-28"],
                    "capabilities":{
                        "tools":{"listChanged":true},
                        "resources":{"listChanged":true,"subscribe":true},
                        "prompts":{"listChanged":true},
                        "extensions":{"io.modelcontextprotocol/tasks":{}}
                    },
                    "ttlMs":0,
                    "cacheScope":"private",
                    "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"modern-sdk-fixture","version":"1"}}
                }),
                Some("ping") => serde_json::json!({}),
                Some("tools/list") => serde_json::json!({
                    "tools":[
                        {"name":"mrtr","inputSchema":{"type":"object"}},
                        {"name":"task","inputSchema":{"type":"object"}}
                    ]
                }),
                Some("tools/call") if message["params"]["name"] == "mrtr" => {
                    if message["params"].get("inputResponses").is_none() {
                        serde_json::json!({
                            "resultType":"input_required",
                            "inputRequests":{"roots":{"method":"roots/list"}},
                            "requestState":"opaque-fixture-state"
                        })
                    } else if message["params"]["requestState"] == "opaque-fixture-state" {
                        serde_json::json!({
                            "resultType":"complete",
                            "content":[{"type":"text","text":"mrtr complete"}],
                            "isError":false
                        })
                    } else {
                        return Ok(serde_json::json!({
                            "jsonrpc":"2.0",
                            "id":id,
                            "error":{"code":-32602,"message":"request state mismatch"}
                        }));
                    }
                }
                Some("tools/call") if message["params"]["name"] == "task" => {
                    self.task_status.store(0, Ordering::Release);
                    serde_json::json!({
                        "resultType":"task",
                        "taskId":"fixture-task",
                        "status":"working",
                        "createdAt":"2026-08-09T00:00:00Z",
                        "lastUpdatedAt":"2026-08-09T00:00:00Z",
                        "ttlMs":60000,
                        "pollIntervalMs":10
                    })
                }
                Some("tasks/get") => self.task(),
                Some("tasks/update") => {
                    self.task_status.store(1, Ordering::Release);
                    self.notify_task().await;
                    serde_json::json!({"resultType":"complete"})
                }
                Some("tasks/cancel") => {
                    self.task_status.store(2, Ordering::Release);
                    self.notify_task().await;
                    serde_json::json!({"resultType":"complete"})
                }
                Some("subscriptions/listen") => {
                    let peer = self.peer.lock().unwrap().clone().ok_or_else(|| HostError {
                        code: -32603,
                        message: "subscription peer unavailable".into(),
                        data: serde_json::Value::Null,
                    })?;
                    let subscription_id = id.clone().unwrap_or_default();
                    peer.notify(
                        "notifications/subscriptions/acknowledged",
                        serde_json::json!({
                            "_meta":{"io.modelcontextprotocol/subscriptionId":subscription_id},
                            "notifications":{"toolsListChanged":true}
                        }),
                    )
                    .await?;
                    peer.notify(
                        "notifications/tools/list_changed",
                        serde_json::json!({
                            "_meta":{"io.modelcontextprotocol/subscriptionId":subscription_id}
                        }),
                    )
                    .await?;
                    if !self
                        .subscription_auto_complete
                        .swap(false, Ordering::AcqRel)
                    {
                        self.subscription_cancelled.notified().await;
                    }
                    serde_json::json!({
                        "resultType":"complete",
                        "_meta":{"io.modelcontextprotocol/subscriptionId":subscription_id}
                    })
                }
                _ => {
                    return Ok(serde_json::json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "error":{"code":-32601,"message":"Method not found"}
                    }));
                }
            };
            Ok(serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}))
        }

        async fn connected(
            &self,
            _context: &InProcessMcpContext,
            peer: InProcessMcpPeer,
        ) -> Result<(), HostError> {
            *self.peer.lock().unwrap() = Some(peer);
            Ok(())
        }
    }

    struct EmptyRootsService;

    #[allow(deprecated)]
    #[async_trait::async_trait]
    impl McpRootsService for EmptyRootsService {
        async fn list_roots(
            &self,
            _context: McpHostContext,
        ) -> Result<mcp_model::ListRootsResult, McpHostServiceError> {
            Ok(mcp_model::ListRootsResult::new(Vec::new()))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_routes_sdk_owned_in_process_mcp() {
        let sampling = MockInferenceServer::start()
            .await
            .expect("sampling provider");
        let root = TempDir::new().expect("root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let contexts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
            .profile(RuntimeProfile::Desktop)
            .in_process_mcp_servers([
                InProcessMcpServer::new(
                    "sdk-fixture",
                    "fixture-id",
                    Arc::new(InProcessFixture {
                        contexts: contexts.clone(),
                    }),
                ),
                InProcessMcpServer::new(
                    "sdk-fixture-two",
                    "fixture-id-two",
                    Arc::new(InProcessFixture {
                        contexts: contexts.clone(),
                    }),
                ),
            ])
            .start()
            .await
            .expect("desktop runtime");
        let session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("session");
        let tools = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(tools) = runtime.list_mcp_tools(&session, Some("sdk-fixture")).await
                    && !tools.is_empty()
                {
                    break tools;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("MCP initialization");
        assert_eq!(tools[0].name, "echo");
        let second_tools = runtime
            .list_mcp_tools(&session, Some("sdk-fixture-two"))
            .await
            .expect("second MCP server shares the actor binding");
        assert_eq!(second_tools[0].name, "echo");
        let servers = runtime
            .list_mcp_servers(&session, false)
            .await
            .expect("server capabilities");
        let negotiated = servers[0]
            .negotiated
            .as_ref()
            .expect("modern discovery metadata");
        assert_eq!(negotiated.protocol_version, "2026-07-28");
        assert!(negotiated.tools);
        assert!(negotiated.resources);
        assert!(negotiated.prompts);
        assert!(negotiated.completions);
        assert!(!negotiated.tasks);
        let result = runtime
            .call_mcp_tool(&session, "sdk-fixture", "echo", serde_json::json!({}))
            .await
            .expect("tool call");
        assert!(
            matches!(&result.content[0], McpContent::Text { text, .. } if text == "in-process ok")
        );
        runtime
            .call_mcp_tool(&session, "sdk-fixture-two", "echo", serde_json::json!({}))
            .await
            .expect("second MCP tool call");
        let observed = contexts.lock().unwrap().clone();
        assert!(!observed.is_empty());
        assert!(observed.iter().all(|context| {
            context.runtime_instance_id > 0
                && context.session_id == session
                && context.session_instance_id == 1
                && matches!(
                    (
                        context.server_name.as_str(),
                        context.registration_id.as_str()
                    ),
                    ("sdk-fixture", "fixture-id") | ("sdk-fixture-two", "fixture-id-two")
                )
        }));
        assert!(
            observed
                .iter()
                .any(|context| context.server_name == "sdk-fixture-two")
        );
        let resources = runtime
            .list_mcp_resources(&session, "sdk-fixture")
            .await
            .expect("resources and templates");
        assert_eq!(resources.resources[0].uri.as_deref(), Some("fixture://one"));
        assert_eq!(
            resources.templates[0].uri_template.as_deref(),
            Some("fixture://{id}")
        );
        let prompts = runtime
            .list_mcp_prompts(&session, "sdk-fixture")
            .await
            .expect("prompts");
        assert_eq!(prompts[0].name, "welcome");
        let prompt = runtime
            .get_mcp_prompt(
                &session,
                "sdk-fixture",
                "welcome",
                Some(serde_json::Map::from_iter([(
                    "who".into(),
                    serde_json::json!("sdk"),
                )])),
            )
            .await
            .expect("prompt get");
        assert_eq!(prompt.raw["messages"][0]["content"]["text"], "hello sdk");
        let completion = runtime
            .complete_mcp_argument(
                &session,
                "sdk-fixture",
                "prompt",
                "welcome",
                "who",
                "al",
                None,
            )
            .await
            .expect("completion");
        assert_eq!(completion.values, ["alice", "alex"]);
        assert!(matches!(
            runtime
                .replace_mcp_servers(
                    &session,
                    vec![McpServerConfig::Stdio {
                        name: "sdk-fixture".into(),
                        command: "/bin/false".into(),
                        args: Vec::new(),
                        env: BTreeMap::new(),
                    }],
                )
                .await,
            Err(Error::InvalidConfig(_))
        ));
        runtime
            .unload_session(session.clone())
            .await
            .expect("unload first session incarnation");
        contexts.lock().unwrap().clear();
        runtime
            .load_session(session.clone(), session_config(workspace))
            .await
            .expect("load second session incarnation");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if runtime
                    .list_mcp_tools(&session, Some("sdk-fixture"))
                    .await
                    .is_ok_and(|tools| !tools.is_empty())
                    && runtime
                        .list_mcp_tools(&session, Some("sdk-fixture-two"))
                        .await
                        .is_ok_and(|tools| !tools.is_empty())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("second MCP initialization");
        assert!(
            contexts
                .lock()
                .unwrap()
                .iter()
                .all(|context| context.session_instance_id == 2)
        );
        runtime.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn modern_mcp_mrtr_tasks_subscriptions_and_generation_safety() {
        let root = TempDir::new().expect("root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let fixture = Arc::new(ModernMcpFixture {
            peer: std::sync::Mutex::new(None),
            task_status: std::sync::atomic::AtomicU8::new(0),
            subscription_cancelled: tokio::sync::Notify::new(),
            subscription_cancellations: std::sync::atomic::AtomicU8::new(0),
            subscription_auto_complete: AtomicBool::new(false),
        });
        let (runtime, _) = Runtime::builder(runtime_config(&root, "http://127.0.0.1:1".to_owned()))
            .profile(RuntimeProfile::Desktop)
            .in_process_mcp_servers([InProcessMcpServer::new(
                "modern-fixture",
                "modern-fixture-id",
                fixture.clone(),
            )])
            .mcp_host_services(
                McpHostServices::default().with_roots(Arc::new(EmptyRootsService), true),
            )
            .start()
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("session starts");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if runtime
                    .list_mcp_tools(&session, Some("modern-fixture"))
                    .await
                    .is_ok_and(|tools| tools.len() == 2)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("modern fixture initializes");

        runtime
            .ping_mcp(&session, "modern-fixture")
            .await
            .expect("ping");
        runtime
            .notify_mcp_roots_list_changed(&session, "modern-fixture")
            .await
            .expect("authorized roots notification");

        let input = runtime
            .call_mcp_tool_once(
                &session,
                "modern-fixture",
                "mrtr",
                serde_json::json!({}),
                None,
            )
            .await
            .expect("first MRTR round");
        let input = match input {
            McpOperationOutcome::InputRequired { input, .. } => input,
            other => panic!("expected input_required, got {other:?}"),
        };
        assert_eq!(input.request_state.as_deref(), Some("opaque-fixture-state"));
        assert_eq!(input.requests[0].kind, McpInputRequestKind::Roots);
        let continuation = input
            .respond(BTreeMap::from([(
                input.requests[0].id.clone(),
                serde_json::json!({"roots": []}),
            )]))
            .expect("bound continuation");
        let stale_continuation = continuation.clone();
        let completed = runtime
            .call_mcp_tool_once(
                &session,
                "modern-fixture",
                "mrtr",
                serde_json::json!({}),
                Some(continuation),
            )
            .await
            .expect("second MRTR round");
        assert!(matches!(
            completed,
            McpOperationOutcome::Complete { result, .. }
                if matches!(&result.content[0], McpContent::Text { text, .. } if text == "mrtr complete")
        ));

        let task = runtime
            .call_mcp_tool_once(
                &session,
                "modern-fixture",
                "task",
                serde_json::json!({}),
                None,
            )
            .await
            .expect("Task creation");
        let handle = match task {
            McpOperationOutcome::Task { handle, .. } => handle,
            other => panic!("expected Task, got {other:?}"),
        };
        let pending = runtime.get_mcp_task(&handle).await.expect("Task status");
        assert_eq!(pending.status, McpTaskStatus::InputRequired);
        runtime
            .update_mcp_task(
                &handle,
                BTreeMap::from([("roots".into(), serde_json::json!({"roots": []}))]),
            )
            .await
            .expect("Task update");
        let completed = runtime.get_mcp_task(&handle).await.expect("completed Task");
        assert_eq!(completed.status, McpTaskStatus::Completed);
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if runtime
                    .events_after(&session, 0)
                    .await
                    .expect("events")
                    .iter()
                    .any(|event| {
                        matches!(
                            &event.update,
                            EventUpdate::McpTaskStatus(event)
                                if event.status == McpTaskStatus::Completed
                                    && event.handle == handle
                        )
                    })
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Task push event");

        let mut subscription = runtime
            .listen_mcp(
                &session,
                "modern-fixture",
                McpSubscriptionFilter {
                    tools_list_changed: true,
                    ..Default::default()
                },
                4,
            )
            .await
            .expect("subscription acknowledged");
        let stale_subscription_generation = subscription.client_id;
        assert!(subscription.acknowledged.tools_list_changed);
        assert!(matches!(
            subscription.next().await.expect("subscription event"),
            Some(McpSubscriptionEvent::ToolsListChanged)
        ));
        subscription.cancel();
        assert!(matches!(
            subscription.next().await.expect("subscription end"),
            Some(McpSubscriptionEvent::Ended(McpSubscriptionEnd::Cancelled))
        ));

        let mut full_subscription = runtime
            .listen_mcp(
                &session,
                "modern-fixture",
                McpSubscriptionFilter {
                    tools_list_changed: true,
                    ..Default::default()
                },
                1,
            )
            .await
            .expect("capacity-one subscription acknowledged");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        full_subscription.cancel();
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                full_subscription.next(),
            )
            .await
            .expect("full queue must not delay cancellation")
            .expect("typed cancellation"),
            Some(McpSubscriptionEvent::Ended(McpSubscriptionEnd::Cancelled))
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while fixture.subscription_cancellations.load(Ordering::Acquire) < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("transport cancellation must bypass the full SDK data queue");

        fixture
            .subscription_auto_complete
            .store(true, Ordering::Release);
        let mut server_completed_subscription = runtime
            .listen_mcp(
                &session,
                "modern-fixture",
                McpSubscriptionFilter {
                    tools_list_changed: true,
                    ..Default::default()
                },
                1,
            )
            .await
            .expect("server-completed subscription acknowledged");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                server_completed_subscription.next(),
            )
            .await
            .expect("server terminal must bypass a full data queue")
            .expect("typed server terminal"),
            Some(McpSubscriptionEvent::Ended(McpSubscriptionEnd::Graceful))
        ));

        let stale = handle.clone();
        runtime
            .unload_session(session.clone())
            .await
            .expect("unload");
        runtime
            .load_session(session.clone(), session_config(workspace))
            .await
            .expect("reload");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if runtime
                    .list_mcp_tools(&session, Some("modern-fixture"))
                    .await
                    .is_ok_and(|tools| tools.len() == 2)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("replacement client initializes");
        assert!(
            runtime.get_mcp_task(&stale).await.is_err(),
            "Task handle from an old client generation must fail closed"
        );
        assert!(
            runtime
                .call_mcp_tool_once(
                    &session,
                    "modern-fixture",
                    "mrtr",
                    serde_json::json!({}),
                    Some(stale_continuation),
                )
                .await
                .is_err(),
            "MRTR continuation from an old connection generation must fail closed"
        );
        assert_eq!(
            subscription.next().await.expect("ended subscription"),
            None,
            "an ended generation-bound subscription must not resume after reconnect"
        );
        let mut replacement_subscription = runtime
            .listen_mcp(
                &session,
                "modern-fixture",
                McpSubscriptionFilter {
                    tools_list_changed: true,
                    ..Default::default()
                },
                4,
            )
            .await
            .expect("replacement subscription");
        assert_ne!(
            replacement_subscription.client_id,
            stale_subscription_generation
        );
        replacement_subscription.cancel();
        runtime.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_external_and_in_process_mcp_name_collisions() {
        let root = TempDir::new().expect("root");
        let result = Runtime::builder(runtime_config(&root, "http://127.0.0.1:1".into()))
            .profile(RuntimeProfile::Desktop)
            .mcp_servers([McpServerConfig::Stdio {
                name: "same-name".into(),
                command: "/bin/false".into(),
                args: Vec::new(),
                env: BTreeMap::new(),
            }])
            .in_process_mcp_servers([InProcessMcpServer::new(
                "same-name",
                "fixture-id",
                Arc::new(InProcessFixture {
                    contexts: Arc::new(std::sync::Mutex::new(Vec::new())),
                }),
            )])
            .start()
            .await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restricted_profile_never_registers_in_process_mcp() {
        let root = TempDir::new().expect("root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let contexts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (runtime, _) = Runtime::builder(runtime_config(&root, "http://127.0.0.1:1".into()))
            .in_process_mcp_servers([InProcessMcpServer::new(
                "sdk-fixture",
                "fixture-id",
                Arc::new(InProcessFixture {
                    contexts: contexts.clone(),
                }),
            )])
            .start()
            .await
            .expect("restricted runtime");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("restricted session");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(contexts.lock().unwrap().is_empty());
        assert!(matches!(
            runtime.list_mcp_servers(&session, false).await,
            Err(Error::Operation(_))
        ));
        assert!(runtime.capabilities().features.iter().any(|feature| {
            feature.namespace == "sdk:in-process-mcp"
                && !feature.enabled
                && feature.disabled_reason.as_deref() == Some("restricted profile")
        }));
        runtime.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_discovers_and_executes_implement_skill_as_an_agent_command() {
        let sampling = MockInferenceServer::start()
            .await
            .expect("sampling provider");
        let root = TempDir::new().expect("root");
        let workspace = root.path().join("workspace");
        let skills = root.path().join("skills");
        let implement = skills.join("implement");
        std::fs::create_dir_all(&workspace).expect("workspace");
        std::fs::create_dir_all(&implement).expect("implement skill directory");
        std::fs::write(
            implement.join("SKILL.md"),
            r#"---
name: implement
description: Implement a requested software change completely.
argument-hint: change request
---
IMPLEMENT_SKILL_BODY: implement $ARGUMENTS and verify it.
"#,
        )
        .expect("implement skill");

        let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
            .profile(RuntimeProfile::Desktop)
            .skill_paths([skills])
            .start()
            .await
            .expect("desktop runtime");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session");
        let commands = runtime
            .list_agent_commands(&session)
            .await
            .expect("live command catalog");
        let implement = commands
            .iter()
            .find(|command| command.name == "implement")
            .expect("implement skill is advertised");
        assert_eq!(implement.input_hint.as_deref(), Some("change request"));
        assert!(commands.iter().any(|command| command.name == "loop"));
        assert!(
            runtime
                .execute_agent_command(&session, "unknown-turn", "not-a-command", None)
                .await
                .is_err(),
            "command execution must be allowlisted against the live catalog"
        );
        runtime
            .execute_agent_command(&session, "implement-turn", "implement", Some("feature-x"))
            .await
            .expect("implement command turn");
        assert!(sampling.requests().iter().any(|request| {
            request.body.as_ref().is_some_and(|body| {
                let body = body.to_string();
                body.contains("IMPLEMENT_SKILL_BODY") && body.contains("feature-x")
            })
        }));
        runtime.shutdown().await.expect("shutdown");

        let restricted_root = TempDir::new().expect("restricted root");
        let restricted_workspace = restricted_root.path().join("workspace");
        std::fs::create_dir(&restricted_workspace).expect("restricted workspace");
        let (restricted, _) = Runtime::builder(runtime_config(&restricted_root, sampling.url()))
            .skill_paths([root.path().join("skills")])
            .start()
            .await
            .expect("restricted runtime");
        let restricted_session = restricted
            .create_session(session_config(restricted_workspace))
            .await
            .expect("restricted session");
        assert!(
            restricted
                .list_agent_commands(&restricted_session)
                .await
                .is_err()
        );
        restricted.shutdown().await.expect("restricted shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_system_prompt_and_rules_reach_the_real_agent_prompt_builder() {
        let sampling = MockInferenceServer::start()
            .await
            .expect("sampling provider");
        let root = TempDir::new().expect("root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let (runtime, _) = Runtime::start(runtime_config(&root, sampling.url()))
            .await
            .expect("runtime");

        let mut override_config = session_config(workspace.clone());
        override_config.system_prompt = Some("SDK_SYSTEM_OVERRIDE".into());
        let override_session = runtime
            .create_session(override_config)
            .await
            .expect("override session");
        runtime
            .prompt(&override_session, "override-turn", "override-marker")
            .await
            .expect("override turn");
        let override_body = request_with_user_marker(&sampling, "override-marker");
        assert!(
            override_body["messages"][0]["content"]
                .as_str()
                .is_some_and(|prompt| prompt.starts_with("SDK_SYSTEM_OVERRIDE"))
        );

        let mut rules_config = session_config(workspace);
        rules_config.rules = Some("SDK_RULES_MARKER: never omit verification.".into());
        let rules_session = runtime
            .create_session(rules_config)
            .await
            .expect("rules session");
        runtime
            .prompt(&rules_session, "rules-turn", "rules-marker")
            .await
            .expect("rules turn");
        let rules_body = request_with_user_marker(&sampling, "rules-marker");
        let rules_prompt = rules_body["messages"][0]["content"]
            .as_str()
            .expect("rules system prompt");
        assert!(rules_prompt.contains("<human_rules>"));
        assert!(rules_prompt.contains("SDK_RULES_MARKER"));

        let mut blank = session_config(root.path().to_path_buf());
        blank.system_prompt = Some("  ".into());
        assert!(matches!(
            runtime.create_session(blank).await,
            Err(Error::InvalidConfig(_))
        ));
        runtime.shutdown().await.expect("shutdown");
    }

    /// Opt-in real gateway verification. No credential or response body is
    /// logged; run explicitly with `OG_AI_GATEWAY` and `OG_API_KEY` set.
    #[ignore = "requires the live OriginGame gateway and incurs a model request"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_gateway_model_calls_sdk_owned_mcp_and_journals_the_turn() {
        let endpoint = std::env::var("OG_AI_GATEWAY")
            .expect("OG_AI_GATEWAY")
            .trim_end_matches('/')
            .to_owned()
            + "/v1";
        let api_key = std::env::var("OG_API_KEY").expect("OG_API_KEY");
        let root = TempDir::new().expect("root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let called = Arc::new(AtomicBool::new(false));
        let config = RuntimeConfig {
            endpoint,
            api_key,
            grok_home: root.path().join("grok"),
            session_storage: root.path().join("sessions"),
            models: vec![ModelSpec {
                id: "grok-4.5".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: false,
                default_reasoning: None,
                reasoning_options: Vec::new(),
            }],
        };
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .yolo_mode(true)
            .in_process_mcp_servers([InProcessMcpServer::new(
                "live-sdk-fixture",
                "live-fixture-id",
                Arc::new(LiveGatewayMcpFixture {
                    called: called.clone(),
                }),
            )])
            .start()
            .await
            .expect("live runtime");
        let session = runtime
            .create_session(SessionConfig {
                cwd: workspace,
                model: "grok-4.5".into(),
                reasoning: None,
                system_prompt: None,
                rules: Some(
                    "When explicitly told to verify the gateway, call the supplied MCP tool before answering."
                        .into(),
                ),
            })
            .await
            .expect("live session");
        let receipt = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            runtime.prompt(
                &session,
                "live-gateway-turn",
                "Call the gateway_probe tool with code LIVE_GATEWAY_E2E. Only after the tool returns, answer LIVE_MCP_TOOL_OK.",
            ),
        )
        .await
        .expect("live gateway timeout")
        .expect("live gateway turn");
        assert_eq!(receipt.outcome, TurnOutcome::End);
        assert!(
            called.load(Ordering::Acquire),
            "the real model did not call MCP"
        );
        let journal = runtime
            .events_after(&session, 0)
            .await
            .expect("live journal");
        assert!(
            journal
                .iter()
                .any(|event| matches!(event.update, EventUpdate::ToolStart(_)))
        );
        assert_eq!(
            journal.last().map(|event| event.sequence),
            Some(receipt.final_sequence)
        );
        runtime.shutdown().await.expect("live runtime shutdown");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_starts_explicit_stdio_mcp_and_restricted_does_not() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let sampling = MockInferenceServer::start()
            .await
            .expect("sampling provider");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let marker = root.path().join("mcp-tools-listed");
        let script = root.path().join("mock-mcp.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{"tools":{},"resources":{},"prompts":{},"completions":{}},"ttlMs":0,"cacheScope":"private","_meta":{"io.modelcontextprotocol/serverInfo":{"name":"origin-runtime-test","version":"1"}}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      : > "$MCP_MARKER"
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo","description":"echo","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
    *'"method":"tools/call"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"ok","annotations":{"audience":["user"]}}],"structuredContent":{"fixture":true},"isError":false,"_meta":{"trace":"fixture"}}}\n' "$id"
      ;;
    *'"method":"resources/read"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"contents":[{"uri":"fixture://readme","mimeType":"text/plain","text":"fixture resource","_meta":{"revision":1}}]}}\n' "$id"
      ;;
    *'"method":"resources/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"resources":[{"uri":"fixture://readme","name":"readme"}]}}\n' "$id"
      ;;
    *'"method":"resources/templates/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"resourceTemplates":[{"uriTemplate":"fixture://{name}","name":"named"}]}}\n' "$id"
      ;;
    *'"method":"prompts/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"prompts":[{"name":"welcome","description":"welcome"}]}}\n' "$id"
      ;;
    *'"method":"prompts/get"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"messages":[{"role":"user","content":{"type":"text","text":"hello"}}]}}\n' "$id"
      ;;
    *'"method":"completion/complete"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"completion":{"values":["alpha"],"total":1,"hasMore":false}}}\n' "$id"
      ;;
  esac
done
"#,
        )
        .expect("MCP script");
        let mcp = McpServerConfig::Stdio {
            name: "fixture".into(),
            command: "/bin/sh".into(),
            args: vec![script.to_string_lossy().into_owned()],
            env: BTreeMap::from([
                ("MCP_MARKER".into(), marker.to_string_lossy().into_owned()),
                ("MCP_SECRET".into(), "catalog-secret".into()),
            ]),
        };

        let (restricted, _) = Runtime::builder(runtime_config(&root, sampling.url()))
            .mcp_servers([mcp.clone()])
            .start()
            .await
            .expect("restricted runtime starts");
        let restricted_session = restricted
            .create_session(session_config(workspace.clone()))
            .await
            .expect("restricted session starts");
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(!marker.exists(), "restricted profile must not start MCP");
        assert!(
            restricted
                .list_mcp_servers(&restricted_session, false)
                .await
                .is_err(),
            "restricted profile must reject typed MCP operations"
        );
        restricted
            .close_session(restricted_session)
            .await
            .expect("restricted session closes");
        restricted.shutdown().await.expect("restricted shuts down");

        let desktop_root = TempDir::new().expect("desktop root");
        let (desktop, _) = Runtime::builder(runtime_config(&desktop_root, sampling.url()))
            .profile(RuntimeProfile::Desktop)
            .mcp_servers([mcp.clone()])
            .start()
            .await
            .expect("desktop runtime starts");
        let session = desktop
            .create_session(session_config(workspace))
            .await
            .expect("desktop session starts");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !marker.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("MCP initialize and tools/list complete");

        let catalog = desktop
            .list_mcp_servers(&session, false)
            .await
            .expect("typed MCP catalog");
        let fixture = catalog
            .iter()
            .find(|server| server.name == "fixture")
            .expect("fixture in catalog");
        assert_eq!(fixture.transport, McpTransportKind::Stdio);
        assert_eq!(fixture.status, Some(McpServerStatus::Ready));
        assert_eq!(fixture.tools.len(), 1);
        assert!(
            !serde_json::to_string(fixture)
                .expect("catalog serializes")
                .contains("catalog-secret"),
            "typed catalog must not expose stdio environment values"
        );

        let tool_result = desktop
            .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({"value": 1}))
            .await
            .expect("direct MCP call");
        assert!(matches!(
            &tool_result.content[0],
            McpContent::Text { text, raw }
                if text == "ok" && raw["annotations"]["audience"][0] == "user"
        ));
        assert_eq!(tool_result.structured_content.unwrap()["fixture"], true);
        assert_eq!(tool_result.meta.unwrap()["trace"], "fixture");

        let resource = desktop
            .read_mcp_resource(&session, "fixture", "fixture://readme")
            .await
            .expect("direct MCP resource read");
        assert_eq!(
            resource.contents[0].text.as_deref(),
            Some("fixture resource")
        );
        assert_eq!(resource.contents[0].raw["_meta"]["revision"], 1);

        desktop
            .set_mcp_tool_enabled(&session, "fixture", "echo", false)
            .await
            .expect("disable MCP tool in session");
        let tools = desktop
            .list_mcp_tools(&session, Some("fixture"))
            .await
            .expect("list disabled tool");
        assert_eq!(tools.len(), 1);
        assert!(!tools[0].enabled);
        desktop
            .set_mcp_tool_enabled(&session, "fixture", "echo", true)
            .await
            .expect("re-enable MCP tool in session");

        desktop
            .set_mcp_server_enabled(&session, "fixture", false)
            .await
            .expect("disable MCP server in session");
        assert!(
            desktop
                .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({}))
                .await
                .is_err(),
            "disabled MCP server must not be callable"
        );
        desktop
            .set_mcp_server_enabled(&session, "fixture", true)
            .await
            .expect("re-enable MCP server in session");
        desktop
            .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({}))
            .await
            .expect("re-enabled MCP server is callable");

        let removed = desktop
            .replace_mcp_servers(&session, Vec::new())
            .await
            .expect("remove session MCP servers atomically");
        assert_eq!(removed.count, 0);
        assert!(
            desktop
                .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({}))
                .await
                .is_err(),
            "removed MCP server must not be callable"
        );
        let replaced = desktop
            .replace_mcp_servers(&session, vec![mcp])
            .await
            .expect("restore session MCP servers atomically");
        assert_eq!(replaced.names, vec!["fixture"]);
        desktop
            .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({}))
            .await
            .expect("replacement MCP server is callable");

        let scheduled = desktop
            .upsert_scheduled_task(
                &session,
                &ScheduledTaskRequest {
                    task_id: None,
                    interval: Some("5m".into()),
                    prompt: Some("inspect the fixture".into()),
                    recurring: true,
                    durable: Some(false),
                    foreground: Some(false),
                    fire_immediately: false,
                },
            )
            .await
            .expect("create scheduled loop without a model turn");
        assert!(!scheduled.updated);
        assert_eq!(scheduled.task.interval_seconds, 300);
        let tasks = desktop
            .list_scheduled_tasks(&session)
            .await
            .expect("list scheduled loops");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, scheduled.task.id);
        let updated = desktop
            .upsert_scheduled_task(
                &session,
                &ScheduledTaskRequest {
                    task_id: Some(scheduled.task.id.clone()),
                    interval: Some("10m".into()),
                    prompt: Some("inspect the updated fixture".into()),
                    recurring: true,
                    durable: None,
                    foreground: None,
                    fire_immediately: false,
                },
            )
            .await
            .expect("update scheduled loop in place");
        assert!(updated.updated);
        assert_eq!(updated.task.interval_seconds, 600);
        assert_eq!(updated.task.id, scheduled.task.id);
        let deleted = desktop
            .delete_scheduled_task(&session, &scheduled.task.id)
            .await
            .expect("delete scheduled loop");
        assert!(deleted.deleted);
        assert!(
            desktop
                .list_scheduled_tasks(&session)
                .await
                .expect("list after delete")
                .is_empty()
        );
        desktop
            .close_session(session)
            .await
            .expect("desktop session closes");
        desktop.shutdown().await.expect("desktop shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_starts_explicit_http_and_sse_mcp_with_host_headers() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let sampling = MockInferenceServer::start()
            .await
            .expect("sampling provider");
        let http = McpHttpMock::start().await;
        let sse = McpHttpMock::start().await;
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
            .profile(RuntimeProfile::Desktop)
            .mcp_servers([
                McpServerConfig::Http {
                    name: "http-fixture".into(),
                    url: http.url.clone(),
                    headers: BTreeMap::from([
                        ("authorization".into(), "Bearer http-secret".into()),
                        ("x-origin-mcp".into(), "http".into()),
                    ]),
                },
                McpServerConfig::Sse {
                    name: "sse-fixture".into(),
                    url: sse.url.clone(),
                    headers: BTreeMap::from([
                        ("authorization".into(), "Bearer sse-secret".into()),
                        ("x-origin-mcp".into(), "sse".into()),
                    ]),
                },
            ])
            .start()
            .await
            .expect("desktop runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("desktop session starts");
        tokio::join!(http.wait_for_tools(), sse.wait_for_tools());
        assert_eq!(
            http.headers(),
            vec![("Bearer http-secret".into(), "http".into())]
        );
        assert_eq!(
            sse.headers(),
            vec![("Bearer sse-secret".into(), "sse".into())]
        );
        runtime
            .close_session(session)
            .await
            .expect("desktop session closes");
        runtime.shutdown().await.expect("desktop shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_media_service_routes_image_generation_and_restricted_stays_closed() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let sampling = MockInferenceServer::start()
            .await
            .expect("sampling provider");
        let media = MediaMock::start().await;
        let restricted_call = sampling.expect_response(
            "restricted image tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "restricted-generate-image",
                "image_gen",
                r#"{"prompt":"must not run","aspect_ratio":"1:1"}"#,
            ),
        );
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let media_service = MediaServiceConfig {
            provider: media_provider(media.url(), "media-secret", "media"),
            image_generation: true,
            image_edit: false,
            video_generation: false,
            image_generation_model: Some("custom-image-model".into()),
            image_edit_model: None,
            image_to_video_model: None,
            reference_to_video_model: None,
        };
        let (restricted, _) = Runtime::builder(runtime_config(&root, sampling.url()))
            .media_service(media_service.clone())
            .start()
            .await
            .expect("restricted runtime starts");
        let restricted_image = restricted
            .capabilities()
            .features
            .into_iter()
            .find(|capability| capability.namespace == "feature:image_generation")
            .expect("image capability");
        assert!(!restricted_image.enabled);
        let restricted_session = restricted
            .create_session(session_config(workspace.clone()))
            .await
            .expect("restricted session starts");
        restricted
            .prompt(
                &restricted_session,
                "restricted-image-turn",
                "try to generate an image",
            )
            .await
            .expect("unknown restricted tool remains a normal turn outcome");
        restricted_call.assert_satisfied();
        assert!(
            media.requests().is_empty(),
            "Restricted must not send any media request"
        );
        restricted.shutdown().await.expect("restricted shuts down");

        let image_call = sampling.expect_response(
            "generate image tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "generate-image",
                "image_gen",
                r#"{"prompt":"a blue square","aspect_ratio":"1:1"}"#,
            ),
        );
        let desktop_root = TempDir::new().expect("desktop root");
        let (runtime, _) = Runtime::builder(runtime_config(&desktop_root, sampling.url()))
            .profile(RuntimeProfile::Desktop)
            .media_service(media_service)
            .start()
            .await
            .expect("desktop runtime starts");
        let image_capability = runtime
            .capabilities()
            .features
            .into_iter()
            .find(|capability| capability.namespace == "feature:image_generation")
            .expect("image capability");
        assert!(image_capability.enabled);
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt(&session, "image-turn", "generate an image")
            .await
            .expect("image turn succeeds");
        image_call.assert_satisfied();

        let media_request = media
            .requests()
            .into_iter()
            .find(|request| request.path == "/v1/images/generations?tenant=media")
            .expect("media API request");
        assert_eq!(
            media_request.authorization.as_deref(),
            Some("Bearer media-secret")
        );
        assert_eq!(media_request.provider_header.as_deref(), Some("media"));
        assert_eq!(media_request.body["model"], "custom-image-model");
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_media_service_wires_edit_and_both_video_models() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let sampling = MockInferenceServer::start()
            .await
            .expect("sampling provider");
        let media = MediaMock::start().await;
        let image = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAQCAIAAAD4YuoOAAAAHUlEQVR42mPQqDhBU8QwasGoBaMWjFowasFQsAAAxdvQH+YmXBQAAAAASUVORK5CYII=";
        let edit_args = serde_json::json!({
            "prompt":"make it green",
            "image":[image],
            "aspect_ratio":"auto"
        })
        .to_string();
        let image_video_args = serde_json::json!({
            "prompt":"animate",
            "image":image,
            "duration":6,
            "resolution_name":"480p"
        })
        .to_string();
        let reference_video_args = serde_json::json!({
            "prompt":"combine",
            "images":[image, image],
            "aspect_ratio":"16:9",
            "duration":6,
            "resolution_name":"480p"
        })
        .to_string();
        let edit_call = sampling.expect_response(
            "edit image tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call("edit-image", "image_edit", &edit_args),
        );
        let image_video_call = sampling.expect_response(
            "image to video tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call("image-video", "image_to_video", &image_video_args),
        );
        let reference_video_call = sampling.expect_response(
            "reference to video tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "reference-video",
                "reference_to_video",
                &reference_video_args,
            ),
        );
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
            .profile(RuntimeProfile::Desktop)
            .media_service(MediaServiceConfig {
                provider: media_provider(media.url(), "media-secret", "media"),
                image_generation: false,
                image_edit: true,
                video_generation: true,
                image_generation_model: None,
                image_edit_model: Some("custom-edit-model".into()),
                image_to_video_model: Some("custom-image-video-model".into()),
                reference_to_video_model: Some("custom-reference-video-model".into()),
            })
            .start()
            .await
            .expect("desktop runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt(&session, "media-turn", "edit and animate images")
            .await
            .expect("media turn succeeds");
        edit_call.assert_satisfied();
        image_video_call.assert_satisfied();
        reference_video_call.assert_satisfied();

        let requests = media.requests();
        let edit = requests
            .iter()
            .find(|request| request.path == "/v1/images/edits?tenant=media")
            .expect("image edit request");
        assert_eq!(edit.body["model"], "custom-edit-model");
        let videos = requests
            .iter()
            .filter(|request| request.path == "/v1/videos/generations?tenant=media")
            .collect::<Vec<_>>();
        assert_eq!(videos.len(), 2);
        assert_eq!(videos[0].body["model"], "custom-image-video-model");
        assert_eq!(videos[1].body["model"], "custom-reference-video-model");
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.path == "/v1/videos/video-1?tenant=media")
                .count(),
            2,
            "video polling must preserve provider query parameters"
        );
        for request in std::iter::once(edit).chain(videos) {
            assert_eq!(
                request.authorization.as_deref(),
                Some("Bearer media-secret")
            );
            assert_eq!(request.provider_header.as_deref(), Some("media"));
        }
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restricted_profile_denies_local_filesystem_without_host_callbacks() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let write_call = server.expect_response(
            "restricted filesystem tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "write-1",
                "search_replace",
                r#"{"file_path":"note.txt","old_string":"before","new_string":"after"}"#,
            ),
        );
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        std::fs::write(workspace.join("note.txt"), "before").expect("fixture");

        // RuntimeConfig intentionally has no host callbacks. Restricted mode
        // must not fall back to LocalFs or TerminalRunner.
        let (runtime, _) = Runtime::start(runtime_config(&root, server.url()))
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("session starts");
        let receipt = runtime
            .prompt(&session, "turn-write", "replace before with after")
            .await
            .expect("denied tool remains a normal model turn outcome");

        write_call.assert_satisfied();
        assert_eq!(receipt.outcome, TurnOutcome::End);
        assert_eq!(
            std::fs::read_to_string(workspace.join("note.txt")).expect("edited file"),
            "before"
        );
        let extension_error = runtime
            .extension_request(ExtensionRequest {
                method: "x.ai/fs/write_file".into(),
                params: serde_json::json!({
                    "sessionId": session.as_str(),
                    "path": "extension.txt",
                    "content": "must not be written",
                    "createDirs": false
                }),
            })
            .await
            .expect_err("Restricted generic extension transport must fail closed");
        assert!(matches!(extension_error, Error::Operation(_)));
        let scheduler_error = runtime
            .list_scheduled_tasks(&session)
            .await
            .expect_err("Restricted typed scheduler transport must fail closed");
        assert!(matches!(scheduler_error, Error::Operation(_)));
        assert!(!workspace.join("extension.txt").exists());
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_host_controls_permission_and_serves_filesystem_callbacks() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        std::fs::write(workspace.join("note.txt"), "before").expect("fixture");
        let host = Arc::new(RecordingHost::default());
        let capabilities = HostCapabilities {
            fs_read: true,
            fs_write: true,
            ..HostCapabilities::default()
        };
        let (runtime, _) = Runtime::builder(runtime_config(&root, server.url()))
            .profile(RuntimeProfile::Desktop)
            .host_capabilities(capabilities)
            .host_delegate(host.clone())
            .start()
            .await
            .expect("desktop runtime starts");
        let session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("session starts");

        let denied_call = server.expect_response(
            "denied filesystem tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "write-denied",
                "search_replace",
                r#"{"file_path":"note.txt","old_string":"before","new_string":"denied"}"#,
            ),
        );
        runtime
            .prompt(&session, "turn-denied", "attempt a denied edit")
            .await
            .expect("a denied tool does not fail the turn transport");
        denied_call.assert_satisfied();
        assert_eq!(
            std::fs::read_to_string(workspace.join("note.txt")).unwrap(),
            "before"
        );
        let denied_methods = host.request_methods();
        assert!(denied_methods.contains(&"session/request_permission".into()));
        assert!(!denied_methods.contains(&"fs/write_text_file".into()));

        host.allow.store(true, Ordering::Release);
        let approved_call = server.expect_response(
            "approved filesystem tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "write-approved",
                "search_replace",
                r#"{"file_path":"note.txt","old_string":"before","new_string":"approved"}"#,
            ),
        );
        runtime
            .prompt(&session, "turn-approved", "perform the approved edit")
            .await
            .expect("approved tool turn succeeds");
        approved_call.assert_satisfied();
        assert_eq!(
            std::fs::read_to_string(workspace.join("note.txt")).unwrap(),
            "approved"
        );
        let approved_methods = host.request_methods();
        assert!(approved_methods.contains(&"fs/read_text_file".into()));
        assert!(approved_methods.contains(&"fs/write_text_file".into()));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while host.notifications().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("agent extension notifications are forwarded to the host");
        assert!(
            host.notifications()
                .iter()
                .all(|notification| !notification.method.is_empty())
        );
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn desktop_host_serves_complete_terminal_lifecycle_including_timeout_kill() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let host = Arc::new(RecordingHost::approving());
        let (runtime, _) = Runtime::builder(runtime_config(&root, server.url()))
            .profile(RuntimeProfile::Desktop)
            .host_capabilities(HostCapabilities {
                terminal: true,
                ..HostCapabilities::default()
            })
            .host_delegate(host.clone())
            .start()
            .await
            .expect("desktop runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");

        host.slow_terminal_wait.store(true, Ordering::Release);
        let terminal_call = server.expect_response(
            "terminal timeout tool call",
            InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
            chat_tool_call(
                "terminal-timeout",
                "run_terminal_command",
                r#"{"command":"sleep 10","timeout":1,"description":"exercise host kill"}"#,
            ),
        );
        runtime
            .prompt(&session, "turn-terminal-timeout", "run the timeout command")
            .await
            .expect("timeout remains a normal tool outcome");
        terminal_call.assert_satisfied();
        let calls = host.request_methods();
        for method in [
            "terminal/create",
            "terminal/wait_for_exit",
            "terminal/kill",
            "terminal/output",
            "terminal/release",
        ] {
            assert!(
                calls.contains(&method.into()),
                "missing {method}: {calls:?}"
            );
        }
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn profiles_extensions_and_explicit_plugin_paths_are_real_agent_capabilities() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let plugin = root.path().join("desktop-plugin");
        std::fs::create_dir(&plugin).expect("plugin dir");
        std::fs::write(plugin.join("plugin.json"), r#"{"name":"desktop-plugin"}"#)
            .expect("plugin manifest");

        let (restricted, _) = Runtime::builder(runtime_config(&root, server.url()))
            .plugin_paths([plugin.clone()])
            .start()
            .await
            .expect("restricted runtime starts");
        let restricted_session = restricted
            .create_session(session_config(workspace.clone()))
            .await
            .expect("restricted session");
        let restricted_plugins = restricted
            .extension_request(ExtensionRequest {
                method: "x.ai/plugins/list".into(),
                params: serde_json::json!({"sessionId":restricted_session.as_str()}),
            })
            .await
            .expect_err("restricted generic extensions are disabled");
        assert!(matches!(restricted_plugins, Error::Operation(_)));
        let restricted_caps = restricted.capabilities();
        assert_eq!(restricted_caps.profile, RuntimeProfile::Restricted);
        assert!(restricted_caps.features.iter().any(|capability| {
            capability.namespace == "feature:app_deployment"
                && !capability.enabled
                && capability.disabled_reason.as_deref()
                    == Some("App Builder deployment is not implemented in this source checkout")
        }));
        assert!(restricted_caps.features.iter().any(|capability| {
            capability.namespace == "sdk:mcp"
                && !capability.enabled
                && capability.disabled_reason.as_deref() == Some("restricted profile")
        }));
        assert!(restricted_caps.features.iter().any(|capability| {
            capability.namespace == "sdk:autonomous-runs"
                && capability.enabled
                && capability.effect_class == "state-agent"
                && capability.host_requirement.is_none()
        }));
        assert!(restricted_caps.features.iter().any(|capability| {
            capability.namespace == "feature:plugins"
                && !capability.enabled
                && capability.disabled_reason.as_deref() == Some("restricted profile")
        }));
        restricted.shutdown().await.expect("restricted shuts down");

        let (desktop, _) = Runtime::builder(runtime_config(&root, server.url()))
            .profile(RuntimeProfile::Desktop)
            .yolo_mode(true)
            .plugin_paths([plugin])
            .start()
            .await
            .expect("desktop runtime starts");
        let desktop_session = desktop
            .create_session(session_config(workspace))
            .await
            .expect("desktop session");
        let desktop_plugins = desktop
            .extension_request(ExtensionRequest {
                method: "x.ai/plugins/list".into(),
                params: serde_json::json!({"sessionId":desktop_session.as_str()}),
            })
            .await
            .expect("desktop plugin list");
        assert!(
            desktop_plugins.result["result"]["plugins"]
                .as_array()
                .is_some_and(|plugins| plugins
                    .iter()
                    .any(|plugin| plugin["name"] == "desktop-plugin"))
        );
        assert_eq!(
            desktop
                .extension_request(ExtensionRequest {
                    method: "x.ai/skills/refresh-baseline".into(),
                    params: serde_json::json!({"futureField":{"preserved":true}}),
                })
                .await
                .expect("known extension")
                .result,
            serde_json::json!({"result":{"ok":true}})
        );
        let unknown = desktop
            .extension_request(ExtensionRequest {
                method: "x.ai/future/not-yet-implemented".into(),
                params: serde_json::json!({"opaque":[1,2,3]}),
            })
            .await
            .expect_err("unknown extension preserves protocol error");
        assert!(matches!(
            unknown,
            Error::Protocol { code: -32601, ref data, .. }
                if data.as_str().is_some_and(|message| message.contains("x.ai/future/not-yet-implemented"))
        ));
        desktop
            .notify_extension(ExtensionNotification {
                method: "x.ai/yolo_mode_changed".into(),
                params: serde_json::json!({"yolo_mode":true}),
            })
            .await
            .expect("extension notification reaches the agent");
        let desktop_caps = desktop.capabilities();
        assert_eq!(desktop_caps.profile, RuntimeProfile::Desktop);
        assert!(desktop_caps.features.iter().any(|capability| {
            capability.namespace == "sdk:extension-bridge" && capability.enabled
        }));
        assert!(desktop_caps.features.iter().any(|capability| {
            capability.namespace == "feature:managed_mcp"
                && !capability.enabled
                && capability
                    .disabled_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("account-product service"))
        }));
        assert!(
            desktop_caps.features.iter().any(|capability| {
                capability.namespace == "feature:plugins" && capability.enabled
            })
        );
        desktop.shutdown().await.expect("desktop shuts down");
    }

    #[tokio::test]
    async fn fixed_model_catalog_is_typed_and_available_in_restricted_profile() {
        let root = TempDir::new().expect("temp root");
        let (runtime, _) = Runtime::start(runtime_config(&root, "http://127.0.0.1:9/v1".into()))
            .await
            .expect("fixed catalog does not require a reachable catalog service");

        let models = runtime.list_models().await.expect("model catalog");
        assert_eq!(models.current_model_id, "test-model");
        assert_eq!(models.available_models.len(), 1);
        assert_eq!(models.available_models[0].id, "test-model");
        let metadata = models.available_models[0]
            .metadata
            .as_ref()
            .expect("model capability metadata");
        assert_eq!(
            metadata.get("totalContextTokens"),
            Some(&serde_json::json!(131_072))
        );
        assert_eq!(
            metadata.get("agentType"),
            Some(&serde_json::json!("grok-build"))
        );
        assert!(runtime.capabilities().features.iter().any(|capability| {
            capability.namespace == "x.ai/models/list"
                && capability.enabled
                && capability.effect_class == "read"
        }));

        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test]
    async fn restricted_session_creation_never_evaluates_workspace_envrc() {
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let marker = root.path().join("envrc-was-evaluated");
        std::fs::write(
            workspace.join(".envrc"),
            format!("printf evaluated > '{}'\n", marker.display()),
        )
        .expect("hostile envrc");

        let (runtime, _) = Runtime::start(runtime_config(&root, "http://127.0.0.1:9/v1".into()))
            .await
            .expect("restricted runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("restricted session starts");

        assert!(
            !marker.exists(),
            "Restricted must not execute a workspace .envrc"
        );
        runtime
            .close_session(session)
            .await
            .expect("session closes");
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test]
    async fn rejects_missing_endpoint_before_starting_worker() {
        let root = TempDir::new().expect("temp root");
        let result = Runtime::start(runtime_config(&root, String::new())).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn provenance_is_exact_and_never_unknown() {
        let provenance = source_provenance();
        assert_eq!(provenance.upstream_release, "1.0.0");
        assert_eq!(provenance.facade_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(provenance.fork_commit.len(), 40);
        assert!(
            provenance
                .fork_commit
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        );
        assert_eq!(provenance.upstream_source_rev.len(), 40);
        assert!(
            provenance
                .upstream_source_rev
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        );
    }

    #[test]
    fn legacy_rewind_receipts_without_exact_target_identity_fail_closed() {
        let legacy = serde_json::json!({
            "operation_id": "legacy-operation",
            "session_id": "legacy-session",
            "target_prompt_index": 2
        });

        assert!(serde_json::from_value::<ConversationRewindReceipt>(legacy).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_real_agent_outside_local_set_and_closes_session() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");

        let (runtime, mut events) = Runtime::start(runtime_config(&root, server.url()))
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(SessionConfig {
                cwd: workspace.clone(),
                model: "test-model".into(),
                reasoning: None,
                system_prompt: None,
                rules: None,
            })
            .await
            .expect("session starts");
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            runtime.prompt(&session, "turn-1", "reply briefly"),
        )
        .await
        .expect("turn timeout")
        .expect("turn succeeds");
        assert_eq!(outcome.outcome, TurnOutcome::End);
        let retained = runtime
            .events_after(&session, 0)
            .await
            .expect("events are retained");
        assert_eq!(
            retained.last().map(|event| event.sequence),
            Some(outcome.final_sequence)
        );
        assert!(matches!(
            retained.last().map(|event| &event.update),
            Some(EventUpdate::TurnFinished(TurnOutcome::End))
        ));
        assert!(retained.iter().any(|event| {
            event.turn_id.as_deref() == Some("turn-1")
                && matches!(&event.update, EventUpdate::UserText(text) if text == "reply briefly")
        }));

        let mut assistant = String::new();
        while let Ok(Some(event)) =
            tokio::time::timeout(std::time::Duration::from_millis(250), events.recv()).await
        {
            let finished = matches!(event.update, EventUpdate::TurnFinished(_));
            if let EventUpdate::AssistantText(text) = &event.update {
                assert_eq!(event.turn_id.as_deref(), Some("turn-1"));
                assistant.push_str(text);
            }
            if finished {
                assert_eq!(event.turn_id.as_deref(), Some("turn-1"));
                break;
            }
        }
        assert!(assistant.contains("Echo:"), "assistant output: {assistant}");
        runtime
            .unload_session(session.clone())
            .await
            .expect("session closes");
        assert!(matches!(
            runtime
                .events_after(&session, outcome.final_sequence)
                .await
                .expect("closed journal remains readable")
                .as_slice(),
            [Event {
                update: EventUpdate::SessionClosed,
                ..
            }]
        ));
        runtime
            .load_session(session.clone(), session_config(workspace))
            .await
            .expect("the same durable session id remains resumable");
        let after_turn = runtime
            .events_after(&session, outcome.final_sequence)
            .await
            .expect("retained close event is recoverable after reload");
        assert!(matches!(
            after_turn.as_slice(),
            [Event {
                update: EventUpdate::SessionClosed,
                ..
            }]
        ));
        assert!(runtime.events_after(&session, u64::MAX).await.is_err());
        assert!(
            runtime
                .events_after(&SessionId::from_stored("missing"), 0)
                .await
                .is_err()
        );
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[test]
    fn rich_prompt_digest_covers_binary_content() {
        let prompt = |data: &str| Prompt {
            blocks: vec![PromptBlock::Image {
                data: data.into(),
                mime_type: "image/png".into(),
                uri: None,
            }],
            metadata: serde_json::json!({"source":"test"}),
        };
        assert_ne!(
            prompt_digest_content(&prompt("AA==")).unwrap(),
            prompt_digest_content(&prompt("AQ==")).unwrap()
        );
        assert_eq!(
            serde_json::from_value::<RuntimeProfile>(serde_json::json!("restricted")).unwrap(),
            RuntimeProfile::Restricted
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rich_prompt_blocks_digest_rewind_and_restart_replay_are_end_to_end() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let config = runtime_config(&root, server.url());
        let prompt = Prompt {
            blocks: vec![
                PromptBlock::Text {
                    text: "rich-wire-marker".into(),
                },
                PromptBlock::Image {
                    data: "iVBORw0KGgoAAAANSUhEUgAAACAAAAAQCAIAAAD4YuoOAAAAHUlEQVR42mPQqDhBU8QwasGoBaMWjFowasFQsAAAxdvQH+YmXBQAAAAASUVORK5CYII=".into(),
                    mime_type: "image/png".into(),
                    uri: Some("attachment://screen.png".into()),
                },
                PromptBlock::Audio {
                    data: "AQ==".into(),
                    mime_type: "audio/wav".into(),
                },
                PromptBlock::ResourceLink {
                    uri: "file:///workspace/reference.txt".into(),
                    name: "reference.txt".into(),
                    mime_type: Some("text/plain".into()),
                },
                PromptBlock::EmbeddedTextResource {
                    uri: "memory://embedded-text".into(),
                    text: "embedded-text-marker".into(),
                    mime_type: Some("text/plain".into()),
                },
                PromptBlock::EmbeddedBlobResource {
                    uri: "memory://embedded-blob".into(),
                    blob: "Ag==".into(),
                    mime_type: Some("application/octet-stream".into()),
                },
            ],
            metadata: serde_json::json!({"desktop":{"captureId":"capture-1"}}),
        };
        let expected_digest = prompt_digest_content(&prompt).expect("rich digest");

        let (runtime, _) = Runtime::start(config.clone())
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("session starts");
        let receipt = runtime
            .prompt_content(&session, "rich-turn", prompt)
            .await
            .expect("rich prompt succeeds through the real agent");
        assert_eq!(receipt.runtime_prompt_index, 0);
        let request = server
            .requests()
            .into_iter()
            .filter_map(|entry| entry.body)
            .find(|body| body.to_string().contains("rich-wire-marker"))
            .expect("rich prompt reached inference");
        let request_wire = request.to_string();
        for marker in [
            "image/png",
            "audio/wav",
            "file:///workspace/reference.txt",
            "embedded-text-marker",
            "application/octet-stream",
        ] {
            assert!(
                request_wire.contains(marker),
                "missing {marker} in inference request: {request_wire}"
            );
        }
        let prompt_events = runtime.events_after(&session, 0).await.expect("events");
        let lossless_non_text = prompt_events
            .iter()
            .filter_map(|event| match &event.update {
                EventUpdate::Unknown { raw, .. } => Some(raw.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(lossless_non_text.contains("attachment://screen.png"));
        assert!(lossless_non_text.contains("memory://embedded-blob"));
        let ledger = runtime.session_ledger(&session).await.expect("ledger");
        assert_eq!(ledger.entries[0].prompt_digest, expected_digest);
        assert_eq!(
            runtime
                .rewind_points(&session)
                .await
                .expect("rewind points")[0]
                .prompt_digest,
            Some(expected_digest.clone())
        );
        runtime
            .unload_session(session.clone())
            .await
            .expect("session unloads");
        runtime.shutdown().await.expect("first runtime shuts down");

        let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
        restarted
            .load_session(session.clone(), session_config(workspace.clone()))
            .await
            .expect("load replays persisted history");
        let replay = restarted
            .events_after(&session, 0)
            .await
            .expect("fresh journal captures replay");
        assert!(!replay.is_empty(), "load must rebuild a fresh journal");
        assert!(replay.iter().all(|event| event.replay));
        assert!(replay.iter().any(|event| {
            matches!(&event.update, EventUpdate::UserText(text) if text == "rich-wire-marker")
        }));
        restarted
            .unload_session(session.clone())
            .await
            .expect("loaded session unloads");
        let sequence_before_resume = restarted
            .events_after(&session, 0)
            .await
            .expect("journal remains")
            .last()
            .expect("journal event")
            .sequence;
        restarted
            .resume_session(session.clone(), session_config(workspace))
            .await
            .expect("resume reattaches without replay");
        assert!(
            restarted
                .events_after(&session, sequence_before_resume)
                .await
                .expect("resume journal query")
                .is_empty(),
            "session/resume must not duplicate historical events"
        );
        assert_eq!(
            restarted
                .rewind_points(&session)
                .await
                .expect("rewind points")[0]
                .prompt_digest,
            Some(expected_digest)
        );
        restarted.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_event_journal_reports_exact_cursor_gap() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let (runtime, _) = Runtime::builder(runtime_config(&root, server.url()))
            .event_journal_capacity(2)
            .start()
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        let receipt = runtime
            .prompt(&session, "journal-turn", "journal marker")
            .await
            .expect("turn succeeds");
        let gap = runtime
            .events_after(&session, 0)
            .await
            .expect_err("old cursor must report eviction");
        assert!(matches!(
            gap,
            Error::EventGap {
                requested: 0,
                oldest_available,
                newest,
            } if oldest_available == receipt.final_sequence - 1 && newest == receipt.final_sequence
        ));
        let tail = runtime
            .events_after(&session, receipt.final_sequence - 2)
            .await
            .expect("oldest retained cursor is readable");
        assert_eq!(tail.len(), 2);
        assert_eq!(
            tail.last().map(|event| event.sequence),
            Some(receipt.final_sequence)
        );
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_changes_preserve_the_prompt_and_existing_conversation() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, server.url());
        config.models = vec![
            ModelSpec {
                id: "fast-route".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: true,
                default_reasoning: Some("high".into()),
                reasoning_options: vec!["high".into()],
            },
            ModelSpec {
                id: "advanced-route".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: true,
                default_reasoning: Some("xhigh".into()),
                reasoning_options: vec!["xhigh".into()],
            },
        ];
        let (runtime, _events) = Runtime::start(config).await.expect("runtime starts");
        let session = runtime
            .create_session(SessionConfig {
                cwd: workspace,
                model: "fast-route".into(),
                reasoning: Some("high".into()),
                system_prompt: None,
                rules: None,
            })
            .await
            .expect("session starts");

        runtime
            .prompt(&session, "turn-fast-1", "route-marker-fast-1")
            .await
            .expect("first fast turn");
        let fast_before = request_with_user_marker(&server, "route-marker-fast-1");
        let system_prompt = fast_before["messages"][0]["content"]
            .as_str()
            .expect("system prompt")
            .as_bytes()
            .to_vec();
        assert_eq!(fast_before["model"], "fast-route");
        assert_eq!(fast_before["reasoning_effort"], "high");

        runtime
            .set_route(&session, "advanced-route", Some("xhigh".into()))
            .await
            .expect("advanced route applies");
        runtime
            .prompt(&session, "turn-advanced", "route-marker-advanced")
            .await
            .expect("advanced turn");
        let advanced = request_with_user_marker(&server, "route-marker-advanced");
        assert_eq!(advanced["model"], "advanced-route");
        assert_eq!(advanced["reasoning_effort"], "xhigh");
        assert_eq!(
            advanced["messages"][0]["content"]
                .as_str()
                .expect("system prompt")
                .as_bytes(),
            system_prompt
        );
        assert!(message_prefix_is_unchanged(&fast_before, &advanced));

        runtime
            .set_route(&session, "fast-route", Some("high".into()))
            .await
            .expect("fast route reapplies");
        runtime
            .prompt(&session, "turn-fast-2", "route-marker-fast-2")
            .await
            .expect("second fast turn");
        let fast_after = request_with_user_marker(&server, "route-marker-fast-2");
        assert_eq!(fast_after["model"], "fast-route");
        assert_eq!(fast_after["reasoning_effort"], "high");
        assert_eq!(
            fast_after["messages"][0]["content"]
                .as_str()
                .expect("system prompt")
                .as_bytes(),
            system_prompt
        );
        assert!(message_prefix_is_unchanged(&advanced, &fast_after));

        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rewind_receipt_recovers_after_native_and_ledger_commit_without_reexecution() {
        use sha2::Digest as _;

        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let config = runtime_config(&root, server.url());
        let (runtime, _events) = Runtime::start(config.clone())
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("session starts");
        let other_session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("other session starts");
        runtime
            .prompt(&session, "turn-0", "prompt zero")
            .await
            .expect("first turn");
        runtime
            .prompt(&session, "turn-1", "prompt one")
            .await
            .expect("second turn");

        let operation_id = "restart-rewind-operation";
        assert!(matches!(
            runtime
                .rewind_status(&session, "never-started-rewind")
                .await
                .expect("absent rewind status"),
            ConversationRewindStatus::Absent
        ));
        let rewind_root = root.path().join("sessions/origin-rewind-receipts");
        std::fs::create_dir_all(&rewind_root).expect("rewind receipt root");
        let digest = format!("{:x}", sha2::Sha256::digest(operation_id.as_bytes()));
        let ledger_before = runtime
            .session_ledger(&session)
            .await
            .expect("ledger before rewind");
        let target_entry = &ledger_before.entries[1];
        std::fs::write(
            rewind_root.join(format!("{digest}.intent.json")),
            serde_json::to_vec(&serde_json::json!({
                "operation_id": operation_id,
                "session_id": session.as_str(),
                "target_prompt_index": 1,
                "target_turn_id": target_entry.turn_id.clone(),
                "target_prompt_digest": target_entry.prompt_digest.clone(),
                "recovery_turn_id": null,
                "recovery_prompt_digest": null
            }))
            .expect("intent json"),
        )
        .expect("simulate a durable intent before native execution");
        assert!(matches!(
            runtime
                .rewind_status(&session, operation_id)
                .await
                .expect("pre-effect pending status"),
            ConversationRewindStatus::Pending { .. }
        ));
        let first = runtime
            .rewind_conversation(&session, operation_id, 1)
            .await
            .expect("first rewind");
        assert_eq!(first.target_prompt_index, 1);
        assert!(matches!(
            runtime
                .rewind_status(&session, operation_id)
                .await
                .expect("receipt status"),
            ConversationRewindStatus::Applied { receipt } if receipt == first
        ));
        assert!(
            runtime
                .rewind_status(&other_session, operation_id)
                .await
                .is_err(),
            "a global operation id cannot expose another session's receipt"
        );
        assert_eq!(
            runtime
                .rewind_conversation(&session, operation_id, 1)
                .await
                .expect("receipt replay"),
            first
        );
        assert!(
            runtime
                .rewind_conversation(&session, operation_id, 0)
                .await
                .is_err(),
            "an operation identity cannot drift to another target"
        );
        std::fs::write(
            rewind_root.join(format!("{digest}.intent.json")),
            serde_json::to_vec(&serde_json::json!({
                "operation_id": operation_id,
                "session_id": session.as_str(),
                "target_prompt_index": 1,
                "target_turn_id": first.target_turn_id.clone(),
                "target_prompt_digest": first.target_prompt_digest.clone(),
                "recovery_turn_id": null,
                "recovery_prompt_digest": null
            }))
            .expect("intent json"),
        )
        .expect("restore stale intent after receipt publication");
        assert!(matches!(
            runtime
                .rewind_status(&session, operation_id)
                .await
                .expect("receipt wins over stale intent"),
            ConversationRewindStatus::Applied { receipt } if receipt == first
        ));
        runtime
            .prompt(&session, "turn-1-reused", "prompt one")
            .await
            .expect("replacement turn reuses the discarded prompt index and text");
        let reused_operation_id = "reused-index-restart-rewind";
        let reused_digest = format!("{:x}", sha2::Sha256::digest(reused_operation_id.as_bytes()));
        let reused_ledger = runtime
            .session_ledger(&session)
            .await
            .expect("ledger after reused prompt index");
        let reused_target = reused_ledger
            .entries
            .last()
            .expect("replacement ledger entry");
        assert_eq!(reused_target.runtime_prompt_index, 1);
        assert_eq!(reused_target.prompt_digest, first.target_prompt_digest);
        assert_ne!(reused_target.turn_id, first.target_turn_id);
        std::fs::write(
            rewind_root.join(format!("{reused_digest}.intent.json")),
            serde_json::to_vec(&serde_json::json!({
                "operation_id": reused_operation_id,
                "session_id": session.as_str(),
                "target_prompt_index": 1,
                "target_turn_id": reused_target.turn_id.clone(),
                "target_prompt_digest": reused_target.prompt_digest.clone(),
                "recovery_turn_id": null,
                "recovery_prompt_digest": null
            }))
            .expect("reused intent json"),
        )
        .expect("persist reused-index intent before native execution");
        let reused_receipt = runtime
            .rewind_conversation(&session, reused_operation_id, 1)
            .await
            .expect("reused-index rewind targets the replacement turn");
        assert_eq!(reused_receipt.target_turn_id, reused_target.turn_id);
        std::fs::write(
            rewind_root.join(format!("{reused_digest}.intent.json")),
            serde_json::to_vec(&serde_json::json!({
                "operation_id": reused_operation_id,
                "session_id": session.as_str(),
                "target_prompt_index": 1,
                "target_turn_id": reused_receipt.target_turn_id.clone(),
                "target_prompt_digest": reused_receipt.target_prompt_digest.clone(),
                "recovery_turn_id": null,
                "recovery_prompt_digest": null
            }))
            .expect("post-effect reused intent json"),
        )
        .expect("simulate crash after reused-index effect and before receipt publication");
        runtime
            .unload_session(session.clone())
            .await
            .expect("session unloads");
        runtime
            .unload_session(other_session)
            .await
            .expect("other session unloads");
        runtime.shutdown().await.expect("first runtime shuts down");

        std::fs::remove_file(rewind_root.join(format!("{reused_digest}.json")))
            .expect("simulate crash before reused-index receipt publication");

        let (restarted, _events) = Runtime::start(config).await.expect("runtime restarts");
        restarted
            .load_session(session.clone(), session_config(workspace))
            .await
            .expect("rewound session reloads");
        assert!(matches!(
            restarted
                .rewind_status(&session, reused_operation_id)
                .await
                .expect("pending status"),
            ConversationRewindStatus::Pending {
                target_prompt_index: 1,
                target_turn_id,
                ..
            } if target_turn_id == reused_receipt.target_turn_id
        ));
        let recovered = restarted
            .rewind_conversation(&session, reused_operation_id, 1)
            .await
            .expect("missing receipt is reconstructed");
        assert_eq!(recovered, reused_receipt);
        let ledger = restarted
            .session_ledger(&session)
            .await
            .expect("ledger loads");
        assert!(matches!(
            ledger.entries[0].state,
            LedgerTurnState::Completed { .. }
        ));
        assert!(matches!(
            ledger.entries[1].state,
            LedgerTurnState::Discarded
        ));
        assert!(matches!(
            ledger.entries[2].state,
            LedgerTurnState::Discarded
        ));
        assert_eq!(restarted.rewind_points(&session).await.unwrap().len(), 1);
        restarted.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_sessions_cancel_close_and_shutdown_are_reconciled() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let (runtime, mut events) = Runtime::start(runtime_config(&root, server.url()))
            .await
            .expect("runtime starts");

        let first = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("first session");
        let second = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("second session");
        server.hold_agent_completions();
        let first_prompt = tokio::spawn({
            let runtime = runtime.clone();
            let first = first.clone();
            async move { runtime.prompt(&first, "first-turn", "first").await }
        });
        let second_prompt = tokio::spawn({
            let runtime = runtime.clone();
            let second = second.clone();
            async move { runtime.prompt(&second, "second-turn", "second").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        runtime.cancel(&first).await.expect("active prompt cancels");
        runtime
            .unload_session(second.clone())
            .await
            .expect("active session closes after cancellation");
        server.release_agent_completions();
        let first_outcome = first_prompt
            .await
            .expect("first prompt joins")
            .expect("settles");
        assert_eq!(first_outcome.outcome, TurnOutcome::Cancelled);
        let second_outcome = second_prompt
            .await
            .expect("second prompt joins")
            .expect("settles");
        assert_eq!(second_outcome.outcome, TurnOutcome::Cancelled);
        runtime.unload_session(first).await.expect("first unloads");

        let mut by_session = std::collections::HashMap::<String, Vec<u64>>::new();
        while let Ok(event) = events.try_recv() {
            by_session
                .entry(event.session_id.as_str().to_owned())
                .or_default()
                .push(event.sequence);
        }
        for sequences in by_session.values() {
            assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
        }
        let application_sessions = by_session
            .keys()
            .filter(|session_id| session_id.as_str() != SessionId::RUNTIME_EVENTS)
            .count();
        assert_eq!(application_sessions, 2);

        runtime.shutdown().await.expect("worker joins");
        runtime.shutdown().await.expect("shutdown is idempotent");
        assert!(matches!(
            runtime.create_session(session_config(workspace)).await,
            Err(Error::Shutdown)
        ));
    }

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
        let host_store = run::LocalRunStore::new(root.path().join("host-run-authority"))
            .expect("Host store opens");
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
            run::OperationId::new(format!("{}_operation", command_prefix.replace('-', "_")))
                .unwrap();
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
            claim_test_session_turn(&first, &created, "completed_ledger", turn_id, prompt_digest)
                .await;
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
}
