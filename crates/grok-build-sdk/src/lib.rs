// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Send/Sync, fail-closed in-process façade for the bundled Grok Build fork.
//! ACP, Grok, and JSON implementation types are confined to the private module.

mod private;

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::mpsc;

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
    pub protocol_version: String,
    pub initialize: serde_json::Value,
    pub profile: RuntimeProfile,
    pub host: HostCapabilities,
    pub generic_extension_transport: bool,
    pub extension_families: Vec<CapabilityDescriptor>,
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
    Unknown {
        tag: String,
        /// Lossless JSON representation of the ACP update that this version of
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
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum LedgerTurnState {
    Pending,
    Completed {
        outcome: TurnOutcome,
        settlement_id: String,
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
        }
    }
    pub async fn start(
        config: RuntimeConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start(config, RuntimeOptions::default())
            .await
            .map(|(inner, events)| (Self { inner }, events))
    }
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.inner.capabilities()
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
    /// Unlike generic ACP model switching, this never rebuilds the harness or
    /// rewrites the system prompt.
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

pub struct RuntimeBuilder {
    config: RuntimeConfig,
    options: RuntimeOptions,
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
    pub fn host_delegate(mut self, value: Arc<dyn HostDelegate>) -> Self {
        self.options.host = Some(value);
        self
    }
    pub async fn start(self) -> Result<(Runtime, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start(self.config, self.options)
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
                    Some("initialize") => (
                        [("mcp-session-id", "origin-runtime-http-test")],
                        Json(serde_json::json!({
                            "jsonrpc":"2.0",
                            "id":request["id"],
                            "result":{
                                "protocolVersion":request["params"]["protocolVersion"],
                                "capabilities":{"tools":{}},
                                "serverInfo":{"name":"origin-runtime-http-test","version":"1"}
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
            .expect("MCP HTTP initialize and tools/list complete");
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
    *'"method":"initialize"'*)
      version=$(printf '%s' "$line" | sed -n 's/.*"protocolVersion":"\([^"]*\)".*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"%s","capabilities":{"tools":{}},"serverInfo":{"name":"origin-runtime-test","version":"1"}}}\n' "$id" "$version"
      ;;
    *'"method":"tools/list"'*)
      : > "$MCP_MARKER"
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo","description":"echo","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
    *'"method":"tools/call"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"ok"}],"isError":false}}\n' "$id"
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
            env: BTreeMap::from([("MCP_MARKER".into(), marker.to_string_lossy().into_owned())]),
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
        restricted
            .close_session(restricted_session)
            .await
            .expect("restricted session closes");
        restricted.shutdown().await.expect("restricted shuts down");

        let desktop_root = TempDir::new().expect("desktop root");
        let (desktop, _) = Runtime::builder(runtime_config(&desktop_root, sampling.url()))
            .profile(RuntimeProfile::Desktop)
            .mcp_servers([mcp])
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
            .extension_families
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
            .extension_families
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
        assert!(!restricted_caps.generic_extension_transport);
        assert!(restricted_caps.extension_families.iter().any(|capability| {
            capability.namespace == "feature:app_deployment"
                && !capability.enabled
                && capability.disabled_reason.as_deref()
                    == Some("App Builder deployment is not implemented in this source checkout")
        }));
        assert!(restricted_caps.extension_families.iter().any(|capability| {
            capability.namespace == "x.ai/plugins"
                && !capability.enabled
                && capability.disabled_reason.as_deref()
                    == Some("generic extensions require the Desktop profile")
        }));
        assert!(restricted_caps.extension_families.iter().any(|capability| {
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
        assert!(desktop_caps.extension_families.iter().any(|capability| {
            capability.namespace == "feature:managed_mcp"
                && !capability.enabled
                && capability
                    .disabled_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("account-product service"))
        }));
        assert!(
            desktop_caps.extension_families.iter().any(|capability| {
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
        assert!(
            runtime
                .capabilities()
                .extension_families
                .iter()
                .any(|capability| {
                    capability.namespace == "x.ai/models/list"
                        && capability.enabled
                        && capability.effect_class == "read"
                })
        );

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
}
