// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

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
/// Values are never read from environment variables by the runtime. The
/// configured secret is sent as an HTTP Bearer token, so a desktop host can
/// point the base URL at its loopback relay and supply a relay-scoped bearer
/// instead of giving the SDK a provider's raw credential. The SDK does not
/// persist this configuration. Secrets are intentionally omitted from both
/// `Debug` and `Serialize`; hosts may deserialize configuration but cannot
/// accidentally export the secret bag.
#[derive(Clone, Default, PartialEq, serde::Deserialize)]
pub struct ApiProviderConfig {
    /// OpenAI-compatible API base URL, including any path prefix (usually
    /// `/v1`). Loopback HTTP endpoints are supported.
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
    /// Application-owned general capability layer. A Session's own layer masks
    /// a general contribution of the same kind and name for that Session only.
    pub general_capabilities: CapabilityLayer,
    pub host: Option<Arc<dyn HostDelegate>>,
    /// Host authority behind the three conversation tools. Installing it is
    /// what makes those tools exist on this Runtime's Sessions.
    pub conversation_delegate: Option<Arc<dyn ConversationDelegate>>,
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
            general_capabilities: CapabilityLayer::default(),
            host: None,
            conversation_delegate: None,
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
