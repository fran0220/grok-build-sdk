// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::capability::validate_capability_name;
use crate::*;

/// Maximum remote or stdio MCP mounts accepted in one Runtime-owned list.
pub const MAX_MCP_MOUNTS: usize = 64;
/// Maximum byte length of one remote MCP endpoint.
pub const MAX_MCP_ENDPOINT_BYTES: usize = 2 * 1024;
/// Maximum number of host-injected headers on one remote MCP mount.
pub const MAX_MCP_HEADERS: usize = 32;
/// Maximum byte length of one MCP HTTP header name.
pub const MAX_MCP_HEADER_NAME_BYTES: usize = 128;
/// Maximum byte length of one MCP HTTP header value.
pub const MAX_MCP_HEADER_VALUE_BYTES: usize = 8 * 1024;
/// Maximum byte length of a stdio MCP command path.
pub const MAX_MCP_STDIO_COMMAND_BYTES: usize = 4 * 1024;
/// Maximum number of arguments supplied to one stdio MCP process.
pub const MAX_MCP_STDIO_ARGS: usize = 256;
/// Maximum byte length of one stdio MCP argument.
pub const MAX_MCP_STDIO_ARG_BYTES: usize = 8 * 1024;
/// Maximum number of host-injected environment entries for one stdio MCP process.
pub const MAX_MCP_STDIO_ENV: usize = 128;
/// Maximum byte length of one stdio MCP environment name.
pub const MAX_MCP_STDIO_ENV_NAME_BYTES: usize = 256;
/// Maximum byte length of one stdio MCP environment value.
pub const MAX_MCP_STDIO_ENV_VALUE_BYTES: usize = 32 * 1024;

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

/// Provider wire protocol. This is the single source of truth for both the
/// endpoint shape and authentication scheme of an explicitly configured
/// model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProtocol {
    #[default]
    OpenAiChatCompletions,
    OpenAiResponses,
    AnthropicMessages,
}

impl ProviderProtocol {
    pub(crate) fn api_backend(self) -> &'static str {
        match self {
            Self::OpenAiChatCompletions => "chat_completions",
            Self::OpenAiResponses => "responses",
            Self::AnthropicMessages => "messages",
        }
    }

    pub(crate) fn auth_scheme(self) -> &'static str {
        match self {
            Self::OpenAiChatCompletions | Self::OpenAiResponses => "bearer",
            Self::AnthropicMessages => "x_api_key",
        }
    }
}

/// Explicit credentials and routing for one model provider.
/// Values are never read from environment variables by the runtime. The
/// selected [`ProviderProtocol`] exclusively determines the request endpoint,
/// wire format, and authentication header; [`ModelSpec::api_backend`] is used
/// only by the legacy `RuntimeConfig` endpoint/key fallback. The SDK does not
/// persist this configuration. Secrets are intentionally omitted from both
/// `Debug` and `Serialize`; hosts may deserialize configuration but cannot
/// accidentally export the secret bag.
#[derive(Clone, Default, PartialEq, serde::Deserialize)]
pub struct ProviderConfig {
    /// OpenAI-compatible API base URL, including any path prefix (usually
    /// `/v1`). Loopback HTTP endpoints are supported.
    #[serde(default)]
    pub protocol: ProviderProtocol,
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

impl ProviderConfig {
    pub fn openai_chat(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::new(
            ProviderProtocol::OpenAiChatCompletions,
            base_url,
            api_key,
            model,
        )
    }

    pub fn openai_responses(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::new(ProviderProtocol::OpenAiResponses, base_url, api_key, model)
    }

    pub fn anthropic(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::new(
            ProviderProtocol::AnthropicMessages,
            base_url,
            api_key,
            model,
        )
    }

    fn new(
        protocol: ProviderProtocol,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            protocol,
            base_url: base_url.into(),
            api_key: api_key.into(),
            model: Some(model.into()),
            headers: BTreeMap::new(),
            query_params: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.api_key.trim().is_empty()
            || self
                .model
                .as_deref()
                .is_some_and(|model| model.trim().is_empty())
        {
            return Err(Error::InvalidConfig(
                "model providers require an API key and non-empty optional model slug".into(),
            ));
        }
        let parsed = url::Url::parse(&self.base_url).map_err(|_| {
            Error::InvalidConfig("model provider base URL must be an absolute URL".into())
        })?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(Error::InvalidConfig(
                "model provider base URL must be http(s) without userinfo or a fragment".into(),
            ));
        }
        let mut normalized_headers = std::collections::HashSet::new();
        for (name, value) in &self.headers {
            let parsed_name = http::HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                Error::InvalidConfig("model provider header name is invalid".into())
            })?;
            if parsed_name == http::header::AUTHORIZATION
                || parsed_name == http::HeaderName::from_static("x-api-key")
            {
                return Err(Error::InvalidConfig(
                    "model provider authentication headers are derived from protocol and api_key"
                        .into(),
                ));
            }
            value.parse::<http::HeaderValue>().map_err(|_| {
                Error::InvalidConfig("model provider header value is invalid".into())
            })?;
            if !normalized_headers.insert(parsed_name) {
                return Err(Error::InvalidConfig(
                    "model provider header names must be unique ignoring case".into(),
                ));
            }
        }
        if self.query_params.keys().any(|name| name.trim().is_empty()) {
            return Err(Error::InvalidConfig(
                "model provider query parameter names must be non-empty".into(),
            ));
        }
        Ok(())
    }
}

/// Compatibility spelling retained for existing embedders.
pub type ApiProviderConfig = ProviderConfig;

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
#[derive(Clone, PartialEq)]
pub enum McpServerConfig {
    Stdio {
        name: String,
        command: PathBuf,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    },
    Http {
        name: String,
        url: String,
        headers: BTreeMap<String, String>,
    },
    Sse {
        name: String,
        url: String,
        headers: BTreeMap<String, String>,
    },
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum McpServerConfigWire {
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

impl<'de> serde::Deserialize<'de> for McpServerConfig {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let wire = <McpServerConfigWire as serde::Deserialize>::deserialize(deserializer)?;
        let value = match wire {
            McpServerConfigWire::Stdio {
                name,
                command,
                args,
                env,
            } => Self::Stdio {
                name,
                command,
                args,
                env,
            },
            McpServerConfigWire::Http { name, url, headers } => Self::Http { name, url, headers },
            McpServerConfigWire::Sse { name, url, headers } => Self::Sse { name, url, headers },
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

impl McpServerConfig {
    /// Constructs a bounded Streamable HTTP mount. Headers are supplied by the
    /// embedding Host and are never present in catalog summaries or events.
    pub fn http(
        name: impl Into<String>,
        url: impl Into<String>,
        headers: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let value = Self::Http {
            name: name.into(),
            url: url.into(),
            headers,
        };
        value.validate()?;
        Ok(value)
    }

    /// Constructs a bounded `Sse` compatibility mount with the same host-owned
    /// header boundary as [`McpServerConfig::http`]. The current runtime treats
    /// this variant as a modern Streamable HTTP endpoint; it does not implement
    /// the legacy SSE lifecycle.
    pub fn sse(
        name: impl Into<String>,
        url: impl Into<String>,
        headers: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let value = Self::Sse {
            name: name.into(),
            url: url.into(),
            headers,
        };
        value.validate()?;
        Ok(value)
    }

    /// Validates the public transport contract before it reaches the embedded
    /// runtime or a helper process. Unknown serde variants/fields are rejected
    /// by the type itself; malformed URLs and header material fail here.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Stdio {
                name,
                command,
                args,
                env,
            } => {
                validate_capability_name(name)?;
                let command_bytes = command.as_os_str().to_string_lossy().len();
                if command_bytes == 0 || command_bytes > MAX_MCP_STDIO_COMMAND_BYTES {
                    return Err(Error::InvalidConfig(format!(
                        "stdio MCP command paths must be 1..={MAX_MCP_STDIO_COMMAND_BYTES} bytes"
                    )));
                }
                if args.len() > MAX_MCP_STDIO_ARGS
                    || args.iter().any(|arg| arg.len() > MAX_MCP_STDIO_ARG_BYTES)
                {
                    return Err(Error::InvalidConfig(format!(
                        "stdio MCP mounts accept at most {MAX_MCP_STDIO_ARGS} arguments of at most {MAX_MCP_STDIO_ARG_BYTES} bytes"
                    )));
                }
                if env.len() > MAX_MCP_STDIO_ENV
                    || env.iter().any(|(key, value)| {
                        key.is_empty()
                            || key.len() > MAX_MCP_STDIO_ENV_NAME_BYTES
                            || key.contains(['=', '\0'])
                            || value.len() > MAX_MCP_STDIO_ENV_VALUE_BYTES
                            || value.contains('\0')
                    })
                {
                    return Err(Error::InvalidConfig(format!(
                        "stdio MCP mounts accept at most {MAX_MCP_STDIO_ENV} valid environment entries with names up to {MAX_MCP_STDIO_ENV_NAME_BYTES} bytes and values up to {MAX_MCP_STDIO_ENV_VALUE_BYTES} bytes"
                    )));
                }
            }
            Self::Http { name, url, headers } | Self::Sse { name, url, headers } => {
                validate_capability_name(name)?;
                if url.is_empty() || url.len() > MAX_MCP_ENDPOINT_BYTES {
                    return Err(Error::InvalidConfig(format!(
                        "remote MCP endpoint must be 1..={MAX_MCP_ENDPOINT_BYTES} bytes"
                    )));
                }
                let parsed = url::Url::parse(url).map_err(|_| {
                    Error::InvalidConfig("remote MCP endpoint must be an absolute URL".into())
                })?;
                if !matches!(parsed.scheme(), "http" | "https")
                    || parsed.host_str().is_none()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || parsed.fragment().is_some()
                {
                    return Err(Error::InvalidConfig(
                        "remote MCP endpoint must be an http(s) URL without userinfo or a fragment"
                            .into(),
                    ));
                }
                if headers.len() > MAX_MCP_HEADERS {
                    return Err(Error::InvalidConfig(format!(
                        "remote MCP mounts accept at most {MAX_MCP_HEADERS} headers"
                    )));
                }
                let mut normalized_names = std::collections::HashSet::new();
                for (name, value) in headers {
                    let parsed_name = http::HeaderName::from_bytes(name.as_bytes());
                    if name.is_empty()
                        || name.len() > MAX_MCP_HEADER_NAME_BYTES
                        || parsed_name.is_err()
                        || value.len() > MAX_MCP_HEADER_VALUE_BYTES
                        || value.parse::<http::HeaderValue>().is_err()
                    {
                        return Err(Error::InvalidConfig(format!(
                            "remote MCP headers require valid names up to {MAX_MCP_HEADER_NAME_BYTES} bytes and values up to {MAX_MCP_HEADER_VALUE_BYTES} bytes"
                        )));
                    }
                    if !normalized_names.insert(parsed_name.expect("validated header name")) {
                        return Err(Error::InvalidConfig(
                            "remote MCP header names must be unique ignoring case".into(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Complete explicit external-service selection for an embedded runtime.
/// Empty/default values preserve the legacy `RuntimeConfig.endpoint` and
/// `RuntimeConfig.api_key` provider for every model.
#[derive(Clone, Default, PartialEq, serde::Deserialize)]
pub struct RuntimeServices {
    #[serde(default)]
    pub model_providers: BTreeMap<String, ProviderConfig>,
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
    pub mcp_elicitation_ui: Option<Arc<dyn McpElicitationUi>>,
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
            mcp_elicitation_ui: None,
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
    /// Forward-compatible catalog metadata from the native runtime contract.
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
