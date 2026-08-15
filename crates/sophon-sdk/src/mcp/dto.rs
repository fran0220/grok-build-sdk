// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::capability::validate_capability_name;
use crate::*;

/// Maximum byte length of a server-assigned MCP Task identity.
pub const MAX_MCP_TASK_ID_BYTES: usize = 256;
/// Maximum encoded bytes accepted when restoring one durable MCP Task identity.
pub const MAX_MCP_TASK_IDENTITY_BYTES: usize = 4 * 1024;
/// Maximum byte length of an MCP structured-input request identity.
pub const MAX_MCP_INPUT_REQUEST_ID_BYTES: usize = 256;
/// Maximum number of structured-input requests in one protocol round.
pub const MAX_MCP_INPUT_REQUESTS: usize = 16;
/// Maximum encoded bytes accepted for one structured-input round.
pub const MAX_MCP_INPUT_PAYLOAD_BYTES: usize = 256 * 1024;
/// Maximum encoded bytes accepted for one MCP Task projection.
pub const MAX_MCP_TASK_PAYLOAD_BYTES: usize = 1024 * 1024;
/// Maximum byte length of a server-supplied Task status message.
pub const MAX_MCP_TASK_STATUS_MESSAGE_BYTES: usize = 4 * 1024;

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
    pub(crate) continuation_identity: Option<McpContinuationIdentity>,
    #[serde(skip)]
    pub(crate) round_binding: Option<Box<McpInputRoundBinding>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct McpInputRoundBinding {
    requests: Vec<McpInputRequest>,
    request_state: Option<String>,
    raw: serde_json::Value,
}

impl McpInputRoundBinding {
    pub(crate) fn capture(input: &McpInputRequired) -> Self {
        Self {
            requests: input.requests.clone(),
            request_state: input.request_state.clone(),
            raw: input.raw.clone(),
        }
    }
}

/// The identity attached to an elicitation shown by the product UI. A Task
/// origin carries the stable Task identity as well as the current connection
/// generation; a direct MRTR round is necessarily generation-bound.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "origin", rename_all = "snake_case")]
pub enum McpElicitationOrigin {
    Operation {
        session_id: SessionId,
        server: String,
        client_id: u64,
    },
    Task {
        identity: McpTaskIdentity,
        client_id: u64,
    },
}

/// One bounded elicitation request routed to the embedding product's UI.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct McpElicitationUiRequest {
    pub origin: McpElicitationOrigin,
    pub request_id: String,
    pub params: crate::mcp_model::ElicitRequestParams,
}

/// Sole public authority that may produce an MCP elicitation answer. The SDK
/// never accepts an elicitation result as an argument to a generic Task or
/// continuation method; it obtains the result by invoking this product-owned
/// UI delegate and binds it to the request identity itself.
#[async_trait::async_trait]
pub trait McpElicitationUi: Send + Sync + 'static {
    async fn elicit(
        &self,
        request: McpElicitationUiRequest,
    ) -> Result<crate::mcp_model::ElicitResult, McpHostServiceError>;
}

pub(crate) struct McpElicitationUiAdapter(pub(crate) Arc<dyn McpElicitationUi>);

#[async_trait::async_trait]
impl xai_grok_mcp::servers::McpUiElicitationService for McpElicitationUiAdapter {
    async fn create_ui_elicitation(
        &self,
        context: xai_grok_mcp::servers::McpUiElicitationContext,
        request: crate::mcp_model::ElicitRequestParams,
    ) -> Result<crate::mcp_model::ElicitResult, McpHostServiceError> {
        let origin = if let Some(task_id) = context.task_id {
            McpElicitationOrigin::Task {
                identity: McpTaskIdentity::new(
                    SessionId::from_stored(context.host.session_id),
                    context.host.server_name,
                    task_id,
                )
                .map_err(|error| McpHostServiceError::denied(error.to_string()))?,
                client_id: context.host.client_id,
            }
        } else {
            McpElicitationOrigin::Operation {
                session_id: SessionId::from_stored(context.host.session_id),
                server: context.host.server_name,
                client_id: context.host.client_id,
            }
        };
        let request = checked_elicitation_request(origin, context.request_id, request)
            .map_err(|error| McpHostServiceError::denied(error.to_string()))?;
        let result = self.0.elicit(request).await?;
        validate_elicitation_result(&result)
            .map_err(|error| McpHostServiceError::denied(error.to_string()))?;
        Ok(result)
    }
}

pub(crate) fn checked_elicitation_request(
    origin: McpElicitationOrigin,
    request_id: String,
    params: crate::mcp_model::ElicitRequestParams,
) -> Result<McpElicitationUiRequest, Error> {
    if !valid_bounded_line(&request_id, MAX_MCP_INPUT_REQUEST_ID_BYTES) {
        return Err(Error::Operation(
            "MCP elicitation request identity is invalid".into(),
        ));
    }
    match &origin {
        McpElicitationOrigin::Operation {
            session_id, server, ..
        } => {
            if !valid_bounded_line(session_id.as_str(), MAX_SESSION_IDENTITY_BYTES)
                || validate_capability_name(server).is_err()
            {
                return Err(Error::Operation(
                    "MCP elicitation operation identity is invalid".into(),
                ));
            }
        }
        McpElicitationOrigin::Task { identity, .. } => {
            McpTaskIdentity::new(
                identity.session_id().clone(),
                identity.server(),
                identity.task_id(),
            )?;
        }
    }
    let bytes = serde_json::to_vec(&params)
        .map_err(|error| Error::Operation(format!("invalid MCP elicitation request: {error}")))?;
    if bytes.len() > MAX_MCP_INPUT_PAYLOAD_BYTES {
        return Err(Error::Operation(format!(
            "MCP elicitation request exceeds {MAX_MCP_INPUT_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(McpElicitationUiRequest {
        origin,
        request_id,
        params,
    })
}

pub(crate) fn validate_elicitation_result(
    result: &crate::mcp_model::ElicitResult,
) -> Result<(), Error> {
    let bytes = serde_json::to_vec(result)
        .map_err(|error| Error::Operation(format!("invalid MCP elicitation answer: {error}")))?;
    if bytes.len() > MAX_MCP_INPUT_PAYLOAD_BYTES {
        return Err(Error::Operation(format!(
            "MCP elicitation answer exceeds {MAX_MCP_INPUT_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct McpContinuation {
    pub(crate) input_responses: McpInputResponses,
    pub(crate) request_state: Option<String>,
    pub(crate) identity: McpContinuationIdentity,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct McpContinuationIdentity {
    pub(crate) session_id: SessionId,
    pub(crate) server: String,
    pub(crate) client_id: u64,
    pub(crate) operation: McpOperationIdentity,
    pub(crate) request_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum McpOperationIdentity {
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
    pub(crate) fn validated_requests(&self) -> Result<&[McpInputRequest], Error> {
        let binding = self.round_binding.as_ref().ok_or_else(|| {
            Error::Operation("MCP input requirement has no live SDK round binding".into())
        })?;
        if self.requests != binding.requests
            || self.request_state != binding.request_state
            || self.raw != binding.raw
        {
            return Err(Error::InvalidConfig(
                "MCP input requirement was modified after it was received".into(),
            ));
        }
        Ok(&binding.requests)
    }

    /// Builds the only continuation accepted by the SDK for this exact MRTR
    /// round. Every advertised request ID must be answered exactly once.
    /// Elicitation answers are deliberately refused here and can only come
    /// from [`McpElicitationUi`] through [`Runtime::resolve_mcp_input_with_ui`].
    pub fn respond(&self, input_responses: McpInputResponses) -> Result<McpContinuation, Error> {
        if self
            .requests
            .iter()
            .any(|request| request.kind == McpInputRequestKind::Elicitation)
        {
            return Err(Error::InvalidConfig(
                "MCP elicitation answers are accepted only from the product UI channel".into(),
            ));
        }
        self.bound_response(input_responses)
    }

    pub(crate) fn bound_response(
        &self,
        input_responses: McpInputResponses,
    ) -> Result<McpContinuation, Error> {
        self.validated_requests()?;
        validate_mcp_input_responses(&input_responses)?;
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

pub(crate) fn validate_mcp_input_responses(
    input_responses: &McpInputResponses,
) -> Result<(), Error> {
    if input_responses.len() > MAX_MCP_INPUT_REQUESTS
        || input_responses
            .keys()
            .any(|id| !valid_bounded_line(id, MAX_MCP_INPUT_REQUEST_ID_BYTES))
    {
        return Err(Error::InvalidConfig(
            "MCP input responses contain too many or invalid request identities".into(),
        ));
    }
    let bytes = serde_json::to_vec(input_responses)
        .map_err(|error| Error::InvalidConfig(format!("invalid MCP input responses: {error}")))?;
    if bytes.len() > MAX_MCP_INPUT_PAYLOAD_BYTES {
        return Err(Error::InvalidConfig(format!(
            "MCP input responses exceed {MAX_MCP_INPUT_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
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

pub(crate) fn valid_bounded_line(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value == value.trim()
        && !value.chars().any(char::is_control)
}

/// Stable, serializable identity of one server-owned MCP Task. Unlike
/// [`McpTaskHandle`], it deliberately contains no connection generation and
/// is therefore the value a Host records durably before restart/re-attach. A
/// Host must not reuse `server` for a different logical MCP authority while
/// any Task under that mount can still be recovered.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "McpTaskIdentityWire")]
pub struct McpTaskIdentity {
    session_id: SessionId,
    server: String,
    task_id: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct McpTaskIdentityWire {
    session_id: SessionId,
    server: String,
    task_id: String,
}

impl McpTaskIdentity {
    /// Restores a Host-persisted identity with a source-byte ceiling before
    /// serde allocates any field values.
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_MCP_TASK_IDENTITY_BYTES {
            return Err(Error::InvalidConfig(format!(
                "MCP Task identity exceeds {MAX_MCP_TASK_IDENTITY_BYTES} encoded bytes"
            )));
        }
        serde_json::from_slice(bytes)
            .map_err(|error| Error::InvalidConfig(format!("invalid MCP Task identity: {error}")))
    }

    pub fn new(
        session_id: SessionId,
        server: impl Into<String>,
        task_id: impl Into<String>,
    ) -> Result<Self, Error> {
        let server = server.into();
        let task_id = task_id.into();
        if !valid_bounded_line(session_id.as_str(), MAX_SESSION_IDENTITY_BYTES)
            || validate_capability_name(&server).is_err()
            || !valid_bounded_line(&task_id, MAX_MCP_TASK_ID_BYTES)
        {
            return Err(Error::InvalidConfig(
                "MCP Task identity contains an invalid session, server, or task identifier".into(),
            ));
        }
        Ok(Self {
            session_id,
            server,
            task_id,
        })
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }
}

impl TryFrom<McpTaskIdentityWire> for McpTaskIdentity {
    type Error = Error;

    fn try_from(value: McpTaskIdentityWire) -> Result<Self, Self::Error> {
        Self::new(value.session_id, value.server, value.task_id)
    }
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

impl McpTaskHandle {
    /// Extracts the validated identity a Host may persist. The generation-bound
    /// handle itself remains appropriate only for live get/update/cancel calls.
    pub fn durable_identity(&self) -> Result<McpTaskIdentity, Error> {
        McpTaskIdentity::new(
            self.session_id.clone(),
            self.server.clone(),
            self.task_id.clone(),
        )
    }
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

/// Result of reconciling a stable Task identity against the current Session
/// mount. `RecoveryRequired` is intentionally non-terminal: an unavailable,
/// unsupported, or ambiguous server answer never becomes fabricated success
/// and never authorizes redispatch of the original operation.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum McpTaskRecovery {
    Reattached { task: Box<McpTask> },
    RecoveryRequired { identity: McpTaskIdentity },
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
        input: Box<McpInputRequired>,
    },
    Task {
        handle: McpTaskHandle,
        task: Box<McpTask>,
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
    pub(crate) events: tokio::sync::mpsc::Receiver<serde_json::Value>,
    pub(crate) terminal: tokio::sync::oneshot::Receiver<serde_json::Value>,
    pub(crate) cancel: Option<tokio::sync::oneshot::Sender<()>>,
    pub(crate) pending_end: Option<McpSubscriptionEnd>,
    pub(crate) ended: bool,
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

impl McpTaskStatusEvent {
    /// Stable identity suitable for the Host's durable projection of this
    /// otherwise bounded in-memory status event.
    pub fn durable_identity(&self) -> Result<McpTaskIdentity, Error> {
        self.handle.durable_identity()
    }
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
