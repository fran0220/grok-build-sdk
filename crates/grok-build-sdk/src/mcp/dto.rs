// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

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
