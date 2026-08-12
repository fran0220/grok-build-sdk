//! Protocol-neutral in-process facade for embedding the Grok runtime.
//!
//! ACP remains an adapter owned by the shell. Embedders call this facade with
//! native values and receive callbacks through [`EmbeddedClient`], so they do
//! not need to depend on the transport protocol or its gateway crate.

use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use agent_client_protocol::{self as acp, Agent as _, Client as _};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use xai_acp_lib::{AcpAgentGatewaySender, AcpGatewayReceiver};

use crate::agent::config::{Config, OriginEmbeddedProfile};
use crate::agent::models::ModelsManager;
use crate::agent::mvp_agent::MvpAgent;
use crate::auth::AuthManager;
use crate::session::state_authority::NativeSessionStateAuthority;

/// Error crossing the native embedded boundary.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
pub struct EmbeddedError {
    pub code: i32,
    pub message: String,
    pub data: Value,
}

impl EmbeddedError {
    pub fn new(code: i32, message: impl Into<String>, data: Value) -> Self {
        Self {
            code,
            message: message.into(),
            data,
        }
    }

    pub fn invalid_params() -> Self {
        Self::new(-32602, "Invalid params", Value::Null)
    }

    pub fn method_not_found() -> Self {
        Self::new(-32601, "Method not found", Value::Null)
    }

    pub fn internal_error() -> Self {
        Self::new(-32603, "Internal error", Value::Null)
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = data;
        self
    }
}

impl From<acp::Error> for EmbeddedError {
    fn from(error: acp::Error) -> Self {
        Self {
            code: i32::from(error.code),
            message: error.message,
            data: error.data.unwrap_or(Value::Null),
        }
    }
}

fn protocol_error(error: EmbeddedError) -> acp::Error {
    acp::Error::new(error.code, error.message).data(error.data)
}

/// Reverse calls emitted by the embedded runtime.
///
/// The method names are runtime operations such as `session/update`,
/// `fs/read_text_file`, and shell extension names. Parameters and results are
/// JSON because extensions are intentionally open-ended.
#[async_trait::async_trait(?Send)]
pub trait EmbeddedClient {
    async fn request(&self, method: &str, params: Value) -> Result<Value, EmbeddedError>;
    async fn notification(&self, method: &str, params: Value) -> Result<(), EmbeddedError>;
}

struct ClientBridge {
    client: Rc<dyn EmbeddedClient>,
}

impl ClientBridge {
    async fn request<T, R>(&self, method: &str, request: T) -> acp::Result<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let params = serde_json::to_value(request).map_err(|_| acp::Error::internal_error())?;
        let result = self
            .client
            .request(method, params)
            .await
            .map_err(protocol_error)?;
        serde_json::from_value(result).map_err(|error| {
            acp::Error::invalid_params()
                .data(serde_json::json!({"embeddedResponseError": error.to_string()}))
        })
    }
}

#[async_trait::async_trait(?Send)]
impl acp::Client for ClientBridge {
    async fn request_permission(
        &self,
        args: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
        self.request("session/request_permission", args).await
    }

    async fn read_text_file(
        &self,
        args: acp::ReadTextFileRequest,
    ) -> acp::Result<acp::ReadTextFileResponse> {
        self.request("fs/read_text_file", args).await
    }

    async fn write_text_file(
        &self,
        args: acp::WriteTextFileRequest,
    ) -> acp::Result<acp::WriteTextFileResponse> {
        self.request("fs/write_text_file", args).await
    }

    async fn create_terminal(
        &self,
        args: acp::CreateTerminalRequest,
    ) -> acp::Result<acp::CreateTerminalResponse> {
        self.request("terminal/create", args).await
    }

    async fn terminal_output(
        &self,
        args: acp::TerminalOutputRequest,
    ) -> acp::Result<acp::TerminalOutputResponse> {
        self.request("terminal/output", args).await
    }

    async fn wait_for_terminal_exit(
        &self,
        args: acp::WaitForTerminalExitRequest,
    ) -> acp::Result<acp::WaitForTerminalExitResponse> {
        self.request("terminal/wait_for_exit", args).await
    }

    async fn kill_terminal(
        &self,
        args: acp::KillTerminalRequest,
    ) -> acp::Result<acp::KillTerminalResponse> {
        self.request("terminal/kill", args).await
    }

    async fn release_terminal(
        &self,
        args: acp::ReleaseTerminalRequest,
    ) -> acp::Result<acp::ReleaseTerminalResponse> {
        self.request("terminal/release", args).await
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> acp::Result<()> {
        let params = serde_json::to_value(args).map_err(|_| acp::Error::internal_error())?;
        self.client
            .notification("session/update", params)
            .await
            .map_err(protocol_error)
    }

    async fn ext_method(&self, args: acp::ExtRequest) -> acp::Result<acp::ExtResponse> {
        let params = serde_json::from_str(args.params.get())
            .unwrap_or_else(|_| Value::String(args.params.get().to_owned()));
        let result = self
            .client
            .request(args.method.as_ref(), params)
            .await
            .map_err(protocol_error)?;
        let raw =
            serde_json::value::to_raw_value(&result).map_err(|_| acp::Error::internal_error())?;
        Ok(acp::ExtResponse::new(Arc::from(raw)))
    }

    async fn ext_notification(&self, args: acp::ExtNotification) -> acp::Result<()> {
        let params = serde_json::from_str(args.params.get())
            .unwrap_or_else(|_| Value::String(args.params.get().to_owned()));
        self.client
            .notification(args.method.as_ref(), params)
            .await
            .map_err(protocol_error)
    }
}

/// Host capabilities advertised to the native runtime.
#[derive(Clone, Debug, Default)]
pub struct EmbeddedClientCapabilities {
    pub fs_read: bool,
    pub fs_write: bool,
    pub terminal: bool,
    pub meta: Map<String, Value>,
}

/// MCP transport configuration accepted by the embedded facade.
#[derive(Clone, Debug)]
pub enum EmbeddedMcpServer {
    Stdio {
        name: String,
        command: PathBuf,
        args: Vec<String>,
        env: Vec<(String, String)>,
    },
    Http {
        name: String,
        url: String,
        headers: Vec<(String, String)>,
    },
    Sse {
        name: String,
        url: String,
        headers: Vec<(String, String)>,
    },
}

impl EmbeddedMcpServer {
    fn into_protocol(self) -> acp::McpServer {
        match self {
            Self::Stdio {
                name,
                command,
                args,
                env,
            } => acp::McpServer::Stdio(
                acp::McpServerStdio::new(name, command).args(args).env(
                    env.into_iter()
                        .map(|(k, v)| acp::EnvVariable::new(k, v))
                        .collect(),
                ),
            ),
            Self::Http { name, url, headers } => acp::McpServer::Http(
                acp::McpServerHttp::new(name, url).headers(
                    headers
                        .into_iter()
                        .map(|(k, v)| acp::HttpHeader::new(k, v))
                        .collect(),
                ),
            ),
            Self::Sse { name, url, headers } => acp::McpServer::Sse(
                acp::McpServerSse::new(name, url).headers(
                    headers
                        .into_iter()
                        .map(|(k, v)| acp::HttpHeader::new(k, v))
                        .collect(),
                ),
            ),
        }
    }
}

impl Serialize for EmbeddedMcpServer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.clone().into_protocol().serialize(serializer)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddedStopReason {
    End,
    Cancelled,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Other,
}

#[derive(Clone, Debug)]
pub struct EmbeddedCloseResponse {
    pub meta: Option<Map<String, Value>>,
}

#[derive(Clone, Debug)]
pub struct EmbeddedMcpRegistration {
    pub name: String,
    pub server_id: String,
}

/// In-process Grok agent entry point. The shell owns all ACP translation.
pub struct EmbeddedAgent {
    agent: Rc<MvpAgent>,
}

impl EmbeddedAgent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: &Config,
        auth: Arc<AuthManager>,
        models: ModelsManager,
        storage_root: PathBuf,
        profile: OriginEmbeddedProfile,
        session_state_authority: Option<Arc<dyn NativeSessionStateAuthority>>,
        client: Rc<dyn EmbeddedClient>,
    ) -> Self {
        let (gateway_tx, gateway_rx) = tokio::sync::mpsc::unbounded_channel();
        let agent = Rc::new(
            MvpAgent::with_origin_embedded_profile_models_and_session_state(
                AcpAgentGatewaySender::new(gateway_tx),
                cfg,
                auth,
                models,
                storage_root,
                profile,
                session_state_authority,
            ),
        );
        tokio::task::spawn_local(
            AcpGatewayReceiver::new(gateway_rx, ClientBridge { client })
                .with_on_meta(xai_file_utils::trace_context::span_from_meta_traceparent)
                .run(),
        );
        Self { agent }
    }

    pub fn set_embedded_mcp_servers(
        &self,
        servers: Vec<EmbeddedMcpRegistration>,
        invoker: Arc<dyn xai_grok_mcp::embedded_transport::EmbeddedMcpInvoker>,
    ) {
        self.agent.set_embedded_mcp_servers(
            servers
                .into_iter()
                .map(|server| xai_grok_mcp::servers::AcpServerEntry {
                    name: server.name,
                    server_id: server.server_id,
                })
                .collect(),
            invoker,
        );
    }

    pub async fn initialize(
        &self,
        capabilities: EmbeddedClientCapabilities,
    ) -> Result<(), EmbeddedError> {
        let request = acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_capabilities(
            acp::ClientCapabilities::new()
                .fs(acp::FileSystemCapabilities::new()
                    .read_text_file(capabilities.fs_read)
                    .write_text_file(capabilities.fs_write))
                .terminal(capabilities.terminal)
                .meta(capabilities.meta),
        );
        self.agent
            .initialize(request)
            .await
            .map(drop)
            .map_err(Into::into)
    }

    pub async fn new_session(
        &self,
        cwd: PathBuf,
        mcp_servers: Vec<EmbeddedMcpServer>,
        meta: Map<String, Value>,
    ) -> Result<String, EmbeddedError> {
        self.agent
            .new_session(
                acp::NewSessionRequest::new(cwd)
                    .mcp_servers(
                        mcp_servers
                            .into_iter()
                            .map(EmbeddedMcpServer::into_protocol)
                            .collect(),
                    )
                    .meta(meta),
            )
            .await
            .map(|response| response.session_id.0.to_string())
            .map_err(Into::into)
    }

    pub async fn load_session(
        &self,
        session_id: String,
        cwd: PathBuf,
        mcp_servers: Vec<EmbeddedMcpServer>,
        meta: Map<String, Value>,
    ) -> Result<(), EmbeddedError> {
        self.agent
            .load_session(
                acp::LoadSessionRequest::new(acp::SessionId::new(session_id), cwd)
                    .mcp_servers(
                        mcp_servers
                            .into_iter()
                            .map(EmbeddedMcpServer::into_protocol)
                            .collect(),
                    )
                    .meta(meta),
            )
            .await
            .map(drop)
            .map_err(Into::into)
    }

    pub async fn resume_session(
        &self,
        session_id: String,
        cwd: PathBuf,
        mcp_servers: Vec<EmbeddedMcpServer>,
        meta: Map<String, Value>,
    ) -> Result<(), EmbeddedError> {
        self.agent
            .resume_session(
                acp::ResumeSessionRequest::new(acp::SessionId::new(session_id), cwd)
                    .mcp_servers(
                        mcp_servers
                            .into_iter()
                            .map(EmbeddedMcpServer::into_protocol)
                            .collect(),
                    )
                    .meta(meta),
            )
            .await
            .map(drop)
            .map_err(Into::into)
    }

    pub async fn prompt(
        &self,
        session_id: String,
        blocks: Vec<Value>,
        meta: Map<String, Value>,
    ) -> Result<EmbeddedStopReason, EmbeddedError> {
        let blocks = blocks
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<acp::ContentBlock>, _>>()
            .map_err(|error| {
                EmbeddedError::invalid_params()
                    .with_data(serde_json::json!({"promptBlockError": error.to_string()}))
            })?;
        let response = self
            .agent
            .prompt(acp::PromptRequest::new(acp::SessionId::new(session_id), blocks).meta(meta))
            .await?;
        Ok(match response.stop_reason {
            acp::StopReason::EndTurn => EmbeddedStopReason::End,
            acp::StopReason::Cancelled => EmbeddedStopReason::Cancelled,
            acp::StopReason::MaxTokens => EmbeddedStopReason::MaxTokens,
            acp::StopReason::MaxTurnRequests => EmbeddedStopReason::MaxTurnRequests,
            acp::StopReason::Refusal => EmbeddedStopReason::Refusal,
            _ => EmbeddedStopReason::Other,
        })
    }

    pub async fn cancel(&self, session_id: String) -> Result<(), EmbeddedError> {
        self.agent
            .cancel(acp::CancelNotification::new(acp::SessionId::new(
                session_id,
            )))
            .await
            .map_err(Into::into)
    }

    pub async fn close_session(
        &self,
        session_id: String,
    ) -> Result<EmbeddedCloseResponse, EmbeddedError> {
        self.agent
            .close_session(acp::CloseSessionRequest::new(acp::SessionId::new(
                session_id,
            )))
            .await
            .map(|response| EmbeddedCloseResponse {
                meta: response.meta,
            })
            .map_err(Into::into)
    }

    pub async fn set_session_model(
        &self,
        session_id: String,
        model: String,
        meta: Option<Map<String, Value>>,
    ) -> Result<(), EmbeddedError> {
        self.agent
            .set_session_model(
                acp::SetSessionModelRequest::new(
                    acp::SessionId::new(session_id),
                    acp::ModelId::new(model),
                )
                .meta(meta),
            )
            .await
            .map(drop)
            .map_err(Into::into)
    }

    pub async fn set_session_mode(
        &self,
        session_id: String,
        mode: String,
    ) -> Result<(), EmbeddedError> {
        self.agent
            .set_session_mode(acp::SetSessionModeRequest::new(
                acp::SessionId::new(session_id),
                acp::SessionModeId::new(mode),
            ))
            .await
            .map(drop)
            .map_err(Into::into)
    }

    pub async fn extension(&self, method: &str, params: Value) -> Result<Value, EmbeddedError> {
        let raw = serde_json::value::to_raw_value(&params)
            .map_err(|_| EmbeddedError::internal_error())?;
        let response = self
            .agent
            .ext_method(acp::ExtRequest::new(method, Arc::from(raw)))
            .await?;
        serde_json::from_str(response.0.get()).map_err(|error| {
            EmbeddedError::internal_error()
                .with_data(serde_json::json!({"extensionResponseError": error.to_string()}))
        })
    }

    pub async fn extension_notification(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(), EmbeddedError> {
        let raw = serde_json::value::to_raw_value(&params)
            .map_err(|_| EmbeddedError::internal_error())?;
        self.agent
            .ext_notification(acp::ExtNotification::new(method, Arc::from(raw)))
            .await
            .map_err(Into::into)
    }

    pub async fn list_sessions(&self) -> Result<Value, EmbeddedError> {
        let response = self
            .agent
            .list_sessions(acp::ListSessionsRequest::new())
            .await?;
        serde_json::to_value(response).map_err(|_| EmbeddedError::internal_error())
    }

    pub fn reload_skills_for_session(&self, session_id: &str) -> bool {
        self.agent
            .reload_skills_for_session(&acp::SessionId::new(session_id))
    }

    pub async fn mcp_operation(
        &self,
        session_id: &str,
        server: String,
        operation: crate::extensions::mcp::McpModernOperation,
    ) -> Result<Value, String> {
        self.agent
            .sdk_mcp_modern_operation(session_id, server, operation)
            .await
    }

    pub async fn mcp_subscribe(
        &self,
        session_id: &str,
        server: String,
        filter: crate::extensions::mcp::McpModernSubscriptionFilter,
        capacity: std::num::NonZeroUsize,
    ) -> Result<crate::extensions::mcp::McpModernSubscription, String> {
        self.agent
            .sdk_mcp_modern_subscribe(session_id, server, filter, capacity)
            .await
    }
}
