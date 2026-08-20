//! MCP extension methods and business logic.
//!
//! - `x.ai/mcp/list` — list available MCP servers (agent-scoped or session-annotated)
//! - `x.ai/mcp/call` — invoke an MCP tool directly, outside the LLM loop
//! - `x.ai/mcp/servers_updated` — local/plugin catalog after launch-dir discovery
//!   or a folder-trust grant (not gateway connectors)
//! - `x.ai/mcp/server_status` — per-server delta pushed by the
//!   `StatusDispatcher` (transport-closed pollers, handshake failures,
//!   config diffs, server-pushed list-changed notifications). See
//!   [`crate::session::mcp_dispatcher`] for the coalescing /
//!   payload-shaping logic. Re-exported below so other crates have a
//!   single import point.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use agent_client_protocol::{self as acp, Client};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as TokioMutex;
// rmcp is quarantined in xai-grok-mcp; see that crate's docs.
use xai_grok_mcp::rmcp;
// `wire::MCP_CALL` is the one cross-SDK contract literal; the agent-only siblings
// live in `mcp_methods` below.
use xai_grok_mcp::wire;

use super::{ExtResult, parse_params, to_ext_response};

/// Agent-only `x.ai/mcp/*` ACP method/notification names.
///
/// Unlike [`wire::MCP_CALL`] (the cross-SDK contract, which stays in
/// `xai_grok_mcp::wire`), these methods are private to the agent↔client channel and
/// are NOT spoken by the SDK. They are centralized here only to avoid scattering the
/// same string literal across dispatch and notification send sites.
pub mod mcp_methods {
    /// Shared prefix that routes every MCP ext method to this module's dispatcher.
    pub const PREFIX: &str = "x.ai/mcp/";

    pub const LIST: &str = "x.ai/mcp/list";
    pub const READ_RESOURCE: &str = "x.ai/mcp/read_resource";
    pub const LIST_RESOURCES: &str = "x.ai/mcp/resources/list";
    pub const LIST_PROMPTS: &str = "x.ai/mcp/prompts/list";
    pub const GET_PROMPT: &str = "x.ai/mcp/prompts/get";
    pub const COMPLETE: &str = "x.ai/mcp/complete";
    pub const AUTH_STATUS: &str = "x.ai/mcp/auth_status";
    pub const AUTH_TRIGGER: &str = "x.ai/mcp/auth_trigger";
    pub const SETUP: &str = "x.ai/mcp/setup";
    pub const TOGGLE: &str = "x.ai/mcp/toggle";
    pub const TOGGLE_TOOL: &str = "x.ai/mcp/toggle_tool";
    pub const UPSERT: &str = "x.ai/mcp/upsert";
    pub const DELETE: &str = "x.ai/mcp/delete";

    pub const SERVERS_UPDATED: &str = "x.ai/mcp/servers_updated";
    pub const TOOLS_CHANGED: &str = "x.ai/mcp/tools_changed";
    pub const INIT_PROGRESS: &str = "x.ai/mcp/init_progress";
}
use crate::agent::MvpAgent;
use crate::session::mcp_servers::{MCP_TOOL_NAME_DELIMITER, McpClient, McpServerName, McpState};

// ── Wire types: mcp/list ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpListRequest {
    #[serde(default)]
    pub session_id: Option<String>,
    /// Fail rather than silently returning the agent-level catalog when the
    /// caller requires a specific live session (used by the embedded SDK).
    #[serde(default)]
    pub require_session: bool,
    /// When false, bypass cache and refetch from cli-chat-proxy, then sync
    /// into live sessions so `search_tool` sees new tools. Use after OAuth
    /// enrollment or disconnect.
    #[serde(default = "default_true")]
    pub cache: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct McpListResponse {
    pub servers: Vec<McpServerEntry>,
}

/// Decode the canonical `x.ai/mcp/list` result without requiring embedded
/// consumers to duplicate the catalog wire schema.
pub fn parse_mcp_list_value(raw: serde_json::Value) -> serde_json::Result<Vec<McpServerEntry>> {
    serde_json::from_value::<McpListResponse>(raw).map(|response| response.servers)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<xai_grok_mcp::servers::McpIcon>,
    pub source: McpServerSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setup: Option<crate::util::config::McpSetupConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setup_values: Option<HashMap<String, String>>,
    #[serde(flatten)]
    pub config: McpServerConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<McpServerSessionState>,
}

/// MCP server config for the `mcp/list` catalog response.
///
/// Distinct from `acp::McpServer` (session/new input) because:
/// - HTTP: exposes `scope`/`scope_id`/`scope_name` for connector selection, NOT headers (auth tokens stay private)
/// - Stdio: same structure but optimized for JSON wire format
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum McpServerConfig {
    #[serde(rename = "http")]
    Http {
        url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        scope: Option<String>,
        #[serde(rename = "scopeId", skip_serializing_if = "Option::is_none")]
        scope_id: Option<String>,
        #[serde(rename = "scopeName", skip_serializing_if = "Option::is_none")]
        scope_name: Option<String>,
    },
    #[serde(rename = "stdio")]
    Stdio {
        command: std::path::PathBuf,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        env: Vec<McpEnvVar>,
    },
    #[serde(rename = "managedGateway")]
    ManagedGateway,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpEnvVar {
    pub name: String,
    pub value: String,
}

fn redact_sdk_catalog_entry(entry: &mut McpServerEntry) {
    entry.setup = None;
    entry.setup_values = None;
    match &mut entry.config {
        McpServerConfig::Http {
            url,
            scope,
            scope_id,
            scope_name,
        } => {
            url.clear();
            *scope = None;
            *scope_id = None;
            *scope_name = None;
        }
        McpServerConfig::Stdio { command, args, env } => {
            *command = std::path::PathBuf::new();
            args.clear();
            env.clear();
        }
        McpServerConfig::ManagedGateway => {}
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum McpServerSource {
    Managed,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerSessionState {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<McpSessionStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<McpToolEntry>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub auth_required: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub setup_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub negotiated: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum McpSessionStatus {
    Ready,
    Initializing,
    #[serde(alias = "setup_required")]
    SetupRequired,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct McpToolEntry {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<xai_grok_mcp::servers::McpIcon>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

// ── Wire types: mcp/call ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct McpCallRequest {
    /// When present: session pool. When absent: agent pool (config.toml only).
    #[serde(default)]
    pub session_id: Option<String>,
    pub server: String,
    /// Endpoint URL — disambiguates when multiple servers share a name.
    #[serde(default)]
    pub server_url: Option<String>,
    pub tool: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpCallResponse {
    /// Raw MCP content blocks. Keeping the protocol JSON intact prevents new
    /// rmcp content variants from being silently flattened or discarded.
    pub content: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
}

// ── Internal types (not serialized to wire) ─────────────────────────

#[derive(Debug, Clone, Default)]
pub struct McpStatusSnapshot {
    pub configs: Vec<acp::McpServer>,
    pub clients: Vec<McpClientStatus>,
    pub auth_required: std::collections::HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct McpClientStatus {
    pub name: String,
    pub status: McpSessionStatus,
    pub tools: Vec<McpToolEntry>,
    pub negotiated: Option<serde_json::Value>,
    pub icons: Vec<xai_grok_mcp::servers::McpIcon>,
}

// ── Notification: mcp/servers_updated ────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServersUpdated {
    pub mcp_servers: Vec<McpServerEntry>,
}

/// Per-server tool-list change push.
///
/// Emitted by [`crate::session::acp_session::AcpSession`] on the
/// post-handshake / auth-recovery and toggle-tool paths. The
/// `session_id` field lets the pager route
/// the push to the owning agent via `find_session_match` rather than
/// falling back to `app.active_view` (a latent multi-agent bug).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct McpToolsChanged {
    /// Session that owns this push. Pager routes via
    /// `find_session_match` so a background-agent push does not land
    /// on the foregrounded agent's modal.
    pub session_id: String,
    /// MCP server whose tool list changed.
    ///
    /// Currently unread by the pager — the pager treats every
    /// `tools_changed` push uniformly as a "schedule a debounced
    /// `mcp/list` refetch" trigger and re-reads the full catalog.
    /// The toggle-tool path therefore leaves this empty for
    /// forward-compat; any future field-aware optimization on the
    /// pager side would need to special-case empty as
    /// "non-server-scoped". No consumer reads that sentinel today.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub server_name: String,
    /// New tool entries for the named server.
    ///
    /// Currently unread by the pager for the same reason as
    /// `server_name` above. Empty on the toggle-tool path; populated
    /// on the post-handshake / auth-recovery paths so future
    /// field-aware consumers can avoid the `mcp/list` round trip.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<McpToolEntry>,
}

// Re-export the `x.ai/mcp/server_status` schema +
// method constant from the dispatcher module so external callers
// have a single import point alongside the other `x.ai/mcp/*`
// types.
//
// The canonical definitions still live in
// [`crate::session::mcp_dispatcher`] because their primary consumer
// is the dispatcher loop (and the unit tests there). The
// `session → extensions` direction is the inverse of the typical
// `extensions → session` flow, but moving the types here would
// require either making the dispatcher import from `extensions`
// (same inversion) or duplicating the schema. Leaving the
// re-export here keeps the single import-point ergonomic without
// duplicating definitions.
pub use crate::session::mcp_dispatcher::{
    McpServerStatus, McpServerStatusPayload, McpServerStatusReason, SERVER_STATUS_METHOD,
    TASK_STATUS_METHOD,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct McpReadResourceRequest {
    #[serde(default)]
    pub session_id: Option<String>,
    pub server: String,
    pub uri: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpReadResourceResponse {
    /// Raw MCP resource blocks. Unknown future variants remain available to
    /// typed SDK consumers instead of being discarded by this control plane.
    pub contents: Vec<serde_json::Value>,
}

/// Push the full MCP catalog to the client. Called in the background after
/// launch-dir MCP discovery so `initialize()` isn't blocked by config walks.
pub async fn notify_servers_updated(
    gateway: &xai_acp_lib::AcpAgentGatewaySender,
    local_servers: &[acp::McpServer],
) {
    let catalog = build_mcp_catalog(local_servers);
    let payload = McpServersUpdated {
        mcp_servers: catalog,
    };
    if let Ok(params) = serde_json::value::to_raw_value(&payload) {
        let notification = acp::ExtNotification::new(mcp_methods::SERVERS_UPDATED, params.into());
        let _ = gateway.ext_notification(notification).await;
        tracing::info!("Sent x.ai/mcp/servers_updated notification to client");
    }
}

// ── Dispatch ────────────────────────────────────────────────────────

/// Inbound `x.ai/mcp/*` methods this agent services, resolved from the wire string.
///
/// Single source of truth for forward-method routing: [`handle`] maps each variant to
/// its handler, and an unknown method yields `None` → `method_not_found`. The reverse
/// method [`wire::MCP_SDK_CALL`] is emit-only (agent→client) and has no variant here,
/// so a stray inbound reverse call is never misrouted to the forward `handle_call`.
#[derive(Debug, PartialEq, Eq)]
enum McpRoute {
    List,
    Call,
    ReadResource,
    ListResources,
    ListPrompts,
    GetPrompt,
    Complete,
    AuthStatus,
    AuthTrigger,
    Setup,
    Toggle,
    ToggleTool,
    Upsert,
    Delete,
}

fn route_mcp_method(method: &str) -> Option<McpRoute> {
    Some(match method {
        mcp_methods::LIST => McpRoute::List,
        wire::MCP_CALL => McpRoute::Call,
        mcp_methods::READ_RESOURCE => McpRoute::ReadResource,
        mcp_methods::LIST_RESOURCES => McpRoute::ListResources,
        mcp_methods::LIST_PROMPTS => McpRoute::ListPrompts,
        mcp_methods::GET_PROMPT => McpRoute::GetPrompt,
        mcp_methods::COMPLETE => McpRoute::Complete,
        mcp_methods::AUTH_STATUS => McpRoute::AuthStatus,
        mcp_methods::AUTH_TRIGGER => McpRoute::AuthTrigger,
        mcp_methods::SETUP => McpRoute::Setup,
        mcp_methods::TOGGLE => McpRoute::Toggle,
        mcp_methods::TOGGLE_TOOL => McpRoute::ToggleTool,
        mcp_methods::UPSERT => McpRoute::Upsert,
        mcp_methods::DELETE => McpRoute::Delete,
        _ => return None,
    })
}

#[tracing::instrument(skip_all, fields(method = %args.method))]
pub async fn handle(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    match route_mcp_method(args.method.as_ref()) {
        Some(McpRoute::List) => handle_list(agent, args).await,
        Some(McpRoute::Call) => handle_call(agent, args).await,
        Some(McpRoute::ReadResource) => handle_read_resource(agent, args).await,
        Some(McpRoute::ListResources) => {
            handle_primitive(agent, args, PrimitiveRoute::Resources).await
        }
        Some(McpRoute::ListPrompts) => handle_primitive(agent, args, PrimitiveRoute::Prompts).await,
        Some(McpRoute::GetPrompt) => handle_primitive(agent, args, PrimitiveRoute::GetPrompt).await,
        Some(McpRoute::Complete) => handle_primitive(agent, args, PrimitiveRoute::Complete).await,
        Some(McpRoute::AuthStatus) => handle_auth_status(agent, args).await,
        Some(McpRoute::AuthTrigger) => handle_auth_trigger(agent, args).await,
        Some(McpRoute::Setup) => handle_setup(agent, args).await,
        Some(McpRoute::Toggle) => handle_toggle(agent, args).await,
        Some(McpRoute::ToggleTool) => handle_toggle_tool(agent, args).await,
        Some(McpRoute::Upsert) => handle_upsert(agent, args).await,
        Some(McpRoute::Delete) => handle_delete(agent, args).await,
        None => Err(acp::Error::method_not_found()),
    }
}

// ── Catalog (shared by mcp/list and InitializeResponse._meta) ───────

/// Extract URL from an MCP server (HTTP/SSE only, None for Stdio).
fn mcp_server_url(server: &acp::McpServer) -> Option<&str> {
    match server {
        acp::McpServer::Http(acp::McpServerHttp { url, .. })
        | acp::McpServer::Sse(acp::McpServerSse { url, .. }) => Some(url.as_str()),
        acp::McpServer::Stdio(acp::McpServerStdio { .. }) => None,
        // TODO(acp-0.10): `McpServer` is #[non_exhaustive].
        _ => None,
    }
}

/// Build MCP server catalog: gateway rows + local servers, deduplicated by name.
/// Pure function — no I/O. Used by `mcp/list`, `InitializeResponse._meta`,
/// and `mcp/servers_updated`.
pub fn build_mcp_catalog(local_servers: &[acp::McpServer]) -> Vec<McpServerEntry> {
    build_mcp_catalog_with_gateway_tools(local_servers, None, &Default::default())
}

pub(crate) fn build_mcp_catalog_with_gateway_tools(
    local_servers: &[acp::McpServer],
    gateway_catalog: Option<&crate::session::managed_mcp::GatewayToolCatalog>,
    disabled_tools: &HashMap<String, HashSet<String>>,
) -> Vec<McpServerEntry> {
    let mut servers: Vec<McpServerEntry> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if let Some(catalog) = gateway_catalog {
        let reauth: HashSet<&str> = catalog
            .connectors_needing_reauth
            .iter()
            .map(String::as_str)
            .collect();
        let mut by_connector: BTreeMap<&str, Vec<&crate::session::managed_mcp::GatewayTool>> =
            BTreeMap::new();
        for tool in &catalog.tools {
            by_connector
                .entry(tool.connector_id.as_str())
                .or_default()
                .push(tool);
        }

        for (connector_id, tools) in by_connector {
            let connector_name = tools
                .first()
                .map(|tool| tool.connector_name.as_str())
                .unwrap_or(connector_id);
            let disabled = disabled_tools.get(connector_id);
            let server_disabled = disabled_tools
                .get(crate::util::config::MANAGED_GATEWAY_DISABLED_CONNECTORS_KEY)
                .is_some_and(|set| set.contains(connector_id));
            let auth_required = reauth.contains(connector_id) || reauth.contains(connector_name);
            let name = managed_gateway_entry_name(connector_id);
            seen.insert(name.clone());
            servers.push(McpServerEntry {
                name,
                display_name: Some(connector_name.to_owned()),
                icons: Vec::new(),
                source: McpServerSource::Managed,
                config: McpServerConfig::ManagedGateway,
                source_label: None,
                setup: None,
                setup_values: None,
                session: Some(McpServerSessionState {
                    enabled: !server_disabled,
                    status: (!auth_required && !server_disabled).then_some(McpSessionStatus::Ready),
                    tools: tools
                        .into_iter()
                        .map(|tool| {
                            let qualified_name = tool.qualified_name();
                            McpToolEntry {
                                name: qualified_name.clone(),
                                icons: Vec::new(),
                                display_name: Some(tool.tool_name.clone()),
                                description: Some(tool.description.clone()),
                                meta: None,
                                enabled: disabled.is_none_or(|set| !set.contains(&qualified_name)),
                            }
                        })
                        .collect(),
                    auth_required,
                    setup_required: false,
                    negotiated: None,
                }),
            });
        }
    }

    // Local servers (HTTP or Stdio)
    for server in local_servers {
        let name = crate::session::mcp_servers::mcp_server_name(server).to_string();
        if seen.insert(name.clone()) {
            let source = McpServerSource::Local;
            let config = match server {
                acp::McpServer::Http(acp::McpServerHttp { url, .. })
                | acp::McpServer::Sse(acp::McpServerSse { url, .. }) => McpServerConfig::Http {
                    url: url.clone(),
                    scope: None,
                    scope_id: None,
                    scope_name: None,
                },
                acp::McpServer::Stdio(acp::McpServerStdio {
                    command, args, env, ..
                }) => McpServerConfig::Stdio {
                    command: command.clone(),
                    args: args.clone(),
                    env: env
                        .iter()
                        .map(|e| McpEnvVar {
                            name: e.name.clone(),
                            value: e.value.clone(),
                        })
                        .collect(),
                },
                // TODO(acp-0.10): `McpServer` is #[non_exhaustive].
                _ => continue,
            };
            servers.push(McpServerEntry {
                name,
                display_name: None,
                icons: Vec::new(),
                source,
                config,
                source_label: None,
                setup: None,
                setup_values: None,
                session: None,
            });
        }
    }

    servers
}

pub const MANAGED_GATEWAY_ENTRY_PREFIX: &str = "managed_gateway:";

fn managed_gateway_entry_name(connector_id: &str) -> String {
    format!("{MANAGED_GATEWAY_ENTRY_PREFIX}{connector_id}")
}

fn managed_gateway_connector_id(entry_name: &str) -> Option<&str> {
    entry_name.strip_prefix(MANAGED_GATEWAY_ENTRY_PREFIX)
}

fn disabled_server_placeholder_entry(name: &str) -> McpServerEntry {
    let is_managed_gateway = name.starts_with(MANAGED_GATEWAY_ENTRY_PREFIX);
    let source = if is_managed_gateway {
        McpServerSource::Managed
    } else {
        McpServerSource::Local
    };
    let config = if is_managed_gateway {
        McpServerConfig::ManagedGateway
    } else {
        McpServerConfig::Stdio {
            command: std::path::PathBuf::new(),
            args: Vec::new(),
            env: Vec::new(),
        }
    };
    McpServerEntry {
        name: name.to_owned(),
        display_name: name
            .strip_prefix(MANAGED_GATEWAY_ENTRY_PREFIX)
            .map(str::to_owned),
        icons: Vec::new(),
        source,
        source_label: None,
        setup: None,
        setup_values: None,
        config,
        session: Some(McpServerSessionState {
            enabled: false,
            status: None,
            tools: vec![],
            auth_required: false,
            setup_required: false,
            negotiated: None,
        }),
    }
}

// ── Session-level operations (called via SessionCommand) ────────────

/// Build session MCP status: which servers are enabled, healthy, and what tools they expose.
/// Clones state under lock then releases — does not hold lock across awaits.
pub(crate) async fn build_mcp_status(
    mcp_state: &Arc<TokioMutex<McpState>>,
    tool_bridge: &Arc<xai_grok_tools::bridge::ToolBridge>,
    event_writer: Option<&xai_grok_session_events::EventWriter>,
) -> McpStatusSnapshot {
    let _build_mcp_status_timer = crate::instrumentation::timer("build_mcp_status");
    let (
        configs,
        clients,
        _is_initializing,
        initializing_servers,
        mcp_tool_meta,
        mcp_tool_icons,
        auth_required,
        init_failed,
        disabled_regs,
    ) = {
        let state = mcp_state.lock().await;
        (
            state.configs.clone(),
            state
                .all_clients()
                .map(|(_, c)| c.clone())
                .collect::<Vec<_>>(),
            state.is_initializing(),
            state.handshaking_servers_cloned(),
            state.mcp_tool_meta.clone(),
            state.mcp_tool_icons.clone(),
            state.auth_required.clone(),
            state.init_failed.clone(),
            // Collect (qualified_name, description) for disabled tools so we
            // can include them in the snapshot without cloning the full registration.
            state
                .disabled_tool_registrations
                .iter()
                .map(|(k, v)| (k.clone(), v.description.clone()))
                .collect::<Vec<_>>(),
        )
    };

    let mut client_statuses = Vec::with_capacity(clients.len());
    let _client_loop_timer = crate::instrumentation::timer("mcp_status_client_loop");

    for client in &clients {
        let name = client.server_name().to_string();
        let prefix = format!("{}{}", name, MCP_TOOL_NAME_DELIMITER);

        let healthy = client.is_healthy().await;
        if let Some(ew) = event_writer {
            ew.emit(xai_grok_session_events::Event::McpHealthCheck {
                server_name: name.clone(),
                healthy,
                client_state: Some(if healthy { "ready" } else { "unavailable" }.to_string()),
            });
        }
        // A server whose background init failed (handshake/`tools/list`
        // error or timeout) is reported as Unavailable even when the
        // transport is still technically alive — otherwise a server that
        // connected but wedged on `tools/list` (0 tools registered) would
        // misleadingly show as Ready.
        let ready = healthy && !init_failed.contains_key(name.as_str());
        let (status, tools) = if ready {
            let _tool_defs_timer = crate::instrumentation::timer("mcp_status_tool_definitions");
            let mut tools: Vec<McpToolEntry> = tool_bridge
                .tool_definitions()
                .await
                .into_iter()
                .filter(|t| t.function.name.starts_with(&prefix))
                .map(|t| {
                    let qualified_name = &t.function.name;
                    let unqualified = qualified_name
                        .strip_prefix(&prefix)
                        .unwrap_or(qualified_name)
                        .to_string();
                    let meta = mcp_tool_meta.get(qualified_name).cloned();
                    let icons = mcp_tool_icons
                        .get(qualified_name)
                        .cloned()
                        .unwrap_or_default();
                    McpToolEntry {
                        name: unqualified,
                        display_name: None,
                        description: t.function.description.clone(),
                        meta,
                        icons,
                        enabled: true,
                    }
                })
                .collect();

            // Include disabled tools from stashed registrations.
            for (qname, desc) in &disabled_regs {
                if qname.starts_with(&prefix) {
                    let unqualified = qname.strip_prefix(&prefix).unwrap_or(qname).to_string();
                    let meta = mcp_tool_meta.get(qname).cloned();
                    let icons = mcp_tool_icons.get(qname).cloned().unwrap_or_default();
                    tools.push(McpToolEntry {
                        name: unqualified,
                        display_name: None,
                        description: Some(desc.clone()),
                        meta,
                        icons,
                        enabled: false,
                    });
                }
            }

            // Stable alphabetical order so tools don't jump around
            // when toggled between enabled and disabled.
            tools.sort_by(|a, b| a.name.cmp(&b.name));

            (McpSessionStatus::Ready, tools)
        } else {
            (McpSessionStatus::Unavailable, vec![])
        };

        let icons = client.server_icons().await;
        client_statuses.push(McpClientStatus {
            name,
            status,
            tools,
            negotiated: client.negotiated_info_json().await,
            icons,
        });
    }

    // Configured but not yet handshaked (either global init or per-server bg init) → Initializing.
    // We use initializing_servers (populated before spawning handshakes) so that
    // slow servers continue showing Initializing after we call finish_init() early.
    for config in &configs {
        let cname = crate::session::mcp_servers::mcp_server_name(config);
        if !client_statuses.iter().any(|c| c.name == cname) && initializing_servers.contains(cname)
        {
            client_statuses.push(McpClientStatus {
                name: cname.to_string(),
                status: McpSessionStatus::Initializing,
                tools: vec![],
                negotiated: None,
                icons: Vec::new(),
            });
        }
    }

    McpStatusSnapshot {
        configs,
        clients: client_statuses,
        auth_required,
    }
}

/// Ensure the agent-level MCP pool is initialized, waiting if another
/// caller is already initializing. Safe to call concurrently.
async fn ensure_agent_pool_initialized(mcp_state: &Arc<TokioMutex<McpState>>) {
    loop {
        let state = mcp_state.lock().await;
        if state.is_initialized() {
            return;
        }
        if state.is_initializing() {
            // Another call is initializing — wait and retry.
            drop(state);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        }
        drop(state);
        let cwd = std::env::current_dir().unwrap_or_default();
        init_agent_mcp_pool(mcp_state, &cwd).await;
        return;
    }
}

/// Spawn config.toml MCP clients into the agent pool. Handshakes happen
/// lazily on first `CallMcpTool`.
pub(crate) async fn init_agent_mcp_pool(
    mcp_state: &Arc<TokioMutex<McpState>>,
    cwd: &std::path::Path,
) {
    use crate::session::mcp_servers::start_mcp_servers;

    let configs = {
        let mut state = mcp_state.lock().await;
        if !state.try_start_init() {
            return;
        }
        state.configs.clone()
    };

    if configs.is_empty() {
        let mut state = mcp_state.lock().await;
        state.finish_init();
        return;
    }

    let noop = xai_grok_session_events::EventWriter::noop();
    // session_less picks Interactive to preserve prior deferred-OAuth behavior. A session-less SDK
    // agent can reach this non-interactively; threading real non-interactivity is a deliberate follow-up.
    let ctx = crate::session::mcp_servers::McpSpawnCtx::session_less(&noop);
    let meta = Default::default();
    let oauth = Default::default();
    let results = start_mcp_servers(configs, Some(cwd), &meta, &oauth, &ctx).await;
    let clients: HashMap<McpServerName, Arc<McpClient>> = results
        .into_iter()
        .filter_map(|r| match r {
            Ok(client) => {
                tracing::info!("Agent MCP server '{}' spawned", client.server_name());
                let name = client.server_name().to_string();
                Some((name, Arc::new(client)))
            }
            Err(e) => {
                tracing::warn!("Failed to spawn agent MCP server: {}", e);
                None
            }
        })
        .collect();

    let mut state = mcp_state.lock().await;
    state.owned_clients = clients;
    state.finish_init();
    tracing::info!(
        "Agent MCP pool: {} servers ready",
        state.owned_clients.len()
    );
}

/// Call an MCP tool directly (outside the LLM tool-use loop).
#[tracing::instrument(name = "mcp.call_tool", skip_all, fields(server_name, tool_name))]
pub async fn call_mcp_tool(
    mcp_state: &Arc<TokioMutex<McpState>>,
    server_name: &str,
    server_url: Option<&str>,
    tool_name: &str,
    arguments: serde_json::Value,
) -> Result<McpCallResponse, String> {
    let client = {
        let state = mcp_state.lock().await;

        // Resolve: (name + url) > url-only > name-only.
        let target = if let Some(url) = server_url {
            let config_name =
                |c: &acp::McpServer| crate::session::mcp_servers::mcp_server_name(c).to_string();
            state
                .configs
                .iter()
                .find(|c| {
                    crate::session::mcp_servers::mcp_server_name(c) == server_name
                        && mcp_server_url(c) == Some(url)
                })
                .map(&config_name)
                .or_else(|| {
                    state
                        .configs
                        .iter()
                        .find(|c| mcp_server_url(c) == Some(url))
                        .map(&config_name)
                })
                .unwrap_or_else(|| server_name.to_string())
        } else {
            server_name.to_string()
        };

        Arc::clone(
            state
                .get_client(&target)
                .ok_or_else(|| format!("server '{}' not found", target))?,
        )
    };

    let tool_timeout_sec = client.tool_timeout_for(tool_name);
    let timeout = std::time::Duration::from_secs(tool_timeout_sec);
    let result = tokio::time::timeout(timeout, client.call_tool(tool_name, arguments))
        .await
        .map_err(|_| format!("tool '{}' timed out after {}s", tool_name, tool_timeout_sec))?
        .map_err(|e| format!("tool call failed: {}", e))?;

    let raw = serde_json::to_value(&result)
        .map_err(|e| format!("failed to serialize MCP tool result: {e}"))?;
    let content = raw["content"].as_array().cloned().unwrap_or_default();

    Ok(McpCallResponse {
        content,
        structured_content: raw.get("structuredContent").cloned(),
        is_error: result.is_error,
        meta: raw.get("_meta").cloned(),
    })
}

// ── mcp/list handler ────────────────────────────────────────────────

async fn handle_list(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    // Latency layout: gateway catalog fetch and the session-state branch
    // (conditional `retry_auth_required_servers` then `build_mcp_status`)
    // run concurrently via tokio::join!. OAuth retries only fire on explicit
    // refresh (cache=false); cached opens skip them so the warm path stays
    // fast.
    let req = parse_params::<McpListRequest>(args)?;

    let cwd = req
        .session_id
        .as_ref()
        .and_then(|sid| agent.get_session_cwd(&acp::SessionId::new(sid.clone())))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());

    // Resolve the session handle synchronously up front so the session-state
    // future can be polled alongside the gateway catalog fetch.
    let session_handle = req.session_id.as_ref().and_then(|sid| {
        let acp_id = acp::SessionId::new(sid.clone());
        agent.get_session_handle(&acp_id)
    });
    if let (Some(sid), None) = (req.session_id.as_ref(), session_handle.as_ref()) {
        if req.require_session {
            return Err(acp::Error::invalid_params().data("session not found"));
        }
        tracing::debug!(
            session_id = %sid,
            "mcp/list: session not found, returning agent-level catalog only"
        );
    }

    let cache = req.cache;
    let session_state_fut = async {
        let handle = session_handle.as_ref()?;
        // Auth retries belong on explicit refresh: skipping them on cached
        // opens saves ~500ms when multiple OAuth servers are configured.
        if !cache {
            handle.retry_auth_required_servers().await;
        }
        Some(handle.get_mcp_status().await)
    };

    let (gateway_catalog, session_snapshot) = tokio::join!(
        agent.fetch_gateway_catalog_for_mcp_list(cache),
        session_state_fut
    );

    let compat = agent.cfg.borrow().compat_resolved;
    let plugin_registry_snapshot = agent.plugin_registry_snapshot();
    // An embedded runtime's catalog is exactly what its host declared; the
    // machine's own MCP configuration is not part of the product surface.
    let caller_declared_only =
        agent.mcp_source_scope() == crate::session::managed_mcp::McpSourceScope::CallerDeclared;
    let local_servers = if caller_declared_only {
        Vec::new()
    } else {
        crate::util::config::load_mcp_servers(&cwd, &compat)
    };
    let disabled_tools = if caller_declared_only {
        Default::default()
    } else {
        crate::util::config::get_all_mcp_disabled_tools(&cwd)
    };
    let mut servers = build_mcp_catalog_with_gateway_tools(
        &local_servers,
        gateway_catalog.as_ref(),
        &disabled_tools,
    );
    let disabled_names = if caller_declared_only {
        Default::default()
    } else {
        crate::util::config::disabled_mcp_server_names(&cwd)
    };
    let setup_entries = if caller_declared_only {
        Default::default()
    } else {
        crate::util::config::collect_mcp_setup_configs(
            &cwd,
            plugin_registry_snapshot.as_deref(),
            &compat,
        )
    };
    let preferences = crate::util::config::load_mcp_preferences().file();
    for (name, setup_entry) in setup_entries {
        if servers.iter().any(|entry| entry.name == name) {
            continue;
        }
        let enabled = !disabled_names.contains(&name);
        let setup_schema = setup_entry.config.setup.clone();
        let (setup, setup_required, status) = match setup_entry
            .config
            .resolve_setup(preferences.servers.get(&name))
        {
            crate::util::config::McpSetupResolution::Required(setup) => {
                (Some(setup), true, Some(McpSessionStatus::SetupRequired))
            }
            // Surface schema/template breakage instead of dropping the row.
            crate::util::config::McpSetupResolution::Invalid(_) => {
                (setup_schema, true, Some(McpSessionStatus::SetupRequired))
            }
            crate::util::config::McpSetupResolution::Resolved(_) => continue,
        };
        let values = preferences
            .servers
            .get(&name)
            .map(|prefs| prefs.values.clone());
        servers.push(McpServerEntry {
            name: name.clone(),
            icons: Vec::new(),
            display_name: None,
            source: McpServerSource::Local,
            source_label: setup_entry
                .source
                .plugin
                .as_ref()
                .map(|plugin| format!("plugin: {plugin}")),
            setup,
            setup_values: values,
            config: McpServerConfig::Http {
                url: String::new(),
                scope: None,
                scope_id: None,
                scope_name: None,
            },
            session: Some(McpServerSessionState {
                enabled,
                status,
                tools: vec![],
                auth_required: false,
                setup_required,
                negotiated: None,
            }),
        });
    }

    // Disabled stubs: only names Space enable can still resolve (see
    // `crate::util::config::mcp_reenable`). Orphans with no definition stay hidden.
    let catalog_names: HashSet<String> = servers.iter().map(|s| s.name.clone()).collect();
    let discovery = crate::session::managed_mcp::McpDiscoveryInputs {
        cwd: &cwd,
        plugin_registry: plugin_registry_snapshot.as_deref(),
        compat: &compat,
    };
    if !caller_declared_only {
        let stubs = crate::util::config::reenableable_disabled_stubs(
            &disabled_names,
            &catalog_names,
            &discovery,
        );
        for name in stubs {
            servers.push(disabled_server_placeholder_entry(&name));
        }
    }

    if let Some(snapshot) = session_snapshot {
        if gateway_catalog.is_some()
            && let Some(disabled) = match session_handle.as_ref() {
                Some(h) => Some(h.managed_gateway_disabled_tool_names().await),
                None => None,
            }
        {
            for entry in &mut servers {
                if entry.source == McpServerSource::Managed
                    && let Some(session) = entry.session.as_mut()
                {
                    let connector_id =
                        managed_gateway_connector_id(&entry.name).unwrap_or(&entry.name);
                    if let Some(tools) = disabled.get(connector_id) {
                        for tool in &mut session.tools {
                            if tools.contains(&tool.name) {
                                tool.enabled = false;
                            }
                        }
                    }
                }
            }
        }
        // `session_snapshot` is `Some` only when `session_handle` resolved,
        // which requires `req.session_id` to have been `Some`. Rather than
        // assert that non-local invariant with `expect` (which a future
        // refactor of `session_state_fut` could silently turn into a panic
        // in a request handler), use a local `if let` guard around the only
        // consumer — the debug log. We emit `%sid` (Display) to match the
        // sibling "session not found" log; `?req.session_id` would wrap the
        // bare string as `Some("...")` and diverge from the earlier format.
        if let Some(sid) = req.session_id.as_ref() {
            tracing::debug!(session_id = %sid, "Annotating mcp/list with session state");
        }
        let catalog_names: std::collections::HashSet<String> =
            servers.iter().map(|s| s.name.clone()).collect();

        // Annotate catalog entries with session state.
        for entry in &mut servers {
            if entry
                .session
                .as_ref()
                .is_some_and(|session| session.setup_required)
            {
                continue;
            }
            let managed_gateway_session = entry.source == McpServerSource::Managed
                && matches!(&entry.config, McpServerConfig::ManagedGateway);
            if managed_gateway_session {
                if let Some(session) = entry.session.as_mut() {
                    let connector_id =
                        managed_gateway_connector_id(&entry.name).unwrap_or(&entry.name);
                    let managed_disabled = disabled_tools
                        .get(crate::util::config::MANAGED_GATEWAY_DISABLED_CONNECTORS_KEY)
                        .is_some_and(|set| set.contains(connector_id));
                    session.enabled = !disabled_names.contains(&entry.name) && !managed_disabled;
                }
                continue;
            }
            let enabled = snapshot
                .configs
                .iter()
                .any(|c| crate::session::mcp_servers::mcp_server_name(c) == entry.name);
            let (status, tools, icons) = snapshot
                .clients
                .iter()
                .find(|c| c.name == entry.name)
                .map(|c| (Some(c.status.clone()), c.tools.clone(), c.icons.clone()))
                .unwrap_or((None, vec![], Vec::new()));
            entry.icons = icons;
            entry.session = Some(McpServerSessionState {
                enabled,
                status,
                tools,
                auth_required: snapshot.auth_required.contains(&entry.name),
                setup_required: false,
                negotiated: snapshot
                    .clients
                    .iter()
                    .find(|client| client.name == entry.name)
                    .and_then(|client| client.negotiated.clone()),
            });
        }

        // Append session-only servers (passed via session/new but not in catalog).
        for client_status in &snapshot.clients {
            if !catalog_names.contains(&client_status.name) {
                servers.push(McpServerEntry {
                    name: client_status.name.clone(),
                    icons: client_status.icons.clone(),
                    display_name: None,
                    source: McpServerSource::Local,
                    source_label: None,
                    setup: None,
                    setup_values: None,
                    config: McpServerConfig::Stdio {
                        command: std::path::PathBuf::new(),
                        args: Vec::new(),
                        env: Vec::new(),
                    },
                    session: Some(McpServerSessionState {
                        enabled: true,
                        status: Some(client_status.status.clone()),
                        tools: client_status.tools.clone(),
                        auth_required: snapshot.auth_required.contains(&client_status.name),
                        setup_required: false,
                        negotiated: client_status.negotiated.clone(),
                    }),
                });
            }
        }
    }

    // Tag servers with the owning plugin (covers both a plugin's .mcp.json and
    // its inline plugin.json mcpServers via the registry's deduped owner map).
    if let Some(registry) = plugin_registry_snapshot.as_ref() {
        for entry in &mut servers {
            if entry.source_label.is_none()
                && let Some(plugin_name) = registry.mcp_server_owner(&entry.name)
            {
                entry.source_label = Some(format!("plugin: {plugin_name}"));
            }
        }
    }
    if req.require_session {
        for entry in &mut servers {
            redact_sdk_catalog_entry(entry);
        }
    }
    to_ext_response(Ok(McpListResponse { servers }))
}

// ── mcp/call handler ────────────────────────────────────────────────

async fn handle_call(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpCallRequest>(args)?;

    let result = match req.session_id {
        Some(sid) => {
            // Session-provided servers: route through the session's MCP pool.
            // Load-race-tolerant: waits for an in-flight `session/load`
            // (reconnect replay after a leader restart) before failing.
            let acp_id = acp::SessionId::new(sid);
            let handle = agent
                .session_handle_waiting_for_load(&acp_id)
                .await
                .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;
            handle
                .call_mcp_tool(req.server, req.server_url, req.tool, req.arguments)
                .await
        }
        None => {
            // No session: use the agent-level MCP pool (config.toml servers).
            let mcp_state = agent.agent_mcp_state();
            ensure_agent_pool_initialized(&mcp_state).await;
            call_mcp_tool(
                &mcp_state,
                &req.server,
                req.server_url.as_deref(),
                &req.tool,
                req.arguments,
            )
            .await
        }
    }
    .map_err(|e| acp::Error::internal_error().data(e))?;

    to_ext_response(Ok(result))
}

async fn handle_read_resource(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpReadResourceRequest>(args)?;

    let result = if let Some(ref sid) = req.session_id {
        // Load-race-tolerant: see `handle_call` above.
        let acp_id = acp::SessionId::new(sid.clone());
        let handle = agent
            .session_handle_waiting_for_load(&acp_id)
            .await
            .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;
        handle
            .read_mcp_resource(req.server.clone(), req.uri.clone())
            .await
    } else {
        let mcp_state = agent.agent_mcp_state();
        ensure_agent_pool_initialized(&mcp_state).await;
        read_mcp_resource(&mcp_state, &req.server, &req.uri).await
    }
    .map_err(|e| acp::Error::internal_error().data(e))?;

    to_ext_response(Ok(result))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpPrimitiveRequest {
    session_id: String,
    server: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    reference: Option<String>,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    argument: Option<String>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    context: Option<HashMap<String, String>>,
}

#[derive(Debug)]
enum PrimitiveRoute {
    Resources,
    Prompts,
    GetPrompt,
    Complete,
}

#[derive(Debug)]
pub enum McpPrimitiveOperation {
    ListResources,
    ListPrompts,
    GetPrompt {
        name: String,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
    },
    Complete {
        reference: String,
        target: String,
        argument: String,
        value: String,
        context: Option<HashMap<String, String>>,
    },
}

/// Headless MCP 2026 operations used by the in-process SDK boundary. These
/// are deliberately separate from ACP extension request DTOs so MRTR state
/// and generation-bound Tasks cannot be lost in the compatibility routes.
#[derive(Debug)]
pub enum McpModernOperation {
    Ping,
    NotifyRootsListChanged,
    CallToolOnce {
        tool_name: String,
        arguments: serde_json::Value,
        input_responses: Option<std::collections::BTreeMap<String, serde_json::Value>>,
        request_state: Option<String>,
        expected_client_id: Option<u64>,
    },
    GetPromptOnce {
        name: String,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
        input_responses: Option<std::collections::BTreeMap<String, serde_json::Value>>,
        request_state: Option<String>,
        expected_client_id: Option<u64>,
    },
    ReadResourceOnce {
        uri: String,
        input_responses: Option<std::collections::BTreeMap<String, serde_json::Value>>,
        request_state: Option<String>,
        expected_client_id: Option<u64>,
    },
    GetTask {
        client_id: u64,
        task_id: String,
    },
    RecoverTask {
        task_id: String,
    },
    UpdateTask {
        client_id: u64,
        task_id: String,
        expected_task: serde_json::Value,
        input_responses: std::collections::BTreeMap<String, serde_json::Value>,
    },
    CancelTask {
        client_id: u64,
        task_id: String,
    },
}

#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct McpModernSubscriptionFilter {
    pub tools_list_changed: bool,
    pub prompts_list_changed: bool,
    pub resources_list_changed: bool,
    pub resource_subscriptions: Vec<String>,
}

#[doc(hidden)]
pub struct McpModernSubscription {
    pub client_id: u64,
    pub acknowledged: serde_json::Value,
    pub events: tokio::sync::mpsc::Receiver<serde_json::Value>,
    pub terminal: tokio::sync::oneshot::Receiver<serde_json::Value>,
    pub cancel: tokio::sync::oneshot::Sender<()>,
}

async fn handle_primitive(
    agent: &MvpAgent,
    args: &acp::ExtRequest,
    route: PrimitiveRoute,
) -> ExtResult {
    let req = parse_params::<McpPrimitiveRequest>(args)?;
    let required = |value: Option<String>, field: &str| {
        value.ok_or_else(|| acp::Error::invalid_params().data(format!("missing {field}")))
    };
    let operation = match route {
        PrimitiveRoute::Resources => McpPrimitiveOperation::ListResources,
        PrimitiveRoute::Prompts => McpPrimitiveOperation::ListPrompts,
        PrimitiveRoute::GetPrompt => McpPrimitiveOperation::GetPrompt {
            name: required(req.name, "name")?,
            arguments: req.arguments,
        },
        PrimitiveRoute::Complete => McpPrimitiveOperation::Complete {
            reference: required(req.reference, "reference")?,
            target: required(req.target, "target")?,
            argument: required(req.argument, "argument")?,
            value: req.value.unwrap_or_default(),
            context: req.context,
        },
    };
    let handle = agent
        .session_handle_waiting_for_load(&acp::SessionId::new(req.session_id))
        .await
        .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;
    let result = handle
        .mcp_primitive(req.server, operation)
        .await
        .map_err(|e| acp::Error::internal_error().data(e))?;
    to_ext_response(Ok(result))
}

pub(crate) async fn run_mcp_primitive(
    mcp_state: &Arc<TokioMutex<McpState>>,
    server_name: &str,
    operation: McpPrimitiveOperation,
) -> Result<serde_json::Value, String> {
    let client = {
        let state = mcp_state.lock().await;
        Arc::clone(
            state
                .get_client(server_name)
                .ok_or_else(|| format!("server '{server_name}' not found"))?,
        )
    };
    match operation {
        McpPrimitiveOperation::ListResources => {
            let (resources, resource_templates) = tokio::try_join!(
                client.list_resources_json(),
                client.list_resource_templates_json()
            )
            .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"resources": resources, "resourceTemplates": resource_templates}))
        }
        McpPrimitiveOperation::ListPrompts => client
            .list_prompts_json()
            .await
            .map(|prompts| serde_json::json!({"prompts": prompts}))
            .map_err(|e| e.to_string()),
        McpPrimitiveOperation::GetPrompt { name, arguments } => client
            .get_prompt_json(name, arguments)
            .await
            .map_err(|e| e.to_string()),
        McpPrimitiveOperation::Complete {
            reference,
            target,
            argument,
            value,
            context,
        } => client
            .complete_argument_json(&reference, target, argument, value, context)
            .await
            .map_err(|e| e.to_string()),
    }
}

pub(crate) async fn run_mcp_modern_operation(
    mcp_state: &Arc<TokioMutex<McpState>>,
    server_name: &str,
    operation: McpModernOperation,
) -> Result<serde_json::Value, String> {
    let client = {
        let state = mcp_state.lock().await;
        Arc::clone(
            state
                .get_client(server_name)
                .ok_or_else(|| format!("server '{server_name}' not found"))?,
        )
    };
    match operation {
        McpModernOperation::Ping => client
            .ping()
            .await
            .map(|client_id| serde_json::json!({"clientId": client_id})),
        McpModernOperation::NotifyRootsListChanged => client
            .notify_roots_list_changed()
            .await
            .map(|client_id| serde_json::json!({"clientId": client_id})),
        McpModernOperation::CallToolOnce {
            tool_name,
            arguments,
            input_responses,
            request_state,
            expected_client_id,
        } => {
            client
                .call_tool_once_json(
                    tool_name,
                    arguments,
                    input_responses,
                    request_state,
                    expected_client_id,
                )
                .await
        }
        McpModernOperation::GetPromptOnce {
            name,
            arguments,
            input_responses,
            request_state,
            expected_client_id,
        } => {
            client
                .get_prompt_once_json(
                    name,
                    arguments,
                    input_responses,
                    request_state,
                    expected_client_id,
                )
                .await
        }
        McpModernOperation::ReadResourceOnce {
            uri,
            input_responses,
            request_state,
            expected_client_id,
        } => {
            client
                .read_resource_once_json(uri, input_responses, request_state, expected_client_id)
                .await
        }
        McpModernOperation::GetTask { client_id, task_id } => {
            client.get_task_json(client_id, task_id).await
        }
        McpModernOperation::RecoverTask { task_id } => client.recover_task_json(task_id).await,
        McpModernOperation::UpdateTask {
            client_id,
            task_id,
            expected_task,
            input_responses,
        } => client
            .update_task_if_current(client_id, task_id, expected_task, input_responses)
            .await
            .map(|()| serde_json::json!({"clientId": client_id})),
        McpModernOperation::CancelTask { client_id, task_id } => client
            .cancel_task(client_id, task_id)
            .await
            .map(|()| serde_json::json!({"clientId": client_id})),
    }
    .map_err(|error| error.to_string())
}

pub(crate) async fn start_mcp_modern_subscription(
    mcp_state: &Arc<TokioMutex<McpState>>,
    server_name: &str,
    filter: McpModernSubscriptionFilter,
    capacity: std::num::NonZeroUsize,
) -> Result<McpModernSubscription, String> {
    let client = {
        let state = mcp_state.lock().await;
        Arc::clone(
            state
                .get_client(server_name)
                .ok_or_else(|| format!("server '{server_name}' not found"))?,
        )
    };
    let service = client
        .ensure_initialized()
        .await
        .map_err(|error| error.to_string())?;
    let client_id = service.connection_generation();
    let mut requested = rmcp::model::SubscriptionFilter::new();
    requested.tools_list_changed = filter.tools_list_changed.then_some(true);
    requested.prompts_list_changed = filter.prompts_list_changed.then_some(true);
    requested.resources_list_changed = filter.resources_list_changed.then_some(true);
    requested.resource_subscriptions =
        (!filter.resource_subscriptions.is_empty()).then_some(filter.resource_subscriptions);
    let mut subscription = service
        .peer()
        .listen_with_capacity(requested, capacity)
        .await
        .map_err(|error| error.to_string())?;
    let acknowledged =
        serde_json::to_value(subscription.acknowledged()).map_err(|error| error.to_string())?;
    let (events_tx, events) = tokio::sync::mpsc::channel(capacity.get());
    let (terminal_tx, terminal) = tokio::sync::oneshot::channel();
    let (cancel, mut cancel_rx) = tokio::sync::oneshot::channel();
    tokio::task::spawn_local(async move {
        let mut terminal_tx = Some(terminal_tx);
        loop {
            let next = tokio::select! {
                biased;
                _ = &mut cancel_rx => {
                    if let Some(terminal_tx) = terminal_tx.take() {
                        let _ = terminal_tx.send(serde_json::json!({
                            "reason": "cancelled",
                        }));
                    }
                    let _ = subscription.cancel_with_reason(
                        Some("SDK subscription cancelled".to_owned()),
                    ).await;
                    break;
                }
                next = subscription.next() => next,
            };
            match next {
                Ok(Some(notification)) => {
                    let Ok(notification) = serde_json::to_value(notification) else {
                        continue;
                    };
                    let event = serde_json::json!({
                        "type": "notification",
                        "notification": notification,
                    });
                    match events_tx.try_send(event) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            if let Some(terminal_tx) = terminal_tx.take() {
                                let _ = terminal_tx.send(serde_json::json!({
                                    "reason": "lagged",
                                    "capacity": capacity.get(),
                                }));
                            }
                            let _ = subscription
                                .cancel_with_reason(Some(
                                    "SDK subscription notification queue is full".to_owned(),
                                ))
                                .await;
                            break;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            let _ = subscription
                                .cancel_with_reason(Some(
                                    "SDK subscription receiver dropped".to_owned(),
                                ))
                                .await;
                            break;
                        }
                    }
                }
                Ok(None) => {
                    let end = match subscription.end() {
                        Some(rmcp::service::SubscriptionEnd::Graceful(result)) => {
                            serde_json::json!({
                                "reason": "graceful",
                                "result": result,
                            })
                        }
                        Some(rmcp::service::SubscriptionEnd::Cancelled) => {
                            serde_json::json!({"reason": "cancelled"})
                        }
                        Some(rmcp::service::SubscriptionEnd::Lagged { capacity }) => {
                            serde_json::json!({
                                "reason": "lagged",
                                "capacity": capacity,
                            })
                        }
                        Some(rmcp::service::SubscriptionEnd::Abrupt) | None => {
                            serde_json::json!({"reason": "abrupt"})
                        }
                        Some(_) => serde_json::json!({"reason": "abrupt"}),
                    };
                    if let Some(terminal_tx) = terminal_tx.take() {
                        let _ = terminal_tx.send(end);
                    }
                    break;
                }
                Err(error) => {
                    if let Some(terminal_tx) = terminal_tx.take() {
                        let _ = terminal_tx.send(serde_json::json!({
                            "reason": "error",
                            "message": error.to_string(),
                        }));
                    }
                    break;
                }
            }
        }
    });
    Ok(McpModernSubscription {
        client_id,
        acknowledged,
        events,
        terminal,
        cancel,
    })
}

pub(crate) async fn read_mcp_resource(
    mcp_state: &Arc<TokioMutex<McpState>>,
    server_name: &str,
    uri: &str,
) -> Result<McpReadResourceResponse, String> {
    let client = {
        let state = mcp_state.lock().await;
        Arc::clone(
            state
                .get_client(server_name)
                .ok_or_else(|| format!("server '{}' not found", server_name))?,
        )
    };

    let mcp_service = client
        .ensure_initialized()
        .await
        .map_err(|e| format!("MCP init failed: {}", e))?;

    let result = mcp_service
        .read_resource(rmcp::model::ReadResourceRequestParams::new(uri))
        .await
        .map_err(|e| format!("resource read failed: {}", e))?;

    if result.contents.is_empty() {
        return Err("empty resource".to_string());
    }

    let raw = serde_json::to_value(&result)
        .map_err(|e| format!("failed to serialize MCP resource result: {e}"))?;
    let contents = raw["contents"].as_array().cloned().unwrap_or_default();

    if contents.is_empty() {
        return Err("resource contained only unsupported content variants".to_string());
    }

    Ok(McpReadResourceResponse { contents })
}

// ── McpResourceProvider bridge ───────────────────────────────────────
//
// Implements the `McpResourceProvider` trait from xai-grok-tools so that
// `ListMcpResources` / `FetchMcpResource` tools can access MCP
// servers without depending on `xai-grok-mcp` directly.

/// Bridge from `McpState` to the `McpResourceProvider` trait.
///
/// Injected into the agent's `SharedResources` via `tool_bridge.update_resource()`
/// at session startup so tools can enumerate and fetch MCP resources.
pub(crate) struct McpStateResourceProvider(pub Arc<TokioMutex<McpState>>);

#[async_trait::async_trait]
impl xai_grok_tools::types::resources::McpResourceProvider for McpStateResourceProvider {
    async fn list_resources(
        &self,
        server: Option<String>,
    ) -> Result<Vec<xai_grok_tools::types::resources::McpResourceInfo>, String> {
        let clients: Vec<(String, Arc<McpClient>)> = {
            let state = self.0.lock().await;
            match &server {
                Some(name) => match state.get_client(name) {
                    Some(c) => vec![(name.clone(), Arc::clone(c))],
                    None => return Err(format!("MCP server '{name}' not found")),
                },
                None => state
                    .all_clients()
                    .map(|(name, client)| (name.to_string(), Arc::clone(client)))
                    .collect(),
            }
        };

        let mut resources = Vec::new();
        for (server_name, client) in clients {
            let mcp_service = match client.ensure_initialized().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        server = %server_name,
                        error = %e,
                        "Failed to initialize MCP server for list_resources"
                    );
                    continue;
                }
            };

            match mcp_service.list_all_resources().await {
                Ok(all_resources) => {
                    for r in all_resources {
                        resources.push(xai_grok_tools::types::resources::McpResourceInfo {
                            uri: r.uri.clone(),
                            name: Some(r.name.clone()),
                            description: r.description.clone(),
                            mime_type: r.mime_type.clone(),
                            server: server_name.clone(),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        server = %server_name,
                        error = %e,
                        "list_resources RPC failed"
                    );
                    if server.is_some() {
                        return Err(format!("list_resources failed for '{server_name}': {e}"));
                    }
                    // For all-servers mode, skip failures and continue.
                }
            }
        }

        Ok(resources)
    }

    async fn read_resource(
        &self,
        server: String,
        uri: String,
    ) -> Result<xai_grok_tools::types::resources::McpResourceReadResult, String> {
        let client = {
            let state = self.0.lock().await;
            Arc::clone(
                state
                    .get_client(&server)
                    .ok_or_else(|| format!("MCP server '{server}' not found"))?,
            )
        };

        let mcp_service = client
            .ensure_initialized()
            .await
            .map_err(|e| format!("MCP init failed: {e}"))?;

        let result = mcp_service
            .read_resource(rmcp::model::ReadResourceRequestParams::new(uri.clone()))
            .await
            .map_err(|e| format!("resource read failed: {e}"))?;

        if result.contents.is_empty() {
            return Err(format!("Resource not found: {uri}"));
        }

        let first = result
            .contents
            .into_iter()
            .find(|c| {
                let supported = matches!(
                    c,
                    rmcp::model::ResourceContents::TextResourceContents { .. }
                        | rmcp::model::ResourceContents::BlobResourceContents { .. }
                );
                if !supported {
                    tracing::warn!(uri, "skipping unknown MCP resource content variant");
                }
                supported
            })
            .ok_or_else(|| format!("Unsupported resource content type for: {uri}"))?;
        match first {
            rmcp::model::ResourceContents::TextResourceContents {
                uri: content_uri,
                mime_type,
                text,
                ..
            } => Ok(xai_grok_tools::types::resources::McpResourceReadResult {
                uri: content_uri,
                name: None,
                description: None,
                mime_type,
                content: Some(xai_grok_tools::types::resources::McpResourceContent::Text(
                    text,
                )),
            }),
            rmcp::model::ResourceContents::BlobResourceContents {
                uri: content_uri,
                mime_type,
                blob,
                ..
            } => Ok(xai_grok_tools::types::resources::McpResourceReadResult {
                uri: content_uri,
                name: None,
                description: None,
                mime_type,
                content: Some(xai_grok_tools::types::resources::McpResourceContent::Blob(
                    blob.into_bytes(),
                )),
            }),
            // Unreachable: `first` is pre-filtered to supported variants, but
            // `ResourceContents` is non_exhaustive so the match must be total.
            _ => Err(format!("Unsupported resource content type for: {uri}")),
        }
    }
}

// ── Auth status / trigger ────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct McpAuthStatusRequest {
    session_id: String,
}

#[derive(serde::Serialize)]
pub struct McpAuthStatusEntry {
    pub server_name: String,
    pub status: &'static str,
}

#[derive(serde::Serialize)]
struct McpAuthStatusResponse {
    servers: Vec<McpAuthStatusEntry>,
}

async fn handle_auth_status(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpAuthStatusRequest>(args)?;
    let acp_id = acp::SessionId::new(req.session_id);
    let handle = agent
        .get_session_handle(&acp_id)
        .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;
    let entries = handle.mcp_auth_status().await;
    to_ext_response(Ok(McpAuthStatusResponse { servers: entries }))
}

#[derive(serde::Deserialize)]
struct McpAuthTriggerRequest {
    session_id: String,
    server_name: String,
}

#[derive(serde::Serialize)]
struct McpAuthTriggerResponse {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    setup: Option<crate::util::config::McpSetupConfig>,
    /// Descriptive failure reason from the shell. `None` on success and on
    /// failures with no detail; surfaced verbatim by the TUI.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn handle_auth_trigger(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpAuthTriggerRequest>(args)?;
    let acp_id = acp::SessionId::new(req.session_id);
    let handle = agent
        .get_session_handle(&acp_id)
        .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;
    let cwd = agent
        .get_session_cwd(&acp_id)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let setup_entries = crate::util::config::collect_mcp_setup_configs(
        &cwd,
        agent.plugin_registry_snapshot().as_deref(),
        &agent.cfg.borrow().compat_resolved,
    );
    let preferences = crate::util::config::load_mcp_preferences().file();
    if let Some(entry) = setup_entries.get(&req.server_name) {
        match entry
            .config
            .resolve_setup(preferences.servers.get(&req.server_name))
        {
            crate::util::config::McpSetupResolution::Required(setup) => {
                return to_ext_response(Ok(McpAuthTriggerResponse {
                    status: "setup_required",
                    setup: Some(setup),
                    error: None,
                }));
            }
            crate::util::config::McpSetupResolution::Invalid(reason) => {
                return to_ext_response(Ok(McpAuthTriggerResponse {
                    status: "setup_required",
                    setup: entry.config.setup.clone(),
                    error: Some(reason),
                }));
            }
            crate::util::config::McpSetupResolution::Resolved(_) => {}
        }
    }
    match handle.mcp_auth_trigger(req.server_name).await {
        Ok(()) => to_ext_response(Ok(McpAuthTriggerResponse {
            status: "authenticated",
            setup: None,
            error: None,
        })),
        Err(e) => {
            tracing::warn!(%e, "MCP auth trigger failed");
            to_ext_response(Ok(McpAuthTriggerResponse {
                status: "failed",
                setup: None,
                error: Some(e),
            }))
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpSetupRequest {
    session_id: String,
    server_name: String,
    values: HashMap<String, String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct McpSetupResponse {
    ok: bool,
}

async fn handle_setup(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpSetupRequest>(args)?;
    let acp_id = acp::SessionId::new(req.session_id.clone());
    let handle = agent
        .get_session_handle(&acp_id)
        .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;
    let cwd = agent
        .get_session_cwd(&acp_id)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let setup_entries = crate::util::config::collect_mcp_setup_configs(
        &cwd,
        agent.plugin_registry_snapshot().as_deref(),
        &agent.cfg.borrow().compat_resolved,
    );
    let entry = setup_entries
        .get(&req.server_name)
        .ok_or_else(|| acp::Error::invalid_params().data("server setup not found"))?;
    let setup = entry
        .config
        .setup
        .as_ref()
        .ok_or_else(|| acp::Error::invalid_params().data("server setup not found"))?;

    // Only schema field ids (never arbitrary client keys).
    let filtered_values: HashMap<String, String> = setup
        .fields
        .iter()
        .filter_map(|field| {
            req.values
                .get(&field.id)
                .map(|value| (field.id.clone(), value.clone()))
        })
        .collect();

    let pending_preferences = crate::util::config::McpServerPreferences {
        values: filtered_values,
        source: Some(entry.source.clone()),
        updated_at: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
    };
    match entry.config.resolve_setup(Some(&pending_preferences)) {
        crate::util::config::McpSetupResolution::Resolved(_) => {}
        crate::util::config::McpSetupResolution::Required(_) => {
            return Err(acp::Error::invalid_params().data("setup values incomplete"));
        }
        crate::util::config::McpSetupResolution::Invalid(reason) => {
            return Err(acp::Error::invalid_params().data(reason));
        }
    }

    let load = crate::util::config::load_mcp_preferences();
    if !load.is_writable() {
        return Err(acp::Error::internal_error().data(
            "MCP preferences file is unreadable; fix or remove mcp_preferences.json before saving",
        ));
    }
    let mut prefs = load.file();
    let previous_entry = prefs.servers.get(&req.server_name).cloned();
    prefs
        .servers
        .insert(req.server_name.clone(), pending_preferences);
    crate::util::config::save_mcp_preferences(&prefs)
        .await
        .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;

    let rollback_prefs = || async {
        let _ = crate::util::config::restore_mcp_preference_server(
            &req.server_name,
            previous_entry.clone(),
        )
        .await;
    };

    // Presence check with personal disable ignored (no config write yet).
    let plugin_reg = agent.plugin_registry_snapshot();
    let compat = agent.cfg.borrow().compat_resolved;
    let discovery = crate::session::managed_mcp::McpDiscoveryInputs {
        cwd: &cwd,
        plugin_registry: plugin_reg.as_deref(),
        compat: &compat,
    };
    let discovered =
        crate::session::managed_mcp::discover_mcp_definitions_ignoring_disable(&discovery);
    let Some(probe) = discovered.get(&req.server_name) else {
        rollback_prefs().await;
        return Err(acp::Error::internal_error().data("server did not resolve after setup"));
    };
    let allowlist = &xai_grok_workspace::permission::resolution::managed_settings().mcp_allowlist;
    if !allowlist.is_server_allowed(probe) {
        rollback_prefs().await;
        let reason =
            crate::session::managed_mcp::McpDisabledReason::for_blocked_server(allowlist, probe);
        return Err(acp::Error::invalid_params().data(reason.to_string()));
    }

    // Clear disable only after resolve succeeds, then merge for a spawnable
    // transport.
    let was_disabled =
        crate::util::config::disabled_mcp_server_names(&cwd).contains(&req.server_name);
    let enable_paths = if was_disabled {
        match crate::util::config::save_mcp_server_enabled_in(&req.server_name, true, &cwd).await {
            Ok(paths) => paths,
            Err(e) => {
                rollback_prefs().await;
                return Err(acp::Error::internal_error().data(format!(
                    "failed to clear disabled MCP server entry after setup resolve: {e}"
                )));
            }
        }
    } else {
        Vec::new()
    };

    let restore_disable = || async {
        if !was_disabled {
            return;
        }
        if let Err(re) = crate::util::config::restore_mcp_server_enabled_after_enable(
            &req.server_name,
            &enable_paths,
        )
        .await
        {
            tracing::warn!(
                server = req.server_name.as_str(),
                error = %re,
                "Failed to restore MCP enable state after setup failure"
            );
        }
    };

    // Prefs + enable-tier restore for failures after enable wrote config.
    let rollback_after_enable = || async {
        rollback_prefs().await;
        restore_disable().await;
    };

    let found = crate::session::managed_mcp::merge_managed_mcp_servers_with_policy(
        vec![],
        &cwd,
        plugin_reg.as_deref(),
        &compat,
        agent.mcp_source_scope(),
    )
    .into_iter()
    .find(|s| crate::session::mcp_servers::mcp_server_name(&s.server) == req.server_name);

    let server = match found {
        Some(s) if s.disabled_reason.is_none() => s.server,
        Some(s) => {
            rollback_after_enable().await;
            return Err(acp::Error::invalid_params().data(
                s.disabled_reason
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "blocked by organization policy".into()),
            ));
        }
        None => {
            rollback_after_enable().await;
            return Err(acp::Error::internal_error().data("server did not resolve after setup"));
        }
    };

    if let Err(e) = handle
        .toggle_mcp_server(req.server_name.clone(), true, Some(server))
        .await
    {
        rollback_after_enable().await;
        return Err(acp::Error::internal_error().data(e.to_string()));
    }

    to_ext_response(Ok(McpSetupResponse { ok: true }))
}

// ── mcp/toggle handler ───────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct McpToggleRequest {
    session_id: String,
    server_name: String,
    enabled: bool,
    /// Do not persist preferences; intended for embedded session control planes.
    #[serde(default)]
    session_local: bool,
}

#[derive(serde::Serialize)]
struct McpToggleResponse {
    ok: bool,
}

async fn handle_toggle(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpToggleRequest>(args)?;
    let acp_id = acp::SessionId::new(req.session_id.clone());

    let handle = agent
        .get_session_handle(&acp_id)
        .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;

    let gateway_connector_id = managed_gateway_connector_id(&req.server_name);

    // Persist re-enable outside the session actor (async I/O). Config mutation
    // happens atomically inside via ToggleMcpServer.
    let server_config = if req.enabled {
        let cwd = agent
            .get_session_cwd(&acp_id)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
        if let Some(connector_id) = gateway_connector_id {
            if !req.session_local
                && let Err(e) =
                    crate::util::config::save_mcp_server_enabled_in(&req.server_name, true, &cwd)
                        .await
            {
                tracing::warn!(
                    server = req.server_name.as_str(),
                    error = %e,
                    "Failed to clear disabled MCP server entry for managed gateway connector"
                );
            }
            if req.session_local {
                handle
                    .toggle_managed_gateway_tool_session_local(
                        connector_id.to_string(),
                        String::new(),
                        true,
                    )
                    .await
            } else {
                handle
                    .toggle_managed_gateway_tool(connector_id.to_string(), String::new(), true)
                    .await
            }
            .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;
            return to_ext_response(Ok(McpToggleResponse { ok: true }));
        }
        if !req.session_local
            && let Err(e) =
                crate::util::config::save_mcp_server_enabled_in(&req.server_name, true, &cwd).await
        {
            tracing::warn!(
                server = req.server_name.as_str(),
                error = %e,
                "Failed to persist server re-enable before lookup"
            );
        }

        // Include the session's admitted client set. Without this seed a
        // session-local disable followed by enable could only resolve servers
        // from ambient config, not servers supplied by an embedded SDK host.
        let client_servers = agent
            .resident_handle(&acp_id)
            .map(|handle| handle.initial_client_mcp_servers)
            .unwrap_or_default();
        let all_servers_with_policy =
            crate::session::managed_mcp::merge_managed_mcp_servers_with_policy(
                client_servers,
                &cwd,
                agent.plugin_registry_snapshot().as_deref(),
                &agent.cfg.borrow().compat_resolved,
                agent.mcp_source_scope(),
            );
        let found = all_servers_with_policy
            .into_iter()
            .find(|s| crate::session::mcp_servers::mcp_server_name(&s.server) == req.server_name);
        match found {
            Some(s) if s.disabled_reason.is_some() => {
                let display = req.server_name.as_str();
                // Capitalize first letter for display.
                let mut chars = display.chars();
                let capitalized: String = match chars.next() {
                    Some(c) => c.to_uppercase().chain(chars).collect(),
                    None => display.to_string(),
                };
                let path = match &s.disabled_reason {
                    Some(
                        crate::session::managed_mcp::McpDisabledReason::Allowlist { source }
                        | crate::session::managed_mcp::McpDisabledReason::Denylist { source },
                    ) => source.display().to_string(),
                    None => String::new(),
                };
                return Err(acp::Error::invalid_params().data(format!(
                    "The server {capitalized} can't be enabled due to an organization policy ({path}).",
                )));
            }
            None => {
                return Err(acp::Error::invalid_params()
                    .data(format!("server '{}' not found in config", req.server_name)));
            }
            _ => {}
        }
        found.map(|s| s.server)
    } else if let Some(connector_id) = gateway_connector_id {
        if req.session_local {
            handle
                .toggle_managed_gateway_tool_session_local(
                    connector_id.to_string(),
                    String::new(),
                    false,
                )
                .await
        } else {
            handle
                .toggle_managed_gateway_tool(connector_id.to_string(), String::new(), false)
                .await
        }
        .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;
        return to_ext_response(Ok(McpToggleResponse { ok: true }));
    } else {
        None
    };

    if req.session_local {
        handle
            .toggle_mcp_server_session_local(req.server_name, req.enabled, server_config)
            .await
    } else {
        handle
            .toggle_mcp_server(req.server_name, req.enabled, server_config)
            .await
    }
    .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;

    to_ext_response(Ok(McpToggleResponse { ok: true }))
}

// ── mcp/toggle_tool handler ─────────────────────────────────────────

#[derive(serde::Deserialize)]
struct McpToggleToolRequest {
    session_id: String,
    server_name: String,
    tool_name: String,
    enabled: bool,
    /// Do not persist preferences; intended for embedded session control planes.
    #[serde(default)]
    session_local: bool,
}

async fn handle_toggle_tool(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpToggleToolRequest>(args)?;
    let acp_id = acp::SessionId::new(req.session_id.clone());

    let handle = agent
        .get_session_handle(&acp_id)
        .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;

    // `managed_gateway:` is reserved, so route by prefix alone — never consult
    // the catalog, or a stale tool toggle would fall back to the local path.
    let gateway_connector_id = managed_gateway_connector_id(&req.server_name);
    let is_managed_gateway = gateway_connector_id.is_some();

    if is_managed_gateway {
        if req.session_local {
            handle
                .toggle_managed_gateway_tool_session_local(
                    gateway_connector_id.unwrap_or(&req.server_name).to_string(),
                    req.tool_name,
                    req.enabled,
                )
                .await
        } else {
            handle
                .toggle_managed_gateway_tool(
                    gateway_connector_id.unwrap_or(&req.server_name).to_string(),
                    req.tool_name,
                    req.enabled,
                )
                .await
        }
    } else if req.session_local {
        handle
            .toggle_mcp_tool_session_local(req.server_name, req.tool_name, req.enabled)
            .await
    } else {
        handle
            .toggle_mcp_tool(req.server_name, req.tool_name, req.enabled)
            .await
    }
    .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;

    to_ext_response(Ok(McpToggleResponse { ok: true }))
}

// ── mcp/upsert handler ──────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct McpUpsertRequest {
    session_id: String,
    server_name: String,
    #[serde(flatten)]
    config: crate::util::config::McpServerConfig,
}

async fn handle_upsert(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpUpsertRequest>(args)?;
    let acp_id = acp::SessionId::new(req.session_id.clone());

    // Persist to config.toml first.
    crate::util::config::save_mcp_server_config(&req.server_name, &req.config)
        .await
        .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;

    // Build the ACP server config for live addition.
    let server_config = req
        .config
        .to_acp_mcp_server(&req.server_name)
        .ok_or_else(|| acp::Error::invalid_params().data("server config is disabled"))?;

    // Reuse the toggle path: enable=true with the built config.
    let handle = agent
        .get_session_handle(&acp_id)
        .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;

    handle
        .toggle_mcp_server(req.server_name, true, Some(server_config))
        .await
        .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;

    to_ext_response(Ok(McpToggleResponse { ok: true }))
}

// ── mcp/delete handler ──────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct McpDeleteRequest {
    session_id: String,
    server_name: String,
}

async fn handle_delete(agent: &MvpAgent, args: &acp::ExtRequest) -> ExtResult {
    let req = parse_params::<McpDeleteRequest>(args)?;
    let acp_id = acp::SessionId::new(req.session_id.clone());

    // Verify the server exists in local config (not managed).
    let existed = crate::util::config::delete_mcp_server_config(&req.server_name)
        .await
        .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;

    if !existed {
        return Err(acp::Error::invalid_params().data(format!(
            "server '{}' not found in config.toml (only locally-configured servers can be deleted)",
            req.server_name
        )));
    }

    // Live teardown: disable the server in the running session.
    let handle = agent
        .get_session_handle(&acp_id)
        .ok_or_else(|| acp::Error::invalid_params().data("session not found"))?;

    handle
        .toggle_mcp_server(req.server_name.clone(), false, None)
        .await
        .map_err(|e| acp::Error::internal_error().data(e.to_string()))?;

    // The toggle path spawns a task that adds the server to
    // `disabled_mcp_servers`. Clear user list only — do not unstick project.
    let _ = crate::util::config::save_user_mcp_server_enabled(&req.server_name, true).await;

    to_ext_response(Ok(McpToggleResponse { ok: true }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The emit-only reverse method (`x.ai/mcp/sdk_call`) shares the `x.ai/mcp/`
    /// prefix, so `mvp_agent`'s dispatcher routes an inbound copy of it to this
    /// module's `handle`. It must NOT collide with any forward route — i.e. it has no
    /// `McpRoute`, so `handle` returns `method_not_found` instead of misrouting a stray
    /// inbound reverse call to `handle_call`.
    #[test]
    fn inbound_sdk_call_has_no_forward_route() {
        assert!(
            wire::MCP_SDK_CALL.starts_with(mcp_methods::PREFIX),
            "reverse method must share the prefix so it reaches handle()"
        );
        assert_eq!(
            route_mcp_method(wire::MCP_SDK_CALL),
            None,
            "inbound x.ai/mcp/sdk_call must not resolve to a forward handler"
        );
        // Sanity: the forward sibling on the same prefix DOES route.
        assert_eq!(route_mcp_method(wire::MCP_CALL), Some(McpRoute::Call));
    }

    fn gateway_tool(
        connector_id: &str,
        connector_name: &str,
        tool_id: &str,
        tool_name: &str,
        call_id: &str,
        description: &str,
    ) -> crate::session::managed_mcp::GatewayTool {
        crate::session::managed_mcp::GatewayTool {
            connector_id: connector_id.into(),
            connector_name: connector_name.into(),
            tool_id: tool_id.into(),
            tool_name: tool_name.into(),
            call_id: call_id.into(),
            description: description.into(),
            json_schema: serde_json::json!({"type": "object"}),
        }
    }

    /// **Pattern-regression test, not an end-to-end `handle_list` test.**
    ///
    /// `handle_list` takes an `&MvpAgent`, which has no lightweight test
    /// constructor; spinning up a fake agent here would be a much larger
    /// refactor than this test warrants. Instead this test mirrors the exact
    /// production structure (resolve session handle synchronously, then
    /// `tokio::join!` a managed-fetch arm with a session-state arm whose
    /// inner future conditionally awaits `retry_auth_required_servers` then
    /// `build_mcp_status`) using stand-in futures, and asserts the two
    /// latency invariants `handle_list` guarantees:
    ///
    /// 1. The two `tokio::join!` arms — gateway catalog fetch on one
    ///    side, and the session-state branch (`retry_auth_required_servers?`
    ///    + `build_mcp_status`) on the other — are polled concurrently, so
    ///    total wall-time ≈ max(t_catalog, t_session) rather than the sum.
    /// 2. `retry_auth_required_servers` is gated on `cache=false`. On cached
    ///    opens it is skipped entirely, removing ~500ms of OAuth retry
    ///    overhead when multiple OAuth servers are configured.
    ///
    /// If a future refactor of `handle_list` changes the structure (e.g.
    /// awaits the arms sequentially, or runs auth retry on cache=true),
    /// this test will *not* fail — it only guards the pattern. The real
    /// behavioural guard is reading the diff against the structure
    /// documented here.
    #[tokio::test(start_paused = true)]
    async fn handle_list_parallel_join_pattern_regression() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use tokio::time::{Duration, Instant};

        async fn run(cache: bool) -> (Duration, bool, usize) {
            let auth_retried = Arc::new(AtomicBool::new(false));
            let max_concurrent = Arc::new(AtomicUsize::new(0));
            let in_flight = Arc::new(AtomicUsize::new(0));

            let bump = {
                let max_concurrent = Arc::clone(&max_concurrent);
                let in_flight = Arc::clone(&in_flight);
                move || {
                    let n = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_concurrent.fetch_max(n, Ordering::SeqCst);
                }
            };
            let drop_ = {
                let in_flight = Arc::clone(&in_flight);
                move || {
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                }
            };

            // Stand-in for `agent.get_managed_mcp_gateway_tool_catalog()` (~1-2s proxy fetch).
            let managed_fut = {
                let bump = bump.clone();
                let drop_ = drop_.clone();
                async move {
                    bump();
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    drop_();
                }
            };

            // Stand-in for the session-state branch: conditional auth retry
            // followed by `build_mcp_status`. Mirrors the closure in
            // `handle_list`.
            let session_fut = {
                let auth_retried = Arc::clone(&auth_retried);
                let bump = bump.clone();
                let drop_ = drop_.clone();
                async move {
                    bump();
                    if !cache {
                        auth_retried.store(true, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    // build_mcp_status is cheap (state-mutex inspect).
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    drop_();
                }
            };

            let start = Instant::now();
            tokio::join!(managed_fut, session_fut);
            (
                start.elapsed(),
                auth_retried.load(Ordering::SeqCst),
                max_concurrent.load(Ordering::SeqCst),
            )
        }

        // cache=true: no auth retry, total ≈ managed fetch alone.
        let (cached_elapsed, cached_auth, cached_overlap) = run(true).await;
        assert!(!cached_auth, "auth retry must be skipped on cache=true");
        assert_eq!(cached_overlap, 2, "futures must run concurrently");
        assert!(
            cached_elapsed < Duration::from_millis(1600),
            "cached handle_list should finish in ~1.5s, got {:?}",
            cached_elapsed
        );

        // cache=false: auth retry runs, but still concurrent with managed
        // fetch — total ≈ max(1500, 500+50) ≈ 1500ms, not 2050ms.
        let (refresh_elapsed, refresh_auth, refresh_overlap) = run(false).await;
        assert!(refresh_auth, "auth retry must run on cache=false");
        assert_eq!(refresh_overlap, 2, "futures must run concurrently");
        assert!(
            refresh_elapsed < Duration::from_millis(1600),
            "refresh handle_list should still finish in ~1.5s (parallel), got {:?}",
            refresh_elapsed
        );
    }

    #[test]
    fn test_mcp_list_response_serialization() {
        let resp = McpListResponse {
            servers: vec![
                McpServerEntry {
                    name: "linear".to_string(),
                    icons: Vec::new(),
                    display_name: None,
                    source: McpServerSource::Local,
                    config: McpServerConfig::Http {
                        url: "https://mcp.linear.app".to_string(),
                        scope: Some("team".to_string()),
                        scope_id: Some("team-uuid-123".to_string()),
                        scope_name: Some("Grok CLI".to_string()),
                    },
                    source_label: None,
                    setup: None,
                    setup_values: None,
                    session: None,
                },
                McpServerEntry {
                    name: "filesystem".to_string(),
                    icons: Vec::new(),
                    display_name: None,
                    source: McpServerSource::Local,
                    source_label: None,
                    setup: None,
                    setup_values: None,
                    config: McpServerConfig::Stdio {
                        command: "/usr/bin/mcp-filesystem".into(),
                        args: vec!["--root".to_string(), "/home".to_string()],
                        env: vec![],
                    },
                    session: Some(McpServerSessionState {
                        enabled: true,
                        status: Some(McpSessionStatus::Ready),
                        auth_required: false,
                        setup_required: false,
                        negotiated: None,
                        tools: vec![McpToolEntry {
                            name: "read_file".to_string(),
                            icons: Vec::new(),
                            display_name: None,
                            description: Some("Read a file".to_string()),
                            meta: None,
                            enabled: true,
                        }],
                    }),
                },
            ],
        };
        let json = serde_json::to_value(&resp).unwrap();
        // [0] local HTTP
        assert_eq!(json["servers"][0]["source"], "local");
        assert_eq!(json["servers"][0]["type"], "http");
        assert_eq!(json["servers"][0]["url"], "https://mcp.linear.app");
        assert_eq!(json["servers"][0]["scope"], "team");
        assert_eq!(json["servers"][0]["scopeId"], "team-uuid-123");
        assert_eq!(json["servers"][0]["scopeName"], "Grok CLI");
        assert!(json["servers"][0].get("session").is_none());
        // Managed gateway connectors are not serialized as local transports.
        let gateway = serde_json::to_value(McpServerEntry {
            name: managed_gateway_entry_name("linear"),
            icons: Vec::new(),
            display_name: Some("linear".to_string()),
            source: McpServerSource::Managed,
            source_label: None,
            setup: None,
            setup_values: None,
            config: McpServerConfig::ManagedGateway,
            session: Some(McpServerSessionState {
                enabled: true,
                status: Some(McpSessionStatus::Ready),
                tools: vec![],
                auth_required: false,
                setup_required: false,
                negotiated: None,
            }),
        })
        .unwrap();
        assert_eq!(gateway["name"], "managed_gateway:linear");
        assert_eq!(gateway["displayName"], "linear");
        assert_eq!(gateway["type"], "managedGateway");
        assert!(gateway.get("command").is_none());
        assert!(gateway.get("url").is_none());
        // [1] local Stdio
        assert_eq!(json["servers"][1]["source"], "local");
        assert_eq!(json["servers"][1]["type"], "stdio");
        assert_eq!(json["servers"][1]["command"], "/usr/bin/mcp-filesystem");
        assert_eq!(
            json["servers"][1]["args"],
            serde_json::json!(["--root", "/home"])
        );
        assert!(json["servers"][1].get("url").is_none());
        assert_eq!(json["servers"][1]["session"]["enabled"], true);
        assert_eq!(json["servers"][1]["session"]["status"], "ready");
        assert_eq!(
            json["servers"][1]["session"]["tools"][0]["name"],
            "read_file"
        );
    }

    #[test]
    fn sdk_catalog_redaction_removes_transport_and_setup_secrets() {
        let mut entry = McpServerEntry {
            name: "secret".to_owned(),
            display_name: None,
            icons: Vec::new(),
            source: McpServerSource::Local,
            source_label: None,
            setup: None,
            setup_values: Some(HashMap::from([(
                "token".to_owned(),
                "setup-secret".to_owned(),
            )])),
            config: McpServerConfig::Stdio {
                command: "/secret/command".into(),
                args: vec!["--token=argument-secret".to_owned()],
                env: vec![McpEnvVar {
                    name: "TOKEN".to_owned(),
                    value: "environment-secret".to_owned(),
                }],
            },
            session: None,
        };
        redact_sdk_catalog_entry(&mut entry);
        let serialized = serde_json::to_string(&entry).unwrap();
        for secret in [
            "/secret/command",
            "argument-secret",
            "environment-secret",
            "setup-secret",
        ] {
            assert!(!serialized.contains(secret), "catalog leaked {secret}");
        }
    }

    #[test]
    fn test_mcp_list_icons_serialization() {
        let entry = McpServerEntry {
            name: "custom".to_string(),
            display_name: Some("Custom".to_string()),
            icons: vec![xai_grok_mcp::servers::McpIcon {
                src: "https://example.com/icon.png".to_string(),
                mime_type: Some("image/png".to_string()),
                sizes: Some(vec!["48x48".to_string()]),
                theme: Some(xai_grok_mcp::servers::McpIconTheme::Dark),
            }],
            source: McpServerSource::Local,
            source_label: None,
            setup: None,
            setup_values: None,
            config: McpServerConfig::Http {
                url: "https://example.com/mcp".to_string(),
                scope: None,
                scope_id: None,
                scope_name: None,
            },
            session: Some(McpServerSessionState {
                enabled: true,
                status: Some(McpSessionStatus::Ready),
                tools: vec![McpToolEntry {
                    name: "ping".to_string(),
                    display_name: None,
                    description: None,
                    meta: None,
                    icons: vec![xai_grok_mcp::servers::McpIcon {
                        src: "data:image/png;base64,aaa".to_string(),
                        mime_type: None,
                        sizes: None,
                        theme: None,
                    }],
                    enabled: true,
                }],
                auth_required: false,
                setup_required: false,
                negotiated: None,
            }),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["icons"][0]["src"], "https://example.com/icon.png");
        assert_eq!(json["icons"][0]["mimeType"], "image/png");
        assert_eq!(json["icons"][0]["sizes"][0], "48x48");
        assert_eq!(json["icons"][0]["theme"], "dark");
        assert_eq!(
            json["session"]["tools"][0]["icons"][0]["src"],
            "data:image/png;base64,aaa"
        );
    }

    #[test]
    fn gateway_catalog_groups_by_connector_name_and_exact_tool_names() {
        let catalog = crate::session::managed_mcp::GatewayToolCatalog {
            tools: vec![
                gateway_tool(
                    "linear",
                    "Linear",
                    "list_issues",
                    "List issues",
                    "linear.list_issues",
                    "List Linear issues",
                ),
                gateway_tool(
                    "linear",
                    "Linear",
                    "create_issue",
                    "Create issue",
                    "linear.create_issue",
                    "Create a Linear issue",
                ),
                gateway_tool(
                    "slack",
                    "Slack",
                    "search",
                    "Search",
                    "slack.search",
                    "Search Slack",
                ),
            ],
            total_tools: 3,
            connectors_needing_reauth: vec!["slack".into()],
        };
        let servers =
            build_mcp_catalog_with_gateway_tools(&[], Some(&catalog), &Default::default());

        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].name, "managed_gateway:linear");
        assert_eq!(servers[0].display_name.as_deref(), Some("Linear"));
        assert_eq!(servers[0].source, McpServerSource::Managed);
        assert!(matches!(servers[0].config, McpServerConfig::ManagedGateway));
        let linear_session = servers[0].session.as_ref().unwrap();
        assert_eq!(linear_session.status, Some(McpSessionStatus::Ready));
        assert!(!linear_session.auth_required);
        let linear_names: Vec<&str> = linear_session
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect();
        assert_eq!(
            linear_names,
            vec!["linear__list_issues", "linear__create_issue"]
        );

        assert_eq!(servers[1].name, "managed_gateway:slack");
        assert_eq!(servers[1].display_name.as_deref(), Some("Slack"));
        let slack_session = servers[1].session.as_ref().unwrap();
        assert!(slack_session.auth_required);
        assert!(slack_session.status.is_none());
        assert_eq!(slack_session.tools[0].name, "slack__search");
        assert_eq!(
            slack_session.tools[0].display_name.as_deref(),
            Some("Search")
        );
    }

    #[test]
    fn gateway_catalog_preserves_local_name_collision() {
        let catalog = crate::session::managed_mcp::GatewayToolCatalog {
            tools: vec![gateway_tool(
                "linear",
                "Linear",
                "list_issues",
                "List issues",
                "linear.list_issues",
                "List Linear issues",
            )],
            total_tools: 1,
            connectors_needing_reauth: vec![],
        };
        let local = acp::McpServer::Stdio(
            acp::McpServerStdio::new("linear", "/usr/bin/local-linear")
                .args(vec![])
                .env(vec![]),
        );

        let servers =
            build_mcp_catalog_with_gateway_tools(&[local], Some(&catalog), &Default::default());

        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].name, "managed_gateway:linear");
        assert_eq!(servers[0].display_name.as_deref(), Some("Linear"));
        assert_eq!(servers[0].source, McpServerSource::Managed);
        assert_eq!(servers[1].name, "linear");
        assert_eq!(servers[1].display_name, None);
        assert_eq!(servers[1].source, McpServerSource::Local);
        assert!(matches!(servers[1].config, McpServerConfig::Stdio { .. }));
    }

    #[test]
    fn gateway_toggle_classification_requires_managed_gateway_entry_id() {
        assert_eq!(
            managed_gateway_connector_id("managed_gateway:linear"),
            Some("linear")
        );
        assert_eq!(managed_gateway_connector_id("linear"), None);
    }

    #[test]
    fn disabled_local_rows_keep_non_gateway_placeholder_config() {
        let entry = disabled_server_placeholder_entry("local_slack");
        assert_eq!(entry.source, McpServerSource::Local);
        assert!(matches!(entry.config, McpServerConfig::Stdio { .. }));
    }

    #[test]
    fn grok_com_local_name_is_not_managed_in_catalog() {
        let local = acp::McpServer::Http(
            acp::McpServerHttp::new("grok_com_slack", "https://mcp.example.test/sse")
                .headers(vec![]),
        );
        let servers = build_mcp_catalog_with_gateway_tools(&[local], None, &Default::default());
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "grok_com_slack");
        assert_eq!(servers[0].source, McpServerSource::Local);
        assert!(matches!(servers[0].config, McpServerConfig::Http { .. }));

        let placeholder = disabled_server_placeholder_entry("grok_com_slack");
        assert_eq!(placeholder.source, McpServerSource::Local);
        assert!(matches!(placeholder.config, McpServerConfig::Stdio { .. }));
    }

    #[test]
    fn gateway_catalog_honors_disabled_connectors_and_tools() {
        let catalog = crate::session::managed_mcp::GatewayToolCatalog {
            tools: vec![
                gateway_tool(
                    "linear",
                    "Linear",
                    "list_issues",
                    "List issues",
                    "linear.list_issues",
                    "List Linear issues",
                ),
                gateway_tool(
                    "linear",
                    "Linear",
                    "create_issue",
                    "Create issue",
                    "linear.create_issue",
                    "Create a Linear issue",
                ),
            ],
            total_tools: 2,
            connectors_needing_reauth: vec![],
        };
        let disabled: HashMap<String, HashSet<String>> = HashMap::from([
            (
                crate::util::config::MANAGED_GATEWAY_DISABLED_CONNECTORS_KEY.to_string(),
                HashSet::from(["linear".to_string()]),
            ),
            (
                "linear".to_string(),
                HashSet::from(["linear__create_issue".to_string()]),
            ),
        ]);
        let servers = build_mcp_catalog_with_gateway_tools(&[], Some(&catalog), &disabled);
        let session = servers[0].session.as_ref().unwrap();
        assert!(!session.enabled);
        assert!(session.status.is_none());
        assert!(session.tools[0].enabled);
        assert!(!session.tools[1].enabled);
    }

    #[test]
    fn test_mcp_call_response_serialization() {
        let resp = McpCallResponse {
            content: vec![serde_json::json!({
                "type": "text",
                "text": "Created issue LIN-123"
            })],
            structured_content: Some(serde_json::json!({"issue": "LIN-123"})),
            is_error: Some(false),
            meta: Some(serde_json::json!({"trace": 1})),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["content"][0]["type"], "text");
        assert_eq!(json["content"][0]["text"], "Created issue LIN-123");
        assert_eq!(json["isError"], false);
        assert_eq!(json["structuredContent"]["issue"], "LIN-123");
        assert_eq!(json["_meta"]["trace"], 1);
    }

    #[test]
    fn test_mcp_list_setup_required_serialization() {
        let entry = McpServerEntry {
            name: "acme".to_string(),
            icons: Vec::new(),
            display_name: None,
            source: McpServerSource::Local,
            source_label: Some("plugin: acme".to_string()),
            setup: Some(crate::util::config::McpSetupConfig {
                fields: vec![crate::util::config::McpSetupField {
                    id: "site".to_string(),
                    label: "Site".to_string(),
                    field_type: crate::util::config::McpSetupFieldType::Select,
                    required: true,
                    default: Some("us1".to_string()),
                    options: vec![crate::util::config::McpSetupOption {
                        label: "US5".to_string(),
                        value: "us5".to_string(),
                    }],
                }],
                variables: HashMap::new(),
            }),
            setup_values: Some(HashMap::from([("site".to_string(), "us5".to_string())])),
            config: McpServerConfig::Http {
                url: String::new(),
                scope: None,
                scope_id: None,
                scope_name: None,
            },
            session: Some(McpServerSessionState {
                enabled: true,
                status: Some(McpSessionStatus::SetupRequired),
                tools: vec![],
                auth_required: false,
                setup_required: true,
                negotiated: None,
            }),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["session"]["status"], "setuprequired");
        assert_eq!(json["session"]["setupRequired"], true);
        assert_eq!(json["setup"]["fields"][0]["id"], "site");
        assert_eq!(json["setupValues"]["site"], "us5");
    }

    #[test]
    fn test_mcp_auth_trigger_response_success_no_error_field() {
        let resp = McpAuthTriggerResponse {
            status: "authenticated",
            setup: None,
            error: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "authenticated");
        assert!(
            json.get("error").is_none(),
            "error field must be omitted on success: {json}"
        );
    }

    #[test]
    fn test_mcp_auth_trigger_response_failure_carries_error() {
        let resp = McpAuthTriggerResponse {
            status: "failed",
            setup: None,
            error: Some("MCP server 'linear' does not use OAuth".to_string()),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "failed");
        assert_eq!(
            json["error"], "MCP server 'linear' does not use OAuth",
            "failure must carry the descriptive error verbatim: {json}"
        );
    }

    #[test]
    fn test_mcp_auth_trigger_response_failure_omits_error_when_none() {
        let resp = McpAuthTriggerResponse {
            status: "failed",
            setup: None,
            error: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "failed");
        assert!(json.get("error").is_none());
    }

    #[test]
    fn test_disabled_session_state_serialization() {
        let entry = McpServerEntry {
            name: "slack".to_string(),
            icons: Vec::new(),
            display_name: None,
            source: McpServerSource::Local,
            source_label: None,
            setup: None,
            setup_values: None,
            config: McpServerConfig::Http {
                url: "https://mcp.slack.com".to_string(),
                scope: Some("user".to_string()),
                scope_id: Some("user-uuid-456".to_string()),
                scope_name: None,
            },
            session: Some(McpServerSessionState {
                enabled: false,
                status: None,
                tools: vec![],
                auth_required: false,
                setup_required: false,
                negotiated: None,
            }),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["type"], "http");
        assert_eq!(json["scope"], "user");
        assert_eq!(json["scopeId"], "user-uuid-456");
        assert_eq!(json["session"]["enabled"], false);
        assert!(json["session"].get("status").is_none());
        assert!(json["session"].get("tools").is_none());
    }
}
