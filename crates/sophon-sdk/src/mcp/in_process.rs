// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

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
