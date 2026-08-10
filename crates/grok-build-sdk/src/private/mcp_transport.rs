use super::*;

pub(super) struct DirectMcpInvoker {
    pub(super) runtime_instance_id: u64,
    pub(super) handlers: HashMap<String, (String, Arc<dyn crate::InProcessMcpHandler>)>,
    pub(super) bindings: Arc<McpBindingRegistry>,
    pub(super) host_services: xai_grok_mcp::servers::McpHostServices,
}

pub(super) struct DirectMcpOutbound {
    pub(super) session_id: String,
    pub(super) binding_id: u64,
    pub(super) bindings: Arc<McpBindingRegistry>,
    pub(super) outbound: tokio::sync::mpsc::Sender<serde_json::Value>,
}

#[async_trait::async_trait]
impl crate::InProcessMcpOutbound for DirectMcpOutbound {
    async fn send(&self, message: serde_json::Value) -> Result<(), crate::HostError> {
        let permit = self
            .outbound
            .reserve()
            .await
            .map_err(|_| crate::HostError {
                code: -32000,
                message: "in-process MCP connection is closed".into(),
                data: serde_json::Value::Null,
            })?;
        self.bindings
            .active_instance(&self.session_id, self.binding_id)
            .map_err(|message| crate::HostError {
                code: -32000,
                message,
                data: serde_json::Value::Null,
            })?;
        permit.send(message);
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct McpBindingRegistry {
    state: std::sync::Mutex<McpBindingState>,
}

#[derive(Default)]
struct McpBindingState {
    next_binding_id: u64,
    session_instances: HashMap<String, u64>,
    active: HashMap<String, ActiveMcpBinding>,
}

#[derive(Clone, Copy)]
struct ActiveMcpBinding {
    binding_id: u64,
    session_instance_id: u64,
}

impl McpBindingRegistry {
    pub(super) fn bind(&self, session_id: &str) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.next_binding_id = state.next_binding_id.saturating_add(1);
        let binding_id = state.next_binding_id;
        let session_instance_id = {
            let instance = state
                .session_instances
                .entry(session_id.to_owned())
                .or_insert(0);
            *instance = instance.saturating_add(1);
            *instance
        };
        state.active.insert(
            session_id.to_owned(),
            ActiveMcpBinding {
                binding_id,
                session_instance_id,
            },
        );
        binding_id
    }

    pub(super) fn active_instance(&self, session_id: &str, binding_id: u64) -> Result<u64, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "embedded MCP binding registry failed".to_owned())?;
        state
            .active
            .get(session_id)
            .filter(|active| active.binding_id == binding_id)
            .map(|active| active.session_instance_id)
            .ok_or_else(|| "embedded MCP actor binding is stale or not resident".to_owned())
    }

    pub(super) fn revoke_binding(&self, session_id: &str, binding_id: u64) {
        if let Ok(mut state) = self.state.lock()
            && state
                .active
                .get(session_id)
                .is_some_and(|active| active.binding_id == binding_id)
        {
            state.active.remove(session_id);
        }
    }

    pub(super) fn revoke_session(&self, session_id: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.active.remove(session_id);
        }
    }
}

pub(super) struct ActiveMcpBindingGuard {
    bindings: Arc<McpBindingRegistry>,
    id: String,
    keep: bool,
}

impl ActiveMcpBindingGuard {
    pub(super) fn new(bindings: Arc<McpBindingRegistry>, id: String) -> Self {
        Self {
            bindings,
            id,
            keep: false,
        }
    }

    pub(super) fn commit(mut self) {
        self.keep = true;
    }
}

impl Drop for ActiveMcpBindingGuard {
    fn drop(&mut self) {
        if !self.keep {
            self.bindings.revoke_session(&self.id);
        }
    }
}

#[async_trait::async_trait]
impl xai_grok_mcp::acp_transport::EmbeddedMcpInvoker for DirectMcpInvoker {
    fn host_services(&self) -> Option<xai_grok_mcp::servers::McpHostServices> {
        (!self.host_services.is_empty()).then(|| self.host_services.clone())
    }

    async fn connect(
        &self,
        session_id: &str,
        binding_id: u64,
        server_id: &str,
        outbound: tokio::sync::mpsc::Sender<serde_json::Value>,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        let handler = self
            .handlers
            .get(server_id)
            .ok_or_else(|| "embedded MCP server is not registered".to_owned())?;
        let session_instance_id = self.bindings.active_instance(session_id, binding_id)?;
        let context = crate::InProcessMcpContext {
            runtime_instance_id: self.runtime_instance_id,
            session_id: SessionId(session_id.to_owned()),
            session_instance_id,
            server_name: handler.0.clone(),
            registration_id: server_id.to_owned(),
        };
        let peer = crate::InProcessMcpPeer::new(Arc::new(DirectMcpOutbound {
            session_id: session_id.to_owned(),
            binding_id,
            bindings: self.bindings.clone(),
            outbound,
        }));
        tokio::time::timeout(timeout, handler.1.connected(&context, peer))
            .await
            .map_err(|_| "embedded MCP connect handler timed out".to_owned())?
            .map_err(|error| error.message)?;
        self.bindings
            .active_instance(session_id, binding_id)
            .map(|_| ())
    }

    fn bind_session(&self, session_id: &str) -> u64 {
        self.bindings.bind(session_id)
    }

    fn unbind_session(&self, session_id: &str, binding_id: u64) {
        self.bindings.revoke_binding(session_id, binding_id);
    }

    async fn invoke(
        &self,
        session_id: &str,
        binding_id: u64,
        server_id: &str,
        message: serde_json::Value,
        timeout: std::time::Duration,
    ) -> Result<serde_json::Value, String> {
        let handler = self
            .handlers
            .get(server_id)
            .ok_or_else(|| "embedded MCP server is not registered".to_owned())?;
        let session_instance_id = self.bindings.active_instance(session_id, binding_id)?;
        let context = crate::InProcessMcpContext {
            runtime_instance_id: self.runtime_instance_id,
            session_id: SessionId(session_id.to_owned()),
            session_instance_id,
            server_name: handler.0.clone(),
            registration_id: server_id.to_owned(),
        };
        let response = tokio::time::timeout(
            timeout,
            handler.1.handle_with_context(&context, message.clone()),
        )
        .await
        .map_err(|_| "embedded MCP handler timed out".to_owned())?
        .map_err(|_| "embedded MCP handler failed".to_owned())?;
        self.bindings.active_instance(session_id, binding_id)?;
        validate_mcp_response(&message, &response)
            .map_err(|_| "embedded MCP handler returned an invalid JSON-RPC response".to_owned())?;
        Ok(response)
    }
}
