// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use crate::*;

impl Runtime {
    pub fn builder(config: RuntimeConfig) -> RuntimeBuilder {
        RuntimeBuilder {
            config,
            options: RuntimeOptions::default(),
            run_store: None,
            evidence_store: None,
            session_state_store: None,
            compaction_observer: None,
        }
    }
}
pub struct RuntimeBuilder {
    config: RuntimeConfig,
    options: RuntimeOptions,
    run_store: Option<Arc<dyn run::RunStore>>,
    evidence_store: Option<Arc<dyn SessionEvidenceStore>>,
    session_state_store: Option<Arc<dyn SessionStateStore>>,
    compaction_observer: Option<Arc<dyn CompactionObserver>>,
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
    /// Installs the application-owned general capability layer. Built-ins,
    /// general skills and general services live here; a Session layer masks a
    /// general contribution of the same kind and name for that Session only.
    pub fn general_capabilities(mut self, value: CapabilityLayer) -> Self {
        self.options.general_capabilities = value;
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
    /// Installs typed roots and sampling services used to fulfill MCP 2026
    /// MRTR input requests. Generic elicitation services are rejected at
    /// startup; use [`RuntimeBuilder::mcp_elicitation_ui`] so only the product
    /// UI can supply an elicitation answer.
    pub fn mcp_host_services(mut self, value: McpHostServices) -> Self {
        self.options.mcp_host_services = value;
        self
    }
    /// Installs the sole MCP elicitation answer authority. The SDK advertises
    /// form and URL elicitation with schema validation, invokes this delegate
    /// with the bound operation/Task identity, and never accepts an elicitation
    /// answer through generic continuation or Task-update arguments.
    pub fn mcp_elicitation_ui(mut self, value: Arc<dyn McpElicitationUi>) -> Self {
        self.options.mcp_elicitation_ui = Some(value);
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
    /// Installs the Host authority behind `conversation_create`,
    /// `conversation_read` and `conversation_send`. The three tools appear on
    /// every Session of this Runtime under the `conversation` mount exactly
    /// when this is installed, and nowhere otherwise. Requires the Desktop
    /// profile, and rejects a colliding mount of the same name.
    pub fn conversation_delegate(mut self, value: Arc<dyn ConversationDelegate>) -> Self {
        self.options.conversation_delegate = Some(value);
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
    /// Replaces local SDK-origin evidence files with the Host's single CAS authority.
    pub fn session_evidence_store(mut self, value: Arc<dyn SessionEvidenceStore>) -> Self {
        self.evidence_store = Some(value);
        self
    }
    /// Replaces native Session transcript, rewind, and compaction persistence
    /// with the Host's single canonical authority. The store is shared by all
    /// Sessions created or loaded by this Runtime.
    pub fn session_state_store(mut self, value: Arc<dyn SessionStateStore>) -> Self {
        self.session_state_store = Some(value);
        self
    }
    /// Installs the one Host audit observer for native Session compaction.
    /// The SDK remains the checkpoint and recovery authority; the observer
    /// receives only bounded, credential-free digest facts.
    pub fn compaction_observer(mut self, value: Arc<dyn CompactionObserver>) -> Self {
        self.compaction_observer = Some(value);
        self
    }
    pub async fn start(self) -> Result<(Runtime, mpsc::UnboundedReceiver<Event>), Error> {
        let conversation = self.options.conversation_delegate.clone();
        let mcp_elicitation_ui = self.options.mcp_elicitation_ui.clone();
        private::Runtime::start_with_stores(
            self.config,
            self.options,
            self.run_store,
            self.evidence_store,
            self.session_state_store,
            self.compaction_observer,
        )
        .await
        .map(|(inner, events)| {
            (
                Runtime {
                    inner,
                    conversation,
                    mcp_elicitation_ui,
                },
                events,
            )
        })
    }
}
