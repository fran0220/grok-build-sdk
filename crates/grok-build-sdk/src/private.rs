use crate::{
    AvailableModel, ConversationRewindReceipt, ConversationRewindStatus, Error, Event, EventUpdate,
    ExtensionNotification, ExtensionRequest, ExtensionResponse, HarnessDigest, HarnessError,
    LedgerTurnState, ModelCatalog, Prompt, PromptBlock, PromptReceipt, RewindPoint,
    RuntimeCapabilities, RuntimeConfig, RuntimeOptions, SessionConfig, SessionEvidenceCommit,
    SessionEvidenceDocument, SessionEvidenceKey, SessionEvidenceKind, SessionEvidenceStore,
    SessionEvidenceVersion, SessionId, SessionLedger, SessionLedgerEntry, TurnBindingKey,
    TurnBindingReceipt, TurnBindingRecord, TurnBindingStatus, TurnOutcome,
};
use agent_client_protocol as acp;
use agent_client_protocol::Agent as _;
use indexmap::IndexMap;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    num::NonZeroU64,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};
use xai_acp_lib::{AcpAgentGatewaySender, AcpGatewayReceiver};
use xai_grok_shell::{
    agent::{
        config::{Config, ModelEntry, ModelEntryConfig, OriginMediaConfig},
        models::ModelsManager,
        mvp_agent::MvpAgent,
    },
    auth::AuthManager,
};

const CANCEL_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
fn to_acp_mcp_server(server: &crate::McpServerConfig) -> acp::McpServer {
    match server {
        crate::McpServerConfig::Stdio {
            name,
            command,
            args,
            env,
        } => acp::McpServer::Stdio(
            acp::McpServerStdio::new(name, command)
                .args(args.clone())
                .env(
                    env.iter()
                        .map(|(k, v)| acp::EnvVariable::new(k, v))
                        .collect(),
                ),
        ),
        crate::McpServerConfig::Http { name, url, headers } => acp::McpServer::Http(
            acp::McpServerHttp::new(name, url).headers(
                headers
                    .iter()
                    .map(|(k, v)| acp::HttpHeader::new(k, v))
                    .collect(),
            ),
        ),
        crate::McpServerConfig::Sse { name, url, headers } => acp::McpServer::Sse(
            acp::McpServerSse::new(name, url).headers(
                headers
                    .iter()
                    .map(|(k, v)| acp::HttpHeader::new(k, v))
                    .collect(),
            ),
        ),
    }
}
type Reply<T> = oneshot::Sender<Result<T, Error>>;
type SessionMeta = serde_json::Map<String, serde_json::Value>;
type PromptUsage = xai_grok_shell::extensions::notification::PromptUsage;

#[derive(Clone, Debug, PartialEq)]
enum CapturedTurnUsage {
    Exact(Option<PromptUsage>),
    Conflict,
}

type TurnUsageMap = Rc<RefCell<HashMap<(String, String), CapturedTurnUsage>>>;
enum Command {
    Create(SessionConfig, Option<HarnessDigest>, Reply<SessionId>),
    Ensure(SessionId, SessionConfig, Reply<SessionId>),
    Load(SessionId, SessionConfig, Option<HarnessDigest>, Reply<()>),
    Resume(SessionId, SessionConfig, Option<HarnessDigest>, Reply<()>),
    Prompt(SessionId, String, String, Reply<PromptReceipt>),
    PromptContent(SessionId, String, Prompt, Reply<PromptReceipt>),
    PromptBound(
        SessionId,
        String,
        Prompt,
        HarnessDigest,
        Reply<TurnBindingReceipt>,
    ),
    ListModels(Reply<ModelCatalog>),
    Extension(ExtensionRequest, Reply<ExtensionResponse>),
    Fork(SessionId, ExtensionRequest, Reply<ExtensionResponse>),
    ExtensionNotification(ExtensionNotification, Reply<()>),
    SetMode(SessionId, String, Reply<()>),
    ListSessions(Reply<serde_json::Value>),
    EventsAfter(SessionId, u64, Reply<Vec<Event>>),
    Cancel(SessionId, Reply<()>),
    SessionLedger(SessionId, Reply<SessionLedger>),
    TurnBindingStatus(SessionId, TurnBindingKey, Reply<crate::TurnBindingStatus>),
    MarkTurnDiscarded(SessionId, String, String, u64, Reply<()>),
    SetRoute(SessionId, String, Option<String>, Reply<()>),
    RewindPoints(SessionId, Reply<Vec<RewindPoint>>),
    Rewind(SessionId, String, u64, Reply<ConversationRewindReceipt>),
    RewindUnsettled(
        SessionId,
        String,
        String,
        String,
        u64,
        Reply<ConversationRewindReceipt>,
    ),
    RewindStatus(SessionId, String, Reply<ConversationRewindStatus>),
    ReplaceMcp(SessionId, Vec<crate::McpServerConfig>, Reply<()>),
    McpModern(
        SessionId,
        String,
        xai_grok_shell::extensions::mcp::McpModernOperation,
        Reply<serde_json::Value>,
    ),
    McpSubscribe(
        SessionId,
        String,
        xai_grok_shell::extensions::mcp::McpModernSubscriptionFilter,
        std::num::NonZeroUsize,
        Reply<xai_grok_shell::extensions::mcp::McpModernSubscription>,
    ),
    Close(SessionId, Reply<()>),
    Delete(SessionId, Reply<()>),
    Unload(SessionId, Reply<()>),
    Shutdown(Reply<()>),
}

#[derive(Clone)]
pub struct Runtime {
    shared: Arc<RuntimeShared>,
}
struct RuntimeShared {
    commands: mpsc::UnboundedSender<Command>,
    join: tokio::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    runs: tokio::sync::Mutex<
        xai_agent_lifecycle::run::RunController<Arc<dyn xai_agent_lifecycle::run::RunStore>>,
    >,
    shutdown: AtomicBool,
    capabilities: RuntimeCapabilities,
}
impl Runtime {
    pub async fn start(
        input: RuntimeConfig,
        options: RuntimeOptions,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        Self::start_with_run_store(input, options, None).await
    }

    pub async fn start_with_run_store(
        input: RuntimeConfig,
        options: RuntimeOptions,
        run_store: Option<Arc<dyn xai_agent_lifecycle::run::RunStore>>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        Self::start_with_stores(input, options, run_store, None, None).await
    }

    pub async fn start_with_stores(
        input: RuntimeConfig,
        options: RuntimeOptions,
        run_store: Option<Arc<dyn xai_agent_lifecycle::run::RunStore>>,
        evidence_store: Option<Arc<dyn SessionEvidenceStore>>,
        session_state_store: Option<Arc<dyn crate::SessionStateStore>>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        validate(&input, &options)?;
        if options.event_journal_capacity == 0 {
            return Err(Error::InvalidConfig(
                "event journal capacity must be positive".into(),
            ));
        }
        let advertises_host_io = options.host_capabilities.fs_read
            || options.host_capabilities.fs_write
            || options.host_capabilities.terminal
            || !options.host_capabilities.extension_methods.is_empty();
        if advertises_host_io && options.host.is_none() {
            return Err(Error::InvalidConfig(
                "host capabilities require a HostDelegate".into(),
            ));
        }
        if options.client_identifier.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "client identifier must not be empty".into(),
            ));
        }
        let run_store: Arc<dyn xai_agent_lifecycle::run::RunStore> = match run_store {
            Some(store) => store,
            None => Arc::new(
                xai_agent_lifecycle::run::LocalRunStore::new(
                    input.session_storage.join("durable-runs"),
                )
                .map_err(run_error)?,
            ),
        };
        let runs = xai_agent_lifecycle::run::RunController::open(run_store).map_err(run_error)?;
        let evidence_store = match evidence_store {
            Some(store) => store,
            None => {
                Arc::new(crate::LocalSessionEvidenceStore::new(&input.session_storage).map_err(op)?)
            }
        };
        let (events, event_rx) = mpsc::unbounded_channel();
        let (commands, command_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let join = std::thread::Builder::new()
            .name("grok-build-sdk".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                match rt {
                    Ok(rt) => {
                        let local = tokio::task::LocalSet::new();
                        local.block_on(&rt, async move {
                            match Core::start(
                                input,
                                options,
                                events,
                                evidence_store,
                                session_state_store,
                            )
                            .await
                            {
                                Ok((core, capabilities)) => {
                                    let _ = ready_tx.send(Ok(capabilities));
                                    Rc::new(core).run(command_rx).await;
                                }
                                Err(e) => {
                                    let _ = ready_tx.send(Err(e));
                                }
                            }
                        });
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(op(e)));
                    }
                }
            })
            .map_err(op)?;
        let capabilities = ready_rx.await.map_err(|_| Error::Shutdown)??;
        Ok((
            Self {
                shared: Arc::new(RuntimeShared {
                    commands,
                    join: tokio::sync::Mutex::new(Some(join)),
                    runs: tokio::sync::Mutex::new(runs),
                    shutdown: AtomicBool::new(false),
                    capabilities,
                }),
            },
            event_rx,
        ))
    }
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.shared.capabilities.clone()
    }
    fn ensure_running(&self) -> Result<(), Error> {
        if self.shared.shutdown.load(Ordering::Acquire) {
            Err(Error::Shutdown)
        } else {
            Ok(())
        }
    }
    pub async fn create_run(
        &self,
        request: xai_agent_lifecycle::run::CreateRunRequest,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .create_run(request, now_ms())
            .map_err(run_error)
    }
    pub async fn get_run(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
    ) -> Result<Option<xai_agent_lifecycle::run::RunEnvelope>, Error> {
        self.ensure_running()?;
        Ok(self.shared.runs.lock().await.get_run(run_id))
    }
    pub async fn reload_run_if_required(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
    ) -> Result<Option<xai_agent_lifecycle::run::RunEnvelope>, Error> {
        self.ensure_running()?;
        let mut runs = self.shared.runs.lock().await;
        if runs.reload_is_required(run_id) {
            runs.reload_run(run_id).map_err(run_error)
        } else {
            Ok(runs.get_run(run_id))
        }
    }
    pub async fn list_runs(&self) -> Result<Vec<xai_agent_lifecycle::run::RunEnvelope>, Error> {
        self.ensure_running()?;
        Ok(self.shared.runs.lock().await.list_runs())
    }
    pub async fn list_recoverable_runs(
        &self,
    ) -> Result<Vec<xai_agent_lifecycle::run::RunEnvelope>, Error> {
        self.ensure_running()?;
        Ok(self.shared.runs.lock().await.list_recoverable_runs())
    }
    pub async fn inspect_run_residency(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
    ) -> Result<xai_agent_lifecycle::run::ResidencyInspection, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .inspect_residency(run_id, now_ms())
            .map_err(run_error)
    }
    pub async fn request_run_wake(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::WakeRequest>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .request_wake(request, now_ms())
            .map_err(run_error)
    }
    pub async fn claim_run_activation(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ClaimActivation,
        >,
    ) -> Result<
        xai_agent_lifecycle::run::CommandOutput<xai_agent_lifecycle::run::ActivationLease>,
        Error,
    > {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .claim_activation(request, now_ms())
            .map_err(run_error)
    }
    pub async fn renew_run_activation(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<(
            xai_agent_lifecycle::run::ActivationFence,
            u64,
        )>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .renew_activation(request, now_ms())
            .map_err(run_error)
    }
    pub async fn release_run_activation(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ActivationFence,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .release_activation(request, now_ms())
            .map_err(run_error)
    }
    pub async fn control_run(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::RunAction>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .control_run(request, now_ms())
            .map_err(run_error)
    }
    pub async fn wake_run(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::RunAction>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .wake_run(request, now_ms())
            .map_err(run_error)
    }
    pub async fn attach_run(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
        cursor: xai_agent_lifecycle::run::RunEventCursor,
    ) -> Result<xai_agent_lifecycle::run::RunAttach, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .attach_run(run_id, cursor)
            .map_err(run_error)
    }
    pub async fn begin_run_recovery(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<()>,
    ) -> Result<xai_agent_lifecycle::run::RecoveryPlan, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .begin_recovery(request, now_ms())
            .map_err(run_error)
    }
    pub async fn run_recovery_plan(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
    ) -> Result<xai_agent_lifecycle::run::RecoveryPlan, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .recovery_plan(run_id)
            .map_err(run_error)
    }
    pub async fn finish_run_recovery(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::RecoveryResolution,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .finish_recovery(request, now_ms())
            .map_err(run_error)
    }
    pub async fn begin_iteration(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::BeginIteration,
        >,
    ) -> Result<
        xai_agent_lifecycle::run::CommandOutput<xai_agent_lifecycle::run::IterationHandle>,
        Error,
    > {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .begin_iteration(request, now_ms())
            .map_err(run_error)
    }
    pub async fn propose_harness(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ProposeHarness,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .propose_harness(request, now_ms())
            .map_err(run_error)
    }
    pub async fn validate_harness(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ValidateHarness,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .validate_harness(request, now_ms())
            .map_err(run_error)
    }
    pub async fn activate_harness(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ActivateHarness,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .activate_harness(request, now_ms())
            .map_err(run_error)
    }
    pub async fn rollback_harness(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::RollbackHarness,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .rollback_harness(request, now_ms())
            .map_err(run_error)
    }
    pub async fn finish_iteration(
        &self,
        callback: xai_agent_lifecycle::run::FinishIteration,
    ) -> Result<xai_agent_lifecycle::run::CallbackResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .finish_iteration(callback, now_ms())
            .map_err(run_error)
    }
    pub async fn prepare_operation(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::PrepareOperation,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .prepare_operation(request, now_ms())
            .map_err(run_error)
    }
    pub async fn claim_effect(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::ClaimEffect>,
    ) -> Result<
        xai_agent_lifecycle::run::CommandOutput<xai_agent_lifecycle::run::CommittedEffect>,
        Error,
    > {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .claim_effect(request, now_ms())
            .map_err(run_error)
    }
    pub async fn acknowledge_effect(
        &self,
        callback: xai_agent_lifecycle::run::EffectCallback,
    ) -> Result<xai_agent_lifecycle::run::CallbackResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .acknowledge_effect(callback, now_ms())
            .map_err(run_error)
    }
    pub async fn reconcile_effect(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ReconcileEffect,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .reconcile_effect(request, now_ms())
            .map_err(run_error)
    }
    pub async fn admit_child(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::AdmitChild>,
    ) -> Result<xai_agent_lifecycle::run::CommandOutput<xai_agent_lifecycle::run::ChildRun>, Error>
    {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .admit_child(request, now_ms())
            .map_err(run_error)
    }
    pub async fn child_callback(
        &self,
        callback: xai_agent_lifecycle::run::ChildCallback,
    ) -> Result<xai_agent_lifecycle::run::CallbackResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .child_callback(callback, now_ms())
            .map_err(run_error)
    }
    pub async fn accept_run_message(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::AcceptMessage>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .accept_message(request, now_ms())
            .map_err(run_error)
    }
    pub async fn transition_run_message(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::TransitionMessage,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .transition_message(request, now_ms())
            .map_err(run_error)
    }
    pub async fn list_models(&self) -> Result<ModelCatalog, Error> {
        self.call(Command::ListModels).await
    }
    async fn call<T>(&self, build: impl FnOnce(Reply<T>) -> Command) -> Result<T, Error> {
        let (tx, rx) = oneshot::channel();
        if self.shared.shutdown.load(Ordering::Acquire) {
            return Err(Error::Shutdown);
        }
        self.shared
            .commands
            .send(build(tx))
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)?
    }
    pub async fn create_session(&self, c: SessionConfig) -> Result<SessionId, Error> {
        self.call(|r| Command::Create(c, None, r)).await
    }
    pub async fn create_session_with_id(
        &self,
        id: SessionId,
        c: SessionConfig,
    ) -> Result<SessionId, Error> {
        self.call(|r| Command::Ensure(id, c, r)).await
    }
    pub async fn create_session_with_harness(
        &self,
        c: SessionConfig,
        digest: HarnessDigest,
    ) -> Result<SessionId, Error> {
        self.call(|r| Command::Create(c, Some(digest), r)).await
    }
    pub async fn load_session(&self, id: SessionId, c: SessionConfig) -> Result<(), Error> {
        self.call(|r| Command::Load(id, c, None, r)).await
    }
    pub async fn load_session_with_harness(
        &self,
        id: SessionId,
        c: SessionConfig,
        digest: HarnessDigest,
    ) -> Result<(), Error> {
        self.call(|r| Command::Load(id, c, Some(digest), r)).await
    }
    pub async fn resume_session(&self, id: SessionId, c: SessionConfig) -> Result<(), Error> {
        self.call(|r| Command::Resume(id, c, None, r)).await
    }
    pub async fn resume_session_with_harness(
        &self,
        id: SessionId,
        c: SessionConfig,
        digest: HarnessDigest,
    ) -> Result<(), Error> {
        self.call(|r| Command::Resume(id, c, Some(digest), r)).await
    }
    pub async fn prompt(
        &self,
        id: &SessionId,
        t: String,
        x: String,
    ) -> Result<PromptReceipt, Error> {
        self.call(|r| Command::Prompt(id.clone(), t, x, r)).await
    }
    pub async fn prompt_content(
        &self,
        id: &SessionId,
        t: String,
        p: Prompt,
    ) -> Result<PromptReceipt, Error> {
        self.call(|r| Command::PromptContent(id.clone(), t, p, r))
            .await
    }
    pub async fn prompt_content_with_harness(
        &self,
        id: &SessionId,
        t: String,
        p: Prompt,
        digest: HarnessDigest,
    ) -> Result<TurnBindingReceipt, Error> {
        self.call(|reply| Command::PromptBound(id.clone(), t, p, digest, reply))
            .await
    }
    pub async fn extension_request(&self, x: ExtensionRequest) -> Result<ExtensionResponse, Error> {
        self.call(|r| Command::Extension(x, r)).await
    }
    pub async fn fork_session(
        &self,
        target: SessionId,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        self.call(|reply| Command::Fork(target, request, reply))
            .await
    }
    pub async fn extension_notification(&self, x: ExtensionNotification) -> Result<(), Error> {
        self.call(|r| Command::ExtensionNotification(x, r)).await
    }
    pub async fn set_mode(&self, id: &SessionId, mode: String) -> Result<(), Error> {
        self.call(|r| Command::SetMode(id.clone(), mode, r)).await
    }
    pub async fn list_sessions(&self) -> Result<serde_json::Value, Error> {
        self.call(Command::ListSessions).await
    }
    pub async fn close_session(&self, id: SessionId) -> Result<(), Error> {
        self.call(|reply| Command::Close(id, reply)).await
    }
    pub async fn delete_session(&self, id: SessionId) -> Result<(), Error> {
        self.call(|reply| Command::Delete(id, reply)).await
    }
    pub async fn events_after(&self, id: &SessionId, sequence: u64) -> Result<Vec<Event>, Error> {
        self.call(|r| Command::EventsAfter(id.clone(), sequence, r))
            .await
    }
    pub async fn cancel(&self, id: &SessionId) -> Result<(), Error> {
        self.call(|r| Command::Cancel(id.clone(), r)).await
    }
    pub async fn session_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        self.call(|reply| Command::SessionLedger(id.clone(), reply))
            .await
    }
    pub async fn turn_binding_status(
        &self,
        id: &SessionId,
        key: TurnBindingKey,
    ) -> Result<TurnBindingStatus, Error> {
        self.call(|reply| Command::TurnBindingStatus(id.clone(), key, reply))
            .await
    }
    pub async fn mark_turn_discarded(
        &self,
        id: &SessionId,
        turn_id: String,
        prompt_digest: String,
        runtime_prompt_index: u64,
    ) -> Result<(), Error> {
        self.call(|reply| {
            Command::MarkTurnDiscarded(
                id.clone(),
                turn_id,
                prompt_digest,
                runtime_prompt_index,
                reply,
            )
        })
        .await
    }
    pub async fn set_route(
        &self,
        id: &SessionId,
        model: String,
        reasoning: Option<String>,
    ) -> Result<(), Error> {
        self.call(|r| Command::SetRoute(id.clone(), model, reasoning, r))
            .await
    }
    pub async fn rewind_points(&self, id: &SessionId) -> Result<Vec<RewindPoint>, Error> {
        self.call(|r| Command::RewindPoints(id.clone(), r)).await
    }
    pub async fn rewind_conversation(
        &self,
        id: &SessionId,
        operation_id: String,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.call(|r| Command::Rewind(id.clone(), operation_id, target_prompt_index, r))
            .await
    }
    pub async fn rewind_unsettled_turn(
        &self,
        id: &SessionId,
        operation_id: String,
        turn_id: String,
        prompt_digest: String,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.call(|reply| {
            Command::RewindUnsettled(
                id.clone(),
                operation_id,
                turn_id,
                prompt_digest,
                target_prompt_index,
                reply,
            )
        })
        .await
    }
    pub async fn rewind_status(
        &self,
        id: &SessionId,
        operation_id: &str,
    ) -> Result<ConversationRewindStatus, Error> {
        self.call(|r| Command::RewindStatus(id.clone(), operation_id.to_owned(), r))
            .await
    }
    pub async fn unload_session(&self, id: SessionId) -> Result<(), Error> {
        self.call(|r| Command::Unload(id, r)).await
    }
    pub async fn replace_mcp_servers(
        &self,
        id: &SessionId,
        servers: Vec<crate::McpServerConfig>,
    ) -> Result<(), Error> {
        self.call(|r| Command::ReplaceMcp(id.clone(), servers, r))
            .await
    }
    pub async fn mcp_modern(
        &self,
        id: &SessionId,
        server: String,
        operation: xai_grok_shell::extensions::mcp::McpModernOperation,
    ) -> Result<serde_json::Value, Error> {
        self.call(|reply| Command::McpModern(id.clone(), server, operation, reply))
            .await
    }
    pub async fn mcp_subscribe(
        &self,
        id: &SessionId,
        server: String,
        filter: xai_grok_shell::extensions::mcp::McpModernSubscriptionFilter,
        capacity: std::num::NonZeroUsize,
    ) -> Result<xai_grok_shell::extensions::mcp::McpModernSubscription, Error> {
        self.call(|reply| Command::McpSubscribe(id.clone(), server, filter, capacity, reply))
            .await
    }
    pub async fn shutdown(&self) -> Result<(), Error> {
        let shutdown_result = if !self.shared.shutdown.swap(true, Ordering::AcqRel) {
            let (tx, rx) = oneshot::channel();
            self.shared
                .commands
                .send(Command::Shutdown(tx))
                .map_err(|_| Error::Shutdown)?;
            rx.await.map_err(|_| Error::Shutdown)?
        } else {
            Ok(())
        };
        let join_result = if let Some(join) = self.shared.join.lock().await.take() {
            tokio::task::spawn_blocking(move || join.join())
                .await
                .map_err(op)?
                .map_err(|_| Error::Operation("runtime worker panicked".into()))
        } else {
            Ok(())
        };
        shutdown_result.and(join_result)
    }
}

struct Client {
    events: mpsc::UnboundedSender<Event>,
    sequences: Rc<RefCell<HashMap<String, u64>>>,
    retained: Rc<RefCell<HashMap<String, VecDeque<Event>>>>,
    capacity: usize,
    host: Option<Arc<dyn crate::HostDelegate>>,
    tool_permission_handler: Option<Arc<dyn crate::ToolPermissionHandler>>,
    host_extension_methods: HashSet<String>,
    agent_hooks: HashMap<String, Arc<dyn crate::AgentHookHandler>>,
    turns: Rc<RefCell<HashMap<String, String>>>,
    turn_usages: TurnUsageMap,
    replay: Rc<RefCell<HashMap<String, ReplayMode>>>,
}

struct DirectMcpInvoker {
    runtime_instance_id: u64,
    handlers: HashMap<String, (String, Arc<dyn crate::InProcessMcpHandler>)>,
    bindings: Arc<McpBindingRegistry>,
    host_services: xai_grok_mcp::servers::McpHostServices,
}

struct DirectMcpOutbound {
    session_id: String,
    binding_id: u64,
    bindings: Arc<McpBindingRegistry>,
    outbound: tokio::sync::mpsc::Sender<serde_json::Value>,
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
struct McpBindingRegistry {
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
    fn bind(&self, session_id: &str) -> u64 {
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

    fn active_instance(&self, session_id: &str, binding_id: u64) -> Result<u64, String> {
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

    fn revoke_binding(&self, session_id: &str, binding_id: u64) {
        if let Ok(mut state) = self.state.lock()
            && state
                .active
                .get(session_id)
                .is_some_and(|active| active.binding_id == binding_id)
        {
            state.active.remove(session_id);
        }
    }

    fn revoke_session(&self, session_id: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.active.remove(session_id);
        }
    }
}

struct ActiveMcpBindingGuard {
    bindings: Arc<McpBindingRegistry>,
    id: String,
    keep: bool,
}

impl ActiveMcpBindingGuard {
    fn new(bindings: Arc<McpBindingRegistry>, id: String) -> Self {
        Self {
            bindings,
            id,
            keep: false,
        }
    }

    fn commit(mut self) {
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

#[derive(Clone, Copy)]
enum ReplayMode {
    Capture,
    Suppress,
}

impl Client {
    fn capture_turn_usage(&self, session_id: &str, update: &serde_json::Value) -> acp::Result<()> {
        if update
            .get("sessionUpdate")
            .and_then(serde_json::Value::as_str)
            != Some("turn_completed")
        {
            return Ok(());
        }
        let Some(root_session_id) =
            xai_grok_shell::origin_runtime::resolve_root_session(session_id, None)
        else {
            return Ok(());
        };
        if root_session_id.as_str() != session_id {
            return Ok(());
        }
        let Some(turn_id) = self.turns.borrow().get(&root_session_id).cloned() else {
            return Ok(());
        };
        if update.get("prompt_id").and_then(serde_json::Value::as_str) != Some(turn_id.as_str()) {
            return Ok(());
        }
        let usage = update
            .get("usage")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| acp::Error::invalid_params())?;
        let key = (root_session_id, turn_id);
        let mut captured = self.turn_usages.borrow_mut();
        match captured.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(CapturedTurnUsage::Exact(usage));
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get() != &CapturedTurnUsage::Exact(usage) {
                    entry.insert(CapturedTurnUsage::Conflict);
                }
            }
        }
        Ok(())
    }

    fn typed_permission_request(
        args: &acp::RequestPermissionRequest,
    ) -> acp::Result<crate::ToolPermissionRequest> {
        let raw = serde_json::to_value(args).map_err(|_| acp::Error::internal_error())?;
        let raw_tool =
            serde_json::to_value(&args.tool_call).map_err(|_| acp::Error::internal_error())?;
        let tool_kind = args.tool_call.fields.kind.map(|kind| match kind {
            acp::ToolKind::Read => crate::ToolKind::Read,
            acp::ToolKind::Edit => crate::ToolKind::Edit,
            acp::ToolKind::Delete => crate::ToolKind::Delete,
            acp::ToolKind::Move => crate::ToolKind::Move,
            acp::ToolKind::Search => crate::ToolKind::Search,
            acp::ToolKind::Execute => crate::ToolKind::Execute,
            acp::ToolKind::Think => crate::ToolKind::Think,
            acp::ToolKind::Fetch => crate::ToolKind::Fetch,
            acp::ToolKind::SwitchMode => crate::ToolKind::SwitchMode,
            _ => crate::ToolKind::Other,
        });
        let status = args.tool_call.fields.status.map(|status| match status {
            acp::ToolCallStatus::Pending => crate::ToolCallStatus::Pending,
            acp::ToolCallStatus::InProgress => crate::ToolCallStatus::InProgress,
            acp::ToolCallStatus::Completed => crate::ToolCallStatus::Completed,
            acp::ToolCallStatus::Failed => crate::ToolCallStatus::Failed,
            _ => crate::ToolCallStatus::Other,
        });
        let options = args
            .options
            .iter()
            .map(|option| {
                let raw = serde_json::to_value(option).map_err(|_| acp::Error::internal_error())?;
                let raw_kind = raw
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("other")
                    .to_owned();
                let kind = match option.kind {
                    acp::PermissionOptionKind::AllowOnce => {
                        crate::ToolPermissionOptionKind::AllowOnce
                    }
                    acp::PermissionOptionKind::AllowAlways => {
                        crate::ToolPermissionOptionKind::AllowAlways
                    }
                    acp::PermissionOptionKind::RejectOnce => {
                        crate::ToolPermissionOptionKind::RejectOnce
                    }
                    acp::PermissionOptionKind::RejectAlways => {
                        crate::ToolPermissionOptionKind::RejectAlways
                    }
                    _ => crate::ToolPermissionOptionKind::Other,
                };
                Ok(crate::ToolPermissionOption {
                    id: option.option_id.0.to_string(),
                    name: option.name.clone(),
                    kind,
                    raw_kind,
                    meta: option.meta.clone().map(serde_json::Value::Object),
                    raw,
                })
            })
            .collect::<acp::Result<Vec<_>>>()?;
        Ok(crate::ToolPermissionRequest {
            session_id: args.session_id.0.to_string(),
            tool_call: crate::ToolCallSummary {
                id: args.tool_call.tool_call_id.0.to_string(),
                title: args.tool_call.fields.title.clone(),
                kind: tool_kind,
                status,
                raw_input: args.tool_call.fields.raw_input.clone(),
                raw_output: args.tool_call.fields.raw_output.clone(),
                raw: raw_tool,
            },
            options,
            raw,
        })
    }
    async fn dispatch_agent_hook(&self, raw: &str) -> acp::Result<crate::AgentHookResponse> {
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|_| acp::Error::invalid_params())?;
        let callback_id = value
            .get("hookCallbackId")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(acp::Error::invalid_params)?;
        let event: crate::AgentHookEvent = serde_json::from_value(
            value
                .get("hookEventName")
                .cloned()
                .ok_or_else(acp::Error::invalid_params)?,
        )
        .map_err(|_| acp::Error::invalid_params())?;
        let session_id = value
            .get("sessionId")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(acp::Error::invalid_params)?
            .to_owned();
        let handler = self
            .agent_hooks
            .get(callback_id)
            .ok_or_else(acp::Error::method_not_found)?;
        let string = |key: &str| value.get(key).and_then(|v| v.as_str()).map(str::to_owned);
        let invocation = crate::AgentHookInvocation {
            event,
            callback_id: callback_id.to_owned(),
            session_id,
            cwd: string("cwd").map(Into::into),
            workspace_root: string("workspaceRoot").map(Into::into),
            timestamp: string("timestamp"),
            prompt_id: string("promptId"),
            permission_mode: string("permissionMode"),
            tool_name: string("toolName"),
            tool_use_id: string("toolUseId"),
            tool_input: value.get("toolInput").cloned(),
            tool_result: value.get("toolResult").cloned(),
            raw: value,
        };
        handler
            .handle(invocation)
            .await
            .map_err(|_| acp::Error::internal_error())
    }
    async fn host_call<T: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        args: T,
    ) -> acp::Result<R> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(acp::Error::method_not_found)?;
        let params = serde_json::to_value(args).map_err(|_| acp::Error::internal_error())?;
        let value = host
            .request(crate::HostRequest {
                method: method.into(),
                params,
            })
            .await
            .map_err(host_acp_error)?;
        serde_json::from_value(value).map_err(|e| {
            acp::Error::invalid_params()
                .data(serde_json::json!({"hostResponseError":e.to_string()}))
        })
    }
    fn emit(&self, sid: String, update: EventUpdate) -> acp::Result<()> {
        let root_session_id = xai_grok_shell::origin_runtime::resolve_root_session(&sid, None)
            // A root is registered after session/load returns. During that
            // call, replayed root updates still need a stable journal key.
            .or_else(|| self.replay.borrow().contains_key(&sid).then(|| sid.clone()))
            .ok_or_else(acp::Error::invalid_params)?;
        self.emit_root(root_session_id, update);
        Ok(())
    }

    fn emit_root(&self, root_session_id: String, update: EventUpdate) {
        let replay = match self.replay.borrow().get(&root_session_id).copied() {
            Some(ReplayMode::Capture) => true,
            Some(ReplayMode::Suppress) => return,
            None => false,
        };
        let mut seq = self.sequences.borrow_mut();
        let n = seq.entry(root_session_id.clone()).or_default();
        *n += 1;
        let event = Event {
            session_id: SessionId(root_session_id.clone()),
            sequence: *n,
            turn_id: self.turns.borrow().get(&root_session_id).cloned(),
            timestamp_ms: now_ms(),
            replay,
            update,
        };
        let mut journal = self.retained.borrow_mut();
        let retained = journal.entry(root_session_id).or_default();
        retained.push_back(event.clone());
        while retained.len() > self.capacity {
            retained.pop_front();
        }
        let _ = self.events.send(event);
    }
}

fn validate_session_ledger(id: &SessionId, ledger: &SessionLedger) -> Result<(), Error> {
    let mut turn_ids = HashSet::new();
    let mut active_index = 0_u64;
    let mut pending = false;
    for entry in &ledger.entries {
        if entry.turn_id.trim().is_empty()
            || entry.turn_id.len() > 512
            || entry.prompt_digest.trim().is_empty()
            || entry.prompt_digest.len() > 160
            || !turn_ids.insert(entry.turn_id.as_str())
        {
            return Err(Error::Operation(
                "native Turn ledger contains an invalid or duplicate identity".into(),
            ));
        }
        if matches!(entry.state, LedgerTurnState::Discarded) {
            continue;
        }
        if entry.runtime_prompt_index != active_index || pending {
            return Err(Error::Operation(
                "native Turn ledger active prompt indices are inconsistent".into(),
            ));
        }
        active_index = active_index
            .checked_add(1)
            .ok_or_else(|| Error::Operation("native Turn ledger index overflow".into()))?;
        match &entry.state {
            LedgerTurnState::Completed {
                outcome,
                settlement_id,
                usage,
            } => {
                let expected = if let Some(usage) = usage {
                    usage.validate().map_err(run_error)?;
                    ledger_settlement_id(
                        id.as_str(),
                        &entry.turn_id,
                        &entry.prompt_digest,
                        entry.runtime_prompt_index,
                        *outcome,
                        usage,
                    )?
                } else {
                    legacy_ledger_settlement_id(
                        id.as_str(),
                        &entry.turn_id,
                        &entry.prompt_digest,
                        entry.runtime_prompt_index,
                        *outcome,
                    )
                };
                if settlement_id != &expected {
                    return Err(Error::Operation(
                        "native Turn ledger settlement identity is invalid".into(),
                    ));
                }
            }
            LedgerTurnState::Pending => pending = true,
            LedgerTurnState::Discarded => unreachable!("discarded entry handled above"),
        }
    }
    Ok(())
}

fn settle_latest_ledger_entry(ledger: &mut SessionLedger, receipt: &PromptReceipt) {
    ledger
        .entries
        .last_mut()
        .expect("the pending ledger entry was just appended")
        .state = LedgerTurnState::Completed {
        outcome: receipt.outcome,
        settlement_id: receipt.settlement_id.clone(),
        usage: Some(receipt.usage.clone()),
    };
}

pub(crate) fn ledger_settlement_id(
    session_id: &str,
    turn_id: &str,
    prompt_digest: &str,
    runtime_prompt_index: u64,
    outcome: TurnOutcome,
    usage: &xai_agent_lifecycle::run::EffectUsage,
) -> Result<String, Error> {
    let session = xai_agent_lifecycle::run::SessionRef::new(session_id).map_err(run_error)?;
    Ok(xai_agent_lifecycle::run::session_turn_settlement_id(
        &session,
        turn_id,
        prompt_digest,
        runtime_prompt_index,
        crate::durable_turn_outcome(outcome),
        usage,
    ))
}

fn legacy_ledger_settlement_id(
    session_id: &str,
    turn_id: &str,
    prompt_digest: &str,
    runtime_prompt_index: u64,
    outcome: TurnOutcome,
) -> String {
    use sha2::Digest as _;
    let mut digest = sha2::Sha256::new();
    // Compatibility identifier retained across the public crate rename.
    digest.update(b"origin-grok-runtime.settlement.v1\0");
    let prompt_index = runtime_prompt_index.to_be_bytes();
    let outcome = format!("{outcome:?}");
    for field in [
        session_id.as_bytes(),
        turn_id.as_bytes(),
        prompt_digest.as_bytes(),
        prompt_index.as_slice(),
        outcome.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("sha256:{:x}", digest.finalize())
}

fn prompt_effect_usage(
    usage: Option<&PromptUsage>,
    wall_ms: u64,
) -> xai_agent_lifecycle::run::EffectUsage {
    use xai_agent_lifecycle::run::{EffectUsage, ResourceDimension, ResourceVector};

    let mut resources = ResourceVector::default()
        .iterations(1)
        .agent_calls(1)
        .agent_concurrency(1)
        .wall_ms(wall_ms);
    let mut unknown = std::collections::BTreeSet::from([ResourceDimension::ArtifactBytes]);
    match usage {
        Some(usage) if !usage.usage_is_incomplete => {
            let totals = &usage.totals;
            if totals.total_tokens > 0
                && totals.model_calls > 0
                && totals
                    .input_tokens
                    .checked_add(totals.output_tokens)
                    .is_some_and(|total| total == totals.total_tokens)
            {
                resources.tokens = totals.total_tokens;
            } else {
                unknown.insert(ResourceDimension::Tokens);
            }
            if totals.api_duration_ms > 0 {
                resources.active_ms = totals.api_duration_ms;
            } else {
                unknown.insert(ResourceDimension::ActiveMs);
            }
            if !totals.cost_is_partial
                && let Some(ticks) = totals.cost_usd_ticks
                && let Ok(ticks) = u64::try_from(ticks)
                && let Some(micros) = ticks.checked_add(9_999).map(|value| value / 10_000)
            {
                resources.cost_micros = micros;
            } else {
                unknown.insert(ResourceDimension::CostMicros);
            }
        }
        _ => {
            unknown.extend([
                ResourceDimension::Tokens,
                ResourceDimension::CostMicros,
                ResourceDimension::ActiveMs,
            ]);
        }
    }
    EffectUsage::measured(resources).unknown(unknown)
}

#[async_trait::async_trait(?Send)]
impl acp::Client for Client {
    async fn request_permission(
        &self,
        args: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
        if let Some(handler) = &self.tool_permission_handler {
            let request = Self::typed_permission_request(&args)?;
            let valid_ids: HashSet<_> = request.options.iter().map(|o| o.id.clone()).collect();
            let decision = handler
                .request_permission(request)
                .await
                // Policy errors can include host-only context or secrets. The
                // agent only needs a fail-closed transport error, not details.
                .map_err(|_| acp::Error::internal_error())?;
            let outcome = match decision {
                crate::ToolPermissionDecision::Cancelled => {
                    acp::RequestPermissionOutcome::Cancelled
                }
                crate::ToolPermissionDecision::Selected(id) if valid_ids.contains(&id) => {
                    acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
                        acp::PermissionOptionId::new(id),
                    ))
                }
                crate::ToolPermissionDecision::Selected(id) => {
                    return Err(acp::Error::invalid_params()
                        .data(serde_json::json!({"invalidPermissionOptionId":id})));
                }
            };
            return Ok(acp::RequestPermissionResponse::new(outcome));
        }
        if self.host.is_none() {
            return Ok(acp::RequestPermissionResponse::new(
                acp::RequestPermissionOutcome::Cancelled,
            ));
        }
        self.host_call("session/request_permission", args).await
    }

    async fn read_text_file(
        &self,
        args: acp::ReadTextFileRequest,
    ) -> acp::Result<acp::ReadTextFileResponse> {
        self.host_call("fs/read_text_file", args).await
    }
    async fn write_text_file(
        &self,
        args: acp::WriteTextFileRequest,
    ) -> acp::Result<acp::WriteTextFileResponse> {
        self.host_call("fs/write_text_file", args).await
    }
    async fn create_terminal(
        &self,
        args: acp::CreateTerminalRequest,
    ) -> acp::Result<acp::CreateTerminalResponse> {
        self.host_call("terminal/create", args).await
    }
    async fn terminal_output(
        &self,
        args: acp::TerminalOutputRequest,
    ) -> acp::Result<acp::TerminalOutputResponse> {
        self.host_call("terminal/output", args).await
    }
    async fn wait_for_terminal_exit(
        &self,
        args: acp::WaitForTerminalExitRequest,
    ) -> acp::Result<acp::WaitForTerminalExitResponse> {
        self.host_call("terminal/wait_for_exit", args).await
    }
    async fn kill_terminal(
        &self,
        args: acp::KillTerminalRequest,
    ) -> acp::Result<acp::KillTerminalResponse> {
        self.host_call("terminal/kill", args).await
    }
    async fn release_terminal(
        &self,
        args: acp::ReleaseTerminalRequest,
    ) -> acp::Result<acp::ReleaseTerminalResponse> {
        self.host_call("terminal/release", args).await
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> acp::Result<()> {
        let payload = serde_json::to_value(&args.update).unwrap_or(serde_json::Value::Null);
        let raw = serde_json::to_string(&payload).unwrap_or_else(|_| "null".into());
        self.capture_turn_usage(&args.session_id.0, &payload)?;
        let update = match args.update {
            acp::SessionUpdate::UserMessageChunk(chunk) => content_update(
                chunk.content,
                EventUpdate::UserText,
                "user_message_non_text",
                &payload,
                &raw,
            ),
            acp::SessionUpdate::AgentMessageChunk(chunk) => content_update(
                chunk.content,
                EventUpdate::AssistantText,
                "agent_message_non_text",
                &payload,
                &raw,
            ),
            acp::SessionUpdate::AgentThoughtChunk(chunk) => content_update(
                chunk.content,
                EventUpdate::ThoughtText,
                "agent_thought_non_text",
                &payload,
                &raw,
            ),
            acp::SessionUpdate::ToolCall(call) => EventUpdate::ToolStart(crate::ToolEvent {
                id: call.tool_call_id.0.to_string(),
                title: call.title,
                kind: format!("{:?}", call.kind),
                status: format!("{:?}", call.status),
                raw_input: call.raw_input.map(|value| value.to_string()),
                raw_output: call.raw_output.map(|value| value.to_string()),
            }),
            acp::SessionUpdate::ToolCallUpdate(update) => {
                EventUpdate::ToolUpdate(crate::ToolEvent {
                    id: update.tool_call_id.0.to_string(),
                    title: update.fields.title.unwrap_or_default(),
                    kind: update
                        .fields
                        .kind
                        .map(|kind| format!("{kind:?}"))
                        .unwrap_or_default(),
                    status: update
                        .fields
                        .status
                        .map(|status| format!("{status:?}"))
                        .unwrap_or_default(),
                    raw_input: update.fields.raw_input.map(|value| value.to_string()),
                    raw_output: update.fields.raw_output.map(|value| value.to_string()),
                })
            }
            acp::SessionUpdate::Plan(plan) => EventUpdate::Plan {
                summary: plan
                    .entries
                    .into_iter()
                    .map(|entry| {
                        format!(
                            "[{:?}/{:?}] {}",
                            entry.status, entry.priority, entry.content
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            },
            acp::SessionUpdate::AvailableCommandsUpdate(update) => EventUpdate::AvailableCommands(
                update
                    .available_commands
                    .into_iter()
                    .map(|command| crate::RuntimeCommand {
                        name: command.name,
                        description: command.description,
                    })
                    .collect(),
            ),
            acp::SessionUpdate::CurrentModeUpdate(update) => {
                EventUpdate::ModeChanged(update.current_mode_id.0.to_string())
            }
            acp::SessionUpdate::ConfigOptionUpdate(update) => EventUpdate::ConfigOptions(
                update
                    .config_options
                    .into_iter()
                    .map(|option| crate::RuntimeConfigOption {
                        id: option.id.0.to_string(),
                        name: option.name,
                        category: option.category.and_then(|category| {
                            serde_json::to_value(category)
                                .ok()
                                .and_then(|value| value.as_str().map(str::to_owned))
                        }),
                        value: match option.kind {
                            acp::SessionConfigKind::Select(select) => {
                                Some(select.current_value.0.to_string())
                            }
                            _ => None,
                        },
                    })
                    .collect(),
            ),
            acp::SessionUpdate::SessionInfoUpdate(update) => EventUpdate::SessionInfo {
                title: update.title.take(),
            },
            _ => EventUpdate::Unknown {
                tag: "unrecognized".into(),
                payload,
                raw,
            },
        };
        self.emit(args.session_id.0.to_string(), update)
    }

    async fn ext_method(&self, args: acp::ExtRequest) -> acp::Result<acp::ExtResponse> {
        if args.method.as_ref() == "x.ai/hooks/run" {
            let response = self.dispatch_agent_hook(args.params.get()).await?;
            let raw = serde_json::value::to_raw_value(&response)
                .map_err(|_| acp::Error::internal_error())?;
            return Ok(acp::ExtResponse::new(Arc::from(raw)));
        }
        if !self.host_extension_methods.contains(args.method.as_ref()) {
            return Err(acp::Error::method_not_found());
        }
        let host = self
            .host
            .as_ref()
            .ok_or_else(acp::Error::method_not_found)?;
        let value = serde_json::from_str(args.params.get())
            .unwrap_or_else(|_| serde_json::Value::String(args.params.get().to_owned()));
        let result = host
            .request(crate::HostRequest {
                method: args.method.to_string(),
                params: value,
            })
            .await
            .map_err(host_acp_error)?;
        let raw =
            serde_json::value::to_raw_value(&result).map_err(|_| acp::Error::internal_error())?;
        Ok(acp::ExtResponse::new(Arc::from(raw)))
    }
    async fn ext_notification(&self, args: acp::ExtNotification) -> acp::Result<()> {
        if args.method.as_ref() == "x.ai/hooks/event" {
            self.dispatch_agent_hook(args.params.get()).await?;
            return Ok(());
        }
        let raw = args.params.get().to_owned();
        let payload =
            serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::Value::String(raw.clone()));
        if args.method.as_ref() == "x.ai/session_notification"
            && let (Some(session_id), Some(update)) = (
                payload.get("sessionId").and_then(serde_json::Value::as_str),
                payload.get("update"),
            )
        {
            self.capture_turn_usage(session_id, update)?;
        }
        let root = payload
            .get("sessionId")
            .and_then(|value| value.as_str())
            .and_then(|session_id| {
                xai_grok_shell::origin_runtime::resolve_root_session(session_id, None)
            })
            .unwrap_or_else(|| SessionId::runtime_events().0);
        let is_mcp_notification = args.method.as_ref().starts_with("x.ai/mcp/");
        let update = match typed_mcp_notification(args.method.as_ref(), &payload) {
            Some(update) => update,
            None if is_mcp_notification => {
                // MCP configuration notifications are shell-owned control-plane
                // data and may contain transport or setup credentials. Unknown
                // methods fail closed instead of entering the public journal or
                // HostDelegate as an untyped raw payload.
                return Ok(());
            }
            None => EventUpdate::Extension {
                method: args.method.to_string(),
                payload: payload.clone(),
                raw: raw.clone(),
            },
        };
        self.emit_root(root, update);
        if !is_mcp_notification && let Some(host) = &self.host {
            host.notification(crate::HostNotification {
                method: args.method.to_string(),
                params: payload,
            })
            .await
            .map_err(host_acp_error)?;
        }
        Ok(())
    }
}

fn validate_mcp_response(
    request: &serde_json::Value,
    response: &serde_json::Value,
) -> acp::Result<()> {
    let Some(request_id) = request.get("id") else {
        return if response.is_null() {
            Ok(())
        } else {
            Err(acp::Error::internal_error())
        };
    };
    let object = response
        .as_object()
        .ok_or_else(acp::Error::internal_error)?;
    if object.get("jsonrpc").and_then(|v| v.as_str()) == Some("2.0")
        && object.get("id") == Some(request_id)
        && (object.contains_key("result") ^ object.contains_key("error"))
    {
        Ok(())
    } else {
        Err(acp::Error::internal_error())
    }
}

fn typed_mcp_notification(method: &str, payload: &serde_json::Value) -> Option<EventUpdate> {
    match method {
        "x.ai/mcp/server_status" => {
            Some(EventUpdate::McpServerStatus(crate::McpServerStatusEvent {
                name: payload["name"].as_str()?.to_owned(),
                source: match payload["source"].as_str() {
                    Some("local") => crate::McpServerSource::Local,
                    Some("managed") => crate::McpServerSource::Managed,
                    _ => crate::McpServerSource::Unknown,
                },
                status: match payload["status"].as_str() {
                    Some("ready") => crate::McpServerStatus::Ready,
                    Some("initializing") => crate::McpServerStatus::Initializing,
                    Some("setuprequired") | Some("setup_required") => {
                        crate::McpServerStatus::SetupRequired
                    }
                    Some("unavailable") => crate::McpServerStatus::Unavailable,
                    Some("needsauth") | Some("needs_auth") => crate::McpServerStatus::NeedsAuth,
                    _ => crate::McpServerStatus::Unknown,
                },
                reason: match payload["reason"].as_str() {
                    Some("transport_closed") => crate::McpServerStatusReason::TransportClosed,
                    Some("handshake_failed") => crate::McpServerStatusReason::HandshakeFailed,
                    Some("config_added") => crate::McpServerStatusReason::ConfigAdded,
                    Some("config_removed") => crate::McpServerStatusReason::ConfigRemoved,
                    Some("config_changed") => crate::McpServerStatusReason::ConfigChanged,
                    Some("disabled") => crate::McpServerStatusReason::Disabled,
                    Some("auth_expired") => crate::McpServerStatusReason::AuthExpired,
                    Some("initialized") => crate::McpServerStatusReason::Initialized,
                    Some("restart_succeeded") => crate::McpServerStatusReason::RestartSucceeded,
                    Some("restart_failed") => crate::McpServerStatusReason::RestartFailed,
                    Some("managed_token_refreshed") => {
                        crate::McpServerStatusReason::ManagedTokenRefreshed
                    }
                    _ => crate::McpServerStatusReason::Unknown,
                },
            }))
        }
        "x.ai/mcp/task_status" => {
            let session_id = SessionId(payload["sessionId"].as_str()?.to_owned());
            let server = payload["server"].as_str()?;
            let client_id = payload["clientId"].as_u64()?;
            let task = payload.get("task")?.as_object()?;
            let task_id = task.get("taskId")?.as_str()?.to_owned();
            let status = crate::parse_task_status(task.get("status")?).ok()?;
            let status_message = match task.get("statusMessage") {
                Some(serde_json::Value::Null) | None => None,
                Some(value) => Some(value.as_str()?.to_owned()),
            };
            let last_updated_at = task.get("lastUpdatedAt")?.as_str()?.to_owned();
            Some(EventUpdate::McpTaskStatus(crate::McpTaskStatusEvent {
                handle: crate::McpTaskHandle {
                    session_id,
                    server: server.to_owned(),
                    client_id,
                    task_id,
                },
                status,
                status_message,
                last_updated_at,
            }))
        }
        "x.ai/mcp/tools_changed" => {
            let server_name = payload["serverName"]
                .as_str()
                .filter(|name| !name.is_empty())
                .map(str::to_owned);
            let tools = payload["tools"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|tool| crate::McpToolInfo {
                    server: server_name.clone().unwrap_or_default(),
                    name: tool["name"].as_str().unwrap_or_default().to_owned(),
                    display_name: tool["displayName"].as_str().map(str::to_owned),
                    description: tool["description"].as_str().map(str::to_owned),
                    enabled: tool["enabled"].as_bool().unwrap_or(true),
                    // Push events are a redacted hint to refetch the explicit
                    // catalog; server-controlled metadata stays off the event
                    // journal and HostDelegate boundary.
                    meta: serde_json::Value::Null,
                })
                .collect();
            Some(EventUpdate::McpToolsChanged(crate::McpToolsChangedEvent {
                server_name,
                tools,
            }))
        }
        "x.ai/mcp/init_progress" => Some(EventUpdate::McpInitializationProgress(
            crate::McpInitializationProgress {
                connected: payload["connected"].as_u64()?.try_into().ok()?,
                total: payload["total"].as_u64()?.try_into().ok()?,
            },
        )),
        "x.ai/mcp/servers_updated" => {
            let mut catalog = serde_json::Map::new();
            catalog.insert("servers".to_owned(), payload.get("mcpServers")?.clone());
            crate::parse_mcp_servers(&serde_json::Value::Object(catalog))
                .ok()
                .map(|mut servers| {
                    for server in &mut servers {
                        for tool in &mut server.tools {
                            tool.meta = serde_json::Value::Null;
                        }
                        if let Some(negotiated) = &mut server.negotiated {
                            negotiated.extensions.clear();
                            negotiated.raw = serde_json::Value::Null;
                        }
                    }
                    servers
                })
                .map(EventUpdate::McpServersChanged)
        }
        _ => None,
    }
}

fn host_acp_error(error: crate::HostError) -> acp::Error {
    acp::Error::new(error.code, error.message).data(error.data)
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn content_update(
    content: acp::ContentBlock,
    text: impl FnOnce(String) -> EventUpdate,
    non_text_tag: &'static str,
    payload: &serde_json::Value,
    raw: &str,
) -> EventUpdate {
    match content {
        acp::ContentBlock::Text(content) => text(content.text),
        _ => EventUpdate::Unknown {
            tag: non_text_tag.into(),
            payload: payload.clone(),
            raw: raw.into(),
        },
    }
}

#[derive(serde::Deserialize)]
struct RewindPointsWire {
    rewind_points: Vec<RewindPointWire>,
}
#[derive(serde::Deserialize)]
struct RewindPointWire {
    prompt_index: u64,
    created_at: String,
    num_file_snapshots: u64,
    has_file_changes: bool,
    prompt_preview: Option<String>,
    origin_prompt_digest: Option<String>,
}
#[derive(serde::Deserialize)]
struct RewindResultWire {
    success: bool,
    target_prompt_index: u64,
    mode: String,
    reverted_files: Vec<String>,
    #[serde(default)]
    clean_files: Vec<String>,
    conflicts: Vec<RewindConflictWire>,
    #[serde(default)]
    prompt_text: Option<String>,
    error: Option<String>,
}
#[derive(serde::Deserialize)]
struct RewindConflictWire {
    path: String,
    conflict_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct RewindIntent {
    operation_id: String,
    session_id: String,
    target_prompt_index: u64,
    target_turn_id: String,
    target_prompt_digest: String,
    recovery_turn_id: Option<String>,
    recovery_prompt_digest: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
enum RewindEvidence {
    Intent(RewindIntent),
    Receipt(ConversationRewindReceipt),
}

fn native_rewind_already_applied(
    points: &[RewindPointWire],
    target_prompt_index: u64,
    ledger: &SessionLedger,
) -> Result<bool, Error> {
    for (expected, point) in points.iter().enumerate() {
        if point.prompt_index != expected as u64 {
            return Err(Error::Operation(
                "native Grok rewind points are not a contiguous prompt history".into(),
            ));
        }
        let expected_entry = ledger.entries.iter().find(|entry| {
            entry.runtime_prompt_index == point.prompt_index
                && !matches!(entry.state, LedgerTurnState::Discarded)
        });
        if expected_entry.is_some_and(|entry| {
            !entry.prompt_digest.starts_with("sha256-v2:")
                && Some(entry.prompt_digest.as_str()) != point.origin_prompt_digest.as_deref()
        }) {
            return Err(Error::Operation(
                "native Grok prompt prefix differs from the durable Turn ledger".into(),
            ));
        }
    }
    let prompt_count = points.len() as u64;
    if prompt_count < target_prompt_index {
        return Err(Error::Operation(
            "native Grok conversation is behind the pending rewind target".into(),
        ));
    }
    Ok(prompt_count == target_prompt_index)
}

#[derive(serde::Deserialize)]
struct UnloadWire {
    success: bool,
    drained: bool,
}

struct TurnReservation {
    turns: Rc<RefCell<HashMap<String, String>>>,
    turn_usages: TurnUsageMap,
    session_id: String,
    turn_id: String,
}

impl Drop for TurnReservation {
    fn drop(&mut self) {
        let mut turns = self.turns.borrow_mut();
        if turns
            .get(&self.session_id)
            .is_some_and(|active_turn| active_turn == &self.turn_id)
        {
            turns.remove(&self.session_id);
        }
        self.turn_usages
            .borrow_mut()
            .remove(&(self.session_id.clone(), self.turn_id.clone()));
    }
}

#[derive(Clone)]
struct SessionBinding {
    model: String,
    reasoning: Option<String>,
    harness_digest: Option<HarnessDigest>,
}

impl SessionBinding {
    fn new(
        config: &SessionConfig,
        effective_reasoning: Option<String>,
        harness_digest: Option<HarnessDigest>,
    ) -> Self {
        Self {
            model: config.model.clone(),
            reasoning: effective_reasoning,
            harness_digest,
        }
    }
}

struct PreparedHarnessTurn {
    prompt_digest: String,
    snapshot_digest: HarnessDigest,
    model: String,
    reasoning: Option<String>,
    after_sequence: u64,
}

struct Core {
    agent: Rc<MvpAgent>,
    session_state_authority: Option<Arc<ShellAuthority>>,
    session_state_store: Option<Arc<dyn crate::SessionStateStore>>,
    session_leases: RefCell<HashMap<String, Box<dyn crate::SessionStateLease>>>,
    events: mpsc::UnboundedSender<Event>,
    catalog: HashMap<String, crate::ModelSpec>,
    sequences: Rc<RefCell<HashMap<String, u64>>>,
    retained: Rc<RefCell<HashMap<String, VecDeque<Event>>>>,
    capacity: usize,
    options: RuntimeOptions,
    resident: RefCell<HashSet<String>>,
    session_bindings: RefCell<HashMap<String, SessionBinding>>,
    mcp_bindings: Arc<McpBindingRegistry>,
    turns: Rc<RefCell<HashMap<String, String>>>,
    turn_usages: TurnUsageMap,
    prompt_tasks: RefCell<HashMap<String, tokio::task::AbortHandle>>,
    replay: Rc<RefCell<HashMap<String, ReplayMode>>>,
    evidence_store: Arc<dyn SessionEvidenceStore>,
    evidence_versions: RefCell<HashMap<SessionEvidenceKey, SessionEvidenceVersion>>,
}

struct SessionLeaseAdmission<'a> {
    leases: &'a RefCell<HashMap<String, Box<dyn crate::SessionStateLease>>>,
    id: String,
    lease: Option<Box<dyn crate::SessionStateLease>>,
}

impl<'a> SessionLeaseAdmission<'a> {
    fn new(
        leases: &'a RefCell<HashMap<String, Box<dyn crate::SessionStateLease>>>,
        id: &SessionId,
        lease: Option<Box<dyn crate::SessionStateLease>>,
    ) -> Self {
        Self {
            leases,
            id: id.0.clone(),
            lease,
        }
    }
}

impl Drop for SessionLeaseAdmission<'_> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.leases.borrow_mut().insert(self.id.clone(), lease);
        }
    }
}

fn quarantine_session_leases(leases: Vec<Box<dyn crate::SessionStateLease>>) {
    static QUARANTINE: std::sync::OnceLock<
        std::sync::Mutex<Vec<Box<dyn crate::SessionStateLease>>>,
    > = std::sync::OnceLock::new();
    QUARANTINE
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(leases);
}

impl Drop for Core {
    fn drop(&mut self) {
        let leases = self
            .session_leases
            .get_mut()
            .drain()
            .map(|(_, lease)| lease)
            .collect();
        quarantine_session_leases(leases);
    }
}

type ShellAuthority = dyn xai_grok_shell::session::state_authority::NativeSessionStateAuthority;

struct SessionStateAuthorityBridge {
    store: Arc<dyn crate::SessionStateStore>,
}

struct SessionStateSessionBridge {
    store: Arc<dyn crate::SessionStateStore>,
    key: crate::SessionKey,
    generation: crate::SessionGeneration,
    identity: xai_grok_shell::session::state_authority::SessionIdentity,
    staged: std::sync::Mutex<Vec<Vec<u8>>>,
}

fn authority_error(
    error: impl ToString,
) -> xai_grok_shell::session::state_authority::AuthorityError {
    xai_grok_shell::session::state_authority::AuthorityError(error.to_string())
}

impl xai_grok_shell::session::state_authority::NativeSessionStateAuthority
    for SessionStateAuthorityBridge
{
    fn inspect(
        &self,
        session_identity: &str,
    ) -> Result<
        xai_grok_shell::session::state_authority::SessionInspection,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::SessionInspection;
        let key = crate::SessionKey::new(session_identity).map_err(authority_error)?;
        Ok(
            match self.store.inspect_slot(&key).map_err(authority_error)? {
                crate::SessionSlot::Vacant => SessionInspection::Vacant,
                crate::SessionSlot::Live(x) => SessionInspection::Live {
                    generation: x.manifest().generation().as_str().to_owned(),
                },
                crate::SessionSlot::Tombstoned { receipt } => SessionInspection::Tombstoned {
                    generation: receipt.generation().as_str().to_owned(),
                },
            },
        )
    }

    fn create(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        session.create_empty()?;
        Ok(Arc::new(session))
    }

    fn open(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        let current = session
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        if current.manifest().generation() != &session.generation {
            return Err(authority_error("session generation mismatch"));
        }
        Ok(Arc::new(session))
    }

    fn publish_fork(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
        records: Vec<xai_grok_shell::session::state_authority::ReplayRecord>,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        session.publish_prepared(records)?;
        Ok(Arc::new(session))
    }

    fn tombstone(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let session = self.session(id)?;
        let expected = session
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        if expected.manifest().generation() != &session.generation {
            return Err(authority_error("session generation mismatch"));
        }
        let request = crate::PreparedSessionDelete::new(session.key.clone(), expected.clone())
            .map_err(authority_error)?;
        let result = self
            .store
            .compare_and_delete(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&session.key)
            .map_err(authority_error)?;
        if crate::delete_reconciled(&result, &slot, &expected) {
            Ok(())
        } else {
            Err(authority_error(
                "delete conflicted or acknowledgement could not be reconciled",
            ))
        }
    }
}

impl SessionStateAuthorityBridge {
    fn session(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<SessionStateSessionBridge, xai_grok_shell::session::state_authority::AuthorityError>
    {
        Ok(SessionStateSessionBridge {
            store: self.store.clone(),
            key: crate::SessionKey::new(&id.identity).map_err(authority_error)?,
            generation: crate::SessionGeneration::new(&id.generation).map_err(authority_error)?,
            identity: id,
            staged: std::sync::Mutex::new(Vec::new()),
        })
    }
}

impl SessionStateSessionBridge {
    fn publish_prepared(
        &self,
        records: Vec<xai_grok_shell::session::state_authority::ReplayRecord>,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        use xai_grok_shell::session::state_authority::{ReplayRecord, RewindOperation};
        if self.current()?.is_some() {
            return Err(authority_error("fork target already exists"));
        }
        let mut previous = None;
        let mut sequence = 0u64;
        let mut objects = Vec::with_capacity(records.len());
        for record in records {
            match record {
                ReplayRecord::Update(bytes) => {
                    objects.extend(self.update_objects(&[bytes], &mut previous, &mut sequence)?);
                }
                ReplayRecord::Checkpoint {
                    name,
                    payload,
                    marker,
                } => {
                    let checkpoint = crate::SessionObject::checkpoint(
                        self.key.clone(),
                        self.generation.clone(),
                        name,
                        payload,
                    )
                    .map_err(authority_error)?;
                    self.put_exact(&checkpoint)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| authority_error("sequence overflow"))?;
                    let publication = crate::SessionObject::publish_checkpoint(
                        self.key.clone(),
                        self.generation.clone(),
                        previous.clone(),
                        sequence,
                        marker,
                        checkpoint.id().clone(),
                    )
                    .map_err(authority_error)?;
                    previous = Some(publication.id().clone());
                    objects.push(publication);
                }
                ReplayRecord::Rewind { operation, marker } => {
                    let (kind, index, payload) = match operation {
                        RewindOperation::AppendPoint { index, payload } => {
                            (crate::RewindKind::AppendPoint, index, payload)
                        }
                        RewindOperation::Truncate { index, payload } => {
                            (crate::RewindKind::Truncate, index, payload)
                        }
                        RewindOperation::Merge { index, payload } => {
                            (crate::RewindKind::Merge, index, payload)
                        }
                    };
                    let rewind = crate::SessionObject::rewind(
                        self.key.clone(),
                        self.generation.clone(),
                        kind,
                        index,
                        payload,
                    )
                    .map_err(authority_error)?;
                    self.put_exact(&rewind)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| authority_error("sequence overflow"))?;
                    let publication = crate::SessionObject::publish_rewind(
                        self.key.clone(),
                        self.generation.clone(),
                        previous.clone(),
                        sequence,
                        marker,
                        rewind.id().clone(),
                    )
                    .map_err(authority_error)?;
                    previous = Some(publication.id().clone());
                    objects.push(publication);
                }
            }
        }
        if objects.is_empty() {
            self.create_empty()
        } else {
            self.commit(objects).map(|_| ())
        }
    }

    fn create_empty(&self) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let manifest =
            crate::SessionManifest::new(self.key.clone(), self.generation.clone(), None, 0, 0)
                .map_err(authority_error)?;
        let request = crate::PreparedManifestCas::new(self.key.clone(), None, manifest, &[])
            .map_err(authority_error)?;
        let intended = request.successor().clone();
        let result = self
            .store
            .compare_and_swap_manifest(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?;
        if crate::manifest_cas_reconciled(&result, &slot, &intended) {
            Ok(())
        } else {
            Err(authority_error(
                "session already exists or creation acknowledgement could not be reconciled",
            ))
        }
    }

    fn current(
        &self,
    ) -> Result<
        Option<crate::LiveSessionDocument>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        match self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?
        {
            crate::SessionSlot::Vacant => Ok(None),
            crate::SessionSlot::Live(x) => Ok(Some(x)),
            crate::SessionSlot::Tombstoned { .. } => Err(authority_error("session is tombstoned")),
        }
    }
    fn put_exact(
        &self,
        object: &crate::SessionObject,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let result = self
            .store
            .put_object(object.clone())
            .map_err(authority_error)?;
        let loaded = if result == crate::ObjectPut::CommitUnknown {
            self.store
                .load_object(&self.key, &self.generation, object.id())
                .map_err(authority_error)?
        } else {
            None
        };
        if crate::object_put_reconciled(&result, loaded.as_ref(), object) {
            Ok(())
        } else {
            Err(authority_error(
                "object acknowledgement could not be reconciled exactly",
            ))
        }
    }
    fn commit(
        &self,
        objects: Vec<crate::SessionObject>,
    ) -> Result<crate::LiveSessionDocument, xai_grok_shell::session::state_authority::AuthorityError>
    {
        let expected = self.current()?;
        if let Some(x) = &expected {
            if x.manifest().generation() != &self.generation {
                return Err(authority_error("session generation mismatch"));
            }
        }
        for object in &objects {
            self.put_exact(object)?;
        }
        let head = objects
            .last()
            .map(|x| x.id().clone())
            .or_else(|| expected.as_ref().and_then(|x| x.manifest().head().cloned()));
        let count = expected
            .as_ref()
            .map_or(0, |x| x.manifest().segment_count())
            .checked_add(objects.len() as u64)
            .ok_or_else(|| authority_error("record count overflow"))?;
        let added = objects
            .iter()
            .map(|x| match x.kind() {
                crate::SessionObjectKind::TranscriptSegment { bytes, .. } => bytes.len() as u64,
                crate::SessionObjectKind::CheckpointPublication { marker_bytes, .. }
                | crate::SessionObjectKind::RewindPublication { marker_bytes, .. } => {
                    marker_bytes.len() as u64
                }
                _ => 0,
            })
            .sum::<u64>();
        let bytes = expected
            .as_ref()
            .map_or(0, |x| x.manifest().transcript_bytes())
            .checked_add(added)
            .ok_or_else(|| authority_error("transcript size overflow"))?;
        let manifest = crate::SessionManifest::new(
            self.key.clone(),
            self.generation.clone(),
            head,
            count,
            bytes,
        )
        .map_err(authority_error)?;
        let request =
            crate::PreparedManifestCas::new(self.key.clone(), expected, manifest, &objects)
                .map_err(authority_error)?;
        let intended = request.successor().clone();
        let result = self
            .store
            .compare_and_swap_manifest(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?;
        if crate::manifest_cas_reconciled(&result, &slot, &intended) {
            Ok(intended)
        } else {
            Err(authority_error(
                "manifest conflicted or acknowledgement could not be reconciled exactly",
            ))
        }
    }
    fn update_objects(
        &self,
        updates: &[Vec<u8>],
        previous: &mut Option<crate::SessionObjectId>,
        sequence: &mut u64,
    ) -> Result<Vec<crate::SessionObject>, xai_grok_shell::session::state_authority::AuthorityError>
    {
        let mut out = Vec::new();
        for bytes in updates {
            if bytes.len() > crate::TARGET_TRANSCRIPT_SEGMENT_BYTES {
                return Err(authority_error("single update exceeds chunk limit"));
            }
            *sequence = sequence
                .checked_add(1)
                .ok_or_else(|| authority_error("sequence overflow"))?;
            let object = crate::SessionObject::transcript(
                self.key.clone(),
                self.generation.clone(),
                previous.clone(),
                *sequence,
                bytes.clone(),
            )
            .map_err(authority_error)?;
            *previous = Some(object.id().clone());
            out.push(object);
        }
        Ok(out)
    }
}

impl xai_grok_shell::session::state_authority::NativeSession for SessionStateSessionBridge {
    fn identity(&self) -> &xai_grok_shell::session::state_authority::SessionIdentity {
        &self.identity
    }
    fn stage_update(
        &self,
        bytes: Vec<u8>,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        if bytes.len() > crate::TARGET_TRANSCRIPT_SEGMENT_BYTES {
            return Err(authority_error("single update exceeds chunk limit"));
        }
        self.staged.lock().map_err(authority_error)?.push(bytes);
        Ok(())
    }
    fn flush(
        &self,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        if !objects.is_empty() {
            self.commit(objects)?;
            staged.clear();
        }
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }
    fn replay_page(
        &self,
        cursor: Option<xai_grok_shell::session::state_authority::ReplayCursor>,
        max_records: usize,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayPage,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::{
            ReplayCursor, ReplayPage, ReplayRecord, RewindOperation,
        };
        if max_records == 0 || max_records > 4096 {
            return Err(authority_error("invalid replay page bound"));
        }
        let doc = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let start = cursor.map_or(1, |c| {
            if c.generation != self.identity.generation {
                u64::MAX
            } else {
                c.next_sequence
            }
        });
        if start == 0 || start > doc.manifest().segment_count() + 1 {
            return Err(authority_error("invalid replay cursor or cursor gap"));
        }
        let mut chain = Vec::new();
        let mut id = doc.manifest().head().cloned();
        while let Some(object_id) = id {
            if chain.len() >= 1_000_000 {
                return Err(authority_error("replay traversal limit exceeded"));
            }
            let object = self
                .store
                .load_object(&self.key, &self.generation, &object_id)
                .map_err(authority_error)?
                .ok_or_else(|| authority_error("missing replay object"))?;
            id = object.previous().cloned();
            chain.push(object);
        }
        chain.reverse();
        if chain.len() as u64 != doc.manifest().segment_count()
            || chain
                .iter()
                .enumerate()
                .any(|(i, x)| x.sequence() != Some(i as u64 + 1))
        {
            return Err(authority_error("corrupt replay chain or cursor gap"));
        }
        let mut records = Vec::new();
        for object in chain
            .into_iter()
            .skip((start - 1) as usize)
            .take(max_records)
        {
            records.push(match object.kind() {
                crate::SessionObjectKind::TranscriptSegment { bytes, .. } => {
                    ReplayRecord::Update(bytes.clone())
                }
                crate::SessionObjectKind::CheckpointPublication {
                    marker_bytes,
                    checkpoint,
                    ..
                } => {
                    let x = self
                        .store
                        .load_object(&self.key, &self.generation, checkpoint)
                        .map_err(authority_error)?
                        .ok_or_else(|| authority_error("missing checkpoint object"))?;
                    match x.kind() {
                        crate::SessionObjectKind::Checkpoint { name, shell_bytes } => {
                            ReplayRecord::Checkpoint {
                                name: name.clone(),
                                payload: shell_bytes.clone(),
                                marker: marker_bytes.clone(),
                            }
                        }
                        _ => return Err(authority_error("invalid checkpoint reference")),
                    }
                }
                crate::SessionObjectKind::RewindPublication {
                    marker_bytes,
                    operation,
                    ..
                } => {
                    let x = self
                        .store
                        .load_object(&self.key, &self.generation, operation)
                        .map_err(authority_error)?
                        .ok_or_else(|| authority_error("missing rewind object"))?;
                    match x.kind() {
                        crate::SessionObjectKind::RewindOperation {
                            kind,
                            index,
                            shell_bytes,
                        } => {
                            let op = match kind {
                                crate::RewindKind::AppendPoint => RewindOperation::AppendPoint {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                                crate::RewindKind::Truncate => RewindOperation::Truncate {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                                crate::RewindKind::Merge => RewindOperation::Merge {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                            };
                            ReplayRecord::Rewind {
                                operation: op,
                                marker: marker_bytes.clone(),
                            }
                        }
                        _ => return Err(authority_error("invalid rewind reference")),
                    }
                }
                _ => return Err(authority_error("unpublished object in replay chain")),
            });
        }
        let next_sequence = start + records.len() as u64;
        Ok(ReplayPage {
            records,
            next: (next_sequence <= doc.manifest().segment_count()).then(|| ReplayCursor {
                generation: self.identity.generation.clone(),
                next_sequence,
            }),
        })
    }
    fn publish_checkpoint(
        &self,
        name: String,
        payload: Vec<u8>,
        marker: Vec<u8>,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let mut objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        let checkpoint = crate::SessionObject::checkpoint(
            self.key.clone(),
            self.generation.clone(),
            name,
            payload,
        )
        .map_err(authority_error)?;
        self.put_exact(&checkpoint)?;
        sequence += 1;
        let publication = crate::SessionObject::publish_checkpoint(
            self.key.clone(),
            self.generation.clone(),
            previous,
            sequence,
            marker,
            checkpoint.id().clone(),
        )
        .map_err(authority_error)?;
        objects.push(publication);
        self.commit(objects)?;
        staged.clear();
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }
    fn publish_rewind(
        &self,
        operation: xai_grok_shell::session::state_authority::RewindOperation,
        marker: Vec<u8>,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::RewindOperation;
        let (kind, index, payload) = match operation {
            RewindOperation::AppendPoint { index, payload } => {
                (crate::RewindKind::AppendPoint, index, payload)
            }
            RewindOperation::Truncate { index, payload } => {
                (crate::RewindKind::Truncate, index, payload)
            }
            RewindOperation::Merge { index, payload } => (crate::RewindKind::Merge, index, payload),
        };
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let mut objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        let op = crate::SessionObject::rewind(
            self.key.clone(),
            self.generation.clone(),
            kind,
            index,
            payload,
        )
        .map_err(authority_error)?;
        self.put_exact(&op)?;
        sequence += 1;
        let publication = crate::SessionObject::publish_rewind(
            self.key.clone(),
            self.generation.clone(),
            previous,
            sequence,
            marker,
            op.id().clone(),
        )
        .map_err(authority_error)?;
        objects.push(publication);
        self.commit(objects)?;
        staged.clear();
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }
}

impl Core {
    async fn start(
        input: RuntimeConfig,
        options: RuntimeOptions,
        events: mpsc::UnboundedSender<Event>,
        evidence_store: Arc<dyn SessionEvidenceStore>,
        session_state_store: Option<Arc<dyn crate::SessionStateStore>>,
    ) -> Result<(Self, RuntimeCapabilities), Error> {
        std::fs::create_dir_all(&input.grok_home).map_err(op)?;
        std::fs::create_dir_all(&input.session_storage).map_err(op)?;
        let mut cfg = match options.profile {
            crate::RuntimeProfile::Restricted => Config::origin_embedded(),
            crate::RuntimeProfile::Desktop => Config::origin_desktop(),
        };
        if options.profile == crate::RuntimeProfile::Desktop {
            cfg.skills.paths = options
                .skill_paths
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect();
            cfg.plugins.cli_plugin_dirs = options.plugin_paths.clone();
        } else {
            cfg.skills.paths.clear();
            cfg.plugins.cli_plugin_dirs.clear();
        }
        let fallback_endpoint = if input.endpoint.trim().is_empty() {
            options
                .services
                .model_providers
                .values()
                .next()
                .expect("validated model provider")
                .base_url
                .clone()
        } else {
            input.endpoint.clone()
        };
        cfg.endpoints.cli_chat_proxy_base_url = Some(fallback_endpoint.clone());
        cfg.endpoints.xai_api_base_url = fallback_endpoint;
        cfg.endpoints.models_base_url = None;
        cfg.endpoints.models_list_url = None;
        cfg.default_model_override = input.models.first().map(|model| model.id.clone());
        cfg.subagent_model_overrides = options
            .services
            .agents
            .subagent_models
            .clone()
            .into_iter()
            .collect();
        let auxiliary_model_slug = |model_id: &str| {
            options
                .services
                .model_providers
                .get(model_id)
                .and_then(|provider| provider.model.as_deref())
                .unwrap_or(model_id)
                .to_owned()
        };
        if let Some(model) = &options.services.agents.web_search_model {
            cfg.web_search_model = auxiliary_model_slug(model);
        } else {
            // Embedded runtimes never fall through to the shell's hidden
            // first-party search model, which can consult ambient credentials.
            cfg.disable_web_search = true;
        }
        cfg.session_summary_model = options
            .services
            .agents
            .session_summary_model
            .as_deref()
            .map(&auxiliary_model_slug);
        cfg.image_description_model = options
            .services
            .agents
            .image_description_model
            .as_deref()
            .map(&auxiliary_model_slug);
        cfg.transcribe_user_images = options.services.agents.image_description_model.is_some();
        if let Some(model) = &options.services.agents.prompt_suggestion_model {
            cfg.prompt_suggest_model_pin =
                xai_grok_shell::config::PromptSuggestModelPin::Pinned(auxiliary_model_slug(model));
        }
        if options.profile == crate::RuntimeProfile::Desktop {
            cfg.origin_media = options
                .services
                .media
                .as_ref()
                .map(|media| OriginMediaConfig {
                    api_key: media.provider.api_key.clone(),
                    base_url: media.provider.base_url.clone(),
                    extra_headers: media.provider.headers.clone().into_iter().collect(),
                    query_params: media.provider.query_params.clone().into_iter().collect(),
                    image_gen_enabled: media.image_generation,
                    image_edit_enabled: media.image_edit,
                    video_gen_enabled: media.video_generation,
                    image_gen_model: media.image_generation_model.clone(),
                    image_edit_model: media.image_edit_model.clone(),
                    image_to_video_model: media.image_to_video_model.clone(),
                    reference_to_video_model: media.reference_to_video_model.clone(),
                });
        }
        let auth = Arc::new(AuthManager::new_origin_embedded(
            input.grok_home.join("origin-auth-disabled.json"),
            cfg.grok_com_config.clone(),
        ));
        let mut fixed = IndexMap::new();
        for model in &input.models {
            let provider = options.services.model_providers.get(&model.id);
            let base_url = provider
                .map(|provider| provider.base_url.as_str())
                .unwrap_or(input.endpoint.as_str());
            let api_key = provider
                .map(|provider| provider.api_key.as_str())
                .unwrap_or(input.api_key.as_str());
            let provider_model = provider
                .and_then(|provider| provider.model.as_deref())
                .unwrap_or(model.id.as_str());
            let extra_headers = provider
                .map(|provider| provider.headers.clone())
                .unwrap_or_default();
            let query_params = provider
                .map(|provider| provider.query_params.clone())
                .unwrap_or_default();
            let entry: ModelEntryConfig = serde_json::from_value(serde_json::json!({
                "id": model.id,
                "model": provider_model,
                "base_url": base_url,
                "api_key": api_key,
                "extra_headers": extra_headers,
                "query_params": query_params,
                "context_window": model.context_window,
                "api_backend": match model.api_backend {
                    crate::ApiBackend::ChatCompletions => "chat_completions",
                    crate::ApiBackend::Responses => "responses",
                },
                "agent_type": "grok-build",
                "reasoning_effort": model.default_reasoning,
                "supports_reasoning_effort": model.supports_reasoning,
                "reasoning_efforts": model.reasoning_options,
            }))
            .map_err(op)?;
            fixed.insert(model.id.clone(), ModelEntry::from_config_entry(&entry));
        }
        let models = ModelsManager::from_origin_fixed(fixed, auth.clone(), cfg.clone())
            .map_err(Error::Operation)?;
        let (gw_tx, gw_rx) = mpsc::unbounded_channel();
        let profile = match options.profile {
            crate::RuntimeProfile::Restricted => {
                xai_grok_shell::agent::config::OriginEmbeddedProfile::Restricted
            }
            crate::RuntimeProfile::Desktop => {
                xai_grok_shell::agent::config::OriginEmbeddedProfile::Desktop
            }
        };
        let session_state_authority: Option<Arc<ShellAuthority>> =
            session_state_store.as_ref().map(|store| {
                Arc::new(SessionStateAuthorityBridge {
                    store: store.clone(),
                }) as Arc<ShellAuthority>
            });
        let agent = Rc::new(
            MvpAgent::with_origin_embedded_profile_models_and_session_state(
                AcpAgentGatewaySender::new(gw_tx),
                &cfg,
                auth,
                models,
                input.session_storage.clone(),
                profile,
                session_state_authority.clone(),
            ),
        );
        static NEXT_RUNTIME_INSTANCE: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let runtime_instance_id = NEXT_RUNTIME_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let mcp_bindings = Arc::new(McpBindingRegistry::default());
        if options.profile == crate::RuntimeProfile::Desktop
            && (!options.in_process_mcp_servers.is_empty() || !options.mcp_host_services.is_empty())
        {
            let servers = options
                .in_process_mcp_servers
                .iter()
                .map(|server| xai_grok_mcp::servers::AcpServerEntry {
                    name: server.name.clone(),
                    server_id: server.server_id.clone(),
                })
                .collect();
            let handlers = options
                .in_process_mcp_servers
                .iter()
                .map(|server| {
                    (
                        server.server_id.clone(),
                        (server.name.clone(), server.handler.clone()),
                    )
                })
                .collect();
            agent.set_embedded_mcp_servers(
                servers,
                Arc::new(DirectMcpInvoker {
                    runtime_instance_id,
                    handlers,
                    bindings: mcp_bindings.clone(),
                    host_services: options.mcp_host_services.clone(),
                }),
            );
        }
        let sequences = Rc::new(RefCell::new(HashMap::new()));
        let retained = Rc::new(RefCell::new(HashMap::new()));
        let turns = Rc::new(RefCell::new(HashMap::new()));
        let turn_usages = Rc::new(RefCell::new(HashMap::new()));
        let replay = Rc::new(RefCell::new(HashMap::new()));
        let client = Client {
            events: events.clone(),
            sequences: sequences.clone(),
            retained: retained.clone(),
            capacity: options.event_journal_capacity,
            host: options.host.clone(),
            tool_permission_handler: if options.profile == crate::RuntimeProfile::Desktop {
                options.tool_permission_handler.clone()
            } else {
                None
            },
            host_extension_methods: options
                .host_capabilities
                .extension_methods
                .iter()
                .cloned()
                .collect(),
            agent_hooks: if options.profile == crate::RuntimeProfile::Desktop {
                options
                    .agent_hooks
                    .iter()
                    .map(|hook| (hook.callback_id.clone(), hook.handler.clone()))
                    .collect()
            } else {
                HashMap::new()
            },
            turns: turns.clone(),
            turn_usages: turn_usages.clone(),
            replay: replay.clone(),
        };
        tokio::task::spawn_local(
            AcpGatewayReceiver::new(gw_rx, client)
                .with_on_meta(xai_file_utils::trace_context::span_from_meta_traceparent)
                .run(),
        );
        // Advertising ACP I/O prevents the shell from falling back to direct
        // process-local filesystem and terminal implementations. Restricted
        // intentionally advertises those routes without a delegate so every
        // operation fails closed; Desktop either uses the host-advertised
        // routes or deliberately retains the native local implementations.
        let restricted_io = options.profile == crate::RuntimeProfile::Restricted;
        let client_caps = acp::ClientCapabilities::new()
            .fs(acp::FileSystemCapabilities::new()
                .read_text_file(restricted_io || options.host_capabilities.fs_read)
                .write_text_file(restricted_io || options.host_capabilities.fs_write))
            .terminal(restricted_io || options.host_capabilities.terminal)
            .meta(client_capability_meta(&options)?);
        agent
            .initialize(
                acp::InitializeRequest::new(acp::ProtocolVersion::V1)
                    .client_capabilities(client_caps),
            )
            .await
            .map_err(|error| protocol("initialize", error))?;
        let capabilities = capabilities_for(&options);
        Ok((
            Self {
                agent,
                session_state_authority,
                session_state_store,
                session_leases: RefCell::new(HashMap::new()),
                events,
                catalog: input
                    .models
                    .into_iter()
                    .map(|model| (model.id.clone(), model))
                    .collect(),
                sequences,
                retained,
                capacity: options.event_journal_capacity,
                options,
                resident: RefCell::new(HashSet::new()),
                session_bindings: RefCell::new(HashMap::new()),
                mcp_bindings,
                turns,
                turn_usages,
                prompt_tasks: RefCell::new(HashMap::new()),
                replay,
                evidence_store,
                evidence_versions: RefCell::new(HashMap::new()),
            },
            capabilities,
        ))
    }
    async fn run(self: Rc<Self>, mut rx: mpsc::UnboundedReceiver<Command>) {
        while let Some(c) = rx.recv().await {
            match c {
                Command::Create(x, harness_digest, r) => {
                    let _ = r.send(self.create(x, harness_digest).await);
                }
                Command::Ensure(id, config, r) => {
                    let _ = r.send(self.ensure(id, config).await);
                }
                Command::Load(i, x, harness_digest, r) => {
                    let _ = r.send(self.load(i, x, harness_digest).await);
                }
                Command::Resume(i, x, harness_digest, r) => {
                    let _ = r.send(self.resume(i, x, harness_digest).await);
                }
                Command::Prompt(i, t, x, r) => {
                    if t.trim().is_empty() {
                        let _ = r.send(Err(Error::InvalidConfig("turn id is required".into())));
                        continue;
                    }
                    if self.turns.borrow().contains_key(&i.0) {
                        let _ = r.send(Err(Error::Operation(
                            "session already has an active prompt".into(),
                        )));
                        continue;
                    }
                    self.turns.borrow_mut().insert(i.0.clone(), t.clone());
                    let this = self.clone();
                    let task_key = i.0.clone();
                    let task_session_id = task_key.clone();
                    let reservation = TurnReservation {
                        turns: self.turns.clone(),
                        turn_usages: self.turn_usages.clone(),
                        session_id: task_session_id.clone(),
                        turn_id: t.clone(),
                    };
                    let task = tokio::task::spawn_local(async move {
                        let _reservation = reservation;
                        let result = this.prompt(i, t, x).await;
                        this.prompt_tasks.borrow_mut().remove(&task_session_id);
                        let _ = r.send(result);
                    });
                    self.prompt_tasks
                        .borrow_mut()
                        .insert(task_key, task.abort_handle());
                }
                Command::PromptContent(i, t, x, r) => {
                    if t.trim().is_empty() {
                        let _ = r.send(Err(Error::InvalidConfig("turn id is required".into())));
                        continue;
                    }
                    if self.turns.borrow().contains_key(&i.0) {
                        let _ = r.send(Err(Error::Operation(
                            "session already has an active prompt".into(),
                        )));
                        continue;
                    }
                    self.turns.borrow_mut().insert(i.0.clone(), t.clone());
                    let this = self.clone();
                    let task_key = i.0.clone();
                    let task_session_id = task_key.clone();
                    let reservation = TurnReservation {
                        turns: self.turns.clone(),
                        turn_usages: self.turn_usages.clone(),
                        session_id: task_session_id.clone(),
                        turn_id: t.clone(),
                    };
                    let task = tokio::task::spawn_local(async move {
                        let _reservation = reservation;
                        let result = this.prompt_content(i, t, x).await;
                        this.prompt_tasks.borrow_mut().remove(&task_session_id);
                        let _ = r.send(result);
                    });
                    self.prompt_tasks
                        .borrow_mut()
                        .insert(task_key, task.abort_handle());
                }
                Command::PromptBound(i, t, prompt, harness_digest, reply) => {
                    if t.trim().is_empty() {
                        let _ = reply.send(Err(Error::InvalidConfig("turn id is required".into())));
                        continue;
                    }
                    if self.turns.borrow().contains_key(&i.0) {
                        let _ = reply.send(Err(Error::Operation(
                            "session already has an active prompt".into(),
                        )));
                        continue;
                    }
                    let prepared = match self.prepare_harness_turn(&i, &prompt, &harness_digest) {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                    };
                    self.turns.borrow_mut().insert(i.0.clone(), t.clone());
                    let this = self.clone();
                    let task_key = i.0.clone();
                    let task_session_id = task_key.clone();
                    let reservation = TurnReservation {
                        turns: self.turns.clone(),
                        turn_usages: self.turn_usages.clone(),
                        session_id: task_session_id.clone(),
                        turn_id: t.clone(),
                    };
                    let task = tokio::task::spawn_local(async move {
                        let _reservation = reservation;
                        let result = this
                            .prompt_content_with_harness(i, t, prompt, prepared)
                            .await;
                        this.prompt_tasks.borrow_mut().remove(&task_session_id);
                        let _ = reply.send(result);
                    });
                    self.prompt_tasks
                        .borrow_mut()
                        .insert(task_key, task.abort_handle());
                }
                Command::ListModels(r) => {
                    let _ = r.send(self.list_models().await);
                }
                Command::Extension(x, r) => {
                    let _ = r.send(self.extension_raw(x).await);
                }
                Command::Fork(target, request, reply) => {
                    let _ = reply.send(self.fork(target, request).await);
                }
                Command::ExtensionNotification(x, r) => {
                    let _ = r.send(self.extension_notification(x).await);
                }
                Command::McpModern(id, server, operation, reply) => {
                    let result = if self.options.profile == crate::RuntimeProfile::Restricted {
                        Err(Error::Operation(
                            "MCP operations require the Desktop profile".into(),
                        ))
                    } else {
                        self.agent
                            .sdk_mcp_modern_operation(id.as_str(), server, operation)
                            .await
                            .map_err(Error::Operation)
                    };
                    let _ = reply.send(result);
                }
                Command::McpSubscribe(id, server, filter, capacity, reply) => {
                    let result = if self.options.profile == crate::RuntimeProfile::Restricted {
                        Err(Error::Operation(
                            "MCP operations require the Desktop profile".into(),
                        ))
                    } else {
                        self.agent
                            .sdk_mcp_modern_subscribe(id.as_str(), server, filter, capacity)
                            .await
                            .map_err(Error::Operation)
                    };
                    let _ = reply.send(result);
                }
                Command::ReplaceMcp(id, servers, r) => {
                    let result = if self.options.profile == crate::RuntimeProfile::Restricted {
                        Err(Error::Operation(
                            "MCP operations require the Desktop profile".into(),
                        ))
                    } else {
                        match validate_mcp_servers(&servers) {
                            Err(error) => Err(error),
                            Ok(())
                                if servers.iter().any(|server| {
                                    let name = match server {
                                        crate::McpServerConfig::Stdio { name, .. }
                                        | crate::McpServerConfig::Http { name, .. }
                                        | crate::McpServerConfig::Sse { name, .. } => name,
                                    };
                                    self.options
                                        .in_process_mcp_servers
                                        .iter()
                                        .any(|embedded| embedded.name == *name)
                                }) =>
                            {
                                Err(Error::InvalidConfig(
                                    "external MCP replacement collides with an in-process server"
                                        .into(),
                                ))
                            }
                            Ok(()) => {
                                let mcp_servers: Vec<acp::McpServer> =
                                    servers.iter().map(to_acp_mcp_server).collect();
                                self.extension::<serde_json::Value>(
                                    "x.ai/session/update_mcp_servers",
                                    serde_json::json!({"sessionId":id.as_str(),"mcpServers":mcp_servers}),
                                )
                                .await
                                .map(drop)
                            }
                        }
                    };
                    let _ = r.send(result);
                }
                Command::SetMode(i, mode, r) => {
                    let _ = r.send(self.set_mode(i, mode).await);
                }
                Command::ListSessions(r) => {
                    let _ = r.send(self.list_sessions().await);
                }
                Command::Cancel(i, r) => {
                    let _ = r.send(self.cancel(i).await);
                }
                Command::EventsAfter(i, sequence, r) => {
                    let _ = r.send(self.events_after(&i, sequence));
                }
                Command::SessionLedger(i, r) => {
                    let _ = r.send(self.session_ledger(&i));
                }
                Command::TurnBindingStatus(i, key, r) => {
                    let _ = r.send(self.turn_binding_status(&i, &key));
                }
                Command::MarkTurnDiscarded(i, turn, digest, prompt_index, r) => {
                    let _ = r.send(self.mark_turn_discarded(&i, turn, digest, prompt_index));
                }
                Command::SetRoute(i, model, reasoning, r) => {
                    let _ = r.send(self.set_route(i, model, reasoning).await);
                }
                Command::RewindPoints(i, r) => {
                    let _ = r.send(self.rewind_points(i).await);
                }
                Command::Rewind(i, operation_id, target, r) => {
                    let _ = r.send(self.rewind_conversation(i, operation_id, target).await);
                }
                Command::RewindUnsettled(
                    i,
                    operation_id,
                    turn_id,
                    prompt_digest,
                    target,
                    reply,
                ) => {
                    let _ = reply.send(
                        self.rewind_conversation_entry(
                            i,
                            operation_id,
                            target,
                            Some((turn_id, prompt_digest)),
                        )
                        .await,
                    );
                }
                Command::RewindStatus(id, operation_id, r) => {
                    let _ = r.send(self.rewind_status(&id, &operation_id));
                }
                Command::Close(i, r) => {
                    let _ = r.send(self.close(i).await);
                }
                Command::Delete(i, r) => {
                    let _ = r.send(self.delete(i).await);
                }
                Command::Unload(i, r) => {
                    let _ = r.send(self.unload(i).await);
                }
                Command::Shutdown(r) => {
                    let mut failures = Vec::new();
                    let active = self.turns.borrow().keys().cloned().collect::<Vec<_>>();
                    for id in active {
                        if let Err(error) = self.cancel(SessionId(id.clone())).await {
                            failures.push(format!("cancel {id}: {error}"));
                        }
                    }
                    let resident = self.resident.borrow().iter().cloned().collect::<Vec<_>>();
                    for id in resident {
                        if let Err(error) = self.unload(SessionId(id.clone())).await {
                            failures.push(format!("unload {id}: {error}"));
                        }
                    }
                    let uncertain_leases = self
                        .session_leases
                        .borrow_mut()
                        .drain()
                        .map(|(_, lease)| lease)
                        .collect();
                    quarantine_session_leases(uncertain_leases);
                    let result = if failures.is_empty() {
                        Ok(())
                    } else {
                        Err(Error::Operation(format!(
                            "native runtime shutdown was incomplete: {}",
                            failures.join("; ")
                        )))
                    };
                    let _ = r.send(result);
                    break;
                }
            }
        }
    }
    fn check_model(&self, model_id: &str, reasoning: Option<&str>) -> Result<(), Error> {
        let model = self.catalog.get(model_id).ok_or_else(|| {
            Error::InvalidConfig(format!("model '{}' is not in the fixed catalog", model_id))
        })?;
        if let Some(reasoning) = reasoning
            && (!model.supports_reasoning
                || !model
                    .reasoning_options
                    .iter()
                    .any(|option| option == reasoning))
        {
            return Err(Error::InvalidConfig(format!(
                "reasoning effort '{reasoning}' is not available for model '{}'",
                model_id
            )));
        }
        Ok(())
    }
    fn effective_reasoning(
        &self,
        model_id: &str,
        reasoning: Option<&str>,
    ) -> Result<Option<String>, Error> {
        self.check_model(model_id, reasoning)?;
        Ok(reasoning.map(str::to_owned).or_else(|| {
            self.catalog
                .get(model_id)
                .and_then(|model| model.default_reasoning.clone())
        }))
    }
    fn check(&self, config: &SessionConfig) -> Result<(), Error> {
        self.check_model(&config.model, config.reasoning.as_deref())?;
        if !config.cwd.is_absolute() || !config.cwd.is_dir() {
            return Err(Error::InvalidConfig(
                "session cwd must be an existing absolute directory".into(),
            ));
        }
        for (name, value) in [
            ("system prompt", config.system_prompt.as_deref()),
            ("rules", config.rules.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                return Err(Error::InvalidConfig(format!(
                    "session {name} must not be blank"
                )));
            }
        }
        Ok(())
    }
    fn session_meta(
        &self,
        config: &SessionConfig,
        effective_reasoning: Option<&str>,
    ) -> Result<SessionMeta, Error> {
        let mut meta = serde_json::json!({
            "modelId": config.model,
            "reasoningEffort": effective_reasoning,
            "clientIdentifier": self.options.client_identifier,
            "yoloMode": self.options.yolo_mode,
        })
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Operation("failed to build session metadata".into()))?;
        if let Some(system_prompt) = &config.system_prompt {
            meta.insert(
                "systemPromptOverride".into(),
                serde_json::Value::String(system_prompt.clone()),
            );
        }
        if let Some(rules) = &config.rules {
            meta.insert("rules".into(), serde_json::Value::String(rules.clone()));
        }
        if self.options.profile == crate::RuntimeProfile::Desktop
            && !self.options.agent_hooks.is_empty()
        {
            let mut groups = serde_json::Map::new();
            for hook in &self.options.agent_hooks {
                groups
                    .entry(hook.event.registration_name())
                    .or_insert_with(|| serde_json::Value::Array(Vec::new()))
                    .as_array_mut()
                    .expect("hook groups are arrays")
                    .push(serde_json::json!({
                        "matcher": hook.matcher,
                        "hookCallbackIds": [hook.callback_id.clone()],
                        "timeout": hook.timeout,
                    }));
            }
            meta.insert("x.ai/hooks".into(), serde_json::Value::Object(groups));
        }
        Ok(meta)
    }

    async fn apply_native_route(
        &self,
        id: &SessionId,
        model: &str,
        effective_reasoning: Option<&str>,
    ) -> Result<(), Error> {
        let meta = serde_json::json!({
            "reasoningEffort": effective_reasoning,
            "originRouteOnly": true,
        })
        .as_object()
        .cloned();
        self.agent
            .set_session_model(
                acp::SetSessionModelRequest::new(
                    acp::SessionId::new(id.0.clone()),
                    acp::ModelId::new(model.to_owned()),
                )
                .meta(meta),
            )
            .await
            .map(|_| ())
            .map_err(|error| protocol("session/set_model", error))
    }

    fn mcp_servers(&self) -> Vec<acp::McpServer> {
        if self.options.profile == crate::RuntimeProfile::Restricted {
            return Vec::new();
        }
        self.options
            .services
            .mcp_servers
            .iter()
            .map(|server| match server {
                crate::McpServerConfig::Stdio {
                    name,
                    command,
                    args,
                    env,
                } => acp::McpServer::Stdio(
                    acp::McpServerStdio::new(name, command)
                        .args(args.clone())
                        .env(
                            env.iter()
                                .map(|(name, value)| acp::EnvVariable::new(name, value))
                                .collect(),
                        ),
                ),
                crate::McpServerConfig::Http { name, url, headers } => acp::McpServer::Http(
                    acp::McpServerHttp::new(name, url).headers(
                        headers
                            .iter()
                            .map(|(name, value)| acp::HttpHeader::new(name, value))
                            .collect(),
                    ),
                ),
                crate::McpServerConfig::Sse { name, url, headers } => acp::McpServer::Sse(
                    acp::McpServerSse::new(name, url).headers(
                        headers
                            .iter()
                            .map(|(name, value)| acp::HttpHeader::new(name, value))
                            .collect(),
                    ),
                ),
            })
            .collect()
    }
    fn emit(&self, id: &SessionId, u: EventUpdate, t: Option<String>) {
        let event = self.retain_event(id, u, t);
        self.publish_event(event);
    }

    fn retain_event(&self, id: &SessionId, u: EventUpdate, t: Option<String>) -> Event {
        let mut s = self.sequences.borrow_mut();
        let n = s.entry(id.0.clone()).or_default();
        *n += 1;
        let event = Event {
            session_id: id.clone(),
            sequence: *n,
            turn_id: t,
            timestamp_ms: now_ms(),
            replay: false,
            update: u,
        };
        let mut journal = self.retained.borrow_mut();
        let retained = journal.entry(id.0.clone()).or_default();
        retained.push_back(event.clone());
        while retained.len() > self.capacity {
            retained.pop_front();
        }
        event
    }

    fn publish_event(&self, event: Event) {
        let _ = self.events.send(event);
    }

    async fn detach_unregistered_session(&self, id: &SessionId) -> Result<(), Error> {
        let response = self
            .extension::<UnloadWire>(
                "origin/session/unload",
                serde_json::json!({"sessionId": id.0}),
            )
            .await?;
        if response.success && response.drained {
            Ok(())
        } else {
            Err(Error::Operation(
                "native session cleanup did not fully drain the actor".into(),
            ))
        }
    }

    async fn create(
        &self,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
    ) -> Result<SessionId, Error> {
        self.check(&config)?;
        if self.session_state_store.is_some() {
            let id = SessionId(uuid::Uuid::now_v7().to_string());
            let generation = uuid::Uuid::now_v7().to_string();
            let lease = self.acquire_session_lease(&id)?;
            self.create_inner(config, harness_digest, Some((id, generation)), lease)
                .await
        } else {
            self.create_inner(config, harness_digest, None, None).await
        }
    }

    fn acquire_session_lease(
        &self,
        id: &SessionId,
    ) -> Result<Option<Box<dyn crate::SessionStateLease>>, Error> {
        self.session_state_store
            .as_ref()
            .map(|store| {
                let key = crate::SessionKey::new(id.as_str()).map_err(op)?;
                store.acquire_session_lease(&key).map_err(op)
            })
            .transpose()
    }

    async fn ensure(&self, id: SessionId, config: SessionConfig) -> Result<SessionId, Error> {
        use sha2::{Digest as _, Sha256};
        self.check(&config)?;
        uuid::Uuid::try_parse(id.as_str()).map_err(|error| {
            Error::InvalidConfig(format!(
                "caller-selected session id must be a UUID: {error}"
            ))
        })?;
        let already_resident = self.resident.borrow().contains(id.as_str());
        let lease = if already_resident {
            None
        } else {
            self.acquire_session_lease(&id)?
        };
        let authority = self.session_state_authority.as_ref().ok_or_else(|| {
            Error::InvalidConfig(
                "create_session_with_id requires a Host session state authority".into(),
            )
        })?;
        let exact = serde_json::to_vec(&config).map_err(op)?;
        let generation = format!("config-sha256:{:x}", Sha256::digest(exact));
        match authority.inspect(id.as_str()).map_err(op)? {
            xai_grok_shell::session::state_authority::SessionInspection::Vacant => {
                self.create_inner(config, None, Some((id, generation)), lease)
                    .await
            }
            xai_grok_shell::session::state_authority::SessionInspection::Live {
                generation: current,
            } if current == generation => {
                if !already_resident {
                    self.attach_with_lease(id.clone(), config, None, false, lease)
                        .await?;
                }
                Ok(id)
            }
            xai_grok_shell::session::state_authority::SessionInspection::Live { .. } => {
                Err(Error::InvalidConfig(
                    "session identity already exists with different config".into(),
                ))
            }
            xai_grok_shell::session::state_authority::SessionInspection::Tombstoned { .. } => Err(
                Error::InvalidConfig("session identity is permanently tombstoned".into()),
            ),
        }
    }

    async fn create_inner(
        &self,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        requested: Option<(SessionId, String)>,
        lease: Option<Box<dyn crate::SessionStateLease>>,
    ) -> Result<SessionId, Error> {
        self.check(&config)?;
        let lease_id = requested.as_ref().map(|(id, _)| id).cloned();
        let _lease_admission = lease_id
            .as_ref()
            .map(|id| SessionLeaseAdmission::new(&self.session_leases, id, lease));
        let effective_reasoning =
            self.effective_reasoning(&config.model, config.reasoning.as_deref())?;
        let binding = SessionBinding::new(&config, effective_reasoning.clone(), harness_digest);
        let mut meta = self.session_meta(&config, effective_reasoning.as_deref())?;
        if let Some((id, generation)) = &requested {
            meta.insert("sessionId".into(), serde_json::Value::String(id.0.clone()));
            meta.insert(
                "sessionStateGeneration".into(),
                serde_json::Value::String(generation.clone()),
            );
        }
        let x = self
            .agent
            .new_session(
                acp::NewSessionRequest::new(config.cwd.clone())
                    .mcp_servers(self.mcp_servers())
                    .meta(meta),
            )
            .await
            .map_err(|error| protocol("session/new", error))?;
        let id = SessionId(x.session_id.0.to_string());
        // `session/new` selects the catalog model but historically does not
        // consume its reasoning override. Apply the same normalized route
        // before exposing the Session so native sampling and receipts agree.
        if let Err(error) = self
            .apply_native_route(&id, &config.model, effective_reasoning.as_deref())
            .await
        {
            return match self.detach_unregistered_session(&id).await {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(Error::Operation(format!(
                    "{error}; native session cleanup failed: {cleanup_error}"
                ))),
            };
        }
        let active_guard = ActiveMcpBindingGuard::new(self.mcp_bindings.clone(), id.0.clone());
        if let Err(error) = self.save_ledger(&id, &SessionLedger::default()) {
            return match self.detach_unregistered_session(&id).await {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(Error::Operation(format!(
                    "{error}; native session cleanup failed: {cleanup_error}"
                ))),
            };
        }
        if !xai_grok_shell::origin_runtime::register_root_session(&id.0) {
            let cleanup = self.detach_unregistered_session(&id).await;
            let mut detail =
                "native session identity collided with an existing embedded root".to_owned();
            if let Err(error) = cleanup {
                detail.push_str(&format!("; native session cleanup failed: {error}"));
            }
            return Err(Error::Operation(detail));
        }
        self.resident.borrow_mut().insert(id.0.clone());
        self.session_bindings
            .borrow_mut()
            .insert(id.0.clone(), binding);
        active_guard.commit();
        self.emit(&id, EventUpdate::SessionStarted, None);
        Ok(id)
    }
    async fn load(
        &self,
        id: SessionId,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
    ) -> Result<(), Error> {
        self.attach(id, config, harness_digest, false).await
    }

    async fn resume(
        &self,
        id: SessionId,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
    ) -> Result<(), Error> {
        self.attach(id, config, harness_digest, true).await
    }

    async fn attach(
        &self,
        id: SessionId,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        resume: bool,
    ) -> Result<(), Error> {
        self.check(&config)?;
        let lease = self.acquire_session_lease(&id)?;
        self.attach_with_lease(id, config, harness_digest, resume, lease)
            .await
    }

    async fn attach_with_lease(
        &self,
        id: SessionId,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        resume: bool,
        lease: Option<Box<dyn crate::SessionStateLease>>,
    ) -> Result<(), Error> {
        self.check(&config)?;
        let _lease_admission = SessionLeaseAdmission::new(&self.session_leases, &id, lease);
        if self.resident.borrow().contains(&id.0) {
            return Err(Error::Operation("session is already resident".into()));
        }
        self.load_ledger(&id)?;
        let effective_reasoning =
            self.effective_reasoning(&config.model, config.reasoning.as_deref())?;
        let binding = SessionBinding::new(&config, effective_reasoning.clone(), harness_digest);
        let active_guard = ActiveMcpBindingGuard::new(self.mcp_bindings.clone(), id.0.clone());
        let meta = self.session_meta(&config, effective_reasoning.as_deref())?;
        struct ReplayGuard<'a>(&'a RefCell<HashMap<String, ReplayMode>>, String);
        impl Drop for ReplayGuard<'_> {
            fn drop(&mut self) {
                self.0.borrow_mut().remove(&self.1);
            }
        }
        let capture_replay = !resume && !self.sequences.borrow().contains_key(&id.0);
        self.replay.borrow_mut().insert(
            id.0.clone(),
            if capture_replay {
                ReplayMode::Capture
            } else {
                ReplayMode::Suppress
            },
        );
        let _guard = ReplayGuard(&self.replay, id.0.clone());
        if resume {
            self.agent
                .resume_session(
                    acp::ResumeSessionRequest::new(acp::SessionId::new(id.0.clone()), config.cwd)
                        .mcp_servers(self.mcp_servers())
                        .meta(meta),
                )
                .await
                .map_err(|error| protocol("session/resume", error))?;
        } else {
            self.agent
                .load_session(
                    acp::LoadSessionRequest::new(acp::SessionId::new(id.0.clone()), config.cwd)
                        .mcp_servers(self.mcp_servers())
                        .meta(meta),
                )
                .await
                .map_err(|error| protocol("session/load", error))?;
        }
        if !xai_grok_shell::origin_runtime::register_root_session(&id.0) {
            return match self.detach_unregistered_session(&id).await {
                Ok(()) => Err(Error::Operation(
                    "loaded session identity collided with an existing embedded root".into(),
                )),
                Err(cleanup_error) => Err(Error::Operation(format!(
                    "loaded session identity collided with an existing embedded root; native session cleanup failed: {cleanup_error}"
                ))),
            };
        }
        self.resident.borrow_mut().insert(id.0.clone());
        self.session_bindings
            .borrow_mut()
            .insert(id.0.clone(), binding);
        active_guard.commit();
        Ok(())
    }
    async fn prompt(&self, id: SessionId, t: String, x: String) -> Result<PromptReceipt, Error> {
        let digest = crate::prompt_digest(&x);
        self.prompt_wire(
            id,
            t,
            vec![acp::ContentBlock::Text(acp::TextContent::new(x))],
            digest,
            serde_json::Value::Null,
            None,
        )
        .await
        .map(|(receipt, _)| receipt)
    }
    async fn prompt_content(
        &self,
        id: SessionId,
        t: String,
        prompt: Prompt,
    ) -> Result<PromptReceipt, Error> {
        if prompt.blocks.is_empty() {
            return Err(Error::InvalidConfig(
                "prompt blocks must not be empty".into(),
            ));
        }
        let mut blocks = Vec::new();
        for block in &prompt.blocks {
            let value = prompt_block_wire(block)?;
            blocks.push(serde_json::from_value(value).map_err(op)?);
        }
        let digest = crate::prompt_digest_content(&prompt)?;
        self.prompt_wire(id, t, blocks, digest, prompt.metadata, None)
            .await
            .map(|(receipt, _)| receipt)
    }
    async fn prompt_content_with_harness(
        &self,
        id: SessionId,
        turn_id: String,
        prompt: Prompt,
        prepared: PreparedHarnessTurn,
    ) -> Result<TurnBindingReceipt, Error> {
        if prompt.blocks.is_empty() {
            return Err(Error::InvalidConfig(
                "prompt blocks must not be empty".into(),
            ));
        }
        let mut blocks = Vec::new();
        for block in &prompt.blocks {
            let value = prompt_block_wire(block)?;
            blocks.push(serde_json::from_value(value).map_err(op)?);
        }
        let prompt_digest = crate::prompt_digest_content(&prompt)?;
        if prompt_digest != prepared.prompt_digest {
            return Err(Error::Operation(
                "prepared harness Turn prompt identity changed before dispatch".into(),
            ));
        }
        let (_, record) = self
            .prompt_wire(
                id,
                turn_id,
                blocks,
                prompt_digest,
                prompt.metadata,
                Some(prepared),
            )
            .await?;
        record
            .map(TurnBindingRecord::into_receipt)
            .ok_or_else(|| Error::Operation("durable Turn binding record was not issued".into()))
    }

    fn prepare_harness_turn(
        &self,
        id: &SessionId,
        prompt: &Prompt,
        requested_digest: &HarnessDigest,
    ) -> Result<PreparedHarnessTurn, Error> {
        self.require_resident(id)?;
        let binding = self
            .session_bindings
            .borrow()
            .get(&id.0)
            .cloned()
            .ok_or_else(|| Error::Operation("session binding is unavailable".into()))?;
        let bound_digest = binding
            .harness_digest
            .ok_or_else(|| Error::Harness(HarnessError::UnboundSession))?;
        if &bound_digest != requested_digest {
            return Err(Error::Harness(HarnessError::BindingMismatch {
                bound: bound_digest,
                requested: requested_digest.clone(),
            }));
        }
        let after_sequence = self
            .sequences
            .borrow()
            .get(&id.0)
            .copied()
            .unwrap_or_default();
        let prompt_digest = crate::prompt_digest_content(prompt)?;
        Ok(PreparedHarnessTurn {
            prompt_digest,
            snapshot_digest: bound_digest,
            model: binding.model,
            reasoning: binding.reasoning,
            after_sequence,
        })
    }
    async fn prompt_wire(
        &self,
        id: SessionId,
        t: String,
        blocks: Vec<acp::ContentBlock>,
        prompt_digest: String,
        metadata: serde_json::Value,
        prepared: Option<PreparedHarnessTurn>,
    ) -> Result<(PromptReceipt, Option<TurnBindingRecord>), Error> {
        self.require_resident(&id)?;
        let mut ledger = self.load_ledger(&id)?;
        if ledger
            .entries
            .iter()
            .any(|entry| matches!(entry.state, LedgerTurnState::Pending))
        {
            return Err(Error::Operation(
                "session has an unreconciled native Turn".into(),
            ));
        }
        if ledger.entries.iter().any(|entry| entry.turn_id == t) {
            return Err(Error::Operation("native Turn id was already used".into()));
        }
        let runtime_prompt_index = ledger
            .entries
            .iter()
            .filter(|entry| !matches!(entry.state, LedgerTurnState::Discarded))
            .count() as u64;
        ledger.entries.push(SessionLedgerEntry {
            turn_id: t.clone(),
            prompt_digest: prompt_digest.clone(),
            runtime_prompt_index,
            state: LedgerTurnState::Pending,
        });
        self.save_ledger(&id, &ledger)?;
        let usage_key = (id.0.clone(), t.clone());
        self.turn_usages.borrow_mut().remove(&usage_key);
        let req = acp::PromptRequest::new(acp::SessionId::new(id.0.clone()), blocks).meta(
            serde_json::json!({
                "originTurnId":t,
                "promptId":t,
                "originPromptDigest": prompt_digest,
                "originMetadata": metadata
            })
            .as_object()
            .cloned(),
        );
        let started = std::time::Instant::now();
        let response = self
            .agent
            .prompt(req)
            .await
            .map_err(|error| protocol("session/prompt", error));
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.turn_usages.borrow_mut().remove(&usage_key);
                return Err(error);
            }
        };
        let outcome = match response.stop_reason {
            acp::StopReason::EndTurn => TurnOutcome::End,
            acp::StopReason::Cancelled => TurnOutcome::Cancelled,
            acp::StopReason::MaxTokens => TurnOutcome::MaxTokens,
            acp::StopReason::MaxTurnRequests => TurnOutcome::MaxTurnRequests,
            acp::StopReason::Refusal => TurnOutcome::Refusal,
            _ => {
                self.turn_usages.borrow_mut().remove(&usage_key);
                return Err(Error::Operation("unrecognized Grok stop reason".into()));
            }
        };
        let raw = serde_json::value::RawValue::from_string(
            serde_json::json!({"sessionId": id.0}).to_string(),
        )
        .map_err(op)?;
        if let Err(error) = self
            .agent
            .ext_method(acp::ExtRequest::new("origin/session/sync", Arc::from(raw)))
            .await
            .map_err(|error| protocol("origin/session/sync", error))
        {
            self.turn_usages.borrow_mut().remove(&usage_key);
            return Err(error);
        }
        let wall_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let native_usage = match self.turn_usages.borrow_mut().remove(&usage_key) {
            Some(CapturedTurnUsage::Exact(usage)) => usage,
            Some(CapturedTurnUsage::Conflict) | None => None,
        };
        let usage = prompt_effect_usage(native_usage.as_ref(), wall_ms);
        let settlement_id = ledger_settlement_id(
            &id.0,
            &t,
            &prompt_digest,
            runtime_prompt_index,
            outcome,
            &usage,
        )?;
        let terminal = self.retain_event(&id, EventUpdate::TurnFinished(outcome), Some(t.clone()));
        let receipt = PromptReceipt {
            outcome,
            final_sequence: terminal.sequence,
            runtime_prompt_index,
            settlement_id,
            usage,
        };
        let Some(prepared) = prepared else {
            settle_latest_ledger_entry(&mut ledger, &receipt);
            self.save_ledger(&id, &ledger)?;
            self.publish_event(terminal);
            return Ok((receipt, None));
        };

        let record = self
            .events_after(&id, prepared.after_sequence)
            .and_then(|events| {
                let binding = TurnBindingReceipt::complete(
                    id.clone(),
                    t,
                    prepared.prompt_digest,
                    prepared.snapshot_digest,
                    prepared.model,
                    prepared.reasoning,
                    prepared.after_sequence,
                    receipt.clone(),
                    &events,
                )
                .map_err(Error::Harness)?;
                TurnBindingRecord::complete(binding, &events).map_err(Error::Harness)
            });
        let record = match record {
            Ok(record) => record,
            Err(error) => {
                settle_latest_ledger_entry(&mut ledger, &receipt);
                self.save_ledger(&id, &ledger)?;
                self.publish_event(terminal);
                return Err(error);
            }
        };
        if let Err(error) = self.save_turn_binding_record(&record) {
            self.publish_event(terminal);
            return Err(error);
        }
        settle_latest_ledger_entry(&mut ledger, &receipt);
        self.save_ledger(&id, &ledger)?;
        self.publish_event(terminal);
        Ok((receipt, Some(record)))
    }

    fn session_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        self.require_resident(id)?;
        self.load_ledger(id)
    }

    fn mark_turn_discarded(
        &self,
        id: &SessionId,
        turn_id: String,
        prompt_digest: String,
        runtime_prompt_index: u64,
    ) -> Result<(), Error> {
        self.require_resident(id)?;
        if self.turns.borrow().contains_key(&id.0) {
            return Err(Error::Operation(
                "cannot discard a Turn while the session is active".into(),
            ));
        }
        let mut ledger = self.load_ledger(id)?;
        if let Some(position) = ledger
            .entries
            .iter()
            .position(|entry| entry.turn_id == turn_id)
        {
            let entry = &ledger.entries[position];
            if entry.prompt_digest != prompt_digest
                || entry.runtime_prompt_index != runtime_prompt_index
            {
                return Err(Error::Operation(
                    "discarded Turn identity does not match the native ledger".into(),
                ));
            }
            if ledger.entries[position + 1..]
                .iter()
                .any(|entry| !matches!(entry.state, LedgerTurnState::Discarded))
            {
                return Err(Error::Operation(
                    "native Turn history can only be discarded from the end".into(),
                ));
            }
            ledger.entries[position].state = LedgerTurnState::Discarded;
        } else {
            let expected_index = ledger
                .entries
                .iter()
                .filter(|entry| !matches!(entry.state, LedgerTurnState::Discarded))
                .count() as u64;
            if runtime_prompt_index != expected_index {
                return Err(Error::Operation(
                    "discarded Turn index does not follow the native ledger".into(),
                ));
            }
            ledger.entries.push(SessionLedgerEntry {
                turn_id,
                prompt_digest,
                runtime_prompt_index,
                state: LedgerTurnState::Discarded,
            });
        }
        self.save_ledger(id, &ledger)
    }

    fn evidence_key(kind: SessionEvidenceKind, identity: String) -> SessionEvidenceKey {
        SessionEvidenceKey { kind, identity }
    }

    fn load_evidence(
        &self,
        key: &SessionEvidenceKey,
        max: usize,
    ) -> Result<Option<Vec<u8>>, Error> {
        let document = self.evidence_store.load(key).map_err(op)?;
        if let Some(SessionEvidenceDocument { version, bytes }) = document {
            if bytes.len() > max {
                return Err(Error::Operation(
                    "session evidence exceeds its bounded schema size".into(),
                ));
            }
            if !version.validates(&bytes) {
                return Err(Error::Operation(
                    "session evidence CAS digest or revision is invalid".into(),
                ));
            }
            self.evidence_versions
                .borrow_mut()
                .insert(key.clone(), version);
            Ok(Some(bytes))
        } else {
            self.evidence_versions.borrow_mut().remove(key);
            Ok(None)
        }
    }

    fn commit_evidence(&self, key: &SessionEvidenceKey, bytes: &[u8]) -> Result<(), Error> {
        let expected = self.evidence_versions.borrow().get(key).cloned();
        let required = SessionEvidenceVersion::successor(expected.as_ref(), bytes).map_err(op)?;
        match self
            .evidence_store
            .compare_and_swap(key, expected.as_ref(), bytes)
            .map_err(op)?
        {
            SessionEvidenceCommit::Committed(version) if version == required => {
                self.evidence_versions
                    .borrow_mut()
                    .insert(key.clone(), version);
                Ok(())
            }
            SessionEvidenceCommit::Committed(_) => Err(Error::Operation(
                "session evidence store returned an invalid successor identity".into(),
            )),
            SessionEvidenceCommit::Conflict => Err(Error::Operation(
                "session evidence CAS conflict; reconciliation is required".into(),
            )),
            SessionEvidenceCommit::CommitUnknown => Err(Error::Operation(
                "session evidence commit acknowledgement is unknown; reconciliation is required"
                    .into(),
            )),
        }
    }

    fn load_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        let key = Self::evidence_key(SessionEvidenceKind::Ledger, id.0.clone());
        let bytes = self.load_evidence(&key, 8 * 1024 * 1024)?.ok_or_else(|| {
            Error::Operation("native Turn ledger is unavailable for session reconciliation".into())
        })?;
        let ledger: SessionLedger = serde_json::from_slice(&bytes).map_err(op)?;
        validate_session_ledger(id, &ledger)?;
        Ok(ledger)
    }

    fn save_ledger(&self, id: &SessionId, ledger: &SessionLedger) -> Result<(), Error> {
        validate_session_ledger(id, ledger)?;
        let bytes = serde_json::to_vec(ledger).map_err(op)?;
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(Error::Operation(
                "native Turn ledger exceeds maximum size".into(),
            ));
        }
        self.commit_evidence(
            &Self::evidence_key(SessionEvidenceKind::Ledger, id.0.clone()),
            &bytes,
        )
    }

    fn load_turn_binding_record(
        &self,
        id: &SessionId,
        turn_id: &str,
    ) -> Result<Option<TurnBindingRecord>, Error> {
        let key = Self::evidence_key(
            SessionEvidenceKind::TurnBinding,
            format!("{}\0{turn_id}", id.0),
        );
        self.load_evidence(&key, crate::MAX_TURN_BINDING_RECORD_BYTES)?
            .map(|bytes| TurnBindingRecord::from_json_slice(&bytes).map_err(Error::Harness))
            .transpose()
    }

    fn save_turn_binding_record(&self, record: &TurnBindingRecord) -> Result<(), Error> {
        let receipt = record.receipt();
        let key = Self::evidence_key(
            SessionEvidenceKind::TurnBinding,
            format!("{}\0{}", receipt.session_id().0, receipt.turn_id()),
        );
        if let Some(existing) =
            self.load_turn_binding_record(receipt.session_id(), receipt.turn_id())?
        {
            return if existing == *record {
                Ok(())
            } else {
                Err(Error::Harness(HarnessError::BindingRecordConflict(
                    "an immutable record already exists for this Session and Turn ID".into(),
                )))
            };
        }
        let bytes = record.to_json_vec().map_err(Error::Harness)?;
        self.commit_evidence(&key, &bytes)
    }

    fn turn_binding_status(
        &self,
        id: &SessionId,
        key: &TurnBindingKey,
    ) -> Result<TurnBindingStatus, Error> {
        self.require_resident(id)?;
        let binding = self
            .session_bindings
            .borrow()
            .get(id.as_str())
            .cloned()
            .ok_or_else(|| Error::Operation("session binding is unavailable".into()))?;
        if binding.harness_digest.as_ref() != Some(key.snapshot_digest())
            || binding.model != key.model()
            || binding.reasoning.as_deref() != key.reasoning()
        {
            return Err(Error::Harness(HarnessError::BindingRecordConflict(
                "the resident Session snapshot or effective route differs from the recovery key"
                    .into(),
            )));
        }
        let mut ledger = self.load_ledger(id)?;
        let entry_position = ledger
            .entries
            .iter()
            .position(|entry| entry.turn_id == key.turn_id());
        if let Some(entry) = entry_position.map(|position| &ledger.entries[position])
            && (entry.prompt_digest != key.prompt_digest()
                || entry.runtime_prompt_index != key.runtime_prompt_index())
        {
            return Err(Error::Harness(HarnessError::BindingRecordConflict(
                "the recovery key conflicts with the durable Turn ledger identity".into(),
            )));
        }
        let Some(record) = self.load_turn_binding_record(id, key.turn_id())? else {
            return Ok(TurnBindingStatus::Absent);
        };
        let receipt = record.receipt();
        if entry_position.is_none() || receipt.session_id() != id || !key.matches_receipt(receipt) {
            return Err(Error::Harness(HarnessError::BindingRecordConflict(
                "the durable record does not match the requested Turn binding identity".into(),
            )));
        }
        let position = entry_position.expect("checked present");
        match &ledger.entries[position].state {
            LedgerTurnState::Completed {
                outcome,
                settlement_id,
                usage,
            } => {
                if *outcome != receipt.outcome()
                    || settlement_id != receipt.settlement_id()
                    || usage.as_ref() != Some(receipt.usage())
                {
                    return Err(Error::Harness(HarnessError::BindingRecordConflict(
                        "the durable record conflicts with the completed Turn ledger evidence"
                            .into(),
                    )));
                }
            }
            LedgerTurnState::Pending => {
                ledger.entries[position].state = LedgerTurnState::Completed {
                    outcome: receipt.outcome(),
                    settlement_id: receipt.settlement_id().to_owned(),
                    usage: Some(receipt.usage().clone()),
                };
                self.save_ledger(id, &ledger)?;
            }
            LedgerTurnState::Discarded => {
                return Err(Error::Harness(HarnessError::BindingRecordConflict(
                    "the durable Turn binding belongs to discarded conversation history".into(),
                )));
            }
        }
        Ok(TurnBindingStatus::Complete { record })
    }

    async fn cancel(&self, id: SessionId) -> Result<(), Error> {
        let session_id = id.0.clone();
        let result = self
            .agent
            .cancel(acp::CancelNotification::new(acp::SessionId::new(id.0)))
            .await
            .map_err(|error| protocol("session/cancel", error));
        if let Err(error) = result {
            if let Some(task) = self.prompt_tasks.borrow_mut().remove(&session_id) {
                task.abort();
            }
            self.turns.borrow_mut().remove(&session_id);
            return Err(error);
        }
        if self.turns.borrow().contains_key(&session_id) {
            let drained = tokio::time::timeout(CANCEL_DRAIN_TIMEOUT, async {
                while self.turns.borrow().contains_key(&session_id) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
            if !drained {
                if let Some(task) = self.prompt_tasks.borrow_mut().remove(&session_id) {
                    task.abort();
                }
                self.turns.borrow_mut().remove(&session_id);
                return Err(Error::Operation(
                    "native Grok prompt ignored cancellation and was force-stopped".into(),
                ));
            }
        }
        Ok(())
    }

    async fn extension<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        params: serde_json::Value,
    ) -> Result<T, Error> {
        let raw = serde_json::value::to_raw_value(&params).map_err(op)?;
        let response = self
            .agent
            .ext_method(acp::ExtRequest::new(method, Arc::from(raw)))
            .await
            .map_err(|error| protocol(method, error))?;
        serde_json::from_str(response.0.get()).map_err(op)
    }
    async fn list_models(&self) -> Result<ModelCatalog, Error> {
        #[derive(serde::Deserialize)]
        struct ModelsListResult {
            result: Option<acp::SessionModelState>,
            error: Option<serde_json::Value>,
        }

        let response: ModelsListResult = self
            .extension("x.ai/models/list", serde_json::json!({}))
            .await?;
        if let Some(error) = response.error {
            return Err(Error::Operation(format!("models/list failed: {error}")));
        }
        let state = response
            .result
            .ok_or_else(|| Error::Operation("models/list response missing result".into()))?;
        Ok(ModelCatalog {
            current_model_id: state.current_model_id.0.to_string(),
            available_models: state
                .available_models
                .into_iter()
                .map(|model| AvailableModel {
                    id: model.model_id.0.to_string(),
                    name: model.name,
                    description: model.description,
                    metadata: model.meta,
                })
                .collect(),
            metadata: state.meta,
        })
    }
    async fn extension_raw(&self, request: ExtensionRequest) -> Result<ExtensionResponse, Error> {
        if request.method.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "extension request method is required".into(),
            ));
        }
        if self.options.profile == crate::RuntimeProfile::Restricted {
            return Err(Error::Operation(
                "generic extension requests require the Desktop profile".into(),
            ));
        }
        if matches!(
            request.method.as_str(),
            "x.ai/session/delete"
                | "origin/session/unload"
                | "x.ai/session/close"
                | "x.ai/session/fork"
        ) {
            return Err(Error::Operation(
                "Session lifecycle methods require their typed Runtime operation".into(),
            ));
        }
        self.extension_raw_unchecked(request).await
    }
    async fn extension_raw_unchecked(
        &self,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        let raw = serde_json::value::to_raw_value(&request.params).map_err(op)?;
        let response = self
            .agent
            .ext_method(acp::ExtRequest::new(request.method.clone(), Arc::from(raw)))
            .await
            .map_err(|e| protocol(&request.method, e))?;
        Ok(ExtensionResponse {
            result: serde_json::from_str(response.0.get()).map_err(op)?,
        })
    }
    async fn fork(
        &self,
        target: SessionId,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        let _lease = self.acquire_session_lease(&target)?;
        self.extension_raw_unchecked(request).await
    }
    async fn extension_notification(&self, request: ExtensionNotification) -> Result<(), Error> {
        if request.method.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "extension notification method is required".into(),
            ));
        }
        if self.options.profile == crate::RuntimeProfile::Restricted {
            return Err(Error::Operation(
                "generic extension notifications require the Desktop profile".into(),
            ));
        }
        let raw = serde_json::value::to_raw_value(&request.params).map_err(op)?;
        self.agent
            .ext_notification(acp::ExtNotification::new(
                request.method.clone(),
                Arc::from(raw),
            ))
            .await
            .map_err(|error| protocol(&request.method, error))
    }
    async fn set_mode(&self, id: SessionId, mode: String) -> Result<(), Error> {
        self.agent
            .set_session_mode(acp::SetSessionModeRequest::new(
                acp::SessionId::new(id.0),
                acp::SessionModeId::new(mode),
            ))
            .await
            .map(|_| ())
            .map_err(|e| protocol("session/set_mode", e))
    }
    async fn list_sessions(&self) -> Result<serde_json::Value, Error> {
        let response = self
            .agent
            .list_sessions(acp::ListSessionsRequest::new())
            .await
            .map_err(|e| protocol("session/list", e))?;
        serde_json::to_value(response).map_err(op)
    }
    async fn set_route(
        &self,
        id: SessionId,
        model: String,
        reasoning: Option<String>,
    ) -> Result<(), Error> {
        let effective_reasoning = self.effective_reasoning(&model, reasoning.as_deref())?;
        if self.turns.borrow().contains_key(&id.0) {
            return Err(Error::Operation(
                "cannot change model during an active prompt".into(),
            ));
        }
        self.apply_native_route(&id, &model, effective_reasoning.as_deref())
            .await?;
        let mut bindings = self.session_bindings.borrow_mut();
        let binding = bindings
            .get_mut(&id.0)
            .ok_or_else(|| Error::Operation("session binding is unavailable".into()))?;
        binding.model = model;
        binding.reasoning = effective_reasoning;
        Ok(())
    }
    async fn rewind_points(&self, id: SessionId) -> Result<Vec<RewindPoint>, Error> {
        self.require_resident(&id)?;
        let ledger = self.load_ledger(&id)?;
        let response: RewindPointsWire = self
            .extension(
                "x.ai/rewind/points",
                serde_json::json!({ "sessionId": id.0 }),
            )
            .await?;
        Ok(response
            .rewind_points
            .into_iter()
            .map(|point| {
                let prompt_digest = ledger
                    .entries
                    .iter()
                    .rev()
                    .find(|entry| {
                        entry.runtime_prompt_index == point.prompt_index
                            && !matches!(entry.state, LedgerTurnState::Discarded)
                    })
                    .map(|entry| entry.prompt_digest.clone())
                    .or(point.origin_prompt_digest);
                RewindPoint {
                    prompt_index: point.prompt_index,
                    prompt_digest,
                    created_at: point.created_at,
                    file_snapshots: point.num_file_snapshots,
                    has_file_changes: point.has_file_changes,
                    prompt_preview: point.prompt_preview,
                }
            })
            .collect())
    }
    async fn rewind_conversation(
        &self,
        id: SessionId,
        operation_id: String,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.rewind_conversation_entry(id, operation_id, target_prompt_index, None)
            .await
    }

    async fn rewind_conversation_entry(
        &self,
        id: SessionId,
        operation_id: String,
        target_prompt_index: u64,
        unsettled_identity: Option<(String, String)>,
    ) -> Result<ConversationRewindReceipt, Error> {
        if operation_id.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "rewind operation id is required".into(),
            ));
        }
        self.require_resident(&id)?;
        if self.turns.borrow().contains_key(&id.0) {
            return Err(Error::Operation(
                "cannot rewind a conversation while the session is active".into(),
            ));
        }
        let (recovery_turn_id, recovery_prompt_digest) = unsettled_identity
            .as_ref()
            .map(|(turn_id, prompt_digest)| (Some(turn_id.as_str()), Some(prompt_digest.as_str())))
            .unwrap_or((None, None));
        let pending_intent = match self.rewind_status(&id, &operation_id)? {
            ConversationRewindStatus::Applied { receipt } => {
                if receipt.session_id == id.0
                    && receipt.target_prompt_index == target_prompt_index
                    && receipt.recovery_turn_id.as_deref() == recovery_turn_id
                    && receipt.recovery_prompt_digest.as_deref() == recovery_prompt_digest
                {
                    return Ok(receipt);
                }
                return Err(Error::Operation(
                    "rewind operation id is already bound to another request identity".into(),
                ));
            }
            ConversationRewindStatus::Pending {
                operation_id,
                session_id,
                target_prompt_index: pending_target,
                target_turn_id,
                target_prompt_digest,
                recovery_turn_id: pending_recovery_turn,
                recovery_prompt_digest: pending_recovery_digest,
            } => {
                let existing = RewindIntent {
                    operation_id,
                    session_id,
                    target_prompt_index: pending_target,
                    target_turn_id,
                    target_prompt_digest,
                    recovery_turn_id: pending_recovery_turn,
                    recovery_prompt_digest: pending_recovery_digest,
                };
                if existing.session_id != id.0
                    || existing.target_prompt_index != target_prompt_index
                    || existing.recovery_turn_id.as_deref() != recovery_turn_id
                    || existing.recovery_prompt_digest.as_deref() != recovery_prompt_digest
                {
                    return Err(Error::Operation(
                        "rewind operation id is already bound to another pending request identity"
                            .into(),
                    ));
                }
                Some(existing)
            }
            ConversationRewindStatus::Absent => None,
        };
        let mut ledger = self.load_ledger(&id)?;
        if unsettled_identity.is_none()
            && ledger
                .entries
                .iter()
                .any(|entry| matches!(entry.state, LedgerTurnState::Pending))
        {
            return Err(Error::Operation(
                "cannot perform a user rewind with an unsettled native Turn".into(),
            ));
        }
        let target_position = ledger
            .entries
            .iter()
            .position(|entry| {
                entry.runtime_prompt_index == target_prompt_index
                    && match (&pending_intent, &unsettled_identity) {
                        (Some(intent), _) => {
                            matches!(
                                entry.state,
                                LedgerTurnState::Pending
                                    | LedgerTurnState::Completed { .. }
                                    | LedgerTurnState::Discarded
                            ) && entry.turn_id == intent.target_turn_id
                                && entry.prompt_digest == intent.target_prompt_digest
                        }
                        (None, None) => {
                            matches!(entry.state, LedgerTurnState::Completed { .. })
                        }
                        (None, Some((turn_id, prompt_digest))) => {
                            (matches!(
                                entry.state,
                                LedgerTurnState::Pending | LedgerTurnState::Completed { .. }
                            )) && entry.turn_id == *turn_id
                                && entry.prompt_digest == *prompt_digest
                        }
                    }
            })
            .ok_or_else(|| {
                Error::Operation(if unsettled_identity.is_some() {
                    "recovery rewind target does not match the pending native Turn".into()
                } else {
                    "rewind target is not a settled entry in the native Turn ledger".into()
                })
            })?;
        if unsettled_identity.is_some()
            && (ledger.entries[..target_position]
                .iter()
                .any(|entry| matches!(entry.state, LedgerTurnState::Pending))
                || ledger.entries[target_position + 1..]
                    .iter()
                    .any(|entry| !matches!(entry.state, LedgerTurnState::Discarded)))
        {
            return Err(Error::Operation(
                "recovery rewind is restricted to the exact unsettled history tail".into(),
            ));
        }
        let target_entry = &ledger.entries[target_position];
        let requested_intent = RewindIntent {
            operation_id: operation_id.clone(),
            session_id: id.0.clone(),
            target_prompt_index,
            target_turn_id: target_entry.turn_id.clone(),
            target_prompt_digest: target_entry.prompt_digest.clone(),
            recovery_turn_id: unsettled_identity
                .as_ref()
                .map(|(turn_id, _)| turn_id.clone()),
            recovery_prompt_digest: unsettled_identity
                .as_ref()
                .map(|(_, prompt_digest)| prompt_digest.clone()),
        };
        if pending_intent
            .as_ref()
            .is_some_and(|existing| existing != &requested_intent)
        {
            return Err(Error::Operation(
                "pending rewind identity differs from the durable Turn ledger".into(),
            ));
        }
        let expected_prompt_digest = target_entry.prompt_digest.clone();
        if pending_intent.is_none() {
            self.save_rewind_intent(&requested_intent)?;
        }

        let native_points: RewindPointsWire = self
            .extension(
                "x.ai/rewind/points",
                serde_json::json!({ "sessionId": id.0 }),
            )
            .await?;
        if !native_rewind_already_applied(
            &native_points.rewind_points,
            target_prompt_index,
            &ledger,
        )? {
            let target_prompt_index_wire = usize::try_from(target_prompt_index)
                .map_err(|_| Error::InvalidConfig("rewind target is out of range".into()))?;
            let response: RewindResultWire = self
                .extension(
                    "x.ai/rewind/execute",
                    serde_json::json!({
                        "sessionId": id.0,
                        "targetPromptIndex": target_prompt_index_wire,
                        "force": true,
                        "mode": "conversation_only",
                    }),
                )
                .await?;
            if !response.success
                || response.target_prompt_index != target_prompt_index
                || response.mode != "conversation_only"
                || !response.reverted_files.is_empty()
                || !response.clean_files.is_empty()
                || !response.conflicts.is_empty()
            {
                return Err(Error::Operation(response.error.unwrap_or_else(|| {
                    if let Some(conflict) = response.conflicts.first() {
                        format!(
                            "native conversation rewind reported {} conflict at {}",
                            conflict.conflict_type, conflict.path
                        )
                    } else if response.mode != "conversation_only" {
                        format!(
                            "native conversation rewind returned unexpected mode {}",
                            response.mode
                        )
                    } else {
                        "native conversation rewind failed or attempted a file mutation".into()
                    }
                })));
            }
            let prompt_text = response.prompt_text.as_deref().ok_or_else(|| {
                Error::Operation("native conversation rewind omitted its target prompt".into())
            })?;
            if !expected_prompt_digest.starts_with("sha256-v2:")
                && crate::prompt_digest(prompt_text) != expected_prompt_digest
            {
                return Err(Error::Operation(
                    "native conversation rewind target differs from the durable Turn ledger".into(),
                ));
            }
        }
        self.extension::<serde_json::Value>(
            "origin/session/sync",
            serde_json::json!({ "sessionId": id.0 }),
        )
        .await?;
        for entry in &mut ledger.entries {
            if entry.runtime_prompt_index >= target_prompt_index {
                entry.state = LedgerTurnState::Discarded;
            }
        }
        self.save_ledger(&id, &ledger)?;
        let receipt = ConversationRewindReceipt {
            operation_id,
            session_id: id.0,
            target_prompt_index,
            target_turn_id: requested_intent.target_turn_id,
            target_prompt_digest: requested_intent.target_prompt_digest,
            recovery_turn_id: requested_intent.recovery_turn_id,
            recovery_prompt_digest: requested_intent.recovery_prompt_digest,
        };
        self.save_rewind_receipt(&receipt)?;
        Ok(receipt)
    }

    fn rewind_key(operation_id: &str) -> SessionEvidenceKey {
        Self::evidence_key(SessionEvidenceKind::Rewind, operation_id.to_owned())
    }

    fn save_rewind_intent(&self, intent: &RewindIntent) -> Result<(), Error> {
        self.commit_evidence(
            &Self::rewind_key(&intent.operation_id),
            &serde_json::to_vec(&RewindEvidence::Intent(intent.clone())).map_err(op)?,
        )
    }

    fn rewind_status(
        &self,
        id: &SessionId,
        operation_id: &str,
    ) -> Result<ConversationRewindStatus, Error> {
        match self.load_evidence(&Self::rewind_key(operation_id), 1024 * 1024)? {
            Some(bytes) => match serde_json::from_slice::<RewindEvidence>(&bytes).map_err(op)? {
                RewindEvidence::Receipt(receipt) => {
                    if receipt.operation_id != operation_id {
                        return Err(Error::Operation("rewind receipt digest mismatch".into()));
                    }
                    if receipt.session_id != id.0 {
                        return Err(Error::Operation(
                            "rewind receipt belongs to a different native session".into(),
                        ));
                    }
                    Ok(ConversationRewindStatus::Applied { receipt })
                }
                RewindEvidence::Intent(intent) => {
                    if intent.operation_id != operation_id || intent.session_id != id.0 {
                        return Err(Error::Operation(
                            "rewind intent identity does not match its evidence key".into(),
                        ));
                    }
                    let valid_identity =
                        |value: &str, max: usize| !value.trim().is_empty() && value.len() <= max;
                    if !valid_identity(&intent.operation_id, 512)
                        || !valid_identity(&intent.session_id, 512)
                        || !valid_identity(&intent.target_turn_id, 512)
                        || !valid_identity(&intent.target_prompt_digest, 160)
                        || match (
                            intent.recovery_turn_id.as_deref(),
                            intent.recovery_prompt_digest.as_deref(),
                        ) {
                            (None, None) => false,
                            (Some(turn), Some(digest)) => {
                                !valid_identity(turn, 512) || !valid_identity(digest, 160)
                            }
                            _ => true,
                        }
                    {
                        return Err(Error::Operation(
                            "rewind intent contains invalid bounded identities".into(),
                        ));
                    }
                    Ok(ConversationRewindStatus::Pending {
                        operation_id: intent.operation_id,
                        session_id: intent.session_id,
                        target_prompt_index: intent.target_prompt_index,
                        target_turn_id: intent.target_turn_id,
                        target_prompt_digest: intent.target_prompt_digest,
                        recovery_turn_id: intent.recovery_turn_id,
                        recovery_prompt_digest: intent.recovery_prompt_digest,
                    })
                }
            },
            None => Ok(ConversationRewindStatus::Absent),
        }
    }

    fn save_rewind_receipt(&self, receipt: &ConversationRewindReceipt) -> Result<(), Error> {
        self.commit_evidence(
            &Self::rewind_key(&receipt.operation_id),
            &serde_json::to_vec(&RewindEvidence::Receipt(receipt.clone())).map_err(op)?,
        )
    }
    fn require_resident(&self, id: &SessionId) -> Result<(), Error> {
        if self.resident.borrow().contains(&id.0) {
            Ok(())
        } else {
            Err(Error::Operation("session is not resident".into()))
        }
    }
    fn events_after(&self, id: &SessionId, sequence: u64) -> Result<Vec<Event>, Error> {
        let current = self
            .sequences
            .borrow()
            .get(&id.0)
            .copied()
            .ok_or_else(|| Error::Operation("unknown session event journal".into()))?;
        if sequence > current {
            return Err(Error::Operation(
                "event cursor is beyond the session sequence".into(),
            ));
        }
        let oldest = self
            .retained
            .borrow()
            .get(&id.0)
            .and_then(|x| x.front())
            .map(|x| x.sequence)
            .unwrap_or(current.saturating_add(1));
        if sequence.saturating_add(1) < oldest {
            return Err(Error::EventGap {
                requested: sequence,
                oldest_available: oldest,
                newest: current,
            });
        }
        Ok(self
            .retained
            .borrow()
            .get(&id.0)
            .into_iter()
            .flatten()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }
    async fn close(&self, id: SessionId) -> Result<(), Error> {
        self.require_resident(&id)?;
        if self.turns.borrow().contains_key(&id.0) {
            self.cancel(id.clone()).await?;
        }
        self.agent
            .close_session(acp::CloseSessionRequest::new(acp::SessionId::new(
                id.0.clone(),
            )))
            .await
            .map_err(|error| protocol("session/close", error))?;
        self.finish_close(&id, true);
        Ok(())
    }
    async fn delete(&self, id: SessionId) -> Result<(), Error> {
        if self.resident.borrow().contains(&id.0) {
            if let Err(error) = self.unload_inner(id.clone(), false).await {
                return Err(error);
            }
        } else if !self.session_leases.borrow().contains_key(&id.0) {
            let lease = self.acquire_session_lease(&id)?;
            if let Some(lease) = lease {
                self.session_leases.borrow_mut().insert(id.0.clone(), lease);
            }
        }
        #[derive(serde::Deserialize)]
        struct DeleteWire {
            success: bool,
        }
        let result = self
            .extension(
                "x.ai/session/delete",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await
            .and_then(|response: DeleteWire| {
                if response.success {
                    Ok(())
                } else {
                    Err(Error::Operation(
                        "native session deletion was not confirmed".into(),
                    ))
                }
            });
        if result.is_ok() {
            self.session_leases.borrow_mut().remove(&id.0);
        }
        result
    }
    async fn unload(&self, id: SessionId) -> Result<(), Error> {
        self.unload_inner(id, true).await
    }
    async fn unload_inner(&self, id: SessionId, release_lease: bool) -> Result<(), Error> {
        self.require_resident(&id)?;
        if self.turns.borrow().contains_key(&id.0) {
            self.cancel(id.clone()).await?;
        }
        let response: UnloadWire = self
            .extension(
                "origin/session/unload",
                serde_json::json!({"sessionId":id.0}),
            )
            .await?;
        if !response.success {
            return Err(Error::Operation(
                "native session unload did not detach the actor".into(),
            ));
        }
        self.finish_close(&id, release_lease && response.drained);
        if response.drained {
            Ok(())
        } else {
            Err(Error::Operation(
                "native session detached but its actor missed the teardown deadline".into(),
            ))
        }
    }
    fn finish_close(&self, id: &SessionId, release_lease: bool) {
        self.emit(id, EventUpdate::SessionClosed, None);
        self.resident.borrow_mut().remove(&id.0);
        self.session_bindings.borrow_mut().remove(&id.0);
        if release_lease {
            self.session_leases.borrow_mut().remove(&id.0);
        }
        self.mcp_bindings.revoke_session(&id.0);
        self.turns.borrow_mut().remove(&id.0);
        self.replay.borrow_mut().remove(&id.0);
        xai_grok_shell::origin_runtime::unregister_session_tree(&id.0);
    }
}
fn validate(c: &RuntimeConfig, options: &RuntimeOptions) -> Result<(), Error> {
    if c.models.is_empty() {
        return Err(Error::InvalidConfig("model catalog is required".into()));
    }
    if c.models
        .iter()
        .any(|m| m.id.trim().is_empty() || NonZeroU64::new(m.context_window).is_none())
    {
        return Err(Error::InvalidConfig(
            "model id and non-zero context window are required".into(),
        ));
    }
    if c.models.iter().any(|model| {
        let options_valid = model
            .reasoning_options
            .iter()
            .all(|option| !option.trim().is_empty())
            && model.reasoning_options.iter().collect::<HashSet<_>>().len()
                == model.reasoning_options.len();
        !options_valid
            || (!model.supports_reasoning
                && (model.default_reasoning.is_some() || !model.reasoning_options.is_empty()))
            || model.default_reasoning.as_ref().is_some_and(|default| {
                !model.supports_reasoning || !model.reasoning_options.contains(default)
            })
    }) {
        return Err(Error::InvalidConfig(
            "model reasoning defaults and options must be unique, non-empty, and consistent".into(),
        ));
    }
    let model_ids: HashSet<&str> = c.models.iter().map(|model| model.id.as_str()).collect();
    if model_ids.len() != c.models.len() {
        return Err(Error::InvalidConfig(
            "catalog model ids must be unique".into(),
        ));
    }
    if options
        .services
        .model_providers
        .keys()
        .any(|model| !model_ids.contains(model.as_str()))
    {
        return Err(Error::InvalidConfig(
            "model provider refers to an unknown catalog model".into(),
        ));
    }
    if options.services.model_providers.values().any(|provider| {
        provider.base_url.trim().is_empty()
            || provider.api_key.trim().is_empty()
            || provider
                .model
                .as_deref()
                .is_some_and(|model| model.trim().is_empty())
            || provider.headers.keys().any(|name| name.trim().is_empty())
            || provider
                .query_params
                .keys()
                .any(|name| name.trim().is_empty())
    }) {
        return Err(Error::InvalidConfig(
            "model providers require a base URL, API key, non-empty optional model slug, and non-empty header/query names".into(),
        ));
    }
    let legacy_provider_available = !c.endpoint.trim().is_empty() && !c.api_key.trim().is_empty();
    if !legacy_provider_available
        && c.models
            .iter()
            .any(|model| !options.services.model_providers.contains_key(&model.id))
    {
        return Err(Error::InvalidConfig(
            "each catalog model requires an explicit provider when the legacy endpoint and API key are empty"
                .into(),
        ));
    }
    let agents = &options.services.agents;
    if agents
        .subagent_models
        .iter()
        .any(|(agent, model)| agent.trim().is_empty() || !model_ids.contains(model.as_str()))
        || [
            agents.web_search_model.as_deref(),
            agents.session_summary_model.as_deref(),
            agents.image_description_model.as_deref(),
            agents.prompt_suggestion_model.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|model| !model_ids.contains(model))
    {
        return Err(Error::InvalidConfig(
            "agent service routing must reference non-empty names and catalog models".into(),
        ));
    }
    if options.services.media.as_ref().is_some_and(|media| {
        media.provider.base_url.trim().is_empty()
            || media.provider.api_key.trim().is_empty()
            || media
                .provider
                .headers
                .keys()
                .any(|name| name.trim().is_empty())
            || media
                .provider
                .query_params
                .keys()
                .any(|name| name.trim().is_empty())
            || [
                media.image_generation_model.as_deref(),
                media.image_edit_model.as_deref(),
                media.image_to_video_model.as_deref(),
                media.reference_to_video_model.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|model| model.trim().is_empty())
    }) {
        return Err(Error::InvalidConfig(
            "media service requires an explicit base URL, API key, non-empty header/query names, and non-empty optional model slugs".into(),
        ));
    }
    validate_mcp_servers(&options.services.mcp_servers)?;
    let mut names: HashSet<&str> = options
        .services
        .mcp_servers
        .iter()
        .map(|server| match server {
            crate::McpServerConfig::Stdio { name, .. }
            | crate::McpServerConfig::Http { name, .. }
            | crate::McpServerConfig::Sse { name, .. } => name.as_str(),
        })
        .collect();
    let mut ids = HashSet::new();
    if options.in_process_mcp_servers.iter().any(|server| {
        server.name.trim().is_empty()
            || server.server_id.trim().is_empty()
            || !names.insert(server.name.as_str())
            || !ids.insert(server.server_id.as_str())
    }) {
        return Err(Error::InvalidConfig(
            "MCP server names and in-process server IDs must be unique and non-empty".into(),
        ));
    }
    if options.profile == crate::RuntimeProfile::Desktop {
        let mut callback_ids = HashSet::new();
        if options.agent_hooks.iter().any(|hook| {
            hook.callback_id.trim().is_empty()
                || !callback_ids.insert(hook.callback_id.as_str())
                || hook.matcher.as_ref().is_some_and(|m| m.trim().is_empty())
                || hook
                    .timeout
                    .is_some_and(|t| !t.is_finite() || t <= 0.0 || t > 600.0)
        }) {
            return Err(Error::InvalidConfig("agent hooks require unique non-empty callback IDs, non-empty matchers, and timeouts in (0, 600] seconds".into()));
        }
    }
    Ok(())
}
fn validate_mcp_servers(servers: &[crate::McpServerConfig]) -> Result<(), Error> {
    let mut mcp_names = HashSet::new();
    if servers.iter().any(|server| {
        let (name, target, key_values) = match server {
            crate::McpServerConfig::Stdio {
                name, command, env, ..
            } => (name.as_str(), command.to_str().unwrap_or(""), env),
            crate::McpServerConfig::Http { name, url, headers }
            | crate::McpServerConfig::Sse { name, url, headers } => {
                (name.as_str(), url.as_str(), headers)
            }
        };
        name.trim().is_empty()
            || target.trim().is_empty()
            || !mcp_names.insert(name)
            || key_values.keys().any(|key| key.trim().is_empty())
    }) {
        return Err(Error::InvalidConfig(
            "MCP servers require unique non-empty names, targets, and variable/header names".into(),
        ));
    }
    Ok(())
}
fn op(e: impl std::fmt::Display) -> Error {
    Error::Operation(e.to_string())
}

fn run_error(error: xai_agent_lifecycle::run::RunError) -> Error {
    Error::DurableRun(error)
}

fn protocol(method: &str, error: acp::Error) -> Error {
    Error::Protocol {
        method: method.into(),
        code: i32::from(error.code),
        message: error.message,
        data: error.data.unwrap_or(serde_json::Value::Null),
        retryable: false,
    }
}
fn prompt_block_wire(block: &PromptBlock) -> Result<serde_json::Value, Error> {
    use base64::Engine as _;

    let validate_base64 = |data: &str, kind: &str| {
        base64::engine::general_purpose::STANDARD
            .decode(data)
            .map(|_| ())
            .map_err(|error| {
                Error::InvalidConfig(format!("{kind} data is not valid base64: {error}"))
            })
    };
    let value = match block {
        PromptBlock::Text { text } => serde_json::json!({"type":"text","text":text}),
        PromptBlock::Image {
            data,
            mime_type,
            uri,
        } => {
            if data.is_empty()
                || !mime_type.starts_with("image/")
                || uri.as_deref().is_some_and(str::is_empty)
            {
                return Err(Error::InvalidConfig("invalid image block".into()));
            }
            validate_base64(data, "image")?;
            serde_json::json!({"type":"image","data":data,"mimeType":mime_type,"uri":uri})
        }
        PromptBlock::Audio { data, mime_type } => {
            if data.is_empty() || !mime_type.starts_with("audio/") {
                return Err(Error::InvalidConfig("invalid audio block".into()));
            }
            validate_base64(data, "audio")?;
            serde_json::json!({"type":"audio","data":data,"mimeType":mime_type})
        }
        PromptBlock::ResourceLink {
            uri,
            name,
            mime_type,
        } => {
            if uri.is_empty() || name.is_empty() {
                return Err(Error::InvalidConfig("invalid resource link".into()));
            }
            serde_json::json!({"type":"resource_link","uri":uri,"name":name,"mimeType":mime_type})
        }
        PromptBlock::EmbeddedTextResource {
            uri,
            text,
            mime_type,
        } => {
            if uri.is_empty() {
                return Err(Error::InvalidConfig(
                    "embedded resource URI is required".into(),
                ));
            }
            serde_json::json!({"type":"resource","resource":{"uri":uri,"text":text,"mimeType":mime_type}})
        }
        PromptBlock::EmbeddedBlobResource {
            uri,
            blob,
            mime_type,
        } => {
            if uri.is_empty() || blob.is_empty() {
                return Err(Error::InvalidConfig(
                    "embedded blob resource URI and data are required".into(),
                ));
            }
            validate_base64(blob, "embedded resource")?;
            serde_json::json!({"type":"resource","resource":{"uri":uri,"blob":blob,"mimeType":mime_type}})
        }
    };
    Ok(value)
}

fn client_capability_meta(options: &RuntimeOptions) -> Result<acp::Meta, Error> {
    let mut meta = match &options.host_capabilities.meta {
        serde_json::Value::Null => acp::Meta::new(),
        serde_json::Value::Object(meta) => meta.clone(),
        _ => {
            return Err(Error::InvalidConfig(
                "host capability metadata must be a JSON object".into(),
            ));
        }
    };
    meta.insert(
        "clientIdentifier".into(),
        serde_json::Value::String(options.client_identifier.clone()),
    );
    let extension_methods = options.host_capabilities.extension_methods.clone();
    meta.insert(
        "originHostExtensionMethods".into(),
        serde_json::to_value(extension_methods).map_err(op)?,
    );
    Ok(meta)
}

fn capabilities_for(options: &RuntimeOptions) -> RuntimeCapabilities {
    const SDK_FEATURES: &[(&str, &str, bool)] = &[
        ("sdk:session-lifecycle", "state", false),
        ("sdk:agent-turns", "agent", false),
        ("sdk:autonomous-runs", "state-agent", false),
        ("sdk:event-journal", "read", false),
        ("sdk:commands", "agent", true),
        ("sdk:scheduler", "background", true),
        ("sdk:workflows", "process", true),
        ("sdk:subagents", "process", true),
        ("sdk:mcp", "network-process", true),
        ("sdk:rewind", "workspace-write", true),
        ("sdk:hooks", "process", true),
        ("sdk:permissions", "policy", true),
    ];
    const OPTIONAL_FEATURES: &[(&str, &str, Option<&str>)] = &[
        ("feature:web_search", "network", None),
        ("feature:web_fetch", "network", None),
        ("feature:memory", "persistent-write", None),
        ("feature:workflows", "process", None),
        ("feature:managed_mcp", "network-process", None),
        ("feature:app_deployment", "network-write", None),
        ("feature:skills", "read", None),
        ("feature:plugins", "process", None),
        ("feature:hooks", "process", None),
        ("feature:lsp", "process", None),
        ("feature:auto_wake", "background", None),
        ("feature:image_generation", "network-write", None),
        ("feature:image_edit", "network-write", None),
        ("feature:video_generation", "network-write", None),
        ("host:filesystem", "host", Some("HostDelegate filesystem")),
        ("host:terminal", "host", Some("HostDelegate terminal")),
        (
            "host:desktop_automation",
            "host",
            Some("HostDelegate extension"),
        ),
    ];
    let mut descriptors = SDK_FEATURES
        .iter()
        .map(|(name, effect, desktop_only)| {
            let enabled = !desktop_only || options.profile == crate::RuntimeProfile::Desktop;
            crate::CapabilityDescriptor {
                namespace: (*name).into(),
                enabled,
                disabled_reason: (!enabled).then(|| "restricted profile".into()),
                effect_class: (*effect).into(),
                host_requirement: None,
            }
        })
        .collect::<Vec<_>>();
    descriptors.push(crate::CapabilityDescriptor {
        namespace: "sdk:in-process-mcp".into(),
        enabled: options.profile == crate::RuntimeProfile::Desktop
            && !options.in_process_mcp_servers.is_empty(),
        disabled_reason: (options.profile != crate::RuntimeProfile::Desktop
            || options.in_process_mcp_servers.is_empty())
        .then(|| {
            if options.profile == crate::RuntimeProfile::Restricted {
                "restricted profile".into()
            } else {
                "no in-process MCP server registered".into()
            }
        }),
        effect_class: "in-process".into(),
        host_requirement: None,
    });
    descriptors.push(crate::CapabilityDescriptor {
        namespace: "sdk:extension-bridge".into(),
        enabled: options.profile == crate::RuntimeProfile::Desktop,
        disabled_reason: (options.profile == crate::RuntimeProfile::Restricted)
            .then(|| "restricted profile".into()),
        effect_class: "extension-defined".into(),
        host_requirement: None,
    });
    descriptors.push(crate::CapabilityDescriptor {
        namespace: "x.ai/models/list".into(),
        enabled: true,
        disabled_reason: None,
        effect_class: "read".into(),
        host_requirement: None,
    });
    descriptors.extend(OPTIONAL_FEATURES.iter().map(|(name, effect, host)| {
        let enabled = match *name {
            "host:filesystem" => {
                options.host_capabilities.fs_read || options.host_capabilities.fs_write
            }
            "host:terminal" => options.host_capabilities.terminal,
            "host:desktop_automation" => !options.host_capabilities.extension_methods.is_empty(),
            "feature:web_search" => {
                options.profile == crate::RuntimeProfile::Desktop
                    && options.services.agents.web_search_model.is_some()
            }
            // These native product paths have no explicit, ambient-free SDK
            // service contract in this checkout. Do not advertise them merely
            // because the generic Desktop extension transport is available.
            "feature:web_fetch" | "feature:memory" | "feature:lsp" => false,
            "feature:managed_mcp" | "feature:app_deployment" => false,
            "feature:image_generation" => {
                options.profile == crate::RuntimeProfile::Desktop
                    && options
                        .services
                        .media
                        .as_ref()
                        .is_some_and(|media| media.image_generation)
            }
            "feature:image_edit" => {
                options.profile == crate::RuntimeProfile::Desktop
                    && options
                        .services
                        .media
                        .as_ref()
                        .is_some_and(|media| media.image_edit)
            }
            "feature:video_generation" => {
                options.profile == crate::RuntimeProfile::Desktop
                    && options
                        .services
                        .media
                        .as_ref()
                        .is_some_and(|media| media.video_generation)
            }
            _ => options.profile == crate::RuntimeProfile::Desktop,
        };
        crate::CapabilityDescriptor {
            namespace: (*name).into(),
            enabled,
            disabled_reason: (!enabled).then(|| {
                if name.starts_with("host:") {
                    "host capability not advertised".into()
                } else if name.starts_with("feature:image") || *name == "feature:video_generation" {
                    if options.profile == crate::RuntimeProfile::Restricted {
                        "restricted profile".into()
                    } else {
                        "media service not configured or operation disabled".into()
                    }
                } else if *name == "feature:web_search" {
                    if options.profile == crate::RuntimeProfile::Restricted {
                        "restricted profile".into()
                    } else {
                        "web-search model service not configured".into()
                    }
                } else if *name == "feature:managed_mcp" {
                    "managed MCP is an account-product service gateway, not a client transport or credential-injection facility; configure explicit MCP transports instead".into()
                } else if *name == "feature:app_deployment" {
                    "App Builder deployment is not implemented in this source checkout".into()
                } else if matches!(*name, "feature:web_fetch" | "feature:memory" | "feature:lsp") {
                    "no explicit ambient-free SDK configuration is available".into()
                } else {
                    "restricted profile".into()
                }
            }),
            effect_class: (*effect).into(),
            host_requirement: host.map(str::to_owned),
        }
    }));
    RuntimeCapabilities {
        profile: options.profile,
        host: options.host_capabilities.clone(),
        features: descriptors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LeaseDropSpy(Arc<std::sync::atomic::AtomicBool>);
    impl crate::SessionStateLease for LeaseDropSpy {}
    impl Drop for LeaseDropSpy {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn uncertain_session_leases_are_quarantined_for_process_lifetime() {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        quarantine_session_leases(vec![Box::new(LeaseDropSpy(dropped.clone()))]);
        assert!(!dropped.load(Ordering::Acquire));
    }

    #[test]
    fn session_state_bridge_round_trips_semantic_port() {
        use xai_grok_shell::session::state_authority::{
            NativeSessionStateAuthority as _, ReplayCursor, ReplayRecord, RewindOperation,
            SessionIdentity, SessionInspection,
        };

        let root = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn crate::SessionStateStore> =
            Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
        let authority = SessionStateAuthorityBridge { store };
        let id = SessionIdentity {
            identity: "session-1".into(),
            generation: "generation-1".into(),
        };
        let session = authority.create(id.clone()).unwrap();
        assert!(
            authority.create(id.clone()).is_err(),
            "create must not reopen or replace an existing live session"
        );
        assert_eq!(
            authority.inspect("session-1").unwrap(),
            SessionInspection::Live {
                generation: "generation-1".into()
            }
        );
        session.stage_update(b"one".to_vec()).unwrap();
        session.stage_update(b"two".to_vec()).unwrap();
        session.flush().unwrap();
        let first = session.replay_page(None, 1).unwrap();
        assert_eq!(first.records, vec![ReplayRecord::Update(b"one".to_vec())]);
        let second = session.replay_page(first.next, 2).unwrap();
        assert_eq!(second.records, vec![ReplayRecord::Update(b"two".to_vec())]);
        assert!(
            session
                .replay_page(
                    Some(ReplayCursor {
                        generation: "wrong".into(),
                        next_sequence: 1
                    }),
                    1
                )
                .is_err()
        );
        assert!(
            session
                .replay_page(
                    Some(ReplayCursor {
                        generation: "generation-1".into(),
                        next_sequence: 99,
                    }),
                    1,
                )
                .is_err(),
            "logical replay cursor gaps must fail closed"
        );
        session
            .publish_checkpoint("cp".into(), b"state".to_vec(), b"cp-marker".to_vec())
            .unwrap();
        session
            .publish_rewind(
                RewindOperation::Truncate {
                    index: 7,
                    payload: b"rewind".to_vec(),
                },
                b"rw-marker".to_vec(),
            )
            .unwrap();
        let all = session.replay_page(None, 10).unwrap();
        assert!(
            matches!(&all.records[2], ReplayRecord::Checkpoint { name, marker, .. } if name == "cp" && marker == b"cp-marker")
        );
        assert!(
            matches!(&all.records[3], ReplayRecord::Rewind { marker, .. } if marker == b"rw-marker")
        );
        let fork_id = SessionIdentity {
            identity: "session-2".into(),
            generation: "fresh-generation".into(),
        };
        let fork = authority
            .publish_fork(fork_id.clone(), all.records.clone())
            .unwrap();
        assert_eq!(fork.identity(), &fork_id);
        assert_eq!(fork.replay_page(None, 10).unwrap().records, all.records);
        assert!(
            authority
                .publish_fork(fork_id, vec![ReplayRecord::Update(b"replacement".to_vec())])
                .is_err(),
            "prepared fork publication must not replace a live generation"
        );
        authority.tombstone(id.clone()).unwrap();
        assert!(matches!(
            authority.inspect("session-1").unwrap(),
            SessionInspection::Tombstoned { .. }
        ));
        assert!(authority.create(id).is_err());
    }

    struct PermissionPolicy {
        decision: Result<crate::ToolPermissionDecision, crate::ToolPermissionError>,
        requests: std::sync::Mutex<Vec<crate::ToolPermissionRequest>>,
    }

    #[async_trait::async_trait]
    impl crate::ToolPermissionHandler for PermissionPolicy {
        async fn request_permission(
            &self,
            request: crate::ToolPermissionRequest,
        ) -> Result<crate::ToolPermissionDecision, crate::ToolPermissionError> {
            self.requests.lock().unwrap().push(request);
            self.decision.clone()
        }
    }

    struct InProcessProbe {
        called: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl crate::InProcessMcpHandler for InProcessProbe {
        async fn handle(
            &self,
            message: serde_json::Value,
        ) -> Result<serde_json::Value, crate::HostError> {
            self.called.store(true, Ordering::Release);
            Ok(match message.get("id") {
                Some(id) => serde_json::json!({"jsonrpc":"2.0","id":id,"result":{}}),
                None => serde_json::Value::Null,
            })
        }
    }

    #[tokio::test]
    async fn direct_mcp_invoker_rejects_unregistered_stale_and_nonresident_bindings() {
        let called = Arc::new(AtomicBool::new(false));
        let handler: Arc<dyn crate::InProcessMcpHandler> = Arc::new(InProcessProbe {
            called: called.clone(),
        });
        let bindings = Arc::new(McpBindingRegistry::default());
        let invoker = DirectMcpInvoker {
            runtime_instance_id: 1,
            handlers: HashMap::from([("registration".into(), ("server".into(), handler))]),
            bindings: bindings.clone(),
            host_services: Default::default(),
        };

        let unregistered = xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
            &invoker,
            "unknown-session",
            u64::MAX,
            "registration",
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
            std::time::Duration::from_secs(1),
        )
        .await
        .expect_err("unregistered bindings fail closed");
        assert!(unregistered.contains("stale or not resident"));

        let old_binding = bindings.bind("closed-session");
        let new_binding = bindings.bind("closed-session");
        let stale = xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
            &invoker,
            "closed-session",
            old_binding,
            "registration",
            serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            std::time::Duration::from_secs(1),
        )
        .await
        .expect_err("replacement invalidates the old actor binding");
        assert!(stale.contains("stale or not resident"));

        xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
            &invoker,
            "closed-session",
            new_binding,
            "registration",
            serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
            std::time::Duration::from_secs(1),
        )
        .await
        .expect("the replacement binding is active");

        bindings.revoke_session("closed-session");
        let error = xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
            &invoker,
            "closed-session",
            new_binding,
            "registration",
            serde_json::json!({"jsonrpc":"2.0","id":4,"method":"tools/list"}),
            std::time::Duration::from_secs(1),
        )
        .await
        .expect_err("nonresident sessions fail closed");
        assert!(error.contains("stale or not resident"));
        assert!(called.load(Ordering::Acquire));
    }

    struct OutboundProbe {
        peer: std::sync::Mutex<Option<crate::InProcessMcpPeer>>,
    }

    #[async_trait::async_trait]
    impl crate::InProcessMcpHandler for OutboundProbe {
        async fn handle(
            &self,
            message: serde_json::Value,
        ) -> Result<serde_json::Value, crate::HostError> {
            Ok(match message.get("id") {
                Some(id) => serde_json::json!({"jsonrpc":"2.0","id":id,"result":{}}),
                None => serde_json::Value::Null,
            })
        }

        async fn connected(
            &self,
            _context: &crate::InProcessMcpContext,
            peer: crate::InProcessMcpPeer,
        ) -> Result<(), crate::HostError> {
            *self.peer.lock().unwrap() = Some(peer);
            Ok(())
        }
    }

    #[tokio::test]
    async fn in_process_outbound_peer_is_bounded_and_generation_bound() {
        let probe = Arc::new(OutboundProbe {
            peer: std::sync::Mutex::new(None),
        });
        let bindings = Arc::new(McpBindingRegistry::default());
        let invoker = DirectMcpInvoker {
            runtime_instance_id: 1,
            handlers: HashMap::from([(
                "registration".into(),
                (
                    "server".into(),
                    probe.clone() as Arc<dyn crate::InProcessMcpHandler>,
                ),
            )]),
            bindings: bindings.clone(),
            host_services: Default::default(),
        };
        let first_binding = bindings.bind("session");
        let (first_tx, mut first_rx) = tokio::sync::mpsc::channel(1);
        xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::connect(
            &invoker,
            "session",
            first_binding,
            "registration",
            first_tx,
            std::time::Duration::from_secs(1),
        )
        .await
        .expect("first peer connects");
        let first_peer = probe.peer.lock().unwrap().clone().expect("first peer");
        first_peer
            .notify("notifications/tools/list_changed", serde_json::json!({}))
            .await
            .expect("active peer pushes");
        assert_eq!(
            first_rx.recv().await.unwrap()["method"],
            "notifications/tools/list_changed"
        );

        let second_binding = bindings.bind("session");
        assert!(
            first_peer
                .notify("notifications/tools/list_changed", serde_json::json!({}))
                .await
                .is_err(),
            "replacement invalidates a retained old peer"
        );
        assert!(first_rx.try_recv().is_err());

        let (second_tx, mut second_rx) = tokio::sync::mpsc::channel(1);
        xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::connect(
            &invoker,
            "session",
            second_binding,
            "registration",
            second_tx,
            std::time::Duration::from_secs(1),
        )
        .await
        .expect("replacement peer connects");
        let second_peer = probe.peer.lock().unwrap().clone().expect("second peer");
        second_peer
            .notify(
                "notifications/resources/list_changed",
                serde_json::json!({}),
            )
            .await
            .expect("replacement peer pushes");
        assert_eq!(
            second_rx.recv().await.unwrap()["method"],
            "notifications/resources/list_changed"
        );
    }

    #[tokio::test]
    async fn in_process_outbound_backpressure_rechecks_generation_before_delivery() {
        let bindings = Arc::new(McpBindingRegistry::default());
        let binding_id = bindings.bind("session");
        let (outbound, mut receiver) = tokio::sync::mpsc::channel(1);
        let peer = crate::InProcessMcpPeer::new(Arc::new(DirectMcpOutbound {
            session_id: "session".into(),
            binding_id,
            bindings: bindings.clone(),
            outbound,
        }));
        peer.notify(
            "notifications/tools/list_changed",
            serde_json::json!({"n":1}),
        )
        .await
        .expect("first notification fills the bounded channel");

        let blocked = {
            let peer = peer.clone();
            tokio::spawn(async move {
                peer.notify(
                    "notifications/tools/list_changed",
                    serde_json::json!({"n":2}),
                )
                .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            !blocked.is_finished(),
            "a full outbound channel must apply backpressure"
        );

        bindings.revoke_session("session");
        assert_eq!(receiver.recv().await.unwrap()["params"]["n"], 1);
        blocked
            .await
            .expect("blocked sender task")
            .expect_err("a sender released after unload must fail closed");
        assert!(receiver.try_recv().is_err());
    }

    struct BlockingInProcessProbe {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl crate::InProcessMcpHandler for BlockingInProcessProbe {
        async fn handle(
            &self,
            message: serde_json::Value,
        ) -> Result<serde_json::Value, crate::HostError> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(serde_json::json!({
                "jsonrpc":"2.0",
                "id":message["id"],
                "result":{}
            }))
        }
    }

    #[tokio::test]
    async fn direct_mcp_invoker_rejects_a_result_after_its_binding_is_revoked() {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let bindings = Arc::new(McpBindingRegistry::default());
        let invoker = Arc::new(DirectMcpInvoker {
            runtime_instance_id: 1,
            handlers: HashMap::from([(
                "registration".into(),
                (
                    "server".into(),
                    Arc::new(BlockingInProcessProbe {
                        started: started.clone(),
                        release: release.clone(),
                    }) as Arc<dyn crate::InProcessMcpHandler>,
                ),
            )]),
            bindings: bindings.clone(),
            host_services: Default::default(),
        });
        let binding_id = bindings.bind("session");
        let invocation = {
            let invoker = invoker.clone();
            tokio::spawn(async move {
                xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
                    invoker.as_ref(),
                    "session",
                    binding_id,
                    "registration",
                    serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
                    std::time::Duration::from_secs(2),
                )
                .await
            })
        };
        started.notified().await;
        bindings.revoke_session("session");
        release.notify_one();
        let error = invocation
            .await
            .expect("invocation task")
            .expect_err("a revoked binding cannot accept a late result");
        assert!(error.contains("stale or not resident"));
    }

    fn permission_client(handler: Option<Arc<dyn crate::ToolPermissionHandler>>) -> Client {
        let (events, _) = mpsc::unbounded_channel();
        Client {
            events,
            sequences: Rc::new(RefCell::new(HashMap::new())),
            retained: Rc::new(RefCell::new(HashMap::new())),
            capacity: 1,
            host: None,
            tool_permission_handler: handler,
            host_extension_methods: HashSet::new(),
            agent_hooks: HashMap::new(),
            turns: Rc::new(RefCell::new(HashMap::new())),
            turn_usages: Rc::new(RefCell::new(HashMap::new())),
            replay: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    fn completed_turn_usage(prompt_id: &str, input_tokens: u64) -> serde_json::Value {
        serde_json::json!({
            "sessionUpdate": "turn_completed",
            "prompt_id": prompt_id,
            "usage": {
                "inputTokens": input_tokens,
                "outputTokens": 2,
                "totalTokens": input_tokens + 2,
                "modelCalls": 1,
                "costUSD": 0.000001
            }
        })
    }

    #[test]
    fn turn_usage_is_bound_to_prompt_identity_and_conflicts_fail_closed() {
        let session_id = "usage-correlation-root";
        let turn_id = "expected-turn";
        assert!(xai_grok_shell::origin_runtime::register_root_session(
            session_id
        ));
        let client = permission_client(None);
        client
            .turns
            .borrow_mut()
            .insert(session_id.into(), turn_id.into());

        client
            .capture_turn_usage(session_id, &completed_turn_usage("wrong-turn", 100))
            .unwrap();
        assert!(client.turn_usages.borrow().is_empty());

        client
            .capture_turn_usage(session_id, &completed_turn_usage(turn_id, 10))
            .unwrap();
        client
            .capture_turn_usage(session_id, &completed_turn_usage(turn_id, 10))
            .unwrap();
        assert!(matches!(
            client
                .turn_usages
                .borrow()
                .get(&(session_id.into(), turn_id.into())),
            Some(CapturedTurnUsage::Exact(Some(usage))) if usage.totals.input_tokens == 10
        ));

        client
            .capture_turn_usage(session_id, &completed_turn_usage(turn_id, 11))
            .unwrap();
        assert_eq!(
            client
                .turn_usages
                .borrow()
                .get(&(session_id.into(), turn_id.into())),
            Some(&CapturedTurnUsage::Conflict)
        );
        xai_grok_shell::origin_runtime::unregister_session_tree(session_id);
    }

    #[test]
    fn child_usage_before_root_receipt_cannot_settle_the_root_turn() {
        let session_id = "usage-child-before-root";
        let child_id = "usage-child-before-child";
        let turn_id = "usage-child-before-turn";
        assert!(xai_grok_shell::origin_runtime::register_root_session(
            session_id
        ));
        assert!(xai_grok_shell::origin_runtime::register_child_session(
            child_id, session_id
        ));
        let client = permission_client(None);
        client
            .turns
            .borrow_mut()
            .insert(session_id.into(), turn_id.into());

        client
            .capture_turn_usage(child_id, &completed_turn_usage(turn_id, 100))
            .unwrap();
        assert!(
            client.turn_usages.borrow().is_empty(),
            "a child receipt cannot create root Turn usage evidence"
        );

        client
            .capture_turn_usage(session_id, &completed_turn_usage(turn_id, 10))
            .unwrap();
        assert!(matches!(
            client
                .turn_usages
                .borrow()
                .get(&(session_id.into(), turn_id.into())),
            Some(CapturedTurnUsage::Exact(Some(usage))) if usage.totals.input_tokens == 10
        ));
        xai_grok_shell::origin_runtime::unregister_session_tree(session_id);
    }

    #[test]
    fn child_usage_after_root_receipt_cannot_replace_the_root_turn() {
        let session_id = "usage-child-after-root";
        let child_id = "usage-child-after-child";
        let turn_id = "usage-child-after-turn";
        assert!(xai_grok_shell::origin_runtime::register_root_session(
            session_id
        ));
        assert!(xai_grok_shell::origin_runtime::register_child_session(
            child_id, session_id
        ));
        let client = permission_client(None);
        client
            .turns
            .borrow_mut()
            .insert(session_id.into(), turn_id.into());

        client
            .capture_turn_usage(session_id, &completed_turn_usage(turn_id, 10))
            .unwrap();
        client
            .capture_turn_usage(child_id, &completed_turn_usage(turn_id, 100))
            .unwrap();
        assert!(matches!(
            client
                .turn_usages
                .borrow()
                .get(&(session_id.into(), turn_id.into())),
            Some(CapturedTurnUsage::Exact(Some(usage))) if usage.totals.input_tokens == 10
        ));
        xai_grok_shell::origin_runtime::unregister_session_tree(session_id);
    }

    #[test]
    fn dropping_turn_reservation_clears_active_identity_and_usage() {
        let session_id = "reservation-cleanup-root";
        let turn_id = "reservation-cleanup-turn";
        let turns = Rc::new(RefCell::new(HashMap::from([(
            session_id.into(),
            turn_id.into(),
        )])));
        let turn_usages = Rc::new(RefCell::new(HashMap::from([(
            (session_id.into(), turn_id.into()),
            CapturedTurnUsage::Conflict,
        )])));
        {
            let _reservation = TurnReservation {
                turns: turns.clone(),
                turn_usages: turn_usages.clone(),
                session_id: session_id.into(),
                turn_id: turn_id.into(),
            };
        }
        assert!(turns.borrow().is_empty());
        assert!(turn_usages.borrow().is_empty());

        turns
            .borrow_mut()
            .insert(session_id.into(), "replacement-turn".into());
        {
            let _stale_reservation = TurnReservation {
                turns: turns.clone(),
                turn_usages: turn_usages.clone(),
                session_id: session_id.into(),
                turn_id: turn_id.into(),
            };
        }
        assert_eq!(
            turns.borrow().get(session_id).map(String::as_str),
            Some("replacement-turn"),
            "a late cancelled task cannot clear a newer Turn reservation"
        );
    }

    fn permission_request() -> acp::RequestPermissionRequest {
        acp::RequestPermissionRequest::new(
            "session-typed",
            acp::ToolCallUpdate::new(
                "call-1",
                acp::ToolCallUpdateFields::new()
                    .title("Run tests")
                    .kind(acp::ToolKind::Execute)
                    .status(acp::ToolCallStatus::Pending)
                    .raw_input(serde_json::json!({"command":"cargo test"}))
                    .raw_output(serde_json::json!({"preview":true})),
            ),
            [
                ("once", "Once", acp::PermissionOptionKind::AllowOnce),
                ("always", "Always", acp::PermissionOptionKind::AllowAlways),
                ("reject", "Reject", acp::PermissionOptionKind::RejectOnce),
                ("never", "Never", acp::PermissionOptionKind::RejectAlways),
            ]
            .into_iter()
            .map(|(id, name, kind)| acp::PermissionOption::new(id, name, kind))
            .collect(),
        )
    }

    #[tokio::test]
    async fn typed_permission_policy_parses_routes_and_fails_closed() {
        use agent_client_protocol::Client as _;
        let policy = Arc::new(PermissionPolicy {
            decision: Ok(crate::ToolPermissionDecision::Selected("always".into())),
            requests: Default::default(),
        });
        let response = permission_client(Some(policy.clone()))
            .request_permission(permission_request())
            .await
            .unwrap();
        assert!(
            matches!(response.outcome, acp::RequestPermissionOutcome::Selected(ref selected) if selected.option_id.0.as_ref() == "always")
        );
        let requests = policy.requests.lock().unwrap();
        let request = &requests[0];
        assert_eq!(request.session_id, "session-typed");
        assert_eq!(request.tool_call.id, "call-1");
        assert_eq!(request.tool_call.title.as_deref(), Some("Run tests"));
        assert_eq!(request.tool_call.kind, Some(crate::ToolKind::Execute));
        assert_eq!(
            request.tool_call.raw_input.as_ref().unwrap()["command"],
            "cargo test"
        );
        assert_eq!(request.raw["toolCall"]["rawOutput"]["preview"], true);
        assert_eq!(
            request.options.iter().map(|o| o.kind).collect::<Vec<_>>(),
            vec![
                crate::ToolPermissionOptionKind::AllowOnce,
                crate::ToolPermissionOptionKind::AllowAlways,
                crate::ToolPermissionOptionKind::RejectOnce,
                crate::ToolPermissionOptionKind::RejectAlways
            ]
        );
        drop(requests);

        let invalid = Arc::new(PermissionPolicy {
            decision: Ok(crate::ToolPermissionDecision::Selected("invented".into())),
            requests: Default::default(),
        });
        let error = permission_client(Some(invalid))
            .request_permission(permission_request())
            .await
            .unwrap_err();
        assert_eq!(i32::from(error.code), -32602);

        let failing = Arc::new(PermissionPolicy {
            decision: Err(crate::ToolPermissionError {
                message: "denied by policy service".into(),
                data: serde_json::json!({"rule":"prod"}),
            }),
            requests: Default::default(),
        });
        let error = permission_client(Some(failing))
            .request_permission(permission_request())
            .await
            .unwrap_err();
        assert_eq!(i32::from(error.code), -32603);

        let cancelled = permission_client(None)
            .request_permission(permission_request())
            .await
            .unwrap();
        assert!(matches!(
            cancelled.outcome,
            acp::RequestPermissionOutcome::Cancelled
        ));
    }

    #[derive(Default)]
    struct EchoHost {
        notifications: std::sync::Mutex<Vec<crate::HostNotification>>,
    }

    #[async_trait::async_trait]
    impl crate::HostDelegate for EchoHost {
        async fn request(
            &self,
            request: crate::HostRequest,
        ) -> Result<serde_json::Value, crate::HostError> {
            Ok(serde_json::json!({
                "method":request.method,
                "params":request.params,
                "host":true
            }))
        }

        async fn notification(
            &self,
            notification: crate::HostNotification,
        ) -> Result<(), crate::HostError> {
            self.notifications
                .lock()
                .expect("notifications lock")
                .push(notification);
            Ok(())
        }
    }

    #[test]
    fn pending_rewind_never_guesses_from_prompt_count_when_prefix_identity_drifted() {
        let ledger = SessionLedger {
            entries: vec![SessionLedgerEntry {
                turn_id: "turn-0".into(),
                prompt_digest: "sha256:expected".into(),
                runtime_prompt_index: 0,
                state: LedgerTurnState::Completed {
                    outcome: TurnOutcome::End,
                    settlement_id: "settlement-0".into(),
                    usage: None,
                },
            }],
        };
        let drifted = RewindPointWire {
            prompt_index: 0,
            created_at: "2026-08-07T00:00:00Z".into(),
            num_file_snapshots: 0,
            has_file_changes: false,
            prompt_preview: None,
            origin_prompt_digest: Some("sha256:other".into()),
        };

        assert!(native_rewind_already_applied(&[drifted], 1, &ledger).is_err());
    }

    #[test]
    fn known_mcp_notifications_are_typed_and_unknown_methods_fail_closed() {
        let payload = serde_json::json!({
            "sessionId": "session-1",
            "name": "fixture",
            "source": "local",
            "status": "needs_auth",
            "reason": "auth_expired",
            "detail": "reauthorize",
            "tools": null,
            "future": {"preserved": true}
        });
        assert!(matches!(
            typed_mcp_notification("x.ai/mcp/server_status", &payload),
            Some(EventUpdate::McpServerStatus(crate::McpServerStatusEvent {
                name,
                status: crate::McpServerStatus::NeedsAuth,
                reason: crate::McpServerStatusReason::AuthExpired,
                ..
            })) if name == "fixture"
        ));
        assert!(typed_mcp_notification("x.ai/mcp/future_notification", &payload).is_none());

        let task_status = typed_mcp_notification(
            "x.ai/mcp/task_status",
            &serde_json::json!({
                "sessionId": "session-1",
                "server": "fixture",
                "clientId": 17,
                "task": {
                    "taskId": "task-1",
                    "status": "completed",
                    "statusMessage": "done",
                    "lastUpdatedAt": "2026-08-09T00:00:00Z",
                    "result": {"secret": "must-not-escape"},
                    "_meta": {"token": "must-not-escape"}
                }
            }),
        )
        .expect("typed Task status event");
        let serialized = serde_json::to_string(&task_status).expect("Task event serializes");
        assert!(!serialized.contains("must-not-escape"));
        assert!(matches!(
            task_status,
            EventUpdate::McpTaskStatus(crate::McpTaskStatusEvent {
                status: crate::McpTaskStatus::Completed,
                handle: crate::McpTaskHandle { client_id: 17, .. },
                ..
            })
        ));
        assert!(
            typed_mcp_notification(
                "x.ai/mcp/task_status",
                &serde_json::json!({
                    "sessionId": "session-1",
                    "server": "fixture",
                    "clientId": 17,
                    "task": {
                        "taskId": "task-1",
                        "status": "future_status",
                        "lastUpdatedAt": "2026-08-09T00:00:00Z"
                    }
                }),
            )
            .is_none()
        );

        let servers = typed_mcp_notification(
            "x.ai/mcp/servers_updated",
            &serde_json::json!({
                "sessionId": "session-1",
                "mcpServers": [{
                    "name": "fixture",
                    "source": "local",
                    "type": "stdio",
                    "env": [{"name": "TOKEN", "value": "must-not-escape"}],
                    "session": {
                        "enabled": true,
                        "tools": [{"name":"echo","_meta":{"token":"tool-meta-secret"}}],
                        "negotiated": {
                            "protocolVersion":"2026-07-28",
                            "capabilities": {
                                "tools":{},
                                "extensions":{"future":{"token":"extension-secret"}},
                                "future":{"token":"capability-raw-secret"}
                            }
                        }
                    }
                }]
            }),
        )
        .expect("typed server catalog event");
        let serialized = serde_json::to_string(&servers).expect("event serializes");
        for secret in [
            "must-not-escape",
            "tool-meta-secret",
            "extension-secret",
            "capability-raw-secret",
        ] {
            assert!(!serialized.contains(secret), "event leaked {secret}");
        }
    }

    #[tokio::test]
    async fn mcp_notifications_never_forward_raw_catalog_secrets_to_the_host() {
        use agent_client_protocol::Client as _;

        let (events, mut event_rx) = mpsc::unbounded_channel();
        let host = Arc::new(EchoHost::default());
        let client = Client {
            events,
            sequences: Rc::new(RefCell::new(HashMap::new())),
            retained: Rc::new(RefCell::new(HashMap::new())),
            capacity: 4,
            host: Some(host.clone()),
            tool_permission_handler: None,
            host_extension_methods: HashSet::new(),
            agent_hooks: HashMap::new(),
            turns: Rc::new(RefCell::new(HashMap::new())),
            turn_usages: Rc::new(RefCell::new(HashMap::new())),
            replay: Rc::new(RefCell::new(HashMap::new())),
        };
        let payload = serde_json::json!({
            "sessionId":"session-1",
            "mcpServers":[
                {
                    "name":"http-fixture",
                    "source":"local",
                    "type":"http",
                    "url":"https://user:url-secret@example.invalid/mcp",
                    "setupValues":{"token":"setup-secret"},
                    "session":{
                        "enabled":true,
                        "tools":[{"name":"echo","_meta":{"token":"tool-meta-secret"}}],
                        "negotiated":{
                            "protocolVersion":"2026-07-28",
                            "capabilities":{
                                "tools":{},
                                "extensions":{"future":{"token":"extension-secret"}},
                                "future":{"token":"capability-raw-secret"}
                            }
                        }
                    }
                },
                {
                    "name":"stdio-fixture",
                    "source":"local",
                    "type":"stdio",
                    "command":"/secret/command",
                    "args":["--token","argument-secret"],
                    "env":[{"name":"TOKEN","value":"environment-secret"}],
                    "session":{"enabled":true,"tools":[]}
                }
            ]
        });
        client
            .ext_notification(acp::ExtNotification::new(
                "x.ai/mcp/servers_updated",
                Arc::from(serde_json::value::to_raw_value(&payload).unwrap()),
            ))
            .await
            .expect("typed MCP catalog notification");
        let event = event_rx.recv().await.expect("redacted typed event");
        let serialized = serde_json::to_string(&event).expect("event serializes");
        for secret in [
            "url-secret",
            "setup-secret",
            "/secret/command",
            "argument-secret",
            "environment-secret",
            "tool-meta-secret",
            "extension-secret",
            "capability-raw-secret",
        ] {
            assert!(!serialized.contains(secret), "journal leaked {secret}");
        }
        assert!(host.notifications.lock().unwrap().is_empty());

        for (method, payload, secrets) in [
            (
                "x.ai/mcp/server_status",
                serde_json::json!({
                    "sessionId":"session-1",
                    "name":"fixture",
                    "source":"local",
                    "status":"unavailable",
                    "reason":"handshake_failed",
                    "detail":"status-detail-secret",
                    "tools":{"token":"status-tools-secret"},
                    "future":{"token":"status-raw-secret"}
                }),
                [
                    "status-detail-secret",
                    "status-tools-secret",
                    "status-raw-secret",
                ],
            ),
            (
                "x.ai/mcp/tools_changed",
                serde_json::json!({
                    "sessionId":"session-1",
                    "serverName":"fixture",
                    "tools":[{"name":"echo","_meta":{"token":"changed-meta-secret"}}],
                    "future":{"token":"changed-raw-secret"}
                }),
                ["changed-meta-secret", "changed-raw-secret", "unused-secret"],
            ),
            (
                "x.ai/mcp/init_progress",
                serde_json::json!({
                    "sessionId":"session-1",
                    "connected":1,
                    "total":2,
                    "future":{"token":"progress-raw-secret"}
                }),
                ["progress-raw-secret", "unused-secret", "unused-secret"],
            ),
        ] {
            client
                .ext_notification(acp::ExtNotification::new(
                    method,
                    Arc::from(serde_json::value::to_raw_value(&payload).unwrap()),
                ))
                .await
                .expect("known MCP notification");
            let event = event_rx.recv().await.expect("typed MCP event");
            let serialized = serde_json::to_string(&event).expect("event serializes");
            for secret in secrets {
                assert!(!serialized.contains(secret), "journal leaked {secret}");
            }
        }
        assert!(host.notifications.lock().unwrap().is_empty());

        client
            .ext_notification(acp::ExtNotification::new(
                "x.ai/mcp/future_catalog",
                Arc::from(
                    serde_json::value::to_raw_value(&serde_json::json!({
                        "sessionId":"session-1",
                        "futureSecret":"must-not-enter-an-untyped-fallback"
                    }))
                    .unwrap(),
                ),
            ))
            .await
            .expect("unknown MCP notifications are suppressed");
        assert!(matches!(
            event_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(host.notifications.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reverse_extension_transport_preserves_json_and_journals_notifications() {
        use agent_client_protocol::Client as _;

        let (events, mut event_rx) = mpsc::unbounded_channel();
        let host = Arc::new(EchoHost::default());
        let client = Client {
            events,
            sequences: Rc::new(RefCell::new(HashMap::new())),
            retained: Rc::new(RefCell::new(HashMap::new())),
            capacity: 4,
            host: Some(host.clone()),
            tool_permission_handler: None,
            host_extension_methods: HashSet::from(["host.desktop/screenshot".into()]),
            agent_hooks: HashMap::new(),
            turns: Rc::new(RefCell::new(HashMap::new())),
            turn_usages: Rc::new(RefCell::new(HashMap::new())),
            replay: Rc::new(RefCell::new(HashMap::new())),
        };
        let params = serde_json::json!({"nested":{"future":[1,true,null]}});
        let raw = serde_json::value::to_raw_value(&params).expect("raw request");
        let response = client
            .ext_method(acp::ExtRequest::new(
                "host.desktop/screenshot",
                Arc::from(raw),
            ))
            .await
            .expect("reverse request");
        let response: serde_json::Value =
            serde_json::from_str(response.0.get()).expect("response json");
        assert_eq!(response["method"], "host.desktop/screenshot");
        assert_eq!(response["params"], params);
        assert_eq!(response["host"], true);

        let raw =
            serde_json::value::to_raw_value(&serde_json::json!({})).expect("raw denied request");
        let denied = client
            .ext_method(acp::ExtRequest::new(
                "host.desktop/unadvertised",
                Arc::from(raw),
            ))
            .await
            .expect_err("unadvertised reverse methods fail closed");
        assert_eq!(i32::from(denied.code), -32601);

        let notification_params = serde_json::json!({"windowId":"w-1","dirty":true});
        let raw = serde_json::value::to_raw_value(&notification_params).expect("raw notification");
        client
            .ext_notification(acp::ExtNotification::new(
                "host.desktop/window_changed",
                Arc::from(raw),
            ))
            .await
            .expect("reverse notification");
        let event = event_rx.recv().await.expect("journal event");
        assert_eq!(event.session_id, SessionId::runtime_events());
        assert!(matches!(
            event.update,
            EventUpdate::Extension { method, payload, raw }
                if method == "host.desktop/window_changed"
                    && payload == notification_params
                    && serde_json::from_str::<serde_json::Value>(&raw).unwrap() == notification_params
        ));
        let notifications = host.notifications.lock().expect("notifications lock");
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].method, "host.desktop/window_changed");
        assert_eq!(notifications[0].params, notification_params);
    }

    struct RecordingHook(std::sync::Mutex<Vec<crate::AgentHookInvocation>>);
    #[async_trait::async_trait]
    impl crate::AgentHookHandler for RecordingHook {
        async fn handle(
            &self,
            invocation: crate::AgentHookInvocation,
        ) -> Result<crate::AgentHookResponse, crate::AgentHookError> {
            self.0.lock().unwrap().push(invocation);
            Ok(crate::AgentHookResponse {
                decision: crate::AgentHookDecision::Deny,
                system_message: Some("policy denied".into()),
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn reverse_hook_transport_is_typed_and_fails_closed() {
        use agent_client_protocol::Client as _;
        let (events, _) = mpsc::unbounded_channel();
        let hook = Arc::new(RecordingHook(std::sync::Mutex::new(Vec::new())));
        let client = Client {
            events,
            sequences: Rc::new(RefCell::new(HashMap::new())),
            retained: Rc::new(RefCell::new(HashMap::new())),
            capacity: 1,
            host: None,
            tool_permission_handler: None,
            host_extension_methods: HashSet::new(),
            agent_hooks: HashMap::from([("pre".into(), hook.clone() as _)]),
            turns: Rc::new(RefCell::new(HashMap::new())),
            turn_usages: Rc::new(RefCell::new(HashMap::new())),
            replay: Rc::new(RefCell::new(HashMap::new())),
        };
        let payload = serde_json::json!({
            "hookCallbackId":"pre", "hookEventName":"pre_tool_use",
            "sessionId":"s", "cwd":"/tmp", "toolName":"write_file",
            "toolUseId":"call", "toolInput":{"path":"a"}, "future":42
        });
        let response = client
            .ext_method(acp::ExtRequest::new(
                "x.ai/hooks/run",
                Arc::from(serde_json::value::to_raw_value(&payload).unwrap()),
            ))
            .await
            .unwrap();
        let response: serde_json::Value = serde_json::from_str(response.0.get()).unwrap();
        assert_eq!(response["decision"], "deny");
        assert_eq!(response["systemMessage"], "policy denied");
        let calls = hook.0.lock().unwrap();
        assert_eq!(calls[0].event, crate::AgentHookEvent::PreToolUse);
        assert_eq!(calls[0].tool_name.as_deref(), Some("write_file"));
        assert_eq!(calls[0].tool_input.as_ref().unwrap()["path"], "a");
        assert_eq!(calls[0].raw["future"], 42);
        drop(calls);

        let unknown = serde_json::json!({
            "hookCallbackId":"missing", "hookEventName":"post_tool_use", "sessionId":"s"
        });
        let error = client
            .ext_notification(acp::ExtNotification::new(
                "x.ai/hooks/event",
                Arc::from(serde_json::value::to_raw_value(&unknown).unwrap()),
            ))
            .await
            .unwrap_err();
        assert_eq!(i32::from(error.code), -32601);
    }
}
