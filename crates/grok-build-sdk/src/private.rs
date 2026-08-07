use crate::{
    ConversationRewindReceipt, ConversationRewindStatus, Error, Event, EventUpdate,
    ExtensionNotification, ExtensionRequest, ExtensionResponse, LedgerTurnState, Prompt,
    PromptBlock, PromptReceipt, RewindPoint, RuntimeCapabilities, RuntimeConfig, RuntimeOptions,
    SessionConfig, SessionId, SessionLedger, SessionLedgerEntry, TurnOutcome,
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
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use xai_acp_lib::{AcpAgentGatewayReceiver, AcpAgentGatewaySender, LineBufferedRead};
use xai_grok_shell::{
    agent::{
        config::{Config, ModelEntry, ModelEntryConfig, OriginMediaConfig},
        models::ModelsManager,
        mvp_agent::MvpAgent,
    },
    auth::AuthManager,
};

const BUFFER: usize = 8 * 1024 * 1024;
const CANCEL_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
type Reply<T> = oneshot::Sender<Result<T, Error>>;
type SessionMeta = serde_json::Map<String, serde_json::Value>;
enum Command {
    Create(SessionConfig, Reply<SessionId>),
    Load(SessionId, SessionConfig, Reply<()>),
    Resume(SessionId, SessionConfig, Reply<()>),
    Prompt(SessionId, String, String, Reply<PromptReceipt>),
    PromptContent(SessionId, String, Prompt, Reply<PromptReceipt>),
    Extension(ExtensionRequest, Reply<ExtensionResponse>),
    ExtensionNotification(ExtensionNotification, Reply<()>),
    SetMode(SessionId, String, Reply<()>),
    ListSessions(Reply<serde_json::Value>),
    EventsAfter(SessionId, u64, Reply<Vec<Event>>),
    Cancel(SessionId, Reply<()>),
    SessionLedger(SessionId, Reply<SessionLedger>),
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
    Close(SessionId, Reply<()>),
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
    shutdown: AtomicBool,
    capabilities: RuntimeCapabilities,
}
impl Runtime {
    pub async fn start(
        input: RuntimeConfig,
        options: RuntimeOptions,
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
                            match Core::start(input, options, events).await {
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
        self.call(|r| Command::Create(c, r)).await
    }
    pub async fn load_session(&self, id: SessionId, c: SessionConfig) -> Result<(), Error> {
        self.call(|r| Command::Load(id, c, r)).await
    }
    pub async fn resume_session(&self, id: SessionId, c: SessionConfig) -> Result<(), Error> {
        self.call(|r| Command::Resume(id, c, r)).await
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
    pub async fn extension_request(&self, x: ExtensionRequest) -> Result<ExtensionResponse, Error> {
        self.call(|r| Command::Extension(x, r)).await
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
    host_extension_methods: HashSet<String>,
    turns: Rc<RefCell<HashMap<String, String>>>,
    replay: Rc<RefCell<HashMap<String, ReplayMode>>>,
}

#[derive(Clone, Copy)]
enum ReplayMode {
    Capture,
    Suppress,
}

impl Client {
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

fn ledger_settlement_id(
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

#[async_trait::async_trait(?Send)]
impl acp::Client for Client {
    async fn request_permission(
        &self,
        args: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
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
        let raw = args.params.get().to_owned();
        let payload =
            serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::Value::String(raw.clone()));
        let root = payload
            .get("sessionId")
            .and_then(|value| value.as_str())
            .and_then(|session_id| {
                xai_grok_shell::origin_runtime::resolve_root_session(session_id, None)
            })
            .unwrap_or_else(|| SessionId::runtime_events().0);
        self.emit_root(
            root,
            EventUpdate::Extension {
                method: args.method.to_string(),
                payload: payload.clone(),
                raw: raw.clone(),
            },
        );
        if let Some(host) = &self.host {
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
    session_id: String,
}

impl Drop for TurnReservation {
    fn drop(&mut self) {
        self.turns.borrow_mut().remove(&self.session_id);
    }
}

struct Core {
    connection: acp::ClientSideConnection,
    events: mpsc::UnboundedSender<Event>,
    catalog: HashMap<String, crate::ModelSpec>,
    sequences: Rc<RefCell<HashMap<String, u64>>>,
    retained: Rc<RefCell<HashMap<String, VecDeque<Event>>>>,
    capacity: usize,
    options: RuntimeOptions,
    resident: RefCell<HashSet<String>>,
    turns: Rc<RefCell<HashMap<String, String>>>,
    prompt_tasks: RefCell<HashMap<String, tokio::task::AbortHandle>>,
    replay: Rc<RefCell<HashMap<String, ReplayMode>>>,
    ledger_root: std::path::PathBuf,
    rewind_root: std::path::PathBuf,
}
impl Core {
    async fn start(
        input: RuntimeConfig,
        options: RuntimeOptions,
        events: mpsc::UnboundedSender<Event>,
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
        let agent = Rc::new(MvpAgent::with_origin_embedded_profile_models(
            AcpAgentGatewaySender::new(gw_tx),
            &cfg,
            auth,
            models,
            input.session_storage.clone(),
            profile,
        ));
        let (c2a_a, c2a_b) = tokio::io::duplex(BUFFER);
        let (a2c_a, a2c_b) = tokio::io::duplex(BUFFER);
        let incoming = LineBufferedRead::spawn_local(c2a_b.compat());
        let (agent_conn, agent_io) =
            acp::AgentSideConnection::new(agent, a2c_a.compat_write(), incoming, |f| {
                tokio::task::spawn_local(f);
            });
        tokio::task::spawn_local(
            AcpAgentGatewayReceiver::new(gw_rx, agent_conn)
                .with_on_meta(xai_file_utils::trace_context::span_from_meta_traceparent)
                .run(),
        );
        tokio::task::spawn_local(agent_io);
        let sequences = Rc::new(RefCell::new(HashMap::new()));
        let retained = Rc::new(RefCell::new(HashMap::new()));
        let turns = Rc::new(RefCell::new(HashMap::new()));
        let replay = Rc::new(RefCell::new(HashMap::new()));
        let incoming = LineBufferedRead::spawn_local(a2c_b.compat());
        let client = Client {
            events: events.clone(),
            sequences: sequences.clone(),
            retained: retained.clone(),
            capacity: options.event_journal_capacity,
            host: options.host.clone(),
            host_extension_methods: options
                .host_capabilities
                .extension_methods
                .iter()
                .cloned()
                .collect(),
            turns: turns.clone(),
            replay: replay.clone(),
        };
        let (connection, io) =
            acp::ClientSideConnection::new(client, c2a_a.compat_write(), incoming, |f| {
                tokio::task::spawn_local(f);
            });
        tokio::task::spawn_local(io);
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
        let initialize = connection
            .initialize(
                acp::InitializeRequest::new(acp::ProtocolVersion::V1)
                    .client_capabilities(client_caps),
            )
            .await
            .map_err(|error| protocol("initialize", error))?;
        let capabilities =
            capabilities_for(&options, serde_json::to_value(&initialize).map_err(op)?);
        Ok((
            Self {
                connection,
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
                turns,
                prompt_tasks: RefCell::new(HashMap::new()),
                replay,
                ledger_root: input.session_storage.join("origin-turn-ledger"),
                rewind_root: input.session_storage.join("origin-rewind-receipts"),
            },
            capabilities,
        ))
    }
    async fn run(self: Rc<Self>, mut rx: mpsc::UnboundedReceiver<Command>) {
        while let Some(c) = rx.recv().await {
            match c {
                Command::Create(x, r) => {
                    let _ = r.send(self.create(x).await);
                }
                Command::Load(i, x, r) => {
                    let _ = r.send(self.load(i, x).await);
                }
                Command::Resume(i, x, r) => {
                    let _ = r.send(self.resume(i, x).await);
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
                        session_id: task_session_id.clone(),
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
                        session_id: task_session_id.clone(),
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
                Command::Extension(x, r) => {
                    let _ = r.send(self.extension_raw(x).await);
                }
                Command::ExtensionNotification(x, r) => {
                    let _ = r.send(self.extension_notification(x).await);
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
    fn check(&self, config: &SessionConfig) -> Result<(), Error> {
        self.check_model(&config.model, config.reasoning.as_deref())?;
        if !config.cwd.is_absolute() || !config.cwd.is_dir() {
            return Err(Error::InvalidConfig(
                "session cwd must be an existing absolute directory".into(),
            ));
        }
        Ok(())
    }
    fn session_meta(&self, config: &SessionConfig) -> Result<SessionMeta, Error> {
        serde_json::json!({
            "modelId": config.model,
            "reasoningEffort": config.reasoning,
            "clientIdentifier": self.options.client_identifier,
            "yoloMode": self.options.yolo_mode,
        })
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Operation("failed to build session metadata".into()))
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

    async fn create(&self, config: SessionConfig) -> Result<SessionId, Error> {
        self.check(&config)?;
        let meta = self.session_meta(&config)?;
        let x = self
            .connection
            .new_session(
                acp::NewSessionRequest::new(config.cwd)
                    .mcp_servers(self.mcp_servers())
                    .meta(meta),
            )
            .await
            .map_err(|error| protocol("session/new", error))?;
        let id = SessionId(x.session_id.0.to_string());
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
            let ledger_cleanup = std::fs::remove_file(self.ledger_path(&id));
            let mut detail =
                "native session identity collided with an existing embedded root".to_owned();
            if let Err(error) = cleanup {
                detail.push_str(&format!("; native session cleanup failed: {error}"));
            }
            if let Err(error) = ledger_cleanup {
                detail.push_str(&format!("; native Turn ledger cleanup failed: {error}"));
            }
            return Err(Error::Operation(detail));
        }
        self.resident.borrow_mut().insert(id.0.clone());
        self.emit(&id, EventUpdate::SessionStarted, None);
        Ok(id)
    }
    async fn load(&self, id: SessionId, config: SessionConfig) -> Result<(), Error> {
        self.attach(id, config, false).await
    }

    async fn resume(&self, id: SessionId, config: SessionConfig) -> Result<(), Error> {
        self.attach(id, config, true).await
    }

    async fn attach(
        &self,
        id: SessionId,
        config: SessionConfig,
        resume: bool,
    ) -> Result<(), Error> {
        self.check(&config)?;
        if self.resident.borrow().contains(&id.0) {
            return Err(Error::Operation("session is already resident".into()));
        }
        self.load_ledger(&id)?;
        let meta = self.session_meta(&config)?;
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
            self.connection
                .resume_session(
                    acp::ResumeSessionRequest::new(acp::SessionId::new(id.0.clone()), config.cwd)
                        .mcp_servers(self.mcp_servers())
                        .meta(meta),
                )
                .await
                .map_err(|error| protocol("session/resume", error))?;
        } else {
            self.connection
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
        )
        .await
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
        self.prompt_wire(id, t, blocks, digest, prompt.metadata)
            .await
    }
    async fn prompt_wire(
        &self,
        id: SessionId,
        t: String,
        blocks: Vec<acp::ContentBlock>,
        prompt_digest: String,
        metadata: serde_json::Value,
    ) -> Result<PromptReceipt, Error> {
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
        let req = acp::PromptRequest::new(acp::SessionId::new(id.0.clone()), blocks).meta(
            serde_json::json!({
                "originTurnId":t,
                "originPromptDigest": prompt_digest,
                "originMetadata": metadata
            })
            .as_object()
            .cloned(),
        );
        let response = self
            .connection
            .prompt(req)
            .await
            .map_err(|error| protocol("session/prompt", error));
        let outcome = match response?.stop_reason {
            acp::StopReason::EndTurn => TurnOutcome::End,
            acp::StopReason::Cancelled => TurnOutcome::Cancelled,
            acp::StopReason::MaxTokens => TurnOutcome::MaxTokens,
            acp::StopReason::MaxTurnRequests => TurnOutcome::MaxTurnRequests,
            acp::StopReason::Refusal => TurnOutcome::Refusal,
            _ => return Err(Error::Operation("unrecognized Grok stop reason".into())),
        };
        let raw = serde_json::value::RawValue::from_string(
            serde_json::json!({"sessionId": id.0}).to_string(),
        )
        .map_err(op)?;
        self.connection
            .ext_method(acp::ExtRequest::new("origin/session/sync", Arc::from(raw)))
            .await
            .map_err(|error| protocol("origin/session/sync", error))?;
        let settlement_id =
            ledger_settlement_id(&id.0, &t, &prompt_digest, runtime_prompt_index, outcome);
        ledger
            .entries
            .last_mut()
            .expect("the pending ledger entry was just appended")
            .state = LedgerTurnState::Completed {
            outcome,
            settlement_id: settlement_id.clone(),
        };
        self.save_ledger(&id, &ledger)?;
        self.emit(&id, EventUpdate::TurnFinished(outcome), Some(t));
        let final_sequence = *self
            .sequences
            .borrow()
            .get(&id.0)
            .ok_or_else(|| Error::Operation("session event sequence is unavailable".into()))?;
        Ok(PromptReceipt {
            outcome,
            final_sequence,
            runtime_prompt_index,
            settlement_id,
        })
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

    fn ledger_path(&self, id: &SessionId) -> std::path::PathBuf {
        use sha2::Digest as _;
        let digest = sha2::Sha256::digest(id.0.as_bytes());
        self.ledger_root.join(format!("{:x}.json", digest))
    }

    fn load_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        let path = self.ledger_path(id);
        let bytes = std::fs::read(&path).map_err(|error| {
            Error::Operation(format!(
                "native Turn ledger is unavailable for session reconciliation: {error}"
            ))
        })?;
        serde_json::from_slice(&bytes).map_err(op)
    }

    fn save_ledger(&self, id: &SessionId, ledger: &SessionLedger) -> Result<(), Error> {
        use std::io::Write as _;
        std::fs::create_dir_all(&self.ledger_root).map_err(op)?;
        let path = self.ledger_path(id);
        let temporary = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(ledger).map_err(op)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(op)?;
        file.write_all(&bytes).map_err(op)?;
        file.sync_all().map_err(op)?;
        drop(file);
        std::fs::rename(&temporary, &path).map_err(op)?;
        #[cfg(unix)]
        std::fs::File::open(&self.ledger_root)
            .and_then(|directory| directory.sync_all())
            .map_err(op)?;
        Ok(())
    }
    async fn cancel(&self, id: SessionId) -> Result<(), Error> {
        let session_id = id.0.clone();
        let result = self
            .connection
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
            .connection
            .ext_method(acp::ExtRequest::new(method, Arc::from(raw)))
            .await
            .map_err(|error| protocol(method, error))?;
        serde_json::from_str(response.0.get()).map_err(op)
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
        let raw = serde_json::value::to_raw_value(&request.params).map_err(op)?;
        let response = self
            .connection
            .ext_method(acp::ExtRequest::new(request.method.clone(), Arc::from(raw)))
            .await
            .map_err(|e| protocol(&request.method, e))?;
        Ok(ExtensionResponse {
            result: serde_json::from_str(response.0.get()).map_err(op)?,
        })
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
        self.connection
            .ext_notification(acp::ExtNotification::new(
                request.method.clone(),
                Arc::from(raw),
            ))
            .await
            .map_err(|e| protocol(&request.method, e))
    }
    async fn set_mode(&self, id: SessionId, mode: String) -> Result<(), Error> {
        self.connection
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
            .connection
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
        self.check_model(&model, reasoning.as_deref())?;
        if self.turns.borrow().contains_key(&id.0) {
            return Err(Error::Operation(
                "cannot change model during an active prompt".into(),
            ));
        }
        let meta = serde_json::json!({
            "reasoningEffort": reasoning,
            "originRouteOnly": true,
        })
        .as_object()
        .cloned();
        self.connection
            .set_session_model(
                acp::SetSessionModelRequest::new(
                    acp::SessionId::new(id.0),
                    acp::ModelId::new(model),
                )
                .meta(meta),
            )
            .await
            .map_err(|error| protocol("session/set_model", error))?;
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
        match std::fs::remove_file(self.rewind_intent_path(&receipt.operation_id)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::Operation(format!(
                    "rewind applied and receipt was saved, but intent cleanup failed: {error}"
                )));
            }
        }
        Ok(receipt)
    }

    fn rewind_intent_path(&self, operation_id: &str) -> std::path::PathBuf {
        use sha2::Digest as _;
        self.rewind_root.join(format!(
            "{:x}.intent.json",
            sha2::Sha256::digest(operation_id.as_bytes())
        ))
    }

    fn load_rewind_intent(&self, operation_id: &str) -> Result<Option<RewindIntent>, Error> {
        match std::fs::read(self.rewind_intent_path(operation_id)) {
            Ok(bytes) => {
                let intent: RewindIntent = serde_json::from_slice(&bytes).map_err(op)?;
                if intent.operation_id != operation_id {
                    return Err(Error::Operation("rewind intent digest mismatch".into()));
                }
                Ok(Some(intent))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(op(error)),
        }
    }

    fn save_rewind_intent(&self, intent: &RewindIntent) -> Result<(), Error> {
        self.save_rewind_document(
            &self.rewind_intent_path(&intent.operation_id),
            &serde_json::to_vec(intent).map_err(op)?,
        )
    }

    fn rewind_receipt_path(&self, operation_id: &str) -> std::path::PathBuf {
        use sha2::Digest as _;
        self.rewind_root.join(format!(
            "{:x}.json",
            sha2::Sha256::digest(operation_id.as_bytes())
        ))
    }

    fn rewind_status(
        &self,
        id: &SessionId,
        operation_id: &str,
    ) -> Result<ConversationRewindStatus, Error> {
        match std::fs::read(self.rewind_receipt_path(operation_id)) {
            Ok(bytes) => {
                let receipt: ConversationRewindReceipt =
                    serde_json::from_slice(&bytes).map_err(op)?;
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
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(intent) = self.load_rewind_intent(operation_id)? else {
                    return Ok(ConversationRewindStatus::Absent);
                };
                if intent.session_id != id.0 {
                    return Err(Error::Operation(
                        "rewind intent belongs to a different native session".into(),
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
            Err(error) => Err(op(error)),
        }
    }

    fn save_rewind_receipt(&self, receipt: &ConversationRewindReceipt) -> Result<(), Error> {
        self.save_rewind_document(
            &self.rewind_receipt_path(&receipt.operation_id),
            &serde_json::to_vec(receipt).map_err(op)?,
        )
    }

    fn save_rewind_document(&self, path: &std::path::Path, bytes: &[u8]) -> Result<(), Error> {
        use std::io::Write as _;
        std::fs::create_dir_all(&self.rewind_root).map_err(op)?;
        let temporary = path.with_extension("json.tmp");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(op)?;
        file.write_all(bytes).map_err(op)?;
        file.sync_all().map_err(op)?;
        drop(file);
        std::fs::rename(temporary, path).map_err(op)?;
        #[cfg(unix)]
        std::fs::File::open(&self.rewind_root)
            .and_then(|directory| directory.sync_all())
            .map_err(op)?;
        Ok(())
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
        self.connection
            .close_session(acp::CloseSessionRequest::new(acp::SessionId::new(
                id.0.clone(),
            )))
            .await
            .map_err(|error| protocol("session/close", error))?;
        self.finish_close(&id);
        Ok(())
    }
    async fn unload(&self, id: SessionId) -> Result<(), Error> {
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
        self.finish_close(&id);
        if response.drained {
            Ok(())
        } else {
            Err(Error::Operation(
                "native session detached but its actor missed the teardown deadline".into(),
            ))
        }
    }
    fn finish_close(&self, id: &SessionId) {
        self.emit(id, EventUpdate::SessionClosed, None);
        self.resident.borrow_mut().remove(&id.0);
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
    let mut mcp_names = HashSet::new();
    if options.services.mcp_servers.iter().any(|server| {
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
    meta.insert(
        "originHostExtensionMethods".into(),
        serde_json::to_value(&options.host_capabilities.extension_methods).map_err(op)?,
    );
    Ok(meta)
}

fn capabilities_for(
    options: &RuntimeOptions,
    initialize: serde_json::Value,
) -> RuntimeCapabilities {
    const FAMILIES: &[&str] = &[
        "auth",
        "session",
        "git",
        "worktree",
        "plugins",
        "marketplace",
        "hooks",
        "hunk",
        "pr",
        "mcp",
        "task",
        "scheduler",
        "subagent",
        "terminal",
        "fs",
        "search",
        "bundle",
        "code",
        "skills",
        "workflows",
        "review",
        "debug",
        "rewind",
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
    let mut descriptors = FAMILIES
        .iter()
        .map(|family| crate::CapabilityDescriptor {
            namespace: format!("x.ai/{family}"),
            // This describes route availability. Individual operations can
            // still reject based on profile, auth, session state, or platform.
            enabled: options.profile == crate::RuntimeProfile::Desktop,
            disabled_reason: (options.profile == crate::RuntimeProfile::Restricted)
                .then(|| "generic extensions require the Desktop profile".into()),
            effect_class: extension_effect(family).into(),
            host_requirement: None,
        })
        .collect::<Vec<_>>();
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
                    "managed MCP is an account-product service; configure explicit MCP transports instead".into()
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
        protocol_version: "1".into(),
        initialize,
        profile: options.profile,
        host: options.host_capabilities.clone(),
        generic_extension_transport: options.profile == crate::RuntimeProfile::Desktop,
        extension_families: descriptors,
    }
}

fn extension_effect(family: &str) -> &'static str {
    match family {
        "session" | "search" | "code" | "skills" => "read",
        "git" | "worktree" | "hunk" | "rewind" | "fs" => "workspace-write",
        "terminal" | "task" | "scheduler" | "subagent" | "workflows" => "process",
        "auth" | "marketplace" | "mcp" | "pr" => "network",
        "plugins" | "hooks" | "bundle" => "process-write",
        "review" | "debug" => "diagnostic",
        _ => "extension-defined",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            host_extension_methods: HashSet::from(["host.desktop/screenshot".into()]),
            turns: Rc::new(RefCell::new(HashMap::new())),
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
}
