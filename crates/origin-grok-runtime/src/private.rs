use crate::{
    ConversationRewindReceipt, ConversationRewindStatus, Error, Event, EventUpdate,
    LedgerTurnState, PromptReceipt, RewindPoint, RuntimeConfig, SessionConfig, SessionId,
    SessionLedger, SessionLedgerEntry, TurnOutcome,
};
use agent_client_protocol as acp;
use agent_client_protocol::Agent as _;
use indexmap::IndexMap;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
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
        config::{Config, ModelEntry, ModelEntryConfig},
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
    Prompt(SessionId, String, String, Reply<PromptReceipt>),
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
}
impl Runtime {
    pub async fn start(
        input: RuntimeConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        validate(&input)?;
        let (events, event_rx) = mpsc::unbounded_channel();
        let (commands, command_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let join = std::thread::Builder::new()
            .name("origin-grok-runtime".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                match rt {
                    Ok(rt) => {
                        let local = tokio::task::LocalSet::new();
                        local.block_on(&rt, async move {
                            match Core::start(input, events).await {
                                Ok(core) => {
                                    let _ = ready_tx.send(Ok(()));
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
        ready_rx.await.map_err(|_| Error::Shutdown)??;
        Ok((
            Self {
                shared: Arc::new(RuntimeShared {
                    commands,
                    join: tokio::sync::Mutex::new(Some(join)),
                    shutdown: AtomicBool::new(false),
                }),
            },
            event_rx,
        ))
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
    pub async fn prompt(
        &self,
        id: &SessionId,
        t: String,
        x: String,
    ) -> Result<PromptReceipt, Error> {
        self.call(|r| Command::Prompt(id.clone(), t, x, r)).await
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
    retained: Rc<RefCell<HashMap<String, Vec<Event>>>>,
    turns: Rc<RefCell<HashMap<String, String>>>,
    replay: Rc<RefCell<HashSet<String>>>,
}
impl Client {
    fn emit(&self, sid: String, update: EventUpdate) -> acp::Result<()> {
        let root_session_id = xai_grok_shell::origin_runtime::resolve_root_session(&sid, None)
            .ok_or_else(acp::Error::invalid_params)?;
        if self.replay.borrow().contains(&root_session_id) {
            return Ok(());
        }
        let mut seq = self.sequences.borrow_mut();
        let n = seq.entry(root_session_id.clone()).or_default();
        *n += 1;
        let event = Event {
            session_id: SessionId(root_session_id.clone()),
            sequence: *n,
            turn_id: self.turns.borrow().get(&root_session_id).cloned(),
            update,
        };
        self.retained
            .borrow_mut()
            .entry(root_session_id)
            .or_default()
            .push(event.clone());
        let _ = self.events.send(event);
        Ok(())
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
        _args: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
        // ACP requires this callback, but embedded sessions use the native
        // unrestricted handle and cannot reach it. Cancellation makes an
        // upstream invariant regression visible instead of adding a host path.
        Ok(acp::RequestPermissionResponse::new(
            acp::RequestPermissionOutcome::Cancelled,
        ))
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> acp::Result<()> {
        let update = match args.update {
            acp::SessionUpdate::UserMessageChunk(chunk) => content_update(
                chunk.content,
                EventUpdate::UserText,
                "user_message_non_text",
            ),
            acp::SessionUpdate::AgentMessageChunk(chunk) => content_update(
                chunk.content,
                EventUpdate::AssistantText,
                "agent_message_non_text",
            ),
            acp::SessionUpdate::AgentThoughtChunk(chunk) => content_update(
                chunk.content,
                EventUpdate::ThoughtText,
                "agent_thought_non_text",
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
            },
        };
        self.emit(args.session_id.0.to_string(), update)
    }
}

fn content_update(
    content: acp::ContentBlock,
    text: impl FnOnce(String) -> EventUpdate,
    non_text_tag: &'static str,
) -> EventUpdate {
    match content {
        acp::ContentBlock::Text(content) => text(content.text),
        _ => EventUpdate::Unknown {
            tag: non_text_tag.into(),
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
        if expected_entry.map(|entry| entry.prompt_digest.as_str())
            != point.origin_prompt_digest.as_deref()
        {
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
    retained: Rc<RefCell<HashMap<String, Vec<Event>>>>,
    resident: RefCell<HashSet<String>>,
    turns: Rc<RefCell<HashMap<String, String>>>,
    prompt_tasks: RefCell<HashMap<String, tokio::task::AbortHandle>>,
    replay: Rc<RefCell<HashSet<String>>>,
    ledger_root: std::path::PathBuf,
    rewind_root: std::path::PathBuf,
}
impl Core {
    async fn start(
        input: RuntimeConfig,
        events: mpsc::UnboundedSender<Event>,
    ) -> Result<Self, Error> {
        std::fs::create_dir_all(&input.grok_home).map_err(op)?;
        std::fs::create_dir_all(&input.session_storage).map_err(op)?;
        let mut cfg = Config::origin_embedded();
        cfg.skills.paths = Vec::new();
        cfg.endpoints.cli_chat_proxy_base_url = Some(input.endpoint.clone());
        cfg.endpoints.xai_api_base_url = input.endpoint.clone();
        cfg.endpoints.models_base_url = None;
        cfg.endpoints.models_list_url = None;
        cfg.default_model_override = input.models.first().map(|model| model.id.clone());
        let auth = Arc::new(AuthManager::new_origin_embedded(
            input.grok_home.join("origin-auth-disabled.json"),
            cfg.grok_com_config.clone(),
        ));
        let mut fixed = IndexMap::new();
        for model in &input.models {
            let entry: ModelEntryConfig = serde_json::from_value(serde_json::json!({
                "id": model.id,
                "model": model.id,
                "base_url": input.endpoint,
                "api_key": input.api_key,
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
        let agent = Rc::new(MvpAgent::with_origin_embedded_models(
            AcpAgentGatewaySender::new(gw_tx),
            &cfg,
            auth,
            models,
            input.session_storage.clone(),
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
        let replay = Rc::new(RefCell::new(HashSet::new()));
        let incoming = LineBufferedRead::spawn_local(a2c_b.compat());
        let client = Client {
            events: events.clone(),
            sequences: sequences.clone(),
            retained: retained.clone(),
            turns: turns.clone(),
            replay: replay.clone(),
        };
        let (connection, io) =
            acp::ClientSideConnection::new(client, c2a_a.compat_write(), incoming, |f| {
                tokio::task::spawn_local(f);
            });
        tokio::task::spawn_local(io);
        connection
            .initialize(acp::InitializeRequest::new(acp::ProtocolVersion::V1))
            .await
            .map_err(op)?;
        Ok(Self {
            connection,
            events,
            catalog: input
                .models
                .into_iter()
                .map(|model| (model.id.clone(), model))
                .collect(),
            sequences,
            retained,
            resident: RefCell::new(HashSet::new()),
            turns,
            prompt_tasks: RefCell::new(HashMap::new()),
            replay,
            ledger_root: input.session_storage.join("origin-turn-ledger"),
            rewind_root: input.session_storage.join("origin-rewind-receipts"),
        })
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
                Command::Prompt(i, t, x, r) => {
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
            "clientIdentifier": "origin-forge",
            "yoloMode": true,
        })
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Operation("failed to build session metadata".into()))
    }
    fn emit(&self, id: &SessionId, u: EventUpdate, t: Option<String>) {
        let mut s = self.sequences.borrow_mut();
        let n = s.entry(id.0.clone()).or_default();
        *n += 1;
        let event = Event {
            session_id: id.clone(),
            sequence: *n,
            turn_id: t,
            update: u,
        };
        self.retained
            .borrow_mut()
            .entry(id.0.clone())
            .or_default()
            .push(event.clone());
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
            .new_session(acp::NewSessionRequest::new(config.cwd).meta(meta))
            .await
            .map_err(op)?;
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
        self.check(&config)?;
        if self.resident.borrow().contains(&id.0) {
            return Err(Error::Operation("session is already resident".into()));
        }
        self.load_ledger(&id)?;
        let meta = self.session_meta(&config)?;
        struct ReplayGuard<'a>(&'a RefCell<HashSet<String>>, String);
        impl Drop for ReplayGuard<'_> {
            fn drop(&mut self) {
                self.0.borrow_mut().remove(&self.1);
            }
        }
        self.replay.borrow_mut().insert(id.0.clone());
        let _guard = ReplayGuard(&self.replay, id.0.clone());
        self.connection
            .load_session(
                acp::LoadSessionRequest::new(acp::SessionId::new(id.0.clone()), config.cwd)
                    .meta(meta),
            )
            .await
            .map_err(op)?;
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
        let prompt_digest = crate::prompt_digest(&x);
        ledger.entries.push(SessionLedgerEntry {
            turn_id: t.clone(),
            prompt_digest: prompt_digest.clone(),
            runtime_prompt_index,
            state: LedgerTurnState::Pending,
        });
        self.save_ledger(&id, &ledger)?;
        let req = acp::PromptRequest::new(
            acp::SessionId::new(id.0.clone()),
            vec![acp::ContentBlock::Text(acp::TextContent::new(x))],
        )
        .meta(serde_json::json!({"originTurnId":t}).as_object().cloned());
        let response = self.connection.prompt(req).await.map_err(op);
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
            .map_err(op)?;
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
            .map_err(op);
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
            .map_err(op)?;
        serde_json::from_str(response.0.get()).map_err(op)
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
            .map_err(op)?;
        Ok(())
    }
    async fn rewind_points(&self, id: SessionId) -> Result<Vec<RewindPoint>, Error> {
        let response: RewindPointsWire = self
            .extension(
                "x.ai/rewind/points",
                serde_json::json!({ "sessionId": id.0 }),
            )
            .await?;
        Ok(response
            .rewind_points
            .into_iter()
            .map(|point| RewindPoint {
                prompt_index: point.prompt_index,
                prompt_digest: point.origin_prompt_digest,
                created_at: point.created_at,
                file_snapshots: point.num_file_snapshots,
                has_file_changes: point.has_file_changes,
                prompt_preview: point.prompt_preview,
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
            if crate::prompt_digest(prompt_text) != expected_prompt_digest {
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
        self.require_resident(id)?;
        let current = self.sequences.borrow().get(&id.0).copied().unwrap_or(0);
        if sequence > current {
            return Err(Error::Operation(
                "event cursor is beyond the session sequence".into(),
            ));
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
        self.emit(&id, EventUpdate::SessionClosed, None);
        self.resident.borrow_mut().remove(&id.0);
        self.turns.borrow_mut().remove(&id.0);
        self.replay.borrow_mut().remove(&id.0);
        xai_grok_shell::origin_runtime::unregister_session_tree(&id.0);
        if response.drained {
            Ok(())
        } else {
            Err(Error::Operation(
                "native session detached but its actor missed the teardown deadline".into(),
            ))
        }
    }
}
fn validate(c: &RuntimeConfig) -> Result<(), Error> {
    if c.endpoint.trim().is_empty() || c.api_key.trim().is_empty() || c.models.is_empty() {
        return Err(Error::InvalidConfig(
            "endpoint, API key, and model catalog are required".into(),
        ));
    }
    if c.models
        .iter()
        .any(|m| m.id.trim().is_empty() || NonZeroU64::new(m.context_window).is_none())
    {
        return Err(Error::InvalidConfig(
            "model id and non-zero context window are required".into(),
        ));
    }
    Ok(())
}
fn op(e: impl std::fmt::Display) -> Error {
    Error::Operation(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
