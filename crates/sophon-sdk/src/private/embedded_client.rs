//! SDK-side callbacks for the shell's protocol-neutral embedded facade.

use super::*;

type EmbeddedResult<T> = Result<T, EmbeddedError>;

pub(super) struct Client {
    pub(super) events: mpsc::UnboundedSender<Event>,
    pub(super) sequences: Rc<RefCell<HashMap<String, u64>>>,
    pub(super) retained: Rc<RefCell<HashMap<String, VecDeque<Event>>>>,
    pub(super) capacity: usize,
    pub(super) host: Option<Arc<dyn crate::HostDelegate>>,
    pub(super) tool_permission_handler: Option<Arc<dyn crate::ToolPermissionHandler>>,
    pub(super) host_extension_methods: HashSet<String>,
    pub(super) agent_hooks: HashMap<String, Arc<dyn crate::AgentHookHandler>>,
    pub(super) turns: Rc<RefCell<HashMap<String, String>>>,
    pub(super) turn_usages: TurnUsageMap,
    pub(super) replay: Rc<RefCell<HashMap<String, ReplayMode>>>,
}

fn required_string(value: &serde_json::Value, key: &str) -> EmbeddedResult<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(EmbeddedError::invalid_params)
}

fn debug_name(value: Option<&str>) -> String {
    value
        .unwrap_or_default()
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(char::to_uppercase)
                .into_iter()
                .flatten()
                .chain(chars)
                .collect::<String>()
        })
        .collect()
}

pub(super) fn content_update(
    content: &serde_json::Value,
    text: impl FnOnce(String) -> EventUpdate,
    non_text_tag: &'static str,
    payload: &serde_json::Value,
    raw: &str,
) -> EventUpdate {
    if content.get("type").and_then(serde_json::Value::as_str) == Some("text")
        && let Some(value) = content.get("text").and_then(serde_json::Value::as_str)
    {
        return text(value.to_owned());
    }
    EventUpdate::Unknown {
        tag: non_text_tag.into(),
        payload: payload.clone(),
        raw: raw.into(),
    }
}

#[derive(Clone, Copy)]
pub(super) enum ReplayMode {
    Capture,
    Suppress,
}

impl Client {
    pub(super) fn capture_turn_usage(
        &self,
        session_id: &str,
        update: &serde_json::Value,
    ) -> EmbeddedResult<()> {
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
            .map_err(|_| EmbeddedError::invalid_params())?;
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
        args: &serde_json::Value,
    ) -> EmbeddedResult<crate::ToolPermissionRequest> {
        let tool = args
            .get("toolCall")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(EmbeddedError::invalid_params)?;
        let raw_tool = serde_json::Value::Object(tool.clone());
        let tool_kind =
            tool.get("kind")
                .and_then(serde_json::Value::as_str)
                .map(|kind| match kind {
                    "read" => crate::ToolKind::Read,
                    "edit" => crate::ToolKind::Edit,
                    "delete" => crate::ToolKind::Delete,
                    "move" => crate::ToolKind::Move,
                    "search" => crate::ToolKind::Search,
                    "execute" => crate::ToolKind::Execute,
                    "think" => crate::ToolKind::Think,
                    "fetch" => crate::ToolKind::Fetch,
                    "switch_mode" => crate::ToolKind::SwitchMode,
                    _ => crate::ToolKind::Other,
                });
        let status = tool
            .get("status")
            .and_then(serde_json::Value::as_str)
            .map(|status| match status {
                "pending" => crate::ToolCallStatus::Pending,
                "in_progress" => crate::ToolCallStatus::InProgress,
                "completed" => crate::ToolCallStatus::Completed,
                "failed" => crate::ToolCallStatus::Failed,
                _ => crate::ToolCallStatus::Other,
            });
        let options = args
            .get("options")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(EmbeddedError::invalid_params)?
            .iter()
            .map(|option| {
                let raw_kind = required_string(option, "kind")?;
                let kind = match raw_kind.as_str() {
                    "allow_once" => crate::ToolPermissionOptionKind::AllowOnce,
                    "allow_always" => crate::ToolPermissionOptionKind::AllowAlways,
                    "reject_once" => crate::ToolPermissionOptionKind::RejectOnce,
                    "reject_always" => crate::ToolPermissionOptionKind::RejectAlways,
                    _ => crate::ToolPermissionOptionKind::Other,
                };
                Ok(crate::ToolPermissionOption {
                    id: required_string(option, "optionId")?,
                    name: required_string(option, "name")?,
                    kind,
                    raw_kind,
                    meta: option.get("_meta").cloned(),
                    raw: option.clone(),
                })
            })
            .collect::<EmbeddedResult<Vec<_>>>()?;
        Ok(crate::ToolPermissionRequest {
            session_id: required_string(args, "sessionId")?,
            tool_call: crate::ToolCallSummary {
                id: required_string(&raw_tool, "toolCallId")?,
                title: tool
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                kind: tool_kind,
                status,
                raw_input: tool.get("rawInput").cloned(),
                raw_output: tool.get("rawOutput").cloned(),
                raw: raw_tool,
            },
            options,
            raw: args.clone(),
        })
    }

    async fn dispatch_agent_hook(
        &self,
        value: serde_json::Value,
    ) -> EmbeddedResult<crate::AgentHookResponse> {
        let callback_id = required_string(&value, "hookCallbackId")?;
        let event: crate::AgentHookEvent = serde_json::from_value(
            value
                .get("hookEventName")
                .cloned()
                .ok_or_else(EmbeddedError::invalid_params)?,
        )
        .map_err(|_| EmbeddedError::invalid_params())?;
        let session_id = required_string(&value, "sessionId")?;
        let handler = self
            .agent_hooks
            .get(&callback_id)
            .ok_or_else(EmbeddedError::method_not_found)?;
        let string = |key: &str| value.get(key).and_then(|v| v.as_str()).map(str::to_owned);
        let invocation = crate::AgentHookInvocation {
            event,
            callback_id,
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
            .map_err(|_| EmbeddedError::internal_error())
    }

    async fn host_call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> EmbeddedResult<serde_json::Value> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(EmbeddedError::method_not_found)?;
        host.request(crate::HostRequest {
            method: method.into(),
            params,
        })
        .await
        .map_err(host_embedded_error)
    }

    fn emit(&self, sid: String, update: EventUpdate) -> EmbeddedResult<()> {
        let root_session_id = xai_grok_shell::origin_runtime::resolve_root_session(&sid, None)
            .or_else(|| self.replay.borrow().contains_key(&sid).then(|| sid.clone()))
            .ok_or_else(EmbeddedError::invalid_params)?;
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

    async fn request_permission(
        &self,
        args: serde_json::Value,
    ) -> EmbeddedResult<serde_json::Value> {
        if let Some(handler) = &self.tool_permission_handler {
            let request = Self::typed_permission_request(&args)?;
            let valid_ids: HashSet<_> = request.options.iter().map(|o| o.id.clone()).collect();
            let decision = handler
                .request_permission(request)
                .await
                .map_err(|_| EmbeddedError::internal_error())?;
            let outcome = match decision {
                crate::ToolPermissionDecision::Cancelled => {
                    serde_json::json!({"outcome": "cancelled"})
                }
                crate::ToolPermissionDecision::Selected(id) if valid_ids.contains(&id) => {
                    serde_json::json!({"outcome": "selected", "optionId": id})
                }
                crate::ToolPermissionDecision::Selected(id) => {
                    return Err(EmbeddedError::invalid_params()
                        .with_data(serde_json::json!({"invalidPermissionOptionId": id})));
                }
            };
            return Ok(serde_json::json!({"outcome": outcome}));
        }
        if self.host.is_none() {
            return Ok(serde_json::json!({"outcome": {"outcome": "cancelled"}}));
        }
        self.host_call("session/request_permission", args).await
    }

    fn session_notification(&self, args: serde_json::Value) -> EmbeddedResult<()> {
        let session_id = required_string(&args, "sessionId")?;
        let payload = args
            .get("update")
            .cloned()
            .ok_or_else(EmbeddedError::invalid_params)?;
        let raw = serde_json::to_string(&payload).unwrap_or_else(|_| "null".into());
        self.capture_turn_usage(&session_id, &payload)?;
        let tag = payload
            .get("sessionUpdate")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let update = match tag {
            "user_message_chunk" => content_update(
                &payload["content"],
                EventUpdate::UserText,
                "user_message_non_text",
                &payload,
                &raw,
            ),
            "agent_message_chunk" => content_update(
                &payload["content"],
                EventUpdate::AssistantText,
                "agent_message_non_text",
                &payload,
                &raw,
            ),
            "agent_thought_chunk" => content_update(
                &payload["content"],
                EventUpdate::ThoughtText,
                "agent_thought_non_text",
                &payload,
                &raw,
            ),
            "tool_call" => EventUpdate::ToolStart(tool_event(&payload)),
            "tool_call_update" => EventUpdate::ToolUpdate(tool_event(&payload)),
            "plan" => EventUpdate::Plan {
                summary: payload["entries"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|entry| {
                        format!(
                            "[{}/{}] {}",
                            debug_name(entry["status"].as_str()),
                            debug_name(entry["priority"].as_str()),
                            entry["content"].as_str().unwrap_or_default()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            },
            "available_commands_update" => EventUpdate::AvailableCommands(
                payload["availableCommands"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|command| crate::RuntimeCommand {
                        name: command["name"].as_str().unwrap_or_default().to_owned(),
                        description: command["description"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned(),
                    })
                    .collect(),
            ),
            "current_mode_update" => EventUpdate::ModeChanged(
                payload["currentModeId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            ),
            "config_option_update" => EventUpdate::ConfigOptions(
                payload["configOptions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|option| crate::RuntimeConfigOption {
                        id: option["id"].as_str().unwrap_or_default().to_owned(),
                        name: option["name"].as_str().unwrap_or_default().to_owned(),
                        category: option["category"].as_str().map(str::to_owned),
                        value: (option["type"].as_str() == Some("select"))
                            .then(|| option["currentValue"].as_str().map(str::to_owned))
                            .flatten(),
                    })
                    .collect(),
            ),
            "session_info_update" => EventUpdate::SessionInfo {
                title: payload
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
            },
            _ => EventUpdate::Unknown {
                tag: "unrecognized".into(),
                payload,
                raw,
            },
        };
        self.emit(session_id, update)
    }

    async fn extension_request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> EmbeddedResult<serde_json::Value> {
        if method == "x.ai/hooks/run" {
            return serde_json::to_value(self.dispatch_agent_hook(params).await?)
                .map_err(|_| EmbeddedError::internal_error());
        }
        if !self.host_extension_methods.contains(method) {
            return Err(EmbeddedError::method_not_found());
        }
        self.host_call(method, params).await
    }

    async fn extension_notification(
        &self,
        method: &str,
        payload: serde_json::Value,
    ) -> EmbeddedResult<()> {
        if method == "x.ai/hooks/event" {
            self.dispatch_agent_hook(payload).await?;
            return Ok(());
        }
        let raw = serde_json::to_string(&payload).unwrap_or_else(|_| "null".into());
        if method == "x.ai/session_notification"
            && let (Some(session_id), Some(update)) = (
                payload.get("sessionId").and_then(serde_json::Value::as_str),
                payload.get("update"),
            )
        {
            self.capture_turn_usage(session_id, update)?;
        }
        let root = payload
            .get("sessionId")
            .and_then(serde_json::Value::as_str)
            .and_then(|session_id| {
                xai_grok_shell::origin_runtime::resolve_root_session(session_id, None)
            })
            .unwrap_or_else(|| SessionId::runtime_events().0);
        let is_mcp_notification = method.starts_with("x.ai/mcp/");
        let update = match typed_mcp_notification(method, &payload) {
            Some(update) => update,
            None if is_mcp_notification => return Ok(()),
            None => EventUpdate::Extension {
                method: method.to_owned(),
                payload: payload.clone(),
                raw,
            },
        };
        self.emit_root(root, update);
        if !is_mcp_notification && let Some(host) = &self.host {
            host.notification(crate::HostNotification {
                method: method.to_owned(),
                params: payload,
            })
            .await
            .map_err(host_embedded_error)?;
        }
        Ok(())
    }
}

fn tool_event(payload: &serde_json::Value) -> crate::ToolEvent {
    crate::ToolEvent {
        id: payload["toolCallId"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        title: payload["title"].as_str().unwrap_or_default().to_owned(),
        kind: debug_name(payload["kind"].as_str()),
        status: debug_name(payload["status"].as_str()),
        raw_input: payload.get("rawInput").map(serde_json::Value::to_string),
        raw_output: payload.get("rawOutput").map(serde_json::Value::to_string),
    }
}

#[async_trait::async_trait(?Send)]
impl xai_grok_shell::embedded::EmbeddedClient for Client {
    async fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> EmbeddedResult<serde_json::Value> {
        match method {
            "session/request_permission" => self.request_permission(params).await,
            "fs/read_text_file"
            | "fs/write_text_file"
            | "terminal/create"
            | "terminal/output"
            | "terminal/wait_for_exit"
            | "terminal/kill"
            | "terminal/release" => self.host_call(method, params).await,
            _ => self.extension_request(method, params).await,
        }
    }

    async fn notification(&self, method: &str, params: serde_json::Value) -> EmbeddedResult<()> {
        if method == "session/update" {
            self.session_notification(params)
        } else {
            self.extension_notification(method, params).await
        }
    }
}

pub(super) fn validate_mcp_response(
    request: &serde_json::Value,
    response: &serde_json::Value,
) -> EmbeddedResult<()> {
    let Some(request_id) = request.get("id") else {
        return if response.is_null() {
            Ok(())
        } else {
            Err(EmbeddedError::internal_error())
        };
    };
    let object = response
        .as_object()
        .ok_or_else(EmbeddedError::internal_error)?;
    if object.get("jsonrpc").and_then(|v| v.as_str()) == Some("2.0")
        && object.get("id") == Some(request_id)
        && (object.contains_key("result") ^ object.contains_key("error"))
    {
        Ok(())
    } else {
        Err(EmbeddedError::internal_error())
    }
}

pub(super) fn typed_mcp_notification(
    method: &str,
    payload: &serde_json::Value,
) -> Option<EventUpdate> {
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
            let identity = crate::McpTaskIdentity::new(session_id, server, task_id).ok()?;
            let status = crate::parse_task_status(task.get("status")?).ok()?;
            let status_message = match task.get("statusMessage") {
                Some(serde_json::Value::Null) | None => None,
                Some(value) => Some(
                    value
                        .as_str()
                        .filter(|message| {
                            message.len() <= crate::MAX_MCP_TASK_STATUS_MESSAGE_BYTES
                        })?
                        .to_owned(),
                ),
            };
            let last_updated_at = task
                .get("lastUpdatedAt")?
                .as_str()
                .filter(|value| crate::valid_bounded_line(value, 128))?
                .to_owned();
            Some(EventUpdate::McpTaskStatus(crate::McpTaskStatusEvent {
                handle: crate::McpTaskHandle {
                    session_id: identity.session_id().clone(),
                    server: identity.server().to_owned(),
                    client_id,
                    task_id: identity.task_id().to_owned(),
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

pub(super) fn host_embedded_error(error: crate::HostError) -> EmbeddedError {
    EmbeddedError::new(error.code, error.message, error.data)
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
