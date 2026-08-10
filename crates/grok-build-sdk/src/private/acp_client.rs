use super::*;

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

pub(super) fn content_update(
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
    ) -> acp::Result<()> {
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

pub(super) fn validate_mcp_response(
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

pub(super) fn host_acp_error(error: crate::HostError) -> acp::Error {
    acp::Error::new(error.code, error.message).data(error.data)
}
pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
