// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use crate::*;

fn validate_non_ui_responses(
    input: &McpInputRequired,
    responses: &McpInputResponses,
) -> Result<(), Error> {
    validate_mcp_input_responses(responses)?;
    let requests = input.validated_requests()?;
    for key in responses.keys() {
        let request = requests
            .iter()
            .find(|request| request.id == *key)
            .ok_or_else(|| {
                Error::InvalidConfig("MCP input responses contain an unrequested identity".into())
            })?;
        if request.kind == McpInputRequestKind::Elicitation {
            return Err(Error::InvalidConfig(
                "MCP elicitation answers are accepted only from the product UI channel".into(),
            ));
        }
    }
    if requests.iter().any(|request| {
        request.kind != McpInputRequestKind::Elicitation && !responses.contains_key(&request.id)
    }) {
        return Err(Error::InvalidConfig(
            "every non-elicitation MCP input request must be answered exactly once".into(),
        ));
    }
    Ok(())
}

impl Runtime {
    async fn mcp_ext(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        self.typed_ext(method, params).await
    }
    /// Returns a redacted catalog for the explicit session. Restricted runtimes fail closed.
    pub async fn list_mcp_servers(
        &self,
        id: &SessionId,
        refresh: bool,
    ) -> Result<Vec<McpServerSummary>, Error> {
        let value = self
            .mcp_ext(
                "x.ai/mcp/list",
                serde_json::json!({
                    "sessionId": id.as_str(),
                    "cache": !refresh,
                    "requireSession": true
                }),
            )
            .await?;
        parse_mcp_servers(&value)
    }
    /// Lists tools currently visible in an explicit session, optionally filtered by server.
    pub async fn list_mcp_tools(
        &self,
        id: &SessionId,
        server: Option<&str>,
    ) -> Result<Vec<McpToolInfo>, Error> {
        let mut tools: Vec<_> = self
            .list_mcp_servers(id, false)
            .await?
            .into_iter()
            .flat_map(|s| s.tools)
            .collect();
        if let Some(server) = server {
            tools.retain(|t| t.server == server);
        }
        Ok(tools)
    }
    /// Calls one MCP tool through the session actor.
    pub async fn call_mcp_tool(
        &self,
        id: &SessionId,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<McpToolResult, Error> {
        let v = self.mcp_ext("x.ai/mcp/call", serde_json::json!({"sessionId":id.as_str(),"server":server,"tool":tool,"arguments":arguments})).await?;
        parse_tool_result(v)
    }
    /// Checks liveness of one initialized MCP server using the protocol ping.
    pub async fn ping_mcp(&self, id: &SessionId, server: &str) -> Result<(), Error> {
        self.inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::Ping,
            )
            .await
            .map(drop)
    }
    /// Notifies a server that the host root set changed. This fails unless a
    /// roots service was installed with `list_changed` authorization.
    pub async fn notify_mcp_roots_list_changed(
        &self,
        id: &SessionId,
        server: &str,
    ) -> Result<(), Error> {
        self.inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::NotifyRootsListChanged,
            )
            .await
            .map(drop)
    }
    /// Executes exactly one MCP 2026 tools/call round. Unlike
    /// [`Runtime::call_mcp_tool`], this preserves MRTR and Task outcomes for
    /// explicit orchestration by the host.
    pub async fn call_mcp_tool_once(
        &self,
        id: &SessionId,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
        continuation: Option<McpContinuation>,
    ) -> Result<McpOperationOutcome<McpToolResult>, Error> {
        let operation = McpOperationIdentity::Tool {
            name: tool.to_owned(),
            arguments: arguments.clone(),
        };
        let (input_responses, request_state, expected_client_id) =
            validate_mcp_continuation(continuation, id, server, &operation)?;
        let value = self
            .inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::CallToolOnce {
                    tool_name: tool.to_owned(),
                    arguments,
                    input_responses,
                    request_state,
                    expected_client_id,
                },
            )
            .await?;
        parse_mcp_operation_outcome(id, server, value, operation, parse_tool_result)
    }
    /// Reads an MCP resource through the session actor.
    pub async fn read_mcp_resource(
        &self,
        id: &SessionId,
        server: &str,
        uri: &str,
    ) -> Result<McpReadResourceResult, Error> {
        let v = self
            .mcp_ext(
                "x.ai/mcp/read_resource",
                serde_json::json!({"sessionId":id.as_str(),"server":server,"uri":uri}),
            )
            .await?;
        parse_resource_result(v)
    }
    /// Executes exactly one MCP 2026 resources/read round.
    pub async fn read_mcp_resource_once(
        &self,
        id: &SessionId,
        server: &str,
        uri: &str,
        continuation: Option<McpContinuation>,
    ) -> Result<McpOperationOutcome<McpReadResourceResult>, Error> {
        let operation = McpOperationIdentity::Resource {
            uri: uri.to_owned(),
        };
        let (input_responses, request_state, expected_client_id) =
            validate_mcp_continuation(continuation, id, server, &operation)?;
        let value = self
            .inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::ReadResourceOnce {
                    uri: uri.to_owned(),
                    input_responses,
                    request_state,
                    expected_client_id,
                },
            )
            .await?;
        parse_mcp_operation_outcome(id, server, value, operation, parse_resource_result)
    }
    /// Lists resources and URI templates from one live session server.
    pub async fn list_mcp_resources(
        &self,
        id: &SessionId,
        server: &str,
    ) -> Result<McpResources, Error> {
        let value = self
            .mcp_ext(
                "x.ai/mcp/resources/list",
                serde_json::json!({"sessionId": id.as_str(), "server": server}),
            )
            .await?;
        let resources = value["resources"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|raw| McpResourceInfo {
                server: server.into(),
                uri: raw["uri"].as_str().map(Into::into),
                name: raw["name"].as_str().map(Into::into),
                raw: raw.clone(),
            })
            .collect();
        let templates = value["resourceTemplates"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|raw| McpResourceTemplateInfo {
                server: server.into(),
                uri_template: raw["uriTemplate"].as_str().map(Into::into),
                name: raw["name"].as_str().map(Into::into),
                raw: raw.clone(),
            })
            .collect();
        Ok(McpResources {
            resources,
            templates,
        })
    }
    pub async fn list_mcp_prompts(
        &self,
        id: &SessionId,
        server: &str,
    ) -> Result<Vec<McpPromptInfo>, Error> {
        let value = self
            .mcp_ext(
                "x.ai/mcp/prompts/list",
                serde_json::json!({"sessionId": id.as_str(), "server": server}),
            )
            .await?;
        Ok(value["prompts"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|raw| McpPromptInfo {
                server: server.into(),
                name: raw["name"].as_str().unwrap_or_default().into(),
                description: raw["description"].as_str().map(Into::into),
                raw: raw.clone(),
            })
            .collect())
    }
    pub async fn get_mcp_prompt(
        &self,
        id: &SessionId,
        server: &str,
        name: &str,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<McpPromptResult, Error> {
        let raw = self.mcp_ext("x.ai/mcp/prompts/get", serde_json::json!({"sessionId": id.as_str(), "server": server, "name": name, "arguments": arguments})).await?;
        Ok(McpPromptResult { raw })
    }
    /// Executes exactly one MCP 2026 prompts/get round.
    pub async fn get_mcp_prompt_once(
        &self,
        id: &SessionId,
        server: &str,
        name: &str,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
        continuation: Option<McpContinuation>,
    ) -> Result<McpOperationOutcome<McpPromptResult>, Error> {
        let operation = McpOperationIdentity::Prompt {
            name: name.to_owned(),
            arguments: arguments.clone(),
        };
        let (input_responses, request_state, expected_client_id) =
            validate_mcp_continuation(continuation, id, server, &operation)?;
        let value = self
            .inner
            .mcp_modern(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::GetPromptOnce {
                    name: name.to_owned(),
                    arguments,
                    input_responses,
                    request_state,
                    expected_client_id,
                },
            )
            .await?;
        parse_mcp_operation_outcome(id, server, value, operation, |raw| {
            Ok(McpPromptResult { raw })
        })
    }
    /// Reads the latest state for a generation-bound MCP Task.
    pub async fn get_mcp_task(&self, handle: &McpTaskHandle) -> Result<McpTask, Error> {
        handle.durable_identity()?;
        let value = self
            .inner
            .mcp_modern(
                &handle.session_id,
                handle.server.clone(),
                xai_grok_shell::extensions::mcp::McpModernOperation::GetTask {
                    client_id: handle.client_id,
                    task_id: handle.task_id.clone(),
                },
            )
            .await?;
        let client_id = value["clientId"].as_u64().ok_or_else(|| {
            Error::Operation("MCP Task response omitted client generation".into())
        })?;
        let raw = value
            .get("result")
            .cloned()
            .ok_or_else(|| Error::Operation("MCP Task response omitted result".into()))?;
        parse_task(&handle.session_id, &handle.server, client_id, raw)
    }
    /// Reconciles one Host-persisted stable Task identity against the current
    /// Session/server connection without replaying the operation that created
    /// it. Success returns a fresh generation-bound handle; any unavailable or
    /// ambiguous answer remains explicitly recovery-required.
    pub async fn recover_mcp_task(
        &self,
        identity: &McpTaskIdentity,
    ) -> Result<McpTaskRecovery, Error> {
        let value = match self
            .inner
            .mcp_modern(
                identity.session_id(),
                identity.server().to_owned(),
                xai_grok_shell::extensions::mcp::McpModernOperation::RecoverTask {
                    task_id: identity.task_id().to_owned(),
                },
            )
            .await
        {
            Ok(value) => value,
            Err(_) => {
                return Ok(McpTaskRecovery::RecoveryRequired {
                    identity: identity.clone(),
                });
            }
        };
        let client_id = value["clientId"].as_u64().ok_or_else(|| {
            Error::Operation("MCP Task recovery omitted client generation".into())
        })?;
        let raw = value
            .get("result")
            .cloned()
            .ok_or_else(|| Error::Operation("MCP Task recovery omitted result".into()))?;
        let task = parse_task(identity.session_id(), identity.server(), client_id, raw)?;
        if task.handle.durable_identity()? != *identity {
            return Err(Error::Operation(
                "MCP Task recovery returned a different stable identity".into(),
            ));
        }
        Ok(McpTaskRecovery::Reattached {
            task: Box::new(task),
        })
    }
    /// Supplies responses to an input_required MCP Task. Task state is read
    /// first and every non-elicitation request must be answered exactly once.
    /// Elicitation is refused; use [`Runtime::resolve_mcp_task_input_with_ui`].
    pub async fn update_mcp_task(
        &self,
        handle: &McpTaskHandle,
        input_responses: McpInputResponses,
    ) -> Result<(), Error> {
        let task = self.get_mcp_task(handle).await?;
        let input = task.input_required.as_ref().ok_or_else(|| {
            Error::Operation("MCP Task is not waiting for structured input".into())
        })?;
        if input
            .requests
            .iter()
            .any(|request| request.kind == McpInputRequestKind::Elicitation)
        {
            return Err(Error::InvalidConfig(
                "MCP elicitation answers are accepted only from the product UI channel".into(),
            ));
        }
        validate_non_ui_responses(input, &input_responses)?;
        self.update_mcp_task_bound(handle, task.raw, input_responses)
            .await
    }

    async fn update_mcp_task_bound(
        &self,
        handle: &McpTaskHandle,
        expected_task: serde_json::Value,
        input_responses: McpInputResponses,
    ) -> Result<(), Error> {
        handle.durable_identity()?;
        validate_mcp_input_responses(&input_responses)?;
        self.inner
            .mcp_modern(
                &handle.session_id,
                handle.server.clone(),
                xai_grok_shell::extensions::mcp::McpModernOperation::UpdateTask {
                    client_id: handle.client_id,
                    task_id: handle.task_id.clone(),
                    expected_task,
                    input_responses,
                },
            )
            .await
            .map(drop)
    }
    /// Obtains every elicitation answer from the installed product UI and
    /// updates the exact generation-bound Task. Callers may provide responses
    /// only for non-elicitation requests in the same round.
    pub async fn resolve_mcp_task_input_with_ui(
        &self,
        handle: &McpTaskHandle,
        non_ui_responses: McpInputResponses,
    ) -> Result<(), Error> {
        let task = self.get_mcp_task(handle).await?;
        let input = task.input_required.as_ref().ok_or_else(|| {
            Error::Operation("MCP Task is not waiting for structured input".into())
        })?;
        let identity = handle.durable_identity()?;
        let responses = self
            .resolve_elicitation_responses(
                input,
                non_ui_responses,
                McpElicitationOrigin::Task {
                    identity,
                    client_id: handle.client_id,
                },
            )
            .await?;
        self.update_mcp_task_bound(handle, task.raw, responses)
            .await
    }
    /// Obtains elicitation answers for one direct MRTR round from the installed
    /// product UI and returns the bound continuation. Callers may supply only
    /// roots/sampling responses; supplying an elicitation key fails closed.
    pub async fn resolve_mcp_input_with_ui(
        &self,
        input: &McpInputRequired,
        non_ui_responses: McpInputResponses,
    ) -> Result<McpContinuation, Error> {
        let identity = input.continuation_identity.as_ref().ok_or_else(|| {
            Error::Operation(
                "this MCP input requirement belongs to a Task; use resolve_mcp_task_input_with_ui"
                    .into(),
            )
        })?;
        let responses = self
            .resolve_elicitation_responses(
                input,
                non_ui_responses,
                McpElicitationOrigin::Operation {
                    session_id: identity.session_id.clone(),
                    server: identity.server.clone(),
                    client_id: identity.client_id,
                },
            )
            .await?;
        input.bound_response(responses)
    }

    async fn resolve_elicitation_responses(
        &self,
        input: &McpInputRequired,
        mut responses: McpInputResponses,
        origin: McpElicitationOrigin,
    ) -> Result<McpInputResponses, Error> {
        let ui = self.mcp_elicitation_ui.as_ref().ok_or_else(|| {
            Error::Operation("no MCP product UI elicitation channel is installed".into())
        })?;
        validate_non_ui_responses(input, &responses)?;
        let mut found_elicitation = false;
        let requests = input.validated_requests()?.to_vec();
        for request in &requests {
            if request.kind != McpInputRequestKind::Elicitation {
                continue;
            }
            found_elicitation = true;
            let params = request
                .raw
                .get("params")
                .cloned()
                .ok_or_else(|| Error::Operation("MCP elicitation omitted params".into()))?;
            let params = serde_json::from_value(params).map_err(|error| {
                Error::Operation(format!("invalid MCP elicitation params: {error}"))
            })?;
            let request_for_ui =
                checked_elicitation_request(origin.clone(), request.id.clone(), params)?;
            let result = ui
                .elicit(request_for_ui)
                .await
                .map_err(|source| Error::Host {
                    method: "mcp/elicitation".into(),
                    source: HostError {
                        code: source.code,
                        message: source.message,
                        data: source.data.unwrap_or(serde_json::Value::Null),
                    },
                })?;
            validate_elicitation_result(&result)?;
            responses.insert(
                request.id.clone(),
                serde_json::to_value(result).map_err(|error| {
                    Error::Operation(format!("invalid MCP elicitation answer: {error}"))
                })?,
            );
        }
        if !found_elicitation {
            return Err(Error::InvalidConfig(
                "the MCP input round contains no elicitation request".into(),
            ));
        }
        validate_mcp_input_responses(&responses)?;
        Ok(responses)
    }
    /// Requests cancellation of a generation-bound MCP Task.
    pub async fn cancel_mcp_task(&self, handle: &McpTaskHandle) -> Result<(), Error> {
        handle.durable_identity()?;
        self.inner
            .mcp_modern(
                &handle.session_id,
                handle.server.clone(),
                xai_grok_shell::extensions::mcp::McpModernOperation::CancelTask {
                    client_id: handle.client_id,
                    task_id: handle.task_id.clone(),
                },
            )
            .await
            .map(drop)
    }
    /// Opens a bounded MCP 2026 `subscriptions/listen` stream and waits for
    /// the server's acknowledgement. Dropping the returned stream cancels the
    /// listen request; reconnects require a new call.
    pub async fn listen_mcp(
        &self,
        id: &SessionId,
        server: &str,
        filter: McpSubscriptionFilter,
        capacity: usize,
    ) -> Result<McpSubscription, Error> {
        let capacity = std::num::NonZeroUsize::new(capacity)
            .filter(|capacity| capacity.get() <= 4096)
            .ok_or_else(|| {
                Error::InvalidConfig("MCP subscription capacity must be in 1..=4096".into())
            })?;
        let bridge = self
            .inner
            .mcp_subscribe(
                id,
                server.to_owned(),
                xai_grok_shell::extensions::mcp::McpModernSubscriptionFilter {
                    tools_list_changed: filter.tools_list_changed,
                    prompts_list_changed: filter.prompts_list_changed,
                    resources_list_changed: filter.resources_list_changed,
                    resource_subscriptions: filter.resource_subscriptions,
                },
                capacity,
            )
            .await?;
        let acknowledged = serde_json::from_value(bridge.acknowledged)
            .map_err(|error| Error::Operation(format!("invalid MCP subscription ack: {error}")))?;
        Ok(McpSubscription {
            session_id: id.clone(),
            server: server.to_owned(),
            client_id: bridge.client_id,
            acknowledged,
            events: bridge.events,
            terminal: bridge.terminal,
            cancel: Some(bridge.cancel),
            pending_end: None,
            ended: false,
        })
    }
    /// Completes either a `prompt` name argument or `resource` URI-template argument.
    pub async fn complete_mcp_argument(
        &self,
        id: &SessionId,
        server: &str,
        reference: &str,
        target: &str,
        argument: &str,
        value: &str,
        context: Option<BTreeMap<String, String>>,
    ) -> Result<McpCompletionResult, Error> {
        if !matches!(reference, "prompt" | "resource") {
            return Err(Error::InvalidConfig(
                "MCP completion reference must be 'prompt' or 'resource'".into(),
            ));
        }
        let raw = self.mcp_ext("x.ai/mcp/complete", serde_json::json!({"sessionId": id.as_str(), "server": server, "reference": reference, "target": target, "argument": argument, "value": value, "context": context})).await?;
        Ok(McpCompletionResult {
            values: raw["values"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(Into::into))
                .collect(),
            total: raw["total"].as_u64(),
            has_more: raw["hasMore"].as_bool(),
            raw,
        })
    }
    /// Returns per-server authentication state for this session.
    pub async fn mcp_auth_status(&self, id: &SessionId) -> Result<Vec<McpAuthStatus>, Error> {
        let v = self
            .mcp_ext(
                "x.ai/mcp/auth_status",
                serde_json::json!({"session_id":id.as_str()}),
            )
            .await?;
        Ok(v.get("servers")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .map(|x| McpAuthStatus {
                server_name: x["server_name"].as_str().unwrap_or_default().into(),
                status: parse_mcp_authentication_state(x["status"].as_str().unwrap_or("unknown")),
                error: None,
            })
            .collect())
    }
    /// Starts authentication and returns its immediate/deferred typed status.
    pub async fn start_mcp_auth(
        &self,
        id: &SessionId,
        server: &str,
    ) -> Result<McpAuthStatus, Error> {
        let v = self
            .mcp_ext(
                "x.ai/mcp/auth_trigger",
                serde_json::json!({"session_id":id.as_str(),"server_name":server}),
            )
            .await?;
        Ok(McpAuthStatus {
            server_name: server.into(),
            status: parse_mcp_authentication_state(v["status"].as_str().unwrap_or("unknown")),
            error: v["error"].as_str().map(Into::into),
        })
    }
    /// Changes server state only in this session; no preference file is mutated.
    pub async fn set_mcp_server_enabled(
        &self,
        id: &SessionId,
        server: &str,
        enabled: bool,
    ) -> Result<(), Error> {
        self.mcp_ext("x.ai/mcp/toggle",serde_json::json!({"session_id":id.as_str(),"server_name":server,"enabled":enabled,"session_local":true})).await.map(drop)
    }
    /// Changes tool state only in this session.
    pub async fn set_mcp_tool_enabled(
        &self,
        id: &SessionId,
        server: &str,
        tool: &str,
        enabled: bool,
    ) -> Result<(), Error> {
        self.mcp_ext("x.ai/mcp/toggle_tool",serde_json::json!({"session_id":id.as_str(),"server_name":server,"tool_name":tool,"enabled":enabled,"session_local":true})).await.map(drop)
    }
    /// Atomically replaces the session's client-provided MCP server set. The
    /// receipt contains names only and never echoes transport credentials.
    pub async fn replace_mcp_servers(
        &self,
        id: &SessionId,
        servers: Vec<McpServerConfig>,
    ) -> Result<McpServerReplacementReceipt, Error> {
        let names = servers
            .iter()
            .map(|s| match s {
                McpServerConfig::Stdio { name, .. }
                | McpServerConfig::Http { name, .. }
                | McpServerConfig::Sse { name, .. } => name.clone(),
            })
            .collect::<Vec<_>>();
        self.inner.replace_mcp_servers(id, servers).await?;
        Ok(McpServerReplacementReceipt {
            count: names.len(),
            names,
        })
    }
}
