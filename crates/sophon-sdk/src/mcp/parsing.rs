// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use crate::*;

pub(crate) fn parse_mcp_subscription_end(
    value: Option<serde_json::Value>,
) -> Result<Option<McpSubscriptionEvent>, Error> {
    let Some(value) = value else {
        return Ok(Some(McpSubscriptionEvent::Ended(
            McpSubscriptionEnd::Abrupt,
        )));
    };
    let end = match value["reason"].as_str() {
        Some("graceful") => McpSubscriptionEnd::Graceful,
        Some("abrupt") => McpSubscriptionEnd::Abrupt,
        Some("cancelled") => McpSubscriptionEnd::Cancelled,
        Some("lagged") => McpSubscriptionEnd::Lagged {
            capacity: value["capacity"]
                .as_u64()
                .and_then(|capacity| usize::try_from(capacity).ok())
                .ok_or_else(|| Error::Operation("invalid MCP subscription capacity".into()))?,
        },
        Some("error") => McpSubscriptionEnd::Error {
            message: value["message"]
                .as_str()
                .ok_or_else(|| Error::Operation("invalid MCP subscription error".into()))?
                .to_owned(),
        },
        _ => {
            return Err(Error::Operation(
                "invalid MCP subscription terminal event".into(),
            ));
        }
    };
    Ok(Some(McpSubscriptionEvent::Ended(end)))
}
pub(crate) fn parse_mcp_authentication_state(status: &str) -> McpAuthenticationState {
    match status {
        "authenticated" => McpAuthenticationState::Authenticated,
        "needs_auth" => McpAuthenticationState::NeedsAuth,
        "setup_required" => McpAuthenticationState::SetupRequired,
        "failed" => McpAuthenticationState::Failed,
        other => McpAuthenticationState::Unknown(other.to_owned()),
    }
}

pub(crate) fn parse_mcp_servers(value: &serde_json::Value) -> Result<Vec<McpServerSummary>, Error> {
    let entries = value
        .get("servers")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::Operation("invalid MCP catalog response".into()))?;
    Ok(entries
        .iter()
        .map(|v| {
            let name = v["name"].as_str().unwrap_or_default().to_owned();
            let session = &v["session"];
            let tools = session["tools"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|t| McpToolInfo {
                    server: name.clone(),
                    name: t["name"].as_str().unwrap_or_default().into(),
                    display_name: t["displayName"].as_str().map(Into::into),
                    description: t["description"].as_str().map(Into::into),
                    enabled: t["enabled"].as_bool().unwrap_or(true),
                    meta: t.get("_meta").cloned().unwrap_or(serde_json::Value::Null),
                })
                .collect();
            McpServerSummary {
                name,
                display_name: v["displayName"].as_str().map(Into::into),
                source: match v["source"].as_str() {
                    Some("local") => McpServerSource::Local,
                    Some("managed") => McpServerSource::Managed,
                    _ => McpServerSource::Unknown,
                },
                transport: match v["type"].as_str() {
                    Some("stdio") => McpTransportKind::Stdio,
                    Some("http") => McpTransportKind::Http,
                    Some("sse") => McpTransportKind::Sse,
                    Some("managedGateway") => McpTransportKind::ManagedGateway,
                    _ => McpTransportKind::Unknown,
                },
                enabled: session["enabled"].as_bool().unwrap_or(false),
                status: session["status"].as_str().map(|s| match s {
                    "ready" => McpServerStatus::Ready,
                    "initializing" => McpServerStatus::Initializing,
                    "setuprequired" | "setup_required" => McpServerStatus::SetupRequired,
                    "unavailable" => McpServerStatus::Unavailable,
                    "needsauth" | "needs_auth" => McpServerStatus::NeedsAuth,
                    _ => McpServerStatus::Unknown,
                }),
                auth_required: session["authRequired"].as_bool().unwrap_or(false),
                setup_required: session["setupRequired"].as_bool().unwrap_or(false),
                tools,
                negotiated: session.get("negotiated").and_then(|negotiated| {
                    let protocol_version = negotiated["protocolVersion"].as_str()?.to_owned();
                    let capabilities = negotiated.get("capabilities")?.clone();
                    let extensions: BTreeMap<String, serde_json::Value> =
                        capabilities["extensions"]
                            .as_object()
                            .map(|values| {
                                values
                                    .iter()
                                    .map(|(name, value)| (name.clone(), value.clone()))
                                    .collect()
                            })
                            .unwrap_or_default();
                    Some(McpNegotiatedCapabilities {
                        protocol_version,
                        tools: capabilities.get("tools").is_some_and(|v| !v.is_null()),
                        resources: capabilities.get("resources").is_some_and(|v| !v.is_null()),
                        prompts: capabilities.get("prompts").is_some_and(|v| !v.is_null()),
                        completions: capabilities
                            .get("completions")
                            .is_some_and(|v| !v.is_null()),
                        logging: capabilities.get("logging").is_some_and(|v| !v.is_null()),
                        tool_list_changed: capabilities["tools"]["listChanged"]
                            .as_bool()
                            .unwrap_or(false),
                        resource_list_changed: capabilities["resources"]["listChanged"]
                            .as_bool()
                            .unwrap_or(false),
                        subscriptions: capabilities["tools"]["listChanged"]
                            .as_bool()
                            .unwrap_or(false)
                            || capabilities["prompts"]["listChanged"]
                                .as_bool()
                                .unwrap_or(false)
                            || capabilities["resources"]["listChanged"]
                                .as_bool()
                                .unwrap_or(false)
                            || capabilities["resources"]["subscribe"]
                                .as_bool()
                                .unwrap_or(false),
                        legacy_resource_subscribe: capabilities["resources"]["subscribe"]
                            .as_bool()
                            .unwrap_or(false),
                        prompt_list_changed: capabilities["prompts"]["listChanged"]
                            .as_bool()
                            .unwrap_or(false),
                        tasks: extensions.contains_key("io.modelcontextprotocol/tasks"),
                        extensions,
                        raw: capabilities,
                    })
                }),
            }
        })
        .collect())
}
pub(crate) fn parse_tool_result(v: serde_json::Value) -> Result<McpToolResult, Error> {
    let blocks = v["content"]
        .as_array()
        .ok_or_else(|| Error::Operation("invalid MCP call response".into()))?;
    let content = blocks
        .iter()
        .cloned()
        .map(|raw| match raw["type"].as_str() {
            Some("text") => McpContent::Text {
                text: raw["text"].as_str().unwrap_or_default().into(),
                raw,
            },
            Some("image") => McpContent::Image {
                data: raw["data"].as_str().unwrap_or_default().into(),
                mime_type: raw["mimeType"].as_str().unwrap_or_default().into(),
                raw,
            },
            Some("audio") => McpContent::Audio {
                data: raw["data"].as_str().unwrap_or_default().into(),
                mime_type: raw["mimeType"].as_str().unwrap_or_default().into(),
                raw,
            },
            Some("resource") => McpContent::EmbeddedResource {
                resource: raw["resource"].clone(),
                raw,
            },
            Some("resource_link") | Some("resourceLink") => McpContent::ResourceLink {
                uri: raw["uri"].as_str().unwrap_or_default().into(),
                name: raw["name"].as_str().unwrap_or_default().into(),
                mime_type: raw["mimeType"].as_str().map(Into::into),
                raw,
            },
            _ => McpContent::Unknown { raw },
        })
        .collect();
    Ok(McpToolResult {
        content,
        structured_content: v.get("structuredContent").cloned(),
        is_error: v["isError"].as_bool(),
        meta: v.get("_meta").cloned(),
    })
}
pub(crate) fn parse_resource_result(v: serde_json::Value) -> Result<McpReadResourceResult, Error> {
    let blocks = v["contents"]
        .as_array()
        .ok_or_else(|| Error::Operation("invalid MCP resource response".into()))?;
    Ok(McpReadResourceResult {
        contents: blocks
            .iter()
            .map(|x| McpReadResourceContent {
                uri: x["uri"].as_str().map(Into::into),
                mime_type: x["mimeType"].as_str().map(Into::into),
                text: x["text"].as_str().map(Into::into),
                blob: x["blob"].as_str().map(Into::into),
                meta: x.get("_meta").cloned(),
                raw: x.clone(),
            })
            .collect(),
    })
}

pub(crate) fn parse_input_required(v: serde_json::Value) -> Result<McpInputRequired, Error> {
    let encoded = serde_json::to_vec(&v)
        .map_err(|error| Error::Operation(format!("invalid MCP input requirement: {error}")))?;
    if encoded.len() > MAX_MCP_INPUT_PAYLOAD_BYTES {
        return Err(Error::Operation(format!(
            "MCP input requirement exceeds {MAX_MCP_INPUT_PAYLOAD_BYTES} bytes"
        )));
    }
    let requests = match v.get("inputRequests") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Object(requests)) => {
            if requests.len() > MAX_MCP_INPUT_REQUESTS {
                return Err(Error::Operation(format!(
                    "MCP input requirement exceeds {MAX_MCP_INPUT_REQUESTS} requests"
                )));
            }
            requests
                .iter()
                .map(|(id, request)| {
                    if !valid_bounded_line(id, MAX_MCP_INPUT_REQUEST_ID_BYTES) {
                        return Err(Error::Operation(
                            "MCP input request identity is invalid".into(),
                        ));
                    }
                    let kind = match request.get("method").and_then(serde_json::Value::as_str) {
                        Some("sampling/createMessage") => McpInputRequestKind::Sampling,
                        Some("elicitation/create") => McpInputRequestKind::Elicitation,
                        Some("roots/list") => McpInputRequestKind::Roots,
                        Some(method) => {
                            return Err(Error::Operation(format!(
                                "unsupported MCP input request method '{method}'"
                            )));
                        }
                        None => {
                            return Err(Error::Operation(
                                "MCP input request omitted its method".into(),
                            ));
                        }
                    };
                    Ok(McpInputRequest {
                        id: id.clone(),
                        kind,
                        raw: request.clone(),
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?
        }
        Some(_) => {
            return Err(Error::Operation(
                "MCP inputRequests must be an object".into(),
            ));
        }
    };
    let request_state = match v.get("requestState") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(state)) => Some(state.clone()),
        Some(_) => {
            return Err(Error::Operation("MCP requestState must be a string".into()));
        }
    };
    if request_state
        .as_deref()
        .is_some_and(|state| state.len() > MAX_MCP_INPUT_PAYLOAD_BYTES)
    {
        return Err(Error::Operation(
            "MCP input request state exceeds its bound".into(),
        ));
    }
    if requests.is_empty() && request_state.is_none() {
        return Err(Error::Operation(
            "invalid MCP input_required response: no requests or request state".into(),
        ));
    }
    let mut input = McpInputRequired {
        requests,
        request_state,
        raw: v,
        continuation_identity: None,
        round_binding: None,
    };
    input.round_binding = Some(Box::new(McpInputRoundBinding::capture(&input)));
    Ok(input)
}

pub(crate) fn parse_task_status(value: &serde_json::Value) -> Result<McpTaskStatus, Error> {
    match value.as_str() {
        Some("working") => Ok(McpTaskStatus::Working),
        Some("input_required") => Ok(McpTaskStatus::InputRequired),
        Some("completed") => Ok(McpTaskStatus::Completed),
        Some("failed") => Ok(McpTaskStatus::Failed),
        Some("cancelled") => Ok(McpTaskStatus::Cancelled),
        _ => Err(Error::Operation("invalid MCP Task status".into())),
    }
}

pub(crate) fn parse_task(
    session_id: &SessionId,
    server: &str,
    client_id: u64,
    raw: serde_json::Value,
) -> Result<McpTask, Error> {
    let encoded = serde_json::to_vec(&raw)
        .map_err(|error| Error::Operation(format!("invalid MCP Task: {error}")))?;
    if encoded.len() > MAX_MCP_TASK_PAYLOAD_BYTES {
        return Err(Error::Operation(format!(
            "MCP Task exceeds {MAX_MCP_TASK_PAYLOAD_BYTES} bytes"
        )));
    }
    let task_id = raw
        .get("taskId")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Operation("MCP Task omitted taskId".into()))?
        .to_owned();
    let identity = McpTaskIdentity::new(session_id.clone(), server, task_id)?;
    let status = parse_task_status(&raw["status"])?;
    let input_required = if status == McpTaskStatus::InputRequired {
        Some(parse_input_required(serde_json::json!({
            "resultType": "input_required",
            "inputRequests": raw.get("inputRequests").cloned().unwrap_or_default(),
        }))?)
    } else {
        None
    };
    let status_message = raw["statusMessage"].as_str().map(str::to_owned);
    if status_message
        .as_deref()
        .is_some_and(|message| message.len() > MAX_MCP_TASK_STATUS_MESSAGE_BYTES)
    {
        return Err(Error::Operation(format!(
            "MCP Task status message exceeds {MAX_MCP_TASK_STATUS_MESSAGE_BYTES} bytes"
        )));
    }
    let created_at = raw["createdAt"]
        .as_str()
        .filter(|value| valid_bounded_line(value, 128))
        .ok_or_else(|| Error::Operation("MCP Task has an invalid createdAt".into()))?
        .to_owned();
    let last_updated_at = raw["lastUpdatedAt"]
        .as_str()
        .filter(|value| valid_bounded_line(value, 128))
        .ok_or_else(|| Error::Operation("MCP Task has an invalid lastUpdatedAt".into()))?
        .to_owned();
    Ok(McpTask {
        handle: McpTaskHandle {
            session_id: identity.session_id().clone(),
            server: identity.server().to_owned(),
            client_id,
            task_id: identity.task_id().to_owned(),
        },
        status,
        status_message,
        created_at,
        last_updated_at,
        ttl_ms: raw["ttl"].as_u64().or_else(|| raw["ttlMs"].as_u64()),
        poll_interval_ms: raw["pollInterval"]
            .as_u64()
            .or_else(|| raw["pollIntervalMs"].as_u64()),
        input_required,
        result: raw.get("result").cloned(),
        error: raw.get("error").cloned(),
        raw,
    })
}

pub(crate) fn parse_mcp_operation_outcome<T>(
    session_id: &SessionId,
    server: &str,
    value: serde_json::Value,
    operation: McpOperationIdentity,
    parse_complete: impl FnOnce(serde_json::Value) -> Result<T, Error>,
) -> Result<McpOperationOutcome<T>, Error> {
    let client_id = value["clientId"]
        .as_u64()
        .ok_or_else(|| Error::Operation("MCP operation omitted client generation".into()))?;
    let result = value
        .get("result")
        .cloned()
        .ok_or_else(|| Error::Operation("MCP operation omitted result".into()))?;
    match value["outcome"].as_str() {
        Some("complete") => Ok(McpOperationOutcome::Complete {
            client_id,
            result: parse_complete(result)?,
        }),
        Some("input_required") => {
            let mut input = parse_input_required(result)?;
            input.continuation_identity = Some(McpContinuationIdentity {
                session_id: session_id.clone(),
                server: server.to_owned(),
                client_id,
                operation,
                request_ids: input
                    .requests
                    .iter()
                    .map(|request| request.id.clone())
                    .collect(),
            });
            Ok(McpOperationOutcome::InputRequired {
                client_id,
                input: Box::new(input),
            })
        }
        Some("task") => {
            let task = parse_task(session_id, server, client_id, result)?;
            Ok(McpOperationOutcome::Task {
                handle: task.handle.clone(),
                task: Box::new(task),
            })
        }
        _ => Err(Error::Operation("unsupported MCP operation outcome".into())),
    }
}

/// What a validated continuation carries forward: the input responses, the
/// task id, and the client id, each present only when the continuation was.
pub(crate) type McpContinuationParts = (Option<McpInputResponses>, Option<String>, Option<u64>);

pub(crate) fn validate_mcp_continuation(
    continuation: Option<McpContinuation>,
    session_id: &SessionId,
    server: &str,
    operation: &McpOperationIdentity,
) -> Result<McpContinuationParts, Error> {
    let Some(continuation) = continuation else {
        return Ok((None, None, None));
    };
    if continuation.identity.session_id != *session_id
        || continuation.identity.server != server
        || continuation.identity.operation != *operation
    {
        return Err(Error::InvalidConfig(
            "MCP continuation does not belong to this session, server, or operation".into(),
        ));
    }
    Ok((
        Some(continuation.input_responses),
        continuation.request_state,
        Some(continuation.identity.client_id),
    ))
}
