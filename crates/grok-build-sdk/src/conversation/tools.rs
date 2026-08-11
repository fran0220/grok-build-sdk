// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! The declared tool surface for peer conversations.
//!
//! The SDK owns the names, the schemas and the bounds; the Host owns the
//! answers. The three tools reach the agent through the SDK-owned in-process
//! MCP mount named [`CONVERSATION_TOOL_SERVER`], which exists on a Session
//! exactly when a [`ConversationDelegate`] is installed.

use super::*;

/// The mount name the three conversation tools are served under.
pub const CONVERSATION_TOOL_SERVER: &str = "conversation";
const CONVERSATION_TOOL_REGISTRATION: &str = "grok-build-sdk.conversation-tools";
const CREATE_TOOL: &str = "conversation_create";
const READ_TOOL: &str = "conversation_read";
const SEND_TOOL: &str = "conversation_send";

/// One declared tool: its name, what it is for, and the bounded shapes of its
/// arguments and its result.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationToolDescriptor {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
}

/// The complete declared surface, in a stable order.
pub fn conversation_tool_descriptors() -> Vec<ConversationToolDescriptor> {
    vec![
        ConversationToolDescriptor {
            name: CREATE_TOOL,
            description: "Create an ordinary durable conversation in a named project and return its identity. The new conversation is a peer, not a child: nothing waits on it and nothing settles it.",
            input_schema: serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["project"],
                "properties": {
                    "project": {
                        "type": "string",
                        "description": "Name of the project the conversation is created in.",
                        "minLength": 1,
                        "maxLength": MAX_PROJECT_NAME_BYTES
                    },
                    "title": {
                        "type": "string",
                        "description": "Optional human-facing title for the new conversation.",
                        "minLength": 1,
                        "maxLength": MAX_CONVERSATION_TITLE_BYTES
                    }
                }
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "required": ["conversation", "project"],
                "properties": {
                    "conversation": { "type": "string" },
                    "project": { "type": "string" },
                    "label": { "type": "string" }
                }
            }),
        },
        ConversationToolDescriptor {
            name: READ_TOOL,
            description: "Return a bounded distillate of another conversation. The raw transcript is never returned and never enters this conversation's context.",
            input_schema: serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["conversation"],
                "properties": {
                    "conversation": {
                        "type": "string",
                        "description": "Identity of the conversation to distill.",
                        "minLength": 1,
                        "maxLength": MAX_CONVERSATION_ID_BYTES
                    },
                    "focus": {
                        "type": "string",
                        "description": "Optional free-text steer for the distillation.",
                        "minLength": 1,
                        "maxLength": MAX_DIGEST_FOCUS_BYTES
                    },
                    "maxDigestBytes": {
                        "type": "integer",
                        "description": "Ceiling on the returned distillate, in bytes.",
                        "minimum": 1,
                        "maximum": MAX_CONVERSATION_DIGEST_BYTES
                    }
                }
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "required": ["conversation", "distillate", "truncated", "coveredMessages"],
                "properties": {
                    "conversation": { "type": "string" },
                    "distillate": { "type": "string" },
                    "truncated": { "type": "boolean" },
                    "coveredMessages": { "type": "integer" }
                }
            }),
        },
        ConversationToolDescriptor {
            name: SEND_TOOL,
            description: "Send one message to another conversation. An idle target starts a turn and a running target queues to turn settlement. Redelivering the same idempotencyKey is the same delivery, never a second one. Replies come back as ordinary sends.",
            input_schema: serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["conversation", "message", "idempotencyKey"],
                "properties": {
                    "conversation": {
                        "type": "string",
                        "description": "Identity of the target conversation.",
                        "minLength": 1,
                        "maxLength": MAX_CONVERSATION_ID_BYTES
                    },
                    "message": {
                        "type": "string",
                        "description": "The message body delivered to the target.",
                        "minLength": 1,
                        "maxLength": MAX_PEER_MESSAGE_BYTES
                    },
                    "idempotencyKey": {
                        "type": "string",
                        "description": "Caller-chosen delivery key. Reuse it verbatim to retry safely.",
                        "minLength": 1,
                        "maxLength": MAX_IDEMPOTENCY_KEY_BYTES
                    },
                    "label": {
                        "type": "string",
                        "description": "Optional display label for this conversation, shown to the target.",
                        "minLength": 1,
                        "maxLength": MAX_CONVERSATION_LABEL_BYTES
                    }
                }
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "required": ["conversation", "delivery", "idempotencyKey"],
                "properties": {
                    "conversation": { "type": "string" },
                    "delivery": { "enum": ["started_turn", "queued", "already_accepted"] },
                    "idempotencyKey": { "type": "string" }
                }
            }),
        },
    ]
}

/// One validated tool call, whatever route it arrived on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationToolCall {
    Create(ConversationCreate),
    Read(ConversationRead),
    Send(ConversationSend),
}

impl ConversationToolCall {
    /// Parses a declared tool name and its raw arguments. Unknown names,
    /// unknown fields, and out-of-bound values all fail closed here, before a
    /// delegate is asked anything.
    pub fn parse(name: &str, arguments: serde_json::Value) -> Result<Self, ConversationError> {
        let arguments = if arguments.is_null() {
            serde_json::json!({})
        } else {
            arguments
        };
        let decoded = match name {
            CREATE_TOOL => serde_json::from_value(arguments).map(Self::Create),
            READ_TOOL => serde_json::from_value(arguments).map(Self::Read),
            SEND_TOOL => serde_json::from_value(arguments).map(Self::Send),
            other => {
                return Err(ConversationError::Unavailable(format!(
                    "'{other}' is not a conversation tool"
                )));
            }
        };
        let call = decoded.map_err(|error| ConversationError::Unavailable(error.to_string()))?;
        if let Self::Read(read) = &call {
            read.validate()?;
        }
        Ok(call)
    }

    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Create(_) => CREATE_TOOL,
            Self::Read(_) => READ_TOOL,
            Self::Send(_) => SEND_TOOL,
        }
    }
}

/// What one conversation tool answered.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(untagged)]
pub enum ConversationToolOutcome {
    Created(ConversationIdentity),
    Read(ConversationDigest),
    Accepted(ConversationAcceptance),
}

pub(crate) async fn dispatch_tool_call(
    delegate: &Arc<dyn ConversationDelegate>,
    caller: &SessionId,
    call: ConversationToolCall,
) -> Result<ConversationToolOutcome, ConversationError> {
    match call {
        ConversationToolCall::Create(request) => dispatch_create(delegate, caller, request)
            .await
            .map(ConversationToolOutcome::Created),
        ConversationToolCall::Read(request) => dispatch_read(delegate, caller, request)
            .await
            .map(ConversationToolOutcome::Read),
        ConversationToolCall::Send(request) => dispatch_send(delegate, caller, request)
            .await
            .map(ConversationToolOutcome::Accepted),
    }
}

/// Serves the three declared tools to the agent and dispatches them to the
/// installed delegate. A rejected argument or a Host refusal is returned as a
/// tool error the agent can read, never as a transport failure.
struct ConversationToolHost {
    delegate: Arc<dyn ConversationDelegate>,
}

impl ConversationToolHost {
    async fn call(&self, caller: &SessionId, params: &serde_json::Value) -> serde_json::Value {
        let name = params.get("name").and_then(serde_json::Value::as_str);
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let outcome = match name {
            None => Err(ConversationError::Unavailable(
                "tools/call requires a tool name".into(),
            )),
            Some(name) => match ConversationToolCall::parse(name, arguments) {
                Err(error) => Err(error),
                Ok(call) => dispatch_tool_call(&self.delegate, caller, call).await,
            },
        };
        match outcome {
            Ok(outcome) => {
                let structured = serde_json::to_value(&outcome).unwrap_or(serde_json::Value::Null);
                serde_json::json!({
                    "content": [{ "type": "text", "text": structured.to_string() }],
                    "structuredContent": structured,
                    "isError": false
                })
            }
            Err(error) => serde_json::json!({
                "content": [{ "type": "text", "text": error.to_string() }],
                "isError": true
            }),
        }
    }
}

#[async_trait::async_trait]
impl InProcessMcpHandler for ConversationToolHost {
    async fn handle(&self, _message: serde_json::Value) -> Result<serde_json::Value, HostError> {
        Err(HostError {
            code: -32600,
            message: "conversation tools require the calling session identity".into(),
            data: serde_json::Value::Null,
        })
    }

    async fn handle_with_context(
        &self,
        context: &InProcessMcpContext,
        message: serde_json::Value,
    ) -> Result<serde_json::Value, HostError> {
        let id = message.get("id").cloned();
        let result = match message.get("method").and_then(serde_json::Value::as_str) {
            Some("server/discover") => serde_json::json!({
                "resultType": "complete",
                "supportedVersions": ["2026-07-28"],
                "capabilities": { "tools": {} },
                "ttlMs": 0,
                "cacheScope": "private",
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {
                        "name": CONVERSATION_TOOL_SERVER,
                        "version": "1"
                    }
                }
            }),
            Some("tools/list") => {
                let tools: Vec<serde_json::Value> = conversation_tool_descriptors()
                    .into_iter()
                    .map(|descriptor| {
                        serde_json::json!({
                            "name": descriptor.name,
                            "description": descriptor.description,
                            "inputSchema": descriptor.input_schema,
                            "outputSchema": descriptor.output_schema
                        })
                    })
                    .collect();
                serde_json::json!({ "tools": tools })
            }
            Some("tools/call") => {
                let params = message
                    .get("params")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                self.call(&context.session_id, &params).await
            }
            _ => {
                return Ok(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32601, "message": "Method not found" }
                }));
            }
        };
        Ok(serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result }))
    }
}

pub(crate) fn conversation_tool_server(
    delegate: Arc<dyn ConversationDelegate>,
) -> InProcessMcpServer {
    InProcessMcpServer::new(
        CONVERSATION_TOOL_SERVER,
        CONVERSATION_TOOL_REGISTRATION,
        Arc::new(ConversationToolHost { delegate }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_declared_surface_is_exactly_three_named_bounded_tools() {
        let descriptors = conversation_tool_descriptors();
        let names: Vec<&str> = descriptors.iter().map(|entry| entry.name).collect();
        assert_eq!(names, vec![CREATE_TOOL, READ_TOOL, SEND_TOOL]);
        for descriptor in &descriptors {
            assert_eq!(descriptor.input_schema["additionalProperties"], false);
            assert!(descriptor.input_schema["required"].is_array());
        }
        assert_eq!(
            descriptors[2].input_schema["properties"]["message"]["maxLength"],
            serde_json::json!(MAX_PEER_MESSAGE_BYTES)
        );
    }

    #[test]
    fn tool_arguments_are_parsed_into_validated_requests_or_fail_closed() {
        let create = ConversationToolCall::parse(
            CREATE_TOOL,
            serde_json::json!({ "project": "Desktop Product", "title": "Release plan" }),
        )
        .expect("a create call");
        assert_eq!(create.tool_name(), CREATE_TOOL);

        let send = ConversationToolCall::parse(
            SEND_TOOL,
            serde_json::json!({
                "conversation": "thread-9",
                "message": "please review",
                "idempotencyKey": "delivery-1"
            }),
        )
        .expect("a send call");
        assert!(matches!(send, ConversationToolCall::Send(_)));

        for (name, arguments) in [
            (CREATE_TOOL, serde_json::json!({})),
            (
                CREATE_TOOL,
                serde_json::json!({ "project": "p", "surprise": true }),
            ),
            (
                READ_TOOL,
                serde_json::json!({ "conversation": "thread-9", "maxDigestBytes": 0 }),
            ),
            (
                READ_TOOL,
                serde_json::json!({
                    "conversation": "thread-9",
                    "maxDigestBytes": MAX_CONVERSATION_DIGEST_BYTES + 1
                }),
            ),
            (
                SEND_TOOL,
                serde_json::json!({
                    "conversation": "thread-9",
                    "message": "   ",
                    "idempotencyKey": "delivery-1"
                }),
            ),
            ("conversation_delete", serde_json::json!({})),
        ] {
            assert!(
                ConversationToolCall::parse(name, arguments.clone()).is_err(),
                "{name} must fail closed on {arguments}"
            );
        }
    }
}
