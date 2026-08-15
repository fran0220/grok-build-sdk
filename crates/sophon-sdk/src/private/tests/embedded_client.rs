//! Embedded client callback tests.

use super::super::*;
use xai_grok_shell::embedded::EmbeddedClient as _;

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

fn permission_request() -> serde_json::Value {
    serde_json::json!({
        "sessionId": "session-typed",
        "toolCall": {
            "toolCallId": "call-1",
            "title": "Run tests",
            "kind": "execute",
            "status": "pending",
            "rawInput": {"command":"cargo test"},
            "rawOutput": {"preview":true}
        },
        "options": [
            {"optionId":"once", "name":"Once", "kind":"allow_once"},
            {"optionId":"always", "name":"Always", "kind":"allow_always"},
            {"optionId":"reject", "name":"Reject", "kind":"reject_once"},
            {"optionId":"never", "name":"Never", "kind":"reject_always"}
        ]
    })
}

#[tokio::test]
async fn typed_permission_policy_parses_routes_and_fails_closed() {
    let policy = Arc::new(PermissionPolicy {
        decision: Ok(crate::ToolPermissionDecision::Selected("always".into())),
        requests: Default::default(),
    });
    let response = permission_client(Some(policy.clone()))
        .request("session/request_permission", permission_request())
        .await
        .unwrap();
    assert_eq!(response["outcome"]["outcome"], "selected");
    assert_eq!(response["outcome"]["optionId"], "always");
    {
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
    }

    let invalid = Arc::new(PermissionPolicy {
        decision: Ok(crate::ToolPermissionDecision::Selected("invented".into())),
        requests: Default::default(),
    });
    let error = permission_client(Some(invalid))
        .request("session/request_permission", permission_request())
        .await
        .unwrap_err();
    assert_eq!(error.code, -32602);

    let failing = Arc::new(PermissionPolicy {
        decision: Err(crate::ToolPermissionError {
            message: "denied by policy service".into(),
            data: serde_json::json!({"rule":"prod"}),
        }),
        requests: Default::default(),
    });
    let error = permission_client(Some(failing))
        .request("session/request_permission", permission_request())
        .await
        .unwrap_err();
    assert_eq!(error.code, -32603);

    let cancelled = permission_client(None)
        .request("session/request_permission", permission_request())
        .await
        .unwrap();
    assert_eq!(cancelled["outcome"]["outcome"], "cancelled");
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
        .notification("x.ai/mcp/servers_updated", payload)
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
            .notification(method, payload)
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
        .notification(
            "x.ai/mcp/future_catalog",
            serde_json::json!({
                "sessionId":"session-1",
                "futureSecret":"must-not-enter-an-untyped-fallback"
            }),
        )
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
    let response = client
        .request("host.desktop/screenshot", params.clone())
        .await
        .expect("reverse request");
    assert_eq!(response["method"], "host.desktop/screenshot");
    assert_eq!(response["params"], params);
    assert_eq!(response["host"], true);

    let denied = client
        .request("host.desktop/unadvertised", serde_json::json!({}))
        .await
        .expect_err("unadvertised reverse methods fail closed");
    assert_eq!(denied.code, -32601);

    let notification_params = serde_json::json!({"windowId":"w-1","dirty":true});
    client
        .notification("host.desktop/window_changed", notification_params.clone())
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
    let response = client.request("x.ai/hooks/run", payload).await.unwrap();
    assert_eq!(response["decision"], "deny");
    assert_eq!(response["systemMessage"], "policy denied");
    {
        let calls = hook.0.lock().unwrap();
        assert_eq!(calls[0].event, crate::AgentHookEvent::PreToolUse);
        assert_eq!(calls[0].tool_name.as_deref(), Some("write_file"));
        assert_eq!(calls[0].tool_input.as_ref().unwrap()["path"], "a");
        assert_eq!(calls[0].raw["future"], 42);
    }

    let unknown = serde_json::json!({
        "hookCallbackId":"missing", "hookEventName":"post_tool_use", "sessionId":"s"
    });
    let error = client
        .notification("x.ai/hooks/event", unknown)
        .await
        .unwrap_err();
    assert_eq!(error.code, -32601);
}
