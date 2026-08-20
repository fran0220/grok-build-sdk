use super::*;

#[test]
fn mcp_transport_kind_preserves_snake_case_wire_names() {
    assert_eq!(
        serde_json::to_value(McpTransportKind::ManagedGateway).expect("transport serializes"),
        serde_json::json!("managed_gateway")
    );
}

#[test]
fn typed_mcp_content_preserves_known_and_unknown_protocol_blocks() {
    let result = parse_tool_result(serde_json::json!({
        "content": [
            {
                "type": "text",
                "text": "hello",
                "annotations": {"audience": ["user"]},
                "_meta": {"trace": 7}
            },
            {"type": "futureBlock", "payload": {"answer": 42}}
        ],
        "structuredContent": {"ok": true},
        "isError": false,
        "_meta": {"requestId": "req-1"}
    }))
    .expect("typed MCP result");

    assert!(matches!(
        &result.content[0],
        McpContent::Text { text, raw }
            if text == "hello"
                && raw["annotations"]["audience"][0] == "user"
                && raw["_meta"]["trace"] == 7
    ));
    assert!(matches!(
        &result.content[1],
        McpContent::Unknown { raw } if raw["payload"]["answer"] == 42
    ));
    assert_eq!(result.structured_content.unwrap()["ok"], true);
    assert_eq!(result.meta.unwrap()["requestId"], "req-1");
}

#[test]
fn typed_mcp_catalog_is_allowlist_redacted() {
    let servers = parse_mcp_servers(&serde_json::json!({
        "servers": [{
            "name": "fixture",
            "displayName": "Fixture MCP",
            "sourceLabel": "plugin: fixture",
            "icons": [
                {"src": " https://example.com/server.png ", "mimeType": " image/png "},
                {"src": "http://insecure.example/server.png"}
            ],
            "source": "local",
            "type": "stdio",
            "command": "/secret/command",
            "args": ["--token", "argument-secret"],
            "env": [{"name": "TOKEN", "value": "environment-secret"}],
            "setupValues": {"token": "setup-secret"},
            "session": {
                "enabled": true,
                "status": "ready",
                "tools": [{
                    "name": "echo",
                    "enabled": true,
                    "icons": [{"src": "data:image/png;base64,aaa", "sizes": ["48x48"]}]
                }]
            }
        }]
    }))
    .expect("catalog parses");

    let json = serde_json::to_string(&servers).expect("summary serializes");
    for secret in [
        "/secret/command",
        "argument-secret",
        "environment-secret",
        "setup-secret",
    ] {
        assert!(!json.contains(secret), "redacted catalog leaked {secret}");
    }
    assert_eq!(servers[0].transport, McpTransportKind::Stdio);
    assert_eq!(servers[0].source_label.as_deref(), Some("plugin: fixture"));
    assert_eq!(servers[0].icons.len(), 1);
    assert_eq!(servers[0].icons[0].src, "https://example.com/server.png");
    assert_eq!(servers[0].icons[0].mime_type.as_deref(), Some("image/png"));
    assert_eq!(servers[0].tools[0].name, "echo");
    assert_eq!(servers[0].tools[0].icons.len(), 1);
    assert_eq!(
        servers[0].tools[0].icons[0].src,
        "data:image/png;base64,aaa"
    );
}

#[test]
fn mcp_catalog_normalizes_auth_and_setup_statuses() {
    let servers = parse_mcp_servers(&serde_json::json!({
        "servers": [
            {
                "name": "auth",
                "source": "local",
                "type": "http",
                "url": "",
                "session": {"enabled": true, "authRequired": true}
            },
            {
                "name": "setup",
                "source": "local",
                "type": "http",
                "url": "",
                "session": {"enabled": false, "setupRequired": true}
            }
        ]
    }))
    .expect("catalog parses");
    assert_eq!(servers[0].status, Some(McpServerStatus::NeedsAuth));
    assert_eq!(servers[1].status, Some(McpServerStatus::SetupRequired));
}

fn test_subscription(
    values: impl IntoIterator<Item = serde_json::Value>,
    terminal_value: Option<serde_json::Value>,
) -> McpSubscription {
    let (tx, events) = tokio::sync::mpsc::channel(1);
    for value in values {
        tx.try_send(value).expect("fixture event fits");
    }
    drop(tx);
    let (terminal_tx, terminal) = tokio::sync::oneshot::channel();
    if let Some(terminal_value) = terminal_value {
        terminal_tx
            .send(terminal_value)
            .expect("fixture terminal receiver is open");
    } else {
        // Model a live producer so queued notification fixtures are parsed
        // before any synthetic terminal closure.
        std::mem::forget(terminal_tx);
    }
    let (cancel, _cancelled) = tokio::sync::oneshot::channel();
    McpSubscription {
        session_id: SessionId("subscription-test".into()),
        server: "fixture".into(),
        client_id: 7,
        acknowledged: McpSubscriptionFilter::default(),
        events,
        terminal,
        cancel: Some(cancel),
        pending_end: None,
        ended: false,
    }
}

#[tokio::test]
async fn modern_mcp_subscription_decodes_all_terminal_states() {
    let fixtures = [
        (
            serde_json::json!({
                "reason":"graceful",
                "result":{"resultType":"complete"}
            }),
            McpSubscriptionEnd::Graceful,
        ),
        (
            serde_json::json!({"reason":"abrupt"}),
            McpSubscriptionEnd::Abrupt,
        ),
        (
            serde_json::json!({"reason":"cancelled"}),
            McpSubscriptionEnd::Cancelled,
        ),
        (
            serde_json::json!({"reason":"lagged","capacity":17}),
            McpSubscriptionEnd::Lagged { capacity: 17 },
        ),
        (
            serde_json::json!({"reason":"error","message":"closed"}),
            McpSubscriptionEnd::Error {
                message: "closed".into(),
            },
        ),
    ];
    for (raw, expected) in fixtures {
        let mut subscription = test_subscription([], Some(raw));
        assert_eq!(
            subscription.next().await.expect("terminal event"),
            Some(McpSubscriptionEvent::Ended(expected))
        );
        assert!(subscription.cancel.is_none());
    }
}

#[tokio::test]
async fn modern_mcp_subscription_rejects_unknown_or_malformed_notifications() {
    for notification in [
        serde_json::json!({"method":"notifications/future"}),
        serde_json::json!({"method":"notifications/resources/updated","params":{}}),
        serde_json::json!({"params":{}}),
    ] {
        let mut subscription = test_subscription(
            [serde_json::json!({
                "type":"notification",
                "notification":notification
            })],
            None,
        );
        assert!(
            subscription.next().await.is_err(),
            "unknown or malformed subscription notifications must fail closed"
        );
    }
}

#[tokio::test]
async fn modern_mcp_subscription_cancel_is_not_blocked_by_a_full_event_queue() {
    let mut subscription = test_subscription(
        [serde_json::json!({
            "type":"notification",
            "notification":{"method":"notifications/tools/list_changed"}
        })],
        None,
    );
    subscription.cancel();
    let event = tokio::time::timeout(std::time::Duration::from_millis(100), subscription.next())
        .await
        .expect("cancellation must not wait for event queue capacity")
        .expect("valid terminal event");
    assert_eq!(
        event,
        Some(McpSubscriptionEvent::Ended(McpSubscriptionEnd::Cancelled))
    );
}

#[test]
fn modern_mcp_mrtr_and_task_parsers_reject_unknown_protocol_variants() {
    assert!(
        parse_input_required(serde_json::json!({
            "resultType":"input_required",
            "inputRequests":{"future":{"method":"future/input"}}
        }))
        .is_err()
    );
    assert!(
        parse_task(
            &SessionId("task-test".into()),
            "fixture",
            1,
            serde_json::json!({
                "taskId":"future-task",
                "status":"future_status",
                "createdAt":"2026-08-09T00:00:00Z",
                "lastUpdatedAt":"2026-08-09T00:00:00Z"
            })
        )
        .is_err()
    );
    for malformed in [
        serde_json::json!({
            "resultType":"input_required",
            "inputRequests":[]
        }),
        serde_json::json!({
            "resultType":"input_required",
            "requestState":7
        }),
    ] {
        assert!(
            parse_input_required(malformed).is_err(),
            "present fields with the wrong protocol type must fail closed"
        );
    }
    assert!(
        parse_input_required(serde_json::json!({
            "resultType":"input_required",
            "requestState":"x".repeat(MAX_MCP_INPUT_PAYLOAD_BYTES)
        }))
        .is_err(),
        "the encoded structured-input round is bounded"
    );
}

#[test]
fn modern_mcp_continuations_are_bound_to_the_exact_origin() {
    let session = SessionId("continuation-session".into());
    let operation = McpOperationIdentity::Tool {
        name: "tool-a".into(),
        arguments: serde_json::json!({"value": 1}),
    };
    let outcome = parse_mcp_operation_outcome(
        &session,
        "server-a",
        serde_json::json!({
            "clientId": 41,
            "outcome": "input_required",
            "result": {
                "resultType": "input_required",
                "inputRequests": {"request-1": {"method": "roots/list"}},
                "requestState": "opaque-state"
            }
        }),
        operation.clone(),
        parse_tool_result,
    )
    .expect("input requirement parses");
    let McpOperationOutcome::InputRequired { input, .. } = outcome else {
        panic!("expected input requirement");
    };
    assert!(input.respond(BTreeMap::new()).is_err());
    let continuation = input
        .respond(BTreeMap::from([(
            "request-1".into(),
            serde_json::json!({"roots": []}),
        )]))
        .expect("exact response IDs are accepted");

    let (responses, request_state, generation) =
        validate_mcp_continuation(Some(continuation.clone()), &session, "server-a", &operation)
            .expect("matching origin is accepted");
    assert_eq!(responses.expect("responses").len(), 1);
    assert_eq!(request_state.as_deref(), Some("opaque-state"));
    assert_eq!(generation, Some(41));

    for (other_session, other_server, other_operation) in [
        (
            SessionId("other-session".into()),
            "server-a",
            operation.clone(),
        ),
        (session.clone(), "server-b", operation.clone()),
        (
            session.clone(),
            "server-a",
            McpOperationIdentity::Tool {
                name: "tool-b".into(),
                arguments: serde_json::json!({"value": 1}),
            },
        ),
        (
            session.clone(),
            "server-a",
            McpOperationIdentity::Prompt {
                name: "tool-a".into(),
                arguments: None,
            },
        ),
    ] {
        assert!(
            validate_mcp_continuation(
                Some(continuation.clone()),
                &other_session,
                other_server,
                &other_operation,
            )
            .is_err(),
            "cross-origin continuation must fail closed"
        );
    }
}

#[test]
fn modern_mcp_continuations_reject_mutated_rounds_and_oversized_answers() {
    let session = SessionId("continuation-session".into());
    let operation = McpOperationIdentity::Tool {
        name: "tool-a".into(),
        arguments: serde_json::json!({}),
    };
    let parse = || {
        parse_mcp_operation_outcome(
            &session,
            "server-a",
            serde_json::json!({
                "clientId": 41,
                "outcome": "input_required",
                "result": {
                    "resultType": "input_required",
                    "inputRequests": {"request-1": {"method": "roots/list"}},
                    "requestState": "opaque-state"
                }
            }),
            operation.clone(),
            parse_tool_result,
        )
        .expect("input requirement parses")
    };

    let McpOperationOutcome::InputRequired { mut input, .. } = parse() else {
        panic!("expected input requirement");
    };
    input.request_state = Some("caller-mutated-state".into());
    assert!(
        input
            .respond(BTreeMap::from([(
                "request-1".into(),
                serde_json::json!({"roots": []}),
            )]))
            .is_err(),
        "a public projection cannot mutate the SDK-bound round"
    );

    let McpOperationOutcome::InputRequired { input, .. } = parse() else {
        panic!("expected input requirement");
    };
    assert!(
        input
            .respond(BTreeMap::from([(
                "request-1".into(),
                serde_json::json!({"roots": ["x".repeat(MAX_MCP_INPUT_PAYLOAD_BYTES)]}),
            )]))
            .is_err(),
        "structured-input answers have an aggregate encoded-byte bound"
    );
}
