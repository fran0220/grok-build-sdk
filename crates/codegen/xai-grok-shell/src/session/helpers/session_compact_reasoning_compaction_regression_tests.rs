use super::*;
use crate::sampling::{Client, ContentPart, ConversationItem, SamplerConfig, ToolCall, rs};
use axum::Router;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::post;
use futures_util::stream;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Minimal ChatCompletions SSE stream: one content token, `stop`, then `[DONE]`.
fn summary_stream() -> Vec<Event> {
    vec![
        Event::default().data(
            json!({
                "id": "chatcmpl-test",
                "object": "chat.completion.chunk",
                "created": 1234567890,
                "model": "test-model",
                "choices": [{
                    "index": 0,
                    "delta": { "role": "assistant", "content": "<summary>ok</summary>" },
                    "finish_reason": "stop"
                }]
            })
            .to_string(),
        ),
        Event::default().data("[DONE]"),
    ]
}

/// SSE stream: a reasoning delta (no content), then a content delta + `stop`.
fn reasoning_then_summary_stream() -> Vec<Event> {
    vec![
        Event::default().data(
            json!({
                "id": "chatcmpl-test",
                "object": "chat.completion.chunk",
                "created": 1234567890,
                "model": "test-model",
                "choices": [{
                    "index": 0,
                    "delta": { "role": "assistant", "reasoning_content": "let me think about the summary" },
                    "finish_reason": null
                }]
            })
            .to_string(),
        ),
        Event::default().data(
            json!({
                "id": "chatcmpl-test",
                "object": "chat.completion.chunk",
                "created": 1234567890,
                "model": "test-model",
                "choices": [{
                    "index": 0,
                    "delta": { "content": "<summary>ok</summary>" },
                    "finish_reason": "stop"
                }]
            })
            .to_string(),
        ),
        Event::default().data("[DONE]"),
    ]
}

/// A reasoning delta that precedes the content delta must not break
/// summary extraction.
#[tokio::test]
async fn chat_completions_compaction_extracts_summary_after_reasoning_delta() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let stream = stream::iter(
                reasoning_then_summary_stream()
                    .into_iter()
                    .map(Ok::<_, std::convert::Infallible>),
            );
            Sse::new(stream).keep_alive(KeepAlive::default())
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let base_url = format!("http://{addr}/v1");
    let config = test_config(&base_url);
    let client = Client::new(config.clone()).unwrap();

    let chat_history = vec![
        ConversationItem::system("You are a helpful assistant."),
        ConversationItem::user("<user_query>\nfix the bug\n</user_query>"),
        ConversationItem::assistant("I fixed it."),
        ConversationItem::user("Summarize the conversation so far."),
    ];

    let output = generate_session_compact(
        chat_history,
        0,
        vec![],
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_secs(30),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|_| panic!("compaction must succeed"));

    // A reasoning delta arriving before the content delta must not break
    // summary extraction — the content channel is returned as the summary.
    assert_eq!(output.content, "<summary>ok</summary>");

    let _ = shutdown_tx.send(());
}

const USER_IMAGE_SENTINEL: &str = "data:image/png;base64,user-image-sentinel";
const TOOL_IMAGE_SENTINEL: &str = "data:image/png;base64,tool-image-sentinel";
const FINAL_PROMPT_SENTINEL: &str = "FINAL_COMPACTION_PROMPT_SENTINEL";

fn image_compaction_history() -> Vec<ConversationItem> {
    vec![
        ConversationItem::system("You are a helpful assistant."),
        ConversationItem::user_with_parts(vec![
            ContentPart::Text {
                text: "user text sentinel".into(),
            },
            ContentPart::Image {
                url: USER_IMAGE_SENTINEL.into(),
            },
        ]),
        ConversationItem::assistant_tool_calls(vec![ToolCall {
            id: "call-image-sentinel".into(),
            name: "read_file".into(),
            arguments: r#"{"target_file":"image.png"}"#.into(),
        }]),
        ConversationItem::tool_result_with_images(
            "call-image-sentinel",
            "tool result text sentinel",
            vec![ContentPart::Image {
                url: TOOL_IMAGE_SENTINEL.into(),
            }],
        ),
        ConversationItem::user(FINAL_PROMPT_SENTINEL),
    ]
}

fn assert_preserved_compaction_body(body: &serde_json::Value) {
    let serialized = body.to_string();
    assert!(serialized.contains(USER_IMAGE_SENTINEL));
    assert!(serialized.contains(TOOL_IMAGE_SENTINEL));
    assert!(serialized.contains("user text sentinel"));
    assert!(serialized.contains("tool result text sentinel"));
    assert!(serialized.contains("call-image-sentinel"));
    assert!(serialized.contains(FINAL_PROMPT_SENTINEL));
}

fn test_config(base_url: &str) -> SamplerConfig {
    SamplerConfig {
        api_key: Some("test-api-key".to_string()),
        base_url: base_url.to_string(),
        model: "test-model".to_string(),
        max_completion_tokens: Some(1000),
        temperature: Some(0.7),
        top_p: None,
        api_backend: ApiBackend::ChatCompletions,
        auth_scheme: Default::default(),
        extra_headers: Default::default(),
        extra_response_includes: Vec::new(),
        query_params: Default::default(),
        env_http_headers: Default::default(),
        context_window: 256_000,
        client_version: None,
        force_http1: false,
        max_retries: None,
        stream_tool_calls: false,
        idle_timeout_secs: None,
        client_identifier: None,
        reasoning_effort: None,
        deployment_id: None,
        user_id: None,
        origin_client: None,
        attribution_callback: None,
        bearer_resolver: None,
        supports_backend_search: false,
        compactions_remaining: None,
        compaction_at_tokens: None,
        doom_loop_recovery: None,
        header_injector: None,
    }
}

#[tokio::test]
async fn chat_completions_compaction_does_not_panic_on_reasoning_sibling() {
    // Mock ChatCompletions endpoint returning a tiny summary stream.
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let stream = stream::iter(
                summary_stream()
                    .into_iter()
                    .map(Ok::<_, std::convert::Infallible>),
            );
            Sse::new(stream).keep_alive(KeepAlive::default())
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let base_url = format!("http://{addr}/v1");
    let config = test_config(&base_url);
    let client = Client::new(config.clone()).unwrap();

    // Responses-API session resumed onto ChatCompletions: a standalone `Reasoning` sibling.
    let chat_history = vec![
        ConversationItem::system("You are a helpful assistant."),
        ConversationItem::user("<user_query>\nfix the bug\n</user_query>"),
        ConversationItem::Reasoning(rs::ReasoningItem {
            id: "r1".to_string(),
            summary: vec![rs::SummaryPart::SummaryText(rs::SummaryTextContent {
                text: "thinking about the bug".to_string(),
            })],
            content: None,
            encrypted_content: None,
            status: None,
        }),
        ConversationItem::assistant("I fixed it."),
        ConversationItem::user("Summarize the conversation so far."),
    ];

    let result = generate_session_compact(
        chat_history,
        0,
        vec![],
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_secs(30),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;

    let output = result
        .unwrap_or_else(|_| panic!("compaction must succeed for a Reasoning-bearing history"));
    assert_eq!(output.content, "<summary>ok</summary>");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn chat_completions_below_trigger_preserves_images_and_tools() {
    use std::sync::{Arc, Mutex};

    // Mock ChatCompletions endpoint that captures each request body.
    let captured: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let cap = captured.clone();
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move |body: axum::Json<serde_json::Value>| {
            let cap = cap.clone();
            async move {
                cap.lock().unwrap().push(body.0);
                let stream = stream::iter(
                    summary_stream()
                        .into_iter()
                        .map(Ok::<_, std::convert::Infallible>),
                );
                Sse::new(stream).keep_alive(KeepAlive::default())
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let base_url = format!("http://{addr}/v1");
    let config = test_config(&base_url);

    let chat_history = image_compaction_history();
    let tools = vec![ToolSpec {
        name: "read_file".to_string(),
        description: Some("Reads a file".to_string()),
        parameters: json!({"type": "object", "properties": {}}),
    }];
    let client = Client::new(config.clone()).unwrap();
    generate_session_compact(
        chat_history.clone(),
        0,
        tools,
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_secs(30),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|_| panic!("compaction with tools must succeed"));

    // Without tools: neither key present.
    let client = Client::new(config.clone()).unwrap();
    generate_session_compact(
        chat_history,
        0,
        vec![],
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_secs(30),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|_| panic!("compaction without tools must succeed"));

    let bodies = captured.lock().unwrap();
    assert_eq!(bodies.len(), 2, "mock must have served both requests");

    let with_tools = &bodies[0];
    assert_eq!(
        with_tools["tool_choice"],
        json!("auto"),
        "default compaction tool_choice is auto"
    );
    let sent_tools = with_tools["tools"]
        .as_array()
        .expect("tools must be attached for prefix-cache alignment");
    assert_eq!(sent_tools.len(), 1);
    assert_eq!(sent_tools[0]["function"]["name"], json!("read_file"));
    assert_preserved_compaction_body(with_tools);

    let without_tools = &bodies[1];
    assert!(
        without_tools.get("tools").is_none(),
        "no tools key when none are passed"
    );
    assert!(
        without_tools.get("tool_choice").is_none(),
        "tool_choice without tools is rejected by OpenAI-compat backends"
    );

    let _ = shutdown_tx.send(());
}

fn responses_summary_stream() -> Vec<Event> {
    vec![
        Event::default().data(
            json!({
                "type": "response.created",
                "sequence_number": 0,
                "response": {
                    "id": "resp_test",
                    "object": "response",
                    "created_at": 1234567890,
                    "model": "test-model",
                    "status": "in_progress",
                    "output": []
                }
            })
            .to_string(),
        ),
        Event::default().data(
            json!({
                "type": "response.output_text.delta",
                "sequence_number": 1,
                "item_id": "msg_test",
                "output_index": 0,
                "content_index": 0,
                "delta": "<summary>ok</summary>"
            })
            .to_string(),
        ),
        Event::default().data(
            json!({
                "type": "response.completed",
                "sequence_number": 2,
                "response": {
                    "id": "resp_test",
                    "object": "response",
                    "created_at": 1234567890,
                    "model": "test-model",
                    "status": "completed",
                    "output": []
                }
            })
            .to_string(),
        ),
    ]
}

fn test_config_responses(base_url: &str) -> SamplerConfig {
    let mut config = test_config(base_url);
    config.api_backend = ApiBackend::Responses;
    config
}

#[tokio::test]
async fn responses_below_trigger_preserves_images_and_tools() {
    use std::sync::{Arc, Mutex};

    let captured: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let cap = captured.clone();
    let app = Router::new().route(
        "/v1/responses",
        post(move |body: axum::Json<serde_json::Value>| {
            let cap = cap.clone();
            async move {
                cap.lock().unwrap().push(body.0);
                let stream = stream::iter(
                    responses_summary_stream()
                        .into_iter()
                        .map(Ok::<_, std::convert::Infallible>),
                );
                Sse::new(stream).keep_alive(KeepAlive::default())
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let base_url = format!("http://{addr}/v1");
    let mut config = test_config_responses(&base_url);
    config.stream_tool_calls = true;

    let chat_history = image_compaction_history();
    let tools = vec![ToolSpec {
        name: "read_file".to_string(),
        description: Some("Reads a file".to_string()),
        parameters: json!({"type": "object", "properties": {}}),
    }];
    let hosted = vec![HostedTool::WebSearch { options: None }];
    let client = Client::new(config.clone()).unwrap();
    generate_session_compact(
        chat_history.clone(),
        0,
        tools,
        hosted,
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_secs(30),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|_| panic!("Responses compaction with tools must succeed"));

    let client = Client::new(config.clone()).unwrap();
    generate_session_compact(
        chat_history,
        0,
        vec![],
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_secs(30),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|_| panic!("Responses compaction without tools must succeed"));

    let bodies = captured.lock().unwrap();
    assert_eq!(bodies.len(), 2, "mock must have served both requests");

    let with_tools = &bodies[0];
    assert_eq!(
        with_tools["tool_choice"],
        json!("auto"),
        "default Responses compaction tool_choice is auto"
    );
    let sent_tools = with_tools["tools"]
        .as_array()
        .expect("tools must be attached for prefix-cache alignment");
    let has_read_file = sent_tools.iter().any(|t| {
        t.get("name") == Some(&json!("read_file"))
            || t.pointer("/name") == Some(&json!("read_file"))
    });
    assert!(
        has_read_file,
        "client function tool must be present: {sent_tools:?}"
    );
    assert!(
        sent_tools
            .iter()
            .any(|t| t.get("type") == Some(&json!("web_search"))),
        "hosted web_search must be present for prefix alignment: {sent_tools:?}"
    );
    assert_preserved_compaction_body(with_tools);
    // The dispatched body must carry every field the prepared request froze,
    // so the observed request equals what the model actually applies.
    assert_eq!(with_tools["store"], false);
    assert_eq!(with_tools["stream"], true);
    assert_eq!(with_tools["stream_tool_calls"], true);
    assert_eq!(with_tools["reasoning"]["summary"], "concise");
    assert_eq!(
        with_tools["include"],
        json!(["reasoning.encrypted_content"])
    );

    let without_tools = &bodies[1];
    assert!(
        without_tools
            .get("tools")
            .map(|t| t.as_array().is_none_or(|a| a.is_empty()))
            .unwrap_or(true),
        "no tools when none are passed"
    );
    assert!(
        without_tools.get("tool_choice").is_none(),
        "tool_choice without tools should be omitted"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn stalled_compaction_stream_times_out_as_transient() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let stream = stream::pending::<Result<Event, std::convert::Infallible>>();
            Sse::new(stream)
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let base_url = format!("http://{addr}/v1");
    let config = test_config(&base_url);
    let client = Client::new(config.clone()).unwrap();

    let chat_history = vec![
        ConversationItem::system("You are a helpful assistant."),
        ConversationItem::user("Summarize the conversation so far."),
    ];

    let result = generate_session_compact(
        chat_history,
        0,
        vec![],
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_millis(150),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;

    match result {
        Err(CompactFailure::Transient(err)) => {
            let data = err
                .data
                .as_ref()
                .and_then(|d| d.as_str())
                .unwrap_or_default();
            assert!(
                data.contains("idle timeout"),
                "expected an idle-timeout transient failure, got: {data}"
            );
        }
        Err(CompactFailure::Deterministic(_) | CompactFailure::Cancelled) => {
            panic!("a stalled stream must be retryable (Transient), not Deterministic/Cancelled")
        }
        Ok(_) => panic!("a stalled stream must not produce a summary"),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn completed_then_stalled_stream_errors_no_salvage() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let events = stream::iter(vec![Ok::<_, std::convert::Infallible>(
                Event::default().data(
                    json!({
                        "id": "chatcmpl-test",
                        "object": "chat.completion.chunk",
                        "created": 1234567890,
                        "model": "test-model",
                        "choices": [{
                            "index": 0,
                            "delta": { "role": "assistant", "content": "<summary>ok</summary>" },
                            "finish_reason": "stop"
                        }]
                    })
                    .to_string(),
                ),
            )])
            .chain(stream::pending::<Result<Event, std::convert::Infallible>>());
            Sse::new(events)
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let base_url = format!("http://{addr}/v1");
    let config = test_config(&base_url);
    let client = Client::new(config.clone()).unwrap();

    let chat_history = vec![
        ConversationItem::system("You are a helpful assistant."),
        ConversationItem::user("Summarize the conversation so far."),
    ];

    let result = generate_session_compact(
        chat_history,
        0,
        vec![],
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_millis(150),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;

    match result {
        Err(CompactFailure::Transient(err)) => {
            let data = err
                .data
                .as_ref()
                .and_then(|d| d.as_str())
                .unwrap_or_default();
            assert!(
                data.contains("idle timeout"),
                "expected an idle-timeout transient failure, got: {data}"
            );
        }
        Err(CompactFailure::Deterministic(_) | CompactFailure::Cancelled) => {
            panic!("a stalled stream must be retryable (Transient), not Deterministic")
        }
        Ok(_) => panic!(
            "salvage removed: a completed-but-unterminated stream must error, not return a summary"
        ),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn substantial_partial_errors_no_salvage() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let body = "x".repeat(2500);
            let events = stream::iter(vec![Ok::<_, std::convert::Infallible>(
                Event::default().data(
                    json!({
                        "id": "chatcmpl-test",
                        "object": "chat.completion.chunk",
                        "created": 1234567890,
                        "model": "test-model",
                        "choices": [{
                            "index": 0,
                            "delta": { "role": "assistant", "content": body }
                        }]
                    })
                    .to_string(),
                ),
            )])
            .chain(stream::pending::<Result<Event, std::convert::Infallible>>());
            Sse::new(events)
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let base_url = format!("http://{addr}/v1");
    let config = test_config(&base_url);
    let client = Client::new(config.clone()).unwrap();

    let chat_history = vec![
        ConversationItem::system("You are a helpful assistant."),
        ConversationItem::user("Summarize the conversation so far."),
    ];

    let result = generate_session_compact(
        chat_history,
        0,
        vec![],
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_millis(150),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;

    match result {
        Err(CompactFailure::Transient(err)) => {
            let data = err
                .data
                .as_ref()
                .and_then(|d| d.as_str())
                .unwrap_or_default();
            assert!(
                data.contains("idle timeout"),
                "expected an idle-timeout transient failure, got: {data}"
            );
        }
        Err(CompactFailure::Deterministic(_) | CompactFailure::Cancelled) => {
            panic!("a stalled stream must be retryable (Transient), not Deterministic")
        }
        Ok(_) => panic!("salvage removed: a substantial partial must error, not be returned"),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn thin_partial_retries_on_stall() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let events = stream::iter(vec![Ok::<_, std::convert::Infallible>(
                Event::default().data(
                    json!({
                        "id": "chatcmpl-test",
                        "object": "chat.completion.chunk",
                        "created": 1234567890,
                        "model": "test-model",
                        "choices": [{
                            "index": 0,
                            "delta": { "role": "assistant", "content": "partial" }
                        }]
                    })
                    .to_string(),
                ),
            )])
            .chain(stream::pending::<Result<Event, std::convert::Infallible>>());
            Sse::new(events)
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let base_url = format!("http://{addr}/v1");
    let config = test_config(&base_url);
    let client = Client::new(config.clone()).unwrap();

    let chat_history = vec![
        ConversationItem::system("You are a helpful assistant."),
        ConversationItem::user("Summarize the conversation so far."),
    ];

    let result = generate_session_compact(
        chat_history,
        0,
        vec![],
        vec![],
        client,
        acp::SessionId::new("test-session"),
        &config,
        std::time::Duration::from_millis(150),
        0,
        crate::util::config::CompactionToolChoice::Auto,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;

    match result {
        Err(CompactFailure::Transient(_)) => {}
        _ => panic!("a thin stalled body must retry (Transient), not salvage"),
    }

    let _ = shutdown_tx.send(());
}

#[test]
fn prepared_digest_tracks_effective_request_and_excludes_transport_credentials() {
    let history = vec![ConversationItem::user("exact request")];
    let mut first = test_config("https://secret.example/private-path");
    first.api_key = Some("credential-one".into());
    first
        .extra_headers
        .insert("authorization".into(), "secret-header-one".into());
    let first_request = prepare_compaction_request(
        history.clone(),
        vec![],
        vec![],
        &first,
        crate::util::config::CompactionToolChoice::Auto,
    );
    let first_facts = first_request.digest_bytes().unwrap();

    let mut transport_changed = first.clone();
    transport_changed.api_key = Some("credential-two".into());
    transport_changed.base_url = "https://other.example/another-private-path".into();
    transport_changed.extra_headers.clear();
    transport_changed.temperature = Some(0.1);
    let transport_facts = prepare_compaction_request(
        history.clone(),
        vec![],
        vec![],
        &transport_changed,
        crate::util::config::CompactionToolChoice::Auto,
    )
    .digest_bytes()
    .unwrap();
    assert_eq!(first_facts.messages, transport_facts.messages);
    assert_eq!(first_facts.tools, transport_facts.tools);
    assert_eq!(first_facts.hosted_tools, transport_facts.hosted_tools);
    assert_eq!(
        first_facts.model_parameters,
        transport_facts.model_parameters
    );
    let mut chat_parameters_changed = transport_changed.clone();
    chat_parameters_changed.max_completion_tokens = Some(7);
    chat_parameters_changed.top_p = Some(0.4);
    assert_ne!(
        first_facts.model_parameters,
        prepare_compaction_request(
            history.clone(),
            vec![],
            vec![],
            &chat_parameters_changed,
            crate::util::config::CompactionToolChoice::Auto,
        )
        .digest_bytes()
        .unwrap()
        .model_parameters,
        "Chat defaults applied at dispatch must be frozen into the observed request",
    );

    let mut responses = first.clone();
    responses.api_backend = ApiBackend::Responses;
    responses.max_completion_tokens = Some(4_096);
    responses.top_p = Some(0.8);
    responses.reasoning_effort = Some(xai_grok_sampling_types::ReasoningEffort::High);
    let responses_facts = prepare_compaction_request(
        history.clone(),
        vec![],
        vec![],
        &responses,
        crate::util::config::CompactionToolChoice::Auto,
    )
    .digest_bytes()
    .unwrap();
    let responses_parameters: serde_json::Value =
        serde_json::from_slice(&responses_facts.model_parameters).unwrap();
    assert_eq!(responses_parameters["store"], false);
    assert_eq!(responses_parameters["stream"], true);
    assert_eq!(responses_parameters["reasoning"]["summary"], "concise");
    assert_eq!(
        responses_parameters["include"],
        serde_json::json!(["reasoning.encrypted_content"]),
    );
    let mut responses_changed = responses.clone();
    responses_changed.max_completion_tokens = Some(2_048);
    assert_ne!(
        responses_facts.model_parameters,
        prepare_compaction_request(
            history.clone(),
            vec![],
            vec![],
            &responses_changed,
            crate::util::config::CompactionToolChoice::Auto,
        )
        .digest_bytes()
        .unwrap()
        .model_parameters,
        "sampler defaults that become effective request fields must be digested",
    );
    responses_changed.max_completion_tokens = responses.max_completion_tokens;
    responses_changed.api_key = Some("different-credential".into());
    responses_changed.base_url = "https://different.example/private".into();
    assert_eq!(
        responses_facts.model_parameters,
        prepare_compaction_request(
            history.clone(),
            vec![],
            vec![],
            &responses_changed,
            crate::util::config::CompactionToolChoice::Auto,
        )
        .digest_bytes()
        .unwrap()
        .model_parameters,
        "transport configuration must remain outside the semantic request digest",
    );
    responses_changed.stream_tool_calls = true;
    let stream_tool_call_facts = prepare_compaction_request(
        history.clone(),
        vec![],
        vec![],
        &responses_changed,
        crate::util::config::CompactionToolChoice::Auto,
    )
    .digest_bytes()
    .unwrap();
    assert_ne!(
        responses_facts.model_parameters, stream_tool_call_facts.model_parameters,
        "the xAI Responses body extension is part of the applying request",
    );
    let colliding_tool = ToolSpec {
        name: "web_search".into(),
        description: Some("shadowed function".into()),
        parameters: serde_json::json!({"type": "object"}),
    };
    let hosted_facts = prepare_compaction_request(
        history.clone(),
        vec![colliding_tool.clone()],
        vec![HostedTool::WebSearch { options: None }],
        &responses,
        crate::util::config::CompactionToolChoice::Auto,
    )
    .digest_bytes()
    .unwrap();
    assert_eq!(hosted_facts.tool_count, 0);
    assert_eq!(hosted_facts.hosted_tool_count, 1);
    assert_ne!(responses_facts.hosted_tools, hosted_facts.hosted_tools);

    let mut messages = responses.clone();
    messages.api_backend = ApiBackend::Messages;
    let messages_facts = prepare_compaction_request(
        history.clone(),
        vec![colliding_tool],
        vec![HostedTool::WebSearch { options: None }],
        &messages,
        crate::util::config::CompactionToolChoice::Auto,
    )
    .digest_bytes()
    .unwrap();
    assert_eq!(messages_facts.tool_count, 1);
    assert_eq!(messages_facts.hosted_tool_count, 0);

    let mut messages_default = messages.clone();
    messages_default.max_completion_tokens = None;
    let converted_history = vec![
        ConversationItem::user("use the tool"),
        ConversationItem::assistant_tool_calls(vec![crate::sampling::ToolCall {
            id: "call:unsafe".into(),
            name: "web_search".into(),
            arguments: "not-json".into(),
        }]),
        ConversationItem::tool_result("call:unsafe", "done"),
        ConversationItem::user("summarize"),
    ];
    let converted_messages = prepare_compaction_request(
        converted_history,
        vec![],
        vec![],
        &messages_default,
        crate::util::config::CompactionToolChoice::Auto,
    );
    let PreparedCompactionRequest::Messages(converted_request) = &converted_messages else {
        panic!("Messages backend must freeze a MessagesRequest")
    };
    assert_eq!(
        converted_request.inner.max_tokens,
        xai_grok_sampler::ANTHROPIC_DEFAULT_MAX_TOKENS,
    );
    assert_eq!(converted_request.inner.stream, Some(true));
    let converted_facts = converted_messages.digest_bytes().unwrap();
    let converted_wire = String::from_utf8(converted_facts.messages).unwrap();
    assert!(!converted_wire.contains("call:unsafe"));
    assert!(converted_wire.contains("call_unsafe"));
    assert!(!converted_wire.contains("not-json"));
    let converted_parameters: serde_json::Value =
        serde_json::from_slice(&converted_facts.model_parameters).unwrap();
    assert_eq!(
        converted_parameters["max_tokens"],
        xai_grok_sampler::ANTHROPIC_DEFAULT_MAX_TOKENS,
    );
    assert_eq!(converted_parameters["stream"], true);

    let mut model_changed = transport_changed;
    model_changed.model = "different-effective-model".into();
    let model_facts = prepare_compaction_request(
        history,
        vec![],
        vec![],
        &model_changed,
        crate::util::config::CompactionToolChoice::Auto,
    )
    .digest_bytes()
    .unwrap();
    assert_ne!(first_facts.model_parameters, model_facts.model_parameters);
    let combined = [
        first_facts.messages,
        first_facts.tools,
        first_facts.hosted_tools,
        first_facts.model_parameters,
    ]
    .concat();
    let combined = String::from_utf8(combined).unwrap();
    for secret in [
        "credential-one",
        "secret-header-one",
        "secret.example",
        "private-path",
    ] {
        assert!(!combined.contains(secret));
    }
}
