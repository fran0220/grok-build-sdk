// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Public-only contract checks for remote mounts, durable MCP Tasks, and the
//! product-UI elicitation authority.

use sophon_sdk::{
    ApiBackend, Error, HostError, InProcessMcpHandler, InProcessMcpServer, MAX_MCP_ENDPOINT_BYTES,
    MAX_MCP_HEADER_VALUE_BYTES, MAX_MCP_STDIO_ARGS, MAX_MCP_TASK_ID_BYTES,
    MAX_MCP_TASK_IDENTITY_BYTES, McpContent, McpElicitationOrigin, McpElicitationService,
    McpElicitationUi, McpElicitationUiRequest, McpHostContext, McpHostServiceError,
    McpHostServices, McpInputRequestKind, McpOperationOutcome, McpServerConfig, McpTaskHandle,
    McpTaskIdentity, McpTaskRecovery, McpTaskStatus, ModelSpec, Runtime, RuntimeConfig,
    RuntimeProfile, SessionConfig,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
};
use tempfile::TempDir;

fn runtime_config(root: &TempDir) -> RuntimeConfig {
    RuntimeConfig {
        endpoint: "http://127.0.0.1:1".into(),
        api_key: "unused-test-key".into(),
        grok_home: root.path().join("grok"),
        session_storage: root.path().join("sessions"),
        models: vec![ModelSpec {
            id: "test-model".into(),
            model_family: None,
            context_window: 131_072,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        }],
    }
}

fn session_config(cwd: PathBuf) -> SessionConfig {
    SessionConfig {
        cwd,
        model: "test-model".into(),
        reasoning: None,
        system_prompt: None,
        rules: None,
    }
}

#[test]
fn remote_mounts_validate_transport_endpoint_headers_and_unknown_fields() {
    let headers = BTreeMap::from([
        ("authorization".into(), "Bearer relay-scoped-token".into()),
        ("x-provider-route".into(), "connection-7".into()),
    ]);
    McpServerConfig::http(
        "provider-http",
        "http://127.0.0.1:8123/mcp",
        headers.clone(),
    )
    .expect("loopback Streamable HTTP mount");
    McpServerConfig::sse("provider-sse", "https://relay.example/mcp", headers)
        .expect("TLS SSE mount");

    for invalid in [
        McpServerConfig::http("provider", "file:///tmp/mcp", BTreeMap::new()),
        McpServerConfig::http(
            "provider",
            "https://secret@relay.example/mcp",
            BTreeMap::new(),
        ),
        McpServerConfig::sse(
            "provider",
            "https://relay.example/mcp#secret",
            BTreeMap::new(),
        ),
        McpServerConfig::http(
            "provider",
            format!(
                "https://relay.example/{}",
                "x".repeat(MAX_MCP_ENDPOINT_BYTES)
            ),
            BTreeMap::new(),
        ),
        McpServerConfig::http(
            "provider",
            "https://relay.example/mcp",
            BTreeMap::from([(
                "authorization".into(),
                "x".repeat(MAX_MCP_HEADER_VALUE_BYTES + 1),
            )]),
        ),
        McpServerConfig::http(
            "provider",
            "https://relay.example/mcp",
            BTreeMap::from([
                ("Authorization".into(), "first".into()),
                ("authorization".into(), "second".into()),
            ]),
        ),
    ] {
        assert!(invalid.is_err(), "invalid remote mount must fail closed");
    }
    assert!(
        serde_json::from_value::<McpServerConfig>(serde_json::json!({
            "type":"websocket",
            "name":"provider",
            "url":"https://relay.example/mcp"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<McpServerConfig>(serde_json::json!({
            "type":"http",
            "name":"provider",
            "url":"https://relay.example/mcp",
            "headers":{},
            "rawProviderCredential":"must-not-exist"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<McpServerConfig>(serde_json::json!({
            "type":"stdio",
            "name":"provider",
            "command":"mcp-provider",
            "args":vec!["x"; MAX_MCP_STDIO_ARGS + 1],
            "env":{}
        }))
        .is_err(),
        "stdio mounts are bounded through the same public decoder"
    );
}

struct TaskFixture {
    status: AtomicU8,
    starts: AtomicU8,
}

impl TaskFixture {
    fn task(&self) -> serde_json::Value {
        if self.status.load(Ordering::Acquire) == 0 {
            serde_json::json!({
                "resultType":"complete",
                "taskId":"provider-task-1",
                "status":"input_required",
                "statusMessage":"waiting for product input",
                "createdAt":"2026-08-11T00:00:00Z",
                "lastUpdatedAt":"2026-08-11T00:00:01Z",
                "ttlMs":60000,
                "inputRequests":{
                    "provider-form":{
                        "method":"elicitation/create",
                        "params":{
                            "mode":"form",
                            "message":"Choose the output format",
                            "requestedSchema":{
                                "type":"object",
                                "properties":{"format":{"type":"string"}},
                                "required":["format"]
                            }
                        }
                    }
                }
            })
        } else {
            serde_json::json!({
                "resultType":"complete",
                "taskId":"provider-task-1",
                "status":"completed",
                "statusMessage":"done",
                "createdAt":"2026-08-11T00:00:00Z",
                "lastUpdatedAt":"2026-08-11T00:00:02Z",
                "ttlMs":60000,
                "result":{"resultType":"complete","content":[{"type":"text","text":"done"}]}
            })
        }
    }
}

#[async_trait::async_trait]
impl InProcessMcpHandler for TaskFixture {
    async fn handle(&self, message: serde_json::Value) -> Result<serde_json::Value, HostError> {
        let id = message.get("id").cloned();
        let result = match message["method"].as_str() {
            Some("server/discover") => serde_json::json!({
                "resultType":"complete",
                "supportedVersions":["2026-07-28"],
                "capabilities":{
                    "tools":{},
                    "extensions":{"io.modelcontextprotocol/tasks":{}}
                },
                "ttlMs":0,
                "cacheScope":"private",
                "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"task-fixture","version":"1"}}
            }),
            Some("tools/list") => serde_json::json!({
                "tools":[{"name":"start","inputSchema":{"type":"object"}}]
            }),
            Some("tools/call") => {
                self.starts.fetch_add(1, Ordering::AcqRel);
                self.status.store(0, Ordering::Release);
                serde_json::json!({
                    "resultType":"task",
                    "taskId":"provider-task-1",
                    "status":"working",
                    "statusMessage":"starting",
                    "createdAt":"2026-08-11T00:00:00Z",
                    "lastUpdatedAt":"2026-08-11T00:00:00Z",
                    "ttlMs":60000,
                    "pollIntervalMs":10
                })
            }
            Some("tasks/get") if message["params"]["taskId"] == "provider-task-1" => self.task(),
            Some("tasks/update") => {
                let response = &message["params"]["inputResponses"]["provider-form"];
                if response["action"] != "accept" || response["content"]["format"] != "json" {
                    return Ok(serde_json::json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "error":{"code":-32602,"message":"invalid product UI answer"}
                    }));
                }
                self.status.store(1, Ordering::Release);
                serde_json::json!({"resultType":"complete"})
            }
            _ => {
                return Ok(serde_json::json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "error":{"code":-32601,"message":"Method not found"}
                }));
            }
        };
        Ok(serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}))
    }
}

#[derive(Default)]
struct ProductUi {
    requests: Mutex<Vec<McpElicitationUiRequest>>,
}

#[async_trait::async_trait]
impl McpElicitationUi for ProductUi {
    async fn elicit(
        &self,
        request: McpElicitationUiRequest,
    ) -> Result<sophon_sdk::mcp_model::ElicitResult, McpHostServiceError> {
        self.requests.lock().unwrap().push(request);
        Ok(sophon_sdk::mcp_model::ElicitResult::new(
            sophon_sdk::mcp_model::ElicitationAction::Accept,
        )
        .with_content(serde_json::json!({"format":"json"})))
    }
}

struct GenericElicitation;

#[async_trait::async_trait]
impl McpElicitationService for GenericElicitation {
    async fn create_elicitation(
        &self,
        _context: McpHostContext,
        _request: sophon_sdk::mcp_model::ElicitRequestParams,
    ) -> Result<sophon_sdk::mcp_model::ElicitResult, McpHostServiceError> {
        Ok(sophon_sdk::mcp_model::ElicitResult::new(
            sophon_sdk::mcp_model::ElicitationAction::Decline,
        ))
    }
}

#[tokio::test]
async fn generic_host_services_cannot_install_a_second_elicitation_answer_path() {
    let root = TempDir::new().expect("root");
    let result = Runtime::builder(runtime_config(&root))
        .profile(RuntimeProfile::Desktop)
        .mcp_host_services(McpHostServices::default().with_elicitation(
            Arc::new(GenericElicitation),
            true,
            true,
            true,
        ))
        .start()
        .await;
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
}

/// Unloads through the documented truthful-retry contract: a teardown that
/// misses its deadline retains the exact actor, binding and lease, so on a
/// heavily loaded machine an unload is slower rather than lost.
async fn unload_with_retries(runtime: &Runtime, session: &sophon_sdk::SessionId) {
    let mut last = None;
    for _ in 0..40 {
        match runtime.unload_session(session.clone()).await {
            Ok(()) => return,
            Err(error) => {
                last = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }
    panic!("session unload kept missing its teardown deadline: {last:?}");
}

async fn wait_for_fixture(runtime: &Runtime, session: &sophon_sdk::SessionId) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if runtime
                .list_mcp_tools(session, Some("provider"))
                .await
                .is_ok_and(|tools| tools.len() == 1)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("MCP fixture initializes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_task_identity_recovers_and_only_the_product_ui_answers_elicitation() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let fixture = Arc::new(TaskFixture {
        status: AtomicU8::new(0),
        starts: AtomicU8::new(0),
    });
    let ui = Arc::new(ProductUi::default());
    let (runtime, _) = Runtime::builder(runtime_config(&root))
        .profile(RuntimeProfile::Desktop)
        .in_process_mcp_servers([InProcessMcpServer::new(
            "provider",
            "provider-fixture",
            fixture.clone(),
        )])
        .mcp_elicitation_ui(ui.clone())
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");
    wait_for_fixture(&runtime, &session).await;

    let task = runtime
        .call_mcp_tool_once(&session, "provider", "start", serde_json::json!({}), None)
        .await
        .expect("Task starts");
    let original = match task {
        McpOperationOutcome::Task { handle, task } => {
            assert_eq!(task.status, McpTaskStatus::Working);
            assert!(task.input_required.is_none());
            handle
        }
        other => panic!("expected Task, got {other:?}"),
    };
    let identity = original.durable_identity().expect("stable identity");
    let persisted = serde_json::to_vec(&identity).expect("identity serializes");
    let identity = McpTaskIdentity::from_json_slice(&persisted)
        .expect("identity validates through its bounded durable decoder");

    unload_with_retries(&runtime, &session).await;
    runtime
        .load_session(session.clone(), session_config(workspace))
        .await
        .expect("session reloads");
    wait_for_fixture(&runtime, &session).await;

    let recovered = match runtime
        .recover_mcp_task(&identity)
        .await
        .expect("recovery result")
    {
        McpTaskRecovery::Reattached { task } => task,
        other => panic!("Task must reattach, got {other:?}"),
    };
    assert_eq!(
        fixture.starts.load(Ordering::Acquire),
        1,
        "recovery must query the existing Task without replaying tools/call"
    );
    assert_ne!(recovered.handle.client_id, original.client_id);
    assert_eq!(
        recovered.input_required.as_ref().unwrap().requests[0].kind,
        McpInputRequestKind::Elicitation
    );
    assert_eq!(
        recovered.handle.durable_identity().unwrap(),
        identity,
        "reattach preserves the durable Task identity"
    );

    assert!(
        runtime
            .update_mcp_task(
                &recovered.handle,
                BTreeMap::from([(
                    "provider-form".into(),
                    serde_json::json!({"action":"accept","content":{"format":"forged"}}),
                )]),
            )
            .await
            .is_err(),
        "generic Task updates cannot forge an elicitation answer"
    );
    runtime
        .resolve_mcp_task_input_with_ui(&recovered.handle, BTreeMap::new())
        .await
        .expect("product UI resolves Task input");
    assert_eq!(
        runtime
            .get_mcp_task(&recovered.handle)
            .await
            .expect("completed status")
            .status,
        McpTaskStatus::Completed
    );
    {
        let requests = ui.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].request_id, "provider-form");
        assert!(matches!(
            &requests[0].origin,
            McpElicitationOrigin::Task { identity: observed, client_id }
                if observed == &identity && *client_id == recovered.handle.client_id
        ));
    }

    runtime
        .call_mcp_tool(&session, "provider", "start", serde_json::json!({}))
        .await
        .expect("automatic Task flow completes through the same product UI");
    assert_eq!(fixture.starts.load(Ordering::Acquire), 2);
    {
        let requests = ui.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].request_id, "provider-form");
        assert!(matches!(
            &requests[1].origin,
            McpElicitationOrigin::Task { identity: observed, .. } if observed == &identity
        ));
    }

    let unknown = McpTaskIdentity::new(session.clone(), "provider", "missing-task").unwrap();
    assert!(matches!(
        runtime.recover_mcp_task(&unknown).await.unwrap(),
        McpTaskRecovery::RecoveryRequired { identity } if identity == unknown
    ));
    unload_with_retries(&runtime, &session).await;
    runtime.shutdown().await.expect("runtime shuts down");
}

/// One recorded HTTP request: the JSON-RPC method it carried and the two
/// host-injected header values exactly as they arrived on the wire.
type RecordedRequest = (String, String, String);

struct RemoteState {
    status: AtomicU8,
    starts: AtomicU8,
    requests: Mutex<Vec<RecordedRequest>>,
}

/// A live Streamable HTTP MCP service for the façade to mount remotely. It
/// speaks the same modern dialect as the in-process fixtures — discovery,
/// tools, durable Tasks and elicitation — while recording, for every request,
/// the header values that actually arrived.
struct RemoteMcpFixture {
    url: String,
    state: Arc<RemoteState>,
    server: tokio::task::JoinHandle<()>,
}

impl RemoteMcpFixture {
    async fn start() -> Self {
        use axum::{
            Json, Router,
            extract::State,
            http::{HeaderMap, StatusCode},
            response::{IntoResponse, Response},
        };

        fn task_body(state: &RemoteState) -> serde_json::Value {
            if state.status.load(Ordering::Acquire) == 0 {
                serde_json::json!({
                    "resultType":"complete",
                    "taskId":"provider-task-1",
                    "status":"input_required",
                    "statusMessage":"waiting for product input",
                    "createdAt":"2026-08-11T00:00:00Z",
                    "lastUpdatedAt":"2026-08-11T00:00:01Z",
                    "ttlMs":60000,
                    "inputRequests":{
                        "provider-form":{
                            "method":"elicitation/create",
                            "params":{
                                "mode":"form",
                                "message":"Choose the output format",
                                "requestedSchema":{
                                    "type":"object",
                                    "properties":{"format":{"type":"string"}},
                                    "required":["format"]
                                }
                            }
                        }
                    }
                })
            } else {
                serde_json::json!({
                    "resultType":"complete",
                    "taskId":"provider-task-1",
                    "status":"completed",
                    "statusMessage":"done",
                    "createdAt":"2026-08-11T00:00:00Z",
                    "lastUpdatedAt":"2026-08-11T00:00:02Z",
                    "ttlMs":60000,
                    "result":{"resultType":"complete","content":[{"type":"text","text":"done"}]}
                })
            }
        }

        async fn handle(
            State(state): State<Arc<RemoteState>>,
            headers: HeaderMap,
            Json(request): Json<serde_json::Value>,
        ) -> Response {
            let method = request["method"].as_str().unwrap_or_default().to_owned();
            let header = |name: &str| {
                headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned()
            };
            state.requests.lock().unwrap().push((
                method.clone(),
                header("authorization"),
                header("x-origin-connection"),
            ));
            let id = request.get("id").cloned();
            let result = match method.as_str() {
                "server/discover" => {
                    return (
                        [("mcp-session-id", "remote-facade-fixture")],
                        Json(serde_json::json!({
                            "jsonrpc":"2.0",
                            "id":id,
                            "result":{
                                "resultType":"complete",
                                "supportedVersions":["2026-07-28"],
                                "capabilities":{
                                    "tools":{},
                                    "extensions":{"io.modelcontextprotocol/tasks":{}}
                                },
                                "ttlMs":0,
                                "cacheScope":"private",
                                "_meta":{"io.modelcontextprotocol/serverInfo":{
                                    "name":"remote-facade-fixture",
                                    "version":"1"
                                }}
                            }
                        })),
                    )
                        .into_response();
                }
                "tools/list" => serde_json::json!({
                    "tools":[
                        {"name":"start","inputSchema":{"type":"object"}},
                        {"name":"ping","inputSchema":{"type":"object"}}
                    ]
                }),
                "tools/call" if request["params"]["name"] == "ping" => serde_json::json!({
                    "content":[{"type":"text","text":"remote pong"}],
                    "isError":false
                }),
                "tools/call" if request["params"]["name"] == "start" => {
                    state.starts.fetch_add(1, Ordering::AcqRel);
                    state.status.store(0, Ordering::Release);
                    serde_json::json!({
                        "resultType":"task",
                        "taskId":"provider-task-1",
                        "status":"working",
                        "statusMessage":"starting",
                        "createdAt":"2026-08-11T00:00:00Z",
                        "lastUpdatedAt":"2026-08-11T00:00:00Z",
                        "ttlMs":60000,
                        "pollIntervalMs":10
                    })
                }
                "tasks/get" if request["params"]["taskId"] == "provider-task-1" => {
                    task_body(&state)
                }
                "tasks/update" => {
                    let response = &request["params"]["inputResponses"]["provider-form"];
                    if response["action"] != "accept" || response["content"]["format"] != "json" {
                        return Json(serde_json::json!({
                            "jsonrpc":"2.0",
                            "id":id,
                            "error":{"code":-32602,"message":"invalid product UI answer"}
                        }))
                        .into_response();
                    }
                    state.status.store(1, Ordering::Release);
                    serde_json::json!({"resultType":"complete"})
                }
                method if method.starts_with("notifications/") => {
                    return StatusCode::ACCEPTED.into_response();
                }
                _ => {
                    return Json(serde_json::json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "error":{"code":-32601,"message":"Method not found"}
                    }))
                    .into_response();
                }
            };
            Json(serde_json::json!({"jsonrpc":"2.0","id":id,"result":result})).into_response()
        }

        let state = Arc::new(RemoteState {
            status: AtomicU8::new(0),
            starts: AtomicU8::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("remote MCP fixture binds");
        let addr = listener.local_addr().expect("remote MCP fixture address");
        // The listen channel also carries the host-injected headers; the
        // fixture records them, then declines the standing stream so nothing
        // outlives a Runtime shutdown. The transport treats 405 as a server
        // without a common SSE stream and continues over POST.
        async fn sse_stream(State(state): State<Arc<RemoteState>>, headers: HeaderMap) -> Response {
            let header = |name: &str| {
                headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned()
            };
            state.requests.lock().unwrap().push((
                "GET".into(),
                header("authorization"),
                header("x-origin-connection"),
            ));
            StatusCode::METHOD_NOT_ALLOWED.into_response()
        }

        let router = Router::new()
            .route("/mcp", axum::routing::get(sse_stream).post(handle))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("remote MCP fixture serves");
        });
        Self {
            url: format!("http://{addr}/mcp"),
            state,
            server,
        }
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.state.requests.lock().unwrap().clone()
    }
}

impl Drop for RemoteMcpFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn wait_for_remote(runtime: &Runtime, session: &sophon_sdk::SessionId, server: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if runtime
                .list_mcp_tools(session, Some(server))
                .await
                .is_ok_and(|tools| tools.len() == 2)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("remote MCP mount initializes");
}

/// The M4 remote-mount gate, proved over live transports rather than config
/// validation: `Http` and `Sse` mounts reach a real Streamable HTTP service,
/// every request carries the host-injected headers exactly as configured, a
/// durable Task's identity survives a full Runtime restart and reattaches
/// without replaying `tools/call`, and structured input is answered only by
/// the product UI channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_remote_mounts_preserve_headers_task_identity_and_the_product_ui_channel() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let http_fixture = RemoteMcpFixture::start().await;
    let sse_fixture = RemoteMcpFixture::start().await;
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let ui = Arc::new(ProductUi::default());
    let mounts = || {
        vec![
            McpServerConfig::http(
                "provider-http",
                http_fixture.url.clone(),
                BTreeMap::from([
                    ("authorization".into(), "Bearer relay-scoped-http".into()),
                    ("x-origin-connection".into(), "connection-7".into()),
                ]),
            )
            .expect("http mount validates"),
            McpServerConfig::sse(
                "provider-sse",
                sse_fixture.url.clone(),
                BTreeMap::from([
                    ("authorization".into(), "Bearer relay-scoped-sse".into()),
                    ("x-origin-connection".into(), "connection-9".into()),
                ]),
            )
            .expect("sse mount validates"),
        ]
    };

    let (runtime, _) = Runtime::builder(runtime_config(&root))
        .profile(RuntimeProfile::Desktop)
        .mcp_servers(mounts())
        .mcp_elicitation_ui(ui.clone())
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");
    wait_for_remote(&runtime, &session, "provider-http").await;
    wait_for_remote(&runtime, &session, "provider-sse").await;

    let pong = runtime
        .call_mcp_tool(&session, "provider-sse", "ping", serde_json::json!({}))
        .await
        .expect("the Sse alias serves a live Streamable HTTP tool call");
    assert!(
        matches!(&pong.content[0], McpContent::Text { text, .. } if text == "remote pong"),
        "unexpected remote tool result: {pong:?}"
    );

    let task = runtime
        .call_mcp_tool_once(
            &session,
            "provider-http",
            "start",
            serde_json::json!({}),
            None,
        )
        .await
        .expect("Task starts over the remote mount");
    let original = match task {
        McpOperationOutcome::Task { handle, task } => {
            assert_eq!(task.status, McpTaskStatus::Working);
            handle
        }
        other => panic!("expected Task, got {other:?}"),
    };
    let persisted =
        serde_json::to_vec(&original.durable_identity().expect("stable identity")).unwrap();

    // A full Runtime restart is the strongest reconnect boundary the SDK owns:
    // the remote service keeps running while every mount, generation and
    // session actor is torn down and rebuilt.
    unload_with_retries(&runtime, &session).await;
    runtime.shutdown().await.expect("first runtime shuts down");
    let (runtime, _) = Runtime::builder(runtime_config(&root))
        .profile(RuntimeProfile::Desktop)
        .mcp_servers(mounts())
        .mcp_elicitation_ui(ui.clone())
        .start()
        .await
        .expect("second runtime starts");
    runtime
        .load_session(session.clone(), session_config(workspace))
        .await
        .expect("session reloads after restart");
    wait_for_remote(&runtime, &session, "provider-http").await;

    let identity = McpTaskIdentity::from_json_slice(&persisted)
        .expect("host-persisted identity bytes restore across the restart");
    let recovered = match runtime
        .recover_mcp_task(&identity)
        .await
        .expect("recovery result")
    {
        McpTaskRecovery::Reattached { task } => task,
        other => panic!("Task must reattach after the restart, got {other:?}"),
    };
    assert_eq!(
        http_fixture.state.starts.load(Ordering::Acquire),
        1,
        "recovery must query the existing Task without replaying tools/call"
    );
    assert_eq!(
        recovered.handle.durable_identity().unwrap(),
        identity,
        "the durable Task identity is preserved across the Runtime restart"
    );

    assert!(
        runtime
            .update_mcp_task(
                &recovered.handle,
                BTreeMap::from([(
                    "provider-form".into(),
                    serde_json::json!({"action":"accept","content":{"format":"forged"}}),
                )]),
            )
            .await
            .is_err(),
        "generic Task updates cannot forge an elicitation answer"
    );
    runtime
        .resolve_mcp_task_input_with_ui(&recovered.handle, BTreeMap::new())
        .await
        .expect("product UI resolves Task input over the remote mount");
    assert_eq!(
        runtime
            .get_mcp_task(&recovered.handle)
            .await
            .expect("completed status")
            .status,
        McpTaskStatus::Completed
    );
    {
        let requests = ui.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0].origin,
            McpElicitationOrigin::Task { identity: observed, client_id }
                if observed == &identity && *client_id == recovered.handle.client_id
        ));
    }

    for (fixture, bearer, connection, transport) in [
        (
            &http_fixture,
            "Bearer relay-scoped-http",
            "connection-7",
            "http",
        ),
        (
            &sse_fixture,
            "Bearer relay-scoped-sse",
            "connection-9",
            "sse",
        ),
    ] {
        let requests = fixture.requests();
        assert!(
            !requests.is_empty(),
            "the {transport} mount received no traffic"
        );
        for (method, authorization, origin_connection) in &requests {
            assert_eq!(
                authorization, bearer,
                "{transport} request `{method}` did not carry the host-injected \
                 Authorization header verbatim"
            );
            assert_eq!(
                origin_connection, connection,
                "{transport} request `{method}` did not carry the host-injected \
                 custom header verbatim"
            );
        }
        let discoveries = requests
            .iter()
            .filter(|(method, ..)| method == "server/discover")
            .count();
        if transport == "http" {
            assert!(
                discoveries >= 2,
                "both Runtime generations must have discovered the {transport} mount"
            );
        }
    }

    unload_with_retries(&runtime, &session).await;
    runtime.shutdown().await.expect("second runtime shuts down");
}

#[test]
fn durable_task_identity_decode_is_bounded_and_fail_closed() {
    let oversized = "x".repeat(MAX_MCP_TASK_ID_BYTES + 1);
    assert!(
        serde_json::from_value::<McpTaskIdentity>(serde_json::json!({
            "session_id":"session-1",
            "server":"provider",
            "task_id":oversized
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<McpTaskIdentity>(serde_json::json!({
            "session_id":"session-1",
            "server":"provider",
            "task_id":"task-1",
            "future":"unknown"
        }))
        .is_err()
    );
    let invalid = McpTaskIdentity::new(
        sophon_sdk::SessionId::from_stored("session-1"),
        "provider",
        "x".repeat(MAX_MCP_TASK_ID_BYTES + 1),
    );
    assert!(matches!(invalid, Err(Error::InvalidConfig(_))));
    assert!(
        McpTaskIdentity::from_json_slice(&vec![b'x'; MAX_MCP_TASK_IDENTITY_BYTES + 1]).is_err(),
        "durable identity restoration rejects oversized source bytes before parsing"
    );
    assert!(
        McpTaskHandle {
            session_id: sophon_sdk::SessionId::from_stored("session-1"),
            server: "provider".into(),
            client_id: 7,
            task_id: "x".repeat(MAX_MCP_TASK_ID_BYTES + 1),
        }
        .durable_identity()
        .is_err(),
        "unchecked generation handles are validated before Task operations"
    );
}
