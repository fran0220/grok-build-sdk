use super::*;

struct McpHttpMock {
    url: String,
    tools_listed: Arc<AtomicBool>,
    headers: Arc<Mutex<Vec<(String, String)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl McpHttpMock {
    async fn start() -> Self {
        use axum::{
            Json, Router,
            extract::State,
            http::{HeaderMap, StatusCode},
            response::{IntoResponse, Response},
            routing::post,
        };

        #[derive(Clone)]
        struct McpState {
            tools_listed: Arc<AtomicBool>,
            headers: Arc<Mutex<Vec<(String, String)>>>,
        }

        async fn handle(
            State(state): State<McpState>,
            headers: HeaderMap,
            Json(request): Json<serde_json::Value>,
        ) -> Response {
            match request["method"].as_str() {
                Some("server/discover") => (
                    [("mcp-session-id", "origin-runtime-http-test")],
                    Json(serde_json::json!({
                        "jsonrpc":"2.0",
                        "id":request["id"],
                        "result":{
                            "resultType":"complete",
                            "supportedVersions":["2026-07-28"],
                            "capabilities":{"tools":{}},
                            "ttlMs":0,
                            "cacheScope":"private",
                            "_meta":{"io.modelcontextprotocol/serverInfo":{
                                "name":"origin-runtime-http-test",
                                "version":"1"
                            }}
                        }
                    })),
                )
                    .into_response(),
                Some("tools/list") => {
                    let authorization = headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    let provider = headers
                        .get("x-origin-mcp")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    state
                        .headers
                        .lock()
                        .expect("MCP HTTP headers")
                        .push((authorization, provider));
                    state.tools_listed.store(true, Ordering::Release);
                    Json(serde_json::json!({
                        "jsonrpc":"2.0",
                        "id":request["id"],
                        "result":{"tools":[{
                            "name":"echo",
                            "description":"echo",
                            "inputSchema":{"type":"object"}
                        }]}
                    }))
                    .into_response()
                }
                _ => StatusCode::ACCEPTED.into_response(),
            }
        }

        let tools_listed = Arc::new(AtomicBool::new(false));
        let headers = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind MCP HTTP mock");
        let addr = listener.local_addr().expect("MCP HTTP mock address");
        let router = Router::new()
            .route("/mcp", post(handle))
            .with_state(McpState {
                tools_listed: tools_listed.clone(),
                headers: headers.clone(),
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("MCP HTTP mock serves");
        });
        Self {
            url: format!("http://{addr}/mcp"),
            tools_listed,
            headers,
            task,
        }
    }

    async fn wait_for_tools(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !self.tools_listed.load(Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("MCP HTTP discovery and tools/list complete");
    }

    fn headers(&self) -> Vec<(String, String)> {
        self.headers.lock().expect("MCP HTTP headers").clone()
    }
}

impl Drop for McpHttpMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) struct InProcessFixture {
    pub(super) contexts: Arc<std::sync::Mutex<Vec<InProcessMcpContext>>>,
}
#[async_trait::async_trait]
impl InProcessMcpHandler for InProcessFixture {
    async fn handle(&self, message: serde_json::Value) -> Result<serde_json::Value, HostError> {
        let id = message.get("id").cloned();
        let result = match message["method"].as_str() {
            Some("server/discover") => serde_json::json!({
                "resultType":"complete",
                "supportedVersions":["2026-07-28"],
                "capabilities":{"tools":{},"resources":{},"prompts":{},"completions":{}},
                "ttlMs":0,
                "cacheScope":"private",
                "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"sdk-fixture","version":"1"}}
            }),
            Some("tools/list") => {
                serde_json::json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]})
            }
            Some("tools/call") => {
                serde_json::json!({"content":[{"type":"text","text":"in-process ok"}],"isError":false})
            }
            Some("resources/list") => {
                serde_json::json!({"resources":[{"uri":"fixture://one","name":"one"}]})
            }
            Some("resources/templates/list") => {
                serde_json::json!({"resourceTemplates":[{"uriTemplate":"fixture://{id}","name":"by id"}]})
            }
            Some("prompts/list") => {
                serde_json::json!({"prompts":[{"name":"welcome","description":"Welcome prompt","arguments":[{"name":"who"}]}]})
            }
            Some("prompts/get") => {
                serde_json::json!({"description":"rendered","messages":[{"role":"user","content":{"type":"text","text":format!("hello {}", message["params"]["arguments"]["who"].as_str().unwrap_or("world"))}}]})
            }
            Some("completion/complete") => {
                serde_json::json!({"completion":{"values":["alice","alex"],"total":2,"hasMore":false}})
            }
            _ => {
                return Ok(
                    serde_json::json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Method not found"}}),
                );
            }
        };
        Ok(serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}))
    }

    async fn handle_with_context(
        &self,
        context: &InProcessMcpContext,
        message: serde_json::Value,
    ) -> Result<serde_json::Value, HostError> {
        self.contexts.lock().unwrap().push(context.clone());
        self.handle(message).await
    }
}

struct LiveGatewayMcpFixture {
    called: Arc<AtomicBool>,
}
#[async_trait::async_trait]
impl InProcessMcpHandler for LiveGatewayMcpFixture {
    async fn handle(&self, message: serde_json::Value) -> Result<serde_json::Value, HostError> {
        let id = message.get("id").cloned();
        let result = match message["method"].as_str() {
            Some("server/discover") => serde_json::json!({
                "resultType":"complete",
                "supportedVersions":["2026-07-28"],
                "capabilities":{"tools":{}},
                "ttlMs":0,
                "cacheScope":"private",
                "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"live-sdk-fixture","version":"1"}}
            }),
            Some("tools/list") => serde_json::json!({
                "tools":[{
                    "name":"gateway_probe",
                    "description":"Required verification tool. Call it with code LIVE_GATEWAY_E2E.",
                    "inputSchema":{
                        "type":"object",
                        "properties":{"code":{"type":"string"}},
                        "required":["code"]
                    }
                }]
            }),
            Some("tools/call") => {
                if message["params"]["arguments"]["code"] == "LIVE_GATEWAY_E2E" {
                    self.called.store(true, Ordering::Release);
                }
                serde_json::json!({
                    "content":[{"type":"text","text":"LIVE_MCP_TOOL_OK"}],
                    "structuredContent":{"verified":true},
                    "isError":false
                })
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

struct ModernMcpFixture {
    peer: std::sync::Mutex<Option<InProcessMcpPeer>>,
    task_status: std::sync::atomic::AtomicU8,
    subscription_cancelled: tokio::sync::Notify,
    subscription_cancellations: std::sync::atomic::AtomicU8,
    subscription_auto_complete: AtomicBool,
}

impl ModernMcpFixture {
    fn task(&self) -> serde_json::Value {
        match self.task_status.load(Ordering::Acquire) {
            0 => serde_json::json!({
                "resultType": "complete",
                "taskId": "fixture-task",
                "status": "input_required",
                "statusMessage": "needs roots",
                "createdAt": "2026-08-09T00:00:00Z",
                "lastUpdatedAt": "2026-08-09T00:00:01Z",
                "ttlMs": 60000,
                "pollIntervalMs": 10,
                "inputRequests": {
                    "roots": {"method": "roots/list"}
                }
            }),
            1 => serde_json::json!({
                "resultType": "complete",
                "taskId": "fixture-task",
                "status": "completed",
                "statusMessage": "done",
                "createdAt": "2026-08-09T00:00:00Z",
                "lastUpdatedAt": "2026-08-09T00:00:02Z",
                "ttlMs": 60000,
                "result": {
                    "resultType": "complete",
                    "content": [{"type":"text","text":"task complete"}],
                    "isError": false
                }
            }),
            _ => serde_json::json!({
                "resultType": "complete",
                "taskId": "fixture-task",
                "status": "cancelled",
                "createdAt": "2026-08-09T00:00:00Z",
                "lastUpdatedAt": "2026-08-09T00:00:03Z",
                "ttlMs": 60000
            }),
        }
    }

    async fn notify_task(&self) {
        let peer = self.peer.lock().unwrap().clone();
        if let Some(peer) = peer {
            let _ = peer.notify("notifications/tasks", self.task()).await;
        }
    }
}

#[async_trait::async_trait]
impl InProcessMcpHandler for ModernMcpFixture {
    async fn handle(&self, message: serde_json::Value) -> Result<serde_json::Value, HostError> {
        let id = message.get("id").cloned();
        if id.is_none() {
            if message["method"] == "notifications/cancelled" {
                self.subscription_cancellations
                    .fetch_add(1, Ordering::AcqRel);
                self.subscription_cancelled.notify_one();
            }
            return Ok(serde_json::Value::Null);
        }
        let result = match message["method"].as_str() {
            Some("server/discover") => serde_json::json!({
                "resultType":"complete",
                "supportedVersions":["2026-07-28"],
                "capabilities":{
                    "tools":{"listChanged":true},
                    "resources":{"listChanged":true,"subscribe":true},
                    "prompts":{"listChanged":true},
                    "extensions":{"io.modelcontextprotocol/tasks":{}}
                },
                "ttlMs":0,
                "cacheScope":"private",
                "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"modern-sdk-fixture","version":"1"}}
            }),
            Some("ping") => serde_json::json!({}),
            Some("tools/list") => serde_json::json!({
                "tools":[
                    {"name":"mrtr","inputSchema":{"type":"object"}},
                    {"name":"task","inputSchema":{"type":"object"}}
                ]
            }),
            Some("tools/call") if message["params"]["name"] == "mrtr" => {
                if message["params"].get("inputResponses").is_none() {
                    serde_json::json!({
                        "resultType":"input_required",
                        "inputRequests":{"roots":{"method":"roots/list"}},
                        "requestState":"opaque-fixture-state"
                    })
                } else if message["params"]["requestState"] == "opaque-fixture-state" {
                    serde_json::json!({
                        "resultType":"complete",
                        "content":[{"type":"text","text":"mrtr complete"}],
                        "isError":false
                    })
                } else {
                    return Ok(serde_json::json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "error":{"code":-32602,"message":"request state mismatch"}
                    }));
                }
            }
            Some("tools/call") if message["params"]["name"] == "task" => {
                self.task_status.store(0, Ordering::Release);
                serde_json::json!({
                    "resultType":"task",
                    "taskId":"fixture-task",
                    "status":"working",
                    "createdAt":"2026-08-09T00:00:00Z",
                    "lastUpdatedAt":"2026-08-09T00:00:00Z",
                    "ttlMs":60000,
                    "pollIntervalMs":10
                })
            }
            Some("tasks/get") => self.task(),
            Some("tasks/update") => {
                self.task_status.store(1, Ordering::Release);
                self.notify_task().await;
                serde_json::json!({"resultType":"complete"})
            }
            Some("tasks/cancel") => {
                self.task_status.store(2, Ordering::Release);
                self.notify_task().await;
                serde_json::json!({"resultType":"complete"})
            }
            Some("subscriptions/listen") => {
                let peer = self.peer.lock().unwrap().clone().ok_or_else(|| HostError {
                    code: -32603,
                    message: "subscription peer unavailable".into(),
                    data: serde_json::Value::Null,
                })?;
                let subscription_id = id.clone().unwrap_or_default();
                peer.notify(
                    "notifications/subscriptions/acknowledged",
                    serde_json::json!({
                        "_meta":{"io.modelcontextprotocol/subscriptionId":subscription_id},
                        "notifications":{"toolsListChanged":true}
                    }),
                )
                .await?;
                peer.notify(
                    "notifications/tools/list_changed",
                    serde_json::json!({
                        "_meta":{"io.modelcontextprotocol/subscriptionId":subscription_id}
                    }),
                )
                .await?;
                if !self
                    .subscription_auto_complete
                    .swap(false, Ordering::AcqRel)
                {
                    self.subscription_cancelled.notified().await;
                }
                serde_json::json!({
                    "resultType":"complete",
                    "_meta":{"io.modelcontextprotocol/subscriptionId":subscription_id}
                })
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

    async fn connected(
        &self,
        _context: &InProcessMcpContext,
        peer: InProcessMcpPeer,
    ) -> Result<(), HostError> {
        *self.peer.lock().unwrap() = Some(peer);
        Ok(())
    }
}

struct EmptyRootsService;

#[allow(deprecated)]
#[async_trait::async_trait]
impl McpRootsService for EmptyRootsService {
    async fn list_roots(
        &self,
        _context: McpHostContext,
    ) -> Result<mcp_model::ListRootsResult, McpHostServiceError> {
        Ok(mcp_model::ListRootsResult::new(Vec::new()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_routes_sdk_owned_in_process_mcp() {
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let contexts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .in_process_mcp_servers([
            InProcessMcpServer::new(
                "sdk-fixture",
                "fixture-id",
                Arc::new(InProcessFixture {
                    contexts: contexts.clone(),
                }),
            ),
            InProcessMcpServer::new(
                "sdk-fixture-two",
                "fixture-id-two",
                Arc::new(InProcessFixture {
                    contexts: contexts.clone(),
                }),
            ),
        ])
        .start()
        .await
        .expect("desktop runtime");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session");
    let tools = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(tools) = runtime.list_mcp_tools(&session, Some("sdk-fixture")).await
                && !tools.is_empty()
            {
                break tools;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("MCP initialization");
    assert_eq!(tools[0].name, "echo");
    let second_tools = runtime
        .list_mcp_tools(&session, Some("sdk-fixture-two"))
        .await
        .expect("second MCP server shares the actor binding");
    assert_eq!(second_tools[0].name, "echo");
    let servers = runtime
        .list_mcp_servers(&session, false)
        .await
        .expect("server capabilities");
    let negotiated = servers[0]
        .negotiated
        .as_ref()
        .expect("modern discovery metadata");
    assert_eq!(negotiated.protocol_version, "2026-07-28");
    assert!(negotiated.tools);
    assert!(negotiated.resources);
    assert!(negotiated.prompts);
    assert!(negotiated.completions);
    assert!(!negotiated.tasks);
    let result = runtime
        .call_mcp_tool(&session, "sdk-fixture", "echo", serde_json::json!({}))
        .await
        .expect("tool call");
    assert!(matches!(&result.content[0], McpContent::Text { text, .. } if text == "in-process ok"));
    runtime
        .call_mcp_tool(&session, "sdk-fixture-two", "echo", serde_json::json!({}))
        .await
        .expect("second MCP tool call");
    let observed = contexts.lock().unwrap().clone();
    assert!(!observed.is_empty());
    assert!(observed.iter().all(|context| {
        context.runtime_instance_id > 0
            && context.session_id == session
            && context.session_instance_id == 1
            && matches!(
                (
                    context.server_name.as_str(),
                    context.registration_id.as_str()
                ),
                ("sdk-fixture", "fixture-id") | ("sdk-fixture-two", "fixture-id-two")
            )
    }));
    assert!(
        observed
            .iter()
            .any(|context| context.server_name == "sdk-fixture-two")
    );
    let resources = runtime
        .list_mcp_resources(&session, "sdk-fixture")
        .await
        .expect("resources and templates");
    assert_eq!(resources.resources[0].uri.as_deref(), Some("fixture://one"));
    assert_eq!(
        resources.templates[0].uri_template.as_deref(),
        Some("fixture://{id}")
    );
    let prompts = runtime
        .list_mcp_prompts(&session, "sdk-fixture")
        .await
        .expect("prompts");
    assert_eq!(prompts[0].name, "welcome");
    let prompt = runtime
        .get_mcp_prompt(
            &session,
            "sdk-fixture",
            "welcome",
            Some(serde_json::Map::from_iter([(
                "who".into(),
                serde_json::json!("sdk"),
            )])),
        )
        .await
        .expect("prompt get");
    assert_eq!(prompt.raw["messages"][0]["content"]["text"], "hello sdk");
    let completion = runtime
        .complete_mcp_argument(
            &session,
            "sdk-fixture",
            "prompt",
            "welcome",
            "who",
            "al",
            None,
        )
        .await
        .expect("completion");
    assert_eq!(completion.values, ["alice", "alex"]);
    assert!(matches!(
        runtime
            .replace_mcp_servers(
                &session,
                vec![McpServerConfig::Stdio {
                    name: "sdk-fixture".into(),
                    command: "/bin/false".into(),
                    args: Vec::new(),
                    env: BTreeMap::new(),
                }],
            )
            .await,
        Err(Error::InvalidConfig(_))
    ));
    runtime
        .unload_session(session.clone())
        .await
        .expect("unload first session incarnation");
    contexts.lock().unwrap().clear();
    runtime
        .load_session(session.clone(), session_config(workspace))
        .await
        .expect("load second session incarnation");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if runtime
                .list_mcp_tools(&session, Some("sdk-fixture"))
                .await
                .is_ok_and(|tools| !tools.is_empty())
                && runtime
                    .list_mcp_tools(&session, Some("sdk-fixture-two"))
                    .await
                    .is_ok_and(|tools| !tools.is_empty())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("second MCP initialization");
    assert!(
        contexts
            .lock()
            .unwrap()
            .iter()
            .all(|context| context.session_instance_id == 2)
    );
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modern_mcp_mrtr_tasks_subscriptions_and_generation_safety() {
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let fixture = Arc::new(ModernMcpFixture {
        peer: std::sync::Mutex::new(None),
        task_status: std::sync::atomic::AtomicU8::new(0),
        subscription_cancelled: tokio::sync::Notify::new(),
        subscription_cancellations: std::sync::atomic::AtomicU8::new(0),
        subscription_auto_complete: AtomicBool::new(false),
    });
    let (runtime, _) = Runtime::builder(runtime_config(&root, "http://127.0.0.1:1".to_owned()))
        .profile(RuntimeProfile::Desktop)
        .in_process_mcp_servers([InProcessMcpServer::new(
            "modern-fixture",
            "modern-fixture-id",
            fixture.clone(),
        )])
        .mcp_host_services(McpHostServices::default().with_roots(Arc::new(EmptyRootsService), true))
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if runtime
                .list_mcp_tools(&session, Some("modern-fixture"))
                .await
                .is_ok_and(|tools| tools.len() == 2)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("modern fixture initializes");

    runtime
        .ping_mcp(&session, "modern-fixture")
        .await
        .expect("ping");
    runtime
        .notify_mcp_roots_list_changed(&session, "modern-fixture")
        .await
        .expect("authorized roots notification");

    let input = runtime
        .call_mcp_tool_once(
            &session,
            "modern-fixture",
            "mrtr",
            serde_json::json!({}),
            None,
        )
        .await
        .expect("first MRTR round");
    let input = match input {
        McpOperationOutcome::InputRequired { input, .. } => input,
        other => panic!("expected input_required, got {other:?}"),
    };
    assert_eq!(input.request_state.as_deref(), Some("opaque-fixture-state"));
    assert_eq!(input.requests[0].kind, McpInputRequestKind::Roots);
    let continuation = input
        .respond(BTreeMap::from([(
            input.requests[0].id.clone(),
            serde_json::json!({"roots": []}),
        )]))
        .expect("bound continuation");
    let stale_continuation = continuation.clone();
    let completed = runtime
        .call_mcp_tool_once(
            &session,
            "modern-fixture",
            "mrtr",
            serde_json::json!({}),
            Some(continuation),
        )
        .await
        .expect("second MRTR round");
    assert!(matches!(
        completed,
        McpOperationOutcome::Complete { result, .. }
            if matches!(&result.content[0], McpContent::Text { text, .. } if text == "mrtr complete")
    ));

    let task = runtime
        .call_mcp_tool_once(
            &session,
            "modern-fixture",
            "task",
            serde_json::json!({}),
            None,
        )
        .await
        .expect("Task creation");
    let handle = match task {
        McpOperationOutcome::Task { handle, .. } => handle,
        other => panic!("expected Task, got {other:?}"),
    };
    let pending = runtime.get_mcp_task(&handle).await.expect("Task status");
    assert_eq!(pending.status, McpTaskStatus::InputRequired);
    runtime
        .update_mcp_task(
            &handle,
            BTreeMap::from([("roots".into(), serde_json::json!({"roots": []}))]),
        )
        .await
        .expect("Task update");
    let completed = runtime.get_mcp_task(&handle).await.expect("completed Task");
    assert_eq!(completed.status, McpTaskStatus::Completed);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if runtime
                .events_after(&session, 0)
                .await
                .expect("events")
                .iter()
                .any(|event| {
                    matches!(
                        &event.update,
                        EventUpdate::McpTaskStatus(event)
                            if event.status == McpTaskStatus::Completed
                                && event.handle == handle
                    )
                })
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Task push event");

    let mut subscription = runtime
        .listen_mcp(
            &session,
            "modern-fixture",
            McpSubscriptionFilter {
                tools_list_changed: true,
                ..Default::default()
            },
            4,
        )
        .await
        .expect("subscription acknowledged");
    let stale_subscription_generation = subscription.client_id;
    assert!(subscription.acknowledged.tools_list_changed);
    assert!(matches!(
        subscription.next().await.expect("subscription event"),
        Some(McpSubscriptionEvent::ToolsListChanged)
    ));
    subscription.cancel();
    assert!(matches!(
        subscription.next().await.expect("subscription end"),
        Some(McpSubscriptionEvent::Ended(McpSubscriptionEnd::Cancelled))
    ));

    let mut full_subscription = runtime
        .listen_mcp(
            &session,
            "modern-fixture",
            McpSubscriptionFilter {
                tools_list_changed: true,
                ..Default::default()
            },
            1,
        )
        .await
        .expect("capacity-one subscription acknowledged");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    full_subscription.cancel();
    assert!(matches!(
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            full_subscription.next(),
        )
        .await
        .expect("full queue must not delay cancellation")
        .expect("typed cancellation"),
        Some(McpSubscriptionEvent::Ended(McpSubscriptionEnd::Cancelled))
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while fixture.subscription_cancellations.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("transport cancellation must bypass the full SDK data queue");

    fixture
        .subscription_auto_complete
        .store(true, Ordering::Release);
    let mut server_completed_subscription = runtime
        .listen_mcp(
            &session,
            "modern-fixture",
            McpSubscriptionFilter {
                tools_list_changed: true,
                ..Default::default()
            },
            1,
        )
        .await
        .expect("server-completed subscription acknowledged");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(matches!(
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            server_completed_subscription.next(),
        )
        .await
        .expect("server terminal must bypass a full data queue")
        .expect("typed server terminal"),
        Some(McpSubscriptionEvent::Ended(McpSubscriptionEnd::Graceful))
    ));

    let stale = handle.clone();
    runtime
        .unload_session(session.clone())
        .await
        .expect("unload");
    runtime
        .load_session(session.clone(), session_config(workspace))
        .await
        .expect("reload");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if runtime
                .list_mcp_tools(&session, Some("modern-fixture"))
                .await
                .is_ok_and(|tools| tools.len() == 2)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replacement client initializes");
    assert!(
        runtime.get_mcp_task(&stale).await.is_err(),
        "Task handle from an old client generation must fail closed"
    );
    assert!(
        runtime
            .call_mcp_tool_once(
                &session,
                "modern-fixture",
                "mrtr",
                serde_json::json!({}),
                Some(stale_continuation),
            )
            .await
            .is_err(),
        "MRTR continuation from an old connection generation must fail closed"
    );
    assert_eq!(
        subscription.next().await.expect("ended subscription"),
        None,
        "an ended generation-bound subscription must not resume after reconnect"
    );
    let mut replacement_subscription = runtime
        .listen_mcp(
            &session,
            "modern-fixture",
            McpSubscriptionFilter {
                tools_list_changed: true,
                ..Default::default()
            },
            4,
        )
        .await
        .expect("replacement subscription");
    assert_ne!(
        replacement_subscription.client_id,
        stale_subscription_generation
    );
    replacement_subscription.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_external_and_in_process_mcp_name_collisions() {
    let root = TempDir::new().expect("root");
    let result = Runtime::builder(runtime_config(&root, "http://127.0.0.1:1".into()))
        .profile(RuntimeProfile::Desktop)
        .mcp_servers([McpServerConfig::Stdio {
            name: "same-name".into(),
            command: "/bin/false".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
        }])
        .in_process_mcp_servers([InProcessMcpServer::new(
            "same-name",
            "fixture-id",
            Arc::new(InProcessFixture {
                contexts: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
        )])
        .start()
        .await;
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_profile_never_registers_in_process_mcp() {
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let contexts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (runtime, _) = Runtime::builder(runtime_config(&root, "http://127.0.0.1:1".into()))
        .in_process_mcp_servers([InProcessMcpServer::new(
            "sdk-fixture",
            "fixture-id",
            Arc::new(InProcessFixture {
                contexts: contexts.clone(),
            }),
        )])
        .start()
        .await
        .expect("restricted runtime");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("restricted session");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(contexts.lock().unwrap().is_empty());
    assert!(matches!(
        runtime.list_mcp_servers(&session, false).await,
        Err(Error::Operation(_))
    ));
    assert!(runtime.capabilities().features.iter().any(|feature| {
        feature.namespace == "sdk:in-process-mcp"
            && !feature.enabled
            && feature.disabled_reason.as_deref() == Some("restricted profile")
    }));
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_discovers_and_executes_implement_skill_as_an_agent_command() {
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    let skills = root.path().join("skills");
    let implement = skills.join("implement");
    std::fs::create_dir_all(&workspace).expect("workspace");
    std::fs::create_dir_all(&implement).expect("implement skill directory");
    std::fs::write(
        implement.join("SKILL.md"),
        r#"---
name: implement
description: Implement a requested software change completely.
argument-hint: change request
---
IMPLEMENT_SKILL_BODY: implement $ARGUMENTS and verify it.
"#,
    )
    .expect("implement skill");

    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .skill_paths([skills])
        .start()
        .await
        .expect("desktop runtime");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session");
    let commands = runtime
        .list_agent_commands(&session)
        .await
        .expect("live command catalog");
    let implement = commands
        .iter()
        .find(|command| command.name == "implement")
        .expect("implement skill is advertised");
    assert_eq!(implement.input_hint.as_deref(), Some("change request"));
    assert!(commands.iter().any(|command| command.name == "loop"));
    assert!(
        runtime
            .execute_agent_command(&session, "unknown-turn", "not-a-command", None)
            .await
            .is_err(),
        "command execution must be allowlisted against the live catalog"
    );
    runtime
        .execute_agent_command(&session, "implement-turn", "implement", Some("feature-x"))
        .await
        .expect("implement command turn");
    assert!(sampling.requests().iter().any(|request| {
        request.body.as_ref().is_some_and(|body| {
            let body = body.to_string();
            body.contains("IMPLEMENT_SKILL_BODY") && body.contains("feature-x")
        })
    }));
    runtime.shutdown().await.expect("shutdown");

    let restricted_root = TempDir::new().expect("restricted root");
    let restricted_workspace = restricted_root.path().join("workspace");
    std::fs::create_dir(&restricted_workspace).expect("restricted workspace");
    let (restricted, _) = Runtime::builder(runtime_config(&restricted_root, sampling.url()))
        .skill_paths([root.path().join("skills")])
        .start()
        .await
        .expect("restricted runtime");
    let restricted_session = restricted
        .create_session(session_config(restricted_workspace))
        .await
        .expect("restricted session");
    assert!(
        restricted
            .list_agent_commands(&restricted_session)
            .await
            .is_err()
    );
    restricted.shutdown().await.expect("restricted shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_system_prompt_and_rules_reach_the_real_agent_prompt_builder() {
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, _) = Runtime::start(runtime_config(&root, sampling.url()))
        .await
        .expect("runtime");

    let mut override_config = session_config(workspace.clone());
    override_config.system_prompt = Some("SDK_SYSTEM_OVERRIDE".into());
    let override_session = runtime
        .create_session(override_config)
        .await
        .expect("override session");
    runtime
        .prompt(&override_session, "override-turn", "override-marker")
        .await
        .expect("override turn");
    let override_body = request_with_user_marker(&sampling, "override-marker");
    assert!(
        override_body["messages"][0]["content"]
            .as_str()
            .is_some_and(|prompt| prompt.starts_with("SDK_SYSTEM_OVERRIDE"))
    );

    let mut rules_config = session_config(workspace);
    rules_config.rules = Some("SDK_RULES_MARKER: never omit verification.".into());
    let rules_session = runtime
        .create_session(rules_config)
        .await
        .expect("rules session");
    runtime
        .prompt(&rules_session, "rules-turn", "rules-marker")
        .await
        .expect("rules turn");
    let rules_body = request_with_user_marker(&sampling, "rules-marker");
    let rules_prompt = rules_body["messages"][0]["content"]
        .as_str()
        .expect("rules system prompt");
    assert!(rules_prompt.contains("<human_rules>"));
    assert!(rules_prompt.contains("SDK_RULES_MARKER"));

    let mut blank = session_config(root.path().to_path_buf());
    blank.system_prompt = Some("  ".into());
    assert!(matches!(
        runtime.create_session(blank).await,
        Err(Error::InvalidConfig(_))
    ));
    runtime.shutdown().await.expect("shutdown");
}

/// Opt-in real gateway verification. No credential or response body is
/// logged; run explicitly with `OG_AI_GATEWAY` and `OG_API_KEY` set.
#[ignore = "requires the live OriginGame gateway and incurs a model request"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_gateway_model_calls_sdk_owned_mcp_and_journals_the_turn() {
    let endpoint = std::env::var("OG_AI_GATEWAY")
        .expect("OG_AI_GATEWAY")
        .trim_end_matches('/')
        .to_owned()
        + "/v1";
    let api_key = std::env::var("OG_API_KEY").expect("OG_API_KEY");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let called = Arc::new(AtomicBool::new(false));
    let config = RuntimeConfig {
        endpoint,
        api_key,
        grok_home: root.path().join("grok"),
        session_storage: root.path().join("sessions"),
        models: vec![ModelSpec {
            id: "grok-4.5".into(),
            model_family: None,
            context_window: 131_072,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        }],
    };
    let (runtime, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .yolo_mode(true)
        .in_process_mcp_servers([InProcessMcpServer::new(
            "live-sdk-fixture",
            "live-fixture-id",
            Arc::new(LiveGatewayMcpFixture {
                called: called.clone(),
            }),
        )])
        .start()
        .await
        .expect("live runtime");
    let session = runtime
        .create_session(SessionConfig {
            cwd: workspace,
            model: "grok-4.5".into(),
            reasoning: None,
            system_prompt: None,
            rules: Some(
                "When explicitly told to verify the gateway, call the supplied MCP tool before answering."
                    .into(),
            ),
        })
        .await
        .expect("live session");
    let receipt = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        runtime.prompt(
            &session,
            "live-gateway-turn",
            "Call the gateway_probe tool with code LIVE_GATEWAY_E2E. Only after the tool returns, answer LIVE_MCP_TOOL_OK.",
        ),
    )
    .await
    .expect("live gateway timeout")
    .expect("live gateway turn");
    assert_eq!(receipt.outcome, TurnOutcome::End);
    assert!(
        called.load(Ordering::Acquire),
        "the real model did not call MCP"
    );
    let journal = runtime
        .events_after(&session, 0)
        .await
        .expect("live journal");
    assert!(
        journal
            .iter()
            .any(|event| matches!(event.update, EventUpdate::ToolStart(_)))
    );
    assert_eq!(
        journal.last().map(|event| event.sequence),
        Some(receipt.final_sequence)
    );
    runtime.shutdown().await.expect("live runtime shutdown");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_starts_explicit_stdio_mcp_and_restricted_does_not() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let marker = root.path().join("mcp-tools-listed");
    let script = root.path().join("mock-mcp.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
*'"method":"server/discover"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{"tools":{},"resources":{},"prompts":{},"completions":{}},"ttlMs":0,"cacheScope":"private","_meta":{"io.modelcontextprotocol/serverInfo":{"name":"origin-runtime-test","version":"1"}}}}\n' "$id"
  ;;
*'"method":"tools/list"'*)
  : > "$MCP_MARKER"
  printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo","description":"echo","inputSchema":{"type":"object"}}]}}\n' "$id"
  ;;
*'"method":"tools/call"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"ok","annotations":{"audience":["user"]}}],"structuredContent":{"fixture":true},"isError":false,"_meta":{"trace":"fixture"}}}\n' "$id"
  ;;
*'"method":"resources/read"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"contents":[{"uri":"fixture://readme","mimeType":"text/plain","text":"fixture resource","_meta":{"revision":1}}]}}\n' "$id"
  ;;
*'"method":"resources/list"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"resources":[{"uri":"fixture://readme","name":"readme"}]}}\n' "$id"
  ;;
*'"method":"resources/templates/list"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"resourceTemplates":[{"uriTemplate":"fixture://{name}","name":"named"}]}}\n' "$id"
  ;;
*'"method":"prompts/list"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"prompts":[{"name":"welcome","description":"welcome"}]}}\n' "$id"
  ;;
*'"method":"prompts/get"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"messages":[{"role":"user","content":{"type":"text","text":"hello"}}]}}\n' "$id"
  ;;
*'"method":"completion/complete"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"completion":{"values":["alpha"],"total":1,"hasMore":false}}}\n' "$id"
  ;;
  esac
done
"#,
    )
    .expect("MCP script");
    let mcp = McpServerConfig::Stdio {
        name: "fixture".into(),
        command: "/bin/sh".into(),
        args: vec![script.to_string_lossy().into_owned()],
        env: BTreeMap::from([
            ("MCP_MARKER".into(), marker.to_string_lossy().into_owned()),
            ("MCP_SECRET".into(), "catalog-secret".into()),
        ]),
    };

    let (restricted, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .mcp_servers([mcp.clone()])
        .start()
        .await
        .expect("restricted runtime starts");
    let restricted_session = restricted
        .create_session(session_config(workspace.clone()))
        .await
        .expect("restricted session starts");
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert!(!marker.exists(), "restricted profile must not start MCP");
    assert!(
        restricted
            .list_mcp_servers(&restricted_session, false)
            .await
            .is_err(),
        "restricted profile must reject typed MCP operations"
    );
    restricted
        .close_session(restricted_session)
        .await
        .expect("restricted session closes");
    restricted.shutdown().await.expect("restricted shuts down");

    let desktop_root = TempDir::new().expect("desktop root");
    let (desktop, _) = Runtime::builder(runtime_config(&desktop_root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .mcp_servers([mcp.clone()])
        .start()
        .await
        .expect("desktop runtime starts");
    let session = desktop
        .create_session(session_config(workspace))
        .await
        .expect("desktop session starts");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("MCP initialize and tools/list complete");

    let catalog = desktop
        .list_mcp_servers(&session, false)
        .await
        .expect("typed MCP catalog");
    let fixture = catalog
        .iter()
        .find(|server| server.name == "fixture")
        .expect("fixture in catalog");
    assert_eq!(fixture.transport, McpTransportKind::Stdio);
    assert_eq!(fixture.status, Some(McpServerStatus::Ready));
    assert_eq!(fixture.tools.len(), 1);
    assert!(
        !serde_json::to_string(fixture)
            .expect("catalog serializes")
            .contains("catalog-secret"),
        "typed catalog must not expose stdio environment values"
    );

    let tool_result = desktop
        .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({"value": 1}))
        .await
        .expect("direct MCP call");
    assert!(matches!(
        &tool_result.content[0],
        McpContent::Text { text, raw }
            if text == "ok" && raw["annotations"]["audience"][0] == "user"
    ));
    assert_eq!(tool_result.structured_content.unwrap()["fixture"], true);
    assert_eq!(tool_result.meta.unwrap()["trace"], "fixture");

    let resource = desktop
        .read_mcp_resource(&session, "fixture", "fixture://readme")
        .await
        .expect("direct MCP resource read");
    assert_eq!(
        resource.contents[0].text.as_deref(),
        Some("fixture resource")
    );
    assert_eq!(resource.contents[0].raw["_meta"]["revision"], 1);

    desktop
        .set_mcp_tool_enabled(&session, "fixture", "echo", false)
        .await
        .expect("disable MCP tool in session");
    let tools = desktop
        .list_mcp_tools(&session, Some("fixture"))
        .await
        .expect("list disabled tool");
    assert_eq!(tools.len(), 1);
    assert!(!tools[0].enabled);
    desktop
        .set_mcp_tool_enabled(&session, "fixture", "echo", true)
        .await
        .expect("re-enable MCP tool in session");

    desktop
        .set_mcp_server_enabled(&session, "fixture", false)
        .await
        .expect("disable MCP server in session");
    assert!(
        desktop
            .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({}))
            .await
            .is_err(),
        "disabled MCP server must not be callable"
    );
    desktop
        .set_mcp_server_enabled(&session, "fixture", true)
        .await
        .expect("re-enable MCP server in session");
    desktop
        .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({}))
        .await
        .expect("re-enabled MCP server is callable");

    let removed = desktop
        .replace_mcp_servers(&session, Vec::new())
        .await
        .expect("remove session MCP servers atomically");
    assert_eq!(removed.count, 0);
    assert!(
        desktop
            .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({}))
            .await
            .is_err(),
        "removed MCP server must not be callable"
    );
    let replaced = desktop
        .replace_mcp_servers(&session, vec![mcp])
        .await
        .expect("restore session MCP servers atomically");
    assert_eq!(replaced.names, vec!["fixture"]);
    desktop
        .call_mcp_tool(&session, "fixture", "echo", serde_json::json!({}))
        .await
        .expect("replacement MCP server is callable");

    let scheduled = desktop
        .upsert_scheduled_task(
            &session,
            &ScheduledTaskRequest {
                task_id: None,
                interval: Some("5m".into()),
                prompt: Some("inspect the fixture".into()),
                recurring: true,
                durable: Some(false),
                foreground: Some(false),
                fire_immediately: false,
            },
        )
        .await
        .expect("create scheduled loop without a model turn");
    assert!(!scheduled.updated);
    assert_eq!(scheduled.task.interval_seconds, 300);
    let tasks = desktop
        .list_scheduled_tasks(&session)
        .await
        .expect("list scheduled loops");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id, scheduled.task.id);
    let updated = desktop
        .upsert_scheduled_task(
            &session,
            &ScheduledTaskRequest {
                task_id: Some(scheduled.task.id.clone()),
                interval: Some("10m".into()),
                prompt: Some("inspect the updated fixture".into()),
                recurring: true,
                durable: None,
                foreground: None,
                fire_immediately: false,
            },
        )
        .await
        .expect("update scheduled loop in place");
    assert!(updated.updated);
    assert_eq!(updated.task.interval_seconds, 600);
    assert_eq!(updated.task.id, scheduled.task.id);
    let deleted = desktop
        .delete_scheduled_task(&session, &scheduled.task.id)
        .await
        .expect("delete scheduled loop");
    assert!(deleted.deleted);
    assert!(
        desktop
            .list_scheduled_tasks(&session)
            .await
            .expect("list after delete")
            .is_empty()
    );
    desktop
        .close_session(session)
        .await
        .expect("desktop session closes");
    desktop.shutdown().await.expect("desktop shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_starts_explicit_http_and_sse_mcp_with_host_headers() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let http = McpHttpMock::start().await;
    let sse = McpHttpMock::start().await;
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .mcp_servers([
            McpServerConfig::Http {
                name: "http-fixture".into(),
                url: http.url.clone(),
                headers: BTreeMap::from([
                    ("authorization".into(), "Bearer http-secret".into()),
                    ("x-origin-mcp".into(), "http".into()),
                ]),
            },
            McpServerConfig::Sse {
                name: "sse-fixture".into(),
                url: sse.url.clone(),
                headers: BTreeMap::from([
                    ("authorization".into(), "Bearer sse-secret".into()),
                    ("x-origin-mcp".into(), "sse".into()),
                ]),
            },
        ])
        .start()
        .await
        .expect("desktop runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("desktop session starts");
    tokio::join!(http.wait_for_tools(), sse.wait_for_tools());
    assert_eq!(
        http.headers(),
        vec![("Bearer http-secret".into(), "http".into())]
    );
    assert_eq!(
        sse.headers(),
        vec![("Bearer sse-secret".into(), "sse".into())]
    );
    runtime
        .close_session(session)
        .await
        .expect("desktop session closes");
    runtime.shutdown().await.expect("desktop shuts down");
}
