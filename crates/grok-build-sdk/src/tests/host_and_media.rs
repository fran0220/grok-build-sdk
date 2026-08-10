use super::mcp_runtime::InProcessFixture;
use super::*;

fn media_provider(base_url: String, api_key: &str, header_value: &str) -> MediaProviderConfig {
    MediaProviderConfig {
        base_url,
        api_key: api_key.into(),
        headers: BTreeMap::from([("x-origin-provider".into(), header_value.into())]),
        query_params: BTreeMap::from([("tenant".into(), "media".into())]),
    }
}

#[derive(Clone, Debug)]
struct MediaRequest {
    path: String,
    authorization: Option<String>,
    provider_header: Option<String>,
    body: serde_json::Value,
}

struct MediaMock {
    addr: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<MediaRequest>>>,
    task: tokio::task::JoinHandle<()>,
}

impl MediaMock {
    async fn start() -> Self {
        use axum::{
            Json, Router,
            extract::OriginalUri,
            http::HeaderMap,
            routing::{get, post},
        };

        let requests = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind media mock");
        let addr = listener.local_addr().expect("media mock address");
        let image_requests = requests.clone();
        let edit_requests = requests.clone();
        let video_requests = requests.clone();
        let video_poll_requests = requests.clone();
        let video_url = format!("http://{addr}/v1/video.mp4");
        let router = Router::new()
            .route(
                "/v1/images/generations",
                post(
                    move |OriginalUri(uri): OriginalUri,
                          headers: HeaderMap,
                          Json(body): Json<serde_json::Value>| {
                        let requests = image_requests.clone();
                        async move {
                            record_media_request(
                                &requests,
                                uri.path_and_query().expect("image request path").as_str(),
                                &headers,
                                body,
                            );
                            Json(serde_json::json!({"data":[{"b64_json":"aGVsbG8="}]}))
                        }
                    },
                ),
            )
            .route(
                "/v1/images/edits",
                post(
                    move |OriginalUri(uri): OriginalUri,
                          headers: HeaderMap,
                          Json(body): Json<serde_json::Value>| {
                        let requests = edit_requests.clone();
                        async move {
                            record_media_request(
                                &requests,
                                uri.path_and_query()
                                    .expect("image edit request path")
                                    .as_str(),
                                &headers,
                                body,
                            );
                            Json(serde_json::json!({"data":[{"b64_json":"aGVsbG8="}]}))
                        }
                    },
                ),
            )
            .route(
                "/v1/videos/generations",
                post(
                    move |OriginalUri(uri): OriginalUri,
                          headers: HeaderMap,
                          Json(body): Json<serde_json::Value>| {
                        let requests = video_requests.clone();
                        async move {
                            record_media_request(
                                &requests,
                                uri.path_and_query().expect("video request path").as_str(),
                                &headers,
                                body,
                            );
                            Json(serde_json::json!({"request_id":"video-1"}))
                        }
                    },
                ),
            )
            .route(
                "/v1/videos/{id}",
                get(move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                    let video_url = video_url.clone();
                    let requests = video_poll_requests.clone();
                    async move {
                        record_media_request(
                            &requests,
                            uri.path_and_query()
                                .expect("video poll request path")
                                .as_str(),
                            &headers,
                            serde_json::Value::Null,
                        );
                        Json(serde_json::json!({
                            "status":"done",
                            "video":{"url":video_url}
                        }))
                    }
                }),
            )
            .route(
                "/v1/video.mp4",
                get(|| async { ([("content-type", "video/mp4")], "mock-video") }),
            );
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("media mock serves");
        });
        Self {
            addr,
            requests,
            task,
        }
    }

    fn url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    fn requests(&self) -> Vec<MediaRequest> {
        self.requests.lock().expect("media requests").clone()
    }
}

fn record_media_request(
    requests: &Mutex<Vec<MediaRequest>>,
    path: &str,
    headers: &axum::http::HeaderMap,
    body: serde_json::Value,
) {
    requests.lock().expect("media requests").push(MediaRequest {
        path: path.into(),
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        provider_header: headers
            .get("x-origin-provider")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body,
    });
}

impl Drop for MediaMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Default)]
struct RecordingHost {
    allow: AtomicBool,
    slow_terminal_wait: AtomicBool,
    requests: Mutex<Vec<HostRequest>>,
    notifications: Mutex<Vec<HostNotification>>,
}

impl RecordingHost {
    fn approving() -> Self {
        Self {
            allow: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn request_methods(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("requests lock")
            .iter()
            .map(|request| request.method.clone())
            .collect()
    }

    fn notifications(&self) -> Vec<HostNotification> {
        self.notifications
            .lock()
            .expect("notifications lock")
            .clone()
    }
}

#[async_trait::async_trait]
impl HostDelegate for RecordingHost {
    async fn request(&self, request: HostRequest) -> Result<serde_json::Value, HostError> {
        self.requests
            .lock()
            .expect("requests lock")
            .push(request.clone());
        match request.method.as_str() {
            "session/request_permission" => {
                let wanted = if self.allow.load(Ordering::Acquire) {
                    "allow_once"
                } else {
                    "reject_once"
                };
                let option_id = request.params["options"]
                    .as_array()
                    .and_then(|options| options.iter().find(|option| option["kind"] == wanted))
                    .and_then(|option| option["optionId"].as_str())
                    .ok_or_else(|| HostError {
                        code: -32602,
                        message: format!("permission request omitted {wanted}"),
                        data: request.params.clone(),
                    })?;
                Ok(serde_json::json!({
                    "outcome": {"outcome":"selected", "optionId":option_id}
                }))
            }
            "fs/read_text_file" => {
                let path = request.params["path"].as_str().ok_or_else(|| HostError {
                    code: -32602,
                    message: "missing path".into(),
                    data: request.params.clone(),
                })?;
                let content = std::fs::read_to_string(path).map_err(|error| HostError {
                    code: -32000,
                    message: error.to_string(),
                    data: serde_json::json!({"path":path}),
                })?;
                Ok(serde_json::json!({"content":content}))
            }
            "fs/write_text_file" => {
                let path = request.params["path"].as_str().ok_or_else(|| HostError {
                    code: -32602,
                    message: "missing path".into(),
                    data: request.params.clone(),
                })?;
                let content = request.params["content"]
                    .as_str()
                    .ok_or_else(|| HostError {
                        code: -32602,
                        message: "missing content".into(),
                        data: request.params.clone(),
                    })?;
                std::fs::write(path, content).map_err(|error| HostError {
                    code: -32000,
                    message: error.to_string(),
                    data: serde_json::json!({"path":path}),
                })?;
                Ok(serde_json::json!({}))
            }
            "terminal/create" => Ok(serde_json::json!({"terminalId":"host-terminal-1"})),
            "terminal/wait_for_exit" => {
                if self.slow_terminal_wait.load(Ordering::Acquire) {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Ok(serde_json::json!({"exitCode":0}))
            }
            "terminal/output" => Ok(serde_json::json!({
                "output":"terminal-from-host\n",
                "truncated":false,
                "exitStatus":{"exitCode":0}
            })),
            "terminal/kill" | "terminal/release" => Ok(serde_json::json!({})),
            "x.ai/folder_trust/request" => Ok(serde_json::json!({"outcome":"reject"})),
            method => Err(HostError {
                code: -32601,
                message: format!("unsupported host method: {method}"),
                data: request.params,
            }),
        }
    }

    async fn notification(&self, notification: HostNotification) -> Result<(), HostError> {
        self.notifications
            .lock()
            .expect("notifications lock")
            .push(notification);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_media_service_routes_image_generation_and_restricted_stays_closed() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let media = MediaMock::start().await;
    let restricted_call = sampling.expect_response(
        "restricted image tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "restricted-generate-image",
            "image_gen",
            r#"{"prompt":"must not run","aspect_ratio":"1:1"}"#,
        ),
    );
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let media_service = MediaServiceConfig {
        provider: media_provider(media.url(), "media-secret", "media"),
        image_generation: true,
        image_edit: false,
        video_generation: false,
        image_generation_model: Some("custom-image-model".into()),
        image_edit_model: None,
        image_to_video_model: None,
        reference_to_video_model: None,
    };
    let (restricted, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .media_service(media_service.clone())
        .start()
        .await
        .expect("restricted runtime starts");
    let restricted_image = restricted
        .capabilities()
        .features
        .into_iter()
        .find(|capability| capability.namespace == "feature:image_generation")
        .expect("image capability");
    assert!(!restricted_image.enabled);
    let restricted_session = restricted
        .create_session(session_config(workspace.clone()))
        .await
        .expect("restricted session starts");
    restricted
        .prompt(
            &restricted_session,
            "restricted-image-turn",
            "try to generate an image",
        )
        .await
        .expect("unknown restricted tool remains a normal turn outcome");
    restricted_call.assert_satisfied();
    assert!(
        media.requests().is_empty(),
        "Restricted must not send any media request"
    );
    restricted.shutdown().await.expect("restricted shuts down");

    let image_call = sampling.expect_response(
        "generate image tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "generate-image",
            "image_gen",
            r#"{"prompt":"a blue square","aspect_ratio":"1:1"}"#,
        ),
    );
    let desktop_root = TempDir::new().expect("desktop root");
    let (runtime, _) = Runtime::builder(runtime_config(&desktop_root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .media_service(media_service)
        .start()
        .await
        .expect("desktop runtime starts");
    let image_capability = runtime
        .capabilities()
        .features
        .into_iter()
        .find(|capability| capability.namespace == "feature:image_generation")
        .expect("image capability");
    assert!(image_capability.enabled);
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt(&session, "image-turn", "generate an image")
        .await
        .expect("image turn succeeds");
    image_call.assert_satisfied();

    let media_request = media
        .requests()
        .into_iter()
        .find(|request| request.path == "/v1/images/generations?tenant=media")
        .expect("media API request");
    assert_eq!(
        media_request.authorization.as_deref(),
        Some("Bearer media-secret")
    );
    assert_eq!(media_request.provider_header.as_deref(), Some("media"));
    assert_eq!(media_request.body["model"], "custom-image-model");
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_media_service_wires_edit_and_both_video_models() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let media = MediaMock::start().await;
    let image = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAQCAIAAAD4YuoOAAAAHUlEQVR42mPQqDhBU8QwasGoBaMWjFowasFQsAAAxdvQH+YmXBQAAAAASUVORK5CYII=";
    let edit_args = serde_json::json!({
        "prompt":"make it green",
        "image":[image],
        "aspect_ratio":"auto"
    })
    .to_string();
    let image_video_args = serde_json::json!({
        "prompt":"animate",
        "image":image,
        "duration":6,
        "resolution_name":"480p"
    })
    .to_string();
    let reference_video_args = serde_json::json!({
        "prompt":"combine",
        "images":[image, image],
        "aspect_ratio":"16:9",
        "duration":6,
        "resolution_name":"480p"
    })
    .to_string();
    let edit_call = sampling.expect_response(
        "edit image tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call("edit-image", "image_edit", &edit_args),
    );
    let image_video_call = sampling.expect_response(
        "image to video tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call("image-video", "image_to_video", &image_video_args),
    );
    let reference_video_call = sampling.expect_response(
        "reference to video tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "reference-video",
            "reference_to_video",
            &reference_video_args,
        ),
    );
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .media_service(MediaServiceConfig {
            provider: media_provider(media.url(), "media-secret", "media"),
            image_generation: false,
            image_edit: true,
            video_generation: true,
            image_generation_model: None,
            image_edit_model: Some("custom-edit-model".into()),
            image_to_video_model: Some("custom-image-video-model".into()),
            reference_to_video_model: Some("custom-reference-video-model".into()),
        })
        .start()
        .await
        .expect("desktop runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt(&session, "media-turn", "edit and animate images")
        .await
        .expect("media turn succeeds");
    edit_call.assert_satisfied();
    image_video_call.assert_satisfied();
    reference_video_call.assert_satisfied();

    let requests = media.requests();
    let edit = requests
        .iter()
        .find(|request| request.path == "/v1/images/edits?tenant=media")
        .expect("image edit request");
    assert_eq!(edit.body["model"], "custom-edit-model");
    let videos = requests
        .iter()
        .filter(|request| request.path == "/v1/videos/generations?tenant=media")
        .collect::<Vec<_>>();
    assert_eq!(videos.len(), 2);
    assert_eq!(videos[0].body["model"], "custom-image-video-model");
    assert_eq!(videos[1].body["model"], "custom-reference-video-model");
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path == "/v1/videos/video-1?tenant=media")
            .count(),
        2,
        "video polling must preserve provider query parameters"
    );
    for request in std::iter::once(edit).chain(videos) {
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer media-secret")
        );
        assert_eq!(request.provider_header.as_deref(), Some("media"));
    }
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_profile_denies_local_filesystem_without_host_callbacks() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let write_call = server.expect_response(
        "restricted filesystem tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "write-1",
            "search_replace",
            r#"{"file_path":"note.txt","old_string":"before","new_string":"after"}"#,
        ),
    );
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    std::fs::write(workspace.join("note.txt"), "before").expect("fixture");

    // RuntimeConfig intentionally has no host callbacks. Restricted mode
    // must not fall back to LocalFs or TerminalRunner.
    let (runtime, _) = Runtime::start(runtime_config(&root, server.url()))
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");
    let receipt = runtime
        .prompt(&session, "turn-write", "replace before with after")
        .await
        .expect("denied tool remains a normal model turn outcome");

    write_call.assert_satisfied();
    assert_eq!(receipt.outcome, TurnOutcome::End);
    assert_eq!(
        std::fs::read_to_string(workspace.join("note.txt")).expect("edited file"),
        "before"
    );
    let extension_error = runtime
        .extension_request(ExtensionRequest {
            method: "x.ai/fs/write_file".into(),
            params: serde_json::json!({
                "sessionId": session.as_str(),
                "path": "extension.txt",
                "content": "must not be written",
                "createDirs": false
            }),
        })
        .await
        .expect_err("Restricted generic extension transport must fail closed");
    assert!(matches!(extension_error, Error::Operation(_)));
    let scheduler_error = runtime
        .list_scheduled_tasks(&session)
        .await
        .expect_err("Restricted typed scheduler transport must fail closed");
    assert!(matches!(scheduler_error, Error::Operation(_)));
    assert!(!workspace.join("extension.txt").exists());
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_host_controls_permission_and_serves_filesystem_callbacks() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    std::fs::write(workspace.join("note.txt"), "before").expect("fixture");
    let host = Arc::new(RecordingHost::default());
    let capabilities = HostCapabilities {
        fs_read: true,
        fs_write: true,
        ..HostCapabilities::default()
    };
    let (runtime, _) = Runtime::builder(runtime_config(&root, server.url()))
        .profile(RuntimeProfile::Desktop)
        .host_capabilities(capabilities)
        .host_delegate(host.clone())
        .start()
        .await
        .expect("desktop runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");

    let denied_call = server.expect_response(
        "denied filesystem tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "write-denied",
            "search_replace",
            r#"{"file_path":"note.txt","old_string":"before","new_string":"denied"}"#,
        ),
    );
    runtime
        .prompt(&session, "turn-denied", "attempt a denied edit")
        .await
        .expect("a denied tool does not fail the turn transport");
    denied_call.assert_satisfied();
    assert_eq!(
        std::fs::read_to_string(workspace.join("note.txt")).unwrap(),
        "before"
    );
    let denied_methods = host.request_methods();
    assert!(denied_methods.contains(&"session/request_permission".into()));
    assert!(!denied_methods.contains(&"fs/write_text_file".into()));

    host.allow.store(true, Ordering::Release);
    let approved_call = server.expect_response(
        "approved filesystem tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "write-approved",
            "search_replace",
            r#"{"file_path":"note.txt","old_string":"before","new_string":"approved"}"#,
        ),
    );
    runtime
        .prompt(&session, "turn-approved", "perform the approved edit")
        .await
        .expect("approved tool turn succeeds");
    approved_call.assert_satisfied();
    assert_eq!(
        std::fs::read_to_string(workspace.join("note.txt")).unwrap(),
        "approved"
    );
    let approved_methods = host.request_methods();
    assert!(approved_methods.contains(&"fs/read_text_file".into()));
    assert!(approved_methods.contains(&"fs/write_text_file".into()));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while host.notifications().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("agent extension notifications are forwarded to the host");
    assert!(
        host.notifications()
            .iter()
            .all(|notification| !notification.method.is_empty())
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

/// The embedding host owns the runtime's configuration. Ambient MCP
/// catalogs and auto-approving permission modes belong to whoever installed
/// the Grok CLI or Claude Code on the machine and must never reach a session
/// the host declared. The workspace below plants both kinds of ambient
/// configuration so the assertion holds on a machine that has neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_runtime_ignores_ambient_mcp_and_permission_configuration() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    std::fs::write(workspace.join("note.txt"), "before").expect("fixture");
    std::fs::write(
        workspace.join(".mcp.json"),
        r#"{"mcpServers":{"ambient-mcp-json-server":{"command":"/bin/false","args":[]}}}"#,
    )
    .expect("ambient .mcp.json");
    std::fs::create_dir(workspace.join(".grok")).expect("ambient grok dir");
    std::fs::write(
        workspace.join(".grok").join("config.toml"),
        "[mcp_servers.ambient_toml_server]\ncommand = \"/bin/false\"\nargs = []\n",
    )
    .expect("ambient config.toml");
    std::fs::create_dir(workspace.join(".claude")).expect("ambient claude dir");
    std::fs::write(
        workspace.join(".claude").join("settings.json"),
        r#"{"permissions":{"defaultMode":"bypassPermissions","allow":["Edit"]}}"#,
    )
    .expect("ambient claude settings");

    let host = Arc::new(RecordingHost::default());
    let (runtime, _) = Runtime::builder(runtime_config(&root, server.url()))
        .profile(RuntimeProfile::Desktop)
        .host_capabilities(HostCapabilities {
            fs_read: true,
            fs_write: true,
            ..HostCapabilities::default()
        })
        .host_delegate(host.clone())
        .in_process_mcp_servers([InProcessMcpServer::new(
            "sdk-fixture",
            "fixture-id",
            Arc::new(InProcessFixture {
                contexts: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
        )])
        .start()
        .await
        .expect("desktop runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");

    let names = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(servers) = runtime.list_mcp_servers(&session, false).await
                && servers
                    .iter()
                    .any(|server| server.status == Some(McpServerStatus::Ready))
            {
                break servers
                    .into_iter()
                    .map(|server| server.name)
                    .collect::<Vec<_>>();
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("declared MCP server initializes");
    assert_eq!(
        names,
        vec!["sdk-fixture".to_owned()],
        "the session catalog must hold exactly the declared servers"
    );

    let denied_call = server.expect_response(
        "ambient auto-approve must not bypass the host",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "write-denied",
            "search_replace",
            r#"{"file_path":"note.txt","old_string":"before","new_string":"ambient"}"#,
        ),
    );
    runtime
        .prompt(&session, "turn-ambient", "attempt an edit")
        .await
        .expect("a denied tool does not fail the turn transport");
    denied_call.assert_satisfied();
    assert!(
        host.request_methods()
            .contains(&"session/request_permission".into()),
        "the host delegate must decide, not ambient permission settings"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("note.txt")).unwrap(),
        "before",
        "the host denied the edit"
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_host_serves_complete_terminal_lifecycle_including_timeout_kill() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let host = Arc::new(RecordingHost::approving());
    let (runtime, _) = Runtime::builder(runtime_config(&root, server.url()))
        .profile(RuntimeProfile::Desktop)
        .host_capabilities(HostCapabilities {
            terminal: true,
            ..HostCapabilities::default()
        })
        .host_delegate(host.clone())
        .start()
        .await
        .expect("desktop runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");

    host.slow_terminal_wait.store(true, Ordering::Release);
    let terminal_call = server.expect_response(
        "terminal timeout tool call",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "terminal-timeout",
            "run_terminal_command",
            r#"{"command":"sleep 10","timeout":1,"description":"exercise host kill"}"#,
        ),
    );
    runtime
        .prompt(&session, "turn-terminal-timeout", "run the timeout command")
        .await
        .expect("timeout remains a normal tool outcome");
    terminal_call.assert_satisfied();
    let calls = host.request_methods();
    for method in [
        "terminal/create",
        "terminal/wait_for_exit",
        "terminal/kill",
        "terminal/output",
        "terminal/release",
    ] {
        assert!(
            calls.contains(&method.into()),
            "missing {method}: {calls:?}"
        );
    }
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn profiles_extensions_and_explicit_plugin_paths_are_real_agent_capabilities() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let plugin = root.path().join("desktop-plugin");
    std::fs::create_dir(&plugin).expect("plugin dir");
    std::fs::write(plugin.join("plugin.json"), r#"{"name":"desktop-plugin"}"#)
        .expect("plugin manifest");

    let (restricted, _) = Runtime::builder(runtime_config(&root, server.url()))
        .plugin_paths([plugin.clone()])
        .start()
        .await
        .expect("restricted runtime starts");
    let restricted_session = restricted
        .create_session(session_config(workspace.clone()))
        .await
        .expect("restricted session");
    let restricted_plugins = restricted
        .extension_request(ExtensionRequest {
            method: "x.ai/plugins/list".into(),
            params: serde_json::json!({"sessionId":restricted_session.as_str()}),
        })
        .await
        .expect_err("restricted generic extensions are disabled");
    assert!(matches!(restricted_plugins, Error::Operation(_)));
    let restricted_caps = restricted.capabilities();
    assert_eq!(restricted_caps.profile, RuntimeProfile::Restricted);
    assert!(restricted_caps.features.iter().any(|capability| {
        capability.namespace == "feature:app_deployment"
            && !capability.enabled
            && capability.disabled_reason.as_deref()
                == Some("App Builder deployment is not implemented in this source checkout")
    }));
    assert!(restricted_caps.features.iter().any(|capability| {
        capability.namespace == "sdk:mcp"
            && !capability.enabled
            && capability.disabled_reason.as_deref() == Some("restricted profile")
    }));
    assert!(restricted_caps.features.iter().any(|capability| {
        capability.namespace == "sdk:autonomous-runs"
            && capability.enabled
            && capability.effect_class == "state-agent"
            && capability.host_requirement.is_none()
    }));
    assert!(restricted_caps.features.iter().any(|capability| {
        capability.namespace == "feature:plugins"
            && !capability.enabled
            && capability.disabled_reason.as_deref() == Some("restricted profile")
    }));
    restricted.shutdown().await.expect("restricted shuts down");

    let (desktop, _) = Runtime::builder(runtime_config(&root, server.url()))
        .profile(RuntimeProfile::Desktop)
        .yolo_mode(true)
        .plugin_paths([plugin])
        .start()
        .await
        .expect("desktop runtime starts");
    let desktop_session = desktop
        .create_session(session_config(workspace))
        .await
        .expect("desktop session");
    let desktop_plugins = desktop
        .extension_request(ExtensionRequest {
            method: "x.ai/plugins/list".into(),
            params: serde_json::json!({"sessionId":desktop_session.as_str()}),
        })
        .await
        .expect("desktop plugin list");
    assert!(
        desktop_plugins.result["result"]["plugins"]
            .as_array()
            .is_some_and(|plugins| plugins
                .iter()
                .any(|plugin| plugin["name"] == "desktop-plugin"))
    );
    assert_eq!(
        desktop
            .extension_request(ExtensionRequest {
                method: "x.ai/skills/refresh-baseline".into(),
                params: serde_json::json!({"futureField":{"preserved":true}}),
            })
            .await
            .expect("known extension")
            .result,
        serde_json::json!({"result":{"ok":true}})
    );
    let unknown = desktop
        .extension_request(ExtensionRequest {
            method: "x.ai/future/not-yet-implemented".into(),
            params: serde_json::json!({"opaque":[1,2,3]}),
        })
        .await
        .expect_err("unknown extension preserves protocol error");
    assert!(matches!(
        unknown,
        Error::Protocol { code: -32601, ref data, .. }
            if data.as_str().is_some_and(|message| message.contains("x.ai/future/not-yet-implemented"))
    ));
    desktop
        .notify_extension(ExtensionNotification {
            method: "x.ai/yolo_mode_changed".into(),
            params: serde_json::json!({"yolo_mode":true}),
        })
        .await
        .expect("extension notification reaches the agent");
    let desktop_caps = desktop.capabilities();
    assert_eq!(desktop_caps.profile, RuntimeProfile::Desktop);
    assert!(desktop_caps.features.iter().any(|capability| {
        capability.namespace == "sdk:extension-bridge" && capability.enabled
    }));
    assert!(desktop_caps.features.iter().any(|capability| {
        capability.namespace == "feature:managed_mcp"
            && !capability.enabled
            && capability
                .disabled_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("account-product service"))
    }));
    assert!(
        desktop_caps
            .features
            .iter()
            .any(|capability| { capability.namespace == "feature:plugins" && capability.enabled })
    );
    desktop.shutdown().await.expect("desktop shuts down");
}
