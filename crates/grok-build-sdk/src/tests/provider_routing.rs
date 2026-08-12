use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_model_providers_route_endpoint_auth_headers_and_wire_model() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fast = MockInferenceServer::start().await.expect("fast provider");
    let deep = MockInferenceServer::start().await.expect("deep provider");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, String::new());
    config.api_key.clear();
    config.models.push(ModelSpec {
        id: "deep-model".into(),
        context_window: 65_536,
        api_backend: ApiBackend::ChatCompletions,
        supports_reasoning: false,
        default_reasoning: None,
        reasoning_options: Vec::new(),
    });

    let mut fast_provider = provider(fast.url(), "fast-secret", "provider-fast", "fast");
    fast_provider
        .query_params
        .insert("tenant".into(), "fast".into());
    let mut deep_provider = provider(deep.url(), "deep-secret", "provider-deep", "deep");
    deep_provider
        .query_params
        .insert("tenant".into(), "deep".into());
    let (runtime, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .model_provider("test-model", fast_provider)
        .model_provider("deep-model", deep_provider)
        .start()
        .await
        .expect("runtime starts solely from explicit providers");
    let fast_session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("fast session");
    runtime
        .prompt(&fast_session, "fast-turn", "provider fast marker")
        .await
        .expect("fast provider turn");
    let deep_session = runtime
        .create_session(SessionConfig {
            cwd: workspace,
            model: "deep-model".into(),
            reasoning: None,
            system_prompt: None,
            rules: None,
        })
        .await
        .expect("deep session");
    runtime
        .prompt(&deep_session, "deep-turn", "provider deep marker")
        .await
        .expect("deep provider turn");

    let fast_body = request_with_user_marker(&fast, "provider fast marker");
    let fast_request = fast
        .requests()
        .into_iter()
        .find(|request| request.body.as_ref() == Some(&fast_body))
        .expect("fast provider foreground request");
    assert_eq!(
        fast_request.authorization.as_deref(),
        Some("Bearer fast-secret")
    );
    assert_eq!(fast_request.header("x-origin-provider"), Some("fast"));
    assert_eq!(fast_request.path, "/v1/chat/completions?tenant=fast");
    assert_eq!(fast_body["model"], "provider-fast");
    let deep_body = request_with_user_marker(&deep, "provider deep marker");
    let deep_request = deep
        .requests()
        .into_iter()
        .find(|request| request.body.as_ref() == Some(&deep_body))
        .expect("deep provider foreground request");
    assert_eq!(
        deep_request.authorization.as_deref(),
        Some("Bearer deep-secret")
    );
    assert_eq!(deep_request.header("x-origin-provider"), Some("deep"));
    assert_eq!(deep_request.path, "/v1/chat/completions?tenant=deep");
    assert_eq!(deep_body["model"], "provider-deep");
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_protocol_controls_responses_and_anthropic_wire_contracts() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    for (protocol, endpoint) in [
        (ProviderProtocol::OpenAiResponses, "/v1/responses"),
        (ProviderProtocol::AnthropicMessages, "/v1/messages"),
    ] {
        let server = MockInferenceServer::start().await.expect("provider");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, String::new());
        config.api_key.clear();
        // The legacy catalog backend deliberately disagrees. An explicit
        // provider protocol must be the sole wire/auth source of truth.
        config.models[0].api_backend = ApiBackend::ChatCompletions;
        let mut explicit = match protocol {
            ProviderProtocol::OpenAiResponses => {
                ProviderConfig::openai_responses(server.url(), "wire-secret", "wire-model")
            }
            ProviderProtocol::AnthropicMessages => {
                ProviderConfig::anthropic(server.url(), "wire-secret", "wire-model")
            }
            ProviderProtocol::OpenAiChatCompletions => unreachable!(),
        };
        explicit
            .headers
            .insert("x-provider-test".into(), "present".into());
        explicit
            .query_params
            .insert("tenant".into(), "protocol".into());
        let (runtime, _) = Runtime::builder(config)
            .profile(RuntimeProfile::Desktop)
            .model_provider("test-model", explicit)
            .start()
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace))
            .await
            .expect("session starts");
        runtime
            .prompt(&session, "protocol-turn", "provider protocol marker")
            .await
            .expect("provider turn succeeds");

        let expected_path = format!("{endpoint}?tenant=protocol");
        let requests = server.requests();
        let request = requests
            .iter()
            .find(|request| {
                request.path == expected_path
                    && request.body.as_ref().is_some_and(|body| {
                        body["model"] == "wire-model"
                            && body.to_string().contains("provider protocol marker")
                    })
            })
            .unwrap_or_else(|| {
                let shapes = requests
                    .iter()
                    .map(|request| {
                        (
                            request.path.clone(),
                            request.body.as_ref().map(|body| body["model"].clone()),
                            request.body.as_ref().is_some_and(|body| {
                                body.to_string().contains("provider protocol marker")
                            }),
                        )
                    })
                    .collect::<Vec<_>>();
                panic!("protocol endpoint request: {shapes:?}")
            });
        let body = request.body.as_ref().expect("JSON request");
        assert_eq!(body["model"], "wire-model");
        assert_eq!(request.header("x-provider-test"), Some("present"));
        assert!(
            body["tools"]
                .as_array()
                .is_some_and(|tools| !tools.is_empty())
        );
        match protocol {
            ProviderProtocol::OpenAiResponses => {
                assert_eq!(request.authorization.as_deref(), Some("Bearer wire-secret"));
                assert!(request.header("x-api-key").is_none());
                assert!(body["input"].is_array());
            }
            ProviderProtocol::AnthropicMessages => {
                assert!(request.authorization.is_none());
                assert_eq!(request.header("x-api-key"), Some("wire-secret"));
                assert_eq!(request.header("anthropic-version"), Some("2023-06-01"));
                assert!(body["messages"].is_array());
                assert!(body.get("system").is_some());
            }
            ProviderProtocol::OpenAiChatCompletions => unreachable!(),
        }
        runtime.shutdown().await.expect("runtime shuts down");
    }
}

#[test]
fn desktop_explicit_provider_isolated_from_hostile_ambient_credentials() {
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "tests::provider_routing::desktop_explicit_provider_isolated_from_hostile_ambient_credentials_child",
            "--exact",
            "--nocapture",
        ])
        .env("ORIGIN_AMBIENT_CREDENTIAL_CHILD", "1")
        .env("XAI_API_KEY", "ambient-secret-must-not-leak")
        .env("GROK_DEPLOYMENT_KEY", "ambient-deployment-must-not-leak")
        .env("GROK_XAI_API_BASE_URL", "http://127.0.0.1:9/ambient")
        .output()
        .expect("run isolated credential child");
    assert!(
        output.status.success(),
        "isolated credential child failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desktop_explicit_provider_isolated_from_hostile_ambient_credentials_child() {
    if std::env::var_os("ORIGIN_AMBIENT_CREDENTIAL_CHILD").is_none() {
        return;
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let explicit = MockInferenceServer::start()
        .await
        .expect("explicit provider");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, String::new());
    config.api_key.clear();
    let (runtime, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .model_provider(
            "test-model",
            provider(
                explicit.url(),
                "explicit-secret",
                "explicit-wire",
                "explicit",
            ),
        )
        .start()
        .await
        .expect("runtime starts without ambient provider state");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt(
            &session,
            "ambient-isolation-turn",
            "ambient isolation marker",
        )
        .await
        .expect("explicit provider turn");

    let body = request_with_user_marker(&explicit, "ambient isolation marker");
    let request = explicit
        .requests()
        .into_iter()
        .find(|request| request.body.as_ref() == Some(&body))
        .expect("explicit foreground request");
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer explicit-secret")
    );
    assert_eq!(body["model"], "explicit-wire");
    assert!(
        !body["tools"].to_string().contains("web_search"),
        "an omitted auxiliary search role must remain disabled"
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auxiliary_session_summary_uses_its_catalog_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let main = MockInferenceServer::start().await.expect("main provider");
    let utility = MockInferenceServer::start()
        .await
        .expect("utility provider");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, String::new());
    config.api_key.clear();
    config.models.push(ModelSpec {
        id: "utility-model".into(),
        context_window: 32_768,
        api_backend: ApiBackend::ChatCompletions,
        supports_reasoning: false,
        default_reasoning: None,
        reasoning_options: Vec::new(),
    });
    let agent_services = AgentServiceConfig {
        session_summary_model: Some("utility-model".into()),
        ..AgentServiceConfig::default()
    };
    let (runtime, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .model_provider(
            "test-model",
            provider(main.url(), "main-secret", "main-wire", "main"),
        )
        .model_provider(
            "utility-model",
            provider(utility.url(), "utility-secret", "utility-wire", "utility"),
        )
        .agent_services(agent_services)
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt(&session, "summary-turn", "summarize provider routing")
        .await
        .expect("turn succeeds");

    let summary_wait = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !utility
            .requests()
            .iter()
            .any(|request| request.path == "/v1/chat/completions")
        {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    if summary_wait.is_err() {
        let main_models = main
            .requests()
            .into_iter()
            .filter_map(|request| request.body)
            .filter_map(|body| body.get("model").cloned())
            .collect::<Vec<_>>();
        panic!("summary provider was not called; main request models: {main_models:?}");
    }
    let summary = utility
        .requests()
        .into_iter()
        .find(|request| request.path == "/v1/chat/completions")
        .expect("summary provider request");
    assert_eq!(
        summary.authorization.as_deref(),
        Some("Bearer utility-secret")
    );
    assert_eq!(summary.header("x-origin-provider"), Some("utility"));
    assert_eq!(summary.body.as_ref().unwrap()["model"], "utility-wire");
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auxiliary_image_description_uses_its_catalog_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let main = MockInferenceServer::start().await.expect("main provider");
    let vision = MockInferenceServer::start().await.expect("vision provider");
    vision.set_response("a blue rectangle");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, String::new());
    config.api_key.clear();
    config.models.push(ModelSpec {
        id: "vision-model".into(),
        context_window: 32_768,
        api_backend: ApiBackend::ChatCompletions,
        supports_reasoning: false,
        default_reasoning: None,
        reasoning_options: Vec::new(),
    });
    let (runtime, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .model_provider(
            "test-model",
            provider(main.url(), "main-secret", "main-wire", "main"),
        )
        .model_provider(
            "vision-model",
            provider(vision.url(), "vision-secret", "vision-wire", "vision"),
        )
        .agent_services(AgentServiceConfig {
            image_description_model: Some("vision-model".into()),
            ..AgentServiceConfig::default()
        })
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt_blocks(
            &session,
            "vision-turn",
            vec![
                PromptBlock::Text {
                    text: "describe image provider marker".into(),
                },
                PromptBlock::Image {
                    data: "iVBORw0KGgoAAAANSUhEUgAAACAAAAAQCAIAAAD4YuoOAAAAHUlEQVR42mPQqDhBU8QwasGoBaMWjFowasFQsAAAxdvQH+YmXBQAAAAASUVORK5CYII=".into(),
                    mime_type: "image/png".into(),
                    uri: None,
                },
            ],
        )
        .await
        .expect("image prompt succeeds");

    let request = vision
        .requests()
        .into_iter()
        .find(|request| request.path == "/v1/chat/completions")
        .expect("vision provider request");
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer vision-secret")
    );
    assert_eq!(request.header("x-origin-provider"), Some("vision"));
    assert_eq!(request.body.as_ref().unwrap()["model"], "vision-wire");
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auxiliary_prompt_suggestion_uses_its_catalog_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let main = MockInferenceServer::start().await.expect("main provider");
    let suggestion = MockInferenceServer::start()
        .await
        .expect("suggestion provider");
    suggestion.set_response("continue");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, String::new());
    config.api_key.clear();
    config.models.push(ModelSpec {
        id: "suggestion-model".into(),
        context_window: 32_768,
        api_backend: ApiBackend::ChatCompletions,
        supports_reasoning: false,
        default_reasoning: None,
        reasoning_options: Vec::new(),
    });
    let (runtime, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .model_provider(
            "test-model",
            provider(main.url(), "main-secret", "main-wire", "main"),
        )
        .model_provider(
            "suggestion-model",
            provider(
                suggestion.url(),
                "suggestion-secret",
                "suggestion-wire",
                "suggestion",
            ),
        )
        .agent_services(AgentServiceConfig {
            prompt_suggestion_model: Some("suggestion-model".into()),
            ..AgentServiceConfig::default()
        })
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt(
            &session,
            "suggestion-seed",
            "finish the task then suggest the next step",
        )
        .await
        .expect("seed turn succeeds");
    let response = runtime
        .extension_request(ExtensionRequest {
            method: "x.ai/suggestPrompt".into(),
            params: serde_json::json!({
                "sessionId": session.as_str(),
                "generation": 7
            }),
        })
        .await
        .expect("suggestion extension succeeds");

    assert_eq!(response.result["generation"], 7);
    assert_eq!(response.result["suggestion"], "continue");
    let request = suggestion
        .requests()
        .into_iter()
        .find(|request| request.path == "/v1/chat/completions")
        .expect("suggestion provider request");
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer suggestion-secret")
    );
    assert_eq!(request.header("x-origin-provider"), Some("suggestion"));
    assert_eq!(request.body.as_ref().unwrap()["model"], "suggestion-wire");
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auxiliary_web_search_uses_its_catalog_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let main = MockInferenceServer::start().await.expect("main provider");
    let search = MockInferenceServer::start().await.expect("search provider");
    let search_response = ScriptedResponse::json(
        200,
        serde_json::json!({
            "id": "resp_search",
            "object": "response",
            "created_at": 1234567890,
            "status": "completed",
            "model": "search-wire",
            "output": [{
                "type": "message",
                "id": "msg_search",
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": "current search result",
                    "annotations": []
                }]
            }]
        }),
    );
    let search_call = search.expect_response(
        "custom web search request",
        InferenceRequestMatcher::auxiliary(InferenceEndpoint::Responses),
        search_response,
    );
    let tool_call = main.expect_response(
        "invoke web search tool",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "search-web",
            "web_search",
            r#"{"query":"current rust release","allowed_domains":["rust-lang.org"]}"#,
        ),
    );
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, String::new());
    config.api_key.clear();
    config.models.push(ModelSpec {
        id: "search-model".into(),
        context_window: 32_768,
        api_backend: ApiBackend::Responses,
        supports_reasoning: false,
        default_reasoning: None,
        reasoning_options: Vec::new(),
    });
    let mut search_provider = provider(search.url(), "search-secret", "search-wire", "search");
    search_provider.protocol = ProviderProtocol::OpenAiResponses;
    search_provider
        .query_params
        .insert("tenant".into(), "search".into());
    let (runtime, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .model_provider(
            "test-model",
            provider(main.url(), "main-secret", "main-wire", "main"),
        )
        .model_provider("search-model", search_provider)
        .agent_services(AgentServiceConfig {
            web_search_model: Some("search-model".into()),
            ..AgentServiceConfig::default()
        })
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt(&session, "search-turn", "search the current rust release")
        .await
        .expect("web search turn succeeds");
    tool_call.assert_satisfied();
    search_call.assert_satisfied();

    let request = search
        .requests()
        .into_iter()
        .find(|request| request.path == "/v1/responses?tenant=search")
        .expect("search provider request");
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer search-secret")
    );
    assert_eq!(request.header("x-origin-provider"), Some("search"));
    assert_eq!(request.body.as_ref().unwrap()["model"], "search-wire");
    assert_eq!(
        request.body.as_ref().unwrap()["input"],
        "current rust release"
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_subagent_model_uses_its_configured_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let parent = MockInferenceServer::start().await.expect("parent provider");
    let child = MockInferenceServer::start().await.expect("child provider");
    let subagent_call = parent.expect_response(
        "spawn configured subagent",
        InferenceRequestMatcher::foreground(InferenceEndpoint::ChatCompletions),
        chat_tool_call(
            "spawn-child",
            "spawn_subagent",
            r#"{"description":"provider routing","prompt":"answer from child","subagent_type":"general-purpose","background":false}"#,
        ),
    );
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, String::new());
    config.api_key.clear();
    config.models.push(ModelSpec {
        id: "child-model".into(),
        context_window: 65_536,
        api_backend: ApiBackend::ChatCompletions,
        supports_reasoning: false,
        default_reasoning: None,
        reasoning_options: Vec::new(),
    });
    let mut agent_services = AgentServiceConfig::default();
    agent_services
        .subagent_models
        .insert("general-purpose".into(), "child-model".into());
    let (runtime, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .model_provider(
            "test-model",
            provider(parent.url(), "parent-secret", "parent-wire", "parent"),
        )
        .model_provider(
            "child-model",
            provider(child.url(), "child-secret", "child-wire", "child"),
        )
        .agent_services(agent_services)
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt(&session, "subagent-turn", "delegate this request")
        .await
        .expect("subagent turn succeeds");
    subagent_call.assert_satisfied();

    let child_request = child
        .requests()
        .into_iter()
        .find(|request| request.path == "/v1/chat/completions")
        .unwrap_or_else(|| {
            let parent_bodies = parent
                .requests()
                .into_iter()
                .filter_map(|request| request.body)
                .collect::<Vec<_>>();
            panic!("child provider received no inference; parent bodies: {parent_bodies:#?}")
        });
    assert_eq!(
        child_request.authorization.as_deref(),
        Some("Bearer child-secret")
    );
    assert_eq!(child_request.header("x-origin-provider"), Some("child"));
    assert_eq!(child_request.body.as_ref().unwrap()["model"], "child-wire");
    runtime.shutdown().await.expect("runtime shuts down");
}
