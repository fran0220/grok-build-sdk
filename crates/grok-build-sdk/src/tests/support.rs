use super::*;

pub(super) fn runtime_config(root: &TempDir, endpoint: String) -> RuntimeConfig {
    RuntimeConfig {
        endpoint,
        api_key: "test-key".into(),
        grok_home: root.path().join("grok"),
        session_storage: root.path().join("sessions"),
        models: vec![ModelSpec {
            id: "test-model".into(),
            context_window: 131_072,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        }],
    }
}

pub(super) fn session_config(cwd: PathBuf) -> SessionConfig {
    SessionConfig {
        cwd,
        model: "test-model".into(),
        reasoning: None,
        system_prompt: None,
        rules: None,
    }
}

pub(super) fn provider(
    base_url: String,
    api_key: &str,
    model: &str,
    header_value: &str,
) -> ApiProviderConfig {
    ApiProviderConfig {
        base_url,
        api_key: api_key.into(),
        model: Some(model.into()),
        headers: BTreeMap::from([("x-origin-provider".into(), header_value.into())]),
        query_params: BTreeMap::new(),
    }
}

pub(super) fn request_with_user_marker(
    server: &MockInferenceServer,
    marker: &str,
) -> serde_json::Value {
    server
        .requests()
        .into_iter()
        .filter(|entry| entry.path.contains("chat/completions") || entry.path.contains("responses"))
        .filter_map(|entry| entry.body)
        .find(|body| {
            body.get("tools")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|tools| !tools.is_empty())
                && body.get("tool_choice").is_none()
                && body
                    .get("messages")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|message| message.get("content"))
                    .any(|content| content.as_str().is_some_and(|text| text.contains(marker)))
        })
        .expect("foreground inference request with marker")
}

pub(super) fn message_prefix_is_unchanged(
    earlier: &serde_json::Value,
    later: &serde_json::Value,
) -> bool {
    let earlier = earlier
        .get("messages")
        .and_then(serde_json::Value::as_array)
        .expect("earlier chat messages");
    let later = later
        .get("messages")
        .and_then(serde_json::Value::as_array)
        .expect("later chat messages");
    later.starts_with(earlier)
}

pub(super) fn chat_tool_call(call_id: &str, name: &str, arguments: &str) -> ScriptedResponse {
    let tool_calls = vec![serde_json::json!({
        "index": 0,
        "id": call_id,
        "type": "function",
        "function": { "name": name, "arguments": arguments }
    })];
    ScriptedResponse::sse(vec![
        SseEvent::data(
            serde_json::json!({
                "id": "chatcmpl-origin-tool",
                "object": "chat.completion.chunk",
                "created": 1234567890,
                "model": "test-model",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": tool_calls
                    },
                    "finish_reason": null
                }]
            })
            .to_string(),
        ),
        SseEvent::data(
            serde_json::json!({
                "id": "chatcmpl-origin-tool",
                "object": "chat.completion.chunk",
                "created": 1234567890,
                "model": "test-model",
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "tool_calls"
                }],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 20,
                    "total_tokens": 30
                }
            })
            .to_string(),
        ),
        SseEvent::data("[DONE]"),
    ])
}
