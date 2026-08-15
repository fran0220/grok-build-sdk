use sophon_sdk::{ProviderConfig, ProviderProtocol};

#[test]
fn constructors_select_the_three_supported_wire_protocols() {
    let chat = ProviderConfig::openai_chat("https://example.test/v1", "chat-key", "chat-model");
    let responses = ProviderConfig::openai_responses(
        "https://example.test/v1",
        "responses-key",
        "response-model",
    );
    let anthropic =
        ProviderConfig::anthropic("https://example.test/v1", "anthropic-key", "claude-model");

    assert_eq!(chat.protocol, ProviderProtocol::OpenAiChatCompletions);
    assert_eq!(responses.protocol, ProviderProtocol::OpenAiResponses);
    assert_eq!(anthropic.protocol, ProviderProtocol::AnthropicMessages);
    assert_eq!(chat.model.as_deref(), Some("chat-model"));
    assert_eq!(responses.model.as_deref(), Some("response-model"));
    assert_eq!(anthropic.model.as_deref(), Some("claude-model"));
    chat.validate().expect("chat provider");
    responses.validate().expect("responses provider");
    anthropic.validate().expect("anthropic provider");
}

#[test]
fn validation_rejects_invalid_urls_headers_and_empty_credentials() {
    let mut invalid_url = ProviderConfig::openai_chat("relative/v1", "key", "model");
    assert!(invalid_url.validate().is_err());

    invalid_url.base_url = "https://user@example.test/v1".into();
    assert!(invalid_url.validate().is_err());

    let mut invalid_header =
        ProviderConfig::openai_responses("https://example.test/v1", "key", "model");
    invalid_header
        .headers
        .insert("bad header".into(), "value".into());
    assert!(invalid_header.validate().is_err());

    let mut overridden_auth =
        ProviderConfig::openai_chat("https://example.test/v1", "key", "model");
    overridden_auth
        .headers
        .insert("Authorization".into(), "other secret".into());
    assert!(overridden_auth.validate().is_err());

    let empty_key = ProviderConfig::anthropic("https://example.test/v1", "", "model");
    assert!(empty_key.validate().is_err());
}
