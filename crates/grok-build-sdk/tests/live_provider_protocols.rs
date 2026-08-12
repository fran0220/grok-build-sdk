use grok_build_sdk::{ProviderConfig, ProviderProtocol};
use xai_grok_sampler::{ApiBackend, AuthScheme, SamplerConfig, SamplingClient};
use xai_grok_sampling_types::{ConversationItem, ConversationRequest};

fn live_base_url(value: String) -> String {
    let value = value.trim_end_matches('/');
    if value.ends_with("/v1") {
        value.to_owned()
    } else {
        format!("{value}/v1")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires ORIGINGAME_BASE_URL, ORIGINGAME_API_KEY, and ORIGINGAME_MODEL"]
async fn live_service_accepts_all_three_provider_protocols() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let base_url = live_base_url(std::env::var("ORIGINGAME_BASE_URL").expect("base URL"));
    let api_key = std::env::var("ORIGINGAME_API_KEY").expect("API key");
    let model = std::env::var("ORIGINGAME_MODEL").expect("model");
    let selected_protocol = std::env::var("ORIGINGAME_PROTOCOL").ok();

    for provider in [
        ProviderConfig::openai_chat(&base_url, &api_key, &model),
        ProviderConfig::openai_responses(&base_url, &api_key, &model),
        ProviderConfig::anthropic(&base_url, &api_key, &model),
    ] {
        let (protocol_name, api_backend, auth_scheme) = match provider.protocol {
            ProviderProtocol::OpenAiChatCompletions => (
                "openai_chat_completions",
                ApiBackend::ChatCompletions,
                AuthScheme::Bearer,
            ),
            ProviderProtocol::OpenAiResponses => (
                "openai_responses",
                ApiBackend::Responses,
                AuthScheme::Bearer,
            ),
            ProviderProtocol::AnthropicMessages => (
                "anthropic_messages",
                ApiBackend::Messages,
                AuthScheme::XApiKey,
            ),
        };
        if selected_protocol
            .as_deref()
            .is_some_and(|selected| selected != protocol_name)
        {
            continue;
        }
        eprintln!("validating live provider protocol: {protocol_name}");
        let client = SamplingClient::new(SamplerConfig {
            api_key: Some(provider.api_key),
            base_url: provider.base_url,
            model: provider.model.expect("wire model"),
            api_backend,
            auth_scheme,
            extra_headers: provider.headers.into_iter().collect(),
            query_params: provider.query_params.into_iter().collect(),
            max_retries: Some(0),
            ..SamplerConfig::default()
        })
        .expect("live sampling client");
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(45),
            client.conversation_collect(ConversationRequest::from_items(vec![
                ConversationItem::user("Reply with exactly SDK_PROVIDER_OK."),
            ])),
        )
        .await
        .expect("live provider request timed out")
        .expect("live provider request succeeds");
        assert!(
            response.assistant_text().contains("SDK_PROVIDER_OK"),
            "live provider response did not contain the expected marker"
        );
    }
}
