use async_trait::async_trait;
use serde_json::Value;
use sophon_sdk::{
    AgentServiceConfig, HostDelegate, HostError, HostNotification, HostRequest, McpServerConfig,
    MediaProviderConfig, MediaServiceConfig, ProviderConfig, Runtime, RuntimeConfig,
    RuntimeProfile,
};
use std::{collections::BTreeMap, path::PathBuf};

struct DesktopHost;
#[async_trait]
impl HostDelegate for DesktopHost {
    async fn request(&self, request: HostRequest) -> Result<Value, HostError> {
        Err(HostError {
            code: -32601,
            message: format!("unsupported: {}", request.method),
            data: Value::Null,
        })
    }
    async fn notification(&self, _notification: HostNotification) -> Result<(), HostError> {
        Ok(())
    }
}

fn provider(base_url: &str, key: &str, wire_model: &str) -> ProviderConfig {
    ProviderConfig::openai_chat(base_url, key, wire_model)
}

// `config.models` is the fixed SDK catalog. `endpoint`/`api_key` may be empty
// when every catalog model gets an explicit provider below.
fn configure(config: RuntimeConfig) -> sophon_sdk::RuntimeBuilder {
    let mut agents = AgentServiceConfig::default();
    agents
        .subagent_models
        .insert("general-purpose".into(), "subagent".into());
    agents.session_summary_model = Some("utility".into());

    Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .model_provider(
            "primary",
            provider("https://models.example/v1", "primary-key", "main-model"),
        )
        .model_provider(
            "subagent",
            provider("https://agents.example/v1", "agent-key", "worker-model"),
        )
        .model_provider(
            "utility",
            provider("https://utility.example/v1", "utility-key", "small-model"),
        )
        .agent_services(agents)
        .media_service(MediaServiceConfig {
            provider: MediaProviderConfig {
                base_url: "https://media.example/v1".into(),
                api_key: "media-key".into(),
                headers: BTreeMap::new(),
                query_params: BTreeMap::from([("tenant".into(), "desktop".into())]),
            },
            image_generation: true,
            image_edit: true,
            video_generation: true,
            image_generation_model: Some("image-gen".into()),
            image_edit_model: Some("image-edit".into()),
            image_to_video_model: Some("image-video".into()),
            reference_to_video_model: Some("reference-video".into()),
        })
        .mcp_servers([McpServerConfig::Stdio {
            name: "project-tools".into(),
            command: PathBuf::from("project-mcp"),
            args: Vec::new(),
            env: BTreeMap::new(),
        }])
        .host_delegate(std::sync::Arc::new(DesktopHost))
}

// After `RuntimeBuilder::start`, hosts can discover the fixed SDK catalog
// without enabling or exposing the generic Desktop extension bridge.
async fn inspect_catalog(runtime: &Runtime) -> Result<(), sophon_sdk::Error> {
    let catalog = runtime.list_models().await?;
    println!("current model: {}", catalog.current_model_id);
    for model in catalog.available_models {
        println!("available model: {} ({})", model.name, model.id);
    }
    Ok(())
}

fn main() {
    let _host: std::sync::Arc<dyn HostDelegate> = std::sync::Arc::new(DesktopHost);
    let _configure = configure;
    let _inspect_catalog = inspect_catalog;
}
