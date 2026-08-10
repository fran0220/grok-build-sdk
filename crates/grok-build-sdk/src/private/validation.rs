use super::*;

pub(super) fn validate(c: &RuntimeConfig, options: &RuntimeOptions) -> Result<(), Error> {
    if c.models.is_empty() {
        return Err(Error::InvalidConfig("model catalog is required".into()));
    }
    if c.models
        .iter()
        .any(|m| m.id.trim().is_empty() || NonZeroU64::new(m.context_window).is_none())
    {
        return Err(Error::InvalidConfig(
            "model id and non-zero context window are required".into(),
        ));
    }
    if c.models.iter().any(|model| {
        let options_valid = model
            .reasoning_options
            .iter()
            .all(|option| !option.trim().is_empty())
            && model.reasoning_options.iter().collect::<HashSet<_>>().len()
                == model.reasoning_options.len();
        !options_valid
            || (!model.supports_reasoning
                && (model.default_reasoning.is_some() || !model.reasoning_options.is_empty()))
            || model.default_reasoning.as_ref().is_some_and(|default| {
                !model.supports_reasoning || !model.reasoning_options.contains(default)
            })
    }) {
        return Err(Error::InvalidConfig(
            "model reasoning defaults and options must be unique, non-empty, and consistent".into(),
        ));
    }
    let model_ids: HashSet<&str> = c.models.iter().map(|model| model.id.as_str()).collect();
    if model_ids.len() != c.models.len() {
        return Err(Error::InvalidConfig(
            "catalog model ids must be unique".into(),
        ));
    }
    if options
        .services
        .model_providers
        .keys()
        .any(|model| !model_ids.contains(model.as_str()))
    {
        return Err(Error::InvalidConfig(
            "model provider refers to an unknown catalog model".into(),
        ));
    }
    if options.services.model_providers.values().any(|provider| {
        provider.base_url.trim().is_empty()
            || provider.api_key.trim().is_empty()
            || provider
                .model
                .as_deref()
                .is_some_and(|model| model.trim().is_empty())
            || provider.headers.keys().any(|name| name.trim().is_empty())
            || provider
                .query_params
                .keys()
                .any(|name| name.trim().is_empty())
    }) {
        return Err(Error::InvalidConfig(
            "model providers require a base URL, API key, non-empty optional model slug, and non-empty header/query names".into(),
        ));
    }
    let legacy_provider_available = !c.endpoint.trim().is_empty() && !c.api_key.trim().is_empty();
    if !legacy_provider_available
        && c.models
            .iter()
            .any(|model| !options.services.model_providers.contains_key(&model.id))
    {
        return Err(Error::InvalidConfig(
            "each catalog model requires an explicit provider when the legacy endpoint and API key are empty"
                .into(),
        ));
    }
    let agents = &options.services.agents;
    if agents
        .subagent_models
        .iter()
        .any(|(agent, model)| agent.trim().is_empty() || !model_ids.contains(model.as_str()))
        || [
            agents.web_search_model.as_deref(),
            agents.session_summary_model.as_deref(),
            agents.image_description_model.as_deref(),
            agents.prompt_suggestion_model.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|model| !model_ids.contains(model))
    {
        return Err(Error::InvalidConfig(
            "agent service routing must reference non-empty names and catalog models".into(),
        ));
    }
    if options.services.media.as_ref().is_some_and(|media| {
        media.provider.base_url.trim().is_empty()
            || media.provider.api_key.trim().is_empty()
            || media
                .provider
                .headers
                .keys()
                .any(|name| name.trim().is_empty())
            || media
                .provider
                .query_params
                .keys()
                .any(|name| name.trim().is_empty())
            || [
                media.image_generation_model.as_deref(),
                media.image_edit_model.as_deref(),
                media.image_to_video_model.as_deref(),
                media.reference_to_video_model.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|model| model.trim().is_empty())
    }) {
        return Err(Error::InvalidConfig(
            "media service requires an explicit base URL, API key, non-empty header/query names, and non-empty optional model slugs".into(),
        ));
    }
    validate_mcp_servers(&options.services.mcp_servers)?;
    let mut names: HashSet<&str> = options
        .services
        .mcp_servers
        .iter()
        .map(|server| match server {
            crate::McpServerConfig::Stdio { name, .. }
            | crate::McpServerConfig::Http { name, .. }
            | crate::McpServerConfig::Sse { name, .. } => name.as_str(),
        })
        .collect();
    let mut ids = HashSet::new();
    if options.in_process_mcp_servers.iter().any(|server| {
        server.name.trim().is_empty()
            || server.server_id.trim().is_empty()
            || !names.insert(server.name.as_str())
            || !ids.insert(server.server_id.as_str())
    }) {
        return Err(Error::InvalidConfig(
            "MCP server names and in-process server IDs must be unique and non-empty".into(),
        ));
    }
    if options.profile == crate::RuntimeProfile::Desktop {
        let mut callback_ids = HashSet::new();
        if options.agent_hooks.iter().any(|hook| {
            hook.callback_id.trim().is_empty()
                || !callback_ids.insert(hook.callback_id.as_str())
                || hook.matcher.as_ref().is_some_and(|m| m.trim().is_empty())
                || hook
                    .timeout
                    .is_some_and(|t| !t.is_finite() || t <= 0.0 || t > 600.0)
        }) {
            return Err(Error::InvalidConfig("agent hooks require unique non-empty callback IDs, non-empty matchers, and timeouts in (0, 600] seconds".into()));
        }
    }
    Ok(())
}
pub(super) fn validate_mcp_servers(servers: &[crate::McpServerConfig]) -> Result<(), Error> {
    let mut mcp_names = HashSet::new();
    if servers.iter().any(|server| {
        let (name, target, key_values) = match server {
            crate::McpServerConfig::Stdio {
                name, command, env, ..
            } => (name.as_str(), command.to_str().unwrap_or(""), env),
            crate::McpServerConfig::Http { name, url, headers }
            | crate::McpServerConfig::Sse { name, url, headers } => {
                (name.as_str(), url.as_str(), headers)
            }
        };
        name.trim().is_empty()
            || target.trim().is_empty()
            || !mcp_names.insert(name)
            || key_values.keys().any(|key| key.trim().is_empty())
    }) {
        return Err(Error::InvalidConfig(
            "MCP servers require unique non-empty names, targets, and variable/header names".into(),
        ));
    }
    Ok(())
}
pub(super) fn op(e: impl std::fmt::Display) -> Error {
    Error::Operation(e.to_string())
}

pub(super) fn run_error(error: xai_agent_lifecycle::run::RunError) -> Error {
    Error::DurableRun(error)
}

pub(super) fn protocol(method: &str, error: acp::Error) -> Error {
    Error::Protocol {
        method: method.into(),
        code: i32::from(error.code),
        message: error.message,
        data: error.data.unwrap_or(serde_json::Value::Null),
        retryable: false,
    }
}
pub(super) fn prompt_block_wire(block: &PromptBlock) -> Result<serde_json::Value, Error> {
    use base64::Engine as _;

    let validate_base64 = |data: &str, kind: &str| {
        base64::engine::general_purpose::STANDARD
            .decode(data)
            .map(|_| ())
            .map_err(|error| {
                Error::InvalidConfig(format!("{kind} data is not valid base64: {error}"))
            })
    };
    let value = match block {
        PromptBlock::Text { text } => serde_json::json!({"type":"text","text":text}),
        PromptBlock::Image {
            data,
            mime_type,
            uri,
        } => {
            if data.is_empty()
                || !mime_type.starts_with("image/")
                || uri.as_deref().is_some_and(str::is_empty)
            {
                return Err(Error::InvalidConfig("invalid image block".into()));
            }
            validate_base64(data, "image")?;
            serde_json::json!({"type":"image","data":data,"mimeType":mime_type,"uri":uri})
        }
        PromptBlock::Audio { data, mime_type } => {
            if data.is_empty() || !mime_type.starts_with("audio/") {
                return Err(Error::InvalidConfig("invalid audio block".into()));
            }
            validate_base64(data, "audio")?;
            serde_json::json!({"type":"audio","data":data,"mimeType":mime_type})
        }
        PromptBlock::ResourceLink {
            uri,
            name,
            mime_type,
        } => {
            if uri.is_empty() || name.is_empty() {
                return Err(Error::InvalidConfig("invalid resource link".into()));
            }
            serde_json::json!({"type":"resource_link","uri":uri,"name":name,"mimeType":mime_type})
        }
        PromptBlock::EmbeddedTextResource {
            uri,
            text,
            mime_type,
        } => {
            if uri.is_empty() {
                return Err(Error::InvalidConfig(
                    "embedded resource URI is required".into(),
                ));
            }
            serde_json::json!({"type":"resource","resource":{"uri":uri,"text":text,"mimeType":mime_type}})
        }
        PromptBlock::EmbeddedBlobResource {
            uri,
            blob,
            mime_type,
        } => {
            if uri.is_empty() || blob.is_empty() {
                return Err(Error::InvalidConfig(
                    "embedded blob resource URI and data are required".into(),
                ));
            }
            validate_base64(blob, "embedded resource")?;
            serde_json::json!({"type":"resource","resource":{"uri":uri,"blob":blob,"mimeType":mime_type}})
        }
    };
    Ok(value)
}

pub(super) fn client_capability_meta(options: &RuntimeOptions) -> Result<acp::Meta, Error> {
    let mut meta = match &options.host_capabilities.meta {
        serde_json::Value::Null => acp::Meta::new(),
        serde_json::Value::Object(meta) => meta.clone(),
        _ => {
            return Err(Error::InvalidConfig(
                "host capability metadata must be a JSON object".into(),
            ));
        }
    };
    meta.insert(
        "clientIdentifier".into(),
        serde_json::Value::String(options.client_identifier.clone()),
    );
    let extension_methods = options.host_capabilities.extension_methods.clone();
    meta.insert(
        "originHostExtensionMethods".into(),
        serde_json::to_value(extension_methods).map_err(op)?,
    );
    Ok(meta)
}

pub(super) fn capabilities_for(options: &RuntimeOptions) -> RuntimeCapabilities {
    const SDK_FEATURES: &[(&str, &str, bool)] = &[
        ("sdk:session-lifecycle", "state", false),
        ("sdk:agent-turns", "agent", false),
        ("sdk:autonomous-runs", "state-agent", false),
        ("sdk:event-journal", "read", false),
        ("sdk:commands", "agent", true),
        ("sdk:scheduler", "background", true),
        ("sdk:workflows", "process", true),
        ("sdk:subagents", "process", true),
        ("sdk:mcp", "network-process", true),
        ("sdk:rewind", "workspace-write", true),
        ("sdk:hooks", "process", true),
        ("sdk:permissions", "policy", true),
    ];
    const OPTIONAL_FEATURES: &[(&str, &str, Option<&str>)] = &[
        ("feature:web_search", "network", None),
        ("feature:web_fetch", "network", None),
        ("feature:memory", "persistent-write", None),
        ("feature:workflows", "process", None),
        ("feature:managed_mcp", "network-process", None),
        ("feature:app_deployment", "network-write", None),
        ("feature:skills", "read", None),
        ("feature:plugins", "process", None),
        ("feature:hooks", "process", None),
        ("feature:lsp", "process", None),
        ("feature:auto_wake", "background", None),
        ("feature:image_generation", "network-write", None),
        ("feature:image_edit", "network-write", None),
        ("feature:video_generation", "network-write", None),
        ("host:filesystem", "host", Some("HostDelegate filesystem")),
        ("host:terminal", "host", Some("HostDelegate terminal")),
        (
            "host:desktop_automation",
            "host",
            Some("HostDelegate extension"),
        ),
    ];
    let mut descriptors = SDK_FEATURES
        .iter()
        .map(|(name, effect, desktop_only)| {
            let enabled = !desktop_only || options.profile == crate::RuntimeProfile::Desktop;
            crate::CapabilityDescriptor {
                namespace: (*name).into(),
                enabled,
                disabled_reason: (!enabled).then(|| "restricted profile".into()),
                effect_class: (*effect).into(),
                host_requirement: None,
            }
        })
        .collect::<Vec<_>>();
    descriptors.push(crate::CapabilityDescriptor {
        namespace: "sdk:in-process-mcp".into(),
        enabled: options.profile == crate::RuntimeProfile::Desktop
            && !options.in_process_mcp_servers.is_empty(),
        disabled_reason: (options.profile != crate::RuntimeProfile::Desktop
            || options.in_process_mcp_servers.is_empty())
        .then(|| {
            if options.profile == crate::RuntimeProfile::Restricted {
                "restricted profile".into()
            } else {
                "no in-process MCP server registered".into()
            }
        }),
        effect_class: "in-process".into(),
        host_requirement: None,
    });
    descriptors.push(crate::CapabilityDescriptor {
        namespace: "sdk:extension-bridge".into(),
        enabled: options.profile == crate::RuntimeProfile::Desktop,
        disabled_reason: (options.profile == crate::RuntimeProfile::Restricted)
            .then(|| "restricted profile".into()),
        effect_class: "extension-defined".into(),
        host_requirement: None,
    });
    descriptors.push(crate::CapabilityDescriptor {
        namespace: "x.ai/models/list".into(),
        enabled: true,
        disabled_reason: None,
        effect_class: "read".into(),
        host_requirement: None,
    });
    descriptors.extend(OPTIONAL_FEATURES.iter().map(|(name, effect, host)| {
        let enabled = match *name {
            "host:filesystem" => {
                options.host_capabilities.fs_read || options.host_capabilities.fs_write
            }
            "host:terminal" => options.host_capabilities.terminal,
            "host:desktop_automation" => !options.host_capabilities.extension_methods.is_empty(),
            "feature:web_search" => {
                options.profile == crate::RuntimeProfile::Desktop
                    && options.services.agents.web_search_model.is_some()
            }
            // These native product paths have no explicit, ambient-free SDK
            // service contract in this checkout. Do not advertise them merely
            // because the generic Desktop extension transport is available.
            "feature:web_fetch" | "feature:memory" | "feature:lsp" => false,
            "feature:managed_mcp" | "feature:app_deployment" => false,
            "feature:image_generation" => {
                options.profile == crate::RuntimeProfile::Desktop
                    && options
                        .services
                        .media
                        .as_ref()
                        .is_some_and(|media| media.image_generation)
            }
            "feature:image_edit" => {
                options.profile == crate::RuntimeProfile::Desktop
                    && options
                        .services
                        .media
                        .as_ref()
                        .is_some_and(|media| media.image_edit)
            }
            "feature:video_generation" => {
                options.profile == crate::RuntimeProfile::Desktop
                    && options
                        .services
                        .media
                        .as_ref()
                        .is_some_and(|media| media.video_generation)
            }
            _ => options.profile == crate::RuntimeProfile::Desktop,
        };
        crate::CapabilityDescriptor {
            namespace: (*name).into(),
            enabled,
            disabled_reason: (!enabled).then(|| {
                if name.starts_with("host:") {
                    "host capability not advertised".into()
                } else if name.starts_with("feature:image") || *name == "feature:video_generation" {
                    if options.profile == crate::RuntimeProfile::Restricted {
                        "restricted profile".into()
                    } else {
                        "media service not configured or operation disabled".into()
                    }
                } else if *name == "feature:web_search" {
                    if options.profile == crate::RuntimeProfile::Restricted {
                        "restricted profile".into()
                    } else {
                        "web-search model service not configured".into()
                    }
                } else if *name == "feature:managed_mcp" {
                    "managed MCP is an account-product service gateway, not a client transport or credential-injection facility; configure explicit MCP transports instead".into()
                } else if *name == "feature:app_deployment" {
                    "App Builder deployment is not implemented in this source checkout".into()
                } else if matches!(*name, "feature:web_fetch" | "feature:memory" | "feature:lsp") {
                    "no explicit ambient-free SDK configuration is available".into()
                } else {
                    "restricted profile".into()
                }
            }),
            effect_class: (*effect).into(),
            host_requirement: host.map(str::to_owned),
        }
    }));
    RuntimeCapabilities {
        profile: options.profile,
        host: options.host_capabilities.clone(),
        features: descriptors,
    }
}
