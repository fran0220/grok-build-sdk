use super::*;

impl Core {
    pub(in crate::private) async fn start(
        input: RuntimeConfig,
        options: RuntimeOptions,
        events: mpsc::UnboundedSender<Event>,
        evidence_store: Arc<dyn SessionEvidenceStore>,
        session_state_store: Option<Arc<dyn crate::SessionStateStore>>,
        compaction_observer: Option<Arc<dyn crate::CompactionObserver>>,
    ) -> Result<(Self, RuntimeCapabilities), Error> {
        std::fs::create_dir_all(&input.grok_home).map_err(op)?;
        std::fs::create_dir_all(&input.session_storage).map_err(op)?;
        let mut cfg = match options.profile {
            crate::RuntimeProfile::Restricted => Config::origin_embedded(),
            crate::RuntimeProfile::Desktop => Config::origin_desktop(),
        };
        if options.profile == crate::RuntimeProfile::Desktop {
            cfg.skills.paths = options
                .skill_paths
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect();
            cfg.plugins.cli_plugin_dirs = options.plugin_paths.clone();
        } else {
            cfg.skills.paths.clear();
            cfg.plugins.cli_plugin_dirs.clear();
        }
        let fallback_endpoint = if input.endpoint.trim().is_empty() {
            options
                .services
                .model_providers
                .values()
                .next()
                .expect("validated model provider")
                .base_url
                .clone()
        } else {
            input.endpoint.clone()
        };
        cfg.endpoints.cli_chat_proxy_base_url = Some(fallback_endpoint.clone());
        cfg.endpoints.xai_api_base_url = fallback_endpoint;
        cfg.endpoints.models_base_url = None;
        cfg.endpoints.models_list_url = None;
        cfg.default_model_override = input.models.first().map(|model| model.id.clone());
        cfg.subagent_model_overrides = options
            .services
            .agents
            .subagent_models
            .clone()
            .into_iter()
            .collect();
        let auxiliary_model_slug = |model_id: &str| {
            options
                .services
                .model_providers
                .get(model_id)
                .and_then(|provider| provider.model.as_deref())
                .unwrap_or(model_id)
                .to_owned()
        };
        if let Some(model) = &options.services.agents.web_search_model {
            cfg.web_search_model = auxiliary_model_slug(model);
        } else {
            // Embedded runtimes never fall through to the shell's hidden
            // first-party search model, which can consult ambient credentials.
            cfg.disable_web_search = true;
        }
        cfg.session_summary_model = options
            .services
            .agents
            .session_summary_model
            .as_deref()
            .map(&auxiliary_model_slug);
        cfg.image_description_model = options
            .services
            .agents
            .image_description_model
            .as_deref()
            .map(&auxiliary_model_slug);
        cfg.transcribe_user_images = options.services.agents.image_description_model.is_some();
        if let Some(model) = &options.services.agents.prompt_suggestion_model {
            cfg.prompt_suggest_model_pin =
                xai_grok_shell::config::PromptSuggestModelPin::Pinned(auxiliary_model_slug(model));
        }
        if options.profile == crate::RuntimeProfile::Desktop {
            cfg.origin_media = options
                .services
                .media
                .as_ref()
                .map(|media| OriginMediaConfig {
                    api_key: media.provider.api_key.clone(),
                    base_url: media.provider.base_url.clone(),
                    extra_headers: media.provider.headers.clone().into_iter().collect(),
                    query_params: media.provider.query_params.clone().into_iter().collect(),
                    image_gen_enabled: media.image_generation,
                    image_edit_enabled: media.image_edit,
                    video_gen_enabled: media.video_generation,
                    image_gen_model: media.image_generation_model.clone(),
                    image_edit_model: media.image_edit_model.clone(),
                    image_to_video_model: media.image_to_video_model.clone(),
                    reference_to_video_model: media.reference_to_video_model.clone(),
                });
        }
        let auth = Arc::new(AuthManager::new_origin_embedded(
            input.grok_home.join("origin-auth-disabled.json"),
            cfg.grok_com_config.clone(),
        ));
        let mut fixed = IndexMap::new();
        for model in &input.models {
            let provider = options.services.model_providers.get(&model.id);
            let base_url = provider
                .map(|provider| provider.base_url.as_str())
                .unwrap_or(input.endpoint.as_str());
            let api_key = provider
                .map(|provider| provider.api_key.as_str())
                .unwrap_or(input.api_key.as_str());
            let provider_model = provider
                .and_then(|provider| provider.model.as_deref())
                .unwrap_or(model.id.as_str());
            let extra_headers = provider
                .map(|provider| provider.headers.clone())
                .unwrap_or_default();
            let query_params = provider
                .map(|provider| provider.query_params.clone())
                .unwrap_or_default();
            let api_backend = provider.map_or_else(
                || match model.api_backend {
                    crate::ApiBackend::ChatCompletions => "chat_completions",
                    crate::ApiBackend::Responses => "responses",
                },
                |provider| provider.protocol.api_backend(),
            );
            let auth_scheme = provider
                .map(|provider| provider.protocol.auth_scheme())
                .unwrap_or("bearer");
            let entry: ModelEntryConfig = serde_json::from_value(serde_json::json!({
                "id": model.id,
                "model": provider_model,
                "model_family": model.model_family,
                "base_url": base_url,
                "api_key": api_key,
                "extra_headers": extra_headers,
                "query_params": query_params,
                "context_window": model.context_window,
                "api_backend": api_backend,
                "auth_scheme": auth_scheme,
                "agent_type": "grok-build",
                "reasoning_effort": model.default_reasoning,
                "supports_reasoning_effort": model.supports_reasoning,
                "reasoning_efforts": model.reasoning_options,
            }))
            .map_err(op)?;
            fixed.insert(model.id.clone(), ModelEntry::from_config_entry(&entry));
        }
        let models = ModelsManager::from_origin_fixed(fixed, auth.clone(), cfg.clone())
            .map_err(Error::Operation)?;
        let profile = match options.profile {
            crate::RuntimeProfile::Restricted => {
                xai_grok_shell::agent::config::OriginEmbeddedProfile::Restricted
            }
            crate::RuntimeProfile::Desktop => {
                xai_grok_shell::agent::config::OriginEmbeddedProfile::Desktop
            }
        };
        let compaction_correlations: CompactionCorrelationMap =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let session_state_authority: Option<Arc<ShellAuthority>> =
            session_state_store.as_ref().map(|store| {
                Arc::new(SessionStateAuthorityBridge {
                    store: store.clone(),
                    observer: compaction_observer.clone(),
                    correlations: compaction_correlations.clone(),
                }) as Arc<ShellAuthority>
            });
        static NEXT_RUNTIME_INSTANCE: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let runtime_instance_id = NEXT_RUNTIME_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let mcp_bindings = Arc::new(McpBindingRegistry::default());
        let sequences = Rc::new(RefCell::new(HashMap::new()));
        let retained = Rc::new(RefCell::new(HashMap::new()));
        let turns = Rc::new(RefCell::new(HashMap::new()));
        let turn_usages = Rc::new(RefCell::new(HashMap::new()));
        let replay = Rc::new(RefCell::new(HashMap::new()));
        let client = Client {
            events: events.clone(),
            sequences: sequences.clone(),
            retained: retained.clone(),
            capacity: options.event_journal_capacity,
            host: options.host.clone(),
            tool_permission_handler: if options.profile == crate::RuntimeProfile::Desktop {
                options.tool_permission_handler.clone()
            } else {
                None
            },
            host_extension_methods: options
                .host_capabilities
                .extension_methods
                .iter()
                .cloned()
                .collect(),
            agent_hooks: if options.profile == crate::RuntimeProfile::Desktop {
                options
                    .agent_hooks
                    .iter()
                    .map(|hook| (hook.callback_id.clone(), hook.handler.clone()))
                    .collect()
            } else {
                HashMap::new()
            },
            turns: turns.clone(),
            turn_usages: turn_usages.clone(),
            replay: replay.clone(),
        };
        let agent = Rc::new(EmbeddedAgent::new(
            &cfg,
            auth,
            models,
            input.session_storage.clone(),
            profile,
            session_state_authority.clone(),
            Rc::new(client),
        ));
        if options.profile == crate::RuntimeProfile::Desktop
            && (!options.in_process_mcp_servers.is_empty()
                || !options.session_in_process_mcp_servers.is_empty()
                || !options.mcp_host_services.is_empty())
        {
            let servers = options
                .in_process_mcp_servers
                .iter()
                .chain(options.session_in_process_mcp_servers.iter())
                .map(|server| EmbeddedMcpRegistration {
                    name: server.name.clone(),
                    server_id: server.server_id.clone(),
                })
                .collect();
            let handlers = options
                .in_process_mcp_servers
                .iter()
                .chain(options.session_in_process_mcp_servers.iter())
                .map(|server| {
                    (
                        server.server_id.clone(),
                        (server.name.clone(), server.handler.clone()),
                    )
                })
                .collect();
            agent.set_embedded_mcp_servers(
                servers,
                Arc::new(DirectMcpInvoker {
                    runtime_instance_id,
                    handlers,
                    bindings: mcp_bindings.clone(),
                    host_services: options.mcp_host_services.clone(),
                }),
            );
        }
        // Advertising host I/O prevents the shell from falling back to direct
        // process-local filesystem and terminal implementations. Restricted
        // intentionally advertises those routes without a delegate so every
        // operation fails closed; Desktop either uses the host-advertised
        // routes or deliberately retains the native local implementations.
        let restricted_io = options.profile == crate::RuntimeProfile::Restricted;
        agent
            .initialize(xai_grok_shell::embedded::EmbeddedClientCapabilities {
                fs_read: restricted_io || options.host_capabilities.fs_read,
                fs_write: restricted_io || options.host_capabilities.fs_write,
                terminal: restricted_io || options.host_capabilities.terminal,
                meta: client_capability_meta(&options)?,
            })
            .await
            .map_err(|error| protocol("initialize", error))?;
        let capabilities = capabilities_for(&options);
        let general_capabilities = capabilities::general_layer(&options)?;
        Ok((
            Self {
                agent,
                session_state_authority,
                session_state_store,
                session_leases: RefCell::new(HashMap::new()),
                events,
                catalog: input
                    .models
                    .into_iter()
                    .map(|model| (model.id.clone(), model))
                    .collect(),
                sequences,
                retained,
                capacity: options.event_journal_capacity,
                options,
                general_capabilities,
                resident: RefCell::new(HashSet::new()),
                session_bindings: RefCell::new(HashMap::new()),
                mcp_bindings,
                turns,
                turn_usages,
                prompt_tasks: RefCell::new(HashMap::new()),
                replay,
                evidence_store,
                evidence_versions: RefCell::new(HashMap::new()),
                compaction_correlations,
            },
            capabilities,
        ))
    }
}
