use super::*;

impl Core {
    pub(super) fn check_model(&self, model_id: &str, reasoning: Option<&str>) -> Result<(), Error> {
        let model = self.catalog.get(model_id).ok_or_else(|| {
            Error::InvalidConfig(format!("model '{}' is not in the fixed catalog", model_id))
        })?;
        if let Some(reasoning) = reasoning
            && (!model.supports_reasoning
                || !model
                    .reasoning_options
                    .iter()
                    .any(|option| option == reasoning))
        {
            return Err(Error::InvalidConfig(format!(
                "reasoning effort '{reasoning}' is not available for model '{}'",
                model_id
            )));
        }
        Ok(())
    }
    pub(super) fn effective_reasoning(
        &self,
        model_id: &str,
        reasoning: Option<&str>,
    ) -> Result<Option<String>, Error> {
        self.check_model(model_id, reasoning)?;
        Ok(reasoning.map(str::to_owned).or_else(|| {
            self.catalog
                .get(model_id)
                .and_then(|model| model.default_reasoning.clone())
        }))
    }
    pub(super) fn check(&self, config: &SessionConfig) -> Result<(), Error> {
        self.check_model(&config.model, config.reasoning.as_deref())?;
        if !config.cwd.is_absolute() || !config.cwd.is_dir() {
            return Err(Error::InvalidConfig(
                "session cwd must be an existing absolute directory".into(),
            ));
        }
        for (name, value) in [
            ("system prompt", config.system_prompt.as_deref()),
            ("rules", config.rules.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                return Err(Error::InvalidConfig(format!(
                    "session {name} must not be blank"
                )));
            }
        }
        Ok(())
    }
    pub(super) fn session_meta(
        &self,
        config: &SessionConfig,
        effective_reasoning: Option<&str>,
        capabilities: &ResolvedCapabilities,
    ) -> Result<SessionMeta, Error> {
        let mut meta = serde_json::json!({
            "modelId": config.model,
            "reasoningEffort": effective_reasoning,
            "clientIdentifier": self.options.client_identifier,
            "yoloMode": self.options.yolo_mode,
        })
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Operation("failed to build session metadata".into()))?;
        if let Some(system_prompt) = &config.system_prompt {
            meta.insert(
                "systemPromptOverride".into(),
                serde_json::Value::String(system_prompt.clone()),
            );
        }
        if let Some(rules) = &config.rules {
            meta.insert("rules".into(), serde_json::Value::String(rules.clone()));
        }
        if let Some(value) = self.capability_meta(capabilities) {
            meta.insert("x.ai/sessionCapabilities".into(), value);
        }
        if self.options.profile == crate::RuntimeProfile::Desktop
            && !self.options.agent_hooks.is_empty()
        {
            let mut groups = serde_json::Map::new();
            for hook in &self.options.agent_hooks {
                groups
                    .entry(hook.event.registration_name())
                    .or_insert_with(|| serde_json::Value::Array(Vec::new()))
                    .as_array_mut()
                    .expect("hook groups are arrays")
                    .push(serde_json::json!({
                        "matcher": hook.matcher,
                        "hookCallbackIds": [hook.callback_id.clone()],
                        "timeout": hook.timeout,
                    }));
            }
            meta.insert("x.ai/hooks".into(), serde_json::Value::Object(groups));
        }
        Ok(meta)
    }

    pub(super) async fn apply_native_route(
        &self,
        id: &SessionId,
        model: &str,
        effective_reasoning: Option<&str>,
    ) -> Result<(), Error> {
        let meta = serde_json::json!({
            "reasoningEffort": effective_reasoning,
            "originRouteOnly": true,
        })
        .as_object()
        .cloned();
        self.agent
            .set_session_model(
                acp::SetSessionModelRequest::new(
                    acp::SessionId::new(id.0.clone()),
                    acp::ModelId::new(model.to_owned()),
                )
                .meta(meta),
            )
            .await
            .map(|_| ())
            .map_err(|error| protocol("session/set_model", error))
    }

    /// The Session's effective MCP mounts: the general layer masked by this
    /// Session's own layer. Restricted runtimes mount nothing.
    pub(super) fn mcp_servers_for(
        &self,
        capabilities: &ResolvedCapabilities,
    ) -> Vec<acp::McpServer> {
        if self.options.profile == crate::RuntimeProfile::Restricted {
            return Vec::new();
        }
        capabilities
            .mcp_services
            .iter()
            .map(to_acp_mcp_server)
            .collect()
    }
    pub(super) fn emit(&self, id: &SessionId, u: EventUpdate, t: Option<String>) {
        let event = self.retain_event(id, u, t);
        self.publish_event(event);
    }

    pub(super) fn retain_event(&self, id: &SessionId, u: EventUpdate, t: Option<String>) -> Event {
        let mut s = self.sequences.borrow_mut();
        let n = s.entry(id.0.clone()).or_default();
        *n += 1;
        let event = Event {
            session_id: id.clone(),
            sequence: *n,
            turn_id: t,
            timestamp_ms: now_ms(),
            replay: false,
            update: u,
        };
        let mut journal = self.retained.borrow_mut();
        let retained = journal.entry(id.0.clone()).or_default();
        retained.push_back(event.clone());
        while retained.len() > self.capacity {
            retained.pop_front();
        }
        event
    }

    pub(super) fn publish_event(&self, event: Event) {
        let _ = self.events.send(event);
    }

    pub(super) async fn detach_unregistered_session(&self, id: &SessionId) -> Result<(), Error> {
        let response = self
            .extension::<UnloadWire>(
                "origin/session/unload",
                serde_json::json!({"sessionId": id.0}),
            )
            .await?;
        if response.success && response.drained {
            Ok(())
        } else {
            Err(Error::Operation(
                "native session cleanup did not fully drain the actor".into(),
            ))
        }
    }

    pub(super) async fn create(
        &self,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        layer: crate::CapabilityLayer,
    ) -> Result<SessionId, Error> {
        self.check(&config)?;
        let capabilities = self.resolve_capabilities(&layer)?;
        if self.session_state_store.is_some() {
            let id = SessionId(uuid::Uuid::now_v7().to_string());
            let generation = uuid::Uuid::now_v7().to_string();
            let lease = self.acquire_session_lease(&id)?;
            self.create_inner(
                config,
                harness_digest,
                capabilities,
                Some((id, generation)),
                lease,
            )
            .await
        } else {
            self.create_inner(config, harness_digest, capabilities, None, None)
                .await
        }
    }

    pub(super) fn acquire_session_lease(
        &self,
        id: &SessionId,
    ) -> Result<Option<Box<dyn crate::SessionStateLease>>, Error> {
        self.session_state_store
            .as_ref()
            .map(|store| {
                let key = crate::SessionKey::new(id.as_str()).map_err(op)?;
                store.acquire_session_lease(&key).map_err(op)
            })
            .transpose()
    }

    pub(super) async fn ensure(
        &self,
        id: SessionId,
        config: SessionConfig,
    ) -> Result<SessionId, Error> {
        use sha2::{Digest as _, Sha256};
        self.check(&config)?;
        uuid::Uuid::try_parse(id.as_str()).map_err(|error| {
            Error::InvalidConfig(format!(
                "caller-selected session id must be a UUID: {error}"
            ))
        })?;
        let already_resident = self.resident.borrow().contains(id.as_str());
        let lease = if already_resident {
            None
        } else {
            self.acquire_session_lease(&id)?
        };
        let authority = self.session_state_authority.as_ref().ok_or_else(|| {
            Error::InvalidConfig(
                "create_session_with_id requires a Host session state authority".into(),
            )
        })?;
        let exact = serde_json::to_vec(&config).map_err(op)?;
        let generation = format!("config-sha256:{:x}", Sha256::digest(exact));
        match authority.inspect(id.as_str()).map_err(op)? {
            xai_grok_shell::session::state_authority::SessionInspection::Vacant => {
                let capabilities = self.resolve_capabilities(&crate::CapabilityLayer::default())?;
                self.create_inner(config, None, capabilities, Some((id, generation)), lease)
                    .await
            }
            xai_grok_shell::session::state_authority::SessionInspection::Live {
                generation: current,
            } if current == generation => {
                if !already_resident {
                    self.attach_with_lease(
                        id.clone(),
                        config,
                        None,
                        crate::CapabilityLayer::default(),
                        false,
                        lease,
                    )
                    .await?;
                }
                Ok(id)
            }
            xai_grok_shell::session::state_authority::SessionInspection::Live { .. } => {
                Err(Error::InvalidConfig(
                    "session identity already exists with different config".into(),
                ))
            }
            xai_grok_shell::session::state_authority::SessionInspection::Tombstoned { .. } => Err(
                Error::InvalidConfig("session identity is permanently tombstoned".into()),
            ),
        }
    }

    pub(super) async fn create_inner(
        &self,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        capabilities: ResolvedCapabilities,
        requested: Option<(SessionId, String)>,
        lease: Option<Box<dyn crate::SessionStateLease>>,
    ) -> Result<SessionId, Error> {
        self.check(&config)?;
        let lease_id = requested.as_ref().map(|(id, _)| id).cloned();
        let mut lease_admission = lease_id
            .as_ref()
            .map(|id| SessionLeaseAdmission::new(&self.session_leases, id, lease));
        let effective_reasoning =
            self.effective_reasoning(&config.model, config.reasoning.as_deref())?;
        let binding = ResidentSessionBinding::new(
            &config,
            effective_reasoning.clone(),
            harness_digest,
            capabilities.resolution.clone(),
        );
        let mut meta = self.session_meta(&config, effective_reasoning.as_deref(), &capabilities)?;
        if let Some((id, generation)) = &requested {
            meta.insert("sessionId".into(), serde_json::Value::String(id.0.clone()));
            meta.insert(
                "sessionStateGeneration".into(),
                serde_json::Value::String(generation.clone()),
            );
        }
        if let Some(admission) = &mut lease_admission {
            admission.dispatch_uncertain();
        }
        let x = self
            .agent
            .new_session(
                acp::NewSessionRequest::new(config.cwd.clone())
                    .mcp_servers(self.mcp_servers_for(&capabilities))
                    .meta(meta),
            )
            .await
            .map_err(|error| protocol("session/new", error))?;
        let id = SessionId(x.session_id.0.to_string());
        // `session/new` selects the catalog model but historically does not
        // consume its reasoning override. Apply the same normalized route
        // before exposing the Session so native sampling and receipts agree.
        if let Err(error) = self
            .apply_native_route(&id, &config.model, effective_reasoning.as_deref())
            .await
        {
            return match self.detach_unregistered_session(&id).await {
                Ok(()) => {
                    if let Some(admission) = &mut lease_admission {
                        admission.release();
                    }
                    Err(error)
                }
                Err(cleanup_error) => Err(Error::Operation(format!(
                    "{error}; native session cleanup failed: {cleanup_error}"
                ))),
            };
        }
        let active_guard = ActiveMcpBindingGuard::new(self.mcp_bindings.clone(), id.0.clone());
        if let Err(error) = self.save_ledger(&id, &SessionLedger::default()) {
            return match self.detach_unregistered_session(&id).await {
                Ok(()) => {
                    if let Some(admission) = &mut lease_admission {
                        admission.release();
                    }
                    Err(error)
                }
                Err(cleanup_error) => Err(Error::Operation(format!(
                    "{error}; native session cleanup failed: {cleanup_error}"
                ))),
            };
        }
        if !xai_grok_shell::origin_runtime::register_root_session(&id.0) {
            let cleanup = self.detach_unregistered_session(&id).await;
            let mut detail =
                "native session identity collided with an existing embedded root".to_owned();
            match cleanup {
                Ok(()) => {
                    if let Some(admission) = &mut lease_admission {
                        admission.release();
                    }
                }
                Err(error) => {
                    detail.push_str(&format!("; native session cleanup failed: {error}"));
                }
            }
            return Err(Error::Operation(detail));
        }
        self.resident.borrow_mut().insert(id.0.clone());
        self.session_bindings
            .borrow_mut()
            .insert(id.0.clone(), binding);
        active_guard.commit();
        if let Some(admission) = &mut lease_admission {
            admission.commit_resident();
        }
        self.emit(&id, EventUpdate::SessionStarted, None);
        Ok(id)
    }
    pub(super) async fn load(
        &self,
        id: SessionId,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        layer: crate::CapabilityLayer,
    ) -> Result<(), Error> {
        self.attach(id, config, harness_digest, layer, false).await
    }

    pub(super) async fn resume(
        &self,
        id: SessionId,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        layer: crate::CapabilityLayer,
    ) -> Result<(), Error> {
        self.attach(id, config, harness_digest, layer, true).await
    }

    pub(super) async fn attach(
        &self,
        id: SessionId,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        layer: crate::CapabilityLayer,
        resume: bool,
    ) -> Result<(), Error> {
        self.check(&config)?;
        let lease = self.acquire_session_lease(&id)?;
        self.attach_with_lease(id, config, harness_digest, layer, resume, lease)
            .await
    }

    pub(super) async fn attach_with_lease(
        &self,
        id: SessionId,
        config: SessionConfig,
        harness_digest: Option<HarnessDigest>,
        layer: crate::CapabilityLayer,
        resume: bool,
        lease: Option<Box<dyn crate::SessionStateLease>>,
    ) -> Result<(), Error> {
        self.check(&config)?;
        let capabilities = self.resolve_capabilities(&layer)?;
        let mut lease_admission = SessionLeaseAdmission::new(&self.session_leases, &id, lease);
        if self.resident.borrow().contains(&id.0) {
            return Err(Error::Operation("session is already resident".into()));
        }
        self.load_ledger(&id)?;
        let effective_reasoning =
            self.effective_reasoning(&config.model, config.reasoning.as_deref())?;
        let binding = ResidentSessionBinding::new(
            &config,
            effective_reasoning.clone(),
            harness_digest,
            capabilities.resolution.clone(),
        );
        let active_guard = ActiveMcpBindingGuard::new(self.mcp_bindings.clone(), id.0.clone());
        let meta = self.session_meta(&config, effective_reasoning.as_deref(), &capabilities)?;
        struct ReplayGuard<'a>(&'a RefCell<HashMap<String, ReplayMode>>, String);
        impl Drop for ReplayGuard<'_> {
            fn drop(&mut self) {
                self.0.borrow_mut().remove(&self.1);
            }
        }
        let capture_replay = !resume && !self.sequences.borrow().contains_key(&id.0);
        self.replay.borrow_mut().insert(
            id.0.clone(),
            if capture_replay {
                ReplayMode::Capture
            } else {
                ReplayMode::Suppress
            },
        );
        let _guard = ReplayGuard(&self.replay, id.0.clone());
        lease_admission.dispatch_uncertain();
        if resume {
            self.agent
                .resume_session(
                    acp::ResumeSessionRequest::new(acp::SessionId::new(id.0.clone()), config.cwd)
                        .mcp_servers(self.mcp_servers_for(&capabilities))
                        .meta(meta),
                )
                .await
                .map_err(|error| protocol("session/resume", error))?;
        } else {
            self.agent
                .load_session(
                    acp::LoadSessionRequest::new(acp::SessionId::new(id.0.clone()), config.cwd)
                        .mcp_servers(self.mcp_servers_for(&capabilities))
                        .meta(meta),
                )
                .await
                .map_err(|error| protocol("session/load", error))?;
        }
        if !xai_grok_shell::origin_runtime::register_root_session(&id.0) {
            return match self.detach_unregistered_session(&id).await {
                Ok(()) => {
                    lease_admission.release();
                    Err(Error::Operation(
                        "loaded session identity collided with an existing embedded root".into(),
                    ))
                }
                Err(cleanup_error) => Err(Error::Operation(format!(
                    "loaded session identity collided with an existing embedded root; native session cleanup failed: {cleanup_error}"
                ))),
            };
        }
        self.resident.borrow_mut().insert(id.0.clone());
        self.session_bindings
            .borrow_mut()
            .insert(id.0.clone(), binding);
        active_guard.commit();
        lease_admission.commit_resident();
        Ok(())
    }
}
