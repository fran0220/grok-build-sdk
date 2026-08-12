use super::*;

impl Core {
    pub(super) async fn cancel(&self, id: SessionId) -> Result<(), Error> {
        let session_id = id.0.clone();
        let result = self
            .agent
            .cancel(id.0)
            .await
            .map_err(|error| protocol("session/cancel", error));
        if let Err(error) = result {
            if let Some(task) = self.prompt_tasks.borrow_mut().remove(&session_id) {
                task.abort();
            }
            self.turns.borrow_mut().remove(&session_id);
            return Err(error);
        }
        if self.turns.borrow().contains_key(&session_id) {
            let drained = tokio::time::timeout(CANCEL_DRAIN_TIMEOUT, async {
                while self.turns.borrow().contains_key(&session_id) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
            if !drained {
                if let Some(task) = self.prompt_tasks.borrow_mut().remove(&session_id) {
                    task.abort();
                }
                self.turns.borrow_mut().remove(&session_id);
                return Err(Error::Operation(
                    "native Grok prompt ignored cancellation and was force-stopped".into(),
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn extension<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        params: serde_json::Value,
    ) -> Result<T, Error> {
        let response = self
            .agent
            .extension(method, params)
            .await
            .map_err(|error| protocol(method, error))?;
        serde_json::from_value(response).map_err(op)
    }
    pub(super) async fn list_models(&self) -> Result<ModelCatalog, Error> {
        #[derive(serde::Deserialize)]
        struct ModelsListResult {
            result: Option<SessionModelState>,
            error: Option<serde_json::Value>,
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct SessionModelState {
            current_model_id: String,
            available_models: Vec<SessionModelInfo>,
            #[serde(rename = "_meta")]
            meta: Option<SessionMeta>,
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct SessionModelInfo {
            model_id: String,
            name: String,
            description: Option<String>,
            #[serde(rename = "_meta")]
            meta: Option<SessionMeta>,
        }

        let response: ModelsListResult = self
            .extension("x.ai/models/list", serde_json::json!({}))
            .await?;
        if let Some(error) = response.error {
            return Err(Error::Operation(format!("models/list failed: {error}")));
        }
        let state = response
            .result
            .ok_or_else(|| Error::Operation("models/list response missing result".into()))?;
        Ok(ModelCatalog {
            current_model_id: state.current_model_id,
            available_models: state
                .available_models
                .into_iter()
                .map(|model| AvailableModel {
                    id: model.model_id,
                    name: model.name,
                    description: model.description,
                    metadata: model.meta,
                })
                .collect(),
            metadata: state.meta,
        })
    }
    pub(super) async fn extension_raw(
        &self,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        if request.method.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "extension request method is required".into(),
            ));
        }
        if self.options.profile == crate::RuntimeProfile::Restricted {
            return Err(Error::Operation(
                "generic extension requests require the Desktop profile".into(),
            ));
        }
        if matches!(
            request.method.as_str(),
            "x.ai/session/delete"
                | "origin/session/unload"
                | "x.ai/session/close"
                | "x.ai/session/fork"
        ) || self.session_state_store.is_some()
            && request.method == "x.ai/git/worktree/resume_session"
        {
            return Err(Error::Operation(
                "Session lifecycle methods require their typed Runtime operation".into(),
            ));
        }
        self.extension_raw_unchecked(request).await
    }
    pub(super) async fn extension_raw_unchecked(
        &self,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        let response = self
            .agent
            .extension(&request.method, request.params)
            .await
            .map_err(|e| protocol(&request.method, e))?;
        Ok(ExtensionResponse { result: response })
    }
    pub(super) async fn fork(
        &self,
        source: SessionId,
        target: SessionId,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        if source == target {
            return Err(Error::InvalidConfig(
                "fork source and target must be different".into(),
            ));
        }
        // The store leases are fail-fast. Acquire absent identities in stable
        // full-id order; a resident source's lease is borrowed, never removed.
        let source_resident = self.resident.borrow().contains(source.as_str());
        let source_held = self.session_leases.borrow().contains_key(source.as_str());
        if self.session_state_store.is_some() {
            match (source_resident, source_held) {
                (true, false) => {
                    return Err(Error::Operation(
                        "resident fork source is missing its authority lease".into(),
                    ));
                }
                (false, true) => {
                    return Err(Error::Operation(
                        "fork source teardown is uncertain; its authority lease remains retained"
                            .into(),
                    ));
                }
                _ => {}
            }
        }
        let mut ids = vec![target.clone()];
        if !source_resident {
            ids.push(source);
        }
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        let mut _leases = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(lease) = self.acquire_session_lease(&id)? {
                _leases.push(lease);
            }
        }
        self.extension_raw_unchecked(request).await
    }
    pub(super) async fn extension_notification(
        &self,
        request: ExtensionNotification,
    ) -> Result<(), Error> {
        if request.method.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "extension notification method is required".into(),
            ));
        }
        if self.options.profile == crate::RuntimeProfile::Restricted {
            return Err(Error::Operation(
                "generic extension notifications require the Desktop profile".into(),
            ));
        }
        self.agent
            .extension_notification(&request.method, request.params)
            .await
            .map_err(|error| protocol(&request.method, error))
    }
    pub(super) async fn set_mode(&self, id: SessionId, mode: String) -> Result<(), Error> {
        self.agent
            .set_session_mode(id.0, mode)
            .await
            .map(|_| ())
            .map_err(|e| protocol("session/set_mode", e))
    }
    pub(super) async fn list_sessions(&self) -> Result<serde_json::Value, Error> {
        self.agent
            .list_sessions()
            .await
            .map_err(|e| protocol("session/list", e))
    }
    pub(super) async fn set_route(
        &self,
        id: SessionId,
        model: String,
        reasoning: Option<String>,
    ) -> Result<(), Error> {
        let effective_reasoning = self.effective_reasoning(&model, reasoning.as_deref())?;
        if self.turns.borrow().contains_key(&id.0) {
            return Err(Error::Operation(
                "cannot change model during an active prompt".into(),
            ));
        }
        self.apply_native_route(&id, &model, effective_reasoning.as_deref())
            .await?;
        let mut bindings = self.session_bindings.borrow_mut();
        let binding = bindings
            .get_mut(&id.0)
            .ok_or_else(|| Error::Operation("session binding is unavailable".into()))?;
        binding.model = model;
        binding.reasoning = effective_reasoning;
        Ok(())
    }
}
