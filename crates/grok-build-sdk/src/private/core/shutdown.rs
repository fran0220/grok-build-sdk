use super::*;

impl Core {
    pub(super) async fn close(&self, id: SessionId) -> Result<(), Error> {
        self.require_resident(&id)?;
        if self.turns.borrow().contains_key(&id.0) {
            self.cancel(id.clone()).await?;
        }
        let response = match self.agent.close_session(id.0.clone()).await {
            Ok(response) => response,
            Err(error) => {
                self.mark_close_uncertain(&id);
                return Err(protocol("session/close", error));
            }
        };
        let outcome = response
            .meta
            .as_ref()
            .and_then(|meta| meta.get("x.ai/closeOutcome"))
            .and_then(serde_json::Value::as_str);
        if close_outcome_releases_lease(outcome) {
            self.finish_close(&id, true);
            Ok(())
        } else {
            if outcome != Some("superseded") {
                self.mark_close_uncertain(&id);
            }
            Err(Error::Operation(format!(
                "native session close was not positively confirmed (outcome: {})",
                outcome.unwrap_or("missing")
            )))
        }
    }
    pub(super) async fn delete(&self, id: SessionId) -> Result<(), Error> {
        if self.resident.borrow().contains(&id.0) {
            if let Err(error) = self.unload_inner(id.clone(), false).await {
                return Err(error);
            }
        } else if !self.session_leases.borrow().contains_key(&id.0) {
            let lease = self.acquire_session_lease(&id)?;
            if let Some(lease) = lease {
                self.session_leases.borrow_mut().insert(id.0.clone(), lease);
            }
        }
        #[derive(serde::Deserialize)]
        struct DeleteWire {
            success: bool,
        }
        let result = self
            .extension(
                "x.ai/session/delete",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await
            .and_then(|response: DeleteWire| {
                if response.success {
                    Ok(())
                } else {
                    Err(Error::Operation(
                        "native session deletion was not confirmed".into(),
                    ))
                }
            });
        if result.is_ok() {
            self.session_leases.borrow_mut().remove(&id.0);
        }
        result
    }
    pub(super) async fn unload(&self, id: SessionId) -> Result<(), Error> {
        self.unload_inner(id, true).await
    }
    pub(super) async fn unload_inner(
        &self,
        id: SessionId,
        release_lease: bool,
    ) -> Result<(), Error> {
        self.require_resident(&id)?;
        if self.turns.borrow().contains_key(&id.0) {
            self.cancel(id.clone()).await?;
        }
        let response: UnloadWire = self
            .extension(
                "origin/session/unload",
                serde_json::json!({"sessionId":id.0}),
            )
            .await?;
        if !response.success {
            return Err(Error::Operation(
                "native session unload did not detach the actor".into(),
            ));
        }
        if response.drained {
            self.finish_close(&id, release_lease);
            Ok(())
        } else {
            Err(Error::Operation(
                "native session detached but its actor missed the teardown deadline".into(),
            ))
        }
    }
    pub(super) fn finish_close(&self, id: &SessionId, release_lease: bool) {
        self.emit(id, EventUpdate::SessionClosed, None);
        self.resident.borrow_mut().remove(&id.0);
        self.session_bindings.borrow_mut().remove(&id.0);
        if release_lease {
            self.session_leases.borrow_mut().remove(&id.0);
        }
        self.mcp_bindings.revoke_session(&id.0);
        self.turns.borrow_mut().remove(&id.0);
        self.replay.borrow_mut().remove(&id.0);
        xai_grok_shell::origin_runtime::unregister_session_tree(&id.0);
    }

    pub(super) fn mark_close_uncertain(&self, id: &SessionId) {
        self.resident.borrow_mut().remove(&id.0);
        self.session_bindings.borrow_mut().remove(&id.0);
        self.mcp_bindings.revoke_session(&id.0);
        self.turns.borrow_mut().remove(&id.0);
        self.replay.borrow_mut().remove(&id.0);
        // Keep both the authority lease and root registration: either may
        // still cover an actor whose close result was commit-unknown.
    }
}
