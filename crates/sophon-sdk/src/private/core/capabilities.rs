use super::*;

/// Merges the two general-layer sources into one application-owned layer.
/// `RuntimeServices::mcp_servers` predates the layering, so it is folded in
/// here rather than left outside it; a Session can therefore mask a
/// runtime-global mount by name.
pub(in crate::private) fn general_layer(
    options: &RuntimeOptions,
) -> Result<crate::CapabilityLayer, Error> {
    let mut layer = options.general_capabilities.clone();
    for server in &options.services.mcp_servers {
        if layer
            .mcp_service_contributions()
            .iter()
            .any(|existing| existing.name() == server.name())
        {
            return Err(crate::CapabilityError::GeneralCollision(server.name().to_owned()).into());
        }
        layer = layer.mcp_service(server.clone());
    }
    layer.validate()?;
    Ok(layer)
}

impl Core {
    /// Masks the general layer with one Session's layer and rejects anything
    /// the Runtime cannot honor. Validation is fail-closed and happens before
    /// any native dispatch.
    pub(super) fn resolve_capabilities(
        &self,
        session: &crate::CapabilityLayer,
    ) -> Result<ResolvedCapabilities, Error> {
        if self.options.profile == crate::RuntimeProfile::Restricted
            && (!session.is_empty() || !self.options.general_capabilities.is_empty())
        {
            return Err(crate::CapabilityError::RestrictedProfile.into());
        }
        if session.is_empty() && self.general_capabilities.is_empty() {
            return Ok(ResolvedCapabilities::default());
        }
        session.validate()?;
        let resolved = resolve_capabilities(&self.general_capabilities, session);
        for (name, model) in &resolved.agent_services {
            if !self.catalog.contains_key(model) {
                return Err(Error::InvalidConfig(format!(
                    "agent service '{name}' routes to model '{model}', which is not in the fixed catalog"
                )));
            }
        }
        for server in &resolved.mcp_services {
            if self
                .options
                .in_process_mcp_servers
                .iter()
                .any(|embedded| embedded.name == server.name())
            {
                return Err(Error::InvalidConfig(format!(
                    "mcp service '{}' collides with an in-process server",
                    server.name()
                )));
            }
        }
        validate_mcp_servers(&resolved.mcp_services)?;
        Ok(resolved)
    }

    pub(super) fn capability_meta(
        &self,
        resolved: &ResolvedCapabilities,
    ) -> Option<serde_json::Value> {
        if self.options.profile == crate::RuntimeProfile::Restricted {
            return None;
        }
        resolved.session_meta(&self.options.skill_paths)
    }

    /// Binds the layer into the native runtime for a Session that is already
    /// resident. Skills and agent services are rebound in place and take
    /// effect on the next Turn; MCP mounts are replaced atomically.
    pub(super) async fn apply_resident_capabilities(
        &self,
        id: &SessionId,
        resolved: &ResolvedCapabilities,
    ) -> Result<(), Error> {
        xai_grok_shell::origin_runtime::bind_session_capabilities(
            id.as_str(),
            self.capability_meta(resolved).as_ref(),
        );
        let mcp_servers: Vec<EmbeddedMcpServer> = resolved
            .mcp_services
            .iter()
            .map(to_embedded_mcp_server)
            .collect();
        self.extension::<serde_json::Value>(
            "x.ai/session/update_mcp_servers",
            serde_json::json!({"sessionId": id.as_str(), "mcpServers": mcp_servers}),
        )
        .await?;
        self.agent.reload_skills_for_session(&id.0);
        Ok(())
    }

    pub(super) async fn set_session_capabilities(
        &self,
        id: SessionId,
        layer: crate::CapabilityLayer,
    ) -> Result<CapabilityResolution, Error> {
        if !self.resident.borrow().contains(&id.0) {
            return Err(Error::Operation("session is not resident".into()));
        }
        if self.turns.borrow().contains_key(&id.0) {
            return Err(Error::Operation(
                "cannot change capabilities during an active prompt".into(),
            ));
        }
        let resolved = self.resolve_capabilities(&layer)?;
        self.apply_resident_capabilities(&id, &resolved).await?;
        let mut bindings = self.session_bindings.borrow_mut();
        let binding = bindings
            .get_mut(&id.0)
            .ok_or_else(|| Error::Operation("session binding is unavailable".into()))?;
        binding.capabilities = resolved.resolution.clone();
        Ok(resolved.resolution)
    }

    pub(super) fn session_capabilities(
        &self,
        id: &SessionId,
    ) -> Result<CapabilityResolution, Error> {
        self.session_bindings
            .borrow()
            .get(&id.0)
            .map(|binding| binding.capabilities.clone())
            .ok_or_else(|| Error::Operation("session is not resident".into()))
    }
}
