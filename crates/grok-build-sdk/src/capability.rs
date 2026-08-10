// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Session-scoped capability layering.
//!
//! One application-owned Runtime serves every Session. Capabilities are
//! contributed by exactly two layers: the **general layer**, configured once
//! on [`RuntimeBuilder`](crate::RuntimeBuilder) and owned by the embedding
//! application, and a **session layer**, bound to one Session at create, load
//! or resume time and replaceable between Turns. A session contribution masks
//! a general contribution of the same kind and name for that Session only;
//! removing it restores the general one. Nothing here starts a second Runtime,
//! registers a runtime, or requires a restart.

use crate::*;

/// Maximum entries one layer may contribute for one capability kind.
pub const MAX_CAPABILITY_LAYER_ENTRIES: usize = 64;
/// Maximum byte length of one capability name.
pub const MAX_CAPABILITY_NAME_BYTES: usize = 128;

/// The kinds of capability a layer can contribute.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    Skill,
    McpService,
    AgentService,
}

impl CapabilityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skill => "skill",
            Self::McpService => "mcp service",
            Self::AgentService => "agent service",
        }
    }
}

impl std::fmt::Display for CapabilityKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Which layer supplied an effective capability.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityOrigin {
    /// Application-owned: built-ins, general skills, general services.
    General,
    /// Contributed by this Session's own layer.
    Session,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CapabilityError {
    #[error(
        "capability name '{0}' must be 1..={MAX_CAPABILITY_NAME_BYTES} bytes of ASCII letters, digits, '-', '_' or '.' and start with a letter or digit"
    )]
    InvalidName(String),
    #[error("one layer contributes the {kind} '{name}' more than once")]
    Duplicate { kind: CapabilityKind, name: String },
    #[error("one layer contributes {actual} {kind} entries; the maximum is {maximum}")]
    TooManyEntries {
        kind: CapabilityKind,
        actual: usize,
        maximum: usize,
    },
    #[error("skill '{name}' must declare an absolute root directory")]
    RelativeSkillRoot { name: String },
    #[error("agent service '{name}' must name a model")]
    MissingModel { name: String },
    #[error("the general layer and the runtime services both contribute the mcp service '{0}'")]
    GeneralCollision(String),
    #[error("capability layers require the Desktop profile")]
    RestrictedProfile,
}

impl From<CapabilityError> for Error {
    fn from(error: CapabilityError) -> Self {
        Self::InvalidConfig(error.to_string())
    }
}

pub(crate) fn validate_capability_name(value: &str) -> Result<(), CapabilityError> {
    let invalid = value.is_empty()
        || value.len() > MAX_CAPABILITY_NAME_BYTES
        || !value.is_char_boundary(0)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || !value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric());
    if invalid {
        return Err(CapabilityError::InvalidName(value.to_owned()));
    }
    Ok(())
}

/// One named skill root contributed by a layer. The root is a directory the
/// agent walks for `SKILL.md` definitions; the name is the layering identity,
/// not the on-disk skill name.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SkillContribution {
    name: String,
    root: PathBuf,
}

impl SkillContribution {
    pub fn new(name: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            root: root.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    fn validate(&self) -> Result<(), CapabilityError> {
        validate_capability_name(&self.name)?;
        if !self.root.is_absolute() {
            return Err(CapabilityError::RelativeSkillRoot {
                name: self.name.clone(),
            });
        }
        Ok(())
    }
}

/// One named agent-service route contributed by a layer. `model` must exist in
/// [`RuntimeConfig::models`](crate::RuntimeConfig); the Session's subagent and
/// auxiliary calls for `name` are sampled from it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentServiceContribution {
    name: String,
    model: String,
}

impl AgentServiceContribution {
    pub fn new(name: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            model: model.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    fn validate(&self) -> Result<(), CapabilityError> {
        validate_capability_name(&self.name)?;
        if self.model.trim().is_empty() {
            return Err(CapabilityError::MissingModel {
                name: self.name.clone(),
            });
        }
        Ok(())
    }
}

/// One layer's complete capability contribution.
///
/// Deliberately neither `Debug` nor `Serialize`: MCP mounts carry transport
/// headers and stdio environment values that must not leak into logs or
/// exports. Hosts may deserialize a layer from their own configuration.
#[derive(Clone, Default, PartialEq, serde::Deserialize)]
pub struct CapabilityLayer {
    #[serde(default)]
    skills: Vec<SkillContribution>,
    #[serde(default)]
    mcp_services: Vec<McpServerConfig>,
    #[serde(default)]
    agent_services: Vec<AgentServiceContribution>,
}

impl CapabilityLayer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn skill(mut self, value: SkillContribution) -> Self {
        self.skills.push(value);
        self
    }

    pub fn skills(mut self, value: impl IntoIterator<Item = SkillContribution>) -> Self {
        self.skills.extend(value);
        self
    }

    pub fn mcp_service(mut self, value: McpServerConfig) -> Self {
        self.mcp_services.push(value);
        self
    }

    pub fn mcp_services(mut self, value: impl IntoIterator<Item = McpServerConfig>) -> Self {
        self.mcp_services.extend(value);
        self
    }

    pub fn agent_service(mut self, value: AgentServiceContribution) -> Self {
        self.agent_services.push(value);
        self
    }

    pub fn agent_services(
        mut self,
        value: impl IntoIterator<Item = AgentServiceContribution>,
    ) -> Self {
        self.agent_services.extend(value);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.skills.is_empty() && self.mcp_services.is_empty() && self.agent_services.is_empty()
    }

    pub fn skill_contributions(&self) -> &[SkillContribution] {
        &self.skills
    }

    pub fn mcp_service_contributions(&self) -> &[McpServerConfig] {
        &self.mcp_services
    }

    pub fn agent_service_contributions(&self) -> &[AgentServiceContribution] {
        &self.agent_services
    }

    /// Fails closed on an invalid name, a duplicate name within this layer,
    /// a relative skill root, a routeless agent service, or an oversized set.
    pub fn validate(&self) -> Result<(), CapabilityError> {
        bound(CapabilityKind::Skill, self.skills.len())?;
        bound(CapabilityKind::McpService, self.mcp_services.len())?;
        bound(CapabilityKind::AgentService, self.agent_services.len())?;
        for skill in &self.skills {
            skill.validate()?;
        }
        for service in &self.mcp_services {
            validate_capability_name(service.name())?;
        }
        for service in &self.agent_services {
            service.validate()?;
        }
        reject_duplicates(CapabilityKind::Skill, self.skills.iter().map(|s| s.name()))?;
        reject_duplicates(
            CapabilityKind::McpService,
            self.mcp_services.iter().map(McpServerConfig::name),
        )?;
        reject_duplicates(
            CapabilityKind::AgentService,
            self.agent_services.iter().map(|s| s.name()),
        )
    }
}

fn bound(kind: CapabilityKind, actual: usize) -> Result<(), CapabilityError> {
    if actual > MAX_CAPABILITY_LAYER_ENTRIES {
        return Err(CapabilityError::TooManyEntries {
            kind,
            actual,
            maximum: MAX_CAPABILITY_LAYER_ENTRIES,
        });
    }
    Ok(())
}

fn reject_duplicates<'a>(
    kind: CapabilityKind,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), CapabilityError> {
    let mut seen = std::collections::BTreeSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(CapabilityError::Duplicate {
                kind,
                name: name.to_owned(),
            });
        }
    }
    Ok(())
}

/// One effective capability after masking, with the layer that supplied it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct CapabilityBinding {
    pub kind: CapabilityKind,
    pub name: String,
    pub origin: CapabilityOrigin,
}

/// One general capability hidden by a same-name session capability.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct MaskedCapability {
    pub kind: CapabilityKind,
    pub name: String,
}

/// The name-level view of what one Session actually sees. It never carries
/// transport credentials, so a host can log or persist it freely.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CapabilityResolution {
    #[serde(default)]
    pub bindings: Vec<CapabilityBinding>,
    #[serde(default)]
    pub masked: Vec<MaskedCapability>,
}

impl CapabilityResolution {
    /// Effective names of one kind, in deterministic order.
    pub fn names(&self, kind: CapabilityKind) -> Vec<&str> {
        self.bindings
            .iter()
            .filter(|binding| binding.kind == kind)
            .map(|binding| binding.name.as_str())
            .collect()
    }

    pub fn origin(&self, kind: CapabilityKind, name: &str) -> Option<CapabilityOrigin> {
        self.bindings
            .iter()
            .find(|binding| binding.kind == kind && binding.name == name)
            .map(|binding| binding.origin)
    }

    pub fn is_masked(&self, kind: CapabilityKind, name: &str) -> bool {
        self.masked
            .iter()
            .any(|masked| masked.kind == kind && masked.name == name)
    }
}

/// The resolved contributions of one Session, in both name form (for the host)
/// and value form (for the native bind).
#[derive(Clone, Default, PartialEq)]
pub(crate) struct ResolvedCapabilities {
    pub(crate) resolution: CapabilityResolution,
    pub(crate) skill_roots: Vec<PathBuf>,
    pub(crate) mcp_services: Vec<McpServerConfig>,
    pub(crate) agent_services: BTreeMap<String, String>,
}

/// Masks the general layer with the session layer. A session contribution
/// replaces the general contribution of the same kind and name; every other
/// general contribution stays. Order is deterministic: general entries keep
/// their configured order, then session entries keep theirs.
pub(crate) fn resolve_capabilities(
    general: &CapabilityLayer,
    session: &CapabilityLayer,
) -> ResolvedCapabilities {
    let mut resolved = ResolvedCapabilities::default();
    let session_skills: std::collections::BTreeSet<&str> =
        session.skills.iter().map(|s| s.name()).collect();
    let session_mcp: std::collections::BTreeSet<&str> = session
        .mcp_services
        .iter()
        .map(McpServerConfig::name)
        .collect();
    let session_agents: std::collections::BTreeSet<&str> =
        session.agent_services.iter().map(|s| s.name()).collect();

    for skill in &general.skills {
        if session_skills.contains(skill.name()) {
            resolved.resolution.masked.push(MaskedCapability {
                kind: CapabilityKind::Skill,
                name: skill.name().to_owned(),
            });
            continue;
        }
        resolved.bind(
            CapabilityKind::Skill,
            skill.name(),
            CapabilityOrigin::General,
        );
        resolved.skill_roots.push(skill.root().to_path_buf());
    }
    for skill in &session.skills {
        resolved.bind(
            CapabilityKind::Skill,
            skill.name(),
            CapabilityOrigin::Session,
        );
        resolved.skill_roots.push(skill.root().to_path_buf());
    }

    for service in &general.mcp_services {
        if session_mcp.contains(service.name()) {
            resolved.resolution.masked.push(MaskedCapability {
                kind: CapabilityKind::McpService,
                name: service.name().to_owned(),
            });
            continue;
        }
        resolved.bind(
            CapabilityKind::McpService,
            service.name(),
            CapabilityOrigin::General,
        );
        resolved.mcp_services.push(service.clone());
    }
    for service in &session.mcp_services {
        resolved.bind(
            CapabilityKind::McpService,
            service.name(),
            CapabilityOrigin::Session,
        );
        resolved.mcp_services.push(service.clone());
    }

    for service in &general.agent_services {
        if session_agents.contains(service.name()) {
            resolved.resolution.masked.push(MaskedCapability {
                kind: CapabilityKind::AgentService,
                name: service.name().to_owned(),
            });
            continue;
        }
        resolved.bind(
            CapabilityKind::AgentService,
            service.name(),
            CapabilityOrigin::General,
        );
        resolved
            .agent_services
            .insert(service.name().to_owned(), service.model().to_owned());
    }
    for service in &session.agent_services {
        resolved.bind(
            CapabilityKind::AgentService,
            service.name(),
            CapabilityOrigin::Session,
        );
        resolved
            .agent_services
            .insert(service.name().to_owned(), service.model().to_owned());
    }
    resolved.resolution.masked.sort();
    resolved
}

impl ResolvedCapabilities {
    fn bind(&mut self, kind: CapabilityKind, name: &str, origin: CapabilityOrigin) {
        self.resolution.bindings.push(CapabilityBinding {
            kind,
            name: name.to_owned(),
            origin,
        });
    }

    /// The `x.ai/sessionCapabilities` session `_meta` payload, or `None` when
    /// this Session adds nothing to the runtime-global configuration.
    pub(crate) fn session_meta(&self, base_skill_paths: &[PathBuf]) -> Option<serde_json::Value> {
        if self.skill_roots.is_empty() && self.agent_services.is_empty() {
            return None;
        }
        let paths: Vec<String> = base_skill_paths
            .iter()
            .chain(self.skill_roots.iter())
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
        Some(serde_json::json!({
            "skills": { "paths": paths },
            "agentServices": self.agent_services,
        }))
    }
}

impl McpServerConfig {
    /// The mount name. It is the layering identity of an MCP contribution.
    pub fn name(&self) -> &str {
        match self {
            Self::Stdio { name, .. } | Self::Http { name, .. } | Self::Sse { name, .. } => name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skills(names: &[&str]) -> CapabilityLayer {
        CapabilityLayer::new().skills(
            names
                .iter()
                .map(|name| SkillContribution::new(*name, format!("/tmp/{name}"))),
        )
    }

    #[test]
    fn session_layer_masks_only_same_named_general_capabilities() {
        let general = skills(&["oracle", "general"]);
        let session = skills(&["oracle"]);
        let resolved = resolve_capabilities(&general, &session);
        assert_eq!(
            resolved.resolution.origin(CapabilityKind::Skill, "oracle"),
            Some(CapabilityOrigin::Session)
        );
        assert_eq!(
            resolved.resolution.origin(CapabilityKind::Skill, "general"),
            Some(CapabilityOrigin::General)
        );
        assert!(
            resolved
                .resolution
                .is_masked(CapabilityKind::Skill, "oracle")
        );
        assert!(
            !resolved
                .resolution
                .is_masked(CapabilityKind::Skill, "general")
        );

        let unmasked = resolve_capabilities(&general, &CapabilityLayer::new());
        assert_eq!(
            unmasked.resolution.origin(CapabilityKind::Skill, "oracle"),
            Some(CapabilityOrigin::General)
        );
        assert!(unmasked.resolution.masked.is_empty());
    }

    #[test]
    fn validation_fails_closed_on_duplicates_bounds_and_names() {
        assert!(matches!(
            skills(&["alpha", "alpha"]).validate(),
            Err(CapabilityError::Duplicate {
                kind: CapabilityKind::Skill,
                ..
            })
        ));
        let oversized: Vec<SkillContribution> = (0..=MAX_CAPABILITY_LAYER_ENTRIES)
            .map(|index| SkillContribution::new(format!("s{index}"), format!("/tmp/s{index}")))
            .collect();
        assert!(matches!(
            CapabilityLayer::new().skills(oversized).validate(),
            Err(CapabilityError::TooManyEntries { .. })
        ));
        assert!(matches!(
            CapabilityLayer::new()
                .skill(SkillContribution::new("-bad", "/tmp/bad"))
                .validate(),
            Err(CapabilityError::InvalidName(_))
        ));
        assert!(matches!(
            CapabilityLayer::new()
                .skill(SkillContribution::new("relative", "tmp/bad"))
                .validate(),
            Err(CapabilityError::RelativeSkillRoot { .. })
        ));
        assert!(matches!(
            CapabilityLayer::new()
                .agent_service(AgentServiceContribution::new("explore", " "))
                .validate(),
            Err(CapabilityError::MissingModel { .. })
        ));
        assert!(skills(&["alpha", "beta"]).validate().is_ok());
    }

    #[test]
    fn layer_decodes_additively_from_partial_documents() {
        let empty: CapabilityLayer = serde_json::from_str("{}").expect("empty layer");
        assert!(empty.is_empty());
        let partial: CapabilityLayer = serde_json::from_value(serde_json::json!({
            "skills": [{ "name": "alpha", "root": "/tmp/alpha" }],
            "future_kind": ["ignored"]
        }))
        .expect("partial layer");
        assert_eq!(partial.skill_contributions().len(), 1);
        assert!(partial.mcp_service_contributions().is_empty());
        assert!(partial.agent_service_contributions().is_empty());
    }
}
