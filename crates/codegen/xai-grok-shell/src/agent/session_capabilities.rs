//! Per-session capability layer supplied by an embedding host through session
//! `_meta`. It lets one runtime serve sessions that see different skill roots
//! and different agent-service routes without a second runtime or a restart.
//!
//! Session actors run on their own threads, so the map is process-global and
//! keyed by the session's unique identity rather than thread-local.

use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use xai_grok_agent::prompt::skills::SkillsConfig;

/// Session `_meta` key carrying the resolved per-session capability layer.
pub(crate) const SESSION_CAPABILITIES_META_KEY: &str = "x.ai/sessionCapabilities";

/// One session's resolved capability layer. The embedding host performs the
/// general/session masking; what arrives here is already effective.
#[derive(Clone, Debug, Default)]
pub(crate) struct SessionCapabilityLayer {
    /// Replaces the global `[skills]` table for this session when present.
    pub(crate) skills: Option<SkillsConfig>,
    /// Subagent name to model id, layered over the global overrides.
    pub(crate) agent_services: HashMap<String, String>,
}

static LAYERS: OnceLock<Mutex<HashMap<String, SessionCapabilityLayer>>> = OnceLock::new();

fn layers() -> &'static Mutex<HashMap<String, SessionCapabilityLayer>> {
    LAYERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn parse(value: &serde_json::Value) -> SessionCapabilityLayer {
    let skills = value
        .get("skills")
        .cloned()
        .and_then(|skills| serde_json::from_value::<SkillsConfig>(skills).ok());
    let agent_services = value
        .get("agentServices")
        .and_then(serde_json::Value::as_object)
        .map(|services| {
            services
                .iter()
                .filter_map(|(name, model)| Some((name.clone(), model.as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default();
    SessionCapabilityLayer {
        skills,
        agent_services,
    }
}

/// Binds (or clears) the layer for one session id from its session `_meta`.
pub(crate) fn bind_from_meta(session_id: &str, meta: Option<&agent_client_protocol::Meta>) {
    let layer = meta
        .and_then(|meta| meta.get(SESSION_CAPABILITIES_META_KEY))
        .map(parse);
    let Ok(mut layers) = layers().lock() else {
        return;
    };
    match layer {
        Some(layer) => {
            layers.insert(session_id.to_owned(), layer);
        }
        None => {
            layers.remove(session_id);
        }
    }
}

pub(crate) fn skills_for(session_id: &str) -> Option<SkillsConfig> {
    layers()
        .lock()
        .ok()
        .and_then(|layers| layers.get(session_id).and_then(|l| l.skills.clone()))
}

pub(crate) fn agent_services_for(session_id: &str) -> HashMap<String, String> {
    layers()
        .lock()
        .ok()
        .and_then(|layers| layers.get(session_id).map(|l| l.agent_services.clone()))
        .unwrap_or_default()
}

/// Drops a session's layer when the embedding host unloads it.
pub(crate) fn release(session_id: &str) {
    if let Ok(mut layers) = layers().lock() {
        layers.remove(session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_releases_and_ignores_absent_meta() {
        let meta: agent_client_protocol::Meta = serde_json::from_value(serde_json::json!({
            SESSION_CAPABILITIES_META_KEY: {
                "skills": { "paths": ["/tmp/a"] },
                "agentServices": { "explore": "fast-model" }
            }
        }))
        .expect("meta");
        bind_from_meta("session-a", Some(&meta));
        assert_eq!(
            skills_for("session-a").expect("skills").paths,
            vec!["/tmp/a".to_owned()]
        );
        assert_eq!(
            agent_services_for("session-a")
                .get("explore")
                .map(String::as_str),
            Some("fast-model")
        );
        assert!(skills_for("session-b").is_none());
        assert!(agent_services_for("session-b").is_empty());

        bind_from_meta("session-a", None);
        assert!(skills_for("session-a").is_none());

        bind_from_meta("session-a", Some(&meta));
        release("session-a");
        assert!(skills_for("session-a").is_none());
    }
}
