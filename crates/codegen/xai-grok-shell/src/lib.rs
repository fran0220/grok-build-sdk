#![allow(
    unused_imports,
    unused_variables,
    unused_mut,
    unreachable_code,
    dead_code
)]
#![warn(unreachable_pub)]
#[cfg(all(test, feature = "dhat-heap"))]
#[global_allocator]
static DHAT_ALLOC: dhat::Alloc = dhat::Alloc;
pub(crate) use xai_grok_telemetry::unified_log;
pub use xai_tracing_macros::{teprintln, timed, tprintln};
pub mod active_sessions;
pub mod agent;
pub mod auth;
pub mod builtin;
pub mod bundle;
pub mod claude_import;
pub mod claude_import_state;
pub mod cli_models;
pub mod config;
pub mod embedded;
pub use xai_grok_shell_base::cpu_profile;
pub use xai_grok_shell_base::env;
pub mod extensions;
pub use xai_grok_workspace::foreign_sessions;
pub mod heap_profile;
pub use xai_grok_http as http;
pub mod inspect;
pub mod instrumentation;
pub mod leader;
pub mod managed_config;
pub mod mcp_doctor;
pub use xai_grok_models as models;
pub mod plugin;
#[doc(hidden)]
pub mod origin_runtime {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static ROOT_SESSIONS: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();

    fn root_sessions() -> &'static Mutex<HashMap<String, String>> {
        ROOT_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Canonical prompt identity shared with the private Origin façade. This
    /// lives in the fork so crash recovery can verify the exact retained
    /// native prefix instead of inferring success from prompt count alone.
    pub fn prompt_digest(text: &str) -> String {
        use sha2::Digest as _;
        let mut digest = sha2::Sha256::new();
        digest.update(b"origin-grok-runtime.prompt.v1\0");
        digest.update((text.len() as u64).to_be_bytes());
        digest.update(text.as_bytes());
        format!("sha256:{:x}", digest.finalize())
    }

    /// Registers one façade-created root session. Only the private embedded
    /// runtime calls this; model input and persisted Grok metadata cannot add
    /// roots to the correlation tree.
    pub fn register_root_session(session_id: &str) -> bool {
        let Ok(mut sessions) = root_sessions().lock() else {
            return false;
        };
        if sessions.contains_key(session_id) {
            return false;
        }
        sessions.insert(session_id.to_owned(), session_id.to_owned());
        true
    }

    /// Rebinds one resident session's capability layer between turns. The
    /// value uses the same shape as the `x.ai/sessionCapabilities` session
    /// `_meta` entry; `None` restores the runtime-global configuration.
    /// Only the private embedded façade calls this, and only from the agent
    /// thread that owns the session.
    pub fn bind_session_capabilities(session_id: &str, value: Option<&serde_json::Value>) {
        let meta = value.map(|value| {
            let mut meta = agent_client_protocol::Meta::new();
            meta.insert(
                crate::agent::session_capabilities::SESSION_CAPABILITIES_META_KEY.to_owned(),
                value.clone(),
            );
            meta
        });
        crate::agent::session_capabilities::bind_from_meta(session_id, meta.as_ref());
    }

    /// Resolves a transport session to its registered root identity.
    /// A child is admitted only when its internally supplied parent is already
    /// in the registered tree, so nesting remains transitive and a child cannot
    /// select an unrelated Forge Thread.
    pub fn resolve_root_session(
        session_id: &str,
        parent_session_id: Option<&str>,
    ) -> Option<String> {
        let mut sessions = root_sessions().lock().ok()?;
        if let Some(root) = sessions.get(session_id) {
            return Some(root.clone());
        }
        let root = sessions.get(parent_session_id?).cloned()?;
        sessions.insert(session_id.to_owned(), root.clone());
        Some(root)
    }

    /// Register a native child before it can emit correlated events.
    pub fn register_child_session(session_id: &str, parent_session_id: &str) -> bool {
        let Ok(mut sessions) = root_sessions().lock() else {
            return false;
        };
        if sessions.contains_key(session_id) {
            return false;
        }
        let Some(root) = sessions.get(parent_session_id).cloned() else {
            return false;
        };
        sessions.insert(session_id.to_owned(), root);
        true
    }

    /// Removes one completed or failed child without affecting the root or its
    /// siblings. Root identities cannot be removed through this function.
    pub fn unregister_child_session(session_id: &str) {
        if let Ok(mut sessions) = root_sessions().lock()
            && sessions
                .get(session_id)
                .is_some_and(|root| root != session_id)
        {
            sessions.remove(session_id);
        }
    }

    /// Removes a root and every descendant transport session when Forge unloads
    /// it. Opaque child IDs never outlive their root session.
    pub fn unregister_session_tree(session_id: &str) {
        if let Ok(mut sessions) = root_sessions().lock() {
            sessions.retain(|_, root| root != session_id);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn prompt_digest_is_domain_separated_and_length_bound() {
            assert_eq!(prompt_digest("same"), prompt_digest("same"));
            assert_ne!(prompt_digest("same"), prompt_digest("same\0"));
        }

        #[test]
        fn session_tree_is_transitive_and_removed_with_its_root() {
            let root = "origin-session-test-root";
            let child = "origin-session-test-child";
            let grandchild = "origin-session-test-grandchild";
            unregister_session_tree(root);

            assert_eq!(resolve_root_session(root, None), None);
            assert_eq!(resolve_root_session(child, Some(root)), None);
            assert!(register_root_session(root));
            assert_eq!(resolve_root_session(root, None).as_deref(), Some(root));
            assert_eq!(
                resolve_root_session(child, Some(root)).as_deref(),
                Some(root)
            );
            assert_eq!(
                resolve_root_session(grandchild, Some(child)).as_deref(),
                Some(root)
            );

            assert!(!register_child_session(child, root));
            assert!(!register_root_session(child));

            unregister_child_session(child);
            assert_eq!(resolve_root_session(child, None), None);
            assert!(register_child_session(child, root));

            unregister_session_tree(root);
            assert_eq!(resolve_root_session(root, None), None);
            assert_eq!(resolve_root_session(child, None), None);
            assert_eq!(resolve_root_session(grandchild, None), None);
        }
    }
}
pub mod relay;
pub mod remote;
pub mod sampling;
pub mod session;
pub mod terminal;
#[cfg(test)]
pub(crate) mod test_support;
pub mod tier;
pub mod tools;
pub mod upload;
pub mod util;
