use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{OnceLock, RwLock},
};

pub mod info;

pub use info::Info;

// Re-export shared feedback wire types used by downstream crates
// (e.g. xai-grok-pager-render).
pub use prod_mc_cli_chat_proxy_types::feedback_types::FeedbackTerminalInfo;

#[derive(Clone)]
struct SessionRootOverride {
    storage_root: PathBuf,
    root_session_id: String,
}

static SESSION_ROOT_OVERRIDES: OnceLock<RwLock<HashMap<String, SessionRootOverride>>> =
    OnceLock::new();

fn session_root_overrides() -> &'static RwLock<HashMap<String, SessionRootOverride>> {
    SESSION_ROOT_OVERRIDES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Bind one embedded root session to host-supplied storage without consulting
/// process-global `GROK_HOME`.
/// `Some(true)` means this call inserted the binding, `Some(false)` means the
/// exact binding already existed, and `None` rejects an identity collision.
pub fn register_session_root(session_id: &str, storage_root: &Path) -> Option<bool> {
    let mut overrides = session_root_overrides()
        .write()
        .unwrap_or_else(|error| error.into_inner());
    match overrides.get(session_id) {
        Some(existing)
            if existing.root_session_id == session_id && existing.storage_root == storage_root =>
        {
            Some(false)
        }
        Some(_) => None,
        None => {
            overrides.insert(
                session_id.to_owned(),
                SessionRootOverride {
                    storage_root: storage_root.to_owned(),
                    root_session_id: session_id.to_owned(),
                },
            );
            Some(true)
        }
    }
}

/// Give a native child session the same explicit storage root as its trusted
/// parent. Returns false when the parent is not an embedded session.
pub fn inherit_session_root(session_id: &str, parent_session_id: &str) -> bool {
    let mut overrides = session_root_overrides()
        .write()
        .unwrap_or_else(|error| error.into_inner());
    let Some(parent) = overrides.get(parent_session_id).cloned() else {
        return false;
    };
    if overrides.contains_key(session_id) {
        return false;
    }
    overrides.insert(session_id.to_owned(), parent);
    true
}

/// Roll back a child registration that failed before the child started. Root
/// removal stays tree-scoped and is handled by [`unregister_session_tree`].
pub fn unregister_inherited_session(session_id: &str) {
    let mut overrides = session_root_overrides()
        .write()
        .unwrap_or_else(|error| error.into_inner());
    if overrides
        .get(session_id)
        .is_some_and(|value| value.root_session_id != session_id)
    {
        overrides.remove(session_id);
    }
}

/// Remove an embedded root and every native child that inherited its storage root.
pub fn unregister_session_tree(session_id: &str) {
    session_root_overrides()
        .write()
        .unwrap_or_else(|error| error.into_inner())
        .retain(|_, value| value.root_session_id != session_id);
}

pub fn session_dir(info: &Info) -> PathBuf {
    if let Some(root) = session_root_overrides()
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .get(info.id.0.as_ref())
        .map(|value| value.storage_root.clone())
    {
        return root
            .join("sessions")
            .join(xai_grok_tools::util::grok_home::encode_cwd_dirname(
                &info.cwd,
            ))
            .join(info.id.to_string());
    }
    xai_grok_tools::util::grok_home::sessions_cwd_dir(&info.cwd).join(info.id.to_string())
}

#[cfg(test)]
mod origin_embedded_tests {
    use super::*;

    #[test]
    fn roots_and_children_reject_identity_collisions() {
        let root = "origin-storage-test-root";
        let other_root = "origin-storage-test-other-root";
        let child = "origin-storage-test-child";
        let first = PathBuf::from("/origin/storage/first");
        let second = PathBuf::from("/origin/storage/second");
        unregister_session_tree(root);
        unregister_session_tree(other_root);

        assert_eq!(register_session_root(root, &first), Some(true));
        assert_eq!(register_session_root(root, &first), Some(false));
        assert_eq!(register_session_root(root, &second), None);
        assert_eq!(register_session_root(other_root, &second), Some(true));
        assert!(inherit_session_root(child, root));
        assert!(!inherit_session_root(child, other_root));
        assert_eq!(register_session_root(child, &second), None);

        unregister_inherited_session(child);
        assert!(inherit_session_root(child, other_root));
        unregister_session_tree(root);
        unregister_session_tree(other_root);
    }
}
