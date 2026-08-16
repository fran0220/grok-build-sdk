//! Session forking functionality
//!
//! Forks a saved session to a new working directory with a new session ID.
//! This creates new session files but does not start the session.

use crate::remote::BackendClient;
const FORK_LOG: &str = "xai_fork";
use crate::session::export::ExportedMetadata;
use crate::session::info::Info;
use crate::session::storage::{CopySessionOptions, JsonlStorageAdapter};
use crate::util::grok_home::grok_home;
use agent_client_protocol as acp;
use std::io;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkSessionRequest {
    pub source_session_id: String,
    pub source_cwd: String,
    pub new_cwd: String,
    /// Client-provided session ID for the forked session.
    /// If None, a new ID will be auto-generated.
    #[serde(default)]
    pub new_session_id: Option<String>,
    /// Optional model ID override for the forked session.
    /// If None, the source session's model will be used.
    #[serde(default)]
    pub new_model_id: Option<String>,
    #[serde(default)]
    pub target_prompt_index: Option<usize>,
    /// Override `session_kind` in the forked summary. Defaults to `"fork"`.
    /// Worktree forks set this to `"worktree"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_kind: Option<String>,
    /// The original workspace directory this worktree session was spawned from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_workspace_dir: Option<String>,
    /// Retry a caller-selected target by proving that its existing native
    /// publication came from this exact source snapshot and request.
    #[serde(default)]
    pub create_or_verify: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkSessionResponse {
    pub new_session_id: String,
    pub chat_messages_copied: usize,
    pub updates_copied: usize,
    pub plan_state_copied: bool,
    /// The working directory of the new forked session
    pub new_cwd: String,
    /// The parent session ID (source session that was forked)
    pub parent_session_id: String,
    /// The model ID of the forked session (may differ from source if overridden)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_model_id: Option<String>,
}

/// Generate a forked session ID.
///
/// Uses a plain UUIDv7 -- no prefix or source embedding. This keeps IDs
/// a constant 36 chars regardless of how many fork rounds occur.
fn generate_fork_session_id(_source_id: &str) -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Fork a saved session to a new working directory.
pub async fn fork_session(
    request: ForkSessionRequest,
    agent_id: &str,
    auth_manager: Option<std::sync::Arc<crate::auth::AuthManager>>,
    storage_root: Option<std::path::PathBuf>,
    authority: Option<
        std::sync::Arc<dyn crate::session::state_authority::NativeSessionStateAuthority>,
    >,
) -> io::Result<ForkSessionResponse> {
    let t0 = std::time::Instant::now();

    let root_dir = storage_root.unwrap_or_else(grok_home);
    let storage = JsonlStorageAdapter::with_root(root_dir.clone());

    // Build source and target Info
    let source_info = Info {
        id: acp::SessionId::new(request.source_session_id.clone()),
        cwd: request.source_cwd.clone(),
    };

    // Use client-provided session ID or generate one
    let new_session_id = request
        .new_session_id
        .clone()
        .unwrap_or_else(|| generate_fork_session_id(&request.source_session_id));

    let target_info = Info {
        id: acp::SessionId::new(new_session_id.clone()),
        cwd: request.new_cwd.clone(),
    };

    // Copy session data with parent tracking.
    // Runs on the blocking thread pool so concurrent fork copies can execute
    // truly in parallel (on a LocalSet, async copy_session_data serializes
    // because the sync disk I/O blocks the single-threaded runtime).
    let options = CopySessionOptions {
        parent_session_id: Some(request.source_session_id.clone()),
        new_model_id: request.new_model_id.clone(),
        target_prompt_index: request.target_prompt_index,
        session_kind: request.session_kind.clone(),
        source_workspace_dir: request.source_workspace_dir.clone(),
        // Carry the parent's compaction segment archive into the fork so the
        // child retains pre-compaction history (the live summary is already
        // copied via chat_history.jsonl).
        copy_compaction_segments: true,
        ..Default::default()
    };

    let result = tokio::task::spawn_blocking(move || {
        if let Some(authority) = authority {
            copy_authority_fork(
                authority.as_ref(),
                &storage,
                &source_info,
                &target_info,
                options,
                request.create_or_verify,
            )
        } else {
            storage.copy_session_data_sync(&source_info, &target_info, options)
        }
    })
    .await
    .map_err(|e| io::Error::other(format!("spawn_blocking panicked: {e}")))??;

    let copy_ms = t0.elapsed().as_millis() as u64;

    // Writeback session to backend (fire-and-forget).
    // This is telemetry-grade: the local fork works without it. All fork
    // state lives locally (session files on disk), and the caller does not
    // depend on synchronous backend registration. The backend eventually
    // learns about the session when the background task completes.
    // Spawning removes the network round-trip (~200-400ms) from the
    // critical path.
    if let Some(am) = auth_manager {
        let sid = new_session_id.clone();
        let cwd = request.new_cwd.clone();
        let parent = request.source_session_id.clone();
        let model = request.new_model_id.clone();
        let aid = agent_id.to_string();
        tokio::spawn(async move {
            if let Err(e) =
                sync_forked_session_to_backend(&sid, &cwd, parent, model, &aid, am).await
            {
                tracing::warn!(
                    session_id = %sid,
                    error = %e,
                    "Failed to register forked session with backend (background)"
                );
            }
        });
    }

    let total_ms = t0.elapsed().as_millis() as u64;
    tracing::info!(
        target: FORK_LOG,
        session_id = %new_session_id,
        source_session = %request.source_session_id,
        copy_ms,
        total_ms,
        chat_copied = result.chat_messages_copied,
        updates_copied = result.updates_copied,
        "FORK_COPY: session data copied (backend sync spawned in background)"
    );

    Ok(ForkSessionResponse {
        new_session_id,
        chat_messages_copied: result.chat_messages_copied,
        updates_copied: result.updates_copied,
        plan_state_copied: result.plan_state_copied,
        new_cwd: request.new_cwd,
        parent_session_id: request.source_session_id,
        new_model_id: request.new_model_id,
    })
}

fn copy_authority_fork(
    authority: &dyn crate::session::state_authority::NativeSessionStateAuthority,
    storage: &JsonlStorageAdapter,
    source_info: &Info,
    target_info: &Info,
    options: CopySessionOptions,
    create_or_verify: bool,
) -> io::Result<crate::session::storage::CopySessionResult> {
    use crate::session::state_authority::{
        ReplayRecord, RewindOperation, SessionIdentity, SessionInspection,
    };
    use crate::session::storage::{
        filter_rewind_by, rewind_step_for_update, truncate_for_prompt_by,
    };

    let generation = match authority
        .inspect(source_info.id.0.as_ref())
        .map_err(|e| io::Error::other(e.to_string()))?
    {
        SessionInspection::Live { generation } => generation,
        _ => return Err(io::Error::other("fork source is not live")),
    };
    let source = authority
        .open(SessionIdentity {
            identity: source_info.id.0.to_string(),
            generation: generation.clone(),
        })
        .map_err(|e| io::Error::other(e.to_string()))?;
    let mut records = Vec::new();
    let mut cursor = None;
    loop {
        let page = source
            .replay_page(cursor, 4096)
            .map_err(|e| io::Error::other(e.to_string()))?;
        records.extend(page.records);
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
        if records.len() > 1_000_000 {
            return Err(io::Error::other("fork traversal limit exceeded"));
        }
    }

    #[derive(Clone)]
    struct Candidate {
        record: usize,
        update: crate::session::storage::SessionUpdate,
    }
    let mut candidates = Vec::new();
    for (record, value) in records.iter().enumerate() {
        let bytes = match value {
            ReplayRecord::Update(bytes) => Some(bytes),
            ReplayRecord::Checkpoint { marker, .. } | ReplayRecord::Rewind { marker, .. }
                if !marker.is_empty() =>
            {
                Some(marker)
            }
            _ => None,
        };
        if let Some(bytes) = bytes {
            candidates.push(Candidate {
                record,
                update: serde_json::from_slice(bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            });
        }
    }
    let retained = if let Some(target) = options.target_prompt_index {
        let mut values = filter_rewind_by(candidates, |x| rewind_step_for_update(&x.update));
        let keep = truncate_for_prompt_by(&values, target, |x| rewind_step_for_update(&x.update));
        values.truncate(keep);
        values
            .into_iter()
            .map(|x| x.record)
            .collect::<std::collections::HashSet<_>>()
    } else {
        candidates.into_iter().map(|x| x.record).collect()
    };
    let target_id = target_info.id.clone();
    let mut prepared = Vec::new();
    let mut updates_copied = 0usize;
    for (index, record) in records.into_iter().enumerate() {
        let rewrite = |bytes: Vec<u8>| -> io::Result<Vec<u8>> {
            let update: crate::session::storage::SessionUpdate = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            serde_json::to_vec(
                &crate::session::storage::jsonl::transform_session_id_in_update(update, &target_id),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        };
        match record {
            ReplayRecord::Update(bytes) if retained.contains(&index) => {
                prepared.push(ReplayRecord::Update(rewrite(bytes)?));
                updates_copied += 1;
            }
            ReplayRecord::Checkpoint {
                name,
                payload,
                marker,
            } if retained.contains(&index) => {
                prepared.push(ReplayRecord::Checkpoint {
                    name,
                    payload,
                    marker: rewrite(marker)?,
                });
                updates_copied += 1;
            }
            ReplayRecord::Rewind { operation, marker } if retained.contains(&index) => {
                prepared.push(ReplayRecord::Rewind {
                    operation,
                    marker: rewrite(marker)?,
                });
                updates_copied += 1;
            }
            ReplayRecord::Rewind {
                operation:
                    RewindOperation::AppendPoint {
                        index: point,
                        payload,
                    },
                marker,
            } if marker.is_empty()
                && options
                    .target_prompt_index
                    .is_none_or(|target| point <= target as u64) =>
            {
                prepared.push(ReplayRecord::Rewind {
                    operation: RewindOperation::AppendPoint {
                        index: point,
                        payload,
                    },
                    marker,
                });
            }
            _ => {}
        }
    }
    let chat_messages_copied =
        crate::session::storage::chat_rebuild::rebuild_chat_history_in_memory(
            &prepared
                .iter()
                .filter_map(|record| match record {
                    ReplayRecord::Update(bytes) => serde_json::from_slice(bytes).ok(),
                    ReplayRecord::Checkpoint { marker, .. }
                    | ReplayRecord::Rewind { marker, .. }
                        if !marker.is_empty() =>
                    {
                        serde_json::from_slice(marker).ok()
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )
        .len();
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ForkPublicationProof<'a> {
        schema: &'static str,
        source_session_id: &'a str,
        source_generation: &'a str,
        source_cwd: &'a str,
        target_session_id: &'a str,
        new_cwd: &'a str,
        new_model_id: &'a Option<String>,
        target_prompt_index: Option<usize>,
        session_kind: &'a Option<String>,
        source_workspace_dir: &'a Option<String>,
        records: &'a [ReplayRecord],
    }
    let target_generation = if create_or_verify {
        use sha2::{Digest as _, Sha256};
        let proof = serde_json::to_vec(&ForkPublicationProof {
            schema: "xai.native-session-fork/1",
            source_session_id: source_info.id.0.as_ref(),
            source_generation: &generation,
            source_cwd: &source_info.cwd,
            target_session_id: target_info.id.0.as_ref(),
            new_cwd: &target_info.cwd,
            new_model_id: &options.new_model_id,
            target_prompt_index: options.target_prompt_index,
            session_kind: &options.session_kind,
            source_workspace_dir: &options.source_workspace_dir,
            records: &prepared,
        })
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        format!("fork-sha256:{:x}", Sha256::digest(proof))
    } else {
        uuid::Uuid::now_v7().to_string()
    };
    match authority
        .inspect(target_info.id.0.as_ref())
        .map_err(|error| io::Error::other(error.to_string()))?
    {
        SessionInspection::Live {
            generation: current,
        } if create_or_verify && current == target_generation => {
            return storage.verify_fork_sidecars_sync(
                target_info,
                updates_copied,
                chat_messages_copied,
            );
        }
        SessionInspection::Vacant => {}
        SessionInspection::Live { .. } => {
            return Err(io::Error::other(
                "fork target identity already exists with a different source snapshot or request",
            ));
        }
        SessionInspection::Tombstoned { .. } => {
            return Err(io::Error::other(
                "fork target identity is permanently tombstoned",
            ));
        }
    }
    if storage.session_dir(target_info).exists() {
        return Err(io::Error::other(
            "fork target sidecar directory already exists without an authoritative publication",
        ));
    }
    let sidecars = storage.copy_fork_sidecars_sync(
        source_info,
        target_info,
        options,
        updates_copied,
        chat_messages_copied,
    )?;
    if let Err(error) = authority.publish_fork(
        SessionIdentity {
            identity: target_info.id.0.to_string(),
            generation: target_generation,
        },
        prepared,
    ) {
        let _ = std::fs::remove_dir_all(storage.session_dir(target_info));
        return Err(io::Error::other(error.to_string()));
    }
    Ok(sidecars)
}

/// Sync a forked session to the backend (for writeback mode).
async fn sync_forked_session_to_backend(
    session_id: &str,
    cwd: &str,
    parent_session_id: String,
    model_id: Option<String>,
    agent_id: &str,
    auth_manager: std::sync::Arc<crate::auth::AuthManager>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = BackendClient::new().with_auth_manager(auth_manager);
    let metadata = ExportedMetadata {
        title: None, // Will be generated later when session runs
        cwd: cwd.to_string(),
        model_id,
        created_at: Some(chrono::Utc::now().to_rfc3339()),
        updated_at: Some(chrono::Utc::now().to_rfc3339()),
        total_messages: Some(0),
        parent_session_id: Some(parent_session_id),
        session_kind: None,
        subagent_type: None,
        subagent_persona: None,
        subagent_role: None,
        fork_context_source: None,
        subagent_depth: None,
        title_is_manual: None,
    };

    client
        .upsert_session(session_id, &metadata, agent_id)
        .await?;
    tracing::info!(
        session_id = %session_id,
        "Forked session registered with backend"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticSession {
        id: crate::session::state_authority::SessionIdentity,
        records: Vec<crate::session::state_authority::ReplayRecord>,
    }

    impl crate::session::state_authority::NativeSession for StaticSession {
        fn identity(&self) -> &crate::session::state_authority::SessionIdentity {
            &self.id
        }
        fn stage_update(
            &self,
            _: Vec<u8>,
        ) -> Result<(), crate::session::state_authority::AuthorityError> {
            unreachable!()
        }
        fn flush(
            &self,
        ) -> Result<
            crate::session::state_authority::ReplayCursor,
            crate::session::state_authority::AuthorityError,
        > {
            unreachable!()
        }
        fn replay_page(
            &self,
            cursor: Option<crate::session::state_authority::ReplayCursor>,
            _: usize,
        ) -> Result<
            crate::session::state_authority::ReplayPage,
            crate::session::state_authority::AuthorityError,
        > {
            if cursor.is_some() {
                return Err(crate::session::state_authority::AuthorityError(
                    "unexpected cursor".into(),
                ));
            }
            Ok(crate::session::state_authority::ReplayPage {
                records: self.records.clone(),
                next: None,
            })
        }
        fn publish_checkpoint(
            &self,
            _: String,
            _: Vec<u8>,
            _: Vec<u8>,
        ) -> Result<
            crate::session::state_authority::ReplayCursor,
            crate::session::state_authority::AuthorityError,
        > {
            unreachable!()
        }
        fn publish_rewind(
            &self,
            _: crate::session::state_authority::RewindOperation,
            _: Vec<u8>,
        ) -> Result<
            crate::session::state_authority::ReplayCursor,
            crate::session::state_authority::AuthorityError,
        > {
            unreachable!()
        }
    }

    struct ForkAuthority {
        source: std::sync::Arc<StaticSession>,
        published: std::sync::Mutex<
            Vec<(
                crate::session::state_authority::SessionIdentity,
                Vec<crate::session::state_authority::ReplayRecord>,
            )>,
        >,
    }

    impl crate::session::state_authority::NativeSessionStateAuthority for ForkAuthority {
        fn inspect(
            &self,
            identity: &str,
        ) -> Result<
            crate::session::state_authority::SessionInspection,
            crate::session::state_authority::AuthorityError,
        > {
            if identity == self.source.id.identity {
                return Ok(crate::session::state_authority::SessionInspection::Live {
                    generation: self.source.id.generation.clone(),
                });
            }
            Ok(self
                .published
                .lock()
                .unwrap()
                .iter()
                .find(|(id, _)| id.identity == identity)
                .map_or(
                    crate::session::state_authority::SessionInspection::Vacant,
                    |(id, _)| crate::session::state_authority::SessionInspection::Live {
                        generation: id.generation.clone(),
                    },
                ))
        }
        fn create(
            &self,
            _: crate::session::state_authority::SessionIdentity,
        ) -> Result<
            std::sync::Arc<dyn crate::session::state_authority::NativeSession>,
            crate::session::state_authority::AuthorityError,
        > {
            unreachable!()
        }
        fn open(
            &self,
            _: crate::session::state_authority::SessionIdentity,
        ) -> Result<
            std::sync::Arc<dyn crate::session::state_authority::NativeSession>,
            crate::session::state_authority::AuthorityError,
        > {
            Ok(self.source.clone())
        }
        fn publish_fork(
            &self,
            id: crate::session::state_authority::SessionIdentity,
            records: Vec<crate::session::state_authority::ReplayRecord>,
        ) -> Result<
            std::sync::Arc<dyn crate::session::state_authority::NativeSession>,
            crate::session::state_authority::AuthorityError,
        > {
            self.published
                .lock()
                .unwrap()
                .push((id.clone(), records.clone()));
            Ok(std::sync::Arc::new(StaticSession { id, records }))
        }
        fn tombstone(
            &self,
            _: crate::session::state_authority::SessionIdentity,
        ) -> Result<(), crate::session::state_authority::AuthorityError> {
            unreachable!()
        }
    }

    fn fork_update(
        id: &str,
        text: &str,
        prompt_index: usize,
    ) -> crate::session::state_authority::ReplayRecord {
        let update =
            crate::session::storage::SessionUpdate::Acp(Box::new(acp::SessionNotification::new(
                acp::SessionId::new(id),
                acp::SessionUpdate::UserMessageChunk(
                    acp::ContentChunk::new(acp::ContentBlock::Text(acp::TextContent::new(text)))
                        .meta(
                            serde_json::json!({"promptIndex": prompt_index})
                                .as_object()
                                .cloned(),
                        ),
                ),
            )));
        crate::session::state_authority::ReplayRecord::Update(serde_json::to_vec(&update).unwrap())
    }

    #[tokio::test]
    async fn authority_full_and_partial_forks_publish_fresh_generations_without_covered_files() {
        use crate::session::storage::StorageAdapter as _;
        let root = tempfile::TempDir::new().unwrap();
        let storage = JsonlStorageAdapter::with_root(root.path().to_path_buf());
        let source_info = Info {
            id: acp::SessionId::new("source"),
            cwd: "/source".into(),
        };
        storage
            .init_session(
                &source_info,
                crate::session::persistence::default_model_id(),
            )
            .await
            .unwrap();
        let source_dir = storage.session_dir(&source_info);
        for covered in ["updates.jsonl", "chat_history.jsonl", "rewind_points.jsonl"] {
            let _ = std::fs::remove_file(source_dir.join(covered));
        }
        let authority = ForkAuthority {
            source: std::sync::Arc::new(StaticSession {
                id: crate::session::state_authority::SessionIdentity {
                    identity: "source".into(),
                    generation: "source-generation".into(),
                },
                records: vec![
                    fork_update("source", "zero", 0),
                    fork_update("source", "one", 1),
                ],
            }),
            published: std::sync::Mutex::new(Vec::new()),
        };
        for (target, cut, expected) in [("full", None, 2usize), ("partial", Some(0), 1usize)] {
            let target_info = Info {
                id: acp::SessionId::new(target),
                cwd: "/target".into(),
            };
            copy_authority_fork(
                &authority,
                &storage,
                &source_info,
                &target_info,
                CopySessionOptions {
                    target_prompt_index: cut,
                    ..Default::default()
                },
                false,
            )
            .unwrap();
            let published = authority.published.lock().unwrap();
            let (identity, records) = published.last().unwrap();
            assert_eq!(identity.identity, target);
            assert_ne!(identity.generation, "source-generation");
            assert_eq!(
                records
                    .iter()
                    .filter(|r| matches!(
                        r,
                        crate::session::state_authority::ReplayRecord::Update(_)
                    ))
                    .count(),
                expected
            );
        }
        fn assert_absent(path: &std::path::Path) {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    assert!(!matches!(
                        path.file_name().and_then(|x| x.to_str()),
                        Some(
                            "updates.jsonl"
                                | "chat_history.jsonl"
                                | "rewind_points.jsonl"
                                | "compaction_checkpoints"
                        )
                    ));
                    if path.is_dir() {
                        assert_absent(&path);
                    }
                }
            }
        }
        assert_absent(root.path());
    }

    #[test]
    fn test_generate_fork_session_id_format() {
        let fork_id = generate_fork_session_id("abc123");

        // Should be a valid UUIDv7 (36 chars with dashes)
        assert_eq!(
            fork_id.len(),
            36,
            "Fork ID should be a standard UUID length"
        );
        assert!(
            uuid::Uuid::parse_str(&fork_id).is_ok(),
            "Fork ID should be a valid UUID: {}",
            fork_id
        );
    }

    #[test]
    fn test_generate_fork_session_id_uniqueness() {
        // Generate multiple IDs rapidly and ensure they're all unique
        let mut ids = std::collections::HashSet::new();
        for _ in 0..100 {
            let fork_id = generate_fork_session_id("any-source");
            assert_eq!(fork_id.len(), 36);
            ids.insert(fork_id);
        }

        // All 100 should be unique
        assert_eq!(ids.len(), 100, "All generated IDs should be unique");
    }

    #[test]
    fn test_generate_fork_session_id_constant_length() {
        // Forking from already-forked sessions should produce same-length IDs
        let id1 = generate_fork_session_id("019c43b5-c4ae-7190-b058-693e24669ba9");
        let id2 = generate_fork_session_id(&id1); // fork of fork
        let id3 = generate_fork_session_id(&id2); // fork of fork of fork

        assert_eq!(id1.len(), 36);
        assert_eq!(id2.len(), 36);
        assert_eq!(id3.len(), 36);
    }

    #[test]
    fn test_fork_session_request_serialization() {
        let request = ForkSessionRequest {
            source_session_id: "abc123".to_string(),
            source_cwd: "/old/project".to_string(),
            new_cwd: "/new/project".to_string(),
            new_session_id: Some("custom-session-id".to_string()),
            new_model_id: Some("grok-3".to_string()),
            target_prompt_index: None,
            ..Default::default()
        };

        let json = serde_json::to_string(&request).unwrap();
        let deserialized: ForkSessionRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.source_session_id, "abc123");
        assert_eq!(deserialized.source_cwd, "/old/project");
        assert_eq!(deserialized.new_cwd, "/new/project");
        assert_eq!(
            deserialized.new_session_id,
            Some("custom-session-id".to_string())
        );
        assert_eq!(deserialized.new_model_id, Some("grok-3".to_string()));
    }

    #[test]
    fn test_fork_session_request_without_optional_fields() {
        // Test that optional fields default to None when not provided
        let json = r#"{"sourceSessionId":"abc123","sourceCwd":"/old","newCwd":"/new"}"#;
        let deserialized: ForkSessionRequest = serde_json::from_str(json).unwrap();

        assert_eq!(deserialized.source_session_id, "abc123");
        assert_eq!(deserialized.new_session_id, None);
        assert_eq!(deserialized.new_model_id, None);
    }

    #[test]
    fn test_fork_session_response_serialization() {
        let response = ForkSessionResponse {
            new_session_id: "fork-abc123-12345678".to_string(),
            chat_messages_copied: 42,
            updates_copied: 156,
            plan_state_copied: true,
            new_cwd: "/new/project".to_string(),
            parent_session_id: "abc123".to_string(),
            new_model_id: Some("grok-3".to_string()),
        };

        let json = serde_json::to_string(&response).unwrap();
        let deserialized: ForkSessionResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.new_session_id, "fork-abc123-12345678");
        assert_eq!(deserialized.chat_messages_copied, 42);
        assert_eq!(deserialized.updates_copied, 156);
        assert!(deserialized.plan_state_copied);
        assert_eq!(deserialized.new_cwd, "/new/project");
        assert_eq!(deserialized.parent_session_id, "abc123");
        assert_eq!(deserialized.new_model_id, Some("grok-3".to_string()));
    }

    #[test]
    fn test_fork_session_response_without_model_override() {
        let response = ForkSessionResponse {
            new_session_id: "fork-abc123-12345678".to_string(),
            chat_messages_copied: 42,
            updates_copied: 156,
            plan_state_copied: true,
            new_cwd: "/new/project".to_string(),
            parent_session_id: "abc123".to_string(),
            new_model_id: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        // new_model_id should not be present in JSON when None
        assert!(!json.contains("new_model_id"));
    }
}
