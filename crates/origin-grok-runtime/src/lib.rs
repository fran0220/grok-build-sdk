// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Send/Sync, fail-closed in-process façade for Origin's bundled Grok fork.
//! ACP, Grok, and JSON implementation types are confined to the private module.

mod private;

use std::path::PathBuf;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct ModelSpec {
    pub id: String,
    pub context_window: u64,
    pub api_backend: ApiBackend,
    pub supports_reasoning: bool,
    pub default_reasoning: Option<String>,
    pub reasoning_options: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum ApiBackend {
    #[default]
    ChatCompletions,
    Responses,
}

#[derive(Clone)]
pub struct RuntimeConfig {
    pub endpoint: String,
    pub api_key: String,
    pub grok_home: PathBuf,
    pub session_storage: PathBuf,
    pub models: Vec<ModelSpec>,
}

#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub cwd: PathBuf,
    pub model: String,
    pub reasoning: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionId(String);
impl SessionId {
    /// Restores an opaque Grok session identifier persisted by the host.
    /// Validation still occurs inside `load_session`; this constructor never
    /// interprets the identifier or exposes Grok protocol types.
    pub fn from_stored(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub session_id: SessionId,
    pub sequence: u64,
    pub turn_id: Option<String>,
    pub update: EventUpdate,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventUpdate {
    SessionStarted,
    UserText(String),
    AssistantText(String),
    ThoughtText(String),
    ToolStart(ToolEvent),
    ToolUpdate(ToolEvent),
    Plan { summary: String },
    AvailableCommands(Vec<RuntimeCommand>),
    ModeChanged(String),
    ConfigOptions(Vec<RuntimeConfigOption>),
    SessionInfo { title: Option<String> },
    Unknown { tag: String },
    TurnFinished(TurnOutcome),
    SessionClosed,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeCommand {
    pub name: String,
    pub description: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfigOption {
    pub id: String,
    pub name: String,
    pub category: Option<String>,
    pub value: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolEvent {
    pub id: String,
    pub title: String,
    pub kind: String,
    pub status: String,
    pub raw_input: Option<String>,
    pub raw_output: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TurnOutcome {
    End,
    Cancelled,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptReceipt {
    pub outcome: TurnOutcome,
    /// Every event through this per-session sequence is retained and queryable
    /// before `prompt` returns.
    pub final_sequence: u64,
    /// Position on the native session's active conversation timeline. Rewinds
    /// retain prompts below their target and later Turns may reuse a discarded
    /// position; callers pair this with the exact prompt digest from the ledger.
    pub runtime_prompt_index: u64,
    /// Stable receipt from the fork-owned durable Turn ledger.
    pub settlement_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum LedgerTurnState {
    Pending,
    Completed {
        outcome: TurnOutcome,
        settlement_id: String,
    },
    Discarded,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionLedgerEntry {
    pub turn_id: String,
    pub prompt_digest: String,
    pub runtime_prompt_index: u64,
    pub state: LedgerTurnState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionLedger {
    pub entries: Vec<SessionLedgerEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConversationRewindReceipt {
    pub operation_id: String,
    pub session_id: String,
    pub target_prompt_index: u64,
    pub target_turn_id: String,
    pub target_prompt_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_prompt_digest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum ConversationRewindStatus {
    Absent,
    Pending {
        operation_id: String,
        session_id: String,
        target_prompt_index: u64,
        target_turn_id: String,
        target_prompt_digest: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_prompt_digest: Option<String>,
    },
    Applied {
        receipt: ConversationRewindReceipt,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewindPoint {
    pub prompt_index: u64,
    /// Origin-only canonical digest of the exact user prompt at this native
    /// timeline position. Hosts use it to reject stale checkpoints after a
    /// rewind reuses a prompt index on a new branch.
    pub prompt_digest: Option<String>,
    pub created_at: String,
    pub file_snapshots: u64,
    pub has_file_changes: bool,
    pub prompt_preview: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid runtime configuration: {0}")]
    InvalidConfig(String),
    #[error("runtime operation failed: {0}")]
    Operation(String),
    #[error("runtime has shut down")]
    Shutdown,
}

#[derive(Clone)]
pub struct Runtime {
    inner: private::Runtime,
}
impl Runtime {
    pub async fn start(
        config: RuntimeConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start(config)
            .await
            .map(|(inner, events)| (Self { inner }, events))
    }
    pub async fn create_session(&self, config: SessionConfig) -> Result<SessionId, Error> {
        self.inner.create_session(config).await
    }
    pub async fn load_session(&self, id: SessionId, config: SessionConfig) -> Result<(), Error> {
        self.inner.load_session(id, config).await
    }
    pub async fn prompt(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<PromptReceipt, Error> {
        self.inner.prompt(id, turn_id.into(), text.into()).await
    }
    /// Returns retained events whose sequence is strictly greater than
    /// `after_sequence`. Unknown or currently unloaded sessions fail closed.
    pub async fn events_after(
        &self,
        id: &SessionId,
        after_sequence: u64,
    ) -> Result<Vec<Event>, Error> {
        self.inner.events_after(id, after_sequence).await
    }
    pub async fn cancel(&self, id: &SessionId) -> Result<(), Error> {
        self.inner.cancel(id).await
    }
    pub async fn session_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        self.inner.session_ledger(id).await
    }
    pub async fn mark_turn_discarded(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        prompt_digest: impl Into<String>,
        runtime_prompt_index: u64,
    ) -> Result<(), Error> {
        self.inner
            .mark_turn_discarded(
                id,
                turn_id.into(),
                prompt_digest.into(),
                runtime_prompt_index,
            )
            .await
    }
    /// Changes only the Origin sampling route for an existing conversation.
    /// Unlike generic ACP model switching, this never rebuilds the harness or
    /// rewrites the system prompt.
    pub async fn set_route(
        &self,
        id: &SessionId,
        model: impl Into<String>,
        reasoning: Option<String>,
    ) -> Result<(), Error> {
        self.inner.set_route(id, model.into(), reasoning).await
    }
    pub async fn rewind_points(&self, id: &SessionId) -> Result<Vec<RewindPoint>, Error> {
        self.inner.rewind_points(id).await
    }
    pub async fn rewind_conversation(
        &self,
        id: &SessionId,
        operation_id: impl Into<String>,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.inner
            .rewind_conversation(id, operation_id.into(), target_prompt_index)
            .await
    }
    /// Removes the exact product-unsettled tail Turn after a Forge host
    /// restart. The native Turn may still be pending or may have completed
    /// before Forge durably recorded its settlement; unlike a user rewind,
    /// this requires the full ledger identity.
    pub async fn rewind_unsettled_turn(
        &self,
        id: &SessionId,
        operation_id: impl Into<String>,
        turn_id: impl Into<String>,
        prompt_digest: impl Into<String>,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.inner
            .rewind_unsettled_turn(
                id,
                operation_id.into(),
                turn_id.into(),
                prompt_digest.into(),
                target_prompt_index,
            )
            .await
    }
    pub async fn rewind_status(
        &self,
        id: &SessionId,
        operation_id: &str,
    ) -> Result<ConversationRewindStatus, Error> {
        self.inner.rewind_status(id, operation_id).await
    }
    pub async fn unload_session(&self, id: SessionId) -> Result<(), Error> {
        self.inner.unload_session(id).await
    }
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.inner.shutdown().await
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceProvenance {
    pub upstream_release: &'static str,
    pub fork_commit: &'static str,
    pub upstream_source_rev: &'static str,
    pub facade_version: &'static str,
    pub dirty: bool,
}
pub fn source_provenance() -> SourceProvenance {
    SourceProvenance {
        upstream_release: "1.0.0",
        fork_commit: env!("ORIGIN_GROK_BUILD_COMMIT"),
        upstream_source_rev: include_str!("../../../SOURCE_REV").trim(),
        facade_version: env!("CARGO_PKG_VERSION"),
        dirty: match env!("ORIGIN_GROK_BUILD_DIRTY") {
            "true" => true,
            "false" => false,
            _ => panic!("build script emitted an invalid dirty marker"),
        },
    }
}

pub fn prompt_digest(text: &str) -> String {
    xai_grok_shell::origin_runtime::prompt_digest(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use xai_grok_test_support::{MockInferenceServer, ScriptedResponse, SseEvent};

    fn runtime_config(root: &TempDir, endpoint: String) -> RuntimeConfig {
        RuntimeConfig {
            endpoint,
            api_key: "test-key".into(),
            grok_home: root.path().join("grok"),
            session_storage: root.path().join("sessions"),
            models: vec![ModelSpec {
                id: "test-model".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: false,
                default_reasoning: None,
                reasoning_options: Vec::new(),
            }],
        }
    }

    fn session_config(cwd: PathBuf) -> SessionConfig {
        SessionConfig {
            cwd,
            model: "test-model".into(),
            reasoning: None,
        }
    }

    fn request_with_user_marker(server: &MockInferenceServer, marker: &str) -> serde_json::Value {
        server
            .requests()
            .into_iter()
            .filter(|entry| {
                entry.path.contains("chat/completions") || entry.path.contains("responses")
            })
            .filter_map(|entry| entry.body)
            .find(|body| {
                body.get("tools")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|tools| !tools.is_empty())
                    && body.get("tool_choice").is_none()
                    && body
                        .get("messages")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|message| message.get("content"))
                        .any(|content| content.as_str().is_some_and(|text| text.contains(marker)))
            })
            .expect("foreground inference request with marker")
    }

    fn message_prefix_is_unchanged(earlier: &serde_json::Value, later: &serde_json::Value) -> bool {
        let earlier = earlier
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .expect("earlier chat messages");
        let later = later
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .expect("later chat messages");
        later.starts_with(earlier)
    }

    fn chat_tool_call(call_id: &str, name: &str, arguments: &str) -> ScriptedResponse {
        let tool_calls = vec![serde_json::json!({
            "index": 0,
            "id": call_id,
            "type": "function",
            "function": { "name": name, "arguments": arguments }
        })];
        ScriptedResponse::sse(vec![
            SseEvent::data(
                serde_json::json!({
                    "id": "chatcmpl-origin-tool",
                    "object": "chat.completion.chunk",
                    "created": 1234567890,
                    "model": "test-model",
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": null,
                            "tool_calls": tool_calls
                        },
                        "finish_reason": null
                    }]
                })
                .to_string(),
            ),
            SseEvent::data(
                serde_json::json!({
                    "id": "chatcmpl-origin-tool",
                    "object": "chat.completion.chunk",
                    "created": 1234567890,
                    "model": "test-model",
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "tool_calls"
                    }],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 20,
                        "total_tokens": 30
                    }
                })
                .to_string(),
            ),
            SseEvent::data("[DONE]"),
        ])
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_unrestricted_mode_uses_local_filesystem_without_host_callbacks() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        server.enqueue_response(
            "/v1/chat/completions",
            chat_tool_call(
                "write-1",
                "search_replace",
                r#"{"file_path":"note.txt","old_string":"before","new_string":"after"}"#,
            ),
        );
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        std::fs::write(workspace.join("note.txt"), "before").expect("fixture");

        // RuntimeConfig intentionally has no host callbacks. Advertising no
        // ACP FS/terminal capabilities selects LocalFs and TerminalRunner.
        let (runtime, _) = Runtime::start(runtime_config(&root, server.url()))
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("session starts");
        let receipt = runtime
            .prompt(&session, "turn-write", "replace before with after")
            .await
            .expect("unrestricted tool turn succeeds without a host callback");

        assert_eq!(receipt.outcome, TurnOutcome::End);
        assert_eq!(
            std::fs::read_to_string(workspace.join("note.txt")).expect("edited file"),
            "after"
        );
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test]
    async fn rejects_missing_endpoint_before_starting_worker() {
        let root = TempDir::new().expect("temp root");
        let result = Runtime::start(runtime_config(&root, String::new())).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn provenance_is_exact_and_never_unknown() {
        let provenance = source_provenance();
        assert_eq!(provenance.upstream_release, "1.0.0");
        assert_eq!(provenance.facade_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(provenance.fork_commit.len(), 40);
        assert!(
            provenance
                .fork_commit
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        );
        assert_eq!(provenance.upstream_source_rev.len(), 40);
        assert!(
            provenance
                .upstream_source_rev
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        );
    }

    #[test]
    fn legacy_rewind_receipts_without_exact_target_identity_fail_closed() {
        let legacy = serde_json::json!({
            "operation_id": "legacy-operation",
            "session_id": "legacy-session",
            "target_prompt_index": 2
        });

        assert!(serde_json::from_value::<ConversationRewindReceipt>(legacy).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_real_agent_outside_local_set_and_closes_session() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");

        let (runtime, mut events) = Runtime::start(runtime_config(&root, server.url()))
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(SessionConfig {
                cwd: workspace.clone(),
                model: "test-model".into(),
                reasoning: None,
            })
            .await
            .expect("session starts");
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            runtime.prompt(&session, "turn-1", "reply briefly"),
        )
        .await
        .expect("turn timeout")
        .expect("turn succeeds");
        assert_eq!(outcome.outcome, TurnOutcome::End);
        let retained = runtime
            .events_after(&session, 0)
            .await
            .expect("events are retained");
        assert_eq!(
            retained.last().map(|event| event.sequence),
            Some(outcome.final_sequence)
        );
        assert!(matches!(
            retained.last().map(|event| &event.update),
            Some(EventUpdate::TurnFinished(TurnOutcome::End))
        ));
        assert!(retained.iter().any(|event| {
            event.turn_id.as_deref() == Some("turn-1")
                && matches!(&event.update, EventUpdate::UserText(text) if text == "reply briefly")
        }));

        let mut assistant = String::new();
        while let Ok(Some(event)) =
            tokio::time::timeout(std::time::Duration::from_millis(250), events.recv()).await
        {
            let finished = matches!(event.update, EventUpdate::TurnFinished(_));
            if let EventUpdate::AssistantText(text) = &event.update {
                assert_eq!(event.turn_id.as_deref(), Some("turn-1"));
                assistant.push_str(text);
            }
            if finished {
                assert_eq!(event.turn_id.as_deref(), Some("turn-1"));
                break;
            }
        }
        assert!(assistant.contains("Echo:"), "assistant output: {assistant}");
        runtime
            .unload_session(session.clone())
            .await
            .expect("session closes");
        assert!(
            runtime
                .events_after(&session, outcome.final_sequence)
                .await
                .is_err()
        );
        runtime
            .load_session(session.clone(), session_config(workspace))
            .await
            .expect("the same durable session id remains resumable");
        let after_turn = runtime
            .events_after(&session, outcome.final_sequence)
            .await
            .expect("retained close event is recoverable after reload");
        assert!(matches!(
            after_turn.as_slice(),
            [Event {
                update: EventUpdate::SessionClosed,
                ..
            }]
        ));
        assert!(runtime.events_after(&session, u64::MAX).await.is_err());
        assert!(
            runtime
                .events_after(&SessionId::from_stored("missing"), 0)
                .await
                .is_err()
        );
        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn route_changes_preserve_the_prompt_and_existing_conversation() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config = runtime_config(&root, server.url());
        config.models = vec![
            ModelSpec {
                id: "fast-route".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: true,
                default_reasoning: Some("high".into()),
                reasoning_options: vec!["high".into()],
            },
            ModelSpec {
                id: "advanced-route".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: true,
                default_reasoning: Some("xhigh".into()),
                reasoning_options: vec!["xhigh".into()],
            },
        ];
        let (runtime, _events) = Runtime::start(config).await.expect("runtime starts");
        let session = runtime
            .create_session(SessionConfig {
                cwd: workspace,
                model: "fast-route".into(),
                reasoning: Some("high".into()),
            })
            .await
            .expect("session starts");

        runtime
            .prompt(&session, "turn-fast-1", "route-marker-fast-1")
            .await
            .expect("first fast turn");
        let fast_before = request_with_user_marker(&server, "route-marker-fast-1");
        let system_prompt = fast_before["messages"][0]["content"]
            .as_str()
            .expect("system prompt")
            .as_bytes()
            .to_vec();
        assert_eq!(fast_before["model"], "fast-route");
        assert_eq!(fast_before["reasoning_effort"], "high");

        runtime
            .set_route(&session, "advanced-route", Some("xhigh".into()))
            .await
            .expect("advanced route applies");
        runtime
            .prompt(&session, "turn-advanced", "route-marker-advanced")
            .await
            .expect("advanced turn");
        let advanced = request_with_user_marker(&server, "route-marker-advanced");
        assert_eq!(advanced["model"], "advanced-route");
        assert_eq!(advanced["reasoning_effort"], "xhigh");
        assert_eq!(
            advanced["messages"][0]["content"]
                .as_str()
                .expect("system prompt")
                .as_bytes(),
            system_prompt
        );
        assert!(message_prefix_is_unchanged(&fast_before, &advanced));

        runtime
            .set_route(&session, "fast-route", Some("high".into()))
            .await
            .expect("fast route reapplies");
        runtime
            .prompt(&session, "turn-fast-2", "route-marker-fast-2")
            .await
            .expect("second fast turn");
        let fast_after = request_with_user_marker(&server, "route-marker-fast-2");
        assert_eq!(fast_after["model"], "fast-route");
        assert_eq!(fast_after["reasoning_effort"], "high");
        assert_eq!(
            fast_after["messages"][0]["content"]
                .as_str()
                .expect("system prompt")
                .as_bytes(),
            system_prompt
        );
        assert!(message_prefix_is_unchanged(&advanced, &fast_after));

        runtime.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rewind_receipt_recovers_after_native_and_ledger_commit_without_reexecution() {
        use sha2::Digest as _;

        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let config = runtime_config(&root, server.url());
        let (runtime, _events) = Runtime::start(config.clone())
            .await
            .expect("runtime starts");
        let session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("session starts");
        let other_session = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("other session starts");
        runtime
            .prompt(&session, "turn-0", "prompt zero")
            .await
            .expect("first turn");
        runtime
            .prompt(&session, "turn-1", "prompt one")
            .await
            .expect("second turn");

        let operation_id = "restart-rewind-operation";
        assert!(matches!(
            runtime
                .rewind_status(&session, "never-started-rewind")
                .await
                .expect("absent rewind status"),
            ConversationRewindStatus::Absent
        ));
        let rewind_root = root.path().join("sessions/origin-rewind-receipts");
        std::fs::create_dir_all(&rewind_root).expect("rewind receipt root");
        let digest = format!("{:x}", sha2::Sha256::digest(operation_id.as_bytes()));
        let ledger_before = runtime
            .session_ledger(&session)
            .await
            .expect("ledger before rewind");
        let target_entry = &ledger_before.entries[1];
        std::fs::write(
            rewind_root.join(format!("{digest}.intent.json")),
            serde_json::to_vec(&serde_json::json!({
                "operation_id": operation_id,
                "session_id": session.as_str(),
                "target_prompt_index": 1,
                "target_turn_id": target_entry.turn_id.clone(),
                "target_prompt_digest": target_entry.prompt_digest.clone(),
                "recovery_turn_id": null,
                "recovery_prompt_digest": null
            }))
            .expect("intent json"),
        )
        .expect("simulate a durable intent before native execution");
        assert!(matches!(
            runtime
                .rewind_status(&session, operation_id)
                .await
                .expect("pre-effect pending status"),
            ConversationRewindStatus::Pending { .. }
        ));
        let first = runtime
            .rewind_conversation(&session, operation_id, 1)
            .await
            .expect("first rewind");
        assert_eq!(first.target_prompt_index, 1);
        assert!(matches!(
            runtime
                .rewind_status(&session, operation_id)
                .await
                .expect("receipt status"),
            ConversationRewindStatus::Applied { receipt } if receipt == first
        ));
        assert!(
            runtime
                .rewind_status(&other_session, operation_id)
                .await
                .is_err(),
            "a global operation id cannot expose another session's receipt"
        );
        assert_eq!(
            runtime
                .rewind_conversation(&session, operation_id, 1)
                .await
                .expect("receipt replay"),
            first
        );
        assert!(
            runtime
                .rewind_conversation(&session, operation_id, 0)
                .await
                .is_err(),
            "an operation identity cannot drift to another target"
        );
        std::fs::write(
            rewind_root.join(format!("{digest}.intent.json")),
            serde_json::to_vec(&serde_json::json!({
                "operation_id": operation_id,
                "session_id": session.as_str(),
                "target_prompt_index": 1,
                "target_turn_id": first.target_turn_id.clone(),
                "target_prompt_digest": first.target_prompt_digest.clone(),
                "recovery_turn_id": null,
                "recovery_prompt_digest": null
            }))
            .expect("intent json"),
        )
        .expect("restore stale intent after receipt publication");
        assert!(matches!(
            runtime
                .rewind_status(&session, operation_id)
                .await
                .expect("receipt wins over stale intent"),
            ConversationRewindStatus::Applied { receipt } if receipt == first
        ));
        runtime
            .prompt(&session, "turn-1-reused", "prompt one")
            .await
            .expect("replacement turn reuses the discarded prompt index and text");
        let reused_operation_id = "reused-index-restart-rewind";
        let reused_digest = format!("{:x}", sha2::Sha256::digest(reused_operation_id.as_bytes()));
        let reused_ledger = runtime
            .session_ledger(&session)
            .await
            .expect("ledger after reused prompt index");
        let reused_target = reused_ledger
            .entries
            .last()
            .expect("replacement ledger entry");
        assert_eq!(reused_target.runtime_prompt_index, 1);
        assert_eq!(reused_target.prompt_digest, first.target_prompt_digest);
        assert_ne!(reused_target.turn_id, first.target_turn_id);
        std::fs::write(
            rewind_root.join(format!("{reused_digest}.intent.json")),
            serde_json::to_vec(&serde_json::json!({
                "operation_id": reused_operation_id,
                "session_id": session.as_str(),
                "target_prompt_index": 1,
                "target_turn_id": reused_target.turn_id.clone(),
                "target_prompt_digest": reused_target.prompt_digest.clone(),
                "recovery_turn_id": null,
                "recovery_prompt_digest": null
            }))
            .expect("reused intent json"),
        )
        .expect("persist reused-index intent before native execution");
        let reused_receipt = runtime
            .rewind_conversation(&session, reused_operation_id, 1)
            .await
            .expect("reused-index rewind targets the replacement turn");
        assert_eq!(reused_receipt.target_turn_id, reused_target.turn_id);
        std::fs::write(
            rewind_root.join(format!("{reused_digest}.intent.json")),
            serde_json::to_vec(&serde_json::json!({
                "operation_id": reused_operation_id,
                "session_id": session.as_str(),
                "target_prompt_index": 1,
                "target_turn_id": reused_receipt.target_turn_id.clone(),
                "target_prompt_digest": reused_receipt.target_prompt_digest.clone(),
                "recovery_turn_id": null,
                "recovery_prompt_digest": null
            }))
            .expect("post-effect reused intent json"),
        )
        .expect("simulate crash after reused-index effect and before receipt publication");
        runtime
            .unload_session(session.clone())
            .await
            .expect("session unloads");
        runtime
            .unload_session(other_session)
            .await
            .expect("other session unloads");
        runtime.shutdown().await.expect("first runtime shuts down");

        std::fs::remove_file(rewind_root.join(format!("{reused_digest}.json")))
            .expect("simulate crash before reused-index receipt publication");

        let (restarted, _events) = Runtime::start(config).await.expect("runtime restarts");
        restarted
            .load_session(session.clone(), session_config(workspace))
            .await
            .expect("rewound session reloads");
        assert!(matches!(
            restarted
                .rewind_status(&session, reused_operation_id)
                .await
                .expect("pending status"),
            ConversationRewindStatus::Pending {
                target_prompt_index: 1,
                target_turn_id,
                ..
            } if target_turn_id == reused_receipt.target_turn_id
        ));
        let recovered = restarted
            .rewind_conversation(&session, reused_operation_id, 1)
            .await
            .expect("missing receipt is reconstructed");
        assert_eq!(recovered, reused_receipt);
        let ledger = restarted
            .session_ledger(&session)
            .await
            .expect("ledger loads");
        assert!(matches!(
            ledger.entries[0].state,
            LedgerTurnState::Completed { .. }
        ));
        assert!(matches!(
            ledger.entries[1].state,
            LedgerTurnState::Discarded
        ));
        assert!(matches!(
            ledger.entries[2].state,
            LedgerTurnState::Discarded
        ));
        assert_eq!(restarted.rewind_points(&session).await.unwrap().len(), 1);
        restarted.shutdown().await.expect("runtime shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_sessions_cancel_close_and_shutdown_are_reconciled() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = MockInferenceServer::start().await.expect("mock server");
        let root = TempDir::new().expect("temp root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let (runtime, mut events) = Runtime::start(runtime_config(&root, server.url()))
            .await
            .expect("runtime starts");

        let first = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("first session");
        let second = runtime
            .create_session(session_config(workspace.clone()))
            .await
            .expect("second session");
        server.hold_agent_completions();
        let first_prompt = tokio::spawn({
            let runtime = runtime.clone();
            let first = first.clone();
            async move { runtime.prompt(&first, "first-turn", "first").await }
        });
        let second_prompt = tokio::spawn({
            let runtime = runtime.clone();
            let second = second.clone();
            async move { runtime.prompt(&second, "second-turn", "second").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        runtime.cancel(&first).await.expect("active prompt cancels");
        runtime
            .unload_session(second.clone())
            .await
            .expect("active session closes after cancellation");
        server.release_agent_completions();
        let first_outcome = first_prompt
            .await
            .expect("first prompt joins")
            .expect("settles");
        assert_eq!(first_outcome.outcome, TurnOutcome::Cancelled);
        let second_outcome = second_prompt
            .await
            .expect("second prompt joins")
            .expect("settles");
        assert_eq!(second_outcome.outcome, TurnOutcome::Cancelled);
        runtime.unload_session(first).await.expect("first unloads");

        let mut by_session = std::collections::HashMap::<String, Vec<u64>>::new();
        while let Ok(event) = events.try_recv() {
            by_session
                .entry(event.session_id.as_str().to_owned())
                .or_default()
                .push(event.sequence);
        }
        for sequences in by_session.values() {
            assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
        }
        assert_eq!(by_session.len(), 2);

        runtime.shutdown().await.expect("worker joins");
        runtime.shutdown().await.expect("shutdown is idempotent");
        assert!(matches!(
            runtime.create_session(session_config(workspace)).await,
            Err(Error::Shutdown)
        ));
    }
}
