// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentCommand {
    pub name: String,
    pub description: String,
    pub input_hint: Option<String>,
    /// Lossless agent command metadata for future command kinds.
    pub meta: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InterjectionReceipt {
    pub interjection_id: Option<String>,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ForkSessionRequest {
    pub source_cwd: PathBuf,
    pub new_cwd: PathBuf,
    pub new_session_id: Option<String>,
    pub new_model_id: Option<String>,
    pub target_prompt_index: Option<usize>,
    pub session_kind: Option<String>,
    pub source_workspace_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkSessionReceipt {
    pub new_session_id: SessionId,
    pub chat_messages_copied: usize,
    pub updates_copied: usize,
    pub plan_state_copied: bool,
    pub new_cwd: PathBuf,
    pub parent_session_id: SessionId,
    pub new_model_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorktreeCopyMode {
    Clean,
    #[default]
    Dirty,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorktreeType {
    #[default]
    Linked,
    Standalone,
    Git,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResumeSessionInWorktreeRequest {
    pub source_cwd: PathBuf,
    #[serde(default)]
    pub copy_mode: WorktreeCopyMode,
    #[serde(default)]
    pub worktree_type: Option<WorktreeType>,
    #[serde(default)]
    pub restore_code: Option<bool>,
    #[serde(default)]
    pub git_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeSessionInWorktreeReceipt {
    pub session_id: SessionId,
    pub worktree_path: PathBuf,
    pub effective_cwd: PathBuf,
    pub remote_restored: bool,
    pub parent_session_id: SessionId,
    pub chat_messages_copied: usize,
    pub updates_copied: usize,
    pub code_restored: bool,
    pub restore_summary: Option<String>,
    pub restore_degree: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowInfo {
    pub name: String,
    pub description: String,
    pub when_to_use: Option<String>,
    pub source: String,
    pub path: Option<PathBuf>,
    /// Lossless workflow entry for forward-compatible fields.
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentSnapshot {
    pub subagent_id: String,
    pub parent_session_id: String,
    pub child_session_id: String,
    pub subagent_type: String,
    pub description: String,
    pub started_at_epoch_ms: u64,
    pub duration_ms: u64,
    pub status: String,
    pub turn_count: Option<u32>,
    pub tool_call_count: Option<u32>,
    pub tokens_used: Option<u64>,
    pub context_window_tokens: Option<u64>,
    pub context_usage_pct: Option<u8>,
    pub tools_used: Option<Vec<String>>,
    pub error_count: Option<u32>,
    pub output: Option<String>,
    pub tool_calls: Option<u32>,
    pub turns: Option<u32>,
    pub worktree_path: Option<PathBuf>,
    pub failure_error: Option<String>,
    pub cancel_reason: Option<String>,
    pub fork_context_source: Option<String>,
    pub fork_parent_prompt_id: Option<String>,
    pub resumed_from: Option<String>,
    /// Lossless snapshot for fields added by newer agents.
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SubagentCancelOutcome {
    Cancelled,
    AlreadyFinished {
        status: String,
    },
    NotFound,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentCancelReceipt {
    pub subagent_id: String,
    pub cancelled: bool,
    pub outcome: Option<SubagentCancelOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize)]
pub struct SessionId(pub(crate) String);

impl<'de> serde::Deserialize<'de> for SessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        if value.is_empty()
            || value.len() > crate::MAX_SESSION_IDENTITY_BYTES
            || value.contains('\0')
        {
            return Err(serde::de::Error::custom("invalid Session identity"));
        }
        Ok(Self(value))
    }
}

impl SessionId {
    pub(crate) const RUNTIME_EVENTS: &'static str = "__origin_runtime__";

    /// Restores an opaque Grok session identifier persisted by the host.
    /// Validation still occurs inside `load_session`; this constructor never
    /// interprets the identifier or exposes Grok protocol types.
    pub fn from_stored(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Reserved journal identity for extension notifications that are not
    /// associated with a session.
    pub fn runtime_events() -> Self {
        Self(Self::RUNTIME_EVENTS.into())
    }
}

/// SDK-owned scheduler request. `task_id == None` creates; otherwise only
/// `interval` and/or `prompt` are updated and the existing phase is retained.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledTaskRequest {
    pub task_id: Option<String>,
    pub interval: Option<String>,
    pub prompt: Option<String>,
    #[serde(default = "sdk_default_true")]
    pub recurring: bool,
    pub durable: Option<bool>,
    pub foreground: Option<bool>,
    #[serde(default)]
    pub fire_immediately: bool,
}
fn sdk_default_true() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledTaskSummary {
    pub id: String,
    pub interval_seconds: u64,
    pub prompt: String,
    pub recurring: bool,
    pub durable: bool,
    pub foreground: bool,
    pub created_at: String,
    pub last_fired_at: Option<String>,
    pub expires_at: Option<String>,
    pub last_subagent: Option<String>,
    pub next_fire_at: String,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ScheduledTaskReceipt {
    pub task: ScheduledTaskSummary,
    pub updated: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteScheduledTaskReceipt {
    pub task_id: String,
    pub deleted: bool,
}
