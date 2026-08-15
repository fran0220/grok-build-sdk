// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use crate::*;

/// Hook events supported by the bundled agent. This SDK type deliberately does
/// not expose the shell's hook implementation types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHookEvent {
    SessionStart,
    SessionEnd,
    Stop,
    StopFailure,
    PreToolUse,
    PostToolUse,
    PostToolUseFailure,
    PermissionDenied,
    UserPromptSubmit,
    Notification,
    SubagentStart,
    SubagentStop,
    SubagentEnd,
    PreCompact,
    PostCompact,
}
impl AgentHookEvent {
    pub(crate) fn registration_name(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::SessionEnd => "SessionEnd",
            Self::Stop => "Stop",
            Self::StopFailure => "StopFailure",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::PostToolUseFailure => "PostToolUseFailure",
            Self::PermissionDenied => "PermissionDenied",
            Self::UserPromptSubmit => "UserPromptSubmit",
            Self::Notification => "Notification",
            Self::SubagentStart => "SubagentStart",
            Self::SubagentStop => "SubagentStop",
            Self::SubagentEnd => "SubagentEnd",
            Self::PreCompact => "PreCompact",
            Self::PostCompact => "PostCompact",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentHookInvocation {
    pub event: AgentHookEvent,
    pub callback_id: String,
    pub session_id: String,
    pub cwd: Option<PathBuf>,
    pub workspace_root: Option<PathBuf>,
    pub timestamp: Option<String>,
    pub prompt_id: Option<String>,
    pub permission_mode: Option<String>,
    /// Present for tool events.
    pub tool_name: Option<String>,
    pub tool_use_id: Option<String>,
    pub tool_input: Option<serde_json::Value>,
    pub tool_result: Option<serde_json::Value>,
    /// Complete reverse-channel payload, including fields added by newer agents.
    pub raw: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHookDecision {
    #[default]
    Continue,
    Deny,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentHookResponse {
    pub decision: AgentHookDecision,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_message: Option<String>,
    #[serde(rename = "continue", skip_serializing_if = "Option::is_none")]
    pub continue_: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_context: Option<String>,
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("agent hook failed: {message}")]
pub struct AgentHookError {
    pub message: String,
}

#[async_trait::async_trait]
pub trait AgentHookHandler: Send + Sync + 'static {
    async fn handle(
        &self,
        invocation: AgentHookInvocation,
    ) -> Result<AgentHookResponse, AgentHookError>;
}

#[derive(Clone)]
pub struct AgentHookRegistration {
    pub callback_id: String,
    pub event: AgentHookEvent,
    pub matcher: Option<String>,
    /// Shell wire timeout in seconds. Must be finite, positive, and at most 600.
    pub timeout: Option<f64>,
    pub handler: Arc<dyn AgentHookHandler>,
}
