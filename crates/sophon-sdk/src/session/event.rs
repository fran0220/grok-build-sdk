// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub session_id: SessionId,
    pub sequence: u64,
    pub turn_id: Option<String>,
    pub timestamp_ms: u64,
    pub replay: bool,
    pub update: EventUpdate,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EventUpdate {
    SessionStarted,
    UserText(String),
    AssistantText(String),
    ThoughtText(String),
    ToolStart(ToolEvent),
    ToolUpdate(ToolEvent),
    Plan {
        summary: String,
    },
    AvailableCommands(Vec<RuntimeCommand>),
    ModeChanged(String),
    ConfigOptions(Vec<RuntimeConfigOption>),
    SessionInfo {
        title: Option<String>,
    },
    McpServerStatus(McpServerStatusEvent),
    McpTaskStatus(McpTaskStatusEvent),
    McpToolsChanged(McpToolsChangedEvent),
    McpInitializationProgress(McpInitializationProgress),
    /// Redacted catalog update. Transport targets, headers, environment and
    /// setup values are intentionally omitted.
    McpServersChanged(Vec<McpServerSummary>),
    Unknown {
        tag: String,
        /// Lossless JSON representation of the agent update that this version of
        /// the SDK does not yet model as a typed variant.
        payload: serde_json::Value,
        /// Original JSON encoding retained for hosts that need byte-for-byte
        /// forwarding or deferred decoding.
        raw: String,
    },
    TurnFinished(TurnOutcome),
    SessionClosed,
    Extension {
        method: String,
        payload: serde_json::Value,
        raw: String,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeCommand {
    pub name: String,
    pub description: String,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeConfigOption {
    pub id: String,
    pub name: String,
    pub category: Option<String>,
    pub value: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// Provider-derived usage bound into `settlement_id`. Unknown dimensions
    /// remain explicit and are never interpreted as zero.
    pub usage: run::EffectUsage,
}
