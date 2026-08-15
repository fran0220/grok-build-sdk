// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

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
    /// SDK-owned canonical digest of the exact user prompt at this native
    /// timeline position. Hosts use it to reject stale checkpoints after a
    /// rewind reuses a prompt index on a new branch.
    pub prompt_digest: Option<String>,
    pub created_at: String,
    pub file_snapshots: u64,
    pub has_file_changes: bool,
    pub prompt_preview: Option<String>,
}

pub(crate) fn rewind_receipt_proves_turn_not_applied(
    receipt: &ConversationRewindReceipt,
    entry: &SessionLedgerEntry,
    turn_id: &str,
    prompt_digest: &str,
) -> bool {
    receipt.target_prompt_index == entry.runtime_prompt_index
        && receipt.target_turn_id == turn_id
        && receipt.target_prompt_digest == prompt_digest
        && receipt.recovery_turn_id.as_deref() == Some(turn_id)
        && receipt.recovery_prompt_digest.as_deref() == Some(prompt_digest)
}
