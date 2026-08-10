// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum LedgerTurnState {
    Pending,
    Completed {
        outcome: TurnOutcome,
        settlement_id: String,
        /// `None` is accepted only for ledgers written before usage-bound
        /// settlements. Such an entry cannot recover an SDK-owned Run effect.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<run::EffectUsage>,
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

/// SDK-owned identity and effective route for an attached Session.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionBinding {
    pub session_id: SessionId,
    pub cwd: PathBuf,
    pub model: String,
    pub reasoning: Option<String>,
    pub harness_digest: Option<HarnessDigest>,
}

/// One command-loop snapshot of an attached Session's replay facts and ledger.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionReplayProbe {
    pub binding: SessionBinding,
    pub requested_after_sequence: u64,
    pub oldest_retained_sequence: u64,
    pub inclusive_end_sequence: u64,
    pub retained_count: usize,
    /// True when events immediately after the requested sequence were evicted.
    pub truncated: bool,
    /// Retained events after the requested sequence, or the retained suffix
    /// when `truncated` is true. Every event is at or before the captured end.
    pub events: Vec<Event>,
    pub ledger: SessionLedger,
}

pub(crate) fn durable_turn_outcome(outcome: TurnOutcome) -> run::SessionTurnOutcome {
    match outcome {
        TurnOutcome::End => run::SessionTurnOutcome::End,
        TurnOutcome::Cancelled => run::SessionTurnOutcome::Cancelled,
        TurnOutcome::MaxTokens => run::SessionTurnOutcome::MaxTokens,
        TurnOutcome::MaxTurnRequests => run::SessionTurnOutcome::MaxTurnRequests,
        TurnOutcome::Refusal => run::SessionTurnOutcome::Refusal,
    }
}

pub(crate) fn measured_session_turn_usage(
    mut usage: run::EffectUsage,
    artifact_bytes: u64,
) -> Result<run::EffectUsage, Error> {
    usage.resources.artifact_bytes = artifact_bytes;
    usage.unknown.remove(&run::ResourceDimension::ArtifactBytes);
    usage.validate()?;
    Ok(usage)
}

pub(crate) fn recovered_session_turn_usage(
    mut usage: run::EffectUsage,
) -> Result<run::EffectUsage, Error> {
    // SessionLedger proves native Turn usage, but cannot prove whether the
    // SDK output artifact was persisted immediately before a crash. Keep the
    // whole dimension explicitly unknown instead of fabricating input-only
    // accounting.
    usage.resources.artifact_bytes = 0;
    usage.unknown.insert(run::ResourceDimension::ArtifactBytes);
    usage.validate()?;
    Ok(usage)
}
