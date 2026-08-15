// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Typed observation and recovery facts for native Session compaction.
//!
//! The protocol deliberately carries only identities, bounded enums, content
//! digests, and exact sizes. Conversation text, prompts, summaries, tool
//! payloads, credentials, provider headers, paths, and raw JSON are not
//! representable at this boundary.

use crate::{
    SessionGeneration, SessionId, SessionObjectId,
    run::{IterationId, OperationId, RunId},
};
use sha2::{Digest as _, Sha256};

pub const MAX_COMPACTION_ID_BYTES: usize = 80;
pub const MAX_COMPACTION_TURN_ID_BYTES: usize = 512;

/// Canonical root digest domain. The canonical bytes are a length-delimited
/// sequence of the path discriminator and the messages, ordinary tools,
/// hosted tools, and effective model-parameter leaf facts.
pub const COMPACTION_INPUT_DIGEST_DOMAIN: &str = "grok-build-sdk.session-compaction.input.v1";
pub const COMPACTION_INPUT_MESSAGES_DIGEST_DOMAIN: &str =
    "grok-build-sdk.session-compaction.input.messages.v1";
pub const COMPACTION_INPUT_TOOLS_DIGEST_DOMAIN: &str =
    "grok-build-sdk.session-compaction.input.tools.v1";
pub const COMPACTION_INPUT_HOSTED_TOOLS_DIGEST_DOMAIN: &str =
    "grok-build-sdk.session-compaction.input.hosted-tools.v1";
pub const COMPACTION_INPUT_MODEL_DIGEST_DOMAIN: &str =
    "grok-build-sdk.session-compaction.input.model-parameters.v1";
/// Canonical digest domain for the model-produced summary bytes.
pub const COMPACTION_SUMMARY_DIGEST_DOMAIN: &str = "grok-build-sdk.session-compaction.summary.v1";
/// Canonical digest domain for the finalized installed conversation state.
pub const COMPACTION_STATE_DIGEST_DOMAIN: &str = "grok-build-sdk.session-compaction.state.v1";
/// Canonical digest domain for the exact serialized checkpoint object payload.
pub const COMPACTION_CHECKPOINT_DIGEST_DOMAIN: &str =
    "grok-build-sdk.session-compaction.checkpoint.v1";
const COMPACTION_OBSERVATION_DIGEST_DOMAIN: &str =
    "grok-build-sdk.session-compaction.observation.v1";

fn valid_text(value: &str, max: usize) -> Result<(), CompactionContractError> {
    if value.is_empty() || value.len() > max || value.contains('\0') {
        Err(CompactionContractError::Invalid)
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompactionContractError {
    #[error("invalid compaction contract")]
    Invalid,
}

/// SDK-minted identity for one exact in-flight compaction request.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct CompactionId(String);

impl CompactionId {
    pub fn from_stored(value: impl Into<String>) -> Result<Self, CompactionContractError> {
        let value = value.into();
        valid_text(&value, MAX_COMPACTION_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn mint() -> Self {
        Self(format!("cmp-{}", uuid::Uuid::new_v4()))
    }

    pub(crate) fn validate(&self) -> Result<(), CompactionContractError> {
        valid_text(&self.0, MAX_COMPACTION_ID_BYTES)
    }
}

impl TryFrom<String> for CompactionId {
    type Error = CompactionContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_stored(value)
    }
}

impl From<CompactionId> for String {
    fn from(value: CompactionId) -> Self {
        value.0
    }
}

/// Bounded Turn correlation. It is separate from Session and Run identities so
/// one namespace cannot accidentally be substituted for another.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct CompactionTurnId(String);

impl CompactionTurnId {
    pub fn new(value: impl Into<String>) -> Result<Self, CompactionContractError> {
        let value = value.into();
        valid_text(&value, MAX_COMPACTION_TURN_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn validate(&self) -> Result<(), CompactionContractError> {
        valid_text(&self.0, MAX_COMPACTION_TURN_ID_BYTES)
    }
}

impl TryFrom<String> for CompactionTurnId {
    type Error = CompactionContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CompactionTurnId> for String {
    fn from(value: CompactionTurnId) -> Self {
        value.0
    }
}

/// Ownership is structurally all-or-none. An autonomous Run effect always
/// carries its Session Turn; ordinary prompts carry only Session + Turn; a
/// manual compaction outside a Turn carries Session only.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompactionOwner {
    Session {
        session: SessionId,
    },
    Turn {
        session: SessionId,
        turn: CompactionTurnId,
    },
    AutonomousTurn {
        session: SessionId,
        turn: CompactionTurnId,
        run: RunId,
        iteration: IterationId,
        operation: OperationId,
    },
}

impl CompactionOwner {
    pub fn session(&self) -> &SessionId {
        match self {
            Self::Session { session }
            | Self::Turn { session, .. }
            | Self::AutonomousTurn { session, .. } => session,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CompactionContractError> {
        valid_text(self.session().as_str(), crate::MAX_SESSION_IDENTITY_BYTES)?;
        match self {
            Self::Session { .. } => Ok(()),
            Self::Turn { turn, .. } => turn.validate(),
            Self::AutonomousTurn {
                turn,
                run,
                operation,
                ..
            } => {
                turn.validate()?;
                RunId::new(run.as_str()).map_err(|_| CompactionContractError::Invalid)?;
                OperationId::new(operation.as_str())
                    .map_err(|_| CompactionContractError::Invalid)?;
                Ok(())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionReason {
    Manual,
    AutomaticThreshold,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionRequestPath {
    SinglePassVerbatim,
    SinglePassFitted,
    SinglePassLossy,
    TwoPassFinal,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct CompactionDigest(String);

impl CompactionDigest {
    pub fn from_stored(value: impl Into<String>) -> Result<Self, CompactionContractError> {
        let value = value.into();
        let valid = value.strip_prefix("sha256:").is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
        if !valid {
            return Err(CompactionContractError::Invalid);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn domain_hash(domain: &str, bytes: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(domain.as_bytes());
        digest.update([0]);
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
        Self(format!("sha256:{:x}", digest.finalize()))
    }

    pub(crate) fn validate(&self) -> Result<(), CompactionContractError> {
        Self::from_stored(&self.0).map(drop)
    }
}

impl TryFrom<String> for CompactionDigest {
    type Error = CompactionContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_stored(value)
    }
}

impl From<CompactionDigest> for String {
    fn from(value: CompactionDigest) -> Self {
        value.0
    }
}

/// Digest and exact canonical byte/item counts. There is intentionally no
/// content accessor.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionContentFacts {
    pub digest: CompactionDigest,
    pub size_bytes: u64,
    pub item_count: u32,
}

impl CompactionContentFacts {
    pub fn from_stored(
        digest: impl Into<String>,
        size_bytes: u64,
        item_count: u32,
    ) -> Result<Self, CompactionContractError> {
        Ok(Self {
            digest: CompactionDigest::from_stored(digest)?,
            size_bytes,
            item_count,
        })
    }

    pub(crate) fn from_bytes(domain: &str, bytes: &[u8], item_count: u32) -> Self {
        Self {
            digest: CompactionDigest::domain_hash(domain, bytes),
            size_bytes: bytes.len() as u64,
            item_count,
        }
    }
}

/// Credential-free leaves of the exact prepared semantic model request. The
/// SDK computes `root` from these facts; provider headers and credentials are
/// structurally absent.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionInputFacts {
    pub path: CompactionRequestPath,
    pub root: CompactionContentFacts,
    pub messages: CompactionContentFacts,
    pub tool_definitions: CompactionContentFacts,
    pub hosted_tool_declarations: CompactionContentFacts,
    pub model_parameters: CompactionContentFacts,
}

impl CompactionInputFacts {
    pub(crate) fn from_leaves(
        path: CompactionRequestPath,
        messages: CompactionContentFacts,
        tool_definitions: CompactionContentFacts,
        hosted_tool_declarations: CompactionContentFacts,
        model_parameters: CompactionContentFacts,
    ) -> Self {
        let mut canonical = Vec::new();
        canonical.push(path as u8);
        for facts in [
            &messages,
            &tool_definitions,
            &hosted_tool_declarations,
            &model_parameters,
        ] {
            canonical.extend(facts.size_bytes.to_be_bytes());
            canonical.extend(facts.item_count.to_be_bytes());
            canonical.extend((facts.digest.as_str().len() as u32).to_be_bytes());
            canonical.extend(facts.digest.as_str().as_bytes());
        }
        let item_count = messages
            .item_count
            .saturating_add(tool_definitions.item_count)
            .saturating_add(hosted_tool_declarations.item_count)
            .saturating_add(model_parameters.item_count);
        let root = CompactionContentFacts::from_bytes(
            COMPACTION_INPUT_DIGEST_DOMAIN,
            &canonical,
            item_count,
        );
        Self {
            path,
            root,
            messages,
            tool_definitions,
            hosted_tool_declarations,
            model_parameters,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CompactionContractError> {
        for facts in [
            &self.root,
            &self.messages,
            &self.tool_definitions,
            &self.hosted_tool_declarations,
            &self.model_parameters,
        ] {
            facts.digest.validate()?;
        }
        let expected = Self::from_leaves(
            self.path,
            self.messages.clone(),
            self.tool_definitions.clone(),
            self.hosted_tool_declarations.clone(),
            self.model_parameters.clone(),
        );
        if expected.root != self.root {
            return Err(CompactionContractError::Invalid);
        }
        Ok(())
    }
}

/// Exact durable chain position captured before the first applying model call.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionStateReference {
    pub session: SessionId,
    pub generation: SessionGeneration,
    pub manifest_revision: u64,
    pub manifest_digest: CompactionDigest,
    pub head: Option<SessionObjectId>,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionIntent {
    pub id: CompactionId,
    pub owner: CompactionOwner,
    pub reason: CompactionReason,
    pub base: CompactionStateReference,
    pub input: CompactionInputFacts,
}

impl CompactionIntent {
    pub fn probe(&self) -> CompactionProbe {
        CompactionProbe {
            session: self.owner.session().clone(),
            generation: self.base.generation.clone(),
            id: self.id.clone(),
            base: self.base.clone(),
            intent_digest: observation_digest("intent", self),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CompactionContractError> {
        self.id.validate()?;
        self.owner.validate()?;
        valid_text(
            self.base.session.as_str(),
            crate::MAX_SESSION_IDENTITY_BYTES,
        )?;
        valid_text(
            self.base.generation.as_str(),
            crate::MAX_SESSION_GENERATION_BYTES,
        )?;
        if self.base.manifest_revision == 0
            || self.base.session != *self.owner.session()
            || self
                .base
                .head
                .as_ref()
                .is_some_and(|head| CompactionDigest::from_stored(head.as_str()).is_err())
        {
            return Err(CompactionContractError::Invalid);
        }
        self.base.manifest_digest.validate()?;
        self.input.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionPublicationReference {
    pub session: SessionId,
    pub generation: SessionGeneration,
    pub publication: SessionObjectId,
    pub checkpoint: SessionObjectId,
    pub sequence: u64,
    pub prompt_index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionReceipt {
    pub intent: CompactionIntent,
    pub publication: CompactionPublicationReference,
    pub summary: CompactionContentFacts,
    pub checkpoint: CompactionContentFacts,
    pub installed_state: CompactionContentFacts,
}

impl CompactionReceipt {
    pub(crate) fn validate(&self) -> Result<(), CompactionContractError> {
        self.intent.validate()?;
        for facts in [&self.summary, &self.checkpoint, &self.installed_state] {
            facts.digest.validate()?;
        }
        if self.publication.sequence == 0
            || self.intent.base.session != *self.intent.owner.session()
            // A replayed publication in a fork has a different Session and
            // generation. Within one Session, generation must remain exact.
            || self.publication.session == self.intent.base.session
                && self.publication.generation != self.intent.base.generation
        {
            return Err(CompactionContractError::Invalid);
        }
        Ok(())
    }
}

/// Immutable typed metadata embedded directly in the publication chain. Raw
/// checkpoint marker bytes are never parsed to recover these facts.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionPublicationRecord {
    pub intent: CompactionIntent,
    pub summary: CompactionContentFacts,
    pub checkpoint: CompactionContentFacts,
    pub installed_state: CompactionContentFacts,
    pub prompt_index: u64,
}

impl CompactionPublicationRecord {
    pub(crate) fn receipt(
        &self,
        session: SessionId,
        generation: SessionGeneration,
        publication: SessionObjectId,
        checkpoint: SessionObjectId,
        sequence: u64,
    ) -> CompactionReceipt {
        CompactionReceipt {
            intent: self.intent.clone(),
            publication: CompactionPublicationReference {
                session,
                generation,
                publication,
                checkpoint,
                sequence,
                prompt_index: self.prompt_index,
            },
            summary: self.summary.clone(),
            checkpoint: self.checkpoint.clone(),
            installed_state: self.installed_state.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CompactionContractError> {
        self.intent.validate()?;
        for facts in [&self.summary, &self.checkpoint, &self.installed_state] {
            facts.digest.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionNotAppliedReason {
    Cancelled,
    ModelFailed,
    InvalidModelOutput,
    InputChanged,
    PublicationAbsent,
    InterruptedBeforePublication,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum CompactionOutcome {
    Applied {
        receipt: CompactionReceipt,
    },
    NotApplied {
        intent: CompactionIntent,
        reason: CompactionNotAppliedReason,
    },
}

impl CompactionOutcome {
    pub fn id(&self) -> &CompactionId {
        match self {
            Self::Applied { receipt } => &receipt.intent.id,
            Self::NotApplied { intent, .. } => &intent.id,
        }
    }
}

/// Exact acknowledgement. Helpers bind the echo to the complete redacted
/// callback payload, so an ID reused with different facts fails closed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionAcknowledgement {
    pub id: CompactionId,
    pub observation_digest: CompactionDigest,
}

impl CompactionAcknowledgement {
    pub fn for_intent(intent: &CompactionIntent) -> Self {
        Self {
            id: intent.id.clone(),
            observation_digest: observation_digest("intent", intent),
        }
    }

    pub fn for_outcome(outcome: &CompactionOutcome) -> Self {
        Self {
            id: outcome.id().clone(),
            observation_digest: observation_digest("outcome", outcome),
        }
    }
}

fn observation_digest<T: serde::Serialize>(kind: &str, payload: &T) -> CompactionDigest {
    let mut bytes = Vec::new();
    bytes.extend((kind.len() as u32).to_be_bytes());
    bytes.extend(kind.as_bytes());
    // These public protocol types contain only bounded, secret-free fields;
    // serde field order is part of this current-only v1 observation contract.
    bytes.extend(serde_json::to_vec(payload).expect("compaction protocol serialization"));
    CompactionDigest::domain_hash(COMPACTION_OBSERVATION_DIGEST_DOMAIN, &bytes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionObserverErrorCode {
    Rejected,
    Conflict,
    Unavailable,
    InvalidAcknowledgement,
}

/// Content-free observer failure. Diagnostics belong in the Host's own audit
/// log; accepting a free-form string here would let a prompt, path, provider
/// response, or credential accidentally cross the typed boundary.
#[derive(
    Clone, Copy, Debug, thiserror::Error, PartialEq, Eq, serde::Deserialize, serde::Serialize,
)]
#[error("compaction observer {code:?}")]
pub struct CompactionObserverError {
    pub code: CompactionObserverErrorCode,
}

impl CompactionObserverError {
    pub const fn new(code: CompactionObserverErrorCode) -> Self {
        Self { code }
    }
}

#[async_trait::async_trait]
pub trait CompactionObserver: Send + Sync + 'static {
    async fn intent(
        &self,
        intent: CompactionIntent,
    ) -> Result<CompactionAcknowledgement, CompactionObserverError>;

    async fn outcome(
        &self,
        outcome: CompactionOutcome,
    ) -> Result<CompactionAcknowledgement, CompactionObserverError>;
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionProbe {
    /// Session chain to inspect. It equals `base.session` for the original
    /// timeline and may name a fork containing a replayed publication.
    pub session: SessionId,
    pub generation: SessionGeneration,
    pub id: CompactionId,
    pub base: CompactionStateReference,
    pub intent_digest: CompactionDigest,
}

impl CompactionProbe {
    /// Retarget this immutable historical query to a known fork generation.
    /// Cross-Session absence is never enough to prove `NotPublished`; only an
    /// embedded matching publication can produce `Applied(Forked)`.
    pub fn in_session(mut self, session: SessionId, generation: SessionGeneration) -> Self {
        self.session = session;
        self.generation = generation;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "relation", rename_all = "snake_case")]
pub enum CompactionTimelineRelation {
    Current,
    Followed,
    Superseded { by: CompactionId },
    Rewound { operation: SessionObjectId },
    Forked { origin_session: SessionId },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionProbeUncertainty {
    GenerationMismatch,
    BaseNotInAncestry,
    MissingObject,
    CorruptObject,
    ConflictingPublication,
    StoreFailure,
    UnstableManifest,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum CompactionProbeResult {
    Applied {
        receipt: CompactionReceipt,
        relation: CompactionTimelineRelation,
        as_of_revision: u64,
        as_of_manifest_digest: CompactionDigest,
    },
    NotPublished {
        base: CompactionStateReference,
        as_of_revision: u64,
        as_of_manifest_digest: CompactionDigest,
    },
    Uncertain {
        reason: CompactionProbeUncertainty,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(domain: &str, value: &[u8]) -> CompactionContentFacts {
        CompactionContentFacts::from_bytes(domain, value, 1)
    }

    fn input(message: &[u8]) -> CompactionInputFacts {
        CompactionInputFacts::from_leaves(
            CompactionRequestPath::SinglePassVerbatim,
            facts("messages", message),
            facts("tools", b"tools"),
            facts("hosted", b"hosted"),
            facts("model", b"model"),
        )
    }

    fn base(session: &SessionId) -> CompactionStateReference {
        CompactionStateReference {
            session: session.clone(),
            generation: SessionGeneration::new("generation").unwrap(),
            manifest_revision: 7,
            manifest_digest: CompactionDigest::from_stored(format!("sha256:{}", "1".repeat(64)))
                .unwrap(),
            head: None,
            sequence: 0,
        }
    }

    fn intent(owner: CompactionOwner, message: &[u8]) -> CompactionIntent {
        let session = owner.session().clone();
        CompactionIntent {
            id: CompactionId::from_stored("cmp-test").unwrap(),
            owner,
            reason: CompactionReason::AutomaticThreshold,
            base: base(&session),
            input: input(message),
        }
    }

    #[test]
    fn ownership_is_structurally_manual_ordinary_or_all_or_none_autonomous() {
        let session = SessionId::from_stored("session");
        let manual = CompactionOwner::Session {
            session: session.clone(),
        };
        let ordinary = CompactionOwner::Turn {
            session: session.clone(),
            turn: CompactionTurnId::new("turn").unwrap(),
        };
        let autonomous = CompactionOwner::AutonomousTurn {
            session: session.clone(),
            turn: CompactionTurnId::new("turn").unwrap(),
            run: RunId::new("run").unwrap(),
            iteration: IterationId::new(3),
            operation: OperationId::new("operation").unwrap(),
        };
        assert_eq!(manual.session(), &session);
        assert_eq!(ordinary.session(), &session);
        assert_eq!(autonomous.session(), &session);
        assert!(intent(manual, b"input").validate().is_ok());
        assert!(intent(ordinary, b"input").validate().is_ok());
        assert!(intent(autonomous, b"input").validate().is_ok());
    }

    #[test]
    fn exact_input_change_changes_root_and_callback_acknowledgement() {
        let session = SessionId::from_stored("session");
        let first = intent(
            CompactionOwner::Session {
                session: session.clone(),
            },
            b"first",
        );
        let second = intent(CompactionOwner::Session { session }, b"second");
        assert_ne!(first.input.root, second.input.root);
        assert_eq!(
            CompactionAcknowledgement::for_intent(&first),
            CompactionAcknowledgement::for_intent(&first)
        );
        assert_ne!(
            CompactionAcknowledgement::for_intent(&first),
            CompactionAcknowledgement::for_intent(&second)
        );
    }

    #[test]
    fn observer_shapes_are_bounded_and_content_free() {
        assert!(CompactionTurnId::new("x".repeat(MAX_COMPACTION_TURN_ID_BYTES + 1)).is_err());
        assert!(
            serde_json::from_value::<CompactionId>(serde_json::json!(
                "x".repeat(MAX_COMPACTION_ID_BYTES + 1)
            ))
            .is_err()
        );
        assert!(
            serde_json::from_value::<CompactionTurnId>(serde_json::json!(
                "x".repeat(MAX_COMPACTION_TURN_ID_BYTES + 1)
            ))
            .is_err()
        );
        assert!(
            serde_json::from_value::<CompactionDigest>(serde_json::json!("sha256:raw")).is_err()
        );
        assert!(
            serde_json::from_value::<CompactionOwner>(serde_json::json!({
                "kind": "session",
                "session": "x".repeat(crate::MAX_SESSION_IDENTITY_BYTES + 1),
            }))
            .is_err()
        );
        let observer_error = serde_json::to_string(&CompactionObserverError::new(
            CompactionObserverErrorCode::Unavailable,
        ))
        .unwrap();
        assert_eq!(observer_error, r#"{"code":"unavailable"}"#);
        let serialized = serde_json::to_string(&intent(
            CompactionOwner::Session {
                session: SessionId::from_stored("session"),
            },
            b"secret transcript body",
        ))
        .unwrap();
        assert!(!serialized.contains("secret transcript body"));
        assert!(serialized.contains("sha256:"));
    }
}
