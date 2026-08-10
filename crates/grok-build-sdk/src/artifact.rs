// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Content-addressed artifact custody with provenance, usage and recovery.
//!
//! [`crate::run`]'s `ArtifactStore` is the Run reducer's blob plumbing: it
//! addresses bytes by SHA-256 and verifies them on read, and that is all the
//! reducer needs. A Host that shows artifacts to a person, keeps them across
//! restarts, and hands them to another worker needs four more answers the
//! reducer never asks for: *what produced this*, *what is it and how long
//! should it live*, *is the stored copy still the copy that was written*, and
//! *who has used it*. This module publishes those answers as one
//! backend-neutral contract.
//!
//! Identity is derived, never declared. An [`ArtifactId`] is the SHA-256 of the
//! content and nothing else, so an [`ArtifactHandle`] — the (id, digest) pair —
//! can only ever name one byte sequence. Writing the same bytes twice is one
//! artifact; writing different bytes while declaring an existing identity is
//! refused rather than reconciled.
//!
//! Damage is reported, never repaired behind the caller's back. A read whose
//! stored bytes no longer address to the requested digest fails as
//! [`ArtifactError::Corrupt`], which is a different answer from
//! [`ArtifactError::Missing`]; the Host decides whether it can supply the
//! bytes again and says so explicitly through [`ArtifactVault::recover`].
//!
//! Time is always supplied by the caller. The vault has no clock, so usage
//! accounting and retention hints are a pure function of stored state and the
//! instants a Host declares — which is what makes the conformance suite
//! deterministic.

mod conformance;
mod local;

pub use conformance::{ArtifactDamage, ArtifactVaultHarness, run_artifact_vault_conformance};
pub use local::{ARTIFACT_VAULT_SCHEMA_MARKER, ARTIFACT_VAULT_SCHEMA_VERSION, LocalArtifactVault};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::path::{Path, PathBuf};

/// Largest artifact the contract accepts, in bytes.
pub const MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
/// Largest media type the contract accepts, in bytes.
pub const MAX_ARTIFACT_MEDIA_TYPE_BYTES: usize = 255;
/// Largest provenance label — producer, operation, program, revision — in bytes.
pub const MAX_ARTIFACT_LABEL_BYTES: usize = 256;
/// Largest number of inputs one instrument observation may cite.
pub const MAX_ARTIFACT_OBSERVATION_INPUTS: usize = 64;

/// The algorithm every artifact identity is derived with. The prefix is part of
/// the identity string so a later algorithm is an additive vocabulary rather
/// than a silent reinterpretation of existing identities.
const ARTIFACT_DIGEST_PREFIX: &str = "sha256-";

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("artifact input is invalid: {0}")]
    Validation(String),
    #[error("artifact {0} is not stored")]
    Missing(ArtifactId),
    #[error("stored artifact is corrupt: {0}")]
    Corrupt(String),
    #[error("artifact storage failed: {0}")]
    Storage(String),
}

fn validation(message: impl Into<String>) -> ArtifactError {
    ArtifactError::Validation(message.into())
}

fn validate_label_text(label: &str, value: &str, limit: usize) -> Result<(), ArtifactError> {
    if value.is_empty() {
        return Err(validation(format!("{label} is empty")));
    }
    if value.len() > limit {
        return Err(validation(format!("{label} exceeds {limit} bytes")));
    }
    if value.chars().any(char::is_control) {
        return Err(validation(format!("{label} contains a control character")));
    }
    if value.trim() != value {
        return Err(validation(format!(
            "{label} has leading or trailing whitespace"
        )));
    }
    Ok(())
}

fn lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn hex_digest(content: &[u8]) -> String {
    format!("{:x}", Sha256::digest(content))
}

/// The SHA-256 of one artifact's content, as lowercase hexadecimal.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactDigest(String);

impl ArtifactDigest {
    pub fn of(content: &[u8]) -> Self {
        Self(hex_digest(content))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, ArtifactError> {
        let value = value.into();
        if !lower_hex_sha256(&value) {
            return Err(validation(
                "artifact digest is not a lowercase hexadecimal SHA-256",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ArtifactDigest {
    type Error = ArtifactError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<ArtifactDigest> for String {
    fn from(value: ArtifactDigest) -> Self {
        value.0
    }
}

impl std::fmt::Display for ArtifactDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The identity of one artifact, derived from its content and nothing else.
///
/// There is deliberately no constructor that takes a caller-chosen name. Two
/// artifacts are the same artifact exactly when their bytes are the same, so a
/// Host cannot create two identities for one payload or one identity for two
/// payloads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactId(String);

impl ArtifactId {
    /// Derives the identity of the given content.
    pub fn for_content(content: &[u8]) -> Self {
        Self(format!("{ARTIFACT_DIGEST_PREFIX}{}", hex_digest(content)))
    }

    /// Derives the identity that a digest addresses, without holding the bytes.
    pub fn from_digest(digest: &ArtifactDigest) -> Self {
        Self(format!("{ARTIFACT_DIGEST_PREFIX}{digest}"))
    }

    /// Re-reads an identity a Host persisted earlier, refusing anything that is
    /// not a well-formed derived identity.
    pub fn parse(value: impl Into<String>) -> Result<Self, ArtifactError> {
        let value = value.into();
        let Some(hex) = value.strip_prefix(ARTIFACT_DIGEST_PREFIX) else {
            return Err(validation(
                "artifact id does not name a supported digest algorithm",
            ));
        };
        if !lower_hex_sha256(hex) {
            return Err(validation(
                "artifact id does not carry a lowercase hexadecimal SHA-256",
            ));
        }
        Ok(Self(value))
    }

    pub fn digest(&self) -> ArtifactDigest {
        ArtifactDigest(self.0[ARTIFACT_DIGEST_PREFIX.len()..].to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ArtifactId {
    type Error = ArtifactError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<ArtifactId> for String {
    fn from(value: ArtifactId) -> Self {
        value.0
    }
}

impl std::fmt::Display for ArtifactId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// An immutable name for exactly one byte sequence.
///
/// The pair is checked at construction, so a handle whose id and digest
/// disagree does not exist. That is what makes the immutability claim
/// structural rather than a rule a backend has to remember: the only way to
/// name different content is to hold a different handle.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ArtifactHandle {
    id: ArtifactId,
    digest: ArtifactDigest,
}

impl ArtifactHandle {
    pub fn for_content(content: &[u8]) -> Self {
        let digest = ArtifactDigest::of(content);
        Self {
            id: ArtifactId::from_digest(&digest),
            digest,
        }
    }

    /// Rebuilds a handle a Host persisted earlier. An id and digest that do not
    /// address the same content are refused rather than trusted.
    pub fn new(id: ArtifactId, digest: ArtifactDigest) -> Result<Self, ArtifactError> {
        if id.digest() != digest {
            return Err(validation(
                "artifact handle names an id and a digest that disagree",
            ));
        }
        Ok(Self { id, digest })
    }

    pub fn id(&self) -> &ArtifactId {
        &self.id
    }

    pub fn digest(&self) -> &ArtifactDigest {
        &self.digest
    }

    /// Whether the given bytes are the bytes this handle names.
    pub fn addresses(&self, content: &[u8]) -> bool {
        hex_digest(content) == self.digest.0
    }
}

/// What an artifact is, as an IANA media type.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactMediaType(String);

impl ArtifactMediaType {
    pub fn new(value: impl Into<String>) -> Result<Self, ArtifactError> {
        let value = value.into();
        validate_label_text("artifact media type", &value, MAX_ARTIFACT_MEDIA_TYPE_BYTES)?;
        let mut parts = value.split('/');
        let valid = match (parts.next(), parts.next(), parts.next()) {
            (Some(type_name), Some(subtype), None) => {
                !type_name.is_empty()
                    && !subtype.is_empty()
                    && value.is_ascii()
                    && !value.chars().any(char::is_whitespace)
            }
            _ => false,
        };
        if !valid {
            return Err(validation("artifact media type is not a type/subtype pair"));
        }
        Ok(Self(value))
    }

    /// The media type to declare when nothing more specific is known.
    pub fn octet_stream() -> Self {
        Self("application/octet-stream".to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ArtifactMediaType {
    type Error = ArtifactError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ArtifactMediaType> for String {
    fn from(value: ArtifactMediaType) -> Self {
        value.0
    }
}

impl std::fmt::Display for ArtifactMediaType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A bounded, single-line name used inside a provenance record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactLabel(String);

impl ArtifactLabel {
    pub fn new(value: impl Into<String>) -> Result<Self, ArtifactError> {
        let value = value.into();
        validate_label_text(
            "artifact provenance label",
            &value,
            MAX_ARTIFACT_LABEL_BYTES,
        )?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ArtifactLabel {
    type Error = ArtifactError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ArtifactLabel> for String {
    fn from(value: ArtifactLabel) -> Self {
        value.0
    }
}

impl std::fmt::Display for ArtifactLabel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why an artifact exists.
///
/// The vocabulary is closed — a Host may not invent a kind — but additive: a
/// later revision may name a new kind, and a Host must treat an unrecognised
/// kind as one it does not display rather than as corruption.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactProvenanceKind {
    /// An ordinary output the operation was asked to produce.
    ProducedOutput,
    /// Content the operation consumed but did not create.
    ConsumedInput,
    /// A diagnostic recording of the operation itself — a log, a trace, a
    /// captured error.
    OperationRecord,
    /// A captured frame or machine-readable measurement that records a program
    /// execution, the inputs it ran against and the revision under
    /// observation. Distinct from an ordinary produced output because it is
    /// evidence *about* an execution rather than a result *of* one, and it is
    /// only meaningful together with its [`ArtifactObservation`].
    InstrumentObservation,
}

/// What an instrument observation observed.
///
/// Carried only by [`ArtifactProvenanceKind::InstrumentObservation`]. It names
/// the executed program, the revision that execution was made against, and the
/// artifacts that were its inputs — so a capture can be reproduced, or shown to
/// be no longer reproducible, without consulting anything outside the vault.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactObservation {
    program: ArtifactLabel,
    revision: ArtifactLabel,
    inputs: Vec<ArtifactId>,
}

impl ArtifactObservation {
    pub fn new(program: ArtifactLabel, revision: ArtifactLabel) -> Self {
        Self {
            program,
            revision,
            inputs: Vec::new(),
        }
    }

    /// Cites one artifact the observed execution ran against.
    pub fn input(mut self, id: ArtifactId) -> Result<Self, ArtifactError> {
        if self.inputs.len() >= MAX_ARTIFACT_OBSERVATION_INPUTS {
            return Err(validation(format!(
                "an observation may cite at most {MAX_ARTIFACT_OBSERVATION_INPUTS} inputs"
            )));
        }
        self.inputs.push(id);
        Ok(self)
    }

    /// The program whose execution this observation records.
    pub fn program(&self) -> &ArtifactLabel {
        &self.program
    }

    /// The revision of the observed subject at the instant of capture.
    pub fn revision(&self) -> &ArtifactLabel {
        &self.revision
    }

    pub fn inputs(&self) -> &[ArtifactId] {
        &self.inputs
    }

    fn validate(&self) -> Result<(), ArtifactError> {
        ArtifactLabel::new(self.program.as_str())?;
        ArtifactLabel::new(self.revision.as_str())?;
        if self.inputs.len() > MAX_ARTIFACT_OBSERVATION_INPUTS {
            return Err(validation("an observation cites too many inputs"));
        }
        for input in &self.inputs {
            ArtifactId::parse(input.as_str())?;
        }
        Ok(())
    }
}

/// Who produced an artifact, and as part of what.
///
/// The producer is named by the three coordinates a Run has: the Run's own
/// identity, the iteration it was in, and the operation that ran. The record is
/// additive — later revisions may carry more — and is stored verbatim, so a
/// Host that displays provenance never has to reconstruct it from a log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactProvenance {
    kind: ArtifactProvenanceKind,
    producer_run: ArtifactLabel,
    iteration: u64,
    operation: ArtifactLabel,
    recorded_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observation: Option<ArtifactObservation>,
}

impl ArtifactProvenance {
    /// Provenance for content an operation produced or consumed.
    ///
    /// [`ArtifactProvenanceKind::InstrumentObservation`] is refused here
    /// because it is meaningless without its observation; use
    /// [`ArtifactProvenance::observed`].
    pub fn produced(
        kind: ArtifactProvenanceKind,
        producer_run: ArtifactLabel,
        iteration: u64,
        operation: ArtifactLabel,
        recorded_at_ms: u64,
    ) -> Result<Self, ArtifactError> {
        if kind == ArtifactProvenanceKind::InstrumentObservation {
            return Err(validation(
                "an instrument observation must carry what it observed",
            ));
        }
        Ok(Self {
            kind,
            producer_run,
            iteration,
            operation,
            recorded_at_ms,
            observation: None,
        })
    }

    /// Provenance for a captured frame or machine-readable measurement of a
    /// program execution.
    pub fn observed(
        producer_run: ArtifactLabel,
        iteration: u64,
        operation: ArtifactLabel,
        recorded_at_ms: u64,
        observation: ArtifactObservation,
    ) -> Result<Self, ArtifactError> {
        observation.validate()?;
        Ok(Self {
            kind: ArtifactProvenanceKind::InstrumentObservation,
            producer_run,
            iteration,
            operation,
            recorded_at_ms,
            observation: Some(observation),
        })
    }

    pub fn kind(&self) -> ArtifactProvenanceKind {
        self.kind
    }

    /// The identity of the Run that produced this artifact.
    pub fn producer_run(&self) -> &ArtifactLabel {
        &self.producer_run
    }

    /// Which iteration of that Run produced it.
    pub fn iteration(&self) -> u64 {
        self.iteration
    }

    /// Which operation within that iteration produced it.
    pub fn operation(&self) -> &ArtifactLabel {
        &self.operation
    }

    pub fn recorded_at_ms(&self) -> u64 {
        self.recorded_at_ms
    }

    /// Present exactly when the kind is
    /// [`ArtifactProvenanceKind::InstrumentObservation`].
    pub fn observation(&self) -> Option<&ArtifactObservation> {
        self.observation.as_ref()
    }

    /// Re-checks a provenance record decoded from storage. Every stored scalar
    /// is checked before it is trusted, so a foreign writer or a damaged page
    /// fails the read rather than attributing an artifact to invented work.
    pub fn validate(&self) -> Result<(), ArtifactError> {
        ArtifactLabel::new(self.producer_run.as_str())?;
        ArtifactLabel::new(self.operation.as_str())?;
        match (self.kind, &self.observation) {
            (ArtifactProvenanceKind::InstrumentObservation, Some(observation)) => {
                observation.validate()
            }
            (ArtifactProvenanceKind::InstrumentObservation, None) => Err(validation(
                "an instrument observation is missing what it observed",
            )),
            (_, Some(_)) => Err(validation(
                "only an instrument observation may carry an observation",
            )),
            (_, None) => Ok(()),
        }
    }
}

/// How long a Host intends to keep an artifact.
///
/// This is a hint the vault stores and returns; it is never a schedule the
/// vault acts on. Deletion belongs to a Host retention policy, which is the
/// only place that can know whether anything still references the content.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRetention {
    /// Droppable as soon as nothing references it.
    Transient,
    /// Keep while the producing Run is live.
    WhileProducerLives,
    /// Keep until an operator policy removes it.
    Durable,
}

impl ArtifactRetention {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::WhileProducerLives => "while_producer_lives",
            Self::Durable => "durable",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ArtifactError> {
        match value {
            "transient" => Ok(Self::Transient),
            "while_producer_lives" => Ok(Self::WhileProducerLives),
            "durable" => Ok(Self::Durable),
            other => Err(validation(format!("unknown artifact retention {other:?}"))),
        }
    }
}

/// Everything a writer declares about content that is not derivable from it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactWrite {
    media_type: ArtifactMediaType,
    retention: ArtifactRetention,
    retain_until_ms: Option<u64>,
    provenance: ArtifactProvenance,
    expected_id: Option<ArtifactId>,
}

impl ArtifactWrite {
    pub fn new(
        media_type: ArtifactMediaType,
        retention: ArtifactRetention,
        provenance: ArtifactProvenance,
    ) -> Self {
        Self {
            media_type,
            retention,
            retain_until_ms: None,
            provenance,
            expected_id: None,
        }
    }

    /// The instant after which a Host's retention policy may drop this content.
    pub fn retain_until(mut self, value: u64) -> Self {
        self.retain_until_ms = Some(value);
        self
    }

    /// Declares the identity the content must have.
    ///
    /// A write whose bytes do not address to this identity is refused before
    /// any storage effect, which is how a caller that believes it is re-writing
    /// known content finds out that it is not.
    pub fn expect_identity(mut self, id: ArtifactId) -> Self {
        self.expected_id = Some(id);
        self
    }

    pub fn media_type(&self) -> &ArtifactMediaType {
        &self.media_type
    }

    pub fn retention(&self) -> ArtifactRetention {
        self.retention
    }

    pub fn retain_until_ms(&self) -> Option<u64> {
        self.retain_until_ms
    }

    pub fn provenance(&self) -> &ArtifactProvenance {
        &self.provenance
    }

    pub fn expected_id(&self) -> Option<&ArtifactId> {
        self.expected_id.as_ref()
    }

    /// Checks the declaration against the content it describes. Backends call
    /// this before any storage effect so the refusal ordering is the contract's
    /// rather than each backend's.
    pub fn validate_for(&self, content: &[u8]) -> Result<ArtifactHandle, ArtifactError> {
        if content.len() > MAX_ARTIFACT_BYTES {
            return Err(validation(format!(
                "artifact exceeds {MAX_ARTIFACT_BYTES} bytes"
            )));
        }
        ArtifactMediaType::new(self.media_type.as_str())?;
        self.provenance.validate()?;
        let handle = ArtifactHandle::for_content(content);
        if let Some(expected) = &self.expected_id
            && expected != handle.id()
        {
            return Err(validation(
                "content does not address to the declared artifact identity",
            ));
        }
        Ok(handle)
    }
}

/// Durable record of how an artifact has been used.
///
/// Counters saturate rather than wrap: a vault that has served more reads than
/// a `u64` can count reports the bound instead of restarting at zero, because
/// an accounting record that silently resets is worse than one that is capped.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactUsage {
    /// Completed, digest-verified reads of the content.
    pub reads: u64,
    /// Completed, digest-verified materializations to a path.
    pub materializations: u64,
    /// Total verified bytes served, across reads and materializations.
    pub bytes_served: u64,
    /// The instant of the most recent read or materialization.
    pub last_used_at_ms: Option<u64>,
}

impl ArtifactUsage {
    pub(crate) fn record_read(&mut self, size: u64, now_ms: u64) {
        self.reads = self.reads.saturating_add(1);
        self.bytes_served = self.bytes_served.saturating_add(size);
        self.last_used_at_ms = Some(now_ms);
    }

    pub(crate) fn record_materialization(&mut self, size: u64, now_ms: u64) {
        self.materializations = self.materializations.saturating_add(1);
        self.bytes_served = self.bytes_served.saturating_add(size);
        self.last_used_at_ms = Some(now_ms);
    }
}

/// Everything the vault durably knows about one artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub handle: ArtifactHandle,
    pub size: u64,
    pub media_type: ArtifactMediaType,
    pub retention: ArtifactRetention,
    pub retain_until_ms: Option<u64>,
    pub provenance: ArtifactProvenance,
    pub written_at_ms: u64,
    /// The instant of the most recent explicit recovery, if the stored copy
    /// has ever been re-materialized after damage. A Host shows this rather
    /// than inferring that custody was always unbroken.
    pub recovered_at_ms: Option<u64>,
    pub usage: ArtifactUsage,
}

/// What a write did to the vault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactPut {
    /// The content was not present and is now stored.
    Stored,
    /// The content was already stored. The stored record is untouched: a second
    /// write of identical bytes never re-dates, re-labels or re-attributes the
    /// artifact that is already there.
    AlreadyPresent,
}

/// The handle a write settled on, and what the write did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactReceipt {
    pub handle: ArtifactHandle,
    pub outcome: ArtifactPut,
}

/// Whether the stored copy is still the copy that was written.
///
/// This is a probe, not a read: it answers rather than failing, so a Host can
/// audit custody without treating every damaged artifact as an error path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactIntegrity {
    /// Present and addressing to its own identity.
    Intact,
    /// No such artifact.
    Missing,
    /// Present, but the stored bytes no longer address to the identity that
    /// names them.
    Corrupt,
}

/// What an explicit recovery did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactRecovery {
    /// The stored copy was already intact; nothing was replaced.
    AlreadyIntact,
    /// Damaged content was replaced by the supplied bytes, which address to the
    /// same identity. The record — provenance, media type, retention, usage —
    /// is unchanged, because identity did not change.
    Restored,
}

/// A verified copy of one artifact at a path outside the vault.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactMaterialization {
    pub path: PathBuf,
    pub size: u64,
    pub digest: ArtifactDigest,
}

/// Durable custody of content-addressed artifacts.
///
/// Implementations own transactions, durability and physical layout. They must
/// persist and fail-closed verify a schema marker and version, refuse stored
/// state they cannot decode within the published bounds, and make every method
/// atomic against every other handle to the same authority — including handles
/// in other processes, which is what makes a second worker's writes visible to
/// the first without either of them coordinating.
///
/// Every method that changes usage takes the caller's instant. Implementations
/// must not consult a clock of their own.
pub trait ArtifactVault: Send + Sync + 'static {
    /// Stores content under its derived identity.
    ///
    /// Idempotent by content: identical bytes are one artifact however many
    /// times they are written. Content that does not address to a declared
    /// [`ArtifactWrite::expect`] identity is refused before any storage effect.
    fn put(
        &self,
        content: &[u8],
        write: &ArtifactWrite,
        now_ms: u64,
    ) -> Result<ArtifactReceipt, ArtifactError>;

    /// Answers everything durably known about one artifact, or `None` when the
    /// identity is unknown. A record that cannot be decoded within the
    /// published bounds is [`ArtifactError::Corrupt`], never `None`: an
    /// artifact that exists but cannot be trusted must not look absent.
    fn inspect(&self, id: &ArtifactId) -> Result<Option<ArtifactRecord>, ArtifactError>;

    /// Reads the content a handle names, verifying that the stored bytes still
    /// address to the handle's digest, and records one read.
    ///
    /// A stored copy that no longer verifies is [`ArtifactError::Corrupt`] and
    /// an identity that was never stored is [`ArtifactError::Missing`]. The two
    /// are never collapsed, because a Host can re-supply the bytes for the
    /// first and cannot for the second.
    fn read(&self, handle: &ArtifactHandle, now_ms: u64) -> Result<Vec<u8>, ArtifactError>;

    /// Probes custody without reading, and without recording usage.
    fn verify(&self, id: &ArtifactId) -> Result<ArtifactIntegrity, ArtifactError>;

    /// Replaces a damaged stored copy with bytes the Host supplies.
    ///
    /// Recovery is explicit and never happens inside a read. The supplied bytes
    /// must address to `id`, so recovery cannot change what an identity means;
    /// an artifact whose stored copy is already intact is left alone and
    /// answered [`ArtifactRecovery::AlreadyIntact`]. An unknown identity is
    /// [`ArtifactError::Missing`]: re-supplying content for an artifact the
    /// vault never accepted is a write, not a recovery.
    fn recover(
        &self,
        id: &ArtifactId,
        content: &[u8],
        now_ms: u64,
    ) -> Result<ArtifactRecovery, ArtifactError>;

    /// Writes a verified copy of the content to `destination` and records one
    /// materialization.
    ///
    /// The copy is written atomically and re-verified after the rename, so a
    /// returned [`ArtifactMaterialization`] means the bytes at that path
    /// addressed to the handle's digest on disk, not merely in memory.
    fn materialize(
        &self,
        handle: &ArtifactHandle,
        destination: &Path,
        now_ms: u64,
    ) -> Result<ArtifactMaterialization, ArtifactError>;

    /// Usage accounting for one artifact, or `None` when the identity is
    /// unknown.
    fn usage(&self, id: &ArtifactId) -> Result<Option<ArtifactUsage>, ArtifactError> {
        Ok(self.inspect(id)?.map(|record| record.usage))
    }
}

/// Writes `content` to `destination` atomically and proves the bytes on disk
/// address to `expected`.
///
/// Published so a Host backend gets the contract's materialization semantics —
/// no partially written destination, no symlink or directory target, and a
/// post-rename verification — without reimplementing them.
pub fn materialize_verified_copy(
    content: &[u8],
    expected: &ArtifactDigest,
    destination: &Path,
) -> Result<ArtifactMaterialization, ArtifactError> {
    if hex_digest(content) != expected.as_str() {
        return Err(ArtifactError::Corrupt(
            "content offered for materialization does not address to its digest".into(),
        ));
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| validation("materialization destination has no parent directory"))?;
    if let Ok(metadata) = std::fs::symlink_metadata(destination)
        && (metadata.file_type().is_symlink() || metadata.is_dir())
    {
        return Err(validation(
            "materialization destination is a directory or a symbolic link",
        ));
    }
    std::fs::create_dir_all(parent).map_err(storage_error)?;
    let temporary = parent.join(format!(
        ".artifact-materialize-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let write = (|| -> Result<(), ArtifactError> {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(storage_error)?;
        file.write_all(content).map_err(storage_error)?;
        file.sync_all().map_err(storage_error)?;
        std::fs::rename(&temporary, destination).map_err(storage_error)
    })();
    if write.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write?;
    let written = std::fs::read(destination).map_err(storage_error)?;
    if hex_digest(&written) != expected.as_str() {
        return Err(ArtifactError::Corrupt(
            "materialized copy does not address to the artifact digest".into(),
        ));
    }
    Ok(ArtifactMaterialization {
        path: destination.to_path_buf(),
        size: written.len() as u64,
        digest: expected.clone(),
    })
}

fn storage_error(error: impl std::fmt::Display) -> ArtifactError {
    ArtifactError::Storage(error.to_string())
}
