use crate::{
    Event, EventUpdate, PromptReceipt, SessionConfig, SessionId, SourceProvenance, TurnOutcome, run,
};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, fmt, str::FromStr};

pub const HARNESS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const MAX_HARNESS_SNAPSHOT_BYTES: usize = 2 * 1024 * 1024;
const MAX_HARNESS_FIELD_BYTES: usize = 1024 * 1024;
const HARNESS_DIGEST_PREFIX: &str = "sha256:";

/// Content identity of one immutable harness snapshot.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct HarnessDigest(String);

impl HarnessDigest {
    fn for_content(content: &HarnessContent) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"grok-build-sdk.harness-snapshot.v1\0");
        hash_optional(&mut digest, content.system_prompt.as_deref());
        hash_optional(&mut digest, content.rules.as_deref());
        Self(format!("{HARNESS_DIGEST_PREFIX}{:x}", digest.finalize()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(value: &str) -> Result<(), HarnessError> {
        let Some(hex) = value.strip_prefix(HARNESS_DIGEST_PREFIX) else {
            return Err(HarnessError::Invalid(
                "harness digest must use the sha256 prefix".into(),
            ));
        };
        if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(HarnessError::Invalid(
                "harness digest must contain exactly 32 SHA-256 bytes".into(),
            ));
        }
        Ok(())
    }
}

impl fmt::Display for HarnessDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for HarnessDigest {
    type Err = HarnessError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::validate(value)?;
        Ok(Self(value.to_ascii_lowercase()))
    }
}

impl<'de> Deserialize<'de> for HarnessDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

fn hash_optional(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        None => digest.update([0]),
    }
}

/// Native harness inputs frozen into a snapshot. It intentionally contains no
/// mutable revision, evidence, activation, history, or rollback state.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessContent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rules: Option<String>,
}

impl HarnessContent {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn system_prompt(mut self, value: impl Into<String>) -> Self {
        self.system_prompt = Some(value.into());
        self
    }

    pub fn rules(mut self, value: impl Into<String>) -> Self {
        self.rules = Some(value.into());
        self
    }

    pub fn system_prompt_value(&self) -> Option<&str> {
        self.system_prompt.as_deref()
    }

    pub fn rules_value(&self) -> Option<&str> {
        self.rules.as_deref()
    }

    fn validate(&self) -> Result<(), HarnessError> {
        validate_optional_field(
            self.system_prompt.as_deref(),
            "system prompt",
            MAX_HARNESS_FIELD_BYTES,
        )?;
        if self.system_prompt.is_none() {
            return Err(HarnessError::Invalid(
                "harness snapshot requires a complete system prompt".into(),
            ));
        }
        validate_optional_field(self.rules.as_deref(), "rules", MAX_HARNESS_FIELD_BYTES)
    }
}

fn validate_optional_field(
    value: Option<&str>,
    name: &str,
    max_bytes: usize,
) -> Result<(), HarnessError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        return Err(HarnessError::Invalid(format!(
            "harness {name} must not be blank"
        )));
    }
    if value.len() > max_bytes {
        return Err(HarnessError::Invalid(format!(
            "harness {name} exceeds {max_bytes} bytes"
        )));
    }
    Ok(())
}

/// Immutable, content-addressed harness input. Deserialization recomputes the
/// digest and rejects tampered or unsupported snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HarnessSnapshot {
    schema_version: u32,
    digest: HarnessDigest,
    content: HarnessContent,
}

impl HarnessSnapshot {
    pub fn new(content: HarnessContent) -> Result<Self, HarnessError> {
        content.validate()?;
        let digest = HarnessDigest::for_content(&content);
        let snapshot = Self {
            schema_version: HARNESS_SNAPSHOT_SCHEMA_VERSION,
            digest,
            content,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn digest(&self) -> &HarnessDigest {
        &self.digest
    }

    pub fn content(&self) -> &HarnessContent {
        &self.content
    }

    pub fn validate(&self) -> Result<(), HarnessError> {
        if self.schema_version != HARNESS_SNAPSHOT_SCHEMA_VERSION {
            return Err(HarnessError::Invalid(format!(
                "unsupported harness snapshot schema {}",
                self.schema_version
            )));
        }
        self.content.validate()?;
        let computed = HarnessDigest::for_content(&self.content);
        if self.digest != computed {
            return Err(HarnessError::DigestMismatch {
                declared: self.digest.clone(),
                computed,
            });
        }
        Ok(())
    }

    /// Materializes only the native immutable session inputs. No SDK-owned
    /// revision or activation record is created.
    pub fn materialize(&self) -> Result<MaterializedHarness, HarnessError> {
        self.validate()?;
        Ok(MaterializedHarness {
            digest: self.digest.clone(),
            system_prompt: self.content.system_prompt.clone(),
            rules: self.content.rules.clone(),
        })
    }

    pub fn to_json_vec(&self) -> Result<Vec<u8>, HarnessError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| HarnessError::Serialization(error.to_string()))?;
        if bytes.len() > MAX_HARNESS_SNAPSHOT_BYTES {
            return Err(HarnessError::SnapshotTooLarge {
                actual: bytes.len(),
                maximum: MAX_HARNESS_SNAPSHOT_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, HarnessError> {
        if bytes.len() > MAX_HARNESS_SNAPSHOT_BYTES {
            return Err(HarnessError::SnapshotTooLarge {
                actual: bytes.len(),
                maximum: MAX_HARNESS_SNAPSHOT_BYTES,
            });
        }
        let wire: HarnessSnapshotWire = serde_json::from_slice(bytes)
            .map_err(|error| HarnessError::Serialization(error.to_string()))?;
        Self::from_wire(wire)
    }

    fn from_wire(wire: HarnessSnapshotWire) -> Result<Self, HarnessError> {
        let snapshot = Self {
            schema_version: wire.schema_version,
            digest: wire.digest,
            content: wire.content,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

#[derive(Deserialize)]
struct HarnessSnapshotWire {
    schema_version: u32,
    digest: HarnessDigest,
    content: HarnessContent,
}

impl<'de> Deserialize<'de> for HarnessSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = HarnessSnapshotWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}

/// Headless materialization of a snapshot into Grok's native Session inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedHarness {
    digest: HarnessDigest,
    system_prompt: Option<String>,
    rules: Option<String>,
}

impl MaterializedHarness {
    pub fn digest(&self) -> &HarnessDigest {
        &self.digest
    }

    pub fn system_prompt(&self) -> Option<&str> {
        self.system_prompt.as_deref()
    }

    pub fn rules(&self) -> Option<&str> {
        self.rules.as_deref()
    }

    /// Replaces the complete harness portion while preserving the Session's
    /// independent cwd, model, and reasoning route.
    pub fn apply_to_session(&self, mut config: SessionConfig) -> SessionConfig {
        let mut prompt = self
            .system_prompt
            .clone()
            .expect("validated snapshots always materialize a complete system prompt");
        if let Some(rules) = &self.rules {
            prompt.push_str("\n\n<human_rules>\n");
            prompt.push_str(rules);
            prompt.push_str("\n</human_rules>");
        }
        config.system_prompt = Some(prompt);
        // Rules are folded into the full override because the native runtime
        // treats `systemPromptOverride` as authoritative on create and attach.
        config.rules = None;
        config
    }
}

/// Owned SDK build provenance frozen into a durable Turn binding receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SdkProvenance {
    upstream_release: String,
    fork_commit: String,
    upstream_source_rev: String,
    facade_version: String,
    dirty: bool,
}

impl SdkProvenance {
    pub(crate) fn current() -> Self {
        Self::from(crate::source_provenance())
    }

    pub fn upstream_release(&self) -> &str {
        &self.upstream_release
    }

    pub fn fork_commit(&self) -> &str {
        &self.fork_commit
    }

    pub fn upstream_source_rev(&self) -> &str {
        &self.upstream_source_rev
    }

    pub fn facade_version(&self) -> &str {
        &self.facade_version
    }

    pub fn dirty(&self) -> bool {
        self.dirty
    }

    fn validate(&self) -> Result<(), HarnessError> {
        validate_text(&self.upstream_release, 128, "upstream release")?;
        validate_commit(&self.fork_commit, "fork commit")?;
        validate_commit(&self.upstream_source_rev, "upstream source revision")?;
        validate_text(&self.facade_version, 128, "façade version")
    }
}

impl From<SourceProvenance> for SdkProvenance {
    fn from(value: SourceProvenance) -> Self {
        Self {
            upstream_release: value.upstream_release.into(),
            fork_commit: value.fork_commit.into(),
            upstream_source_rev: value.upstream_source_rev.into(),
            facade_version: value.facade_version.into(),
            dirty: value.dirty,
        }
    }
}

#[derive(Deserialize)]
struct SdkProvenanceWire {
    upstream_release: String,
    fork_commit: String,
    upstream_source_rev: String,
    facade_version: String,
    dirty: bool,
}

impl<'de> Deserialize<'de> for SdkProvenance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SdkProvenanceWire::deserialize(deserializer)?;
        let value = Self {
            upstream_release: wire.upstream_release,
            fork_commit: wire.fork_commit,
            upstream_source_rev: wire.upstream_source_rev,
            facade_version: wire.facade_version,
            dirty: wire.dirty,
        };
        value.validate().map_err(D::Error::custom)?;
        Ok(value)
    }
}

fn validate_text(value: &str, maximum: usize, name: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty() || value.len() > maximum {
        return Err(HarnessError::Invalid(format!(
            "Turn binding {name} must contain 1..={maximum} bytes"
        )));
    }
    Ok(())
}

fn validate_commit(value: &str, name: &str) -> Result<(), HarnessError> {
    if value.len() != 40 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(HarnessError::Invalid(format!(
            "Turn binding {name} must be a full Git object ID"
        )));
    }
    Ok(())
}

/// An event range verified as contiguous through the Turn's terminal event.
/// `after_sequence` is exclusive and `final_sequence` is inclusive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CompleteEventCursor {
    after_sequence: u64,
    final_sequence: u64,
    event_count: u64,
}

impl CompleteEventCursor {
    fn new(after_sequence: u64, final_sequence: u64) -> Result<Self, HarnessError> {
        let event_count = final_sequence.checked_sub(after_sequence).ok_or_else(|| {
            HarnessError::IncompleteTurn(
                "terminal event cursor precedes the pre-Turn cursor".into(),
            )
        })?;
        let cursor = Self {
            after_sequence,
            final_sequence,
            event_count,
        };
        cursor.validate()?;
        Ok(cursor)
    }

    pub fn after_sequence(&self) -> u64 {
        self.after_sequence
    }

    pub fn final_sequence(&self) -> u64 {
        self.final_sequence
    }

    pub fn event_count(&self) -> u64 {
        self.event_count
    }

    fn validate(&self) -> Result<(), HarnessError> {
        let expected = self
            .final_sequence
            .checked_sub(self.after_sequence)
            .filter(|count| *count > 0)
            .ok_or_else(|| {
                HarnessError::IncompleteTurn(
                    "complete event cursor must contain at least one event".into(),
                )
            })?;
        if self.event_count != expected {
            return Err(HarnessError::IncompleteTurn(
                "complete event cursor count does not match its range".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct CompleteEventCursorWire {
    after_sequence: u64,
    final_sequence: u64,
    event_count: u64,
}

impl<'de> Deserialize<'de> for CompleteEventCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CompleteEventCursorWire::deserialize(deserializer)?;
        let cursor = Self {
            after_sequence: wire.after_sequence,
            final_sequence: wire.final_sequence,
            event_count: wire.event_count,
        };
        cursor.validate().map_err(D::Error::custom)?;
        Ok(cursor)
    }
}

/// Runtime-issued proof that one settled native Turn used one immutable
/// harness snapshot and route and has a complete retained event range.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TurnBindingReceipt {
    binding_id: String,
    session_id: SessionId,
    turn_id: String,
    prompt_digest: String,
    snapshot_digest: HarnessDigest,
    model: String,
    reasoning: Option<String>,
    sdk_provenance: SdkProvenance,
    complete_cursor: CompleteEventCursor,
    outcome: TurnOutcome,
    runtime_prompt_index: u64,
    settlement_id: String,
    usage: run::EffectUsage,
}

impl TurnBindingReceipt {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn complete(
        session_id: SessionId,
        turn_id: String,
        prompt_digest: String,
        snapshot_digest: HarnessDigest,
        model: String,
        reasoning: Option<String>,
        after_sequence: u64,
        prompt: PromptReceipt,
        events: &[Event],
    ) -> Result<Self, HarnessError> {
        let cursor = CompleteEventCursor::new(after_sequence, prompt.final_sequence)?;
        if events.len() as u64 != cursor.event_count {
            return Err(HarnessError::IncompleteTurn(format!(
                "Turn event range contains {} of {} events",
                events.len(),
                cursor.event_count
            )));
        }
        for (offset, event) in events.iter().enumerate() {
            let expected_sequence = after_sequence
                .checked_add(offset as u64 + 1)
                .ok_or_else(|| HarnessError::IncompleteTurn("event sequence overflow".into()))?;
            if event.session_id != session_id || event.sequence != expected_sequence || event.replay
            {
                return Err(HarnessError::IncompleteTurn(
                    "Turn event range is not a contiguous live session stream".into(),
                ));
            }
            if event
                .turn_id
                .as_deref()
                .is_some_and(|event_turn| event_turn != turn_id)
            {
                return Err(HarnessError::IncompleteTurn(
                    "Turn event range contains another Turn identity".into(),
                ));
            }
        }
        let terminal = events.last().ok_or_else(|| {
            HarnessError::IncompleteTurn("Turn event range has no terminal event".into())
        })?;
        if terminal.turn_id.as_deref() != Some(turn_id.as_str())
            || !matches!(terminal.update, EventUpdate::TurnFinished(value) if value == prompt.outcome)
            || terminal.sequence != prompt.final_sequence
        {
            return Err(HarnessError::IncompleteTurn(
                "Turn event range does not end at the matching terminal event".into(),
            ));
        }
        if events[..events.len() - 1]
            .iter()
            .any(|event| matches!(event.update, EventUpdate::TurnFinished(_)))
        {
            return Err(HarnessError::IncompleteTurn(
                "Turn event range contains an early terminal event".into(),
            ));
        }
        let receipt = Self {
            binding_id: String::new(),
            session_id,
            turn_id,
            prompt_digest,
            snapshot_digest,
            model,
            reasoning,
            sdk_provenance: SdkProvenance::current(),
            complete_cursor: cursor,
            outcome: prompt.outcome,
            runtime_prompt_index: prompt.runtime_prompt_index,
            settlement_id: prompt.settlement_id,
            usage: prompt.usage,
        };
        let mut receipt = receipt;
        receipt.binding_id = receipt.compute_binding_id()?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn binding_id(&self) -> &str {
        &self.binding_id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub fn prompt_digest(&self) -> &str {
        &self.prompt_digest
    }

    pub fn snapshot_digest(&self) -> &HarnessDigest {
        &self.snapshot_digest
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn reasoning(&self) -> Option<&str> {
        self.reasoning.as_deref()
    }

    pub fn sdk_provenance(&self) -> &SdkProvenance {
        &self.sdk_provenance
    }

    pub fn complete_cursor(&self) -> &CompleteEventCursor {
        &self.complete_cursor
    }

    pub fn outcome(&self) -> TurnOutcome {
        self.outcome
    }

    pub fn runtime_prompt_index(&self) -> u64 {
        self.runtime_prompt_index
    }

    pub fn settlement_id(&self) -> &str {
        &self.settlement_id
    }

    pub fn usage(&self) -> &run::EffectUsage {
        &self.usage
    }

    fn validate(&self) -> Result<(), HarnessError> {
        if self.binding_id != self.compute_binding_id()? {
            return Err(HarnessError::Invalid(
                "Turn binding receipt identity does not match its contents".into(),
            ));
        }
        validate_text(self.session_id.as_str(), 512, "Session ID")?;
        validate_text(&self.turn_id, 512, "Turn ID")?;
        validate_prompt_digest(&self.prompt_digest)?;
        validate_text(&self.model, 512, "model")?;
        if let Some(reasoning) = &self.reasoning {
            validate_text(reasoning, 128, "reasoning effort")?;
        }
        self.sdk_provenance.validate()?;
        self.complete_cursor.validate()?;
        validate_text(&self.settlement_id, 512, "settlement ID")?;
        self.usage
            .validate()
            .map_err(|error| HarnessError::Invalid(error.to_string()))?;
        let expected_settlement = crate::private::ledger_settlement_id(
            self.session_id.as_str(),
            &self.turn_id,
            &self.prompt_digest,
            self.runtime_prompt_index,
            self.outcome,
            &self.usage,
        )
        .map_err(|error| HarnessError::Invalid(error.to_string()))?;
        if self.settlement_id != expected_settlement {
            return Err(HarnessError::Invalid(
                "Turn binding settlement does not match its Turn evidence".into(),
            ));
        }
        Ok(())
    }

    fn compute_binding_id(&self) -> Result<String, HarnessError> {
        let payload = TurnBindingPayload {
            session_id: &self.session_id,
            turn_id: &self.turn_id,
            prompt_digest: &self.prompt_digest,
            snapshot_digest: &self.snapshot_digest,
            model: &self.model,
            reasoning: self.reasoning.as_deref(),
            sdk_provenance: &self.sdk_provenance,
            complete_cursor: &self.complete_cursor,
            outcome: self.outcome,
            runtime_prompt_index: self.runtime_prompt_index,
            settlement_id: &self.settlement_id,
            usage: &self.usage,
        };
        let mut value = serde_json::to_value(payload)
            .map_err(|error| HarnessError::Serialization(error.to_string()))?;
        crate::canonicalize_json(&mut value);
        let bytes = serde_json::to_vec(&value)
            .map_err(|error| HarnessError::Serialization(error.to_string()))?;
        let mut digest = Sha256::new();
        digest.update(b"grok-build-sdk.turn-binding.v1\0");
        digest.update(bytes);
        Ok(format!("sha256:{:x}", digest.finalize()))
    }
}

#[derive(Serialize)]
struct TurnBindingPayload<'a> {
    session_id: &'a SessionId,
    turn_id: &'a str,
    prompt_digest: &'a str,
    snapshot_digest: &'a HarnessDigest,
    model: &'a str,
    reasoning: Option<&'a str>,
    sdk_provenance: &'a SdkProvenance,
    complete_cursor: &'a CompleteEventCursor,
    outcome: TurnOutcome,
    runtime_prompt_index: u64,
    settlement_id: &'a str,
    usage: &'a run::EffectUsage,
}

fn validate_prompt_digest(value: &str) -> Result<(), HarnessError> {
    let hex = value
        .strip_prefix("sha256:")
        .or_else(|| value.strip_prefix("sha256-v2:"))
        .ok_or_else(|| {
            HarnessError::Invalid("Turn binding prompt digest has an unknown scheme".into())
        })?;
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(HarnessError::Invalid(
            "Turn binding prompt digest must contain 32 SHA-256 bytes".into(),
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
struct TurnBindingReceiptWire {
    binding_id: String,
    session_id: SessionId,
    turn_id: String,
    prompt_digest: String,
    snapshot_digest: HarnessDigest,
    model: String,
    reasoning: Option<String>,
    sdk_provenance: SdkProvenance,
    complete_cursor: CompleteEventCursor,
    outcome: TurnOutcome,
    runtime_prompt_index: u64,
    settlement_id: String,
    usage: run::EffectUsage,
}

impl<'de> Deserialize<'de> for TurnBindingReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TurnBindingReceiptWire::deserialize(deserializer)?;
        let receipt = Self {
            binding_id: wire.binding_id,
            session_id: wire.session_id,
            turn_id: wire.turn_id,
            prompt_digest: wire.prompt_digest,
            snapshot_digest: wire.snapshot_digest,
            model: wire.model,
            reasoning: wire.reasoning,
            sdk_provenance: wire.sdk_provenance,
            complete_cursor: wire.complete_cursor,
            outcome: wire.outcome,
            runtime_prompt_index: wire.runtime_prompt_index,
            settlement_id: wire.settlement_id,
            usage: wire.usage,
        };
        receipt.validate().map_err(D::Error::custom)?;
        Ok(receipt)
    }
}

/// One typed change proposed against an immutable snapshot.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "value", rename_all = "snake_case")]
pub enum HarnessRefinement {
    SetSystemPrompt(String),
    SetRules(String),
    ClearRules,
}

impl HarnessRefinement {
    fn target(&self) -> &'static str {
        match self {
            Self::SetSystemPrompt(_) => "system_prompt",
            Self::SetRules(_) | Self::ClearRules => "rules",
        }
    }

    fn validate(&self) -> Result<(), HarnessError> {
        match self {
            Self::SetSystemPrompt(value) => {
                validate_optional_field(Some(value), "system prompt", MAX_HARNESS_FIELD_BYTES)
            }
            Self::SetRules(value) => {
                validate_optional_field(Some(value), "rules", MAX_HARNESS_FIELD_BYTES)
            }
            Self::ClearRules => Ok(()),
        }
    }
}

/// Typed optimistic patch. Applying it checks only immutable content identity;
/// the Host remains responsible for revision CAS, commit, evidence, activation,
/// history, and rollback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HarnessRefinementPatch {
    base_digest: HarnessDigest,
    changes: Vec<HarnessRefinement>,
}

impl HarnessRefinementPatch {
    pub fn new(
        base_digest: HarnessDigest,
        changes: impl IntoIterator<Item = HarnessRefinement>,
    ) -> Result<Self, HarnessError> {
        let patch = Self {
            base_digest,
            changes: changes.into_iter().collect(),
        };
        patch.validate()?;
        Ok(patch)
    }

    pub fn base_digest(&self) -> &HarnessDigest {
        &self.base_digest
    }

    pub fn changes(&self) -> &[HarnessRefinement] {
        &self.changes
    }

    pub fn validate(&self) -> Result<(), HarnessError> {
        if self.changes.is_empty() {
            return Err(HarnessError::Invalid(
                "harness refinement patch must contain at least one change".into(),
            ));
        }
        let mut targets = BTreeSet::new();
        for change in &self.changes {
            change.validate()?;
            if !targets.insert(change.target()) {
                return Err(HarnessError::Invalid(format!(
                    "harness refinement target '{}' occurs more than once",
                    change.target()
                )));
            }
        }
        Ok(())
    }

    pub fn apply(&self, base: &HarnessSnapshot) -> Result<HarnessSnapshot, HarnessError> {
        self.validate()?;
        base.validate()?;
        if &self.base_digest != base.digest() {
            return Err(HarnessError::StaleBase {
                expected: self.base_digest.clone(),
                actual: base.digest().clone(),
            });
        }
        let mut content = base.content.clone();
        for change in &self.changes {
            match change {
                HarnessRefinement::SetSystemPrompt(value) => {
                    content.system_prompt = Some(value.clone());
                }
                HarnessRefinement::SetRules(value) => content.rules = Some(value.clone()),
                HarnessRefinement::ClearRules => content.rules = None,
            }
        }
        HarnessSnapshot::new(content)
    }
}

#[derive(Deserialize)]
struct HarnessRefinementPatchWire {
    base_digest: HarnessDigest,
    changes: Vec<HarnessRefinement>,
}

impl<'de> Deserialize<'de> for HarnessRefinementPatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = HarnessRefinementPatchWire::deserialize(deserializer)?;
        Self::new(wire.base_digest, wire.changes).map_err(D::Error::custom)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum HarnessError {
    #[error("invalid harness contract: {0}")]
    Invalid(String),
    #[error("harness snapshot is {actual} bytes; maximum is {maximum}")]
    SnapshotTooLarge { actual: usize, maximum: usize },
    #[error("harness snapshot digest mismatch: declared {declared}, computed {computed}")]
    DigestMismatch {
        declared: HarnessDigest,
        computed: HarnessDigest,
    },
    #[error("stale harness refinement base: expected {expected}, actual {actual}")]
    StaleBase {
        expected: HarnessDigest,
        actual: HarnessDigest,
    },
    #[error("Session harness binding mismatch: bound {bound}, requested {requested}")]
    BindingMismatch {
        bound: HarnessDigest,
        requested: HarnessDigest,
    },
    #[error("Session was not created or resumed with an immutable harness snapshot")]
    UnboundSession,
    #[error("incomplete Turn binding: {0}")]
    IncompleteTurn(String),
    #[error("harness serialization failed: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_deserialization_rejects_a_rehashed_inconsistent_settlement() {
        let session_id = SessionId::from_stored("receipt-settlement-session");
        let turn_id = "receipt-settlement-turn".to_owned();
        let prompt_digest = format!("sha256:{}", "1".repeat(64));
        let usage = run::EffectUsage::default();
        let settlement_id = crate::private::ledger_settlement_id(
            session_id.as_str(),
            &turn_id,
            &prompt_digest,
            0,
            TurnOutcome::End,
            &usage,
        )
        .unwrap();
        let mut receipt = TurnBindingReceipt {
            binding_id: String::new(),
            session_id,
            turn_id,
            prompt_digest,
            snapshot_digest: format!("sha256:{}", "2".repeat(64)).parse().unwrap(),
            model: "settlement-model".into(),
            reasoning: Some("high".into()),
            sdk_provenance: SdkProvenance::current(),
            complete_cursor: CompleteEventCursor {
                after_sequence: 0,
                final_sequence: 1,
                event_count: 1,
            },
            outcome: TurnOutcome::End,
            runtime_prompt_index: 0,
            settlement_id,
            usage,
        };
        receipt.binding_id = receipt.compute_binding_id().unwrap();
        receipt.validate().unwrap();

        receipt.outcome = TurnOutcome::Cancelled;
        receipt.binding_id = receipt.compute_binding_id().unwrap();
        assert!(
            serde_json::from_value::<TurnBindingReceipt>(serde_json::to_value(receipt).unwrap())
                .is_err(),
            "a fresh binding hash cannot legitimize contradictory settlement evidence"
        );
    }
}
