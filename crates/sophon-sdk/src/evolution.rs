// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Continual-harness self-evolution aggregate.
//!
//! This module is the mutable authority behind harness evolution. It owns a
//! scoped ledger of typed supplemental entries (prompt notes, memories, skill
//! contracts, subagent specifications), applies validated refinement proposals
//! with per-edit optimistic concurrency, records append-only refinement events
//! with before/after snapshots, and supports atomic rollback.
//!
//! Evolution never mutates a running Turn. The ledger renders into a marked
//! supplement section, and [`evolved_refinement`] converts that render into a
//! typed [`HarnessRefinementPatch`] against one immutable base
//! [`HarnessSnapshot`]. Activating an evolved harness therefore always means
//! deriving a new content-addressed snapshot, so existing Turn binding
//! receipts keep proving exactly which evolved harness each Turn used.

mod conformance;
mod store;

pub use conformance::run_evolution_store_conformance;
pub use store::{
    EVOLUTION_STORE_SCHEMA_MARKER, EVOLUTION_STORE_SCHEMA_VERSION, EvolutionCommit, EvolutionStore,
    EvolutionStoreError, LocalEvolutionStore, evolution_commit_reconciled,
};

use crate::{
    HarnessError, HarnessEvidenceKind, HarnessEvidenceRef, HarnessRefinement,
    HarnessRefinementPatch, HarnessSnapshot, MAX_HARNESS_EVIDENCE_REFS, SessionId,
};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, fmt, str::FromStr};

pub const EVOLUTION_STATE_SCHEMA_VERSION: u32 = 1;
pub const REFINEMENT_EVENT_SCHEMA_VERSION: u32 = 1;
pub const MAX_EVOLUTION_STATE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_REFINEMENT_EVENT_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_HARNESS_ENTRIES_PER_SCOPE: usize = 256;
/// Write-time budget for one scope's total entry text. Enforcing the budget at
/// apply time keeps rendering infallible and keeps every evolved system prompt
/// under the immutable snapshot field limits.
pub const MAX_HARNESS_SCOPE_CONTENT_BYTES: usize = 192 * 1024;
pub const MAX_HARNESS_ENTRY_ID_BYTES: usize = 128;
pub const MAX_HARNESS_ENTRY_TITLE_BYTES: usize = 256;
pub const MAX_HARNESS_ENTRY_CONTENT_BYTES: usize = 64 * 1024;
pub const MAX_HARNESS_SKILL_ARGUMENTS: usize = 32;
pub const MAX_REFINEMENT_EDITS: usize = 32;
pub const MAX_REFINEMENT_TEXT_BYTES: usize = 4 * 1024;
pub const MAX_PLANNER_TRANSCRIPT_BYTES: usize = 80 * 1024;
pub const MAX_RENDERED_REFINEMENT_HISTORY: usize = 5;
pub const HARNESS_SUPPLEMENT_BEGIN: &str = "<continual_harness>";
pub const HARNESS_SUPPLEMENT_END: &str = "</continual_harness>";

const MAX_ENTRY_SOURCE_BYTES: usize = 128;
const MAX_SKILL_INVOCATION_BYTES: usize = 1024;
const MAX_SKILL_ARGUMENT_NAME_BYTES: usize = 64;
const MAX_SKILL_ARGUMENT_DOC_BYTES: usize = 1024;
const MAX_SESSION_SCOPE_ID_BYTES: usize = 512;
const REFINEMENT_EVENT_DIGEST_DOMAIN: &[u8] = b"sophon-sdk.harness-refinement-event.v1\0";

#[non_exhaustive]
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum EvolutionError {
    #[error("invalid harness evolution contract: {0}")]
    Invalid(String),
    #[error("evolution state is {actual} bytes; maximum is {maximum}")]
    StateTooLarge { actual: usize, maximum: usize },
    #[error("refinement event is {actual} bytes; maximum is {maximum}")]
    EventTooLarge { actual: usize, maximum: usize },
    #[error("harness scope mismatch: expected {expected}, actual {actual}")]
    ScopeMismatch { expected: String, actual: String },
    #[error("evolution revision overflow")]
    RevisionOverflow,
    #[error("refinement rollback conflict: {0}")]
    RollbackConflict(String),
    #[error("malformed harness supplement markers: {0}")]
    MalformedSupplement(String),
    #[error(transparent)]
    Harness(#[from] HarnessError),
    #[error("harness evolution serialization failed: {0}")]
    Serialization(String),
}

fn invalid(message: impl Into<String>) -> EvolutionError {
    EvolutionError::Invalid(message.into())
}

fn validate_text(
    value: &str,
    maximum: usize,
    name: &str,
    forbid_markers: bool,
) -> Result<(), EvolutionError> {
    if value.trim().is_empty() || value.len() > maximum {
        return Err(invalid(format!("{name} must contain 1..={maximum} bytes")));
    }
    if forbid_markers
        && (value.contains(HARNESS_SUPPLEMENT_BEGIN) || value.contains(HARNESS_SUPPLEMENT_END))
    {
        return Err(invalid(format!(
            "{name} must not contain harness supplement markers"
        )));
    }
    Ok(())
}

/// Where one evolution ledger lives. Session entries overlay global entries of
/// the same ID in the merged view; promotion to global is always an explicit
/// separate proposal against the global scope.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HarnessScope {
    Global,
    Session(SessionId),
}

impl HarnessScope {
    pub fn session(id: impl Into<String>) -> Self {
        Self::Session(SessionId::from_stored(id))
    }

    pub fn is_global(&self) -> bool {
        matches!(self, Self::Global)
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Session(_) => "session",
        }
    }

    fn validate(&self) -> Result<(), EvolutionError> {
        match self {
            Self::Global => Ok(()),
            Self::Session(id) => validate_text(
                id.as_str(),
                MAX_SESSION_SCOPE_ID_BYTES,
                "session scope ID",
                true,
            ),
        }
    }
}

impl fmt::Display for HarnessScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Global => formatter.write_str("global"),
            Self::Session(id) => write!(formatter, "session:{}", id.as_str()),
        }
    }
}

impl FromStr for HarnessScope {
    type Err = EvolutionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let scope = if value == "global" {
            Self::Global
        } else if let Some(id) = value.strip_prefix("session:") {
            Self::Session(SessionId::from_stored(id))
        } else {
            return Err(invalid(format!("unknown harness scope '{value}'")));
        };
        scope.validate()?;
        Ok(scope)
    }
}

impl Serialize for HarnessScope {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for HarnessScope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

/// The supplemental kinds a refinement may evolve. The base system prompt is
/// deliberately not a kind: evolution only ever composes a marked supplement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessEntryKind {
    Prompt,
    Memory,
    Skill,
    Subagent,
}

impl HarnessEntryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Memory => "memory",
            Self::Skill => "skill",
            Self::Subagent => "subagent",
        }
    }

    fn section_heading(self) -> &'static str {
        match self {
            Self::Prompt => "Prompt Notes",
            Self::Memory => "Memories",
            Self::Skill => "Skills",
            Self::Subagent => "Subagents",
        }
    }
}

impl fmt::Display for HarnessEntryKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Routing contract for one already-available capability. A skill entry never
/// carries executable code; it tells future Turns how to call something the
/// Host or capability layer has installed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessSkillContract {
    invocation: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    arguments: BTreeMap<String, String>,
}

impl HarnessSkillContract {
    pub fn new(invocation: impl Into<String>) -> Self {
        Self {
            invocation: invocation.into(),
            arguments: BTreeMap::new(),
        }
    }

    pub fn argument(mut self, name: impl Into<String>, doc: impl Into<String>) -> Self {
        self.arguments.insert(name.into(), doc.into());
        self
    }

    pub fn invocation(&self) -> &str {
        &self.invocation
    }

    pub fn arguments(&self) -> &BTreeMap<String, String> {
        &self.arguments
    }

    fn validate(&self) -> Result<(), EvolutionError> {
        validate_text(
            &self.invocation,
            MAX_SKILL_INVOCATION_BYTES,
            "skill invocation",
            true,
        )?;
        if self.arguments.len() > MAX_HARNESS_SKILL_ARGUMENTS {
            return Err(invalid(format!(
                "skill contract declares {} arguments; maximum is {MAX_HARNESS_SKILL_ARGUMENTS}",
                self.arguments.len()
            )));
        }
        for (name, doc) in &self.arguments {
            if name.is_empty()
                || name.len() > MAX_SKILL_ARGUMENT_NAME_BYTES
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            {
                return Err(invalid(format!("invalid skill argument name '{name}'")));
            }
            validate_text(
                doc,
                MAX_SKILL_ARGUMENT_DOC_BYTES,
                "skill argument documentation",
                true,
            )?;
        }
        Ok(())
    }
}

/// One versioned supplemental entry in a scope's evolution ledger.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEntry {
    id: String,
    kind: HarnessEntryKind,
    title: String,
    content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    skill: Option<HarnessSkillContract>,
    source: String,
    version: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
}

impl HarnessEntry {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn kind(&self) -> HarnessEntryKind {
        self.kind
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn skill(&self) -> Option<&HarnessSkillContract> {
        self.skill.as_ref()
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn created_at_ms(&self) -> u64 {
        self.created_at_ms
    }

    pub fn updated_at_ms(&self) -> u64 {
        self.updated_at_ms
    }

    fn text_bytes(&self) -> usize {
        self.title.len()
            + self.content.len()
            + self.skill.as_ref().map_or(0, |skill| {
                skill.invocation.len()
                    + skill
                        .arguments
                        .iter()
                        .map(|(name, doc)| name.len() + doc.len())
                        .sum::<usize>()
            })
    }

    fn validate(&self) -> Result<(), EvolutionError> {
        validate_entry_id(&self.id)?;
        validate_text(
            &self.title,
            MAX_HARNESS_ENTRY_TITLE_BYTES,
            "entry title",
            true,
        )?;
        validate_text(
            &self.content,
            MAX_HARNESS_ENTRY_CONTENT_BYTES,
            "entry content",
            true,
        )?;
        validate_text(&self.source, MAX_ENTRY_SOURCE_BYTES, "entry source", true)?;
        match (&self.skill, self.kind) {
            (Some(contract), HarnessEntryKind::Skill) => contract.validate()?,
            (Some(_), kind) => {
                return Err(invalid(format!(
                    "a {kind} entry must not carry a skill contract"
                )));
            }
            (None, HarnessEntryKind::Skill) => {
                return Err(invalid("a skill entry requires a skill contract"));
            }
            (None, _) => {}
        }
        if self.version == 0 {
            return Err(invalid("entry versions start at 1"));
        }
        if self.updated_at_ms < self.created_at_ms {
            return Err(invalid("entry update time precedes its creation time"));
        }
        Ok(())
    }
}

fn validate_entry_id(value: &str) -> Result<(), EvolutionError> {
    if value.is_empty()
        || value.len() > MAX_HARNESS_ENTRY_ID_BYTES
        || !value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || byte == b'-'
                || byte == b'_'
                || byte == b'.'
        })
    {
        return Err(invalid(format!(
            "entry ID '{value}' must be 1..={MAX_HARNESS_ENTRY_ID_BYTES} bytes of lowercase \
             letters, digits, '-', '_' or '.' and start with a letter or digit"
        )));
    }
    Ok(())
}

fn derive_entry_id(title: &str) -> Result<String, EvolutionError> {
    let mut slug = String::new();
    let mut pending_separator = false;
    for character in title.chars() {
        if character.is_ascii_alphanumeric() {
            if pending_separator && !slug.is_empty() {
                slug.push('-');
            }
            pending_separator = false;
            slug.push(character.to_ascii_lowercase());
        } else {
            pending_separator = true;
        }
        if slug.len() >= MAX_HARNESS_ENTRY_ID_BYTES {
            break;
        }
    }
    validate_entry_id(&slug)
        .map_err(|_| invalid(format!("cannot derive an entry ID from title '{title}'")))?;
    Ok(slug)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefinementAction {
    Create,
    Update,
    Delete,
}

impl fmt::Display for RefinementAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        })
    }
}

/// One structured edit inside a refinement proposal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RefinementEdit {
    action: RefinementAction,
    kind: HarnessEntryKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    skill: Option<HarnessSkillContract>,
    /// Optimistic concurrency baseline: the entry version this edit was
    /// planned against. A mismatch rejects only this edit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_version: Option<u64>,
    reason: String,
}

impl RefinementEdit {
    pub fn create(
        kind: HarnessEntryKind,
        title: impl Into<String>,
        content: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            action: RefinementAction::Create,
            kind,
            id: None,
            title: Some(title.into()),
            content: Some(content.into()),
            skill: None,
            expected_version: None,
            reason: reason.into(),
        }
    }

    pub fn update(
        kind: HarnessEntryKind,
        id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            action: RefinementAction::Update,
            kind,
            id: Some(id.into()),
            title: None,
            content: None,
            skill: None,
            expected_version: None,
            reason: reason.into(),
        }
    }

    pub fn delete(
        kind: HarnessEntryKind,
        id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            action: RefinementAction::Delete,
            kind,
            id: Some(id.into()),
            title: None,
            content: None,
            skill: None,
            expected_version: None,
            reason: reason.into(),
        }
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_content(mut self, content: impl Into<String>) -> Self {
        self.content = Some(content.into());
        self
    }

    pub fn with_skill(mut self, skill: HarnessSkillContract) -> Self {
        self.skill = Some(skill);
        self
    }

    pub fn with_expected_version(mut self, version: u64) -> Self {
        self.expected_version = Some(version);
        self
    }

    pub fn action(&self) -> RefinementAction {
        self.action
    }

    pub fn kind(&self) -> HarnessEntryKind {
        self.kind
    }

    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    fn validate(&self) -> Result<(), EvolutionError> {
        validate_text(&self.reason, MAX_REFINEMENT_TEXT_BYTES, "edit reason", true)?;
        if let Some(id) = &self.id {
            validate_entry_id(id)?;
        }
        match self.action {
            RefinementAction::Create => {
                if self.title.is_none() || self.content.is_none() {
                    return Err(invalid("a create edit requires a title and content"));
                }
            }
            RefinementAction::Update => {
                if self.id.is_none() {
                    return Err(invalid("an update edit requires an entry ID"));
                }
                if self.title.is_none() && self.content.is_none() && self.skill.is_none() {
                    return Err(invalid("an update edit must change at least one field"));
                }
            }
            RefinementAction::Delete => {
                if self.id.is_none() {
                    return Err(invalid("a delete edit requires an entry ID"));
                }
                if self.title.is_some() || self.content.is_some() || self.skill.is_some() {
                    return Err(invalid("a delete edit must not carry replacement content"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct RefinementEditWire {
    action: RefinementAction,
    kind: HarnessEntryKind,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    skill: Option<HarnessSkillContract>,
    #[serde(default)]
    expected_version: Option<u64>,
    reason: String,
}

impl<'de> Deserialize<'de> for RefinementEdit {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = RefinementEditWire::deserialize(deserializer)?;
        let edit = Self {
            action: wire.action,
            kind: wire.kind,
            id: wire.id,
            title: wire.title,
            content: wire.content,
            skill: wire.skill,
            expected_version: wire.expected_version,
            reason: wire.reason,
        };
        edit.validate().map_err(D::Error::custom)?;
        Ok(edit)
    }
}

/// A structured refinement plan against exactly one scope. A proposal is
/// content, not authority: it changes nothing until a ledger applies it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RefinementProposal {
    summary: String,
    rationale: String,
    expected_outcome: String,
    edits: Vec<RefinementEdit>,
}

impl RefinementProposal {
    pub fn new(
        summary: impl Into<String>,
        rationale: impl Into<String>,
        expected_outcome: impl Into<String>,
        edits: impl IntoIterator<Item = RefinementEdit>,
    ) -> Result<Self, EvolutionError> {
        let proposal = Self {
            summary: summary.into(),
            rationale: rationale.into(),
            expected_outcome: expected_outcome.into(),
            edits: edits.into_iter().collect(),
        };
        proposal.validate()?;
        Ok(proposal)
    }

    /// Parses a planner model reply. Accepts a fenced or bare JSON object and
    /// rejects everything else; truncated output fails closed instead of
    /// applying a partial plan.
    pub fn from_model_reply(reply: &str) -> Result<Self, EvolutionError> {
        let body = extract_json_object(reply)?;
        let wire: RefinementProposalWire = serde_json::from_str(body)
            .map_err(|error| EvolutionError::Serialization(error.to_string()))?;
        Self::new(
            wire.summary,
            wire.rationale,
            wire.expected_outcome,
            wire.edits,
        )
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn rationale(&self) -> &str {
        &self.rationale
    }

    pub fn expected_outcome(&self) -> &str {
        &self.expected_outcome
    }

    pub fn edits(&self) -> &[RefinementEdit] {
        &self.edits
    }

    pub fn validate(&self) -> Result<(), EvolutionError> {
        validate_text(
            &self.summary,
            MAX_REFINEMENT_TEXT_BYTES,
            "proposal summary",
            true,
        )?;
        validate_text(
            &self.rationale,
            MAX_REFINEMENT_TEXT_BYTES,
            "proposal rationale",
            true,
        )?;
        validate_text(
            &self.expected_outcome,
            MAX_REFINEMENT_TEXT_BYTES,
            "proposal expected outcome",
            true,
        )?;
        if self.edits.is_empty() || self.edits.len() > MAX_REFINEMENT_EDITS {
            return Err(invalid(format!(
                "a proposal must contain 1..={MAX_REFINEMENT_EDITS} edits"
            )));
        }
        for edit in &self.edits {
            edit.validate()?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct RefinementProposalWire {
    summary: String,
    rationale: String,
    expected_outcome: String,
    edits: Vec<RefinementEdit>,
}

impl<'de> Deserialize<'de> for RefinementProposal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = RefinementProposalWire::deserialize(deserializer)?;
        Self::new(
            wire.summary,
            wire.rationale,
            wire.expected_outcome,
            wire.edits,
        )
        .map_err(D::Error::custom)
    }
}

fn extract_json_object(reply: &str) -> Result<&str, EvolutionError> {
    let trimmed = reply.trim();
    let start = trimmed.find('{').ok_or_else(|| {
        EvolutionError::Serialization("planner reply contains no JSON object".into())
    })?;
    let end = trimmed.rfind('}').ok_or_else(|| {
        EvolutionError::Serialization("planner reply contains an unterminated JSON object".into())
    })?;
    if end < start {
        return Err(EvolutionError::Serialization(
            "planner reply contains no complete JSON object".into(),
        ));
    }
    Ok(&trimmed[start..=end])
}

/// One committed edit with its exact before/after entry snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AppliedEdit {
    index: u32,
    entry_id: String,
    action: RefinementAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    before: Option<HarnessEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    after: Option<HarnessEntry>,
}

impl AppliedEdit {
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn entry_id(&self) -> &str {
        &self.entry_id
    }

    pub fn action(&self) -> RefinementAction {
        self.action
    }

    pub fn before(&self) -> Option<&HarnessEntry> {
        self.before.as_ref()
    }

    pub fn after(&self) -> Option<&HarnessEntry> {
        self.after.as_ref()
    }

    fn validate(&self) -> Result<(), EvolutionError> {
        validate_entry_id(&self.entry_id)?;
        let (before_expected, after_expected) = match self.action {
            RefinementAction::Create => (false, true),
            RefinementAction::Update => (true, true),
            RefinementAction::Delete => (true, false),
        };
        if self.before.is_some() != before_expected || self.after.is_some() != after_expected {
            return Err(invalid(format!(
                "a {} applied edit records inconsistent before/after snapshots",
                self.action
            )));
        }
        for entry in self.before.iter().chain(self.after.iter()) {
            entry.validate()?;
            if entry.id != self.entry_id {
                return Err(invalid(
                    "an applied edit snapshot does not address its own entry",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct AppliedEditWire {
    index: u32,
    entry_id: String,
    action: RefinementAction,
    #[serde(default)]
    before: Option<HarnessEntry>,
    #[serde(default)]
    after: Option<HarnessEntry>,
}

impl<'de> Deserialize<'de> for AppliedEdit {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = AppliedEditWire::deserialize(deserializer)?;
        let edit = Self {
            index: wire.index,
            entry_id: wire.entry_id,
            action: wire.action,
            before: wire.before,
            after: wire.after,
        };
        edit.validate().map_err(D::Error::custom)?;
        Ok(edit)
    }
}

/// One edit the ledger refused, kept as durable evidence of the failed intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedEdit {
    index: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry_id: Option<String>,
    error: String,
}

impl RejectedEdit {
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn entry_id(&self) -> Option<&str> {
        self.entry_id.as_deref()
    }

    pub fn error(&self) -> &str {
        &self.error
    }

    fn validate(&self) -> Result<(), EvolutionError> {
        validate_text(
            &self.error,
            MAX_REFINEMENT_TEXT_BYTES,
            "rejection error",
            false,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RefinementEventKind {
    Applied,
    Rollback { of_event_id: String },
}

/// Append-only record of one refinement application or rollback. The event ID
/// is a content address, so stored history cannot be silently rewritten.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RefinementEvent {
    schema_version: u32,
    event_id: String,
    scope: HarnessScope,
    kind: RefinementEventKind,
    based_on_revision: u64,
    resulting_revision: u64,
    summary: String,
    rationale: String,
    expected_outcome: String,
    applied: Vec<AppliedEdit>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rejected: Vec<RejectedEdit>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    evidence: Vec<HarnessEvidenceRef>,
    created_at_ms: u64,
}

impl RefinementEvent {
    #[allow(clippy::too_many_arguments)]
    fn build(
        scope: HarnessScope,
        kind: RefinementEventKind,
        based_on_revision: u64,
        summary: String,
        rationale: String,
        expected_outcome: String,
        applied: Vec<AppliedEdit>,
        rejected: Vec<RejectedEdit>,
        evidence: Vec<HarnessEvidenceRef>,
        created_at_ms: u64,
    ) -> Result<Self, EvolutionError> {
        let resulting_revision = based_on_revision
            .checked_add(1)
            .ok_or(EvolutionError::RevisionOverflow)?;
        let mut event = Self {
            schema_version: REFINEMENT_EVENT_SCHEMA_VERSION,
            event_id: String::new(),
            scope,
            kind,
            based_on_revision,
            resulting_revision,
            summary,
            rationale,
            expected_outcome,
            applied,
            rejected,
            evidence,
            created_at_ms,
        };
        event.event_id = event.compute_event_id()?;
        event.validate()?;
        Ok(event)
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn scope(&self) -> &HarnessScope {
        &self.scope
    }

    pub fn kind(&self) -> &RefinementEventKind {
        &self.kind
    }

    pub fn based_on_revision(&self) -> u64 {
        self.based_on_revision
    }

    pub fn resulting_revision(&self) -> u64 {
        self.resulting_revision
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn rationale(&self) -> &str {
        &self.rationale
    }

    pub fn expected_outcome(&self) -> &str {
        &self.expected_outcome
    }

    pub fn applied(&self) -> &[AppliedEdit] {
        &self.applied
    }

    pub fn rejected(&self) -> &[RejectedEdit] {
        &self.rejected
    }

    pub fn evidence(&self) -> &[HarnessEvidenceRef] {
        &self.evidence
    }

    pub fn created_at_ms(&self) -> u64 {
        self.created_at_ms
    }

    pub fn to_json_vec(&self) -> Result<Vec<u8>, EvolutionError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| EvolutionError::Serialization(error.to_string()))?;
        if bytes.len() > MAX_REFINEMENT_EVENT_BYTES {
            return Err(EvolutionError::EventTooLarge {
                actual: bytes.len(),
                maximum: MAX_REFINEMENT_EVENT_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, EvolutionError> {
        if bytes.len() > MAX_REFINEMENT_EVENT_BYTES {
            return Err(EvolutionError::EventTooLarge {
                actual: bytes.len(),
                maximum: MAX_REFINEMENT_EVENT_BYTES,
            });
        }
        let wire: RefinementEventWire = serde_json::from_slice(bytes)
            .map_err(|error| EvolutionError::Serialization(error.to_string()))?;
        Self::from_wire(wire)
    }

    fn from_wire(wire: RefinementEventWire) -> Result<Self, EvolutionError> {
        let event = Self {
            schema_version: wire.schema_version,
            event_id: wire.event_id,
            scope: wire.scope,
            kind: wire.kind,
            based_on_revision: wire.based_on_revision,
            resulting_revision: wire.resulting_revision,
            summary: wire.summary,
            rationale: wire.rationale,
            expected_outcome: wire.expected_outcome,
            applied: wire.applied,
            rejected: wire.rejected,
            evidence: wire.evidence,
            created_at_ms: wire.created_at_ms,
        };
        event.validate()?;
        Ok(event)
    }

    fn validate(&self) -> Result<(), EvolutionError> {
        if self.schema_version != REFINEMENT_EVENT_SCHEMA_VERSION {
            return Err(invalid(format!(
                "unsupported refinement event schema {}",
                self.schema_version
            )));
        }
        self.scope.validate()?;
        if self
            .based_on_revision
            .checked_add(1)
            .is_none_or(|next| next != self.resulting_revision)
        {
            return Err(invalid(
                "a refinement event must advance its revision by exactly one",
            ));
        }
        validate_text(
            &self.summary,
            MAX_REFINEMENT_TEXT_BYTES,
            "event summary",
            true,
        )?;
        validate_text(
            &self.rationale,
            MAX_REFINEMENT_TEXT_BYTES,
            "event rationale",
            true,
        )?;
        validate_text(
            &self.expected_outcome,
            MAX_REFINEMENT_TEXT_BYTES,
            "event expected outcome",
            true,
        )?;
        if let RefinementEventKind::Rollback { of_event_id } = &self.kind {
            validate_sha256_id(of_event_id, "rolled-back event ID")?;
        }
        if self.applied.is_empty() && self.rejected.is_empty() {
            return Err(invalid("a refinement event must record at least one edit"));
        }
        if self.applied.len() + self.rejected.len() > MAX_REFINEMENT_EDITS {
            return Err(invalid(format!(
                "a refinement event records more than {MAX_REFINEMENT_EDITS} edits"
            )));
        }
        let mut indexes = std::collections::BTreeSet::new();
        for index in self
            .applied
            .iter()
            .map(AppliedEdit::index)
            .chain(self.rejected.iter().map(RejectedEdit::index))
        {
            if !indexes.insert(index) {
                return Err(invalid("a refinement event records one edit index twice"));
            }
        }
        for edit in &self.applied {
            edit.validate()?;
        }
        for rejection in &self.rejected {
            rejection.validate()?;
        }
        if self.evidence.len() > MAX_HARNESS_EVIDENCE_REFS {
            return Err(invalid(format!(
                "a refinement event cites more than {MAX_HARNESS_EVIDENCE_REFS} evidence references"
            )));
        }
        if self.event_id != self.compute_event_id()? {
            return Err(invalid(
                "refinement event identity does not match its contents",
            ));
        }
        Ok(())
    }

    fn compute_event_id(&self) -> Result<String, EvolutionError> {
        let payload = RefinementEventPayload {
            schema_version: self.schema_version,
            scope: &self.scope,
            kind: &self.kind,
            based_on_revision: self.based_on_revision,
            resulting_revision: self.resulting_revision,
            summary: &self.summary,
            rationale: &self.rationale,
            expected_outcome: &self.expected_outcome,
            applied: &self.applied,
            rejected: &self.rejected,
            evidence: &self.evidence,
            created_at_ms: self.created_at_ms,
        };
        digest_json(REFINEMENT_EVENT_DIGEST_DOMAIN, &payload)
    }
}

#[derive(Serialize)]
struct RefinementEventPayload<'a> {
    schema_version: u32,
    scope: &'a HarnessScope,
    kind: &'a RefinementEventKind,
    based_on_revision: u64,
    resulting_revision: u64,
    summary: &'a str,
    rationale: &'a str,
    expected_outcome: &'a str,
    applied: &'a [AppliedEdit],
    rejected: &'a [RejectedEdit],
    evidence: &'a [HarnessEvidenceRef],
    created_at_ms: u64,
}

#[derive(Deserialize)]
struct RefinementEventWire {
    schema_version: u32,
    event_id: String,
    scope: HarnessScope,
    kind: RefinementEventKind,
    based_on_revision: u64,
    resulting_revision: u64,
    summary: String,
    rationale: String,
    expected_outcome: String,
    applied: Vec<AppliedEdit>,
    #[serde(default)]
    rejected: Vec<RejectedEdit>,
    #[serde(default)]
    evidence: Vec<HarnessEvidenceRef>,
    created_at_ms: u64,
}

impl<'de> Deserialize<'de> for RefinementEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = RefinementEventWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}

fn digest_json<T: Serialize>(domain: &[u8], value: &T) -> Result<String, EvolutionError> {
    let mut value = serde_json::to_value(value)
        .map_err(|error| EvolutionError::Serialization(error.to_string()))?;
    crate::canonicalize_json(&mut value);
    let bytes = serde_json::to_vec(&value)
        .map_err(|error| EvolutionError::Serialization(error.to_string()))?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn validate_sha256_id(value: &str, name: &str) -> Result<(), EvolutionError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(invalid(format!("{name} must use the sha256 prefix")));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(format!(
            "{name} must contain exactly 32 SHA-256 bytes"
        )));
    }
    Ok(())
}

/// One scope's complete evolution ledger state. The revision is a state-machine
/// counter advanced by exactly one per committed event; stores CAS on it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct EvolutionState {
    schema_version: u32,
    scope: HarnessScope,
    revision: u64,
    entries: BTreeMap<String, HarnessEntry>,
}

impl EvolutionState {
    pub fn new(scope: HarnessScope) -> Result<Self, EvolutionError> {
        scope.validate()?;
        Ok(Self {
            schema_version: EVOLUTION_STATE_SCHEMA_VERSION,
            scope,
            revision: 0,
            entries: BTreeMap::new(),
        })
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn scope(&self) -> &HarnessScope {
        &self.scope
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn entries(&self) -> impl Iterator<Item = &HarnessEntry> {
        self.entries.values()
    }

    pub fn entry(&self, id: &str) -> Option<&HarnessEntry> {
        self.entries.get(id)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Applies one proposal with per-edit isolation: each valid edit commits,
    /// each invalid edit becomes durable rejection evidence, and the revision
    /// advances exactly once. `source` names the actor recorded on touched
    /// entries, such as `refine` or `host`.
    pub fn apply(
        &self,
        proposal: &RefinementProposal,
        source: &str,
        evidence: impl IntoIterator<Item = HarnessEvidenceRef>,
        now_ms: u64,
    ) -> Result<(Self, RefinementEvent), EvolutionError> {
        proposal.validate()?;
        validate_text(source, MAX_ENTRY_SOURCE_BYTES, "refinement source", true)?;
        let mut entries = self.entries.clone();
        let mut applied = Vec::new();
        let mut rejected = Vec::new();
        for (index, edit) in proposal.edits.iter().enumerate() {
            let index = u32::try_from(index)
                .map_err(|_| invalid("a proposal edit index exceeds the supported range"))?;
            match apply_edit(&mut entries, edit, source, now_ms) {
                Ok((entry_id, before, after)) => applied.push(AppliedEdit {
                    index,
                    entry_id,
                    action: edit.action,
                    before,
                    after,
                }),
                Err(error) => rejected.push(RejectedEdit {
                    index,
                    entry_id: edit.id.clone(),
                    error: error.to_string(),
                }),
            }
        }
        let event = RefinementEvent::build(
            self.scope.clone(),
            RefinementEventKind::Applied,
            self.revision,
            proposal.summary.clone(),
            proposal.rationale.clone(),
            proposal.expected_outcome.clone(),
            applied,
            rejected,
            evidence.into_iter().collect(),
            now_ms,
        )?;
        let state = Self {
            schema_version: self.schema_version,
            scope: self.scope.clone(),
            revision: event.resulting_revision,
            entries,
        };
        state.validate()?;
        Ok((state, event))
    }

    /// Reverses one applied event atomically. Unlike `apply`, rollback is
    /// all-or-nothing: any entry touched since the event fails the whole
    /// rollback closed instead of guessing a merge.
    pub fn rollback(
        &self,
        event: &RefinementEvent,
        reason: &str,
        now_ms: u64,
    ) -> Result<(Self, RefinementEvent), EvolutionError> {
        event.validate()?;
        validate_text(reason, MAX_REFINEMENT_TEXT_BYTES, "rollback reason", true)?;
        if event.scope != self.scope {
            return Err(EvolutionError::ScopeMismatch {
                expected: self.scope.to_string(),
                actual: event.scope.to_string(),
            });
        }
        if !matches!(event.kind, RefinementEventKind::Applied) {
            return Err(EvolutionError::RollbackConflict(
                "only an applied refinement event can be rolled back".into(),
            ));
        }
        let mut entries = self.entries.clone();
        let mut inverted = Vec::new();
        for edit in event.applied.iter().rev() {
            let current = entries.get(&edit.entry_id);
            match (&edit.before, &edit.after) {
                (None, Some(after)) => {
                    if current != Some(after) {
                        return Err(rollback_conflict(&edit.entry_id));
                    }
                    entries.remove(&edit.entry_id);
                }
                (Some(before), Some(after)) => {
                    if current != Some(after) {
                        return Err(rollback_conflict(&edit.entry_id));
                    }
                    entries.insert(edit.entry_id.clone(), before.clone());
                }
                (Some(before), None) => {
                    if current.is_some() {
                        return Err(rollback_conflict(&edit.entry_id));
                    }
                    entries.insert(edit.entry_id.clone(), before.clone());
                }
                (None, None) => {
                    return Err(EvolutionError::RollbackConflict(
                        "an applied edit records no snapshots".into(),
                    ));
                }
            }
            inverted.push(AppliedEdit {
                index: edit.index,
                entry_id: edit.entry_id.clone(),
                action: match edit.action {
                    RefinementAction::Create => RefinementAction::Delete,
                    RefinementAction::Update => RefinementAction::Update,
                    RefinementAction::Delete => RefinementAction::Create,
                },
                before: edit.after.clone(),
                after: edit.before.clone(),
            });
        }
        let evidence = vec![HarnessEvidenceRef::new(
            HarnessEvidenceKind::Refinement,
            event.event_id.clone(),
        )?];
        let rollback_event = RefinementEvent::build(
            self.scope.clone(),
            RefinementEventKind::Rollback {
                of_event_id: event.event_id.clone(),
            },
            self.revision,
            format!(
                "rollback of refinement {}",
                &event.event_id[..15.min(event.event_id.len())]
            ),
            reason.to_owned(),
            "restore the exact pre-refinement harness entries".to_owned(),
            inverted,
            Vec::new(),
            evidence,
            now_ms,
        )?;
        let state = Self {
            schema_version: self.schema_version,
            scope: self.scope.clone(),
            revision: rollback_event.resulting_revision,
            entries,
        };
        state.validate()?;
        Ok((state, rollback_event))
    }

    fn total_text_bytes(&self) -> usize {
        self.entries.values().map(HarnessEntry::text_bytes).sum()
    }

    pub fn validate(&self) -> Result<(), EvolutionError> {
        if self.schema_version != EVOLUTION_STATE_SCHEMA_VERSION {
            return Err(invalid(format!(
                "unsupported evolution state schema {}",
                self.schema_version
            )));
        }
        self.scope.validate()?;
        if self.entries.len() > MAX_HARNESS_ENTRIES_PER_SCOPE {
            return Err(invalid(format!(
                "a scope holds {} entries; maximum is {MAX_HARNESS_ENTRIES_PER_SCOPE}",
                self.entries.len()
            )));
        }
        if self.total_text_bytes() > MAX_HARNESS_SCOPE_CONTENT_BYTES {
            return Err(invalid(format!(
                "a scope's entry text exceeds {MAX_HARNESS_SCOPE_CONTENT_BYTES} bytes"
            )));
        }
        for (key, entry) in &self.entries {
            entry.validate()?;
            if key != &entry.id {
                return Err(invalid("an entry is stored under a foreign ID"));
            }
        }
        Ok(())
    }

    pub fn to_json_vec(&self) -> Result<Vec<u8>, EvolutionError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| EvolutionError::Serialization(error.to_string()))?;
        if bytes.len() > MAX_EVOLUTION_STATE_BYTES {
            return Err(EvolutionError::StateTooLarge {
                actual: bytes.len(),
                maximum: MAX_EVOLUTION_STATE_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, EvolutionError> {
        if bytes.len() > MAX_EVOLUTION_STATE_BYTES {
            return Err(EvolutionError::StateTooLarge {
                actual: bytes.len(),
                maximum: MAX_EVOLUTION_STATE_BYTES,
            });
        }
        let wire: EvolutionStateWire = serde_json::from_slice(bytes)
            .map_err(|error| EvolutionError::Serialization(error.to_string()))?;
        let state = Self {
            schema_version: wire.schema_version,
            scope: wire.scope,
            revision: wire.revision,
            entries: wire.entries,
        };
        state.validate()?;
        Ok(state)
    }
}

fn rollback_conflict(entry_id: &str) -> EvolutionError {
    EvolutionError::RollbackConflict(format!(
        "entry '{entry_id}' changed after the event being rolled back"
    ))
}

#[derive(Deserialize)]
struct EvolutionStateWire {
    schema_version: u32,
    scope: HarnessScope,
    revision: u64,
    entries: BTreeMap<String, HarnessEntry>,
}

impl<'de> Deserialize<'de> for EvolutionState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = EvolutionStateWire::deserialize(deserializer)?;
        let state = Self {
            schema_version: wire.schema_version,
            scope: wire.scope,
            revision: wire.revision,
            entries: wire.entries,
        };
        state.validate().map_err(D::Error::custom)?;
        Ok(state)
    }
}

type EditOutcome = (String, Option<HarnessEntry>, Option<HarnessEntry>);

fn apply_edit(
    entries: &mut BTreeMap<String, HarnessEntry>,
    edit: &RefinementEdit,
    source: &str,
    now_ms: u64,
) -> Result<EditOutcome, EvolutionError> {
    edit.validate()?;
    let entry_id = match (&edit.id, &edit.title) {
        (Some(id), _) => id.clone(),
        (None, Some(title)) => derive_entry_id(title)?,
        (None, None) => return Err(invalid("an edit names no entry")),
    };
    match edit.action {
        RefinementAction::Create => {
            if entries.contains_key(&entry_id) {
                return Err(invalid(format!("entry '{entry_id}' already exists")));
            }
            let entry = HarnessEntry {
                id: entry_id.clone(),
                kind: edit.kind,
                title: edit
                    .title
                    .clone()
                    .expect("validated create edits carry a title"),
                content: edit
                    .content
                    .clone()
                    .expect("validated create edits carry content"),
                skill: edit.skill.clone(),
                source: source.to_owned(),
                version: 1,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
            };
            entry.validate()?;
            entries.insert(entry_id.clone(), entry.clone());
            if let Err(error) = check_budgets(entries) {
                entries.remove(&entry_id);
                return Err(error);
            }
            Ok((entry_id, None, Some(entry)))
        }
        RefinementAction::Update => {
            let existing = entries
                .get(&entry_id)
                .ok_or_else(|| invalid(format!("entry '{entry_id}' does not exist")))?
                .clone();
            check_edit_preconditions(edit, &existing)?;
            let mut updated = existing.clone();
            if let Some(title) = &edit.title {
                updated.title = title.clone();
            }
            if let Some(content) = &edit.content {
                updated.content = content.clone();
            }
            if let Some(skill) = &edit.skill {
                updated.skill = Some(skill.clone());
            }
            updated.source = source.to_owned();
            updated.version = existing
                .version
                .checked_add(1)
                .ok_or(EvolutionError::RevisionOverflow)?;
            updated.updated_at_ms = now_ms;
            updated.validate()?;
            entries.insert(entry_id.clone(), updated.clone());
            if let Err(error) = check_budgets(entries) {
                entries.insert(entry_id.clone(), existing);
                return Err(error);
            }
            Ok((entry_id, Some(existing), Some(updated)))
        }
        RefinementAction::Delete => {
            let existing = entries
                .get(&entry_id)
                .ok_or_else(|| invalid(format!("entry '{entry_id}' does not exist")))?
                .clone();
            check_edit_preconditions(edit, &existing)?;
            entries.remove(&entry_id);
            Ok((entry_id, Some(existing), None))
        }
    }
}

fn check_edit_preconditions(
    edit: &RefinementEdit,
    existing: &HarnessEntry,
) -> Result<(), EvolutionError> {
    if existing.kind != edit.kind {
        return Err(invalid(format!(
            "entry '{}' is a {} entry, not a {} entry",
            existing.id, existing.kind, edit.kind
        )));
    }
    if let Some(expected) = edit.expected_version
        && expected != existing.version
    {
        return Err(invalid(format!(
            "entry '{}' changed during refinement planning: expected version {expected}, \
             actual {}",
            existing.id, existing.version
        )));
    }
    Ok(())
}

fn check_budgets(entries: &BTreeMap<String, HarnessEntry>) -> Result<(), EvolutionError> {
    if entries.len() > MAX_HARNESS_ENTRIES_PER_SCOPE {
        return Err(invalid(format!(
            "the scope would exceed {MAX_HARNESS_ENTRIES_PER_SCOPE} entries"
        )));
    }
    let total: usize = entries.values().map(HarnessEntry::text_bytes).sum();
    if total > MAX_HARNESS_SCOPE_CONTENT_BYTES {
        return Err(invalid(format!(
            "the scope's entry text would exceed {MAX_HARNESS_SCOPE_CONTENT_BYTES} bytes"
        )));
    }
    Ok(())
}

/// Renders the merged supplement section for a scope pair, session entries
/// overlaying global entries of the same ID. Returns `None` when there is
/// nothing to inject. Rendering is infallible by construction once the inputs
/// validate: budgets are enforced at apply time.
pub fn render_harness_supplement(
    global: Option<&EvolutionState>,
    session: Option<&EvolutionState>,
    recent_events: &[RefinementEvent],
) -> Result<Option<String>, EvolutionError> {
    if let Some(state) = global {
        state.validate()?;
        if !state.scope.is_global() {
            return Err(EvolutionError::ScopeMismatch {
                expected: "global".into(),
                actual: state.scope.to_string(),
            });
        }
    }
    if let Some(state) = session {
        state.validate()?;
        if state.scope.is_global() {
            return Err(EvolutionError::ScopeMismatch {
                expected: "session".into(),
                actual: state.scope.to_string(),
            });
        }
    }
    let mut merged: BTreeMap<&str, (&HarnessEntry, &'static str)> = BTreeMap::new();
    for entry in global.into_iter().flat_map(EvolutionState::entries) {
        merged.insert(&entry.id, (entry, "global"));
    }
    for entry in session.into_iter().flat_map(EvolutionState::entries) {
        merged.insert(&entry.id, (entry, "session"));
    }
    if merged.is_empty() {
        return Ok(None);
    }
    let mut body = String::new();
    body.push_str(HARNESS_SUPPLEMENT_BEGIN);
    body.push_str(
        "\n# Continual Harness State\nThis supplement was produced by validated refinements of \
         earlier work. Follow the prompt notes, treat memories as established facts, and use \
         skills and subagents through their stated contracts.\n",
    );
    for kind in [
        HarnessEntryKind::Prompt,
        HarnessEntryKind::Memory,
        HarnessEntryKind::Skill,
        HarnessEntryKind::Subagent,
    ] {
        let section: Vec<&(&HarnessEntry, &'static str)> = merged
            .values()
            .filter(|(entry, _)| entry.kind == kind)
            .collect();
        if section.is_empty() {
            continue;
        }
        body.push_str("\n## ");
        body.push_str(kind.section_heading());
        body.push('\n');
        for (entry, scope) in section {
            body.push_str(&format!(
                "\n### {} ({scope} · {} · v{})\n{}\n",
                entry.title, entry.id, entry.version, entry.content
            ));
            if let Some(skill) = &entry.skill {
                body.push_str(&format!("Invocation: `{}`\n", skill.invocation));
                for (name, doc) in &skill.arguments {
                    body.push_str(&format!("- `{name}`: {doc}\n"));
                }
            }
        }
    }
    let mut history: Vec<&RefinementEvent> = recent_events.iter().collect();
    history.sort_by_key(|event| std::cmp::Reverse(event.resulting_revision));
    history.truncate(MAX_RENDERED_REFINEMENT_HISTORY);
    if !history.is_empty() {
        body.push_str("\n## Recent Refinements\n");
        for event in history {
            body.push_str(&format!(
                "- [{} rev {}] {} (applied {}, rejected {})\n",
                event.scope.label(),
                event.resulting_revision,
                event.summary,
                event.applied.len(),
                event.rejected.len()
            ));
        }
    }
    body.push_str(HARNESS_SUPPLEMENT_END);
    Ok(Some(body))
}

/// Derives the typed refinement that evolves one immutable base snapshot to
/// carry the current merged ledger. Returns `None` when the base snapshot
/// already carries exactly this supplement. The patch CASes on the base
/// digest and cites the producing refinement events as evidence, so applying
/// it yields a new content-addressed snapshot with a complete audit chain.
pub fn evolved_refinement(
    base: &HarnessSnapshot,
    global: Option<&EvolutionState>,
    session: Option<&EvolutionState>,
    recent_events: &[RefinementEvent],
) -> Result<Option<HarnessRefinementPatch>, EvolutionError> {
    base.validate().map_err(EvolutionError::Harness)?;
    let supplement = render_harness_supplement(global, session, recent_events)?;
    let base_prompt = base
        .content()
        .system_prompt_value()
        .expect("validated snapshots always carry a complete system prompt");
    let stripped = strip_harness_supplement(base_prompt)?;
    let stripped = stripped.trim_end();
    let next_prompt = match supplement {
        Some(section) if stripped.is_empty() => section,
        Some(section) => format!("{stripped}\n\n{section}"),
        None => stripped.to_owned(),
    };
    if next_prompt == base_prompt {
        return Ok(None);
    }
    let mut evidence = Vec::new();
    for event in recent_events.iter().take(MAX_HARNESS_EVIDENCE_REFS) {
        evidence.push(HarnessEvidenceRef::new(
            HarnessEvidenceKind::Refinement,
            event.event_id.clone(),
        )?);
    }
    let patch = HarnessRefinementPatch::new(
        base.digest().clone(),
        [HarnessRefinement::SetSystemPrompt(next_prompt)],
    )?
    .with_evidence(evidence)?;
    Ok(Some(patch))
}

/// Removes an existing marked supplement section. Unpaired markers fail
/// closed: a prompt that half-contains a supplement is evidence of tampering
/// or truncation, never something to silently rewrite.
fn strip_harness_supplement(prompt: &str) -> Result<String, EvolutionError> {
    let Some(begin) = prompt.find(HARNESS_SUPPLEMENT_BEGIN) else {
        if prompt.contains(HARNESS_SUPPLEMENT_END) {
            return Err(EvolutionError::MalformedSupplement(
                "an end marker appears without a begin marker".into(),
            ));
        }
        return Ok(prompt.to_owned());
    };
    let after_begin = begin + HARNESS_SUPPLEMENT_BEGIN.len();
    let Some(end_offset) = prompt[after_begin..].find(HARNESS_SUPPLEMENT_END) else {
        return Err(EvolutionError::MalformedSupplement(
            "a begin marker appears without an end marker".into(),
        ));
    };
    let end = after_begin + end_offset + HARNESS_SUPPLEMENT_END.len();
    let remainder = &prompt[end..];
    if remainder.contains(HARNESS_SUPPLEMENT_BEGIN) || remainder.contains(HARNESS_SUPPLEMENT_END) {
        return Err(EvolutionError::MalformedSupplement(
            "a prompt contains more than one supplement section".into(),
        ));
    }
    let mut stripped = prompt[..begin].trim_end().to_owned();
    let tail = remainder.trim_start();
    if !tail.is_empty() {
        if !stripped.is_empty() {
            stripped.push_str("\n\n");
        }
        stripped.push_str(tail);
    }
    Ok(stripped)
}

/// Builds the dedicated planner Turn for one refinement pass. The planner is
/// content only: a Host or agent runs it on any Session and feeds the reply to
/// [`RefinementProposal::from_model_reply`], then applies the proposal through
/// [`EvolutionState::apply`] and an [`EvolutionStore`] commit.
#[derive(Clone, Debug)]
pub struct RefinementPlanner {
    transcript_excerpt: String,
    harness_overview: Option<String>,
    history_lines: Vec<String>,
    instructions: Option<String>,
    allow_global: bool,
}

impl RefinementPlanner {
    /// Keeps the most recent `MAX_PLANNER_TRANSCRIPT_BYTES` of the transcript;
    /// the tail carries the freshest evidence.
    pub fn new(transcript: &str) -> Self {
        let excerpt = if transcript.len() > MAX_PLANNER_TRANSCRIPT_BYTES {
            let mut start = transcript.len() - MAX_PLANNER_TRANSCRIPT_BYTES;
            while !transcript.is_char_boundary(start) {
                start += 1;
            }
            &transcript[start..]
        } else {
            transcript
        };
        Self {
            transcript_excerpt: excerpt.to_owned(),
            harness_overview: None,
            history_lines: Vec::new(),
            instructions: None,
            allow_global: false,
        }
    }

    pub fn harness_overview(mut self, overview: impl Into<String>) -> Self {
        self.harness_overview = Some(overview.into());
        self
    }

    pub fn recent_events(mut self, events: &[RefinementEvent]) -> Self {
        self.history_lines = events
            .iter()
            .map(|event| {
                format!(
                    "- [{} rev {}] {} (applied {}, rejected {}): expected {}",
                    event.scope.label(),
                    event.resulting_revision,
                    event.summary,
                    event.applied.len(),
                    event.rejected.len(),
                    event.expected_outcome
                )
            })
            .collect();
        self
    }

    pub fn instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = Some(instructions.into());
        self
    }

    /// Permits the planner to target the global scope. Global remains an
    /// explicit separate proposal; session-scoped plans stay the default.
    pub fn allow_global(mut self, allow: bool) -> Self {
        self.allow_global = allow;
        self
    }

    pub fn system_prompt(&self) -> String {
        let scope_policy = if self.allow_global {
            "Edits target ONE scope. Prefer the session scope; propose global edits only for \
             stable, broadly reusable lessons, and say so in the rationale."
        } else {
            "Edits target the session scope only. If a lesson deserves global promotion, say so \
             in the rationale but keep the edits session-scoped."
        };
        format!(
            "You are the harness refinement planner for an autonomous coding agent. You convert \
             trajectory evidence into the smallest useful set of edits to the agent's continual \
             harness: supplemental prompt notes, memories, skill contracts, and subagent \
             specifications.\n\nRules:\n- Never modify or restate the base system prompt; the \
             harness is supplemental only.\n- Every edit must be justified by concrete evidence \
             from the trajectory; skip one-off noise and unsupported hypotheses.\n- Classify \
             correctly: behavioral guidance is `prompt`, facts and outcomes are `memory`, \
             callable-capability routing is `skill`, delegation roles are `subagent`.\n- A \
             `skill` edit must include a `skill` object with an `invocation` string and an \
             `arguments` map describing an already-available capability; never invent \
             capabilities.\n- For update/delete edits, copy `expected_version` from the current \
             harness overview.\n- Emit the smallest change that captures the lesson; prefer \
             updating an existing entry over creating a near-duplicate.\n- {scope_policy}\n\n\
             Reply with ONE JSON object and nothing else:\n{{\n  \"summary\": \"...\",\n  \
             \"rationale\": \"...\",\n  \"expected_outcome\": \"...\",\n  \"edits\": [\n    {{\n      \
             \"action\": \"create|update|delete\",\n      \"kind\": \
             \"prompt|memory|skill|subagent\",\n      \"id\": \"existing-id-for-update-or-delete\",\n      \
             \"title\": \"...\",\n      \"content\": \"...\",\n      \"skill\": {{\"invocation\": \
             \"...\", \"arguments\": {{\"name\": \"doc\"}}}},\n      \"expected_version\": 1,\n      \
             \"reason\": \"...\"\n    }}\n  ]\n}}\n\nIf the trajectory contains nothing worth \
             persisting, reply with the literal string NO_REFINEMENT instead of JSON."
        )
    }

    pub fn user_prompt(&self) -> String {
        let mut prompt = String::new();
        prompt.push_str("## Current harness\n");
        match &self.harness_overview {
            Some(overview) if !overview.trim().is_empty() => {
                prompt.push_str(overview);
                prompt.push('\n');
            }
            _ => prompt.push_str("(empty)\n"),
        }
        prompt.push_str("\n## Recent refinement outcomes\n");
        if self.history_lines.is_empty() {
            prompt.push_str("(none)\n");
        } else {
            for line in &self.history_lines {
                prompt.push_str(line);
                prompt.push('\n');
            }
        }
        if let Some(instructions) = &self.instructions {
            prompt.push_str("\n## Focus\n");
            prompt.push_str(instructions);
            prompt.push('\n');
        }
        prompt.push_str("\n## Trajectory\n");
        prompt.push_str(&self.transcript_excerpt);
        prompt
    }
}

/// The planner's explicit no-op verdict.
pub const NO_REFINEMENT_REPLY: &str = "NO_REFINEMENT";

/// Interprets a planner reply, distinguishing the explicit no-op verdict from
/// a structured proposal and from malformed output.
pub fn parse_planner_reply(reply: &str) -> Result<Option<RefinementProposal>, EvolutionError> {
    if reply.trim() == NO_REFINEMENT_REPLY {
        return Ok(None);
    }
    RefinementProposal::from_model_reply(reply).map(Some)
}

#[cfg(test)]
mod tests;
