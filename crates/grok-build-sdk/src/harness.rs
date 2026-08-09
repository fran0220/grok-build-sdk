use crate::SessionConfig;
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
        config.system_prompt = self.system_prompt.clone();
        config.rules = self.rules.clone();
        config
    }
}

/// One typed change proposed against an immutable snapshot.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "value", rename_all = "snake_case")]
pub enum HarnessRefinement {
    SetSystemPrompt(String),
    ClearSystemPrompt,
    SetRules(String),
    ClearRules,
}

impl HarnessRefinement {
    fn target(&self) -> &'static str {
        match self {
            Self::SetSystemPrompt(_) | Self::ClearSystemPrompt => "system_prompt",
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
            Self::ClearSystemPrompt | Self::ClearRules => Ok(()),
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
                HarnessRefinement::ClearSystemPrompt => content.system_prompt = None,
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
    #[error("harness serialization failed: {0}")]
    Serialization(String),
}
