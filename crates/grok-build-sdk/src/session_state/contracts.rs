use super::codec::*;

pub const SESSION_LOG_SCHEMA_MARKER: &str = "grok-build-sdk.session-log";
pub const SESSION_LOG_SCHEMA_VERSION: u32 = 2;
pub const MAX_SESSION_OBJECT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SESSION_MANIFEST_BYTES: usize = 64 * 1024;
pub const TARGET_TRANSCRIPT_SEGMENT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SESSION_IDENTITY_BYTES: usize = 1024;
pub const MAX_SESSION_GENERATION_BYTES: usize = 1024;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SessionStateStoreError {
    #[error("session log validation failed: {0}")]
    Validation(String),
    #[error("session log is corrupt: {0}")]
    Corrupt(String),
    #[error("session log storage failed: {0}")]
    Storage(String),
}

macro_rules! text_type {
    ($name:ident, $max:ident, $label:literal, $method:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize)]
        pub struct $name(pub(super) String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, SessionStateStoreError> {
                let value = value.into();
                valid_text(&value, $max, $label)?;
                Ok(Self(value))
            }
            pub fn $method(&self) -> &str {
                &self.0
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}
text_type!(
    SessionKey,
    MAX_SESSION_IDENTITY_BYTES,
    "session identity",
    session_identity
);
text_type!(
    SessionGeneration,
    MAX_SESSION_GENERATION_BYTES,
    "session generation",
    as_str
);

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize)]
pub struct SessionObjectId(pub(super) String);
impl SessionObjectId {
    pub fn from_stored(value: impl Into<String>) -> Result<Self, SessionStateStoreError> {
        let value = value.into();
        validate_digest(&value).map_err(as_corrupt)?;
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> serde::Deserialize<'de> for SessionObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_stored(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RewindKind {
    AppendPoint,
    Truncate,
    Merge,
}

/// Durable fence for an observed compaction. `IntentPending` proves that no
/// matching checkpoint publication or terminal nonapplication has committed.
/// `NotAppliedPending` preserves the exact terminal reason across a lost Host
/// acknowledgement. `EvidencePending` proves publication committed but
/// volatile installation and/or Host outcome acknowledgement still requires
/// idempotent recovery.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum CompactionManifestState {
    #[default]
    None,
    IntentPending(crate::CompactionIntent),
    NotAppliedPending {
        intent: crate::CompactionIntent,
        reason: crate::CompactionNotAppliedReason,
    },
    EvidencePending(crate::CompactionReceipt),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum SessionObjectKind {
    TranscriptSegment {
        previous: Option<SessionObjectId>,
        sequence: u64,
        bytes: Vec<u8>,
    },
    Checkpoint {
        name: String,
        shell_bytes: Vec<u8>,
    },
    RewindOperation {
        kind: RewindKind,
        index: u64,
        shell_bytes: Vec<u8>,
    },
    CheckpointPublication {
        previous: Option<SessionObjectId>,
        sequence: u64,
        marker_bytes: Vec<u8>,
        checkpoint: SessionObjectId,
    },
    CompactionPublication {
        previous: Option<SessionObjectId>,
        sequence: u64,
        marker_bytes: Vec<u8>,
        checkpoint: SessionObjectId,
        record: crate::CompactionPublicationRecord,
    },
    RewindPublication {
        previous: Option<SessionObjectId>,
        sequence: u64,
        marker_bytes: Vec<u8>,
        operation: SessionObjectId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionObject {
    pub(super) id: SessionObjectId,
    pub(super) session: SessionKey,
    pub(super) generation: SessionGeneration,
    pub(super) kind: SessionObjectKind,
    pub(super) bytes: Vec<u8>,
}
impl SessionObject {
    pub fn transcript(
        session: SessionKey,
        generation: SessionGeneration,
        previous: Option<SessionObjectId>,
        sequence: u64,
        bytes: Vec<u8>,
    ) -> Result<Self, SessionStateStoreError> {
        Self::build(
            session,
            generation,
            SessionObjectKind::TranscriptSegment {
                previous,
                sequence,
                bytes,
            },
        )
    }
    pub fn checkpoint(
        session: SessionKey,
        generation: SessionGeneration,
        name: impl Into<String>,
        shell_bytes: Vec<u8>,
    ) -> Result<Self, SessionStateStoreError> {
        Self::build(
            session,
            generation,
            SessionObjectKind::Checkpoint {
                name: name.into(),
                shell_bytes,
            },
        )
    }
    pub fn rewind(
        session: SessionKey,
        generation: SessionGeneration,
        kind: RewindKind,
        index: u64,
        shell_bytes: Vec<u8>,
    ) -> Result<Self, SessionStateStoreError> {
        Self::build(
            session,
            generation,
            SessionObjectKind::RewindOperation {
                kind,
                index,
                shell_bytes,
            },
        )
    }
    pub fn publish_checkpoint(
        session: SessionKey,
        generation: SessionGeneration,
        previous: Option<SessionObjectId>,
        sequence: u64,
        marker_bytes: Vec<u8>,
        checkpoint: SessionObjectId,
    ) -> Result<Self, SessionStateStoreError> {
        Self::build(
            session,
            generation,
            SessionObjectKind::CheckpointPublication {
                previous,
                sequence,
                marker_bytes,
                checkpoint,
            },
        )
    }
    pub fn publish_rewind(
        session: SessionKey,
        generation: SessionGeneration,
        previous: Option<SessionObjectId>,
        sequence: u64,
        marker_bytes: Vec<u8>,
        operation: SessionObjectId,
    ) -> Result<Self, SessionStateStoreError> {
        Self::build(
            session,
            generation,
            SessionObjectKind::RewindPublication {
                previous,
                sequence,
                marker_bytes,
                operation,
            },
        )
    }
    pub fn publish_compaction(
        session: SessionKey,
        generation: SessionGeneration,
        previous: Option<SessionObjectId>,
        sequence: u64,
        marker_bytes: Vec<u8>,
        checkpoint: SessionObjectId,
        record: crate::CompactionPublicationRecord,
    ) -> Result<Self, SessionStateStoreError> {
        Self::build(
            session,
            generation,
            SessionObjectKind::CompactionPublication {
                previous,
                sequence,
                marker_bytes,
                checkpoint,
                record,
            },
        )
    }
    fn build(
        session: SessionKey,
        generation: SessionGeneration,
        kind: SessionObjectKind,
    ) -> Result<Self, SessionStateStoreError> {
        validate_kind(&kind)?;
        let bytes = encode_object(&session, &generation, &kind)?;
        let id = SessionObjectId(digest(&bytes));
        Ok(Self {
            id,
            session,
            generation,
            kind,
            bytes,
        })
    }
    pub fn from_stored(
        id: SessionObjectId,
        declared_size: u64,
        bytes: Vec<u8>,
    ) -> Result<Self, SessionStateStoreError> {
        if declared_size > MAX_SESSION_OBJECT_BYTES as u64 {
            return Err(corrupt("declared object size exceeds 64 MiB"));
        }
        if declared_size != bytes.len() as u64 {
            return Err(corrupt("object declared size mismatch"));
        }
        if digest(&bytes) != id.0 {
            return Err(corrupt("object digest mismatch"));
        }
        let (session, generation, kind) = decode_object(&bytes)?;
        validate_kind(&kind).map_err(as_corrupt)?;
        Ok(Self {
            id,
            session,
            generation,
            kind,
            bytes,
        })
    }
    pub fn id(&self) -> &SessionObjectId {
        &self.id
    }
    pub fn session(&self) -> &SessionKey {
        &self.session
    }
    pub fn generation(&self) -> &SessionGeneration {
        &self.generation
    }
    pub fn kind(&self) -> &SessionObjectKind {
        &self.kind
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn declared_size(&self) -> u64 {
        self.bytes.len() as u64
    }
    pub fn previous(&self) -> Option<&SessionObjectId> {
        chain_parts(&self.kind).and_then(|x| x.0)
    }
    pub fn sequence(&self) -> Option<u64> {
        chain_parts(&self.kind).map(|x| x.1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionManifest {
    pub(super) session: SessionKey,
    pub(super) generation: SessionGeneration,
    pub(super) head: Option<SessionObjectId>,
    pub(super) segment_count: u64,
    pub(super) transcript_bytes: u64,
    pub(super) compaction_state: CompactionManifestState,
    pub(super) bytes: Vec<u8>,
}
impl SessionManifest {
    pub fn new(
        session: SessionKey,
        generation: SessionGeneration,
        head: Option<SessionObjectId>,
        segment_count: u64,
        transcript_bytes: u64,
    ) -> Result<Self, SessionStateStoreError> {
        Self::new_with_compaction_state(
            session,
            generation,
            head,
            segment_count,
            transcript_bytes,
            CompactionManifestState::None,
        )
    }
    pub fn new_with_compaction_state(
        session: SessionKey,
        generation: SessionGeneration,
        head: Option<SessionObjectId>,
        segment_count: u64,
        transcript_bytes: u64,
        compaction_state: CompactionManifestState,
    ) -> Result<Self, SessionStateStoreError> {
        if head.is_none() && (segment_count != 0 || transcript_bytes != 0) {
            return Err(validation("empty head must have zero counters"));
        }
        validate_compaction_state(&compaction_state)?;
        let bytes = encode_manifest(
            &session,
            &generation,
            head.as_ref(),
            segment_count,
            transcript_bytes,
            &compaction_state,
        )?;
        Ok(Self {
            session,
            generation,
            head,
            segment_count,
            transcript_bytes,
            compaction_state,
            bytes,
        })
    }
    pub fn from_stored(bytes: Vec<u8>) -> Result<Self, SessionStateStoreError> {
        if bytes.len() > MAX_SESSION_MANIFEST_BYTES {
            return Err(corrupt("manifest exceeds 64 KiB"));
        }
        decode_manifest(&bytes)
    }
    pub fn session(&self) -> &SessionKey {
        &self.session
    }
    pub fn generation(&self) -> &SessionGeneration {
        &self.generation
    }
    pub fn head(&self) -> Option<&SessionObjectId> {
        self.head.as_ref()
    }
    pub fn segment_count(&self) -> u64 {
        self.segment_count
    }
    pub fn transcript_bytes(&self) -> u64 {
        self.transcript_bytes
    }
    pub fn compaction_state(&self) -> &CompactionManifestState {
        &self.compaction_state
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> String {
        digest(&self.bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestVersion {
    pub(super) revision: u64,
    pub(super) digest: String,
}
impl ManifestVersion {
    pub fn from_stored(
        revision: u64,
        digest_value: impl Into<String>,
    ) -> Result<Self, SessionStateStoreError> {
        let digest_value = digest_value.into();
        if revision == 0 || revision > i64::MAX as u64 {
            return Err(corrupt("invalid manifest revision"));
        }
        validate_digest(&digest_value).map_err(as_corrupt)?;
        Ok(Self {
            revision,
            digest: digest_value,
        })
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveSessionDocument {
    pub(super) version: ManifestVersion,
    pub(super) manifest: SessionManifest,
}
impl LiveSessionDocument {
    pub fn from_stored(
        version: ManifestVersion,
        manifest: SessionManifest,
    ) -> Result<Self, SessionStateStoreError> {
        if version.digest != manifest.digest() {
            return Err(corrupt("manifest digest mismatch"));
        }
        Ok(Self { version, manifest })
    }
    pub fn version(&self) -> &ManifestVersion {
        &self.version
    }
    pub fn manifest(&self) -> &SessionManifest {
        &self.manifest
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteReceipt {
    pub(super) generation: SessionGeneration,
    pub(super) revision: u64,
    pub(super) prior_digest: String,
}
impl DeleteReceipt {
    pub fn from_stored(
        generation: SessionGeneration,
        revision: u64,
        prior_digest: impl Into<String>,
    ) -> Result<Self, SessionStateStoreError> {
        let prior_digest = prior_digest.into();
        validate_digest(&prior_digest).map_err(as_corrupt)?;
        if revision == 0 {
            return Err(corrupt("invalid delete revision"));
        }
        Ok(Self {
            generation,
            revision,
            prior_digest,
        })
    }
    pub fn generation(&self) -> &SessionGeneration {
        &self.generation
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn prior_digest(&self) -> &str {
        &self.prior_digest
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum SessionSlot {
    Vacant,
    Live(LiveSessionDocument),
    Tombstoned { receipt: DeleteReceipt },
}

#[derive(Clone, Debug)]
pub struct PreparedManifestCas {
    pub(super) key: SessionKey,
    pub(super) expected: Option<LiveSessionDocument>,
    pub(super) successor: LiveSessionDocument,
    pub(super) suffix: Vec<SessionObjectId>,
}
impl PreparedManifestCas {
    pub fn new(
        key: SessionKey,
        expected: Option<LiveSessionDocument>,
        manifest: SessionManifest,
        suffix: &[SessionObject],
    ) -> Result<Self, SessionStateStoreError> {
        if manifest.session != key {
            return Err(validation("manifest session mismatch"));
        }
        if let Some(e) = &expected
            && (e.manifest.session != key || e.manifest.generation != manifest.generation)
        {
            return Err(validation("manifest identity or generation changed"));
        }
        let revision = expected
            .as_ref()
            .map_or(Some(1), |e| e.version.revision.checked_add(1))
            .filter(|x| *x <= i64::MAX as u64)
            .ok_or_else(|| validation("manifest revision overflow"))?;
        validate_suffix(&manifest, expected.as_ref().map(|x| &x.manifest), suffix)?;
        validate_compaction_transition(expected.as_ref(), &manifest, suffix)?;
        let successor = LiveSessionDocument {
            version: ManifestVersion {
                revision,
                digest: manifest.digest(),
            },
            manifest,
        };
        Ok(Self {
            key,
            expected,
            successor,
            suffix: suffix.iter().map(|x| x.id.clone()).collect(),
        })
    }
    pub fn key(&self) -> &SessionKey {
        &self.key
    }
    pub fn expected(&self) -> Option<&LiveSessionDocument> {
        self.expected.as_ref()
    }
    pub fn successor(&self) -> &LiveSessionDocument {
        &self.successor
    }
    pub fn suffix(&self) -> &[SessionObjectId] {
        &self.suffix
    }
}

pub(super) fn validate_compaction_state(
    state: &CompactionManifestState,
) -> Result<(), SessionStateStoreError> {
    match state {
        CompactionManifestState::None => Ok(()),
        CompactionManifestState::IntentPending(intent) => intent.validate().map_err(validation),
        CompactionManifestState::NotAppliedPending { intent, .. } => {
            intent.validate().map_err(validation)
        }
        CompactionManifestState::EvidencePending(receipt) => receipt.validate().map_err(validation),
    }
}

fn validate_compaction_transition(
    expected: Option<&LiveSessionDocument>,
    successor: &SessionManifest,
    suffix: &[SessionObject],
) -> Result<(), SessionStateStoreError> {
    let prior = expected
        .map(|document| document.manifest.compaction_state())
        .unwrap_or(&CompactionManifestState::None);
    let next = successor.compaction_state();
    if prior == next {
        if matches!(prior, CompactionManifestState::None)
            || expected.is_some_and(|document| {
                suffix.is_empty()
                    && document.manifest.head == successor.head
                    && document.manifest.segment_count == successor.segment_count
                    && document.manifest.transcript_bytes == successor.transcript_bytes
            })
        {
            return Ok(());
        }
        return Err(validation(
            "ordinary Session publication is fenced during native compaction",
        ));
    }
    let chain_unchanged = suffix.is_empty()
        && expected.is_some_and(|document| {
            document.manifest.head == successor.head
                && document.manifest.segment_count == successor.segment_count
                && document.manifest.transcript_bytes == successor.transcript_bytes
        });
    match (prior, next) {
        (CompactionManifestState::None, CompactionManifestState::IntentPending(intent))
            if chain_unchanged
                && expected.is_some_and(|document| {
                    intent.base.session.as_str() == document.manifest.session.session_identity()
                        && intent.base.generation == document.manifest.generation
                        && intent.base.manifest_revision == document.version.revision
                        && intent.base.manifest_digest.as_str() == document.version.digest
                        && intent.base.head == document.manifest.head
                        && intent.base.sequence == document.manifest.segment_count
                }) =>
        {
            Ok(())
        }
        (CompactionManifestState::IntentPending(_), CompactionManifestState::None)
        | (CompactionManifestState::NotAppliedPending { .. }, CompactionManifestState::None)
        | (CompactionManifestState::EvidencePending(_), CompactionManifestState::None)
            if chain_unchanged =>
        {
            Ok(())
        }
        (
            CompactionManifestState::IntentPending(intent),
            CompactionManifestState::NotAppliedPending {
                intent: pending, ..
            },
        ) if chain_unchanged && intent == pending => Ok(()),
        (
            CompactionManifestState::IntentPending(intent),
            CompactionManifestState::EvidencePending(receipt),
        ) => {
            let Some(SessionObject {
                id,
                kind:
                    SessionObjectKind::CompactionPublication {
                        checkpoint, record, ..
                    },
                ..
            }) = suffix.last()
            else {
                return Err(validation(
                    "evidence-pending transition lacks a typed compaction publication",
                ));
            };
            if receipt.intent != *intent
                || receipt.publication.publication != *id
                || receipt.publication.checkpoint != *checkpoint
                || receipt.publication.session.as_str() != successor.session.session_identity()
                || receipt.publication.generation != successor.generation
                || receipt.publication.sequence != successor.segment_count
                || receipt.intent != record.intent
                || receipt.summary != record.summary
                || receipt.checkpoint != record.checkpoint
                || receipt.installed_state != record.installed_state
                || receipt.publication.prompt_index != record.prompt_index
            {
                return Err(validation(
                    "evidence-pending receipt differs from its intent or publication",
                ));
            }
            Ok(())
        }
        _ => Err(validation("invalid compaction manifest transition")),
    }
}
#[derive(Clone, Debug)]
pub struct PreparedSessionDelete {
    pub(super) key: SessionKey,
    pub(super) expected: LiveSessionDocument,
    pub(super) receipt: DeleteReceipt,
}
impl PreparedSessionDelete {
    pub fn new(
        key: SessionKey,
        expected: LiveSessionDocument,
    ) -> Result<Self, SessionStateStoreError> {
        if expected.manifest.session != key {
            return Err(validation("delete identity mismatch"));
        }
        let receipt = DeleteReceipt {
            generation: expected.manifest.generation.clone(),
            revision: expected.version.revision,
            prior_digest: expected.version.digest.clone(),
        };
        Ok(Self {
            key,
            expected,
            receipt,
        })
    }
    pub fn key(&self) -> &SessionKey {
        &self.key
    }
    pub fn expected(&self) -> &LiveSessionDocument {
        &self.expected
    }
    pub fn receipt(&self) -> &DeleteReceipt {
        &self.receipt
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectPut {
    Stored,
    AlreadyPresent,
    CommitUnknown,
}
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum ManifestCas {
    Committed(LiveSessionDocument),
    Conflict,
    CommitUnknown,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionDelete {
    Deleted(DeleteReceipt),
    Conflict,
    CommitUnknown,
}
pub fn object_put_reconciled(
    result: &ObjectPut,
    loaded: Option<&SessionObject>,
    intended: &SessionObject,
) -> bool {
    matches!(result, ObjectPut::Stored | ObjectPut::AlreadyPresent)
        || matches!(result, ObjectPut::CommitUnknown) && loaded == Some(intended)
}
pub fn manifest_cas_reconciled(
    result: &ManifestCas,
    slot: &SessionSlot,
    intended: &LiveSessionDocument,
) -> bool {
    matches!(result,ManifestCas::Committed(x) if x==intended)
        || matches!(result, ManifestCas::CommitUnknown)
            && matches!(slot,SessionSlot::Live(x) if x==intended)
}
pub fn delete_reconciled(
    result: &SessionDelete,
    slot: &SessionSlot,
    expected: &LiveSessionDocument,
) -> bool {
    let intended = DeleteReceipt {
        generation: expected.manifest.generation.clone(),
        revision: expected.version.revision,
        prior_digest: expected.version.digest.clone(),
    };
    matches!(result, SessionDelete::Deleted(receipt) if receipt == &intended)
        || matches!(result, SessionDelete::CommitUnknown)
            && matches!(slot,SessionSlot::Tombstoned{receipt} if receipt == &intended)
}

pub trait SessionStateStore: Send + Sync + 'static {
    /// Acquires exclusive admission for one Session identity. The returned
    /// lease must exclude other processes as well as other threads and remain
    /// held for the complete resident or deletion lifetime.
    fn acquire_session_lease(
        &self,
        _key: &SessionKey,
    ) -> Result<Box<dyn SessionStateLease>, SessionStateStoreError> {
        Err(validation(
            "SessionStateStore does not provide cross-runtime Session admission fencing",
        ))
    }
    fn inspect_slot(&self, key: &SessionKey) -> Result<SessionSlot, SessionStateStoreError>;
    fn load_object(
        &self,
        key: &SessionKey,
        generation: &SessionGeneration,
        id: &SessionObjectId,
    ) -> Result<Option<SessionObject>, SessionStateStoreError>;
    fn put_object(&self, object: SessionObject) -> Result<ObjectPut, SessionStateStoreError>;
    fn compare_and_swap_manifest(
        &self,
        request: PreparedManifestCas,
    ) -> Result<ManifestCas, SessionStateStoreError>;
    fn compare_and_delete(
        &self,
        request: PreparedSessionDelete,
    ) -> Result<SessionDelete, SessionStateStoreError>;
}

/// Opaque exclusive Session admission lease. Dropping it releases admission.
pub trait SessionStateLease: Send + Sync + 'static {}
