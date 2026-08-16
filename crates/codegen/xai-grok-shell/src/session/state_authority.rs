//! Semantic native-session persistence port for embedded hosts.
//!
//! The shell owns these concepts, but deliberately knows nothing about the
//! SDK's object, digest, manifest, CAS, or storage ABI.

use std::sync::Arc;

#[doc(hidden)]
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("native session state authority failed: {0}")]
pub struct AuthorityError(pub String);

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionIdentity {
    pub identity: String,
    pub generation: String,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionInspection {
    Vacant,
    Live { generation: String },
    Tombstoned { generation: String },
}

/// Stable position between published records. Cursors are scoped to one
/// identity/generation and are invalid after rewind or recreation.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayCursor {
    pub generation: String,
    pub next_sequence: u64,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum RewindOperation {
    AppendPoint { index: u64, payload: Vec<u8> },
    Truncate { index: u64, payload: Vec<u8> },
    Merge { index: u64, payload: Vec<u8> },
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum ReplayRecord {
    Update(Vec<u8>),
    Checkpoint {
        name: String,
        payload: Vec<u8>,
        marker: Vec<u8>,
    },
    Compaction {
        name: String,
        payload: Vec<u8>,
        marker: Vec<u8>,
        record: Box<NativeCompactionReplayRecord>,
    },
    Rewind {
        operation: RewindOperation,
        marker: Vec<u8>,
    },
}

/// Typed prompt ownership supplied directly by an embedded SDK. None of these
/// fields are reconstructed from prompt metadata or Host callbacks.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeCompactionOwner {
    Session,
    Turn {
        turn_id: String,
    },
    AutonomousTurn {
        turn_id: String,
        run_id: String,
        iteration: u64,
        operation_id: String,
    },
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum NativeCompactionReason {
    Manual,
    AutomaticThreshold,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum NativeCompactionRequestPath {
    SinglePassVerbatim,
    SinglePassFitted,
    SinglePassLossy,
    TwoPassFinal,
}

/// One content-free leaf digest over exact canonical semantic request bytes.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct NativeCompactionDigestFacts {
    pub digest: String,
    pub size_bytes: u64,
    pub item_count: u32,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeCompactionInput {
    pub owner: NativeCompactionOwner,
    pub reason: NativeCompactionReason,
    pub path: NativeCompactionRequestPath,
    pub messages: NativeCompactionDigestFacts,
    pub tool_definitions: NativeCompactionDigestFacts,
    pub hosted_tool_declarations: NativeCompactionDigestFacts,
    pub model_parameters: NativeCompactionDigestFacts,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeCompactionBegin {
    Disabled,
    Acknowledged { compaction_id: String },
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeCompactionNotAppliedReason {
    Cancelled,
    ModelFailed,
    InvalidModelOutput,
    InputChanged,
    PublicationAbsent,
    InterruptedBeforePublication,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeCompactionRecovery {
    None,
    EvidencePending {
        compaction_id: String,
        checkpoint_payload: Vec<u8>,
        installed_state: NativeCompactionDigestFacts,
    },
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeCompactionPublicationRecord {
    pub compaction_id: String,
    pub summary: NativeCompactionDigestFacts,
    pub checkpoint: NativeCompactionDigestFacts,
    pub installed_state: NativeCompactionDigestFacts,
    pub prompt_index: u64,
}

/// Complete content-free metadata required to preserve a historical
/// compaction publication across a native Session fork.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct NativeCompactionReplayRecord {
    pub compaction_id: String,
    pub owner: NativeCompactionReplayOwner,
    pub reason: NativeCompactionReason,
    pub base: NativeCompactionBase,
    pub path: NativeCompactionRequestPath,
    pub messages: NativeCompactionDigestFacts,
    pub tool_definitions: NativeCompactionDigestFacts,
    pub hosted_tool_declarations: NativeCompactionDigestFacts,
    pub model_parameters: NativeCompactionDigestFacts,
    pub summary: NativeCompactionDigestFacts,
    pub checkpoint: NativeCompactionDigestFacts,
    pub installed_state: NativeCompactionDigestFacts,
    pub prompt_index: u64,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum NativeCompactionReplayOwner {
    Session {
        session_id: String,
    },
    Turn {
        session_id: String,
        turn_id: String,
    },
    AutonomousTurn {
        session_id: String,
        turn_id: String,
        run_id: String,
        iteration: u64,
        operation_id: String,
    },
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct NativeCompactionBase {
    pub session_id: String,
    pub generation: String,
    pub manifest_revision: u64,
    pub manifest_digest: String,
    pub head: Option<String>,
    pub sequence: u64,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeCompactionPublication {
    pub record: NativeCompactionPublicationRecord,
    pub name: String,
    pub payload: Vec<u8>,
    pub marker: Vec<u8>,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeCompactionErrorKind {
    Absent,
    Rejected,
    Conflict,
    Uncertain,
    Observer,
}

#[doc(hidden)]
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("native compaction {kind:?}: {message}")]
pub struct NativeCompactionError {
    pub kind: NativeCompactionErrorKind,
    pub message: String,
}

impl NativeCompactionError {
    pub fn disabled() -> Self {
        Self {
            kind: NativeCompactionErrorKind::Absent,
            message: "typed native compaction protocol is not configured".into(),
        }
    }
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayPage {
    pub records: Vec<ReplayRecord>,
    pub next: Option<ReplayCursor>,
}

/// One opened native session. Staging is process-local until `flush`; all
/// other publication methods atomically flush staged updates with their marker.
#[doc(hidden)]
#[async_trait::async_trait]
pub trait NativeSession: Send + Sync + 'static {
    fn identity(&self) -> &SessionIdentity;
    fn stage_update(&self, bytes: Vec<u8>) -> Result<(), AuthorityError>;
    fn flush(&self) -> Result<ReplayCursor, AuthorityError>;
    fn replay_page(
        &self,
        cursor: Option<ReplayCursor>,
        max_records: usize,
    ) -> Result<ReplayPage, AuthorityError>;
    fn publish_checkpoint(
        &self,
        name: String,
        payload: Vec<u8>,
        marker: Vec<u8>,
    ) -> Result<ReplayCursor, AuthorityError>;
    fn publish_rewind(
        &self,
        operation: RewindOperation,
        marker: Vec<u8>,
    ) -> Result<ReplayCursor, AuthorityError>;

    async fn begin_compaction(
        &self,
        _input: NativeCompactionInput,
    ) -> Result<NativeCompactionBegin, NativeCompactionError> {
        Ok(NativeCompactionBegin::Disabled)
    }

    async fn compaction_not_applied(
        &self,
        _compaction_id: String,
        _reason: NativeCompactionNotAppliedReason,
    ) -> Result<(), NativeCompactionError> {
        Err(NativeCompactionError::disabled())
    }

    async fn publish_compaction(
        &self,
        _publication: NativeCompactionPublication,
    ) -> Result<(), NativeCompactionError> {
        Err(NativeCompactionError::disabled())
    }

    async fn compaction_applied(
        &self,
        _compaction_id: String,
    ) -> Result<(), NativeCompactionError> {
        Err(NativeCompactionError::disabled())
    }

    async fn recover_compaction(&self) -> Result<NativeCompactionRecovery, NativeCompactionError> {
        Ok(NativeCompactionRecovery::None)
    }
}

#[doc(hidden)]
pub trait NativeSessionStateAuthority: Send + Sync + 'static {
    fn inspect(&self, identity: &str) -> Result<SessionInspection, AuthorityError>;
    fn create(&self, id: SessionIdentity) -> Result<Arc<dyn NativeSession>, AuthorityError>;
    fn open(&self, id: SessionIdentity) -> Result<Arc<dyn NativeSession>, AuthorityError>;
    /// Atomically publishes a fully prepared fork. The target identity must be
    /// vacant and use a fresh generation; no partially copied target becomes
    /// visible if publication fails.
    fn publish_fork(
        &self,
        id: SessionIdentity,
        records: Vec<ReplayRecord>,
    ) -> Result<Arc<dyn NativeSession>, AuthorityError>;
    fn tombstone(&self, id: SessionIdentity) -> Result<(), AuthorityError>;
}
