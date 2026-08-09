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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RewindOperation {
    AppendPoint { index: u64, payload: Vec<u8> },
    Truncate { index: u64, payload: Vec<u8> },
    Merge { index: u64, payload: Vec<u8> },
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayRecord {
    Update(Vec<u8>),
    Checkpoint {
        name: String,
        payload: Vec<u8>,
        marker: Vec<u8>,
    },
    Rewind {
        operation: RewindOperation,
        marker: Vec<u8>,
    },
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
}

#[doc(hidden)]
pub trait NativeSessionStateAuthority: Send + Sync + 'static {
    fn inspect(&self, identity: &str) -> Result<SessionInspection, AuthorityError>;
    fn create(&self, id: SessionIdentity) -> Result<Arc<dyn NativeSession>, AuthorityError>;
    fn open(&self, id: SessionIdentity) -> Result<Arc<dyn NativeSession>, AuthorityError>;
    fn tombstone(&self, id: SessionIdentity) -> Result<(), AuthorityError>;
}
