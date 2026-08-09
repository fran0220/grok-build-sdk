//! Canonical native-session persistence boundary for embedded hosts.
//!
//! This module intentionally contains no SDK dependency. The SDK adapts its
//! public content-addressed object/manifest ABI to these shell-owned traits.

use std::sync::Arc;

/// Opaque error returned by a Host-owned canonical session authority.
#[doc(hidden)]
#[derive(Clone, Debug, thiserror::Error)]
#[error("canonical session state authority failed: {0}")]
pub struct AuthorityError(pub String);

/// A stored immutable object, encoded by the canonical ABI.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalObject {
    pub id: String,
    pub declared_size: u64,
    pub bytes: Vec<u8>,
}

/// A versioned canonical manifest document.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalManifest {
    pub revision: u64,
    pub digest: String,
    pub bytes: Vec<u8>,
}

/// Current durable state of a session identity.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalSlot {
    Vacant,
    Live(CanonicalManifest),
    Tombstoned {
        generation: String,
        revision: u64,
        prior_digest: String,
    },
}

/// Outcome of staging an immutable object.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonicalObjectPut {
    Stored,
    AlreadyPresent,
    CommitUnknown,
}

/// Outcome of publishing a manifest with compare-and-swap.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalManifestCas {
    Committed(CanonicalManifest),
    Conflict,
    CommitUnknown,
}

/// Outcome of tombstoning a session generation.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalDelete {
    Deleted {
        generation: String,
        revision: u64,
        prior_digest: String,
    },
    Conflict,
    CommitUnknown,
}

/// Exact CAS input. `suffix` contains the immutable objects extending the
/// expected head and is validated again by the Host authority.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct CanonicalManifestCasRequest {
    pub expected: Option<CanonicalManifest>,
    pub successor_bytes: Vec<u8>,
    pub suffix: Vec<CanonicalObject>,
}

/// Bounded object reader used while reconstructing a canonical session.
#[doc(hidden)]
pub trait CanonicalSessionReplay: Send + Sync + 'static {
    fn load_object(&self, id: &str) -> Result<Option<CanonicalObject>, AuthorityError>;
}

/// One session identity and generation opened from the shared authority.
#[doc(hidden)]
pub trait CanonicalSession: Send + Sync + 'static {
    fn session_identity(&self) -> &str;
    fn generation(&self) -> &str;
    fn replay(&self) -> Arc<dyn CanonicalSessionReplay>;
    fn put_object(&self, object: CanonicalObject) -> Result<CanonicalObjectPut, AuthorityError>;
    fn compare_and_swap_manifest(
        &self,
        request: CanonicalManifestCasRequest,
    ) -> Result<CanonicalManifestCas, AuthorityError>;
    fn compare_and_delete(
        &self,
        expected: CanonicalManifest,
    ) -> Result<CanonicalDelete, AuthorityError>;
}

/// Runtime-wide canonical authority. One instance is shared by every native
/// session; callers key all durable state by identity and generation.
#[doc(hidden)]
pub trait CanonicalSessionStateAuthority: Send + Sync + 'static {
    fn inspect_slot(&self, session_identity: &str) -> Result<CanonicalSlot, AuthorityError>;
    fn open_session(
        &self,
        session_identity: &str,
        generation: &str,
    ) -> Result<Arc<dyn CanonicalSession>, AuthorityError>;
}
