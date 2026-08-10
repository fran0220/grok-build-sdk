// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use rusqlite::OptionalExtension as _;
use sha2::{Digest as _, Sha256};
use std::{path::PathBuf, sync::Mutex};

pub const SESSION_EVIDENCE_SCHEMA_MARKER: &str = "grok-build-sdk.session-evidence";
pub const SESSION_EVIDENCE_SCHEMA_VERSION: u32 = 1;
pub const MAX_SESSION_EVIDENCE_BYTES: usize = 8 * 1024 * 1024;

/// Namespace for SDK-validated session-origin evidence. The store treats the
/// payload as opaque bytes; reducer, schema and identity validation stay in the SDK.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SessionEvidenceKind {
    Ledger,
    Rewind,
    TurnBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionEvidenceKey {
    pub kind: SessionEvidenceKind,
    /// SDK-defined stable identity (Session ID, rewind operation ID, or
    /// Session ID + Turn ID). Hosts must not reinterpret this value.
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionEvidenceVersion {
    pub revision: u64,
    pub digest: String,
}

impl SessionEvidenceVersion {
    /// Computes the only valid CAS successor identity. The digest is lowercase
    /// SHA-256 over the exact opaque payload bytes, prefixed with `sha256:`.
    pub fn successor(
        current: Option<&SessionEvidenceVersion>,
        bytes: &[u8],
    ) -> Result<Self, SessionEvidenceStoreError> {
        if bytes.len() > MAX_SESSION_EVIDENCE_BYTES {
            return Err(SessionEvidenceStoreError::Storage(
                "session evidence exceeds 8 MiB".into(),
            ));
        }
        let revision = current.map_or(Ok(1), |version| {
            version
                .revision
                .checked_add(1)
                .ok_or_else(|| SessionEvidenceStoreError::Storage("revision overflow".into()))
        })?;
        Ok(Self {
            revision,
            digest: format!("sha256:{:x}", Sha256::digest(bytes)),
        })
    }

    pub fn validates(&self, bytes: &[u8]) -> bool {
        self.revision > 0
            && self.digest == format!("sha256:{:x}", Sha256::digest(bytes))
            && bytes.len() <= MAX_SESSION_EVIDENCE_BYTES
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionEvidenceDocument {
    pub version: SessionEvidenceVersion,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionEvidenceCommit {
    Committed(SessionEvidenceVersion),
    Conflict,
    /// The authority may or may not have committed. Callers must reconcile by
    /// loading; they must never dispatch/retry an effect based on this result.
    CommitUnknown,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionEvidenceStoreError {
    #[error("session evidence storage failed: {0}")]
    Storage(String),
}

/// Single authoritative persistence boundary for SDK-origin session evidence.
/// Implementations own transactions, migrations, encryption, backup and lifecycle.
/// They must persist and fail-closed verify `SESSION_EVIDENCE_SCHEMA_MARKER` and
/// `SESSION_EVIDENCE_SCHEMA_VERSION`. `compare_and_swap` must atomically compare
/// both revision and digest (or absence), and return the exact successor from
/// [`SessionEvidenceVersion::successor`].
pub trait SessionEvidenceStore: Send + Sync + 'static {
    fn load(
        &self,
        key: &SessionEvidenceKey,
    ) -> Result<Option<SessionEvidenceDocument>, SessionEvidenceStoreError>;
    fn compare_and_swap(
        &self,
        key: &SessionEvidenceKey,
        expected: Option<&SessionEvidenceVersion>,
        bytes: &[u8],
    ) -> Result<SessionEvidenceCommit, SessionEvidenceStoreError>;
}

/// Local filesystem reference/default authority. Production hosts should inject
/// their own store; injection replaces this implementation and is never mirrored.
pub struct LocalSessionEvidenceStore {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}

impl LocalSessionEvidenceStore {
    pub fn new(session_storage: impl Into<PathBuf>) -> Result<Self, SessionEvidenceStoreError> {
        let root = session_storage.into();
        std::fs::create_dir_all(&root).map_err(storage_error)?;
        let path = root.join("origin-session-evidence.sqlite3");
        let existed = path.exists();
        let connection = rusqlite::Connection::open(&path).map_err(storage_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage_error)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                 CREATE TABLE IF NOT EXISTS evidence(
                   kind INTEGER NOT NULL,
                   identity TEXT NOT NULL,
                   revision INTEGER NOT NULL,
                   digest TEXT NOT NULL,
                   payload BLOB NOT NULL,
                   PRIMARY KEY(kind,identity)
                 );",
            )
            .map_err(storage_error)?;
        let metadata_count: u64 = connection
            .query_row("SELECT COUNT(*) FROM metadata", [], |row| row.get(0))
            .map_err(storage_error)?;
        if metadata_count == 0 {
            if existed {
                return Err(SessionEvidenceStoreError::Storage(
                    "existing session evidence store has no schema metadata".into(),
                ));
            }
            let transaction = connection.unchecked_transaction().map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT INTO metadata(key,value) VALUES('schema_marker',?1)",
                    [SESSION_EVIDENCE_SCHEMA_MARKER],
                )
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT INTO metadata(key,value) VALUES('schema_version',?1)",
                    [SESSION_EVIDENCE_SCHEMA_VERSION.to_string()],
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
        } else {
            let marker: Option<String> = connection
                .query_row(
                    "SELECT value FROM metadata WHERE key='schema_marker'",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(storage_error)?;
            let version: Option<String> = connection
                .query_row(
                    "SELECT value FROM metadata WHERE key='schema_version'",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(storage_error)?;
            if marker.as_deref() != Some(SESSION_EVIDENCE_SCHEMA_MARKER)
                || version.as_deref() != Some("1")
                || metadata_count != 2
            {
                return Err(SessionEvidenceStoreError::Storage(
                    "session evidence schema marker/version mismatch".into(),
                ));
            }
        }
        Ok(Self {
            path,
            connection: Mutex::new(connection),
        })
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl serde::Serialize for SessionEvidenceVersion {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        (&self.revision, &self.digest).serialize(s)
    }
}
impl<'de> serde::Deserialize<'de> for SessionEvidenceVersion {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (revision, digest) = <(u64, String)>::deserialize(d)?;
        Ok(Self { revision, digest })
    }
}

impl SessionEvidenceStore for LocalSessionEvidenceStore {
    fn load(
        &self,
        key: &SessionEvidenceKey,
    ) -> Result<Option<SessionEvidenceDocument>, SessionEvidenceStoreError> {
        let mut connection = self.connection.lock().map_err(storage_error)?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
            .map_err(storage_error)?;
        let result = transaction.query_row(
            "SELECT revision,digest,length(payload) FROM evidence
             WHERE kind=?1 AND identity=?2",
            rusqlite::params![kind_id(key.kind), key.identity],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                ))
            },
        );
        match result {
            Ok((revision, digest, size)) => {
                if size > MAX_SESSION_EVIDENCE_BYTES as u64 {
                    return Err(SessionEvidenceStoreError::Storage(
                        "session evidence exceeds 8 MiB".into(),
                    ));
                }
                let bytes: Vec<u8> = transaction
                    .query_row(
                        "SELECT payload FROM evidence WHERE kind=?1 AND identity=?2",
                        rusqlite::params![kind_id(key.kind), key.identity],
                        |row| row.get(0),
                    )
                    .map_err(storage_error)?;
                let version = SessionEvidenceVersion { revision, digest };
                if !version.validates(&bytes) {
                    return Err(SessionEvidenceStoreError::Storage(
                        "local session evidence identity is invalid".into(),
                    ));
                }
                transaction.commit().map_err(storage_error)?;
                Ok(Some(SessionEvidenceDocument { version, bytes }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                transaction.commit().map_err(storage_error)?;
                Ok(None)
            }
            Err(error) => Err(storage_error(error)),
        }
    }
    fn compare_and_swap(
        &self,
        key: &SessionEvidenceKey,
        expected: Option<&SessionEvidenceVersion>,
        bytes: &[u8],
    ) -> Result<SessionEvidenceCommit, SessionEvidenceStoreError> {
        let mut connection = self.connection.lock().map_err(storage_error)?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let current_header = transaction
            .query_row(
                "SELECT revision,digest,length(payload) FROM evidence
                 WHERE kind=?1 AND identity=?2",
                rusqlite::params![kind_id(key.kind), key.identity],
                |row| {
                    Ok((
                        SessionEvidenceVersion {
                            revision: row.get(0)?,
                            digest: row.get(1)?,
                        },
                        row.get::<_, u64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?;
        let current = if let Some((version, size)) = current_header {
            if size > MAX_SESSION_EVIDENCE_BYTES as u64 {
                return Err(SessionEvidenceStoreError::Storage(
                    "session evidence exceeds 8 MiB".into(),
                ));
            }
            let payload: Vec<u8> = transaction
                .query_row(
                    "SELECT payload FROM evidence WHERE kind=?1 AND identity=?2",
                    rusqlite::params![kind_id(key.kind), key.identity],
                    |row| row.get(0),
                )
                .map_err(storage_error)?;
            if !version.validates(&payload) {
                return Err(SessionEvidenceStoreError::Storage(
                    "local session evidence identity is invalid".into(),
                ));
            }
            Some(version)
        } else {
            None
        };
        if current.as_ref() != expected {
            return Ok(SessionEvidenceCommit::Conflict);
        }
        let version = SessionEvidenceVersion::successor(current.as_ref(), bytes)?;
        transaction
            .execute(
                "INSERT INTO evidence(kind,identity,revision,digest,payload)
                 VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(kind,identity) DO UPDATE SET
                   revision=excluded.revision,digest=excluded.digest,payload=excluded.payload",
                rusqlite::params![
                    kind_id(key.kind),
                    key.identity,
                    version.revision,
                    version.digest,
                    bytes
                ],
            )
            .map_err(storage_error)?;
        match transaction.commit() {
            Ok(()) => Ok(SessionEvidenceCommit::Committed(version)),
            Err(_) => Ok(SessionEvidenceCommit::CommitUnknown),
        }
    }
}

fn kind_id(kind: SessionEvidenceKind) -> u8 {
    match kind {
        SessionEvidenceKind::Ledger => 1,
        SessionEvidenceKind::Rewind => 2,
        SessionEvidenceKind::TurnBinding => 3,
    }
}

fn storage_error(error: impl std::fmt::Display) -> SessionEvidenceStoreError {
    SessionEvidenceStoreError::Storage(error.to_string())
}

fn evidence_suite_error(message: impl Into<String>) -> SessionEvidenceStoreError {
    SessionEvidenceStoreError::Storage(format!("conformance: {}", message.into()))
}

/// Runs the backend-neutral session-evidence contract against independent
/// handles to one authority.
///
/// The opener answers with a handle for each [`ConformanceOpen`] phase against
/// the same authority: `Fresh` and `Concurrent` must observe each other's
/// commits, and `Reopen` must be a handle taken after the earlier ones were
/// dropped. A host backend that passes this suite has the same validation
/// ordering, CAS identity, bounds and restart semantics as
/// [`LocalSessionEvidenceStore`].
pub fn run_session_evidence_conformance<F>(mut open: F) -> Result<(), SessionEvidenceStoreError>
where
    F: FnMut(
        crate::session_state::ConformanceOpen,
    ) -> Result<std::sync::Arc<dyn SessionEvidenceStore>, SessionEvidenceStoreError>,
{
    use crate::session_state::ConformanceOpen;

    let store = open(ConformanceOpen::Fresh)?;
    let key = SessionEvidenceKey {
        kind: SessionEvidenceKind::Ledger,
        identity: "session-evidence-conformance".into(),
    };

    if store.load(&key)?.is_some() {
        return Err(evidence_suite_error("fresh authority is not empty"));
    }

    // Creating from absence returns exactly the successor identity the SDK
    // computes, so a host cannot invent its own revision or digest.
    let expected_first = SessionEvidenceVersion::successor(None, b"first")?;
    match store.compare_and_swap(&key, None, b"first")? {
        SessionEvidenceCommit::Committed(version) if version == expected_first => {}
        other => {
            return Err(evidence_suite_error(format!(
                "create-from-absent returned {other:?} rather than the exact successor"
            )));
        }
    }
    match store.load(&key)? {
        Some(document) if document.version == expected_first && document.bytes == b"first" => {}
        _ => return Err(evidence_suite_error("committed document did not load back")),
    }

    // Every stale expectation conflicts rather than overwriting.
    if store.compare_and_swap(&key, None, b"duplicate")? != SessionEvidenceCommit::Conflict {
        return Err(evidence_suite_error(
            "create-from-absent over an existing document was accepted",
        ));
    }
    let stale_digest = SessionEvidenceVersion {
        revision: expected_first.revision,
        digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
    };
    if store.compare_and_swap(&key, Some(&stale_digest), b"conflict")?
        != SessionEvidenceCommit::Conflict
    {
        return Err(evidence_suite_error("a stale digest was accepted"));
    }
    let stale_revision = SessionEvidenceVersion {
        revision: expected_first.revision + 1,
        digest: expected_first.digest.clone(),
    };
    if store.compare_and_swap(&key, Some(&stale_revision), b"conflict")?
        != SessionEvidenceCommit::Conflict
    {
        return Err(evidence_suite_error("a stale revision was accepted"));
    }
    match store.load(&key)? {
        Some(document) if document.bytes == b"first" => {}
        _ => {
            return Err(evidence_suite_error(
                "a conflicting swap changed the stored document",
            ));
        }
    }

    // Two independent handles racing the same expectation: exactly one wins.
    let concurrent = open(ConformanceOpen::Concurrent)?;
    match concurrent.load(&key)? {
        Some(document) if document.version == expected_first => {}
        _ => {
            return Err(evidence_suite_error(
                "a concurrent handle did not observe the committed document",
            ));
        }
    }
    let first_outcome = store.compare_and_swap(&key, Some(&expected_first), b"winner")?;
    let second_outcome = concurrent.compare_and_swap(&key, Some(&expected_first), b"loser")?;
    let committed = match (&first_outcome, &second_outcome) {
        (SessionEvidenceCommit::Committed(version), SessionEvidenceCommit::Conflict) => {
            version.clone()
        }
        (SessionEvidenceCommit::Conflict, SessionEvidenceCommit::Committed(version)) => {
            version.clone()
        }
        _ => {
            return Err(evidence_suite_error(format!(
                "a contended CAS produced {first_outcome:?} and {second_outcome:?} rather than one \
                 winner and one conflict"
            )));
        }
    };
    if committed.revision != expected_first.revision + 1 {
        return Err(evidence_suite_error(
            "the winning revision did not advance by exactly one",
        ));
    }

    // Bounds are the SDK's, not the backend's: an oversize payload is refused
    // and leaves the current document intact.
    let oversize = vec![0_u8; MAX_SESSION_EVIDENCE_BYTES + 1];
    if store
        .compare_and_swap(&key, Some(&committed), &oversize)
        .is_ok()
    {
        return Err(evidence_suite_error(
            "a payload over the 8 MiB bound was accepted",
        ));
    }
    let before_restart = store.load(&key)?.ok_or_else(|| {
        evidence_suite_error("the document vanished after a refused oversize CAS")
    })?;
    if before_restart.version != committed {
        return Err(evidence_suite_error(
            "a refused oversize CAS changed the stored version",
        ));
    }

    // Kinds and identities are separate documents, never one shared slot.
    let other_kind = SessionEvidenceKey {
        kind: SessionEvidenceKind::Rewind,
        identity: key.identity.clone(),
    };
    let other_identity = SessionEvidenceKey {
        kind: key.kind,
        identity: "session-evidence-conformance-other".into(),
    };
    for neighbour in [&other_kind, &other_identity] {
        if store.load(neighbour)?.is_some() {
            return Err(evidence_suite_error(
                "an unrelated evidence key shared another key's document",
            ));
        }
        if !matches!(
            store.compare_and_swap(neighbour, None, b"neighbour")?,
            SessionEvidenceCommit::Committed(_)
        ) {
            return Err(evidence_suite_error(
                "an unrelated evidence key could not be created",
            ));
        }
    }

    drop(concurrent);
    drop(store);

    // Restart: the exact version and bytes survive, and a pre-restart
    // expectation still commits.
    let reopened = open(ConformanceOpen::Reopen)?;
    match reopened.load(&key)? {
        Some(document) if document == before_restart => {}
        _ => {
            return Err(evidence_suite_error(
                "the document did not survive a restart unchanged",
            ));
        }
    }
    if !matches!(
        reopened.compare_and_swap(&key, Some(&committed), b"after-restart")?,
        SessionEvidenceCommit::Committed(_)
    ) {
        return Err(evidence_suite_error(
            "a pre-restart expectation was refused after restart",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn local_store_passes_the_public_session_evidence_conformance() {
        let root = tempfile::tempdir().unwrap();
        run_session_evidence_conformance(|_| {
            Ok(Arc::new(LocalSessionEvidenceStore::new(root.path())?)
                as Arc<dyn SessionEvidenceStore>)
        })
        .unwrap();
    }

    #[test]
    fn local_store_enforces_revision_and_digest_cas_across_reopen() {
        let root = tempfile::tempdir().unwrap();
        let key = SessionEvidenceKey {
            kind: SessionEvidenceKind::Ledger,
            identity: "session-1".into(),
        };
        let store = LocalSessionEvidenceStore::new(root.path()).unwrap();
        let SessionEvidenceCommit::Committed(first) =
            store.compare_and_swap(&key, None, b"one").unwrap()
        else {
            panic!("initial commit was not acknowledged")
        };
        assert_eq!(
            store.compare_and_swap(&key, None, b"duplicate").unwrap(),
            SessionEvidenceCommit::Conflict
        );
        let stale = SessionEvidenceVersion {
            revision: first.revision,
            digest: "sha256:wrong".into(),
        };
        assert_eq!(
            store
                .compare_and_swap(&key, Some(&stale), b"conflict")
                .unwrap(),
            SessionEvidenceCommit::Conflict
        );
        drop(store);
        let reopened = LocalSessionEvidenceStore::new(root.path()).unwrap();
        let loaded = reopened.load(&key).unwrap().unwrap();
        assert_eq!(loaded.bytes, b"one");
        assert_eq!(loaded.version, first);
    }

    #[test]
    fn local_store_serializes_cas_across_independent_connections() {
        let root = tempfile::tempdir().unwrap();
        let key = SessionEvidenceKey {
            kind: SessionEvidenceKind::Ledger,
            identity: "shared-session".into(),
        };
        let first = LocalSessionEvidenceStore::new(root.path()).unwrap();
        let second = LocalSessionEvidenceStore::new(root.path()).unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let spawn = |store: LocalSessionEvidenceStore, bytes: &'static [u8]| {
            let key = key.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.compare_and_swap(&key, None, bytes).unwrap()
            })
        };
        let a = spawn(first, b"first");
        let b = spawn(second, b"second");
        barrier.wait();
        let results = [a.join().unwrap(), b.join().unwrap()];
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, SessionEvidenceCommit::Committed(_)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == SessionEvidenceCommit::Conflict)
                .count(),
            1
        );
    }

    #[test]
    fn local_store_rejects_incomplete_schema_metadata() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalSessionEvidenceStore::new(root.path()).unwrap();
        let path = store.path().to_owned();
        drop(store);
        let connection = rusqlite::Connection::open(path).unwrap();
        connection
            .execute("DELETE FROM metadata WHERE key='schema_marker'", [])
            .unwrap();
        drop(connection);
        assert!(LocalSessionEvidenceStore::new(root.path()).is_err());
    }

    #[test]
    fn local_store_cas_rejects_corrupted_current_payload() {
        let root = tempfile::tempdir().unwrap();
        let key = SessionEvidenceKey {
            kind: SessionEvidenceKind::Ledger,
            identity: "corrupted-session".into(),
        };
        let store = LocalSessionEvidenceStore::new(root.path()).unwrap();
        let SessionEvidenceCommit::Committed(first) =
            store.compare_and_swap(&key, None, b"original").unwrap()
        else {
            panic!("initial commit was not acknowledged")
        };
        let connection = rusqlite::Connection::open(store.path()).unwrap();
        connection
            .execute(
                "UPDATE evidence SET payload=?1 WHERE kind=?2 AND identity=?3",
                rusqlite::params![b"corrupted", kind_id(key.kind), key.identity],
            )
            .unwrap();
        drop(connection);

        assert!(matches!(
            store.compare_and_swap(&key, Some(&first), b"replacement"),
            Err(SessionEvidenceStoreError::Storage(_))
        ));
        let connection = rusqlite::Connection::open(store.path()).unwrap();
        let payload: Vec<u8> = connection
            .query_row(
                "SELECT payload FROM evidence WHERE kind=?1 AND identity=?2",
                rusqlite::params![kind_id(key.kind), key.identity],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(payload, b"corrupted");
    }
}
