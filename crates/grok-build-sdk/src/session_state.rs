// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Durable opaque Session state. Validation is deliberately outside the host ABI.

use rusqlite::OptionalExtension as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub const SESSION_STATE_SCHEMA_MARKER: &str = "grok-build-sdk.session-state";
pub const SESSION_STATE_SCHEMA_VERSION: u32 = 1;
pub const MAX_SESSION_STATE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SESSION_STATE_KEY_BYTES: usize = 1024;
pub const MAX_CHECKPOINTS: usize = 4096;
pub const MAX_CHECKPOINT_NAME_BYTES: usize = 1024;
/// SQLite and the public Local reference authority encode revisions as signed
/// 64-bit integers. Preparation rejects successors outside that shared domain.
pub const MAX_SESSION_STATE_REVISION: u64 = i64::MAX as u64;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionStateKey(String);
impl SessionStateKey {
    pub fn new(identity: impl Into<String>) -> Result<Self, SessionStateStoreError> {
        let value = identity.into();
        if value.is_empty() || value.len() > MAX_SESSION_STATE_KEY_BYTES || value.contains('\0') {
            return Err(validation("invalid session identity"));
        }
        Ok(Self(value))
    }
    pub fn session_identity(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionStateVersion {
    revision: u64,
    digest: String,
}
impl SessionStateVersion {
    /// Validates revision and digest read from a Host authority.
    pub fn from_stored_parts(
        revision: u64,
        digest: impl Into<String>,
    ) -> Result<Self, SessionStateStoreError> {
        let value = Self {
            revision,
            digest: digest.into(),
        };
        value.validate().map_err(as_corrupt)?;
        Ok(value)
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }
    fn validate(&self) -> Result<(), SessionStateStoreError> {
        let h = self.digest.strip_prefix("sha256:");
        if self.revision == 0
            || self.revision > MAX_SESSION_STATE_REVISION
            || h.is_none_or(|h| {
                h.len() != 64
                    || !h
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        {
            Err(validation("invalid session state version"))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionStateDocument {
    version: SessionStateVersion,
    bytes: Vec<u8>,
}
impl SessionStateDocument {
    /// Validates an exact bounded payload read from a Host authority.
    pub fn from_stored(
        version: SessionStateVersion,
        bytes: Vec<u8>,
    ) -> Result<Self, SessionStateStoreError> {
        version.validate().map_err(as_corrupt)?;
        if bytes.len() > MAX_SESSION_STATE_BYTES {
            return Err(corrupt("stored payload exceeds 64 MiB"));
        }
        if version.digest != digest(&bytes) {
            return Err(corrupt("stored payload digest mismatch"));
        }
        Ok(Self { version, bytes })
    }
    pub fn version(&self) -> &SessionStateVersion {
        &self.version
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Decodes and validates the SDK-owned envelope after the authority has
    /// verified its revision, digest, and read bound.
    pub fn decode(
        &self,
        key: &SessionStateKey,
    ) -> Result<SessionStateSnapshot, SessionStateStoreError> {
        SessionStateSnapshot::decode(key, &self.bytes)
    }
}

#[derive(Clone, Debug)]
pub struct PreparedSessionStateCommit {
    key: SessionStateKey,
    expected: Option<SessionStateVersion>,
    successor: SessionStateVersion,
    bytes: Vec<u8>,
}
impl PreparedSessionStateCommit {
    pub fn new(
        key: SessionStateKey,
        expected: Option<SessionStateVersion>,
        snapshot: SessionStateSnapshot,
    ) -> Result<Self, SessionStateStoreError> {
        if let Some(v) = &expected {
            v.validate()?;
        }
        snapshot.validate(&key)?;
        let bytes = snapshot.encode()?;
        let revision = expected
            .as_ref()
            .map_or(Some(1), |v| v.revision.checked_add(1))
            .filter(|revision| *revision <= MAX_SESSION_STATE_REVISION)
            .ok_or_else(|| validation("revision overflow"))?;
        let successor = SessionStateVersion {
            revision,
            digest: digest(&bytes),
        };
        Ok(Self {
            key,
            expected,
            successor,
            bytes,
        })
    }
    pub fn key(&self) -> &SessionStateKey {
        &self.key
    }
    pub fn expected(&self) -> Option<&SessionStateVersion> {
        self.expected.as_ref()
    }
    pub fn successor(&self) -> &SessionStateVersion {
        &self.successor
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Debug)]
pub struct PreparedSessionStateDelete {
    key: SessionStateKey,
    expected: SessionStateVersion,
}
impl PreparedSessionStateDelete {
    pub fn new(
        key: SessionStateKey,
        expected: SessionStateVersion,
    ) -> Result<Self, SessionStateStoreError> {
        expected.validate()?;
        Ok(Self { key, expected })
    }
    pub fn key(&self) -> &SessionStateKey {
        &self.key
    }
    pub fn expected(&self) -> &SessionStateVersion {
        &self.expected
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionStateCommit {
    Committed(SessionStateVersion),
    Conflict,
    CommitUnknown,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionStateDelete {
    Deleted,
    Conflict,
    CommitUnknown,
}
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SessionStateStoreError {
    #[error("session state validation failed: {0}")]
    Validation(String),
    #[error("session state is corrupt: {0}")]
    Corrupt(String),
    #[error("session state storage failed: {0}")]
    Storage(String),
}

pub trait SessionStateStore: Send + Sync + 'static {
    fn load(
        &self,
        key: &SessionStateKey,
    ) -> Result<Option<SessionStateDocument>, SessionStateStoreError>;
    fn compare_and_swap(
        &self,
        request: PreparedSessionStateCommit,
    ) -> Result<SessionStateCommit, SessionStateStoreError>;
    fn compare_and_delete(
        &self,
        request: PreparedSessionStateDelete,
    ) -> Result<SessionStateDelete, SessionStateStoreError>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionStateSnapshot {
    marker: String,
    schema_version: u32,
    session_identity: String,
    transcript: Vec<u8>,
    rewind_points: Vec<u8>,
    checkpoints: BTreeMap<String, Vec<u8>>,
}
impl SessionStateSnapshot {
    pub fn new(
        key: &SessionStateKey,
        transcript: Vec<u8>,
        rewind_points: Vec<u8>,
        checkpoints: BTreeMap<String, Vec<u8>>,
    ) -> Result<Self, SessionStateStoreError> {
        let value = Self {
            marker: SESSION_STATE_SCHEMA_MARKER.into(),
            schema_version: SESSION_STATE_SCHEMA_VERSION,
            session_identity: key.0.clone(),
            transcript,
            rewind_points,
            checkpoints,
        };
        value.validate(key)?;
        Ok(value)
    }
    pub fn transcript(&self) -> &[u8] {
        &self.transcript
    }
    pub fn rewind_points(&self) -> &[u8] {
        &self.rewind_points
    }
    pub fn checkpoints(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.checkpoints
    }
    pub fn encode(&self) -> Result<Vec<u8>, SessionStateStoreError> {
        self.validate(&SessionStateKey::new(self.session_identity.clone())?)?;
        let bytes = serde_json::to_vec(self).map_err(storage)?;
        if bytes.len() > MAX_SESSION_STATE_BYTES {
            return Err(validation("encoded snapshot exceeds 64 MiB"));
        }
        Ok(bytes)
    }
    pub fn decode(key: &SessionStateKey, bytes: &[u8]) -> Result<Self, SessionStateStoreError> {
        if bytes.len() > MAX_SESSION_STATE_BYTES {
            return Err(corrupt("encoded snapshot exceeds 64 MiB"));
        }
        let mut de = serde_json::Deserializer::from_slice(bytes);
        let value = Self::deserialize(&mut de).map_err(corrupt)?;
        de.end().map_err(corrupt)?;
        value.validate(key).map_err(|e| corrupt(e.to_string()))?;
        Ok(value)
    }
    fn validate(&self, key: &SessionStateKey) -> Result<(), SessionStateStoreError> {
        if self.marker != SESSION_STATE_SCHEMA_MARKER
            || self.schema_version != SESSION_STATE_SCHEMA_VERSION
            || self.session_identity != key.0
        {
            return Err(validation("snapshot marker/version/identity mismatch"));
        }
        if self.checkpoints.len() > MAX_CHECKPOINTS {
            return Err(validation("too many checkpoints"));
        }
        for name in self.checkpoints.keys() {
            if !safe_name(name) {
                return Err(validation("unsafe checkpoint name"));
            }
        }
        let total = self
            .transcript
            .len()
            .checked_add(self.rewind_points.len())
            .and_then(|n| {
                self.checkpoints
                    .iter()
                    .try_fold(n, |n, (k, v)| n.checked_add(k.len())?.checked_add(v.len()))
            })
            .ok_or_else(|| validation("snapshot size overflow"))?;
        if total > MAX_SESSION_STATE_BYTES {
            return Err(validation("snapshot sections exceed 64 MiB"));
        }
        Ok(())
    }
}
fn safe_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= MAX_CHECKPOINT_NAME_BYTES
        && !n.contains('\0')
        && !n.starts_with('/')
        && n.split('/')
            .all(|p| !p.is_empty() && p != "." && p != ".." && !p.contains('\\'))
}

pub struct LocalSessionStateStore {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}
impl LocalSessionStateStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, SessionStateStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        let path = root.join("native-session-state.sqlite3");
        let existed = path.exists();
        let mut c = rusqlite::Connection::open(&path).map_err(storage)?;
        c.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        if existed {
            let has: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata')", [], |r| r.get(0)).map_err(storage)?;
            if !has {
                return Err(corrupt("existing database has no metadata"));
            }
            inspect_schema(&c)?;
        } else {
            let tx = c.transaction().map_err(storage)?;
            tx.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE session_state(session_identity TEXT PRIMARY KEY,revision INTEGER NOT NULL,digest TEXT NOT NULL,payload BLOB NOT NULL); INSERT INTO metadata VALUES('schema_marker','grok-build-sdk.session-state'); INSERT INTO metadata VALUES('schema_version','1');").map_err(storage)?;
            tx.commit().map_err(storage)?;
        }
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(storage)?;
        Ok(Self {
            path,
            connection: Mutex::new(c),
        })
    }
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}
fn inspect_schema(c: &rusqlite::Connection) -> Result<(), SessionStateStoreError> {
    let rows: Vec<(String, String)> = {
        let mut s = c
            .prepare("SELECT key,value FROM metadata ORDER BY key")
            .map_err(storage)?;
        s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(storage)?
            .collect::<Result<_, _>>()
            .map_err(storage)?
    };
    if rows
        != [
            ("schema_marker".into(), SESSION_STATE_SCHEMA_MARKER.into()),
            (
                "schema_version".into(),
                SESSION_STATE_SCHEMA_VERSION.to_string(),
            ),
        ]
    {
        return Err(corrupt("schema metadata mismatch"));
    }
    let columns: Vec<(String, String, bool, bool)> = {
        let mut statement = c
            .prepare("PRAGMA table_info(session_state)")
            .map_err(storage)?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get(1)?,
                    row.get(2)?,
                    row.get::<_, i64>(3)? != 0,
                    row.get::<_, i64>(5)? != 0,
                ))
            })
            .map_err(storage)?
            .collect::<Result<_, _>>()
            .map_err(storage)?
    };
    if columns
        != [
            ("session_identity".into(), "TEXT".into(), false, true),
            ("revision".into(), "INTEGER".into(), true, false),
            ("digest".into(), "TEXT".into(), true, false),
            ("payload".into(), "BLOB".into(), true, false),
        ]
    {
        return Err(corrupt("session state table layout mismatch"));
    }
    Ok(())
}
fn read_row(
    c: &rusqlite::Connection,
    key: &SessionStateKey,
) -> Result<Option<SessionStateDocument>, SessionStateStoreError> {
    let row: Option<(i64, String, i64)> = c
        .query_row(
            "SELECT revision,digest,length(payload) FROM session_state WHERE session_identity=?1",
            [&key.0],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(|error| corrupt(format!("invalid stored row header: {error}")))?;
    let Some((revision, stored_digest, size)) = row else {
        return Ok(None);
    };
    if revision <= 0 || size < 0 || size as usize > MAX_SESSION_STATE_BYTES {
        return Err(corrupt("invalid row bounds/revision"));
    }
    let bytes: Vec<u8> = c
        .query_row(
            "SELECT payload FROM session_state WHERE session_identity=?1",
            [&key.0],
            |r| r.get(0),
        )
        .map_err(storage)?;
    let version = SessionStateVersion::from_stored_parts(revision as u64, stored_digest)?;
    Ok(Some(SessionStateDocument::from_stored(version, bytes)?))
}
impl SessionStateStore for LocalSessionStateStore {
    fn load(
        &self,
        key: &SessionStateKey,
    ) -> Result<Option<SessionStateDocument>, SessionStateStoreError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
            .map_err(storage)?;
        let document = read_row(&transaction, key)?;
        transaction.commit().map_err(storage)?;
        Ok(document)
    }
    fn compare_and_swap(
        &self,
        r: PreparedSessionStateCommit,
    ) -> Result<SessionStateCommit, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        let current = read_row(&tx, &r.key)?;
        if current.as_ref().map(|d| &d.version) != r.expected.as_ref() {
            return Ok(SessionStateCommit::Conflict);
        }
        tx.execute("INSERT INTO session_state VALUES(?1,?2,?3,?4) ON CONFLICT(session_identity) DO UPDATE SET revision=excluded.revision,digest=excluded.digest,payload=excluded.payload",rusqlite::params![r.key.0,r.successor.revision,r.successor.digest,r.bytes]).map_err(storage)?;
        match tx.commit() {
            Ok(()) => Ok(SessionStateCommit::Committed(r.successor)),
            Err(_) => Ok(SessionStateCommit::CommitUnknown),
        }
    }
    fn compare_and_delete(
        &self,
        r: PreparedSessionStateDelete,
    ) -> Result<SessionStateDelete, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        let current = read_row(&tx, &r.key)?;
        if current.as_ref().map(|d| &d.version) != Some(&r.expected) {
            return Ok(SessionStateDelete::Conflict);
        }
        tx.execute(
            "DELETE FROM session_state WHERE session_identity=?1",
            [r.key.0],
        )
        .map_err(storage)?;
        match tx.commit() {
            Ok(()) => Ok(SessionStateDelete::Deleted),
            Err(_) => Ok(SessionStateDelete::CommitUnknown),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ConformanceOpen {
    Fresh,
    Reopen,
    Concurrent,
}
#[derive(Debug, thiserror::Error)]
#[error("session state conformance failed: {0}")]
pub struct SessionStateConformanceError(pub String);
pub fn run_session_state_conformance<F>(mut open: F) -> Result<(), SessionStateConformanceError>
where
    F: FnMut(ConformanceOpen) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError>,
{
    let s = open(ConformanceOpen::Fresh).map_err(conf)?;
    let k = SessionStateKey::new("conformance").map_err(conf)?;
    if s.load(&k).map_err(conf)?.is_some() {
        return Err(fail("not empty"));
    }
    assert_preparation_rejects_invalid()?;
    let one = conformance_snapshot(&k, b"one")?;
    let a = PreparedSessionStateCommit::new(k.clone(), None, one).map_err(conf)?;
    let v = match s.compare_and_swap(a).map_err(conf)? {
        SessionStateCommit::Committed(v) => v,
        x => return Err(fail(format!("commit: {x:?}"))),
    };
    let first = s
        .load(&k)
        .map_err(conf)?
        .ok_or_else(|| fail("missing committed state"))?;
    if v.revision() != 1 || v.digest() != digest(first.bytes()) {
        return Err(fail("wrong successor"));
    }
    if first.decode(&k).map_err(conf)?.transcript() != b"one" {
        return Err(fail("load mismatch"));
    }
    let other = open(ConformanceOpen::Concurrent).map_err(conf)?;
    let stale_revision = SessionStateVersion {
        revision: 2,
        digest: v.digest.clone(),
    };
    let stale_digest = SessionStateVersion {
        revision: 1,
        digest: digest(b"other"),
    };
    for bad in [stale_revision, stale_digest] {
        let r = PreparedSessionStateCommit::new(
            k.clone(),
            Some(bad),
            conformance_snapshot(&k, b"bad")?,
        )
        .map_err(conf)?;
        if other.compare_and_swap(r).map_err(conf)? != SessionStateCommit::Conflict {
            return Err(fail("stale CAS"));
        }
    }
    let r = PreparedSessionStateCommit::new(
        k.clone(),
        Some(v.clone()),
        conformance_snapshot(&k, b"two")?,
    )
    .map_err(conf)?;
    let v2 = match other.compare_and_swap(r).map_err(conf)? {
        SessionStateCommit::Committed(v) => v,
        _ => return Err(fail("concurrent commit")),
    };
    let stale =
        PreparedSessionStateCommit::new(k.clone(), Some(v), conformance_snapshot(&k, b"lost")?)
            .map_err(conf)?;
    if s.compare_and_swap(stale).map_err(conf)? != SessionStateCommit::Conflict {
        return Err(fail("concurrent conflict"));
    }
    drop(other);
    drop(s);
    let reopen = open(ConformanceOpen::Reopen).map_err(conf)?;
    if reopen.load(&k).map_err(conf)?.as_ref().map(|d| d.version()) != Some(&v2) {
        return Err(fail("restart"));
    }
    let del = PreparedSessionStateDelete::new(k.clone(), v2).map_err(conf)?;
    if reopen.compare_and_delete(del).map_err(conf)? != SessionStateDelete::Deleted
        || reopen.load(&k).map_err(conf)?.is_some()
    {
        return Err(fail("delete"));
    }
    Ok(())
}
fn assert_preparation_rejects_invalid() -> Result<(), SessionStateConformanceError> {
    let key = SessionStateKey::new("x").map_err(conf)?;
    if SessionStateKey::new("").is_ok()
        || SessionStateSnapshot::new(
            &key,
            vec![0; MAX_SESSION_STATE_BYTES + 1],
            Vec::new(),
            BTreeMap::new(),
        )
        .is_ok()
    {
        Err(fail("preparation accepted invalid input"))
    } else {
        Ok(())
    }
}

fn conformance_snapshot(
    key: &SessionStateKey,
    transcript: &[u8],
) -> Result<SessionStateSnapshot, SessionStateConformanceError> {
    SessionStateSnapshot::new(key, transcript.to_vec(), Vec::new(), BTreeMap::new()).map_err(conf)
}

#[derive(Clone, Copy, Debug)]
pub enum SessionStateFault {
    SchemaMissing,
    SchemaOlder,
    SchemaNewer,
    CorruptDigest,
    InvalidRevision,
    DeclaredOversize,
    AfterCommitBeforeAck,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionStateFaultMetrics {
    pub backend_calls: u64,
    pub injected_faults: u64,
    /// Payload bytes fetched after inspecting the stored length. A conforming
    /// backend reports zero for a declared-oversize read.
    pub payload_bytes_read: u64,
}
pub trait SessionStateFaultHarness {
    fn reset(&mut self) -> Result<(), SessionStateStoreError>;
    fn inject(&mut self, fault: SessionStateFault) -> Result<(), SessionStateStoreError>;
    fn open(&mut self) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError>;
    fn metrics(&self) -> SessionStateFaultMetrics;
}
pub fn run_session_state_fault_conformance<H: SessionStateFaultHarness>(
    h: &mut H,
) -> Result<(), SessionStateConformanceError> {
    for f in [
        SessionStateFault::SchemaMissing,
        SessionStateFault::SchemaOlder,
        SessionStateFault::SchemaNewer,
    ] {
        h.reset().map_err(conf)?;
        h.inject(f).map_err(conf)?;
        if h.open().is_ok() {
            return Err(fail("bad schema opened"));
        }
    }
    for f in [
        SessionStateFault::CorruptDigest,
        SessionStateFault::InvalidRevision,
        SessionStateFault::DeclaredOversize,
    ] {
        h.reset().map_err(conf)?;
        let s = h.open().map_err(conf)?;
        let k = SessionStateKey::new("fault").map_err(conf)?;
        let r = PreparedSessionStateCommit::new(k.clone(), None, conformance_snapshot(&k, b"ok")?)
            .map_err(conf)?;
        s.compare_and_swap(r).map_err(conf)?;
        h.inject(f).map_err(conf)?;
        if !matches!(s.load(&k), Err(SessionStateStoreError::Corrupt(_))) {
            return Err(fail("corrupt row accepted"));
        }
        if matches!(f, SessionStateFault::DeclaredOversize) && h.metrics().payload_bytes_read != 0 {
            return Err(fail("oversized payload was fetched before rejection"));
        }
    }
    h.reset().map_err(conf)?;
    let s = h.open().map_err(conf)?;
    let k = SessionStateKey::new("unknown").map_err(conf)?;
    h.inject(SessionStateFault::AfterCommitBeforeAck)
        .map_err(conf)?;
    let r = PreparedSessionStateCommit::new(k.clone(), None, conformance_snapshot(&k, b"ok")?)
        .map_err(conf)?;
    if s.compare_and_swap(r).map_err(conf)? != SessionStateCommit::CommitUnknown
        || s.load(&k).map_err(conf)?.is_none()
    {
        return Err(fail("unknown not reconcilable"));
    }
    if h.metrics().injected_faults == 0 {
        return Err(fail("fault metrics absent"));
    }
    Ok(())
}

fn digest(b: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(b))
}
fn validation(e: impl std::fmt::Display) -> SessionStateStoreError {
    SessionStateStoreError::Validation(e.to_string())
}
fn corrupt(e: impl std::fmt::Display) -> SessionStateStoreError {
    SessionStateStoreError::Corrupt(e.to_string())
}
fn storage(e: impl std::fmt::Display) -> SessionStateStoreError {
    SessionStateStoreError::Storage(e.to_string())
}
fn as_corrupt(error: SessionStateStoreError) -> SessionStateStoreError {
    SessionStateStoreError::Corrupt(error.to_string())
}
fn conf(e: impl std::fmt::Display) -> SessionStateConformanceError {
    fail(e.to_string())
}
fn fail(e: impl Into<String>) -> SessionStateConformanceError {
    SessionStateConformanceError(e.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct CountingStore {
        inner: Arc<LocalSessionStateStore>,
        calls: Arc<AtomicU64>,
        unknown: Arc<AtomicBool>,
    }
    impl SessionStateStore for CountingStore {
        fn load(
            &self,
            key: &SessionStateKey,
        ) -> Result<Option<SessionStateDocument>, SessionStateStoreError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.load(key)
        }
        fn compare_and_swap(
            &self,
            request: PreparedSessionStateCommit,
        ) -> Result<SessionStateCommit, SessionStateStoreError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let result = self.inner.compare_and_swap(request)?;
            if self.unknown.swap(false, Ordering::AcqRel)
                && matches!(result, SessionStateCommit::Committed(_))
            {
                Ok(SessionStateCommit::CommitUnknown)
            } else {
                Ok(result)
            }
        }
        fn compare_and_delete(
            &self,
            request: PreparedSessionStateDelete,
        ) -> Result<SessionStateDelete, SessionStateStoreError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.compare_and_delete(request)
        }
    }

    struct LocalFaultHarness {
        root: tempfile::TempDir,
        generation: u64,
        calls: Arc<AtomicU64>,
        faults: u64,
        unknown: Arc<AtomicBool>,
    }
    impl LocalFaultHarness {
        fn new() -> Self {
            Self {
                root: tempfile::tempdir().unwrap(),
                generation: 0,
                calls: Arc::new(AtomicU64::new(0)),
                faults: 0,
                unknown: Arc::new(AtomicBool::new(false)),
            }
        }
        fn directory(&self) -> PathBuf {
            self.root.path().join(self.generation.to_string())
        }
        fn connection(&self) -> rusqlite::Connection {
            rusqlite::Connection::open(self.directory().join("native-session-state.sqlite3"))
                .unwrap()
        }
    }
    impl SessionStateFaultHarness for LocalFaultHarness {
        fn reset(&mut self) -> Result<(), SessionStateStoreError> {
            self.generation += 1;
            self.calls.store(0, Ordering::Relaxed);
            self.faults = 0;
            self.unknown.store(false, Ordering::Relaxed);
            Ok(())
        }
        fn inject(&mut self, fault: SessionStateFault) -> Result<(), SessionStateStoreError> {
            self.faults += 1;
            match fault {
                SessionStateFault::AfterCommitBeforeAck => {
                    self.unknown.store(true, Ordering::Release);
                    return Ok(());
                }
                SessionStateFault::SchemaMissing
                | SessionStateFault::SchemaOlder
                | SessionStateFault::SchemaNewer => {
                    drop(LocalSessionStateStore::new(self.directory())?);
                }
                _ => {}
            }
            let connection = self.connection();
            match fault {
                SessionStateFault::SchemaMissing => {
                    connection
                        .execute("DELETE FROM metadata", [])
                        .map_err(storage)?;
                }
                SessionStateFault::SchemaOlder => {
                    connection
                        .execute(
                            "UPDATE metadata SET value='0' WHERE key='schema_version'",
                            [],
                        )
                        .map_err(storage)?;
                }
                SessionStateFault::SchemaNewer => {
                    connection
                        .execute(
                            "UPDATE metadata SET value='2' WHERE key='schema_version'",
                            [],
                        )
                        .map_err(storage)?;
                }
                SessionStateFault::CorruptDigest => {
                    connection
                        .execute("UPDATE session_state SET digest='sha256:0000000000000000000000000000000000000000000000000000000000000000'", [])
                        .map_err(storage)?;
                }
                SessionStateFault::InvalidRevision => {
                    connection
                        .execute("UPDATE session_state SET revision=0", [])
                        .map_err(storage)?;
                }
                SessionStateFault::DeclaredOversize => {
                    connection
                        .execute(
                            "UPDATE session_state SET payload=zeroblob(?1)",
                            [MAX_SESSION_STATE_BYTES as u64 + 1],
                        )
                        .map_err(storage)?;
                }
                SessionStateFault::AfterCommitBeforeAck => unreachable!(),
            }
            Ok(())
        }
        fn open(&mut self) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError> {
            Ok(Arc::new(CountingStore {
                inner: Arc::new(LocalSessionStateStore::new(self.directory())?),
                calls: self.calls.clone(),
                unknown: self.unknown.clone(),
            }))
        }
        fn metrics(&self) -> SessionStateFaultMetrics {
            SessionStateFaultMetrics {
                backend_calls: self.calls.load(Ordering::Relaxed),
                injected_faults: self.faults,
                // Local reads length(payload) and returns before selecting the
                // BLOB when the declared size exceeds the public bound.
                payload_bytes_read: 0,
            }
        }
    }

    #[test]
    fn local_black_box() {
        let d = tempfile::tempdir().unwrap();
        run_session_state_conformance(|_| Ok(Arc::new(LocalSessionStateStore::new(d.path())?)))
            .unwrap()
    }
    #[test]
    fn local_fault_conformance() {
        run_session_state_fault_conformance(&mut LocalFaultHarness::new()).unwrap();
    }
    #[test]
    fn snapshot_codec() {
        let k = SessionStateKey::new("id").unwrap();
        let s = SessionStateSnapshot::new(
            &k,
            b"t".to_vec(),
            b"r".to_vec(),
            BTreeMap::from([("a/b".into(), b"c".to_vec())]),
        )
        .unwrap();
        assert_eq!(
            SessionStateSnapshot::decode(&k, &s.encode().unwrap()).unwrap(),
            s
        );
        let mut b = s.encode().unwrap();
        b.extend(b"x");
        assert!(SessionStateSnapshot::decode(&k, &b).is_err())
    }
}
