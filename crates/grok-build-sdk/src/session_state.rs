// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Current-only, content-addressed Session log storage.
//!
//! There is deliberately no GC operation in this release. Implementations may eventually
//! collect objects unreachable from every live manifest, but only under an operator-defined
//! backup, replication, and retention policy.

use rusqlite::{OptionalExtension as _, TransactionBehavior};
use sha2::{Digest as _, Sha256};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub const SESSION_LOG_SCHEMA_MARKER: &str = "grok-build-sdk.session-log";
pub const SESSION_LOG_SCHEMA_VERSION: u32 = 1;
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
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub struct $name(String);
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionObjectId(String);
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RewindKind {
    AppendPoint,
    Truncate,
    Merge,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
    RewindPublication {
        previous: Option<SessionObjectId>,
        sequence: u64,
        marker_bytes: Vec<u8>,
        operation: SessionObjectId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionObject {
    id: SessionObjectId,
    session: SessionKey,
    generation: SessionGeneration,
    kind: SessionObjectKind,
    bytes: Vec<u8>,
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
        chain_parts(&self.kind).map(|x| x.0).flatten()
    }
    pub fn sequence(&self) -> Option<u64> {
        chain_parts(&self.kind).map(|x| x.1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionManifest {
    session: SessionKey,
    generation: SessionGeneration,
    head: Option<SessionObjectId>,
    segment_count: u64,
    transcript_bytes: u64,
    bytes: Vec<u8>,
}
impl SessionManifest {
    pub fn new(
        session: SessionKey,
        generation: SessionGeneration,
        head: Option<SessionObjectId>,
        segment_count: u64,
        transcript_bytes: u64,
    ) -> Result<Self, SessionStateStoreError> {
        if head.is_none() && (segment_count != 0 || transcript_bytes != 0) {
            return Err(validation("empty head must have zero counters"));
        }
        let bytes = encode_manifest(
            &session,
            &generation,
            head.as_ref(),
            segment_count,
            transcript_bytes,
        )?;
        Ok(Self {
            session,
            generation,
            head,
            segment_count,
            transcript_bytes,
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
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> String {
        digest(&self.bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestVersion {
    revision: u64,
    digest: String,
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
    version: ManifestVersion,
    manifest: SessionManifest,
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
    generation: SessionGeneration,
    revision: u64,
    prior_digest: String,
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
pub enum SessionSlot {
    Vacant,
    Live(LiveSessionDocument),
    Tombstoned { receipt: DeleteReceipt },
}

#[derive(Clone, Debug)]
pub struct PreparedManifestCas {
    key: SessionKey,
    expected: Option<LiveSessionDocument>,
    successor: LiveSessionDocument,
    suffix: Vec<SessionObjectId>,
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
        if let Some(e) = &expected {
            if e.manifest.session != key || e.manifest.generation != manifest.generation {
                return Err(validation("manifest identity or generation changed"));
            }
        }
        let revision = expected
            .as_ref()
            .map_or(Some(1), |e| e.version.revision.checked_add(1))
            .filter(|x| *x <= i64::MAX as u64)
            .ok_or_else(|| validation("manifest revision overflow"))?;
        validate_suffix(&manifest, expected.as_ref().map(|x| &x.manifest), suffix)?;
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
#[derive(Clone, Debug)]
pub struct PreparedSessionDelete {
    key: SessionKey,
    expected: LiveSessionDocument,
    receipt: DeleteReceipt,
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

pub struct LocalSessionStateStore {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}
impl LocalSessionStateStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, SessionStateStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        let path = root.join("native-session-log.sqlite3");
        let legacy_path = root.join("native-session-state.sqlite3");
        let existed = path.exists();
        if !existed && legacy_path.exists() {
            return Err(corrupt(
                "legacy whole-snapshot session state requires explicit migration or discard",
            ));
        }
        let mut c = rusqlite::Connection::open(&path).map_err(storage)?;
        c.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        if existed {
            inspect_schema(&c)?
        } else {
            let tx = c.transaction().map_err(storage)?;
            tx.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY NOT NULL,value TEXT NOT NULL);CREATE TABLE objects(session_identity TEXT NOT NULL,generation TEXT NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(session_identity,generation,id));CREATE TABLE manifests(session_identity TEXT PRIMARY KEY NOT NULL,generation TEXT NOT NULL,revision INTEGER NOT NULL,digest TEXT NOT NULL,size INTEGER NOT NULL,payload BLOB NOT NULL);CREATE TABLE tombstones(session_identity TEXT PRIMARY KEY NOT NULL,generation TEXT NOT NULL,revision INTEGER NOT NULL,prior_digest TEXT NOT NULL);INSERT INTO metadata VALUES('schema_marker','grok-build-sdk.session-log');INSERT INTO metadata VALUES('schema_version','1');").map_err(storage)?;
            tx.commit().map_err(storage)?
        }
        c.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;")
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

impl SessionStateStore for LocalSessionStateStore {
    fn inspect_slot(&self, key: &SessionKey) -> Result<SessionSlot, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(storage)?;
        let out = inspect_tx(&tx, key)?;
        tx.commit().map_err(storage)?;
        Ok(out)
    }
    fn load_object(
        &self,
        key: &SessionKey,
        generation: &SessionGeneration,
        id: &SessionObjectId,
    ) -> Result<Option<SessionObject>, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(storage)?;
        let out = load_tx(&tx, key, generation, id)?;
        tx.commit().map_err(storage)?;
        Ok(out)
    }
    fn put_object(&self, o: SessionObject) -> Result<ObjectPut, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c.transaction().map_err(storage)?;
        let n = tx
            .execute(
                "INSERT OR IGNORE INTO objects VALUES(?1,?2,?3,?4,?5)",
                rusqlite::params![
                    o.session.0,
                    o.generation.0,
                    o.id.0,
                    o.bytes.len() as u64,
                    o.bytes
                ],
            )
            .map_err(storage)?;
        if n == 0 && load_tx(&tx, &o.session, &o.generation, &o.id)?.as_ref() != Some(&o) {
            return Err(corrupt("content ID collision"));
        }
        match tx.commit() {
            Ok(()) => Ok(if n == 0 {
                ObjectPut::AlreadyPresent
            } else {
                ObjectPut::Stored
            }),
            Err(_) => Ok(ObjectPut::CommitUnknown),
        }
    }
    fn compare_and_swap_manifest(
        &self,
        r: PreparedManifestCas,
    ) -> Result<ManifestCas, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        if matches!(
            inspect_header(&tx, &r.key)?,
            Header::Tombstone | Header::Live(_)
        ) != r.expected.is_some()
            || !current_matches(&tx, &r)?
        {
            return Ok(ManifestCas::Conflict);
        }
        let objects = r
            .suffix
            .iter()
            .map(|id| {
                load_tx(&tx, &r.key, &r.successor.manifest.generation, id)?
                    .ok_or_else(|| corrupt("staged suffix object missing"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_suffix(
            &r.successor.manifest,
            r.expected.as_ref().map(|x| &x.manifest),
            &objects,
        )?;
        validate_references(&tx, &objects)?;
        let m = &r.successor.manifest;
        tx.execute("INSERT INTO manifests VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(session_identity) DO UPDATE SET generation=excluded.generation,revision=excluded.revision,digest=excluded.digest,size=excluded.size,payload=excluded.payload",rusqlite::params![r.key.0,m.generation.0,r.successor.version.revision,r.successor.version.digest,m.bytes.len() as u64,m.bytes]).map_err(storage)?;
        match tx.commit() {
            Ok(()) => Ok(ManifestCas::Committed(r.successor)),
            Err(_) => Ok(ManifestCas::CommitUnknown),
        }
    }
    fn compare_and_delete(
        &self,
        r: PreparedSessionDelete,
    ) -> Result<SessionDelete, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let current = load_live_document(&tx, &r.key, false)?;
        if current.as_ref() != Some(&r.expected) {
            return Ok(SessionDelete::Conflict);
        }
        let receipt = r.receipt;
        tx.execute(
            "DELETE FROM manifests WHERE session_identity=?1",
            [&r.key.0],
        )
        .map_err(storage)?;
        tx.execute(
            "INSERT INTO tombstones VALUES(?1,?2,?3,?4)",
            rusqlite::params![
                r.key.0,
                receipt.generation.0,
                receipt.revision,
                receipt.prior_digest
            ],
        )
        .map_err(storage)?;
        match tx.commit() {
            Ok(()) => Ok(SessionDelete::Deleted(receipt)),
            Err(_) => Ok(SessionDelete::CommitUnknown),
        }
    }
}

enum Header {
    Vacant,
    Tombstone,
    Live(()),
}
fn inspect_header(
    c: &rusqlite::Connection,
    key: &SessionKey,
) -> Result<Header, SessionStateStoreError> {
    if c.query_row(
        "SELECT 1 FROM tombstones WHERE session_identity=?1",
        [&key.0],
        |r| r.get::<_, i64>(0),
    )
    .optional()
    .map_err(storage)?
    .is_some()
    {
        return Ok(Header::Tombstone);
    }
    Ok(
        if c.query_row(
            "SELECT 1 FROM manifests WHERE session_identity=?1",
            [&key.0],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .map_err(storage)?
        .is_some()
        {
            Header::Live(())
        } else {
            Header::Vacant
        },
    )
}
fn inspect_tx(
    c: &rusqlite::Connection,
    key: &SessionKey,
) -> Result<SessionSlot, SessionStateStoreError> {
    if let Some((g, r, d)) = c
        .query_row(
            "SELECT generation,revision,prior_digest FROM tombstones WHERE session_identity=?1",
            [&key.0],
            |x| {
                Ok((
                    x.get::<_, String>(0)?,
                    x.get::<_, i64>(1)?,
                    x.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(storage)?
    {
        return Ok(SessionSlot::Tombstoned {
            receipt: DeleteReceipt::from_stored(
                SessionGeneration::new(g).map_err(as_corrupt)?,
                u64::try_from(r).map_err(corrupt)?,
                d,
            )?,
        });
    }
    Ok(match load_live_document(c, key, true)? {
        None => SessionSlot::Vacant,
        Some(x) => SessionSlot::Live(x),
    })
}
fn load_live_document(
    c: &rusqlite::Connection,
    key: &SessionKey,
    traverse: bool,
) -> Result<Option<LiveSessionDocument>, SessionStateStoreError> {
    let row: Option<(String, i64, String, i64, i64)> = c
        .query_row(
            "SELECT generation,revision,digest,size,length(payload) FROM manifests WHERE session_identity=?1",
            [&key.0],
            |x| {
                Ok((
                    x.get(0)?,
                    x.get(1)?,
                    x.get(2)?,
                    x.get(3)?,
                    x.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(storage)?;
    let Some((g, r, d, size, payload_size)) = row else {
        return Ok(None);
    };
    let bytes = bounded_blob(
        c,
        "manifests",
        "session_identity",
        &key.0,
        size,
        payload_size,
        MAX_SESSION_MANIFEST_BYTES,
    )?;
    let m = SessionManifest::from_stored(bytes)?;
    if m.session != *key || m.generation.0 != g {
        return Err(corrupt("stored manifest identity mismatch"));
    }
    let live = LiveSessionDocument::from_stored(
        ManifestVersion::from_stored(u64::try_from(r).map_err(corrupt)?, d)?,
        m,
    )?;
    if traverse {
        traverse_manifest(c, &live.manifest)?
    }
    Ok(Some(live))
}
fn load_tx(
    c: &rusqlite::Connection,
    key: &SessionKey,
    g: &SessionGeneration,
    id: &SessionObjectId,
) -> Result<Option<SessionObject>, SessionStateStoreError> {
    let sizes: Option<(i64, i64)> = c
        .query_row(
            "SELECT size,length(payload) FROM objects WHERE session_identity=?1 AND generation=?2 AND id=?3",
            rusqlite::params![key.0, g.0, id.0],
            |x| Ok((x.get(0)?, x.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    let Some((size, payload_size)) = sizes else {
        return Ok(None);
    };
    let bytes = bounded_object_blob(c, key, g, id, size, payload_size)?;
    let o = SessionObject::from_stored(id.clone(), u64::try_from(size).map_err(corrupt)?, bytes)?;
    if o.session != *key || o.generation != *g {
        return Err(corrupt("stored object scope mismatch"));
    }
    Ok(Some(o))
}
fn bounded_object_blob(
    c: &rusqlite::Connection,
    k: &SessionKey,
    g: &SessionGeneration,
    id: &SessionObjectId,
    size: i64,
    payload_size: i64,
) -> Result<Vec<u8>, SessionStateStoreError> {
    if size < 0 || payload_size != size || size as usize > MAX_SESSION_OBJECT_BYTES {
        return Err(corrupt("invalid object declared size"));
    }
    c.query_row(
        "SELECT payload FROM objects WHERE session_identity=?1 AND generation=?2 AND id=?3",
        rusqlite::params![k.0, g.0, id.0],
        |x| x.get(0),
    )
    .map_err(storage)
}
fn bounded_blob(
    c: &rusqlite::Connection,
    table: &str,
    column: &str,
    value: &str,
    size: i64,
    payload_size: i64,
    max: usize,
) -> Result<Vec<u8>, SessionStateStoreError> {
    if size < 0 || payload_size != size || size as usize > max {
        return Err(corrupt("invalid declared size"));
    }
    c.query_row(
        &format!("SELECT payload FROM {table} WHERE {column}=?1"),
        [value],
        |x| x.get(0),
    )
    .map_err(storage)
}
fn traverse_manifest(
    c: &rusqlite::Connection,
    m: &SessionManifest,
) -> Result<(), SessionStateStoreError> {
    let mut next = m.head.clone();
    let mut expected = m.segment_count;
    let mut bytes = 0u64;
    while let Some(id) = next {
        if expected == 0 {
            return Err(corrupt("chain longer than segment_count"));
        }
        let o = load_tx(c, &m.session, &m.generation, &id)?
            .ok_or_else(|| corrupt("chain object missing"))?;
        if o.sequence() != Some(expected) {
            return Err(corrupt("chain sequence mismatch"));
        }
        validate_references(c, std::slice::from_ref(&o))?;
        bytes = bytes
            .checked_add(chain_bytes(&o.kind))
            .ok_or_else(|| corrupt("transcript counter overflow"))?;
        next = o.previous().cloned();
        expected -= 1
    }
    if expected != 0 || bytes != m.transcript_bytes {
        return Err(corrupt("manifest counters do not match chain"));
    }
    Ok(())
}
fn validate_references(
    c: &rusqlite::Connection,
    objects: &[SessionObject],
) -> Result<(), SessionStateStoreError> {
    for o in objects {
        match &o.kind {
            SessionObjectKind::CheckpointPublication { checkpoint, .. } => {
                match load_tx(c, &o.session, &o.generation, checkpoint)?.map(|x| x.kind) {
                    Some(SessionObjectKind::Checkpoint { .. }) => {}
                    _ => {
                        return Err(corrupt(
                            "checkpoint publication has missing or wrong-kind reference",
                        ));
                    }
                }
            }
            SessionObjectKind::RewindPublication { operation, .. } => {
                match load_tx(c, &o.session, &o.generation, operation)?.map(|x| x.kind) {
                    Some(SessionObjectKind::RewindOperation { .. }) => {}
                    _ => {
                        return Err(corrupt(
                            "rewind publication has missing or wrong-kind reference",
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
fn validate_suffix(
    successor: &SessionManifest,
    expected: Option<&SessionManifest>,
    suffix: &[SessionObject],
) -> Result<(), SessionStateStoreError> {
    let mut prev = expected.and_then(|x| x.head.clone());
    let mut sequence = expected.map_or(0, |x| x.segment_count);
    let mut bytes = expected.map_or(0, |x| x.transcript_bytes);
    for o in suffix {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| validation("segment count overflow"))?;
        if o.session != successor.session
            || o.generation != successor.generation
            || o.previous() != prev.as_ref()
            || o.sequence() != Some(sequence)
        {
            return Err(validation("suffix does not extend expected head exactly"));
        }
        bytes = bytes
            .checked_add(chain_bytes(&o.kind))
            .ok_or_else(|| validation("transcript byte overflow"))?;
        prev = Some(o.id.clone())
    }
    if successor.head != prev
        || successor.segment_count != sequence
        || successor.transcript_bytes != bytes
    {
        return Err(validation("successor counters or head do not match suffix"));
    }
    Ok(())
}
fn current_matches(
    c: &rusqlite::Connection,
    r: &PreparedManifestCas,
) -> Result<bool, SessionStateStoreError> {
    Ok(match &r.expected {
        None => matches!(inspect_header(c, &r.key)?, Header::Vacant),
        Some(e) => load_live_document(c, &r.key, false)?.as_ref() == Some(e),
    })
}

fn inspect_schema(c: &rusqlite::Connection) -> Result<(), SessionStateStoreError> {
    let metadata: Vec<(String, String)> = c
        .prepare("SELECT key,value FROM metadata ORDER BY key")
        .map_err(|_| corrupt("existing database has no current metadata"))?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(storage)?
        .collect::<Result<_, _>>()
        .map_err(storage)?;
    if metadata
        != [
            ("schema_marker".into(), SESSION_LOG_SCHEMA_MARKER.into()),
            ("schema_version".into(), "1".into()),
        ]
    {
        return Err(corrupt("schema marker/version mismatch"));
    }
    for (table, expected) in [
        (
            "metadata",
            vec![("key", "TEXT", 1, 1), ("value", "TEXT", 1, 0)],
        ),
        (
            "objects",
            vec![
                ("session_identity", "TEXT", 1, 1),
                ("generation", "TEXT", 1, 2),
                ("id", "TEXT", 1, 3),
                ("size", "INTEGER", 1, 0),
                ("payload", "BLOB", 1, 0),
            ],
        ),
        (
            "manifests",
            vec![
                ("session_identity", "TEXT", 1, 1),
                ("generation", "TEXT", 1, 0),
                ("revision", "INTEGER", 1, 0),
                ("digest", "TEXT", 1, 0),
                ("size", "INTEGER", 1, 0),
                ("payload", "BLOB", 1, 0),
            ],
        ),
        (
            "tombstones",
            vec![
                ("session_identity", "TEXT", 1, 1),
                ("generation", "TEXT", 1, 0),
                ("revision", "INTEGER", 1, 0),
                ("prior_digest", "TEXT", 1, 0),
            ],
        ),
    ] {
        let mut s = c
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(storage)?;
        let got: Vec<(String, String, i64, i64)> = s
            .query_map([], |r| Ok((r.get(1)?, r.get(2)?, r.get(3)?, r.get(5)?)))
            .map_err(storage)?
            .collect::<Result<_, _>>()
            .map_err(storage)?;
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(a, b, n, p)| (a.into(), b.into(), n, p))
            .collect();
        if got != expected {
            return Err(corrupt(format!("{table} table constraints mismatch")));
        }
    }
    Ok(())
}

const MAGIC: &[u8] = b"grok-build-sdk.session-log\0\x01";
fn encode_object(
    k: &SessionKey,
    g: &SessionGeneration,
    kind: &SessionObjectKind,
) -> Result<Vec<u8>, SessionStateStoreError> {
    let mut b = MAGIC.to_vec();
    put_bytes(&mut b, k.0.as_bytes());
    put_bytes(&mut b, g.0.as_bytes());
    match kind {
        SessionObjectKind::TranscriptSegment {
            previous,
            sequence,
            bytes,
        } => {
            b.push(1);
            put_ref(&mut b, previous.as_ref());
            b.extend(sequence.to_be_bytes());
            put_bytes64(&mut b, bytes)
        }
        SessionObjectKind::Checkpoint { name, shell_bytes } => {
            b.push(2);
            put_bytes(&mut b, name.as_bytes());
            put_bytes64(&mut b, shell_bytes)
        }
        SessionObjectKind::RewindOperation {
            kind,
            index,
            shell_bytes,
        } => {
            b.push(match kind {
                RewindKind::AppendPoint => 3,
                RewindKind::Truncate => 4,
                RewindKind::Merge => 5,
            });
            b.extend(index.to_be_bytes());
            put_bytes64(&mut b, shell_bytes)
        }
        SessionObjectKind::CheckpointPublication {
            previous,
            sequence,
            marker_bytes,
            checkpoint,
        } => {
            b.push(6);
            put_ref(&mut b, previous.as_ref());
            b.extend(sequence.to_be_bytes());
            put_bytes64(&mut b, marker_bytes);
            put_ref(&mut b, Some(checkpoint))
        }
        SessionObjectKind::RewindPublication {
            previous,
            sequence,
            marker_bytes,
            operation,
        } => {
            b.push(7);
            put_ref(&mut b, previous.as_ref());
            b.extend(sequence.to_be_bytes());
            put_bytes64(&mut b, marker_bytes);
            put_ref(&mut b, Some(operation))
        }
    }
    if b.len() > MAX_SESSION_OBJECT_BYTES {
        return Err(validation("object exceeds 64 MiB"));
    }
    Ok(b)
}
fn decode_object(
    b: &[u8],
) -> Result<(SessionKey, SessionGeneration, SessionObjectKind), SessionStateStoreError> {
    if b.len() > MAX_SESSION_OBJECT_BYTES || !b.starts_with(MAGIC) {
        return Err(corrupt("object marker/version mismatch"));
    }
    let mut p = MAGIC.len();
    let k = SessionKey::new(String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?)
        .map_err(as_corrupt)?;
    let g = SessionGeneration::new(String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?)
        .map_err(as_corrupt)?;
    let tag = take(b, &mut p, 1)?[0];
    let kind = match tag {
        1 => SessionObjectKind::TranscriptSegment {
            previous: get_ref(b, &mut p)?,
            sequence: get_u64(b, &mut p)?,
            bytes: get_bytes64(b, &mut p)?,
        },
        2 => SessionObjectKind::Checkpoint {
            name: String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?,
            shell_bytes: get_bytes64(b, &mut p)?,
        },
        3..=5 => SessionObjectKind::RewindOperation {
            kind: if tag == 3 {
                RewindKind::AppendPoint
            } else if tag == 4 {
                RewindKind::Truncate
            } else {
                RewindKind::Merge
            },
            index: get_u64(b, &mut p)?,
            shell_bytes: get_bytes64(b, &mut p)?,
        },
        6 | 7 => {
            let previous = get_ref(b, &mut p)?;
            let sequence = get_u64(b, &mut p)?;
            let marker_bytes = get_bytes64(b, &mut p)?;
            let reference =
                get_ref(b, &mut p)?.ok_or_else(|| corrupt("publication missing reference"))?;
            if tag == 6 {
                SessionObjectKind::CheckpointPublication {
                    previous,
                    sequence,
                    marker_bytes,
                    checkpoint: reference,
                }
            } else {
                SessionObjectKind::RewindPublication {
                    previous,
                    sequence,
                    marker_bytes,
                    operation: reference,
                }
            }
        }
        _ => return Err(corrupt("invalid object kind")),
    };
    if p != b.len() {
        return Err(corrupt("trailing object bytes"));
    }
    Ok((k, g, kind))
}
fn encode_manifest(
    k: &SessionKey,
    g: &SessionGeneration,
    h: Option<&SessionObjectId>,
    count: u64,
    bytes: u64,
) -> Result<Vec<u8>, SessionStateStoreError> {
    let mut b = MAGIC.to_vec();
    put_bytes(&mut b, k.0.as_bytes());
    put_bytes(&mut b, g.0.as_bytes());
    put_ref(&mut b, h);
    b.extend(count.to_be_bytes());
    b.extend(bytes.to_be_bytes());
    if b.len() > MAX_SESSION_MANIFEST_BYTES {
        return Err(validation("manifest exceeds 64 KiB"));
    }
    Ok(b)
}
fn decode_manifest(b: &[u8]) -> Result<SessionManifest, SessionStateStoreError> {
    if !b.starts_with(MAGIC) {
        return Err(corrupt("manifest marker/version mismatch"));
    }
    let mut p = MAGIC.len();
    let session = SessionKey::new(String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?)
        .map_err(as_corrupt)?;
    let generation =
        SessionGeneration::new(String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?)
            .map_err(as_corrupt)?;
    let head = get_ref(b, &mut p)?;
    let segment_count = get_u64(b, &mut p)?;
    let transcript_bytes = get_u64(b, &mut p)?;
    if p != b.len() || head.is_none() && (segment_count != 0 || transcript_bytes != 0) {
        return Err(corrupt("invalid manifest"));
    }
    Ok(SessionManifest {
        session,
        generation,
        head,
        segment_count,
        transcript_bytes,
        bytes: b.to_vec(),
    })
}
fn chain_parts(k: &SessionObjectKind) -> Option<(Option<&SessionObjectId>, u64)> {
    match k {
        SessionObjectKind::TranscriptSegment {
            previous, sequence, ..
        }
        | SessionObjectKind::CheckpointPublication {
            previous, sequence, ..
        }
        | SessionObjectKind::RewindPublication {
            previous, sequence, ..
        } => Some((previous.as_ref(), *sequence)),
        _ => None,
    }
}
fn chain_bytes(k: &SessionObjectKind) -> u64 {
    match k {
        SessionObjectKind::TranscriptSegment { bytes, .. } => bytes.len() as u64,
        SessionObjectKind::CheckpointPublication { marker_bytes, .. }
        | SessionObjectKind::RewindPublication { marker_bytes, .. } => marker_bytes.len() as u64,
        _ => 0,
    }
}
fn validate_kind(k: &SessionObjectKind) -> Result<(), SessionStateStoreError> {
    if let Some((p, s)) = chain_parts(k) {
        if s == 0 || s == 1 && p.is_some() || s > 1 && p.is_none() {
            return Err(validation("chain previous/sequence mismatch"));
        }
    }
    if let SessionObjectKind::Checkpoint { name, .. } = k {
        valid_text(name, 1024, "checkpoint name")?
    }
    Ok(())
}
fn put_bytes(b: &mut Vec<u8>, v: &[u8]) {
    b.extend((v.len() as u32).to_be_bytes());
    b.extend(v)
}
fn put_bytes64(b: &mut Vec<u8>, v: &[u8]) {
    b.extend((v.len() as u64).to_be_bytes());
    b.extend(v)
}
fn get_bytes(b: &[u8], p: &mut usize) -> Result<Vec<u8>, SessionStateStoreError> {
    let n = u32::from_be_bytes(take(b, p, 4)?.try_into().unwrap()) as usize;
    Ok(take(b, p, n)?.to_vec())
}
fn get_bytes64(b: &[u8], p: &mut usize) -> Result<Vec<u8>, SessionStateStoreError> {
    let n = usize::try_from(get_u64(b, p)?).map_err(|_| corrupt("length overflow"))?;
    Ok(take(b, p, n)?.to_vec())
}
fn get_u64(b: &[u8], p: &mut usize) -> Result<u64, SessionStateStoreError> {
    Ok(u64::from_be_bytes(take(b, p, 8)?.try_into().unwrap()))
}
fn put_ref(b: &mut Vec<u8>, r: Option<&SessionObjectId>) {
    match r {
        None => b.push(0),
        Some(r) => {
            b.push(1);
            b.extend(r.0.strip_prefix("sha256:").unwrap().as_bytes())
        }
    }
}
fn get_ref(b: &[u8], p: &mut usize) -> Result<Option<SessionObjectId>, SessionStateStoreError> {
    match take(b, p, 1)?[0] {
        0 => Ok(None),
        1 => SessionObjectId::from_stored(format!(
            "sha256:{}",
            String::from_utf8(take(b, p, 64)?.to_vec()).map_err(corrupt)?
        ))
        .map(Some),
        _ => Err(corrupt("invalid reference tag")),
    }
}
fn take<'a>(b: &'a [u8], p: &mut usize, n: usize) -> Result<&'a [u8], SessionStateStoreError> {
    let end = p.checked_add(n).ok_or_else(|| corrupt("length overflow"))?;
    let out = b
        .get(*p..end)
        .ok_or_else(|| corrupt("truncated encoding"))?;
    *p = end;
    Ok(out)
}
fn valid_text(v: &str, max: usize, n: &str) -> Result<(), SessionStateStoreError> {
    if v.is_empty() || v.len() > max || v.contains('\0') {
        Err(validation(format!("invalid {n}")))
    } else {
        Ok(())
    }
}
fn validate_digest(v: &str) -> Result<(), SessionStateStoreError> {
    if v.strip_prefix("sha256:").is_none_or(|h| {
        h.len() != 64
            || !h
                .bytes()
                .all(|x| x.is_ascii_digit() || (b'a'..=b'f').contains(&x))
    }) {
        Err(validation("invalid sha256 content ID"))
    } else {
        Ok(())
    }
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
fn as_corrupt(e: SessionStateStoreError) -> SessionStateStoreError {
    corrupt(e)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConformanceOpen {
    Fresh,
    Concurrent,
    Reopen,
}

fn suite_error(message: impl Into<String>) -> SessionStateStoreError {
    SessionStateStoreError::Corrupt(format!("conformance: {}", message.into()))
}

/// Runs the backend-neutral contract against independent handles to one authority.
pub fn run_session_state_conformance<F>(mut open: F) -> Result<(), SessionStateStoreError>
where
    F: FnMut(ConformanceOpen) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError>,
{
    let store = open(ConformanceOpen::Fresh)?;
    let key = SessionKey::new("session-state-conformance")?;
    let generation = SessionGeneration::new("generation-1")?;
    if store.inspect_slot(&key)? != SessionSlot::Vacant {
        return Err(suite_error("fresh slot is not vacant"));
    }

    let orphan = SessionObject::checkpoint(
        key.clone(),
        generation.clone(),
        "orphan",
        b"orphan".to_vec(),
    )?;
    let orphan_id = orphan.id().clone();
    store.put_object(orphan.clone())?;
    if store.load_object(&key, &generation, orphan.id())? != Some(orphan) {
        return Err(suite_error("scoped object roundtrip"));
    }
    if store
        .load_object(&SessionKey::new("other-session")?, &generation, &orphan_id)?
        .is_some()
    {
        return Err(suite_error("object escaped session scope"));
    }

    let first =
        SessionObject::transcript(key.clone(), generation.clone(), None, 1, b"first".to_vec())?;
    store.put_object(first.clone())?;
    let first_request = PreparedManifestCas::new(
        key.clone(),
        None,
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(first.id().clone()),
            1,
            5,
        )?,
        std::slice::from_ref(&first),
    )?;
    let exact_first = first_request.successor().clone();
    if store.compare_and_swap_manifest(first_request)?
        != ManifestCas::Committed(exact_first.clone())
    {
        return Err(suite_error(
            "initial CAS did not return exact prepared successor",
        ));
    }

    let wrong_payload = SessionObject::rewind(
        key.clone(),
        generation.clone(),
        RewindKind::Merge,
        1,
        b"wrong-kind".to_vec(),
    )?;
    let wrong_publication = SessionObject::publish_checkpoint(
        key.clone(),
        generation.clone(),
        Some(first.id().clone()),
        2,
        b"invalid-reference".to_vec(),
        wrong_payload.id().clone(),
    )?;
    store.put_object(wrong_payload)?;
    store.put_object(wrong_publication.clone())?;
    let wrong_request = PreparedManifestCas::new(
        key.clone(),
        Some(exact_first.clone()),
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(wrong_publication.id().clone()),
            2,
            5 + b"invalid-reference".len() as u64,
        )?,
        std::slice::from_ref(&wrong_publication),
    )?;
    if store.compare_and_swap_manifest(wrong_request).is_ok() {
        return Err(suite_error("wrong-kind publication reference accepted"));
    }

    let checkpoint = SessionObject::checkpoint(
        key.clone(),
        generation.clone(),
        "named-checkpoint",
        b"checkpoint-payload".to_vec(),
    )?;
    let checkpoint_marker = b"checkpoint-marker".to_vec();
    let checkpoint_pub = SessionObject::publish_checkpoint(
        key.clone(),
        generation.clone(),
        Some(first.id().clone()),
        2,
        checkpoint_marker.clone(),
        checkpoint.id().clone(),
    )?;
    for object in [&checkpoint, &checkpoint_pub] {
        store.put_object(object.clone())?;
    }
    let cp_request = PreparedManifestCas::new(
        key.clone(),
        Some(exact_first.clone()),
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(checkpoint_pub.id().clone()),
            2,
            5 + checkpoint_marker.len() as u64,
        )?,
        std::slice::from_ref(&checkpoint_pub),
    )?;
    let exact_cp = cp_request.successor().clone();
    if store.compare_and_swap_manifest(cp_request)? != ManifestCas::Committed(exact_cp.clone()) {
        return Err(suite_error("checkpoint publication"));
    }
    match store
        .load_object(&key, &generation, checkpoint_pub.id())?
        .map(|o| o.kind)
    {
        Some(SessionObjectKind::CheckpointPublication {
            marker_bytes,
            checkpoint: reference,
            ..
        }) if marker_bytes == checkpoint_marker && reference == *checkpoint.id() => {}
        _ => {
            return Err(suite_error(
                "checkpoint marker/reference kind did not roundtrip",
            ));
        }
    }
    match store
        .load_object(&key, &generation, checkpoint.id())?
        .map(|o| o.kind)
    {
        Some(SessionObjectKind::Checkpoint { name, shell_bytes })
            if name == "named-checkpoint" && shell_bytes == b"checkpoint-payload" => {}
        _ => return Err(suite_error("checkpoint payload/name did not roundtrip")),
    }

    let rewind = SessionObject::rewind(
        key.clone(),
        generation.clone(),
        RewindKind::Truncate,
        7,
        b"rewind-payload".to_vec(),
    )?;
    let rewind_marker = b"rewind-marker".to_vec();
    let rewind_pub = SessionObject::publish_rewind(
        key.clone(),
        generation.clone(),
        Some(checkpoint_pub.id().clone()),
        3,
        rewind_marker.clone(),
        rewind.id().clone(),
    )?;
    for object in [&rewind, &rewind_pub] {
        store.put_object(object.clone())?;
    }
    let rw_request = PreparedManifestCas::new(
        key.clone(),
        Some(exact_cp.clone()),
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(rewind_pub.id().clone()),
            3,
            exact_cp.manifest().transcript_bytes() + rewind_marker.len() as u64,
        )?,
        std::slice::from_ref(&rewind_pub),
    )?;
    let exact_rw = rw_request.successor().clone();
    if store.compare_and_swap_manifest(rw_request)? != ManifestCas::Committed(exact_rw.clone()) {
        return Err(suite_error("rewind publication"));
    }
    match store
        .load_object(&key, &generation, rewind_pub.id())?
        .map(|o| o.kind)
    {
        Some(SessionObjectKind::RewindPublication {
            marker_bytes,
            operation,
            ..
        }) if marker_bytes == rewind_marker && operation == *rewind.id() => {}
        _ => {
            return Err(suite_error(
                "rewind marker/reference kind did not roundtrip",
            ));
        }
    }

    // Two genuinely independent handles race from exactly the same expected document.
    let left = open(ConformanceOpen::Concurrent)?;
    let right = open(ConformanceOpen::Concurrent)?;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let make = |bytes: &'static [u8]| -> Result<(SessionObject, PreparedManifestCas), SessionStateStoreError> {
        let object = SessionObject::transcript(key.clone(), generation.clone(), Some(rewind_pub.id().clone()), 4, bytes.to_vec())?;
        let request = PreparedManifestCas::new(key.clone(), Some(exact_rw.clone()),
            SessionManifest::new(key.clone(), generation.clone(), Some(object.id().clone()), 4, exact_rw.manifest().transcript_bytes() + bytes.len() as u64)?,
            std::slice::from_ref(&object))?;
        Ok((object, request))
    };
    let (lo, lr) = make(b"left")?;
    let (ro, rr) = make(b"right")?;
    left.put_object(lo)?;
    right.put_object(ro)?;
    let run =
        |s: Arc<dyn SessionStateStore>, b: Arc<std::sync::Barrier>, r: PreparedManifestCas| {
            std::thread::spawn(move || {
                b.wait();
                s.compare_and_swap_manifest(r)
            })
        };
    let left_thread = run(left, barrier.clone(), lr);
    let right_thread = run(right, barrier, rr);
    let a = left_thread
        .join()
        .map_err(|_| suite_error("race thread panicked"))??;
    let b = right_thread
        .join()
        .map_err(|_| suite_error("race thread panicked"))??;
    if usize::from(matches!(a, ManifestCas::Committed(_)))
        + usize::from(matches!(b, ManifestCas::Committed(_)))
        != 1
        || usize::from(a == ManifestCas::Conflict) + usize::from(b == ManifestCas::Conflict) != 1
    {
        return Err(suite_error("race was not one commit/one conflict"));
    }
    let winner = match store.inspect_slot(&key)? {
        SessionSlot::Live(x) => x,
        _ => return Err(suite_error("winner missing")),
    };
    let stale_object = SessionObject::transcript(
        key.clone(),
        generation.clone(),
        Some(rewind_pub.id().clone()),
        4,
        b"stale".to_vec(),
    )?;
    store.put_object(stale_object.clone())?;
    let stale = PreparedManifestCas::new(
        key.clone(),
        Some(exact_rw),
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(stale_object.id().clone()),
            4,
            5 + checkpoint_marker.len() as u64 + rewind_marker.len() as u64 + 5,
        )?,
        std::slice::from_ref(&stale_object),
    )?;
    if store.compare_and_swap_manifest(stale)? != ManifestCas::Conflict
        || store.inspect_slot(&key)? != SessionSlot::Live(winner.clone())
    {
        return Err(suite_error("stale writer changed winner"));
    }

    // Advance with individually bounded objects until cumulative transcript exceeds 64 MiB.
    let mut live = winner;
    for n in 0..65u8 {
        let payload = vec![n; 1024 * 1024];
        let object = SessionObject::transcript(
            key.clone(),
            generation.clone(),
            live.manifest().head().cloned(),
            live.manifest().segment_count() + 1,
            payload,
        )?;
        store.put_object(object.clone())?;
        let request = PreparedManifestCas::new(
            key.clone(),
            Some(live.clone()),
            SessionManifest::new(
                key.clone(),
                generation.clone(),
                Some(object.id().clone()),
                live.manifest().segment_count() + 1,
                live.manifest().transcript_bytes() + 1024 * 1024,
            )?,
            std::slice::from_ref(&object),
        )?;
        live = request.successor().clone();
        if store.compare_and_swap_manifest(request)? != ManifestCas::Committed(live.clone()) {
            return Err(suite_error("large cumulative transcript commit"));
        }
    }
    if live.manifest().transcript_bytes() <= MAX_SESSION_OBJECT_BYTES as u64 {
        return Err(suite_error(
            "cumulative transcript did not exceed object bound",
        ));
    }
    drop(store);
    let reopened = open(ConformanceOpen::Reopen)?;
    if reopened.inspect_slot(&key)? != SessionSlot::Live(live.clone()) {
        return Err(suite_error("restart full traversal"));
    }
    let delete = PreparedSessionDelete::new(key.clone(), live.clone())?;
    let intended_receipt = delete.receipt().clone();
    let receipt = match reopened.compare_and_delete(delete)? {
        SessionDelete::Deleted(x) if x == intended_receipt => x,
        _ => return Err(suite_error("CAS delete")),
    };
    if reopened.inspect_slot(&key)?
        != (SessionSlot::Tombstoned {
            receipt: receipt.clone(),
        })
    {
        return Err(suite_error("tombstone missing"));
    }
    drop(reopened);
    let reopened = open(ConformanceOpen::Reopen)?;
    if reopened.inspect_slot(&key)? != (SessionSlot::Tombstoned { receipt }) {
        return Err(suite_error("tombstone not permanent"));
    }
    let recreate = SessionObject::transcript(
        key.clone(),
        generation.clone(),
        None,
        1,
        b"recreate".to_vec(),
    )?;
    reopened.put_object(recreate.clone())?;
    let request = PreparedManifestCas::new(
        key.clone(),
        None,
        SessionManifest::new(key.clone(), generation, Some(recreate.id().clone()), 1, 8)?,
        std::slice::from_ref(&recreate),
    )?;
    if reopened.compare_and_swap_manifest(request)? != ManifestCas::Conflict {
        return Err(suite_error("tombstone allowed recreation"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionStateFault {
    AfterObjectPutBeforeAck,
    AfterManifestCommitBeforeAck,
    AfterDeleteBeforeAck,
    CorruptObjectPayload,
    RemoveObject,
    DeclareObjectOversize,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionStateFaultMetrics {
    pub backend_calls: u64,
    pub payload_bytes_read: u64,
    pub injected_faults: u64,
}
pub trait SessionStateFaultHarness {
    fn reset(&mut self) -> Result<(), SessionStateStoreError>;
    fn open(&mut self) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError>;
    fn inject(
        &mut self,
        fault: SessionStateFault,
        key: &SessionKey,
        generation: &SessionGeneration,
        object: Option<&SessionObjectId>,
    ) -> Result<(), SessionStateStoreError>;
    fn metrics(&self) -> SessionStateFaultMetrics;
}

/// Orchestrates real operations around typed backend fault injection points.
pub fn run_session_state_fault_conformance<H: SessionStateFaultHarness>(
    h: &mut H,
) -> Result<(), SessionStateStoreError> {
    let key = SessionKey::new("session-state-fault-conformance")?;
    let generation = SessionGeneration::new("generation-1")?;
    h.reset()?;
    let store = h.open()?;
    let object = SessionObject::transcript(
        key.clone(),
        generation.clone(),
        None,
        1,
        b"object-unknown".to_vec(),
    )?;
    h.inject(
        SessionStateFault::AfterObjectPutBeforeAck,
        &key,
        &generation,
        Some(object.id()),
    )?;
    let result = store.put_object(object.clone())?;
    if result != ObjectPut::CommitUnknown
        || !object_put_reconciled(
            &result,
            store.load_object(&key, &generation, object.id())?.as_ref(),
            &object,
        )
    {
        return Err(suite_error(
            "object CommitUnknown did not reconcile exact ID/bytes",
        ));
    }
    let request = PreparedManifestCas::new(
        key.clone(),
        None,
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(object.id().clone()),
            1,
            14,
        )?,
        std::slice::from_ref(&object),
    )?;
    let intended = request.successor().clone();
    h.inject(
        SessionStateFault::AfterManifestCommitBeforeAck,
        &key,
        &generation,
        None,
    )?;
    let result = store.compare_and_swap_manifest(request)?;
    let slot = store.inspect_slot(&key)?;
    if result != ManifestCas::CommitUnknown
        || !manifest_cas_reconciled(&result, &slot, &intended)
        || slot != SessionSlot::Live(intended.clone())
    {
        return Err(suite_error(
            "manifest CommitUnknown did not reconcile exact document",
        ));
    }
    h.inject(
        SessionStateFault::AfterDeleteBeforeAck,
        &key,
        &generation,
        None,
    )?;
    let result =
        store.compare_and_delete(PreparedSessionDelete::new(key.clone(), intended.clone())?)?;
    let slot = store.inspect_slot(&key)?;
    if result != SessionDelete::CommitUnknown || !delete_reconciled(&result, &slot, &intended) {
        return Err(suite_error(
            "delete CommitUnknown did not reconcile exact tombstone",
        ));
    }

    for fault in [
        SessionStateFault::RemoveObject,
        SessionStateFault::CorruptObjectPayload,
    ] {
        h.reset()?;
        let store = h.open()?;
        let object = SessionObject::transcript(
            key.clone(),
            generation.clone(),
            None,
            1,
            b"damage".to_vec(),
        )?;
        store.put_object(object.clone())?;
        let request = PreparedManifestCas::new(
            key.clone(),
            None,
            SessionManifest::new(
                key.clone(),
                generation.clone(),
                Some(object.id().clone()),
                1,
                6,
            )?,
            std::slice::from_ref(&object),
        )?;
        store.compare_and_swap_manifest(request)?;
        h.inject(fault, &key, &generation, Some(object.id()))?;
        if !matches!(
            store.inspect_slot(&key),
            Err(SessionStateStoreError::Corrupt(_))
        ) {
            return Err(suite_error("missing/corrupt object did not fail closed"));
        }
    }
    h.reset()?;
    let store = h.open()?;
    let object = SessionObject::transcript(
        key.clone(),
        generation.clone(),
        None,
        1,
        b"oversize".to_vec(),
    )?;
    store.put_object(object.clone())?;
    h.inject(
        SessionStateFault::DeclareObjectOversize,
        &key,
        &generation,
        Some(object.id()),
    )?;
    let before = h.metrics().payload_bytes_read;
    if !matches!(
        store.load_object(&key, &generation, object.id()),
        Err(SessionStateStoreError::Corrupt(_))
    ) || h.metrics().payload_bytes_read != before
    {
        return Err(suite_error("declared oversize fetched payload"));
    }
    if h.metrics().backend_calls == 0 || h.metrics().injected_faults == 0 {
        return Err(suite_error("fault metrics missing"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct FaultStore {
        inner: LocalSessionStateStore,
        calls: Arc<AtomicU64>,
        reads: Arc<AtomicU64>,
        object_unknown: Arc<AtomicBool>,
        manifest_unknown: Arc<AtomicBool>,
        delete_unknown: Arc<AtomicBool>,
    }
    impl SessionStateStore for FaultStore {
        fn inspect_slot(&self, key: &SessionKey) -> Result<SessionSlot, SessionStateStoreError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.inspect_slot(key)
        }
        fn load_object(
            &self,
            key: &SessionKey,
            generation: &SessionGeneration,
            id: &SessionObjectId,
        ) -> Result<Option<SessionObject>, SessionStateStoreError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let result = self.inner.load_object(key, generation, id);
            if let Ok(Some(object)) = &result {
                self.reads
                    .fetch_add(object.declared_size(), Ordering::Relaxed);
            }
            result
        }
        fn put_object(&self, object: SessionObject) -> Result<ObjectPut, SessionStateStoreError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let result = self.inner.put_object(object)?;
            if self.object_unknown.swap(false, Ordering::AcqRel)
                && matches!(result, ObjectPut::Stored | ObjectPut::AlreadyPresent)
            {
                Ok(ObjectPut::CommitUnknown)
            } else {
                Ok(result)
            }
        }
        fn compare_and_swap_manifest(
            &self,
            request: PreparedManifestCas,
        ) -> Result<ManifestCas, SessionStateStoreError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let result = self.inner.compare_and_swap_manifest(request)?;
            if self.manifest_unknown.swap(false, Ordering::AcqRel)
                && matches!(result, ManifestCas::Committed(_))
            {
                Ok(ManifestCas::CommitUnknown)
            } else {
                Ok(result)
            }
        }
        fn compare_and_delete(
            &self,
            request: PreparedSessionDelete,
        ) -> Result<SessionDelete, SessionStateStoreError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let result = self.inner.compare_and_delete(request)?;
            if self.delete_unknown.swap(false, Ordering::AcqRel)
                && matches!(result, SessionDelete::Deleted(_))
            {
                Ok(SessionDelete::CommitUnknown)
            } else {
                Ok(result)
            }
        }
    }
    struct LocalFaultHarness {
        root: tempfile::TempDir,
        epoch: u64,
        calls: Arc<AtomicU64>,
        reads: Arc<AtomicU64>,
        faults: u64,
        object_unknown: Arc<AtomicBool>,
        manifest_unknown: Arc<AtomicBool>,
        delete_unknown: Arc<AtomicBool>,
    }
    impl LocalFaultHarness {
        fn new() -> Self {
            Self {
                root: tempfile::tempdir().unwrap(),
                epoch: 0,
                calls: Arc::new(AtomicU64::new(0)),
                reads: Arc::new(AtomicU64::new(0)),
                faults: 0,
                object_unknown: Arc::new(AtomicBool::new(false)),
                manifest_unknown: Arc::new(AtomicBool::new(false)),
                delete_unknown: Arc::new(AtomicBool::new(false)),
            }
        }
        fn directory(&self) -> PathBuf {
            self.root.path().join(self.epoch.to_string())
        }
    }
    impl SessionStateFaultHarness for LocalFaultHarness {
        fn reset(&mut self) -> Result<(), SessionStateStoreError> {
            self.epoch += 1;
            self.calls.store(0, Ordering::Relaxed);
            self.reads.store(0, Ordering::Relaxed);
            self.faults = 0;
            self.object_unknown.store(false, Ordering::Relaxed);
            self.manifest_unknown.store(false, Ordering::Relaxed);
            self.delete_unknown.store(false, Ordering::Relaxed);
            Ok(())
        }
        fn open(&mut self) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError> {
            Ok(Arc::new(FaultStore {
                inner: LocalSessionStateStore::new(self.directory())?,
                calls: self.calls.clone(),
                reads: self.reads.clone(),
                object_unknown: self.object_unknown.clone(),
                manifest_unknown: self.manifest_unknown.clone(),
                delete_unknown: self.delete_unknown.clone(),
            }))
        }
        fn inject(
            &mut self,
            fault: SessionStateFault,
            key: &SessionKey,
            generation: &SessionGeneration,
            object: Option<&SessionObjectId>,
        ) -> Result<(), SessionStateStoreError> {
            self.faults += 1;
            match fault {
                SessionStateFault::AfterObjectPutBeforeAck => {
                    self.object_unknown.store(true, Ordering::Release)
                }
                SessionStateFault::AfterManifestCommitBeforeAck => {
                    self.manifest_unknown.store(true, Ordering::Release)
                }
                SessionStateFault::AfterDeleteBeforeAck => {
                    self.delete_unknown.store(true, Ordering::Release)
                }
                mutation => {
                    let id = object.ok_or_else(|| validation("object fault requires object ID"))?;
                    let connection = rusqlite::Connection::open(
                        self.directory().join("native-session-log.sqlite3"),
                    )
                    .map_err(storage)?;
                    let sql = match mutation {
                        SessionStateFault::CorruptObjectPayload => {
                            "UPDATE objects SET payload=X'00' WHERE session_identity=?1 AND generation=?2 AND id=?3"
                        }
                        SessionStateFault::RemoveObject => {
                            "DELETE FROM objects WHERE session_identity=?1 AND generation=?2 AND id=?3"
                        }
                        SessionStateFault::DeclareObjectOversize => {
                            "UPDATE objects SET size=67108865 WHERE session_identity=?1 AND generation=?2 AND id=?3"
                        }
                        _ => unreachable!(),
                    };
                    connection
                        .execute(sql, rusqlite::params![key.0, generation.0, id.0])
                        .map_err(storage)?;
                }
            }
            Ok(())
        }
        fn metrics(&self) -> SessionStateFaultMetrics {
            SessionStateFaultMetrics {
                backend_calls: self.calls.load(Ordering::Relaxed),
                payload_bytes_read: self.reads.load(Ordering::Relaxed),
                injected_faults: self.faults,
            }
        }
    }

    #[test]
    fn local_conformance() {
        let d = tempfile::tempdir().unwrap();
        run_session_state_conformance(|_| Ok(Arc::new(LocalSessionStateStore::new(d.path())?)))
            .unwrap();
    }
    #[test]
    fn local_fault_conformance() {
        run_session_state_fault_conformance(&mut LocalFaultHarness::new()).unwrap();
    }
}
