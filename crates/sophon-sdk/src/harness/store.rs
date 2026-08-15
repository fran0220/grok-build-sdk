// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{HarnessDigest, HarnessError, HarnessSnapshot, MAX_HARNESS_SNAPSHOT_BYTES};
use rusqlite::OptionalExtension as _;
use std::{path::PathBuf, sync::Mutex};

pub const HARNESS_STORE_SCHEMA_MARKER: &str = "sophon-sdk.harness-snapshot-store";
pub const HARNESS_STORE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum HarnessStoreError {
    #[error("harness snapshot validation failed: {0}")]
    Validation(String),
    #[error("stored harness snapshot is corrupt: {0}")]
    Corrupt(String),
    #[error("harness snapshot storage failed: {0}")]
    Storage(String),
}

impl From<HarnessError> for HarnessStoreError {
    fn from(error: HarnessError) -> Self {
        Self::Validation(error.to_string())
    }
}

/// Outcome of writing one immutable snapshot under its own content address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HarnessPut {
    Stored,
    AlreadyPresent,
    /// The authority may or may not have committed. Callers reconcile by
    /// loading the digest; they never treat this as a failure to store.
    CommitUnknown,
}

/// Content-addressed, append-only persistence boundary for immutable harness
/// snapshots.
///
/// This is a Host-injectable contract, not SDK state: the Runtime never reads
/// or writes it, and a Session retains only the digest it was bound to. There
/// is deliberately no update, replace or delete operation. A change to harness
/// content is a new snapshot with a new digest; the bytes reachable under an
/// existing digest must never change. Retention or garbage collection may
/// exist in a backend, but only as removal of unreachable content under an
/// operator-defined policy — never as a rewrite of live content.
pub trait HarnessStore: Send + Sync + 'static {
    /// Loads the snapshot stored under `digest`, verifying that the stored
    /// bytes still address to the requested digest.
    fn get(&self, digest: &HarnessDigest) -> Result<Option<HarnessSnapshot>, HarnessStoreError>;

    /// Stores one validated snapshot under its own digest. Storing an already
    /// present digest is idempotent and must leave the stored bytes untouched.
    fn put(&self, snapshot: &HarnessSnapshot) -> Result<HarnessPut, HarnessStoreError>;

    fn contains(&self, digest: &HarnessDigest) -> Result<bool, HarnessStoreError> {
        Ok(self.get(digest)?.is_some())
    }
}

/// Decides whether a `put` outcome is settled. An unknown commit is settled
/// only when the exact intended snapshot is readable under its digest.
pub fn harness_put_reconciled(
    result: &HarnessPut,
    loaded: Option<&HarnessSnapshot>,
    intended: &HarnessSnapshot,
) -> bool {
    matches!(result, HarnessPut::Stored | HarnessPut::AlreadyPresent)
        || matches!(result, HarnessPut::CommitUnknown) && loaded == Some(intended)
}

/// Local filesystem reference authority. Production Hosts inject their own
/// implementation; injection replaces this one and is never mirrored.
pub struct LocalHarnessStore {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}

impl LocalHarnessStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, HarnessStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        let path = root.join("harness-snapshots.sqlite3");
        let existed = path.exists();
        let connection = rusqlite::Connection::open(&path).map_err(storage)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                 CREATE TABLE IF NOT EXISTS snapshots(
                   digest TEXT PRIMARY KEY NOT NULL,
                   size INTEGER NOT NULL,
                   payload BLOB NOT NULL
                 );",
            )
            .map_err(storage)?;
        let metadata: Vec<(String, String)> = connection
            .prepare("SELECT key,value FROM metadata ORDER BY key")
            .map_err(storage)?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(storage)?
            .collect::<Result<_, _>>()
            .map_err(storage)?;
        if metadata.is_empty() {
            if existed {
                return Err(HarnessStoreError::Corrupt(
                    "existing harness snapshot store has no schema metadata".into(),
                ));
            }
            let transaction = connection.unchecked_transaction().map_err(storage)?;
            transaction
                .execute(
                    "INSERT INTO metadata(key,value) VALUES('schema_marker',?1)",
                    [HARNESS_STORE_SCHEMA_MARKER],
                )
                .map_err(storage)?;
            transaction
                .execute(
                    "INSERT INTO metadata(key,value) VALUES('schema_version',?1)",
                    [HARNESS_STORE_SCHEMA_VERSION.to_string()],
                )
                .map_err(storage)?;
            transaction.commit().map_err(storage)?;
        } else if metadata
            != [
                ("schema_marker".into(), HARNESS_STORE_SCHEMA_MARKER.into()),
                (
                    "schema_version".into(),
                    HARNESS_STORE_SCHEMA_VERSION.to_string(),
                ),
            ]
        {
            return Err(HarnessStoreError::Corrupt(
                "harness snapshot schema marker/version mismatch".into(),
            ));
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

impl HarnessStore for LocalHarnessStore {
    fn get(&self, digest: &HarnessDigest) -> Result<Option<HarnessSnapshot>, HarnessStoreError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
            .map_err(storage)?;
        let stored = load(&transaction, digest)?;
        transaction.commit().map_err(storage)?;
        Ok(stored)
    }

    fn put(&self, snapshot: &HarnessSnapshot) -> Result<HarnessPut, HarnessStoreError> {
        let bytes = snapshot.to_json_vec()?;
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO snapshots(digest,size,payload) VALUES(?1,?2,?3)",
                rusqlite::params![snapshot.digest().as_str(), bytes.len() as u64, bytes],
            )
            .map_err(storage)?;
        if inserted == 0 && load(&transaction, snapshot.digest())?.as_ref() != Some(snapshot) {
            return Err(HarnessStoreError::Corrupt(
                "stored content under this digest is not the addressed snapshot".into(),
            ));
        }
        match transaction.commit() {
            Ok(()) => Ok(if inserted == 0 {
                HarnessPut::AlreadyPresent
            } else {
                HarnessPut::Stored
            }),
            Err(_) => Ok(HarnessPut::CommitUnknown),
        }
    }
}

fn load(
    connection: &rusqlite::Connection,
    digest: &HarnessDigest,
) -> Result<Option<HarnessSnapshot>, HarnessStoreError> {
    let header: Option<(i64, i64)> = connection
        .query_row(
            "SELECT size,length(payload) FROM snapshots WHERE digest=?1",
            [digest.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    let Some((size, payload_size)) = header else {
        return Ok(None);
    };
    if size < 0 || size != payload_size || size as usize > MAX_HARNESS_SNAPSHOT_BYTES {
        return Err(HarnessStoreError::Corrupt(
            "stored harness snapshot has an invalid declared size".into(),
        ));
    }
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT payload FROM snapshots WHERE digest=?1",
            [digest.as_str()],
            |row| row.get(0),
        )
        .map_err(storage)?;
    let snapshot =
        HarnessSnapshot::from_json_slice(&bytes).map_err(|error| corrupt(error.to_string()))?;
    if snapshot.digest() != digest {
        return Err(corrupt(
            "stored harness snapshot does not address its own key",
        ));
    }
    Ok(Some(snapshot))
}

fn storage(error: impl std::fmt::Display) -> HarnessStoreError {
    HarnessStoreError::Storage(error.to_string())
}

fn corrupt(message: impl Into<String>) -> HarnessStoreError {
    HarnessStoreError::Corrupt(message.into())
}
