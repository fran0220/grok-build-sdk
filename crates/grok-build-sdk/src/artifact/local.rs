// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    ArtifactDigest, ArtifactError, ArtifactHandle, ArtifactId, ArtifactIntegrity,
    ArtifactMaterialization, ArtifactMediaType, ArtifactProvenance, ArtifactPut, ArtifactReceipt,
    ArtifactRecord, ArtifactRecovery, ArtifactRetention, ArtifactUsage, ArtifactVault,
    ArtifactWrite, MAX_ARTIFACT_BYTES, materialize_verified_copy,
};
use rusqlite::TransactionBehavior;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

pub const ARTIFACT_VAULT_SCHEMA_MARKER: &str = "grok-build-sdk.artifact-vault";
pub const ARTIFACT_VAULT_SCHEMA_VERSION: u32 = 1;

/// Local filesystem reference authority. Production Hosts inject their own
/// implementation gated by [`super::run_artifact_vault_conformance`]; injection
/// replaces this one and is never mirrored.
///
/// Content lives in the same transactional store as its record, so a write is
/// one atomic fact rather than a blob and a row that can disagree after a
/// crash. Two handles on one root are two connections to one database, which
/// is what makes a second worker's writes visible to the first.
pub struct LocalArtifactVault {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}

impl LocalArtifactVault {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ArtifactError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        set_private_dir(&root)?;
        let path = root.join("artifact-vault.sqlite3");
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
                 CREATE TABLE IF NOT EXISTS artifacts(
                   artifact_id TEXT PRIMARY KEY NOT NULL,
                   digest TEXT NOT NULL,
                   size INTEGER NOT NULL,
                   media_type TEXT NOT NULL,
                   retention TEXT NOT NULL,
                   retain_until_ms INTEGER,
                   provenance TEXT NOT NULL,
                   written_at_ms INTEGER NOT NULL,
                   payload BLOB NOT NULL,
                   reads INTEGER NOT NULL DEFAULT 0,
                   materializations INTEGER NOT NULL DEFAULT 0,
                   bytes_served INTEGER NOT NULL DEFAULT 0,
                   last_used_at_ms INTEGER,
                   recovered_at_ms INTEGER
                 );",
            )
            .map_err(storage)?;
        verify_schema(&connection, existed)?;
        set_private_file(&path)?;
        Ok(Self {
            path,
            connection: Mutex::new(connection),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn verify_schema(connection: &rusqlite::Connection, existed: bool) -> Result<(), ArtifactError> {
    let metadata: Vec<(String, String)> = connection
        .prepare("SELECT key,value FROM metadata ORDER BY key")
        .map_err(storage)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(storage)?
        .collect::<Result<_, _>>()
        .map_err(storage)?;
    if metadata.is_empty() {
        if existed {
            return Err(ArtifactError::Corrupt(
                "existing artifact vault has no schema metadata".into(),
            ));
        }
        let transaction = connection.unchecked_transaction().map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO metadata(key,value) VALUES('schema_marker',?1)",
                [ARTIFACT_VAULT_SCHEMA_MARKER],
            )
            .map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO metadata(key,value) VALUES('schema_version',?1)",
                [ARTIFACT_VAULT_SCHEMA_VERSION.to_string()],
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        return Ok(());
    }
    if metadata
        != [
            ("schema_marker".into(), ARTIFACT_VAULT_SCHEMA_MARKER.into()),
            (
                "schema_version".into(),
                ARTIFACT_VAULT_SCHEMA_VERSION.to_string(),
            ),
        ]
    {
        return Err(ArtifactError::Corrupt(
            "artifact vault schema marker/version mismatch".into(),
        ));
    }
    Ok(())
}

/// One decoded row whose declarations have been re-checked, but whose content
/// has not yet been addressed. Whether the payload still hashes to the identity
/// naming it is a separate question, deliberately: a record can be readable and
/// its content damaged, and the two answers are not the same answer.
struct StoredArtifact {
    handle: ArtifactHandle,
    size: u64,
    media_type: ArtifactMediaType,
    retention: ArtifactRetention,
    retain_until_ms: Option<u64>,
    provenance: ArtifactProvenance,
    written_at_ms: u64,
    recovered_at_ms: Option<u64>,
    usage: ArtifactUsage,
    payload: Vec<u8>,
}

impl StoredArtifact {
    fn into_record(self) -> ArtifactRecord {
        ArtifactRecord {
            handle: self.handle,
            size: self.size,
            media_type: self.media_type,
            retention: self.retention,
            retain_until_ms: self.retain_until_ms,
            provenance: self.provenance,
            written_at_ms: self.written_at_ms,
            recovered_at_ms: self.recovered_at_ms,
            usage: self.usage,
        }
    }

    fn is_intact(&self) -> bool {
        self.handle.addresses(&self.payload) && self.payload.len() as u64 == self.size
    }
}

const COLUMNS: &str = "artifact_id,digest,size,media_type,retention,retain_until_ms,provenance,\
                       written_at_ms,payload,reads,materializations,bytes_served,last_used_at_ms,\
                       recovered_at_ms";

/// Every stored scalar is re-checked before it is trusted. A row a foreign
/// writer, a partial restore or a damaged page could have produced must fail
/// the read rather than present invented custody to a Host.
fn decode(row: &rusqlite::Row<'_>) -> Result<StoredArtifact, ArtifactError> {
    let id = ArtifactId::parse(row.get::<_, String>(0).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let digest = ArtifactDigest::parse(row.get::<_, String>(1).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let handle = ArtifactHandle::new(id, digest).map_err(|error| corrupt(error.to_string()))?;
    let size = non_negative(row, 2)?;
    let media_type = ArtifactMediaType::new(row.get::<_, String>(3).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let retention = ArtifactRetention::parse(&row.get::<_, String>(4).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let retain_until_ms = optional_non_negative(row, 5)?;
    let provenance: ArtifactProvenance =
        serde_json::from_str(&row.get::<_, String>(6).map_err(storage)?)
            .map_err(|error| corrupt(format!("stored provenance is undecodable: {error}")))?;
    provenance
        .validate()
        .map_err(|error| corrupt(error.to_string()))?;
    let written_at_ms = non_negative(row, 7)?;
    let payload: Vec<u8> = row.get(8).map_err(storage)?;
    if payload.len() > MAX_ARTIFACT_BYTES || size > MAX_ARTIFACT_BYTES as u64 {
        return Err(corrupt("stored artifact exceeds the published bound"));
    }
    Ok(StoredArtifact {
        handle,
        size,
        media_type,
        retention,
        retain_until_ms,
        provenance,
        written_at_ms,
        recovered_at_ms: optional_non_negative(row, 13)?,
        usage: ArtifactUsage {
            reads: non_negative(row, 9)?,
            materializations: non_negative(row, 10)?,
            bytes_served: non_negative(row, 11)?,
            last_used_at_ms: optional_non_negative(row, 12)?,
        },
        payload,
    })
}

fn non_negative(row: &rusqlite::Row<'_>, index: usize) -> Result<u64, ArtifactError> {
    let value: i64 = row.get(index).map_err(storage)?;
    u64::try_from(value).map_err(|_| corrupt("stored artifact row holds a negative counter"))
}

fn optional_non_negative(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> Result<Option<u64>, ArtifactError> {
    match row.get::<_, Option<i64>>(index).map_err(storage)? {
        None => Ok(None),
        Some(value) => u64::try_from(value)
            .map(Some)
            .map_err(|_| corrupt("stored artifact row holds a negative instant")),
    }
}

fn load(
    connection: &rusqlite::Connection,
    id: &ArtifactId,
) -> Result<Option<StoredArtifact>, ArtifactError> {
    let mut statement = connection
        .prepare_cached(&format!(
            "SELECT {COLUMNS} FROM artifacts WHERE artifact_id=?1"
        ))
        .map_err(storage)?;
    let mut rows = statement.query([id.as_str()]).map_err(storage)?;
    let Some(row) = rows.next().map_err(storage)? else {
        return Ok(None);
    };
    let stored = decode(row)?;
    if stored.handle.id() != id {
        return Err(corrupt("stored artifact does not address its own key"));
    }
    Ok(Some(stored))
}

fn record_usage(
    connection: &rusqlite::Connection,
    id: &ArtifactId,
    usage: ArtifactUsage,
) -> Result<(), ArtifactError> {
    connection
        .execute(
            "UPDATE artifacts SET reads=?2,materializations=?3,bytes_served=?4,\
             last_used_at_ms=?5 WHERE artifact_id=?1",
            rusqlite::params![
                id.as_str(),
                as_i64(usage.reads)?,
                as_i64(usage.materializations)?,
                as_i64(usage.bytes_served)?,
                usage.last_used_at_ms.map(as_i64).transpose()?,
            ],
        )
        .map_err(storage)?;
    Ok(())
}

/// Loads the content a handle names, or says exactly why it cannot.
fn load_intact(
    connection: &rusqlite::Connection,
    handle: &ArtifactHandle,
) -> Result<StoredArtifact, ArtifactError> {
    let Some(stored) = load(connection, handle.id())? else {
        return Err(ArtifactError::Missing(handle.id().clone()));
    };
    if !stored.is_intact() {
        return Err(corrupt(format!(
            "stored content for {} no longer addresses to its identity",
            handle.id()
        )));
    }
    Ok(stored)
}

impl ArtifactVault for LocalArtifactVault {
    fn put(
        &self,
        content: &[u8],
        write: &ArtifactWrite,
        now_ms: u64,
    ) -> Result<ArtifactReceipt, ArtifactError> {
        let handle = write.validate_for(content)?;
        let provenance = serde_json::to_string(write.provenance())
            .map_err(|error| ArtifactError::Storage(error.to_string()))?;
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO artifacts(artifact_id,digest,size,media_type,retention,\
                 retain_until_ms,provenance,written_at_ms,payload) \
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                rusqlite::params![
                    handle.id().as_str(),
                    handle.digest().as_str(),
                    content.len() as i64,
                    write.media_type().as_str(),
                    write.retention().as_str(),
                    write.retain_until_ms().map(as_i64).transpose()?,
                    provenance,
                    as_i64(now_ms)?,
                    content,
                ],
            )
            .map_err(storage)?;
        if inserted == 0 {
            // Content addressing makes a present row the same bytes by
            // construction; a row that disagrees is damage, not a collision to
            // resolve in favour of the newer writer.
            let present = load(&transaction, handle.id())?
                .ok_or_else(|| corrupt("an artifact vanished inside its own write"))?;
            if present.handle != handle {
                return Err(corrupt(
                    "stored content under this identity is not the addressed content",
                ));
            }
        }
        transaction.commit().map_err(storage)?;
        Ok(ArtifactReceipt {
            handle,
            outcome: if inserted == 0 {
                ArtifactPut::AlreadyPresent
            } else {
                ArtifactPut::Stored
            },
        })
    }

    fn inspect(&self, id: &ArtifactId) -> Result<Option<ArtifactRecord>, ArtifactError> {
        let connection = self.connection.lock().map_err(storage)?;
        Ok(load(&connection, id)?.map(StoredArtifact::into_record))
    }

    fn read(&self, handle: &ArtifactHandle, now_ms: u64) -> Result<Vec<u8>, ArtifactError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let stored = load_intact(&transaction, handle)?;
        let mut usage = stored.usage;
        usage.record_read(stored.size, now_ms);
        record_usage(&transaction, handle.id(), usage)?;
        transaction.commit().map_err(storage)?;
        Ok(stored.payload)
    }

    fn verify(&self, id: &ArtifactId) -> Result<ArtifactIntegrity, ArtifactError> {
        let connection = self.connection.lock().map_err(storage)?;
        Ok(match load(&connection, id)? {
            None => ArtifactIntegrity::Missing,
            Some(stored) if stored.is_intact() => ArtifactIntegrity::Intact,
            Some(_) => ArtifactIntegrity::Corrupt,
        })
    }

    fn recover(
        &self,
        id: &ArtifactId,
        content: &[u8],
        now_ms: u64,
    ) -> Result<ArtifactRecovery, ArtifactError> {
        if content.len() > MAX_ARTIFACT_BYTES {
            return Err(ArtifactError::Validation(format!(
                "artifact exceeds {MAX_ARTIFACT_BYTES} bytes"
            )));
        }
        if &ArtifactId::for_content(content) != id {
            return Err(ArtifactError::Validation(
                "recovery content does not address to the artifact identity".into(),
            ));
        }
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let Some(stored) = load(&transaction, id)? else {
            return Err(ArtifactError::Missing(id.clone()));
        };
        if stored.is_intact() {
            return Ok(ArtifactRecovery::AlreadyIntact);
        }
        transaction
            .execute(
                "UPDATE artifacts SET payload=?2,size=?3,recovered_at_ms=?4 WHERE artifact_id=?1",
                rusqlite::params![id.as_str(), content, content.len() as i64, as_i64(now_ms)?],
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        Ok(ArtifactRecovery::Restored)
    }

    fn materialize(
        &self,
        handle: &ArtifactHandle,
        destination: &Path,
        now_ms: u64,
    ) -> Result<ArtifactMaterialization, ArtifactError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let stored = load_intact(&transaction, handle)?;
        let materialization =
            materialize_verified_copy(&stored.payload, handle.digest(), destination)?;
        let mut usage = stored.usage;
        usage.record_materialization(stored.size, now_ms);
        record_usage(&transaction, handle.id(), usage)?;
        transaction.commit().map_err(storage)?;
        Ok(materialization)
    }
}

fn as_i64(value: u64) -> Result<i64, ArtifactError> {
    i64::try_from(value)
        .map_err(|_| ArtifactError::Validation("value exceeds the storable range".into()))
}

fn storage(error: impl std::fmt::Display) -> ArtifactError {
    ArtifactError::Storage(error.to_string())
}

fn corrupt(message: impl Into<String>) -> ArtifactError {
    ArtifactError::Corrupt(message.into())
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<(), ArtifactError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(storage)
}

#[cfg(not(unix))]
fn set_private_dir(_: &Path) -> Result<(), ArtifactError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<(), ArtifactError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(storage)
}

#[cfg(not(unix))]
fn set_private_file(_: &Path) -> Result<(), ArtifactError> {
    Ok(())
}
