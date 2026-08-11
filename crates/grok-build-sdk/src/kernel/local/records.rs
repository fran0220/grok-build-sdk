// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! What this backend writes down, and how it refuses to read back anything it
//! did not write.
//!
//! Every decode here is fail-closed: a receipt is verified against the digest
//! stored beside it and against the identity it claims, a state column and a
//! receipt column that disagree are a corruption rather than a guess, and an
//! out-of-range integer is refused instead of being clamped. A store that has
//! been edited underneath the authority therefore stops the authority, which
//! is the only answer that keeps a receipt worth trusting.

use super::super::{
    KernelError, KernelExecutionBounds, KernelExecutionId, KernelExecutionKey,
    KernelExecutionReceipt, KernelGeneration, KernelLabel, KernelSessionBounds, KernelSessionId,
    KernelSessionReceipt, KernelSpec, corrupt, validation,
};
use super::{KERNEL_RUNTIME_SCHEMA_MARKER, KERNEL_RUNTIME_SCHEMA_VERSION};
use crate::artifact::ArtifactDigest;
use crate::program::{ProgramLabel, ProgramPath};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Durable records
// ---------------------------------------------------------------------------

/// Everything durable about a spec that a receipt needs.
///
/// It holds the image's identity and its declared bounds, and deliberately not
/// the argument vector or the environment values: a receipt is answered from
/// `spec_digest`, so a Host's literal environment content never has to reach
/// this store to make one.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SpecRecord {
    pub(super) program: ProgramPath,
    pub(super) protocol: KernelLabel,
    pub(super) working_root: PathBuf,
    pub(super) spec_digest: ArtifactDigest,
    pub(super) bounds: KernelSessionBounds,
}

impl SpecRecord {
    pub(super) fn of(spec: &KernelSpec) -> Self {
        Self {
            program: spec.program().clone(),
            protocol: spec.protocol().clone(),
            working_root: spec.working_root().to_path_buf(),
            spec_digest: spec.spec_digest(),
            bounds: spec.bounds(),
        }
    }
}

/// Everything durable about one submission: its identity, not its source.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SubmissionRecord {
    pub(super) source_digest: ArtifactDigest,
    pub(super) spec_digest: ArtifactDigest,
    pub(super) bounds: KernelExecutionBounds,
}
// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

pub(super) struct StoredIncarnation {
    pub(super) generation: KernelGeneration,
    pub(super) owner: ProgramLabel,
    pub(super) pid: u32,
    pub(super) opened_at_ms: u64,
    pub(super) spec: SpecRecord,
    pub(super) executions: u64,
    pub(super) captured_bytes: u64,
    pub(super) receipt: Option<KernelSessionReceipt>,
}

pub(super) struct StoredExecution {
    pub(super) key: KernelExecutionKey,
    pub(super) sequence: u64,
    pub(super) owner: ProgramLabel,
    pub(super) started_at_ms: u64,
    pub(super) record: SubmissionRecord,
    pub(super) receipt: Option<KernelExecutionReceipt>,
}

const INCARNATION_COLUMNS: &str = "generation,state,owner,pid,opened_at_ms,spec,executions,\
     captured_bytes,receipt,receipt_digest";

pub(super) fn load_incarnation(
    connection: &rusqlite::Connection,
    session: &KernelSessionId,
    generation: Option<KernelGeneration>,
) -> Result<Option<StoredIncarnation>, KernelError> {
    let generation = match generation {
        Some(generation) => generation,
        None => {
            let current: Option<i64> = connection
                .query_row(
                    "SELECT current_generation FROM sessions WHERE session_id=?1",
                    [session.as_str()],
                    |row| row.get(0),
                )
                .map_or_else(
                    |error| match error {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        other => Err(storage(other)),
                    },
                    |value| Ok(Some(value)),
                )?;
            let Some(current) = current else {
                return Ok(None);
            };
            KernelGeneration::new(
                u64::try_from(current)
                    .map_err(|_| corrupt("a stored session names a negative generation"))?,
            )
        }
    };
    let mut statement = connection
        .prepare_cached(&format!(
            "SELECT {INCARNATION_COLUMNS} FROM incarnations WHERE session_id=?1 AND generation=?2"
        ))
        .map_err(storage)?;
    let mut rows = statement
        .query(rusqlite::params![
            session.as_str(),
            as_i64(generation.get())?
        ])
        .map_err(storage)?;
    let Some(row) = rows.next().map_err(storage)? else {
        return Ok(None);
    };
    Ok(Some(decode_incarnation(session, row)?))
}

fn decode_incarnation(
    session: &KernelSessionId,
    row: &rusqlite::Row<'_>,
) -> Result<StoredIncarnation, KernelError> {
    let generation = KernelGeneration::new(
        u64::try_from(row.get::<_, i64>(0).map_err(storage)?)
            .map_err(|_| corrupt("a stored incarnation names a negative generation"))?,
    );
    generation
        .validate()
        .map_err(|error| corrupt(error.to_string()))?;
    let settled = match row.get::<_, String>(1).map_err(storage)?.as_str() {
        "live" => false,
        "settled" => true,
        other => return Err(corrupt(format!("unknown incarnation state {other:?}"))),
    };
    let owner = ProgramLabel::new(row.get::<_, String>(2).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let pid = u32::try_from(row.get::<_, i64>(3).map_err(storage)?)
        .map_err(|_| corrupt("a stored incarnation holds a pid outside the OS range"))?;
    let opened_at_ms = u64::try_from(row.get::<_, i64>(4).map_err(storage)?)
        .map_err(|_| corrupt("a stored incarnation holds a negative instant"))?;
    let spec: SpecRecord = serde_json::from_str(&row.get::<_, String>(5).map_err(storage)?)
        .map_err(|error| corrupt(format!("a stored kernel spec is undecodable: {error}")))?;
    spec.bounds
        .validate()
        .map_err(|error| corrupt(error.to_string()))?;
    let executions = u64::try_from(row.get::<_, i64>(6).map_err(storage)?)
        .map_err(|_| corrupt("a stored incarnation counts negative executions"))?;
    let captured_bytes = u64::try_from(row.get::<_, i64>(7).map_err(storage)?)
        .map_err(|_| corrupt("a stored incarnation counts negative captured bytes"))?;
    let encoded: Option<String> = row.get(8).map_err(storage)?;
    let digest: Option<String> = row.get(9).map_err(storage)?;
    let receipt = match (settled, encoded, digest) {
        (false, None, None) => None,
        (true, Some(encoded), Some(digest)) => {
            let receipt: KernelSessionReceipt =
                serde_json::from_str(&encoded).map_err(|error| {
                    corrupt(format!("a stored session receipt is undecodable: {error}"))
                })?;
            let expected =
                ArtifactDigest::parse(digest).map_err(|error| corrupt(error.to_string()))?;
            receipt.verify(&expected)?;
            if receipt.session != *session || receipt.generation != generation {
                return Err(corrupt(
                    "a stored session receipt names another incarnation",
                ));
            }
            Some(receipt)
        }
        _ => {
            return Err(corrupt(
                "a stored incarnation's state and receipt disagree with each other",
            ));
        }
    };
    Ok(StoredIncarnation {
        generation,
        owner,
        pid,
        opened_at_ms,
        spec,
        executions,
        captured_bytes,
        receipt,
    })
}

const EXECUTION_COLUMNS: &str = "session_id,generation,execution_id,sequence,state,owner,started_at_ms,submission,receipt,\
     receipt_digest";

pub(super) fn load_execution(
    connection: &rusqlite::Connection,
    key: &KernelExecutionKey,
) -> Result<Option<StoredExecution>, KernelError> {
    let mut statement = connection
        .prepare_cached(&format!(
            "SELECT {EXECUTION_COLUMNS} FROM executions \
             WHERE session_id=?1 AND generation=?2 AND execution_id=?3"
        ))
        .map_err(storage)?;
    let mut rows = statement
        .query(rusqlite::params![
            key.session().as_str(),
            as_i64(key.generation().get())?,
            key.execution().as_str(),
        ])
        .map_err(storage)?;
    let Some(row) = rows.next().map_err(storage)? else {
        return Ok(None);
    };
    let stored = decode_execution(row)?;
    if stored.key != *key {
        return Err(corrupt("a stored execution does not address its own key"));
    }
    Ok(Some(stored))
}

pub(super) fn load_pending(
    connection: &rusqlite::Connection,
    session: &KernelSessionId,
    generation: KernelGeneration,
) -> Result<Vec<StoredExecution>, KernelError> {
    let mut statement = connection
        .prepare_cached(&format!(
            "SELECT {EXECUTION_COLUMNS} FROM executions \
             WHERE session_id=?1 AND generation=?2 AND state='in_flight' ORDER BY sequence"
        ))
        .map_err(storage)?;
    let mut rows = statement
        .query(rusqlite::params![
            session.as_str(),
            as_i64(generation.get())?
        ])
        .map_err(storage)?;
    let mut pending = Vec::new();
    while let Some(row) = rows.next().map_err(storage)? {
        pending.push(decode_execution(row)?);
    }
    Ok(pending)
}

pub(super) fn load_incarnation_receipts(
    connection: &rusqlite::Connection,
    session: &KernelSessionId,
    generation: KernelGeneration,
) -> Result<Vec<KernelExecutionReceipt>, KernelError> {
    let mut statement = connection
        .prepare_cached(&format!(
            "SELECT {EXECUTION_COLUMNS} FROM executions \
             WHERE session_id=?1 AND generation=?2 AND state='settled' ORDER BY sequence"
        ))
        .map_err(storage)?;
    let mut rows = statement
        .query(rusqlite::params![
            session.as_str(),
            as_i64(generation.get())?
        ])
        .map_err(storage)?;
    let mut receipts = Vec::new();
    while let Some(row) = rows.next().map_err(storage)? {
        if let Some(receipt) = decode_execution(row)?.receipt {
            receipts.push(receipt);
        }
    }
    Ok(receipts)
}

fn decode_execution(row: &rusqlite::Row<'_>) -> Result<StoredExecution, KernelError> {
    let session = KernelSessionId::new(row.get::<_, String>(0).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let generation = KernelGeneration::new(
        u64::try_from(row.get::<_, i64>(1).map_err(storage)?)
            .map_err(|_| corrupt("a stored execution names a negative generation"))?,
    );
    let execution = KernelExecutionId::new(row.get::<_, String>(2).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let key = KernelExecutionKey::new(session, generation, execution)
        .map_err(|error| corrupt(error.to_string()))?;
    let sequence = u64::try_from(row.get::<_, i64>(3).map_err(storage)?)
        .map_err(|_| corrupt("a stored execution holds a negative sequence"))?;
    let settled = match row.get::<_, String>(4).map_err(storage)?.as_str() {
        "in_flight" => false,
        "settled" => true,
        other => return Err(corrupt(format!("unknown execution state {other:?}"))),
    };
    let owner = ProgramLabel::new(row.get::<_, String>(5).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let started_at_ms = u64::try_from(row.get::<_, i64>(6).map_err(storage)?)
        .map_err(|_| corrupt("a stored execution holds a negative instant"))?;
    let record: SubmissionRecord = serde_json::from_str(&row.get::<_, String>(7).map_err(storage)?)
        .map_err(|error| corrupt(format!("a stored submission is undecodable: {error}")))?;
    record
        .bounds
        .validate()
        .map_err(|error| corrupt(error.to_string()))?;
    let encoded: Option<String> = row.get(8).map_err(storage)?;
    let digest: Option<String> = row.get(9).map_err(storage)?;
    let receipt = match (settled, encoded, digest) {
        (false, None, None) => None,
        (true, Some(encoded), Some(digest)) => {
            let receipt: KernelExecutionReceipt =
                serde_json::from_str(&encoded).map_err(|error| {
                    corrupt(format!("a stored kernel receipt is undecodable: {error}"))
                })?;
            let expected =
                ArtifactDigest::parse(digest).map_err(|error| corrupt(error.to_string()))?;
            receipt.verify(&expected)?;
            if receipt.key != key {
                return Err(corrupt("a stored kernel receipt names another execution"));
            }
            Some(receipt)
        }
        _ => {
            return Err(corrupt(
                "a stored execution's state and receipt disagree with each other",
            ));
        }
    };
    Ok(StoredExecution {
        key,
        sequence,
        owner,
        started_at_ms,
        record,
        receipt,
    })
}

pub(super) fn insert_incarnation(
    transaction: &rusqlite::Transaction<'_>,
    session: &KernelSessionId,
    generation: KernelGeneration,
    owner: &ProgramLabel,
    pid: u32,
    now_ms: u64,
    spec: &SpecRecord,
) -> Result<(), KernelError> {
    transaction
        .execute(
            "INSERT INTO incarnations(session_id,generation,state,owner,pid,opened_at_ms,\
             last_activity_ms,spec,executions,captured_bytes) \
             VALUES(?1,?2,'live',?3,?4,?5,?5,?6,0,0)",
            rusqlite::params![
                session.as_str(),
                as_i64(generation.get())?,
                owner.as_str(),
                i64::from(pid),
                as_i64(now_ms)?,
                serde_json::to_string(spec).map_err(storage)?,
            ],
        )
        .map_err(storage)?;
    Ok(())
}

pub(super) fn verify_schema(
    connection: &rusqlite::Connection,
    existed: bool,
) -> Result<(), KernelError> {
    let metadata: Vec<(String, String)> = connection
        .prepare("SELECT key,value FROM metadata ORDER BY key")
        .map_err(storage)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(storage)?
        .collect::<Result<_, _>>()
        .map_err(storage)?;
    if metadata.is_empty() {
        if existed {
            return Err(corrupt(
                "existing kernel runtime store has no schema metadata",
            ));
        }
        let transaction = connection.unchecked_transaction().map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO metadata(key,value) VALUES('schema_marker',?1)",
                [KERNEL_RUNTIME_SCHEMA_MARKER],
            )
            .map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO metadata(key,value) VALUES('schema_version',?1)",
                [KERNEL_RUNTIME_SCHEMA_VERSION.to_string()],
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        return Ok(());
    }
    if metadata
        != [
            ("schema_marker".into(), KERNEL_RUNTIME_SCHEMA_MARKER.into()),
            (
                "schema_version".into(),
                KERNEL_RUNTIME_SCHEMA_VERSION.to_string(),
            ),
        ]
    {
        return Err(corrupt("kernel runtime schema marker/version mismatch"));
    }
    Ok(())
}

pub(super) fn as_i64(value: u64) -> Result<i64, KernelError> {
    i64::try_from(value).map_err(|_| validation("value exceeds the storable range"))
}

pub(super) fn storage(error: impl std::fmt::Display) -> KernelError {
    KernelError::Storage(error.to_string())
}

/// Carries a program-layer refusal across the seam without changing what it
/// says: a Host that rejected a capture as invalid should not read as a store
/// that broke.
pub(super) fn from_program(error: crate::program::ProgramError) -> KernelError {
    match error {
        crate::program::ProgramError::Validation(message) => KernelError::Validation(message),
        crate::program::ProgramError::Corrupt(message) => KernelError::Corrupt(message),
        other => KernelError::Storage(other.to_string()),
    }
}

#[cfg(unix)]
pub(super) fn set_private_dir(path: &Path) -> Result<(), KernelError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(storage)
}

#[cfg(not(unix))]
pub(super) fn set_private_dir(_: &Path) -> Result<(), KernelError> {
    Ok(())
}

#[cfg(unix)]
pub(super) fn set_private_file(path: &Path) -> Result<(), KernelError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(storage)
}

#[cfg(not(unix))]
pub(super) fn set_private_file(_: &Path) -> Result<(), KernelError> {
    Ok(())
}
