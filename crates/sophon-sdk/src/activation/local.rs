// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    ActivationClaim, ActivationClaimRequest, ActivationCoordinator, ActivationDisposition,
    ActivationError, ActivationFencingToken, ActivationGrant, ActivationHandle, ActivationItemId,
    ActivationItemState, ActivationLeaseState, ActivationRenewal, ActivationSettlement,
    ActivationWake, ActivationWakeOutcome, ActivationWorkerId, MAX_ACTIVATION_LEASE_MS,
    MAX_ACTIVATION_PAYLOAD_BYTES,
};
use rusqlite::TransactionBehavior;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

pub const ACTIVATION_STORE_SCHEMA_MARKER: &str = "sophon-sdk.activation-coordinator";
pub const ACTIVATION_STORE_SCHEMA_VERSION: u32 = 1;

/// Local filesystem reference authority. Production Hosts inject their own
/// implementation gated by [`super::run_activation_coordinator_conformance`];
/// injection replaces this one and is never mirrored.
pub struct LocalActivationCoordinator {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}

impl LocalActivationCoordinator {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ActivationError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        set_private_dir(&root)?;
        let path = root.join("activation-coordinator.sqlite3");
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
                 CREATE TABLE IF NOT EXISTS work_items(
                   item_id TEXT PRIMARY KEY NOT NULL,
                   due_at_ms INTEGER,
                   payload BLOB NOT NULL,
                   payload_size INTEGER NOT NULL,
                   attempt INTEGER NOT NULL,
                   fencing_token INTEGER NOT NULL,
                   lease_worker TEXT,
                   lease_token INTEGER,
                   lease_expires_at_ms INTEGER,
                   settled_at_ms INTEGER,
                   settled_token INTEGER
                 );
                 CREATE INDEX IF NOT EXISTS work_items_due ON work_items(due_at_ms,item_id);",
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

fn verify_schema(connection: &rusqlite::Connection, existed: bool) -> Result<(), ActivationError> {
    let metadata: Vec<(String, String)> = connection
        .prepare("SELECT key,value FROM metadata ORDER BY key")
        .map_err(storage)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(storage)?
        .collect::<Result<_, _>>()
        .map_err(storage)?;
    if metadata.is_empty() {
        if existed {
            return Err(ActivationError::Corrupt(
                "existing activation store has no schema metadata".into(),
            ));
        }
        let transaction = connection.unchecked_transaction().map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO metadata(key,value) VALUES('schema_marker',?1)",
                [ACTIVATION_STORE_SCHEMA_MARKER],
            )
            .map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO metadata(key,value) VALUES('schema_version',?1)",
                [ACTIVATION_STORE_SCHEMA_VERSION.to_string()],
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        return Ok(());
    }
    if metadata
        != [
            (
                "schema_marker".into(),
                ACTIVATION_STORE_SCHEMA_MARKER.into(),
            ),
            (
                "schema_version".into(),
                ACTIVATION_STORE_SCHEMA_VERSION.to_string(),
            ),
        ]
    {
        return Err(ActivationError::Corrupt(
            "activation store schema marker/version mismatch".into(),
        ));
    }
    Ok(())
}

/// One decoded row, already checked against the published bounds.
struct StoredItem {
    item_id: ActivationItemId,
    due_at_ms: Option<u64>,
    payload: Vec<u8>,
    attempt: u64,
    fencing_token: u64,
    lease: Option<ActivationLeaseState>,
    settled_at_ms: Option<u64>,
    settled_token: Option<u64>,
}

impl StoredItem {
    fn into_state(self) -> ActivationItemState {
        ActivationItemState {
            item_id: self.item_id,
            due_at_ms: self.due_at_ms,
            payload: self.payload,
            attempt: self.attempt,
            fencing_token: ActivationFencingToken::new(self.fencing_token),
            lease: self.lease,
            settled_at_ms: self.settled_at_ms,
        }
    }

    fn held_by(&self, now_ms: u64) -> Option<&ActivationLeaseState> {
        self.lease
            .as_ref()
            .filter(|lease| lease.expires_at_ms > now_ms)
    }
}

const COLUMNS: &str = "item_id,due_at_ms,payload,payload_size,attempt,fencing_token,\
                       lease_worker,lease_token,lease_expires_at_ms,settled_at_ms,settled_token";

fn decode(row: &rusqlite::Row<'_>) -> Result<StoredItem, rusqlite::Error> {
    Ok(StoredItem {
        item_id: ActivationItemId(row.get::<_, String>(0)?),
        due_at_ms: row.get::<_, Option<i64>>(1)?.map(|value| value as u64),
        payload: row.get::<_, Vec<u8>>(2)?,
        attempt: row.get::<_, i64>(4)? as u64,
        fencing_token: row.get::<_, i64>(5)? as u64,
        lease: match (
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, Option<i64>>(8)?,
        ) {
            (Some(worker), Some(token), Some(expires)) => Some(ActivationLeaseState {
                worker_id: ActivationWorkerId(worker),
                token: ActivationFencingToken::new(token as u64),
                expires_at_ms: expires as u64,
            }),
            _ => None,
        },
        settled_at_ms: row.get::<_, Option<i64>>(9)?.map(|value| value as u64),
        settled_token: row.get::<_, Option<i64>>(10)?.map(|value| value as u64),
    })
}

/// Every stored scalar is re-checked before it is trusted: a row that a foreign
/// writer, a partial restore or a corrupt page could have produced must fail
/// the read rather than schedule work under invented identity or timing.
fn validate(row: &rusqlite::Row<'_>, item: &StoredItem) -> Result<(), ActivationError> {
    let declared: i64 = row.get(3).map_err(storage)?;
    let negative = row.get::<_, i64>(4).map_err(storage)? < 0
        || row.get::<_, i64>(5).map_err(storage)? < 0
        || row
            .get::<_, Option<i64>>(1)
            .map_err(storage)?
            .is_some_and(|value| value < 0)
        || row
            .get::<_, Option<i64>>(7)
            .map_err(storage)?
            .is_some_and(|value| value < 0)
        || row
            .get::<_, Option<i64>>(8)
            .map_err(storage)?
            .is_some_and(|value| value < 0)
        || row
            .get::<_, Option<i64>>(9)
            .map_err(storage)?
            .is_some_and(|value| value < 0)
        || row
            .get::<_, Option<i64>>(10)
            .map_err(storage)?
            .is_some_and(|value| value < 0);
    if negative {
        return Err(corrupt("stored activation row holds a negative counter"));
    }
    if declared < 0 || declared as usize != item.payload.len() {
        return Err(corrupt("stored payload does not match its declared size"));
    }
    if item.payload.len() > MAX_ACTIVATION_PAYLOAD_BYTES {
        return Err(corrupt("stored payload exceeds the published bound"));
    }
    ActivationItemId::new(item.item_id.as_str()).map_err(|error| corrupt(error.to_string()))?;
    if let Some(lease) = &item.lease {
        ActivationWorkerId::new(lease.worker_id.as_str())
            .map_err(|error| corrupt(error.to_string()))?;
        if lease.token.get() > item.fencing_token {
            return Err(corrupt("stored lease outruns the item's fencing token"));
        }
    }
    if item
        .settled_token
        .is_some_and(|token| token > item.fencing_token)
    {
        return Err(corrupt(
            "stored settlement outruns the item's fencing token",
        ));
    }
    if item.due_at_ms.is_some() && item.settled_at_ms.is_some() {
        return Err(corrupt("stored item is both due and settled"));
    }
    Ok(())
}

fn load(
    connection: &rusqlite::Connection,
    item_id: &ActivationItemId,
) -> Result<Option<StoredItem>, ActivationError> {
    let mut statement = connection
        .prepare_cached(&format!(
            "SELECT {COLUMNS} FROM work_items WHERE item_id=?1"
        ))
        .map_err(storage)?;
    let mut rows = statement.query([item_id.as_str()]).map_err(storage)?;
    let Some(row) = rows.next().map_err(storage)? else {
        return Ok(None);
    };
    let item = decode(row).map_err(storage)?;
    validate(row, &item)?;
    Ok(Some(item))
}

fn grant(
    transaction: &rusqlite::Transaction<'_>,
    item: &StoredItem,
    request: &ActivationClaimRequest,
) -> Result<ActivationGrant, ActivationError> {
    let token = item
        .fencing_token
        .checked_add(1)
        .ok_or_else(|| corrupt("fencing token overflow"))?;
    let attempt = item
        .attempt
        .checked_add(1)
        .ok_or_else(|| corrupt("attempt counter overflow"))?;
    let expires_at_ms = request.expires_at_ms();
    let due_at_ms = item
        .due_at_ms
        .ok_or_else(|| corrupt("granting a work item with no due time"))?;
    transaction
        .execute(
            "UPDATE work_items SET attempt=?2,fencing_token=?3,lease_worker=?4,lease_token=?3,\
             lease_expires_at_ms=?5,settled_at_ms=NULL WHERE item_id=?1",
            rusqlite::params![
                item.item_id.as_str(),
                as_i64(attempt)?,
                as_i64(token)?,
                request.worker_id().as_str(),
                as_i64(expires_at_ms)?,
            ],
        )
        .map_err(storage)?;
    Ok(ActivationGrant::new(
        item.item_id.clone(),
        request.worker_id().clone(),
        ActivationFencingToken::new(token),
        due_at_ms,
        expires_at_ms,
        attempt,
        item.payload.clone(),
    ))
}

impl ActivationCoordinator for LocalActivationCoordinator {
    fn wake(
        &self,
        request: &ActivationWake,
        now_ms: u64,
    ) -> Result<ActivationWakeOutcome, ActivationError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let outcome = match load(&transaction, request.item_id())? {
            None => {
                transaction
                    .execute(
                        "INSERT INTO work_items(item_id,due_at_ms,payload,payload_size,attempt,\
                         fencing_token) VALUES(?1,?2,?3,?4,0,0)",
                        rusqlite::params![
                            request.item_id().as_str(),
                            as_i64(request.due_at_ms())?,
                            request.payload(),
                            request.payload().len() as i64,
                        ],
                    )
                    .map_err(storage)?;
                ActivationWakeOutcome::Registered
            }
            Some(item) if item.held_by(now_ms).is_some() => ActivationWakeOutcome::Held,
            Some(item)
                if item.due_at_ms == Some(request.due_at_ms())
                    && item.payload == request.payload() =>
            {
                ActivationWakeOutcome::Unchanged
            }
            Some(_) => {
                transaction
                    .execute(
                        "UPDATE work_items SET due_at_ms=?2,payload=?3,payload_size=?4,\
                         settled_at_ms=NULL WHERE item_id=?1",
                        rusqlite::params![
                            request.item_id().as_str(),
                            as_i64(request.due_at_ms())?,
                            request.payload(),
                            request.payload().len() as i64,
                        ],
                    )
                    .map_err(storage)?;
                ActivationWakeOutcome::Rescheduled
            }
        };
        transaction.commit().map_err(storage)?;
        Ok(outcome)
    }

    fn claim_due(
        &self,
        request: &ActivationClaimRequest,
    ) -> Result<Vec<ActivationGrant>, ActivationError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let now = as_i64(request.now_ms())?;
        let candidates = {
            let mut statement = transaction
                .prepare_cached(&format!(
                    "SELECT {COLUMNS} FROM work_items WHERE due_at_ms IS NOT NULL AND due_at_ms<=?1 \
                     AND (lease_expires_at_ms IS NULL OR lease_expires_at_ms<=?1) \
                     ORDER BY due_at_ms ASC,item_id ASC LIMIT ?2"
                ))
                .map_err(storage)?;
            let mut rows = statement
                .query(rusqlite::params![now, request.limit() as i64])
                .map_err(storage)?;
            let mut candidates = Vec::new();
            while let Some(row) = rows.next().map_err(storage)? {
                let item = decode(row).map_err(storage)?;
                validate(row, &item)?;
                candidates.push(item);
            }
            candidates
        };
        let mut grants = Vec::with_capacity(candidates.len());
        for item in &candidates {
            grants.push(grant(&transaction, item, request)?);
        }
        transaction.commit().map_err(storage)?;
        Ok(grants)
    }

    fn claim_item(
        &self,
        item_id: &ActivationItemId,
        request: &ActivationClaimRequest,
    ) -> Result<ActivationClaim, ActivationError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let Some(item) = load(&transaction, item_id)? else {
            return Ok(ActivationClaim::Unknown);
        };
        let claim = if let Some(lease) = item.held_by(request.now_ms()) {
            ActivationClaim::Held {
                worker_id: lease.worker_id.clone(),
                expires_at_ms: lease.expires_at_ms,
            }
        } else {
            match item.due_at_ms {
                None => ActivationClaim::Settled,
                Some(due) if due > request.now_ms() => ActivationClaim::NotDue { due_at_ms: due },
                Some(_) => ActivationClaim::Granted(grant(&transaction, &item, request)?),
            }
        };
        transaction.commit().map_err(storage)?;
        Ok(claim)
    }

    fn renew(
        &self,
        handle: &ActivationHandle,
        lease_ms: u64,
        now_ms: u64,
    ) -> Result<ActivationRenewal, ActivationError> {
        if lease_ms == 0 || lease_ms > MAX_ACTIVATION_LEASE_MS {
            return Err(ActivationError::Validation(format!(
                "activation lease must be between 1ms and {MAX_ACTIVATION_LEASE_MS}ms"
            )));
        }
        let expires_at_ms = now_ms.checked_add(lease_ms).ok_or_else(|| {
            ActivationError::Validation("lease expiry overflows the millisecond clock".into())
        })?;
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let renewal = match load(&transaction, handle.item_id())? {
            Some(item)
                if item.held_by(now_ms).is_some_and(|lease| {
                    lease.worker_id == *handle.worker_id() && lease.token == handle.token()
                }) =>
            {
                transaction
                    .execute(
                        "UPDATE work_items SET lease_expires_at_ms=?2 WHERE item_id=?1",
                        rusqlite::params![handle.item_id().as_str(), as_i64(expires_at_ms)?],
                    )
                    .map_err(storage)?;
                ActivationRenewal::Renewed { expires_at_ms }
            }
            _ => ActivationRenewal::Fenced,
        };
        transaction.commit().map_err(storage)?;
        Ok(renewal)
    }

    fn release(
        &self,
        handle: &ActivationHandle,
        disposition: ActivationDisposition,
        now_ms: u64,
    ) -> Result<ActivationSettlement, ActivationError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let Some(item) = load(&transaction, handle.item_id())? else {
            return Ok(ActivationSettlement::Fenced);
        };
        // The idempotency answer is checked before the lease, because a retried
        // release arrives after its own lease is already gone.
        if item.settled_token == Some(handle.token().get()) {
            return Ok(ActivationSettlement::AlreadySettled);
        }
        let held = item.held_by(now_ms).is_some_and(|lease| {
            lease.worker_id == *handle.worker_id() && lease.token == handle.token()
        });
        if !held {
            return Ok(ActivationSettlement::Fenced);
        }
        let (due_at_ms, settled_at_ms) = match disposition {
            ActivationDisposition::Complete => (None, Some(now_ms)),
            ActivationDisposition::Yield { due_at_ms } => (Some(due_at_ms), None),
        };
        transaction
            .execute(
                "UPDATE work_items SET due_at_ms=?2,settled_at_ms=?3,settled_token=?4,\
                 lease_worker=NULL,lease_token=NULL,lease_expires_at_ms=NULL WHERE item_id=?1",
                rusqlite::params![
                    handle.item_id().as_str(),
                    due_at_ms.map(as_i64).transpose()?,
                    settled_at_ms.map(as_i64).transpose()?,
                    as_i64(handle.token().get())?,
                ],
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        Ok(ActivationSettlement::Settled)
    }

    fn inspect(
        &self,
        item_id: &ActivationItemId,
    ) -> Result<Option<ActivationItemState>, ActivationError> {
        let connection = self.connection.lock().map_err(storage)?;
        Ok(load(&connection, item_id)?.map(StoredItem::into_state))
    }

    fn purge_settled(&self, settled_before_ms: u64) -> Result<usize, ActivationError> {
        let connection = self.connection.lock().map_err(storage)?;
        let removed = connection
            .execute(
                "DELETE FROM work_items WHERE settled_at_ms IS NOT NULL AND settled_at_ms<=?1 \
                 AND due_at_ms IS NULL AND lease_worker IS NULL",
                [as_i64(settled_before_ms)?],
            )
            .map_err(storage)?;
        Ok(removed)
    }
}

fn as_i64(value: u64) -> Result<i64, ActivationError> {
    i64::try_from(value).map_err(|_| {
        ActivationError::Validation("millisecond value exceeds the storable range".into())
    })
}

fn storage(error: impl std::fmt::Display) -> ActivationError {
    ActivationError::Storage(error.to_string())
}

fn corrupt(message: impl Into<String>) -> ActivationError {
    ActivationError::Corrupt(message.into())
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<(), ActivationError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(storage)
}

#[cfg(not(unix))]
fn set_private_dir(_: &Path) -> Result<(), ActivationError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<(), ActivationError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(storage)
}

#[cfg(not(unix))]
fn set_private_file(_: &Path) -> Result<(), ActivationError> {
    Ok(())
}
