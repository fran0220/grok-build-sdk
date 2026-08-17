// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    EvolutionError, EvolutionState, HarnessScope, MAX_EVOLUTION_STATE_BYTES,
    MAX_REFINEMENT_EVENT_BYTES, RefinementEvent,
};
use rusqlite::OptionalExtension as _;
use std::{path::PathBuf, sync::Mutex};

pub const EVOLUTION_STORE_SCHEMA_MARKER: &str = "sophon-sdk.harness-evolution-store";
pub const EVOLUTION_STORE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum EvolutionStoreError {
    #[error("evolution state validation failed: {0}")]
    Validation(String),
    #[error("stored evolution state is corrupt: {0}")]
    Corrupt(String),
    #[error("evolution storage failed: {0}")]
    Storage(String),
}

impl From<EvolutionError> for EvolutionStoreError {
    fn from(error: EvolutionError) -> Self {
        Self::Validation(error.to_string())
    }
}

/// Outcome of committing one refinement transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvolutionCommit {
    Committed,
    /// The stored revision is not the event's baseline. Nothing was written;
    /// the caller reloads, replans against the fresh state, and retries.
    RevisionConflict {
        stored_revision: Option<u64>,
    },
    /// The authority may or may not have committed. Callers reconcile by
    /// reloading the scope; they never treat this as a failure to commit.
    CommitUnknown,
}

/// Durable authority for scoped evolution ledgers.
///
/// A commit is one atomic transition: it appends exactly one refinement event
/// and replaces the scope state with the event's resulting revision, guarded
/// by compare-and-swap on the stored revision. Event history is append-only;
/// there is deliberately no operation that rewrites or deletes a committed
/// event.
pub trait EvolutionStore: Send + Sync + 'static {
    fn load(&self, scope: &HarnessScope) -> Result<Option<EvolutionState>, EvolutionStoreError>;

    /// Commits `state` and its producing `event` iff the stored revision still
    /// equals `event.based_on_revision()` (absent counts as revision 0).
    fn commit(
        &self,
        state: &EvolutionState,
        event: &RefinementEvent,
    ) -> Result<EvolutionCommit, EvolutionStoreError>;

    /// Returns up to `limit` events for one scope, newest first.
    fn history(
        &self,
        scope: &HarnessScope,
        limit: usize,
    ) -> Result<Vec<RefinementEvent>, EvolutionStoreError>;

    fn event(
        &self,
        scope: &HarnessScope,
        event_id: &str,
    ) -> Result<Option<RefinementEvent>, EvolutionStoreError>;
}

/// Decides whether a commit outcome is settled. An unknown commit is settled
/// only when the exact intended state is readable at its resulting revision.
pub fn evolution_commit_reconciled(
    result: &EvolutionCommit,
    loaded: Option<&EvolutionState>,
    intended: &EvolutionState,
) -> bool {
    matches!(result, EvolutionCommit::Committed)
        || matches!(result, EvolutionCommit::CommitUnknown) && loaded == Some(intended)
}

fn check_transition(
    state: &EvolutionState,
    event: &RefinementEvent,
) -> Result<(), EvolutionStoreError> {
    state.validate()?;
    if state.scope() != event.scope() {
        return Err(EvolutionStoreError::Validation(
            "a commit's state and event address different scopes".into(),
        ));
    }
    if state.revision() != event.resulting_revision() {
        return Err(EvolutionStoreError::Validation(
            "a commit's state revision is not the event's resulting revision".into(),
        ));
    }
    Ok(())
}

/// Local filesystem reference authority. Production Hosts inject their own
/// implementation; injection replaces this one and is never mirrored.
pub struct LocalEvolutionStore {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}

impl LocalEvolutionStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, EvolutionStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        let path = root.join("harness-evolution.sqlite3");
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
                 CREATE TABLE IF NOT EXISTS states(
                   scope_key TEXT PRIMARY KEY NOT NULL,
                   revision INTEGER NOT NULL,
                   payload BLOB NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS events(
                   scope_key TEXT NOT NULL,
                   resulting_revision INTEGER NOT NULL,
                   event_id TEXT NOT NULL,
                   payload BLOB NOT NULL,
                   PRIMARY KEY(scope_key,resulting_revision),
                   UNIQUE(scope_key,event_id)
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
                return Err(EvolutionStoreError::Corrupt(
                    "existing evolution store has no schema metadata".into(),
                ));
            }
            let transaction = connection.unchecked_transaction().map_err(storage)?;
            transaction
                .execute(
                    "INSERT INTO metadata(key,value) VALUES('schema_marker',?1)",
                    [EVOLUTION_STORE_SCHEMA_MARKER],
                )
                .map_err(storage)?;
            transaction
                .execute(
                    "INSERT INTO metadata(key,value) VALUES('schema_version',?1)",
                    [EVOLUTION_STORE_SCHEMA_VERSION.to_string()],
                )
                .map_err(storage)?;
            transaction.commit().map_err(storage)?;
        } else if metadata
            != [
                ("schema_marker".into(), EVOLUTION_STORE_SCHEMA_MARKER.into()),
                (
                    "schema_version".into(),
                    EVOLUTION_STORE_SCHEMA_VERSION.to_string(),
                ),
            ]
        {
            return Err(EvolutionStoreError::Corrupt(
                "evolution store schema marker/version mismatch".into(),
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

impl EvolutionStore for LocalEvolutionStore {
    fn load(&self, scope: &HarnessScope) -> Result<Option<EvolutionState>, EvolutionStoreError> {
        let connection = self.connection.lock().map_err(storage)?;
        load_state(&connection, scope)
    }

    fn commit(
        &self,
        state: &EvolutionState,
        event: &RefinementEvent,
    ) -> Result<EvolutionCommit, EvolutionStoreError> {
        check_transition(state, event)?;
        let state_bytes = state.to_json_vec()?;
        let event_bytes = event.to_json_vec()?;
        let scope_key = state.scope().to_string();
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        let stored_revision: Option<u64> = transaction
            .query_row(
                "SELECT revision FROM states WHERE scope_key=?1",
                [scope_key.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        if stored_revision.unwrap_or(0) != event.based_on_revision() {
            return Ok(EvolutionCommit::RevisionConflict { stored_revision });
        }
        transaction
            .execute(
                "INSERT INTO events(scope_key,resulting_revision,event_id,payload) \
                 VALUES(?1,?2,?3,?4)",
                rusqlite::params![
                    scope_key,
                    event.resulting_revision(),
                    event.event_id(),
                    event_bytes
                ],
            )
            .map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO states(scope_key,revision,payload) VALUES(?1,?2,?3) \
                 ON CONFLICT(scope_key) DO UPDATE SET revision=?2,payload=?3",
                rusqlite::params![scope_key, state.revision(), state_bytes],
            )
            .map_err(storage)?;
        match transaction.commit() {
            Ok(()) => Ok(EvolutionCommit::Committed),
            Err(_) => Ok(EvolutionCommit::CommitUnknown),
        }
    }

    fn history(
        &self,
        scope: &HarnessScope,
        limit: usize,
    ) -> Result<Vec<RefinementEvent>, EvolutionStoreError> {
        let connection = self.connection.lock().map_err(storage)?;
        let mut statement = connection
            .prepare(
                "SELECT payload FROM events WHERE scope_key=?1 \
                 ORDER BY resulting_revision DESC LIMIT ?2",
            )
            .map_err(storage)?;
        let payloads: Vec<Vec<u8>> = statement
            .query_map(rusqlite::params![scope.to_string(), limit as u64], |row| {
                row.get(0)
            })
            .map_err(storage)?
            .collect::<Result<_, _>>()
            .map_err(storage)?;
        payloads.iter().map(|bytes| decode_event(bytes)).collect()
    }

    fn event(
        &self,
        scope: &HarnessScope,
        event_id: &str,
    ) -> Result<Option<RefinementEvent>, EvolutionStoreError> {
        let connection = self.connection.lock().map_err(storage)?;
        let payload: Option<Vec<u8>> = connection
            .query_row(
                "SELECT payload FROM events WHERE scope_key=?1 AND event_id=?2",
                rusqlite::params![scope.to_string(), event_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        payload.as_deref().map(decode_event).transpose()
    }
}

fn load_state(
    connection: &rusqlite::Connection,
    scope: &HarnessScope,
) -> Result<Option<EvolutionState>, EvolutionStoreError> {
    let row: Option<(u64, Vec<u8>)> = connection
        .query_row(
            "SELECT revision,payload FROM states WHERE scope_key=?1",
            [scope.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    let Some((revision, bytes)) = row else {
        return Ok(None);
    };
    if bytes.len() > MAX_EVOLUTION_STATE_BYTES {
        return Err(corrupt("stored evolution state exceeds its size bound"));
    }
    let state =
        EvolutionState::from_json_slice(&bytes).map_err(|error| corrupt(error.to_string()))?;
    if state.scope() != scope || state.revision() != revision {
        return Err(corrupt(
            "stored evolution state does not address its own scope and revision",
        ));
    }
    Ok(Some(state))
}

fn decode_event(bytes: &[u8]) -> Result<RefinementEvent, EvolutionStoreError> {
    if bytes.len() > MAX_REFINEMENT_EVENT_BYTES {
        return Err(corrupt("stored refinement event exceeds its size bound"));
    }
    RefinementEvent::from_json_slice(bytes).map_err(|error| corrupt(error.to_string()))
}

fn storage(error: impl std::fmt::Display) -> EvolutionStoreError {
    EvolutionStoreError::Storage(error.to_string())
}

fn corrupt(message: impl Into<String>) -> EvolutionStoreError {
    EvolutionStoreError::Corrupt(message.into())
}
