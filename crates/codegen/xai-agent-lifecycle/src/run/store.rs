use super::model::{
    IterationManifest, MAX_RUN_ENVELOPE_BYTES, RUN_SCHEMA_VERSION, RunEnvelope, RunError, RunId,
    RunRevision, RunStatus,
};
use rusqlite::OptionalExtension as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Stable namespace for the current-only durable Run schema. Host stores must
/// persist this marker together with [`RUN_SCHEMA_VERSION`] and reject any
/// mismatch before loading or committing Run data.
pub const RUN_SCHEMA_MARKER: &str = "xai-agent-lifecycle.run-envelope";

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunSchemaMarker {
    pub namespace: &'static str,
    pub version: u32,
    pub max_envelope_bytes: usize,
}

pub const CURRENT_RUN_SCHEMA: RunSchemaMarker = RunSchemaMarker {
    namespace: RUN_SCHEMA_MARKER,
    version: RUN_SCHEMA_VERSION,
    max_envelope_bytes: MAX_RUN_ENVELOPE_BYTES,
};

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct StoreCommit {
    pub run_id: RunId,
    pub expected_revision: Option<RunRevision>,
    pub next: RunEnvelope,
    pub finished_iteration: Option<IterationManifest>,
}

/// SDK-validated and bounded representation of one atomic store commit.
///
/// Host implementations should call [`StoreCommit::validate_and_encode`] as
/// the first step of `RunStore::commit`, then use only this value when binding
/// transaction parameters. This preserves the exact validation/error ordering
/// used by [`LocalRunStore`] without a lossy JSON validation round-trip.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PreparedStoreCommit {
    pub schema: RunSchemaMarker,
    pub run_id: RunId,
    pub expected_revision: Option<RunRevision>,
    pub next_revision: RunRevision,
    pub tombstoned: bool,
    pub envelope: Vec<u8>,
    pub finished_iteration: Option<PreparedIteration>,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PreparedIteration {
    pub iteration_id: u64,
    pub manifest: Vec<u8>,
}

impl StoreCommit {
    /// Applies the SDK-owned validator and produces bounded canonical storage
    /// payloads. Validation order is part of the public RunStore contract.
    pub fn validate_and_encode(&self) -> Result<PreparedStoreCommit, RunError> {
        if self.next.run.id != self.run_id {
            return Err(RunError::Integrity(
                "commit Run id differs from envelope Run id".into(),
            ));
        }
        self.next.validate()?;
        let expected_next = self
            .expected_revision
            .map(|revision| {
                revision
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| RunError::Integrity("Run revision overflow".into()))
            })
            .transpose()?
            .unwrap_or(1);
        if self.next.run.revision.get() != expected_next {
            return Err(RunError::Integrity(
                "commit revision is not the expected successor".into(),
            ));
        }
        let envelope =
            serde_json::to_vec(&self.next).map_err(|error| RunError::Storage(error.to_string()))?;
        if envelope.len() > MAX_RUN_ENVELOPE_BYTES {
            return Err(RunError::Validation(
                "serialized Run envelope exceeds 16 MiB".into(),
            ));
        }
        let finished_iteration = self
            .finished_iteration
            .as_ref()
            .map(|iteration| {
                iteration.validate()?;
                if iteration.finished_at_ms.is_none()
                    || !self
                        .next
                        .run
                        .iterations
                        .iter()
                        .any(|stored| stored == iteration)
                {
                    return Err(RunError::Integrity(
                        "finished iteration is not the exact committed Run iteration".into(),
                    ));
                }
                let manifest = serde_json::to_vec(iteration)
                    .map_err(|error| RunError::Storage(error.to_string()))?;
                if manifest.len() > MAX_RUN_ENVELOPE_BYTES {
                    return Err(RunError::Validation(
                        "serialized iteration manifest exceeds 16 MiB".into(),
                    ));
                }
                Ok(PreparedIteration {
                    iteration_id: iteration.iteration_id.get(),
                    manifest,
                })
            })
            .transpose()?;
        Ok(PreparedStoreCommit {
            schema: CURRENT_RUN_SCHEMA,
            run_id: self.run_id.clone(),
            expected_revision: self.expected_revision,
            next_revision: self.next.run.revision,
            tombstoned: self.next.run.status == RunStatus::Tombstoned,
            envelope,
            finished_iteration,
        })
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreCommitResult {
    Applied,
    Conflict { actual: Option<RunRevision> },
    Tombstoned,
    CommitUnknown(String),
}

/// Acknowledged Run persistence.
///
/// A successful `commit` atomically advances the snapshot, event journal,
/// command receipt table and operation outbox. `CommitUnknown` is deliberately
/// not reported as a retryable error: callers must reload and reconcile before
/// attempting another mutation. Implementations must persist and verify
/// [`CURRENT_RUN_SCHEMA`] and must begin `commit` with
/// [`StoreCommit::validate_and_encode`].
pub trait RunStore: Send + Sync + 'static {
    fn load(&self, run_id: &RunId) -> Result<Option<RunEnvelope>, RunError>;
    fn list(&self) -> Result<Vec<RunEnvelope>, RunError>;
    fn commit(&self, commit: StoreCommit) -> Result<StoreCommitResult, RunError>;
}

impl<T> RunStore for Arc<T>
where
    T: RunStore + ?Sized,
{
    fn load(&self, run_id: &RunId) -> Result<Option<RunEnvelope>, RunError> {
        (**self).load(run_id)
    }

    fn list(&self) -> Result<Vec<RunEnvelope>, RunError> {
        (**self).list()
    }

    fn commit(&self, commit: StoreCommit) -> Result<StoreCommitResult, RunError> {
        (**self).commit(commit)
    }
}

#[derive(Clone)]
pub struct LocalRunStore {
    path: PathBuf,
    connection: Arc<Mutex<rusqlite::Connection>>,
}

impl LocalRunStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, RunError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(io_error)?;
        set_private_dir(&root)?;
        let path = root.join("runs.sqlite3");
        let existed = path.exists();
        let connection = rusqlite::Connection::open(&path).map_err(sql_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sql_error)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 PRAGMA foreign_keys=ON;
                 CREATE TABLE IF NOT EXISTS metadata(
                   key TEXT PRIMARY KEY,
                   value INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS runs(
                   run_id TEXT PRIMARY KEY,
                   revision INTEGER NOT NULL,
                   tombstoned INTEGER NOT NULL,
                   envelope BLOB NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS iterations(
                   run_id TEXT NOT NULL,
                   iteration_id INTEGER NOT NULL,
                   manifest BLOB NOT NULL,
                   PRIMARY KEY(run_id, iteration_id),
                   FOREIGN KEY(run_id) REFERENCES runs(run_id)
                 );",
            )
            .map_err(sql_error)?;
        let metadata_count: u64 = connection
            .query_row("SELECT COUNT(*) FROM metadata", [], |row| row.get(0))
            .map_err(sql_error)?;
        if metadata_count == 0 {
            if existed {
                return Err(RunError::Integrity(
                    "existing Run store has no schema metadata".into(),
                ));
            }
            let transaction = connection.unchecked_transaction().map_err(sql_error)?;
            transaction
                .execute(
                    "INSERT INTO metadata(key,value) VALUES('schema_version',?1)",
                    [RUN_SCHEMA_VERSION],
                )
                .map_err(sql_error)?;
            transaction
                .execute(
                    "INSERT INTO metadata(key,value) VALUES('schema_marker',?1)",
                    [RUN_SCHEMA_MARKER],
                )
                .map_err(sql_error)?;
            transaction.commit().map_err(sql_error)?;
        } else {
            let version: Option<u32> = connection
                .query_row(
                    "SELECT value FROM metadata WHERE key='schema_version'",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            let marker: Option<String> = connection
                .query_row(
                    "SELECT value FROM metadata WHERE key='schema_marker'",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if version.is_some_and(|version| version != RUN_SCHEMA_VERSION) {
                return Err(RunError::UnsupportedSchema(version.unwrap()));
            }
            if version != Some(RUN_SCHEMA_VERSION)
                || marker.as_deref() != Some(RUN_SCHEMA_MARKER)
                || metadata_count != 2
            {
                return Err(RunError::Integrity(
                    "Run store schema marker/version metadata is incomplete".into(),
                ));
            }
        }
        if metadata_count > 0 && metadata_count != 2 {
            return Err(RunError::Integrity(
                "Run store schema marker/version metadata is incomplete".into(),
            ));
        }
        set_private_file(&path)?;
        Ok(Self {
            path,
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl RunStore for LocalRunStore {
    fn load(&self, run_id: &RunId) -> Result<Option<RunEnvelope>, RunError> {
        let connection = self.lock()?;
        let result = connection.query_row(
            "SELECT envelope FROM runs WHERE run_id=?1",
            [run_id.as_str()],
            |row| row.get::<_, Vec<u8>>(0),
        );
        match result {
            Ok(bytes) => {
                let envelope = decode_envelope(&bytes)?;
                if envelope.run.id != *run_id {
                    return Err(RunError::Integrity(
                        "Run store returned an envelope for a different Run id".into(),
                    ));
                }
                Ok(Some(envelope))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(sql_error(error)),
        }
    }

    fn list(&self) -> Result<Vec<RunEnvelope>, RunError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare("SELECT envelope FROM runs ORDER BY run_id")
            .map_err(sql_error)?;
        statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .map_err(sql_error)?
            .map(|row| {
                row.map_err(sql_error)
                    .and_then(|bytes| decode_envelope(&bytes))
            })
            .collect()
    }

    fn commit(&self, commit: StoreCommit) -> Result<StoreCommitResult, RunError> {
        let prepared = commit.validate_and_encode()?;

        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = transaction.query_row(
            "SELECT revision,tombstoned FROM runs WHERE run_id=?1",
            [commit.run_id.as_str()],
            |row| {
                Ok((
                    RunRevision::new(row.get::<_, u64>(0)?),
                    row.get::<_, bool>(1)?,
                ))
            },
        );
        match current {
            Ok((_revision, true)) => return Ok(StoreCommitResult::Tombstoned),
            Ok((revision, false)) if commit.expected_revision == Some(revision) => {}
            Ok((revision, false)) => {
                return Ok(StoreCommitResult::Conflict {
                    actual: Some(revision),
                });
            }
            Err(rusqlite::Error::QueryReturnedNoRows) if commit.expected_revision.is_none() => {}
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Ok(StoreCommitResult::Conflict { actual: None });
            }
            Err(error) => return Err(sql_error(error)),
        }

        // Creation of a root Run is serialized by the IMMEDIATE transaction.
        // This makes the one-nonterminal-Run-per-Session invariant durable,
        // rather than merely a controller cache check.
        if commit.expected_revision.is_none() && !commit.next.run.status.is_terminal() {
            let mut statement = transaction
                .prepare("SELECT envelope FROM runs WHERE tombstoned=0")
                .map_err(sql_error)?;
            let rows = statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))
                .map_err(sql_error)?;
            for row in rows {
                let existing = decode_envelope(&row.map_err(sql_error)?)?;
                if existing.run.session == commit.next.run.session
                    && !existing.run.status.is_terminal()
                {
                    return Ok(StoreCommitResult::Conflict { actual: None });
                }
            }
        }

        transaction
            .execute(
                "INSERT INTO runs(run_id,revision,tombstoned,envelope)
                 VALUES(?1,?2,?3,?4)
                 ON CONFLICT(run_id) DO UPDATE SET
                   revision=excluded.revision,
                   tombstoned=excluded.tombstoned,
                   envelope=excluded.envelope",
                rusqlite::params![
                    prepared.run_id.as_str(),
                    prepared.next_revision.get(),
                    prepared.tombstoned,
                    prepared.envelope,
                ],
            )
            .map_err(sql_error)?;
        if let Some(iteration) = prepared.finished_iteration {
            transaction
                .execute(
                    "INSERT INTO iterations(run_id,iteration_id,manifest)
                     VALUES(?1,?2,?3)
                     ON CONFLICT(run_id,iteration_id) DO UPDATE SET manifest=excluded.manifest",
                    rusqlite::params![
                        commit.run_id.as_str(),
                        iteration.iteration_id,
                        iteration.manifest
                    ],
                )
                .map_err(sql_error)?;
        }
        match transaction.commit() {
            Ok(()) => Ok(StoreCommitResult::Applied),
            Err(error) => Ok(StoreCommitResult::CommitUnknown(error.to_string())),
        }
    }
}

impl LocalRunStore {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, rusqlite::Connection>, RunError> {
        self.connection
            .lock()
            .map_err(|_| RunError::Storage("Run store lock is poisoned".into()))
    }
}

fn decode_envelope(bytes: &[u8]) -> Result<RunEnvelope, RunError> {
    RunEnvelope::from_json_slice(bytes)
}

fn sql_error(error: rusqlite::Error) -> RunError {
    RunError::Storage(error.to_string())
}

fn io_error(error: std::io::Error) -> RunError {
    RunError::Storage(error.to_string())
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<(), RunError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(io_error)
}

#[cfg(not(unix))]
fn set_private_dir(_: &Path) -> Result<(), RunError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<(), RunError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(io_error)
}

#[cfg(not(unix))]
fn set_private_file(_: &Path) -> Result<(), RunError> {
    Ok(())
}
