// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    CaptureRecord, CredentialResolver, EnvironmentBinding, ExecutionId, ExecutionReceipt,
    ExecutionStatus, ExitDisposition, Liveness, LivenessProbe, ProcessIdentity, ProgramBounds,
    ProgramError, ProgramLaunch, ProgramOutputSink, ProgramPath, ProgramRuntime, ProgramStream,
    ReconcileOutcome, corrupt, validation,
};
use crate::artifact::{
    ArtifactDigest, ArtifactError, ArtifactHandle, ArtifactLabel, ArtifactMediaType,
    ArtifactProvenance, ArtifactProvenanceKind, ArtifactRetention, ArtifactVault, ArtifactWrite,
};
use rusqlite::TransactionBehavior;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub const PROGRAM_RUNTIME_SCHEMA_MARKER: &str = "sophon-sdk.program-runtime";
pub const PROGRAM_RUNTIME_SCHEMA_VERSION: u32 = 1;

/// How often the deadline/cancel loop looks at a running child.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Binds captured program output into an [`ArtifactVault`].
///
/// This is the adapter that keeps [`ProgramOutputSink`] and the vault contract
/// independent while still letting a Host that has both get the binding without
/// writing any code. Captured output is stored as an
/// [`ArtifactProvenanceKind::OperationRecord`], because a captured stream is a
/// diagnostic recording of an execution rather than a result the program was
/// asked to produce.
pub struct ArtifactVaultOutputSink<V> {
    vault: V,
    producer: ArtifactLabel,
}

impl<V: ArtifactVault> ArtifactVaultOutputSink<V> {
    pub fn new(vault: V, producer: ArtifactLabel) -> Self {
        Self { vault, producer }
    }

    pub fn vault(&self) -> &V {
        &self.vault
    }
}

impl<V: ArtifactVault> ProgramOutputSink for ArtifactVaultOutputSink<V> {
    fn store(
        &self,
        execution: &ExecutionId,
        stream: ProgramStream,
        content: &[u8],
        now_ms: u64,
    ) -> Result<ArtifactHandle, ProgramError> {
        let operation = ArtifactLabel::new(format!("{execution}.{}", stream.as_str()))
            .map_err(artifact_error)?;
        let write = ArtifactWrite::new(
            ArtifactMediaType::octet_stream(),
            ArtifactRetention::WhileProducerLives,
            ArtifactProvenance::produced(
                ArtifactProvenanceKind::OperationRecord,
                self.producer.clone(),
                0,
                operation,
                now_ms,
            )
            .map_err(artifact_error)?,
        );
        Ok(self
            .vault
            .put(content, &write, now_ms)
            .map_err(artifact_error)?
            .handle)
    }
}

fn artifact_error(error: ArtifactError) -> ProgramError {
    match error {
        ArtifactError::Validation(message) => ProgramError::Validation(message),
        ArtifactError::Corrupt(message) => ProgramError::Corrupt(message),
        other => ProgramError::Storage(other.to_string()),
    }
}

/// A pid-based liveness probe.
///
/// It can prove that no process holds the pid (`Gone`) and that some process
/// does (`Live`), and it deliberately does not claim more than that: after a
/// reboot or on a busy machine a pid may have been reused, so a Host with
/// stronger evidence — a container id, a job object, a service manager's own
/// record — should supply its own [`LivenessProbe`] rather than trusting this
/// one across a reboot. On platforms where the query is unavailable it answers
/// `Unknown` instead of guessing.
#[derive(Clone, Copy, Debug, Default)]
pub struct OsLivenessProbe;

impl LivenessProbe for OsLivenessProbe {
    #[cfg(unix)]
    fn probe(&self, process: &ProcessIdentity) -> Result<Liveness, ProgramError> {
        let pid = i32::try_from(process.pid())
            .map_err(|_| validation("process identity names a pid outside the OS range"))?;
        // Signal 0 performs the permission and existence checks without
        // delivering anything.
        let answered = unsafe { libc::kill(pid, 0) };
        if answered == 0 {
            return Ok(Liveness::Live);
        }
        match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => Ok(Liveness::Gone),
            // The process exists and belongs to someone else.
            Some(libc::EPERM) => Ok(Liveness::Live),
            _ => Ok(Liveness::Unknown),
        }
    }

    #[cfg(not(unix))]
    fn probe(&self, _: &ProcessIdentity) -> Result<Liveness, ProgramError> {
        Ok(Liveness::Unknown)
    }
}

/// Everything durable about a launch that a receipt needs and that survives the
/// process that created it. It holds digests and handle names, never arguments,
/// environment values or secrets.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LaunchRecord {
    program: ProgramPath,
    arguments_digest: ArtifactDigest,
    environment_digest: ArtifactDigest,
    working_root: PathBuf,
    credential_handles: Vec<super::CredentialHandleName>,
    bounds: ProgramBounds,
}

impl LaunchRecord {
    fn of(launch: &ProgramLaunch) -> Self {
        Self {
            program: launch.program().clone(),
            arguments_digest: launch.arguments_digest(),
            environment_digest: launch.environment_digest(),
            working_root: launch.working_root().to_path_buf(),
            credential_handles: launch.credential_handles(),
            bounds: launch.bounds(),
        }
    }

    fn receipt(
        &self,
        execution: &ExecutionId,
        disposition: ExitDisposition,
        started_at_ms: u64,
        settled_at_ms: u64,
        stdout: Option<CaptureRecord>,
        stderr: Option<CaptureRecord>,
    ) -> ExecutionReceipt {
        ExecutionReceipt {
            execution: execution.clone(),
            program: self.program.clone(),
            arguments_digest: self.arguments_digest.clone(),
            environment_digest: self.environment_digest.clone(),
            working_root: self.working_root.clone(),
            credential_handles: self.credential_handles.clone(),
            bounds: self.bounds,
            disposition,
            started_at_ms,
            settled_at_ms,
            stdout,
            stderr,
        }
    }
}

/// One captured stream, still in memory.
struct Capture {
    content: Vec<u8>,
    produced_bytes: u64,
}

/// A child this handle launched and is therefore able to wait on.
struct Live {
    child: Mutex<Child>,
    cancelled: AtomicBool,
    readers: Mutex<Option<(JoinHandle<Capture>, JoinHandle<Capture>)>>,
    settling: Mutex<()>,
    record: LaunchRecord,
    started_at_ms: u64,
    started_at: Instant,
}

/// Local reference authority: real processes plus the durable store.
///
/// Production Hosts inject their own implementation gated by
/// [`super::run_program_runtime_conformance`]; injection replaces this one and
/// is never mirrored. It is nevertheless usable as-is by a Host that is happy
/// with direct `std::process` spawning on the machine the Host runs on.
///
/// The owner token is fresh per instance. That is what makes restart
/// reconciliation work without a clock: a row whose owner is not this
/// instance's token was written by a handle that is not this one, so its fate
/// is unknown here by construction rather than by inference.
pub struct LocalProgramRuntime {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
    owner: super::ProgramLabel,
    sink: Arc<dyn ProgramOutputSink>,
    live: Mutex<HashMap<ExecutionId, Arc<Live>>>,
}

impl LocalProgramRuntime {
    pub fn new(
        root: impl Into<PathBuf>,
        sink: Arc<dyn ProgramOutputSink>,
    ) -> Result<Self, ProgramError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        set_private_dir(&root)?;
        let path = root.join("program-runtime.sqlite3");
        let existed = path.exists();
        let connection = rusqlite::Connection::open(&path).map_err(storage)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(storage)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                 CREATE TABLE IF NOT EXISTS executions(
                   execution_id TEXT PRIMARY KEY NOT NULL,
                   state TEXT NOT NULL,
                   owner TEXT NOT NULL,
                   pid INTEGER NOT NULL,
                   started_at_ms INTEGER NOT NULL,
                   launch TEXT NOT NULL,
                   receipt TEXT,
                   receipt_digest TEXT
                 );",
            )
            .map_err(storage)?;
        verify_schema(&connection, existed)?;
        set_private_file(&path)?;
        Ok(Self {
            path,
            connection: Mutex::new(connection),
            owner: super::ProgramLabel::new(format!(
                "program-runtime-{}",
                uuid::Uuid::new_v4().simple()
            ))?,
            sink,
            live: Mutex::new(HashMap::new()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The token identifying this instance in durable rows.
    pub fn owner(&self) -> &super::ProgramLabel {
        &self.owner
    }

    /// Forgets a running execution's process without settling it, leaving the
    /// durable row exactly as a crash would.
    ///
    /// Published for conformance harnesses and Host tests: an orphan cannot be
    /// produced through the contract, so a backend that claims to detect one
    /// has to be able to create one.
    pub fn abandon_for_test(&self, execution: &ExecutionId) -> Result<(), ProgramError> {
        let live = self
            .live
            .lock()
            .map_err(storage)?
            .remove(execution)
            .ok_or_else(|| ProgramError::NotFound(execution.clone()))?;
        let mut child = live.child.lock().map_err(storage)?;
        let _ = child.kill();
        let _ = child.wait();
        Ok(())
    }

    fn settle(
        &self,
        execution: &ExecutionId,
        receipt: &ExecutionReceipt,
    ) -> Result<ExecutionReceipt, ProgramError> {
        let encoded = serde_json::to_string(receipt).map_err(storage)?;
        let digest = receipt.digest();
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        if let Some(existing) = load_settled(&transaction, execution)? {
            // Replaying a settle is an answer, not a second settlement. The
            // stored receipt wins even if the replay would have written a
            // different one, because the first answer is the one a Host may
            // already have acted on.
            transaction.commit().map_err(storage)?;
            return Ok(existing);
        }
        let changed = transaction
            .execute(
                "UPDATE executions SET state='settled',receipt=?2,receipt_digest=?3 \
                 WHERE execution_id=?1 AND state='running'",
                rusqlite::params![execution.as_str(), encoded, digest.as_str()],
            )
            .map_err(storage)?;
        if changed == 0 {
            return Err(ProgramError::NotFound(execution.clone()));
        }
        transaction.commit().map_err(storage)?;
        Ok(receipt.clone())
    }

    fn capture(
        &self,
        execution: &ExecutionId,
        stream: ProgramStream,
        bounds: ProgramBounds,
        capture: Capture,
        now_ms: u64,
    ) -> Result<CaptureRecord, ProgramError> {
        let handle = self
            .sink
            .store(execution, stream, &capture.content, now_ms)?;
        if !handle.addresses(&capture.content) {
            return Err(corrupt(
                "the output sink bound a captured stream to content that is not that stream",
            ));
        }
        let record = CaptureRecord {
            stream,
            artifact: handle,
            captured_bytes: capture.content.len() as u64,
            produced_bytes: capture.produced_bytes,
            declared_bound: bounds.capture_bytes(stream),
            truncated: capture.produced_bytes > capture.content.len() as u64,
        };
        record.validate()?;
        Ok(record)
    }
}

/// Drains a stream to EOF, keeping the first `bound` bytes and counting the
/// rest.
///
/// Draining past the bound rather than closing the pipe is deliberate: a
/// program whose stdout stops being read blocks forever, so a backend that
/// stopped reading would turn a truncation into a hang and would have no
/// honest produced-byte count to report.
fn drain<R: std::io::Read + Send + 'static>(mut source: R, bound: u64) -> JoinHandle<Capture> {
    std::thread::spawn(move || {
        let mut content = Vec::new();
        let mut produced_bytes = 0u64;
        let mut buffer = [0u8; 8192];
        loop {
            match source.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    produced_bytes = produced_bytes.saturating_add(read as u64);
                    let room = bound.saturating_sub(content.len() as u64) as usize;
                    if room > 0 {
                        content.extend_from_slice(&buffer[..read.min(room)]);
                    }
                }
            }
        }
        Capture {
            content,
            produced_bytes,
        }
    })
}

impl ProgramRuntime for LocalProgramRuntime {
    fn launch(
        &self,
        execution: &ExecutionId,
        launch: &ProgramLaunch,
        credentials: &dyn CredentialResolver,
        now_ms: u64,
    ) -> Result<ProcessIdentity, ProgramError> {
        launch.validate()?;
        if self.inspect(execution)?.is_some() {
            return Err(ProgramError::Conflict(format!(
                "execution {execution} is already known to this authority"
            )));
        }

        let mut command = Command::new(launch.program().as_path());
        command
            .args(launch.arguments())
            .current_dir(launch.working_root())
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, binding) in launch.environment_bindings() {
            match binding {
                EnvironmentBinding::Literal(value) => {
                    command.env(name, value);
                }
                EnvironmentBinding::Credential(handle) => {
                    // Resolution happens here and nowhere else. The value is
                    // moved into the command's environment and dropped — and
                    // therefore zeroed — before this function returns.
                    let resolved = credentials.resolve(handle)?;
                    command.env(name, resolved.expose_for_spawn());
                }
            }
        }

        let record = LaunchRecord::of(launch);
        let encoded = serde_json::to_string(&record).map_err(storage)?;
        // Every child this backend creates is waited on: `wait` reaps it, and
        // `cancel` and `abandon_for_test` kill and reap it. The session-scoped
        // enrollment helper the lint points at is a tokio-child API belonging
        // to the shell, and this contract is deliberately synchronous and
        // independent of it.
        #[allow(
            clippy::disallowed_methods,
            reason = "the child is always waited on by this runtime"
        )]
        let mut child = command
            .spawn()
            .map_err(|error| ProgramError::Spawn(error.to_string()))?;
        let started_at = Instant::now();
        let pid = child.id();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ProgramError::Spawn("child has no stdout pipe".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ProgramError::Spawn("child has no stderr pipe".into()))?;
        let bounds = launch.bounds();
        let readers = (
            drain(stdout, bounds.stdout_capture_bytes()),
            drain(stderr, bounds.stderr_capture_bytes()),
        );

        let inserted = {
            let connection = self.connection.lock().map_err(storage)?;
            connection
                .execute(
                    "INSERT OR IGNORE INTO executions(execution_id,state,owner,pid,started_at_ms,launch) \
                     VALUES(?1,'running',?2,?3,?4,?5)",
                    rusqlite::params![
                        execution.as_str(),
                        self.owner.as_str(),
                        i64::from(pid),
                        as_i64(now_ms)?,
                        encoded,
                    ],
                )
                .map_err(storage)?
        };
        if inserted == 0 {
            let _ = child.kill();
            let _ = child.wait();
            return Err(ProgramError::Conflict(format!(
                "execution {execution} was durably claimed by another launch"
            )));
        }

        let identity = ProcessIdentity::new(pid, self.owner.clone())?;
        self.live.lock().map_err(storage)?.insert(
            execution.clone(),
            Arc::new(Live {
                child: Mutex::new(child),
                cancelled: AtomicBool::new(false),
                readers: Mutex::new(Some(readers)),
                settling: Mutex::new(()),
                record,
                started_at_ms: now_ms,
                started_at,
            }),
        );
        Ok(identity)
    }

    fn wait(&self, execution: &ExecutionId) -> Result<ExecutionReceipt, ProgramError> {
        let live = self.live.lock().map_err(storage)?.get(execution).cloned();
        let Some(live) = live else {
            return match self.inspect(execution)? {
                Some(ExecutionStatus::Settled(receipt)) => Ok(*receipt),
                Some(_) => Err(ProgramError::Unowned(execution.clone())),
                None => Err(ProgramError::NotFound(execution.clone())),
            };
        };
        let _settling = live.settling.lock().map_err(storage)?;
        if let Some(receipt) = {
            let connection = self.connection.lock().map_err(storage)?;
            load_settled(&connection, execution)?
        } {
            return Ok(receipt);
        }

        let deadline = Duration::from_millis(live.record.bounds.deadline_ms());
        let (status, stopped) = loop {
            let mut child = live.child.lock().map_err(storage)?;
            if let Some(status) = child.try_wait().map_err(storage)? {
                // The flag is set only where a cancel found a live child and
                // killed it, so reading it here attributes the death to the
                // cancel that caused it without dressing a natural exit up as
                // one.
                break (
                    status,
                    live.cancelled
                        .load(Ordering::SeqCst)
                        .then_some(ExitDisposition::Cancelled),
                );
            }
            let cancelled = live.cancelled.load(Ordering::SeqCst);
            let elapsed = live.started_at.elapsed();
            if cancelled || elapsed >= deadline {
                let _ = child.kill();
                let status = child.wait().map_err(storage)?;
                break (
                    status,
                    Some(if cancelled {
                        ExitDisposition::Cancelled
                    } else {
                        ExitDisposition::TimedOut
                    }),
                );
            }
            drop(child);
            std::thread::sleep(POLL_INTERVAL);
        };
        let settled_at_ms = live
            .started_at_ms
            .saturating_add(live.started_at.elapsed().as_millis() as u64);
        let disposition = stopped.unwrap_or_else(|| disposition_of(&status));

        let readers = live
            .readers
            .lock()
            .map_err(storage)?
            .take()
            .ok_or_else(|| corrupt("a settled execution lost its capture readers"))?;
        let stdout_capture = readers
            .0
            .join()
            .map_err(|_| corrupt("the stdout capture thread panicked"))?;
        let stderr_capture = readers
            .1
            .join()
            .map_err(|_| corrupt("the stderr capture thread panicked"))?;
        let bounds = live.record.bounds;
        let stdout = self.capture(
            execution,
            ProgramStream::Stdout,
            bounds,
            stdout_capture,
            settled_at_ms,
        )?;
        let stderr = self.capture(
            execution,
            ProgramStream::Stderr,
            bounds,
            stderr_capture,
            settled_at_ms,
        )?;

        let receipt = live.record.receipt(
            execution,
            disposition,
            live.started_at_ms,
            settled_at_ms,
            Some(stdout),
            Some(stderr),
        );
        let settled = self.settle(execution, &receipt)?;
        self.live.lock().map_err(storage)?.remove(execution);
        Ok(settled)
    }

    fn cancel(&self, execution: &ExecutionId) -> Result<(), ProgramError> {
        let live = self.live.lock().map_err(storage)?.get(execution).cloned();
        let Some(live) = live else {
            return match self.inspect(execution)? {
                Some(ExecutionStatus::Settled(_)) => Ok(()),
                Some(_) => Err(ProgramError::Unowned(execution.clone())),
                None => Err(ProgramError::NotFound(execution.clone())),
            };
        };
        let mut child = live.child.lock().map_err(storage)?;
        if child.try_wait().map_err(storage)?.is_some() {
            // The cancel lost the race. Recording it would rewrite an exit that
            // already happened as a cancellation, so the flag stays clear and
            // the disposition the process actually had survives.
            return Ok(());
        }
        live.cancelled.store(true, Ordering::SeqCst);
        let _ = child.kill();
        Ok(())
    }

    fn inspect(&self, execution: &ExecutionId) -> Result<Option<ExecutionStatus>, ProgramError> {
        let connection = self.connection.lock().map_err(storage)?;
        let Some(row) = load(&connection, execution)? else {
            return Ok(None);
        };
        Ok(Some(row.into_status(&self.owner)?))
    }

    fn requiring_reconciliation(&self) -> Result<Vec<ExecutionId>, ProgramError> {
        let connection = self.connection.lock().map_err(storage)?;
        let mut statement = connection
            .prepare_cached(
                "SELECT execution_id FROM executions WHERE state='running' AND owner<>?1 \
                 ORDER BY execution_id",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map([self.owner.as_str()], |row| row.get::<_, String>(0))
            .map_err(storage)?;
        let mut executions = Vec::new();
        for row in rows {
            executions.push(
                ExecutionId::new(row.map_err(storage)?)
                    .map_err(|error| corrupt(error.to_string()))?,
            );
        }
        Ok(executions)
    }

    fn reconcile(
        &self,
        execution: &ExecutionId,
        liveness: &dyn LivenessProbe,
        now_ms: u64,
    ) -> Result<ReconcileOutcome, ProgramError> {
        let status = self
            .inspect(execution)?
            .ok_or_else(|| ProgramError::NotFound(execution.clone()))?;
        let (started_at_ms, process) = match status {
            ExecutionStatus::Settled(receipt) => return Ok(ReconcileOutcome::Settled(receipt)),
            ExecutionStatus::Running {
                started_at_ms,
                process,
            }
            | ExecutionStatus::Uncertain {
                started_at_ms,
                process,
            } => (started_at_ms, process),
        };
        match liveness.probe(&process)? {
            Liveness::Live => Ok(ReconcileOutcome::StillRunning),
            Liveness::Unknown => Ok(ReconcileOutcome::Uncertain),
            Liveness::Gone => {
                let record = {
                    let connection = self.connection.lock().map_err(storage)?;
                    load(&connection, execution)?
                        .ok_or_else(|| ProgramError::NotFound(execution.clone()))?
                        .launch
                };
                // Captured output is deliberately absent rather than empty: the
                // process may have written plenty before it vanished, and this
                // handle never read the pipes. Claiming an empty stdout would
                // be as much of a fabrication as claiming success.
                let receipt = record.receipt(
                    execution,
                    ExitDisposition::Interrupted,
                    started_at_ms,
                    now_ms.max(started_at_ms),
                    None,
                    None,
                );
                Ok(ReconcileOutcome::Settled(Box::new(
                    self.settle(execution, &receipt)?,
                )))
            }
        }
    }
}

fn disposition_of(status: &std::process::ExitStatus) -> ExitDisposition {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return ExitDisposition::Signalled { signal };
        }
    }
    ExitDisposition::Exited {
        code: status.code().unwrap_or(-1),
    }
}

/// One decoded row whose scalars have been re-checked. Every stored value is
/// validated before it is trusted, so a foreign writer or a damaged page fails
/// the read rather than presenting an invented account of what ran.
struct StoredExecution {
    execution: ExecutionId,
    owner: super::ProgramLabel,
    pid: u32,
    started_at_ms: u64,
    launch: LaunchRecord,
    receipt: Option<ExecutionReceipt>,
}

impl StoredExecution {
    fn into_status(self, owner: &super::ProgramLabel) -> Result<ExecutionStatus, ProgramError> {
        if let Some(receipt) = self.receipt {
            return Ok(ExecutionStatus::Settled(Box::new(receipt)));
        }
        let process = ProcessIdentity::new(self.pid, self.owner.clone())
            .map_err(|error| corrupt(error.to_string()))?;
        let started_at_ms = self.started_at_ms;
        Ok(if &self.owner == owner {
            ExecutionStatus::Running {
                started_at_ms,
                process,
            }
        } else {
            ExecutionStatus::Uncertain {
                started_at_ms,
                process,
            }
        })
    }
}

const COLUMNS: &str = "execution_id,state,owner,pid,started_at_ms,launch,receipt,receipt_digest";

fn decode(row: &rusqlite::Row<'_>) -> Result<StoredExecution, ProgramError> {
    let execution = ExecutionId::new(row.get::<_, String>(0).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let settled = match row.get::<_, String>(1).map_err(storage)?.as_str() {
        "running" => false,
        "settled" => true,
        other => return Err(corrupt(format!("unknown execution state {other:?}"))),
    };
    let owner = super::ProgramLabel::new(row.get::<_, String>(2).map_err(storage)?)
        .map_err(|error| corrupt(error.to_string()))?;
    let pid = u32::try_from(row.get::<_, i64>(3).map_err(storage)?)
        .map_err(|_| corrupt("stored execution row holds a pid outside the OS range"))?;
    let started_at_ms = u64::try_from(row.get::<_, i64>(4).map_err(storage)?)
        .map_err(|_| corrupt("stored execution row holds a negative instant"))?;
    let launch: LaunchRecord = serde_json::from_str(&row.get::<_, String>(5).map_err(storage)?)
        .map_err(|error| corrupt(format!("stored launch record is undecodable: {error}")))?;
    launch
        .bounds
        .validate()
        .map_err(|error| corrupt(error.to_string()))?;
    let encoded: Option<String> = row.get(6).map_err(storage)?;
    let digest: Option<String> = row.get(7).map_err(storage)?;
    let receipt = match (settled, encoded, digest) {
        (false, None, None) => None,
        (true, Some(encoded), Some(digest)) => {
            let receipt: ExecutionReceipt = serde_json::from_str(&encoded)
                .map_err(|error| corrupt(format!("stored receipt is undecodable: {error}")))?;
            let expected =
                ArtifactDigest::parse(digest).map_err(|error| corrupt(error.to_string()))?;
            receipt.verify(&expected)?;
            if receipt.execution != execution {
                return Err(corrupt("a stored receipt names another execution"));
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
        execution,
        owner,
        pid,
        started_at_ms,
        launch,
        receipt,
    })
}

fn load(
    connection: &rusqlite::Connection,
    execution: &ExecutionId,
) -> Result<Option<StoredExecution>, ProgramError> {
    let mut statement = connection
        .prepare_cached(&format!(
            "SELECT {COLUMNS} FROM executions WHERE execution_id=?1"
        ))
        .map_err(storage)?;
    let mut rows = statement.query([execution.as_str()]).map_err(storage)?;
    let Some(row) = rows.next().map_err(storage)? else {
        return Ok(None);
    };
    let stored = decode(row)?;
    if &stored.execution != execution {
        return Err(corrupt("a stored execution does not address its own key"));
    }
    Ok(Some(stored))
}

fn load_settled(
    connection: &rusqlite::Connection,
    execution: &ExecutionId,
) -> Result<Option<ExecutionReceipt>, ProgramError> {
    Ok(load(connection, execution)?.and_then(|stored| stored.receipt))
}

fn verify_schema(connection: &rusqlite::Connection, existed: bool) -> Result<(), ProgramError> {
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
                "existing program runtime store has no schema metadata",
            ));
        }
        let transaction = connection.unchecked_transaction().map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO metadata(key,value) VALUES('schema_marker',?1)",
                [PROGRAM_RUNTIME_SCHEMA_MARKER],
            )
            .map_err(storage)?;
        transaction
            .execute(
                "INSERT INTO metadata(key,value) VALUES('schema_version',?1)",
                [PROGRAM_RUNTIME_SCHEMA_VERSION.to_string()],
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        return Ok(());
    }
    if metadata
        != [
            ("schema_marker".into(), PROGRAM_RUNTIME_SCHEMA_MARKER.into()),
            (
                "schema_version".into(),
                PROGRAM_RUNTIME_SCHEMA_VERSION.to_string(),
            ),
        ]
    {
        return Err(corrupt("program runtime schema marker/version mismatch"));
    }
    Ok(())
}

fn as_i64(value: u64) -> Result<i64, ProgramError> {
    i64::try_from(value).map_err(|_| validation("value exceeds the storable range"))
}

fn storage(error: impl std::fmt::Display) -> ProgramError {
    ProgramError::Storage(error.to_string())
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<(), ProgramError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(storage)
}

#[cfg(not(unix))]
fn set_private_dir(_: &Path) -> Result<(), ProgramError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<(), ProgramError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(storage)
}

#[cfg(not(unix))]
fn set_private_file(_: &Path) -> Result<(), ProgramError> {
    Ok(())
}
