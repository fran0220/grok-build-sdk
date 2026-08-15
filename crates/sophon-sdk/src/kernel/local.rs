// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    KernelCheckpointRef, KernelDisposition, KernelError, KernelExecutionDisposition,
    KernelExecutionKey, KernelExecutionReceipt, KernelExecutionStatus, KernelGeneration,
    KernelLabel, KernelReconcileOutcome, KernelRestore, KernelRuntime, KernelSessionId,
    KernelSessionReceipt, KernelSessionStatus, KernelSpec, KernelSubmission,
    MAX_KERNEL_CHECKPOINT_BYTES, MAX_KERNEL_NON_RESTORABLE_FACTS, MAX_KERNEL_RESTORABLE_FACTS,
    NonRestorableFact, RestorableFact, corrupt, validate_capture, validation,
};
use crate::artifact::ArtifactDigest;
use crate::program::{
    CaptureRecord, ExecutionId, Liveness, LivenessProbe, ProcessIdentity, ProgramLabel,
    ProgramOutputSink, ProgramStream,
};
use rusqlite::TransactionBehavior;
use std::{
    collections::HashMap,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver, TryRecvError},
    },
    time::{Duration, Instant},
};

pub const KERNEL_RUNTIME_SCHEMA_MARKER: &str = "sophon-sdk.kernel-runtime";
pub const KERNEL_RUNTIME_SCHEMA_VERSION: u32 = 1;

/// The dialect [`LocalKernelRuntime`] speaks. A [`KernelSpec`] that declares a
/// different protocol is refused rather than driven, and because the protocol
/// is folded into [`KernelSpec::spec_digest`], a checkpoint taken under one
/// dialect can never be restored under another.
pub const LOCAL_KERNEL_PROTOCOL: &str = "sophon-sdk.kernel.line/1";

/// The byte sequence that marks a protocol frame in a kernel's output.
///
/// Frames are found by scanning the byte stream rather than by reading lines,
/// because a fragment may legitimately produce output that has no trailing
/// newline. A kernel image speaking this protocol must never write these bytes
/// as ordinary output, and [`LocalKernelRuntime`] refuses a submission whose
/// source contains them.
pub const KERNEL_FRAME_MARK: &[u8] = b"\x1e\x1e";

/// How often the settle loop looks at a running fragment.
const POLL_INTERVAL: Duration = Duration::from_millis(5);
/// How long a cooperating kernel is given to answer an interrupt before it is
/// killed and the execution settles as
/// [`KernelExecutionDisposition::KernelDied`].
const INTERRUPT_GRACE: Duration = Duration::from_millis(750);
/// How long a checkpoint or restore may take before the kernel is treated as
/// gone. Neither runs caller code, so this is a protocol timeout rather than a
/// declared bound.
const CONTROL_DEADLINE: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Durable records
// ---------------------------------------------------------------------------

mod records;

use records::{
    SpecRecord, SubmissionRecord, as_i64, from_program, insert_incarnation, load_execution,
    load_incarnation, load_incarnation_receipts, load_pending, set_private_dir, set_private_file,
    storage, verify_schema,
};

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

/// How a kernel said one directive ended.
#[derive(Clone, Debug, PartialEq, Eq)]
enum FrameStatus {
    Ok,
    Raised(KernelLabel),
    Cancelled,
    Rejected(KernelLabel),
}

impl FrameStatus {
    fn parse(value: &str) -> Result<Self, KernelError> {
        let (head, rest) = match value.split_once(' ') {
            Some((head, rest)) => (head, rest),
            None => (value, ""),
        };
        match head {
            "ok" => Ok(Self::Ok),
            "cancelled" => Ok(Self::Cancelled),
            "raised" => Ok(Self::Raised(KernelLabel::new(rest)?)),
            "rejected" => Ok(Self::Rejected(KernelLabel::new(rest)?)),
            other => Err(KernelError::Start(format!(
                "kernel answered an unknown frame status {other:?}"
            ))),
        }
    }
}

/// One directive's worth of one stream: what was kept, what was written, and
/// how the directive ended. `status` is `None` when the stream reached EOF
/// before a frame arrived — that is, when the kernel died mid-directive.
struct Segment {
    content: Vec<u8>,
    produced_bytes: u64,
    status: Option<FrameStatus>,
}

/// One long-lived reader over one of the kernel's streams.
///
/// The thread outlives every individual execution because the pipe does: a
/// reader created per execution would either lose bytes written after its
/// frame or block the kernel by not draining. It keeps reading past the current
/// capture bound so `produced_bytes` stays an honest count rather than a number
/// that stops when the buffer does.
struct StreamReader {
    bound: Arc<AtomicU64>,
    segments: Mutex<Receiver<Segment>>,
}

impl StreamReader {
    fn spawn<R: Read + Send + 'static>(source: R) -> Self {
        let bound = Arc::new(AtomicU64::new(0));
        let (sender, receiver) = std::sync::mpsc::channel();
        let thread_bound = Arc::clone(&bound);
        std::thread::spawn(move || read_frames(source, &thread_bound, &sender));
        Self {
            bound,
            segments: Mutex::new(receiver),
        }
    }

    fn arm(&self, bound: u64) {
        self.bound.store(bound, Ordering::SeqCst);
    }

    fn try_take(&self) -> Result<Option<Segment>, KernelError> {
        match self.segments.lock().map_err(storage)?.try_recv() {
            Ok(segment) => Ok(Some(segment)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Ok(Some(Segment {
                content: Vec::new(),
                produced_bytes: 0,
                status: None,
            })),
        }
    }
}

fn read_frames<R: Read>(
    mut source: R,
    bound: &Arc<AtomicU64>,
    sender: &std::sync::mpsc::Sender<Segment>,
) {
    let mut content = Vec::new();
    let mut produced_bytes = 0u64;
    let mut pending: Vec<u8> = Vec::new();
    let mut buffer = [0u8; 8192];
    let commit = |content: &mut Vec<u8>, produced: &mut u64, bytes: &[u8]| {
        *produced = produced.saturating_add(bytes.len() as u64);
        let room = bound
            .load(Ordering::SeqCst)
            .saturating_sub(content.len() as u64) as usize;
        if room > 0 {
            content.extend_from_slice(&bytes[..bytes.len().min(room)]);
        }
    };
    loop {
        let read = match source.read(&mut buffer) {
            Ok(0) | Err(_) => {
                // EOF mid-directive: whatever was pending is output the kernel
                // produced before it died, and the absent status is what says
                // it died.
                commit(&mut content, &mut produced_bytes, &pending);
                let _ = sender.send(Segment {
                    content: std::mem::take(&mut content),
                    produced_bytes,
                    status: None,
                });
                return;
            }
            Ok(read) => read,
        };
        pending.extend_from_slice(&buffer[..read]);
        loop {
            let Some(at) = find(&pending, KERNEL_FRAME_MARK) else {
                // The mark may straddle two reads, so everything but the last
                // byte is known not to open one.
                let safe = pending.len().saturating_sub(KERNEL_FRAME_MARK.len() - 1);
                commit(&mut content, &mut produced_bytes, &pending[..safe]);
                pending.drain(..safe);
                break;
            };
            let after = at + KERNEL_FRAME_MARK.len();
            let Some(end) = pending[after..].iter().position(|byte| *byte == b'\n') else {
                break;
            };
            let line = String::from_utf8_lossy(&pending[after..after + end]).into_owned();
            let status = line
                .strip_prefix("done ")
                .map(FrameStatus::parse)
                .and_then(Result::ok);
            commit(&mut content, &mut produced_bytes, &pending[..at]);
            if sender
                .send(Segment {
                    content: std::mem::take(&mut content),
                    produced_bytes,
                    status,
                })
                .is_err()
            {
                return;
            }
            produced_bytes = 0;
            pending.drain(..after + end + 1);
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---------------------------------------------------------------------------
// Live incarnations
// ---------------------------------------------------------------------------

struct InFlight {
    key: KernelExecutionKey,
    sequence: u64,
    record: SubmissionRecord,
    sink: Arc<dyn ProgramOutputSink>,
    started_at_ms: u64,
    started_at: Instant,
    cancelled: Arc<AtomicBool>,
}

struct Live {
    generation: KernelGeneration,
    spec: SpecRecord,
    pid: u32,
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    stdout: StreamReader,
    stderr: StreamReader,
    inflight: Mutex<Option<InFlight>>,
    settling: Mutex<()>,
    executions: Mutex<u64>,
    last_activity_ms: Mutex<u64>,
}

impl Live {
    fn write(&self, bytes: &[u8]) -> Result<(), KernelError> {
        let mut stdin = self.stdin.lock().map_err(storage)?;
        stdin
            .write_all(bytes)
            .and_then(|()| stdin.flush())
            .map_err(|error| KernelError::Start(format!("kernel stopped reading: {error}")))
    }

    fn interrupt(&self) {
        interrupt_process(self.pid);
    }

    fn kill(&self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn exit_code(&self) -> Option<i32> {
        let mut child = self.child.lock().ok()?;
        match child.try_wait() {
            Ok(Some(status)) => Some(status.code().unwrap_or(-1)),
            _ => None,
        }
    }
}

#[cfg(unix)]
fn interrupt_process(pid: u32) {
    if let Ok(pid) = i32::try_from(pid) {
        // SAFETY: `kill` with a valid signal number has no memory effects; a
        // stale pid is reported as an error this call deliberately ignores.
        unsafe {
            libc::kill(pid, libc::SIGINT);
        }
    }
}

#[cfg(not(unix))]
fn interrupt_process(_: u32) {}

// ---------------------------------------------------------------------------
// The backend
// ---------------------------------------------------------------------------

/// Local reference authority: a real persistent child process per incarnation,
/// plus the durable store.
///
/// Production Hosts inject their own implementation gated by
/// [`super::run_kernel_runtime_conformance`]; injection replaces this one and
/// is never mirrored. It is nevertheless usable as-is by a Host whose kernel
/// image speaks [`LOCAL_KERNEL_PROTOCOL`]:
///
/// | Direction | Frame |
/// |---|---|
/// | Host → kernel | `\x1e\x1eexec`, the fragment's lines, `\x1e\x1eend` |
/// | Host → kernel | `\x1e\x1echeckpoint` |
/// | Host → kernel | `\x1e\x1erestore`, the payload's lines, `\x1e\x1eend` |
/// | Host → kernel | `\x1e\x1eclose` |
/// | kernel → Host | `\x1e\x1edone ok` / `done raised <class>` / `done cancelled` / `done rejected <reason>`, on **both** stdout and stderr |
///
/// A checkpoint's payload is the stdout the kernel writes before its frame, in
/// the line format `kernel-checkpoint 1`, then `bind <key> <value>`,
/// `module <name>` and `lost <kind> [label] <count>` lines. The Host's copy of
/// that payload goes to the caller's [`ProgramOutputSink`]; this backend also
/// keeps its own copy, because the sink is a write-only seam and a restore has
/// to hand the bytes back to a fresh process. The two are bound by digest and
/// the stored copy is verified against the checkpoint's own
/// [`ArtifactHandle`] before it is replayed.
///
/// Cancellation is scoped: the kernel is interrupted and given
/// [`INTERRUPT_GRACE`] to abandon the fragment. A kernel that cooperates leaves
/// the session live and the execution settles
/// [`KernelExecutionDisposition::Cancelled`]; a kernel that does not is killed
/// and the execution settles [`KernelExecutionDisposition::KernelDied`],
/// because the disposition has to say what actually happened.
///
/// The owner token is fresh per instance, which is what makes restart
/// reconciliation work without a clock: a row whose owner is not this
/// instance's token was written by a handle that is not this one, so its fate
/// is unknown here by construction rather than by inference.
pub struct LocalKernelRuntime {
    path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
    owner: ProgramLabel,
    live: Mutex<HashMap<KernelSessionId, Arc<Live>>>,
}

impl LocalKernelRuntime {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, KernelError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        set_private_dir(&root)?;
        let path = root.join("kernel-runtime.sqlite3");
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
                 CREATE TABLE IF NOT EXISTS sessions(
                   session_id TEXT PRIMARY KEY NOT NULL,
                   current_generation INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS incarnations(
                   session_id TEXT NOT NULL,
                   generation INTEGER NOT NULL,
                   state TEXT NOT NULL,
                   owner TEXT NOT NULL,
                   pid INTEGER NOT NULL,
                   opened_at_ms INTEGER NOT NULL,
                   last_activity_ms INTEGER NOT NULL,
                   spec TEXT NOT NULL,
                   executions INTEGER NOT NULL,
                   captured_bytes INTEGER NOT NULL,
                   receipt TEXT,
                   receipt_digest TEXT,
                   PRIMARY KEY(session_id,generation)
                 );
                 CREATE TABLE IF NOT EXISTS executions(
                   session_id TEXT NOT NULL,
                   generation INTEGER NOT NULL,
                   execution_id TEXT NOT NULL,
                   sequence INTEGER NOT NULL,
                   state TEXT NOT NULL,
                   owner TEXT NOT NULL,
                   started_at_ms INTEGER NOT NULL,
                   submission TEXT NOT NULL,
                   receipt TEXT,
                   receipt_digest TEXT,
                   PRIMARY KEY(session_id,generation,execution_id)
                 );
                 CREATE TABLE IF NOT EXISTS checkpoints(
                   artifact_id TEXT PRIMARY KEY NOT NULL,
                   session_id TEXT NOT NULL,
                   generation INTEGER NOT NULL,
                   payload BLOB NOT NULL
                 );",
            )
            .map_err(storage)?;
        verify_schema(&connection, existed)?;
        set_private_file(&path)?;
        Ok(Self {
            path,
            connection: Mutex::new(connection),
            owner: ProgramLabel::new(format!("kernel-runtime-{}", uuid::Uuid::new_v4().simple()))
                .map_err(|error| validation(error.to_string()))?,
            live: Mutex::new(HashMap::new()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The token identifying this instance in durable rows.
    pub fn owner(&self) -> &ProgramLabel {
        &self.owner
    }

    /// Kills a live kernel and forgets it without settling the durable record,
    /// leaving the store exactly as a crash would.
    ///
    /// Published for conformance harnesses and Host tests: an orphan cannot be
    /// produced through the contract, so a backend that claims to detect one has
    /// to be able to create one.
    pub fn abandon_for_test(&self, session: &KernelSessionId) -> Result<(), KernelError> {
        let live = self
            .live
            .lock()
            .map_err(storage)?
            .remove(session)
            .ok_or_else(|| KernelError::SessionNotFound(session.clone()))?;
        live.kill();
        Ok(())
    }

    fn spawn(&self, spec: &KernelSpec) -> Result<Live, KernelError> {
        let mut command = Command::new(spec.program().as_path());
        command
            .args(spec.arguments())
            .current_dir(spec.working_root())
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, value) in spec.environment_bindings() {
            command.env(name, value);
        }
        // Every child this backend creates is waited on: `close`, `kill` and
        // `abandon_for_test` reap it. The session-scoped enrollment helper the
        // lint points at is a tokio-child API belonging to the shell, and this
        // contract is deliberately synchronous and independent of it.
        #[allow(
            clippy::disallowed_methods,
            reason = "the child is always waited on by this runtime"
        )]
        let mut child = command
            .spawn()
            .map_err(|error| KernelError::Start(error.to_string()))?;
        let pid = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| KernelError::Start("kernel has no stdin pipe".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| KernelError::Start("kernel has no stdout pipe".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| KernelError::Start("kernel has no stderr pipe".into()))?;
        Ok(Live {
            generation: KernelGeneration::FIRST,
            spec: SpecRecord::of(spec),
            pid,
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            stdout: StreamReader::spawn(stdout),
            stderr: StreamReader::spawn(stderr),
            inflight: Mutex::new(None),
            settling: Mutex::new(()),
            executions: Mutex::new(0),
            last_activity_ms: Mutex::new(0),
        })
    }

    fn live_of(&self, session: &KernelSessionId) -> Result<Option<Arc<Live>>, KernelError> {
        Ok(self.live.lock().map_err(storage)?.get(session).cloned())
    }

    fn execution_receipt(
        &self,
        key: &KernelExecutionKey,
    ) -> Result<Option<KernelExecutionReceipt>, KernelError> {
        let connection = self.connection.lock().map_err(storage)?;
        Ok(load_execution(&connection, key)?.and_then(|execution| execution.receipt))
    }

    /// Answers why a session this handle does not hold live cannot be used.
    fn refuse_absent(&self, session: &KernelSessionId) -> KernelError {
        match self.inspect_session(session) {
            Ok(Some(KernelSessionStatus::Settled(_))) => KernelError::NotLive(session.clone()),
            Ok(Some(_)) => KernelError::Unowned(session.clone()),
            Ok(None) => KernelError::SessionNotFound(session.clone()),
            Err(error) => error,
        }
    }

    /// Runs one control directive — checkpoint or restore — and answers the two
    /// streams' segments.
    fn control(&self, live: &Live, frame: &[u8]) -> Result<(Segment, Segment), KernelError> {
        live.stdout.arm(MAX_KERNEL_CHECKPOINT_BYTES);
        live.stderr.arm(MAX_KERNEL_CHECKPOINT_BYTES);
        live.write(frame)?;
        let started = Instant::now();
        let mut stdout = None;
        let mut stderr = None;
        while stdout.is_none() || stderr.is_none() {
            if stdout.is_none() {
                stdout = live.stdout.try_take()?;
            }
            if stderr.is_none() {
                stderr = live.stderr.try_take()?;
            }
            if stdout.is_some() && stderr.is_some() {
                break;
            }
            if started.elapsed() > CONTROL_DEADLINE {
                live.kill();
                return Err(KernelError::Start(
                    "kernel did not answer a control directive".into(),
                ));
            }
            std::thread::sleep(POLL_INTERVAL);
        }
        Ok((
            stdout.expect("stdout segment is present"),
            stderr.expect("stderr segment is present"),
        ))
    }

    fn store_capture(
        &self,
        key: &KernelExecutionKey,
        sink: &Arc<dyn ProgramOutputSink>,
        stream: ProgramStream,
        segment: &Segment,
        bound: u64,
        now_ms: u64,
    ) -> Result<CaptureRecord, KernelError> {
        let handle = sink
            .store(&sink_identity(key)?, stream, &segment.content, now_ms)
            .map_err(from_program)?;
        if !handle.addresses(&segment.content) {
            return Err(corrupt(
                "the output sink bound a captured stream to content that is not that stream",
            ));
        }
        let record = CaptureRecord {
            stream,
            artifact: handle,
            captured_bytes: segment.content.len() as u64,
            produced_bytes: segment.produced_bytes,
            declared_bound: bound,
            truncated: segment.produced_bytes > segment.content.len() as u64,
        };
        validate_capture(&record)?;
        Ok(record)
    }

    fn settle_execution(
        &self,
        receipt: &KernelExecutionReceipt,
    ) -> Result<KernelExecutionReceipt, KernelError> {
        let encoded = serde_json::to_string(receipt).map_err(storage)?;
        let digest = receipt.digest();
        let captured = [receipt.stdout.as_ref(), receipt.stderr.as_ref()]
            .into_iter()
            .flatten()
            .map(|capture| capture.captured_bytes)
            .sum::<u64>();
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        if let Some(stored) =
            load_execution(&transaction, &receipt.key)?.and_then(|execution| execution.receipt)
        {
            // Replaying a settle is an answer, not a second settlement.
            transaction.commit().map_err(storage)?;
            return Ok(stored);
        }
        let changed = transaction
            .execute(
                "UPDATE executions SET state='settled',receipt=?4,receipt_digest=?5 \
                 WHERE session_id=?1 AND generation=?2 AND execution_id=?3 AND state='in_flight'",
                rusqlite::params![
                    receipt.key.session().as_str(),
                    as_i64(receipt.key.generation().get())?,
                    receipt.key.execution().as_str(),
                    encoded,
                    digest.as_str(),
                ],
            )
            .map_err(storage)?;
        if changed == 0 {
            return Err(KernelError::ExecutionNotFound(
                receipt.key.execution().clone(),
            ));
        }
        transaction
            .execute(
                "UPDATE incarnations SET captured_bytes=captured_bytes+?3 \
                 WHERE session_id=?1 AND generation=?2",
                rusqlite::params![
                    receipt.key.session().as_str(),
                    as_i64(receipt.key.generation().get())?,
                    as_i64(captured)?,
                ],
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        Ok(receipt.clone())
    }

    /// Settles one incarnation and every execution still in flight under it, in
    /// one transaction.
    ///
    /// The coupling is the point: a session receipt that settled while an
    /// execution receipt was still missing would let a Host conclude a fragment
    /// succeeded because nothing said otherwise.
    fn settle_session(
        &self,
        session: &KernelSessionId,
        generation: KernelGeneration,
        disposition: &KernelDisposition,
        inflight: &KernelExecutionDisposition,
        now_ms: u64,
    ) -> Result<(KernelSessionReceipt, Vec<KernelExecutionReceipt>), KernelError> {
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let stored = load_incarnation(&transaction, session, Some(generation))?
            .ok_or_else(|| KernelError::SessionNotFound(session.clone()))?;
        if let Some(receipt) = stored.receipt {
            let executions = load_incarnation_receipts(&transaction, session, generation)?
                .into_iter()
                .filter(|execution| execution.disposition == *inflight)
                .collect();
            transaction.commit().map_err(storage)?;
            return Ok((receipt, executions));
        }

        let mut settled = Vec::new();
        for pending in load_pending(&transaction, session, generation)? {
            let receipt = KernelExecutionReceipt {
                key: pending.key.clone(),
                sequence: pending.sequence,
                source_digest: pending.record.source_digest.clone(),
                spec_digest: pending.record.spec_digest.clone(),
                bounds: pending.record.bounds,
                disposition: inflight.clone(),
                started_at_ms: pending.started_at_ms,
                // Captured output is deliberately absent rather than empty:
                // the kernel may have written plenty before it vanished, and
                // nobody read it. Claiming an empty stdout would be as much of
                // a fabrication as claiming success.
                settled_at_ms: now_ms.max(pending.started_at_ms),
                stdout: None,
                stderr: None,
                checkpoint: None,
            };
            transaction
                .execute(
                    "UPDATE executions SET state='settled',receipt=?4,receipt_digest=?5 \
                     WHERE session_id=?1 AND generation=?2 AND execution_id=?3",
                    rusqlite::params![
                        receipt.key.session().as_str(),
                        as_i64(receipt.key.generation().get())?,
                        receipt.key.execution().as_str(),
                        serde_json::to_string(&receipt).map_err(storage)?,
                        receipt.digest().as_str(),
                    ],
                )
                .map_err(storage)?;
            settled.push(receipt);
        }

        let receipt = KernelSessionReceipt {
            session: session.clone(),
            generation,
            spec_digest: stored.spec.spec_digest.clone(),
            disposition: disposition.clone(),
            opened_at_ms: stored.opened_at_ms,
            settled_at_ms: now_ms.max(stored.opened_at_ms),
            executions: stored.executions,
            captured_bytes: stored.captured_bytes,
        };
        transaction
            .execute(
                "UPDATE incarnations SET state='settled',receipt=?3,receipt_digest=?4 \
                 WHERE session_id=?1 AND generation=?2",
                rusqlite::params![
                    session.as_str(),
                    as_i64(generation.get())?,
                    serde_json::to_string(&receipt).map_err(storage)?,
                    receipt.digest().as_str(),
                ],
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        self.live.lock().map_err(storage)?.remove(session);
        Ok((receipt, settled))
    }

    /// Settles the session because a declared session-level limit was reached.
    fn settle_ceiling(
        &self,
        session: &KernelSessionId,
        live: &Live,
        ceiling: &str,
        now_ms: u64,
    ) -> Result<KernelError, KernelError> {
        live.kill();
        self.settle_session(
            session,
            live.generation,
            &KernelDisposition::CeilingReached {
                ceiling: KernelLabel::new(ceiling)?,
            },
            &KernelExecutionDisposition::KernelDied,
            now_ms,
        )?;
        Ok(KernelError::NotLive(session.clone()))
    }
}

/// The identity captured output is stored under.
///
/// A sink is typed in [`ExecutionId`], which is bounded at 128 bytes, while a
/// kernel key is three bounded parts and can exceed that. The readable form is
/// used whenever it fits, because a Host reading its vault should see which
/// execution produced an artifact; the digest form is the fallback that keeps
/// long identities addressable rather than refused.
fn sink_identity(key: &KernelExecutionKey) -> Result<ExecutionId, KernelError> {
    let readable = format!("{}.{}.{}", key.session(), key.generation(), key.execution());
    ExecutionId::new(readable.clone())
        .or_else(|_| {
            ExecutionId::new(format!(
                "kernel.{}",
                ArtifactDigest::of(readable.as_bytes())
            ))
        })
        .map_err(|error| validation(error.to_string()))
}

fn checkpoint_identity(
    session: &KernelSessionId,
    generation: KernelGeneration,
) -> Result<ExecutionId, KernelError> {
    ExecutionId::new(format!("{session}.{generation}.checkpoint"))
        .or_else(|_| {
            ExecutionId::new(format!(
                "kernel.{}.checkpoint",
                ArtifactDigest::of(session.as_str().as_bytes())
            ))
        })
        .map_err(|error| validation(error.to_string()))
}

impl KernelRuntime for LocalKernelRuntime {
    fn open(
        &self,
        session: &KernelSessionId,
        spec: &KernelSpec,
        now_ms: u64,
    ) -> Result<KernelGeneration, KernelError> {
        spec.validate()?;
        ensure_protocol(spec)?;
        if self.inspect_session(session)?.is_some() {
            return Err(KernelError::Conflict(format!(
                "kernel session {session} is already known to this authority"
            )));
        }
        let live = Arc::new(Live {
            generation: KernelGeneration::FIRST,
            ..self.spawn(spec)?
        });
        *live.last_activity_ms.lock().map_err(storage)? = now_ms;

        let inserted = {
            let mut connection = self.connection.lock().map_err(storage)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(storage)?;
            let inserted = transaction
                .execute(
                    "INSERT OR IGNORE INTO sessions(session_id,current_generation) VALUES(?1,1)",
                    [session.as_str()],
                )
                .map_err(storage)?;
            if inserted > 0 {
                insert_incarnation(
                    &transaction,
                    session,
                    KernelGeneration::FIRST,
                    &self.owner,
                    live.pid,
                    now_ms,
                    &live.spec,
                )?;
            }
            transaction.commit().map_err(storage)?;
            inserted
        };
        if inserted == 0 {
            live.kill();
            return Err(KernelError::Conflict(format!(
                "kernel session {session} was durably claimed by another open"
            )));
        }
        self.live
            .lock()
            .map_err(storage)?
            .insert(session.clone(), live);
        Ok(KernelGeneration::FIRST)
    }

    fn submit(
        &self,
        key: &KernelExecutionKey,
        submission: &KernelSubmission,
        sink: &Arc<dyn ProgramOutputSink>,
        now_ms: u64,
    ) -> Result<(), KernelError> {
        key.validate()?;
        submission.validate()?;
        if submission.source().contains('\u{1e}') {
            return Err(validation(
                "a submission's source contains the protocol frame mark",
            ));
        }
        let session = key.session();
        let live = self
            .live_of(session)?
            .ok_or_else(|| self.refuse_absent(session))?;
        if live.generation != key.generation() {
            return Err(KernelError::Conflict(format!(
                "kernel session {session} is incarnation {} and not {}",
                live.generation,
                key.generation()
            )));
        }

        let last_activity = *live.last_activity_ms.lock().map_err(storage)?;
        if now_ms.saturating_sub(last_activity) > live.spec.bounds.idle_deadline_ms() {
            live.kill();
            self.settle_session(
                session,
                live.generation,
                &KernelDisposition::IdleExpired,
                &KernelExecutionDisposition::KernelDied,
                now_ms,
            )?;
            return Err(KernelError::NotLive(session.clone()));
        }

        let sequence = {
            let mut executions = live.executions.lock().map_err(storage)?;
            if *executions >= live.spec.bounds.max_executions() {
                drop(executions);
                return Err(self.settle_ceiling(session, &live, "max_executions", now_ms)?);
            }
            *executions += 1;
            *executions
        };

        let mut inflight = live.inflight.lock().map_err(storage)?;
        if let Some(existing) = inflight.as_ref() {
            return Err(KernelError::Conflict(format!(
                "kernel session {session} is already executing {}",
                existing.key.execution()
            )));
        }
        let record = SubmissionRecord {
            source_digest: submission.source_digest(),
            spec_digest: live.spec.spec_digest.clone(),
            bounds: submission.bounds(),
        };
        let inserted = {
            let mut connection = self.connection.lock().map_err(storage)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(storage)?;
            let inserted = transaction
                .execute(
                    "INSERT OR IGNORE INTO executions(session_id,generation,execution_id,sequence,\
                     state,owner,started_at_ms,submission) VALUES(?1,?2,?3,?4,'in_flight',?5,?6,?7)",
                    rusqlite::params![
                        session.as_str(),
                        as_i64(key.generation().get())?,
                        key.execution().as_str(),
                        as_i64(sequence)?,
                        self.owner.as_str(),
                        as_i64(now_ms)?,
                        serde_json::to_string(&record).map_err(storage)?,
                    ],
                )
                .map_err(storage)?;
            if inserted > 0 {
                transaction
                    .execute(
                        "UPDATE incarnations SET executions=?3,last_activity_ms=?4 \
                         WHERE session_id=?1 AND generation=?2",
                        rusqlite::params![
                            session.as_str(),
                            as_i64(key.generation().get())?,
                            as_i64(sequence)?,
                            as_i64(now_ms)?,
                        ],
                    )
                    .map_err(storage)?;
            }
            transaction.commit().map_err(storage)?;
            inserted
        };
        if inserted == 0 {
            *live.executions.lock().map_err(storage)? = sequence.saturating_sub(1);
            return Err(KernelError::Conflict(format!(
                "kernel execution {} is already known in session {session}",
                key.execution()
            )));
        }
        *live.last_activity_ms.lock().map_err(storage)? = now_ms;

        live.stdout.arm(submission.bounds().stdout_capture_bytes());
        live.stderr.arm(submission.bounds().stderr_capture_bytes());
        let mut frame = String::from("\u{1e}\u{1e}exec\n");
        frame.push_str(submission.source());
        if !frame.ends_with('\n') {
            frame.push('\n');
        }
        frame.push_str("\u{1e}\u{1e}end\n");
        live.write(frame.as_bytes())?;
        *inflight = Some(InFlight {
            key: key.clone(),
            sequence,
            record,
            sink: Arc::clone(sink),
            started_at_ms: now_ms,
            started_at: Instant::now(),
            cancelled: Arc::new(AtomicBool::new(false)),
        });
        Ok(())
    }

    fn wait(&self, key: &KernelExecutionKey) -> Result<KernelExecutionReceipt, KernelError> {
        key.validate()?;
        let Some(live) = self.live_of(key.session())? else {
            return match self.inspect_execution(key)? {
                Some(KernelExecutionStatus::Settled(receipt)) => Ok(*receipt),
                Some(_) => Err(KernelError::Unowned(key.session().clone())),
                None => Err(KernelError::ExecutionNotFound(key.execution().clone())),
            };
        };
        let _settling = live.settling.lock().map_err(storage)?;
        if let Some(receipt) = self.execution_receipt(key)? {
            return Ok(receipt);
        }
        let (sequence, record, sink, started_at_ms, started_at, cancelled) = {
            let inflight = live.inflight.lock().map_err(storage)?;
            let Some(inflight) = inflight.as_ref().filter(|entry| entry.key == *key) else {
                return Err(KernelError::ExecutionNotFound(key.execution().clone()));
            };
            (
                inflight.sequence,
                inflight.record.clone(),
                Arc::clone(&inflight.sink),
                inflight.started_at_ms,
                inflight.started_at,
                Arc::clone(&inflight.cancelled),
            )
        };

        let deadline = Duration::from_millis(record.bounds.deadline_ms());
        let mut stdout: Option<Segment> = None;
        let mut stderr: Option<Segment> = None;
        let mut stop: Option<KernelExecutionDisposition> = None;
        let mut interrupted_at: Option<Instant> = None;
        loop {
            if stdout.is_none() {
                stdout = live.stdout.try_take()?;
            }
            if stderr.is_none() {
                stderr = live.stderr.try_take()?;
            }
            if stdout.is_some() && stderr.is_some() {
                break;
            }
            if stop.is_none() {
                if cancelled.load(Ordering::SeqCst) {
                    stop = Some(KernelExecutionDisposition::Cancelled);
                } else if started_at.elapsed() >= deadline {
                    stop = Some(KernelExecutionDisposition::TimedOut);
                }
                if stop.is_some() {
                    live.interrupt();
                    interrupted_at = Some(Instant::now());
                }
            } else if interrupted_at.is_some_and(|at| at.elapsed() > INTERRUPT_GRACE) {
                // The kernel would not abandon the fragment, so the only way to
                // stop it is to end the process. That is a different fact from
                // a cancel, and the disposition says so.
                live.kill();
                interrupted_at = None;
            }
            std::thread::sleep(POLL_INTERVAL);
        }
        let stdout = stdout.expect("stdout segment is present");
        let stderr = stderr.expect("stderr segment is present");
        let settled_at_ms = started_at_ms.saturating_add(started_at.elapsed().as_millis() as u64);

        let disposition = match (&stdout.status, &stderr.status) {
            (Some(status), Some(_)) => match status {
                FrameStatus::Ok => KernelExecutionDisposition::Completed,
                FrameStatus::Raised(class) => KernelExecutionDisposition::Raised {
                    error_class: class.clone(),
                },
                FrameStatus::Cancelled => stop
                    .clone()
                    .unwrap_or(KernelExecutionDisposition::Cancelled),
                FrameStatus::Rejected(reason) => KernelExecutionDisposition::Raised {
                    error_class: reason.clone(),
                },
            },
            _ => KernelExecutionDisposition::KernelDied,
        };
        let died = disposition == KernelExecutionDisposition::KernelDied;

        let stdout_capture = self.store_capture(
            key,
            &sink,
            ProgramStream::Stdout,
            &stdout,
            record.bounds.stdout_capture_bytes(),
            settled_at_ms,
        )?;
        let stderr_capture = self.store_capture(
            key,
            &sink,
            ProgramStream::Stderr,
            &stderr,
            record.bounds.stderr_capture_bytes(),
            settled_at_ms,
        )?;
        let receipt = KernelExecutionReceipt {
            key: key.clone(),
            sequence,
            source_digest: record.source_digest.clone(),
            spec_digest: record.spec_digest.clone(),
            bounds: record.bounds,
            disposition,
            started_at_ms,
            settled_at_ms,
            stdout: Some(stdout_capture),
            stderr: Some(stderr_capture),
            checkpoint: None,
        };
        let settled = self.settle_execution(&receipt)?;
        *live.inflight.lock().map_err(storage)? = None;
        *live.last_activity_ms.lock().map_err(storage)? = settled_at_ms;

        if died {
            let exit = live.exit_code();
            live.kill();
            self.settle_session(
                key.session(),
                live.generation,
                &exit.map_or(KernelDisposition::Interrupted, |code| {
                    KernelDisposition::Exited { code }
                }),
                &KernelExecutionDisposition::KernelDied,
                settled_at_ms,
            )?;
        } else {
            let captured = {
                let connection = self.connection.lock().map_err(storage)?;
                load_incarnation(&connection, key.session(), Some(live.generation))?
                    .map_or(0, |incarnation| incarnation.captured_bytes)
            };
            if captured > live.spec.bounds.max_captured_bytes() {
                self.settle_ceiling(key.session(), &live, "max_captured_bytes", settled_at_ms)?;
            }
        }
        Ok(settled)
    }

    fn cancel(&self, key: &KernelExecutionKey) -> Result<(), KernelError> {
        key.validate()?;
        let Some(live) = self.live_of(key.session())? else {
            return match self.inspect_execution(key)? {
                Some(KernelExecutionStatus::Settled(_)) => Ok(()),
                Some(_) => Err(KernelError::Unowned(key.session().clone())),
                None => Err(KernelError::ExecutionNotFound(key.execution().clone())),
            };
        };
        let inflight = live.inflight.lock().map_err(storage)?;
        let Some(inflight) = inflight.as_ref().filter(|entry| entry.key == *key) else {
            return match self.inspect_execution(key)? {
                // A cancel that lost the race is an answer, not a rewrite: the
                // disposition the execution actually had survives.
                Some(_) => Ok(()),
                None => Err(KernelError::ExecutionNotFound(key.execution().clone())),
            };
        };
        inflight.cancelled.store(true, Ordering::SeqCst);
        live.interrupt();
        Ok(())
    }

    fn close(
        &self,
        session: &KernelSessionId,
        now_ms: u64,
    ) -> Result<KernelSessionReceipt, KernelError> {
        let Some(live) = self.live_of(session)? else {
            return match self.inspect_session(session)? {
                Some(KernelSessionStatus::Settled(receipt)) => Ok(*receipt),
                Some(_) => Err(KernelError::Unowned(session.clone())),
                None => Err(KernelError::SessionNotFound(session.clone())),
            };
        };
        let _ = live.write(b"\x1e\x1eclose\n");
        let closed_at = Instant::now();
        while closed_at.elapsed() < INTERRUPT_GRACE {
            if live.exit_code().is_some() {
                break;
            }
            std::thread::sleep(POLL_INTERVAL);
        }
        live.kill();
        let (receipt, _) = self.settle_session(
            session,
            live.generation,
            &KernelDisposition::Closed,
            &KernelExecutionDisposition::KernelDied,
            now_ms,
        )?;
        Ok(receipt)
    }

    fn checkpoint(
        &self,
        session: &KernelSessionId,
        sink: &Arc<dyn ProgramOutputSink>,
        now_ms: u64,
    ) -> Result<KernelCheckpointRef, KernelError> {
        let live = self
            .live_of(session)?
            .ok_or_else(|| self.refuse_absent(session))?;
        let _settling = live.settling.lock().map_err(storage)?;
        if live.inflight.lock().map_err(storage)?.is_some() {
            return Err(KernelError::Conflict(format!(
                "kernel session {session} cannot be checkpointed while it is executing"
            )));
        }
        let (stdout, stderr) = self.control(&live, b"\x1e\x1echeckpoint\n")?;
        match stdout.status {
            Some(FrameStatus::Ok) => {}
            Some(other) => {
                return Err(KernelError::Start(format!(
                    "kernel refused a checkpoint: {other:?}"
                )));
            }
            None => {
                live.kill();
                self.settle_session(
                    session,
                    live.generation,
                    &KernelDisposition::Interrupted,
                    &KernelExecutionDisposition::KernelDied,
                    now_ms,
                )?;
                return Err(KernelError::NotLive(session.clone()));
            }
        }
        if stdout.produced_bytes > stdout.content.len() as u64 {
            return Err(validation(format!(
                "kernel snapshot exceeds {MAX_KERNEL_CHECKPOINT_BYTES} bytes"
            )));
        }
        if !stderr.content.is_empty() {
            return Err(corrupt(
                "a kernel wrote to stderr while producing a snapshot",
            ));
        }

        let (restorable, non_restorable) = parse_snapshot(&stdout.content)?;
        let sequence = *live.executions.lock().map_err(storage)?;
        let handle = sink
            .store(
                &checkpoint_identity(session, live.generation)?,
                ProgramStream::Stdout,
                &stdout.content,
                now_ms,
            )
            .map_err(from_program)?;
        if !handle.addresses(&stdout.content) {
            return Err(corrupt(
                "the output sink bound a snapshot to content that is not that snapshot",
            ));
        }
        self.connection
            .lock()
            .map_err(storage)?
            .execute(
                "INSERT OR REPLACE INTO checkpoints(artifact_id,session_id,generation,payload) \
                 VALUES(?1,?2,?3,?4)",
                rusqlite::params![
                    handle.id().as_str(),
                    session.as_str(),
                    as_i64(live.generation.get())?,
                    stdout.content,
                ],
            )
            .map_err(storage)?;

        let checkpoint = KernelCheckpointRef {
            artifact: handle,
            session: session.clone(),
            generation: live.generation,
            after_sequence: sequence,
            spec_digest: live.spec.spec_digest.clone(),
            taken_at_ms: now_ms,
            restorable,
            non_restorable,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    fn restore(
        &self,
        session: &KernelSessionId,
        checkpoint: &KernelCheckpointRef,
        spec: &KernelSpec,
        now_ms: u64,
    ) -> Result<KernelRestore, KernelError> {
        spec.validate()?;
        ensure_protocol(spec)?;
        checkpoint.validate()?;
        let offered = spec.spec_digest();
        if offered != checkpoint.spec_digest {
            return Ok(KernelRestore::SpecMismatch {
                expected: checkpoint.spec_digest.clone(),
                found: offered,
            });
        }
        let status = self
            .inspect_session(session)?
            .ok_or_else(|| KernelError::SessionNotFound(session.clone()))?;
        let generation = match status {
            KernelSessionStatus::Settled(receipt) => receipt.generation,
            _ => {
                return Err(KernelError::Conflict(format!(
                    "kernel session {session} must be closed or reconciled before it is restored"
                )));
            }
        };
        let next = generation
            .next()
            .ok_or_else(|| corrupt("kernel session generations are exhausted"))?;

        let payload: Vec<u8> = {
            let connection = self.connection.lock().map_err(storage)?;
            connection
                .query_row(
                    "SELECT payload FROM checkpoints WHERE artifact_id=?1",
                    [checkpoint.artifact.id().as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| corrupt("a checkpoint's payload is not held by this authority"))?
        };
        if !checkpoint.artifact.addresses(&payload) {
            return Err(corrupt(
                "a stored snapshot does not address to the checkpoint that names it",
            ));
        }

        let live = self.spawn(spec)?;
        let mut frame = String::from("\u{1e}\u{1e}restore\n");
        frame.push_str(&String::from_utf8_lossy(&payload));
        if !frame.ends_with('\n') {
            frame.push('\n');
        }
        frame.push_str("\u{1e}\u{1e}end\n");
        let (stdout, _) = self.control(&live, frame.as_bytes())?;
        match stdout.status {
            Some(FrameStatus::Ok) => {}
            Some(FrameStatus::Rejected(reason)) => {
                live.kill();
                return Ok(KernelRestore::Rejected { reason });
            }
            Some(other) => {
                live.kill();
                return Err(KernelError::Start(format!(
                    "kernel answered a restore with {other:?}"
                )));
            }
            None => {
                live.kill();
                return Err(KernelError::Start(
                    "kernel died while accepting a snapshot".into(),
                ));
            }
        }

        let restored = Arc::new(Live {
            generation: next,
            ..live
        });
        *restored.last_activity_ms.lock().map_err(storage)? = now_ms;
        {
            let mut connection = self.connection.lock().map_err(storage)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(storage)?;
            insert_incarnation(
                &transaction,
                session,
                next,
                &self.owner,
                restored.pid,
                now_ms,
                &restored.spec,
            )?;
            transaction
                .execute(
                    "UPDATE sessions SET current_generation=?2 WHERE session_id=?1",
                    rusqlite::params![session.as_str(), as_i64(next.get())?],
                )
                .map_err(storage)?;
            transaction.commit().map_err(storage)?;
        }
        self.live
            .lock()
            .map_err(storage)?
            .insert(session.clone(), restored);
        Ok(KernelRestore::Restored {
            session: session.clone(),
            generation: next,
            lost: checkpoint.non_restorable.clone(),
        })
    }

    fn inspect_session(
        &self,
        session: &KernelSessionId,
    ) -> Result<Option<KernelSessionStatus>, KernelError> {
        let connection = self.connection.lock().map_err(storage)?;
        let Some(stored) = load_incarnation(&connection, session, None)? else {
            return Ok(None);
        };
        drop(connection);
        if let Some(receipt) = stored.receipt {
            return Ok(Some(KernelSessionStatus::Settled(Box::new(receipt))));
        }
        let process = ProcessIdentity::new(stored.pid, stored.owner.clone())
            .map_err(|error| corrupt(error.to_string()))?;
        let owned =
            stored.owner == self.owner && self.live.lock().map_err(storage)?.contains_key(session);
        Ok(Some(if owned {
            KernelSessionStatus::Live {
                generation: stored.generation,
                process,
                opened_at_ms: stored.opened_at_ms,
                executions: stored.executions,
            }
        } else {
            KernelSessionStatus::Uncertain {
                generation: stored.generation,
                process,
                opened_at_ms: stored.opened_at_ms,
                executions: stored.executions,
            }
        }))
    }

    fn inspect_execution(
        &self,
        key: &KernelExecutionKey,
    ) -> Result<Option<KernelExecutionStatus>, KernelError> {
        let connection = self.connection.lock().map_err(storage)?;
        let Some(stored) = load_execution(&connection, key)? else {
            return Ok(None);
        };
        drop(connection);
        if let Some(receipt) = stored.receipt {
            return Ok(Some(KernelExecutionStatus::Settled(Box::new(receipt))));
        }
        let owned = stored.owner == self.owner
            && self
                .live_of(key.session())?
                .map(|live| {
                    live.inflight
                        .lock()
                        .map(|inflight| inflight.as_ref().is_some_and(|entry| entry.key == *key))
                        .unwrap_or(false)
                })
                .unwrap_or(false);
        Ok(Some(if owned {
            KernelExecutionStatus::InFlight {
                sequence: stored.sequence,
                started_at_ms: stored.started_at_ms,
            }
        } else {
            KernelExecutionStatus::Uncertain {
                sequence: stored.sequence,
                started_at_ms: stored.started_at_ms,
            }
        }))
    }

    fn requiring_reconciliation(&self) -> Result<Vec<KernelSessionId>, KernelError> {
        let held: Vec<KernelSessionId> =
            self.live.lock().map_err(storage)?.keys().cloned().collect();
        let connection = self.connection.lock().map_err(storage)?;
        let mut statement = connection
            .prepare_cached(
                "SELECT i.session_id FROM incarnations i JOIN sessions s \
                 ON s.session_id=i.session_id AND s.current_generation=i.generation \
                 WHERE i.state='live' ORDER BY i.session_id",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(storage)?;
        let mut sessions = Vec::new();
        for row in rows {
            let session = KernelSessionId::new(row.map_err(storage)?)
                .map_err(|error| corrupt(error.to_string()))?;
            if !held.contains(&session) {
                sessions.push(session);
            }
        }
        Ok(sessions)
    }

    fn reconcile(
        &self,
        session: &KernelSessionId,
        liveness: &dyn LivenessProbe,
        now_ms: u64,
    ) -> Result<KernelReconcileOutcome, KernelError> {
        let status = self
            .inspect_session(session)?
            .ok_or_else(|| KernelError::SessionNotFound(session.clone()))?;
        let (generation, process) = match status {
            KernelSessionStatus::Settled(receipt) => {
                let executions = {
                    let connection = self.connection.lock().map_err(storage)?;
                    load_incarnation_receipts(&connection, session, receipt.generation)?
                }
                .into_iter()
                .filter(|execution| {
                    execution.disposition == KernelExecutionDisposition::Interrupted
                })
                .collect();
                return Ok(KernelReconcileOutcome::Settled {
                    session: receipt,
                    executions,
                });
            }
            KernelSessionStatus::Live {
                generation,
                process,
                ..
            }
            | KernelSessionStatus::Uncertain {
                generation,
                process,
                ..
            } => (generation, process),
        };
        match liveness.probe(&process).map_err(from_program)? {
            Liveness::Live => Ok(KernelReconcileOutcome::StillLive),
            Liveness::Unknown => Ok(KernelReconcileOutcome::Uncertain),
            Liveness::Gone => {
                let (session_receipt, executions) = self.settle_session(
                    session,
                    generation,
                    &KernelDisposition::Interrupted,
                    &KernelExecutionDisposition::Interrupted,
                    now_ms,
                )?;
                Ok(KernelReconcileOutcome::Settled {
                    session: Box::new(session_receipt),
                    executions,
                })
            }
        }
    }
}

fn ensure_protocol(spec: &KernelSpec) -> Result<(), KernelError> {
    if spec.protocol().as_str() != LOCAL_KERNEL_PROTOCOL {
        return Err(validation(format!(
            "this backend speaks {LOCAL_KERNEL_PROTOCOL} and not {}",
            spec.protocol()
        )));
    }
    Ok(())
}

/// Reads a snapshot's typed declarations. The payload's *content* is the
/// kernel's business; what the contract needs from it is what it claims to
/// carry and what it admits it lost.
fn parse_snapshot(
    payload: &[u8],
) -> Result<(Vec<RestorableFact>, Vec<NonRestorableFact>), KernelError> {
    let text = std::str::from_utf8(payload)
        .map_err(|_| corrupt("a kernel snapshot is not valid UTF-8"))?;
    let mut lines = text.lines();
    if lines.next() != Some("kernel-checkpoint 1") {
        return Err(corrupt("a kernel snapshot has no recognisable header"));
    }
    let mut bindings = 0u64;
    let mut modules = 0u64;
    let mut non_restorable = Vec::new();
    for line in lines {
        let mut fields = line.split(' ');
        match fields.next() {
            Some("bind") => bindings += 1,
            Some("module") => modules += 1,
            Some("lost") => non_restorable.push(parse_loss(&mut fields)?),
            Some("") | None => {}
            Some(other) => {
                return Err(corrupt(format!(
                    "a kernel snapshot declares an unknown line {other:?}"
                )));
            }
        }
    }
    let mut restorable = Vec::new();
    if bindings > 0 {
        restorable.push(RestorableFact::Bindings { count: bindings });
    }
    if modules > 0 {
        restorable.push(RestorableFact::Modules { count: modules });
    }
    if restorable.len() > MAX_KERNEL_RESTORABLE_FACTS
        || non_restorable.len() > MAX_KERNEL_NON_RESTORABLE_FACTS
    {
        return Err(corrupt("a kernel snapshot declares too many facts"));
    }
    Ok((restorable, non_restorable))
}

fn parse_loss(fields: &mut std::str::Split<'_, char>) -> Result<NonRestorableFact, KernelError> {
    let kind = fields
        .next()
        .ok_or_else(|| corrupt("a kernel snapshot declares a loss with no kind"))?;
    let rest: Vec<&str> = fields.collect();
    let count = |value: Option<&&str>| -> Result<u64, KernelError> {
        value
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| corrupt("a kernel snapshot declares a loss with no count"))
    };
    Ok(match kind {
        "open_file" => NonRestorableFact::OpenFile {
            count: count(rest.first())?,
        },
        "network_connection" => NonRestorableFact::NetworkConnection {
            count: count(rest.first())?,
        },
        "child_process" => NonRestorableFact::ChildProcess {
            count: count(rest.first())?,
        },
        "concurrent_task" => NonRestorableFact::ConcurrentTask {
            count: count(rest.first())?,
        },
        "foreign_handle" => NonRestorableFact::ForeignHandle {
            kind: KernelLabel::new(*rest.first().unwrap_or(&""))?,
            count: count(rest.get(1))?,
        },
        "unserialisable_value" => NonRestorableFact::UnserialisableValue {
            kind: KernelLabel::new(*rest.first().unwrap_or(&""))?,
            count: count(rest.get(1))?,
        },
        "external_mutation" => NonRestorableFact::ExternalMutation {
            kind: KernelLabel::new(*rest.first().unwrap_or(&""))?,
        },
        other => {
            return Err(corrupt(format!(
                "a kernel snapshot declares an unknown loss {other:?}"
            )));
        }
    })
}
