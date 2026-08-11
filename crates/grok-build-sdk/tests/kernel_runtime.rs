// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! The persistent kernel contract, exercised the way a Host exercises it:
//! through the public façade, against the reference runtime and a real
//! long-lived child process, and against a backend that reports a lost session
//! as a clean shutdown.

use grok_build_sdk::{
    ArtifactHandle, ArtifactLabel, ArtifactVault, ArtifactVaultOutputSink,
    CONFORMANCE_KERNEL_ERROR_CLASS, CONFORMANCE_KERNEL_SECRET, CONFORMANCE_KERNEL_STATE_NAME,
    CONFORMANCE_KERNEL_STATE_VALUE, CONFORMANCE_KERNEL_STDERR_MARK, CONFORMANCE_KERNEL_STDOUT_MARK,
    ConformanceOpen, KernelDamage, KernelDisposition, KernelError, KernelExecutionBounds,
    KernelExecutionDisposition, KernelExecutionId, KernelExecutionKey, KernelGeneration,
    KernelLabel, KernelReconcileOutcome, KernelRestore, KernelRuntime, KernelRuntimeHarness,
    KernelScript, KernelSessionBounds, KernelSessionId, KernelSessionReceipt, KernelSessionStatus,
    KernelSpec, KernelSubmission, LOCAL_KERNEL_PROTOCOL, Liveness, LivenessProbe,
    LocalArtifactVault, LocalKernelRuntime, MAX_KERNEL_EXECUTION_DEADLINE_MS, ProcessIdentity,
    ProgramError, ProgramOutputSink, ProgramPath, run_kernel_runtime_conformance,
};
use std::{path::Path, sync::Arc};

const NOW: u64 = 1_700_000_000_000;
/// The variable the harness plants the suite's secret in. It is an ordinary
/// literal binding under a name no provider library reads, which is the only
/// door the contract leaves open — and the point of the control is that even
/// this never reaches durable state.
const SECRET_VARIABLE: &str = "KERNEL_CONFORMANCE_TOKEN";

/// The scriptable kernel this crate ships for exactly this purpose.
fn fixture_program() -> ProgramPath {
    ProgramPath::new(env!("CARGO_BIN_EXE_kernel_fixture"))
        .expect("the fixture kernel has an absolute path")
}

fn fragment_source(script: KernelScript) -> String {
    match script {
        KernelScript::Settles => {
            format!(
                "echo {CONFORMANCE_KERNEL_STDOUT_MARK}\nwarn {CONFORMANCE_KERNEL_STDERR_MARK}\n"
            )
        }
        KernelScript::BindsState => {
            format!("set {CONFORMANCE_KERNEL_STATE_NAME} {CONFORMANCE_KERNEL_STATE_VALUE}\n")
        }
        KernelScript::ReadsState => format!("get {CONFORMANCE_KERNEL_STATE_NAME}\n"),
        KernelScript::Raises => format!("raise {CONFORMANCE_KERNEL_ERROR_CLASS}\n"),
        KernelScript::Floods => "emit 65536\n".to_owned(),
        KernelScript::Sleeps => "sleep 30000\n".to_owned(),
        KernelScript::HoldsUnserialisableState => "hold-file\n".to_owned(),
        other => panic!("the harness does not supply a fragment for {other:?}"),
    }
}

struct FixedProbe(Liveness);

impl LivenessProbe for FixedProbe {
    fn probe(&self, _: &ProcessIdentity) -> Result<Liveness, ProgramError> {
        Ok(self.0)
    }
}

/// Everything one authority needs: a store root, a vault-backed sink, and a
/// working root the kernel runs in.
struct Fixture {
    store: tempfile::TempDir,
    vault_root: tempfile::TempDir,
    work: tempfile::TempDir,
    /// Every handle opened so far, so a crash can be staged on whichever one
    /// holds the session live.
    handles: Vec<Arc<LocalKernelRuntime>>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            store: tempfile::tempdir().unwrap(),
            vault_root: tempfile::tempdir().unwrap(),
            work: tempfile::tempdir().unwrap(),
            handles: Vec::new(),
        }
    }

    fn vault(&self) -> LocalArtifactVault {
        LocalArtifactVault::new(self.vault_root.path()).expect("the vault opens")
    }

    fn sink(&self) -> Arc<dyn ProgramOutputSink> {
        Arc::new(ArtifactVaultOutputSink::new(
            self.vault(),
            ArtifactLabel::new("kernel_runtime_tests").expect("a valid producer label"),
        ))
    }

    fn runtime(&mut self) -> Arc<LocalKernelRuntime> {
        let runtime =
            Arc::new(LocalKernelRuntime::new(self.store.path()).expect("the runtime opens"));
        self.handles.push(Arc::clone(&runtime));
        runtime
    }

    fn spec(&self, bounds: KernelSessionBounds) -> KernelSpec {
        spec_in(self.work.path(), bounds)
    }

    fn captured(&self, artifact: &ArtifactHandle) -> Vec<u8> {
        self.vault()
            .read(artifact, NOW)
            .expect("a captured stream is readable")
    }

    fn store_path(&self) -> std::path::PathBuf {
        self.store.path().join("kernel-runtime.sqlite3")
    }

    fn connection(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.store_path()).expect("the store opens")
    }
}

fn spec_in(work: &Path, bounds: KernelSessionBounds) -> KernelSpec {
    KernelSpec::new(
        fixture_program(),
        KernelLabel::new(LOCAL_KERNEL_PROTOCOL).expect("the protocol is a valid label"),
        work,
        bounds,
    )
    .expect("a spec under an existing root is valid")
    .environment(SECRET_VARIABLE, CONFORMANCE_KERNEL_SECRET)
    .expect("a literal environment binding is valid")
}

impl KernelRuntimeHarness for Fixture {
    fn open(&mut self, _: ConformanceOpen) -> Result<Arc<dyn KernelRuntime>, KernelError> {
        Ok(self.runtime())
    }

    fn spec(&mut self, bounds: KernelSessionBounds) -> Result<KernelSpec, KernelError> {
        Ok(Fixture::spec(self, bounds))
    }

    fn fragment(
        &mut self,
        script: KernelScript,
        bounds: KernelExecutionBounds,
    ) -> Result<KernelSubmission, KernelError> {
        KernelSubmission::new(fragment_source(script), bounds)
    }

    fn sink(&mut self) -> Result<Arc<dyn ProgramOutputSink>, KernelError> {
        Ok(Fixture::sink(self))
    }

    fn abandon(&mut self, session: &KernelSessionId) -> Result<(), KernelError> {
        for handle in &self.handles {
            if handle.abandon_for_test(session).is_ok() {
                return Ok(());
            }
        }
        Err(KernelError::SessionNotFound(session.clone()))
    }

    fn damage(
        &mut self,
        session: &KernelSessionId,
        damage: KernelDamage,
    ) -> Result<(), KernelError> {
        let connection = self.connection();
        let changed = match damage {
            // The receipt is edited and its stored digest is left alone, which
            // is what a store edited underneath the authority looks like.
            KernelDamage::Receipt => {
                let stored: String = connection
                    .query_row(
                        "SELECT receipt FROM incarnations WHERE session_id=?1 AND state='settled'",
                        [session.as_str()],
                        |row| row.get(0),
                    )
                    .expect("a settled incarnation has a receipt");
                let mut receipt: serde_json::Value =
                    serde_json::from_str(&stored).expect("a stored receipt is JSON");
                receipt["executions"] = serde_json::json!(9_999);
                connection
                    .execute(
                        "UPDATE incarnations SET receipt=?2 WHERE session_id=?1",
                        rusqlite::params![session.as_str(), receipt.to_string()],
                    )
                    .expect("the receipt is writable")
            }
            KernelDamage::Record => connection
                .execute(
                    "UPDATE incarnations SET spec='not-a-spec' WHERE session_id=?1",
                    [session.as_str()],
                )
                .expect("the record is writable"),
            other => panic!("the harness cannot stage {other:?}"),
        };
        assert!(changed > 0, "the damage did not reach any row");
        Ok(())
    }

    fn durable_bytes(&mut self) -> Result<Vec<u8>, KernelError> {
        let mut bytes = std::fs::read(self.store_path()).expect("the store is readable");
        // In WAL mode the newest rows live beside the database rather than in
        // it, so a control that read only the one file could pass vacuously.
        for suffix in ["-wal", "-shm"] {
            let path = self.store_path().with_extension(format!("sqlite3{suffix}"));
            if let Ok(extra) = std::fs::read(&path) {
                bytes.extend_from_slice(&extra);
            }
        }
        Ok(bytes)
    }

    fn captured(&mut self, artifact: &ArtifactHandle) -> Result<Vec<u8>, KernelError> {
        Ok(Fixture::captured(self, artifact))
    }
}

fn session(value: &str) -> KernelSessionId {
    KernelSessionId::new(value).expect("a valid session id")
}

fn key(session: &KernelSessionId, generation: KernelGeneration, id: &str) -> KernelExecutionKey {
    KernelExecutionKey::new(
        session.clone(),
        generation,
        KernelExecutionId::new(id).expect("a valid execution id"),
    )
    .expect("a valid execution key")
}

fn session_bounds() -> KernelSessionBounds {
    KernelSessionBounds::new(MAX_KERNEL_EXECUTION_DEADLINE_MS, 64, 16 * 1024 * 1024)
        .expect("declared session bounds are valid")
}

fn execution_bounds(deadline_ms: u64, capture: u64) -> KernelExecutionBounds {
    KernelExecutionBounds::new(deadline_ms, capture, capture)
        .expect("declared execution bounds are valid")
}

fn run(
    runtime: &Arc<LocalKernelRuntime>,
    fixture: &Fixture,
    key: &KernelExecutionKey,
    source: &str,
    bounds: KernelExecutionBounds,
    now_ms: u64,
) -> grok_build_sdk::KernelExecutionReceipt {
    let submission = KernelSubmission::new(source, bounds).expect("a valid submission");
    let sink = fixture.sink();
    runtime
        .submit(key, &submission, &sink, now_ms)
        .expect("the fragment is accepted");
    runtime.wait(key).expect("the fragment settles")
}

// ---------------------------------------------------------------------------
// (1) The contract, against a real persistent process.
// ---------------------------------------------------------------------------

/// The reference backend satisfies the whole published contract.
#[test]
fn the_reference_kernel_runtime_is_conformant() {
    let mut fixture = Fixture::new();
    run_kernel_runtime_conformance(&mut fixture).expect("the reference runtime is conformant");
}

/// State lives in the process, not in the store: a value bound by one fragment
/// is read by the next, and the session outlives both.
#[test]
fn a_session_holds_state_across_executions() {
    let mut fixture = Fixture::new();
    let runtime = fixture.runtime();
    let held = session("thread-7.kernel");
    let generation = runtime
        .open(&held, &fixture.spec(session_bounds()), NOW)
        .expect("the kernel opens");

    run(
        &runtime,
        &fixture,
        &key(&held, generation, "bind"),
        "set total 41\n",
        execution_bounds(60_000, 4096),
        NOW + 10,
    );
    let receipt = run(
        &runtime,
        &fixture,
        &key(&held, generation, "read"),
        "set total 42\nget total\n",
        execution_bounds(60_000, 4096),
        NOW + 20,
    );
    assert_eq!(receipt.sequence, 2);
    assert_eq!(
        fixture.captured(&receipt.stdout.expect("stdout was captured").artifact),
        b"total=42\n"
    );

    let closed = runtime.close(&held, NOW + 30).expect("the kernel closes");
    assert_eq!(closed.disposition, KernelDisposition::Closed);
    assert_eq!(closed.executions, 2);
    closed
        .verify(&closed.digest())
        .expect("a fresh session receipt addresses to its own digest");
}

/// A kernel that will not abandon a fragment is killed, and the receipt says
/// the kernel died rather than that the fragment was cancelled. Cancellation is
/// scoped by cooperation, and the disposition is where that shows.
#[test]
fn an_uncooperative_kernel_dies_rather_than_reporting_a_cancel() {
    let mut fixture = Fixture::new();
    let runtime = fixture.runtime();
    let stubborn = session("thread-7.stubborn");
    let spec = fixture
        .spec(session_bounds())
        .argument("--uncooperative")
        .expect("a valid argument");
    let generation = runtime
        .open(&stubborn, &spec, NOW)
        .expect("the kernel opens");

    let sleeping = key(&stubborn, generation, "sleeps");
    let submission = KernelSubmission::new("sleep 30000\n", execution_bounds(400, 4096))
        .expect("a valid submission");
    let sink = fixture.sink();
    runtime
        .submit(&sleeping, &submission, &sink, NOW + 10)
        .expect("the fragment is accepted");

    let receipt = runtime.wait(&sleeping).expect("the fragment settles");
    assert_eq!(
        receipt.disposition,
        KernelExecutionDisposition::KernelDied,
        "a kernel that ignored its interrupt reported a clean cancel",
    );
    let settled = runtime
        .session_receipt(&stubborn)
        .expect("the store answers")
        .expect("a session whose kernel died has a receipt");
    assert!(
        !matches!(settled.disposition, KernelDisposition::Closed),
        "a session whose kernel was killed claims it closed cleanly",
    );
}

/// A kernel image may refuse a snapshot. That is an answer, not a session:
/// nothing is started, and the Host is told to reconstruct from durable inputs
/// instead of being handed a process that quietly lost its state.
#[test]
fn a_refused_snapshot_starts_nothing() {
    let mut fixture = Fixture::new();
    let runtime = fixture.runtime();
    let refusing = session("thread-7.refusing");
    // The refusal is a property of the image, so the argument that causes it is
    // part of the spec digest and therefore part of both the checkpoint and the
    // restore. A Host cannot reach this answer by swapping images underneath.
    let spec = fixture
        .spec(session_bounds())
        .argument("--reject-restore")
        .expect("a valid argument");
    let generation = runtime
        .open(&refusing, &spec, NOW)
        .expect("the kernel opens");
    run(
        &runtime,
        &fixture,
        &key(&refusing, generation, "bind"),
        "set total 42\n",
        execution_bounds(60_000, 4096),
        NOW + 10,
    );
    let sink = fixture.sink();
    let checkpoint = runtime
        .checkpoint(&refusing, &sink, NOW + 20)
        .expect("the kernel checkpoints");
    assert_eq!(checkpoint.spec_digest, spec.spec_digest());
    runtime
        .close(&refusing, NOW + 30)
        .expect("the kernel closes");

    match runtime
        .restore(&refusing, &checkpoint, &spec, NOW + 40)
        .expect("the restore is answered")
    {
        KernelRestore::Rejected { reason } => {
            assert_eq!(reason.as_str(), "image-refused-snapshot");
        }
        other => panic!("an image that refuses snapshots answered {other:?}"),
    }
    assert!(
        runtime
            .inspect_session(&refusing)
            .expect("the store answers")
            .expect("a closed session is still known")
            .is_settled(),
        "a refused restore left a session behind",
    );
}

/// A session that sat idle past its declared window is settled by name, and the
/// next submission is refused rather than silently starting a new process.
#[test]
fn an_idle_session_expires_by_name() {
    let mut fixture = Fixture::new();
    let runtime = fixture.runtime();
    let idle = session("thread-7.idle");
    let bounds = KernelSessionBounds::new(1_000, 8, 4096).expect("valid session bounds");
    let generation = runtime
        .open(&idle, &fixture.spec(bounds), NOW)
        .expect("the kernel opens");

    let submission = KernelSubmission::new("echo late\n", execution_bounds(60_000, 4096))
        .expect("a valid submission");
    let sink = fixture.sink();
    let refused = runtime.submit(
        &key(&idle, generation, "late"),
        &submission,
        &sink,
        NOW + 5_000,
    );
    assert!(
        matches!(refused, Err(KernelError::NotLive(_))),
        "an expired session accepted a fragment: {refused:?}",
    );
    let receipt = runtime
        .session_receipt(&idle)
        .expect("the store answers")
        .expect("an expired session has a receipt");
    assert_eq!(receipt.disposition, KernelDisposition::IdleExpired);
}

// ---------------------------------------------------------------------------
// (2) The negative control: a backend that reports a lost session as a clean
// shutdown fails the suite.
// ---------------------------------------------------------------------------

/// Reconciles an orphan into a receipt that says the session closed cleanly and
/// that nothing was in flight — the exact optimism the contract exists to
/// forbid.
struct OptimisticKernelRuntime(Arc<LocalKernelRuntime>);

impl KernelRuntime for OptimisticKernelRuntime {
    fn open(
        &self,
        session: &KernelSessionId,
        spec: &KernelSpec,
        now_ms: u64,
    ) -> Result<KernelGeneration, KernelError> {
        self.0.open(session, spec, now_ms)
    }

    fn submit(
        &self,
        key: &KernelExecutionKey,
        submission: &KernelSubmission,
        sink: &Arc<dyn ProgramOutputSink>,
        now_ms: u64,
    ) -> Result<(), KernelError> {
        self.0.submit(key, submission, sink, now_ms)
    }

    fn wait(
        &self,
        key: &KernelExecutionKey,
    ) -> Result<grok_build_sdk::KernelExecutionReceipt, KernelError> {
        self.0.wait(key)
    }

    fn cancel(&self, key: &KernelExecutionKey) -> Result<(), KernelError> {
        self.0.cancel(key)
    }

    fn close(
        &self,
        session: &KernelSessionId,
        now_ms: u64,
    ) -> Result<KernelSessionReceipt, KernelError> {
        self.0.close(session, now_ms)
    }

    fn checkpoint(
        &self,
        session: &KernelSessionId,
        sink: &Arc<dyn ProgramOutputSink>,
        now_ms: u64,
    ) -> Result<grok_build_sdk::KernelCheckpointRef, KernelError> {
        self.0.checkpoint(session, sink, now_ms)
    }

    fn restore(
        &self,
        session: &KernelSessionId,
        checkpoint: &grok_build_sdk::KernelCheckpointRef,
        spec: &KernelSpec,
        now_ms: u64,
    ) -> Result<KernelRestore, KernelError> {
        self.0.restore(session, checkpoint, spec, now_ms)
    }

    fn inspect_session(
        &self,
        session: &KernelSessionId,
    ) -> Result<Option<KernelSessionStatus>, KernelError> {
        self.0.inspect_session(session)
    }

    fn inspect_execution(
        &self,
        key: &KernelExecutionKey,
    ) -> Result<Option<grok_build_sdk::KernelExecutionStatus>, KernelError> {
        self.0.inspect_execution(key)
    }

    fn requiring_reconciliation(&self) -> Result<Vec<KernelSessionId>, KernelError> {
        self.0.requiring_reconciliation()
    }

    fn reconcile(
        &self,
        session: &KernelSessionId,
        liveness: &dyn LivenessProbe,
        now_ms: u64,
    ) -> Result<KernelReconcileOutcome, KernelError> {
        let outcome = self.0.reconcile(session, liveness, now_ms)?;
        let KernelReconcileOutcome::Settled {
            session: receipt, ..
        } = outcome
        else {
            return Ok(outcome);
        };
        Ok(KernelReconcileOutcome::Settled {
            session: Box::new(KernelSessionReceipt {
                disposition: KernelDisposition::Closed,
                ..*receipt
            }),
            executions: Vec::new(),
        })
    }
}

struct OptimisticFixture(Fixture);

impl KernelRuntimeHarness for OptimisticFixture {
    fn open(&mut self, phase: ConformanceOpen) -> Result<Arc<dyn KernelRuntime>, KernelError> {
        let _ = phase;
        Ok(Arc::new(OptimisticKernelRuntime(self.0.runtime())))
    }

    fn spec(&mut self, bounds: KernelSessionBounds) -> Result<KernelSpec, KernelError> {
        KernelRuntimeHarness::spec(&mut self.0, bounds)
    }

    fn fragment(
        &mut self,
        script: KernelScript,
        bounds: KernelExecutionBounds,
    ) -> Result<KernelSubmission, KernelError> {
        KernelRuntimeHarness::fragment(&mut self.0, script, bounds)
    }

    fn sink(&mut self) -> Result<Arc<dyn ProgramOutputSink>, KernelError> {
        KernelRuntimeHarness::sink(&mut self.0)
    }

    fn abandon(&mut self, session: &KernelSessionId) -> Result<(), KernelError> {
        self.0.abandon(session)
    }

    fn damage(
        &mut self,
        session: &KernelSessionId,
        damage: KernelDamage,
    ) -> Result<(), KernelError> {
        KernelRuntimeHarness::damage(&mut self.0, session, damage)
    }

    fn durable_bytes(&mut self) -> Result<Vec<u8>, KernelError> {
        KernelRuntimeHarness::durable_bytes(&mut self.0)
    }

    fn captured(&mut self, artifact: &ArtifactHandle) -> Result<Vec<u8>, KernelError> {
        KernelRuntimeHarness::captured(&mut self.0, artifact)
    }
}

/// The suite is worth running only if it can fail. A backend that answers a
/// lost session with a clean shutdown is rejected.
#[test]
fn a_backend_that_fabricates_a_clean_shutdown_fails_the_suite() {
    let mut fixture = OptimisticFixture(Fixture::new());
    let outcome = run_kernel_runtime_conformance(&mut fixture);
    let error = outcome.expect_err("an optimistic backend passed the conformance suite");
    assert!(
        matches!(error, KernelError::Corrupt(_)),
        "the suite rejected an optimistic backend for the wrong reason: {error}",
    );
}

/// A backend that answers a lost session as still live is rejected too: a
/// session that never resolves is as much of a lie as one that resolves into
/// success.
#[test]
fn an_orphan_reported_as_live_is_not_a_settlement() {
    let mut fixture = Fixture::new();
    let runtime = fixture.runtime();
    let orphan = session("thread-7.orphan");
    let generation = runtime
        .open(&orphan, &fixture.spec(session_bounds()), NOW)
        .expect("the kernel opens");
    let sleeping = key(&orphan, generation, "sleeps");
    let submission = KernelSubmission::new(
        "sleep 30000\n",
        execution_bounds(MAX_KERNEL_EXECUTION_DEADLINE_MS, 4096),
    )
    .expect("a valid submission");
    let sink = fixture.sink();
    runtime
        .submit(&sleeping, &submission, &sink, NOW + 10)
        .expect("the fragment is accepted");
    runtime
        .abandon_for_test(&orphan)
        .expect("the session is abandoned");

    assert_eq!(
        runtime
            .reconcile(&orphan, &FixedProbe(Liveness::Live), NOW + 20)
            .expect("the probe is answered"),
        KernelReconcileOutcome::StillLive,
    );
    assert!(
        runtime
            .execution_receipt(&sleeping)
            .expect("the store answers")
            .is_none(),
        "a session reported as live settled its executions anyway",
    );

    let outcome = runtime
        .reconcile(&orphan, &FixedProbe(Liveness::Gone), NOW + 30)
        .expect("the probe is answered");
    let KernelReconcileOutcome::Settled {
        session: receipt,
        executions,
    } = outcome
    else {
        panic!("a gone orphan reconciled as {outcome:?}");
    };
    assert_eq!(receipt.disposition, KernelDisposition::Interrupted);
    assert_eq!(executions.len(), 1);
    assert_eq!(
        executions[0].disposition,
        KernelExecutionDisposition::Interrupted
    );
    assert!(
        executions[0].stdout.is_none(),
        "an interrupted execution claimed output nobody read",
    );
}
