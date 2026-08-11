// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    CaptureRecord, CredentialHandleName, CredentialResolver, ExecutionId, ExecutionReceipt,
    ExecutionStatus, ExitDisposition, Liveness, LivenessProbe, MAX_PROGRAM_CAPTURE_BYTES,
    MAX_PROGRAM_DEADLINE_MS, ProcessIdentity, ProgramBounds, ProgramError, ProgramLaunch,
    ProgramRuntime, ProgramStream, ReconcileOutcome, ResolvedCredential,
};
use crate::artifact::ArtifactHandle;
use crate::session_state::ConformanceOpen;
use std::sync::Arc;

type Runtime = Arc<dyn ProgramRuntime>;

/// The exact bytes a [`ProgramScript::Settles`] program writes to stdout, with
/// no trailing newline.
pub const CONFORMANCE_STDOUT_MARK: &str = "program conformance stdout";
/// The exact bytes a [`ProgramScript::Settles`] program writes to stderr, with
/// no trailing newline.
pub const CONFORMANCE_STDERR_MARK: &str = "program conformance stderr";
/// The status a [`ProgramScript::Fails`] program exits with.
pub const CONFORMANCE_EXIT_CODE: i32 = 7;
/// The least number of stdout bytes a [`ProgramScript::Floods`] program writes.
pub const CONFORMANCE_FLOOD_BYTES: u64 = 64 * 1024;
/// The environment variable a [`ProgramScript::ReportsCredentialLength`]
/// program reads its credential from.
pub const CONFORMANCE_CREDENTIAL_VARIABLE: &str = "PROGRAM_CONFORMANCE_TOKEN";
/// The credential handle name that program's launch attaches.
pub const CONFORMANCE_CREDENTIAL_HANDLE: &str = "program.conformance.token";
/// The secret this suite's resolver answers with. It must appear nowhere in
/// any receipt, durable row or captured artifact.
pub const CONFORMANCE_SECRET: &str = "conformance-secret-value";

/// The programs a harness supplies, described by what they do rather than by
/// how they are written.
///
/// The suite never writes a script itself: a Host proving a Windows backend
/// needs a different program text than one proving a POSIX backend, and the
/// contract under test is the runtime's, not the shell's. What the suite does
/// fix is the observable behaviour of each script, exactly, so the assertions
/// below are about the runtime rather than about scripting.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgramScript {
    /// Writes [`CONFORMANCE_STDOUT_MARK`] to stdout and
    /// [`CONFORMANCE_STDERR_MARK`] to stderr, each followed by exactly one
    /// newline, then exits 0.
    Settles,
    /// Writes nothing and exits with [`CONFORMANCE_EXIT_CODE`].
    Fails,
    /// Writes at least [`CONFORMANCE_FLOOD_BYTES`] bytes to stdout, then
    /// exits 0.
    Floods,
    /// Runs for at least thirty seconds unless it is stopped, writing nothing.
    Sleeps,
    /// Writes `len=<n>` and exactly one newline to stdout, where `n` is the
    /// byte length of [`CONFORMANCE_CREDENTIAL_VARIABLE`]'s value, then exits
    /// 0. It must never write the value itself.
    ReportsCredentialLength,
}

/// What the conformance suite needs from a backend beyond the contract itself.
pub trait ProgramRuntimeHarness {
    /// Answers a handle onto one authority for each phase. `Fresh` and
    /// `Concurrent` must observe each other's durable state; `Reopen` is taken
    /// after the earlier handles were dropped and stands in for a restart.
    fn open(&mut self, phase: ConformanceOpen) -> Result<Runtime, ProgramError>;

    /// A launch that runs the described program under the given bounds, in a
    /// working root the backend owns. A [`ProgramScript::ReportsCredentialLength`]
    /// launch must bind [`CONFORMANCE_CREDENTIAL_VARIABLE`] to
    /// [`CONFORMANCE_CREDENTIAL_HANDLE`] and must not declare the value as a
    /// literal.
    fn launch(
        &mut self,
        script: ProgramScript,
        bounds: ProgramBounds,
    ) -> Result<ProgramLaunch, ProgramError>;

    /// Stops tracking a running execution and terminates its process without
    /// settling the durable record, leaving the store exactly as a crash would.
    /// An orphan is unreachable through the contract by design, so a backend
    /// that claims to detect one has to be able to cause one.
    fn abandon(&mut self, execution: &ExecutionId) -> Result<(), ProgramError>;

    /// Every byte the backend has durably written for its execution state. The
    /// suite reads it to prove that no secret is in there.
    fn durable_bytes(&mut self) -> Result<Vec<u8>, ProgramError>;

    /// The captured content behind an artifact handle a receipt cites.
    fn captured(&mut self, artifact: &ArtifactHandle) -> Result<Vec<u8>, ProgramError>;
}

fn suite_error(message: impl Into<String>) -> ProgramError {
    ProgramError::Corrupt(format!("conformance: {}", message.into()))
}

fn require(condition: bool, message: impl Into<String>) -> Result<(), ProgramError> {
    if condition {
        Ok(())
    } else {
        Err(suite_error(message))
    }
}

fn expect<T>(value: Option<T>, message: &str) -> Result<T, ProgramError> {
    value.ok_or_else(|| suite_error(message))
}

const BASE_MS: u64 = 1_700_000_000_000;
const SETTLED: &str = "conformance.settled";
const REFUSED: &str = "conformance.refused";
const FAILING: &str = "conformance.failing";
const FLOODING: &str = "conformance.flooding";
const CREDENTIALLED: &str = "conformance.credentialled";
const CANCELLED: &str = "conformance.cancelled";
const TIMED_OUT: &str = "conformance.timed-out";
const SHARED: &str = "conformance.shared";
const ORPHANED: &str = "conformance.orphaned";

/// Answers only the handle this suite declares, so a backend that spawns with a
/// missing or invented credential is caught by the program itself.
struct SuiteResolver;

impl CredentialResolver for SuiteResolver {
    fn resolve(&self, handle: &CredentialHandleName) -> Result<ResolvedCredential, ProgramError> {
        if handle.as_str() != CONFORMANCE_CREDENTIAL_HANDLE {
            return Err(ProgramError::Validation(format!(
                "conformance: unknown credential handle {handle}"
            )));
        }
        ResolvedCredential::new(CONFORMANCE_SECRET)
    }
}

/// Stands in for a keychain that has locked, expired or lost the secret.
struct RefusingResolver;

impl CredentialResolver for RefusingResolver {
    fn resolve(&self, _: &CredentialHandleName) -> Result<ResolvedCredential, ProgramError> {
        Err(ProgramError::Validation(
            "conformance: the credential authority refused".into(),
        ))
    }
}

/// A launch with no credential bindings still needs a resolver argument; this
/// one proves the backend never calls it.
struct UnusedResolver;

impl CredentialResolver for UnusedResolver {
    fn resolve(&self, handle: &CredentialHandleName) -> Result<ResolvedCredential, ProgramError> {
        Err(ProgramError::Validation(format!(
            "conformance: a launch with no credential bindings resolved {handle}"
        )))
    }
}

/// A probe with a fixed answer, so the suite tests the runtime's reaction to
/// each liveness verdict rather than the platform's ability to produce it.
struct ScriptedProbe(Liveness);

impl LivenessProbe for ScriptedProbe {
    fn probe(&self, _: &ProcessIdentity) -> Result<Liveness, ProgramError> {
        Ok(self.0)
    }
}

fn bounds(deadline_ms: u64, capture: u64) -> Result<ProgramBounds, ProgramError> {
    ProgramBounds::new(deadline_ms, capture, capture)
}

fn identity(value: &str) -> Result<ExecutionId, ProgramError> {
    ExecutionId::new(value)
}

/// Runs the backend-neutral program execution contract against independent
/// handles to one authority.
///
/// The contract this proves is the one a Host depends on when it tells a person
/// what a program did: every execution is named before it runs and receipted
/// after it settles, the receipt is digest-verified and replaying a settle
/// answers rather than re-runs, cancellation and an elapsed deadline settle
/// honestly instead of as success, output beyond a declared bound is a recorded
/// truncation rather than a silent gap, an attached credential reaches the
/// process and reaches nothing else, and an execution that was running when the
/// Host died is either found alive, settled as interrupted, or left uncertain —
/// never reported as having succeeded.
pub fn run_program_runtime_conformance<H: ProgramRuntimeHarness>(
    harness: &mut H,
) -> Result<(), ProgramError> {
    let runtime = harness.open(ConformanceOpen::Fresh)?;
    let settled = an_execution_is_named_then_receipted(&runtime, harness)?;
    a_settle_is_replayed_rather_than_repeated(&runtime, &settled)?;
    declared_bounds_are_validated_before_anything_runs(&runtime, harness)?;
    a_failure_is_reported_as_a_failure(&runtime, harness)?;
    output_beyond_its_bound_is_recorded_truncation(&runtime, harness)?;
    a_credential_reaches_the_process_and_nothing_else(&runtime, harness)?;
    a_cancelled_execution_settles_as_cancelled(&runtime, harness)?;
    an_elapsed_deadline_settles_as_timed_out(&runtime, harness)?;

    let concurrent = harness.open(ConformanceOpen::Concurrent)?;
    another_handle_sees_an_execution_it_does_not_own(&runtime, &concurrent, harness)?;

    let orphan = identity(ORPHANED)?;
    runtime.launch(
        &orphan,
        &harness.launch(
            ProgramScript::Sleeps,
            bounds(MAX_PROGRAM_DEADLINE_MS, 4096)?,
        )?,
        &UnusedResolver,
        BASE_MS + 9_000,
    )?;
    harness.abandon(&orphan)?;
    drop(concurrent);
    drop(runtime);

    let reopened = harness.open(ConformanceOpen::Reopen)?;
    a_restart_surfaces_the_crash_time_backlog(&reopened, &orphan)?;
    an_uncertain_probe_leaves_an_execution_uncertain(&reopened, &orphan)?;
    an_orphan_settles_as_interrupted(&reopened, &orphan)?;
    a_receipt_survives_a_restart_unchanged(&reopened, &settled)?;
    Ok(())
}

/// An execution is named by its caller before it exists, and a settled
/// execution answers a receipt that says what ran, under what, and how it
/// ended.
fn an_execution_is_named_then_receipted<H: ProgramRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<ExecutionReceipt, ProgramError> {
    let execution = identity(SETTLED)?;
    require(
        runtime.inspect(&execution)?.is_none() && runtime.receipt(&execution)?.is_none(),
        "a fresh authority already knows an execution",
    )?;
    require(
        matches!(runtime.wait(&execution), Err(ProgramError::NotFound(_)))
            && matches!(runtime.cancel(&execution), Err(ProgramError::NotFound(_))),
        "an authority answered for an execution it was never given",
    )?;

    let launch = harness.launch(ProgramScript::Settles, bounds(60_000, 4096)?)?;
    let process = runtime.launch(&execution, &launch, &UnusedResolver, BASE_MS)?;
    require(
        process.pid() != 0,
        "a launched execution has no process identity",
    )?;
    match expect(
        runtime.inspect(&execution)?,
        "a launched execution vanished",
    )? {
        ExecutionStatus::Running {
            started_at_ms,
            process: recorded,
        } => require(
            started_at_ms == BASE_MS && recorded == process,
            "a running execution misreported its start instant or process",
        )?,
        other => {
            return Err(suite_error(format!(
                "a launched execution reported {other:?} rather than running"
            )));
        }
    }
    require(
        matches!(
            runtime.launch(&execution, &launch, &UnusedResolver, BASE_MS + 1),
            Err(ProgramError::Conflict(_))
        ),
        "one execution identity admitted a second process",
    )?;

    let receipt = runtime.wait(&execution)?;
    require(
        receipt.execution == execution
            && receipt.program == *launch.program()
            && receipt.arguments_digest == launch.arguments_digest()
            && receipt.environment_digest == launch.environment_digest()
            && receipt.working_root == launch.working_root()
            && receipt.bounds == launch.bounds(),
        "a receipt did not name the program, arguments, environment, working root and bounds it ran with",
    )?;
    require(
        receipt.disposition == ExitDisposition::Exited { code: 0 } && receipt.succeeded(),
        format!(
            "a program that exits zero settled as {:?}",
            receipt.disposition
        ),
    )?;
    require(
        receipt.started_at_ms == BASE_MS && receipt.settled_at_ms >= BASE_MS,
        "a receipt did not carry the wall instants its caller declared",
    )?;
    require(
        receipt.duration_ms() == receipt.settled_at_ms - receipt.started_at_ms,
        "a receipt's duration disagrees with its own instants",
    )?;

    let stdout = expect(receipt.stdout.clone(), "a receipt captured no stdout")?;
    let stderr = expect(receipt.stderr.clone(), "a receipt captured no stderr")?;
    require(
        stdout.stream == ProgramStream::Stdout && stderr.stream == ProgramStream::Stderr,
        "a receipt bound its captures to the wrong streams",
    )?;
    require(
        !stdout.truncated && !stderr.truncated && !receipt.truncated(),
        "a program well inside its bounds reported truncation",
    )?;
    require(
        harness.captured(&stdout.artifact)? == format!("{CONFORMANCE_STDOUT_MARK}\n").as_bytes()
            && harness.captured(&stderr.artifact)?
                == format!("{CONFORMANCE_STDERR_MARK}\n").as_bytes(),
        "captured output is not the output the program produced",
    )?;
    require(
        stdout
            .artifact
            .addresses(&harness.captured(&stdout.artifact)?),
        "a captured stream's artifact handle does not address its own content",
    )?;
    receipt.verify(&receipt.digest())?;
    Ok(receipt)
}

/// A settled execution has one answer. Asking again replays it; nothing runs
/// twice and nothing is rewritten.
fn a_settle_is_replayed_rather_than_repeated(
    runtime: &Runtime,
    settled: &ExecutionReceipt,
) -> Result<(), ProgramError> {
    let replayed = runtime.wait(&settled.execution)?;
    require(
        &replayed == settled && replayed.digest() == settled.digest(),
        "waiting on a settled execution did not replay its receipt unchanged",
    )?;
    require(
        expect(
            runtime.receipt(&settled.execution)?,
            "a settled execution has no receipt",
        )? == *settled,
        "a receipt read directly disagrees with the receipt a wait answered",
    )?;
    require(
        expect(
            runtime.inspect(&settled.execution)?,
            "a settled execution vanished",
        )?
        .is_settled(),
        "a settled execution did not inspect as settled",
    )?;
    runtime.cancel(&settled.execution)?;
    require(
        expect(
            runtime.receipt(&settled.execution)?,
            "cancelling a settled execution erased its receipt",
        )? == *settled,
        "cancelling a settled execution rewrote the settlement that already happened",
    )?;
    Ok(())
}

/// Bounds are declared at launch and checked there, and a launch that cannot be
/// honoured leaves nothing behind.
fn declared_bounds_are_validated_before_anything_runs<H: ProgramRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), ProgramError> {
    require(
        ProgramBounds::new(0, 4096, 4096).is_err(),
        "a launch with no deadline was accepted, so nothing would ever settle it",
    )?;
    require(
        ProgramBounds::new(MAX_PROGRAM_DEADLINE_MS + 1, 4096, 4096).is_err(),
        "a deadline beyond the published bound was accepted",
    )?;
    require(
        ProgramBounds::new(60_000, MAX_PROGRAM_CAPTURE_BYTES + 1, 4096).is_err()
            && ProgramBounds::new(60_000, 4096, MAX_PROGRAM_CAPTURE_BYTES + 1).is_err(),
        "a capture bound beyond the published bound was accepted",
    )?;

    let execution = identity(REFUSED)?;
    let launch = harness.launch(
        ProgramScript::ReportsCredentialLength,
        bounds(60_000, 4096)?,
    )?;
    require(
        runtime
            .launch(&execution, &launch, &RefusingResolver, BASE_MS + 100)
            .is_err(),
        "a launch whose credential could not be resolved started anyway",
    )?;
    require(
        runtime.inspect(&execution)?.is_none(),
        "a refused launch left durable state behind",
    )?;
    Ok(())
}

/// A non-zero exit is reported as the status it was, and is never success.
fn a_failure_is_reported_as_a_failure<H: ProgramRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), ProgramError> {
    let execution = identity(FAILING)?;
    let launch = harness.launch(ProgramScript::Fails, bounds(60_000, 4096)?)?;
    runtime.launch(&execution, &launch, &UnusedResolver, BASE_MS + 200)?;
    let receipt = runtime.wait(&execution)?;
    require(
        receipt.disposition
            == ExitDisposition::Exited {
                code: CONFORMANCE_EXIT_CODE,
            },
        format!(
            "a program exiting {CONFORMANCE_EXIT_CODE} settled as {:?}",
            receipt.disposition
        ),
    )?;
    require(
        !receipt.succeeded(),
        "a non-zero exit was reported as success",
    )?;
    Ok(())
}

/// Output past a declared bound is a fact on the receipt, not a silent loss.
fn output_beyond_its_bound_is_recorded_truncation<H: ProgramRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), ProgramError> {
    let execution = identity(FLOODING)?;
    let bound = 1_024;
    let launch = harness.launch(ProgramScript::Floods, bounds(60_000, bound)?)?;
    runtime.launch(&execution, &launch, &UnusedResolver, BASE_MS + 300)?;
    let receipt = runtime.wait(&execution)?;
    require(
        receipt.disposition == ExitDisposition::Exited { code: 0 },
        "a program that outran its capture bound was not allowed to finish",
    )?;
    let stdout = expect(
        receipt.stdout.clone(),
        "a flooding execution captured no stdout",
    )?;
    require(
        stdout.declared_bound == bound && stdout.captured_bytes == bound,
        format!(
            "a capture kept {} bytes under a {bound}-byte bound",
            stdout.captured_bytes
        ),
    )?;
    require(
        stdout.truncated && receipt.truncated(),
        "output lost to a bound was not recorded as truncation",
    )?;
    require(
        stdout.produced_bytes >= CONFORMANCE_FLOOD_BYTES
            && stdout.produced_bytes > stdout.captured_bytes,
        "a truncated capture did not report how much the program actually wrote",
    )?;
    require(
        harness.captured(&stdout.artifact)?.len() as u64 == stdout.captured_bytes,
        "a truncated capture's artifact does not hold the bytes the receipt claims",
    )?;
    check_capture_consistency(&stdout)?;
    Ok(())
}

fn check_capture_consistency(capture: &CaptureRecord) -> Result<(), ProgramError> {
    if capture.captured_bytes > capture.declared_bound
        || capture.captured_bytes > capture.produced_bytes
        || capture.truncated != (capture.produced_bytes > capture.captured_bytes)
    {
        return Err(suite_error(
            "a capture record's counts and truncation flag disagree with each other",
        ));
    }
    Ok(())
}

/// A credential handle names a secret; it never carries one. The value reaches
/// the process and appears in no receipt, no durable row and no captured
/// artifact.
fn a_credential_reaches_the_process_and_nothing_else<H: ProgramRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), ProgramError> {
    let execution = identity(CREDENTIALLED)?;
    let launch = harness.launch(
        ProgramScript::ReportsCredentialLength,
        bounds(60_000, 4096)?,
    )?;
    let handle = CredentialHandleName::new(CONFORMANCE_CREDENTIAL_HANDLE)?;
    require(
        launch.credential_handles() == [handle.clone()],
        "a credential launch did not attach exactly the declared handle",
    )?;
    runtime.launch(&execution, &launch, &SuiteResolver, BASE_MS + 400)?;
    let receipt = runtime.wait(&execution)?;
    require(
        receipt.succeeded(),
        "a program given its credential did not succeed",
    )?;

    let stdout = expect(
        receipt.stdout.clone(),
        "a credential execution captured no stdout",
    )?;
    let captured = harness.captured(&stdout.artifact)?;
    require(
        captured == format!("len={}\n", CONFORMANCE_SECRET.len()).as_bytes(),
        "the resolved credential did not reach the process with its declared value",
    )?;
    require(
        receipt.credential_handles == [handle],
        "a receipt did not name the credential handles the launch attached",
    )?;

    let encoded = serde_json::to_string(&receipt)
        .map_err(|error| suite_error(format!("a receipt is not serializable: {error}")))?;
    require(
        !encoded.contains(CONFORMANCE_SECRET),
        "a receipt carries the secret it was launched with",
    )?;
    let secret = CONFORMANCE_SECRET.as_bytes();
    require(
        !contains(&harness.durable_bytes()?, secret),
        "durable execution state carries the secret the launch resolved",
    )?;
    require(
        !contains(&captured, secret),
        "captured output carries the secret the launch resolved",
    )?;
    Ok(())
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// A caller that changes its mind gets an honest cancellation, not a
/// success and not a hang.
fn a_cancelled_execution_settles_as_cancelled<H: ProgramRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), ProgramError> {
    let execution = identity(CANCELLED)?;
    let launch = harness.launch(
        ProgramScript::Sleeps,
        bounds(MAX_PROGRAM_DEADLINE_MS, 4096)?,
    )?;
    runtime.launch(&execution, &launch, &UnusedResolver, BASE_MS + 500)?;
    runtime.cancel(&execution)?;
    // A repeated cancel is an answer, not a second effect.
    runtime.cancel(&execution)?;
    let receipt = runtime.wait(&execution)?;
    require(
        receipt.disposition == ExitDisposition::Cancelled,
        format!("a cancelled execution settled as {:?}", receipt.disposition),
    )?;
    require(
        !receipt.succeeded(),
        "a cancelled execution was reported as success",
    )?;
    require(
        receipt.settled_at_ms >= receipt.started_at_ms,
        "a cancelled execution settled before it started",
    )?;
    Ok(())
}

/// A declared deadline is enforced by the runtime, and the execution it stops
/// says so.
fn an_elapsed_deadline_settles_as_timed_out<H: ProgramRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), ProgramError> {
    let execution = identity(TIMED_OUT)?;
    let launch = harness.launch(ProgramScript::Sleeps, bounds(250, 4096)?)?;
    require(
        launch.bounds().deadline_ms() == 250,
        "a harness ignored the deadline the suite declared",
    )?;
    runtime.launch(&execution, &launch, &UnusedResolver, BASE_MS + 600)?;
    let receipt = runtime.wait(&execution)?;
    require(
        receipt.disposition == ExitDisposition::TimedOut,
        format!(
            "an execution past its deadline settled as {:?}",
            receipt.disposition
        ),
    )?;
    require(
        !receipt.succeeded(),
        "an execution stopped by its deadline was reported as success",
    )?;
    require(
        receipt.bounds.deadline_ms() == 250,
        "a timed-out receipt did not report the deadline that stopped it",
    )?;
    Ok(())
}

/// Two handles on one authority are one authority. The handle that did not
/// launch an execution can see it, must not claim to know its fate, and must
/// not settle it while it is alive.
fn another_handle_sees_an_execution_it_does_not_own<H: ProgramRuntimeHarness>(
    runtime: &Runtime,
    concurrent: &Runtime,
    harness: &mut H,
) -> Result<(), ProgramError> {
    let settled = identity(SETTLED)?;
    require(
        expect(
            concurrent.receipt(&settled)?,
            "a concurrent handle cannot see a settled execution",
        )? == runtime.wait(&settled)?,
        "two handles on one authority disagree about a settled receipt",
    )?;

    let execution = identity(SHARED)?;
    let launch = harness.launch(
        ProgramScript::Sleeps,
        bounds(MAX_PROGRAM_DEADLINE_MS, 4096)?,
    )?;
    runtime.launch(&execution, &launch, &UnusedResolver, BASE_MS + 700)?;
    match expect(
        concurrent.inspect(&execution)?,
        "a concurrent handle cannot see a running execution",
    )? {
        ExecutionStatus::Uncertain { started_at_ms, .. } => require(
            started_at_ms == BASE_MS + 700,
            "an uncertain execution misreported when it started",
        )?,
        other => {
            return Err(suite_error(format!(
                "a handle that did not launch an execution reported {other:?} rather than uncertain"
            )));
        }
    }
    require(
        matches!(concurrent.wait(&execution), Err(ProgramError::Unowned(_))),
        "a handle waited on an execution it did not launch instead of reconciling it",
    )?;
    require(
        concurrent.requiring_reconciliation()?.contains(&execution),
        "an execution owned by another handle was not offered for reconciliation",
    )?;
    require(
        concurrent.reconcile(&execution, &ScriptedProbe(Liveness::Live), BASE_MS + 800)?
            == ReconcileOutcome::StillRunning,
        "reconciling a live execution did not answer that it is still running",
    )?;
    require(
        concurrent.receipt(&execution)?.is_none(),
        "reconciling a live execution settled it anyway",
    )?;
    require(
        !runtime.requiring_reconciliation()?.contains(&execution),
        "a handle offered its own running execution for reconciliation",
    )?;

    runtime.cancel(&execution)?;
    let receipt = runtime.wait(&execution)?;
    require(
        receipt.disposition == ExitDisposition::Cancelled,
        "an execution cancelled by its owner did not settle as cancelled",
    )?;
    require(
        concurrent.reconcile(&execution, &ScriptedProbe(Liveness::Gone), BASE_MS + 900)?
            == ReconcileOutcome::Settled(Box::new(receipt)),
        "reconciling a settled execution did not replay its receipt",
    )?;
    Ok(())
}

/// A restart finds exactly the executions that were running when the previous
/// owner stopped, and finds them as uncertain rather than as anything else.
fn a_restart_surfaces_the_crash_time_backlog(
    runtime: &Runtime,
    orphan: &ExecutionId,
) -> Result<(), ProgramError> {
    require(
        runtime.requiring_reconciliation()? == vec![orphan.clone()],
        "a reopened authority did not surface exactly the crash-time backlog",
    )?;
    match expect(
        runtime.inspect(orphan)?,
        "an execution did not survive a restart",
    )? {
        ExecutionStatus::Uncertain { started_at_ms, .. } => require(
            started_at_ms == BASE_MS + 9_000,
            "a reopened execution misreported when it started",
        )?,
        other => {
            return Err(suite_error(format!(
                "an execution that was running at a crash reopened as {other:?}"
            )));
        }
    }
    require(
        matches!(runtime.wait(orphan), Err(ProgramError::Unowned(_))),
        "a reopened authority waited on a process it does not own",
    )?;
    Ok(())
}

/// An inconclusive probe is a real answer. Nothing is settled and nothing is
/// forgotten.
fn an_uncertain_probe_leaves_an_execution_uncertain(
    runtime: &Runtime,
    orphan: &ExecutionId,
) -> Result<(), ProgramError> {
    require(
        runtime.reconcile(orphan, &ScriptedProbe(Liveness::Unknown), BASE_MS + 10_000)?
            == ReconcileOutcome::Uncertain,
        "an inconclusive probe did not leave the execution uncertain",
    )?;
    require(
        runtime.receipt(orphan)?.is_none() && runtime.requiring_reconciliation()?.contains(orphan),
        "an inconclusive probe settled or forgot an execution it could not establish",
    )?;
    Ok(())
}

/// A process that is gone settles as interrupted. It never settles as success,
/// and it never claims to have captured output nobody read.
fn an_orphan_settles_as_interrupted(
    runtime: &Runtime,
    orphan: &ExecutionId,
) -> Result<(), ProgramError> {
    let outcome = runtime.reconcile(orphan, &ScriptedProbe(Liveness::Gone), BASE_MS + 11_000)?;
    let ReconcileOutcome::Settled(receipt) = outcome else {
        return Err(suite_error(format!(
            "reconciling an orphan answered {outcome:?} rather than settling it"
        )));
    };
    require(
        receipt.disposition == ExitDisposition::Interrupted,
        format!("an orphan settled as {:?}", receipt.disposition),
    )?;
    require(
        !receipt.succeeded(),
        "an orphan was settled as a successful execution",
    )?;
    require(
        receipt.stdout.is_none() && receipt.stderr.is_none() && !receipt.truncated(),
        "an orphan's receipt claims captured output that nobody read",
    )?;
    require(
        receipt.execution == *orphan && receipt.settled_at_ms >= receipt.started_at_ms,
        "an orphan's receipt misnames the execution or settles before it started",
    )?;
    receipt.verify(&receipt.digest())?;

    require(
        !runtime.requiring_reconciliation()?.contains(orphan),
        "a reconciled orphan is still offered for reconciliation",
    )?;
    let again = runtime.reconcile(orphan, &ScriptedProbe(Liveness::Gone), BASE_MS + 12_000)?;
    require(
        again == ReconcileOutcome::Settled(receipt.clone()),
        "reconciling a settled orphan twice produced a second settlement",
    )?;
    require(
        expect(
            runtime.receipt(orphan)?,
            "a settled orphan lost its receipt",
        )? == *receipt,
        "a settled orphan's stored receipt disagrees with the one reconciliation answered",
    )?;
    Ok(())
}

/// A receipt is append-only: a restart returns exactly the bytes that were
/// written, and they still address to their own digest.
fn a_receipt_survives_a_restart_unchanged(
    runtime: &Runtime,
    settled: &ExecutionReceipt,
) -> Result<(), ProgramError> {
    let after = expect(
        runtime.receipt(&settled.execution)?,
        "a receipt did not survive a restart",
    )?;
    require(
        after == *settled && after.digest() == settled.digest(),
        "a receipt changed across a restart",
    )?;
    after.verify(&settled.digest())?;
    require(
        expect(
            runtime.inspect(&settled.execution)?,
            "a settled execution vanished across a restart",
        )?
        .is_settled(),
        "a settled execution reopened as something other than settled",
    )?;
    Ok(())
}
