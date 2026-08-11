// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    KERNEL_RESERVED_ENVIRONMENT_NAMES, KernelCheckpointRef, KernelDisposition, KernelError,
    KernelExecutionBounds, KernelExecutionDisposition, KernelExecutionId, KernelExecutionKey,
    KernelExecutionReceipt, KernelExecutionStatus, KernelGeneration, KernelReconcileOutcome,
    KernelRestore, KernelRuntime, KernelSessionBounds, KernelSessionId, KernelSessionStatus,
    KernelSpec, KernelSubmission, MAX_KERNEL_CAPTURE_BYTES, MAX_KERNEL_EXECUTION_DEADLINE_MS,
    MAX_KERNEL_SOURCE_BYTES,
};
use crate::artifact::ArtifactHandle;
use crate::program::{Liveness, LivenessProbe, ProcessIdentity, ProgramError, ProgramOutputSink};
use crate::session_state::ConformanceOpen;
use std::sync::Arc;

type Runtime = Arc<dyn KernelRuntime>;
type Sink = Arc<dyn ProgramOutputSink>;

/// The exact bytes a [`KernelScript::Settles`] fragment writes to stdout,
/// followed by exactly one newline.
pub const CONFORMANCE_KERNEL_STDOUT_MARK: &str = "kernel conformance stdout";
/// The exact bytes a [`KernelScript::Settles`] fragment writes to stderr,
/// followed by exactly one newline.
pub const CONFORMANCE_KERNEL_STDERR_MARK: &str = "kernel conformance stderr";
/// The name a [`KernelScript::BindsState`] fragment binds.
pub const CONFORMANCE_KERNEL_STATE_NAME: &str = "conformance_state";
/// The value it binds to that name.
pub const CONFORMANCE_KERNEL_STATE_VALUE: &str = "conformance-state-value";
/// The class a [`KernelScript::Raises`] fragment raises with.
pub const CONFORMANCE_KERNEL_ERROR_CLASS: &str = "ConformanceError";
/// The least number of stdout bytes a [`KernelScript::Floods`] fragment writes.
pub const CONFORMANCE_KERNEL_FLOOD_BYTES: u64 = 64 * 1024;
/// A secret the harness plants in the session's environment. It must appear
/// nowhere in durable state, in any receipt, or in any captured artifact.
pub const CONFORMANCE_KERNEL_SECRET: &str = "kernel-conformance-secret-value";

/// The fragments a harness supplies, described by what they do rather than by
/// how they are written.
///
/// The suite never writes kernel source itself: a Host proving a Python kernel
/// and a Host proving a shell kernel need different text, and the contract
/// under test is the runtime's. What is fixed here, exactly, is the observable
/// behaviour of each fragment.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KernelScript {
    /// Writes [`CONFORMANCE_KERNEL_STDOUT_MARK`] to stdout and
    /// [`CONFORMANCE_KERNEL_STDERR_MARK`] to stderr, each with exactly one
    /// trailing newline, and completes.
    Settles,
    /// Binds [`CONFORMANCE_KERNEL_STATE_NAME`] to
    /// [`CONFORMANCE_KERNEL_STATE_VALUE`] in session state and completes,
    /// writing nothing.
    BindsState,
    /// Writes `<name>=<value>` and one newline to stdout for the name
    /// [`KernelScript::BindsState`] binds, then completes. An unbound name
    /// writes an empty value rather than raising.
    ReadsState,
    /// Raises [`CONFORMANCE_KERNEL_ERROR_CLASS`].
    Raises,
    /// Writes at least [`CONFORMANCE_KERNEL_FLOOD_BYTES`] to stdout, then
    /// completes.
    Floods,
    /// Runs for at least thirty seconds unless it is stopped, writing nothing.
    Sleeps,
    /// Takes hold of state the kernel cannot serialise — an open file handle
    /// will do — and completes, so a following checkpoint has a
    /// non-restorable fact to declare.
    HoldsUnserialisableState,
}

/// How the suite damages durable state to prove a backend refuses it.
///
/// Both are unreachable through the contract by design, which is exactly why a
/// backend that claims to detect them has to be able to cause them.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KernelDamage {
    /// Edits a stored receipt so it no longer addresses to its own digest,
    /// leaving every other column intact.
    Receipt,
    /// Replaces a stored session row with bytes the backend cannot decode
    /// within its published bounds.
    Record,
}

/// What the conformance suite needs from a backend beyond the contract itself.
pub trait KernelRuntimeHarness {
    /// Answers a handle onto one authority for each phase. `Fresh` and
    /// `Concurrent` must observe each other's durable state; `Reopen` is taken
    /// after the earlier handles were dropped and stands in for a restart.
    fn open(&mut self, phase: ConformanceOpen) -> Result<Runtime, KernelError>;

    /// A spec for the backend's kernel image under the given bounds, in a
    /// working root the backend owns.
    ///
    /// It must bind [`CONFORMANCE_KERNEL_SECRET`] as a literal environment
    /// value under a name of the harness's choosing. That is what makes
    /// `no_secret_reaches_durable_state` a real test rather than a vacuous one:
    /// the suite plants the secret through the only door the contract has, and
    /// then proves the backend did not write it down.
    fn spec(&mut self, bounds: KernelSessionBounds) -> Result<KernelSpec, KernelError>;

    /// The source of one described fragment under the given bounds.
    fn fragment(
        &mut self,
        script: KernelScript,
        bounds: KernelExecutionBounds,
    ) -> Result<KernelSubmission, KernelError>;

    /// The sink captured output is stored in.
    fn sink(&mut self) -> Result<Sink, KernelError>;

    /// Kills a session's kernel process without settling its durable record,
    /// leaving the store exactly as a crash would.
    fn abandon(&mut self, session: &KernelSessionId) -> Result<(), KernelError>;

    /// Damages durable state in the described way.
    fn damage(
        &mut self,
        session: &KernelSessionId,
        damage: KernelDamage,
    ) -> Result<(), KernelError>;

    /// Every byte the backend has durably written for its kernel state. The
    /// suite reads it to prove no secret is in there.
    fn durable_bytes(&mut self) -> Result<Vec<u8>, KernelError>;

    /// The captured content behind an artifact handle a receipt or checkpoint
    /// cites.
    fn captured(&mut self, artifact: &ArtifactHandle) -> Result<Vec<u8>, KernelError>;
}

fn suite_error(message: impl Into<String>) -> KernelError {
    KernelError::Corrupt(format!("conformance: {}", message.into()))
}

fn require(condition: bool, message: impl Into<String>) -> Result<(), KernelError> {
    if condition {
        Ok(())
    } else {
        Err(suite_error(message))
    }
}

fn expect<T>(value: Option<T>, message: &str) -> Result<T, KernelError> {
    value.ok_or_else(|| suite_error(message))
}

const BASE_MS: u64 = 1_700_000_000_000;
const OPENED: &str = "conformance.opened";
const STATEFUL: &str = "conformance.stateful";
const RESTORED: &str = "conformance.restored";
const LOSSY: &str = "conformance.lossy";
const CEILINGED: &str = "conformance.ceilinged";
const ORPHANED: &str = "conformance.orphaned";
const DAMAGED: &str = "conformance.damaged";

/// A probe with a fixed answer, so the suite tests the runtime's reaction to
/// each liveness verdict rather than the platform's ability to produce it.
struct ScriptedProbe(Liveness);

impl LivenessProbe for ScriptedProbe {
    fn probe(&self, _: &ProcessIdentity) -> Result<Liveness, ProgramError> {
        Ok(self.0)
    }
}

fn session(value: &str) -> Result<KernelSessionId, KernelError> {
    KernelSessionId::new(value)
}

fn key(
    session: &KernelSessionId,
    generation: KernelGeneration,
    execution: &str,
) -> Result<KernelExecutionKey, KernelError> {
    KernelExecutionKey::new(
        session.clone(),
        generation,
        KernelExecutionId::new(execution)?,
    )
}

fn session_bounds() -> Result<KernelSessionBounds, KernelError> {
    KernelSessionBounds::new(MAX_KERNEL_EXECUTION_DEADLINE_MS, 64, 16 * 1024 * 1024)
}

fn execution_bounds(deadline_ms: u64, capture: u64) -> Result<KernelExecutionBounds, KernelError> {
    KernelExecutionBounds::new(deadline_ms, capture, capture)
}

/// Runs one fragment to settlement and answers its receipt.
fn run<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    key: &KernelExecutionKey,
    script: KernelScript,
    bounds: KernelExecutionBounds,
    now_ms: u64,
) -> Result<KernelExecutionReceipt, KernelError> {
    let submission = harness.fragment(script, bounds)?;
    let sink = harness.sink()?;
    runtime.submit(key, &submission, &sink, now_ms)?;
    runtime.wait(key)
}

/// Runs the backend-neutral persistent kernel contract against independent
/// handles to one authority.
///
/// The contract this proves is the one a Host depends on when it tells a person
/// what a long-lived kernel did: a session is named before it exists and
/// receipted after it settles, executions inside it are ordered and share
/// state, a raise is a raise and a truncation is a recorded truncation, a
/// cancel scoped to one fragment leaves the session usable, a checkpoint
/// addresses its own payload and enumerates what it could not carry, a restore
/// hands that loss back rather than presenting a session as whole, and a
/// session that was live when the Host died is found alive, settled as
/// interrupted, or left uncertain — never reported as having succeeded.
///
/// Nine of the ten negative controls in the design are here. The tenth,
/// *a fenced workflow driver cannot commit*, constrains the driver rather than
/// the backend and is proved beside the driver itself, in
/// [`crate::WorkflowDriver`]'s own controls; a backend has no fence to be
/// stale.
pub fn run_kernel_runtime_conformance<H: KernelRuntimeHarness>(
    harness: &mut H,
) -> Result<(), KernelError> {
    let runtime = harness.open(ConformanceOpen::Fresh)?;
    declared_bounds_are_validated_before_anything_runs(&runtime, harness)?;
    no_credential_can_be_declared(harness)?;
    a_session_is_named_then_opened_then_settled(&runtime, harness)?;

    let stateful = session(STATEFUL)?;
    let generation = runtime.open(&stateful, &harness.spec(session_bounds()?)?, BASE_MS)?;
    let settled = executions_share_state_in_order(&runtime, harness, &stateful, generation)?;
    a_settled_execution_is_replayed_rather_than_repeated(&runtime, &settled)?;
    a_duplicate_execution_identity_conflicts(&runtime, harness, &settled)?;
    a_raise_is_reported_as_a_raise(&runtime, harness, &stateful, generation)?;
    output_beyond_its_bound_is_recorded_truncation(&runtime, harness, &stateful, generation)?;
    a_cancelled_execution_leaves_the_session_live(&runtime, harness, &stateful, generation)?;
    an_elapsed_deadline_settles_as_timed_out(&runtime, harness, &stateful, generation)?;
    let checkpoint = a_checkpoint_addresses_its_own_payload(&runtime, harness, &stateful)?;
    a_checkpoint_declares_something(&checkpoint)?;
    a_checkpoint_from_another_image_is_refused(&runtime, harness, &stateful, &checkpoint)?;

    a_restore_carries_forward_the_declared_restorable_subset(&runtime, harness)?;
    a_restore_declares_what_it_lost(&runtime, harness)?;
    a_session_ceiling_stops_the_session(&runtime, harness)?;
    no_secret_reaches_durable_state(harness)?;

    let concurrent = harness.open(ConformanceOpen::Concurrent)?;
    concurrent_handles_agree(&concurrent, &stateful, &settled)?;
    runtime.close(&stateful, BASE_MS + 60_000)?;

    let orphan = session(ORPHANED)?;
    let orphan_key = key(&orphan, KernelGeneration::FIRST, "orphaned.1")?;
    runtime.open(&orphan, &harness.spec(session_bounds()?)?, BASE_MS + 70_000)?;
    let sleeping = harness.fragment(
        KernelScript::Sleeps,
        execution_bounds(MAX_KERNEL_EXECUTION_DEADLINE_MS, 4096)?,
    )?;
    let sink = harness.sink()?;
    runtime.submit(&orphan_key, &sleeping, &sink, BASE_MS + 70_100)?;
    harness.abandon(&orphan)?;
    drop(concurrent);
    drop(runtime);

    let reopened = harness.open(ConformanceOpen::Reopen)?;
    a_restart_surfaces_the_crash_time_backlog(&reopened, &orphan)?;
    an_uncertain_probe_leaves_a_session_uncertain(&reopened, &orphan)?;
    an_orphan_session_settles_as_interrupted(&reopened, &orphan, &orphan_key)?;
    receipts_survive_a_restart_unchanged(&reopened, &settled)?;
    a_tampered_receipt_fails_verification(&reopened, harness, &orphan)?;
    an_undecodable_row_is_corrupt_not_absent(&reopened, harness)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Positive properties
// ---------------------------------------------------------------------------

/// A session is named by its caller before it exists, is durably live before
/// `open` returns, and settles into a receipt that verifies.
fn a_session_is_named_then_opened_then_settled<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), KernelError> {
    let opened = session(OPENED)?;
    require(
        runtime.inspect_session(&opened)?.is_none(),
        "a fresh authority already knows a session",
    )?;
    require(
        matches!(
            runtime.close(&opened, BASE_MS),
            Err(KernelError::SessionNotFound(_))
        ),
        "an authority closed a session it was never given",
    )?;

    let generation = runtime.open(&opened, &harness.spec(session_bounds()?)?, BASE_MS)?;
    require(
        generation == KernelGeneration::FIRST,
        "a first incarnation is not generation one",
    )?;
    match expect(
        runtime.inspect_session(&opened)?,
        "an opened session vanished",
    )? {
        KernelSessionStatus::Live {
            generation: recorded,
            process,
            opened_at_ms,
            executions,
        } => require(
            recorded == generation
                && process.pid() != 0
                && opened_at_ms == BASE_MS
                && executions == 0,
            "an opened session was recorded with something other than what opened it",
        )?,
        other => return Err(suite_error(format!("an opened session reads as {other:?}"))),
    }
    require(
        matches!(
            runtime.open(&opened, &harness.spec(session_bounds()?)?, BASE_MS),
            Err(KernelError::Conflict(_))
        ),
        "a second open of one session identity was not a conflict",
    )?;

    let receipt = runtime.close(&opened, BASE_MS + 1_000)?;
    receipt.verify(&receipt.digest())?;
    require(
        receipt.session == opened
            && receipt.generation == generation
            && receipt.disposition == KernelDisposition::Closed
            && receipt.opened_at_ms == BASE_MS
            && receipt.settled_at_ms == BASE_MS + 1_000,
        "a closed session's receipt does not describe the session that closed",
    )?;
    require(
        runtime.close(&opened, BASE_MS + 2_000)? == receipt,
        "closing a settled session settled it a second time",
    )?;
    require(
        expect(
            runtime.session_receipt(&opened)?,
            "a settled session lost its receipt",
        )? == receipt,
        "a settled session's stored receipt disagrees with the one close answered",
    )?;
    Ok(())
}

/// One incarnation runs one fragment at a time, in order, over shared state:
/// what one execution binds the next one reads, and sequence numbers are dense
/// from one.
fn executions_share_state_in_order<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    stateful: &KernelSessionId,
    generation: KernelGeneration,
) -> Result<KernelExecutionReceipt, KernelError> {
    let bounds = execution_bounds(60_000, 4096)?;
    let bind = key(stateful, generation, "stateful.bind")?;
    let bound = run(
        runtime,
        harness,
        &bind,
        KernelScript::BindsState,
        bounds,
        BASE_MS + 100,
    )?;
    require(
        bound.sequence == 1 && bound.disposition == KernelExecutionDisposition::Completed,
        "the first execution of a session is not a completed sequence one",
    )?;
    bound.verify(&bound.digest())?;

    let read = key(stateful, generation, "stateful.read")?;
    let observed = run(
        runtime,
        harness,
        &read,
        KernelScript::ReadsState,
        bounds,
        BASE_MS + 200,
    )?;
    require(
        observed.sequence == 2,
        "a session's second execution is not sequence two",
    )?;
    let captured = harness.captured(
        &expect(
            observed.stdout.as_ref(),
            "an execution that wrote to stdout has no capture",
        )?
        .artifact,
    )?;
    require(
        String::from_utf8_lossy(&captured).contains(&format!(
            "{CONFORMANCE_KERNEL_STATE_NAME}={CONFORMANCE_KERNEL_STATE_VALUE}"
        )),
        "an execution did not observe the state an earlier execution bound",
    )?;

    let settles = key(stateful, generation, "stateful.settles")?;
    let receipt = run(
        runtime,
        harness,
        &settles,
        KernelScript::Settles,
        bounds,
        BASE_MS + 300,
    )?;
    let stdout =
        harness.captured(&expect(receipt.stdout.as_ref(), "no stdout capture")?.artifact)?;
    let stderr =
        harness.captured(&expect(receipt.stderr.as_ref(), "no stderr capture")?.artifact)?;
    require(
        stdout == format!("{CONFORMANCE_KERNEL_STDOUT_MARK}\n").into_bytes()
            && stderr == format!("{CONFORMANCE_KERNEL_STDERR_MARK}\n").into_bytes(),
        "a settling fragment's captured streams are not what it wrote",
    )?;
    require(
        receipt.sequence == 3 && !receipt.truncated(),
        "a settling fragment is out of order or falsely truncated",
    )?;
    Ok(receipt)
}

/// A settled execution is an answer, not a second run.
fn a_settled_execution_is_replayed_rather_than_repeated(
    runtime: &Runtime,
    settled: &KernelExecutionReceipt,
) -> Result<(), KernelError> {
    let again = runtime.wait(&settled.key)?;
    require(
        again == *settled && again.digest() == settled.digest(),
        "waiting on a settled execution did not replay its receipt",
    )?;
    require(
        expect(
            runtime.execution_receipt(&settled.key)?,
            "a settled execution lost its receipt",
        )? == *settled,
        "a settled execution's stored receipt disagrees with the one wait answered",
    )?;
    require(
        runtime.cancel(&settled.key).is_ok() && runtime.wait(&settled.key)? == *settled,
        "cancelling a settled execution rewrote its settlement",
    )?;
    Ok(())
}

/// A raise is reported as a raise, with its class, and never as success.
fn a_raise_is_reported_as_a_raise<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    stateful: &KernelSessionId,
    generation: KernelGeneration,
) -> Result<(), KernelError> {
    let raised = run(
        runtime,
        harness,
        &key(stateful, generation, "stateful.raises")?,
        KernelScript::Raises,
        execution_bounds(60_000, 4096)?,
        BASE_MS + 400,
    )?;
    match &raised.disposition {
        KernelExecutionDisposition::Raised { error_class } => require(
            error_class.as_str() == CONFORMANCE_KERNEL_ERROR_CLASS,
            "a raise was reported with a class the fragment did not raise",
        )?,
        other => return Err(suite_error(format!("a raise was reported as {other:?}"))),
    }
    require(
        !raised.succeeded(),
        "a raised execution reports that it succeeded",
    )?;
    Ok(())
}

/// Output beyond a declared bound is a recorded truncation rather than a silent
/// gap.
fn output_beyond_its_bound_is_recorded_truncation<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    stateful: &KernelSessionId,
    generation: KernelGeneration,
) -> Result<(), KernelError> {
    let bound = 4096;
    let flooded = run(
        runtime,
        harness,
        &key(stateful, generation, "stateful.floods")?,
        KernelScript::Floods,
        execution_bounds(60_000, bound)?,
        BASE_MS + 500,
    )?;
    let capture = expect(
        flooded.stdout.as_ref(),
        "a flooding execution captured no stdout",
    )?;
    require(
        capture.truncated
            && capture.captured_bytes <= bound
            && capture.produced_bytes > capture.captured_bytes
            && capture.produced_bytes >= CONFORMANCE_KERNEL_FLOOD_BYTES
            && capture.declared_bound == bound,
        "a flood was not recorded as a bounded, counted truncation",
    )?;
    require(
        flooded.truncated() && flooded.disposition == KernelExecutionDisposition::Completed,
        "a truncated execution did not complete or did not report truncation",
    )?;
    require(
        harness.captured(&capture.artifact)?.len() as u64 == capture.captured_bytes,
        "a capture record's byte count disagrees with the artifact it cites",
    )?;
    Ok(())
}

/// Cancellation is scoped to one fragment: the session survives it and keeps
/// its state.
fn a_cancelled_execution_leaves_the_session_live<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    stateful: &KernelSessionId,
    generation: KernelGeneration,
) -> Result<(), KernelError> {
    let sleeping = key(stateful, generation, "stateful.cancelled")?;
    let submission = harness.fragment(
        KernelScript::Sleeps,
        execution_bounds(MAX_KERNEL_EXECUTION_DEADLINE_MS, 4096)?,
    )?;
    let sink = harness.sink()?;
    runtime.submit(&sleeping, &submission, &sink, BASE_MS + 600)?;

    let canceller = Arc::clone(runtime);
    let cancelled_key = sleeping.clone();
    let thread = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(150));
        canceller.cancel(&cancelled_key)
    });
    let receipt = runtime.wait(&sleeping)?;
    thread
        .join()
        .map_err(|_| suite_error("the cancelling thread panicked"))??;
    require(
        receipt.disposition == KernelExecutionDisposition::Cancelled,
        format!("a cancelled execution settled as {:?}", receipt.disposition),
    )?;

    let after = run(
        runtime,
        harness,
        &key(stateful, generation, "stateful.after-cancel")?,
        KernelScript::ReadsState,
        execution_bounds(60_000, 4096)?,
        BASE_MS + 700,
    )?;
    require(
        after.disposition == KernelExecutionDisposition::Completed,
        "a session did not survive a cancelled execution",
    )?;
    let captured =
        harness.captured(&expect(after.stdout.as_ref(), "no stdout capture")?.artifact)?;
    require(
        String::from_utf8_lossy(&captured).contains(CONFORMANCE_KERNEL_STATE_VALUE),
        "a session lost its state when one of its executions was cancelled",
    )?;
    Ok(())
}

/// An elapsed deadline settles as a timeout, not as success and not as a hang.
fn an_elapsed_deadline_settles_as_timed_out<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    stateful: &KernelSessionId,
    generation: KernelGeneration,
) -> Result<(), KernelError> {
    let receipt = run(
        runtime,
        harness,
        &key(stateful, generation, "stateful.timed-out")?,
        KernelScript::Sleeps,
        execution_bounds(250, 4096)?,
        BASE_MS + 800,
    )?;
    require(
        receipt.disposition == KernelExecutionDisposition::TimedOut,
        format!(
            "an execution past its deadline settled as {:?}",
            receipt.disposition
        ),
    )?;
    Ok(())
}

/// A checkpoint is evidence that addresses its own payload and says what it
/// carries.
fn a_checkpoint_addresses_its_own_payload<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    stateful: &KernelSessionId,
) -> Result<KernelCheckpointRef, KernelError> {
    let sink = harness.sink()?;
    let checkpoint = runtime.checkpoint(stateful, &sink, BASE_MS + 900)?;
    checkpoint.validate()?;
    let payload = harness.captured(&checkpoint.artifact)?;
    require(
        checkpoint.artifact.addresses(&payload),
        "a checkpoint does not address the payload it cites",
    )?;
    require(
        !checkpoint.restorable.is_empty(),
        "a checkpoint of a session with state declares no restorable state",
    )?;
    require(
        checkpoint.session == *stateful && checkpoint.taken_at_ms == BASE_MS + 900,
        "a checkpoint does not describe the session it was taken from",
    )?;
    require(
        String::from_utf8_lossy(&payload).contains(CONFORMANCE_KERNEL_STATE_VALUE),
        "a checkpoint of a session with bound state did not carry it",
    )?;
    Ok(checkpoint)
}

/// A restore starts a strictly later incarnation carrying the state the
/// checkpoint declared restorable.
fn a_restore_carries_forward_the_declared_restorable_subset<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), KernelError> {
    let restored = session(RESTORED)?;
    let spec = harness.spec(session_bounds()?)?;
    let first = runtime.open(&restored, &spec, BASE_MS + 1_000)?;
    run(
        runtime,
        harness,
        &key(&restored, first, "restored.bind")?,
        KernelScript::BindsState,
        execution_bounds(60_000, 4096)?,
        BASE_MS + 1_100,
    )?;
    let sink = harness.sink()?;
    let checkpoint = runtime.checkpoint(&restored, &sink, BASE_MS + 1_200)?;
    runtime.close(&restored, BASE_MS + 1_300)?;

    let generation = match runtime.restore(&restored, &checkpoint, &spec, BASE_MS + 1_400)? {
        KernelRestore::Restored {
            session: named,
            generation,
            lost,
        } => {
            require(
                named == restored && lost == checkpoint.non_restorable,
                "a restore named another session or another loss",
            )?;
            require(
                generation > first,
                "a restored incarnation is not strictly later than the one it came from",
            )?;
            generation
        }
        other => return Err(suite_error(format!("a restore answered {other:?}"))),
    };
    let observed = run(
        runtime,
        harness,
        &key(&restored, generation, "restored.read")?,
        KernelScript::ReadsState,
        execution_bounds(60_000, 4096)?,
        BASE_MS + 1_500,
    )?;
    require(
        observed.sequence == 1,
        "a restored incarnation did not restart its execution sequence at one",
    )?;
    let captured =
        harness.captured(&expect(observed.stdout.as_ref(), "no stdout capture")?.artifact)?;
    require(
        String::from_utf8_lossy(&captured).contains(CONFORMANCE_KERNEL_STATE_VALUE),
        "a restore did not carry forward the state its checkpoint declared restorable",
    )?;
    runtime.close(&restored, BASE_MS + 1_600)?;
    Ok(())
}

/// A restore hands back what was lost. There is no way to receive the session
/// without receiving the loss.
fn a_restore_declares_what_it_lost<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), KernelError> {
    let lossy = session(LOSSY)?;
    let spec = harness.spec(session_bounds()?)?;
    let generation = runtime.open(&lossy, &spec, BASE_MS + 2_000)?;
    run(
        runtime,
        harness,
        &key(&lossy, generation, "lossy.holds")?,
        KernelScript::HoldsUnserialisableState,
        execution_bounds(60_000, 4096)?,
        BASE_MS + 2_100,
    )?;
    let sink = harness.sink()?;
    let checkpoint = runtime.checkpoint(&lossy, &sink, BASE_MS + 2_200)?;
    require(
        !checkpoint.non_restorable.is_empty(),
        "a checkpoint of a session holding unserialisable state declared no loss",
    )?;
    runtime.close(&lossy, BASE_MS + 2_300)?;
    match runtime.restore(&lossy, &checkpoint, &spec, BASE_MS + 2_400)? {
        KernelRestore::Restored { lost, .. } => require(
            lost == checkpoint.non_restorable,
            "a restore handed back a different loss than the checkpoint declared",
        )?,
        other => return Err(suite_error(format!("a restore answered {other:?}"))),
    }
    runtime.close(&lossy, BASE_MS + 2_500)?;
    Ok(())
}

/// A restart's backlog is exactly the sessions that were live when the Host
/// died, and it is answered without a clock.
fn a_restart_surfaces_the_crash_time_backlog(
    runtime: &Runtime,
    orphan: &KernelSessionId,
) -> Result<(), KernelError> {
    let backlog = runtime.requiring_reconciliation()?;
    require(
        backlog.contains(orphan),
        "a restart did not surface a session that was live when the Host died",
    )?;
    require(
        backlog.windows(2).all(|pair| pair[0] < pair[1]),
        "a reconciliation backlog is not in identity order",
    )?;
    Ok(())
}

/// An orphan settles honestly as interrupted, taking every execution that was
/// in flight with it, and settles only once.
fn an_orphan_session_settles_as_interrupted(
    runtime: &Runtime,
    orphan: &KernelSessionId,
    orphan_key: &KernelExecutionKey,
) -> Result<(), KernelError> {
    let outcome = runtime.reconcile(orphan, &ScriptedProbe(Liveness::Gone), BASE_MS + 80_000)?;
    let KernelReconcileOutcome::Settled {
        session: receipt,
        executions,
    } = outcome
    else {
        return Err(suite_error(format!(
            "a gone orphan reconciled as {outcome:?}"
        )));
    };
    require(
        receipt.disposition == KernelDisposition::Interrupted,
        "an orphan settled as something other than interrupted",
    )?;
    receipt.verify(&receipt.digest())?;
    require(
        executions.len() == 1
            && executions[0].key == *orphan_key
            && executions[0].disposition == KernelExecutionDisposition::Interrupted,
        "an orphan's in-flight execution did not settle with the session that owned it",
    )?;
    require(
        executions[0].stdout.is_none() && executions[0].stderr.is_none(),
        "an interrupted execution claimed captured output nobody read",
    )?;
    require(
        !runtime.requiring_reconciliation()?.contains(orphan),
        "a settled orphan is still in the reconciliation backlog",
    )?;
    let again = runtime.reconcile(orphan, &ScriptedProbe(Liveness::Gone), BASE_MS + 90_000)?;
    require(
        again
            == KernelReconcileOutcome::Settled {
                session: receipt,
                executions,
            },
        "reconciling a settled orphan twice produced a second settlement",
    )?;
    Ok(())
}

/// A receipt is append-only: a restart returns exactly the bytes that were
/// written, and they still address to their own digest.
fn receipts_survive_a_restart_unchanged(
    runtime: &Runtime,
    settled: &KernelExecutionReceipt,
) -> Result<(), KernelError> {
    let after = expect(
        runtime.execution_receipt(&settled.key)?,
        "a receipt did not survive a restart",
    )?;
    require(
        after == *settled && after.digest() == settled.digest(),
        "a receipt changed across a restart",
    )?;
    after.verify(&settled.digest())?;
    require(
        expect(
            runtime.inspect_session(settled.key.session())?,
            "a settled session vanished across a restart",
        )?
        .is_settled(),
        "a settled session reopened as something other than settled",
    )?;
    Ok(())
}

/// Two handles onto one authority agree about who owns what: the session one
/// handle holds live reads as uncertain to the other, never as its own and
/// never as settled.
fn concurrent_handles_agree(
    concurrent: &Runtime,
    stateful: &KernelSessionId,
    settled: &KernelExecutionReceipt,
) -> Result<(), KernelError> {
    match expect(
        concurrent.inspect_session(stateful)?,
        "a concurrent handle cannot see a session at all",
    )? {
        KernelSessionStatus::Uncertain { .. } => {}
        other => {
            return Err(suite_error(format!(
                "a concurrent handle reads another handle's live session as {other:?}"
            )));
        }
    }
    require(
        matches!(
            concurrent.inspect_execution(&settled.key)?,
            Some(KernelExecutionStatus::Settled(_))
        ),
        "a concurrent handle disagrees about a settled execution",
    )?;
    require(
        matches!(
            concurrent.close(stateful, BASE_MS + 65_000),
            Err(KernelError::Unowned(_))
        ),
        "a concurrent handle closed a session it does not own",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Negative controls
// ---------------------------------------------------------------------------

/// N1. No credential can be declared.
///
/// The structural half of this control is not a test and cannot be: it is that
/// this module contains no credential type, that no [`KernelRuntime`] method
/// takes a [`crate::CredentialResolver`], and that
/// [`KernelSpec::environment`] takes a `String` value with no sibling that
/// takes a [`crate::CredentialHandleName`]. A Host cannot attach a credential
/// to a kernel because the shapes to do it with do not exist; that is checked
/// by the compiler on every build of this crate, and a change that broke it
/// would have to add a type rather than pass a bad value.
///
/// The runtime half is here: the published reserved names are refused, so the
/// remaining door — naming a literal after the variable a provider library
/// reads — is closed too.
fn no_credential_can_be_declared<H: KernelRuntimeHarness>(
    harness: &mut H,
) -> Result<(), KernelError> {
    for name in KERNEL_RESERVED_ENVIRONMENT_NAMES {
        for candidate in [name.to_ascii_uppercase(), name.to_ascii_lowercase()] {
            let spec = harness.spec(session_bounds()?)?;
            require(
                matches!(
                    spec.environment(candidate.clone(), CONFORMANCE_KERNEL_SECRET),
                    Err(KernelError::Validation(_))
                ),
                format!("a kernel spec accepted the reserved name {candidate}"),
            )?;
        }
    }
    let reserved = harness
        .spec(session_bounds()?)?
        .reserving("HOST_PRIVATE_TOKEN")?;
    require(
        matches!(
            reserved.environment("host_private_token", CONFORMANCE_KERNEL_SECRET),
            Err(KernelError::Validation(_))
        ),
        "a Host-reserved environment name was accepted anyway",
    )?;
    Ok(())
}

/// N2. No secret reaches durable state.
fn no_secret_reaches_durable_state<H: KernelRuntimeHarness>(
    harness: &mut H,
) -> Result<(), KernelError> {
    let durable = harness.durable_bytes()?;
    require(
        !contains(&durable, CONFORMANCE_KERNEL_SECRET.as_bytes()),
        "a secret the Host bound into a kernel's environment reached durable state",
    )?;
    Ok(())
}

/// N3. An inconclusive probe changes nothing, however many times it is asked.
fn an_uncertain_probe_leaves_a_session_uncertain(
    runtime: &Runtime,
    orphan: &KernelSessionId,
) -> Result<(), KernelError> {
    for attempt in 0..3 {
        let outcome = runtime.reconcile(
            orphan,
            &ScriptedProbe(Liveness::Unknown),
            BASE_MS + 75_000 + attempt,
        )?;
        require(
            outcome == KernelReconcileOutcome::Uncertain,
            "an inconclusive probe resolved a session anyway",
        )?;
        require(
            runtime.session_receipt(orphan)?.is_none(),
            "an inconclusive probe settled a session",
        )?;
        require(
            runtime.requiring_reconciliation()?.contains(orphan),
            "an inconclusive probe removed a session from the backlog",
        )?;
    }
    Ok(())
}

/// N4. A checkpoint produced by another kernel image is refused rather than
/// attempted, and nothing is started.
fn a_checkpoint_from_another_image_is_refused<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    stateful: &KernelSessionId,
    checkpoint: &KernelCheckpointRef,
) -> Result<(), KernelError> {
    let other = session("conformance.other-image")?;
    // A different declared ceiling is a different image as far as the digest is
    // concerned, which is the point: the digest covers everything that changes
    // what the process is.
    let different = harness.spec(KernelSessionBounds::new(60_000, 8, 4096)?)?;
    let mut foreign = checkpoint.clone();
    foreign.session = other.clone();
    match runtime.restore(&other, &foreign, &different, BASE_MS + 3_000)? {
        KernelRestore::SpecMismatch { expected, found } => require(
            expected == checkpoint.spec_digest && found == different.spec_digest(),
            "a spec mismatch named the wrong images",
        )?,
        other => {
            return Err(suite_error(format!(
                "a restore into another image answered {other:?}"
            )));
        }
    }
    require(
        runtime.inspect_session(&other)?.is_none(),
        "a refused restore started a session anyway",
    )?;
    require(
        matches!(
            runtime.inspect_session(stateful)?,
            Some(KernelSessionStatus::Live { .. })
        ),
        "a refused restore disturbed the session it was not for",
    )?;
    Ok(())
}

/// N5. A checkpoint that declares nothing never enumerated its losses, so it is
/// corrupt rather than restorable.
fn a_checkpoint_declares_something(checkpoint: &KernelCheckpointRef) -> Result<(), KernelError> {
    let mut empty = checkpoint.clone();
    empty.restorable.clear();
    empty.non_restorable.clear();
    require(
        matches!(empty.validate(), Err(KernelError::Corrupt(_))),
        "a checkpoint that declares neither restorable nor non-restorable state validated",
    )?;
    Ok(())
}

/// N6. A tampered receipt is corrupt, never absent and never a fresh answer.
fn a_tampered_receipt_fails_verification<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    orphan: &KernelSessionId,
) -> Result<(), KernelError> {
    harness.damage(orphan, KernelDamage::Receipt)?;
    require(
        matches!(
            runtime.inspect_session(orphan),
            Err(KernelError::Corrupt(_))
        ),
        "a tampered session receipt was answered rather than refused",
    )?;
    require(
        matches!(
            runtime.session_receipt(orphan),
            Err(KernelError::Corrupt(_))
        ),
        "a tampered session receipt was handed back as a receipt",
    )?;
    Ok(())
}

/// N7. Durable state that cannot be decoded within the published bounds is
/// corrupt, never absent: `Ok(None)` would read as *this never happened*.
fn an_undecodable_row_is_corrupt_not_absent<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), KernelError> {
    let damaged = session(DAMAGED)?;
    let spec = harness.spec(session_bounds()?)?;
    runtime.open(&damaged, &spec, BASE_MS + 100_000)?;
    runtime.close(&damaged, BASE_MS + 100_100)?;
    harness.damage(&damaged, KernelDamage::Record)?;
    require(
        matches!(
            runtime.inspect_session(&damaged),
            Err(KernelError::Corrupt(_))
        ),
        "an undecodable session row was answered as absent or as fact",
    )?;
    Ok(())
}

/// N8. A repeated execution identity is a conflict, not a second run.
fn a_duplicate_execution_identity_conflicts<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
    settled: &KernelExecutionReceipt,
) -> Result<(), KernelError> {
    let submission = harness.fragment(KernelScript::BindsState, execution_bounds(60_000, 4096)?)?;
    let sink = harness.sink()?;
    require(
        matches!(
            runtime.submit(&settled.key, &submission, &sink, BASE_MS + 1_000),
            Err(KernelError::Conflict(_))
        ),
        "a repeated execution identity ran a second time",
    )?;
    require(
        runtime.wait(&settled.key)? == *settled,
        "a refused duplicate submission disturbed the settlement it collided with",
    )?;
    Ok(())
}

/// N9. A declared session ceiling stops the session, by name, and further
/// submissions are refused rather than queued.
fn a_session_ceiling_stops_the_session<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), KernelError> {
    let ceilinged = session(CEILINGED)?;
    let spec = harness.spec(KernelSessionBounds::new(
        MAX_KERNEL_EXECUTION_DEADLINE_MS,
        1,
        16 * 1024 * 1024,
    )?)?;
    let generation = runtime.open(&ceilinged, &spec, BASE_MS + 4_000)?;
    let bounds = execution_bounds(60_000, 4096)?;
    run(
        runtime,
        harness,
        &key(&ceilinged, generation, "ceilinged.1")?,
        KernelScript::Settles,
        bounds,
        BASE_MS + 4_100,
    )?;
    let submission = harness.fragment(KernelScript::Settles, bounds)?;
    let sink = harness.sink()?;
    require(
        matches!(
            runtime.submit(
                &key(&ceilinged, generation, "ceilinged.2")?,
                &submission,
                &sink,
                BASE_MS + 4_200,
            ),
            Err(KernelError::NotLive(_))
        ),
        "a session past its declared execution ceiling accepted another fragment",
    )?;
    let receipt = expect(
        runtime.session_receipt(&ceilinged)?,
        "a session stopped by a ceiling has no receipt",
    )?;
    match &receipt.disposition {
        KernelDisposition::CeilingReached { ceiling } => require(
            ceiling.as_str() == "max_executions",
            "a ceiling settlement did not name the ceiling that was reached",
        )?,
        other => {
            return Err(suite_error(format!(
                "a session stopped by a ceiling settled as {other:?}"
            )));
        }
    }
    Ok(())
}

/// Bounds are the contract's own, and they are checked before a process
/// exists: a workflow that cannot terminate never starts.
fn declared_bounds_are_validated_before_anything_runs<H: KernelRuntimeHarness>(
    runtime: &Runtime,
    harness: &mut H,
) -> Result<(), KernelError> {
    require(
        KernelSessionBounds::new(0, 1, 1).is_err()
            && KernelSessionBounds::new(1, 0, 1).is_err()
            && KernelSessionBounds::new(1, 1, 0).is_err(),
        "a zero session bound was accepted",
    )?;
    require(
        KernelExecutionBounds::new(0, 1, 1).is_err()
            && KernelExecutionBounds::new(MAX_KERNEL_EXECUTION_DEADLINE_MS + 1, 1, 1).is_err()
            && KernelExecutionBounds::new(1, MAX_KERNEL_CAPTURE_BYTES + 1, 1).is_err()
            && KernelExecutionBounds::new(1, 1, MAX_KERNEL_CAPTURE_BYTES + 1).is_err(),
        "an out-of-range execution bound was accepted",
    )?;
    require(
        KernelSubmission::new(
            "x".repeat(MAX_KERNEL_SOURCE_BYTES + 1),
            execution_bounds(60_000, 4096)?,
        )
        .is_err(),
        "a source beyond the published bound was accepted",
    )?;

    let unopened = session("conformance.never-opened")?;
    let submission = harness.fragment(KernelScript::Settles, execution_bounds(60_000, 4096)?)?;
    let sink = harness.sink()?;
    require(
        matches!(
            runtime.submit(
                &key(&unopened, KernelGeneration::FIRST, "never.1")?,
                &submission,
                &sink,
                BASE_MS,
            ),
            Err(KernelError::SessionNotFound(_))
        ),
        "an authority accepted a fragment for a session that was never opened",
    )?;
    require(
        runtime.inspect_session(&unopened)?.is_none(),
        "a refused submission wrote durable state",
    )?;
    Ok(())
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
