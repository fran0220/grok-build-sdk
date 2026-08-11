// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! The program execution contract, exercised the way a Host exercises it:
//! through the public façade, against the reference runtime and its real
//! processes, and against a backend that reports an orphan as a success.

use grok_build_sdk::{
    ArtifactHandle, ArtifactLabel, ArtifactVault, ArtifactVaultOutputSink, CONFORMANCE_EXIT_CODE,
    CONFORMANCE_FLOOD_BYTES, CONFORMANCE_SECRET, CONFORMANCE_STDERR_MARK, CONFORMANCE_STDOUT_MARK,
    ConformanceOpen, CredentialHandleName, CredentialResolver, EnvironmentBinding, ExecutionId,
    ExecutionStatus, ExitDisposition, Liveness, LivenessProbe, LocalArtifactVault,
    LocalProgramRuntime, MAX_PROGRAM_CAPTURE_BYTES, MAX_PROGRAM_DEADLINE_MS,
    MAX_PROGRAM_EXECUTION_ID_BYTES, OsLivenessProbe, ProcessIdentity, ProgramBounds, ProgramError,
    ProgramLabel, ProgramLaunch, ProgramOutputSink, ProgramPath, ProgramRuntime,
    ProgramRuntimeHarness, ProgramScript, ProgramStream, ReconcileOutcome, ResolvedCredential,
    run_program_runtime_conformance,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

const NOW: u64 = 1_700_000_000_000;
const CREDENTIAL_VARIABLE: &str = grok_build_sdk::CONFORMANCE_CREDENTIAL_VARIABLE;
const CREDENTIAL_HANDLE: &str = grok_build_sdk::CONFORMANCE_CREDENTIAL_HANDLE;

// ---------------------------------------------------------------------------
// Fixture: a POSIX script per conformance program, a vault-backed sink, and a
// harness that can crash a running execution.
// ---------------------------------------------------------------------------

fn script_text(script: ProgramScript) -> String {
    match script {
        ProgramScript::Settles => format!(
            "printf '%s\\n' '{CONFORMANCE_STDOUT_MARK}'\n\
             printf '%s\\n' '{CONFORMANCE_STDERR_MARK}' 1>&2\n\
             exit 0\n"
        ),
        ProgramScript::Fails => format!("exit {CONFORMANCE_EXIT_CODE}\n"),
        ProgramScript::Floods => {
            // 128 lines of 512 bytes is 64 KiB before the newlines.
            "line=$(printf 'x%.0s' $(seq 1 512))\n\
             i=0\n\
             while [ $i -lt 128 ]; do printf '%s\\n' \"$line\"; i=$((i+1)); done\n\
             exit 0\n"
                .to_owned()
        }
        ProgramScript::Sleeps => "exec /bin/sleep 30\n".to_owned(),
        ProgramScript::ReportsCredentialLength => {
            format!("printf 'len=%s\\n' \"${{#{CREDENTIAL_VARIABLE}}}\"\nexit 0\n")
        }
        other => panic!("the harness does not supply a program for {other:?}"),
    }
}

fn write_script(root: &Path, script: ProgramScript) -> PathBuf {
    let path = root.join(format!("{script:?}.sh").to_lowercase());
    std::fs::write(&path, script_text(script)).expect("the script is writable");
    path
}

fn launch_of(root: &Path, script: ProgramScript, bounds: ProgramBounds) -> ProgramLaunch {
    let launch = ProgramLaunch::new(
        ProgramPath::new("/bin/sh").expect("/bin/sh is an absolute path"),
        root,
        bounds,
    )
    .expect("a launch under an existing root is valid")
    .argument(write_script(root, script).to_string_lossy().into_owned())
    .expect("a script path is a valid argument")
    // `seq` and `sleep` are not shell builtins, so the flood and sleep scripts
    // need a search path. It is a literal, so it is part of the environment
    // digest.
    .environment("PATH", "/usr/bin:/bin")
    .expect("PATH is a valid environment binding");
    if script == ProgramScript::ReportsCredentialLength {
        launch
            .credential(
                CREDENTIAL_VARIABLE,
                CredentialHandleName::new(CREDENTIAL_HANDLE).expect("a valid handle name"),
            )
            .expect("a credential binding is valid")
    } else {
        launch
    }
}

struct KeychainResolver;

impl CredentialResolver for KeychainResolver {
    fn resolve(&self, _: &CredentialHandleName) -> Result<ResolvedCredential, ProgramError> {
        ResolvedCredential::new(CONFORMANCE_SECRET)
    }
}

struct NoCredentials;

impl CredentialResolver for NoCredentials {
    fn resolve(&self, handle: &CredentialHandleName) -> Result<ResolvedCredential, ProgramError> {
        Err(ProgramError::Validation(format!("unexpected {handle}")))
    }
}

struct FixedProbe(Liveness);

impl LivenessProbe for FixedProbe {
    fn probe(&self, _: &ProcessIdentity) -> Result<Liveness, ProgramError> {
        Ok(self.0)
    }
}

/// Everything one authority needs: a store root, a vault-backed output sink and
/// a working root the scripts live in.
struct Fixture {
    store: tempfile::TempDir,
    vault_root: tempfile::TempDir,
    work: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            store: tempfile::tempdir().unwrap(),
            vault_root: tempfile::tempdir().unwrap(),
            work: tempfile::tempdir().unwrap(),
        }
    }

    fn vault(&self) -> LocalArtifactVault {
        LocalArtifactVault::new(self.vault_root.path()).expect("the vault opens")
    }

    fn sink(&self) -> Arc<dyn ProgramOutputSink> {
        Arc::new(ArtifactVaultOutputSink::new(
            self.vault(),
            ArtifactLabel::new("program_runtime_tests").expect("a valid producer label"),
        ))
    }

    fn runtime(&self) -> Arc<LocalProgramRuntime> {
        Arc::new(
            LocalProgramRuntime::new(self.store.path(), self.sink()).expect("the runtime opens"),
        )
    }

    fn launch(&self, script: ProgramScript, bounds: ProgramBounds) -> ProgramLaunch {
        launch_of(self.work.path(), script, bounds)
    }

    fn captured(&self, artifact: &ArtifactHandle) -> Vec<u8> {
        self.vault()
            .read(artifact, NOW)
            .expect("a captured stream is readable")
    }
}

fn bounds(deadline_ms: u64, capture: u64) -> ProgramBounds {
    ProgramBounds::new(deadline_ms, capture, capture).expect("declared bounds are valid")
}

fn execution(value: &str) -> ExecutionId {
    ExecutionId::new(value).expect("a valid execution id")
}

fn settled(
    runtime: &Arc<LocalProgramRuntime>,
    fixture: &Fixture,
    id: &str,
    script: ProgramScript,
    bounds: ProgramBounds,
) -> grok_build_sdk::ExecutionReceipt {
    let execution = execution(id);
    runtime
        .launch(
            &execution,
            &fixture.launch(script, bounds),
            &NoCredentials,
            NOW,
        )
        .expect("the program launches");
    runtime.wait(&execution).expect("the program settles")
}

// ---------------------------------------------------------------------------
// (1) Execution identity and receipts.
// ---------------------------------------------------------------------------

/// A settled execution answers a receipt that names what ran, under what, and
/// where its output went.
#[test]
fn a_settled_execution_yields_a_receipt_naming_what_ran() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let launch = fixture.launch(ProgramScript::Settles, bounds(60_000, 4096));
    let id = execution("thread-7.iteration-2.build");

    let process = runtime
        .launch(&id, &launch, &NoCredentials, NOW)
        .expect("the program launches");
    assert_eq!(process.owner(), runtime.owner());
    assert!(process.pid() > 0);

    let receipt = runtime.wait(&id).expect("the program settles");
    assert_eq!(receipt.execution, id);
    assert_eq!(receipt.program.as_str(), "/bin/sh");
    assert_eq!(receipt.arguments_digest, launch.arguments_digest());
    assert_eq!(receipt.environment_digest, launch.environment_digest());
    assert_eq!(receipt.working_root, fixture.work.path());
    assert_eq!(receipt.disposition, ExitDisposition::Exited { code: 0 });
    assert!(receipt.succeeded());
    assert_eq!(receipt.started_at_ms, NOW);
    assert!(receipt.settled_at_ms >= NOW);

    let stdout = receipt.stdout.clone().expect("stdout was captured");
    let stderr = receipt.stderr.clone().expect("stderr was captured");
    assert_eq!(
        fixture.captured(&stdout.artifact),
        format!("{CONFORMANCE_STDOUT_MARK}\n").as_bytes()
    );
    assert_eq!(
        fixture.captured(&stderr.artifact),
        format!("{CONFORMANCE_STDERR_MARK}\n").as_bytes()
    );
    assert_eq!(stdout.stream, ProgramStream::Stdout);
    assert!(!stdout.truncated && !receipt.truncated());
    receipt
        .verify(&receipt.digest())
        .expect("a fresh receipt addresses to its own digest");
}

/// An identity is claimed once. A retry after an unknown outcome asks about the
/// execution it named; it does not start a second one.
#[test]
fn one_execution_identity_admits_one_process() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let id = execution("thread-7.iteration-2.build");
    let launch = fixture.launch(ProgramScript::Sleeps, bounds(MAX_PROGRAM_DEADLINE_MS, 4096));

    runtime
        .launch(&id, &launch, &NoCredentials, NOW)
        .expect("the program launches");
    assert!(matches!(
        runtime.launch(&id, &launch, &NoCredentials, NOW + 1),
        Err(ProgramError::Conflict(_))
    ));
    runtime.cancel(&id).expect("the execution is cancellable");
    let receipt = runtime.wait(&id).expect("the execution settles");

    // Settled is still claimed: an identity is never reusable.
    assert!(matches!(
        runtime.launch(&id, &launch, &NoCredentials, NOW + 2),
        Err(ProgramError::Conflict(_))
    ));
    assert_eq!(
        runtime.wait(&id).expect("a settled execution replays"),
        receipt,
        "replaying a settle must answer rather than re-run"
    );
}

/// A receipt is digest-verified on read, so a row edited underneath the
/// contract fails the read instead of presenting an altered account.
#[test]
fn an_edited_receipt_fails_the_read_rather_than_being_served() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let receipt = settled(
        &runtime,
        &fixture,
        "thread-7.edited",
        ProgramScript::Settles,
        bounds(60_000, 4096),
    );
    drop(runtime);

    let connection =
        rusqlite::Connection::open(fixture.store.path().join("program-runtime.sqlite3")).unwrap();
    let stored: String = connection
        .query_row(
            "SELECT receipt FROM executions WHERE execution_id=?1",
            [receipt.execution.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let edited = stored.replace(r#""exited":{"code":0}"#, r#""exited":{"code":9}"#);
    assert_ne!(edited, stored, "the fixture must actually edit the receipt");
    connection
        .execute(
            "UPDATE executions SET receipt=?2 WHERE execution_id=?1",
            rusqlite::params![receipt.execution.as_str(), edited],
        )
        .unwrap();
    drop(connection);

    let reopened = fixture.runtime();
    assert!(
        matches!(
            reopened.inspect(&receipt.execution),
            Err(ProgramError::Corrupt(_))
        ),
        "an edited receipt must fail the read"
    );
}

/// Identity, path, label and bound newtypes refuse what the contract does not
/// accept, before anything can run.
#[test]
fn the_contract_refuses_malformed_declarations() {
    assert!(ExecutionId::new("").is_err());
    assert!(ExecutionId::new(" leading").is_err());
    assert!(ExecutionId::new("with\nnewline").is_err());
    assert!(ExecutionId::new("x".repeat(MAX_PROGRAM_EXECUTION_ID_BYTES + 1)).is_err());
    assert!(ProgramPath::new("sh").is_err(), "a relative program path");
    assert!(ProgramLabel::new("  ").is_err());
    assert!(CredentialHandleName::new("").is_err());

    assert!(ProgramBounds::new(0, 4096, 4096).is_err(), "no deadline");
    assert!(ProgramBounds::new(MAX_PROGRAM_DEADLINE_MS + 1, 1, 1).is_err());
    assert!(ProgramBounds::new(1, MAX_PROGRAM_CAPTURE_BYTES + 1, 1).is_err());

    let work = tempfile::tempdir().unwrap();
    let base = ProgramLaunch::new(
        ProgramPath::new("/bin/sh").unwrap(),
        work.path(),
        bounds(1_000, 16),
    )
    .unwrap();
    assert!(base.clone().argument("with\0nul").is_err());
    assert!(base.clone().environment("1BAD", "value").is_err());
    assert!(base.clone().environment("HAS-DASH", "value").is_err());
    assert!(
        base.clone()
            .environment("GOOD", "value")
            .unwrap()
            .environment("GOOD", "again")
            .is_err(),
        "one variable cannot be bound twice"
    );
    assert!(
        ProgramLaunch::new(
            ProgramPath::new("/bin/sh").unwrap(),
            "relative/root",
            bounds(1_000, 16)
        )
        .is_err(),
        "a relative working root"
    );
    assert!(
        ProgramLaunch::new(
            ProgramPath::new("/bin/sh").unwrap(),
            work.path().join("absent"),
            bounds(1_000, 16)
        )
        .unwrap()
        .validate()
        .is_err(),
        "a working root that is not a directory"
    );
}

/// Two executions that differ only in their arguments or environment have
/// different digests, and a credential's digest contribution is its handle
/// name rather than its value.
#[test]
fn argument_and_environment_digests_identify_the_launch() {
    let work = tempfile::tempdir().unwrap();
    let base = || {
        ProgramLaunch::new(
            ProgramPath::new("/bin/sh").unwrap(),
            work.path(),
            bounds(1_000, 16),
        )
        .unwrap()
    };
    let one = base().argument("-c").unwrap().argument("true").unwrap();
    let other = base().argument("-ctrue").unwrap();
    assert_ne!(
        one.arguments_digest(),
        other.arguments_digest(),
        "a length-prefixed encoding must not let two argument vectors collide"
    );
    assert_eq!(base().arguments_digest(), base().arguments_digest());

    let handle = CredentialHandleName::new(CREDENTIAL_HANDLE).unwrap();
    let by_handle = base().credential("TOKEN", handle.clone()).unwrap();
    let by_literal = base().environment("TOKEN", CONFORMANCE_SECRET).unwrap();
    assert_ne!(
        by_handle.environment_digest(),
        by_literal.environment_digest(),
        "a credential binding must not digest as though it were a literal"
    );
    assert_eq!(
        by_handle.environment_digest(),
        base()
            .credential("TOKEN", handle)
            .unwrap()
            .environment_digest(),
        "an environment digest must be stable across credential rotation"
    );
    assert!(matches!(
        by_handle.environment_bindings().get("TOKEN"),
        Some(EnvironmentBinding::Credential(_))
    ));
}

// ---------------------------------------------------------------------------
// (2) Cancellation and timeout.
// ---------------------------------------------------------------------------

/// A cancel settles honestly, and a repeated cancel is an answer rather than a
/// second effect.
#[test]
fn a_cancelled_execution_settles_as_cancelled_and_never_as_success() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let id = execution("thread-7.cancelled");
    runtime
        .launch(
            &id,
            &fixture.launch(ProgramScript::Sleeps, bounds(MAX_PROGRAM_DEADLINE_MS, 4096)),
            &NoCredentials,
            NOW,
        )
        .expect("the program launches");
    runtime.cancel(&id).expect("cancel is accepted");
    runtime.cancel(&id).expect("cancel is idempotent");

    let receipt = runtime.wait(&id).expect("a cancelled execution settles");
    assert_eq!(receipt.disposition, ExitDisposition::Cancelled);
    assert!(!receipt.succeeded());
    runtime
        .cancel(&id)
        .expect("cancelling a settlement answers");
    assert_eq!(
        runtime.receipt(&id).unwrap().unwrap(),
        receipt,
        "a late cancel must not rewrite a settlement that already happened"
    );
}

/// A declared deadline is the runtime's to enforce, and the receipt reports
/// both the disposition and the bound that produced it.
#[test]
fn an_execution_past_its_declared_deadline_settles_as_timed_out() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let receipt = settled(
        &runtime,
        &fixture,
        "thread-7.timed-out",
        ProgramScript::Sleeps,
        bounds(250, 4096),
    );
    assert_eq!(receipt.disposition, ExitDisposition::TimedOut);
    assert!(!receipt.succeeded());
    assert_eq!(receipt.bounds.deadline_ms(), 250);
    assert!(
        receipt.duration_ms() >= 250,
        "a timed-out receipt must report at least the deadline it outlived"
    );
}

/// A non-zero exit is the status it was.
#[test]
fn a_failing_program_settles_with_the_status_it_exited_with() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let receipt = settled(
        &runtime,
        &fixture,
        "thread-7.failing",
        ProgramScript::Fails,
        bounds(60_000, 4096),
    );
    assert_eq!(
        receipt.disposition,
        ExitDisposition::Exited {
            code: CONFORMANCE_EXIT_CODE
        }
    );
    assert!(!receipt.succeeded());
}

// ---------------------------------------------------------------------------
// (3) Output bounds.
// ---------------------------------------------------------------------------

/// Output past the declared bound is a recorded fact with an honest
/// produced-byte count, and the kept prefix is a content-addressed artifact.
#[test]
fn output_past_its_bound_is_a_recorded_truncation_not_a_silent_loss() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let receipt = settled(
        &runtime,
        &fixture,
        "thread-7.flooding",
        ProgramScript::Floods,
        bounds(60_000, 1_024),
    );
    assert_eq!(receipt.disposition, ExitDisposition::Exited { code: 0 });

    let stdout = receipt.stdout.clone().expect("stdout was captured");
    assert!(stdout.truncated && receipt.truncated());
    assert_eq!(stdout.captured_bytes, 1_024);
    assert_eq!(stdout.declared_bound, 1_024);
    assert!(stdout.produced_bytes >= CONFORMANCE_FLOOD_BYTES);
    let captured = fixture.captured(&stdout.artifact);
    assert_eq!(captured.len(), 1_024);
    assert!(
        stdout.artifact.addresses(&captured),
        "a capture handle must address the bytes it names"
    );

    let stderr = receipt.stderr.expect("stderr was captured");
    assert!(!stderr.truncated && stderr.produced_bytes == 0);
}

/// A sink that binds output to bytes that are not that output is refused rather
/// than believed.
#[test]
fn a_sink_that_misbinds_captured_output_fails_the_settlement() {
    struct MisbindingSink;

    impl ProgramOutputSink for MisbindingSink {
        fn store(
            &self,
            _: &ExecutionId,
            _: ProgramStream,
            _: &[u8],
            _: u64,
        ) -> Result<ArtifactHandle, ProgramError> {
            Ok(ArtifactHandle::for_content(b"not the captured output"))
        }
    }

    let store = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let runtime = LocalProgramRuntime::new(store.path(), Arc::new(MisbindingSink)).unwrap();
    let id = execution("thread-7.misbound");
    runtime
        .launch(
            &id,
            &launch_of(work.path(), ProgramScript::Settles, bounds(60_000, 4096)),
            &NoCredentials,
            NOW,
        )
        .unwrap();
    assert!(matches!(runtime.wait(&id), Err(ProgramError::Corrupt(_))));
}

// ---------------------------------------------------------------------------
// (4) Scoped short-lived credential handles.
// ---------------------------------------------------------------------------

/// The value reaches the process and reaches nothing else: not the receipt, not
/// the store, not the captured output.
#[test]
fn a_credential_handle_resolves_at_spawn_and_is_never_persisted() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let id = execution("thread-7.credentialled");
    runtime
        .launch(
            &id,
            &fixture.launch(ProgramScript::ReportsCredentialLength, bounds(60_000, 4096)),
            &KeychainResolver,
            NOW,
        )
        .expect("the program launches");
    let receipt = runtime.wait(&id).expect("the program settles");
    assert!(receipt.succeeded());

    let stdout = receipt.stdout.clone().expect("stdout was captured");
    assert_eq!(
        fixture.captured(&stdout.artifact),
        format!("len={}\n", CONFORMANCE_SECRET.len()).as_bytes(),
        "the resolved value must reach the process"
    );
    assert_eq!(
        receipt.credential_handles,
        vec![CredentialHandleName::new(CREDENTIAL_HANDLE).unwrap()],
        "a receipt names handles, never values"
    );

    let encoded = serde_json::to_string(&receipt).unwrap();
    assert!(!encoded.contains(CONFORMANCE_SECRET));
    let mut durable = std::fs::read(fixture.store.path().join("program-runtime.sqlite3")).unwrap();
    if let Ok(wal) = std::fs::read(fixture.store.path().join("program-runtime.sqlite3-wal")) {
        durable.extend_from_slice(&wal);
    }
    assert!(
        !durable
            .windows(CONFORMANCE_SECRET.len())
            .any(|window| window == CONFORMANCE_SECRET.as_bytes()),
        "durable execution state must not contain the secret"
    );
}

/// A resolver that refuses stops the launch before a process exists and leaves
/// no durable state behind.
#[test]
fn a_refused_credential_stops_the_launch_before_anything_runs() {
    struct Refusing;

    impl CredentialResolver for Refusing {
        fn resolve(&self, _: &CredentialHandleName) -> Result<ResolvedCredential, ProgramError> {
            Err(ProgramError::Validation("the keychain is locked".into()))
        }
    }

    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let id = execution("thread-7.refused");
    assert!(
        runtime
            .launch(
                &id,
                &fixture.launch(ProgramScript::ReportsCredentialLength, bounds(60_000, 4096)),
                &Refusing,
                NOW,
            )
            .is_err()
    );
    assert!(
        runtime.inspect(&id).unwrap().is_none(),
        "a refused launch must leave nothing behind"
    );
}

/// The only type that can hold secret material refuses to show it and keeps
/// nothing after it is dropped.
#[test]
fn resolved_credentials_do_not_reveal_themselves() {
    let resolved = ResolvedCredential::new(CONFORMANCE_SECRET).unwrap();
    assert_eq!(resolved.expose_for_spawn(), CONFORMANCE_SECRET);
    assert_eq!(format!("{resolved:?}"), "ResolvedCredential(<redacted>)");
    assert!(ResolvedCredential::new("").is_err());
    assert!(ResolvedCredential::new("with\0nul").is_err());
}

// ---------------------------------------------------------------------------
// (5) Restart reconcile and orphan detection.
// ---------------------------------------------------------------------------

/// A reopened authority finds the crash-time backlog, calls it uncertain, and
/// settles an orphan as interrupted rather than as anything better.
#[test]
fn a_reopened_authority_settles_an_orphan_as_interrupted() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let id = execution("thread-7.orphaned");
    runtime
        .launch(
            &id,
            &fixture.launch(ProgramScript::Sleeps, bounds(MAX_PROGRAM_DEADLINE_MS, 4096)),
            &NoCredentials,
            NOW,
        )
        .expect("the program launches");
    runtime.abandon_for_test(&id).expect("the owner crashes");
    drop(runtime);

    let reopened = fixture.runtime();
    assert_eq!(
        reopened.requiring_reconciliation().unwrap(),
        vec![id.clone()]
    );
    assert!(matches!(
        reopened.inspect(&id).unwrap(),
        Some(ExecutionStatus::Uncertain { .. })
    ));
    assert!(matches!(reopened.wait(&id), Err(ProgramError::Unowned(_))));

    assert_eq!(
        reopened
            .reconcile(&id, &FixedProbe(Liveness::Unknown), NOW + 1_000)
            .unwrap(),
        ReconcileOutcome::Uncertain,
        "an inconclusive probe must leave the execution uncertain"
    );
    assert!(reopened.receipt(&id).unwrap().is_none());

    let outcome = reopened
        .reconcile(&id, &FixedProbe(Liveness::Gone), NOW + 2_000)
        .unwrap();
    let ReconcileOutcome::Settled(receipt) = outcome else {
        panic!("a gone process must settle");
    };
    assert_eq!(receipt.disposition, ExitDisposition::Interrupted);
    assert!(!receipt.succeeded());
    assert!(receipt.stdout.is_none() && receipt.stderr.is_none());
    assert!(reopened.requiring_reconciliation().unwrap().is_empty());
    assert_eq!(
        reopened
            .reconcile(&id, &FixedProbe(Liveness::Gone), NOW + 3_000)
            .unwrap(),
        ReconcileOutcome::Settled(receipt),
        "reconciling twice must replay rather than settle again"
    );
}

/// A live process is found alive and is not settled by the handle that asks.
#[test]
fn a_second_handle_finds_a_live_execution_alive_and_leaves_it_alone() {
    let fixture = Fixture::new();
    let runtime = fixture.runtime();
    let concurrent = fixture.runtime();
    let id = execution("thread-7.shared");
    let process = runtime
        .launch(
            &id,
            &fixture.launch(ProgramScript::Sleeps, bounds(MAX_PROGRAM_DEADLINE_MS, 4096)),
            &NoCredentials,
            NOW,
        )
        .expect("the program launches");

    assert!(matches!(
        concurrent.inspect(&id).unwrap(),
        Some(ExecutionStatus::Uncertain { .. })
    ));
    assert_eq!(
        concurrent
            .reconcile(&id, &OsLivenessProbe, NOW + 10)
            .unwrap(),
        ReconcileOutcome::StillRunning,
        "the OS probe must find a process that is genuinely running"
    );
    assert!(concurrent.receipt(&id).unwrap().is_none());
    assert!(matches!(
        runtime.inspect(&id).unwrap(),
        Some(ExecutionStatus::Running { .. })
    ));

    runtime.cancel(&id).unwrap();
    let receipt = runtime.wait(&id).unwrap();
    assert_eq!(receipt.disposition, ExitDisposition::Cancelled);
    assert_eq!(
        concurrent
            .reconcile(&id, &OsLivenessProbe, NOW + 20)
            .unwrap(),
        ReconcileOutcome::Settled(Box::new(receipt)),
        "reconciling a settled execution replays its receipt"
    );
    assert_eq!(
        OsLivenessProbe.probe(&process).unwrap(),
        Liveness::Gone,
        "the OS probe must report a reaped process as gone"
    );
}

/// A store that is not this contract's store stops the Host rather than
/// presenting foreign rows as executions.
#[test]
fn the_runtime_fails_closed_on_a_foreign_or_damaged_store() {
    let fixture = Fixture::new();
    let foreign = tempfile::tempdir().unwrap();
    let connection =
        rusqlite::Connection::open(foreign.path().join("program-runtime.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
             INSERT INTO metadata(key,value) VALUES('schema_marker','another.product.programs');
             INSERT INTO metadata(key,value) VALUES('schema_version','1');",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        LocalProgramRuntime::new(foreign.path(), fixture.sink()),
        Err(ProgramError::Corrupt(_))
    ));

    let unmarked = tempfile::tempdir().unwrap();
    let connection =
        rusqlite::Connection::open(unmarked.path().join("program-runtime.sqlite3")).unwrap();
    connection
        .execute_batch("CREATE TABLE unrelated(x INTEGER);")
        .unwrap();
    drop(connection);
    assert!(matches!(
        LocalProgramRuntime::new(unmarked.path(), fixture.sink()),
        Err(ProgramError::Corrupt(_))
    ));

    let runtime = fixture.runtime();
    let receipt = settled(
        &runtime,
        &fixture,
        "thread-7.damaged",
        ProgramScript::Settles,
        bounds(60_000, 4096),
    );
    drop(runtime);
    let connection =
        rusqlite::Connection::open(fixture.store.path().join("program-runtime.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE executions SET launch='{\"program\":\"/bin/sh\"}' WHERE execution_id=?1",
            [receipt.execution.as_str()],
        )
        .unwrap();
    drop(connection);
    assert!(
        matches!(
            fixture.runtime().inspect(&receipt.execution),
            Err(ProgramError::Corrupt(_))
        ),
        "an undecodable launch record must fail the read"
    );
}

// ---------------------------------------------------------------------------
// (6, 7) The reference runtime passes the suite; a backend that fabricates
// success for an orphan does not.
// ---------------------------------------------------------------------------

struct ReferenceHarness {
    fixture: Fixture,
    /// Every handle this harness has opened. `abandon` has to reach whichever
    /// one owns the process, which is not necessarily the most recent.
    opened: Vec<Arc<LocalProgramRuntime>>,
}

impl ReferenceHarness {
    fn new() -> Self {
        Self {
            fixture: Fixture::new(),
            opened: Vec::new(),
        }
    }

    fn latest(&self) -> Arc<LocalProgramRuntime> {
        Arc::clone(self.opened.last().expect("an open runtime"))
    }
}

impl ProgramRuntimeHarness for ReferenceHarness {
    fn open(&mut self, _: ConformanceOpen) -> Result<Arc<dyn ProgramRuntime>, ProgramError> {
        let runtime = self.fixture.runtime();
        self.opened.push(Arc::clone(&runtime));
        Ok(runtime as Arc<dyn ProgramRuntime>)
    }

    fn launch(
        &mut self,
        script: ProgramScript,
        bounds: ProgramBounds,
    ) -> Result<ProgramLaunch, ProgramError> {
        Ok(self.fixture.launch(script, bounds))
    }

    fn abandon(&mut self, execution: &ExecutionId) -> Result<(), ProgramError> {
        for runtime in &self.opened {
            if runtime.abandon_for_test(execution).is_ok() {
                return Ok(());
            }
        }
        Err(ProgramError::NotFound(execution.clone()))
    }

    fn durable_bytes(&mut self) -> Result<Vec<u8>, ProgramError> {
        let mut bytes = std::fs::read(self.fixture.store.path().join("program-runtime.sqlite3"))
            .map_err(|error| ProgramError::Storage(error.to_string()))?;
        if let Ok(wal) = std::fs::read(
            self.fixture
                .store
                .path()
                .join("program-runtime.sqlite3-wal"),
        ) {
            bytes.extend_from_slice(&wal);
        }
        Ok(bytes)
    }

    fn captured(&mut self, artifact: &ArtifactHandle) -> Result<Vec<u8>, ProgramError> {
        Ok(self.fixture.captured(artifact))
    }
}

/// The reference runtime is the contract's own proof: real processes, a real
/// store, and every requirement asserted by the published suite.
#[test]
fn the_reference_program_runtime_passes_its_own_conformance() {
    let mut harness = ReferenceHarness::new();
    run_program_runtime_conformance(&mut harness).expect("the reference runtime conforms");
}

/// (7) A backend that reports an orphan as a successful execution is exactly
/// the fabricated settlement this contract exists to prevent.
#[test]
fn a_backend_that_fabricates_success_for_an_orphan_fails_the_conformance() {
    /// Delegates every honest answer to the reference runtime, but settles a
    /// gone process as a clean exit — so a Host would record work as done that
    /// nobody ever observed finishing.
    struct OptimisticRuntime {
        inner: Arc<LocalProgramRuntime>,
    }

    impl ProgramRuntime for OptimisticRuntime {
        fn launch(
            &self,
            execution: &ExecutionId,
            launch: &ProgramLaunch,
            credentials: &dyn CredentialResolver,
            now_ms: u64,
        ) -> Result<ProcessIdentity, ProgramError> {
            self.inner.launch(execution, launch, credentials, now_ms)
        }

        fn wait(
            &self,
            execution: &ExecutionId,
        ) -> Result<grok_build_sdk::ExecutionReceipt, ProgramError> {
            self.inner.wait(execution)
        }

        fn cancel(&self, execution: &ExecutionId) -> Result<(), ProgramError> {
            self.inner.cancel(execution)
        }

        fn inspect(
            &self,
            execution: &ExecutionId,
        ) -> Result<Option<ExecutionStatus>, ProgramError> {
            self.inner.inspect(execution)
        }

        fn requiring_reconciliation(&self) -> Result<Vec<ExecutionId>, ProgramError> {
            self.inner.requiring_reconciliation()
        }

        fn reconcile(
            &self,
            execution: &ExecutionId,
            liveness: &dyn LivenessProbe,
            now_ms: u64,
        ) -> Result<ReconcileOutcome, ProgramError> {
            match self.inner.reconcile(execution, liveness, now_ms)? {
                ReconcileOutcome::Settled(receipt)
                    if receipt.disposition == ExitDisposition::Interrupted =>
                {
                    let mut optimistic = *receipt;
                    optimistic.disposition = ExitDisposition::Exited { code: 0 };
                    Ok(ReconcileOutcome::Settled(Box::new(optimistic)))
                }
                honest => Ok(honest),
            }
        }
    }

    struct OptimisticHarness(ReferenceHarness);

    impl ProgramRuntimeHarness for OptimisticHarness {
        fn open(
            &mut self,
            phase: ConformanceOpen,
        ) -> Result<Arc<dyn ProgramRuntime>, ProgramError> {
            self.0.open(phase)?;
            Ok(Arc::new(OptimisticRuntime {
                inner: self.0.latest(),
            }) as Arc<dyn ProgramRuntime>)
        }

        fn launch(
            &mut self,
            script: ProgramScript,
            bounds: ProgramBounds,
        ) -> Result<ProgramLaunch, ProgramError> {
            self.0.launch(script, bounds)
        }

        fn abandon(&mut self, execution: &ExecutionId) -> Result<(), ProgramError> {
            self.0.abandon(execution)
        }

        fn durable_bytes(&mut self) -> Result<Vec<u8>, ProgramError> {
            self.0.durable_bytes()
        }

        fn captured(&mut self, artifact: &ArtifactHandle) -> Result<Vec<u8>, ProgramError> {
            self.0.captured(artifact)
        }
    }

    let mut harness = OptimisticHarness(ReferenceHarness::new());
    let error = run_program_runtime_conformance(&mut harness)
        .expect_err("a backend that fabricates success cannot pass");
    assert!(
        error.to_string().contains("conformance: "),
        "the suite should name itself in the failure: {error}"
    );
}
