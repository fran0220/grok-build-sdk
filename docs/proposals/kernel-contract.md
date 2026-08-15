# Proposal: persistent kernel contract and bounded workflow driver

Status: approved and implemented. This document is now the design record for
the shipped contract; where the implementation departs from the sketch below,
§7 says so and the code is authoritative.

Scope: the shape of a new `crate::kernel` contract module and the rules a
bounded workflow driver must follow. Read alongside `src/program.rs`
(`ProgramRuntime`), `src/artifact.rs` (`ArtifactVault`), `src/activation.rs`
(`ActivationCoordinator`) and `src/prime.rs` (`ProgramDriver`,
`PersistentKernelDriver`). Everything below follows the house rules those
modules already hold: validated newtypes, constants that publish every bound,
fail-closed decode, append-only digest-verified receipts, a local reference
implementation, a conformance suite with negative controls, no clock inside the
contract, host-neutral vocabulary.

---

## 1. Motivation and scope

### 1.1 What `ProgramRuntime` already answers

`ProgramRuntime` owns the durable custody of a *one-shot* execution: a process
is named before it is spawned, its bounds are declared at launch, its output is
captured to an `ArtifactHandle` with an honest truncation record, and it settles
into exactly one `ExitDisposition`. A Host that dies mid-execution finds the
execution again through `requiring_reconciliation` and settles the orphan as
`ExitDisposition::Interrupted` — never as success.

That contract assumes the interesting unit is a process that starts, produces,
and stops. Its identity is `ExecutionId`, and identity and process lifetime are
the same thing.

### 1.2 What it does not answer

A *persistent programmatic kernel* is a long-lived interpreter or VM session —
a Python kernel, a notebook backend, a language REPL, a scripting VM — that
outlives any single Turn and accumulates in-memory state across many
executions. Its identity and its process lifetime are deliberately *not* the
same thing:

- Many executions run inside one process, in order, sharing state.
- An execution can be cancelled without the session ending.
- The session can be checkpointed and later restored into a *different*
  process, and the restored state is necessarily a subset of what was there.
- The process can die while the session is still a thing the Host has a name
  for and a durable expectation about.

Modelling this as repeated `ProgramRuntime` launches loses exactly the property
the kernel exists for: shared state between executions. Modelling it by
weakening `ProgramRuntime` would cost the one-shot contract its clarity. So this
is a sibling contract that shares vocabulary rather than an extension of an
existing one.

### 1.3 What this is not

Three exclusions are load-bearing and belong in the module documentation, not
just in this proposal.

**Not a second agent loop.** The kernel executes code fragments a caller hands
it. It does not choose what to run, does not call a model, does not iterate, and
has no notion of a Turn. Every decision to submit an execution comes from the
Run reducer through the existing seam. Nothing in this contract may grow a
method that means "keep going".

**Not durable truth.** Kernel in-memory state is *evidence*, never authority. A
checkpoint is an artifact-addressed snapshot that a Host may use to shorten
recovery; it is never the source of a fact the Host needs. Every durable fact
about what happened lives in the Run (operations, receipts) and in the artifact
vault (inputs and outputs). This is the same rule `PersistentKernelDriver`
already states — *a checkpoint is evidence, not authority* — and this contract
makes it structural rather than advisory.

**Not silently lossy.** Kernel state is always either reconstructible from
durable inputs, or explicitly declared lost with a typed reason the Host must
surface. There is no third case. A restore that quietly produces a session
missing an open file handle, a network connection, or a child process is the
failure mode this contract exists to prevent.

---

## 2. Contract sketch

Signatures only. Bodies, storage layout and the local reference implementation
are out of scope for the approval decision.

### 2.1 Bounds

Every limit is a public constant, as in `program.rs`.

```rust
pub const MAX_KERNEL_SESSION_ID_BYTES: usize = 128;
pub const MAX_KERNEL_EXECUTION_ID_BYTES: usize = 128;
pub const MAX_KERNEL_SOURCE_BYTES: usize = 1024 * 1024;
pub const MAX_KERNEL_CAPTURE_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_KERNEL_EXECUTION_DEADLINE_MS: u64 = 24 * 60 * 60 * 1000;
pub const MAX_KERNEL_IDLE_DEADLINE_MS: u64 = 24 * 60 * 60 * 1000;
/// Executions one session may accumulate before it must be closed and
/// reopened. Sequence numbers are dense and never reused.
pub const MAX_KERNEL_SESSION_EXECUTIONS: u64 = 100_000;
pub const MAX_KERNEL_NON_RESTORABLE_FACTS: usize = 256;
/// Bounded so a checkpoint cannot become a de facto state store.
pub const MAX_KERNEL_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_KERNEL_LABEL_BYTES: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    #[error("kernel input is invalid: {0}")]
    Validation(String),
    #[error("kernel session {0} is not known to this authority")]
    SessionNotFound(KernelSessionId),
    #[error("kernel execution {0} is not known to this authority")]
    ExecutionNotFound(KernelExecutionId),
    #[error("kernel state conflicts with durable state: {0}")]
    Conflict(String),
    #[error("kernel session {0} is owned by another handle")]
    Unowned(KernelSessionId),
    #[error("kernel session {0} is not live")]
    NotLive(KernelSessionId),
    #[error("kernel could not be started: {0}")]
    Start(String),
    #[error("kernel storage failed: {0}")]
    Storage(String),
    #[error("durable kernel state is corrupt: {0}")]
    Corrupt(String),
}
```

### 2.2 Session identity

```rust
/// The durable identity of one kernel session, chosen by the caller.
///
/// Caller-supplied for the same reason `ExecutionId` is: a Host that crashes
/// between deciding to start a kernel and hearing that it started can only ask
/// *does session X exist* if it named X first.
pub struct KernelSessionId(String);

/// Monotonic incarnation counter for one session identity.
///
/// A session identity survives process death; a *generation* does not. Every
/// successful `open` or `restore` mints the next generation and never reuses a
/// value, so a receipt or a checkpoint always says which incarnation produced
/// it. This is the same fencing discipline as `ActivationFencingToken`.
pub struct KernelGeneration(u64);

/// The identity of one execution *within* one session.
///
/// Scoped rather than global: two sessions may use the same execution name and
/// mean different things, and the pair is what a receipt addresses.
pub struct KernelExecutionId(String);

/// The addressable identity of one execution: session, generation, execution.
pub struct KernelExecutionKey {
    session: KernelSessionId,
    generation: KernelGeneration,
    execution: KernelExecutionId,
}
```

### 2.3 Opening a session

```rust
/// A kernel image the Host is willing to run.
///
/// The program path and its argument vector are declared exactly as in
/// `ProgramLaunch`, absolute-path rule included, so a receipt's claim about
/// what ran stays verifiable.
pub struct KernelSpec {
    program: ProgramPath,
    arguments: Vec<String>,
    /// Literal environment bindings only. See §2.8.
    environment: BTreeMap<String, String>,
    working_root: PathBuf,
    bounds: KernelSessionBounds,
}

impl KernelSpec {
    pub fn new(
        program: ProgramPath,
        working_root: impl Into<PathBuf>,
        bounds: KernelSessionBounds,
    ) -> Result<Self, KernelError>;

    pub fn argument(self, value: impl Into<String>) -> Result<Self, KernelError>;
    pub fn environment(
        self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, KernelError>;

    /// Identifies the image without reproducing it, using the same
    /// length-prefixed canonical encoding as `program.rs`.
    pub fn spec_digest(&self) -> ArtifactDigest;
    pub fn validate(&self) -> Result<(), KernelError>;
}

/// Limits the whole session is held to.
pub struct KernelSessionBounds {
    /// The session settles as `KernelDisposition::IdleExpired` if no execution
    /// is submitted within this window. Zero is refused: a session nothing will
    /// ever settle is the failure this contract exists to prevent.
    idle_deadline_ms: u64,
    /// Ceiling on executions this incarnation may accept.
    max_executions: u64,
    /// Ceiling on total captured bytes across this incarnation's executions.
    max_captured_bytes: u64,
}
```

### 2.4 Submitting and cancelling executions, bounded output

Capture vocabulary is *reused*, not re-invented: an execution's captured output
is described by `program::CaptureRecord`, bound to `ArtifactHandle`, with
`produced_bytes`, `captured_bytes`, `declared_bound` and `truncated` carrying
exactly the meanings they already have. Storage goes through the same
`ProgramOutputSink` seam, so a Host with a vault gets the binding for free
through `ArtifactVaultOutputSink`.

```rust
/// One unit of work handed to a live session.
pub struct KernelSubmission {
    /// The fragment to execute. Content, not a path, so the receipt's
    /// `source_digest` addresses exactly what ran.
    source: String,
    bounds: KernelExecutionBounds,
}

/// Per-execution limits. Declared at submit so a receipt can report the limit
/// that produced a truncation or a timeout.
pub struct KernelExecutionBounds {
    deadline_ms: u64,
    stdout_capture_bytes: u64,
    stderr_capture_bytes: u64,
}

/// How one execution inside a session ended.
///
/// Deliberately narrower than `ExitDisposition`: a kernel execution does not
/// exit a process, so `Signalled` and `FailedToStart` have no meaning here, and
/// `KernelDied` is a case `ExitDisposition` has no name for.
#[non_exhaustive]
pub enum KernelExecutionDisposition {
    /// The fragment ran to completion without raising.
    Completed,
    /// The fragment raised. `error_class` is a bounded, non-secret label the
    /// kernel reported; the detail is in the captured stderr artifact.
    Raised { error_class: KernelLabel },
    /// A caller asked for this execution to stop and the session survived.
    Cancelled,
    /// The declared deadline elapsed.
    TimedOut,
    /// The kernel process ended while this execution was in flight. The work
    /// may have completed, partly completed or not started.
    KernelDied,
    /// The execution was in flight when its owner died and the kernel is gone.
    Interrupted,
}

impl KernelExecutionDisposition {
    /// Success is exactly one thing.
    pub fn is_success(self) -> bool;
    pub fn as_token(self) -> String;
    pub fn parse(value: &str) -> Result<Self, KernelError>;
}

/// The durable, digest-verified account of one settled kernel execution.
/// Written once, never edited.
pub struct KernelExecutionReceipt {
    pub key: KernelExecutionKey,
    /// Dense position of this execution in its session, from 1.
    pub sequence: u64,
    pub source_digest: ArtifactDigest,
    pub spec_digest: ArtifactDigest,
    pub bounds: KernelExecutionBounds,
    pub disposition: KernelExecutionDisposition,
    /// The wall instant the caller declared at submit.
    pub started_at_ms: u64,
    /// `started_at_ms` plus the elapsed time the backend measured.
    pub settled_at_ms: u64,
    pub stdout: Option<CaptureRecord>,
    pub stderr: Option<CaptureRecord>,
    /// The checkpoint taken immediately after this execution, when one was
    /// requested. Evidence only.
    pub checkpoint: Option<KernelCheckpointRef>,
}

impl KernelExecutionReceipt {
    pub fn digest(&self) -> ArtifactDigest;
    pub fn verify(&self, expected: &ArtifactDigest) -> Result<(), KernelError>;
    pub fn truncated(&self) -> bool;
    pub fn succeeded(&self) -> bool;
}
```

Cancellation semantics mirror `ProgramRuntime::cancel` and add one rule the
one-shot contract does not need: **cancel is scoped**. Cancelling an execution
asks the kernel to abandon that fragment and leaves the session live; killing a
session is a separate, explicit call. A backend that can only cancel by killing
the kernel must settle the execution as `KernelDied`, not as `Cancelled` — the
disposition has to say what actually happened.

### 2.5 Checkpoints: evidence with an explicit restorable subset

```rust
/// A named, artifact-addressed snapshot of session state.
///
/// A checkpoint is *evidence*. It records what the kernel was able to
/// serialise, what it deliberately did not, and the incarnation it came from.
/// Nothing in the Host may treat it as the authority for a fact.
pub struct KernelCheckpointRef {
    /// Content-addressed snapshot payload.
    artifact: ArtifactHandle,
    session: KernelSessionId,
    generation: KernelGeneration,
    /// The execution sequence this checkpoint was taken after.
    after_sequence: u64,
    /// The kernel image the snapshot was produced by. A restore into a
    /// different image is refused rather than attempted.
    spec_digest: ArtifactDigest,
    taken_at_ms: u64,
    /// What the kernel claims it captured, as typed declarations.
    restorable: Vec<RestorableFact>,
    /// What the kernel declares it could not capture. Never empty by
    /// accident: see `KernelCheckpointRef::validate`.
    non_restorable: Vec<NonRestorableFact>,
}

/// A category of state a checkpoint claims to carry.
#[non_exhaustive]
pub enum RestorableFact {
    /// Named top-level bindings, counted, not enumerated by value.
    Bindings { count: u64 },
    /// Loaded modules or packages by bounded label.
    Modules { count: u64 },
    /// A Host-defined category the kernel image documents.
    Declared { kind: KernelLabel, count: u64 },
}

/// A category of state a checkpoint declares is lost across restore.
///
/// This is the whole of requirement three. It is a typed value that travels
/// with the checkpoint and comes back out of `restore`, so a Host cannot
/// consume a restored session without being handed the list of things that did
/// not come back. There is no representation of "restored, probably complete".
#[non_exhaustive]
pub enum NonRestorableFact {
    /// Open file descriptors held by the kernel.
    OpenFile { count: u64 },
    /// Live network connections.
    NetworkConnection { count: u64 },
    /// Child processes the kernel spawned.
    ChildProcess { count: u64 },
    /// Threads, coroutines or tasks that were running.
    ConcurrentTask { count: u64 },
    /// Handles onto external systems the kernel cannot serialise.
    ForeignHandle { kind: KernelLabel, count: u64 },
    /// Values whose type the kernel could not serialise.
    UnserialisableValue { kind: KernelLabel, count: u64 },
    /// Filesystem mutations made outside the working root, which a restore
    /// does not and must not undo.
    ExternalMutation { kind: KernelLabel },
}

impl KernelCheckpointRef {
    /// Re-checks a checkpoint decoded from storage. In particular it refuses a
    /// checkpoint that declares neither restorable nor non-restorable facts,
    /// because a snapshot that claims nothing is a snapshot whose losses were
    /// never enumerated.
    pub fn validate(&self) -> Result<(), KernelError>;
    pub fn digest(&self) -> ArtifactDigest;
}

/// What a restore produced.
#[non_exhaustive]
pub enum KernelRestore {
    /// A new incarnation is live. `lost` is the checkpoint's non-restorable
    /// declaration, handed back so the calling Host must receive it in order
    /// to receive the session at all.
    Restored {
        session: KernelSessionId,
        generation: KernelGeneration,
        lost: Vec<NonRestorableFact>,
    },
    /// The checkpoint is well-formed but was produced by a different kernel
    /// image. Nothing was started.
    SpecMismatch { expected: ArtifactDigest, found: ArtifactDigest },
    /// The kernel image refused the snapshot. Nothing was started; the Host
    /// must reconstruct from durable inputs instead.
    Rejected { reason: KernelLabel },
}
```

The important structural property: `KernelRestore::Restored` carries `lost` by
value. There is no accessor that yields a live session handle without it, so
"the Host forgot to surface the loss" requires the Host to actively discard a
value it was handed, rather than to omit a call it did not know about.

### 2.6 Liveness, settlement and reconcile

Mirrors `ProgramRuntime` one-for-one, reusing `ProcessIdentity`, `Liveness` and
`LivenessProbe` unchanged.

```rust
/// How a whole session ended.
#[non_exhaustive]
pub enum KernelDisposition {
    /// A caller closed it.
    Closed,
    /// The kernel process exited on its own.
    Exited { code: i32 },
    /// The idle deadline elapsed.
    IdleExpired,
    /// A declared session ceiling was reached.
    CeilingReached { ceiling: KernelLabel },
    /// The session was live when its owner died, and the process is gone.
    Interrupted,
    /// No kernel process was ever created.
    FailedToStart,
}

/// What the authority currently knows about one session.
#[non_exhaustive]
pub enum KernelSessionStatus {
    Live { generation: KernelGeneration, process: ProcessIdentity, .. },
    /// Durably recorded as live, but not by this handle. Nothing about its
    /// fate is known until it is reconciled, and it never decays into success.
    Uncertain { generation: KernelGeneration, process: ProcessIdentity, .. },
    Settled(Box<KernelSessionReceipt>),
}
```

An orphan session settles as `KernelDisposition::Interrupted`, and *every
in-flight execution it owned settles as*
`KernelExecutionDisposition::Interrupted` in the same transaction. That
coupling is the rule a reviewer should check hardest: a session receipt that
settles while an execution receipt is still missing would let a Host conclude a
fragment succeeded because nothing said otherwise.

### 2.7 The trait

```rust
/// Durable custody of persistent kernel sessions.
///
/// Implementations own process creation, transactions and physical layout.
/// They persist and fail-closed verify a schema marker and version, refuse
/// stored state they cannot decode within the published bounds, and make every
/// method atomic against every other handle to the same authority — including
/// handles in other processes.
///
/// Implementations own a monotonic duration source and no wall clock. Every
/// wall instant that reaches a receipt is derived from a `now_ms` a caller
/// declared.
pub trait KernelRuntime: Send + Sync + 'static {
    /// Validates the spec, starts the kernel and durably records the session as
    /// live before returning. A session identity that is already live or
    /// settled is `KernelError::Conflict`, never a second process.
    fn open(
        &self,
        session: &KernelSessionId,
        spec: &KernelSpec,
        now_ms: u64,
    ) -> Result<KernelGeneration, KernelError>;

    /// Submits one fragment and durably records it as in flight before
    /// returning. A submission whose execution identity is already known in
    /// this session is `KernelError::Conflict`, so a retried submit after an
    /// unknown outcome cannot double-execute.
    fn submit(
        &self,
        key: &KernelExecutionKey,
        submission: &KernelSubmission,
        sink: &dyn ProgramOutputSink,
        now_ms: u64,
    ) -> Result<(), KernelError>;

    /// Blocks until the execution settles and answers its receipt. Waiting on
    /// an already-settled execution replays its stored receipt unchanged.
    fn wait(&self, key: &KernelExecutionKey) -> Result<KernelExecutionReceipt, KernelError>;

    /// Asks one in-flight execution to stop, leaving the session live.
    /// Idempotent; never rewrites a settlement that already happened.
    fn cancel(&self, key: &KernelExecutionKey) -> Result<(), KernelError>;

    /// Ends the session, settling it and every in-flight execution together.
    fn close(
        &self,
        session: &KernelSessionId,
        now_ms: u64,
    ) -> Result<KernelSessionReceipt, KernelError>;

    /// Takes an evidence snapshot of a live session.
    fn checkpoint(
        &self,
        session: &KernelSessionId,
        sink: &dyn ProgramOutputSink,
        now_ms: u64,
    ) -> Result<KernelCheckpointRef, KernelError>;

    /// Starts a new incarnation from a checkpoint. The new incarnation is
    /// recorded durably before this returns, exactly as `open` does.
    fn restore(
        &self,
        session: &KernelSessionId,
        checkpoint: &KernelCheckpointRef,
        spec: &KernelSpec,
        now_ms: u64,
    ) -> Result<KernelRestore, KernelError>;

    fn inspect_session(
        &self,
        session: &KernelSessionId,
    ) -> Result<Option<KernelSessionStatus>, KernelError>;

    fn inspect_execution(
        &self,
        key: &KernelExecutionKey,
    ) -> Result<Option<KernelExecutionStatus>, KernelError>;

    /// Sessions this authority durably believes are live but that this handle
    /// does not own, in identity order. After a restart this is exactly the
    /// crash-time backlog.
    fn requiring_reconciliation(&self) -> Result<Vec<KernelSessionId>, KernelError>;

    /// Resolves an uncertain session using the caller's liveness evidence. A
    /// live kernel answers `StillRunning` and settles nothing. A gone kernel
    /// settles the session as `Interrupted` and every in-flight execution as
    /// `Interrupted`. An inconclusive probe leaves everything uncertain.
    fn reconcile(
        &self,
        session: &KernelSessionId,
        liveness: &dyn LivenessProbe,
        now_ms: u64,
    ) -> Result<KernelReconcileOutcome, KernelError>;
}
```

### 2.8 Credentials: structural, not policy

The rule is that a kernel process receives no provider credential and no relay
bearer. The contract makes this true by construction, in three ways:

1. **There is no credential type in this module.** `KernelSpec::environment`
   takes a literal `String` and there is no `credential` builder,
   no `EnvironmentBinding` enum, and therefore no shape in which a handle name
   could be attached.
2. **No method takes a `CredentialResolver`.** `open`, `restore` and `submit`
   have no parameter a secret could arrive through. This is the same argument
   `program.rs` makes about its durable state, applied one level earlier: in
   `ProgramRuntime` a secret can reach the process but not the record; here it
   cannot reach the process either, because the signature has nowhere to put it.
3. **Validation refuses reserved names.** `KernelSpec::validate` refuses
   environment names in a published reserved set (the provider and relay
   variable names the Host uses), so a Host cannot smuggle a value in as a
   literal by naming it the thing a kernel library would read.

A kernel that needs a network capability gets it the same way a plugin does:
over MCP, from a process that already has a credential boundary. That is a Host
decision and appears nowhere in this contract.

---

## 3. Bounded workflow driver

### 3.1 The rule

A workflow is a bounded sequence of steps that a Host wants to run unattended
and be able to resume after a crash. The temptation is to give it a state
store. **This proposal explicitly does not.** A workflow executes entirely
through durable Run intents and receipts that already exist, and every piece of
its state has exactly one existing owner.

### 3.2 Where each piece of state already lives

| State | Existing owner | Mechanism |
|---|---|---|
| Which workflow is due, and when | `ActivationCoordinator` | `ActivationWake` / `claim_due` |
| Who may execute it right now | `ActivationCoordinator` | `ActivationFencingToken`, lease + renew |
| Whether the outcome was recorded | `ActivationCoordinator` | `ActivationSettlement::AlreadySettled` |
| Step sequence and current position | `run` | `IterationId`, `BeginIteration` / `FinishIteration` |
| One step's declared intent | `run` | `PrepareOperation` + `EffectSpec` |
| One step's exclusive execution right | `run` | `ClaimEffect` + `ActivationFence` |
| One step's outcome | `run` | `EffectReceipt`, `acknowledge_effect` |
| Unknown-outcome recovery | `run` | `ReconcileEffect`, `ReconcileDecision` |
| Step inputs and outputs | `ArtifactVault` / `ArtifactStore` | `ArtifactRef`, digest-verified |
| Resource consumption | `run` | `ResourceVector` reservation and `EffectUsage` |
| Kernel session state | `KernelRuntime` | evidence only, never consulted for truth |

There is no row without an owner, which is the argument for adding no store.

### 3.3 Ceilings

```rust
/// Declared ceilings one workflow run is held to. Validated before the first
/// step is prepared, so a workflow that cannot terminate never starts.
pub struct WorkflowCeilings {
    /// Maximum steps. Zero is refused.
    max_steps: u32,
    /// Maximum wall time from first claim to settlement, in milliseconds.
    max_wall_ms: u64,
    /// Maximum consecutive steps that may settle unsuccessfully before the
    /// workflow stops. Zero is refused.
    max_consecutive_failures: u32,
    /// Resource budget for the whole workflow. Every finite dimension must be
    /// finite and non-zero, enforced exactly as
    /// `ProviderCapability::validate_dispatch_bound` already does.
    budget: ResourceVector,
}

/// One step's declaration. It is a `ProgramIntent` or a kernel submission, plus
/// its share of the ceilings; it is not a new durable object.
pub struct WorkflowStepIntent {
    run_id: RunId,
    operation_id: OperationId,
    iteration_id: IterationId,
    action: WorkflowAction,
    reservation: ResourceVector,
}

#[non_exhaustive]
pub enum WorkflowAction {
    Program(ProgramIntent),
    KernelSubmit { session: KernelSessionId, submission: ArtifactRef },
}

/// Why a workflow stopped. Every ceiling has its own name; there is no
/// variant meaning "ran out of something".
#[non_exhaustive]
pub enum WorkflowDisposition {
    Completed,
    StepCeiling,
    WallCeiling,
    BudgetCeiling { dimension: ResourceDimension },
    ConsecutiveFailureCeiling,
    Cancelled,
    Interrupted,
}
```

### 3.4 The driver loop, in terms of existing calls

The driver is a thin function over machinery that already exists. Written out
so the "no parallel state store" claim is checkable:

1. `ActivationCoordinator::claim_due` yields an `ActivationGrant` whose payload
   names the `RunId`. The grant's token becomes the `ActivationFence` on every
   `ClaimEffect` below, so a superseded driver cannot commit a step.
2. Load the Run. `RunRevision` from the snapshot is the CAS value for every
   mutation. The *current step index is the Run's iteration count* — the driver
   holds no counter of its own.
3. Check ceilings against the Run snapshot: `max_steps` against iterations
   already begun, `max_wall_ms` against the caller's `now_ms` minus the Run's
   recorded start, `budget` against accumulated `EffectUsage`. All four inputs
   are read from the Run.
4. `BeginIteration`, then `prepare_operation` with the step's `EffectSpec`, then
   `claim_effect` with the reservation and the activation fence.
5. Execute through `ProgramDriver::execute` or `KernelRuntime::submit` +
   `wait`. Verify the receipt against the claim exactly as
   `Runtime::execute_program` already does.
6. `acknowledge_effect` with `EffectOutcome::Applied` or `Unknown`. `Unknown` is
   a real answer and leaves the operation for `reconcile_effect`.
7. `renew` the activation lease between steps. A `Fenced` renewal means stop
   immediately, mid-workflow, without settling anything.
8. On the terminal step, `FinishIteration` and then `release` with
   `ActivationDisposition::Complete`; on a ceiling, `release` with `Complete`
   and the disposition recorded in the Run; on a resumable pause, `release`
   with `Yield { due_at_ms }`.
9. On restart, the backlog is `RecoveryPlan` from the Run plus
   `KernelRuntime::requiring_reconciliation`. There is no third list to consult.

The only new durable bytes anywhere in §3 are the `WorkflowCeilings` and the
`WorkflowDisposition`, and both belong inside the Run envelope as ordinary Run
content.

---

## 4. Conformance suite outline

Same shape as `run_program_runtime_conformance`: a harness trait the backend
implements, `ConformanceOpen` phases (`Fresh`, `Concurrent`, `Reopen`), fixed
kernel fragments described by behaviour rather than by text, and named
properties. Every property below is one function.

**Harness**

```rust
pub trait KernelRuntimeHarness {
    fn open(&mut self, phase: ConformanceOpen) -> Result<Arc<dyn KernelRuntime>, KernelError>;
    fn spec(&mut self, bounds: KernelSessionBounds) -> Result<KernelSpec, KernelError>;
    fn fragment(&mut self, script: KernelScript) -> Result<KernelSubmission, KernelError>;
    /// Kills the kernel process without settling the durable record, leaving
    /// the store exactly as a crash would.
    fn abandon(&mut self, session: &KernelSessionId) -> Result<(), KernelError>;
    fn durable_bytes(&mut self) -> Result<Vec<u8>, KernelError>;
    fn captured(&mut self, artifact: &ArtifactHandle) -> Result<Vec<u8>, KernelError>;
}

#[non_exhaustive]
pub enum KernelScript {
    /// Writes a known mark to stdout and stderr and completes.
    Settles,
    /// Binds a known name to a known value and completes. Used to prove state
    /// survives across executions and across restore.
    BindsState,
    /// Reads the name `BindsState` bound and writes its value.
    ReadsState,
    /// Raises with a known error class.
    Raises,
    /// Writes at least the flood threshold to stdout, then completes.
    Floods,
    /// Runs for at least thirty seconds unless stopped.
    Sleeps,
    /// Opens a file handle and leaves it open. Used to force a checkpoint to
    /// declare a non-restorable fact.
    HoldsUnserialisableState,
}
```

**Positive properties**

1. `a_session_is_named_then_opened_then_settled` — `open` records liveness
   before returning; `close` yields a session receipt that verifies.
2. `executions_share_state_in_order` — `BindsState` then `ReadsState` observes
   the bound value; sequence numbers are dense from 1.
3. `a_settled_execution_is_replayed_rather_than_repeated` — a second `wait`
   returns the identical receipt, artifact handles included.
4. `declared_bounds_are_validated_before_anything_runs` — zero deadline,
   over-limit capture bound and over-limit source are refused with no process
   created and nothing durable written.
5. `a_raise_is_reported_as_a_raise` — `Raised` with its class, never
   `Completed`.
6. `output_beyond_its_bound_is_recorded_truncation` — `Floods` yields
   `truncated == true`, `produced_bytes > captured_bytes`,
   `captured_bytes <= declared_bound`.
7. `a_cancelled_execution_leaves_the_session_live` — a cancelled `Sleeps`
   settles `Cancelled` and a following `ReadsState` still works.
8. `an_elapsed_deadline_settles_as_timed_out`.
9. `a_checkpoint_addresses_its_own_payload`.
10. `a_restore_carries_forward_the_declared_restorable_subset` — `BindsState`,
    checkpoint, `abandon`, `restore`, `ReadsState` observes the value; the new
    generation is strictly greater.
11. `a_restore_declares_what_it_lost` — after `HoldsUnserialisableState`,
    `non_restorable` is non-empty and `Restored` hands back the same list.
12. `a_restart_surfaces_the_crash_time_backlog`.
13. `an_orphan_session_settles_as_interrupted` — with every in-flight execution
    settling `Interrupted` in the same reconcile.
14. `receipts_survive_a_restart_unchanged`.
15. `concurrent_handles_agree` — a `Concurrent` handle sees the `Fresh`
    handle's session as `Uncertain`, not as its own.

**Negative controls**

- N1 `no_credential_can_be_declared` — reserved environment names are refused
  by `KernelSpec::validate`; a compile-fail case asserts no trait method
  accepts a resolver.
- N2 `no_secret_reaches_durable_state` — a planted secret appears nowhere in
  `durable_bytes()`.
- N3 `an_uncertain_probe_leaves_a_session_uncertain` — `Liveness::Unknown`
  changes nothing, however many times it is asked.
- N4 `a_checkpoint_from_another_image_is_refused` — a differing `spec_digest`
  answers `SpecMismatch` with no process created.
- N5 `a_checkpoint_that_declares_nothing_is_corrupt` — empty `restorable` and
  empty `non_restorable` fails `validate` rather than restoring silently.
- N6 `a_tampered_receipt_fails_verification` — `Corrupt`, not `None`.
- N7 `an_undecodable_row_is_corrupt_not_absent` — `inspect_session` answers
  `Corrupt`, never `Ok(None)`.
- N8 `a_duplicate_execution_identity_conflicts` — `Conflict`, not a second run.
- N9 `a_session_ceiling_stops_the_session` — `CeilingReached`, further submits
  refused.
- N10 `a_fenced_workflow_driver_cannot_commit` — a stale `ActivationFence` on
  `claim_effect` is refused, so a superseded driver's step never reaches
  `acknowledge_effect`.

---

## 5. Open questions for the approver

**Q1. One session, one in-flight execution — or many?**
Allowing concurrent executions inside a session means the receipt ordering no
longer implies state ordering, and `sequence` stops being meaningful.
*Recommendation: strictly one in-flight execution per session incarnation.* A
Host that wants parallelism opens more sessions. If this is wrong we can add a
`max_concurrent_executions` bound additively later; we cannot remove ordering
guarantees later.

**Q2. Should `restore` be allowed to partially succeed?**
A kernel image might restore most bindings and fail on one.
*Recommendation: no partial variant.* `Restored` already carries
`non_restorable`, which is where a per-value failure belongs
(`UnserialisableValue`). A separate `PartiallyRestored` would be a second way
to say the same thing and a place for ambiguity to grow.

**Q3. Should checkpoint payload bytes go through `ArtifactVault` or through the
`ProgramOutputSink` seam?**
*Recommendation: the sink seam*, exactly as capture does, for the reason
`program.rs` gives: it keeps the kernel contract usable by a Host that has not
adopted a vault, while a Host with a vault gets the binding for free.

**Q4. Does the kernel contract get its own error type or reuse `ProgramError`?**
*Recommendation: its own `KernelError`.* `ProgramError::NotFound(ExecutionId)`
cannot name a session, and widening `ProgramError` would change a stable public
enum for a neighbour's benefit.

**Q5. Where does `WorkflowCeilings` live — `kernel`, `run`, or a new
`workflow` module?**
*Recommendation: `run`.* Ceilings are Run content; the driver is the only new
code and it can live beside `prime.rs`. A `workflow` module would invite a
`WorkflowStore` next, which §3 exists to prevent.

**Q6. Does the workflow driver own step selection, or does the caller?**
*Recommendation: the caller.* The driver takes a step generator supplied per
invocation and only enforces ceilings, durability and fencing. A driver that
chooses the next step is one refactor away from being a second agent loop.

**Q7. Should `KernelSpec` carry a Host-declared kernel *protocol* version, so a
Host can refuse an image that speaks a dialect it does not know?**
*Recommendation: yes, as a bounded `KernelLabel` folded into `spec_digest`.*
This is cheap now and unpleasant to add once checkpoints exist in the wild.

**Q8. Reserved environment names (§2.8, item 3) — published constant list, or
Host-supplied?**
*Recommendation: a published `const KERNEL_RESERVED_ENVIRONMENT_NAMES: &[&str]`
plus a Host-supplied extension set.* A purely Host-supplied list makes the
structural claim unverifiable in conformance.

---

## 6. Compatibility and evolution

### 6.1 Relationship to `PersistentKernelDriver`

`PersistentKernelDriver` stays, unchanged in role. The relationship is exactly
the one `ProgramDriver` and `ProgramRuntime` already have, and the doc comment
on `ProgramDriver` states it: the driver trait is the *Run reducer's dispatch
seam* — it hands a durably claimed intent to whatever executes it and takes a
receipt back — while the runtime trait is where ownership, bounds, honest
settlement and crash reconciliation live.

So:

- `PersistentKernelDriver::checkpoint` / `restore` remain the reducer-facing
  calls, still typed in `ArtifactRef` and `ProgramHandle`.
- `KernelRuntime` is what a Host implements once and then drives that seam
  from. `KernelCheckpointRef::artifact` converts to the `ArtifactRef` the seam
  wants; `KernelSessionId` + `KernelGeneration` map onto `ProgramHandle`'s
  `id` + `generation`, which is what those fields were shaped for.
- The doc comment on `PersistentKernelDriver` gains a pointer to
  `KernelRuntime`, mirroring the pointer `ProgramDriver` already carries.

Nothing is superseded and nothing is deleted. This matters because the SDK is
consumed by a Host under a current-only policy: if this proposal replaced
`PersistentKernelDriver`, the consumer would have to delete readers and writers
in the same change, and the seam's shape is not the thing that needs to change.

### 6.2 One gap to close

`PersistentKernelDriver::restore` returns `ProgramHandle` and therefore has no
place to put `Vec<NonRestorableFact>` — the seam as it stands *can* lose the
declaration §2.5 works to preserve. Two ways out:

- (a) Widen the seam's return to a `KernelRestoreReceipt` carrying the handle
  and the lost list. This is a breaking change to a pre-1.0 public trait.
- (b) Require the Host adapter to read `non_restorable` off the
  `KernelCheckpointRef` it already holds before calling the seam, and treat
  surfacing as the adapter's obligation.

*Recommendation: (a).* Option (b) puts the one property this contract is for
back into "a discipline every Host has to remember", which is precisely the
critique `program.rs` makes of pre-contract credential handling. The trait is
pre-1.0 and has one known consumer; the change is cheap now and expensive after
release.

### 6.3 Versioning

- The module publishes `KERNEL_RUNTIME_SCHEMA_MARKER` and
  `KERNEL_RUNTIME_SCHEMA_VERSION`, verified fail-closed on open, exactly as
  `PROGRAM_RUNTIME_SCHEMA_VERSION` is.
- Every public enum in this contract is `#[non_exhaustive]`, so a later
  disposition, a later `NonRestorableFact` kind or a later `RestorableFact`
  kind is additive.
- Bounds constants may rise without a major version and may not fall.
- Adding a trait method requires a default body or a major version.
- Checkpoints are addressed by `spec_digest`; a kernel image change is a new
  digest and therefore a `SpecMismatch`, not a silent reinterpretation. This is
  the same reasoning `ARTIFACT_DIGEST_PREFIX` uses to make a future algorithm
  additive.
- Pre-release, there is exactly one current schema. If this contract's schema
  is superseded before release, its readers, writers, fixtures and stored
  state go in the same change; there is no dual-read path.

---

## 7. What shipped

Every §5 recommendation was adopted as written, and §6.2 was closed with option
(a). The contract lives in `crates/sophon-sdk/src/kernel.rs`, its reference
backend in `src/kernel/local.rs` (durable records in
`src/kernel/local/records.rs`), its conformance suite in
`src/kernel/conformance.rs`, the bounded driver in `src/workflow.rs`, and the
scriptable kernel the suite runs against in `src/bin/kernel_fixture.rs`. The
Host-facing tests are `crates/sophon-sdk/tests/kernel_runtime.rs`.

### 7.1 Departures from the sketch

- **`KernelRuntimeHarness::fragment` takes bounds.** The suite has to submit the
  same described fragment under different declared bounds — a flood under a
  small capture bound, a sleep under a short deadline — so the bounds are a
  parameter rather than a property of the script.
- **The harness gained `sink` and `damage`.** Captured output goes to a
  caller-supplied `ProgramOutputSink`, so the harness has to supply one.
  `damage` stages the two controls that are unreachable through the contract by
  design: a receipt edited out from under its own digest (N6) and a row the
  backend cannot decode (N7).
- **`MAX_KERNEL_RESTORABLE_FACTS` was added.** The sketch bounded only the
  non-restorable list. Decoding a stored checkpoint has to be fail-closed
  symmetrically, so both lists are bounded.
- **N1's compile-fail case is a documented structural argument plus a runtime
  assertion.** No `trybuild` case was added. The structural half — no credential
  type in the module, no method taking a `CredentialResolver`, and no sibling of
  `KernelSpec::environment` that takes a handle name — is checked by the
  compiler on every build of this crate, and breaking it would require adding a
  type rather than passing a bad value. The runtime half asserts that every
  published reserved name, in either case, is refused.
- **N10 lives beside the driver, not in the backend suite.** A stale
  `ActivationFence` constrains the workflow driver; a `KernelRuntime` backend
  has no fence to be stale. `WorkflowDriver::claim` is the only shape in this
  SDK that mints a claim for a workflow step and it always attaches the fence,
  which is what makes the reducer's existing stale-fence refusal reachable
  rather than remembered. That is asserted in `src/workflow.rs`'s own tests; the
  reducer's refusal itself is the reducer's tested invariant.
- **`WorkflowDriver::admit` takes a `RunEnvelope`.** The sketch's §3.4 loop is
  expressed as an admission decision plus a claim constructor rather than as one
  driving function, because the surrounding calls — `claim_due`,
  `BeginIteration`, `prepare_operation`, `acknowledge_effect`, `renew`,
  `release` — are already public and already ordered by the reducer. Wrapping
  them in a second loop would have added the one thing §3.1 refuses: another
  place where a workflow's position is remembered.

### 7.2 What proves it

`run_kernel_runtime_conformance` implements the fifteen positive properties and
nine of the ten negative controls, and `LocalKernelRuntime` is gated through it
against a real long-lived child process. A backend that answers a lost session
with a clean shutdown is proved to fail the suite in
`a_backend_that_fabricates_a_clean_shutdown_fails_the_suite`.
