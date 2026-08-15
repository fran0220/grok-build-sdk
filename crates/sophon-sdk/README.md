# Sophon SDK

The `sophon-sdk` crate is a trusted, in-process Rust boundary around the bundled Grok agent. Its public contract is a typed Rust API backed by the shell's native embedded facade; no transport service or transport request types are part of the SDK. `Runtime::start` uses the restricted profile. Trusted applications that need the full agent surface should use `Runtime::builder(config).profile(RuntimeProfile::Desktop)`, explicitly advertise `HostCapabilities`, and install a `HostDelegate` when host filesystem or terminal delegation is required.

## Explicit providers, not account login

An embedding application can supply every inference credential directly. It does not need Grok account authentication:

- `RuntimeConfig.models` defines the fixed catalog and backend contract.
- `Runtime::list_models` reads that live host-owned catalog through the typed
  `x.ai/models/list` contract, including forward-compatible metadata for
  context-window, agent-harness, and reasoning-effort discovery. It is
  available in both profiles without enabling the generic extension bridge.
- `RuntimeBuilder::model_provider` or `RuntimeServices::model_providers` selects a protocol, base URL, literal API key, provider wire-model slug, request headers, and query parameters independently for each catalog model. When every model has an explicit provider, the legacy `RuntimeConfig.endpoint` and `api_key` may be empty.
- `AgentServiceConfig` routes built-in subagent names and the web-search, session-summary, image-description, and prompt-suggestion auxiliary calls to catalog models. Those catalog models can each use a different provider.
- `MediaProviderConfig` and `MediaServiceConfig` independently enable image generation, image editing, image-to-video, and reference-to-video, including an explicit API URL, key, headers, query parameters, and four model slugs. Query parameters are preserved on image generation/edit and video start/poll requests. The static media credential cannot be replaced by the primary model's rotating credential.
- `McpServerConfig` injects bounded trusted stdio or Streamable HTTP MCP transports without reading user configuration files. `McpServerConfig::http` and `McpServerConfig::sse` validate the remote endpoint and Host-injected headers before mounting; raw provider credentials can remain behind a Host relay while the SDK receives only a relay-scoped header. `Sse` is a configuration-compatibility alias for a modern Streamable HTTP endpoint; legacy SSE lifecycle behavior is not supported. `InProcessMcpServer` registers SDK-owned servers through direct process-local dispatch, without a child process, reverse RPC, or second MCP state store. `InProcessMcpContext` identifies the runtime, session incarnation, server name, and registration ID on every callback.

Explicit model providers use the repository's existing sampler and agent loop. Three provider protocols are supported: OpenAI Chat Completions, OpenAI Responses, and Anthropic Messages. The provider protocol is authoritative for endpoint shape and authentication; the catalog model's legacy `api_backend` cannot override it. Media providers must implement the xAI Imagine-compatible image/video endpoints and payloads; this SDK does not pretend that an arbitrary diffusion or video API has that contract. Web search similarly uses Grok's existing model-backed web-search path, not an arbitrary third-party search REST schema. Account-only xAI product services remain separate optional product capabilities and are not implied by a custom API key.

Provider and MCP secret-bearing types deliberately omit both `Debug` and `Serialize`; they support `Deserialize` for host-owned configuration input without offering an accidental secret-export path. An explicit provider never resolves its key from an environment variable, Grok login, or ambient Grok config. Unoverridden catalog models retain the legacy endpoint/key fallback for compatibility. Optional auxiliary roles are disabled when omitted rather than falling through to an ambient first-party credential.

Choose a provider constructor according to the relay's wire contract. A base
URL includes the API prefix but not the operation path (for example,
`https://api.openai.com/v1`):

```rust
let chat = ProviderConfig::openai_chat(base_url, api_key, "grok-4.5");
let responses = ProviderConfig::openai_responses(base_url, api_key, "grok-4.5");
let messages = ProviderConfig::anthropic(base_url, api_key, "grok-4.5");
```

OpenAI protocols send `Authorization: Bearer …`. Anthropic Messages sends
`x-api-key` and `anthropic-version: 2023-06-01`. Custom headers and query
parameters may be added to the returned configuration, but authentication
headers cannot be overridden. The SDK does not persist provider configuration.
Catalog or credential changes are admitted by draining the current Runtime and
starting its replacement with the new fixed configuration; the SDK
intentionally has no runtime registry or mutable provider-credential store.

## Native desktop M1–M4 public-contract map

This table records the minimum embedding contract and prevents product hosts
from replacing runtime-native behavior with a second harness, registry, or
executable.

| Milestone concern | Current public contract | Gap / decision |
|---|---|---|
| Application model catalog | `RuntimeConfig::models`, `ModelSpec`, `Runtime::list_models` | Complete for a Host-owned fixed catalog. Refresh revisions and connection health remain Host state; restart the drained Runtime to admit a new catalog. |
| Provider endpoint + credential | `ProviderConfig`, `ProviderProtocol`, `RuntimeBuilder::model_provider` | Complete for OpenAI Chat Completions, OpenAI Responses, and Anthropic Messages. `api_key` is sent using the protocol-defined authentication header and may be a loopback-relay token. Provider raw credentials need not enter the SDK. |
| One Runtime, one Session per Host Thread | `Runtime`, `create_session`, `create_session_with_id`, `load_session`, `resume_session`, `unload_session`, `delete_session` | Complete; no registry or external executable is required. `create_session_with_id` gives the Host a crash-safe, idempotently retryable Thread↔Session identity when `SessionStateStore` is installed; `delete_session` coordinates actor teardown with permanent authority deletion. A timed-out unload retains the exact native actor thread, session-tree registration, SDK binding, and lease for a truthful retry; final Runtime shutdown reports incomplete unloads and transfers unfinished actors to join-based reconciliation rather than detaching them. |
| Session cwd/model/reasoning | `SessionConfig::{cwd, model, reasoning}`, `Runtime::set_route` | Complete for M1. Explicit reasoning wins; omission resolves to the validated fixed-catalog default on create/load/resume and route changes. |
| Restart, recovery, receipt, cursor | `PromptReceipt`, `SessionLedger`, rewind receipts, `events_after`, Run reconciliation/attach APIs | Complete for M1. A cursor gap is typed and fails closed. |
| Host-owned native Session state | `RuntimeBuilder::session_state_store`, `SessionStateStore`, chunked `SessionObject`s, CAS `SessionManifest`, `LocalSessionStateStore` | Complete. With a Host store installed it is the sole authority for transcript/history, rewind state, and compaction checkpoints; its Session leases fence create/load/resume/delete and both sides of fork/worktree-resume across Runtime instances. Covered JSONL files are neither read nor projected. Without injection the legacy JSONL backend remains available. |
| Immutable harness materialization | `HarnessSnapshot`, `HarnessContent`, `MaterializedHarness` | First-batch contract on this branch. A snapshot requires the complete system prompt; rules are deterministically folded into that authoritative override for native create/load/resume. There is no mutable SDK harness store. |
| Harness snapshot persistence | `HarnessStore`, `LocalHarnessStore`, `HarnessPut`, `harness_put_reconciled`, `run_harness_store_conformance` | Content-addressed and append-only. Hosts inject the authority; the Runtime keeps only the bound digest and never reads or writes stored snapshots. No update, replace, or delete operation exists, and the conformance suite rejects a backend that replaces content under a digest. |
| Turn binding | `TurnBindingReceipt`, `CompleteEventCursor`, `SdkProvenance`, harness-aware Session/prompt methods | First-batch contract on this branch. Provider-wire tests cover exact prompt replacement, rules update/removal, effective routes, load/resume and Runtime restart before a receipt is issued. |
| Optimistic refinement | `HarnessRefinementPatch`, `HarnessRefinement`, `HarnessEvidenceRef`, `HarnessEvidenceKind` | First-batch contract on this branch. Patch application rejects stale content identity and duplicate typed targets, and a patch carries the bounded typed evidence it cites. The Host commits revisions, evidence, activation, history and rollback. |
| Child Run / A2A | `admit_run_child`, `settle_run_child`, `accept_run_message`, `transition_run_message` | Durable admission, reservations, fenced settlement, de-duplication, and ordered mailbox state use the existing Run reducer. The shell subagent coordinator remains a UI/transport adapter and is not silently treated as Run authority. Hosts execute child placement and feed its typed settlement callback. |
| Durable activation coordination | `ActivationCoordinator`, `LocalActivationCoordinator`, `ActivationWake`, `ActivationClaimRequest`, `ActivationGrant`, `ActivationHandle`, `ActivationDisposition`, `run_activation_coordinator_conformance` | Complete. The durable queue in front of Run activation: identity-keyed work items with a due time, claims fenced by a strictly monotonic per-item token, renewal, completion or yield, and expiry-based recovery. Two supervisors on one authority grant a due item exactly once, a superseded worker is refused rather than tolerated, and a retried completion is an answer rather than a second execution. |
| Artifact custody | `ArtifactVault`, `LocalArtifactVault`, `ArtifactId`, `ArtifactHandle`, `ArtifactWrite`, `ArtifactProvenance`, `ArtifactObservation`, `ArtifactRecord`, `ArtifactIntegrity`, `ArtifactRecovery`, `ArtifactUsage`, `run_artifact_vault_conformance` | Complete. Identity is the SHA-256 of the content, so a handle names one byte sequence forever; provenance names the producing Run, iteration and operation, and an instrument observation additionally names the program execution, its inputs and the revision under observation. Damage is reported rather than served and is repaired only by an explicit recovery that cannot change what an identity means. Reads and materializations are durably counted, and two workers on one authority observe each other and converge on one artifact. |
| Program execution custody | `ProgramRuntime`, `LocalProgramRuntime`, `ExecutionId`, `ProgramLaunch`, `ProgramBounds`, `ExecutionReceipt`, `ExitDisposition`, `CaptureRecord`, `CredentialHandleName`, `CredentialResolver`, `ProgramOutputSink`, `LivenessProbe`, `ReconcileOutcome`, `run_program_runtime_conformance` | Complete. Every execution is named by its caller before it runs and receipted after it settles; the receipt names the program, the argument and environment digests, the working root, the attached credential handles, the exit disposition, the timing and the artifact handles of captured output, and is digest-verified on read. A cancel and an elapsed declared deadline settle as `Cancelled` and `TimedOut`, output past a declared capture bound is a recorded truncation with an honest produced-byte count, and an execution that was running at a crash is found alive, settled `Interrupted`, or left uncertain — never reported as success. Secrets are unrepresentable in durable state: a launch binds a handle name and the value exists only between the caller's resolver and the spawn. |
| Per-Session capability layering | `CapabilityLayer`, `RuntimeBuilder::general_capabilities`, `create_session_with_capabilities`, `create_session_with_harness_and_capabilities`, `load_session_with_capabilities`, `resume_session_with_capabilities`, `set_session_capabilities`, `session_capabilities` | Complete for skills, MCP mounts and agent-service routes. One application-owned general layer is masked per Session by name and kind, so per-project activation and per-Session routing need neither a Runtime restart nor a second Runtime. |
| Remote MCP provider boundary | `McpServerConfig::http`, `McpServerConfig::sse`, `McpServerConfig::validate`, `replace_mcp_servers` | Complete for bounded HTTP mounts with Host-injected headers. The Host may expose a relay URL and relay-scoped bearer while retaining raw provider credentials outside the SDK. `Sse` remains a Streamable HTTP compatibility spelling, not a legacy SSE implementation. The public façade suite mounts a live Streamable HTTP service over both spellings and proves every request — discovery, the listen stream, tools, Tasks and elicitation — carries the Host-injected headers verbatim, and that a durable Task identity reattaches across a full Runtime restart with only Host-persisted bytes. |
| Durable MCP Task reconciliation | `McpTaskIdentity`, `McpTaskHandle::durable_identity`, `McpTaskStatusEvent::durable_identity`, `Runtime::recover_mcp_task`, `McpTaskRecovery` | Complete as a Host-persistence contract. The Host persists the stable identity and its status projection; recovery queries the current mount without replaying Task creation and returns either a fresh generation-bound handle or explicit `RecoveryRequired`. The SDK does not claim a durable Task store. |
| Product-UI MCP elicitation | `McpElicitationUi`, `RuntimeBuilder::mcp_elicitation_ui`, `resolve_mcp_input_with_ui`, `resolve_mcp_task_input_with_ui` | Complete for bounded form/URL answers through one product-owned delegate. Generic continuation and Task-update arguments reject elicitation answers, request identities are preserved, and Task rounds are rechecked before submission. Roots and sampling remain separate typed Host services. |
| Peer conversations | `InputSource`, `prompt_from`, `prompt_content_from`, `ConversationDelegate`, `RuntimeBuilder::conversation_delegate`, `conversation_tool_descriptors`, `ConversationCreate`, `ConversationRead`, `ConversationSend`, `ConversationAcceptance`, `ConversationDigest`, `create_conversation`, `read_conversation`, `send_to_conversation`, `invoke_conversation_tool` | Complete as a contract. A Turn's prompt states whether a person or another conversation produced it, and a peer source carries the originating conversation identity into the durable ledger; an absent source is the user and an unknown one fails the read. The three tools are declared, bounded and dispatched by the SDK and answered entirely by the Host: it owns conversation existence, the send queue and transcript distillation, and no raw transcript crosses the boundary. There is no parent/child coupling, no wait primitive and no mailbox reducer. |
| Persistent kernel custody | `KernelRuntime`, `LocalKernelRuntime`, `KernelSessionId`, `KernelGeneration`, `KernelExecutionKey`, `KernelSpec`, `KernelSessionBounds`, `KernelSubmission`, `KernelExecutionBounds`, `KernelExecutionDisposition`, `KernelDisposition`, `KernelCheckpointRef`, `RestorableFact`, `NonRestorableFact`, `KernelRestore`, `KernelReconcileOutcome`, `KernelRestoreReceipt`, `run_kernel_runtime_conformance` | Complete. The durable authority for a kernel whose state survives between executions: a session is named by its caller before it exists, one incarnation runs one fragment at a time so a receipt's sequence is also the order state was mutated in, and every execution settles into a digest-verified receipt that says `Completed`, `Raised`, `Cancelled`, `TimedOut`, `Interrupted` or `KernelDied` — never silence. A checkpoint is evidence, not authority: it addresses its own payload, enumerates what it carried as `RestorableFact`s and what it could not as `NonRestorableFact`s, and a restore hands that loss back with the new incarnation so no caller can receive a session without receiving what it lost. A checkpoint from a different image is a `SpecMismatch` rather than a silent reinterpretation, a declared session ceiling settles the session by the name of the ceiling that was reached, and a session that was live at a crash is found alive, settled `Interrupted` together with every execution in flight under it, or left uncertain. Credentials are structurally absent: no type here can carry one, no method takes a resolver, and `KERNEL_RESERVED_ENVIRONMENT_NAMES` is refused by `KernelSpec::validate`. |
| Bounded workflow driver | `WorkflowCeilings`, `WorkflowStepIntent`, `WorkflowAction`, `WorkflowDisposition`, `WorkflowAdmission`, `WorkflowDriver` | Complete as a contract, and deliberately storeless. A workflow is a bounded sequence of steps executed entirely through Run intents, activation grants and artifact identities that already exist: the step index is the Run's own iteration count, exclusivity is the activation fence every claim carries, and the outcome of a step is an `EffectReceipt`. What this adds is the declared ceilings — steps, wall time, consecutive failures and a finite resource budget, all validated before the first step is prepared so a workflow that cannot terminate never starts — and a disposition that names the ceiling that stopped it rather than saying it ran out of something. |
| Continuation / gates | Generation-bound `McpContinuation`; Run-scoped `GateRequest`, `GateEvaluation`, `GateProvider` | M3 audit only. MCP continuation is one non-serializable live MRTR retry and a gate evaluation is an immediate provider result. Neither supplies a durable Host aggregate with identity/revision, ownership transfer, replay cursor or content-bound receipt. |

The dependency order is M1 baseline → immutable snapshot/refinement façade →
runtime-generated Turn binding receipt → Host revision/evidence/activation
integration → narrow M3 schemas and receipts. M3a can begin only by connecting
the existing durable child identity/callback token to the native coordinator
and defining admission, cancellation and settlement receipts; A2A mailbox
delivery follows that identity boundary. M3b may then define durable
continuation/gate ownership and replay receipts on top of Turn and child
cursors. M3c remains blocked until an actual internal kernel driver has a
stable handle plus checkpoint, cancel, restart and settlement boundaries;
terminal/PTY APIs must not be renamed into a kernel façade. This keeps every
public change additive and independently reviewable.

## Profiles and trust boundary

`Restricted` is the default and remains fail-closed for plugins, MCP, subagents, workflows, network tools, media tools, and workspace `.envrc` evaluation. Supplying their configuration does not enable them. `Desktop` restores the repository-native feature surface inside the embedded storage/process boundary; each media operation is still independently gated by `MediaServiceConfig`.

Restricted filesystem and terminal calls are explicitly rejected unless the host advertises and implements the matching `HostDelegate` capability; they never fall back to the runtime process's local machine. In Desktop, an advertised host capability still routes through `HostDelegate`, while an unadvertised filesystem or terminal capability deliberately retains Grok's native local desktop implementation.

Agent commands, scheduler operations, workflows, subagents, MCP, hooks, permissions, rewind, sessions, and model discovery have typed methods. `Runtime::capabilities` reports these SDK features rather than protocol method namespaces. For forward compatibility, the generic extension request/notification bridge also preserves JSON and protocol errors for current and future `x.ai/*` methods in `Desktop`; it is disabled wholesale in `Restricted`, so privileged filesystem, terminal, plugin, worktree, and process methods cannot bypass that profile. Session lifecycle operations are excluded from the generic bridge; Host-authority worktree resume must use its typed, two-identity fenced operation. The typed, read-only `Runtime::list_models` wrapper remains available in Restricted because it only inspects the host-supplied fixed catalog. **Do not expose the Desktop bridge directly to a WebView or untrusted renderer.** Validate and authorize calls in the Rust main process.

Screenshots, accessibility trees (AX/UIA/AT-SPI), OCR, and mouse/keyboard automation are not native Grok capabilities; a desktop host must provide those through an audited `HostDelegate`. Rich prompt blocks can be submitted independently of TUI support. The current sampling layer has no native audio part, so audio is preserved losslessly as a data-URI text attachment rather than silently discarded.

The event receiver provides push delivery. `events_after` reads the same bounded per-session journal and reports `Error::EventGap` when a cursor was evicted.

## Immutable harness and Turn binding

`HarnessSnapshot` freezes the native system-prompt/rules inputs under a
domain-separated SHA-256 content identity. Its fields are private, generic
deserialization validates the declared digest, and the bounded
`from_json_slice` entry point rejects oversized durable input before parsing.
`MaterializedHarness::apply_to_session` preserves Session `cwd`, `model`, and
`reasoning`, while replacing the complete native system-prompt override. Rules
are folded into that override under `<human_rules>` rather than sent as a
second native input, so the snapshot digest never covers content skipped by
provider inference.

`HarnessRefinementPatch` is a typed optimistic transform against one snapshot
digest. It rejects a stale base and multiple changes to one target, then
returns another uncommitted immutable snapshot. It has no revision number or
activation operation: the Host remains the sole owner of revision CAS,
evidence, activation, history, and rollback. `with_evidence` attaches up to
`MAX_HARNESS_EVIDENCE_REFS` typed `HarnessEvidenceRef` citations — a
`HarnessEvidenceKind` namespace, a bounded identity, and an optional SHA-256
content pin — so a refinement names the settled Turn, artifact, or evaluation
that produced it. Evidence rides on the patch and never enters the successor
snapshot, so citing evidence cannot move a content address, and a patch
serialized before evidence existed still decodes.

`HarnessStore` is the optional, Host-injectable, content-addressed persistence
boundary for snapshots; its marker/version is
`sophon-sdk.harness-snapshot-store`/1. The Runtime never reads or writes
it: a resident Session retains only the digest it was bound to, so any
per-Session harness state the SDK holds is a projection keyed by digest rather
than a second copy of harness content. The contract is deliberately
append-only — `get`, `put`, `contains`, and nothing that updates, replaces, or
deletes live content. Writing a present digest is idempotent, an unknown commit
is settled through `harness_put_reconciled`, and both SDK byte bounds and
digest verification are enforced on read and write. `LocalHarnessStore` is the
SQLite reference implementation; a Host backend proves the same semantics with
`run_harness_store_conformance`, which fails any backend that lets a later
write replace the bytes reachable under a digest.

Use `create_session_with_harness`, `load_session_with_harness`, or
`resume_session_with_harness` to bind one Session incarnation to a snapshot.
`prompt_with_harness` and `prompt_content_with_harness` issue a
`TurnBindingReceipt` only after the native Turn settles and the SDK verifies a
contiguous live event range ending at its matching terminal event. The receipt
identity covers Session/Turn/prompt settlement, snapshot digest, selected
model/reasoning, exact SDK source provenance, usage, and the complete cursor.
Snapshot mismatch fails before dispatch; an event gap fails closed after the
settled Turn and remains recoverable through the existing Session ledger.
Reasoning in the receipt is the same effective value sent to native metadata
and observed on the provider wire: an explicit Session value, otherwise the
validated default from the Runtime's fixed catalog.

## Durable autonomous Runs: first vertical slice

`GoalSpec` is immutable goal input: objective, acceptance criteria, constraints, and required evidence. It is not another lifecycle state machine. `run::RunRecord` is the sole authority for long-running work, while the existing Session Turn ledger remains the sole prompt-settlement and rewind-evidence ledger. The Run stores a typed reference and receipt for each Turn; it does not copy conversation history into a second writable ledger.

This revision implements one executable driver, `AutonomousTurnLoop`, end to end:

1. A Host creates a Run and invokes a bounded `AutonomousActivation`. The SDK freezes the iteration context and builds the next goal prompt.
2. The SDK commits the Session Turn intent and a fenced claim with a durable resource reservation before calling `Runtime::prompt`. Effect class is fixed by SDK driver code, not selected by model output.
3. `Runtime::prompt` durably writes Pending and Completed SessionLedger entries around native dispatch. Completed entries bind provider-derived usage into the settlement identity; missing, incomplete, or partial accounting remains typed unknown usage rather than zero. The Run accepts only an exact typed receipt bound to Session, Turn ID, prompt digest, prompt index, outcome, usage, and settlement ID.
4. Gates and the skeptic `GoalVerifier` decide whether an iteration may complete the Run. Reaching an iteration/agent budget produces `Waiting(BudgetExhausted)`, never success.
5. On restart, the previous controller epoch is fenced before SessionLedger/rewind reconciliation. Missing, conflicting, merely Discarded, or otherwise uncertain evidence remains `Recovering`; an uncertain Turn is never guessed or silently repeated. Paused, waiting, cancelled, and failed states survive reconciliation and require an explicit Resume where applicable.

The public façade exposes `create_run`, `get_run`, `list_runs`, `list_recoverable_runs`, `control_run`, `wake_run`, `attach_run`, `reconcile_run`, `resolve_run_recovery`, and `autonomous_turn_loop(...).activate(...)`. Low-level prepare/claim/acknowledge/iteration choreography is intentionally not part of the normal SDK façade. `RunId`, `RunRevision`, `RunEventCursor`, `ControllerEpoch`, `OperationId`, and `IterationId` use distinct Rust types and namespaces; Session `Event.sequence` is not a Run cursor. `attach_run` falls back to `RunAttach::Snapshot` when bounded journal replay is not contiguous.

Schema v4 includes authoritative residency without embedding a scheduler. Hosts call `request_run_wake` to durably coalesce typed `WakeReason`s and the earliest deadline, then `claim_run_activation`, `renew_run_activation`, and `release_run_activation` around one bounded worker activation. Claims carry typed worker identity plus epoch, random token, and expiry; an unexpired claim excludes every other worker, while an expired claim may be taken over with a new fence. Pause and cancel clear wake/deadline/claim and advance the epoch, so late workers fail closed. At process start the Host calls `inspect_run_residency` for each Run, re-arms future deadlines, and immediately handles overdue work; claiming an overdue deadline includes `WakeReason::CatchUp`. The shell scheduler, if used, is only a timer/worker-placement adapter and must not keep a parallel lifecycle store.

The default `LocalRunStore` is a standalone/reference SQLite authority with transactional revision CAS. `Runtime::start_with_run_store` and `RuntimeBuilder::run_store` replace **only that Run SQLite store** with one Host-provided authority; they do not mirror or write through to a second Run store. A custom store must persist `CURRENT_RUN_SCHEMA` (marker `xai-agent-lifecycle.run-envelope`, version 4), reject mismatches, call `StoreCommit::validate_and_encode` before opening its write transaction, and atomically commit the prepared snapshot, event journal, command receipt, outbox, and optional finished-iteration payload under the requested revision CAS. This public preparation chokepoint preserves the SDK validator's exact error variants and ordering; JSON round-trip validation is not equivalent. Acknowledgement uncertainty must be returned as `CommitUnknown`.

`SessionEvidenceStore` is the separate, host-agnostic single authority for SDK-origin `SessionLedger`, rewind intent/receipt, and immutable harness Turn-binding documents. Payload schemas, bounded parsing, identity, settlement digests and transition decisions remain SDK-owned; the Host implementation owns connections/paths, transactions, migrations, encryption, backup and lifecycle. The current marker/version is `sophon-sdk.session-evidence`/1. CAS compares revision and digest: absence advances to revision 1, otherwise checked `current + 1`; the digest is `sha256:` plus lowercase SHA-256 of the exact payload bytes. Implementations must return the exact value produced by `SessionEvidenceVersion::successor`. `Conflict`, a malformed successor, or `CommitUnknown` always fails closed. Pending is acknowledged before native prompt dispatch, rewind intent before native rewind, intent-to-receipt is one CAS replacement, and binding evidence is acknowledged before ledger settlement. `RuntimeBuilder::session_evidence_store` replaces the local reference store without mirroring. `Runtime::start_with_stores` avoids startup API combinations when both production authorities are injected. Current-only schemas require an explicit offline migration or deliberate discard before startup.

`SessionStateStore` is the chunked native persistence boundary without shell
protocol types. Its current-only `sophon-sdk.session-log`/2 contract
stores immutable SHA-256-addressed Session objects scoped by validated
`SessionKey` + `SessionGeneration`: chain transcript segments and publication
records, and separately referenced checkpoint/rewind payloads. Publication records
preserve exact marker bytes. A 64 KiB CAS manifest/head is prepared from the full
expected live document and a validated suffix. Objects are bounded at 64 MiB
(transcript target about 4 MiB),
while checked `u64` counters permit unbounded total history. Publication verifies
reference kind, name where applicable, identity, and generation. Slot inspection
fully verifies the chain and distinguishes
Vacant, Live, and permanent Tombstoned state, preventing identity ABA. Delete
atomically tombstones/removes only the manifest. This release exposes no GC API;
backends may eventually collect unreachable objects only under an operator-defined
retention policy. For every `CommitUnknown`, use the exported reconciliation helpers
with the exact scoped object, manifest successor, or tombstone receipt and never
blindly repeat a native action.
`LocalSessionStateStore` is the current-only SQLite reference implementation.
Production backends can run `run_session_state_conformance` and
`run_session_state_fault_conformance`; together they exercise competing CAS,
restart/tombstone behavior, compound publication, missing/corrupt/oversized
objects, bounded payload reads, and exact acknowledgement-loss reconciliation.

`RuntimeBuilder::session_state_store` installs one shared authority for every
Session in the Runtime. A neutral shell semantic port supplies stable replay
cursors and typed transcript, checkpoint, rewind, fork, and tombstone
operations; the SDK-owned adapter alone owns chunking, bounded chain traversal,
immutable object staging, CAS publication, and exact `CommitUnknown`
reconciliation. Conflict, corruption, missing/oversized objects, replay gaps,
or unresolved acknowledgement uncertainty fail closed. Checkpoint markers and
payloads, and rewind markers and operations, publish atomically. Full and
partial forks receive fresh generations. `Runtime::create_session_with_id`
derives a generation from the exact `SessionConfig`, so retrying the same UUID
and config after an unknown acknowledgement reopens idempotently, while config
drift and tombstones are rejected.

### Native Session compaction evidence

`RuntimeBuilder::compaction_observer` installs one typed, asynchronous audit
observer for native Session compaction. It requires `session_state_store`; the
SDK remains the sole recovery, retry, checkpoint-publication, and in-memory
installation authority. The Host acknowledges content-free `CompactionIntent`
and `CompactionOutcome` values for long-term audit only. Generic
`PreCompact`/`PostCompact` hooks remain informational and are never accepted as
intent, publication, or outcome evidence.

An applying compaction is serialized per Session. The shell first freezes the
exact credential-free semantic model request, including the selected single-
or two-pass path. The SDK durably fences the exact base manifest and awaits an
intent acknowledgement before the first applying model call. Rejection makes
no model call. Cancellation, model failure, invalid output, or a changed input
produces an acknowledged `NotApplied` outcome and no checkpoint publication or
conversation replacement. A successful summary is sanitized, fallback-checked,
and fork-prefix-resolved before the final checkpoint is serialized. The SDK
then atomically publishes the checkpoint plus its typed compaction record,
installs those exact published conversation items, and awaits the Host's
`Applied` acknowledgement before ancillary resets, `PostCompact`, or Turn
continuation. Unknown publication or unresolved outcome evidence fences the
Session. Restart replays published evidence and repeats idempotent callbacks;
it never asks the Host to reconstruct Session state or autonomous ownership.

All digest hashes use SHA-256 over `domain || NUL || u64_be(byte_length) ||
bytes`. The canonical v1 domains are exported as
`COMPACTION_INPUT_DIGEST_DOMAIN`,
`COMPACTION_INPUT_MESSAGES_DIGEST_DOMAIN`,
`COMPACTION_INPUT_TOOLS_DIGEST_DOMAIN`,
`COMPACTION_INPUT_HOSTED_TOOLS_DIGEST_DOMAIN`,
`COMPACTION_INPUT_MODEL_DIGEST_DOMAIN`,
`COMPACTION_SUMMARY_DIGEST_DOMAIN`, `COMPACTION_STATE_DIGEST_DOMAIN`, and
`COMPACTION_CHECKPOINT_DIGEST_DOMAIN`. The input root is a length-delimited
sequence of the request-path discriminator and each leaf's exact byte count,
item count, and digest. API keys, authorization/extra headers, endpoints,
paths, tracing fields, and generated request/session/client IDs are added only
at dispatch and are absent from every digest leaf. Observer DTOs can represent
only bounded identities/enums, digests, sizes, counts, and references; observer
errors are coded and content-free.

`Runtime::probe_compaction` is non-mutating. `Applied` means the immutable
chain contains the exact compaction ID, intent digest, publication record, and
integrity-checked checkpoint; rewind, supersession, following records, and fork
replay are reported only as timeline relations. `NotPublished` is returned
only when the exact origin generation and base manifest can be reconstructed
from a complete, stable, integrity-checked ancestry. Missing objects, gaps,
counter or generation mismatches, corruption, conflicting IDs, unstable reads,
and store failures return `Uncertain`.

Startup still creates `grok_home` and `session_storage` for uncovered shell
sidecars and native tool/process/terminal state. In Host Session-state mode it
does not read, write, create, import, or fork-copy `updates.jsonl`,
`chat_history.jsonl`, `rewind_points.jsonl`, or
`compaction_checkpoints/**`; chat history and rewind replay are rebuilt in
memory from the authority. Without injection, the legacy JSONL implementation
is unchanged. The `Event` receiver and `events_after` journal remain bounded
in-memory delivery only and are not durable evidence.

## Durable activation coordination

`ActivationCoordinator` is the durable authority a Host's in-application
supervisor asks *what is due* and *may I execute it*. It sits in front of Run
activation rather than inside it: `claim_run_activation` fences one already
loaded Run's controller, while the coordinator decides which work is due and
which supervisor may touch it at all, without loading anything. Its marker and
version are `sophon-sdk.activation-coordinator`/1.

The unit is a work item: a validated `ActivationItemId`, a due time, and an
opaque payload the coordinator never interprets. `wake` registers or reschedules
one by identity, so a duplicate schedule is `Unchanged` rather than a second
item, and a work item under a live lease answers `Held` rather than moving under
its worker. `claim_due` takes up to a bounded batch of due items in due order,
oldest first and ties broken by identity, so a supervisor that was offline
catches up in schedule order and never sees work before its time; `claim_item`
answers `Granted`, `Held`, `NotDue`, `Settled`, or `Unknown` for one named item.

A grant carries an `ActivationFencingToken` that is strictly monotonic per item
and never reused. `renew` extends the lease, and `release` records either
`ActivationDisposition::Complete` or a `Yield` back into the queue. Every one of
those carries the token, and the coordinator answers `Fenced` — not an error and
not success — to any assertion whose token it is no longer honouring. Expiry,
not liveness detection, returns a crashed worker's item to the queue; the
successor claim advances the token, which is exactly what invalidates the
crashed worker if it ever returns. Releasing twice with the same token answers
`AlreadySettled`, so a supervisor that crashed between commit and
acknowledgement retries safely instead of executing the work again. Every method
takes the caller's instant; the coordinator has no clock of its own.

Identities, payloads, lease durations and batch sizes are bounded by the
contract rather than by a backend, and every stored scalar is re-validated on
read, so a foreign schema marker, a damaged row, or a settlement that outruns
its own fencing counter fails the read instead of scheduling invented work.
`LocalActivationCoordinator` is the SQLite reference authority; a Host backend
proves the same semantics with `run_activation_coordinator_conformance`, which
fails any backend that grants contended work twice, honours a superseded token,
or forgets a settlement across a restart.

## Artifact custody

`ArtifactVault` is the durable authority a Host asks *what is this artifact*,
*what produced it*, *is the stored copy still the copy that was written*, and
*who has used it*. It sits beside `run::ArtifactStore`, which is the Run
reducer's blob plumbing and answers only the first half of the first question.
The vault's marker and version are `sophon-sdk.artifact-vault`/1.

Identity is derived, never declared. An `ArtifactId` is `sha256-` followed by
the SHA-256 of the content, and an `ArtifactHandle` is the (id, digest) pair,
checked at construction so a handle whose two halves disagree does not exist.
Writing identical bytes twice is one artifact and answers `AlreadyPresent`
without re-dating, re-labelling or re-attributing the record already there;
`ArtifactWrite::expect_identity` lets a writer declare the identity it believes
it is writing, and content that does not address to it is refused before any
storage effect. That is what makes the immutability claim structural rather
than a rule each backend has to remember.

`ArtifactProvenance` names the producer by the three coordinates a Run has —
the Run's identity, the iteration, and the operation — and carries a closed but
additive `ArtifactProvenanceKind`. Alongside the ordinary produced-output,
consumed-input and operation-record kinds there is `InstrumentObservation`: a
captured frame or machine-readable measurement that records a program
execution, and which therefore carries an `ArtifactObservation` naming the
executed program, the artifacts it ran against, and the revision under
observation. That kind is unconstructible without its observation, and the
other kinds are unconstructible with one. `ArtifactWrite` also declares an
`ArtifactMediaType` and an `ArtifactRetention` hint with an optional
`retain_until_ms`; all of it is bounded by the contract, stored verbatim and
returned by `inspect`. Retention is a hint the vault stores, never a schedule
it acts on — only a Host policy can know whether anything still references the
content.

Damage is reported rather than served. `verify` answers `Intact`, `Missing` or
`Corrupt` without reading; `read` fails as `ArtifactError::Missing` for an
identity that was never stored and `ArtifactError::Corrupt` for a stored copy
that no longer addresses to its digest, and the two are never collapsed because
a Host can re-supply the bytes for one and cannot for the other. Repair is
`recover`, which is explicit, never happens inside a read, refuses content that
does not address to the same identity, leaves an already-intact artifact alone,
and records the instant it ran in `ArtifactRecord::recovered_at_ms`.

`materialize` writes a verified copy to a path: atomic rename, then a
re-verification of the bytes on disk, so a returned `ArtifactMaterialization`
means the copy addressed to the digest as it now exists rather than as it was
in memory. `ArtifactUsage` durably counts reads, materializations and bytes
served with saturating counters, and a read that served nothing is not counted
as a use. Two vaults opened on one root observe each other's writes, and
workers racing identical content converge on one artifact.

`LocalArtifactVault` is the SQLite reference authority; every stored scalar is
re-validated on read, so a foreign schema marker, an undecodable provenance
record or a negative counter fails the read instead of presenting invented
custody. A Host backend proves the same semantics with
`run_artifact_vault_conformance`, which drives an `ArtifactVaultHarness` so the
backend can damage its own storage under the contract; the suite fails any
backend that hides damage, serves a corrupt copy, re-labels content on a
repeated write, or forgets usage across a restart.

## Program execution custody

`ProgramRuntime` is the durable authority a Host asks *what did this execution
run*, *how did it end*, *where did its output go*, and *what happened to the
things that were running when I died*. It sits beside `ProgramDriver`, which is
the Run reducer's dispatch seam and answers none of those. The runtime's marker
and version are `sophon-sdk.program-runtime`/1.

An execution is named by its caller before it exists. `ExecutionId` is supplied
rather than generated because a Host that crashes between deciding to run
something and hearing that it ran can only ask *did execution X happen* if it
named X first; a second `launch` under a known identity — running or settled —
is `ProgramError::Conflict` rather than a second process. A settled execution
yields an `ExecutionReceipt` naming the program path, the argument and
environment digests, the working root, the credential handles that were
attached, the `ExitDisposition`, the declared `ProgramBounds`, the start and
settle instants, and a `CaptureRecord` per stream. Receipts are append-only and
digest-verified: a store keeps `ExecutionReceipt::digest` alongside the receipt
and recomputes it on read, so a row edited underneath the contract fails the
read instead of presenting an altered account. Waiting on a settled execution
replays its receipt; nothing runs twice.

Settlement is never fabricated. `ExitDisposition::Exited { code: 0 }` is the
only success, and every other way an execution can stop has its own name:
`Cancelled` for a caller that changed its mind, `TimedOut` for a declared
deadline that elapsed, `Signalled` for a death the program did not ask for, and
`Interrupted` for a process that was running when its owner died and is gone
when the owner returns. A late `cancel` never rewrites a settlement that
already happened.

Bounds are declared at launch, not chosen by a backend, so that a receipt can
report the limit that produced its outcome. `ProgramBounds` carries a non-zero
deadline and a per-stream capture bound, all validated before a process exists.
A program that outruns its capture bound keeps producing output — a backend
that stopped reading would turn a truncation into a hang — and its
`CaptureRecord` reports `captured_bytes`, `produced_bytes` and `truncated`, so
the loss is a fact a Host can show rather than a gap it has to infer.

Captured output binds to `ArtifactHandle`, and storage is the one-method
`ProgramOutputSink` seam. That is deliberate: the handle is a derived-identity
value type with no storage behaviour, so sharing it makes an execution's output
resolvable by whatever custody a Host runs, while the sink keeps a Host that
runs programs from being forced to adopt an `ArtifactVault`. A Host that has
one gets the binding for free through `ArtifactVaultOutputSink`. A backend
verifies a returned handle against the bytes it supplied, so a sink cannot bind
an execution's output to content that is not that output.

Secrets are unrepresentable in durable state. A launch binds a variable to a
`CredentialHandleName` — a name a keychain or relay knows a secret by — and
there is no constructor, accessor or conversion that turns one into secret
material. The value is produced at spawn time by the caller's
`CredentialResolver`, exists only as a `ResolvedCredential` (no `Serialize`, no
revealing `Debug`, zeroed in place on drop), and reaches nothing but the child's
environment. The environment digest covers the variable name and the handle
name, so it is stable across credential rotation: rotating a secret does not
change what was run. A resolver that refuses stops the launch before a process
exists and leaves no durable state behind.

Restart is reconciliation, not inference. `requiring_reconciliation` answers
exactly the executions this authority durably believes are running but this
handle does not own — after a restart, the crash-time backlog — and they
`inspect` as `ExecutionStatus::Uncertain` until something resolves them.
`reconcile` takes a caller-supplied `LivenessProbe` because the trustworthy
answer is deployment-specific: `Liveness::Live` answers `StillRunning` and
settles nothing, `Gone` settles `Interrupted` with no captured output (nobody
read those pipes, and claiming an empty stdout would be as much of a
fabrication as claiming success), and `Unknown` answers
`ReconcileOutcome::Uncertain` and leaves the execution exactly as uncertain as
it was. `OsLivenessProbe` is the reference pid probe and documents that it
cannot see through pid reuse across a reboot.

Wall time belongs to the caller. A backend owns a monotonic duration source so
it can enforce a deadline and nothing else; every instant in a receipt is the
`now_ms` declared at launch plus measured elapsed time.

`LocalProgramRuntime` is the reference authority: real `std::process` spawning
with a cleared environment, bounded draining capture threads, and a SQLite
store in which every stored scalar is re-validated on read, so a foreign schema
marker or an undecodable launch record fails the read rather than presenting an
invented account of what ran. A Host backend proves the same semantics with
`run_program_runtime_conformance`, which drives a `ProgramRuntimeHarness` so the
backend supplies its own programs and can crash one of its own executions under
the contract; the suite fails any backend that fabricates success for an orphan,
settles an uncertain probe, re-runs a replayed settle, loses output silently,
persists a resolved secret, or lets one identity start two processes.

## Persistent kernel custody

`KernelRuntime` is the durable authority a Host asks *is this kernel still the
one I opened*, *what did this fragment do to its state*, and *what did the
kernel lose when it was restored*. It is not a second agent loop: it holds no
model, no tools and no turn. It executes fragments a caller already decided to
run.

Three exclusions are load-bearing. A kernel session's in-memory state is
**evidence, never authority** — nothing durable is ever concluded from it, and a
Host that needs a fact reads it from the Run, the vault or the session store. A
checkpoint is likewise evidence: it addresses its own payload by digest, is
bound to the `spec_digest` of the image that produced it, and declares both what
it carried and what it could not. And **loss is never silent**: a
`NonRestorableFact` list travels with the restored session in
`KernelRestore::Restored`, so there is no accessor anywhere that yields a live
restored session without the loss beside it.

One incarnation runs one fragment at a time. That is what makes a receipt's
`sequence` mean the order state was mutated in; a Host that wants parallelism
opens more sessions, which is a different session identity and therefore a
different state. Cancellation is scoped to the fragment: a kernel that abandons
its work cooperatively leaves the session live and settles `Cancelled`, and a
kernel that will not is killed and settles `KernelDied`, because those are
different facts.

Credentials are absent structurally rather than by policy. There is no
credential type in the module, no method takes a `CredentialResolver`, and
`KernelSpec::environment` binds a literal `String` with no sibling that takes a
handle — so a Host cannot attach a secret to a kernel because the shape to do
it with does not exist. `KERNEL_RESERVED_ENVIRONMENT_NAMES` closes the
remaining door, refusing the variable names a provider library would read, and
`KernelSpec::reserving` lets a Host add its own. A kernel that needs a network
capability gets it over MCP, from a process that already has a credential
boundary.

`LocalKernelRuntime` is the reference authority: a real persistent child process
per incarnation speaking `LOCAL_KERNEL_PROTOCOL`, long-lived bounded capture
readers that keep counting past the bound so truncation is honest, and a SQLite
store whose every scalar is re-validated on read, so an undecodable row or a
receipt that no longer addresses to its own digest fails the read rather than
presenting an invented account. A Host backend proves the same semantics with
`run_kernel_runtime_conformance`, which drives a `KernelRuntimeHarness` so the
backend supplies its own kernel image and can crash or damage its own state
under the contract; the suite fails any backend that fabricates a clean shutdown
for an orphan, settles an uncertain probe, re-runs a replayed settle, restores a
checkpoint that enumerated no losses, lets one execution identity run twice, or
lets a durable secret survive.

## Bounded workflow driver

`WorkflowDriver` runs a bounded sequence of steps unattended and adds no state
store, because every piece of a workflow's state already has exactly one owner:
the activation coordinator owns which workflow is due and who may run it, the
Run owns the step sequence, each step's declared intent, its exclusive right to
execute and its outcome, the vault owns step inputs and outputs, and
`KernelRuntime` owns kernel session state as evidence. The step index is the
Run's own iteration count; the driver holds no counter.

What it contributes is the two things that did not exist. `WorkflowCeilings`
declares maximum steps, maximum wall time, maximum consecutive failures and a
finite resource budget, all validated at construction, so a workflow that cannot
terminate never starts. And `WorkflowDisposition` names each ceiling separately
— `StepCeiling`, `WallCeiling`, `BudgetCeiling { dimension }`,
`ConsecutiveFailureCeiling` — because a Host telling a person why an unattended
sequence stopped cannot do it from a variant meaning *ran out of something*.
Every claim is minted by `WorkflowDriver::claim`, which always attaches the
activation fence, so a superseded driver's step is refused by the reducer before
it can be acknowledged.

`AutonomousTurnLoop` currently has enforceable exact upper bounds only for iteration count, agent calls, and concurrency. Until a model/runtime capability contract supplies enforceable per-Turn maxima, finite `tokens`, `cost_micros`, `active_ms`, `wall_ms`, or `artifact_bytes` budgets are rejected before an iteration or prompt is dispatched. Use `u64::MAX` to mark those dimensions explicitly unbounded. Actual typed usage is still settled and recorded; an overrun or unknown value against a finite reservation durably enters recovery rather than being treated as free work.

| SDK owns | Embedding Host owns |
|---|---|
| Run reducer and lifecycle invariants, bounded loop, budgets, gates, verifier policy, intent/outbox, command de-duplication, epoch/token fencing, receipts, recovery decisions and attach contract | Worker/process placement, OS daemon/service residency, durable timer implementation and invoking bounded activations |
| Activation coordination semantics: due ordering, claim exclusivity, monotonic fencing tokens, expiry-based recovery, idempotent settlement, bounds and fail-closed decoding | The timer that decides when to sweep, the supervisor loop and its renewal cadence, what a work item means, and the retention policy behind `purge_settled` |
| Artifact custody semantics: derived identity, digest verification on read, immutable handles, provenance and observation vocabulary, declared size/media-type/retention hints, missing-versus-corrupt answers, explicit identity-preserving recovery, usage accounting and verified materialization | Physical artifact bytes and their placement, encryption, backup and replication; what an artifact means to a person; the retention policy that acts on the stored hints; which artifacts are shown, exported or garbage collected |
| Persistent kernel semantics: session identity and incarnation minting, one-fragment-at-a-time ordering, declared session and execution bounds, honest dispositions including the difference between a cooperative cancel and a kernel that had to be killed, checkpoint declaration and spec-digest addressing, the restore answer that carries its own loss, and probe-driven reconciliation of a crash-time backlog | The kernel image and the dialect it speaks, what a fragment means, where a snapshot's bytes physically live, the working root and its contents, whether a lost fact is worth reconstructing and from what, and any network capability the kernel needs — which arrives over MCP, from a process that already has a credential boundary |
| Program execution semantics: caller-supplied execution identity and one-process-per-identity claiming, receipt content and digest verification, honest exit dispositions, declared deadline and capture bounds, truncation accounting, credential handle vocabulary and spawn-time resolution, artifact binding of captured output, restart backlog and probe-driven reconciliation | Process placement and isolation, cgroups/job objects/containers, the authority that holds secrets behind a handle name, the liveness evidence a `LivenessProbe` answers from, where captured artifacts physically live, retention of settled executions, and what an execution means to a person |
| Peer conversation semantics: the input-source vocabulary and its fail-closed decoding, validated conversation/project/label/delivery-key shapes, the declared tool names, schemas and argument bounds, the digest ceiling and truncation vocabulary, and the checks that an answer is about the conversation, project and delivery key that were asked about | Which conversations exist and what a project is, the one send queue and whether a delivery starts a Turn or waits for settlement, delivery-key retention, how a transcript is distilled and what belongs in a digest, and every reply or completion notice — which are ordinary reverse sends, not mechanism |
| SessionLedger/rewind/binding schemas; native Session object/chunk schemas, validation, replay and publication semantics; CAS transition intent and fail-closed reconciliation; artifact identity/integrity and provider contracts | Physical Run, session-evidence, and native Session-state persistence; transactions/migrations/encryption/backup/lifecycle; uncovered shell-sidecar placement; credentials, providers, workspace, queues, policy and UI |

`ProviderSet` supplies typed artifact, gate, verifier, approval, and telemetry contracts. Local defaults store content-addressed artifacts and fail gates, verification, and approval closed until the Host installs explicit providers.

### This is not yet full Prime Agent parity

Durable wake intent, timer deadline, worker lease/takeover, child reservation/callbacks, mailbox delivery, immutable Harness activation pins, and `ProgramRuntime` execution/reconciliation now run through the authoritative reducer and public façade. Production residency must claim a lease and invoke `AutonomousTurnLoop::activate_claimed`; shell scheduler/subagent mechanisms are adapters only. Program execution is product-connectable when the Host supplies `ProgramRuntime` and `ArtifactStore`; the short-lived opaque credential is passed only to the Host driver while the Run stores its non-secret key identity/generation/scope.

The remaining explicit gaps are a built-in persistent kernel and a native bounded Rhai Run driver. `PersistentKernelDriver` is a Host contract only: a VM/kernel checkpoint is evidence, never durable truth. `ProgramContext` durably pins versioned skill descriptors and compaction continuity; native compaction now receives the already-claimed Run/iteration/operation correlation directly from the autonomous SessionTurn path, while shell skill reload remains separate. All pre-v4 Run databases/envelopes and pre-v2 native Session object encodings are rejected rather than silently upgraded; migration requires an explicit offline policy. Consumer integration requirements and product-wiring status are machine-readable in `consumer-integration.json`.

The Run API uses non-exhaustive public enums/DTO constructors, checked identifier deserialization, conservative unknown-value handling, and a checked-in fixture documenting the current v4 shape. Durable JSON must enter through bounded, validated `RunEnvelope::from_json_slice` or `RunEnvelope::from_json_reader`; generic serde deserialization performs recursive schema validation but cannot impose a source-byte limit. The same-revision fixture is not described as historical compatibility evidence; release fixtures become immutable only after their originating release ships.

## Agent API coverage

The SDK does not wrap the TUI. It exposes the stateful agent actor below it:

| Grok Build capability | SDK surface |
|---|---|
| Session create/load/resume/unload, cancel, rewind and durable Turn reconciliation | `Runtime` session and ledger methods |
| Text, image, audio and embedded-resource prompts | `prompt` / `prompt_content` |
| Prompts that state where they came from | `prompt_from` / `prompt_content_from` with an `InputSource` |
| Talking to another conversation | `conversation_create`, `conversation_read` and `conversation_send`, served on the `conversation` mount behind a `ConversationDelegate` |
| System-prompt replacement and host rules | `SessionConfig::system_prompt` / `rules` |
| Mid-turn steering and follow-up | `interject` |
| Built-ins, skills and workflows | `list_agent_commands` / `execute_agent_command` |
| `/implement` | A dynamically discovered skill; it appears as `implement` in the live command catalog and executes through the standard agent-turn path |
| `/loop` | Direct typed scheduler CRUD via `upsert_scheduled_task`, `list_scheduled_tasks` and `delete_scheduled_task`; the model-interpreted slash command remains discoverable too |
| Session fork and worktree resume | `fork_session` / `resume_session_in_worktree` |
| Workflow discovery | `list_workflows` |
| Subagent execution | Model-driven task tools in a normal Turn; live inspection and cancellation via `list_running_subagents`, `get_subagent` and `cancel_subagent` |
| Tool approval policy | `ToolPermissionHandler`; selected option IDs are checked against the agent's request before they are accepted |
| Pre/post tool and lifecycle hooks | `AgentHookRegistration` / `AgentHookHandler`, including blocking `PreToolUse`, `Stop` and `SubagentStop` gates |
| Host filesystem, terminal and application extensions | `HostDelegate`, gated by explicit `HostCapabilities` |
| Unknown future agent events | Lossless `Unknown` event fallback; no public generic protocol bridge |

Command execution intentionally goes through the agent's canonical slash-command parser after allowlisting the name against the live session catalog. This preserves skill substitution, tool restrictions, workflow semantics, and future built-ins; it is not a second command implementation in the SDK.

## MCP protocol coverage

All production transports use rmcp 3.1.2 and the modern discovery lifecycle. They require `server/discover` and negotiate only protocol version `2026-07-28`. There is no legacy `initialize` fallback, including for JSON-RPC `METHOD_NOT_FOUND`; unsupported versions and malformed, unauthorized, or timed-out discovery attempts fail closed.

The public session-scoped MCP API covers:

- server/tool catalogs with transport and setup credentials removed, plus tool calls and tool/server enablement; explicit catalog calls retain server-provided tool metadata and negotiated capability details for hosts that request them;
- resource list, resource-template list and resource reads, including single-round MRTR continuations;
- prompt list/get and prompt/resource argument completion, including single-round MRTR continuations;
- single-round tool calls with typed complete, input-required, and Task outcomes;
- generation-bound Task get/update/cancel operations, stable Host-persistable `McpTaskIdentity`, non-replaying recovery through `recover_mcp_task`, and ordered, allowlisted Task-status events; Task pushes never expose the server's raw Task object, result, error, or `_meta` fields;
- bounded `subscriptions/listen` streams for tool, prompt, resource-list, and individual-resource changes, with explicit acknowledgement, non-blocking cancellation, lag, and transport-end states; notification variants expose only their typed allowlisted fields, and subscriptions never silently resume after reconnect;
- typed, capability-gated roots and sampling services plus a dedicated product-UI elicitation delegate for MRTR input requests, and authorized roots-list-change notification;
- protocol ping;
- HTTP OAuth status/start and atomic server replacement;
- SDK-owned, identity-aware, full-duplex in-process MCP servers through `InProcessMcpHandler`; their bounded notification peer is invalidated when the owning session incarnation is unloaded or replaced;
- typed server-status, tools-changed and initialization-progress events. Push events omit raw payloads, status details, tool metadata and capability-extension values; unknown MCP control-plane notifications are suppressed rather than exposing unreviewed configuration data.

Modern roots, model sampling, and elicitation requests are carried by MRTR `inputRequests`. Roots and sampling may be answered through installed typed host services or an `McpContinuation` created with `McpInputRequired::respond`; elicitation answers are accepted only from the installed `McpElicitationUi`. A continuation is bound to its session incarnation, server, connection generation, operation kind and target; cross-operation reuse, mutation of the projected input round, and reuse after reconnect fail closed, while the opaque `requestState` is returned unchanged. The legacy unrestricted reverse-request path is not used for these roles. Capabilities are advertised only when the corresponding typed service is installed and authorized. Unknown input-request methods fail closed.

The SDK deliberately does not call legacy `resources/subscribe` / `resources/unsubscribe`, expose a generic server-to-client request peer, or add a transport compatibility service. Deprecated pre-2026 logging and direct roots/sampling request forms are retained only where rmcp's protocol model requires them; they are not the modern public execution path. Negotiated capability fields report what the server advertised for the selected version and remain distinct from host authorization.

## Session capability layering

Capabilities resolve in two layers. The application installs a *general* layer
once on `RuntimeBuilder::general_capabilities`: the built-ins, shared skills and
shared MCP mounts every Session should see. `RuntimeBuilder::mcp_servers`
remains supported and is folded into that same general layer, so an existing
embedding keeps its behavior unchanged.

Each Session may additionally carry its own `CapabilityLayer`, bound at
`create_session_with_capabilities` (or the `_with_harness_and_capabilities`,
`load_…` and `resume_…` forms) and replaceable between Turns with
`set_session_capabilities`. A Session contribution *masks* a general
contribution of the same kind and name; every other name stays visible.
`session_capabilities` reports the effective names, each one's
`CapabilityOrigin`, and the masked general entries.

```rust
let (runtime, _events) = Runtime::builder(config)
    .profile(RuntimeProfile::Desktop)
    .general_capabilities(
        CapabilityLayer::new()
            .skill(SkillContribution::new("general-skills", shared_skill_root)),
    )
    .start()
    .await?;

let session = runtime
    .create_session_with_capabilities(
        session_config,
        CapabilityLayer::new()
            .skill(SkillContribution::new("project-skills", project_skill_root))
            .mcp_service(project_mcp_mount)
            .agent_service(AgentServiceContribution::new("explore", "fast-model")),
    )
    .await?;
```

Layering is not a permission system: it selects which contributions a Session
observes, and it never grants or withholds authority. Validation is fail-closed
and runs before anything reaches the native runtime — layers require the
`Desktop` profile, duplicate names within a layer, empty or oversized names,
relative skill roots, MCP mount names that collide with an in-process server,
agent-service models outside the fixed catalog, and layers beyond
`MAX_CAPABILITY_LAYER_ENTRIES` are all rejected as `Error::InvalidConfig`.
Rebinding a resident Session is rejected while a prompt is in flight, and the
Session actor, its incarnation and its durable ledger are untouched by a
rebind: the change is observed by the next Turn on that Session alone.

`CapabilityLayer` deliberately implements neither `Debug` nor `Serialize`
because MCP mounts carry environment secrets and bearer headers.

## Peer conversations

An agent that wants durable, user-visible parallel work talks to an ordinary
conversation rather than spawning something. Ephemeral in-Turn subagents remain
the separate mechanism they always were: they live inside one Turn, they are
invisible between Turns, and they settle back into their caller. A peer
conversation does none of that. It has no parent, nothing waits on it, nothing
settles it, and it is reachable from any Session on the Runtime — target choice
is the agent's judgement, and the guardrail is that every message is visible in
a real conversation a person can open.

### Where a prompt came from

`InputSource` states whether a person produced a prompt or another conversation
did, and a peer source carries the originating `ConversationId` plus an optional
display label. `prompt_from` and `prompt_content_from` take it; `prompt` and
`prompt_content` are exactly the `InputSource::User` forms of the same call, so
an existing embedding is unchanged. The source is recorded in the Session's
durable ledger next to the Turn's identity and digest, so a restart, a replay
and an inspection all agree about who spoke.

The field is additive but deliberately not forward-tolerant. An entry with no
source is the user — which is what every ledger written before this contract
meant, and a user Turn's entry is still byte-identical to those. A *stated*
source this build does not know fails the read instead of being quietly
attributed to a person, because a message misattributed to the user is exactly
the failure the field exists to prevent.

### The three tools

Installing a `ConversationDelegate` on `RuntimeBuilder::conversation_delegate`
is what makes `conversation_create`, `conversation_read` and
`conversation_send` exist; without one they are absent from every Session, and
`Runtime::capabilities` reports `sdk:conversation-tools` as disabled with the
reason. They are served to the agent on the SDK-owned in-process mount named
`conversation`, so they follow the same Desktop-only routing and the same
name-collision rules as every other mount; a delegate under the Restricted
profile, or a mount of that name claimed by something else, fails closed at
`start`.

The SDK owns the contract and nothing else. It declares the names, the JSON
schemas and every bound; it parses arguments into validated newtypes —
`ConversationId`, `ProjectName`, `ConversationLabel`, `PeerMessage`,
`IdempotencyKey` — and rejects unknown fields, unknown tool names and
out-of-range values before the Host is asked anything. It never stores a
conversation, never runs a queue, and never sees a transcript.

`conversation_create` names a target project and returns the created identity;
it is expected to run the same host command path as user conversation creation,
and the SDK refuses an answer about a different project. `conversation_send`
carries a caller-chosen `idempotencyKey`, so a retry after an uncertain result
is the same delivery: the acceptance answers `StartedTurn` for an idle target,
`Queued` for a running one, and `AlreadyAccepted` when that key was already
admitted. `conversation_read` returns a bounded distillate; the read declares
its ceiling, `MAX_CONVERSATION_DIGEST_BYTES` is the absolute one, truncation is
reported as a fact rather than a silently shortened answer, and a digest that
outruns its declared bound is refused. The distillation is entirely host-side,
which is the point: the raw transcript never enters the calling Session's
context.

An answer about the wrong conversation, the wrong project, or the wrong
delivery key is a failure, not a result — a delegate cannot silently retarget a
create, a read or a delivery. A Host refusal, by contrast, is returned to the
agent as a readable tool error rather than a transport fault.

```rust
let (runtime, _events) = Runtime::builder(config)
    .profile(RuntimeProfile::Desktop)
    .conversation_delegate(host_conversations.clone())
    .start()
    .await?;

// The same validated contract the agent reaches as a tool.
let peer = runtime
    .create_conversation(&session, ConversationCreate::new(ProjectName::new("Desktop Product")?))
    .await?;
let acceptance = runtime
    .send_to_conversation(
        &session,
        ConversationSend::new(
            peer.conversation.clone(),
            PeerMessage::new("please review the release plan")?,
            IdempotencyKey::new("delivery-1")?,
        ),
    )
    .await?;

// The Host delivers it as an ordinary Turn that says where it came from.
runtime
    .prompt_from(
        &target_session,
        "turn-1",
        "please review the release plan",
        InputSource::Peer { conversation: origin, label: Some(label) },
    )
    .await?;
```

Replies and completion notices are ordinary reverse sends by convention. There
is no reply channel, no correlation identifier and no mailbox reducer here,
because a conversation that answers another conversation is just a conversation
sending a message.

## Capability boundaries

The SDK exposes every embeddable implementation present in this source tree; it does not claim to contain product code that is absent upstream. In particular, App Builder deployment is compiled as a disabled stub in this checkout, managed MCP catalog services use a separate account-product protocol, and OS screenshot/accessibility/OCR/input automation must be supplied by the desktop host. Those boundaries are reported as unavailable or host-provided rather than represented as working native SDK features.

Capability descriptors describe public typed SDK features, not every internal shell route or named xAI product service. Public releases must preserve this distinction.

## Development and verification

Use the gates in increasing cost order:

1. Run the fastest no-compilation source-layout preflight from the repository
   root: `crates/sophon-sdk/scripts/check-source-layout.sh`.
2. Run the Cargo-integrated version of the same policy with
   `cargo test -p sophon-sdk --test sdk_contracts source_layout`. It remains
   part of the automatic Cargo test suite and is fast when the build cache is
   warm. Most integration contracts share this harness so Cargo links the
   large upstream runtime closure once instead of once per source file;
   process-sensitive and live-provider suites retain separate process isolation.
3. Check formatting and every SDK target:
   `cargo fmt --all -- --check`, then
   `cargo check -p sophon-sdk --all-targets`.
4. During iteration, run the narrow domain test that covers the change. For
   Session-state work, use
   `cargo test -p sophon-sdk session_state::tests`,
   `cargo test -p sophon-sdk --test sdk_contracts session_state_store`, and
   `cargo test -p sophon-sdk --test sdk_contracts the_reference_session_state_store`.
5. Before merge or push, run the full SDK suite serially:
   `RUST_TEST_THREADS=1 cargo test -p sophon-sdk`. The embedded runtime tests
   share process-global native state and enforce bounded actor teardown, so
   running several of them concurrently can exhaust the teardown budget rather
   than provide useful parallel coverage. Focused tests shorten iteration;
   they do not replace this final proof.
6. Run comprehensive linting with
   `cargo clippy -p sophon-sdk --all-targets`.

The repository's DotSlash-managed `bin/protoc` keeps Cargo's proto dependency
fingerprints stable. If a local environment intentionally uses a system
`protoc` instead of DotSlash, export `PROTOC` as its absolute path; a bare
`protoc` fallback is recorded as a missing relative input and needlessly
rebuilds its reverse dependency tree on every Cargo invocation.

`lib.rs` and `mod.rs` files are composition roots: keep them focused on module
declarations and reexports. Put `Runtime` methods in the matching domain file
under `runtime/`. Split modules by reason to change, and do not create
catch-all `utils` or `common` dumping grounds. The layout gate limits
`src/lib.rs` to 300 physical lines and every other Rust source in this package
to 2,000, with no legacy exceptions; split ownership instead of casually
raising either limit.

## Public release status

This repository can be published as an Apache-2.0 source release or consumed from a pinned public Git tag, provided the bundled third-party notices and upstream provenance remain intact. The crate is intentionally `publish = false`: its current `xai-grok-*` dependency closure is workspace-local and cannot yet be resolved independently by crates.io. A crates.io release requires publishing or replacing that full dependency closure, removing workspace-only patches, and validating a packaged source archive first. Do not present a Git release as a crates.io-compatible standalone package until those gates pass.

Cargo patch declarations are not inherited from Git dependencies. An external full-SDK workspace such as Sophon must reproduce this repository root's exact `[patch.crates-io] async-openai` pin (or consume the repository as its workspace root); otherwise dependency resolution can select a different crates.io implementation. This is an integration and build-reproducibility requirement, not part of the Durable Run state contract.

```toml
[patch.crates-io]
async-openai = { git = "https://github.com/our-forks/async-openai.git", rev = "95b52ebdedf42143083cf3d6f0e0be7c84e9c808" }
```

For the current upstream-synchronized release, a Rust host can pin the SDK
without relying on a moving branch:

```toml
[dependencies]
sophon-sdk = { git = "https://github.com/fran0220/sophon-sdk", tag = "v0.3.0" }
```
