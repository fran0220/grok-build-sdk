# Grok Build SDK

The `grok-build-sdk` crate is a trusted, in-process Rust boundary around the bundled Grok agent. Its public contract is a typed Rust API: it does not start an ACP service, expose ACP request types, or require an ACP client. `Runtime::start` uses the restricted profile. Trusted applications that need the full agent surface should use `Runtime::builder(config).profile(RuntimeProfile::Desktop)`, explicitly advertise `HostCapabilities`, and install a `HostDelegate` when host filesystem or terminal delegation is required.

## Explicit providers, not account login

An embedding application can supply every inference credential directly. It does not need Grok account authentication:

- `RuntimeConfig.models` defines the fixed catalog and backend contract.
- `Runtime::list_models` reads that live host-owned catalog through the typed
  `x.ai/models/list` contract, including forward-compatible metadata for
  context-window, agent-harness, and reasoning-effort discovery. It is
  available in both profiles without enabling the generic extension bridge.
- `RuntimeBuilder::model_provider` or `RuntimeServices::model_providers` selects a base URL, literal API key, provider wire-model slug, request headers, and query parameters independently for each catalog model. When every model has an explicit provider, the legacy `RuntimeConfig.endpoint` and `api_key` may be empty.
- `AgentServiceConfig` routes built-in subagent names and the web-search, session-summary, image-description, and prompt-suggestion auxiliary calls to catalog models. Those catalog models can each use a different provider.
- `MediaProviderConfig` and `MediaServiceConfig` independently enable image generation, image editing, image-to-video, and reference-to-video, including an explicit API URL, key, headers, query parameters, and four model slugs. Query parameters are preserved on image generation/edit and video start/poll requests. The static media credential cannot be replaced by the primary model's rotating credential.
- `McpServerConfig` injects trusted stdio or Streamable HTTP MCP transports without reading user configuration files. `Sse` is a configuration-compatibility alias for a modern Streamable HTTP endpoint; legacy SSE lifecycle behavior is not supported. `InProcessMcpServer` registers SDK-owned servers through direct process-local dispatch, without a child process, reverse RPC, or second MCP state store. `InProcessMcpContext` identifies the runtime, session incarnation, server name, and registration ID on every callback.

Explicit model providers use the repository's real Chat Completions or Responses backends. Media providers must implement the xAI Imagine-compatible image/video endpoints and payloads; this SDK does not pretend that an arbitrary diffusion or video API has that contract. Web search similarly uses Grok's existing model-backed web-search path, not an arbitrary third-party search REST schema. Account-only xAI product services remain separate optional product capabilities and are not implied by a custom API key.

Provider and MCP secret-bearing types deliberately omit both `Debug` and `Serialize`; they support `Deserialize` for host-owned configuration input without offering an accidental secret-export path. An explicit provider never resolves its key from an environment variable, Grok login, or ambient Grok config. Unoverridden catalog models retain the legacy endpoint/key fallback for compatibility. Optional auxiliary roles are disabled when omitted rather than falling through to an ambient first-party credential.

For a desktop credential boundary, set `ApiProviderConfig.base_url` to the
Host's loopback OpenAI-compatible relay and set `api_key` to the relay-scoped
bearer. The SDK sends that value as `Authorization: Bearer …` and does not
persist the provider configuration. Raw provider credentials can therefore
remain in the Host's OS-keychain/relay boundary. Catalog or credential changes
are admitted by draining the current Runtime and starting its replacement with
the new fixed configuration; the SDK intentionally has no runtime registry or
mutable provider-credential store.

## Native desktop M1–M3 public-contract map

This table records the minimum embedding contract and prevents product hosts
from replacing runtime-native behavior with a second harness, registry, or
executable.

| Milestone concern | Current public contract | Gap / decision |
|---|---|---|
| Application model catalog | `RuntimeConfig::models`, `ModelSpec`, `Runtime::list_models` | Complete for a Host-owned fixed catalog. Refresh revisions and connection health remain Host state; restart the drained Runtime to admit a new catalog. |
| Provider endpoint + relay bearer | `ApiProviderConfig`, `RuntimeBuilder::model_provider` | Complete. `api_key` is the Bearer value and may be a loopback-relay token. Provider raw credentials need not enter the SDK. |
| One Runtime, one Session per Host Thread | `Runtime`, `create_session`, `create_session_with_id`, `load_session`, `resume_session`, `unload_session`, `delete_session` | Complete; no registry or external executable is required. `create_session_with_id` gives the Host a crash-safe, idempotently retryable Thread↔Session identity when `SessionStateStore` is installed; `delete_session` coordinates actor teardown with permanent authority deletion. |
| Session cwd/model/reasoning | `SessionConfig::{cwd, model, reasoning}`, `Runtime::set_route` | Complete for M1. Explicit reasoning wins; omission resolves to the validated fixed-catalog default on create/load/resume and route changes. |
| Restart, recovery, receipt, cursor | `PromptReceipt`, `SessionLedger`, rewind receipts, `events_after`, Run reconciliation/attach APIs | Complete for M1. A cursor gap is typed and fails closed. |
| Host-owned native Session state | `RuntimeBuilder::session_state_store`, `SessionStateStore`, chunked `SessionObject`s, CAS `SessionManifest`, `LocalSessionStateStore` | Complete. With a Host store installed it is the sole authority for transcript/history, rewind state, and compaction checkpoints; its Session leases fence create/load/resume/delete and both sides of fork/worktree-resume across Runtime instances. Covered JSONL files are neither read nor projected. Without injection the legacy JSONL backend remains available. |
| Immutable harness materialization | `HarnessSnapshot`, `HarnessContent`, `MaterializedHarness` | First-batch contract on this branch. A snapshot requires the complete system prompt; rules are deterministically folded into that authoritative override for native create/load/resume. There is no mutable SDK harness store. |
| Harness snapshot persistence | `HarnessStore`, `LocalHarnessStore`, `HarnessPut`, `harness_put_reconciled`, `run_harness_store_conformance` | Content-addressed and append-only. Hosts inject the authority; the Runtime keeps only the bound digest and never reads or writes stored snapshots. No update, replace, or delete operation exists, and the conformance suite rejects a backend that replaces content under a digest. |
| Turn binding | `TurnBindingReceipt`, `CompleteEventCursor`, `SdkProvenance`, harness-aware Session/prompt methods | First-batch contract on this branch. Provider-wire tests cover exact prompt replacement, rules update/removal, effective routes, load/resume and Runtime restart before a receipt is issued. |
| Optimistic refinement | `HarnessRefinementPatch`, `HarnessRefinement`, `HarnessEvidenceRef`, `HarnessEvidenceKind` | First-batch contract on this branch. Patch application rejects stale content identity and duplicate typed targets, and a patch carries the bounded typed evidence it cites. The Host commits revisions, evidence, activation, history and rollback. |
| Child Run / A2A | `admit_run_child`, `settle_run_child`, `accept_run_message`, `transition_run_message` | Durable admission, reservations, fenced settlement, de-duplication, and ordered mailbox state use the existing Run reducer. The shell subagent coordinator remains a UI/transport adapter and is not silently treated as Run authority. Hosts execute child placement and feed its typed settlement callback. |
| Per-Session capability layering | `CapabilityLayer`, `RuntimeBuilder::general_capabilities`, `create_session_with_capabilities`, `create_session_with_harness_and_capabilities`, `load_session_with_capabilities`, `resume_session_with_capabilities`, `set_session_capabilities`, `session_capabilities` | Complete for skills, MCP mounts and agent-service routes. One application-owned general layer is masked per Session by name and kind, so per-project activation and per-Session routing need neither a Runtime restart nor a second Runtime. |
| Persistent kernel | `TerminalBackend`, background task handles, native terminal/PTY/process tools | M3 audit only. Persistent shell state restores cwd/environment around newly spawned commands; it is not a checkpointable programmatic kernel with durable identity, execution receipt, cancel/restart and state-restore semantics. No internal kernel implementation is suitable to publish. |
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
`grok-build-sdk.harness-snapshot-store`/1. The Runtime never reads or writes
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

`SessionEvidenceStore` is the separate, host-agnostic single authority for SDK-origin `SessionLedger`, rewind intent/receipt, and immutable harness Turn-binding documents. Payload schemas, bounded parsing, identity, settlement digests and transition decisions remain SDK-owned; the Host implementation owns connections/paths, transactions, migrations, encryption, backup and lifecycle. The current marker/version is `grok-build-sdk.session-evidence`/1. CAS compares revision and digest: absence advances to revision 1, otherwise checked `current + 1`; the digest is `sha256:` plus lowercase SHA-256 of the exact payload bytes. Implementations must return the exact value produced by `SessionEvidenceVersion::successor`. `Conflict`, a malformed successor, or `CommitUnknown` always fails closed. Pending is acknowledged before native prompt dispatch, rewind intent before native rewind, intent-to-receipt is one CAS replacement, and binding evidence is acknowledged before ledger settlement. `RuntimeBuilder::session_evidence_store` replaces the local reference store without mirroring. `Runtime::start_with_stores` avoids startup API combinations when both production authorities are injected. Current-only schemas require an explicit offline migration or deliberate discard before startup.

`SessionStateStore` is the chunked native persistence boundary without shell
protocol types. Its current-only `grok-build-sdk.session-log`/1 contract
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

Startup still creates `grok_home` and `session_storage` for uncovered shell
sidecars and native tool/process/terminal state. In Host Session-state mode it
does not read, write, create, import, or fork-copy `updates.jsonl`,
`chat_history.jsonl`, `rewind_points.jsonl`, or
`compaction_checkpoints/**`; chat history and rewind replay are rebuilt in
memory from the authority. Without injection, the legacy JSONL implementation
is unchanged. The `Event` receiver and `events_after` journal remain bounded
in-memory delivery only and are not durable evidence.

`AutonomousTurnLoop` currently has enforceable exact upper bounds only for iteration count, agent calls, and concurrency. Until a model/runtime capability contract supplies enforceable per-Turn maxima, finite `tokens`, `cost_micros`, `active_ms`, `wall_ms`, or `artifact_bytes` budgets are rejected before an iteration or prompt is dispatched. Use `u64::MAX` to mark those dimensions explicitly unbounded. Actual typed usage is still settled and recorded; an overrun or unknown value against a finite reservation durably enters recovery rather than being treated as free work.

| SDK owns | Embedding Host owns |
|---|---|
| Run reducer and lifecycle invariants, bounded loop, budgets, gates, verifier policy, intent/outbox, command de-duplication, epoch/token fencing, receipts, recovery decisions and attach contract | Worker/process placement, OS daemon/service residency, durable timer implementation and invoking bounded activations |
| SessionLedger/rewind/binding schemas; native Session object/chunk schemas, validation, replay and publication semantics; CAS transition intent and fail-closed reconciliation; artifact identity/integrity and provider contracts | Physical Run, session-evidence, and native Session-state persistence; transactions/migrations/encryption/backup/lifecycle; uncovered shell-sidecar placement; credentials, providers, workspace, queues, policy and UI |

`ProviderSet` supplies typed artifact, gate, verifier, approval, and telemetry contracts. Local defaults store content-addressed artifacts and fail gates, verification, and approval closed until the Host installs explicit providers.

### This is not yet full Prime Agent parity

Durable wake intent, timer deadline, worker lease/takeover, child reservation/callbacks, mailbox delivery, immutable Harness activation pins, and `ProgramRuntime` execution/reconciliation now run through the authoritative reducer and public façade. Production residency must claim a lease and invoke `AutonomousTurnLoop::activate_claimed`; shell scheduler/subagent mechanisms are adapters only. Program execution is product-connectable when the Host supplies `ProgramRuntime` and `ArtifactStore`; the short-lived opaque credential is passed only to the Host driver while the Run stores its non-secret key identity/generation/scope.

The remaining explicit gaps are a built-in persistent kernel, a native bounded Rhai Run driver, and direct adaptation of native shell compaction into Run effects. `PersistentKernelDriver` is a Host contract only: a VM/kernel checkpoint is evidence, never durable truth. `ProgramContext` durably pins versioned skill descriptors and compaction continuity, but this does not claim that shell skill reload or native compaction already uses that path. All pre-v4 Run databases/envelopes are rejected rather than silently upgraded; migration requires an explicit offline policy. Consumer integration requirements and product-wiring status are machine-readable in `consumer-integration.json`.

The Run API uses non-exhaustive public enums/DTO constructors, checked identifier deserialization, conservative unknown-value handling, and a checked-in fixture documenting the current v4 shape. Durable JSON must enter through bounded, validated `RunEnvelope::from_json_slice` or `RunEnvelope::from_json_reader`; generic serde deserialization performs recursive schema validation but cannot impose a source-byte limit. The same-revision fixture is not described as historical compatibility evidence; release fixtures become immutable only after their originating release ships.

## Agent API coverage

The SDK does not wrap the TUI. It exposes the stateful agent actor below it:

| Grok Build capability | SDK surface |
|---|---|
| Session create/load/resume/unload, cancel, rewind and durable Turn reconciliation | `Runtime` session and ledger methods |
| Text, image, audio and embedded-resource prompts | `prompt` / `prompt_content` |
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
- generation-bound Task get/update/cancel operations and ordered, allowlisted Task-status events; Task pushes never expose the server's raw Task object, result, error, or `_meta` fields;
- bounded `subscriptions/listen` streams for tool, prompt, resource-list, and individual-resource changes, with explicit acknowledgement, non-blocking cancellation, lag, and transport-end states; notification variants expose only their typed allowlisted fields, and subscriptions never silently resume after reconnect;
- typed, capability-gated roots, sampling, and elicitation host services for MRTR input requests, plus authorized roots-list-change notification;
- protocol ping;
- HTTP OAuth status/start and atomic server replacement;
- SDK-owned, identity-aware, full-duplex in-process MCP servers through `InProcessMcpHandler`; their bounded notification peer is invalidated when the owning session incarnation is unloaded or replaced;
- typed server-status, tools-changed and initialization-progress events. Push events omit raw payloads, status details, tool metadata and capability-extension values; unknown MCP control-plane notifications are suppressed rather than exposing unreviewed configuration data.

Modern roots, model sampling, and elicitation requests are carried by MRTR `inputRequests` and answered through installed typed host services or an `McpContinuation` created with `McpInputRequired::respond`. A continuation is bound to its session incarnation, server, connection generation, operation kind and target; cross-operation reuse and reuse after reconnect fail closed, while the opaque `requestState` is returned unchanged. The legacy unrestricted reverse-request path is not used for these roles. Capabilities are advertised only when the corresponding typed service is installed and authorized. Unknown input-request methods fail closed.

The SDK deliberately does not call legacy `resources/subscribe` / `resources/unsubscribe`, expose a generic server-to-client request peer, or add an ACP compatibility service. Deprecated pre-2026 logging and direct roots/sampling request forms are retained only where rmcp's protocol model requires them; they are not the modern public execution path. Negotiated capability fields report what the server advertised for the selected version and remain distinct from host authorization.

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

## Capability boundaries

The SDK exposes every embeddable implementation present in this source tree; it does not claim to contain product code that is absent upstream. In particular, App Builder deployment is compiled as a disabled stub in this checkout, managed MCP catalog services use a separate account-product protocol, and OS screenshot/accessibility/OCR/input automation must be supplied by the desktop host. Those boundaries are reported as unavailable or host-provided rather than represented as working native SDK features.

Capability descriptors describe public typed SDK features, not every internal shell route or named xAI product service. Public releases must preserve this distinction.

## Development and verification

Use the gates in increasing cost order:

1. Run the fastest no-compilation source-layout preflight from the repository
   root: `crates/grok-build-sdk/scripts/check-source-layout.sh`.
2. Run the Cargo-integrated version of the same policy with
   `cargo test -p grok-build-sdk --test source_layout`. It remains part of the
   automatic Cargo test suite and is fast when the build cache is warm.
3. Check formatting and every SDK target:
   `cargo fmt --all -- --check`, then
   `cargo check -p grok-build-sdk --all-targets`.
4. During iteration, run the narrow domain test that covers the change. For
   Session-state work, use
   `cargo test -p grok-build-sdk session_state::tests`,
   `cargo test -p grok-build-sdk --test session_state_store`, and
   `cargo test -p grok-build-sdk --test host_backend_conformance the_reference_session_state_store`.
5. Before merge or push, run the full SDK suite:
   `cargo test -p grok-build-sdk`. Focused tests shorten iteration; they do not
   replace this final proof.
6. Run comprehensive linting with
   `cargo clippy -p grok-build-sdk --all-targets`.

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
grok-build-sdk = { git = "https://github.com/fran0220/grok-build-sdk", tag = "v0.2.0" }
```
