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
| One Runtime, one Session per Host Thread | `Runtime`, `create_session`, `load_session`, `resume_session`, `unload_session` | Complete; no registry or external executable is required. The Host owns the Thread↔Session mapping. |
| Session cwd/model/reasoning | `SessionConfig::{cwd, model, reasoning}`, `Runtime::set_route` | Complete. Route changes preserve the native conversation and harness. |
| Restart, recovery, receipt, cursor | `PromptReceipt`, `SessionLedger`, rewind receipts, `events_after`, Run reconciliation/attach APIs | Complete for M1. A cursor gap is typed and fails closed. |
| Immutable harness materialization | `HarnessSnapshot`, `HarnessContent`, `MaterializedHarness` | Complete. The content-addressed snapshot is validated and materialized into native `SessionConfig`; there is no mutable SDK harness store. |
| Turn binding | `PromptReceipt` binds prompt index, settlement and usage only | M2 façade gap: bind the immutable snapshot digest, selected model/reasoning, owned SDK provenance and a verified complete event-cursor range. |
| Optimistic refinement | `HarnessRefinementPatch`, `HarnessRefinement` | Complete. Patch application rejects stale content identity and duplicate typed targets. The Host commits revisions, evidence, activation, history and rollback. |
| Child agent / A2A | Live subagent inspection/cancellation exists; lifecycle internals reserve child/mailbox concepts | M3 audit only. There is no durable façade handle or Project-scoped A2A contract to expose safely yet. |
| Persistent kernel | Native terminal/process tools | M3 audit only. A PTY is not a checkpointable programmatic kernel; no suitable internal implementation exists to publish. |
| Continuation / gates | MCP request continuations and Run gate providers are narrow, existing contracts | M3 audit only. Neither is a durable Host continuation/gate aggregate; do not generalize them in the first batch. |

The dependency order is M1 baseline → immutable snapshot/refinement façade →
runtime-generated Turn binding receipt → Host revision/evidence/activation
integration → narrow M3 schemas and receipts. Within M3, define durable child
identity and callback receipts before A2A, then continuation/gate ownership,
and only then a kernel driver if a real internal checkpoint/cancel boundary
exists. This keeps every public change additive and independently reviewable.

## Profiles and trust boundary

`Restricted` is the default and remains fail-closed for plugins, MCP, subagents, workflows, network tools, media tools, and workspace `.envrc` evaluation. Supplying their configuration does not enable them. `Desktop` restores the repository-native feature surface inside the embedded storage/process boundary; each media operation is still independently gated by `MediaServiceConfig`.

Restricted filesystem and terminal calls are explicitly rejected unless the host advertises and implements the matching `HostDelegate` capability; they never fall back to the runtime process's local machine. In Desktop, an advertised host capability still routes through `HostDelegate`, while an unadvertised filesystem or terminal capability deliberately retains Grok's native local desktop implementation.

Agent commands, scheduler operations, workflows, subagents, MCP, hooks, permissions, rewind, sessions, and model discovery have typed methods. `Runtime::capabilities` reports these SDK features rather than protocol method namespaces. For forward compatibility, the generic extension request/notification bridge also preserves JSON and protocol errors for current and future `x.ai/*` methods in `Desktop`; it is disabled wholesale in `Restricted`, so privileged filesystem, terminal, plugin, worktree, and process methods cannot bypass that profile. The typed, read-only `Runtime::list_models` wrapper remains available in Restricted because it only inspects the host-supplied fixed catalog. **Do not expose the Desktop bridge directly to a WebView or untrusted renderer.** Validate and authorize calls in the Rust main process.

Screenshots, accessibility trees (AX/UIA/AT-SPI), OCR, and mouse/keyboard automation are not native Grok capabilities; a desktop host must provide those through an audited `HostDelegate`. Rich prompt blocks can be submitted independently of TUI support. The current sampling layer has no native audio part, so audio is preserved losslessly as a data-URI text attachment rather than silently discarded.

The event receiver provides push delivery. `events_after` reads the same bounded per-session journal and reports `Error::EventGap` when a cursor was evicted.

## Durable autonomous Runs: first vertical slice

`GoalSpec` is immutable goal input: objective, acceptance criteria, constraints, and required evidence. It is not another lifecycle state machine. `run::RunRecord` is the sole authority for long-running work, while the existing Session Turn ledger remains the sole prompt-settlement and rewind-evidence ledger. The Run stores a typed reference and receipt for each Turn; it does not copy conversation history into a second writable ledger.

This revision implements one executable driver, `AutonomousTurnLoop`, end to end:

1. A Host creates a Run and invokes a bounded `AutonomousActivation`. The SDK freezes the iteration context and builds the next goal prompt.
2. The SDK commits the Session Turn intent and a fenced claim with a durable resource reservation before calling `Runtime::prompt`. Effect class is fixed by SDK driver code, not selected by model output.
3. `Runtime::prompt` durably writes Pending and Completed SessionLedger entries around native dispatch. Completed entries bind provider-derived usage into the settlement identity; missing, incomplete, or partial accounting remains typed unknown usage rather than zero. The Run accepts only an exact typed receipt bound to Session, Turn ID, prompt digest, prompt index, outcome, usage, and settlement ID.
4. Gates and the skeptic `GoalVerifier` decide whether an iteration may complete the Run. Reaching an iteration/agent budget produces `Waiting(BudgetExhausted)`, never success.
5. On restart, the previous controller epoch is fenced before SessionLedger/rewind reconciliation. Missing, conflicting, merely Discarded, or otherwise uncertain evidence remains `Recovering`; an uncertain Turn is never guessed or silently repeated. Paused, waiting, cancelled, and failed states survive reconciliation and require an explicit Resume where applicable.

The public façade exposes `create_run`, `get_run`, `list_runs`, `list_recoverable_runs`, `control_run`, `wake_run`, `attach_run`, `reconcile_run`, `resolve_run_recovery`, and `autonomous_turn_loop(...).activate(...)`. Low-level prepare/claim/acknowledge/iteration choreography is intentionally not part of the normal SDK façade. `RunId`, `RunRevision`, `RunEventCursor`, `ControllerEpoch`, `OperationId`, and `IterationId` use distinct Rust types and namespaces; Session `Event.sequence` is not a Run cursor. `attach_run` falls back to `RunAttach::Snapshot` when bounded journal replay is not contiguous.

The default `LocalRunStore` is a standalone SQLite authority with transactional revision CAS. `Runtime::start_with_run_store` and `RuntimeBuilder::run_store` replace **only that Run SQLite store** with one Host-provided authority; they do not mirror or write through to a second Run store. A custom store must atomically commit snapshot, event journal, command receipt, and outbox state, and must report acknowledgement uncertainty as `CommitUnknown`.

This is not a remote-only Runtime mode. Startup still creates `grok_home` and `session_storage`; the native session, SessionLedger, and rewind receipts remain local-filesystem authorities for conversation and Turn evidence. A Host that needs remote workers must place or synchronize that local session storage consistently in addition to implementing `RunStore`. Injecting `RunStore` alone does not relocate those authorities.

`AutonomousTurnLoop` currently has enforceable exact upper bounds only for iteration count, agent calls, and concurrency. Until a model/runtime capability contract supplies enforceable per-Turn maxima, finite `tokens`, `cost_micros`, `active_ms`, `wall_ms`, or `artifact_bytes` budgets are rejected before an iteration or prompt is dispatched. Use `u64::MAX` to mark those dimensions explicitly unbounded. Actual typed usage is still settled and recorded; an overrun or unknown value against a finite reservation durably enters recovery rather than being treated as free work.

| SDK owns | Embedding Host owns |
|---|---|
| Run reducer and lifecycle invariants, bounded loop, budgets, gates, verifier policy, intent/outbox, command de-duplication, epoch/token fencing, receipts, recovery decisions and attach contract | Worker/process placement, OS daemon/service residency, durable timer implementation and invoking bounded activations |
| SessionLedger reconciliation, artifact identity/integrity and fail-closed provider contracts | Credentials and rotation, provider implementations, workspace backend, remote storage/queues, organization policy and UI |

`ProviderSet` supplies typed artifact, gate, verifier, approval, and telemetry contracts. Local defaults store content-addressed artifacts and fail gates, verification, and approval closed until the Host installs explicit providers.

### This is not yet full Prime Agent parity

The target SDK architecture still includes heartbeat/schedule runtime semantics, background worker claim/resume/wake residency, child Runs and A2A mailbox, context/artifact policy and compaction, skills, Host-owned mutable Harness revisions with SDK validation/materialization of immutable `HarnessSnapshot` values, bounded Rhai workflow execution, and a `ProgramRuntime`/`PersistentKernelDriver` boundary for persistent programmable environments. Kernel/VM snapshots may be best effort; durable truth will remain explicit Run state, artifacts, handles, effects, and receipts. Rhai will be a bounded workflow driver, not a claim of RLM/IPython equivalence.

Some lifecycle support types reserve future driver, child, mailbox, and revision concepts, but they are not wired through the SDK façade and are not claimed as working parity. Run schema v2 rejects non-`AutonomousTurnLoop` creation. Schema v1 Run SQLite databases and envelopes are rejected rather than silently upgraded because v1 lacks durable reservations and usage-bound receipts; migration requires an explicit offline policy. `HarnessDescriptor` negotiation and provider credential rotation are also unimplemented additive follow-ups, not hidden behavior in this release.

The Run API uses non-exhaustive public enums/DTO constructors, checked identifier deserialization, conservative unknown-value handling, and a checked-in fixture documenting the current v2 shape. Durable JSON must enter through bounded, validated `RunEnvelope::from_json_slice` or `RunEnvelope::from_json_reader`; generic serde deserialization performs recursive schema validation but cannot impose a source-byte limit. The same-revision fixture is not described as historical compatibility evidence; release fixtures become immutable only after their originating release ships.

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
| Session fork | `fork_session` |
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

## Capability boundaries

The SDK exposes every embeddable implementation present in this source tree; it does not claim to contain product code that is absent upstream. In particular, App Builder deployment is compiled as a disabled stub in this checkout, managed MCP catalog services use a separate account-product protocol, and OS screenshot/accessibility/OCR/input automation must be supplied by the desktop host. Those boundaries are reported as unavailable or host-provided rather than represented as working native SDK features.

Capability descriptors describe public typed SDK features, not every internal shell route or named xAI product service. Public releases must preserve this distinction.

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
