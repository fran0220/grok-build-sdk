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

## Profiles and trust boundary

`Restricted` is the default and remains fail-closed for plugins, MCP, subagents, workflows, network tools, media tools, and workspace `.envrc` evaluation. Supplying their configuration does not enable them. `Desktop` restores the repository-native feature surface inside the embedded storage/process boundary; each media operation is still independently gated by `MediaServiceConfig`.

Restricted filesystem and terminal calls are explicitly rejected unless the host advertises and implements the matching `HostDelegate` capability; they never fall back to the runtime process's local machine. In Desktop, an advertised host capability still routes through `HostDelegate`, while an unadvertised filesystem or terminal capability deliberately retains Grok's native local desktop implementation.

Agent commands, scheduler operations, workflows, subagents, MCP, hooks, permissions, rewind, sessions, and model discovery have typed methods. `Runtime::capabilities` reports these SDK features rather than protocol method namespaces. For forward compatibility, the generic extension request/notification bridge also preserves JSON and protocol errors for current and future `x.ai/*` methods in `Desktop`; it is disabled wholesale in `Restricted`, so privileged filesystem, terminal, plugin, worktree, and process methods cannot bypass that profile. The typed, read-only `Runtime::list_models` wrapper remains available in Restricted because it only inspects the host-supplied fixed catalog. **Do not expose the Desktop bridge directly to a WebView or untrusted renderer.** Validate and authorize calls in the Rust main process.

Screenshots, accessibility trees (AX/UIA/AT-SPI), OCR, and mouse/keyboard automation are not native Grok capabilities; a desktop host must provide those through an audited `HostDelegate`. Rich prompt blocks can be submitted independently of TUI support. The current sampling layer has no native audio part, so audio is preserved losslessly as a data-URI text attachment rather than silently discarded.

The event receiver provides push delivery. `events_after` reads the same bounded per-session journal and reports `Error::EventGap` when a cursor was evicted.

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

For the current upstream-synchronized release, a Rust host can pin the SDK
without relying on a moving branch:

```toml
[dependencies]
grok-build-sdk = { git = "https://github.com/fran0220/grok-build-sdk", tag = "v0.2.0" }
```
