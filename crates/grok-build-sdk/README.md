# Grok Build SDK

The `grok-build-sdk` crate is a trusted, in-process Rust main-process boundary around the bundled Grok agent. `Runtime::start` retains the restricted, compatibility profile. Trusted desktop applications should use `Runtime::builder(config).profile(RuntimeProfile::Desktop)`, explicitly advertise `HostCapabilities`, and install a `HostDelegate`.

## Explicit providers, not account login

An embedding application can supply every inference credential directly. It does not need Grok account authentication:

- `RuntimeConfig.models` defines the fixed catalog and backend contract.
- `RuntimeBuilder::model_provider` or `RuntimeServices::model_providers` selects a base URL, literal API key, provider wire-model slug, request headers, and query parameters independently for each catalog model. When every model has an explicit provider, the legacy `RuntimeConfig.endpoint` and `api_key` may be empty.
- `AgentServiceConfig` routes built-in subagent names and the web-search, session-summary, image-description, and prompt-suggestion auxiliary calls to catalog models. Those catalog models can each use a different provider.
- `MediaProviderConfig` and `MediaServiceConfig` independently enable image generation, image editing, image-to-video, and reference-to-video, including an explicit API URL, key, headers, query parameters, and four model slugs. Query parameters are preserved on image generation/edit and video start/poll requests. The static media credential cannot be replaced by the primary model's rotating credential.
- `McpServerConfig` injects trusted stdio, HTTP, or SSE MCP transports without reading user configuration files.

Explicit model providers use the repository's real Chat Completions or Responses backends. Media providers must implement the xAI Imagine-compatible image/video endpoints and payloads; this SDK does not pretend that an arbitrary diffusion or video API has that contract. Web search similarly uses Grok's existing model-backed web-search path, not an arbitrary third-party search REST schema. Account-only xAI product services remain separate optional product capabilities and are not implied by a custom API key.

Provider and MCP secret-bearing types deliberately omit both `Debug` and `Serialize`; they support `Deserialize` for host-owned configuration input without offering an accidental secret-export path. An explicit provider never resolves its key from an environment variable, Grok login, or ambient Grok config. Unoverridden catalog models retain the legacy endpoint/key fallback for compatibility. Optional auxiliary roles are disabled when omitted rather than falling through to an ambient first-party credential.

## Profiles and trust boundary

`Restricted` is the default and remains fail-closed for plugins, MCP, subagents, workflows, network tools, and media tools. Supplying their configuration does not enable them. `Desktop` restores the repository-native feature surface inside the embedded storage/process boundary; each media operation is still independently gated by `MediaServiceConfig`.

Restricted filesystem and terminal calls are explicitly rejected unless the host advertises and implements the matching `HostDelegate` capability; they never fall back to the runtime process's local machine. In Desktop, an advertised host capability still routes through `HostDelegate`, while an unadvertised filesystem or terminal capability deliberately retains Grok's native local desktop implementation.

The generic extension request/notification bridge preserves JSON and protocol errors and supports current and future `x.ai/*` methods in `Desktop`. It is disabled wholesale in `Restricted`, so privileged `x.ai/fs/*`, terminal, plugin, worktree, and process methods cannot bypass that profile. **Do not expose the Desktop bridge directly to a WebView or untrusted renderer.** Validate and authorize calls in the Rust main process.

Grok-native capability families are reported by `Runtime::capabilities`; this is family-level discovery, not an authoritative registry of every current or future extension method. Screenshots, accessibility trees (AX/UIA/AT-SPI), OCR, and mouse/keyboard automation are not native Grok capabilities; a desktop host must provide those through an audited `HostDelegate` extension. Rich ACP prompt blocks can be sent even where the captured initialize response does not advertise image/audio support; discovery never fabricates those flags. The current sampling layer has no native audio part, so ACP audio is preserved losslessly as a data-URI text attachment rather than silently discarded.

The event receiver is retained for compatibility. `events_after` reads a bounded per-session journal and reports `Error::EventGap` when a cursor was evicted.

## Capability completeness

The SDK exposes every embeddable implementation present in this source tree; it does not claim to contain product code that is absent upstream. In particular, App Builder deployment is compiled as a disabled stub in this checkout, managed MCP catalog services use a separate account-product protocol, and OS screenshot/accessibility/OCR/input automation must be supplied by the desktop host. Those boundaries are reported as unavailable or host-provided rather than represented as working native SDK features.

The generic extension bridge and capability descriptors are forward-compatible integration surfaces, not evidence that every named xAI product service is implemented. Public releases must preserve this distinction.

## Public release status

This repository can be published as an Apache-2.0 source release or consumed from a pinned public Git tag, provided the bundled third-party notices and upstream provenance remain intact. The crate is intentionally `publish = false`: its current `xai-grok-*` dependency closure is workspace-local and cannot yet be resolved independently by crates.io. A crates.io release requires publishing or replacing that full dependency closure, removing workspace-only patches, and validating a packaged source archive first. Do not present a Git release as a crates.io-compatible standalone package until those gates pass.

After the repository rename and a `v0.1.0` tag are published, a Rust host can pin the SDK without relying on a moving branch:

```toml
[dependencies]
grok-build-sdk = { git = "https://github.com/fran0220/grok-build-sdk", tag = "v0.1.0" }
```
