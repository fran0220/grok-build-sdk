use crate::{
    AvailableModel, CapabilityLayer, CapabilityResolution, ConversationRewindReceipt,
    ConversationRewindStatus, Error, Event, EventUpdate, ExtensionNotification, ExtensionRequest,
    ExtensionResponse, HarnessDigest, HarnessError, InputSource, LedgerTurnState, ModelCatalog,
    Prompt, PromptBlock, PromptReceipt, ResolvedCapabilities, RewindPoint, RuntimeCapabilities,
    RuntimeConfig, RuntimeOptions, SessionConfig, SessionEvidenceCommit, SessionEvidenceDocument,
    SessionEvidenceKey, SessionEvidenceKind, SessionEvidenceStore, SessionEvidenceVersion,
    SessionId, SessionLedger, SessionLedgerEntry, SessionReplayProbe, TurnBindingKey,
    TurnBindingReceipt, TurnBindingRecord, TurnBindingStatus, TurnOutcome, resolve_capabilities,
};
use agent_client_protocol as acp;
use agent_client_protocol::Agent as _;
use indexmap::IndexMap;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    num::NonZeroU64,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot, watch};
use xai_acp_lib::{AcpAgentGatewaySender, AcpGatewayReceiver};
use xai_grok_shell::{
    agent::{
        config::{Config, ModelEntry, ModelEntryConfig, OriginMediaConfig},
        models::ModelsManager,
        mvp_agent::MvpAgent,
    },
    auth::AuthManager,
};

const CANCEL_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
fn to_acp_mcp_server(server: &crate::McpServerConfig) -> acp::McpServer {
    match server {
        crate::McpServerConfig::Stdio {
            name,
            command,
            args,
            env,
        } => acp::McpServer::Stdio(
            acp::McpServerStdio::new(name, command)
                .args(args.clone())
                .env(
                    env.iter()
                        .map(|(k, v)| acp::EnvVariable::new(k, v))
                        .collect(),
                ),
        ),
        crate::McpServerConfig::Http { name, url, headers } => acp::McpServer::Http(
            acp::McpServerHttp::new(name, url).headers(
                headers
                    .iter()
                    .map(|(k, v)| acp::HttpHeader::new(k, v))
                    .collect(),
            ),
        ),
        crate::McpServerConfig::Sse { name, url, headers } => acp::McpServer::Sse(
            acp::McpServerSse::new(name, url).headers(
                headers
                    .iter()
                    .map(|(k, v)| acp::HttpHeader::new(k, v))
                    .collect(),
            ),
        ),
    }
}
type Reply<T> = oneshot::Sender<Result<T, Error>>;
type SessionMeta = serde_json::Map<String, serde_json::Value>;
type PromptUsage = xai_grok_shell::extensions::notification::PromptUsage;

#[derive(Clone, Debug, PartialEq)]
enum CapturedTurnUsage {
    Exact(Option<PromptUsage>),
    Conflict,
}

type TurnUsageMap = Rc<RefCell<HashMap<(String, String), CapturedTurnUsage>>>;
#[derive(Clone)]
struct AutonomousCompactionCorrelation {
    run: crate::run::RunId,
    iteration: crate::run::IterationId,
    operation: crate::run::OperationId,
}
type CompactionCorrelationMap =
    Arc<std::sync::Mutex<HashMap<(String, String), AutonomousCompactionCorrelation>>>;
enum Command {
    Create(
        SessionConfig,
        Option<HarnessDigest>,
        CapabilityLayer,
        Reply<SessionId>,
    ),
    Ensure(SessionId, SessionConfig, Reply<SessionId>),
    Load(
        SessionId,
        SessionConfig,
        Option<HarnessDigest>,
        CapabilityLayer,
        Reply<()>,
    ),
    Resume(
        SessionId,
        SessionConfig,
        Option<HarnessDigest>,
        CapabilityLayer,
        Reply<()>,
    ),
    SetCapabilities(SessionId, CapabilityLayer, Reply<CapabilityResolution>),
    SessionCapabilities(SessionId, Reply<CapabilityResolution>),
    Prompt(SessionId, String, String, InputSource, Reply<PromptReceipt>),
    PromptAutonomous(
        SessionId,
        String,
        String,
        AutonomousCompactionCorrelation,
        Reply<PromptReceipt>,
    ),
    PromptContent(SessionId, String, Prompt, InputSource, Reply<PromptReceipt>),
    PromptBound(
        SessionId,
        String,
        Prompt,
        HarnessDigest,
        Reply<TurnBindingReceipt>,
    ),
    ListModels(Reply<ModelCatalog>),
    Extension(ExtensionRequest, Reply<ExtensionResponse>),
    Fork(
        SessionId,
        SessionId,
        ExtensionRequest,
        Reply<ExtensionResponse>,
    ),
    ExtensionNotification(ExtensionNotification, Reply<()>),
    SetMode(SessionId, String, Reply<()>),
    ListSessions(Reply<serde_json::Value>),
    EventsAfter(SessionId, u64, Reply<Vec<Event>>),
    ProbeSessionReplay(SessionId, u64, Reply<SessionReplayProbe>),
    Cancel(SessionId, Reply<()>),
    SessionLedger(SessionId, Reply<SessionLedger>),
    TurnBindingStatus(SessionId, TurnBindingKey, Reply<crate::TurnBindingStatus>),
    MarkTurnDiscarded(SessionId, String, String, u64, Reply<()>),
    SetRoute(SessionId, String, Option<String>, Reply<()>),
    RewindPoints(SessionId, Reply<Vec<RewindPoint>>),
    Rewind(SessionId, String, u64, Reply<ConversationRewindReceipt>),
    RewindUnsettled(
        SessionId,
        String,
        String,
        String,
        u64,
        Reply<ConversationRewindReceipt>,
    ),
    RewindStatus(SessionId, String, Reply<ConversationRewindStatus>),
    ReplaceMcp(SessionId, Vec<crate::McpServerConfig>, Reply<()>),
    McpModern(
        SessionId,
        String,
        xai_grok_shell::extensions::mcp::McpModernOperation,
        Reply<serde_json::Value>,
    ),
    McpSubscribe(
        SessionId,
        String,
        xai_grok_shell::extensions::mcp::McpModernSubscriptionFilter,
        std::num::NonZeroUsize,
        Reply<xai_grok_shell::extensions::mcp::McpModernSubscription>,
    ),
    Close(SessionId, Reply<()>),
    Delete(SessionId, Reply<()>),
    Unload(SessionId, Reply<()>),
    Shutdown(Reply<()>),
}

mod acp_client;
mod core;
mod evidence;
mod mcp_transport;
mod runtime;
mod session_authority;
mod validation;
mod worker;

use acp_client::*;
use core::*;
pub(crate) use evidence::ledger_settlement_id;
use evidence::*;
use mcp_transport::*;
pub(crate) use runtime::Runtime;
use session_authority::*;
use validation::*;
use worker::*;

#[cfg(test)]
mod tests;
