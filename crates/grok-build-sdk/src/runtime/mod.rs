// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid runtime configuration: {0}")]
    InvalidConfig(String),
    #[error("runtime operation failed: {0}")]
    Operation(String),
    #[error("runtime has shut down")]
    Shutdown,
    #[error("protocol error in {method}: {code}: {message}")]
    Protocol {
        method: String,
        code: i32,
        message: String,
        data: serde_json::Value,
        retryable: bool,
    },
    #[error("host error in {method}: {source}")]
    Host { method: String, source: HostError },
    #[error("event journal gap: requested {requested}, oldest {oldest_available}, newest {newest}")]
    EventGap {
        requested: u64,
        oldest_available: u64,
        newest: u64,
    },
    #[error(transparent)]
    DurableRun(#[from] run::RunError),
    #[error(transparent)]
    Harness(#[from] HarnessError),
}

pub(crate) fn run_reconcile_command_id(
    parent: &run::CommandId,
    operation: &run::OperationId,
) -> Result<run::CommandId, Error> {
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(format!("{}\0{}", parent.as_str(), operation.as_str()));
    run::CommandId::new(format!("reconcile_{digest:x}")).map_err(|error| {
        Error::Operation(format!(
            "could not derive durable reconciliation command: {error}"
        ))
    })
}

#[derive(Clone)]
pub struct Runtime {
    pub(crate) inner: private::Runtime,
}

impl Runtime {
    pub async fn start(
        config: RuntimeConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start(config, RuntimeOptions::default())
            .await
            .map(|(inner, events)| (Self { inner }, events))
    }
    /// Starts the SDK with one Host-provided Run authority. The supplied store
    /// replaces (rather than mirrors) the standalone SQLite store, so snapshot,
    /// journal, outbox and command receipts still have exactly one writer.
    pub async fn start_with_run_store(
        config: RuntimeConfig,
        store: Arc<dyn run::RunStore>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start_with_run_store(config, RuntimeOptions::default(), Some(store))
            .await
            .map(|(inner, events)| (Self { inner }, events))
    }
    /// Starts with both Host-owned persistence authorities. Each injected store
    /// replaces its local default; neither is mirrored.
    pub async fn start_with_stores(
        config: RuntimeConfig,
        run_store: Arc<dyn run::RunStore>,
        evidence_store: Arc<dyn SessionEvidenceStore>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        private::Runtime::start_with_stores(
            config,
            RuntimeOptions::default(),
            Some(run_store),
            Some(evidence_store),
            None,
        )
        .await
        .map(|(inner, events)| (Self { inner }, events))
    }
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.inner.capabilities()
    }
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.inner.shutdown().await
    }
}

mod builder;
mod extensions;
mod mcp;
mod runs;
mod sessions;

pub use builder::RuntimeBuilder;
pub use extensions::{ExtensionNotification, ExtensionRequest, ExtensionResponse};
