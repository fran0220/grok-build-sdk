use super::*;

#[derive(Clone)]
pub(crate) struct Runtime {
    shared: Arc<RuntimeShared>,
}
struct RuntimeShared {
    commands: mpsc::UnboundedSender<Command>,
    lifecycle: Arc<LifecycleOwner>,
    completion: watch::Receiver<Option<LifecycleOutcome>>,
    runs: tokio::sync::Mutex<
        xai_agent_lifecycle::run::RunController<Arc<dyn xai_agent_lifecycle::run::RunStore>>,
    >,
    shutdown: AtomicBool,
    capabilities: RuntimeCapabilities,
}

impl Runtime {
    pub async fn start(
        input: RuntimeConfig,
        options: RuntimeOptions,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        Self::start_with_run_store(input, options, None).await
    }

    pub async fn start_with_run_store(
        input: RuntimeConfig,
        options: RuntimeOptions,
        run_store: Option<Arc<dyn xai_agent_lifecycle::run::RunStore>>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        Self::start_with_stores(input, options, run_store, None, None).await
    }

    pub async fn start_with_stores(
        input: RuntimeConfig,
        options: RuntimeOptions,
        run_store: Option<Arc<dyn xai_agent_lifecycle::run::RunStore>>,
        evidence_store: Option<Arc<dyn SessionEvidenceStore>>,
        session_state_store: Option<Arc<dyn crate::SessionStateStore>>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>), Error> {
        validate(&input, &options)?;
        if options.event_journal_capacity == 0 {
            return Err(Error::InvalidConfig(
                "event journal capacity must be positive".into(),
            ));
        }
        let advertises_host_io = options.host_capabilities.fs_read
            || options.host_capabilities.fs_write
            || options.host_capabilities.terminal
            || !options.host_capabilities.extension_methods.is_empty();
        if advertises_host_io && options.host.is_none() {
            return Err(Error::InvalidConfig(
                "host capabilities require a HostDelegate".into(),
            ));
        }
        if options.client_identifier.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "client identifier must not be empty".into(),
            ));
        }
        let run_store: Arc<dyn xai_agent_lifecycle::run::RunStore> = match run_store {
            Some(store) => store,
            None => Arc::new(
                xai_agent_lifecycle::run::LocalRunStore::new(
                    input.session_storage.join("durable-runs"),
                )
                .map_err(run_error)?,
            ),
        };
        let runs = xai_agent_lifecycle::run::RunController::open(run_store).map_err(run_error)?;
        let evidence_store = match evidence_store {
            Some(store) => store,
            None => {
                Arc::new(crate::LocalSessionEvidenceStore::new(&input.session_storage).map_err(op)?)
            }
        };
        let (events, event_rx) = mpsc::unbounded_channel();
        let (commands, command_rx) = mpsc::unbounded_channel();
        let (startup_tx, startup_rx) = oneshot::channel();
        let (completion_tx, completion) = watch::channel(None);
        let lifecycle = spawn_worker_lifecycle(
            commands.clone(),
            startup_tx,
            completion_tx,
            move |lifecycle| {
                let startup = StartupReporter::new(lifecycle);
                std::thread::Builder::new()
                    .name("grok-build-sdk".into())
                    .spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build();
                        match rt {
                            Ok(rt) => {
                                let local = tokio::task::LocalSet::new();
                                local.block_on(&rt, async move {
                                    match Core::start(
                                        input,
                                        options,
                                        events,
                                        evidence_store,
                                        session_state_store,
                                    )
                                    .await
                                    {
                                        Ok((core, capabilities)) => {
                                            startup.report(Ok(capabilities));
                                            Rc::new(core).run(command_rx).await;
                                        }
                                        Err(error) => {
                                            startup.report(Err(error));
                                        }
                                    }
                                });
                            }
                            Err(error) => {
                                startup.report(Err(op(error)));
                            }
                        }
                    })
                    .map_err(op)
            },
        )?;
        let capabilities = startup_rx.await.map_err(|_| Error::Shutdown)??;
        Ok((
            Self {
                shared: Arc::new(RuntimeShared {
                    commands,
                    lifecycle,
                    completion,
                    runs: tokio::sync::Mutex::new(runs),
                    shutdown: AtomicBool::new(false),
                    capabilities,
                }),
            },
            event_rx,
        ))
    }
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.shared.capabilities.clone()
    }
    pub(super) fn ensure_running(&self) -> Result<(), Error> {
        if self.shared.shutdown.load(Ordering::Acquire) {
            Err(Error::Shutdown)
        } else {
            Ok(())
        }
    }
    pub async fn create_run(
        &self,
        request: xai_agent_lifecycle::run::CreateRunRequest,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .create_run(request, now_ms())
            .map_err(run_error)
    }
    pub async fn get_run(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
    ) -> Result<Option<xai_agent_lifecycle::run::RunEnvelope>, Error> {
        self.ensure_running()?;
        Ok(self.shared.runs.lock().await.get_run(run_id))
    }
    pub async fn reload_run_if_required(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
    ) -> Result<Option<xai_agent_lifecycle::run::RunEnvelope>, Error> {
        self.ensure_running()?;
        let mut runs = self.shared.runs.lock().await;
        if runs.reload_is_required(run_id) {
            runs.reload_run(run_id).map_err(run_error)
        } else {
            Ok(runs.get_run(run_id))
        }
    }
    pub async fn list_runs(&self) -> Result<Vec<xai_agent_lifecycle::run::RunEnvelope>, Error> {
        self.ensure_running()?;
        Ok(self.shared.runs.lock().await.list_runs())
    }
    pub async fn list_recoverable_runs(
        &self,
    ) -> Result<Vec<xai_agent_lifecycle::run::RunEnvelope>, Error> {
        self.ensure_running()?;
        Ok(self.shared.runs.lock().await.list_recoverable_runs())
    }
    pub async fn inspect_run_residency(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
    ) -> Result<xai_agent_lifecycle::run::ResidencyInspection, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .inspect_residency(run_id, now_ms())
            .map_err(run_error)
    }
    pub async fn request_run_wake(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::WakeRequest>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .request_wake(request, now_ms())
            .map_err(run_error)
    }
    pub async fn claim_run_activation(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ClaimActivation,
        >,
    ) -> Result<
        xai_agent_lifecycle::run::CommandOutput<xai_agent_lifecycle::run::ActivationLease>,
        Error,
    > {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .claim_activation(request, now_ms())
            .map_err(run_error)
    }
    pub async fn renew_run_activation(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<(
            xai_agent_lifecycle::run::ActivationFence,
            u64,
        )>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .renew_activation(request, now_ms())
            .map_err(run_error)
    }
    pub async fn release_run_activation(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ActivationFence,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .release_activation(request, now_ms())
            .map_err(run_error)
    }
    pub async fn control_run(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::RunAction>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .control_run(request, now_ms())
            .map_err(run_error)
    }
    pub async fn wake_run(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::RunAction>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .wake_run(request, now_ms())
            .map_err(run_error)
    }
    pub async fn attach_run(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
        cursor: xai_agent_lifecycle::run::RunEventCursor,
    ) -> Result<xai_agent_lifecycle::run::RunAttach, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .attach_run(run_id, cursor)
            .map_err(run_error)
    }
    pub async fn begin_run_recovery(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<()>,
    ) -> Result<xai_agent_lifecycle::run::RecoveryPlan, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .begin_recovery(request, now_ms())
            .map_err(run_error)
    }
    pub async fn run_recovery_plan(
        &self,
        run_id: &xai_agent_lifecycle::run::RunId,
    ) -> Result<xai_agent_lifecycle::run::RecoveryPlan, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .recovery_plan(run_id)
            .map_err(run_error)
    }
    pub async fn finish_run_recovery(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::RecoveryResolution,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .finish_recovery(request, now_ms())
            .map_err(run_error)
    }
    pub async fn begin_iteration(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::BeginIteration,
        >,
    ) -> Result<
        xai_agent_lifecycle::run::CommandOutput<xai_agent_lifecycle::run::IterationHandle>,
        Error,
    > {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .begin_iteration(request, now_ms())
            .map_err(run_error)
    }
    pub async fn propose_harness(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ProposeHarness,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .propose_harness(request, now_ms())
            .map_err(run_error)
    }
    pub async fn validate_harness(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ValidateHarness,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .validate_harness(request, now_ms())
            .map_err(run_error)
    }
    pub async fn activate_harness(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ActivateHarness,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .activate_harness(request, now_ms())
            .map_err(run_error)
    }
    pub async fn rollback_harness(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::RollbackHarness,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .rollback_harness(request, now_ms())
            .map_err(run_error)
    }
    pub async fn finish_iteration(
        &self,
        callback: xai_agent_lifecycle::run::FinishIteration,
    ) -> Result<xai_agent_lifecycle::run::CallbackResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .finish_iteration(callback, now_ms())
            .map_err(run_error)
    }
    pub async fn prepare_operation(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::PrepareOperation,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .prepare_operation(request, now_ms())
            .map_err(run_error)
    }
    pub async fn claim_effect(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::ClaimEffect>,
    ) -> Result<
        xai_agent_lifecycle::run::CommandOutput<xai_agent_lifecycle::run::CommittedEffect>,
        Error,
    > {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .claim_effect(request, now_ms())
            .map_err(run_error)
    }
    pub async fn acknowledge_effect(
        &self,
        callback: xai_agent_lifecycle::run::EffectCallback,
    ) -> Result<xai_agent_lifecycle::run::CallbackResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .acknowledge_effect(callback, now_ms())
            .map_err(run_error)
    }
    pub async fn reconcile_effect(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::ReconcileEffect,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .reconcile_effect(request, now_ms())
            .map_err(run_error)
    }
    pub async fn admit_child(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::AdmitChild>,
    ) -> Result<xai_agent_lifecycle::run::CommandOutput<xai_agent_lifecycle::run::ChildRun>, Error>
    {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .admit_child(request, now_ms())
            .map_err(run_error)
    }
    pub async fn child_callback(
        &self,
        callback: xai_agent_lifecycle::run::ChildCallback,
    ) -> Result<xai_agent_lifecycle::run::CallbackResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .child_callback(callback, now_ms())
            .map_err(run_error)
    }
    pub async fn accept_run_message(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<xai_agent_lifecycle::run::AcceptMessage>,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .accept_message(request, now_ms())
            .map_err(run_error)
    }
    pub async fn transition_run_message(
        &self,
        request: xai_agent_lifecycle::run::MutationRequest<
            xai_agent_lifecycle::run::TransitionMessage,
        >,
    ) -> Result<xai_agent_lifecycle::run::RunCommandResult, Error> {
        self.ensure_running()?;
        self.shared
            .runs
            .lock()
            .await
            .transition_message(request, now_ms())
            .map_err(run_error)
    }
    pub async fn list_models(&self) -> Result<ModelCatalog, Error> {
        self.call(Command::ListModels).await
    }
    pub(super) async fn call<T>(
        &self,
        build: impl FnOnce(Reply<T>) -> Command,
    ) -> Result<T, Error> {
        let (tx, rx) = oneshot::channel();
        if self.shared.shutdown.load(Ordering::Acquire) {
            return Err(Error::Shutdown);
        }
        self.shared
            .commands
            .send(build(tx))
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)?
    }
    pub async fn create_session(&self, c: SessionConfig) -> Result<SessionId, Error> {
        self.call(|r| Command::Create(c, None, r)).await
    }
    pub async fn create_session_with_id(
        &self,
        id: SessionId,
        c: SessionConfig,
    ) -> Result<SessionId, Error> {
        self.call(|r| Command::Ensure(id, c, r)).await
    }
    pub async fn create_session_with_harness(
        &self,
        c: SessionConfig,
        digest: HarnessDigest,
    ) -> Result<SessionId, Error> {
        self.call(|r| Command::Create(c, Some(digest), r)).await
    }
    pub async fn load_session(&self, id: SessionId, c: SessionConfig) -> Result<(), Error> {
        self.call(|r| Command::Load(id, c, None, r)).await
    }
    pub async fn load_session_with_harness(
        &self,
        id: SessionId,
        c: SessionConfig,
        digest: HarnessDigest,
    ) -> Result<(), Error> {
        self.call(|r| Command::Load(id, c, Some(digest), r)).await
    }
    pub async fn resume_session(&self, id: SessionId, c: SessionConfig) -> Result<(), Error> {
        self.call(|r| Command::Resume(id, c, None, r)).await
    }
    pub async fn resume_session_with_harness(
        &self,
        id: SessionId,
        c: SessionConfig,
        digest: HarnessDigest,
    ) -> Result<(), Error> {
        self.call(|r| Command::Resume(id, c, Some(digest), r)).await
    }
    pub async fn prompt(
        &self,
        id: &SessionId,
        t: String,
        x: String,
    ) -> Result<PromptReceipt, Error> {
        self.call(|r| Command::Prompt(id.clone(), t, x, r)).await
    }
    pub async fn prompt_content(
        &self,
        id: &SessionId,
        t: String,
        p: Prompt,
    ) -> Result<PromptReceipt, Error> {
        self.call(|r| Command::PromptContent(id.clone(), t, p, r))
            .await
    }
    pub async fn prompt_content_with_harness(
        &self,
        id: &SessionId,
        t: String,
        p: Prompt,
        digest: HarnessDigest,
    ) -> Result<TurnBindingReceipt, Error> {
        self.call(|reply| Command::PromptBound(id.clone(), t, p, digest, reply))
            .await
    }
    pub async fn extension_request(&self, x: ExtensionRequest) -> Result<ExtensionResponse, Error> {
        self.call(|r| Command::Extension(x, r)).await
    }
    pub async fn fork_session(
        &self,
        source: SessionId,
        target: SessionId,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        self.call(|reply| Command::Fork(source, target, request, reply))
            .await
    }
    pub async fn extension_notification(&self, x: ExtensionNotification) -> Result<(), Error> {
        self.call(|r| Command::ExtensionNotification(x, r)).await
    }
    pub async fn set_mode(&self, id: &SessionId, mode: String) -> Result<(), Error> {
        self.call(|r| Command::SetMode(id.clone(), mode, r)).await
    }
    pub async fn list_sessions(&self) -> Result<serde_json::Value, Error> {
        self.call(Command::ListSessions).await
    }
    pub async fn close_session(&self, id: SessionId) -> Result<(), Error> {
        self.call(|reply| Command::Close(id, reply)).await
    }
    pub async fn delete_session(&self, id: SessionId) -> Result<(), Error> {
        self.call(|reply| Command::Delete(id, reply)).await
    }
    pub async fn events_after(&self, id: &SessionId, sequence: u64) -> Result<Vec<Event>, Error> {
        self.call(|r| Command::EventsAfter(id.clone(), sequence, r))
            .await
    }
    pub async fn probe_session_replay(
        &self,
        id: &SessionId,
        after_sequence: u64,
    ) -> Result<SessionReplayProbe, Error> {
        self.call(|reply| Command::ProbeSessionReplay(id.clone(), after_sequence, reply))
            .await
    }
    pub async fn cancel(&self, id: &SessionId) -> Result<(), Error> {
        self.call(|r| Command::Cancel(id.clone(), r)).await
    }
    pub async fn session_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        self.call(|reply| Command::SessionLedger(id.clone(), reply))
            .await
    }
    pub async fn turn_binding_status(
        &self,
        id: &SessionId,
        key: TurnBindingKey,
    ) -> Result<TurnBindingStatus, Error> {
        self.call(|reply| Command::TurnBindingStatus(id.clone(), key, reply))
            .await
    }
    pub async fn mark_turn_discarded(
        &self,
        id: &SessionId,
        turn_id: String,
        prompt_digest: String,
        runtime_prompt_index: u64,
    ) -> Result<(), Error> {
        self.call(|reply| {
            Command::MarkTurnDiscarded(
                id.clone(),
                turn_id,
                prompt_digest,
                runtime_prompt_index,
                reply,
            )
        })
        .await
    }
    pub async fn set_route(
        &self,
        id: &SessionId,
        model: String,
        reasoning: Option<String>,
    ) -> Result<(), Error> {
        self.call(|r| Command::SetRoute(id.clone(), model, reasoning, r))
            .await
    }
    pub async fn rewind_points(&self, id: &SessionId) -> Result<Vec<RewindPoint>, Error> {
        self.call(|r| Command::RewindPoints(id.clone(), r)).await
    }
    pub async fn rewind_conversation(
        &self,
        id: &SessionId,
        operation_id: String,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.call(|r| Command::Rewind(id.clone(), operation_id, target_prompt_index, r))
            .await
    }
    pub async fn rewind_unsettled_turn(
        &self,
        id: &SessionId,
        operation_id: String,
        turn_id: String,
        prompt_digest: String,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.call(|reply| {
            Command::RewindUnsettled(
                id.clone(),
                operation_id,
                turn_id,
                prompt_digest,
                target_prompt_index,
                reply,
            )
        })
        .await
    }
    pub async fn rewind_status(
        &self,
        id: &SessionId,
        operation_id: &str,
    ) -> Result<ConversationRewindStatus, Error> {
        self.call(|r| Command::RewindStatus(id.clone(), operation_id.to_owned(), r))
            .await
    }
    pub async fn unload_session(&self, id: SessionId) -> Result<(), Error> {
        self.call(|r| Command::Unload(id, r)).await
    }
    pub async fn replace_mcp_servers(
        &self,
        id: &SessionId,
        servers: Vec<crate::McpServerConfig>,
    ) -> Result<(), Error> {
        self.call(|r| Command::ReplaceMcp(id.clone(), servers, r))
            .await
    }
    pub async fn mcp_modern(
        &self,
        id: &SessionId,
        server: String,
        operation: xai_grok_shell::extensions::mcp::McpModernOperation,
    ) -> Result<serde_json::Value, Error> {
        self.call(|reply| Command::McpModern(id.clone(), server, operation, reply))
            .await
    }
    pub async fn mcp_subscribe(
        &self,
        id: &SessionId,
        server: String,
        filter: xai_grok_shell::extensions::mcp::McpModernSubscriptionFilter,
        capacity: std::num::NonZeroUsize,
    ) -> Result<xai_grok_shell::extensions::mcp::McpModernSubscription, Error> {
        self.call(|reply| Command::McpSubscribe(id.clone(), server, filter, capacity, reply))
            .await
    }
    pub async fn shutdown(&self) -> Result<(), Error> {
        if !self.shared.shutdown.swap(true, Ordering::AcqRel) {
            self.shared.lifecycle.shutdown();
        }
        wait_for_completion(&mut self.shared.completion.clone()).await
    }
}
