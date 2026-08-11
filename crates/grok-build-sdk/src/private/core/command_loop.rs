use super::*;

impl Core {
    pub(in crate::private) async fn run(self: Rc<Self>, mut rx: mpsc::UnboundedReceiver<Command>) {
        while let Some(c) = rx.recv().await {
            match c {
                Command::Create(x, harness_digest, layer, r) => {
                    let _ = r.send(self.create(x, harness_digest, layer).await);
                }
                Command::Ensure(id, config, r) => {
                    let _ = r.send(self.ensure(id, config).await);
                }
                Command::Load(i, x, harness_digest, layer, r) => {
                    let _ = r.send(self.load(i, x, harness_digest, layer).await);
                }
                Command::Resume(i, x, harness_digest, layer, r) => {
                    let _ = r.send(self.resume(i, x, harness_digest, layer).await);
                }
                Command::SetCapabilities(i, layer, r) => {
                    let _ = r.send(self.set_session_capabilities(i, layer).await);
                }
                Command::SessionCapabilities(i, r) => {
                    let _ = r.send(self.session_capabilities(&i));
                }
                Command::Prompt(i, t, x, source, r) => {
                    if t.trim().is_empty() {
                        let _ = r.send(Err(Error::InvalidConfig("turn id is required".into())));
                        continue;
                    }
                    if self.turns.borrow().contains_key(&i.0) {
                        let _ = r.send(Err(Error::Operation(
                            "session already has an active prompt".into(),
                        )));
                        continue;
                    }
                    self.turns.borrow_mut().insert(i.0.clone(), t.clone());
                    let this = self.clone();
                    let task_key = i.0.clone();
                    let task_session_id = task_key.clone();
                    let reservation = TurnReservation {
                        turns: self.turns.clone(),
                        turn_usages: self.turn_usages.clone(),
                        session_id: task_session_id.clone(),
                        turn_id: t.clone(),
                    };
                    let task = tokio::task::spawn_local(async move {
                        let _reservation = reservation;
                        let result = this.prompt(i, t, x, source).await;
                        this.prompt_tasks.borrow_mut().remove(&task_session_id);
                        let _ = r.send(result);
                    });
                    self.prompt_tasks
                        .borrow_mut()
                        .insert(task_key, task.abort_handle());
                }
                Command::PromptContent(i, t, x, source, r) => {
                    if t.trim().is_empty() {
                        let _ = r.send(Err(Error::InvalidConfig("turn id is required".into())));
                        continue;
                    }
                    if self.turns.borrow().contains_key(&i.0) {
                        let _ = r.send(Err(Error::Operation(
                            "session already has an active prompt".into(),
                        )));
                        continue;
                    }
                    self.turns.borrow_mut().insert(i.0.clone(), t.clone());
                    let this = self.clone();
                    let task_key = i.0.clone();
                    let task_session_id = task_key.clone();
                    let reservation = TurnReservation {
                        turns: self.turns.clone(),
                        turn_usages: self.turn_usages.clone(),
                        session_id: task_session_id.clone(),
                        turn_id: t.clone(),
                    };
                    let task = tokio::task::spawn_local(async move {
                        let _reservation = reservation;
                        let result = this.prompt_content(i, t, x, source).await;
                        this.prompt_tasks.borrow_mut().remove(&task_session_id);
                        let _ = r.send(result);
                    });
                    self.prompt_tasks
                        .borrow_mut()
                        .insert(task_key, task.abort_handle());
                }
                Command::PromptBound(i, t, prompt, harness_digest, reply) => {
                    if t.trim().is_empty() {
                        let _ = reply.send(Err(Error::InvalidConfig("turn id is required".into())));
                        continue;
                    }
                    if self.turns.borrow().contains_key(&i.0) {
                        let _ = reply.send(Err(Error::Operation(
                            "session already has an active prompt".into(),
                        )));
                        continue;
                    }
                    let prepared = match self.prepare_harness_turn(&i, &prompt, &harness_digest) {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                    };
                    self.turns.borrow_mut().insert(i.0.clone(), t.clone());
                    let this = self.clone();
                    let task_key = i.0.clone();
                    let task_session_id = task_key.clone();
                    let reservation = TurnReservation {
                        turns: self.turns.clone(),
                        turn_usages: self.turn_usages.clone(),
                        session_id: task_session_id.clone(),
                        turn_id: t.clone(),
                    };
                    let task = tokio::task::spawn_local(async move {
                        let _reservation = reservation;
                        let result = this
                            .prompt_content_with_harness(i, t, prompt, prepared)
                            .await;
                        this.prompt_tasks.borrow_mut().remove(&task_session_id);
                        let _ = reply.send(result);
                    });
                    self.prompt_tasks
                        .borrow_mut()
                        .insert(task_key, task.abort_handle());
                }
                Command::ListModels(r) => {
                    let _ = r.send(self.list_models().await);
                }
                Command::Extension(x, r) => {
                    let _ = r.send(self.extension_raw(x).await);
                }
                Command::Fork(source, target, request, reply) => {
                    let _ = reply.send(self.fork(source, target, request).await);
                }
                Command::ExtensionNotification(x, r) => {
                    let _ = r.send(self.extension_notification(x).await);
                }
                Command::McpModern(id, server, operation, reply) => {
                    let result = if self.options.profile == crate::RuntimeProfile::Restricted {
                        Err(Error::Operation(
                            "MCP operations require the Desktop profile".into(),
                        ))
                    } else {
                        self.agent
                            .sdk_mcp_modern_operation(id.as_str(), server, operation)
                            .await
                            .map_err(Error::Operation)
                    };
                    let _ = reply.send(result);
                }
                Command::McpSubscribe(id, server, filter, capacity, reply) => {
                    let result = if self.options.profile == crate::RuntimeProfile::Restricted {
                        Err(Error::Operation(
                            "MCP operations require the Desktop profile".into(),
                        ))
                    } else {
                        self.agent
                            .sdk_mcp_modern_subscribe(id.as_str(), server, filter, capacity)
                            .await
                            .map_err(Error::Operation)
                    };
                    let _ = reply.send(result);
                }
                Command::ReplaceMcp(id, servers, r) => {
                    let result = if self.options.profile == crate::RuntimeProfile::Restricted {
                        Err(Error::Operation(
                            "MCP operations require the Desktop profile".into(),
                        ))
                    } else {
                        match validate_mcp_servers(&servers) {
                            Err(error) => Err(error),
                            Ok(())
                                if servers.iter().any(|server| {
                                    let name = match server {
                                        crate::McpServerConfig::Stdio { name, .. }
                                        | crate::McpServerConfig::Http { name, .. }
                                        | crate::McpServerConfig::Sse { name, .. } => name,
                                    };
                                    self.options
                                        .in_process_mcp_servers
                                        .iter()
                                        .any(|embedded| embedded.name == *name)
                                }) =>
                            {
                                Err(Error::InvalidConfig(
                                    "external MCP replacement collides with an in-process server"
                                        .into(),
                                ))
                            }
                            Ok(()) => {
                                let mcp_servers: Vec<acp::McpServer> =
                                    servers.iter().map(to_acp_mcp_server).collect();
                                self.extension::<serde_json::Value>(
                                    "x.ai/session/update_mcp_servers",
                                    serde_json::json!({"sessionId":id.as_str(),"mcpServers":mcp_servers}),
                                )
                                .await
                                .map(drop)
                            }
                        }
                    };
                    let _ = r.send(result);
                }
                Command::SetMode(i, mode, r) => {
                    let _ = r.send(self.set_mode(i, mode).await);
                }
                Command::ListSessions(r) => {
                    let _ = r.send(self.list_sessions().await);
                }
                Command::Cancel(i, r) => {
                    let _ = r.send(self.cancel(i).await);
                }
                Command::EventsAfter(i, sequence, r) => {
                    let _ = r.send(self.events_after(&i, sequence));
                }
                Command::ProbeSessionReplay(i, after_sequence, reply) => {
                    let _ = reply.send(self.probe_session_replay(&i, after_sequence));
                }
                Command::SessionLedger(i, r) => {
                    let _ = r.send(self.session_ledger(&i));
                }
                Command::TurnBindingStatus(i, key, r) => {
                    let _ = r.send(self.turn_binding_status(&i, &key));
                }
                Command::MarkTurnDiscarded(i, turn, digest, prompt_index, r) => {
                    let _ = r.send(self.mark_turn_discarded(&i, turn, digest, prompt_index));
                }
                Command::SetRoute(i, model, reasoning, r) => {
                    let _ = r.send(self.set_route(i, model, reasoning).await);
                }
                Command::RewindPoints(i, r) => {
                    let _ = r.send(self.rewind_points(i).await);
                }
                Command::Rewind(i, operation_id, target, r) => {
                    let _ = r.send(self.rewind_conversation(i, operation_id, target).await);
                }
                Command::RewindUnsettled(
                    i,
                    operation_id,
                    turn_id,
                    prompt_digest,
                    target,
                    reply,
                ) => {
                    let _ = reply.send(
                        self.rewind_conversation_entry(
                            i,
                            operation_id,
                            target,
                            Some((turn_id, prompt_digest)),
                        )
                        .await,
                    );
                }
                Command::RewindStatus(id, operation_id, r) => {
                    let _ = r.send(self.rewind_status(&id, &operation_id));
                }
                Command::Close(i, r) => {
                    let _ = r.send(self.close(i).await);
                }
                Command::Delete(i, r) => {
                    let _ = r.send(self.delete(i).await);
                }
                Command::Unload(i, r) => {
                    let _ = r.send(self.unload(i).await);
                }
                Command::Shutdown(r) => {
                    let mut failures = Vec::new();
                    let active = self.turns.borrow().keys().cloned().collect::<Vec<_>>();
                    for id in active {
                        if let Err(error) = self.cancel(SessionId(id.clone())).await {
                            failures.push(format!("cancel {id}: {error}"));
                        }
                    }
                    let resident = self.resident.borrow().iter().cloned().collect::<Vec<_>>();
                    for id in resident {
                        if let Err(error) = self.unload(SessionId(id.clone())).await {
                            failures.push(format!("unload {id}: {error}"));
                        }
                    }
                    let uncertain_leases = self
                        .session_leases
                        .borrow_mut()
                        .drain()
                        .map(|(_, lease)| lease)
                        .collect();
                    quarantine_session_leases(uncertain_leases);
                    let result = if failures.is_empty() {
                        Ok(())
                    } else {
                        Err(Error::Operation(format!(
                            "native runtime shutdown was incomplete: {}",
                            failures.join("; ")
                        )))
                    };
                    let _ = r.send(result);
                    break;
                }
            }
        }
    }
}
