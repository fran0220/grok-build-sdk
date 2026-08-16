// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use crate::*;

fn parse_subagent_snapshot(value: serde_json::Value) -> Result<SubagentSnapshot, Error> {
    let required = |name: &str| {
        value[name]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::Operation(format!("subagent snapshot omitted {name}")))
    };
    Ok(SubagentSnapshot {
        subagent_id: required("subagentId")?,
        parent_session_id: required("parentSessionId")?,
        child_session_id: required("childSessionId")?,
        subagent_type: required("subagentType")?,
        description: required("description")?,
        started_at_epoch_ms: value["startedAtEpochMs"].as_u64().unwrap_or_default(),
        duration_ms: value["durationMs"].as_u64().unwrap_or_default(),
        status: value["status"].as_str().unwrap_or("running").to_owned(),
        turn_count: value["turnCount"].as_u64().and_then(|v| v.try_into().ok()),
        tool_call_count: value["toolCallCount"]
            .as_u64()
            .and_then(|v| v.try_into().ok()),
        tokens_used: value["tokensUsed"].as_u64(),
        context_window_tokens: value["contextWindowTokens"].as_u64(),
        context_usage_pct: value["contextUsagePct"]
            .as_u64()
            .and_then(|v| v.try_into().ok()),
        tools_used: value["toolsUsed"].as_array().map(|tools| {
            tools
                .iter()
                .filter_map(|tool| tool.as_str().map(str::to_owned))
                .collect()
        }),
        error_count: value["errorCount"].as_u64().and_then(|v| v.try_into().ok()),
        output: value["output"].as_str().map(str::to_owned),
        tool_calls: value["toolCalls"].as_u64().and_then(|v| v.try_into().ok()),
        turns: value["turns"].as_u64().and_then(|v| v.try_into().ok()),
        worktree_path: value["worktreePath"].as_str().map(PathBuf::from),
        failure_error: value["failureError"].as_str().map(str::to_owned),
        cancel_reason: value["cancelReason"].as_str().map(str::to_owned),
        fork_context_source: value["forkContextSource"].as_str().map(str::to_owned),
        fork_parent_prompt_id: value["forkParentPromptId"].as_str().map(str::to_owned),
        resumed_from: value["resumedFrom"].as_str().map(str::to_owned),
        raw: value,
    })
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExtensionRequest {
    pub method: String,
    pub params: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExtensionResponse {
    pub result: serde_json::Value,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExtensionNotification {
    pub method: String,
    pub params: serde_json::Value,
}

impl Runtime {
    /// Returns the live fixed model catalog through Grok Build's
    /// `x.ai/models/list` contract. Unlike generic extension requests, this
    /// typed, read-only operation is also available in Restricted runtimes.
    pub async fn list_models(&self) -> Result<ModelCatalog, Error> {
        self.inner.list_models().await
    }
    pub async fn extension_request(
        &self,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse, Error> {
        self.inner.extension_request(request).await
    }
    pub async fn extension_notification(&self, request: ExtensionRequest) -> Result<(), Error> {
        self.inner
            .extension_notification(ExtensionNotification {
                method: request.method,
                params: request.params,
            })
            .await
    }
    pub async fn notify_extension(&self, notification: ExtensionNotification) -> Result<(), Error> {
        self.inner.extension_notification(notification).await
    }
    pub(crate) async fn raw_ext(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        Ok(self
            .extension_request(ExtensionRequest {
                method: method.to_owned(),
                params,
            })
            .await?
            .result)
    }
    pub(crate) async fn typed_ext(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let envelope = self
            .extension_request(ExtensionRequest {
                method: method.into(),
                params,
            })
            .await?
            .result;
        if let Some(error) = envelope.get("error").filter(|value| !value.is_null()) {
            let detail = error
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| error.to_string());
            return Err(Error::Operation(format!(
                "extension '{method}' failed: {detail}"
            )));
        }
        envelope
            .get("result")
            .cloned()
            .filter(|value| !value.is_null())
            .ok_or_else(|| Error::Operation(format!("extension '{method}' returned no result")))
    }
    /// Creates or updates a recurring task in an explicit session.
    pub async fn upsert_scheduled_task(
        &self,
        id: &SessionId,
        request: &ScheduledTaskRequest,
    ) -> Result<ScheduledTaskReceipt, Error> {
        let mut params =
            serde_json::to_value(request).map_err(|e| Error::Operation(e.to_string()))?;
        params["sessionId"] = serde_json::Value::String(id.as_str().to_owned());
        serde_json::from_value(self.typed_ext("x.ai/scheduler/create", params).await?)
            .map_err(|e| Error::Operation(format!("invalid scheduler create response: {e}")))
    }
    /// Lists tasks owned by an explicit session.
    pub async fn list_scheduled_tasks(
        &self,
        id: &SessionId,
    ) -> Result<Vec<ScheduledTaskSummary>, Error> {
        let value = self
            .typed_ext(
                "x.ai/scheduler/list",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await?;
        serde_json::from_value(value.get("tasks").cloned().unwrap_or_default())
            .map_err(|e| Error::Operation(format!("invalid scheduler list response: {e}")))
    }
    pub async fn delete_scheduled_task(
        &self,
        id: &SessionId,
        task_id: &str,
    ) -> Result<DeleteScheduledTaskReceipt, Error> {
        serde_json::from_value(
            self.typed_ext(
                "x.ai/scheduler/delete",
                serde_json::json!({"sessionId": id.as_str(), "taskId": task_id}),
            )
            .await?,
        )
        .map_err(|e| Error::Operation(format!("invalid scheduler delete response: {e}")))
    }
    /// Discovers built-ins, skills (including `implement`) and workflows
    /// available to this live session.
    pub async fn list_agent_commands(&self, id: &SessionId) -> Result<Vec<AgentCommand>, Error> {
        let value = self
            .raw_ext(
                "x.ai/commands/list",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await?;
        let commands = value["commands"]
            .as_array()
            .ok_or_else(|| Error::Operation("invalid commands/list response".into()))?;
        Ok(commands
            .iter()
            .filter_map(|command| {
                Some(AgentCommand {
                    name: command["name"].as_str()?.to_owned(),
                    description: command["description"].as_str()?.to_owned(),
                    input_hint: command["input"]["hint"].as_str().map(str::to_owned),
                    meta: command.get("_meta").cloned(),
                })
            })
            .collect())
    }
    /// Executes a discovered built-in, skill or workflow command as a normal
    /// agent turn. The command is allowlisted against the live catalog first.
    /// Use [`Runtime::upsert_scheduled_task`] for `/loop` when no model-based
    /// interpretation of the schedule is desired.
    pub async fn execute_agent_command(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        name: &str,
        arguments: Option<&str>,
    ) -> Result<PromptReceipt, Error> {
        if name.is_empty()
            || name.starts_with('/')
            || name.chars().any(|ch| ch.is_whitespace() || ch.is_control())
        {
            return Err(Error::InvalidConfig(
                "agent command name must be non-empty and contain no slash prefix or whitespace"
                    .into(),
            ));
        }
        let commands = self.list_agent_commands(id).await?;
        if !commands.iter().any(|command| command.name == name) {
            return Err(Error::Operation(format!(
                "agent command '{name}' is not available in this session"
            )));
        }
        let prompt = match arguments.filter(|value| !value.is_empty()) {
            Some(arguments) => format!("/{name} {arguments}"),
            None => format!("/{name}"),
        };
        self.prompt(id, turn_id, prompt).await
    }
    /// Queues a steering/follow-up instruction into the currently running turn.
    pub async fn interject(
        &self,
        id: &SessionId,
        text: impl Into<String>,
        interjection_id: Option<String>,
    ) -> Result<InterjectionReceipt, Error> {
        let status = self
            .typed_ext(
                "x.ai/interject",
                serde_json::json!({
                    "sessionId": id.as_str(),
                    "text": text.into(),
                    "interjectionId": interjection_id
                }),
            )
            .await?;
        Ok(InterjectionReceipt {
            interjection_id,
            status: status["status"].as_str().unwrap_or("unknown").to_owned(),
        })
    }
    /// Copies a persisted conversation branch. The returned child is not
    /// resident until the host calls [`Runtime::load_session`].
    pub async fn fork_session(
        &self,
        source: &SessionId,
        request: &ForkSessionRequest,
    ) -> Result<ForkSessionReceipt, Error> {
        self.fork_session_with_publication(
            source,
            request,
            crate::session::ForkSessionPublication::Create,
        )
        .await
    }
    /// Creates a caller-selected persisted conversation branch, or returns the
    /// exact prior publication when the same request committed before its
    /// response was lost. Any difference in source snapshot, cut, or target
    /// configuration fails closed. The returned child is not resident until
    /// the host calls [`Runtime::load_session`].
    pub async fn fork_session_create_or_verify(
        &self,
        source: &SessionId,
        request: &ForkSessionRequest,
    ) -> Result<ForkSessionReceipt, Error> {
        if request.new_session_id.is_none() {
            return Err(Error::InvalidConfig(
                "create-or-verify fork requires a caller-selected target session id".into(),
            ));
        }
        self.fork_session_with_publication(
            source,
            request,
            crate::session::ForkSessionPublication::CreateOrVerify,
        )
        .await
    }
    async fn fork_session_with_publication(
        &self,
        source: &SessionId,
        request: &ForkSessionRequest,
        publication: crate::session::ForkSessionPublication,
    ) -> Result<ForkSessionReceipt, Error> {
        let target = request
            .new_session_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        let extension = ExtensionRequest {
            method: "x.ai/session/fork".into(),
            params: serde_json::json!({
            "sourceSessionId": source.as_str(),
            "sourceCwd": request.source_cwd,
            "newCwd": request.new_cwd,
            "newSessionId": target,
            "newModelId": request.new_model_id,
            "targetPromptIndex": request.target_prompt_index,
            "sessionKind": request.session_kind,
            "sourceWorkspaceDir": request.source_workspace_dir,
            "createOrVerify": publication == crate::session::ForkSessionPublication::CreateOrVerify
            }),
        };
        let response = match publication {
            crate::session::ForkSessionPublication::Create => {
                self.inner
                    .fork_session(source.clone(), SessionId::from_stored(target), extension)
                    .await?
            }
            crate::session::ForkSessionPublication::CreateOrVerify => {
                self.inner
                    .fork_session_create_or_verify(
                        source.clone(),
                        SessionId::from_stored(target),
                        extension,
                    )
                    .await?
            }
        };
        let value = response.result;
        serde_json::from_value(value)
            .map_err(|e| Error::Operation(format!("invalid session/fork response: {e}")))
    }
    /// Resumes a persisted Session in a new worktree. Host-authority mode
    /// fences both the source and the selected target for the complete copy.
    pub async fn resume_session_in_worktree(
        &self,
        source: &SessionId,
        request: &ResumeSessionInWorktreeRequest,
    ) -> Result<ResumeSessionInWorktreeReceipt, Error> {
        let target = SessionId::from_stored(uuid::Uuid::now_v7().to_string());
        let response = self
            .inner
            .fork_session(
                source.clone(),
                target.clone(),
                ExtensionRequest {
                    method: "x.ai/git/worktree/resume_session".into(),
                    params: serde_json::json!({
                        "sessionId": source.as_str(),
                        "sourceCwd": request.source_cwd,
                        "copyMode": request.copy_mode,
                        "worktreeType": request.worktree_type,
                        "restoreCode": request.restore_code,
                        "gitRef": request.git_ref,
                        "newSessionId": target.as_str(),
                    }),
                },
            )
            .await?
            .result;
        if let Some(error) = response.get("error").filter(|error| !error.is_null()) {
            return Err(Error::Operation(format!("worktree resume failed: {error}")));
        }
        let value = response.get("result").cloned().unwrap_or(response);
        serde_json::from_value(value)
            .map_err(|error| Error::Operation(format!("invalid worktree resume response: {error}")))
    }
    /// Lists launchable workflows for this live session.
    pub async fn list_workflows(&self, id: &SessionId) -> Result<Vec<WorkflowInfo>, Error> {
        let value = self
            .typed_ext(
                "x.ai/workflows/list",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await?;
        Ok(value["workflows"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|workflow| {
                Some(WorkflowInfo {
                    name: workflow["name"].as_str()?.to_owned(),
                    description: workflow["description"].as_str()?.to_owned(),
                    when_to_use: workflow["whenToUse"].as_str().map(str::to_owned),
                    source: workflow["source"].as_str().unwrap_or("unknown").to_owned(),
                    path: workflow["path"].as_str().map(PathBuf::from),
                    raw: workflow.clone(),
                })
            })
            .collect())
    }
    /// Lists subagents currently running under this root session.
    pub async fn list_running_subagents(
        &self,
        id: &SessionId,
    ) -> Result<Vec<SubagentSnapshot>, Error> {
        let value = self
            .typed_ext(
                "x.ai/subagent/list_running",
                serde_json::json!({"sessionId": id.as_str()}),
            )
            .await?;
        value["subagents"]
            .as_array()
            .into_iter()
            .flatten()
            .cloned()
            .map(parse_subagent_snapshot)
            .collect()
    }
    /// Gets a live or terminal subagent snapshot, optionally waiting for it to finish.
    pub async fn get_subagent(
        &self,
        subagent_id: &str,
        block: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Option<SubagentSnapshot>, Error> {
        let value = self
            .typed_ext(
                "x.ai/subagent/get",
                serde_json::json!({
                    "subagentId": subagent_id,
                    "block": block,
                    "timeoutMs": timeout_ms
                }),
            )
            .await?;
        value
            .get("snapshot")
            .filter(|snapshot| !snapshot.is_null())
            .cloned()
            .map(parse_subagent_snapshot)
            .transpose()
    }
    /// Cancels a subagent by its globally unique subagent id.
    pub async fn cancel_subagent(&self, subagent_id: &str) -> Result<SubagentCancelReceipt, Error> {
        serde_json::from_value(
            self.typed_ext(
                "x.ai/subagent/cancel",
                serde_json::json!({"subagentId": subagent_id}),
            )
            .await?,
        )
        .map_err(|e| Error::Operation(format!("invalid subagent/cancel response: {e}")))
    }
}
