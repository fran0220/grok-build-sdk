// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use crate::*;

impl Runtime {
    pub async fn prompt_content(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        prompt: Prompt,
    ) -> Result<PromptReceipt, Error> {
        self.inner.prompt_content(id, turn_id.into(), prompt).await
    }
    pub async fn prompt_blocks(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        blocks: Vec<PromptBlock>,
    ) -> Result<PromptReceipt, Error> {
        self.prompt_content(
            id,
            turn_id,
            Prompt {
                blocks,
                metadata: serde_json::Value::Null,
            },
        )
        .await
    }
    pub async fn set_mode(&self, id: &SessionId, mode: impl Into<String>) -> Result<(), Error> {
        self.inner.set_mode(id, mode.into()).await
    }
    pub async fn list_sessions(&self) -> Result<serde_json::Value, Error> {
        self.inner.list_sessions().await
    }
    pub async fn close_session(&self, id: SessionId) -> Result<(), Error> {
        self.inner.close_session(id).await
    }
    /// Permanently deletes a Session after stopping any live actor. With an
    /// injected [`SessionStateStore`], the exact authoritative manifest is
    /// tombstoned before uncovered local sidecars are removed; retries of an
    /// already completed deletion succeed, and the identity cannot be reused.
    pub async fn delete_session(&self, id: SessionId) -> Result<(), Error> {
        self.inner.delete_session(id).await
    }
    pub async fn create_session(&self, config: SessionConfig) -> Result<SessionId, Error> {
        self.inner.create_session(config).await
    }
    /// Creates a Session that carries its own capability layer. The layer
    /// masks the application-owned general layer by kind and name for this
    /// Session only; every other Session on this Runtime is unaffected and no
    /// restart or second Runtime is involved.
    pub async fn create_session_with_capabilities(
        &self,
        config: SessionConfig,
        capabilities: CapabilityLayer,
    ) -> Result<SessionId, Error> {
        self.inner
            .create_session_with_capabilities(config, None, capabilities)
            .await
    }
    /// Combines an immutable harness snapshot with a Session capability layer.
    pub async fn create_session_with_harness_and_capabilities(
        &self,
        config: SessionConfig,
        snapshot: &HarnessSnapshot,
        capabilities: CapabilityLayer,
    ) -> Result<SessionId, Error> {
        let materialized = snapshot.materialize()?;
        self.inner
            .create_session_with_capabilities(
                materialized.apply_to_session(config),
                Some(snapshot.digest().clone()),
                capabilities,
            )
            .await
    }
    /// Loads and replays a durable Session under its own capability layer.
    pub async fn load_session_with_capabilities(
        &self,
        id: SessionId,
        config: SessionConfig,
        capabilities: CapabilityLayer,
    ) -> Result<(), Error> {
        self.inner
            .load_session_with_capabilities(id, config, None, capabilities)
            .await
    }
    /// Resumes without replay under its own capability layer.
    pub async fn resume_session_with_capabilities(
        &self,
        id: SessionId,
        config: SessionConfig,
        capabilities: CapabilityLayer,
    ) -> Result<(), Error> {
        self.inner
            .resume_session_with_capabilities(id, config, None, capabilities)
            .await
    }
    /// Replaces a resident Session's capability layer. The new layer is
    /// resolved against the general layer and bound in place, so the next Turn
    /// on this Session — and only this Session — observes it. Rejected during
    /// an active prompt.
    pub async fn set_session_capabilities(
        &self,
        id: &SessionId,
        capabilities: CapabilityLayer,
    ) -> Result<CapabilityResolution, Error> {
        self.inner.set_session_capabilities(id, capabilities).await
    }
    /// Reads the effective capability names bound to a resident Session.
    pub async fn session_capabilities(
        &self,
        id: &SessionId,
    ) -> Result<CapabilityResolution, Error> {
        self.inner.session_capabilities(id).await
    }
    /// Creates the exact caller-selected Session identity, or idempotently
    /// reopens it when the complete configuration matches. This current-only
    /// contract requires a Host [`SessionStateStore`]; a different config or a
    /// tombstoned identity fails closed.
    pub async fn create_session_with_id(
        &self,
        id: SessionId,
        config: SessionConfig,
    ) -> Result<SessionId, Error> {
        self.inner.create_session_with_id(id, config).await
    }
    /// Creates a native Session whose complete harness inputs are materialized
    /// from one immutable snapshot. The Runtime retains only the digest needed
    /// to issue Turn binding receipts; revision and activation remain Host state.
    pub async fn create_session_with_harness(
        &self,
        config: SessionConfig,
        snapshot: &HarnessSnapshot,
    ) -> Result<SessionId, Error> {
        let materialized = snapshot.materialize()?;
        self.inner
            .create_session_with_harness(
                materialized.apply_to_session(config),
                snapshot.digest().clone(),
            )
            .await
    }
    pub async fn load_session(&self, id: SessionId, config: SessionConfig) -> Result<(), Error> {
        self.inner.load_session(id, config).await
    }
    /// Loads and replays a durable Session with the Host-selected immutable
    /// snapshot materialized for this incarnation.
    pub async fn load_session_with_harness(
        &self,
        id: SessionId,
        config: SessionConfig,
        snapshot: &HarnessSnapshot,
    ) -> Result<(), Error> {
        let materialized = snapshot.materialize()?;
        self.inner
            .load_session_with_harness(
                id,
                materialized.apply_to_session(config),
                snapshot.digest().clone(),
            )
            .await
    }
    /// Resumes a durable session without replaying its historical updates.
    /// Use [`Self::load_session`] when the host needs history replay to rebuild
    /// a fresh event journal.
    pub async fn resume_session(&self, id: SessionId, config: SessionConfig) -> Result<(), Error> {
        self.inner.resume_session(id, config).await
    }
    /// Resumes without replay and binds this Session incarnation to the
    /// Host-selected immutable snapshot.
    pub async fn resume_session_with_harness(
        &self,
        id: SessionId,
        config: SessionConfig,
        snapshot: &HarnessSnapshot,
    ) -> Result<(), Error> {
        let materialized = snapshot.materialize()?;
        self.inner
            .resume_session_with_harness(
                id,
                materialized.apply_to_session(config),
                snapshot.digest().clone(),
            )
            .await
    }
    pub async fn prompt(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<PromptReceipt, Error> {
        self.inner.prompt(id, turn_id.into(), text.into()).await
    }
    /// Executes a native text Turn and issues a receipt only after the complete
    /// live event range through the terminal event has been retained and
    /// verified. A journal gap fails closed after the settled Turn.
    pub async fn prompt_with_harness(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        text: impl Into<String>,
        snapshot: &HarnessSnapshot,
    ) -> Result<TurnBindingReceipt, Error> {
        self.prompt_content_with_harness(
            id,
            turn_id,
            Prompt {
                blocks: vec![PromptBlock::Text { text: text.into() }],
                metadata: serde_json::Value::Null,
            },
            snapshot,
        )
        .await
    }
    /// Rich-content equivalent of [`Self::prompt_with_harness`].
    pub async fn prompt_content_with_harness(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        prompt: Prompt,
        snapshot: &HarnessSnapshot,
    ) -> Result<TurnBindingReceipt, Error> {
        snapshot.validate()?;
        self.inner
            .prompt_content_with_harness(id, turn_id.into(), prompt, snapshot.digest().clone())
            .await
    }
    /// Recovers the immutable record for an exact harness-aware Turn. The
    /// Session must be reattached with the same snapshot and effective route;
    /// any conflicting ledger, snapshot, route, prompt, or index fails closed.
    pub async fn turn_binding_status(
        &self,
        id: &SessionId,
        key: TurnBindingKey,
    ) -> Result<TurnBindingStatus, Error> {
        self.inner.turn_binding_status(id, key).await
    }
    /// Returns retained events whose sequence is strictly greater than
    /// `after_sequence`. Unknown or currently unloaded sessions fail closed.
    pub async fn events_after(
        &self,
        id: &SessionId,
        after_sequence: u64,
    ) -> Result<Vec<Event>, Error> {
        self.inner.events_after(id, after_sequence).await
    }
    /// Atomically snapshots the resident binding, retained replay range and
    /// validated Session ledger. A truncated journal is returned successfully
    /// with its retained suffix; unknown or unloaded Sessions fail closed.
    pub async fn probe_session_replay(
        &self,
        id: &SessionId,
        after_sequence: u64,
    ) -> Result<SessionReplayProbe, Error> {
        self.inner.probe_session_replay(id, after_sequence).await
    }
    pub async fn cancel(&self, id: &SessionId) -> Result<(), Error> {
        self.inner.cancel(id).await
    }
    pub async fn session_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        self.inner.session_ledger(id).await
    }
    pub async fn mark_turn_discarded(
        &self,
        id: &SessionId,
        turn_id: impl Into<String>,
        prompt_digest: impl Into<String>,
        runtime_prompt_index: u64,
    ) -> Result<(), Error> {
        self.inner
            .mark_turn_discarded(
                id,
                turn_id.into(),
                prompt_digest.into(),
                runtime_prompt_index,
            )
            .await
    }
    /// Changes only the SDK sampling route for an existing conversation.
    /// This never rebuilds the harness or rewrites the system prompt.
    pub async fn set_route(
        &self,
        id: &SessionId,
        model: impl Into<String>,
        reasoning: Option<String>,
    ) -> Result<(), Error> {
        self.inner.set_route(id, model.into(), reasoning).await
    }
    pub async fn rewind_points(&self, id: &SessionId) -> Result<Vec<RewindPoint>, Error> {
        self.inner.rewind_points(id).await
    }
    pub async fn rewind_conversation(
        &self,
        id: &SessionId,
        operation_id: impl Into<String>,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.inner
            .rewind_conversation(id, operation_id.into(), target_prompt_index)
            .await
    }
    /// Removes the exact product-unsettled tail Turn after an SDK host
    /// restart. The native Turn may still be pending or may have completed
    /// before the host durably recorded its settlement; unlike a user rewind,
    /// this requires the full ledger identity.
    pub async fn rewind_unsettled_turn(
        &self,
        id: &SessionId,
        operation_id: impl Into<String>,
        turn_id: impl Into<String>,
        prompt_digest: impl Into<String>,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.inner
            .rewind_unsettled_turn(
                id,
                operation_id.into(),
                turn_id.into(),
                prompt_digest.into(),
                target_prompt_index,
            )
            .await
    }
    pub async fn rewind_status(
        &self,
        id: &SessionId,
        operation_id: &str,
    ) -> Result<ConversationRewindStatus, Error> {
        self.inner.rewind_status(id, operation_id).await
    }
    pub async fn unload_session(&self, id: SessionId) -> Result<(), Error> {
        self.inner.unload_session(id).await
    }
}
