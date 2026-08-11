use super::*;

impl Core {
    pub(super) async fn prompt(
        &self,
        id: SessionId,
        t: String,
        x: String,
        source: InputSource,
    ) -> Result<PromptReceipt, Error> {
        let digest = crate::prompt_digest(&x);
        self.prompt_wire(
            id,
            t,
            vec![acp::ContentBlock::Text(acp::TextContent::new(x))],
            digest,
            serde_json::Value::Null,
            source,
            None,
        )
        .await
        .map(|(receipt, _)| receipt)
    }
    pub(super) async fn prompt_content(
        &self,
        id: SessionId,
        t: String,
        prompt: Prompt,
        source: InputSource,
    ) -> Result<PromptReceipt, Error> {
        if prompt.blocks.is_empty() {
            return Err(Error::InvalidConfig(
                "prompt blocks must not be empty".into(),
            ));
        }
        let mut blocks = Vec::new();
        for block in &prompt.blocks {
            let value = prompt_block_wire(block)?;
            blocks.push(serde_json::from_value(value).map_err(op)?);
        }
        let digest = crate::prompt_digest_content(&prompt)?;
        self.prompt_wire(id, t, blocks, digest, prompt.metadata, source, None)
            .await
            .map(|(receipt, _)| receipt)
    }
    pub(super) async fn prompt_content_with_harness(
        &self,
        id: SessionId,
        turn_id: String,
        prompt: Prompt,
        prepared: PreparedHarnessTurn,
    ) -> Result<TurnBindingReceipt, Error> {
        if prompt.blocks.is_empty() {
            return Err(Error::InvalidConfig(
                "prompt blocks must not be empty".into(),
            ));
        }
        let mut blocks = Vec::new();
        for block in &prompt.blocks {
            let value = prompt_block_wire(block)?;
            blocks.push(serde_json::from_value(value).map_err(op)?);
        }
        let prompt_digest = crate::prompt_digest_content(&prompt)?;
        if prompt_digest != prepared.prompt_digest {
            return Err(Error::Operation(
                "prepared harness Turn prompt identity changed before dispatch".into(),
            ));
        }
        let (_, record) = self
            .prompt_wire(
                id,
                turn_id,
                blocks,
                prompt_digest,
                prompt.metadata,
                InputSource::User,
                Some(prepared),
            )
            .await?;
        record
            .map(TurnBindingRecord::into_receipt)
            .ok_or_else(|| Error::Operation("durable Turn binding record was not issued".into()))
    }

    pub(super) fn prepare_harness_turn(
        &self,
        id: &SessionId,
        prompt: &Prompt,
        requested_digest: &HarnessDigest,
    ) -> Result<PreparedHarnessTurn, Error> {
        self.require_resident(id)?;
        let binding = self
            .session_bindings
            .borrow()
            .get(&id.0)
            .cloned()
            .ok_or_else(|| Error::Operation("session binding is unavailable".into()))?;
        let bound_digest = binding
            .harness_digest
            .ok_or_else(|| Error::Harness(HarnessError::UnboundSession))?;
        if &bound_digest != requested_digest {
            return Err(Error::Harness(HarnessError::BindingMismatch {
                bound: bound_digest,
                requested: requested_digest.clone(),
            }));
        }
        let after_sequence = self
            .sequences
            .borrow()
            .get(&id.0)
            .copied()
            .unwrap_or_default();
        let prompt_digest = crate::prompt_digest_content(prompt)?;
        Ok(PreparedHarnessTurn {
            prompt_digest,
            snapshot_digest: bound_digest,
            model: binding.model,
            reasoning: binding.reasoning,
            after_sequence,
        })
    }
    pub(super) async fn prompt_wire(
        &self,
        id: SessionId,
        t: String,
        blocks: Vec<acp::ContentBlock>,
        prompt_digest: String,
        metadata: serde_json::Value,
        source: InputSource,
        prepared: Option<PreparedHarnessTurn>,
    ) -> Result<(PromptReceipt, Option<TurnBindingRecord>), Error> {
        self.require_resident(&id)?;
        let mut ledger = self.load_ledger(&id)?;
        if ledger
            .entries
            .iter()
            .any(|entry| matches!(entry.state, LedgerTurnState::Pending))
        {
            return Err(Error::Operation(
                "session has an unreconciled native Turn".into(),
            ));
        }
        if ledger.entries.iter().any(|entry| entry.turn_id == t) {
            return Err(Error::Operation("native Turn id was already used".into()));
        }
        let runtime_prompt_index = ledger
            .entries
            .iter()
            .filter(|entry| !matches!(entry.state, LedgerTurnState::Discarded))
            .count() as u64;
        ledger.entries.push(SessionLedgerEntry {
            turn_id: t.clone(),
            prompt_digest: prompt_digest.clone(),
            runtime_prompt_index,
            state: LedgerTurnState::Pending,
            source: source.clone(),
        });
        self.save_ledger(&id, &ledger)?;
        let usage_key = (id.0.clone(), t.clone());
        self.turn_usages.borrow_mut().remove(&usage_key);
        let req = acp::PromptRequest::new(acp::SessionId::new(id.0.clone()), blocks).meta(
            serde_json::json!({
                "originTurnId":t,
                "promptId":t,
                "originPromptDigest": prompt_digest,
                "originInputSource": serde_json::to_value(&source).map_err(op)?,
                "originMetadata": metadata
            })
            .as_object()
            .cloned(),
        );
        let started = std::time::Instant::now();
        let response = self
            .agent
            .prompt(req)
            .await
            .map_err(|error| protocol("session/prompt", error));
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.turn_usages.borrow_mut().remove(&usage_key);
                return Err(error);
            }
        };
        let outcome = match response.stop_reason {
            acp::StopReason::EndTurn => TurnOutcome::End,
            acp::StopReason::Cancelled => TurnOutcome::Cancelled,
            acp::StopReason::MaxTokens => TurnOutcome::MaxTokens,
            acp::StopReason::MaxTurnRequests => TurnOutcome::MaxTurnRequests,
            acp::StopReason::Refusal => TurnOutcome::Refusal,
            _ => {
                self.turn_usages.borrow_mut().remove(&usage_key);
                return Err(Error::Operation("unrecognized Grok stop reason".into()));
            }
        };
        let raw = serde_json::value::RawValue::from_string(
            serde_json::json!({"sessionId": id.0}).to_string(),
        )
        .map_err(op)?;
        if let Err(error) = self
            .agent
            .ext_method(acp::ExtRequest::new("origin/session/sync", Arc::from(raw)))
            .await
            .map_err(|error| protocol("origin/session/sync", error))
        {
            self.turn_usages.borrow_mut().remove(&usage_key);
            return Err(error);
        }
        let wall_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let native_usage = match self.turn_usages.borrow_mut().remove(&usage_key) {
            Some(CapturedTurnUsage::Exact(usage)) => usage,
            Some(CapturedTurnUsage::Conflict) | None => None,
        };
        let usage = prompt_effect_usage(native_usage.as_ref(), wall_ms);
        let settlement_id = ledger_settlement_id(
            &id.0,
            &t,
            &prompt_digest,
            runtime_prompt_index,
            outcome,
            &usage,
        )?;
        let terminal = self.retain_event(&id, EventUpdate::TurnFinished(outcome), Some(t.clone()));
        let receipt = PromptReceipt {
            outcome,
            final_sequence: terminal.sequence,
            runtime_prompt_index,
            settlement_id,
            usage,
        };
        let Some(prepared) = prepared else {
            settle_latest_ledger_entry(&mut ledger, &receipt);
            self.save_ledger(&id, &ledger)?;
            self.publish_event(terminal);
            return Ok((receipt, None));
        };

        let record = self
            .events_after(&id, prepared.after_sequence)
            .and_then(|events| {
                let binding = TurnBindingReceipt::complete(
                    id.clone(),
                    t,
                    prepared.prompt_digest,
                    prepared.snapshot_digest,
                    prepared.model,
                    prepared.reasoning,
                    prepared.after_sequence,
                    receipt.clone(),
                    &events,
                )
                .map_err(Error::Harness)?;
                TurnBindingRecord::complete(binding, &events).map_err(Error::Harness)
            });
        let record = match record {
            Ok(record) => record,
            Err(error) => {
                settle_latest_ledger_entry(&mut ledger, &receipt);
                self.save_ledger(&id, &ledger)?;
                self.publish_event(terminal);
                return Err(error);
            }
        };
        if let Err(error) = self.save_turn_binding_record(&record) {
            self.publish_event(terminal);
            return Err(error);
        }
        settle_latest_ledger_entry(&mut ledger, &receipt);
        self.save_ledger(&id, &ledger)?;
        self.publish_event(terminal);
        Ok((receipt, Some(record)))
    }
}
