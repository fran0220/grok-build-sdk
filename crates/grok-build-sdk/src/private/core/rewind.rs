use super::*;

impl Core {
    pub(super) async fn rewind_points(&self, id: SessionId) -> Result<Vec<RewindPoint>, Error> {
        self.require_resident(&id)?;
        let ledger = self.load_ledger(&id)?;
        let response: RewindPointsWire = self
            .extension(
                "x.ai/rewind/points",
                serde_json::json!({ "sessionId": id.0 }),
            )
            .await?;
        Ok(response
            .rewind_points
            .into_iter()
            .map(|point| {
                let prompt_digest = ledger
                    .entries
                    .iter()
                    .rev()
                    .find(|entry| {
                        entry.runtime_prompt_index == point.prompt_index
                            && !matches!(entry.state, LedgerTurnState::Discarded)
                    })
                    .map(|entry| entry.prompt_digest.clone())
                    .or(point.origin_prompt_digest);
                RewindPoint {
                    prompt_index: point.prompt_index,
                    prompt_digest,
                    created_at: point.created_at,
                    file_snapshots: point.num_file_snapshots,
                    has_file_changes: point.has_file_changes,
                    prompt_preview: point.prompt_preview,
                }
            })
            .collect())
    }
    pub(super) async fn rewind_conversation(
        &self,
        id: SessionId,
        operation_id: String,
        target_prompt_index: u64,
    ) -> Result<ConversationRewindReceipt, Error> {
        self.rewind_conversation_entry(id, operation_id, target_prompt_index, None)
            .await
    }

    pub(super) async fn rewind_conversation_entry(
        &self,
        id: SessionId,
        operation_id: String,
        target_prompt_index: u64,
        unsettled_identity: Option<(String, String)>,
    ) -> Result<ConversationRewindReceipt, Error> {
        if operation_id.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "rewind operation id is required".into(),
            ));
        }
        self.require_resident(&id)?;
        if self.turns.borrow().contains_key(&id.0) {
            return Err(Error::Operation(
                "cannot rewind a conversation while the session is active".into(),
            ));
        }
        let (recovery_turn_id, recovery_prompt_digest) = unsettled_identity
            .as_ref()
            .map(|(turn_id, prompt_digest)| (Some(turn_id.as_str()), Some(prompt_digest.as_str())))
            .unwrap_or((None, None));
        let pending_intent = match self.rewind_status(&id, &operation_id)? {
            ConversationRewindStatus::Applied { receipt } => {
                if receipt.session_id == id.0
                    && receipt.target_prompt_index == target_prompt_index
                    && receipt.recovery_turn_id.as_deref() == recovery_turn_id
                    && receipt.recovery_prompt_digest.as_deref() == recovery_prompt_digest
                {
                    return Ok(receipt);
                }
                return Err(Error::Operation(
                    "rewind operation id is already bound to another request identity".into(),
                ));
            }
            ConversationRewindStatus::Pending {
                operation_id,
                session_id,
                target_prompt_index: pending_target,
                target_turn_id,
                target_prompt_digest,
                recovery_turn_id: pending_recovery_turn,
                recovery_prompt_digest: pending_recovery_digest,
            } => {
                let existing = RewindIntent {
                    operation_id,
                    session_id,
                    target_prompt_index: pending_target,
                    target_turn_id,
                    target_prompt_digest,
                    recovery_turn_id: pending_recovery_turn,
                    recovery_prompt_digest: pending_recovery_digest,
                };
                if existing.session_id != id.0
                    || existing.target_prompt_index != target_prompt_index
                    || existing.recovery_turn_id.as_deref() != recovery_turn_id
                    || existing.recovery_prompt_digest.as_deref() != recovery_prompt_digest
                {
                    return Err(Error::Operation(
                        "rewind operation id is already bound to another pending request identity"
                            .into(),
                    ));
                }
                Some(existing)
            }
            ConversationRewindStatus::Absent => None,
        };
        let mut ledger = self.load_ledger(&id)?;
        if unsettled_identity.is_none()
            && ledger
                .entries
                .iter()
                .any(|entry| matches!(entry.state, LedgerTurnState::Pending))
        {
            return Err(Error::Operation(
                "cannot perform a user rewind with an unsettled native Turn".into(),
            ));
        }
        let target_position = ledger
            .entries
            .iter()
            .position(|entry| {
                entry.runtime_prompt_index == target_prompt_index
                    && match (&pending_intent, &unsettled_identity) {
                        (Some(intent), _) => {
                            matches!(
                                entry.state,
                                LedgerTurnState::Pending
                                    | LedgerTurnState::Completed { .. }
                                    | LedgerTurnState::Discarded
                            ) && entry.turn_id == intent.target_turn_id
                                && entry.prompt_digest == intent.target_prompt_digest
                        }
                        (None, None) => {
                            matches!(entry.state, LedgerTurnState::Completed { .. })
                        }
                        (None, Some((turn_id, prompt_digest))) => {
                            (matches!(
                                entry.state,
                                LedgerTurnState::Pending | LedgerTurnState::Completed { .. }
                            )) && entry.turn_id == *turn_id
                                && entry.prompt_digest == *prompt_digest
                        }
                    }
            })
            .ok_or_else(|| {
                Error::Operation(if unsettled_identity.is_some() {
                    "recovery rewind target does not match the pending native Turn".into()
                } else {
                    "rewind target is not a settled entry in the native Turn ledger".into()
                })
            })?;
        if unsettled_identity.is_some()
            && (ledger.entries[..target_position]
                .iter()
                .any(|entry| matches!(entry.state, LedgerTurnState::Pending))
                || ledger.entries[target_position + 1..]
                    .iter()
                    .any(|entry| !matches!(entry.state, LedgerTurnState::Discarded)))
        {
            return Err(Error::Operation(
                "recovery rewind is restricted to the exact unsettled history tail".into(),
            ));
        }
        let target_entry = &ledger.entries[target_position];
        let requested_intent = RewindIntent {
            operation_id: operation_id.clone(),
            session_id: id.0.clone(),
            target_prompt_index,
            target_turn_id: target_entry.turn_id.clone(),
            target_prompt_digest: target_entry.prompt_digest.clone(),
            recovery_turn_id: unsettled_identity
                .as_ref()
                .map(|(turn_id, _)| turn_id.clone()),
            recovery_prompt_digest: unsettled_identity
                .as_ref()
                .map(|(_, prompt_digest)| prompt_digest.clone()),
        };
        if pending_intent
            .as_ref()
            .is_some_and(|existing| existing != &requested_intent)
        {
            return Err(Error::Operation(
                "pending rewind identity differs from the durable Turn ledger".into(),
            ));
        }
        let expected_prompt_digest = target_entry.prompt_digest.clone();
        if pending_intent.is_none() {
            self.save_rewind_intent(&requested_intent)?;
        }

        let native_points: RewindPointsWire = self
            .extension(
                "x.ai/rewind/points",
                serde_json::json!({ "sessionId": id.0 }),
            )
            .await?;
        if !native_rewind_already_applied(
            &native_points.rewind_points,
            target_prompt_index,
            &ledger,
        )? {
            let target_prompt_index_wire = usize::try_from(target_prompt_index)
                .map_err(|_| Error::InvalidConfig("rewind target is out of range".into()))?;
            let response: RewindResultWire = self
                .extension(
                    "x.ai/rewind/execute",
                    serde_json::json!({
                        "sessionId": id.0,
                        "targetPromptIndex": target_prompt_index_wire,
                        "force": true,
                        "mode": "conversation_only",
                    }),
                )
                .await?;
            if !response.success
                || response.target_prompt_index != target_prompt_index
                || response.mode != "conversation_only"
                || !response.reverted_files.is_empty()
                || !response.clean_files.is_empty()
                || !response.conflicts.is_empty()
            {
                return Err(Error::Operation(response.error.unwrap_or_else(|| {
                    if let Some(conflict) = response.conflicts.first() {
                        format!(
                            "native conversation rewind reported {} conflict at {}",
                            conflict.conflict_type, conflict.path
                        )
                    } else if response.mode != "conversation_only" {
                        format!(
                            "native conversation rewind returned unexpected mode {}",
                            response.mode
                        )
                    } else {
                        "native conversation rewind failed or attempted a file mutation".into()
                    }
                })));
            }
            let prompt_text = response.prompt_text.as_deref().ok_or_else(|| {
                Error::Operation("native conversation rewind omitted its target prompt".into())
            })?;
            if !expected_prompt_digest.starts_with("sha256-v2:")
                && crate::prompt_digest(prompt_text) != expected_prompt_digest
            {
                return Err(Error::Operation(
                    "native conversation rewind target differs from the durable Turn ledger".into(),
                ));
            }
        }
        self.extension::<serde_json::Value>(
            "origin/session/sync",
            serde_json::json!({ "sessionId": id.0 }),
        )
        .await?;
        for entry in &mut ledger.entries {
            if entry.runtime_prompt_index >= target_prompt_index {
                entry.state = LedgerTurnState::Discarded;
            }
        }
        self.save_ledger(&id, &ledger)?;
        let receipt = ConversationRewindReceipt {
            operation_id,
            session_id: id.0,
            target_prompt_index,
            target_turn_id: requested_intent.target_turn_id,
            target_prompt_digest: requested_intent.target_prompt_digest,
            recovery_turn_id: requested_intent.recovery_turn_id,
            recovery_prompt_digest: requested_intent.recovery_prompt_digest,
        };
        self.save_rewind_receipt(&receipt)?;
        Ok(receipt)
    }

    pub(super) fn rewind_key(operation_id: &str) -> SessionEvidenceKey {
        Self::evidence_key(SessionEvidenceKind::Rewind, operation_id.to_owned())
    }

    pub(super) fn save_rewind_intent(&self, intent: &RewindIntent) -> Result<(), Error> {
        self.commit_evidence(
            &Self::rewind_key(&intent.operation_id),
            &serde_json::to_vec(&RewindEvidence::Intent(intent.clone())).map_err(op)?,
        )
    }

    pub(super) fn rewind_status(
        &self,
        id: &SessionId,
        operation_id: &str,
    ) -> Result<ConversationRewindStatus, Error> {
        match self.load_evidence(&Self::rewind_key(operation_id), 1024 * 1024)? {
            Some(bytes) => match serde_json::from_slice::<RewindEvidence>(&bytes).map_err(op)? {
                RewindEvidence::Receipt(receipt) => {
                    if receipt.operation_id != operation_id {
                        return Err(Error::Operation("rewind receipt digest mismatch".into()));
                    }
                    if receipt.session_id != id.0 {
                        return Err(Error::Operation(
                            "rewind receipt belongs to a different native session".into(),
                        ));
                    }
                    Ok(ConversationRewindStatus::Applied { receipt })
                }
                RewindEvidence::Intent(intent) => {
                    if intent.operation_id != operation_id || intent.session_id != id.0 {
                        return Err(Error::Operation(
                            "rewind intent identity does not match its evidence key".into(),
                        ));
                    }
                    let valid_identity =
                        |value: &str, max: usize| !value.trim().is_empty() && value.len() <= max;
                    if !valid_identity(&intent.operation_id, 512)
                        || !valid_identity(&intent.session_id, 512)
                        || !valid_identity(&intent.target_turn_id, 512)
                        || !valid_identity(&intent.target_prompt_digest, 160)
                        || match (
                            intent.recovery_turn_id.as_deref(),
                            intent.recovery_prompt_digest.as_deref(),
                        ) {
                            (None, None) => false,
                            (Some(turn), Some(digest)) => {
                                !valid_identity(turn, 512) || !valid_identity(digest, 160)
                            }
                            _ => true,
                        }
                    {
                        return Err(Error::Operation(
                            "rewind intent contains invalid bounded identities".into(),
                        ));
                    }
                    Ok(ConversationRewindStatus::Pending {
                        operation_id: intent.operation_id,
                        session_id: intent.session_id,
                        target_prompt_index: intent.target_prompt_index,
                        target_turn_id: intent.target_turn_id,
                        target_prompt_digest: intent.target_prompt_digest,
                        recovery_turn_id: intent.recovery_turn_id,
                        recovery_prompt_digest: intent.recovery_prompt_digest,
                    })
                }
            },
            None => Ok(ConversationRewindStatus::Absent),
        }
    }

    pub(super) fn save_rewind_receipt(
        &self,
        receipt: &ConversationRewindReceipt,
    ) -> Result<(), Error> {
        self.commit_evidence(
            &Self::rewind_key(&receipt.operation_id),
            &serde_json::to_vec(&RewindEvidence::Receipt(receipt.clone())).map_err(op)?,
        )
    }
}
