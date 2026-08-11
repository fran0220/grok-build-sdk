use super::*;

impl Core {
    pub(super) fn session_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        self.require_resident(id)?;
        self.load_ledger(id)
    }

    pub(super) fn mark_turn_discarded(
        &self,
        id: &SessionId,
        turn_id: String,
        prompt_digest: String,
        runtime_prompt_index: u64,
    ) -> Result<(), Error> {
        self.require_resident(id)?;
        if self.turns.borrow().contains_key(&id.0) {
            return Err(Error::Operation(
                "cannot discard a Turn while the session is active".into(),
            ));
        }
        let mut ledger = self.load_ledger(id)?;
        if let Some(position) = ledger
            .entries
            .iter()
            .position(|entry| entry.turn_id == turn_id)
        {
            let entry = &ledger.entries[position];
            if entry.prompt_digest != prompt_digest
                || entry.runtime_prompt_index != runtime_prompt_index
            {
                return Err(Error::Operation(
                    "discarded Turn identity does not match the native ledger".into(),
                ));
            }
            if ledger.entries[position + 1..]
                .iter()
                .any(|entry| !matches!(entry.state, LedgerTurnState::Discarded))
            {
                return Err(Error::Operation(
                    "native Turn history can only be discarded from the end".into(),
                ));
            }
            ledger.entries[position].state = LedgerTurnState::Discarded;
        } else {
            let expected_index = ledger
                .entries
                .iter()
                .filter(|entry| !matches!(entry.state, LedgerTurnState::Discarded))
                .count() as u64;
            if runtime_prompt_index != expected_index {
                return Err(Error::Operation(
                    "discarded Turn index does not follow the native ledger".into(),
                ));
            }
            ledger.entries.push(SessionLedgerEntry {
                turn_id,
                prompt_digest,
                runtime_prompt_index,
                state: LedgerTurnState::Discarded,
                source: InputSource::User,
            });
        }
        self.save_ledger(id, &ledger)
    }

    pub(super) fn evidence_key(kind: SessionEvidenceKind, identity: String) -> SessionEvidenceKey {
        SessionEvidenceKey { kind, identity }
    }

    pub(super) fn load_evidence(
        &self,
        key: &SessionEvidenceKey,
        max: usize,
    ) -> Result<Option<Vec<u8>>, Error> {
        let document = self.evidence_store.load(key).map_err(op)?;
        if let Some(SessionEvidenceDocument { version, bytes }) = document {
            if bytes.len() > max {
                return Err(Error::Operation(
                    "session evidence exceeds its bounded schema size".into(),
                ));
            }
            if !version.validates(&bytes) {
                return Err(Error::Operation(
                    "session evidence CAS digest or revision is invalid".into(),
                ));
            }
            self.evidence_versions
                .borrow_mut()
                .insert(key.clone(), version);
            Ok(Some(bytes))
        } else {
            self.evidence_versions.borrow_mut().remove(key);
            Ok(None)
        }
    }

    pub(super) fn commit_evidence(
        &self,
        key: &SessionEvidenceKey,
        bytes: &[u8],
    ) -> Result<(), Error> {
        let expected = self.evidence_versions.borrow().get(key).cloned();
        let required = SessionEvidenceVersion::successor(expected.as_ref(), bytes).map_err(op)?;
        match self
            .evidence_store
            .compare_and_swap(key, expected.as_ref(), bytes)
            .map_err(op)?
        {
            SessionEvidenceCommit::Committed(version) if version == required => {
                self.evidence_versions
                    .borrow_mut()
                    .insert(key.clone(), version);
                Ok(())
            }
            SessionEvidenceCommit::Committed(_) => Err(Error::Operation(
                "session evidence store returned an invalid successor identity".into(),
            )),
            SessionEvidenceCommit::Conflict => Err(Error::Operation(
                "session evidence CAS conflict; reconciliation is required".into(),
            )),
            SessionEvidenceCommit::CommitUnknown => Err(Error::Operation(
                "session evidence commit acknowledgement is unknown; reconciliation is required"
                    .into(),
            )),
        }
    }

    pub(super) fn load_ledger(&self, id: &SessionId) -> Result<SessionLedger, Error> {
        let key = Self::evidence_key(SessionEvidenceKind::Ledger, id.0.clone());
        let bytes = self.load_evidence(&key, 8 * 1024 * 1024)?.ok_or_else(|| {
            Error::Operation("native Turn ledger is unavailable for session reconciliation".into())
        })?;
        let ledger: SessionLedger = serde_json::from_slice(&bytes).map_err(op)?;
        validate_session_ledger(id, &ledger)?;
        Ok(ledger)
    }

    pub(super) fn save_ledger(&self, id: &SessionId, ledger: &SessionLedger) -> Result<(), Error> {
        validate_session_ledger(id, ledger)?;
        let bytes = serde_json::to_vec(ledger).map_err(op)?;
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(Error::Operation(
                "native Turn ledger exceeds maximum size".into(),
            ));
        }
        self.commit_evidence(
            &Self::evidence_key(SessionEvidenceKind::Ledger, id.0.clone()),
            &bytes,
        )
    }

    pub(super) fn load_turn_binding_record(
        &self,
        id: &SessionId,
        turn_id: &str,
    ) -> Result<Option<TurnBindingRecord>, Error> {
        let key = Self::evidence_key(
            SessionEvidenceKind::TurnBinding,
            format!("{}\0{turn_id}", id.0),
        );
        self.load_evidence(&key, crate::MAX_TURN_BINDING_RECORD_BYTES)?
            .map(|bytes| TurnBindingRecord::from_json_slice(&bytes).map_err(Error::Harness))
            .transpose()
    }

    pub(super) fn save_turn_binding_record(&self, record: &TurnBindingRecord) -> Result<(), Error> {
        let receipt = record.receipt();
        let key = Self::evidence_key(
            SessionEvidenceKind::TurnBinding,
            format!("{}\0{}", receipt.session_id().0, receipt.turn_id()),
        );
        if let Some(existing) =
            self.load_turn_binding_record(receipt.session_id(), receipt.turn_id())?
        {
            return if existing == *record {
                Ok(())
            } else {
                Err(Error::Harness(HarnessError::BindingRecordConflict(
                    "an immutable record already exists for this Session and Turn ID".into(),
                )))
            };
        }
        let bytes = record.to_json_vec().map_err(Error::Harness)?;
        self.commit_evidence(&key, &bytes)
    }

    pub(super) fn turn_binding_status(
        &self,
        id: &SessionId,
        key: &TurnBindingKey,
    ) -> Result<TurnBindingStatus, Error> {
        self.require_resident(id)?;
        let binding = self
            .session_bindings
            .borrow()
            .get(id.as_str())
            .cloned()
            .ok_or_else(|| Error::Operation("session binding is unavailable".into()))?;
        if binding.harness_digest.as_ref() != Some(key.snapshot_digest())
            || binding.model != key.model()
            || binding.reasoning.as_deref() != key.reasoning()
        {
            return Err(Error::Harness(HarnessError::BindingRecordConflict(
                "the resident Session snapshot or effective route differs from the recovery key"
                    .into(),
            )));
        }
        let mut ledger = self.load_ledger(id)?;
        let entry_position = ledger
            .entries
            .iter()
            .position(|entry| entry.turn_id == key.turn_id());
        if let Some(entry) = entry_position.map(|position| &ledger.entries[position])
            && (entry.prompt_digest != key.prompt_digest()
                || entry.runtime_prompt_index != key.runtime_prompt_index())
        {
            return Err(Error::Harness(HarnessError::BindingRecordConflict(
                "the recovery key conflicts with the durable Turn ledger identity".into(),
            )));
        }
        let Some(record) = self.load_turn_binding_record(id, key.turn_id())? else {
            return Ok(TurnBindingStatus::Absent);
        };
        let receipt = record.receipt();
        if entry_position.is_none() || receipt.session_id() != id || !key.matches_receipt(receipt) {
            return Err(Error::Harness(HarnessError::BindingRecordConflict(
                "the durable record does not match the requested Turn binding identity".into(),
            )));
        }
        let position = entry_position.expect("checked present");
        match &ledger.entries[position].state {
            LedgerTurnState::Completed {
                outcome,
                settlement_id,
                usage,
            } => {
                if *outcome != receipt.outcome()
                    || settlement_id != receipt.settlement_id()
                    || usage.as_ref() != Some(receipt.usage())
                {
                    return Err(Error::Harness(HarnessError::BindingRecordConflict(
                        "the durable record conflicts with the completed Turn ledger evidence"
                            .into(),
                    )));
                }
            }
            LedgerTurnState::Pending => {
                ledger.entries[position].state = LedgerTurnState::Completed {
                    outcome: receipt.outcome(),
                    settlement_id: receipt.settlement_id().to_owned(),
                    usage: Some(receipt.usage().clone()),
                };
                self.save_ledger(id, &ledger)?;
            }
            LedgerTurnState::Discarded => {
                return Err(Error::Harness(HarnessError::BindingRecordConflict(
                    "the durable Turn binding belongs to discarded conversation history".into(),
                )));
            }
        }
        Ok(TurnBindingStatus::Complete { record })
    }
}
