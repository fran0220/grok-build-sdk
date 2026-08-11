use super::*;

pub(super) type ShellAuthority =
    dyn xai_grok_shell::session::state_authority::NativeSessionStateAuthority;

pub(super) struct SessionStateAuthorityBridge {
    pub(super) store: Arc<dyn crate::SessionStateStore>,
    pub(super) observer: Option<Arc<dyn crate::CompactionObserver>>,
    pub(super) correlations: CompactionCorrelationMap,
}

struct SessionStateSessionBridge {
    store: Arc<dyn crate::SessionStateStore>,
    key: crate::SessionKey,
    generation: crate::SessionGeneration,
    identity: xai_grok_shell::session::state_authority::SessionIdentity,
    staged: std::sync::Mutex<Vec<Vec<u8>>>,
    observer: Option<Arc<dyn crate::CompactionObserver>>,
    correlations: CompactionCorrelationMap,
}

fn authority_error(
    error: impl ToString,
) -> xai_grok_shell::session::state_authority::AuthorityError {
    xai_grok_shell::session::state_authority::AuthorityError(error.to_string())
}

impl xai_grok_shell::session::state_authority::NativeSessionStateAuthority
    for SessionStateAuthorityBridge
{
    fn inspect(
        &self,
        session_identity: &str,
    ) -> Result<
        xai_grok_shell::session::state_authority::SessionInspection,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::SessionInspection;
        let key = crate::SessionKey::new(session_identity).map_err(authority_error)?;
        Ok(
            match self.store.inspect_slot(&key).map_err(authority_error)? {
                crate::SessionSlot::Vacant => SessionInspection::Vacant,
                crate::SessionSlot::Live(x) => SessionInspection::Live {
                    generation: x.manifest().generation().as_str().to_owned(),
                },
                crate::SessionSlot::Tombstoned { receipt } => SessionInspection::Tombstoned {
                    generation: receipt.generation().as_str().to_owned(),
                },
            },
        )
    }

    fn create(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        session.create_empty()?;
        Ok(Arc::new(session))
    }

    fn open(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        let current = session
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        if current.manifest().generation() != &session.generation {
            return Err(authority_error("session generation mismatch"));
        }
        Ok(Arc::new(session))
    }

    fn publish_fork(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
        records: Vec<xai_grok_shell::session::state_authority::ReplayRecord>,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        session.publish_prepared(records)?;
        Ok(Arc::new(session))
    }

    fn tombstone(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let session = self.session(id)?;
        let expected = session
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        if expected.manifest().generation() != &session.generation {
            return Err(authority_error("session generation mismatch"));
        }
        let request = crate::PreparedSessionDelete::new(session.key.clone(), expected.clone())
            .map_err(authority_error)?;
        let result = self
            .store
            .compare_and_delete(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&session.key)
            .map_err(authority_error)?;
        if crate::delete_reconciled(&result, &slot, &expected) {
            Ok(())
        } else {
            Err(authority_error(
                "delete conflicted or acknowledgement could not be reconciled",
            ))
        }
    }
}

impl SessionStateAuthorityBridge {
    fn session(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<SessionStateSessionBridge, xai_grok_shell::session::state_authority::AuthorityError>
    {
        Ok(SessionStateSessionBridge {
            store: self.store.clone(),
            key: crate::SessionKey::new(&id.identity).map_err(authority_error)?,
            generation: crate::SessionGeneration::new(&id.generation).map_err(authority_error)?,
            identity: id,
            staged: std::sync::Mutex::new(Vec::new()),
            observer: self.observer.clone(),
            correlations: self.correlations.clone(),
        })
    }
}

impl SessionStateSessionBridge {
    fn compaction_error(
        kind: xai_grok_shell::session::state_authority::NativeCompactionErrorKind,
        message: impl Into<String>,
    ) -> xai_grok_shell::session::state_authority::NativeCompactionError {
        xai_grok_shell::session::state_authority::NativeCompactionError {
            kind,
            message: message.into(),
        }
    }

    fn compaction_uncertain(
        error: impl ToString,
    ) -> xai_grok_shell::session::state_authority::NativeCompactionError {
        Self::compaction_error(
            xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Uncertain,
            error.to_string(),
        )
    }

    fn compaction_conflict(
        message: impl Into<String>,
    ) -> xai_grok_shell::session::state_authority::NativeCompactionError {
        Self::compaction_error(
            xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Conflict,
            message,
        )
    }

    fn session_id(&self) -> crate::SessionId {
        crate::SessionId::from_stored(&self.identity.identity)
    }

    fn state_reference(
        &self,
        document: &crate::LiveSessionDocument,
    ) -> Result<
        crate::CompactionStateReference,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        Ok(crate::CompactionStateReference {
            session: self.session_id(),
            generation: self.generation.clone(),
            manifest_revision: document.version().revision(),
            manifest_digest: crate::CompactionDigest::from_stored(document.version().digest())
                .map_err(Self::compaction_uncertain)?,
            head: document.manifest().head().cloned(),
            sequence: document.manifest().segment_count(),
        })
    }

    fn is_exact_pending_base(
        &self,
        document: &crate::LiveSessionDocument,
        intent: &crate::CompactionIntent,
    ) -> Result<bool, xai_grok_shell::session::state_authority::NativeCompactionError> {
        let base_manifest = crate::SessionManifest::new(
            self.key.clone(),
            self.generation.clone(),
            document.manifest().head().cloned(),
            document.manifest().segment_count(),
            document.manifest().transcript_bytes(),
        )
        .map_err(Self::compaction_uncertain)?;
        Ok(intent.base.session == self.session_id()
            && intent.base.generation == self.generation
            && intent.base.head == document.manifest().head().cloned()
            && intent.base.sequence == document.manifest().segment_count()
            && intent.base.manifest_digest.as_str() == base_manifest.digest()
            && intent
                .base
                .manifest_revision
                .checked_add(1)
                .is_some_and(|revision| revision == document.version().revision()))
    }

    fn content_facts(
        facts: xai_grok_shell::session::state_authority::NativeCompactionDigestFacts,
    ) -> Result<
        crate::CompactionContentFacts,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        crate::CompactionContentFacts::from_stored(facts.digest, facts.size_bytes, facts.item_count)
            .map_err(Self::compaction_uncertain)
    }

    fn native_facts(
        facts: &crate::CompactionContentFacts,
    ) -> xai_grok_shell::session::state_authority::NativeCompactionDigestFacts {
        xai_grok_shell::session::state_authority::NativeCompactionDigestFacts {
            digest: facts.digest.as_str().to_owned(),
            size_bytes: facts.size_bytes,
            item_count: facts.item_count,
        }
    }

    fn native_replay_record(
        record: &crate::CompactionPublicationRecord,
    ) -> xai_grok_shell::session::state_authority::NativeCompactionReplayRecord {
        use xai_grok_shell::session::state_authority::{
            NativeCompactionBase, NativeCompactionReason, NativeCompactionReplayOwner,
            NativeCompactionReplayRecord, NativeCompactionRequestPath,
        };
        let owner = match &record.intent.owner {
            crate::CompactionOwner::Session { session } => NativeCompactionReplayOwner::Session {
                session_id: session.as_str().to_owned(),
            },
            crate::CompactionOwner::Turn { session, turn } => NativeCompactionReplayOwner::Turn {
                session_id: session.as_str().to_owned(),
                turn_id: turn.as_str().to_owned(),
            },
            crate::CompactionOwner::AutonomousTurn {
                session,
                turn,
                run,
                iteration,
                operation,
            } => NativeCompactionReplayOwner::AutonomousTurn {
                session_id: session.as_str().to_owned(),
                turn_id: turn.as_str().to_owned(),
                run_id: run.as_str().to_owned(),
                iteration: iteration.get(),
                operation_id: operation.as_str().to_owned(),
            },
        };
        NativeCompactionReplayRecord {
            compaction_id: record.intent.id.as_str().to_owned(),
            owner,
            reason: match record.intent.reason {
                crate::CompactionReason::Manual => NativeCompactionReason::Manual,
                crate::CompactionReason::AutomaticThreshold => {
                    NativeCompactionReason::AutomaticThreshold
                }
            },
            base: NativeCompactionBase {
                session_id: record.intent.base.session.as_str().to_owned(),
                generation: record.intent.base.generation.as_str().to_owned(),
                manifest_revision: record.intent.base.manifest_revision,
                manifest_digest: record.intent.base.manifest_digest.as_str().to_owned(),
                head: record
                    .intent
                    .base
                    .head
                    .as_ref()
                    .map(|head| head.as_str().to_owned()),
                sequence: record.intent.base.sequence,
            },
            path: match record.intent.input.path {
                crate::CompactionRequestPath::SinglePassVerbatim => {
                    NativeCompactionRequestPath::SinglePassVerbatim
                }
                crate::CompactionRequestPath::SinglePassFitted => {
                    NativeCompactionRequestPath::SinglePassFitted
                }
                crate::CompactionRequestPath::SinglePassLossy => {
                    NativeCompactionRequestPath::SinglePassLossy
                }
                crate::CompactionRequestPath::TwoPassFinal => {
                    NativeCompactionRequestPath::TwoPassFinal
                }
            },
            messages: Self::native_facts(&record.intent.input.messages),
            tool_definitions: Self::native_facts(&record.intent.input.tool_definitions),
            hosted_tool_declarations: Self::native_facts(
                &record.intent.input.hosted_tool_declarations,
            ),
            model_parameters: Self::native_facts(&record.intent.input.model_parameters),
            summary: Self::native_facts(&record.summary),
            checkpoint: Self::native_facts(&record.checkpoint),
            installed_state: Self::native_facts(&record.installed_state),
            prompt_index: record.prompt_index,
        }
    }

    fn replay_record(
        record: xai_grok_shell::session::state_authority::NativeCompactionReplayRecord,
    ) -> Result<
        crate::CompactionPublicationRecord,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        use xai_grok_shell::session::state_authority::{
            NativeCompactionReason, NativeCompactionReplayOwner, NativeCompactionRequestPath,
        };
        let owner = match record.owner {
            NativeCompactionReplayOwner::Session { session_id } => {
                crate::CompactionOwner::Session {
                    session: crate::SessionId::from_stored(session_id),
                }
            }
            NativeCompactionReplayOwner::Turn {
                session_id,
                turn_id,
            } => crate::CompactionOwner::Turn {
                session: crate::SessionId::from_stored(session_id),
                turn: crate::CompactionTurnId::new(turn_id).map_err(Self::compaction_uncertain)?,
            },
            NativeCompactionReplayOwner::AutonomousTurn {
                session_id,
                turn_id,
                run_id,
                iteration,
                operation_id,
            } => crate::CompactionOwner::AutonomousTurn {
                session: crate::SessionId::from_stored(session_id),
                turn: crate::CompactionTurnId::new(turn_id).map_err(Self::compaction_uncertain)?,
                run: crate::run::RunId::new(run_id).map_err(Self::compaction_uncertain)?,
                iteration: crate::run::IterationId::new(iteration),
                operation: crate::run::OperationId::new(operation_id)
                    .map_err(Self::compaction_uncertain)?,
            },
        };
        let path = match record.path {
            NativeCompactionRequestPath::SinglePassVerbatim => {
                crate::CompactionRequestPath::SinglePassVerbatim
            }
            NativeCompactionRequestPath::SinglePassFitted => {
                crate::CompactionRequestPath::SinglePassFitted
            }
            NativeCompactionRequestPath::SinglePassLossy => {
                crate::CompactionRequestPath::SinglePassLossy
            }
            NativeCompactionRequestPath::TwoPassFinal => crate::CompactionRequestPath::TwoPassFinal,
        };
        let publication = crate::CompactionPublicationRecord {
            intent: crate::CompactionIntent {
                id: crate::CompactionId::from_stored(record.compaction_id)
                    .map_err(Self::compaction_uncertain)?,
                owner,
                reason: match record.reason {
                    NativeCompactionReason::Manual => crate::CompactionReason::Manual,
                    NativeCompactionReason::AutomaticThreshold => {
                        crate::CompactionReason::AutomaticThreshold
                    }
                },
                base: crate::CompactionStateReference {
                    session: crate::SessionId::from_stored(record.base.session_id),
                    generation: crate::SessionGeneration::new(record.base.generation)
                        .map_err(Self::compaction_uncertain)?,
                    manifest_revision: record.base.manifest_revision,
                    manifest_digest: crate::CompactionDigest::from_stored(
                        record.base.manifest_digest,
                    )
                    .map_err(Self::compaction_uncertain)?,
                    head: record
                        .base
                        .head
                        .map(crate::SessionObjectId::from_stored)
                        .transpose()
                        .map_err(Self::compaction_uncertain)?,
                    sequence: record.base.sequence,
                },
                input: crate::CompactionInputFacts::from_leaves(
                    path,
                    Self::content_facts(record.messages)?,
                    Self::content_facts(record.tool_definitions)?,
                    Self::content_facts(record.hosted_tool_declarations)?,
                    Self::content_facts(record.model_parameters)?,
                ),
            },
            summary: Self::content_facts(record.summary)?,
            checkpoint: Self::content_facts(record.checkpoint)?,
            installed_state: Self::content_facts(record.installed_state)?,
            prompt_index: record.prompt_index,
        };
        publication.validate().map_err(Self::compaction_uncertain)?;
        Ok(publication)
    }

    fn owner(
        &self,
        owner: xai_grok_shell::session::state_authority::NativeCompactionOwner,
    ) -> Result<
        crate::CompactionOwner,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        use xai_grok_shell::session::state_authority::NativeCompactionOwner;
        let session = self.session_id();
        Ok(match owner {
            NativeCompactionOwner::Session => crate::CompactionOwner::Session { session },
            NativeCompactionOwner::Turn { turn_id } => {
                let turn =
                    crate::CompactionTurnId::new(&turn_id).map_err(Self::compaction_uncertain)?;
                let correlation = self
                    .correlations
                    .lock()
                    .map_err(Self::compaction_uncertain)?
                    .get(&(session.as_str().to_owned(), turn_id.clone()))
                    .cloned();
                match correlation {
                    None => crate::CompactionOwner::Turn {
                        session: session.clone(),
                        turn: turn.clone(),
                    },
                    Some(correlation) => crate::CompactionOwner::AutonomousTurn {
                        session,
                        turn,
                        run: correlation.run,
                        iteration: correlation.iteration,
                        operation: correlation.operation,
                    },
                }
            }
            NativeCompactionOwner::AutonomousTurn {
                turn_id,
                run_id,
                iteration,
                operation_id,
            } => crate::CompactionOwner::AutonomousTurn {
                session,
                turn: crate::CompactionTurnId::new(turn_id).map_err(Self::compaction_uncertain)?,
                run: crate::run::RunId::new(run_id).map_err(Self::compaction_uncertain)?,
                iteration: crate::run::IterationId::new(iteration),
                operation: crate::run::OperationId::new(operation_id)
                    .map_err(Self::compaction_uncertain)?,
            },
        })
    }

    fn intent_from_input(
        &self,
        id: crate::CompactionId,
        base: crate::CompactionStateReference,
        input: xai_grok_shell::session::state_authority::NativeCompactionInput,
    ) -> Result<
        crate::CompactionIntent,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        use xai_grok_shell::session::state_authority::{
            NativeCompactionReason, NativeCompactionRequestPath,
        };
        let path = match input.path {
            NativeCompactionRequestPath::SinglePassVerbatim => {
                crate::CompactionRequestPath::SinglePassVerbatim
            }
            NativeCompactionRequestPath::SinglePassFitted => {
                crate::CompactionRequestPath::SinglePassFitted
            }
            NativeCompactionRequestPath::SinglePassLossy => {
                crate::CompactionRequestPath::SinglePassLossy
            }
            NativeCompactionRequestPath::TwoPassFinal => crate::CompactionRequestPath::TwoPassFinal,
        };
        let intent = crate::CompactionIntent {
            id,
            owner: self.owner(input.owner)?,
            reason: match input.reason {
                NativeCompactionReason::Manual => crate::CompactionReason::Manual,
                NativeCompactionReason::AutomaticThreshold => {
                    crate::CompactionReason::AutomaticThreshold
                }
            },
            base,
            input: crate::CompactionInputFacts::from_leaves(
                path,
                Self::content_facts(input.messages)?,
                Self::content_facts(input.tool_definitions)?,
                Self::content_facts(input.hosted_tool_declarations)?,
                Self::content_facts(input.model_parameters)?,
            ),
        };
        intent.validate().map_err(Self::compaction_uncertain)?;
        Ok(intent)
    }

    fn manifest_with_compaction(
        &self,
        document: &crate::LiveSessionDocument,
        state: crate::CompactionManifestState,
    ) -> Result<
        crate::SessionManifest,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        crate::SessionManifest::new_with_compaction_state(
            self.key.clone(),
            self.generation.clone(),
            document.manifest().head().cloned(),
            document.manifest().segment_count(),
            document.manifest().transcript_bytes(),
            state,
        )
        .map_err(Self::compaction_uncertain)
    }

    fn compaction_cas(
        &self,
        expected: crate::LiveSessionDocument,
        manifest: crate::SessionManifest,
        suffix: &[crate::SessionObject],
    ) -> Result<
        crate::LiveSessionDocument,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        let request =
            crate::PreparedManifestCas::new(self.key.clone(), Some(expected), manifest, suffix)
                .map_err(Self::compaction_uncertain)?;
        let intended = request.successor().clone();
        let result = self
            .store
            .compare_and_swap_manifest(request)
            .map_err(Self::compaction_uncertain)?;
        let slot = self
            .store
            .inspect_slot(&self.key)
            .map_err(Self::compaction_uncertain)?;
        if crate::manifest_cas_reconciled(&result, &slot, &intended) {
            Ok(intended)
        } else if matches!(result, crate::ManifestCas::Conflict) {
            Err(Self::compaction_conflict("Session manifest changed"))
        } else {
            Err(Self::compaction_uncertain(
                "manifest commit acknowledgement could not be reconciled exactly",
            ))
        }
    }

    fn put_compaction_object(
        &self,
        object: &crate::SessionObject,
    ) -> Result<(), xai_grok_shell::session::state_authority::NativeCompactionError> {
        let result = self
            .store
            .put_object(object.clone())
            .map_err(Self::compaction_uncertain)?;
        let loaded = if result == crate::ObjectPut::CommitUnknown {
            self.store
                .load_object(&self.key, &self.generation, object.id())
                .map_err(Self::compaction_uncertain)?
        } else {
            None
        };
        if crate::object_put_reconciled(&result, loaded.as_ref(), object) {
            Ok(())
        } else {
            Err(Self::compaction_uncertain(
                "object commit acknowledgement could not be reconciled exactly",
            ))
        }
    }

    fn verified_compaction_checkpoint(
        &self,
        checkpoint: &crate::SessionObjectId,
        facts: &crate::CompactionContentFacts,
    ) -> Result<crate::SessionObject, xai_grok_shell::session::state_authority::NativeCompactionError>
    {
        let object = self
            .store
            .load_object(&self.key, &self.generation, checkpoint)
            .map_err(Self::compaction_uncertain)?
            .ok_or_else(|| Self::compaction_uncertain("missing compaction checkpoint"))?;
        if object.id() != checkpoint
            || object.session() != &self.key
            || object.generation() != &self.generation
        {
            return Err(Self::compaction_uncertain(
                "compaction checkpoint identity differs from its reference",
            ));
        }
        let crate::SessionObjectKind::Checkpoint { shell_bytes, .. } = object.kind() else {
            return Err(Self::compaction_uncertain(
                "compaction checkpoint reference has the wrong object kind",
            ));
        };
        let exact = crate::CompactionContentFacts::from_bytes(
            crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
            shell_bytes,
            facts.item_count,
        );
        if &exact != facts {
            return Err(Self::compaction_uncertain(
                "compaction checkpoint differs from its publication facts",
            ));
        }
        Ok(object)
    }

    /// Reconcile a caller retry after the compound publication committed but
    /// its acknowledgement was lost. Only byte-for-byte/typed-fact identity is
    /// accepted; reusing an ID with any different payload fails closed.
    fn reconcile_published_compaction(
        &self,
        receipt: &crate::CompactionReceipt,
        publication: &xai_grok_shell::session::state_authority::NativeCompactionPublication,
    ) -> Result<(), xai_grok_shell::session::state_authority::NativeCompactionError> {
        if receipt.intent.id.as_str() != publication.record.compaction_id {
            return Err(Self::compaction_conflict("compaction id differs"));
        }
        let expected_record = crate::CompactionPublicationRecord {
            intent: receipt.intent.clone(),
            summary: Self::content_facts(publication.record.summary.clone())?,
            checkpoint: Self::content_facts(publication.record.checkpoint.clone())?,
            installed_state: Self::content_facts(publication.record.installed_state.clone())?,
            prompt_index: publication.record.prompt_index,
        };
        let receipt_record = crate::CompactionPublicationRecord {
            intent: receipt.intent.clone(),
            summary: receipt.summary.clone(),
            checkpoint: receipt.checkpoint.clone(),
            installed_state: receipt.installed_state.clone(),
            prompt_index: receipt.publication.prompt_index,
        };
        if expected_record != receipt_record {
            return Err(Self::compaction_conflict(
                "compaction publication retry has different typed facts",
            ));
        }
        let checkpoint = self
            .verified_compaction_checkpoint(&receipt.publication.checkpoint, &receipt.checkpoint)?;
        match checkpoint.kind() {
            crate::SessionObjectKind::Checkpoint { name, shell_bytes }
                if name == &publication.name && shell_bytes == &publication.payload => {}
            crate::SessionObjectKind::Checkpoint { .. } => {
                return Err(Self::compaction_conflict(
                    "compaction checkpoint retry has different bytes",
                ));
            }
            _ => unreachable!("verified compaction checkpoint"),
        }
        let chained = self
            .store
            .load_object(
                &self.key,
                &self.generation,
                &receipt.publication.publication,
            )
            .map_err(Self::compaction_uncertain)?
            .ok_or_else(|| Self::compaction_uncertain("missing compaction publication"))?;
        match chained.kind() {
            crate::SessionObjectKind::CompactionPublication {
                marker_bytes,
                checkpoint,
                record,
                ..
            } if chained.id() == &receipt.publication.publication
                && chained.session() == &self.key
                && chained.generation() == &self.generation
                && marker_bytes == &publication.marker
                && checkpoint == &receipt.publication.checkpoint
                && record == &expected_record =>
            {
                Ok(())
            }
            _ => Err(Self::compaction_conflict(
                "compaction publication retry differs from committed evidence",
            )),
        }
    }

    async fn observe_intent(
        &self,
        intent: &crate::CompactionIntent,
    ) -> Result<(), xai_grok_shell::session::state_authority::NativeCompactionError> {
        let observer = self.observer.as_ref().ok_or_else(|| {
            xai_grok_shell::session::state_authority::NativeCompactionError::disabled()
        })?;
        let acknowledgement = observer.intent(intent.clone()).await.map_err(|error| {
            Self::compaction_error(
                if error.code == crate::CompactionObserverErrorCode::Rejected {
                    xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Rejected
                } else {
                    xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Observer
                },
                error.to_string(),
            )
        })?;
        if acknowledgement != crate::CompactionAcknowledgement::for_intent(intent) {
            return Err(Self::compaction_error(
                xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Observer,
                "Host returned a mismatched intent acknowledgement",
            ));
        }
        Ok(())
    }

    async fn observe_outcome(
        &self,
        outcome: &crate::CompactionOutcome,
    ) -> Result<(), xai_grok_shell::session::state_authority::NativeCompactionError> {
        let observer = self.observer.as_ref().ok_or_else(|| {
            xai_grok_shell::session::state_authority::NativeCompactionError::disabled()
        })?;
        let acknowledgement = observer.outcome(outcome.clone()).await.map_err(|error| {
            Self::compaction_error(
                xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Observer,
                error.to_string(),
            )
        })?;
        if acknowledgement != crate::CompactionAcknowledgement::for_outcome(outcome) {
            return Err(Self::compaction_error(
                xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Observer,
                "Host returned a mismatched outcome acknowledgement",
            ));
        }
        Ok(())
    }

    fn publish_prepared(
        &self,
        records: Vec<xai_grok_shell::session::state_authority::ReplayRecord>,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        use xai_grok_shell::session::state_authority::{ReplayRecord, RewindOperation};
        if self.current()?.is_some() {
            return Err(authority_error("fork target already exists"));
        }
        let mut previous = None;
        let mut sequence = 0u64;
        let mut objects = Vec::with_capacity(records.len());
        for record in records {
            match record {
                ReplayRecord::Update(bytes) => {
                    objects.extend(self.update_objects(&[bytes], &mut previous, &mut sequence)?);
                }
                ReplayRecord::Checkpoint {
                    name,
                    payload,
                    marker,
                } => {
                    let checkpoint = crate::SessionObject::checkpoint(
                        self.key.clone(),
                        self.generation.clone(),
                        name,
                        payload,
                    )
                    .map_err(authority_error)?;
                    self.put_exact(&checkpoint)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| authority_error("sequence overflow"))?;
                    let publication = crate::SessionObject::publish_checkpoint(
                        self.key.clone(),
                        self.generation.clone(),
                        previous.clone(),
                        sequence,
                        marker,
                        checkpoint.id().clone(),
                    )
                    .map_err(authority_error)?;
                    previous = Some(publication.id().clone());
                    objects.push(publication);
                }
                ReplayRecord::Compaction {
                    name,
                    payload,
                    marker,
                    record,
                } => {
                    let record = Self::replay_record(*record).map_err(authority_error)?;
                    let checkpoint_facts = crate::CompactionContentFacts::from_bytes(
                        crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
                        &payload,
                        record.checkpoint.item_count,
                    );
                    if checkpoint_facts != record.checkpoint {
                        return Err(authority_error(
                            "forked compaction checkpoint differs from its publication facts",
                        ));
                    }
                    let checkpoint = crate::SessionObject::checkpoint(
                        self.key.clone(),
                        self.generation.clone(),
                        name,
                        payload,
                    )
                    .map_err(authority_error)?;
                    self.put_exact(&checkpoint)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| authority_error("sequence overflow"))?;
                    let publication = crate::SessionObject::publish_compaction(
                        self.key.clone(),
                        self.generation.clone(),
                        previous.clone(),
                        sequence,
                        marker,
                        checkpoint.id().clone(),
                        record,
                    )
                    .map_err(authority_error)?;
                    previous = Some(publication.id().clone());
                    objects.push(publication);
                }
                ReplayRecord::Rewind { operation, marker } => {
                    let (kind, index, payload) = match operation {
                        RewindOperation::AppendPoint { index, payload } => {
                            (crate::RewindKind::AppendPoint, index, payload)
                        }
                        RewindOperation::Truncate { index, payload } => {
                            (crate::RewindKind::Truncate, index, payload)
                        }
                        RewindOperation::Merge { index, payload } => {
                            (crate::RewindKind::Merge, index, payload)
                        }
                    };
                    let rewind = crate::SessionObject::rewind(
                        self.key.clone(),
                        self.generation.clone(),
                        kind,
                        index,
                        payload,
                    )
                    .map_err(authority_error)?;
                    self.put_exact(&rewind)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| authority_error("sequence overflow"))?;
                    let publication = crate::SessionObject::publish_rewind(
                        self.key.clone(),
                        self.generation.clone(),
                        previous.clone(),
                        sequence,
                        marker,
                        rewind.id().clone(),
                    )
                    .map_err(authority_error)?;
                    previous = Some(publication.id().clone());
                    objects.push(publication);
                }
            }
        }
        if objects.is_empty() {
            self.create_empty()
        } else {
            self.commit(objects).map(|_| ())
        }
    }

    fn create_empty(&self) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let manifest =
            crate::SessionManifest::new(self.key.clone(), self.generation.clone(), None, 0, 0)
                .map_err(authority_error)?;
        let request = crate::PreparedManifestCas::new(self.key.clone(), None, manifest, &[])
            .map_err(authority_error)?;
        let intended = request.successor().clone();
        let result = self
            .store
            .compare_and_swap_manifest(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?;
        if crate::manifest_cas_reconciled(&result, &slot, &intended) {
            Ok(())
        } else {
            Err(authority_error(
                "session already exists or creation acknowledgement could not be reconciled",
            ))
        }
    }

    fn current(
        &self,
    ) -> Result<
        Option<crate::LiveSessionDocument>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        match self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?
        {
            crate::SessionSlot::Vacant => Ok(None),
            crate::SessionSlot::Live(x) => Ok(Some(x)),
            crate::SessionSlot::Tombstoned { .. } => Err(authority_error("session is tombstoned")),
        }
    }
    fn put_exact(
        &self,
        object: &crate::SessionObject,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let result = self
            .store
            .put_object(object.clone())
            .map_err(authority_error)?;
        let loaded = if result == crate::ObjectPut::CommitUnknown {
            self.store
                .load_object(&self.key, &self.generation, object.id())
                .map_err(authority_error)?
        } else {
            None
        };
        if crate::object_put_reconciled(&result, loaded.as_ref(), object) {
            Ok(())
        } else {
            Err(authority_error(
                "object acknowledgement could not be reconciled exactly",
            ))
        }
    }
    fn commit(
        &self,
        objects: Vec<crate::SessionObject>,
    ) -> Result<crate::LiveSessionDocument, xai_grok_shell::session::state_authority::AuthorityError>
    {
        let expected = self.current()?;
        if let Some(x) = &expected {
            if x.manifest().generation() != &self.generation {
                return Err(authority_error("session generation mismatch"));
            }
            if !objects.is_empty()
                && !matches!(
                    x.manifest().compaction_state(),
                    crate::CompactionManifestState::None
                )
            {
                return Err(authority_error(
                    "Session publication is fenced during native compaction",
                ));
            }
        }
        for object in &objects {
            self.put_exact(object)?;
        }
        let head = objects
            .last()
            .map(|x| x.id().clone())
            .or_else(|| expected.as_ref().and_then(|x| x.manifest().head().cloned()));
        let count = expected
            .as_ref()
            .map_or(0, |x| x.manifest().segment_count())
            .checked_add(objects.len() as u64)
            .ok_or_else(|| authority_error("record count overflow"))?;
        let added = objects
            .iter()
            .map(|x| match x.kind() {
                crate::SessionObjectKind::TranscriptSegment { bytes, .. } => bytes.len() as u64,
                crate::SessionObjectKind::CheckpointPublication { marker_bytes, .. }
                | crate::SessionObjectKind::CompactionPublication { marker_bytes, .. }
                | crate::SessionObjectKind::RewindPublication { marker_bytes, .. } => {
                    marker_bytes.len() as u64
                }
                _ => 0,
            })
            .sum::<u64>();
        let bytes = expected
            .as_ref()
            .map_or(0, |x| x.manifest().transcript_bytes())
            .checked_add(added)
            .ok_or_else(|| authority_error("transcript size overflow"))?;
        let manifest = crate::SessionManifest::new_with_compaction_state(
            self.key.clone(),
            self.generation.clone(),
            head,
            count,
            bytes,
            expected
                .as_ref()
                .map(|document| document.manifest().compaction_state().clone())
                .unwrap_or_default(),
        )
        .map_err(authority_error)?;
        let request =
            crate::PreparedManifestCas::new(self.key.clone(), expected, manifest, &objects)
                .map_err(authority_error)?;
        let intended = request.successor().clone();
        let result = self
            .store
            .compare_and_swap_manifest(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?;
        if crate::manifest_cas_reconciled(&result, &slot, &intended) {
            Ok(intended)
        } else {
            Err(authority_error(
                "manifest conflicted or acknowledgement could not be reconciled exactly",
            ))
        }
    }
    fn update_objects(
        &self,
        updates: &[Vec<u8>],
        previous: &mut Option<crate::SessionObjectId>,
        sequence: &mut u64,
    ) -> Result<Vec<crate::SessionObject>, xai_grok_shell::session::state_authority::AuthorityError>
    {
        let mut out = Vec::new();
        for bytes in updates {
            if bytes.len() > crate::TARGET_TRANSCRIPT_SEGMENT_BYTES {
                return Err(authority_error("single update exceeds chunk limit"));
            }
            *sequence = sequence
                .checked_add(1)
                .ok_or_else(|| authority_error("sequence overflow"))?;
            let object = crate::SessionObject::transcript(
                self.key.clone(),
                self.generation.clone(),
                previous.clone(),
                *sequence,
                bytes.clone(),
            )
            .map_err(authority_error)?;
            *previous = Some(object.id().clone());
            out.push(object);
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl xai_grok_shell::session::state_authority::NativeSession for SessionStateSessionBridge {
    fn identity(&self) -> &xai_grok_shell::session::state_authority::SessionIdentity {
        &self.identity
    }
    fn stage_update(
        &self,
        bytes: Vec<u8>,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        if bytes.len() > crate::TARGET_TRANSCRIPT_SEGMENT_BYTES {
            return Err(authority_error("single update exceeds chunk limit"));
        }
        self.staged.lock().map_err(authority_error)?.push(bytes);
        Ok(())
    }
    fn flush(
        &self,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        if !objects.is_empty() {
            self.commit(objects)?;
            staged.clear();
        }
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }
    fn replay_page(
        &self,
        cursor: Option<xai_grok_shell::session::state_authority::ReplayCursor>,
        max_records: usize,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayPage,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::{
            ReplayCursor, ReplayPage, ReplayRecord, RewindOperation,
        };
        if max_records == 0 || max_records > 4096 {
            return Err(authority_error("invalid replay page bound"));
        }
        let doc = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let start = cursor.map_or(1, |c| {
            if c.generation != self.identity.generation {
                u64::MAX
            } else {
                c.next_sequence
            }
        });
        if start == 0 || start > doc.manifest().segment_count() + 1 {
            return Err(authority_error("invalid replay cursor or cursor gap"));
        }
        let mut chain = Vec::new();
        let mut id = doc.manifest().head().cloned();
        while let Some(object_id) = id {
            if chain.len() >= 1_000_000 {
                return Err(authority_error("replay traversal limit exceeded"));
            }
            let object = self
                .store
                .load_object(&self.key, &self.generation, &object_id)
                .map_err(authority_error)?
                .ok_or_else(|| authority_error("missing replay object"))?;
            id = object.previous().cloned();
            chain.push(object);
        }
        chain.reverse();
        if chain.len() as u64 != doc.manifest().segment_count()
            || chain
                .iter()
                .enumerate()
                .any(|(i, x)| x.sequence() != Some(i as u64 + 1))
        {
            return Err(authority_error("corrupt replay chain or cursor gap"));
        }
        let mut records = Vec::new();
        for object in chain
            .into_iter()
            .skip((start - 1) as usize)
            .take(max_records)
        {
            records.push(match object.kind() {
                crate::SessionObjectKind::TranscriptSegment { bytes, .. } => {
                    ReplayRecord::Update(bytes.clone())
                }
                crate::SessionObjectKind::CheckpointPublication {
                    marker_bytes,
                    checkpoint,
                    ..
                } => {
                    let x = self
                        .store
                        .load_object(&self.key, &self.generation, checkpoint)
                        .map_err(authority_error)?
                        .ok_or_else(|| authority_error("missing checkpoint object"))?;
                    match x.kind() {
                        crate::SessionObjectKind::Checkpoint { name, shell_bytes } => {
                            ReplayRecord::Checkpoint {
                                name: name.clone(),
                                payload: shell_bytes.clone(),
                                marker: marker_bytes.clone(),
                            }
                        }
                        _ => return Err(authority_error("invalid checkpoint reference")),
                    }
                }
                crate::SessionObjectKind::CompactionPublication {
                    marker_bytes,
                    checkpoint,
                    record,
                    ..
                } => {
                    let x = self
                        .verified_compaction_checkpoint(checkpoint, &record.checkpoint)
                        .map_err(authority_error)?;
                    match x.kind() {
                        crate::SessionObjectKind::Checkpoint { name, shell_bytes } => {
                            ReplayRecord::Compaction {
                                name: name.clone(),
                                payload: shell_bytes.clone(),
                                marker: marker_bytes.clone(),
                                record: Box::new(Self::native_replay_record(record)),
                            }
                        }
                        _ => {
                            return Err(authority_error("invalid compaction checkpoint reference"));
                        }
                    }
                }
                crate::SessionObjectKind::RewindPublication {
                    marker_bytes,
                    operation,
                    ..
                } => {
                    let x = self
                        .store
                        .load_object(&self.key, &self.generation, operation)
                        .map_err(authority_error)?
                        .ok_or_else(|| authority_error("missing rewind object"))?;
                    match x.kind() {
                        crate::SessionObjectKind::RewindOperation {
                            kind,
                            index,
                            shell_bytes,
                        } => {
                            let op = match kind {
                                crate::RewindKind::AppendPoint => RewindOperation::AppendPoint {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                                crate::RewindKind::Truncate => RewindOperation::Truncate {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                                crate::RewindKind::Merge => RewindOperation::Merge {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                            };
                            ReplayRecord::Rewind {
                                operation: op,
                                marker: marker_bytes.clone(),
                            }
                        }
                        _ => return Err(authority_error("invalid rewind reference")),
                    }
                }
                _ => return Err(authority_error("unpublished object in replay chain")),
            });
        }
        let next_sequence = start + records.len() as u64;
        Ok(ReplayPage {
            records,
            next: (next_sequence <= doc.manifest().segment_count()).then(|| ReplayCursor {
                generation: self.identity.generation.clone(),
                next_sequence,
            }),
        })
    }
    fn publish_checkpoint(
        &self,
        name: String,
        payload: Vec<u8>,
        marker: Vec<u8>,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let mut objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        let checkpoint = crate::SessionObject::checkpoint(
            self.key.clone(),
            self.generation.clone(),
            name,
            payload,
        )
        .map_err(authority_error)?;
        self.put_exact(&checkpoint)?;
        sequence += 1;
        let publication = crate::SessionObject::publish_checkpoint(
            self.key.clone(),
            self.generation.clone(),
            previous,
            sequence,
            marker,
            checkpoint.id().clone(),
        )
        .map_err(authority_error)?;
        objects.push(publication);
        self.commit(objects)?;
        staged.clear();
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }
    fn publish_rewind(
        &self,
        operation: xai_grok_shell::session::state_authority::RewindOperation,
        marker: Vec<u8>,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::RewindOperation;
        let (kind, index, payload) = match operation {
            RewindOperation::AppendPoint { index, payload } => {
                (crate::RewindKind::AppendPoint, index, payload)
            }
            RewindOperation::Truncate { index, payload } => {
                (crate::RewindKind::Truncate, index, payload)
            }
            RewindOperation::Merge { index, payload } => (crate::RewindKind::Merge, index, payload),
        };
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let mut objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        let op = crate::SessionObject::rewind(
            self.key.clone(),
            self.generation.clone(),
            kind,
            index,
            payload,
        )
        .map_err(authority_error)?;
        self.put_exact(&op)?;
        sequence += 1;
        let publication = crate::SessionObject::publish_rewind(
            self.key.clone(),
            self.generation.clone(),
            previous,
            sequence,
            marker,
            op.id().clone(),
        )
        .map_err(authority_error)?;
        objects.push(publication);
        self.commit(objects)?;
        staged.clear();
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }

    async fn begin_compaction(
        &self,
        input: xai_grok_shell::session::state_authority::NativeCompactionInput,
    ) -> Result<
        xai_grok_shell::session::state_authority::NativeCompactionBegin,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        use xai_grok_shell::session::state_authority::NativeCompactionBegin;
        if self.observer.is_none() {
            return Ok(NativeCompactionBegin::Disabled);
        }
        self.flush().map_err(Self::compaction_uncertain)?;
        let current = self
            .current()
            .map_err(Self::compaction_uncertain)?
            .ok_or_else(|| Self::compaction_uncertain("session is not live"))?;
        if current.manifest().generation() != &self.generation {
            return Err(Self::compaction_uncertain("session generation mismatch"));
        }
        let intent = match current.manifest().compaction_state() {
            crate::CompactionManifestState::None => {
                let intent = self.intent_from_input(
                    crate::CompactionId::mint(),
                    self.state_reference(&current)?,
                    input,
                )?;
                let manifest = self.manifest_with_compaction(
                    &current,
                    crate::CompactionManifestState::IntentPending(intent.clone()),
                )?;
                self.compaction_cas(current, manifest, &[])?;
                intent
            }
            crate::CompactionManifestState::IntentPending(existing) => {
                let candidate =
                    self.intent_from_input(existing.id.clone(), existing.base.clone(), input)?;
                if candidate != *existing {
                    return Err(Self::compaction_conflict(
                        "compaction input changed while its intent is pending",
                    ));
                }
                existing.clone()
            }
            crate::CompactionManifestState::EvidencePending(_) => {
                return Err(Self::compaction_uncertain(
                    "published compaction outcome evidence is still pending",
                ));
            }
            crate::CompactionManifestState::NotAppliedPending { .. } => {
                return Err(Self::compaction_uncertain(
                    "nonapplication outcome evidence is still pending",
                ));
            }
        };
        if let Err(error) = self.observe_intent(&intent).await {
            if error.kind
                == xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Rejected
            {
                let current = self
                    .current()
                    .map_err(Self::compaction_uncertain)?
                    .ok_or_else(|| Self::compaction_uncertain("session is not live"))?;
                let crate::CompactionManifestState::IntentPending(pending) =
                    current.manifest().compaction_state()
                else {
                    return Err(Self::compaction_uncertain(
                        "rejected intent fence changed before rollback",
                    ));
                };
                if pending != &intent {
                    return Err(Self::compaction_uncertain(
                        "rejected intent differs from its durable fence",
                    ));
                }
                let manifest =
                    self.manifest_with_compaction(&current, crate::CompactionManifestState::None)?;
                self.compaction_cas(current, manifest, &[])?;
            }
            return Err(error);
        }
        Ok(NativeCompactionBegin::Acknowledged {
            compaction_id: intent.id.as_str().to_owned(),
        })
    }

    async fn compaction_not_applied(
        &self,
        compaction_id: String,
        reason: xai_grok_shell::session::state_authority::NativeCompactionNotAppliedReason,
    ) -> Result<(), xai_grok_shell::session::state_authority::NativeCompactionError> {
        use xai_grok_shell::session::state_authority::NativeCompactionNotAppliedReason;
        let reason = match reason {
            NativeCompactionNotAppliedReason::Cancelled => {
                crate::CompactionNotAppliedReason::Cancelled
            }
            NativeCompactionNotAppliedReason::ModelFailed => {
                crate::CompactionNotAppliedReason::ModelFailed
            }
            NativeCompactionNotAppliedReason::InvalidModelOutput => {
                crate::CompactionNotAppliedReason::InvalidModelOutput
            }
            NativeCompactionNotAppliedReason::InputChanged => {
                crate::CompactionNotAppliedReason::InputChanged
            }
            NativeCompactionNotAppliedReason::PublicationAbsent => {
                crate::CompactionNotAppliedReason::PublicationAbsent
            }
            NativeCompactionNotAppliedReason::InterruptedBeforePublication => {
                crate::CompactionNotAppliedReason::InterruptedBeforePublication
            }
        };
        let current = self
            .current()
            .map_err(Self::compaction_uncertain)?
            .ok_or_else(|| Self::compaction_uncertain("session is not live"))?;
        let (pending, intent) = match current.manifest().compaction_state() {
            crate::CompactionManifestState::IntentPending(intent) => {
                if intent.id.as_str() != compaction_id {
                    return Err(Self::compaction_conflict("compaction id differs"));
                }
                let intent = intent.clone();
                let manifest = self.manifest_with_compaction(
                    &current,
                    crate::CompactionManifestState::NotAppliedPending {
                        intent: intent.clone(),
                        reason,
                    },
                )?;
                (self.compaction_cas(current, manifest, &[])?, intent)
            }
            crate::CompactionManifestState::NotAppliedPending {
                intent,
                reason: pending_reason,
            } => {
                if intent.id.as_str() != compaction_id || *pending_reason != reason {
                    return Err(Self::compaction_conflict(
                        "nonapplication retry differs from durable outcome evidence",
                    ));
                }
                (current.clone(), intent.clone())
            }
            crate::CompactionManifestState::None
            | crate::CompactionManifestState::EvidencePending(_) => {
                return Err(Self::compaction_conflict(
                    "no matching unpublished compaction intent",
                ));
            }
        };
        let outcome = crate::CompactionOutcome::NotApplied {
            intent: intent.clone(),
            reason,
        };
        self.observe_outcome(&outcome).await?;
        let current = self
            .current()
            .map_err(Self::compaction_uncertain)?
            .ok_or_else(|| Self::compaction_uncertain("session is not live"))?;
        if current.manifest().compaction_state() == &crate::CompactionManifestState::None {
            return Ok(());
        }
        if current != pending {
            return Err(Self::compaction_uncertain(
                "nonapplication outcome fence changed before acknowledgement",
            ));
        }
        let manifest =
            self.manifest_with_compaction(&current, crate::CompactionManifestState::None)?;
        self.compaction_cas(current, manifest, &[])?;
        Ok(())
    }

    async fn publish_compaction(
        &self,
        publication: xai_grok_shell::session::state_authority::NativeCompactionPublication,
    ) -> Result<(), xai_grok_shell::session::state_authority::NativeCompactionError> {
        let current = self
            .current()
            .map_err(Self::compaction_uncertain)?
            .ok_or_else(|| Self::compaction_uncertain("session is not live"))?;
        let intent = match current.manifest().compaction_state() {
            crate::CompactionManifestState::IntentPending(intent) => intent.clone(),
            crate::CompactionManifestState::EvidencePending(receipt) => {
                return self.reconcile_published_compaction(receipt, &publication);
            }
            crate::CompactionManifestState::NotAppliedPending { .. } => {
                return Err(Self::compaction_conflict(
                    "compaction already has terminal nonapplication evidence",
                ));
            }
            crate::CompactionManifestState::None => {
                return Err(Self::compaction_conflict(
                    "no matching unpublished compaction intent",
                ));
            }
        };
        if intent.id.as_str() != publication.record.compaction_id {
            return Err(Self::compaction_conflict("compaction id differs"));
        }
        if !self.is_exact_pending_base(&current, &intent)? {
            return Err(Self::compaction_conflict(
                "Session no longer equals the exact compaction base",
            ));
        }
        if !self
            .staged
            .lock()
            .map_err(Self::compaction_uncertain)?
            .is_empty()
        {
            return Err(Self::compaction_conflict(
                "transcript changed after compaction intent",
            ));
        }
        let checkpoint_facts = Self::content_facts(publication.record.checkpoint.clone())?;
        let exact_checkpoint = crate::CompactionContentFacts::from_bytes(
            crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
            &publication.payload,
            checkpoint_facts.item_count,
        );
        if checkpoint_facts != exact_checkpoint {
            return Err(Self::compaction_conflict(
                "checkpoint digest or exact size differs from payload",
            ));
        }
        let checkpoint = crate::SessionObject::checkpoint(
            self.key.clone(),
            self.generation.clone(),
            publication.name,
            publication.payload,
        )
        .map_err(Self::compaction_uncertain)?;
        self.put_compaction_object(&checkpoint)?;
        let sequence = current
            .manifest()
            .segment_count()
            .checked_add(1)
            .ok_or_else(|| Self::compaction_uncertain("sequence overflow"))?;
        let record = crate::CompactionPublicationRecord {
            intent: intent.clone(),
            summary: Self::content_facts(publication.record.summary)?,
            checkpoint: checkpoint_facts,
            installed_state: Self::content_facts(publication.record.installed_state)?,
            prompt_index: publication.record.prompt_index,
        };
        record.validate().map_err(Self::compaction_uncertain)?;
        let chained = crate::SessionObject::publish_compaction(
            self.key.clone(),
            self.generation.clone(),
            current.manifest().head().cloned(),
            sequence,
            publication.marker,
            checkpoint.id().clone(),
            record.clone(),
        )
        .map_err(Self::compaction_uncertain)?;
        self.put_compaction_object(&chained)?;
        let receipt = record.receipt(
            self.session_id(),
            self.generation.clone(),
            chained.id().clone(),
            checkpoint.id().clone(),
            sequence,
        );
        receipt.validate().map_err(Self::compaction_uncertain)?;
        let transcript_bytes = current
            .manifest()
            .transcript_bytes()
            .checked_add(match chained.kind() {
                crate::SessionObjectKind::CompactionPublication { marker_bytes, .. } => {
                    marker_bytes.len() as u64
                }
                _ => unreachable!("constructed compaction publication"),
            })
            .ok_or_else(|| Self::compaction_uncertain("transcript size overflow"))?;
        let manifest = crate::SessionManifest::new_with_compaction_state(
            self.key.clone(),
            self.generation.clone(),
            Some(chained.id().clone()),
            sequence,
            transcript_bytes,
            crate::CompactionManifestState::EvidencePending(receipt),
        )
        .map_err(Self::compaction_uncertain)?;
        self.compaction_cas(current, manifest, &[chained])?;
        Ok(())
    }

    async fn compaction_applied(
        &self,
        compaction_id: String,
    ) -> Result<(), xai_grok_shell::session::state_authority::NativeCompactionError> {
        let current = self
            .current()
            .map_err(Self::compaction_uncertain)?
            .ok_or_else(|| Self::compaction_uncertain("session is not live"))?;
        let crate::CompactionManifestState::EvidencePending(receipt) =
            current.manifest().compaction_state()
        else {
            return Err(Self::compaction_conflict(
                "no matching published compaction evidence",
            ));
        };
        if receipt.intent.id.as_str() != compaction_id {
            return Err(Self::compaction_conflict("compaction id differs"));
        }
        let outcome = crate::CompactionOutcome::Applied {
            receipt: receipt.clone(),
        };
        self.observe_outcome(&outcome).await?;
        let manifest =
            self.manifest_with_compaction(&current, crate::CompactionManifestState::None)?;
        self.compaction_cas(current, manifest, &[])?;
        Ok(())
    }

    async fn recover_compaction(
        &self,
    ) -> Result<
        xai_grok_shell::session::state_authority::NativeCompactionRecovery,
        xai_grok_shell::session::state_authority::NativeCompactionError,
    > {
        use xai_grok_shell::session::state_authority::{
            NativeCompactionNotAppliedReason, NativeCompactionRecovery,
        };
        let current = self
            .current()
            .map_err(Self::compaction_uncertain)?
            .ok_or_else(|| Self::compaction_uncertain("session is not live"))?;
        match current.manifest().compaction_state() {
            crate::CompactionManifestState::None => Ok(NativeCompactionRecovery::None),
            crate::CompactionManifestState::IntentPending(intent) => {
                self.compaction_not_applied(
                    intent.id.as_str().to_owned(),
                    NativeCompactionNotAppliedReason::InterruptedBeforePublication,
                )
                .await?;
                Ok(NativeCompactionRecovery::None)
            }
            crate::CompactionManifestState::NotAppliedPending { intent, reason } => {
                let reason = match reason {
                    crate::CompactionNotAppliedReason::Cancelled => {
                        NativeCompactionNotAppliedReason::Cancelled
                    }
                    crate::CompactionNotAppliedReason::ModelFailed => {
                        NativeCompactionNotAppliedReason::ModelFailed
                    }
                    crate::CompactionNotAppliedReason::InvalidModelOutput => {
                        NativeCompactionNotAppliedReason::InvalidModelOutput
                    }
                    crate::CompactionNotAppliedReason::InputChanged => {
                        NativeCompactionNotAppliedReason::InputChanged
                    }
                    crate::CompactionNotAppliedReason::PublicationAbsent => {
                        NativeCompactionNotAppliedReason::PublicationAbsent
                    }
                    crate::CompactionNotAppliedReason::InterruptedBeforePublication => {
                        NativeCompactionNotAppliedReason::InterruptedBeforePublication
                    }
                };
                self.compaction_not_applied(intent.id.as_str().to_owned(), reason)
                    .await?;
                Ok(NativeCompactionRecovery::None)
            }
            crate::CompactionManifestState::EvidencePending(receipt) => {
                let checkpoint = self.verified_compaction_checkpoint(
                    &receipt.publication.checkpoint,
                    &receipt.checkpoint,
                )?;
                let crate::SessionObjectKind::Checkpoint { shell_bytes, .. } = checkpoint.kind()
                else {
                    unreachable!("verified compaction checkpoint")
                };
                Ok(NativeCompactionRecovery::EvidencePending {
                    compaction_id: receipt.intent.id.as_str().to_owned(),
                    checkpoint_payload: shell_bytes.clone(),
                    installed_state: Self::native_facts(&receipt.installed_state),
                })
            }
        }
    }
}

#[cfg(test)]
mod compaction_tests;
