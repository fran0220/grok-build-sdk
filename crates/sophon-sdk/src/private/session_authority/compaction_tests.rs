use super::*;
use crate::SessionStateStore as _;
use xai_grok_shell::session::state_authority::{
    NativeCompactionBegin, NativeCompactionDigestFacts, NativeCompactionInput,
    NativeCompactionNotAppliedReason, NativeCompactionOwner, NativeCompactionPublication,
    NativeCompactionPublicationRecord, NativeCompactionReason, NativeCompactionRecovery,
    NativeCompactionRequestPath, NativeSessionStateAuthority as _, SessionIdentity,
};

#[derive(Default)]
struct Observer {
    intents: std::sync::Mutex<Vec<crate::CompactionIntent>>,
    outcomes: std::sync::Mutex<Vec<crate::CompactionOutcome>>,
    reject_intent: std::sync::atomic::AtomicBool,
    fail_outcome: std::sync::atomic::AtomicBool,
    mismatch_intent: std::sync::atomic::AtomicBool,
    mismatch_outcome: std::sync::atomic::AtomicBool,
}

struct AckLossStore {
    inner: crate::LocalSessionStateStore,
    object_unknowns: std::sync::atomic::AtomicUsize,
    manifest_unknown: std::sync::atomic::AtomicBool,
    fail_object_after_store: std::sync::atomic::AtomicBool,
    manifest_unreconciled: std::sync::atomic::AtomicBool,
    fail_next_inspect: std::sync::atomic::AtomicBool,
}

struct ProbeFaultStore {
    inner: crate::LocalSessionStateStore,
    missing: std::sync::Mutex<Option<crate::SessionObjectId>>,
    substitute: std::sync::Mutex<Option<(crate::SessionObjectId, crate::SessionObject)>>,
    slot_override: std::sync::Mutex<Option<crate::LiveSessionDocument>>,
    fail_load: std::sync::atomic::AtomicBool,
}

impl crate::SessionStateStore for ProbeFaultStore {
    fn inspect_slot(
        &self,
        key: &crate::SessionKey,
    ) -> Result<crate::SessionSlot, crate::SessionStateStoreError> {
        if let Some(document) = self.slot_override.lock().unwrap().clone() {
            Ok(crate::SessionSlot::Live(document))
        } else {
            self.inner.inspect_slot(key)
        }
    }

    fn load_object(
        &self,
        key: &crate::SessionKey,
        generation: &crate::SessionGeneration,
        id: &crate::SessionObjectId,
    ) -> Result<Option<crate::SessionObject>, crate::SessionStateStoreError> {
        if self
            .fail_load
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(crate::SessionStateStoreError::Storage(
                "injected probe read failure".into(),
            ));
        }
        if self.missing.lock().unwrap().as_ref() == Some(id) {
            return Ok(None);
        }
        if let Some((expected, object)) = self.substitute.lock().unwrap().as_ref()
            && expected == id
        {
            return Ok(Some(object.clone()));
        }
        self.inner.load_object(key, generation, id)
    }

    fn put_object(
        &self,
        object: crate::SessionObject,
    ) -> Result<crate::ObjectPut, crate::SessionStateStoreError> {
        self.inner.put_object(object)
    }

    fn compare_and_swap_manifest(
        &self,
        request: crate::PreparedManifestCas,
    ) -> Result<crate::ManifestCas, crate::SessionStateStoreError> {
        self.inner.compare_and_swap_manifest(request)
    }

    fn compare_and_delete(
        &self,
        request: crate::PreparedSessionDelete,
    ) -> Result<crate::SessionDelete, crate::SessionStateStoreError> {
        self.inner.compare_and_delete(request)
    }
}

impl crate::SessionStateStore for AckLossStore {
    fn acquire_session_lease(
        &self,
        key: &crate::SessionKey,
    ) -> Result<Box<dyn crate::SessionStateLease>, crate::SessionStateStoreError> {
        self.inner.acquire_session_lease(key)
    }

    fn inspect_slot(
        &self,
        key: &crate::SessionKey,
    ) -> Result<crate::SessionSlot, crate::SessionStateStoreError> {
        if self
            .fail_next_inspect
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(crate::SessionStateStoreError::Storage(
                "injected acknowledgement-loss reread failure".into(),
            ));
        }
        self.inner.inspect_slot(key)
    }

    fn load_object(
        &self,
        key: &crate::SessionKey,
        generation: &crate::SessionGeneration,
        id: &crate::SessionObjectId,
    ) -> Result<Option<crate::SessionObject>, crate::SessionStateStoreError> {
        self.inner.load_object(key, generation, id)
    }

    fn put_object(
        &self,
        object: crate::SessionObject,
    ) -> Result<crate::ObjectPut, crate::SessionStateStoreError> {
        let result = self.inner.put_object(object)?;
        if self
            .fail_object_after_store
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(crate::SessionStateStoreError::Storage(
                "injected crash after object commit".into(),
            ));
        }
        let remaining = self
            .object_unknowns
            .load(std::sync::atomic::Ordering::Acquire);
        if remaining > 0 {
            self.object_unknowns
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            Ok(crate::ObjectPut::CommitUnknown)
        } else {
            Ok(result)
        }
    }

    fn compare_and_swap_manifest(
        &self,
        request: crate::PreparedManifestCas,
    ) -> Result<crate::ManifestCas, crate::SessionStateStoreError> {
        let result = self.inner.compare_and_swap_manifest(request)?;
        if self
            .manifest_unreconciled
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.fail_next_inspect
                .store(true, std::sync::atomic::Ordering::Release);
            return Ok(crate::ManifestCas::CommitUnknown);
        }
        if self
            .manifest_unknown
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            Ok(crate::ManifestCas::CommitUnknown)
        } else {
            Ok(result)
        }
    }

    fn compare_and_delete(
        &self,
        request: crate::PreparedSessionDelete,
    ) -> Result<crate::SessionDelete, crate::SessionStateStoreError> {
        self.inner.compare_and_delete(request)
    }
}

#[async_trait::async_trait]
impl crate::CompactionObserver for Observer {
    async fn intent(
        &self,
        intent: crate::CompactionIntent,
    ) -> Result<crate::CompactionAcknowledgement, crate::CompactionObserverError> {
        self.intents.lock().unwrap().push(intent.clone());
        if self
            .reject_intent
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::CompactionObserverError::new(
                crate::CompactionObserverErrorCode::Rejected,
            ));
        }
        let mut acknowledgement = crate::CompactionAcknowledgement::for_intent(&intent);
        if self
            .mismatch_intent
            .load(std::sync::atomic::Ordering::Acquire)
        {
            acknowledgement.observation_digest = crate::CompactionDigest::domain_hash(
                "test.compaction.mismatched-intent",
                b"mismatch",
            );
        }
        Ok(acknowledgement)
    }

    async fn outcome(
        &self,
        outcome: crate::CompactionOutcome,
    ) -> Result<crate::CompactionAcknowledgement, crate::CompactionObserverError> {
        self.outcomes.lock().unwrap().push(outcome.clone());
        if self.fail_outcome.load(std::sync::atomic::Ordering::Acquire) {
            return Err(crate::CompactionObserverError::new(
                crate::CompactionObserverErrorCode::Unavailable,
            ));
        }
        let mut acknowledgement = crate::CompactionAcknowledgement::for_outcome(&outcome);
        if self
            .mismatch_outcome
            .load(std::sync::atomic::Ordering::Acquire)
        {
            acknowledgement.observation_digest = crate::CompactionDigest::domain_hash(
                "test.compaction.mismatched-outcome",
                b"mismatch",
            );
        }
        Ok(acknowledgement)
    }
}

fn facts(domain: &str, bytes: &[u8], item_count: u32) -> NativeCompactionDigestFacts {
    let facts = crate::CompactionContentFacts::from_bytes(domain, bytes, item_count);
    NativeCompactionDigestFacts {
        digest: facts.digest.as_str().to_owned(),
        size_bytes: facts.size_bytes,
        item_count,
    }
}

fn input(message: &[u8]) -> NativeCompactionInput {
    input_for(
        NativeCompactionOwner::Turn {
            turn_id: "turn".into(),
        },
        message,
    )
}

fn input_for(owner: NativeCompactionOwner, message: &[u8]) -> NativeCompactionInput {
    NativeCompactionInput {
        owner,
        reason: NativeCompactionReason::AutomaticThreshold,
        path: NativeCompactionRequestPath::SinglePassVerbatim,
        messages: facts("messages", message, 1),
        tool_definitions: facts("tools", b"tools", 1),
        hosted_tool_declarations: facts("hosted", b"hosted", 1),
        model_parameters: facts("model", b"model", 1),
    }
}

fn create_session(
    store: Arc<dyn crate::SessionStateStore>,
    observer: Arc<Observer>,
    identity: &str,
) -> Arc<dyn xai_grok_shell::session::state_authority::NativeSession> {
    let authority = SessionStateAuthorityBridge {
        store,
        observer: Some(observer),
        correlations: Arc::new(std::sync::Mutex::new(HashMap::new())),
    };
    authority
        .create(SessionIdentity {
            identity: identity.into(),
            generation: "generation".into(),
        })
        .unwrap()
}

async fn publish_test_compaction(
    session: &Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
    compaction_id: &str,
    checkpoint_payload: Vec<u8>,
    prompt_index: u64,
) {
    session
        .publish_compaction(NativeCompactionPublication {
            record: NativeCompactionPublicationRecord {
                compaction_id: compaction_id.into(),
                summary: facts(crate::COMPACTION_SUMMARY_DIGEST_DOMAIN, b"summary", 1),
                checkpoint: facts(
                    crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
                    &checkpoint_payload,
                    1,
                ),
                installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1),
                prompt_index,
            },
            name: format!("compaction_checkpoints/{compaction_id}.json"),
            payload: checkpoint_payload,
            marker: format!("marker-{compaction_id}").into_bytes(),
        })
        .await
        .unwrap();
    session
        .compaction_applied(compaction_id.into())
        .await
        .unwrap();
}

#[tokio::test]
async fn owner_correlation_is_manual_ordinary_or_sdk_claimed_autonomous() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let observer = Arc::new(Observer::default());
    let correlations = Arc::new(std::sync::Mutex::new(HashMap::new()));
    correlations.lock().unwrap().insert(
        ("autonomous".into(), "turn".into()),
        AutonomousCompactionCorrelation {
            run: crate::run::RunId::new("run").unwrap(),
            iteration: crate::run::IterationId::new(4),
            operation: crate::run::OperationId::new("operation").unwrap(),
        },
    );
    let authority = SessionStateAuthorityBridge {
        store,
        observer: Some(observer.clone()),
        correlations,
    };
    let mut sessions = Vec::new();
    for identity in ["manual", "ordinary", "autonomous"] {
        sessions.push(
            authority
                .create(SessionIdentity {
                    identity: identity.into(),
                    generation: format!("{identity}-generation"),
                })
                .unwrap(),
        );
    }

    let inputs = [
        input_for(NativeCompactionOwner::Session, b"manual"),
        input(b"ordinary"),
        input(b"autonomous"),
    ];
    for (session, input) in sessions.iter().zip(inputs) {
        let NativeCompactionBegin::Acknowledged { compaction_id } =
            session.begin_compaction(input).await.unwrap()
        else {
            panic!("protocol unexpectedly disabled")
        };
        session
            .compaction_not_applied(compaction_id, NativeCompactionNotAppliedReason::Cancelled)
            .await
            .unwrap();
    }

    let intents = observer.intents.lock().unwrap();
    assert!(matches!(
        intents[0].owner,
        crate::CompactionOwner::Session { .. }
    ));
    assert!(matches!(
        intents[1].owner,
        crate::CompactionOwner::Turn { .. }
    ));
    assert!(matches!(
        &intents[2].owner,
        crate::CompactionOwner::AutonomousTurn {
            run,
            iteration,
            operation,
            ..
        } if run.as_str() == "run" && iteration.get() == 4 && operation.as_str() == "operation"
    ));
}

#[tokio::test]
async fn every_not_applied_reason_commits_no_checkpoint_publication() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let observer = Arc::new(Observer::default());
    let reasons = [
        NativeCompactionNotAppliedReason::Cancelled,
        NativeCompactionNotAppliedReason::ModelFailed,
        NativeCompactionNotAppliedReason::InvalidModelOutput,
        NativeCompactionNotAppliedReason::InputChanged,
        NativeCompactionNotAppliedReason::PublicationAbsent,
        NativeCompactionNotAppliedReason::InterruptedBeforePublication,
    ];
    for (index, reason) in reasons.iter().copied().enumerate() {
        let identity = format!("not-applied-{index}");
        let session = create_session(store.clone(), observer.clone(), &identity);
        let NativeCompactionBegin::Acknowledged { compaction_id } = session
            .begin_compaction(input(identity.as_bytes()))
            .await
            .unwrap()
        else {
            panic!("protocol unexpectedly disabled")
        };
        session
            .compaction_not_applied(compaction_id, reason)
            .await
            .unwrap();
        let crate::SessionSlot::Live(document) = store
            .inspect_slot(&crate::SessionKey::new(identity).unwrap())
            .unwrap()
        else {
            panic!("session must remain live")
        };
        assert_eq!(document.manifest().head(), None);
        assert_eq!(document.manifest().segment_count(), 0);
        assert_eq!(
            document.manifest().compaction_state(),
            &crate::CompactionManifestState::None,
        );
    }
    assert_eq!(observer.outcomes.lock().unwrap().len(), reasons.len());
}

#[tokio::test]
async fn mismatched_acknowledgements_fence_and_retry_the_exact_payload() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer.clone(), "mismatched-ack");
    observer
        .mismatch_intent
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(session.begin_compaction(input(b"request")).await.is_err());
    let key = crate::SessionKey::new("mismatched-ack").unwrap();
    let crate::SessionSlot::Live(document) = store.inspect_slot(&key).unwrap() else {
        panic!("session must remain live")
    };
    assert!(matches!(
        document.manifest().compaction_state(),
        crate::CompactionManifestState::IntentPending(_)
    ));

    observer
        .mismatch_intent
        .store(false, std::sync::atomic::Ordering::Release);
    let NativeCompactionBegin::Acknowledged { compaction_id } =
        session.begin_compaction(input(b"request")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    let checkpoint = b"checkpoint".to_vec();
    session
        .publish_compaction(NativeCompactionPublication {
            record: NativeCompactionPublicationRecord {
                compaction_id: compaction_id.clone(),
                summary: facts(crate::COMPACTION_SUMMARY_DIGEST_DOMAIN, b"summary", 1),
                checkpoint: facts(crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN, &checkpoint, 1),
                installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1),
                prompt_index: 1,
            },
            name: "compaction_checkpoints/mismatched.json".into(),
            payload: checkpoint,
            marker: b"marker".to_vec(),
        })
        .await
        .unwrap();
    observer
        .mismatch_outcome
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(
        session
            .compaction_applied(compaction_id.clone())
            .await
            .is_err()
    );
    assert!(matches!(
        session.recover_compaction().await.unwrap(),
        NativeCompactionRecovery::EvidencePending { .. }
    ));
    observer
        .mismatch_outcome
        .store(false, std::sync::atomic::Ordering::Release);
    session.compaction_applied(compaction_id).await.unwrap();
    let intents = observer.intents.lock().unwrap().clone();
    assert_eq!(intents[0], intents[1]);
    let outcomes = observer.outcomes.lock().unwrap().clone();
    assert_eq!(outcomes[0], outcomes[1]);
}

#[tokio::test]
async fn native_compaction_is_idempotent_fenced_and_recoverable() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer.clone(), "session");

    let first = session.begin_compaction(input(b"request")).await.unwrap();
    let NativeCompactionBegin::Acknowledged { compaction_id } = first else {
        panic!("protocol unexpectedly disabled")
    };
    let retry = session.begin_compaction(input(b"request")).await.unwrap();
    assert_eq!(
        retry,
        NativeCompactionBegin::Acknowledged {
            compaction_id: compaction_id.clone()
        }
    );
    assert_eq!(observer.intents.lock().unwrap().len(), 2);
    assert!(session.begin_compaction(input(b"changed")).await.is_err());
    assert_eq!(observer.intents.lock().unwrap().len(), 2);

    let intent = observer.intents.lock().unwrap()[0].clone();
    let probe = intent.probe();
    let checkpoint_payload = b"exact checkpoint".to_vec();
    let publication = NativeCompactionPublication {
        record: NativeCompactionPublicationRecord {
            compaction_id: compaction_id.clone(),
            summary: facts(crate::COMPACTION_SUMMARY_DIGEST_DOMAIN, b"summary", 1),
            checkpoint: facts(
                crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
                &checkpoint_payload,
                1,
            ),
            installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1),
            prompt_index: 4,
        },
        name: "compaction_checkpoints/checkpoint.json".into(),
        payload: checkpoint_payload.clone(),
        marker: b"marker".to_vec(),
    };
    session
        .publish_compaction(publication.clone())
        .await
        .unwrap();
    session
        .publish_compaction(publication.clone())
        .await
        .expect("identical publication retry must reconcile");
    let mut conflict = publication;
    conflict.marker.push(b'!');
    assert!(
        session.publish_compaction(conflict).await.is_err(),
        "same ID with different publication bytes must fail closed"
    );

    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::Applied {
            relation: crate::CompactionTimelineRelation::Current,
            ..
        }
    ));
    assert_eq!(observer.outcomes.lock().unwrap().len(), 0);
    assert_eq!(
        session.recover_compaction().await.unwrap(),
        NativeCompactionRecovery::EvidencePending {
            compaction_id: compaction_id.clone(),
            checkpoint_payload,
            installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1,),
        }
    );
    observer
        .fail_outcome
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(
        session
            .compaction_applied(compaction_id.clone())
            .await
            .is_err(),
        "unacknowledged Applied evidence must fence"
    );
    assert!(matches!(
        session.recover_compaction().await.unwrap(),
        NativeCompactionRecovery::EvidencePending { .. }
    ));
    observer
        .fail_outcome
        .store(false, std::sync::atomic::Ordering::Release);
    session
        .compaction_applied(compaction_id.clone())
        .await
        .unwrap();
    assert!(matches!(
        &observer.outcomes.lock().unwrap()[0],
        crate::CompactionOutcome::Applied { receipt }
            if receipt.intent.id.as_str() == compaction_id
    ));
    let outcomes = observer.outcomes.lock().unwrap();
    assert_eq!(
        outcomes[0], outcomes[1],
        "outcome callback retry must carry the exact same payload"
    );
    drop(outcomes);
    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe),
        crate::CompactionProbeResult::Applied { .. }
    ));
}

#[tokio::test]
async fn recovery_classifies_unpublished_intent_as_not_applied() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer.clone(), "unpublished");
    let NativeCompactionBegin::Acknowledged { .. } =
        session.begin_compaction(input(b"request")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    let probe = observer.intents.lock().unwrap()[0].probe();
    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::NotPublished { .. }
    ));
    assert_eq!(
        session.recover_compaction().await.unwrap(),
        NativeCompactionRecovery::None
    );
    assert!(matches!(
        &observer.outcomes.lock().unwrap()[0],
        crate::CompactionOutcome::NotApplied {
            reason: crate::CompactionNotAppliedReason::InterruptedBeforePublication,
            ..
        }
    ));
    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::BaseNotInAncestry,
        }
    ));
}

#[tokio::test]
async fn recovery_retries_the_exact_durable_not_applied_outcome() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer.clone(), "not-applied-retry");
    let NativeCompactionBegin::Acknowledged { compaction_id } =
        session.begin_compaction(input(b"request")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    observer
        .fail_outcome
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(
        session
            .compaction_not_applied(compaction_id, NativeCompactionNotAppliedReason::ModelFailed,)
            .await
            .is_err(),
        "lost Host acknowledgement must leave durable outcome evidence",
    );
    let key = crate::SessionKey::new("not-applied-retry").unwrap();
    let crate::SessionSlot::Live(pending) = store.inspect_slot(&key).unwrap() else {
        panic!("session must remain live")
    };
    assert!(matches!(
        pending.manifest().compaction_state(),
        crate::CompactionManifestState::NotAppliedPending {
            reason: crate::CompactionNotAppliedReason::ModelFailed,
            ..
        }
    ));

    observer
        .fail_outcome
        .store(false, std::sync::atomic::Ordering::Release);
    assert_eq!(
        session.recover_compaction().await.unwrap(),
        NativeCompactionRecovery::None,
    );
    let outcomes = observer.outcomes.lock().unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(
        outcomes[0], outcomes[1],
        "recovery must not replace the known model-failure outcome with an interruption",
    );
    drop(outcomes);
    let crate::SessionSlot::Live(recovered) = store.inspect_slot(&key).unwrap() else {
        panic!("session must remain live")
    };
    assert_eq!(
        recovered.manifest().compaction_state(),
        &crate::CompactionManifestState::None,
    );
}

#[tokio::test]
async fn compound_publication_reconciles_object_and_manifest_ack_loss() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(AckLossStore {
        inner: crate::LocalSessionStateStore::new(root.path()).unwrap(),
        object_unknowns: std::sync::atomic::AtomicUsize::new(0),
        manifest_unknown: std::sync::atomic::AtomicBool::new(false),
        fail_object_after_store: std::sync::atomic::AtomicBool::new(false),
        manifest_unreconciled: std::sync::atomic::AtomicBool::new(false),
        fail_next_inspect: std::sync::atomic::AtomicBool::new(false),
    });
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer, "ack-loss");
    let NativeCompactionBegin::Acknowledged { compaction_id } =
        session.begin_compaction(input(b"request")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    store
        .object_unknowns
        .store(2, std::sync::atomic::Ordering::Release);
    store
        .manifest_unknown
        .store(true, std::sync::atomic::Ordering::Release);
    let checkpoint_payload = b"checkpoint after unknown acknowledgements".to_vec();
    session
        .publish_compaction(NativeCompactionPublication {
            record: NativeCompactionPublicationRecord {
                compaction_id: compaction_id.clone(),
                summary: facts(crate::COMPACTION_SUMMARY_DIGEST_DOMAIN, b"summary", 1),
                checkpoint: facts(
                    crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
                    &checkpoint_payload,
                    1,
                ),
                installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1),
                prompt_index: 1,
            },
            name: "compaction_checkpoints/ack-loss.json".into(),
            payload: checkpoint_payload.clone(),
            marker: b"marker".to_vec(),
        })
        .await
        .expect("exact rereads must reconcile every unknown acknowledgement");
    assert_eq!(
        session.recover_compaction().await.unwrap(),
        NativeCompactionRecovery::EvidencePending {
            compaction_id,
            checkpoint_payload,
            installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1),
        }
    );
}

#[tokio::test]
async fn crash_after_checkpoint_object_proves_absence_before_not_applied() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(AckLossStore {
        inner: crate::LocalSessionStateStore::new(root.path()).unwrap(),
        object_unknowns: std::sync::atomic::AtomicUsize::new(0),
        manifest_unknown: std::sync::atomic::AtomicBool::new(false),
        fail_object_after_store: std::sync::atomic::AtomicBool::new(false),
        manifest_unreconciled: std::sync::atomic::AtomicBool::new(false),
        fail_next_inspect: std::sync::atomic::AtomicBool::new(false),
    });
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer.clone(), "checkpoint-crash");
    let NativeCompactionBegin::Acknowledged { compaction_id } =
        session.begin_compaction(input(b"request")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    let probe = observer.intents.lock().unwrap()[0].probe();
    store
        .fail_object_after_store
        .store(true, std::sync::atomic::Ordering::Release);
    let checkpoint_payload = b"orphan checkpoint".to_vec();
    assert!(
        session
            .publish_compaction(NativeCompactionPublication {
                record: NativeCompactionPublicationRecord {
                    compaction_id,
                    summary: facts(crate::COMPACTION_SUMMARY_DIGEST_DOMAIN, b"summary", 1,),
                    checkpoint: facts(
                        crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
                        &checkpoint_payload,
                        1,
                    ),
                    installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1,),
                    prompt_index: 1,
                },
                name: "compaction_checkpoints/orphan.json".into(),
                payload: checkpoint_payload,
                marker: b"marker".to_vec(),
            })
            .await
            .is_err()
    );
    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::NotPublished { .. }
    ));
    assert_eq!(
        session.recover_compaction().await.unwrap(),
        NativeCompactionRecovery::None
    );
    assert!(matches!(
        &observer.outcomes.lock().unwrap()[0],
        crate::CompactionOutcome::NotApplied {
            reason: crate::CompactionNotAppliedReason::InterruptedBeforePublication,
            ..
        }
    ));
    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::BaseNotInAncestry,
        }
    ));
}

#[tokio::test]
async fn unreconciled_manifest_commit_recovers_published_evidence() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(AckLossStore {
        inner: crate::LocalSessionStateStore::new(root.path()).unwrap(),
        object_unknowns: std::sync::atomic::AtomicUsize::new(0),
        manifest_unknown: std::sync::atomic::AtomicBool::new(false),
        fail_object_after_store: std::sync::atomic::AtomicBool::new(false),
        manifest_unreconciled: std::sync::atomic::AtomicBool::new(false),
        fail_next_inspect: std::sync::atomic::AtomicBool::new(false),
    });
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer, "manifest-unknown");
    let NativeCompactionBegin::Acknowledged { compaction_id } =
        session.begin_compaction(input(b"request")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    store
        .manifest_unreconciled
        .store(true, std::sync::atomic::Ordering::Release);
    let checkpoint_payload = b"durable checkpoint".to_vec();
    assert!(
        session
            .publish_compaction(NativeCompactionPublication {
                record: NativeCompactionPublicationRecord {
                    compaction_id: compaction_id.clone(),
                    summary: facts(crate::COMPACTION_SUMMARY_DIGEST_DOMAIN, b"summary", 1,),
                    checkpoint: facts(
                        crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
                        &checkpoint_payload,
                        1,
                    ),
                    installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1,),
                    prompt_index: 1,
                },
                name: "compaction_checkpoints/durable.json".into(),
                payload: checkpoint_payload.clone(),
                marker: b"marker".to_vec(),
            })
            .await
            .is_err(),
        "unknown manifest acknowledgement with failed reread must fence"
    );
    assert_eq!(
        session.recover_compaction().await.unwrap(),
        NativeCompactionRecovery::EvidencePending {
            compaction_id,
            checkpoint_payload,
            installed_state: facts(crate::COMPACTION_STATE_DIGEST_DOMAIN, b"installed", 1),
        }
    );
}

#[tokio::test]
async fn rejected_intent_restores_logical_state_and_pending_intent_fences_updates() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer.clone(), "rejection");
    observer
        .reject_intent
        .store(true, std::sync::atomic::Ordering::Release);
    let error = session
        .begin_compaction(input(b"rejected"))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind,
        xai_grok_shell::session::state_authority::NativeCompactionErrorKind::Rejected
    );
    let key = crate::SessionKey::new("rejection").unwrap();
    let crate::SessionSlot::Live(document) = store.inspect_slot(&key).unwrap() else {
        panic!("session must remain live")
    };
    assert!(matches!(
        document.manifest().compaction_state(),
        crate::CompactionManifestState::None
    ));

    observer
        .reject_intent
        .store(false, std::sync::atomic::Ordering::Release);
    let NativeCompactionBegin::Acknowledged { compaction_id } =
        session.begin_compaction(input(b"accepted")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    session
        .stage_update(b"must remain staged".to_vec())
        .unwrap();
    assert!(
        session.flush().is_err(),
        "pending intent must fence ordinary chain updates"
    );
    session
        .compaction_not_applied(
            compaction_id,
            xai_grok_shell::session::state_authority::NativeCompactionNotAppliedReason::Cancelled,
        )
        .await
        .unwrap();
    session.flush().unwrap();
    let crate::SessionSlot::Live(document) = store.inspect_slot(&key).unwrap() else {
        panic!("session must remain live")
    };
    assert_eq!(document.manifest().segment_count(), 1);
}

#[tokio::test]
async fn probe_preserves_rewind_supersession_prompt_reuse_and_fork_identity() {
    use xai_grok_shell::session::state_authority::{ReplayRecord, RewindOperation};

    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let observer = Arc::new(Observer::default());
    let authority = SessionStateAuthorityBridge {
        store: store.clone(),
        observer: Some(observer.clone()),
        correlations: Arc::new(std::sync::Mutex::new(HashMap::new())),
    };
    let origin = authority
        .create(SessionIdentity {
            identity: "origin".into(),
            generation: "origin-generation".into(),
        })
        .unwrap();
    origin.stage_update(b"base".to_vec()).unwrap();
    origin.flush().unwrap();

    let NativeCompactionBegin::Acknowledged {
        compaction_id: first_id,
    } = origin.begin_compaction(input(b"first")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    let first_probe = observer.intents.lock().unwrap()[0].probe();
    publish_test_compaction(&origin, &first_id, b"first-checkpoint".to_vec(), 9).await;
    origin
        .publish_rewind(
            RewindOperation::Truncate {
                index: 9,
                payload: b"rewind".to_vec(),
            },
            b"rewind-marker".to_vec(),
        )
        .unwrap();
    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), first_probe.clone()),
        crate::CompactionProbeResult::Applied {
            relation: crate::CompactionTimelineRelation::Rewound { .. },
            ..
        }
    ));

    let NativeCompactionBegin::Acknowledged {
        compaction_id: second_id,
    } = origin.begin_compaction(input(b"second")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    publish_test_compaction(&origin, &second_id, b"second-checkpoint".to_vec(), 9).await;
    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), first_probe.clone()),
        crate::CompactionProbeResult::Applied {
            relation: crate::CompactionTimelineRelation::Superseded { by },
            ..
        } if by.as_str() == second_id
    ));

    let records = origin.replay_page(None, 4096).unwrap().records;
    assert!(
        records
            .iter()
            .any(|record| matches!(record, ReplayRecord::Compaction { .. }))
    );
    authority
        .publish_fork(
            SessionIdentity {
                identity: "fork".into(),
                generation: "fork-generation".into(),
            },
            records,
        )
        .unwrap();
    let fork_probe = first_probe.clone().in_session(
        crate::SessionId::from_stored("fork"),
        crate::SessionGeneration::new("fork-generation").unwrap(),
    );
    assert!(matches!(
        crate::private::runtime::probe_compaction(store.as_ref(), fork_probe),
        crate::CompactionProbeResult::Applied {
            relation: crate::CompactionTimelineRelation::Forked { origin_session },
            ..
        } if origin_session.as_str() == "origin"
    ));
    let wrong_generation = first_probe.in_session(
        crate::SessionId::from_stored("origin"),
        crate::SessionGeneration::new("wrong-generation").unwrap(),
    );
    assert_eq!(
        crate::private::runtime::probe_compaction(store.as_ref(), wrong_generation),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::GenerationMismatch,
        }
    );
}

#[tokio::test]
async fn probe_fails_closed_for_missing_corrupt_and_store_faults() {
    let root = tempfile::TempDir::new().unwrap();
    let store = Arc::new(ProbeFaultStore {
        inner: crate::LocalSessionStateStore::new(root.path()).unwrap(),
        missing: std::sync::Mutex::new(None),
        substitute: std::sync::Mutex::new(None),
        slot_override: std::sync::Mutex::new(None),
        fail_load: std::sync::atomic::AtomicBool::new(false),
    });
    let observer = Arc::new(Observer::default());
    let session = create_session(store.clone(), observer.clone(), "probe-faults");
    let NativeCompactionBegin::Acknowledged { compaction_id } =
        session.begin_compaction(input(b"request")).await.unwrap()
    else {
        panic!("protocol unexpectedly disabled")
    };
    let probe = observer.intents.lock().unwrap()[0].probe();
    publish_test_compaction(&session, &compaction_id, b"checkpoint".to_vec(), 3).await;
    let receipt = match crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()) {
        crate::CompactionProbeResult::Applied { receipt, .. } => receipt,
        result => panic!("expected Applied before injecting faults, got {result:?}"),
    };

    *store.missing.lock().unwrap() = Some(receipt.publication.publication.clone());
    assert_eq!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::MissingObject,
        }
    );
    *store.missing.lock().unwrap() = None;

    *store.missing.lock().unwrap() = Some(receipt.publication.checkpoint.clone());
    assert_eq!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::MissingObject,
        }
    );
    *store.missing.lock().unwrap() = None;

    let key = crate::SessionKey::new("probe-faults").unwrap();
    let wrong_object = store
        .inner
        .load_object(
            &key,
            &crate::SessionGeneration::new("generation").unwrap(),
            &receipt.publication.publication,
        )
        .unwrap()
        .unwrap();
    *store.substitute.lock().unwrap() =
        Some((receipt.publication.checkpoint.clone(), wrong_object));
    assert_eq!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::CorruptObject,
        }
    );
    *store.substitute.lock().unwrap() = None;

    let crate::SessionSlot::Live(current) = store.inner.inspect_slot(&key).unwrap() else {
        panic!("session must remain live")
    };
    let malformed = crate::SessionManifest::new(
        key.clone(),
        current.manifest().generation().clone(),
        current.manifest().head().cloned(),
        current.manifest().segment_count(),
        current.manifest().transcript_bytes() + 1,
    )
    .unwrap();
    let malformed = crate::LiveSessionDocument::from_stored(
        crate::ManifestVersion::from_stored(current.version().revision(), malformed.digest())
            .unwrap(),
        malformed,
    )
    .unwrap();
    *store.slot_override.lock().unwrap() = Some(malformed);
    assert_eq!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::CorruptObject,
        }
    );
    *store.slot_override.lock().unwrap() = None;

    let gap = crate::SessionManifest::new(
        key,
        current.manifest().generation().clone(),
        current.manifest().head().cloned(),
        current.manifest().segment_count() + 1,
        current.manifest().transcript_bytes(),
    )
    .unwrap();
    let gap = crate::LiveSessionDocument::from_stored(
        crate::ManifestVersion::from_stored(current.version().revision(), gap.digest()).unwrap(),
        gap,
    )
    .unwrap();
    *store.slot_override.lock().unwrap() = Some(gap);
    assert_eq!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::CorruptObject,
        }
    );
    *store.slot_override.lock().unwrap() = None;

    store
        .fail_load
        .store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(
        crate::private::runtime::probe_compaction(store.as_ref(), probe.clone()),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::StoreFailure,
        }
    );

    let mut conflicting_probe = probe;
    conflicting_probe.intent_digest =
        crate::CompactionDigest::domain_hash("test.conflicting-intent", b"different");
    assert_eq!(
        crate::private::runtime::probe_compaction(store.as_ref(), conflicting_probe),
        crate::CompactionProbeResult::Uncertain {
            reason: crate::CompactionProbeUncertainty::ConflictingPublication,
        }
    );
}
