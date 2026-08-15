use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_session_evidence_store_replaces_all_default_sdk_evidence_files() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let host_runs =
        Arc::new(run::LocalRunStore::new(root.path().join("host-runs")).expect("Host Run store"));
    let host_evidence = Arc::new(
        LocalSessionEvidenceStore::new(root.path().join("host-session-evidence"))
            .expect("Host evidence store"),
    );
    let (runtime, _) = Runtime::start_with_stores(config.clone(), host_runs, host_evidence.clone())
        .await
        .expect("runtime starts with Host authorities");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    runtime
        .prompt(&session, "host-evidence-turn", "evidence")
        .await
        .expect("ledger settles");
    let ledger = host_evidence
        .load(&SessionEvidenceKey {
            kind: SessionEvidenceKind::Ledger,
            identity: session.as_str().into(),
        })
        .expect("Host authority loads")
        .expect("Host ledger exists");
    assert!(ledger.version.validates(&ledger.bytes));
    for directory in [
        "origin-turn-ledger",
        "origin-rewind-receipts",
        "origin-harness-turn-bindings",
    ] {
        assert!(
            !config.session_storage.join(directory).exists(),
            "injected evidence authority must replace default {directory}"
        );
    }
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_session_state_store_replaces_covered_jsonl_and_restarts() {
    fn assert_no_covered_files(root: &std::path::Path) {
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                assert_ne!(
                    path.file_name().and_then(|name| name.to_str()),
                    Some("compaction_checkpoints"),
                    "Host mode must not create covered checkpoint directories"
                );
                assert_no_covered_files(&path);
            } else {
                assert!(
                    !matches!(
                        path.file_name().and_then(|name| name.to_str()),
                        Some("updates.jsonl" | "chat_history.jsonl" | "rewind_points.jsonl")
                    ),
                    "Host mode created covered file {}",
                    path.display()
                );
            }
        }
    }

    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let store = Arc::new(
        LocalSessionStateStore::new(root.path().join("host-native-sessions"))
            .expect("Host Session store"),
    );
    let (runtime, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("runtime starts with Host Session authority");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");
    let requested = SessionId::from_stored(uuid::Uuid::new_v4().to_string());
    let requested_config = session_config(workspace.clone());
    assert_eq!(
        runtime
            .create_session_with_id(requested.clone(), requested_config.clone())
            .await
            .expect("caller-selected identity is created"),
        requested
    );
    assert_eq!(
        runtime
            .create_session_with_id(requested.clone(), requested_config.clone())
            .await
            .expect("same exact create retries idempotently"),
        requested
    );
    let mut conflicting_config = requested_config.clone();
    conflicting_config.rules = Some("different exact config".into());
    assert!(matches!(
        runtime
            .create_session_with_id(requested.clone(), conflicting_config)
            .await,
        Err(Error::InvalidConfig(_))
    ));
    runtime
        .prompt(&session, "host-session-state-turn", "state")
        .await
        .expect("turn settles");
    runtime.shutdown().await.expect("runtime shuts down");
    assert_no_covered_files(&config.session_storage);

    let (runtime, _) = Runtime::builder(config.clone())
        .session_state_store(store)
        .start()
        .await
        .expect("runtime restarts with Host Session authority");
    runtime
        .load_session(session.clone(), session_config(workspace))
        .await
        .expect("session reloads from Host authority without JSONL fallback");
    assert_eq!(
        runtime
            .create_session_with_id(requested.clone(), requested_config.clone())
            .await
            .expect("exact caller-selected identity reopens after restart"),
        requested
    );
    runtime
        .shutdown()
        .await
        .expect("restarted runtime shuts down");
    assert_no_covered_files(&config.session_storage);
}

struct DeleteProbeStore {
    inner: LocalSessionStateStore,
    behavior: Mutex<DeleteProbeBehavior>,
    sidecar: Mutex<Option<std::path::PathBuf>>,
    delete_pause: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    inspect_pause: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    lease_pause: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    inspect_calls: std::sync::atomic::AtomicU64,
    lease_conflicts: std::sync::atomic::AtomicU64,
    observed_sidecar_before_delete: AtomicBool,
}

#[derive(Clone, Copy)]
enum DeleteProbeBehavior {
    Normal,
    CommitUnknown,
    Conflict,
}

impl DeleteProbeStore {
    fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: LocalSessionStateStore::new(root).expect("Host Session store"),
            behavior: Mutex::new(DeleteProbeBehavior::Normal),
            sidecar: Mutex::new(None),
            delete_pause: Mutex::new(None),
            inspect_pause: Mutex::new(None),
            lease_pause: Mutex::new(None),
            inspect_calls: std::sync::atomic::AtomicU64::new(0),
            lease_conflicts: std::sync::atomic::AtomicU64::new(0),
            observed_sidecar_before_delete: AtomicBool::new(false),
        }
    }

    fn set_behavior(&self, behavior: DeleteProbeBehavior) {
        *self.behavior.lock().unwrap() = behavior;
    }

    fn observe_sidecar(&self, path: std::path::PathBuf) {
        *self.sidecar.lock().unwrap() = Some(path);
    }

    fn pause_next_delete(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *self.delete_pause.lock().unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    fn pause_next_inspect(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *self.inspect_pause.lock().unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    fn pause_next_lease(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *self.lease_pause.lock().unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }
}

impl SessionStateStore for DeleteProbeStore {
    fn acquire_session_lease(
        &self,
        key: &SessionKey,
    ) -> Result<Box<dyn SessionStateLease>, SessionStateStoreError> {
        let result = self.inner.acquire_session_lease(key);
        if result.is_err() {
            self.lease_conflicts.fetch_add(1, Ordering::AcqRel);
        } else if let Some((entered, release)) = self.lease_pause.lock().unwrap().take() {
            entered.send(()).expect("lease observer remains live");
            release.recv().expect("lease release remains live");
        }
        result
    }

    fn inspect_slot(&self, key: &SessionKey) -> Result<SessionSlot, SessionStateStoreError> {
        self.inspect_calls.fetch_add(1, Ordering::AcqRel);
        if let Some((entered, release)) = self.inspect_pause.lock().unwrap().take() {
            entered.send(()).expect("inspect observer remains live");
            release.recv().expect("inspect release remains live");
        }
        self.inner.inspect_slot(key)
    }

    fn load_object(
        &self,
        key: &SessionKey,
        generation: &SessionGeneration,
        id: &SessionObjectId,
    ) -> Result<Option<SessionObject>, SessionStateStoreError> {
        self.inner.load_object(key, generation, id)
    }

    fn put_object(&self, object: SessionObject) -> Result<ObjectPut, SessionStateStoreError> {
        self.inner.put_object(object)
    }

    fn compare_and_swap_manifest(
        &self,
        request: PreparedManifestCas,
    ) -> Result<ManifestCas, SessionStateStoreError> {
        self.inner.compare_and_swap_manifest(request)
    }

    fn compare_and_delete(
        &self,
        request: PreparedSessionDelete,
    ) -> Result<SessionDelete, SessionStateStoreError> {
        if let Some((entered, release)) = self.delete_pause.lock().unwrap().take() {
            entered.send(()).expect("delete observer remains live");
            release.recv().expect("delete release remains live");
        }
        if self
            .sidecar
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|path| path.exists())
        {
            self.observed_sidecar_before_delete
                .store(true, Ordering::Release);
        }
        match *self.behavior.lock().unwrap() {
            DeleteProbeBehavior::Normal => self.inner.compare_and_delete(request),
            DeleteProbeBehavior::CommitUnknown => {
                let result = self.inner.compare_and_delete(request)?;
                assert!(matches!(result, SessionDelete::Deleted(_)));
                Ok(SessionDelete::CommitUnknown)
            }
            DeleteProbeBehavior::Conflict => Ok(SessionDelete::Conflict),
        }
    }
}

fn find_session_dir(root: &std::path::Path, session: &SessionId) -> std::path::PathBuf {
    fn visit(root: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
        for entry in std::fs::read_dir(root).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|value| value.to_str()) == Some(name) {
                    return Some(path);
                }
                if let Some(found) = visit(&path, name) {
                    return Some(found);
                }
            }
        }
        None
    }
    visit(root, session.as_str()).expect("native Session sidecar directory")
}

fn assert_no_covered_session_jsonl(root: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            assert_no_covered_session_jsonl(&path);
        } else {
            assert!(!matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("updates.jsonl" | "chat_history.jsonl" | "rewind_points.jsonl")
            ));
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_delete_live_commit_unknown_retry() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let store = Arc::new(DeleteProbeStore::new(root.path().join("host-state")));
    let (runtime, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("runtime starts");
    let id = SessionId::from_stored(uuid::Uuid::new_v4().to_string());
    let session_config = session_config(workspace);
    runtime
        .create_session_with_id(id.clone(), session_config.clone())
        .await
        .expect("session is live");
    let session_dir = find_session_dir(&config.session_storage, &id);
    let sidecar = session_dir.join("uncovered-sidecar");
    std::fs::write(&sidecar, b"sidecar").expect("sidecar");
    store.observe_sidecar(sidecar.clone());
    store.set_behavior(DeleteProbeBehavior::CommitUnknown);

    runtime
        .delete_session(id.clone())
        .await
        .expect("exact CommitUnknown tombstone reconciles");
    assert!(store.observed_sidecar_before_delete.load(Ordering::Acquire));
    assert!(!sidecar.exists(), "sidecars are removed after tombstoning");
    assert!(matches!(
        store
            .inspect_slot(&SessionKey::new(id.as_str()).unwrap())
            .unwrap(),
        SessionSlot::Tombstoned { .. }
    ));
    assert!(runtime.unload_session(id.clone()).await.is_err());
    assert!(
        runtime
            .create_session_with_id(id.clone(), session_config.clone())
            .await
            .is_err(),
        "a tombstoned identity cannot be recreated"
    );
    runtime.shutdown().await.expect("runtime shuts down");

    let (restarted, _) = Runtime::builder(config.clone())
        .session_state_store(store)
        .start()
        .await
        .expect("runtime restarts");
    restarted
        .delete_session(id)
        .await
        .expect("completed deletion retries idempotently after restart");
    restarted.shutdown().await.expect("runtime shuts down");
    assert_no_covered_session_jsonl(&config.session_storage);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_delete_of_unloaded_session_fails_closed_before_sidecar_cleanup() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let store = Arc::new(DeleteProbeStore::new(root.path().join("host-state")));
    let (runtime, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("runtime starts");
    let id = SessionId::from_stored(uuid::Uuid::new_v4().to_string());
    runtime
        .create_session_with_id(id.clone(), session_config(workspace))
        .await
        .expect("session is live");
    let sidecar = find_session_dir(&config.session_storage, &id).join("uncovered-sidecar");
    std::fs::write(&sidecar, b"sidecar").expect("sidecar");
    store.observe_sidecar(sidecar.clone());
    runtime
        .unload_session(id.clone())
        .await
        .expect("session unloads");
    store.set_behavior(DeleteProbeBehavior::Conflict);

    assert!(runtime.delete_session(id.clone()).await.is_err());
    assert!(sidecar.exists(), "authority conflict preserves sidecars");
    assert!(matches!(
        store
            .inspect_slot(&SessionKey::new(id.as_str()).unwrap())
            .unwrap(),
        SessionSlot::Live(_)
    ));
    assert!(runtime.unload_session(id.clone()).await.is_err());

    std::fs::remove_file(sidecar.parent().unwrap().join("summary.json"))
        .expect("remove summary to exercise ID-based cleanup");
    store.set_behavior(DeleteProbeBehavior::Normal);
    runtime
        .delete_session(id)
        .await
        .expect("unloaded Session deletes without a readable summary");
    assert!(!sidecar.exists());
    runtime.shutdown().await.expect("runtime shuts down");
    assert_no_covered_session_jsonl(&config.session_storage);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_store_fences_admission_from_unload_through_tombstone() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let store = Arc::new(DeleteProbeStore::new(root.path().join("host-state")));
    let (runtime_a, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime A starts");
    let (runtime_b, _) = Runtime::builder(config)
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime B starts");
    let id = SessionId::from_stored(uuid::Uuid::new_v4().to_string());
    let session = session_config(workspace);
    runtime_a
        .create_session_with_id(id.clone(), session.clone())
        .await
        .expect("Runtime A owns the live Session");
    let (delete_entered, release_delete) = store.pause_next_delete();
    let deleting = {
        let runtime = runtime_a.clone();
        let id = id.clone();
        tokio::spawn(async move {
            // Deletion first unloads the resident actor, and an unload whose
            // teardown misses its deadline retains the actor for a truthful
            // retry. Retrying here is the documented Host pattern; the store
            // pause still fires exactly once, on the attempt that reaches the
            // authority delete.
            loop {
                match runtime.delete_session(id.clone()).await {
                    Err(Error::Operation(message))
                        if message.contains("missed the teardown deadline") =>
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }
                    result => break result,
                }
            }
        })
    };
    tokio::task::spawn_blocking(move || {
        delete_entered.recv_timeout(std::time::Duration::from_secs(120))
    })
    .await
    .expect("delete observer joins")
    .expect("A pauses after unload and before tombstone");

    let inspections_before_b = store.inspect_calls.load(Ordering::Acquire);
    let admission = runtime_b.load_session(id.clone(), session.clone()).await;
    assert!(
        admission.is_err(),
        "Runtime B must not establish a live actor while A holds deletion admission"
    );
    assert_eq!(store.lease_conflicts.load(Ordering::Acquire), 1);
    assert_eq!(
        store.inspect_calls.load(Ordering::Acquire),
        inspections_before_b,
        "Runtime B must fail at admission before inspecting or opening authority state"
    );
    assert!(runtime_b.unload_session(id.clone()).await.is_err());
    release_delete.send(()).expect("release Runtime A");
    deleting
        .await
        .expect("delete task joins")
        .expect("Runtime A completes deletion");
    assert!(matches!(
        store
            .inspect_slot(&SessionKey::new(id.as_str()).unwrap())
            .unwrap(),
        SessionSlot::Tombstoned { .. }
    ));
    assert!(
        runtime_b.load_session(id.clone(), session).await.is_err(),
        "after A succeeds, B must observe the tombstone rather than become resident"
    );
    assert!(runtime_b.unload_session(id).await.is_err());
    runtime_a.shutdown().await.expect("Runtime A shuts down");
    runtime_b.shutdown().await.expect("Runtime B shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_holds_unloaded_source_and_target_leases_through_publication() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    let child_workspace = root.path().join("child-workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    std::fs::create_dir(&child_workspace).expect("child workspace");
    let config = runtime_config(&root, server.url());
    let store = Arc::new(DeleteProbeStore::new(root.path().join("host-state")));
    let (runtime_a, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime A starts");
    let (runtime_b, _) = Runtime::builder(config)
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime B starts");
    let source = runtime_a
        .create_session(session_config(workspace.clone()))
        .await
        .expect("source session");
    runtime_a
        .unload_session(source.clone())
        .await
        .expect("source unloads so fork must acquire it temporarily");
    let target = uuid::Uuid::now_v7().to_string();
    let request = ForkSessionRequest {
        source_cwd: workspace,
        new_cwd: child_workspace,
        new_session_id: Some(target.clone()),
        new_model_id: None,
        target_prompt_index: None,
        session_kind: None,
        source_workspace_dir: None,
    };
    let (inspect_entered, release_inspect) = store.pause_next_inspect();
    let forking = {
        let runtime = runtime_a.clone();
        let source = source.clone();
        tokio::spawn(async move { runtime.fork_session(&source, &request).await })
    };
    tokio::task::spawn_blocking(move || inspect_entered.recv())
        .await
        .expect("inspect observer joins")
        .expect("fork pauses after both leases and before authority traversal");

    assert!(
        runtime_b.delete_session(source.clone()).await.is_err(),
        "source deletion must fail fast while fork traverses source authority"
    );
    assert!(store.lease_conflicts.load(Ordering::Acquire) >= 1);
    release_inspect.send(()).expect("release fork inspection");
    let receipt = forking
        .await
        .expect("fork task joins")
        .expect("fork publishes while both identities remain fenced");
    assert_eq!(receipt.new_session_id.as_str(), target);
    assert_eq!(receipt.parent_session_id, source);

    runtime_a.shutdown().await.expect("Runtime A shuts down");
    runtime_b.shutdown().await.expect("Runtime B shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverse_forks_fail_fast_without_order_deadlock() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let store = Arc::new(DeleteProbeStore::new(root.path().join("host-state")));
    let (runtime_a, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime A starts");
    let (runtime_b, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime B starts");
    let low = SessionId::from_stored("00000000-0000-7000-8000-000000000001");
    let high = SessionId::from_stored("ffffffff-ffff-7fff-bfff-ffffffffffff");
    let session = session_config(workspace.clone());
    runtime_a
        .create_session_with_id(low.clone(), session.clone())
        .await
        .expect("low source");
    runtime_a
        .unload_session(low.clone())
        .await
        .expect("low unloads");
    runtime_a
        .create_session_with_id(high.clone(), session)
        .await
        .expect("high source");
    runtime_a
        .unload_session(high.clone())
        .await
        .expect("high unloads");
    let low_summary = find_session_dir(&config.session_storage, &low).join("summary.json");
    let high_summary = find_session_dir(&config.session_storage, &high).join("summary.json");
    let low_summary_before = std::fs::read(&low_summary).expect("low summary");
    let high_summary_before = std::fs::read(&high_summary).expect("high summary");
    let low_to_high = ForkSessionRequest {
        source_cwd: workspace.clone(),
        new_cwd: root.path().join("high-target"),
        new_session_id: Some(high.as_str().to_owned()),
        new_model_id: None,
        target_prompt_index: None,
        session_kind: None,
        source_workspace_dir: None,
    };
    let high_to_low = ForkSessionRequest {
        source_cwd: workspace,
        new_cwd: root.path().join("low-target"),
        new_session_id: Some(low.as_str().to_owned()),
        new_model_id: None,
        target_prompt_index: None,
        session_kind: None,
        source_workspace_dir: None,
    };
    let (lease_entered, release_lease) = store.pause_next_lease();
    let a = tokio::spawn({
        let runtime = runtime_a.clone();
        let low = low.clone();
        async move { runtime.fork_session(&low, &low_to_high).await }
    });
    tokio::task::spawn_blocking(move || lease_entered.recv())
        .await
        .expect("lease observer joins")
        .expect("first fork pauses while holding the lower ordered identity");
    let b = tokio::spawn({
        let runtime = runtime_b.clone();
        let high = high.clone();
        async move { runtime.fork_session(&high, &high_to_low).await }
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), b)
            .await
            .expect("reverse fork must fail fast, never wait for the lower lease")
            .expect("B joins")
            .is_err()
    );
    release_lease.send(()).expect("release lower lease");
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), a)
            .await
            .expect("A completes after its ordered lease is released")
            .expect("A joins")
            .is_err()
    );
    assert!(store.lease_conflicts.load(Ordering::Acquire) >= 1);
    assert_eq!(
        std::fs::read(low_summary).expect("low summary survives"),
        low_summary_before
    );
    assert_eq!(
        std::fs::read(high_summary).expect("high summary survives"),
        high_summary_before
    );

    runtime_a.shutdown().await.expect("Runtime A shuts down");
    runtime_b.shutdown().await.expect("Runtime B shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_worktree_resume_denies_raw_bypass_and_typed_path_is_fenced() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let config = runtime_config(&root, server.url());
    let store = Arc::new(DeleteProbeStore::new(root.path().join("host-state")));
    let (runtime_a, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime A starts");
    let (runtime_b, _) = Runtime::builder(config)
        .profile(RuntimeProfile::Desktop)
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime B starts");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let source = runtime_a
        .create_session(session_config(workspace.clone()))
        .await
        .expect("Runtime A owns source");
    let error = runtime_b
        .extension_request(ExtensionRequest {
            method: "x.ai/git/worktree/resume_session".into(),
            params: serde_json::json!({}),
        })
        .await
        .expect_err("Host generic lifecycle transport is denied");
    assert!(error.to_string().contains("typed Runtime operation"));
    let conflicts = store.lease_conflicts.load(Ordering::Acquire);
    let typed = runtime_b
        .resume_session_in_worktree(
            &source,
            &ResumeSessionInWorktreeRequest {
                source_cwd: workspace,
                copy_mode: WorktreeCopyMode::Clean,
                worktree_type: None,
                restore_code: Some(false),
                git_ref: None,
            },
        )
        .await;
    assert!(typed.is_err(), "typed path must acquire the source fence");
    assert_eq!(store.lease_conflicts.load(Ordering::Acquire), conflicts + 1);
    runtime_a.shutdown().await.expect("Runtime A shuts down");
    runtime_b.shutdown().await.expect("Runtime B shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_host_worktree_resume_uses_exact_authority_source_without_jsonl_summary() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(&workspace)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", root.path())
            .output()
            .expect("git command runs");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init"]);
    std::fs::write(workspace.join("tracked.txt"), b"tracked").expect("tracked file");
    git(&["add", "tracked.txt"]);
    git(&[
        "-c",
        "user.name=SDK Test",
        "-c",
        "user.email=sdk@example.invalid",
        "commit",
        "-m",
        "initial",
    ]);

    let config = runtime_config(&root, server.url());
    let store = Arc::new(DeleteProbeStore::new(root.path().join("host-state")));
    let (runtime, _) = Runtime::builder(config.clone())
        .session_state_store(store.clone())
        .start()
        .await
        .expect("Runtime starts");
    let source = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("source Session");
    runtime
        .unload_session(source.clone())
        .await
        .expect("source unloads");
    let source_dir = find_session_dir(&config.session_storage, &source);
    std::fs::remove_file(source_dir.join("summary.json"))
        .expect("remove non-authoritative source summary");
    assert_no_covered_session_jsonl(&config.session_storage);

    let receipt = runtime
        .resume_session_in_worktree(
            &source,
            &ResumeSessionInWorktreeRequest {
                source_cwd: workspace,
                copy_mode: WorktreeCopyMode::Clean,
                worktree_type: Some(WorktreeType::Git),
                restore_code: Some(false),
                git_ref: None,
            },
        )
        .await
        .expect("typed worktree resume uses Host authority without source JSONL metadata");
    assert_eq!(receipt.parent_session_id, source);
    assert!(matches!(
        store
            .inspect_slot(&SessionKey::new(receipt.session_id.as_str()).unwrap())
            .unwrap(),
        SessionSlot::Live(_)
    ));
    assert_no_covered_session_jsonl(&config.session_storage);
    runtime.shutdown().await.expect("Runtime shuts down");
}
