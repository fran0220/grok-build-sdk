use super::*;

// These tests exercise the shell's process-global root-session registry. Once
// grouped under one module, libtest schedules them together, so serialize this
// domain to keep each journal assertion isolated.
static SESSION_LIFECYCLE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn fixed_model_catalog_is_typed_and_available_in_restricted_profile() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let root = TempDir::new().expect("temp root");
    let (runtime, _) = Runtime::start(runtime_config(&root, "http://127.0.0.1:9/v1".into()))
        .await
        .expect("fixed catalog does not require a reachable catalog service");

    let models = runtime.list_models().await.expect("model catalog");
    assert_eq!(models.current_model_id, "test-model");
    assert_eq!(models.available_models.len(), 1);
    assert_eq!(models.available_models[0].id, "test-model");
    let metadata = models.available_models[0]
        .metadata
        .as_ref()
        .expect("model capability metadata");
    assert_eq!(
        metadata.get("totalContextTokens"),
        Some(&serde_json::json!(131_072))
    );
    assert_eq!(
        metadata.get("agentType"),
        Some(&serde_json::json!("grok-build"))
    );
    assert!(runtime.capabilities().features.iter().any(|capability| {
        capability.namespace == "x.ai/models/list"
            && capability.enabled
            && capability.effect_class == "read"
    }));

    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test]
async fn restricted_session_creation_never_evaluates_workspace_envrc() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let marker = root.path().join("envrc-was-evaluated");
    std::fs::write(
        workspace.join(".envrc"),
        format!("printf evaluated > '{}'\n", marker.display()),
    )
    .expect("hostile envrc");

    let (runtime, _) = Runtime::start(runtime_config(&root, "http://127.0.0.1:9/v1".into()))
        .await
        .expect("restricted runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("restricted session starts");

    assert!(
        !marker.exists(),
        "Restricted must not execute a workspace .envrc"
    );
    runtime
        .close_session(session)
        .await
        .expect("session closes");
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test]
async fn rejects_missing_endpoint_before_starting_worker() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let root = TempDir::new().expect("temp root");
    let result = Runtime::start(runtime_config(&root, String::new())).await;
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
}

#[test]
fn ledger_turn_state_preserves_tagged_camel_case_wire_shape() {
    assert_eq!(
        serde_json::to_value(LedgerTurnState::Pending).expect("ledger state serializes"),
        serde_json::json!({"state": "pending"})
    );
}

#[test]
fn provenance_is_exact_and_never_unknown() {
    let provenance = source_provenance();
    assert_eq!(provenance.upstream_release, "1.0.0");
    assert_eq!(provenance.facade_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(provenance.fork_commit.len(), 40);
    assert!(
        provenance
            .fork_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    );
    assert_eq!(provenance.upstream_source_rev.len(), 40);
    assert!(
        provenance
            .upstream_source_rev
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    );
}

#[test]
fn legacy_rewind_receipts_without_exact_target_identity_fail_closed() {
    let legacy = serde_json::json!({
        "operation_id": "legacy-operation",
        "session_id": "legacy-session",
        "target_prompt_index": 2
    });

    assert!(serde_json::from_value::<ConversationRewindReceipt>(legacy).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runs_real_agent_outside_local_set_and_closes_session() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");

    let (runtime, mut events) = Runtime::start(runtime_config(&root, server.url()))
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(SessionConfig {
            cwd: workspace.clone(),
            model: "test-model".into(),
            reasoning: None,
            system_prompt: None,
            rules: None,
        })
        .await
        .expect("session starts");
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        runtime.prompt(&session, "turn-1", "reply briefly"),
    )
    .await
    .expect("turn timeout")
    .expect("turn succeeds");
    assert_eq!(outcome.outcome, TurnOutcome::End);
    let retained = runtime
        .events_after(&session, 0)
        .await
        .expect("events are retained");
    assert_eq!(
        retained.last().map(|event| event.sequence),
        Some(outcome.final_sequence)
    );
    assert!(matches!(
        retained.last().map(|event| &event.update),
        Some(EventUpdate::TurnFinished(TurnOutcome::End))
    ));
    assert!(retained.iter().any(|event| {
        event.turn_id.as_deref() == Some("turn-1")
            && matches!(&event.update, EventUpdate::UserText(text) if text == "reply briefly")
    }));

    let mut assistant = String::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(250), events.recv()).await
    {
        let finished = matches!(event.update, EventUpdate::TurnFinished(_));
        if let EventUpdate::AssistantText(text) = &event.update {
            assert_eq!(event.turn_id.as_deref(), Some("turn-1"));
            assistant.push_str(text);
        }
        if finished {
            assert_eq!(event.turn_id.as_deref(), Some("turn-1"));
            break;
        }
    }
    assert!(assistant.contains("Echo:"), "assistant output: {assistant}");
    runtime
        .unload_session(session.clone())
        .await
        .expect("session closes");
    assert!(matches!(
        runtime
            .events_after(&session, outcome.final_sequence)
            .await
            .expect("closed journal remains readable")
            .as_slice(),
        [Event {
            update: EventUpdate::SessionClosed,
            ..
        }]
    ));
    runtime
        .load_session(session.clone(), session_config(workspace))
        .await
        .expect("the same durable session id remains resumable");
    let after_turn = runtime
        .events_after(&session, outcome.final_sequence)
        .await
        .expect("retained close event is recoverable after reload");
    assert!(matches!(
        after_turn.as_slice(),
        [Event {
            update: EventUpdate::SessionClosed,
            ..
        }]
    ));
    assert!(runtime.events_after(&session, u64::MAX).await.is_err());
    assert!(
        runtime
            .events_after(&SessionId::from_stored("missing"), 0)
            .await
            .is_err()
    );
    runtime.shutdown().await.expect("runtime shuts down");
}

#[test]
fn rich_prompt_digest_covers_binary_content() {
    let prompt = |data: &str| Prompt {
        blocks: vec![PromptBlock::Image {
            data: data.into(),
            mime_type: "image/png".into(),
            uri: None,
        }],
        metadata: serde_json::json!({"source":"test"}),
    };
    assert_ne!(
        prompt_digest_content(&prompt("AA==")).unwrap(),
        prompt_digest_content(&prompt("AQ==")).unwrap()
    );
    assert_eq!(
        serde_json::from_value::<RuntimeProfile>(serde_json::json!("restricted")).unwrap(),
        RuntimeProfile::Restricted
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rich_prompt_blocks_digest_rewind_and_restart_replay_are_end_to_end() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let prompt = Prompt {
        blocks: vec![
            PromptBlock::Text {
                text: "rich-wire-marker".into(),
            },
            PromptBlock::Image {
                data: "iVBORw0KGgoAAAANSUhEUgAAACAAAAAQCAIAAAD4YuoOAAAAHUlEQVR42mPQqDhBU8QwasGoBaMWjFowasFQsAAAxdvQH+YmXBQAAAAASUVORK5CYII=".into(),
                mime_type: "image/png".into(),
                uri: Some("attachment://screen.png".into()),
            },
            PromptBlock::Audio {
                data: "AQ==".into(),
                mime_type: "audio/wav".into(),
            },
            PromptBlock::ResourceLink {
                uri: "file:///workspace/reference.txt".into(),
                name: "reference.txt".into(),
                mime_type: Some("text/plain".into()),
            },
            PromptBlock::EmbeddedTextResource {
                uri: "memory://embedded-text".into(),
                text: "embedded-text-marker".into(),
                mime_type: Some("text/plain".into()),
            },
            PromptBlock::EmbeddedBlobResource {
                uri: "memory://embedded-blob".into(),
                blob: "Ag==".into(),
                mime_type: Some("application/octet-stream".into()),
            },
        ],
        metadata: serde_json::json!({"desktop":{"captureId":"capture-1"}}),
    };
    let expected_digest = prompt_digest_content(&prompt).expect("rich digest");

    let (runtime, _) = Runtime::start(config.clone())
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");
    let receipt = runtime
        .prompt_content(&session, "rich-turn", prompt)
        .await
        .expect("rich prompt succeeds through the real agent");
    assert_eq!(receipt.runtime_prompt_index, 0);
    let request = server
        .requests()
        .into_iter()
        .filter_map(|entry| entry.body)
        .find(|body| body.to_string().contains("rich-wire-marker"))
        .expect("rich prompt reached inference");
    let request_wire = request.to_string();
    for marker in [
        "image/png",
        "audio/wav",
        "file:///workspace/reference.txt",
        "embedded-text-marker",
        "application/octet-stream",
    ] {
        assert!(
            request_wire.contains(marker),
            "missing {marker} in inference request: {request_wire}"
        );
    }
    let prompt_events = runtime.events_after(&session, 0).await.expect("events");
    let lossless_non_text = prompt_events
        .iter()
        .filter_map(|event| match &event.update {
            EventUpdate::Unknown { raw, .. } => Some(raw.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(lossless_non_text.contains("attachment://screen.png"));
    assert!(lossless_non_text.contains("memory://embedded-blob"));
    let ledger = runtime.session_ledger(&session).await.expect("ledger");
    assert_eq!(ledger.entries[0].prompt_digest, expected_digest);
    assert_eq!(
        runtime
            .rewind_points(&session)
            .await
            .expect("rewind points")[0]
            .prompt_digest,
        Some(expected_digest.clone())
    );
    runtime
        .unload_session(session.clone())
        .await
        .expect("session unloads");
    runtime.shutdown().await.expect("first runtime shuts down");

    let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .load_session(session.clone(), session_config(workspace.clone()))
        .await
        .expect("load replays persisted history");
    let replay = restarted
        .events_after(&session, 0)
        .await
        .expect("fresh journal captures replay");
    assert!(!replay.is_empty(), "load must rebuild a fresh journal");
    assert!(replay.iter().all(|event| event.replay));
    assert!(replay.iter().any(|event| {
        matches!(&event.update, EventUpdate::UserText(text) if text == "rich-wire-marker")
    }));
    restarted
        .unload_session(session.clone())
        .await
        .expect("loaded session unloads");
    let sequence_before_resume = restarted
        .events_after(&session, 0)
        .await
        .expect("journal remains")
        .last()
        .expect("journal event")
        .sequence;
    restarted
        .resume_session(session.clone(), session_config(workspace))
        .await
        .expect("resume reattaches without replay");
    assert!(
        restarted
            .events_after(&session, sequence_before_resume)
            .await
            .expect("resume journal query")
            .is_empty(),
        "session/resume must not duplicate historical events"
    );
    assert_eq!(
        restarted
            .rewind_points(&session)
            .await
            .expect("rewind points")[0]
            .prompt_digest,
        Some(expected_digest)
    );
    restarted.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_event_journal_reports_exact_cursor_gap() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, _) = Runtime::builder(runtime_config(&root, server.url()))
        .event_journal_capacity(2)
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session starts");
    let receipt = runtime
        .prompt(&session, "journal-turn", "journal marker")
        .await
        .expect("turn succeeds");
    let gap = runtime
        .events_after(&session, 0)
        .await
        .expect_err("old cursor must report eviction");
    assert!(matches!(
        gap,
        Error::EventGap {
            requested: 0,
            oldest_available,
            newest,
        } if oldest_available == receipt.final_sequence - 1 && newest == receipt.final_sequence
    ));
    let tail = runtime
        .events_after(&session, receipt.final_sequence - 2)
        .await
        .expect("oldest retained cursor is readable");
    assert_eq!(tail.len(), 2);
    assert_eq!(
        tail.last().map(|event| event.sequence),
        Some(receipt.final_sequence)
    );
    let truncated = runtime
        .probe_session_replay(&session, 0)
        .await
        .expect("probe returns a truncated retained suffix");
    assert!(truncated.truncated);
    assert_eq!(
        truncated.oldest_retained_sequence,
        receipt.final_sequence - 1
    );
    assert_eq!(truncated.inclusive_end_sequence, receipt.final_sequence);
    assert_eq!(truncated.retained_count, 2);
    assert_eq!(truncated.events, tail);
    assert_eq!(truncated.ledger.entries.len(), 1);

    let boundary = runtime
        .probe_session_replay(&session, truncated.oldest_retained_sequence - 1)
        .await
        .expect("oldest minus one is not a gap");
    assert!(!boundary.truncated);
    assert_eq!(boundary.events, truncated.events);
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_replay_probe_snapshots_binding_route_journal_ledger_and_residency() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, server.url());
    config.models = vec![
        ModelSpec {
            id: "default-route".into(),
            context_window: 131_072,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: true,
            default_reasoning: Some("high".into()),
            reasoning_options: vec!["high".into()],
        },
        ModelSpec {
            id: "updated-route".into(),
            context_window: 131_072,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: true,
            default_reasoning: Some("xhigh".into()),
            reasoning_options: vec!["xhigh".into()],
        },
    ];
    let created_config = SessionConfig {
        cwd: workspace.clone(),
        model: "default-route".into(),
        reasoning: None,
        system_prompt: None,
        rules: None,
    };
    let (runtime, _) = Runtime::start(config.clone())
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(created_config)
        .await
        .expect("session starts");
    let initial = runtime
        .probe_session_replay(&session, 0)
        .await
        .expect("created binding is readable");
    assert_eq!(initial.binding.session_id, session);
    assert_eq!(initial.binding.cwd, workspace);
    assert_eq!(initial.binding.model, "default-route");
    assert_eq!(initial.binding.reasoning.as_deref(), Some("high"));
    assert_eq!(initial.requested_after_sequence, 0);
    assert_eq!(initial.oldest_retained_sequence, 1);
    assert_eq!(
        initial.events.last().map(|event| event.sequence),
        Some(initial.inclusive_end_sequence)
    );
    assert_eq!(initial.retained_count, initial.events.len());
    assert!(!initial.truncated);
    assert!(initial.ledger.entries.is_empty());

    let receipt = runtime
        .prompt(&session, "probe-turn", "probe marker")
        .await
        .expect("turn succeeds");
    let snapshot = runtime
        .probe_session_replay(&session, initial.inclusive_end_sequence)
        .await
        .expect("post-turn snapshot");
    assert_eq!(snapshot.inclusive_end_sequence, receipt.final_sequence);
    assert_eq!(
        snapshot.events.last().map(|event| event.sequence),
        Some(snapshot.inclusive_end_sequence)
    );
    assert!(snapshot.events.windows(2).all(|pair| {
        pair[0].sequence + 1 == pair[1].sequence
            && pair[0].sequence > snapshot.requested_after_sequence
    }));
    assert!(
        snapshot
            .events
            .last()
            .is_some_and(|event| matches!(&event.update, EventUpdate::TurnFinished(_)))
    );
    assert!(matches!(
        &snapshot.ledger.entries[0].state,
        LedgerTurnState::Completed { settlement_id, .. }
            if settlement_id == &receipt.settlement_id
    ));

    runtime
        .set_route(&session, "updated-route", None)
        .await
        .expect("route updates");
    let updated = runtime
        .probe_session_replay(&session, receipt.final_sequence)
        .await
        .expect("updated binding is readable");
    assert_eq!(updated.binding.model, "updated-route");
    assert_eq!(updated.binding.reasoning.as_deref(), Some("xhigh"));
    assert!(updated.events.iter().all(|event| {
        event.sequence > receipt.final_sequence && event.sequence <= updated.inclusive_end_sequence
    }));
    assert!(
        runtime
            .probe_session_replay(&session, updated.inclusive_end_sequence + 1)
            .await
            .is_err(),
        "future cursors fail"
    );
    assert!(
        runtime
            .probe_session_replay(&SessionId::from_stored("missing"), 0)
            .await
            .is_err(),
        "unknown Sessions fail closed"
    );

    runtime
        .unload_session(session.clone())
        .await
        .expect("session unloads");
    assert!(
        runtime.probe_session_replay(&session, 0).await.is_err(),
        "unloaded Sessions fail closed"
    );
    assert!(
        runtime
            .events_after(&session, receipt.final_sequence)
            .await
            .is_ok(),
        "legacy journal access remains available after unload"
    );
    runtime.shutdown().await.expect("runtime shuts down");

    let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .load_session(
            session.clone(),
            SessionConfig {
                cwd: workspace.clone(),
                model: "updated-route".into(),
                reasoning: None,
                system_prompt: None,
                rules: None,
            },
        )
        .await
        .expect("session loads");
    let loaded = restarted
        .probe_session_replay(&session, 0)
        .await
        .expect("loaded binding is readable");
    assert_eq!(loaded.binding.session_id, session);
    assert_eq!(loaded.binding.cwd, workspace);
    assert_eq!(loaded.binding.model, "updated-route");
    assert_eq!(loaded.binding.reasoning.as_deref(), Some("xhigh"));
    assert_eq!(loaded.ledger.entries.len(), 1);
    restarted.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_changes_preserve_the_prompt_and_existing_conversation() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = runtime_config(&root, server.url());
    config.models = vec![
        ModelSpec {
            id: "fast-route".into(),
            context_window: 131_072,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: true,
            default_reasoning: Some("high".into()),
            reasoning_options: vec!["high".into()],
        },
        ModelSpec {
            id: "advanced-route".into(),
            context_window: 131_072,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: true,
            default_reasoning: Some("xhigh".into()),
            reasoning_options: vec!["xhigh".into()],
        },
    ];
    let (runtime, _events) = Runtime::start(config).await.expect("runtime starts");
    let session = runtime
        .create_session(SessionConfig {
            cwd: workspace,
            model: "fast-route".into(),
            reasoning: Some("high".into()),
            system_prompt: None,
            rules: None,
        })
        .await
        .expect("session starts");

    runtime
        .prompt(&session, "turn-fast-1", "route-marker-fast-1")
        .await
        .expect("first fast turn");
    let fast_before = request_with_user_marker(&server, "route-marker-fast-1");
    let system_prompt = fast_before["messages"][0]["content"]
        .as_str()
        .expect("system prompt")
        .as_bytes()
        .to_vec();
    assert_eq!(fast_before["model"], "fast-route");
    assert_eq!(fast_before["reasoning_effort"], "high");

    runtime
        .set_route(&session, "advanced-route", Some("xhigh".into()))
        .await
        .expect("advanced route applies");
    runtime
        .prompt(&session, "turn-advanced", "route-marker-advanced")
        .await
        .expect("advanced turn");
    let advanced = request_with_user_marker(&server, "route-marker-advanced");
    assert_eq!(advanced["model"], "advanced-route");
    assert_eq!(advanced["reasoning_effort"], "xhigh");
    assert_eq!(
        advanced["messages"][0]["content"]
            .as_str()
            .expect("system prompt")
            .as_bytes(),
        system_prompt
    );
    assert!(message_prefix_is_unchanged(&fast_before, &advanced));

    runtime
        .set_route(&session, "fast-route", Some("high".into()))
        .await
        .expect("fast route reapplies");
    runtime
        .prompt(&session, "turn-fast-2", "route-marker-fast-2")
        .await
        .expect("second fast turn");
    let fast_after = request_with_user_marker(&server, "route-marker-fast-2");
    assert_eq!(fast_after["model"], "fast-route");
    assert_eq!(fast_after["reasoning_effort"], "high");
    assert_eq!(
        fast_after["messages"][0]["content"]
            .as_str()
            .expect("system prompt")
            .as_bytes(),
        system_prompt
    );
    assert!(message_prefix_is_unchanged(&advanced, &fast_after));

    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rewind_receipt_and_ledger_survive_authority_restart_without_reexecution() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let evidence =
        Arc::new(LocalSessionEvidenceStore::new(&config.session_storage).expect("evidence store"));
    let (runtime, _events) = Runtime::start(config.clone())
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session starts");
    runtime
        .prompt(&session, "turn-0", "prompt zero")
        .await
        .expect("first turn");
    runtime
        .prompt(&session, "turn-1", "prompt one")
        .await
        .expect("second turn");

    let operation_id = "restart-rewind-operation";
    assert!(matches!(
        runtime
            .rewind_status(&session, "never-started-rewind")
            .await
            .expect("absent rewind status"),
        ConversationRewindStatus::Absent
    ));
    let first = runtime
        .rewind_conversation(&session, operation_id, 1)
        .await
        .expect("first rewind");
    assert_eq!(first.target_prompt_index, 1);
    assert!(matches!(
        runtime
            .rewind_status(&session, operation_id)
            .await
            .expect("receipt status"),
        ConversationRewindStatus::Applied { receipt } if receipt == first
    ));
    assert_eq!(
        runtime
            .rewind_conversation(&session, operation_id, 1)
            .await
            .expect("receipt replay"),
        first
    );
    assert!(
        runtime
            .rewind_conversation(&session, operation_id, 0)
            .await
            .is_err(),
        "an operation identity cannot drift to another target"
    );
    let rewind_key = SessionEvidenceKey {
        kind: SessionEvidenceKind::Rewind,
        identity: operation_id.into(),
    };
    let durable_receipt = evidence
        .load(&rewind_key)
        .expect("evidence authority loads")
        .expect("rewind evidence exists");
    assert!(durable_receipt.version.validates(&durable_receipt.bytes));
    runtime
        .unload_session(session.clone())
        .await
        .expect("session unloads");
    runtime.shutdown().await.expect("first runtime shuts down");

    let (restarted, _events) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .load_session(session.clone(), session_config(workspace))
        .await
        .expect("rewound session reloads");
    assert!(matches!(
        restarted
            .rewind_status(&session, operation_id)
            .await
            .expect("receipt status after restart"),
        ConversationRewindStatus::Applied { receipt } if receipt == first
    ));
    let recovered = restarted
        .rewind_conversation(&session, operation_id, 1)
        .await
        .expect("receipt replay after restart");
    assert_eq!(recovered, first);
    let ledger = restarted
        .session_ledger(&session)
        .await
        .expect("ledger loads");
    assert!(matches!(
        ledger.entries[0].state,
        LedgerTurnState::Completed { .. }
    ));
    assert!(matches!(
        ledger.entries[1].state,
        LedgerTurnState::Discarded
    ));
    assert_eq!(restarted.rewind_points(&session).await.unwrap().len(), 1);
    restarted.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sessions_cancel_close_and_shutdown_are_reconciled() {
    let _guard = SESSION_LIFECYCLE_LOCK.lock().await;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock server");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let (runtime, mut events) = Runtime::start(runtime_config(&root, server.url()))
        .await
        .expect("runtime starts");

    let first = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("first session");
    let second = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("second session");
    server.hold_agent_completions();
    let first_prompt = tokio::spawn({
        let runtime = runtime.clone();
        let first = first.clone();
        async move { runtime.prompt(&first, "first-turn", "first").await }
    });
    let second_prompt = tokio::spawn({
        let runtime = runtime.clone();
        let second = second.clone();
        async move { runtime.prompt(&second, "second-turn", "second").await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    runtime.cancel(&first).await.expect("active prompt cancels");
    runtime
        .unload_session(second.clone())
        .await
        .expect("active session closes after cancellation");
    server.release_agent_completions();
    let first_outcome = first_prompt
        .await
        .expect("first prompt joins")
        .expect("settles");
    assert_eq!(first_outcome.outcome, TurnOutcome::Cancelled);
    let second_outcome = second_prompt
        .await
        .expect("second prompt joins")
        .expect("settles");
    assert_eq!(second_outcome.outcome, TurnOutcome::Cancelled);
    runtime.unload_session(first).await.expect("first unloads");

    let mut by_session = std::collections::HashMap::<String, Vec<u64>>::new();
    while let Ok(event) = events.try_recv() {
        by_session
            .entry(event.session_id.as_str().to_owned())
            .or_default()
            .push(event.sequence);
    }
    for sequences in by_session.values() {
        assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
    }
    let application_sessions = by_session
        .keys()
        .filter(|session_id| session_id.as_str() != SessionId::RUNTIME_EVENTS)
        .count();
    assert_eq!(application_sessions, 2);

    runtime.shutdown().await.expect("worker joins");
    runtime.shutdown().await.expect("shutdown is idempotent");
    assert!(matches!(
        runtime.create_session(session_config(workspace)).await,
        Err(Error::Shutdown)
    ));
}
