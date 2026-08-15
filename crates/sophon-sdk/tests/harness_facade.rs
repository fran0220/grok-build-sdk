use sophon_sdk::{
    ApiBackend, Error, HarnessContent, HarnessError, HarnessEvidenceKind, HarnessEvidenceRef,
    HarnessRefinement, HarnessRefinementPatch, HarnessSnapshot, LedgerTurnState, ModelSpec, Prompt,
    PromptBlock, Runtime, RuntimeConfig, SessionConfig, TurnBindingKey, TurnBindingStatus,
    TurnOutcome, prompt_digest, prompt_digest_content,
};
use std::path::PathBuf;
use tempfile::TempDir;
use xai_grok_test_support::MockInferenceServer;

#[test]
fn immutable_snapshot_is_content_addressed_validated_and_materialized() {
    let content = HarnessContent::new()
        .system_prompt("You are the desktop coding agent.")
        .rules("Keep changes reviewable.");
    let snapshot = HarnessSnapshot::new(content).expect("valid immutable snapshot");
    let equivalent = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("You are the desktop coding agent.")
            .rules("Keep changes reviewable."),
    )
    .expect("equivalent snapshot");
    assert_eq!(snapshot.digest(), equivalent.digest());
    assert_eq!(
        snapshot.digest().as_str(),
        "sha256:3c359b72becdf6b9b7f235f9c44eb4f626f82999c67227ffd866695b737a31a8"
    );

    let materialized = snapshot.materialize().expect("headless materialization");
    let session = materialized.apply_to_session(SessionConfig {
        cwd: PathBuf::from("/host/project"),
        model: "model-route".into(),
        reasoning: Some("high".into()),
        system_prompt: Some("stale prompt".into()),
        rules: None,
    });
    assert_eq!(session.cwd, PathBuf::from("/host/project"));
    assert_eq!(session.model, "model-route");
    assert_eq!(session.reasoning.as_deref(), Some("high"));
    assert_eq!(
        session.system_prompt.as_deref(),
        Some(
            "You are the desktop coding agent.\n\n<human_rules>\nKeep changes reviewable.\n</human_rules>"
        )
    );
    assert_eq!(session.rules, None);

    let bytes = snapshot.to_json_vec().expect("snapshot serialization");
    assert_eq!(
        HarnessSnapshot::from_json_slice(&bytes).expect("validated round trip"),
        snapshot
    );

    let mut tampered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    tampered["content"]["rules"] = serde_json::json!("silently changed");
    let error = HarnessSnapshot::from_json_slice(&serde_json::to_vec(&tampered).unwrap())
        .expect_err("content tampering must invalidate the address");
    assert!(matches!(error, HarnessError::DigestMismatch { .. }));
    assert!(matches!(
        HarnessSnapshot::new(HarnessContent::new().rules("rules without a complete prompt")),
        Err(HarnessError::Invalid(_))
    ));
}

#[test]
fn typed_refinement_is_optimistic_and_returns_an_uncommitted_snapshot() {
    let base = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("base prompt")
            .rules("base rules"),
    )
    .unwrap();
    let patch = HarnessRefinementPatch::new(
        base.digest().clone(),
        [
            HarnessRefinement::SetSystemPrompt("refined prompt".into()),
            HarnessRefinement::ClearRules,
        ],
    )
    .expect("typed patch");

    let refined = patch.apply(&base).expect("matching optimistic base");
    assert_ne!(refined.digest(), base.digest());
    assert_eq!(
        refined.content().system_prompt_value(),
        Some("refined prompt")
    );
    assert_eq!(refined.content().rules_value(), None);
    assert!(matches!(
        patch.apply(&refined),
        Err(HarnessError::StaleBase { .. })
    ));

    let duplicate = HarnessRefinementPatch::new(
        base.digest().clone(),
        [
            HarnessRefinement::ClearRules,
            HarnessRefinement::SetRules("last writer must not win".into()),
        ],
    )
    .expect_err("one typed target may be changed only once");
    assert!(matches!(duplicate, HarnessError::Invalid(_)));
}

#[test]
fn the_canonical_digest_is_stable_across_every_path_that_produces_one_snapshot() {
    let direct = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("stable prompt")
            .rules("stable rules"),
    )
    .unwrap();
    let reordered = HarnessSnapshot::new(
        HarnessContent::new()
            .rules("stable rules")
            .system_prompt("stable prompt"),
    )
    .unwrap();
    let refined = HarnessRefinementPatch::new(
        HarnessSnapshot::new(
            HarnessContent::new()
                .system_prompt("stable prompt")
                .rules("superseded rules"),
        )
        .unwrap()
        .digest()
        .clone(),
        [HarnessRefinement::SetRules("stable rules".into())],
    )
    .unwrap()
    .apply(
        &HarnessSnapshot::new(
            HarnessContent::new()
                .system_prompt("stable prompt")
                .rules("superseded rules"),
        )
        .unwrap(),
    )
    .unwrap();
    let decoded = HarnessSnapshot::from_json_slice(&direct.to_json_vec().unwrap()).unwrap();

    // The address follows the content, never the route the content took.
    for produced in [&reordered, &refined, &decoded] {
        assert_eq!(produced.digest(), direct.digest());
        assert_eq!(
            produced.to_json_vec().unwrap(),
            direct.to_json_vec().unwrap()
        );
        assert_eq!(
            produced.materialize().unwrap(),
            direct.materialize().unwrap()
        );
        assert_eq!(produced.materialize().unwrap().digest(), direct.digest());
    }
    assert_eq!(
        direct.digest().as_str(),
        "sha256:9b5844a31a11125b3700397a4da8d87d539c05398ccd0cc5ecf818bf701af537"
    );

    // Field boundaries and absence are part of the address, so no two
    // different harnesses can collide by concatenation.
    let boundary = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("stable prompts")
            .rules("table rules"),
    )
    .unwrap();
    let absent =
        HarnessSnapshot::new(HarnessContent::new().system_prompt("stable prompt")).unwrap();
    assert_ne!(boundary.digest(), direct.digest());
    assert_ne!(absent.digest(), direct.digest());
    assert_eq!(
        absent.digest().as_str(),
        "sha256:89ecef2e53252fc679e9e30211137293c862af6f95d639158c70e4f26826ff76"
    );
}

#[test]
fn a_refinement_cites_evidence_and_only_applies_to_the_base_it_names() {
    let base = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("cited base prompt")
            .rules("cited base rules"),
    )
    .unwrap();
    let other = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("unrelated prompt")
            .rules("unrelated rules"),
    )
    .unwrap();
    let patch = HarnessRefinementPatch::new(
        base.digest().clone(),
        [HarnessRefinement::SetRules("cited successor rules".into())],
    )
    .unwrap()
    .with_evidence([
        HarnessEvidenceRef::new(HarnessEvidenceKind::TurnBinding, "turn-binding-identity").unwrap(),
        HarnessEvidenceRef::new(HarnessEvidenceKind::Evaluation, "acceptance-check")
            .unwrap()
            .with_digest(format!("sha256:{}", "c".repeat(64)))
            .unwrap(),
    ])
    .unwrap();

    assert_eq!(patch.base_digest(), base.digest());
    assert_eq!(patch.evidence().len(), 2);
    let successor = patch.apply(&base).expect("the named base applies");
    assert_eq!(
        successor.content().rules_value(),
        Some("cited successor rules")
    );

    let stale = patch
        .apply(&other)
        .expect_err("a patch cannot silently retarget another base");
    match stale {
        HarnessError::StaleBase { expected, actual } => {
            assert_eq!(&expected, base.digest());
            assert_eq!(&actual, other.digest());
        }
        error => panic!("expected a stale base error, got {error}"),
    }
    assert!(matches!(
        patch.apply(&successor),
        Err(HarnessError::StaleBase { .. })
    ));
}

fn runtime_config(root: &TempDir, endpoint: String) -> RuntimeConfig {
    RuntimeConfig {
        endpoint,
        api_key: "host-relay-bearer".into(),
        grok_home: root.path().join("grok"),
        session_storage: root.path().join("sessions"),
        models: vec![
            ModelSpec {
                id: "fast".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: true,
                default_reasoning: Some("high".into()),
                reasoning_options: vec!["low".into(), "high".into()],
            },
            ModelSpec {
                id: "deep".into(),
                context_window: 131_072,
                api_backend: ApiBackend::ChatCompletions,
                supports_reasoning: true,
                default_reasoning: Some("xhigh".into()),
                reasoning_options: vec!["xhigh".into()],
            },
        ],
    }
}

fn session_config(cwd: PathBuf, model: &str, reasoning: Option<&str>) -> SessionConfig {
    SessionConfig {
        cwd,
        model: model.into(),
        reasoning: reasoning.map(str::to_owned),
        system_prompt: None,
        rules: None,
    }
}

fn foreground_request(server: &MockInferenceServer, marker: &str) -> serde_json::Value {
    server
        .requests()
        .into_iter()
        .filter(|entry| entry.path.contains("chat/completions") || entry.path.contains("responses"))
        .filter_map(|entry| entry.body)
        .find(|body| {
            body.get("tools")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|tools| !tools.is_empty())
                && body.get("tool_choice").is_none()
                && body
                    .get("messages")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|message| message.get("content"))
                    .any(|content| content.as_str().is_some_and(|text| text.contains(marker)))
        })
        .unwrap_or_else(|| panic!("foreground provider request containing {marker}"))
}

fn assert_provider_binding(
    server: &MockInferenceServer,
    marker: &str,
    expected_system_prompt: &str,
    expected_model: &str,
    expected_reasoning: &str,
    receipt: &sophon_sdk::TurnBindingReceipt,
    snapshot: &HarnessSnapshot,
) -> serde_json::Value {
    let request = foreground_request(server, marker);
    assert_eq!(
        request["messages"][0]["content"].as_str(),
        Some(expected_system_prompt),
        "provider wire must use exactly the materialized snapshot"
    );
    assert_eq!(request["model"], expected_model);
    assert_eq!(request["reasoning_effort"], expected_reasoning);
    assert_eq!(receipt.snapshot_digest(), snapshot.digest());
    assert_eq!(receipt.model(), expected_model);
    assert_eq!(receipt.reasoning(), Some(expected_reasoning));
    request
}

async fn wait_for_provider_marker(server: &MockInferenceServer, marker: &str) {
    for _ in 0..100 {
        if server
            .request_bodies()
            .iter()
            .any(|body| body.to_string().contains(marker))
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("provider did not receive marker {marker}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_wire_and_receipt_bind_complete_harness_and_effective_route_across_reattach() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let mut invalid_catalog = config.clone();
    invalid_catalog.models[0].default_reasoning = Some("not-an-option".into());
    assert!(matches!(
        Runtime::start(invalid_catalog).await,
        Err(Error::InvalidConfig(_))
    ));
    let created_snapshot = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("create-system-v1")
            .rules("create-rules-v1"),
    )
    .unwrap();
    let loaded_snapshot = HarnessRefinementPatch::new(
        created_snapshot.digest().clone(),
        [
            HarnessRefinement::SetSystemPrompt("load-system-v2".into()),
            HarnessRefinement::SetRules("load-rules-v2".into()),
        ],
    )
    .unwrap()
    .apply(&created_snapshot)
    .unwrap();
    let resumed_snapshot = HarnessRefinementPatch::new(
        loaded_snapshot.digest().clone(),
        [
            HarnessRefinement::SetSystemPrompt("resume-system-v3".into()),
            HarnessRefinement::ClearRules,
        ],
    )
    .unwrap()
    .apply(&loaded_snapshot)
    .unwrap();

    let (runtime, _) = Runtime::start(config.clone())
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session_with_harness(
            session_config(workspace.clone(), "fast", Some("low")),
            &created_snapshot,
        )
        .await
        .expect("bound session");
    let first = runtime
        .prompt_with_harness(
            &session,
            "bound-turn-create",
            "provider-marker-create",
            &created_snapshot,
        )
        .await
        .expect("complete binding receipt");
    assert_eq!(first.session_id(), &session);
    assert_eq!(first.turn_id(), "bound-turn-create");
    let create_request = assert_provider_binding(
        &server,
        "provider-marker-create",
        "create-system-v1\n\n<human_rules>\ncreate-rules-v1\n</human_rules>",
        "fast",
        "low",
        &first,
        &created_snapshot,
    );
    assert!(create_request.to_string().contains("create-rules-v1"));
    assert_eq!(first.outcome(), TurnOutcome::End);
    assert_eq!(
        first.complete_cursor().event_count() as usize,
        runtime
            .events_after(&session, first.complete_cursor().after_sequence())
            .await
            .unwrap()
            .len()
    );
    assert_eq!(
        first.complete_cursor().final_sequence(),
        runtime
            .events_after(&session, first.complete_cursor().after_sequence())
            .await
            .unwrap()
            .last()
            .unwrap()
            .sequence
    );
    assert_eq!(first.sdk_provenance().facade_version(), "0.2.0");
    assert!(first.binding_id().starts_with("sha256:"));
    let encoded = serde_json::to_vec(&first).unwrap();
    assert_eq!(
        serde_json::from_slice::<sophon_sdk::TurnBindingReceipt>(&encoded).unwrap(),
        first
    );
    let mut tampered: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    tampered["model"] = serde_json::json!("tampered-model");
    assert!(
        serde_json::from_value::<sophon_sdk::TurnBindingReceipt>(tampered).is_err(),
        "binding identity must cover the selected model"
    );

    runtime
        .set_route(&session, "deep", None)
        .await
        .expect("route changes to the validated catalog default");
    let second = runtime
        .prompt_with_harness(
            &session,
            "bound-turn-route-default",
            "provider-marker-route-default",
            &created_snapshot,
        )
        .await
        .expect("route-bound receipt");
    assert_provider_binding(
        &server,
        "provider-marker-route-default",
        "create-system-v1\n\n<human_rules>\ncreate-rules-v1\n</human_rules>",
        "deep",
        "xhigh",
        &second,
        &created_snapshot,
    );
    runtime
        .unload_session(session.clone())
        .await
        .expect("session unloads");

    runtime
        .load_session_with_harness(
            session.clone(),
            session_config(workspace.clone(), "fast", None),
            &loaded_snapshot,
        )
        .await
        .expect("session cold-loads with a replacement snapshot");
    let loaded = runtime
        .prompt_with_harness(
            &session,
            "bound-turn-load",
            "provider-marker-load",
            &loaded_snapshot,
        )
        .await
        .expect("receipt after cold load");
    let load_request = assert_provider_binding(
        &server,
        "provider-marker-load",
        "load-system-v2\n\n<human_rules>\nload-rules-v2\n</human_rules>",
        "fast",
        "high",
        &loaded,
        &loaded_snapshot,
    );
    let load_wire = load_request.to_string();
    assert!(!load_wire.contains("create-system-v1"));
    assert!(!load_wire.contains("create-rules-v1"));
    runtime
        .unload_session(session.clone())
        .await
        .expect("loaded session unloads");
    runtime.shutdown().await.expect("runtime shuts down");

    let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .resume_session_with_harness(
            session.clone(),
            session_config(workspace, "deep", None),
            &resumed_snapshot,
        )
        .await
        .expect("bound session resumes without replay");
    let recovered = restarted
        .prompt_with_harness(
            &session,
            "bound-turn-after-restart",
            "provider-marker-resume-restart",
            &resumed_snapshot,
        )
        .await
        .expect("receipt after restart");
    assert_eq!(recovered.runtime_prompt_index(), 3);
    let resumed_request = assert_provider_binding(
        &server,
        "provider-marker-resume-restart",
        "resume-system-v3",
        "deep",
        "xhigh",
        &recovered,
        &resumed_snapshot,
    );
    let resumed_wire = resumed_request.to_string();
    assert!(!resumed_wire.contains("load-system-v2"));
    assert!(!resumed_wire.contains("load-rules-v2"));
    assert!(!resumed_wire.contains("<human_rules>"));
    restarted.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binding_rejects_snapshot_mismatch_before_dispatch_and_cursor_gap_after_dispatch() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let bound = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("bound prompt")
            .rules("bound"),
    )
    .unwrap();
    let stale = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("stale prompt")
            .rules("stale"),
    )
    .unwrap();
    let (runtime, _) = Runtime::builder(runtime_config(&root, server.url()))
        .event_journal_capacity(1)
        .start()
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session_with_harness(session_config(workspace, "fast", Some("high")), &bound)
        .await
        .unwrap();

    assert!(matches!(
        runtime
            .prompt_with_harness(&session, "stale-turn", "must not dispatch", &stale)
            .await,
        Err(Error::Harness(HarnessError::BindingMismatch { .. }))
    ));
    assert!(
        runtime
            .session_ledger(&session)
            .await
            .unwrap()
            .entries
            .is_empty()
    );

    assert!(matches!(
        runtime
            .prompt_with_harness(&session, "gap-turn", "dispatch then detect gap", &bound)
            .await,
        Err(Error::EventGap { .. })
    ));
    assert!(matches!(
        runtime.session_ledger(&session).await.unwrap().entries[0].state,
        LedgerTurnState::Completed { .. }
    ));
    runtime.shutdown().await.expect("runtime shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_prompt_result_recovers_the_exact_durable_binding_after_restart() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let config = runtime_config(&root, server.url());
    let snapshot = HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("durable crash-window system")
            .rules("durable crash-window rules"),
    )
    .unwrap();
    let turn_id = "lost-binding-turn";
    let prompt_text = "provider-marker-lost-binding";
    let prompt = Prompt {
        blocks: vec![PromptBlock::Text {
            text: prompt_text.into(),
        }],
        metadata: serde_json::Value::Null,
    };
    let expected_prompt_digest = prompt_digest_content(&prompt).unwrap();
    let key = TurnBindingKey::new(
        turn_id,
        expected_prompt_digest.clone(),
        0,
        snapshot.digest().clone(),
        "fast",
        Some("low".into()),
    )
    .unwrap();

    let (runtime, _) = Runtime::start(config.clone()).await.unwrap();
    let session = runtime
        .create_session_with_harness(
            session_config(workspace.clone(), "fast", Some("low")),
            &snapshot,
        )
        .await
        .unwrap();
    server.hold_agent_completions();
    let caller = tokio::spawn({
        let runtime = runtime.clone();
        let session = session.clone();
        let snapshot = snapshot.clone();
        async move {
            runtime
                .prompt_with_harness(&session, turn_id, prompt_text, &snapshot)
                .await
        }
    });
    wait_for_provider_marker(&server, prompt_text).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    server.release_agent_completions();

    let ledger = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let ledger = runtime.session_ledger(&session).await.unwrap();
            if matches!(
                ledger.entries.first().map(|entry| &entry.state),
                Some(LedgerTurnState::Completed { .. })
            ) {
                break ledger;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native Turn settles after its caller disappears");
    assert_eq!(ledger.entries[0].prompt_digest, expected_prompt_digest);
    let before_restart = match runtime
        .turn_binding_status(&session, key.clone())
        .await
        .unwrap()
    {
        TurnBindingStatus::Complete { record } => record,
        _ => panic!("durable binding record must exist before terminal success is exposed"),
    };
    assert_eq!(
        runtime
            .turn_binding_status(&session, key.clone())
            .await
            .unwrap(),
        TurnBindingStatus::Complete {
            record: before_restart.clone()
        },
        "status lookup is idempotent"
    );
    let receipt_bytes = serde_json::to_vec(before_restart.receipt()).unwrap();
    let record_bytes = before_restart.to_json_vec().unwrap();
    assert_eq!(
        sophon_sdk::TurnBindingRecord::from_json_slice(&record_bytes).unwrap(),
        before_restart
    );
    runtime.shutdown().await.unwrap();

    let (restarted, _) = Runtime::start(config).await.unwrap();
    restarted
        .resume_session_with_harness(
            session.clone(),
            session_config(workspace, "fast", Some("low")),
            &snapshot,
        )
        .await
        .unwrap();
    let recovered = match restarted
        .turn_binding_status(&session, key.clone())
        .await
        .unwrap()
    {
        TurnBindingStatus::Complete { record } => record,
        _ => panic!("durable binding record must survive Runtime restart"),
    };
    assert_eq!(recovered, before_restart);
    assert_eq!(
        serde_json::to_vec(recovered.receipt()).unwrap(),
        receipt_bytes
    );
    assert_eq!(
        recovered.receipt().binding_id(),
        before_restart.receipt().binding_id()
    );
    assert!(
        restarted
            .events_after(
                &session,
                recovered.receipt().complete_cursor().after_sequence()
            )
            .await
            .is_err(),
        "a recovered historical cursor is evidence, not a claim that the old live journal survived"
    );

    let conflicting_snapshot =
        HarnessSnapshot::new(HarnessContent::new().system_prompt("conflicting recovery snapshot"))
            .unwrap();
    for conflict in [
        TurnBindingKey::new(
            turn_id,
            expected_prompt_digest.clone(),
            0,
            conflicting_snapshot.digest().clone(),
            "fast",
            Some("low".into()),
        )
        .unwrap(),
        TurnBindingKey::new(
            turn_id,
            expected_prompt_digest.clone(),
            0,
            snapshot.digest().clone(),
            "deep",
            Some("xhigh".into()),
        )
        .unwrap(),
        TurnBindingKey::new(
            turn_id,
            prompt_digest("conflicting prompt"),
            0,
            snapshot.digest().clone(),
            "fast",
            Some("low".into()),
        )
        .unwrap(),
    ] {
        assert!(matches!(
            restarted.turn_binding_status(&session, conflict).await,
            Err(Error::Harness(HarnessError::BindingRecordConflict(_)))
        ));
    }
    restarted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ledger_without_a_binding_record_remains_compatible_and_reports_absent() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("temp root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let snapshot =
        HarnessSnapshot::new(HarnessContent::new().system_prompt("legacy ledger compatibility"))
            .unwrap();
    let (runtime, _) = Runtime::start(runtime_config(&root, server.url()))
        .await
        .unwrap();
    let session = runtime
        .create_session_with_harness(session_config(workspace, "fast", Some("high")), &snapshot)
        .await
        .unwrap();
    runtime
        .prompt(&session, "ordinary-turn", "ordinary prompt without binding")
        .await
        .unwrap();
    let ledger = runtime.session_ledger(&session).await.unwrap();
    assert!(matches!(
        ledger.entries[0].state,
        LedgerTurnState::Completed { .. }
    ));
    assert_eq!(
        runtime
            .turn_binding_status(
                &session,
                TurnBindingKey::new(
                    "ordinary-turn",
                    prompt_digest("ordinary prompt without binding"),
                    0,
                    snapshot.digest().clone(),
                    "fast",
                    Some("high".into()),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TurnBindingStatus::Absent
    );
    assert!(
        !serde_json::to_string(&ledger).unwrap().contains("binding"),
        "the existing Session ledger schema remains unchanged"
    );
    runtime.shutdown().await.unwrap();
}
