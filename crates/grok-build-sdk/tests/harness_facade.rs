use grok_build_sdk::{
    HarnessContent, HarnessError, HarnessRefinement, HarnessRefinementPatch, HarnessSnapshot,
    SessionConfig,
};
use std::path::PathBuf;

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
        "sha256:99c21b4c709a064b3bca6ff5bbf36609f39ada7b1f46a59e0df215edbcf9e516"
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
        Some("You are the desktop coding agent.")
    );
    assert_eq!(session.rules.as_deref(), Some("Keep changes reviewable."));

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
