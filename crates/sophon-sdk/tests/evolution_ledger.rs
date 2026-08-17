// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! The continual-harness evolution contract, exercised the way a Host
//! exercises it: plan → validate → apply → commit → render → derive an
//! evolved immutable snapshot → roll back, with every activation flowing
//! through the existing content-addressed harness machinery.

use std::sync::Arc;

use sophon_sdk::{
    ConformanceOpen, EvolutionCommit, EvolutionError, EvolutionState, EvolutionStore,
    HARNESS_SUPPLEMENT_BEGIN, HARNESS_SUPPLEMENT_END, HarnessContent, HarnessEntryKind,
    HarnessEvidenceKind, HarnessScope, HarnessSkillContract, HarnessSnapshot, LocalEvolutionStore,
    RefinementEdit, RefinementPlanner, RefinementProposal, evolution_commit_reconciled,
    evolved_refinement, parse_planner_reply, render_harness_supplement,
    run_evolution_store_conformance,
};

fn base_snapshot() -> HarnessSnapshot {
    HarnessSnapshot::new(
        HarnessContent::new()
            .system_prompt("You are a careful coding agent.")
            .rules("Never push without instruction."),
    )
    .unwrap()
}

fn proposal(edits: Vec<RefinementEdit>) -> RefinementProposal {
    RefinementProposal::new(
        "persist trajectory lessons",
        "the trajectory produced reusable evidence",
        "future turns reuse the lessons without rediscovery",
        edits,
    )
    .unwrap()
}

#[test]
fn the_reference_evolution_store_passes_the_public_evolution_conformance() {
    let directory = tempfile::tempdir().unwrap();
    run_evolution_store_conformance(|phase| {
        assert!(matches!(
            phase,
            ConformanceOpen::Fresh | ConformanceOpen::Concurrent | ConformanceOpen::Reopen
        ));
        Ok(Arc::new(LocalEvolutionStore::new(directory.path())?) as Arc<dyn EvolutionStore>)
    })
    .expect("the reference evolution store satisfies its own published contract");
}

/// The complete self-evolution loop: a planned proposal evolves the ledger,
/// the ledger renders into a marked supplement, the supplement derives a new
/// immutable snapshot via a CAS patch citing the refinement event, and a
/// rollback produces a third snapshot equal in content to the original.
#[test]
fn a_refinement_evolves_the_immutable_snapshot_and_rolls_back_cleanly() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalEvolutionStore::new(directory.path()).unwrap();
    let scope = HarnessScope::session("evolving-session");
    let base = base_snapshot();

    // Plan → parse, exactly as a Host feeds a planner Turn's reply back.
    let reply = r#"```json
    {
      "summary": "record the build system and a review skill",
      "rationale": "both were discovered through repeated trajectory evidence",
      "expected_outcome": "future turns build correctly and delegate reviews",
      "edits": [
        {"action": "create", "kind": "memory", "title": "Build Uses Bazel",
         "content": "This workspace builds with bazel build //... , not cargo alone.",
         "reason": "two failed cargo-only builds"},
        {"action": "create", "kind": "skill", "title": "Diff Reviewer",
         "content": "Reviews a diff for regressions before commit.",
         "skill": {"invocation": "await skills.review.run(diff)",
                   "arguments": {"diff": "unified diff text"}},
         "reason": "used successfully twice"}
      ]
    }
    ```"#;
    let proposal = parse_planner_reply(reply).unwrap().unwrap();

    // Apply and commit under revision CAS.
    let state = EvolutionState::new(scope.clone()).unwrap();
    let (state, event) = state.apply(&proposal, "refine", [], 1_000).unwrap();
    assert_eq!(event.applied().len(), 2);
    assert!(event.rejected().is_empty());
    let commit = store.commit(&state, &event).unwrap();
    assert!(evolution_commit_reconciled(
        &commit,
        store.load(&scope).unwrap().as_ref(),
        &state
    ));

    // Render → derive the evolved immutable snapshot.
    let history = store.history(&scope, 8).unwrap();
    let patch = evolved_refinement(&base, None, Some(&state), &history)
        .unwrap()
        .expect("a non-empty ledger evolves the snapshot");
    assert_eq!(patch.evidence().len(), 1);
    assert_eq!(patch.evidence()[0].kind(), HarnessEvidenceKind::Refinement);
    assert_eq!(patch.evidence()[0].identity(), event.event_id());
    let evolved = patch.apply(&base).unwrap();
    assert_ne!(evolved.digest(), base.digest());
    let evolved_prompt = evolved.content().system_prompt_value().unwrap();
    assert!(evolved_prompt.starts_with("You are a careful coding agent."));
    assert!(evolved_prompt.contains(HARNESS_SUPPLEMENT_BEGIN));
    assert!(evolved_prompt.contains("Build Uses Bazel"));
    assert!(evolved_prompt.contains("`await skills.review.run(diff)`"));
    assert!(evolved_prompt.ends_with(HARNESS_SUPPLEMENT_END));

    // Deriving again from the evolved snapshot is a fixpoint: no new patch.
    assert!(
        evolved_refinement(&evolved, None, Some(&state), &history)
            .unwrap()
            .is_none()
    );

    // Roll back, commit the rollback, and derive once more: the supplement
    // disappears and the prompt returns to the base content.
    let (rolled_back, rollback_event) =
        state.rollback(&event, "lessons were wrong", 2_000).unwrap();
    assert!(matches!(
        store.commit(&rolled_back, &rollback_event).unwrap(),
        EvolutionCommit::Committed
    ));
    let restored_patch = evolved_refinement(&evolved, None, Some(&rolled_back), &[])
        .unwrap()
        .expect("removing the supplement evolves the snapshot again");
    let restored = restored_patch.apply(&evolved).unwrap();
    assert_eq!(
        restored.content().system_prompt_value(),
        base.content().system_prompt_value()
    );
}

#[test]
fn session_entries_overlay_global_entries_in_the_merged_render() {
    let global = EvolutionState::new(HarnessScope::Global).unwrap();
    let (global, _) = global
        .apply(
            &proposal(vec![
                RefinementEdit::create(
                    HarnessEntryKind::Prompt,
                    "House Style",
                    "Global: keep functions short.",
                    "global lesson",
                ),
                RefinementEdit::create(
                    HarnessEntryKind::Memory,
                    "Org Fact",
                    "Global fact that survives the overlay.",
                    "global lesson",
                ),
            ]),
            "host",
            [],
            1_000,
        )
        .unwrap();
    let session = EvolutionState::new(HarnessScope::session("s-1")).unwrap();
    let (session, _) = session
        .apply(
            &proposal(vec![
                RefinementEdit::create(
                    HarnessEntryKind::Prompt,
                    "House Style",
                    "Session: this project prefers long-form modules.",
                    "project-specific override",
                )
                .with_id("house-style"),
            ]),
            "refine",
            [],
            2_000,
        )
        .unwrap();
    let rendered = render_harness_supplement(Some(&global), Some(&session), &[])
        .unwrap()
        .unwrap();
    assert!(rendered.contains("Session: this project prefers long-form modules."));
    assert!(!rendered.contains("Global: keep functions short."));
    assert!(rendered.contains("Global fact that survives the overlay."));
    assert!(rendered.contains("(session · house-style · v1)"));
    assert!(rendered.contains("(global · org-fact · v1)"));

    // Scope confusion fails closed.
    assert!(matches!(
        render_harness_supplement(Some(&session), None, &[]),
        Err(EvolutionError::ScopeMismatch { .. })
    ));
    assert!(matches!(
        render_harness_supplement(None, Some(&global), &[]),
        Err(EvolutionError::ScopeMismatch { .. })
    ));
}

#[test]
fn a_tampered_prompt_with_unpaired_markers_fails_closed() {
    let scope = HarnessScope::session("tamper");
    let state = EvolutionState::new(scope).unwrap();
    let (state, _) = state
        .apply(
            &proposal(vec![RefinementEdit::create(
                HarnessEntryKind::Memory,
                "A Fact",
                "content",
                "reason",
            )]),
            "refine",
            [],
            1_000,
        )
        .unwrap();
    let tampered = HarnessSnapshot::new(HarnessContent::new().system_prompt(format!(
        "prompt with a dangling {HARNESS_SUPPLEMENT_BEGIN} marker"
    )))
    .unwrap();
    assert!(matches!(
        evolved_refinement(&tampered, None, Some(&state), &[]),
        Err(EvolutionError::MalformedSupplement(_))
    ));
}

#[test]
fn the_planner_turn_carries_overview_history_and_scope_policy() {
    let scope = HarnessScope::session("planner");
    let state = EvolutionState::new(scope).unwrap();
    let (state, event) = state
        .apply(
            &proposal(vec![
                RefinementEdit::create(
                    HarnessEntryKind::Skill,
                    "Deploy Checker",
                    "Verifies a deploy before announcing success.",
                    "prevented one bad deploy",
                )
                .with_skill(HarnessSkillContract::new("await skills.deploy_check.run()")),
            ]),
            "refine",
            [],
            1_000,
        )
        .unwrap();
    let overview = render_harness_supplement(None, Some(&state), &[])
        .unwrap()
        .unwrap();
    let planner = RefinementPlanner::new("user: deploy failed\nagent: fixed by checking first")
        .harness_overview(overview)
        .recent_events(std::slice::from_ref(&event))
        .instructions("focus on deployment lessons");

    let system = planner.system_prompt();
    assert!(system.contains("NO_REFINEMENT"));
    assert!(system.contains("session scope only"));
    let system_global = planner.clone().allow_global(true).system_prompt();
    assert!(system_global.contains("global"));

    let user = planner.user_prompt();
    assert!(user.contains("Deploy Checker"));
    assert!(user.contains("persist trajectory lessons"));
    assert!(user.contains("focus on deployment lessons"));
    assert!(user.contains("deploy failed"));
}
