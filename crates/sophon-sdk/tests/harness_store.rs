// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! The content-addressed harness snapshot contract, exercised the way a Host
//! exercises it: through the public façade, against the reference
//! implementation, and against a backend that behaves like a mutable store.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use sophon_sdk::{
    ConformanceOpen, HarnessContent, HarnessDigest, HarnessError, HarnessEvidenceKind,
    HarnessEvidenceRef, HarnessPut, HarnessRefinement, HarnessRefinementPatch, HarnessSnapshot,
    HarnessStore, HarnessStoreError, LocalHarnessStore, MAX_HARNESS_EVIDENCE_REFS,
    run_harness_store_conformance,
};

fn snapshot(prompt: &str, rules: Option<&str>) -> HarnessSnapshot {
    let mut content = HarnessContent::new().system_prompt(prompt);
    if let Some(rules) = rules {
        content = content.rules(rules);
    }
    HarnessSnapshot::new(content).expect("valid snapshot")
}

#[test]
fn the_reference_harness_store_passes_the_public_harness_store_conformance() {
    let directory = tempfile::tempdir().unwrap();
    run_harness_store_conformance(|phase| {
        assert!(matches!(
            phase,
            ConformanceOpen::Fresh | ConformanceOpen::Concurrent | ConformanceOpen::Reopen
        ));
        Ok(Arc::new(LocalHarnessStore::new(directory.path())?) as Arc<dyn HarnessStore>)
    })
    .expect("the reference harness store satisfies its own published contract");
}

/// A backend that lets a later write replace the bytes reachable under a
/// digest is exactly the second mutable harness store this contract forbids.
#[test]
fn a_backend_that_replaces_content_under_a_digest_fails_the_harness_store_conformance() {
    #[derive(Default)]
    struct MutableStore(Mutex<HashMap<String, HarnessSnapshot>>);
    impl HarnessStore for MutableStore {
        fn get(
            &self,
            digest: &HarnessDigest,
        ) -> Result<Option<HarnessSnapshot>, HarnessStoreError> {
            Ok(self.0.lock().unwrap().get(digest.as_str()).cloned())
        }
        fn put(&self, snapshot: &HarnessSnapshot) -> Result<HarnessPut, HarnessStoreError> {
            self.0
                .lock()
                .unwrap()
                .insert(snapshot.digest().as_str().to_owned(), snapshot.clone());
            Ok(HarnessPut::Stored)
        }
    }

    let store = Arc::new(MutableStore::default());
    let error = run_harness_store_conformance(|_| Ok(store.clone() as Arc<dyn HarnessStore>))
        .expect_err("an overwriting backend cannot pass a content-addressed contract");
    assert!(
        error.to_string().contains("never replaced"),
        "unexpected conformance failure: {error}"
    );
}

#[test]
fn refinement_produces_a_new_address_and_never_disturbs_the_base_content() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalHarnessStore::new(directory.path()).unwrap();
    let base = snapshot("base instructions", Some("base rules"));
    assert_eq!(store.put(&base).unwrap(), HarnessPut::Stored);

    let refined = HarnessRefinementPatch::new(
        base.digest().clone(),
        [HarnessRefinement::SetRules("refined rules".into())],
    )
    .unwrap()
    .with_evidence([HarnessEvidenceRef::new(
        HarnessEvidenceKind::TurnBinding,
        format!("sha256:{}", "a".repeat(64)),
    )
    .unwrap()])
    .unwrap()
    .apply(&base)
    .expect("matching base");
    assert_eq!(store.put(&refined).unwrap(), HarnessPut::Stored);

    assert_ne!(refined.digest(), base.digest());
    assert_eq!(store.get(base.digest()).unwrap().as_ref(), Some(&base));
    assert_eq!(
        store.get(refined.digest()).unwrap().as_ref(),
        Some(&refined)
    );
    assert_eq!(
        store
            .get(base.digest())
            .unwrap()
            .unwrap()
            .content()
            .rules_value(),
        Some("base rules"),
        "committing a successor must not rewrite the base content address"
    );
}

#[test]
fn stored_content_fails_closed_when_a_backend_corrupts_it() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalHarnessStore::new(directory.path()).unwrap();
    let stored = snapshot("corruption probe", None);
    store.put(&stored).unwrap();
    let path = store.path().to_owned();
    drop(store);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE snapshots SET payload=?1 WHERE digest=?2",
            rusqlite::params![
                b"{\"not\":\"a snapshot\"}".to_vec(),
                stored.digest().as_str()
            ],
        )
        .unwrap();
    drop(connection);

    let reopened = LocalHarnessStore::new(directory.path()).unwrap();
    assert!(matches!(
        reopened.get(stored.digest()),
        Err(HarnessStoreError::Corrupt(_))
    ));
}

#[test]
fn the_store_rejects_incomplete_schema_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalHarnessStore::new(directory.path()).unwrap();
    let path = store.path().to_owned();
    drop(store);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("DELETE FROM metadata WHERE key='schema_marker'", [])
        .unwrap();
    drop(connection);
    assert!(matches!(
        LocalHarnessStore::new(directory.path()),
        Err(HarnessStoreError::Corrupt(_))
    ));
}

#[test]
fn cited_evidence_is_bounded_typed_and_deduplicated() {
    let base = snapshot("evidence base", None);
    let reference =
        HarnessEvidenceRef::new(HarnessEvidenceKind::Evaluation, "evaluation-1").unwrap();
    assert_eq!(reference.kind(), HarnessEvidenceKind::Evaluation);
    assert_eq!(reference.identity(), "evaluation-1");
    assert_eq!(reference.digest(), None);
    let pinned = HarnessEvidenceRef::new(HarnessEvidenceKind::Artifact, "artifact-1")
        .unwrap()
        .with_digest(format!("sha256:{}", "b".repeat(64)))
        .unwrap();
    assert_eq!(
        pinned.digest(),
        Some(format!("sha256:{}", "b".repeat(64)).as_str())
    );
    assert!(matches!(
        HarnessEvidenceRef::new(HarnessEvidenceKind::Artifact, "artifact-1")
            .unwrap()
            .with_digest("not-a-digest"),
        Err(HarnessError::Invalid(_))
    ));
    assert!(matches!(
        HarnessEvidenceRef::new(HarnessEvidenceKind::Artifact, "  "),
        Err(HarnessError::Invalid(_))
    ));

    let patch = HarnessRefinementPatch::new(base.digest().clone(), [HarnessRefinement::ClearRules])
        .unwrap();
    assert!(patch.evidence().is_empty());
    assert!(matches!(
        patch
            .clone()
            .with_evidence([reference.clone(), reference.clone()]),
        Err(HarnessError::Invalid(_))
    ));
    assert!(matches!(
        patch
            .clone()
            .with_evidence((0..=MAX_HARNESS_EVIDENCE_REFS).map(|index| {
                HarnessEvidenceRef::new(HarnessEvidenceKind::Artifact, format!("artifact-{index}"))
                    .unwrap()
            })),
        Err(HarnessError::Invalid(_))
    ));

    // Evidence rides on the patch, so citing it cannot move the successor's
    // content address.
    let cited = patch
        .clone()
        .with_evidence([reference.clone(), pinned.clone()])
        .unwrap();
    assert_eq!(cited.evidence(), [reference, pinned].as_slice());
    let uncited_base = snapshot("evidence base", Some("rules to clear"));
    let uncited_patch = HarnessRefinementPatch::new(
        uncited_base.digest().clone(),
        [HarnessRefinement::ClearRules],
    )
    .unwrap();
    assert_eq!(
        uncited_patch.apply(&uncited_base).unwrap().digest(),
        uncited_patch
            .with_evidence([HarnessEvidenceRef::new(
                HarnessEvidenceKind::TurnBinding,
                "turn-binding-1"
            )
            .unwrap()])
            .unwrap()
            .apply(&uncited_base)
            .unwrap()
            .digest()
    );
}

#[test]
fn a_serialized_patch_without_evidence_still_decodes() {
    let base = snapshot("compatibility base", None);
    let legacy = serde_json::json!({
        "base_digest": base.digest().as_str(),
        "changes": [{"operation": "set_rules", "value": "added rules"}],
    });
    let patch: HarnessRefinementPatch = serde_json::from_value(legacy).expect("additive decode");
    assert!(patch.evidence().is_empty());
    assert_eq!(
        patch.apply(&base).unwrap().content().rules_value(),
        Some("added rules")
    );

    let cited = patch
        .with_evidence([HarnessEvidenceRef::new(
            HarnessEvidenceKind::TurnBinding,
            "turn-binding-2",
        )
        .unwrap()])
        .unwrap();
    let encoded = serde_json::to_value(&cited).unwrap();
    assert_eq!(
        encoded["evidence"],
        serde_json::json!([{"kind": "turn_binding", "identity": "turn-binding-2"}])
    );
    assert_eq!(
        serde_json::from_value::<HarnessRefinementPatch>(encoded).unwrap(),
        cited
    );
}

/// The façade keeps no mutable harness state: no public harness API takes
/// `&mut self`, and the store contract publishes no replace, update or delete
/// operation. This reads the shipped source because the guarantee is about the
/// absence of an API, which no runtime call can demonstrate.
#[test]
fn the_public_harness_surface_offers_no_mutation_of_stored_content() {
    let crate_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let sources = [
        "src/harness.rs",
        "src/harness/store.rs",
        "src/harness/conformance.rs",
    ];
    let mut mutable_methods = Vec::new();
    let mut store_methods = Vec::new();
    for source in sources {
        let text = std::fs::read_to_string(crate_root.join(source)).unwrap();
        for line in text.lines().map(str::trim) {
            if !line.starts_with("pub fn ") && !line.starts_with("fn ") {
                continue;
            }
            if line.contains("&mut self") {
                mutable_methods.push(format!("{source}: {line}"));
            }
            if source == "src/harness/store.rs" && line.starts_with("fn ") {
                store_methods.push(line.to_owned());
            }
        }
    }
    assert!(
        mutable_methods.is_empty(),
        "harness types must not expose in-place mutation: {mutable_methods:?}"
    );
    for forbidden in ["update", "replace", "delete", "remove", "set_", "overwrite"] {
        assert!(
            !store_methods
                .iter()
                .any(|method| method.contains(&format!("fn {forbidden}"))),
            "the harness store contract must not publish a '{forbidden}' operation: \
             {store_methods:?}"
        );
    }
}
