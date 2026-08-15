//! The four host-backend conformance suites, exercised the way a downstream
//! product exercises them: through the public façade, against the reference
//! implementations. A host that swaps in its own authority runs exactly these
//! functions against its own store.

use std::sync::Arc;

use sophon_sdk::{
    ConformanceOpen, LocalSessionEvidenceStore, LocalSessionStateStore, SessionEvidenceStore,
    SessionStateStore,
    run::{
        ArtifactStore, LocalArtifactStore, LocalRunStore, RunStore, StoreConformanceOpen,
        run_artifact_store_conformance, run_run_store_conformance,
    },
    run_session_evidence_conformance, run_session_state_conformance,
};

#[test]
fn the_reference_run_store_passes_the_public_run_store_conformance() {
    let directory = tempfile::tempdir().unwrap();
    run_run_store_conformance(|phase| {
        assert!(matches!(
            phase,
            StoreConformanceOpen::Fresh
                | StoreConformanceOpen::Concurrent
                | StoreConformanceOpen::Reopen
        ));
        Ok(Arc::new(LocalRunStore::new(directory.path())?) as Arc<dyn RunStore>)
    })
    .expect("the reference Run store satisfies its own published contract");
}

#[test]
fn the_reference_evidence_store_passes_the_public_evidence_conformance() {
    let directory = tempfile::tempdir().unwrap();
    run_session_evidence_conformance(|_| {
        Ok(Arc::new(LocalSessionEvidenceStore::new(directory.path())?)
            as Arc<dyn SessionEvidenceStore>)
    })
    .expect("the reference session evidence store satisfies its own published contract");
}

#[test]
fn the_reference_session_state_store_passes_the_public_transcript_conformance() {
    let directory = tempfile::tempdir().unwrap();
    run_session_state_conformance(|phase| {
        assert!(matches!(
            phase,
            ConformanceOpen::Fresh | ConformanceOpen::Concurrent | ConformanceOpen::Reopen
        ));
        Ok(Arc::new(LocalSessionStateStore::new(directory.path())?) as Arc<dyn SessionStateStore>)
    })
    .expect("the reference session state store satisfies its own published contract");
}

#[test]
fn the_reference_artifact_store_passes_the_public_artifact_conformance() {
    let directory = tempfile::tempdir().unwrap();
    run_artifact_store_conformance(|_| {
        Ok(Arc::new(LocalArtifactStore::new(directory.path(), 4096)?) as Arc<dyn ArtifactStore>)
    })
    .expect("the reference artifact store satisfies its own published contract");
}

/// A backend that quietly invents its own revision instead of committing the
/// prepared successor is exactly the failure these suites exist to catch.
#[test]
fn a_backend_that_drops_commits_fails_the_run_store_conformance() {
    struct Amnesiac;
    impl RunStore for Amnesiac {
        fn load(
            &self,
            _: &sophon_sdk::run::RunId,
        ) -> Result<Option<sophon_sdk::run::RunEnvelope>, sophon_sdk::run::RunError> {
            Ok(None)
        }
        fn list(&self) -> Result<Vec<sophon_sdk::run::RunEnvelope>, sophon_sdk::run::RunError> {
            Ok(Vec::new())
        }
        fn commit(
            &self,
            _: sophon_sdk::run::StoreCommit,
        ) -> Result<sophon_sdk::run::StoreCommitResult, sophon_sdk::run::RunError> {
            Ok(sophon_sdk::run::StoreCommitResult::Applied)
        }
    }

    let error = run_run_store_conformance(|_| Ok(Arc::new(Amnesiac) as Arc<dyn RunStore>))
        .expect_err("a store that forgets every commit cannot pass");
    assert!(
        error.to_string().contains("conformance"),
        "the suite should name itself in the failure: {error}"
    );
}
