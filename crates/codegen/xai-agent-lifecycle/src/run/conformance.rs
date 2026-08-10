//! Backend-neutral contracts every host-injected Run and artifact store must
//! satisfy.
//!
//! A host that replaces [`LocalRunStore`](super::LocalRunStore) or
//! [`LocalArtifactStore`](super::LocalArtifactStore) is replacing the
//! authority, not decorating it. These suites are the executable definition of
//! what "the same semantics as the reference implementation" means: validation
//! ordering, CAS identity, tombstones, commit-unknown handling, byte bounds and
//! restart behaviour. They are ordinary public API so a downstream product can
//! run them against its production backend in its own test suite.

use std::sync::Arc;

use super::{
    ArtifactStore, CURRENT_RUN_SCHEMA, CapabilityPolicy, CreateRunRequest, GoalSpec,
    HarnessSnapshotPin, MAX_RUN_ENVELOPE_BYTES, ResourceVector, RunController, RunDriverSpec,
    RunEnvelope, RunError, RunEvent, RunEventCursor, RunEventKind, RunId, RunRevision, RunStatus,
    RunStore, SessionRef, StoreCommit, StoreCommitResult,
};
use crate::run::CommandId;

/// Which handle onto one authority the suite is asking for.
///
/// `Fresh` and `Concurrent` must observe each other's commits. `Reopen` is
/// taken after the earlier handles were dropped and stands in for a restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreConformanceOpen {
    Fresh,
    Concurrent,
    Reopen,
}

fn suite_error(message: impl Into<String>) -> RunError {
    RunError::Integrity(format!("conformance: {}", message.into()))
}

fn conformance_request(run: &str, session: &str) -> CreateRunRequest {
    let capabilities = ["session.turn".to_owned()];
    CreateRunRequest::new(
        CommandId::new(format!("{run}-create")).expect("conformance command id"),
        SessionRef::new(session).expect("conformance session"),
        GoalSpec::new("run store conformance"),
        RunDriverSpec::AutonomousTurnLoop {
            session: SessionRef::new(session).expect("conformance session"),
            strategy_revision: 0,
        },
        CapabilityPolicy::new(
            capabilities.clone(),
            capabilities.clone(),
            capabilities.clone(),
        ),
        ResourceVector::default()
            .iterations(16)
            .agent_calls(16)
            .agent_concurrency(2)
            .active_ms(600_000)
            .wall_ms(600_000)
            .tokens(100_000)
            .cost_micros(1_000_000)
            .artifact_bytes(1_000_000),
    )
    .run_id(RunId::new(run).expect("conformance run id"))
    .harness_snapshot(HarnessSnapshotPin {
        digest: "b".repeat(64),
        descriptor_digest: "c".repeat(64),
        capability_revision: "conformance-v1".into(),
        negotiated_capabilities: Default::default(),
        revision: 1,
        evidence: Vec::new(),
        provenance: "conformance".into(),
    })
}

/// Produces the next envelope for `current` without going through the reducer,
/// so the suite can drive the store's CAS directly.
fn successor(current: &RunEnvelope, status: RunStatus, event: &str) -> RunEnvelope {
    let revision = RunRevision::new(current.run.revision.get() + 1);
    let cursor = RunEventCursor::new(current.run.event_cursor.get() + 1);
    let mut next = current.clone();
    next.run.revision = revision;
    next.run.status = status;
    next.run.event_cursor = cursor;
    next.events.push_back(RunEvent {
        cursor,
        revision,
        kind: RunEventKind::new(event).expect("conformance event kind"),
        at_ms: cursor.get(),
    });
    next
}

/// Runs the backend-neutral [`RunStore`] contract against independent handles
/// to one authority.
///
/// The opener answers with a handle for each [`StoreConformanceOpen`] phase.
/// Passing this suite means a host store applies the SDK's validation ordering,
/// revision CAS, one-nonterminal-Run-per-Session rule, tombstone finality and
/// restart semantics exactly as [`LocalRunStore`](super::LocalRunStore) does.
pub fn run_run_store_conformance<F>(mut open: F) -> Result<(), RunError>
where
    F: FnMut(StoreConformanceOpen) -> Result<Arc<dyn RunStore>, RunError>,
{
    let store = open(StoreConformanceOpen::Fresh)?;
    let run = RunId::new("run_conformance").expect("conformance run id");

    if store.load(&run)?.is_some() {
        return Err(suite_error("fresh authority already holds a Run"));
    }
    if !store.list()?.is_empty() {
        return Err(suite_error("fresh authority listed Runs"));
    }

    // Creation goes through the reducer so the suite commits only envelopes the
    // SDK itself considers valid.
    let mut controller = RunController::open(Arc::clone(&store))?;
    let created = controller.create_run(conformance_request("run_conformance", "session_a"), 1)?;
    let first = created.snapshot.clone();
    if first.run.revision != RunRevision::new(1) {
        return Err(suite_error("a created Run did not start at revision 1"));
    }
    match store.load(&run)? {
        Some(loaded) if loaded == first => {}
        _ => return Err(suite_error("a created Run did not load back unchanged")),
    }
    if store.list()?.len() != 1 {
        return Err(suite_error("a created Run was not listed exactly once"));
    }

    // Validation ordering is the SDK's: an envelope whose id disagrees with the
    // commit, and a revision that is not the exact successor, are both refused
    // before any storage effect.
    let mut wrong_id = successor(&first, RunStatus::UserPaused, "conformance_wrong_id");
    wrong_id.run.id = RunId::new("run_conformance_other").expect("conformance run id");
    if store
        .commit(StoreCommit {
            run_id: run.clone(),
            expected_revision: Some(first.run.revision),
            next: wrong_id,
            finished_iteration: None,
        })
        .is_ok()
    {
        return Err(suite_error(
            "a commit whose envelope names another Run was accepted",
        ));
    }
    let mut skipped = successor(&first, RunStatus::UserPaused, "conformance_skipped");
    skipped.run.revision = RunRevision::new(first.run.revision.get() + 2);
    if store
        .commit(StoreCommit {
            run_id: run.clone(),
            expected_revision: Some(first.run.revision),
            next: skipped,
            finished_iteration: None,
        })
        .is_ok()
    {
        return Err(suite_error("a commit that skipped a revision was accepted"));
    }
    match store.load(&run)? {
        Some(loaded) if loaded.run.revision == first.run.revision => {}
        _ => return Err(suite_error("a refused commit changed the stored Run")),
    }

    // Recreating an existing Run from absence conflicts rather than clobbering.
    if store.commit(StoreCommit {
        run_id: run.clone(),
        expected_revision: None,
        next: first.clone(),
        finished_iteration: None,
    })? != (StoreCommitResult::Conflict {
        actual: Some(first.run.revision),
    }) {
        return Err(suite_error(
            "create-from-absent over an existing Run did not conflict",
        ));
    }

    // A second nonterminal Run in the same Session is refused durably, not just
    // by a controller cache.
    let mut sibling_controller = RunController::open(Arc::clone(&store))?;
    if sibling_controller
        .create_run(
            conformance_request("run_conformance_sibling", "session_a"),
            2,
        )
        .is_ok()
    {
        return Err(suite_error(
            "a second nonterminal Run was admitted for one Session",
        ));
    }

    // Two independent handles racing the same expectation: exactly one applies.
    let concurrent = open(StoreConformanceOpen::Concurrent)?;
    match concurrent.load(&run)? {
        Some(loaded) if loaded == first => {}
        _ => {
            return Err(suite_error(
                "a concurrent handle did not observe the created Run",
            ));
        }
    }
    let winner = successor(&first, RunStatus::UserPaused, "conformance_winner");
    let loser = successor(&first, RunStatus::Blocked, "conformance_loser");
    let first_outcome = store.commit(StoreCommit {
        run_id: run.clone(),
        expected_revision: Some(first.run.revision),
        next: winner.clone(),
        finished_iteration: None,
    })?;
    let second_outcome = concurrent.commit(StoreCommit {
        run_id: run.clone(),
        expected_revision: Some(first.run.revision),
        next: loser,
        finished_iteration: None,
    })?;
    let applied = match (&first_outcome, &second_outcome) {
        (StoreCommitResult::Applied, StoreCommitResult::Conflict { actual }) => {
            if *actual != Some(winner.run.revision) {
                return Err(suite_error(
                    "a conflict did not report the actual stored revision",
                ));
            }
            winner.clone()
        }
        (StoreCommitResult::Conflict { .. }, StoreCommitResult::Applied) => store
            .load(&run)?
            .ok_or_else(|| suite_error("the applied Run vanished"))?,
        _ => {
            return Err(suite_error(format!(
                "a contended commit produced {first_outcome:?} and {second_outcome:?} rather than \
                 one applied and one conflict"
            )));
        }
    };
    if applied.run.revision != RunRevision::new(first.run.revision.get() + 1) {
        return Err(suite_error(
            "the applied revision did not advance by exactly one",
        ));
    }

    drop(concurrent);
    drop(store);

    // Restart: the exact envelope survives and a pre-restart expectation still
    // applies.
    let reopened = open(StoreConformanceOpen::Reopen)?;
    match reopened.load(&run)? {
        Some(loaded) if loaded == applied => {}
        _ => return Err(suite_error("the Run did not survive a restart unchanged")),
    }
    let terminal = successor(&applied, RunStatus::Cancelled, "conformance_cancelled");
    if reopened.commit(StoreCommit {
        run_id: run.clone(),
        expected_revision: Some(applied.run.revision),
        next: terminal.clone(),
        finished_iteration: None,
    })? != StoreCommitResult::Applied
    {
        return Err(suite_error(
            "a pre-restart expectation was refused after restart",
        ));
    }

    // A tombstone is final: later commits report Tombstoned rather than
    // conflicting or applying, and the tombstoned envelope stays readable.
    let tombstone = successor(&terminal, RunStatus::Tombstoned, "conformance_tombstoned");
    if reopened.commit(StoreCommit {
        run_id: run.clone(),
        expected_revision: Some(terminal.run.revision),
        next: tombstone.clone(),
        finished_iteration: None,
    })? != StoreCommitResult::Applied
    {
        return Err(suite_error("a Run could not be tombstoned"));
    }
    let after_tombstone = successor(&tombstone, RunStatus::UserPaused, "conformance_resurrect");
    if reopened.commit(StoreCommit {
        run_id: run.clone(),
        expected_revision: Some(tombstone.run.revision),
        next: after_tombstone,
        finished_iteration: None,
    })? != StoreCommitResult::Tombstoned
    {
        return Err(suite_error("a tombstoned Run accepted a later commit"));
    }
    match reopened.load(&run)? {
        Some(loaded) if loaded.run.status == RunStatus::Tombstoned => {}
        _ => return Err(suite_error("a tombstoned Run stopped loading")),
    }

    // The schema identity a host must persist is the SDK's, not its own.
    let prepared = StoreCommit {
        run_id: run.clone(),
        expected_revision: Some(tombstone.run.revision),
        next: successor(&tombstone, RunStatus::UserPaused, "conformance_schema"),
        finished_iteration: None,
    }
    .validate_and_encode()?;
    if prepared.schema != CURRENT_RUN_SCHEMA || prepared.envelope.len() > MAX_RUN_ENVELOPE_BYTES {
        return Err(suite_error("prepared commits lost the current Run schema"));
    }
    Ok(())
}

/// Runs the backend-neutral [`ArtifactStore`] contract against independent
/// handles to one authority.
///
/// Passing this suite means a host artifact backend is content-addressed with
/// verified digests, refuses malformed digests and oversize reads, keeps
/// reachability durable, and survives a restart, exactly as
/// [`LocalArtifactStore`](super::LocalArtifactStore) does.
pub fn run_artifact_store_conformance<F>(mut open: F) -> Result<(), RunError>
where
    F: FnMut(StoreConformanceOpen) -> Result<Arc<dyn ArtifactStore>, RunError>,
{
    let store = open(StoreConformanceOpen::Fresh)?;
    let payload = b"durable conformance evidence".to_vec();

    let metadata = store.put(&payload, "run")?;
    if metadata.size != payload.len() as u64 {
        return Err(suite_error(
            "stored artifact size disagrees with the payload",
        ));
    }
    if metadata.digest != format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(&payload)) {
        return Err(suite_error(
            "artifact identity is not the SHA-256 of its bytes",
        ));
    }
    if !metadata.reachable || metadata.pinned {
        return Err(suite_error(
            "a freshly stored artifact was not reachable and unpinned",
        ));
    }
    if store.get(&metadata.digest, payload.len() as u64)? != payload {
        return Err(suite_error("an artifact did not read back unchanged"));
    }

    // Content addressing means storing the same bytes twice is one artifact.
    let again = store.put(&payload, "run")?;
    if again.digest != metadata.digest {
        return Err(suite_error(
            "identical bytes produced two artifact identities",
        ));
    }

    // A digest that is not a digest never reaches the filesystem.
    for malformed in ["../etc/passwd", "", "not-a-digest", &"f".repeat(63)] {
        if store.get(malformed, 1024).is_ok() {
            return Err(suite_error(format!(
                "a malformed digest {malformed:?} was read"
            )));
        }
        if store.set_reachability(malformed, true, true).is_ok() {
            return Err(suite_error(format!(
                "a malformed digest {malformed:?} accepted a reachability change"
            )));
        }
    }

    // A caller's read bound is a bound: an artifact larger than it is refused
    // rather than truncated.
    if store
        .get(&metadata.digest, (payload.len() - 1) as u64)
        .is_ok()
    {
        return Err(suite_error(
            "an artifact over the caller's read bound was returned",
        ));
    }

    // An absent artifact is an error, never empty bytes.
    let absent = "a".repeat(64);
    if store.get(&absent, 1024).is_ok() {
        return Err(suite_error("an artifact that was never stored was read"));
    }

    store.set_reachability(&metadata.digest, true, false)?;
    let concurrent = open(StoreConformanceOpen::Concurrent)?;
    if concurrent.get(&metadata.digest, payload.len() as u64)? != payload {
        return Err(suite_error(
            "a concurrent handle did not observe the stored artifact",
        ));
    }

    drop(concurrent);
    drop(store);

    let reopened = open(StoreConformanceOpen::Reopen)?;
    if reopened.get(&metadata.digest, payload.len() as u64)? != payload {
        return Err(suite_error("an artifact did not survive a restart"));
    }
    reopened.set_reachability(&metadata.digest, false, true)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::{LocalArtifactStore, LocalRunStore};

    #[test]
    fn local_run_store_passes_the_public_conformance() {
        let directory = tempfile::tempdir().unwrap();
        run_run_store_conformance(|_| {
            Ok(Arc::new(LocalRunStore::new(directory.path())?) as Arc<dyn RunStore>)
        })
        .unwrap();
    }

    #[test]
    fn local_artifact_store_passes_the_public_conformance() {
        let directory = tempfile::tempdir().unwrap();
        run_artifact_store_conformance(|_| {
            Ok(Arc::new(LocalArtifactStore::new(directory.path(), 1024)?)
                as Arc<dyn ArtifactStore>)
        })
        .unwrap();
    }
}
