use super::super::*;

struct LeaseDropSpy(Arc<std::sync::atomic::AtomicBool>);
impl crate::SessionStateLease for LeaseDropSpy {}
impl Drop for LeaseDropSpy {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[test]
fn uncertain_session_leases_are_quarantined_for_process_lifetime() {
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    quarantine_session_leases(vec![Box::new(LeaseDropSpy(dropped.clone()))]);
    assert!(!dropped.load(Ordering::Acquire));
}

#[test]
fn session_lease_admission_releases_safe_failures_and_commits_only_residency() {
    let leases = RefCell::new(HashMap::new());
    let id = SessionId("admission-state".into());

    let safe_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    drop(SessionLeaseAdmission::new(
        &leases,
        &id,
        Some(Box::new(LeaseDropSpy(safe_dropped.clone()))),
    ));
    assert!(safe_dropped.load(Ordering::Acquire));
    assert!(leases.borrow().is_empty());

    let resident_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut resident = SessionLeaseAdmission::new(
        &leases,
        &id,
        Some(Box::new(LeaseDropSpy(resident_dropped.clone()))),
    );
    resident.commit_resident();
    drop(resident);
    assert!(!resident_dropped.load(Ordering::Acquire));
    assert!(leases.borrow_mut().remove(id.as_str()).is_some());
    assert!(resident_dropped.load(Ordering::Acquire));
}

#[test]
fn uncertain_session_lease_admission_does_not_reopen_identity() {
    let leases = RefCell::new(HashMap::new());
    let id = SessionId("uncertain-admission".into());
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut admission =
        SessionLeaseAdmission::new(&leases, &id, Some(Box::new(LeaseDropSpy(dropped.clone()))));
    admission.dispatch_uncertain();
    drop(admission);
    assert!(!dropped.load(Ordering::Acquire));
    assert!(leases.borrow().is_empty());
}

#[test]
fn facade_close_releases_only_positively_verified_outcomes() {
    assert!(close_outcome_releases_lease(Some("closed")));
    assert!(close_outcome_releases_lease(Some("notResident")));
    assert!(!close_outcome_releases_lease(Some("superseded")));
    assert!(!close_outcome_releases_lease(Some("drainTimedOut")));
    assert!(!close_outcome_releases_lease(None));
    assert!(!close_outcome_releases_lease(Some("futureOutcome")));
}

#[test]
fn session_state_bridge_round_trips_semantic_port() {
    use xai_grok_shell::session::state_authority::{
        NativeSessionStateAuthority as _, ReplayCursor, ReplayRecord, RewindOperation,
        SessionIdentity, SessionInspection,
    };

    let root = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn crate::SessionStateStore> =
        Arc::new(crate::LocalSessionStateStore::new(root.path()).unwrap());
    let authority = SessionStateAuthorityBridge { store };
    let id = SessionIdentity {
        identity: "session-1".into(),
        generation: "generation-1".into(),
    };
    let session = authority.create(id.clone()).unwrap();
    assert!(
        authority.create(id.clone()).is_err(),
        "create must not reopen or replace an existing live session"
    );
    assert_eq!(
        authority.inspect("session-1").unwrap(),
        SessionInspection::Live {
            generation: "generation-1".into()
        }
    );
    session.stage_update(b"one".to_vec()).unwrap();
    session.stage_update(b"two".to_vec()).unwrap();
    session.flush().unwrap();
    let first = session.replay_page(None, 1).unwrap();
    assert_eq!(first.records, vec![ReplayRecord::Update(b"one".to_vec())]);
    let second = session.replay_page(first.next, 2).unwrap();
    assert_eq!(second.records, vec![ReplayRecord::Update(b"two".to_vec())]);
    assert!(
        session
            .replay_page(
                Some(ReplayCursor {
                    generation: "wrong".into(),
                    next_sequence: 1
                }),
                1
            )
            .is_err()
    );
    assert!(
        session
            .replay_page(
                Some(ReplayCursor {
                    generation: "generation-1".into(),
                    next_sequence: 99,
                }),
                1,
            )
            .is_err(),
        "logical replay cursor gaps must fail closed"
    );
    session
        .publish_checkpoint("cp".into(), b"state".to_vec(), b"cp-marker".to_vec())
        .unwrap();
    session
        .publish_rewind(
            RewindOperation::Truncate {
                index: 7,
                payload: b"rewind".to_vec(),
            },
            b"rw-marker".to_vec(),
        )
        .unwrap();
    let all = session.replay_page(None, 10).unwrap();
    assert!(
        matches!(&all.records[2], ReplayRecord::Checkpoint { name, marker, .. } if name == "cp" && marker == b"cp-marker")
    );
    assert!(
        matches!(&all.records[3], ReplayRecord::Rewind { marker, .. } if marker == b"rw-marker")
    );
    let fork_id = SessionIdentity {
        identity: "session-2".into(),
        generation: "fresh-generation".into(),
    };
    let fork = authority
        .publish_fork(fork_id.clone(), all.records.clone())
        .unwrap();
    assert_eq!(fork.identity(), &fork_id);
    assert_eq!(fork.replay_page(None, 10).unwrap().records, all.records);
    assert!(
        authority
            .publish_fork(fork_id, vec![ReplayRecord::Update(b"replacement".to_vec())])
            .is_err(),
        "prepared fork publication must not replace a live generation"
    );
    authority.tombstone(id.clone()).unwrap();
    assert!(matches!(
        authority.inspect("session-1").unwrap(),
        SessionInspection::Tombstoned { .. }
    ));
    assert!(authority.create(id).is_err());
}
