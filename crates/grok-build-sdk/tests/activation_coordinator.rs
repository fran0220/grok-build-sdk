// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! The durable activation contract, exercised the way an unattended supervisor
//! exercises it: through the public façade, against the reference
//! implementation, and against a backend that does not honour leases.

use grok_build_sdk::{
    ActivationClaim, ActivationClaimRequest, ActivationCoordinator, ActivationDisposition,
    ActivationError, ActivationFencingToken, ActivationGrant, ActivationHandle, ActivationItemId,
    ActivationItemState, ActivationRenewal, ActivationSettlement, ActivationWake,
    ActivationWakeOutcome, ActivationWorkerId, ConformanceOpen, LocalActivationCoordinator,
    MAX_ACTIVATION_LEASE_MS, MAX_ACTIVATION_PAYLOAD_BYTES, run_activation_coordinator_conformance,
};
use std::sync::Arc;

const NOW: u64 = 1_700_000_000_000;

fn item(name: &str) -> ActivationItemId {
    ActivationItemId::new(name).expect("valid work item id")
}

fn worker(name: &str) -> ActivationWorkerId {
    ActivationWorkerId::new(name).expect("valid worker id")
}

fn claim(name: &str, now_ms: u64, lease_ms: u64) -> ActivationClaimRequest {
    ActivationClaimRequest::new(worker(name), now_ms, lease_ms, 8).expect("valid claim request")
}

fn grant_of(claim: ActivationClaim) -> ActivationGrant {
    match claim {
        ActivationClaim::Granted(grant) => grant,
        other => panic!("expected a grant, got {other:?}"),
    }
}

#[test]
fn the_reference_activation_coordinator_passes_the_public_conformance() {
    let directory = tempfile::tempdir().unwrap();
    run_activation_coordinator_conformance(|phase| {
        assert!(matches!(
            phase,
            ConformanceOpen::Fresh | ConformanceOpen::Concurrent | ConformanceOpen::Reopen
        ));
        Ok(Arc::new(LocalActivationCoordinator::new(directory.path())?)
            as Arc<dyn ActivationCoordinator>)
    })
    .expect("the reference coordinator satisfies its own published contract");
}

/// (a) Two supervisors on one durable authority — the case an application hits
/// when a second window, a stale process or a restarted service overlaps with
/// the first. Exactly one of them may ever execute a given work item.
#[test]
fn two_coordinators_on_one_store_grant_a_due_work_item_exactly_once() {
    let directory = tempfile::tempdir().unwrap();
    let scheduler = LocalActivationCoordinator::new(directory.path()).unwrap();
    let contended = item("contended");
    scheduler
        .wake(&ActivationWake::new(contended.clone(), NOW), NOW)
        .unwrap();

    let grants: Vec<ActivationGrant> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|index| {
                let root = directory.path().to_owned();
                let contended = contended.clone();
                scope.spawn(move || {
                    let coordinator = LocalActivationCoordinator::new(&root).unwrap();
                    let request = claim(&format!("worker-{index}"), NOW, 60_000);
                    let swept = coordinator.claim_due(&request).unwrap();
                    let targeted = coordinator.claim_item(&contended, &request).unwrap();
                    let mut taken = swept;
                    if let ActivationClaim::Granted(grant) = targeted {
                        taken.push(grant);
                    }
                    taken
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect()
    });

    assert_eq!(
        grants.len(),
        1,
        "eight supervisors racing one due work item produced {} grants",
        grants.len()
    );
    let state = scheduler.inspect(&contended).unwrap().unwrap();
    assert_eq!(state.attempt, 1);
    assert_eq!(
        state.lease.as_ref().map(|lease| lease.token),
        Some(grants[0].token()),
        "the durable lease must name the one worker that won"
    );
    assert!(!state.is_claimable_at(NOW));
}

/// (b) A supervisor that crashed mid-work: its lease expires, the work returns
/// to the queue with the token advanced, and the ghost is refused if it ever
/// comes back.
#[test]
fn a_superseded_worker_is_refused_after_expiry_and_reclaim() {
    let directory = tempfile::tempdir().unwrap();
    let coordinator = LocalActivationCoordinator::new(directory.path()).unwrap();
    let crashed_work = item("crashed-work");
    coordinator
        .wake(&ActivationWake::new(crashed_work.clone(), NOW), NOW)
        .unwrap();

    let lost = grant_of(
        coordinator
            .claim_item(&crashed_work, &claim("crashed", NOW, 1_000))
            .unwrap(),
    );
    assert!(matches!(
        coordinator
            .claim_item(&crashed_work, &claim("successor", NOW + 999, 1_000))
            .unwrap(),
        ActivationClaim::Held { .. }
    ));

    let reclaimed = grant_of(
        coordinator
            .claim_item(&crashed_work, &claim("successor", NOW + 1_000, 1_000))
            .unwrap(),
    );
    assert!(reclaimed.token().get() > lost.token().get());
    assert_eq!(reclaimed.attempt(), 2);

    assert_eq!(
        coordinator
            .renew(&lost.handle(), 1_000, NOW + 1_001)
            .unwrap(),
        ActivationRenewal::Fenced
    );
    assert_eq!(
        coordinator
            .release(&lost.handle(), ActivationDisposition::Complete, NOW + 1_002)
            .unwrap(),
        ActivationSettlement::Fenced
    );
    assert_eq!(
        coordinator
            .release(
                &lost.handle(),
                ActivationDisposition::Yield {
                    due_at_ms: NOW + 5_000
                },
                NOW + 1_003,
            )
            .unwrap(),
        ActivationSettlement::Fenced
    );

    // The refusals changed nothing: the successor still holds live work.
    let state = coordinator.inspect(&crashed_work).unwrap().unwrap();
    assert_eq!(state.settled_at_ms, None);
    assert_eq!(
        state.lease.as_ref().map(|lease| lease.token),
        Some(reclaimed.token())
    );
    assert_eq!(
        coordinator
            .renew(&reclaimed.handle(), 1_000, NOW + 1_004)
            .unwrap(),
        ActivationRenewal::Renewed {
            expires_at_ms: NOW + 2_004
        }
    );
}

/// (c) Completion is idempotent. A supervisor that is unsure whether its
/// release landed — the ordinary outcome of a crash between commit and
/// acknowledgement — retries and is told the truth instead of running again.
#[test]
fn completing_a_work_item_twice_is_an_answer_rather_than_a_second_execution() {
    let directory = tempfile::tempdir().unwrap();
    let coordinator = LocalActivationCoordinator::new(directory.path()).unwrap();
    let once_only = item("once-only");
    coordinator
        .wake(&ActivationWake::new(once_only.clone(), NOW), NOW)
        .unwrap();
    let held = grant_of(
        coordinator
            .claim_item(&once_only, &claim("solo", NOW, 60_000))
            .unwrap(),
    );

    assert_eq!(
        coordinator
            .release(&held.handle(), ActivationDisposition::Complete, NOW + 10)
            .unwrap(),
        ActivationSettlement::Settled
    );
    for retry in 0..3 {
        assert_eq!(
            coordinator
                .release(
                    &held.handle(),
                    ActivationDisposition::Complete,
                    NOW + 20 + retry,
                )
                .unwrap(),
            ActivationSettlement::AlreadySettled,
            "retry {retry} of a settled release must be a no-op answer"
        );
    }
    // A retry that asks for a different outcome must not rewrite the recorded one.
    assert_eq!(
        coordinator
            .release(
                &held.handle(),
                ActivationDisposition::Yield {
                    due_at_ms: NOW + 100
                },
                NOW + 30,
            )
            .unwrap(),
        ActivationSettlement::AlreadySettled
    );
    let state = coordinator.inspect(&once_only).unwrap().unwrap();
    assert_eq!(state.due_at_ms, None);
    assert_eq!(state.settled_at_ms, Some(NOW + 10));
    assert_eq!(state.attempt, 1);
    assert_eq!(
        coordinator
            .claim_item(&once_only, &claim("solo", NOW + 40, 60_000))
            .unwrap(),
        ActivationClaim::Settled
    );

    // Retention is the only thing that drops the settlement, and it drops
    // nothing that is still scheduled.
    coordinator
        .wake(
            &ActivationWake::new(item("still-scheduled"), NOW + 10_000),
            NOW,
        )
        .unwrap();
    assert_eq!(coordinator.purge_settled(NOW).unwrap(), 0);
    assert_eq!(coordinator.purge_settled(NOW + 10).unwrap(), 1);
    assert!(coordinator.inspect(&once_only).unwrap().is_none());
    assert!(
        coordinator
            .inspect(&item("still-scheduled"))
            .unwrap()
            .is_some()
    );
}

/// (d) Wake ordering: work surfaces when it is due, oldest first, and a
/// supervisor that has been offline catches up in the order the schedule was
/// written rather than in storage order.
#[test]
fn due_work_surfaces_in_due_order_and_never_before_its_time() {
    let directory = tempfile::tempdir().unwrap();
    let coordinator = LocalActivationCoordinator::new(directory.path()).unwrap();
    for (name, due) in [
        ("late", NOW + 3_000),
        ("early", NOW + 1_000),
        ("middle", NOW + 2_000),
        ("future", NOW + 90_000),
    ] {
        assert_eq!(
            coordinator
                .wake(&ActivationWake::new(item(name), due), NOW)
                .unwrap(),
            ActivationWakeOutcome::Registered
        );
    }

    assert!(
        coordinator
            .claim_due(&claim("supervisor", NOW + 999, 60_000))
            .unwrap()
            .is_empty(),
        "no work is due one millisecond before the first due time"
    );

    let first = coordinator
        .claim_due(&claim("supervisor", NOW + 1_000, 60_000))
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|grant| grant.item_id().as_str())
            .collect::<Vec<_>>(),
        ["early"]
    );

    // Offline until well past three due times: they catch up in due order and
    // the item that is not yet due stays out of the sweep.
    let caught_up =
        ActivationClaimRequest::new(worker("supervisor"), NOW + 5_000, 60_000, 8).unwrap();
    let rest = coordinator.claim_due(&caught_up).unwrap();
    assert_eq!(
        rest.iter()
            .map(|grant| grant.item_id().as_str())
            .collect::<Vec<_>>(),
        ["middle", "late"]
    );
    assert!(
        rest.iter()
            .all(|grant| grant.due_at_ms() <= NOW + 5_000 && grant.expires_at_ms() == NOW + 65_000)
    );
    assert_eq!(
        coordinator.claim_item(&item("future"), &caught_up).unwrap(),
        ActivationClaim::NotDue {
            due_at_ms: NOW + 90_000
        }
    );

    // A yielded item rejoins the queue at the time it asked for, not before.
    assert_eq!(
        coordinator
            .release(
                &first[0].handle(),
                ActivationDisposition::Yield {
                    due_at_ms: NOW + 6_000
                },
                NOW + 5_001,
            )
            .unwrap(),
        ActivationSettlement::Settled
    );
    assert!(
        coordinator
            .claim_due(&claim("supervisor", NOW + 5_999, 60_000))
            .unwrap()
            .is_empty()
    );
    let resumed = coordinator
        .claim_due(&claim("supervisor", NOW + 6_000, 60_000))
        .unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].item_id().as_str(), "early");
    assert_eq!(resumed[0].attempt(), 2);
}

/// (e) A store written by something else, or damaged under the coordinator,
/// must stop the supervisor rather than schedule invented work.
#[test]
fn the_coordinator_fails_closed_on_a_foreign_or_damaged_store() {
    let directory = tempfile::tempdir().unwrap();
    let coordinator = LocalActivationCoordinator::new(directory.path()).unwrap();
    let probe = item("corruption-probe");
    coordinator
        .wake(&ActivationWake::new(probe.clone(), NOW), NOW)
        .unwrap();
    let path = coordinator.path().to_owned();
    drop(coordinator);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE metadata SET value='some.other.product.queue' WHERE key='schema_marker'",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(
        matches!(
            LocalActivationCoordinator::new(directory.path()),
            Err(ActivationError::Corrupt(_))
        ),
        "a store written by another product must not be adopted"
    );

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE metadata SET value='grok-build-sdk.activation-coordinator' WHERE \
             key='schema_marker'",
            [],
        )
        .unwrap();
    connection
        .execute("DELETE FROM metadata WHERE key='schema_version'", [])
        .unwrap();
    drop(connection);
    assert!(matches!(
        LocalActivationCoordinator::new(directory.path()),
        Err(ActivationError::Corrupt(_))
    ));

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "INSERT INTO metadata(key,value) VALUES('schema_version','1')",
            [],
        )
        .unwrap();
    connection
        .execute("UPDATE work_items SET payload_size=4096", [])
        .unwrap();
    drop(connection);
    let reopened = LocalActivationCoordinator::new(directory.path()).unwrap();
    assert!(matches!(
        reopened.inspect(&probe),
        Err(ActivationError::Corrupt(_))
    ));
    assert!(matches!(
        reopened.claim_due(&claim("supervisor", NOW, 60_000)),
        Err(ActivationError::Corrupt(_))
    ));

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE work_items SET payload_size=0,attempt=-1,fencing_token=3",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        reopened.inspect(&probe),
        Err(ActivationError::Corrupt(_))
    ));

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("UPDATE work_items SET attempt=0,settled_token=9", [])
        .unwrap();
    drop(connection);
    assert!(
        matches!(reopened.inspect(&probe), Err(ActivationError::Corrupt(_))),
        "a settlement that outruns the fencing counter is not a state this \
         coordinator can have produced"
    );
}

/// Bounds are the contract's, not the backend's, and a refused input never
/// reaches storage.
#[test]
fn identities_payloads_and_leases_are_bounded_by_the_contract() {
    assert!(ActivationItemId::new("").is_err());
    assert!(ActivationItemId::new(" leading").is_err());
    assert!(ActivationItemId::new("new\nline").is_err());
    assert!(ActivationItemId::new("x".repeat(257)).is_err());
    assert!(ActivationWorkerId::new("y".repeat(257)).is_err());
    assert!(
        ActivationWake::new(item("bounded"), NOW)
            .with_payload(vec![0u8; MAX_ACTIVATION_PAYLOAD_BYTES + 1])
            .is_err()
    );
    assert!(
        ActivationWake::new(item("bounded"), NOW)
            .with_payload(vec![0u8; MAX_ACTIVATION_PAYLOAD_BYTES])
            .is_ok()
    );
    assert!(ActivationClaimRequest::new(worker("w"), NOW, 0, 8).is_err());
    assert!(ActivationClaimRequest::new(worker("w"), NOW, MAX_ACTIVATION_LEASE_MS + 1, 8).is_err());
    assert!(ActivationClaimRequest::new(worker("w"), NOW, 1_000, 0).is_err());
    assert!(ActivationClaimRequest::new(worker("w"), NOW, 1_000, 257).is_err());
    assert!(ActivationClaimRequest::new(worker("w"), u64::MAX, 1_000, 8).is_err());

    let directory = tempfile::tempdir().unwrap();
    let coordinator = LocalActivationCoordinator::new(directory.path()).unwrap();
    let bounded = item("bounded");
    coordinator
        .wake(&ActivationWake::new(bounded.clone(), NOW), NOW)
        .unwrap();
    let held = grant_of(
        coordinator
            .claim_item(&bounded, &claim("w", NOW, 1_000))
            .unwrap(),
    );
    assert!(coordinator.renew(&held.handle(), 0, NOW).is_err());
    assert!(
        coordinator
            .renew(&held.handle(), MAX_ACTIVATION_LEASE_MS + 1, NOW)
            .is_err()
    );
    assert_eq!(
        coordinator
            .inspect(&bounded)
            .unwrap()
            .unwrap()
            .lease
            .unwrap()
            .expires_at_ms,
        NOW + 1_000,
        "a refused renewal must leave the lease exactly as it was"
    );
}

/// A backend that hands the same work to everyone who asks is exactly the
/// double-execution this contract exists to prevent.
#[test]
fn a_backend_that_ignores_leases_fails_the_activation_coordinator_conformance() {
    struct AlwaysGrants;
    impl ActivationCoordinator for AlwaysGrants {
        fn wake(
            &self,
            _: &ActivationWake,
            _: u64,
        ) -> Result<ActivationWakeOutcome, ActivationError> {
            Ok(ActivationWakeOutcome::Registered)
        }
        fn claim_due(
            &self,
            request: &ActivationClaimRequest,
        ) -> Result<Vec<ActivationGrant>, ActivationError> {
            Ok(vec![ActivationGrant::new(
                ActivationItemId::new("conformance.alpha")?,
                request.worker_id().clone(),
                ActivationFencingToken::new(1),
                request.now_ms(),
                request.now_ms() + request.lease_ms(),
                1,
                Vec::new(),
            )])
        }
        fn claim_item(
            &self,
            item_id: &ActivationItemId,
            request: &ActivationClaimRequest,
        ) -> Result<ActivationClaim, ActivationError> {
            Ok(ActivationClaim::Granted(ActivationGrant::new(
                item_id.clone(),
                request.worker_id().clone(),
                ActivationFencingToken::new(1),
                request.now_ms(),
                request.now_ms() + request.lease_ms(),
                1,
                Vec::new(),
            )))
        }
        fn renew(
            &self,
            _: &ActivationHandle,
            lease_ms: u64,
            now_ms: u64,
        ) -> Result<ActivationRenewal, ActivationError> {
            Ok(ActivationRenewal::Renewed {
                expires_at_ms: now_ms + lease_ms,
            })
        }
        fn release(
            &self,
            _: &ActivationHandle,
            _: ActivationDisposition,
            _: u64,
        ) -> Result<ActivationSettlement, ActivationError> {
            Ok(ActivationSettlement::Settled)
        }
        fn inspect(
            &self,
            _: &ActivationItemId,
        ) -> Result<Option<ActivationItemState>, ActivationError> {
            Ok(None)
        }
        fn purge_settled(&self, _: u64) -> Result<usize, ActivationError> {
            Ok(0)
        }
    }

    let coordinator = Arc::new(AlwaysGrants);
    let error = run_activation_coordinator_conformance(|_| {
        Ok(coordinator.clone() as Arc<dyn ActivationCoordinator>)
    })
    .expect_err("a backend that never fences cannot pass");
    assert!(
        error.to_string().contains("conformance"),
        "the suite should name itself in the failure: {error}"
    );
}
