// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    ActivationClaim, ActivationClaimRequest, ActivationCoordinator, ActivationDisposition,
    ActivationError, ActivationGrant, ActivationHandle, ActivationItemId, ActivationRenewal,
    ActivationSettlement, ActivationWake, ActivationWakeOutcome, ActivationWorkerId,
    MAX_ACTIVATION_LEASE_MS,
};
use crate::session_state::ConformanceOpen;
use std::sync::Arc;

type Coordinator = Arc<dyn ActivationCoordinator>;

fn suite_error(message: impl Into<String>) -> ActivationError {
    ActivationError::Corrupt(format!("conformance: {}", message.into()))
}

fn expect<T>(value: Option<T>, message: &str) -> Result<T, ActivationError> {
    value.ok_or_else(|| suite_error(message))
}

fn require(condition: bool, message: impl Into<String>) -> Result<(), ActivationError> {
    if condition {
        Ok(())
    } else {
        Err(suite_error(message))
    }
}

const BASE_MS: u64 = 1_700_000_000_000;

fn item(name: &str) -> Result<ActivationItemId, ActivationError> {
    ActivationItemId::new(format!("conformance.{name}"))
}

fn request(
    worker: &ActivationWorkerId,
    now_ms: u64,
    lease_ms: u64,
    limit: usize,
) -> Result<ActivationClaimRequest, ActivationError> {
    ActivationClaimRequest::new(worker.clone(), now_ms, lease_ms, limit)
}

fn granted(claim: ActivationClaim, message: &str) -> Result<ActivationGrant, ActivationError> {
    match claim {
        ActivationClaim::Granted(grant) => Ok(grant),
        other => Err(suite_error(format!("{message}: answered {other:?}"))),
    }
}

/// Runs the backend-neutral activation coordination contract against
/// independent handles to one authority.
///
/// The opener answers with a handle for each [`ConformanceOpen`] phase against
/// the same authority: `Fresh` and `Concurrent` must observe each other's
/// writes and contend with each other, and `Reopen` must be a handle taken
/// after the earlier ones were dropped — the suite uses it to prove the fencing
/// counter and every settlement survived a restart.
///
/// The contract this proves is the one an unattended supervisor depends on:
/// due work surfaces in due order and never early, exactly one worker holds an
/// item at a time, a superseded worker is refused rather than tolerated, a
/// retried completion is an answer rather than a second execution, and a
/// crashed worker's item returns to the queue with its token advanced.
pub fn run_activation_coordinator_conformance<F>(mut open: F) -> Result<(), ActivationError>
where
    F: FnMut(ConformanceOpen) -> Result<Coordinator, ActivationError>,
{
    let coordinator = open(ConformanceOpen::Fresh)?;
    let worker_a = ActivationWorkerId::new("conformance-worker-a")?;
    let worker_b = ActivationWorkerId::new("conformance-worker-b")?;

    // An unknown identity is answered, never invented.
    let alpha = item("alpha")?;
    require(
        coordinator.inspect(&alpha)?.is_none(),
        "a fresh authority already knows a work item",
    )?;
    require(
        coordinator.claim_item(&alpha, &request(&worker_a, BASE_MS, 1_000, 8)?)?
            == ActivationClaim::Unknown,
        "claiming an unknown work item was not refused as unknown",
    )?;
    require(
        coordinator
            .claim_due(&request(&worker_a, BASE_MS, 1_000, 8)?)?
            .is_empty(),
        "a fresh authority granted work that was never scheduled",
    )?;

    // Waking is idempotent by identity: the same schedule twice is one item.
    let due = BASE_MS + 100;
    let wake = ActivationWake::new(alpha.clone(), due).with_payload(b"alpha-payload".to_vec())?;
    require(
        coordinator.wake(&wake, BASE_MS)? == ActivationWakeOutcome::Registered,
        "the first wake of an identity was not a registration",
    )?;
    require(
        coordinator.wake(&wake, BASE_MS)? == ActivationWakeOutcome::Unchanged,
        "a duplicate wake was treated as new work",
    )?;
    let moved = ActivationWake::new(alpha.clone(), due + 50).with_payload(b"alpha-payload")?;
    require(
        coordinator.wake(&moved, BASE_MS)? == ActivationWakeOutcome::Rescheduled,
        "moving a due time was not reported as a reschedule",
    )?;
    require(
        coordinator.wake(&wake, BASE_MS)? == ActivationWakeOutcome::Rescheduled,
        "moving a due time back was not reported as a reschedule",
    )?;

    // Nothing is granted before its time.
    require(
        coordinator.claim_item(&alpha, &request(&worker_a, due - 1, 1_000, 8)?)?
            == ActivationClaim::NotDue { due_at_ms: due },
        "a work item was claimable before it was due",
    )?;
    require(
        coordinator
            .claim_due(&request(&worker_a, due - 1, 1_000, 8)?)?
            .is_empty(),
        "a sweep before the due time returned work",
    )?;

    // Two supervisors sweep the same instant; exactly one may execute.
    let concurrent = open(ConformanceOpen::Concurrent)?;
    let first = coordinator.claim_due(&request(&worker_a, due, 1_000, 8)?)?;
    let second = concurrent.claim_due(&request(&worker_b, due, 1_000, 8)?)?;
    require(
        first.len() + second.len() == 1,
        format!(
            "two supervisors took {} grants of one due work item; exactly one may execute",
            first.len() + second.len()
        ),
    )?;
    let (holder, loser, grant) = if first.len() == 1 {
        (&worker_a, &worker_b, first[0].clone())
    } else {
        (&worker_b, &worker_a, second[0].clone())
    };
    require(
        grant.item_id() == &alpha
            && grant.worker_id() == holder
            && grant.due_at_ms() == due
            && grant.attempt() == 1
            && grant.payload() == b"alpha-payload"
            && grant.expires_at_ms() == due + 1_000,
        "a grant did not describe the work item it granted",
    )?;
    require(
        grant.token().get() > 0,
        "a grant minted a fencing token that cannot fence anything",
    )?;

    // The loser is told why, and cannot take the item by asking again.
    match concurrent.claim_item(&alpha, &request(loser, due, 1_000, 8)?)? {
        ActivationClaim::Held {
            worker_id,
            expires_at_ms,
        } => require(
            &worker_id == holder && expires_at_ms == grant.expires_at_ms(),
            "a refused claim misreported who holds the work item",
        )?,
        other => {
            return Err(suite_error(format!(
                "claiming an item under a live lease answered {other:?} rather than a refusal"
            )));
        }
    }

    // Only the holder may extend, and a fabricated fence never wins.
    let handle = grant.handle();
    require(
        coordinator.renew(&handle, 2_000, due + 500)?
            == (ActivationRenewal::Renewed {
                expires_at_ms: due + 2_500,
            }),
        "the holder could not extend its own lease",
    )?;
    let foreign = ActivationHandle::new(alpha.clone(), loser.clone(), grant.token());
    require(
        concurrent.renew(&foreign, 2_000, due + 600)? == ActivationRenewal::Fenced,
        "a worker that never held the item was allowed to renew it",
    )?;
    require(
        concurrent.release(&foreign, ActivationDisposition::Complete, due + 600)?
            == ActivationSettlement::Fenced,
        "a worker that never held the item was allowed to complete it",
    )?;

    // Completion is idempotent, and a completed item is not due again.
    let completed_at = due + 700;
    require(
        coordinator.release(&handle, ActivationDisposition::Complete, completed_at)?
            == ActivationSettlement::Settled,
        "the holder could not complete its own work item",
    )?;
    require(
        coordinator.release(&handle, ActivationDisposition::Complete, completed_at + 1)?
            == ActivationSettlement::AlreadySettled,
        "a retried completion was not answered as already settled",
    )?;
    require(
        concurrent.release(&handle, ActivationDisposition::Complete, completed_at + 2)?
            == ActivationSettlement::AlreadySettled,
        "a retried completion through another handle was not answered as already settled",
    )?;
    require(
        coordinator.claim_item(&alpha, &request(&worker_b, completed_at + 3, 1_000, 8)?)?
            == ActivationClaim::Settled,
        "a completed work item was still claimable",
    )?;
    let state = expect(coordinator.inspect(&alpha)?, "a completed item vanished")?;
    require(
        state.due_at_ms.is_none()
            && state.lease.is_none()
            && state.settled_at_ms == Some(completed_at)
            && state.attempt == 1
            && state.fencing_token == grant.token(),
        "a completed item did not record its own settlement",
    )?;

    // Waking a completed identity schedules it again without reusing a token.
    let later =
        ActivationWake::new(alpha.clone(), BASE_MS + 10_000).with_payload(b"alpha-again")?;
    require(
        coordinator.wake(&later, completed_at + 4)? == ActivationWakeOutcome::Rescheduled,
        "waking a settled identity did not reschedule it",
    )?;

    ordering(&coordinator, &worker_a)?;
    expiry(&coordinator, &concurrent, &worker_a, &worker_b)?;

    // A live lease is the worker's to settle; the schedule is not moved under it.
    let zeta = item("zeta")?;
    coordinator.wake(&ActivationWake::new(zeta.clone(), BASE_MS + 900), BASE_MS)?;
    let held = granted(
        coordinator.claim_item(&zeta, &request(&worker_a, BASE_MS + 900, 5_000, 8)?)?,
        "a due item was not granted",
    )?;
    require(
        coordinator.wake(
            &ActivationWake::new(zeta.clone(), BASE_MS + 950),
            BASE_MS + 901,
        )? == ActivationWakeOutcome::Held,
        "a work item under a live lease was rescheduled behind its worker's back",
    )?;
    require(
        coordinator.release(
            &held.handle(),
            ActivationDisposition::Yield {
                due_at_ms: BASE_MS + 20_000,
            },
            BASE_MS + 902,
        )? == ActivationSettlement::Settled,
        "a holder could not yield its own work item",
    )?;
    require(
        coordinator.release(
            &held.handle(),
            ActivationDisposition::Yield {
                due_at_ms: BASE_MS + 30_000,
            },
            BASE_MS + 903,
        )? == ActivationSettlement::AlreadySettled,
        "a retried yield was not answered as already settled",
    )?;
    require(
        coordinator.claim_item(&zeta, &request(&worker_a, BASE_MS + 1_000, 1_000, 8)?)?
            == ActivationClaim::NotDue {
                due_at_ms: BASE_MS + 20_000,
            },
        "a yielded work item did not return to the queue at the time it asked for",
    )?;

    // A lease bound is the contract's, not the backend's.
    require(
        coordinator
            .renew(&held.handle(), 0, BASE_MS + 1_000)
            .is_err()
            && coordinator
                .renew(&held.handle(), MAX_ACTIVATION_LEASE_MS + 1, BASE_MS + 1_000)
                .is_err(),
        "a renewal outside the published lease bound was accepted",
    )?;

    let before_restart = [
        coordinator.inspect(&alpha)?,
        coordinator.inspect(&zeta)?,
        coordinator.inspect(&item("epsilon")?)?,
    ];
    drop(concurrent);
    drop(coordinator);

    // Restart: every settlement and every fencing counter survives.
    let reopened = open(ConformanceOpen::Reopen)?;
    let after_restart = [
        reopened.inspect(&alpha)?,
        reopened.inspect(&zeta)?,
        reopened.inspect(&item("epsilon")?)?,
    ];
    require(
        after_restart == before_restart,
        "activation state did not survive a restart unchanged",
    )?;
    let resumed = granted(
        reopened.claim_item(&alpha, &request(&worker_a, BASE_MS + 10_000, 1_000, 8)?)?,
        "a rescheduled item was not claimable after restart",
    )?;
    require(
        resumed.token().get() > grant.token().get() && resumed.attempt() == 2,
        "a fencing token restarted rather than advanced across a restart",
    )?;
    require(
        reopened.release(&handle, ActivationDisposition::Complete, BASE_MS + 10_001)?
            == ActivationSettlement::AlreadySettled,
        "a settlement recorded before a restart was forgotten after it",
    )?;
    let impostor = ActivationHandle::new(alpha.clone(), worker_b.clone(), resumed.token());
    require(
        reopened.release(&impostor, ActivationDisposition::Complete, BASE_MS + 10_001)?
            == ActivationSettlement::Fenced,
        "a worker presenting another worker's live token was allowed to settle",
    )?;
    reopened.release(
        &resumed.handle(),
        ActivationDisposition::Complete,
        BASE_MS + 10_002,
    )?;

    // Retention removes settled work and nothing else.
    require(
        reopened.purge_settled(BASE_MS)? == 0,
        "retention removed work that had not settled by the given instant",
    )?;
    let purged = reopened.purge_settled(BASE_MS + 100_000)?;
    require(
        purged >= 2 && reopened.inspect(&item("epsilon")?)?.is_none(),
        "retention did not remove settled work",
    )?;
    require(
        reopened.inspect(&zeta)?.is_some_and(|state| {
            state.due_at_ms == Some(BASE_MS + 20_000) && state.settled_at_ms.is_none()
        }),
        "retention removed work that is still scheduled",
    )?;
    Ok(())
}

/// Due work surfaces oldest first, ties broken by identity, and work that is
/// not yet due never surfaces at all.
fn ordering(coordinator: &Coordinator, worker: &ActivationWorkerId) -> Result<(), ActivationError> {
    let beta = item("ordering-beta")?;
    let delta = item("ordering-delta")?;
    let gamma = item("ordering-gamma")?;
    coordinator.wake(&ActivationWake::new(gamma.clone(), BASE_MS + 500), BASE_MS)?;
    coordinator.wake(&ActivationWake::new(delta.clone(), BASE_MS + 300), BASE_MS)?;
    coordinator.wake(&ActivationWake::new(beta.clone(), BASE_MS + 300), BASE_MS)?;

    let sweep = coordinator.claim_due(&request(worker, BASE_MS + 400, 1_000, 8)?)?;
    let taken: Vec<&str> = sweep.iter().map(|grant| grant.item_id().as_str()).collect();
    require(
        taken == [beta.as_str(), delta.as_str()],
        format!("a due sweep answered {taken:?} rather than the due items in due order"),
    )?;
    require(
        sweep.iter().all(|grant| grant.due_at_ms() <= BASE_MS + 400),
        "a due sweep returned work before its time",
    )?;

    let rest = coordinator.claim_due(&request(worker, BASE_MS + 600, 1_000, 8)?)?;
    require(
        rest.len() == 1 && rest[0].item_id() == &gamma,
        "a later sweep did not surface the work that had since come due",
    )?;

    // A bounded sweep takes the oldest work, not an arbitrary subset.
    for grant in sweep.iter().chain(rest.iter()) {
        coordinator.release(
            &grant.handle(),
            ActivationDisposition::Complete,
            BASE_MS + 700,
        )?;
    }
    coordinator.wake(&ActivationWake::new(beta.clone(), BASE_MS + 300), BASE_MS)?;
    coordinator.wake(&ActivationWake::new(delta.clone(), BASE_MS + 200), BASE_MS)?;
    let bounded = coordinator.claim_due(&request(worker, BASE_MS + 400, 1_000, 1)?)?;
    require(
        bounded.len() == 1 && bounded[0].item_id() == &delta,
        "a bounded sweep did not take the oldest due work first",
    )?;
    coordinator.release(
        &bounded[0].handle(),
        ActivationDisposition::Complete,
        BASE_MS + 700,
    )?;
    let remaining = coordinator.claim_due(&request(worker, BASE_MS + 400, 1_000, 8)?)?;
    require(
        remaining.len() == 1 && remaining[0].item_id() == &beta,
        "a bounded sweep left the remaining due work unreachable",
    )?;
    coordinator.release(
        &remaining[0].handle(),
        ActivationDisposition::Complete,
        BASE_MS + 700,
    )?;
    Ok(())
}

/// A worker that stops without releasing loses the item at expiry, and the
/// successor claim advances the token that invalidates it.
fn expiry(
    coordinator: &Coordinator,
    concurrent: &Coordinator,
    crashed: &ActivationWorkerId,
    successor: &ActivationWorkerId,
) -> Result<(), ActivationError> {
    let epsilon = item("epsilon")?;
    coordinator.wake(
        &ActivationWake::new(epsilon.clone(), BASE_MS + 800),
        BASE_MS,
    )?;
    let lost = granted(
        coordinator.claim_item(&epsilon, &request(crashed, BASE_MS + 800, 50, 8)?)?,
        "a due item was not granted to the first worker",
    )?;

    // Before expiry the crashed worker still owns it, so nobody else may take it.
    require(
        matches!(
            concurrent.claim_item(&epsilon, &request(successor, BASE_MS + 820, 1_000, 8)?)?,
            ActivationClaim::Held { .. }
        ),
        "a live lease was taken over before it expired",
    )?;

    let reclaimed = granted(
        concurrent.claim_item(&epsilon, &request(successor, BASE_MS + 900, 1_000, 8)?)?,
        "an expired lease did not make the work item claimable again",
    )?;
    require(
        reclaimed.token().get() > lost.token().get() && reclaimed.attempt() == 2,
        "reclaiming an expired lease did not advance the fencing token",
    )?;
    require(
        coordinator.renew(&lost.handle(), 1_000, BASE_MS + 901)? == ActivationRenewal::Fenced,
        "a superseded worker was allowed to renew",
    )?;
    require(
        coordinator.release(
            &lost.handle(),
            ActivationDisposition::Complete,
            BASE_MS + 902,
        )? == ActivationSettlement::Fenced,
        "a superseded worker was allowed to record an outcome",
    )?;
    require(
        coordinator
            .inspect(&epsilon)?
            .is_some_and(|state| state.settled_at_ms.is_none() && state.lease.is_some()),
        "a refused settlement still changed the work item",
    )?;
    require(
        concurrent.release(
            &reclaimed.handle(),
            ActivationDisposition::Complete,
            BASE_MS + 903,
        )? == ActivationSettlement::Settled,
        "the successor could not complete the work it took over",
    )?;
    Ok(())
}
