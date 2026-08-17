// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    EvolutionCommit, EvolutionState, EvolutionStore, EvolutionStoreError, HarnessEntryKind,
    HarnessScope, RefinementEdit, RefinementProposal, evolution_commit_reconciled,
};
use crate::session_state::ConformanceOpen;
use std::sync::Arc;

fn suite_error(message: impl Into<String>) -> EvolutionStoreError {
    EvolutionStoreError::Corrupt(format!("conformance: {}", message.into()))
}

fn proposal(edits: Vec<RefinementEdit>) -> Result<RefinementProposal, EvolutionStoreError> {
    Ok(RefinementProposal::new(
        "conformance refinement",
        "exercise the durable evolution contract",
        "the committed transition is observable and append-only",
        edits,
    )?)
}

/// Runs the backend-neutral evolution ledger contract against independent
/// handles to one authority.
///
/// The opener answers with a handle for each [`ConformanceOpen`] phase against
/// the same authority: `Fresh` and `Concurrent` must observe each other's
/// writes, and `Reopen` must be a handle taken after the earlier ones were
/// dropped. The suite proves commits are revision-CASed atomic transitions and
/// that event history is append-only and content-addressed.
pub fn run_evolution_store_conformance<F>(mut open: F) -> Result<(), EvolutionStoreError>
where
    F: FnMut(ConformanceOpen) -> Result<Arc<dyn EvolutionStore>, EvolutionStoreError>,
{
    let store = open(ConformanceOpen::Fresh)?;
    let scope = HarnessScope::session("evolution-conformance");
    if store.load(&scope)?.is_some() {
        return Err(suite_error("fresh authority is not empty"));
    }
    if !store.history(&scope, 16)?.is_empty() {
        return Err(suite_error("fresh authority has event history"));
    }

    // First transition: revision 0 -> 1.
    let base = EvolutionState::new(scope.clone())?;
    let (first_state, first_event) = base.apply(
        &proposal(vec![RefinementEdit::create(
            HarnessEntryKind::Memory,
            "Conformance Fact",
            "The suite created this entry.",
            "conformance evidence",
        )])?,
        "conformance",
        [],
        1_000,
    )?;
    let commit = store.commit(&first_state, &first_event)?;
    if !evolution_commit_reconciled(&commit, store.load(&scope)?.as_ref(), &first_state) {
        return Err(suite_error("the first commit did not settle"));
    }
    if store.load(&scope)?.as_ref() != Some(&first_state) {
        return Err(suite_error("a committed state did not load back exactly"));
    }

    // A stale baseline is refused without writing anything.
    let (stale_state, stale_event) = base.apply(
        &proposal(vec![RefinementEdit::create(
            HarnessEntryKind::Memory,
            "Stale Fact",
            "Planned against a stale revision.",
            "conformance evidence",
        )])?,
        "conformance",
        [],
        2_000,
    )?;
    match store.commit(&stale_state, &stale_event)? {
        EvolutionCommit::RevisionConflict { stored_revision } => {
            if stored_revision != Some(first_state.revision()) {
                return Err(suite_error("a conflict did not report the stored revision"));
            }
        }
        EvolutionCommit::Committed => {
            return Err(suite_error("a stale baseline was committed"));
        }
        EvolutionCommit::CommitUnknown => {
            if store.load(&scope)?.as_ref() == Some(&stale_state) {
                return Err(suite_error("a stale baseline was committed"));
            }
        }
    }
    if store.load(&scope)?.as_ref() != Some(&first_state) {
        return Err(suite_error("a refused commit changed the stored state"));
    }
    if store.history(&scope, 16)?.len() != 1 {
        return Err(suite_error("a refused commit changed the event history"));
    }

    // A concurrent handle observes the committed transition and can extend it.
    let concurrent = open(ConformanceOpen::Concurrent)?;
    let observed = concurrent
        .load(&scope)?
        .ok_or_else(|| suite_error("a concurrent handle cannot observe the committed state"))?;
    if observed != first_state {
        return Err(suite_error(
            "a concurrent handle observed a different state",
        ));
    }
    let (second_state, second_event) = observed.apply(
        &proposal(vec![
            RefinementEdit::update(
                HarnessEntryKind::Memory,
                "conformance-fact",
                "sharpen the fact",
            )
            .with_content("The suite updated this entry.")
            .with_expected_version(1),
        ])?,
        "conformance",
        [],
        3_000,
    )?;
    let commit = concurrent.commit(&second_state, &second_event)?;
    if !evolution_commit_reconciled(&commit, concurrent.load(&scope)?.as_ref(), &second_state) {
        return Err(suite_error("the concurrent commit did not settle"));
    }

    // Rollback commits as one more append-only transition.
    let (third_state, rollback_event) =
        second_state.rollback(&second_event, "conformance rollback", 4_000)?;
    let commit = store.commit(&third_state, &rollback_event)?;
    if !evolution_commit_reconciled(&commit, store.load(&scope)?.as_ref(), &third_state) {
        return Err(suite_error("the rollback commit did not settle"));
    }
    if third_state
        .entry("conformance-fact")
        .map(|entry| entry.version())
        != Some(1)
    {
        return Err(suite_error("rollback did not restore the before snapshot"));
    }

    // History is newest-first, complete, and addressable by event ID.
    let history = store.history(&scope, 16)?;
    if history.len() != 3
        || history[0] != rollback_event
        || history[1] != second_event
        || history[2] != first_event
    {
        return Err(suite_error(
            "event history is not complete and newest-first",
        ));
    }
    if store.history(&scope, 1)? != vec![rollback_event.clone()] {
        return Err(suite_error("a history limit was not honored"));
    }
    if store.event(&scope, second_event.event_id())?.as_ref() != Some(&second_event) {
        return Err(suite_error("an event is not addressable by its ID"));
    }
    if store
        .event(&scope, &format!("sha256:{}", "0".repeat(64)))?
        .is_some()
    {
        return Err(suite_error("an unknown event ID resolved to an event"));
    }

    // Scopes are isolated.
    if store.load(&HarnessScope::Global)?.is_some()
        || !store.history(&HarnessScope::Global, 16)?.is_empty()
    {
        return Err(suite_error("a session commit leaked into the global scope"));
    }

    drop(store);
    drop(concurrent);

    // Reopening preserves the exact committed state and history.
    let reopened = open(ConformanceOpen::Reopen)?;
    if reopened.load(&scope)?.as_ref() != Some(&third_state) {
        return Err(suite_error("reopening lost the committed state"));
    }
    if reopened.history(&scope, 16)?.len() != 3 {
        return Err(suite_error("reopening lost the event history"));
    }
    Ok(())
}
