// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    HarnessContent, HarnessPut, HarnessSnapshot, HarnessStore, HarnessStoreError,
    MAX_HARNESS_SNAPSHOT_BYTES, harness_put_reconciled,
};
use crate::session_state::ConformanceOpen;
use std::sync::Arc;

fn suite_error(message: impl Into<String>) -> HarnessStoreError {
    HarnessStoreError::Corrupt(format!("conformance: {}", message.into()))
}

fn snapshot(prompt: &str, rules: Option<&str>) -> Result<HarnessSnapshot, HarnessStoreError> {
    let mut content = HarnessContent::new().system_prompt(prompt);
    if let Some(rules) = rules {
        content = content.rules(rules);
    }
    Ok(HarnessSnapshot::new(content)?)
}

/// Runs the backend-neutral harness snapshot contract against independent
/// handles to one authority.
///
/// The opener answers with a handle for each [`ConformanceOpen`] phase against
/// the same authority: `Fresh` and `Concurrent` must observe each other's
/// writes, and `Reopen` must be a handle taken after the earlier ones were
/// dropped. The suite proves the store is content-addressed and append-only:
/// the bytes reachable under a digest are the same before and after every
/// legal and refused operation, and there is no path that replaces them.
pub fn run_harness_store_conformance<F>(mut open: F) -> Result<(), HarnessStoreError>
where
    F: FnMut(ConformanceOpen) -> Result<Arc<dyn HarnessStore>, HarnessStoreError>,
{
    let store = open(ConformanceOpen::Fresh)?;
    let first = snapshot("conformance base instructions", Some("conformance rules"))?;
    let second = snapshot("conformance base instructions", None)?;
    if first.digest() == second.digest() {
        return Err(suite_error(
            "two different contents produced one content address",
        ));
    }

    if store.get(first.digest())?.is_some() || store.contains(first.digest())? {
        return Err(suite_error("fresh authority is not empty"));
    }

    if store.put(&first)? != HarnessPut::Stored {
        return Err(suite_error("the first write of a digest was not stored"));
    }
    if store.get(first.digest())?.as_ref() != Some(&first) || !store.contains(first.digest())? {
        return Err(suite_error("a stored snapshot did not load back exactly"));
    }

    // Re-writing a present digest is idempotent and never rewrites content.
    let repeat = store.put(&first)?;
    if repeat != HarnessPut::AlreadyPresent
        && !(repeat == HarnessPut::CommitUnknown
            && harness_put_reconciled(&repeat, store.get(first.digest())?.as_ref(), &first))
    {
        return Err(suite_error(format!(
            "re-writing a present digest reported {repeat:?} rather than an idempotent write; one \
             content address is written once and never replaced"
        )));
    }
    if store.get(first.digest())?.as_ref() != Some(&first) {
        return Err(suite_error("an idempotent write changed stored content"));
    }

    // A changed harness is a different address, never an in-place update.
    if store.put(&second)? != HarnessPut::Stored {
        return Err(suite_error("a second distinct snapshot was not stored"));
    }
    if store.get(first.digest())?.as_ref() != Some(&first)
        || store.get(second.digest())?.as_ref() != Some(&second)
    {
        return Err(suite_error(
            "storing a successor snapshot disturbed an existing content address",
        ));
    }

    // Bounds are the SDK's, not the backend's, and a refusal stores nothing.
    let oversize = snapshot(
        &"o".repeat(MAX_HARNESS_SNAPSHOT_BYTES / 2),
        Some(&"r".repeat(MAX_HARNESS_SNAPSHOT_BYTES / 2)),
    )?;
    if store.put(&oversize).is_ok() {
        return Err(suite_error(
            "a snapshot over the published byte bound was accepted",
        ));
    }
    if store.get(oversize.digest())?.is_some() {
        return Err(suite_error("a refused oversize write left content behind"));
    }

    // Two independent handles racing the same new digest both settle on the
    // one immutable content.
    let concurrent = open(ConformanceOpen::Concurrent)?;
    if concurrent.get(first.digest())?.as_ref() != Some(&first) {
        return Err(suite_error(
            "a concurrent handle did not observe stored content",
        ));
    }
    let third = snapshot(
        "conformance contended instructions",
        Some("contended rules"),
    )?;
    let outcomes = [store.put(&third)?, concurrent.put(&third)?];
    for outcome in &outcomes {
        if !harness_put_reconciled(outcome, store.get(third.digest())?.as_ref(), &third) {
            return Err(suite_error(format!(
                "a contended write settled as {outcome:?} without the addressed content"
            )));
        }
    }
    if outcomes
        .iter()
        .filter(|outcome| **outcome == HarnessPut::Stored)
        .count()
        > 1
    {
        return Err(suite_error(
            "a contended write reported two first-time stores of one digest",
        ));
    }

    let before_restart = [
        store.get(first.digest())?,
        store.get(second.digest())?,
        store.get(third.digest())?,
    ];
    drop(concurrent);
    drop(store);

    // Restart: every content address resolves to exactly the same snapshot,
    // and a repeated write still refuses to become an update.
    let reopened = open(ConformanceOpen::Reopen)?;
    let after_restart = [
        reopened.get(first.digest())?,
        reopened.get(second.digest())?,
        reopened.get(third.digest())?,
    ];
    if after_restart != before_restart {
        return Err(suite_error(
            "stored content did not survive a restart unchanged",
        ));
    }
    let reopened_repeat = reopened.put(&first)?;
    if !harness_put_reconciled(
        &reopened_repeat,
        reopened.get(first.digest())?.as_ref(),
        &first,
    ) {
        return Err(suite_error(
            "a repeated write after restart did not settle on the addressed content",
        ));
    }
    if reopened.get(first.digest())?.as_ref() != Some(&first) {
        return Err(suite_error(
            "a repeated write after restart changed stored content",
        ));
    }
    Ok(())
}
