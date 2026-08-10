use super::*;

pub(super) fn validate_session_ledger(id: &SessionId, ledger: &SessionLedger) -> Result<(), Error> {
    let mut turn_ids = HashSet::new();
    let mut active_index = 0_u64;
    let mut pending = false;
    for entry in &ledger.entries {
        if entry.turn_id.trim().is_empty()
            || entry.turn_id.len() > 512
            || entry.prompt_digest.trim().is_empty()
            || entry.prompt_digest.len() > 160
            || !turn_ids.insert(entry.turn_id.as_str())
        {
            return Err(Error::Operation(
                "native Turn ledger contains an invalid or duplicate identity".into(),
            ));
        }
        if matches!(entry.state, LedgerTurnState::Discarded) {
            continue;
        }
        if entry.runtime_prompt_index != active_index || pending {
            return Err(Error::Operation(
                "native Turn ledger active prompt indices are inconsistent".into(),
            ));
        }
        active_index = active_index
            .checked_add(1)
            .ok_or_else(|| Error::Operation("native Turn ledger index overflow".into()))?;
        match &entry.state {
            LedgerTurnState::Completed {
                outcome,
                settlement_id,
                usage,
            } => {
                let expected = if let Some(usage) = usage {
                    usage.validate().map_err(run_error)?;
                    ledger_settlement_id(
                        id.as_str(),
                        &entry.turn_id,
                        &entry.prompt_digest,
                        entry.runtime_prompt_index,
                        *outcome,
                        usage,
                    )?
                } else {
                    legacy_ledger_settlement_id(
                        id.as_str(),
                        &entry.turn_id,
                        &entry.prompt_digest,
                        entry.runtime_prompt_index,
                        *outcome,
                    )
                };
                if settlement_id != &expected {
                    return Err(Error::Operation(
                        "native Turn ledger settlement identity is invalid".into(),
                    ));
                }
            }
            LedgerTurnState::Pending => pending = true,
            LedgerTurnState::Discarded => unreachable!("discarded entry handled above"),
        }
    }
    Ok(())
}

pub(super) fn settle_latest_ledger_entry(ledger: &mut SessionLedger, receipt: &PromptReceipt) {
    ledger
        .entries
        .last_mut()
        .expect("the pending ledger entry was just appended")
        .state = LedgerTurnState::Completed {
        outcome: receipt.outcome,
        settlement_id: receipt.settlement_id.clone(),
        usage: Some(receipt.usage.clone()),
    };
}

pub(crate) fn ledger_settlement_id(
    session_id: &str,
    turn_id: &str,
    prompt_digest: &str,
    runtime_prompt_index: u64,
    outcome: TurnOutcome,
    usage: &xai_agent_lifecycle::run::EffectUsage,
) -> Result<String, Error> {
    let session = xai_agent_lifecycle::run::SessionRef::new(session_id).map_err(run_error)?;
    Ok(xai_agent_lifecycle::run::session_turn_settlement_id(
        &session,
        turn_id,
        prompt_digest,
        runtime_prompt_index,
        crate::durable_turn_outcome(outcome),
        usage,
    ))
}

pub(super) fn legacy_ledger_settlement_id(
    session_id: &str,
    turn_id: &str,
    prompt_digest: &str,
    runtime_prompt_index: u64,
    outcome: TurnOutcome,
) -> String {
    use sha2::Digest as _;
    let mut digest = sha2::Sha256::new();
    // Compatibility identifier retained across the public crate rename.
    digest.update(b"origin-grok-runtime.settlement.v1\0");
    let prompt_index = runtime_prompt_index.to_be_bytes();
    let outcome = format!("{outcome:?}");
    for field in [
        session_id.as_bytes(),
        turn_id.as_bytes(),
        prompt_digest.as_bytes(),
        prompt_index.as_slice(),
        outcome.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("sha256:{:x}", digest.finalize())
}

pub(super) fn prompt_effect_usage(
    usage: Option<&PromptUsage>,
    wall_ms: u64,
) -> xai_agent_lifecycle::run::EffectUsage {
    use xai_agent_lifecycle::run::{EffectUsage, ResourceDimension, ResourceVector};

    let mut resources = ResourceVector::default()
        .iterations(1)
        .agent_calls(1)
        .agent_concurrency(1)
        .wall_ms(wall_ms);
    let mut unknown = std::collections::BTreeSet::from([ResourceDimension::ArtifactBytes]);
    match usage {
        Some(usage) if !usage.usage_is_incomplete => {
            let totals = &usage.totals;
            if totals.total_tokens > 0
                && totals.model_calls > 0
                && totals
                    .input_tokens
                    .checked_add(totals.output_tokens)
                    .is_some_and(|total| total == totals.total_tokens)
            {
                resources.tokens = totals.total_tokens;
            } else {
                unknown.insert(ResourceDimension::Tokens);
            }
            if totals.api_duration_ms > 0 {
                resources.active_ms = totals.api_duration_ms;
            } else {
                unknown.insert(ResourceDimension::ActiveMs);
            }
            if !totals.cost_is_partial
                && let Some(ticks) = totals.cost_usd_ticks
                && let Ok(ticks) = u64::try_from(ticks)
                && let Some(micros) = ticks.checked_add(9_999).map(|value| value / 10_000)
            {
                resources.cost_micros = micros;
            } else {
                unknown.insert(ResourceDimension::CostMicros);
            }
        }
        _ => {
            unknown.extend([
                ResourceDimension::Tokens,
                ResourceDimension::CostMicros,
                ResourceDimension::ActiveMs,
            ]);
        }
    }
    EffectUsage::measured(resources).unknown(unknown)
}
