use super::super::*;

#[test]
fn pending_rewind_never_guesses_from_prompt_count_when_prefix_identity_drifted() {
    let ledger = SessionLedger {
        entries: vec![SessionLedgerEntry {
            turn_id: "turn-0".into(),
            prompt_digest: "sha256:expected".into(),
            runtime_prompt_index: 0,
            state: LedgerTurnState::Completed {
                outcome: TurnOutcome::End,
                settlement_id: "settlement-0".into(),
                usage: None,
            },
        }],
    };
    let drifted = RewindPointWire {
        prompt_index: 0,
        created_at: "2026-08-07T00:00:00Z".into(),
        num_file_snapshots: 0,
        has_file_changes: false,
        prompt_preview: None,
        origin_prompt_digest: Some("sha256:other".into()),
    };

    assert!(native_rewind_already_applied(&[drifted], 1, &ledger).is_err());
}
