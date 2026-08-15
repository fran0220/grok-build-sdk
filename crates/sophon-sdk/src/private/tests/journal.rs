use super::super::*;

#[test]
fn session_replay_probe_observes_an_empty_retained_journal_exactly() {
    let gap = observe_journal_snapshot(None, 5, 0).expect("empty suffix is observable");
    assert_eq!(gap.oldest_retained_sequence, 6);
    assert_eq!(gap.inclusive_end_sequence, 5);
    assert_eq!(gap.retained_count, 0);
    assert!(gap.truncated);
    assert!(gap.events.is_empty());

    let empty = observe_journal_snapshot(None, 5, 5).expect("end cursor is valid");
    assert_eq!(empty.oldest_retained_sequence, 6);
    assert_eq!(empty.inclusive_end_sequence, 5);
    assert_eq!(empty.retained_count, 0);
    assert!(!empty.truncated);
    assert!(empty.events.is_empty());
    assert!(observe_journal_snapshot(None, 5, 6).is_err());
}
