pub(super) fn git_status_is_dirty(status: &str) -> bool {
    status.lines().any(|record| record != "?? .cargo-ok")
}

#[cfg(test)]
mod tests {
    use super::git_status_is_dirty;

    #[test]
    fn clean_status_and_cargo_root_marker_are_clean() {
        assert!(!git_status_is_dirty(""));
        assert!(!git_status_is_dirty("?? .cargo-ok"));
        assert!(!git_status_is_dirty("?? .cargo-ok\n"));
    }

    #[test]
    fn tracked_or_staged_changes_are_dirty() {
        for status in [
            " M tracked.rs",
            "M  staged.rs",
            "MM staged-and-tracked.rs",
            "A  added.rs",
            "D  deleted.rs",
            "R  old.rs -> new.rs",
            " M .cargo-ok",
            "R  .cargo-ok -> renamed",
        ] {
            assert!(
                git_status_is_dirty(status),
                "status should be dirty: {status}"
            );
        }
    }

    #[test]
    fn every_other_untracked_path_is_dirty() {
        for status in [
            "?? nested/.cargo-ok",
            "?? .cargo-ok.backup",
            "?? cargo-ok",
            "?? .cargo-ok/child",
            "?? other-file",
        ] {
            assert!(
                git_status_is_dirty(status),
                "status should be dirty: {status}"
            );
        }
    }

    #[test]
    fn cargo_root_marker_combined_with_any_change_is_dirty() {
        for status in [
            "?? .cargo-ok\n M tracked.rs",
            "M  staged.rs\n?? .cargo-ok",
            "?? .cargo-ok\n?? other-file",
            "R  .cargo-ok -> renamed\n?? .cargo-ok",
        ] {
            assert!(
                git_status_is_dirty(status),
                "status should be dirty: {status}"
            );
        }
    }
}
