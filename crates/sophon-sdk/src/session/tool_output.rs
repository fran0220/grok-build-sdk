// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! What a settled tool call reported, read off its own output.
//!
//! [`ToolEvent::raw_output`] carries the runtime's own JSON for a call's
//! result. A Host that wants to say what a call did — how many lines an edit
//! changed, what a command printed — should not have to guess at that shape,
//! so the two facts a transcript needs are read here against the runtime's
//! typed output instead of by field-name archaeology in every Host.
//!
//! Both accessors answer only for the shapes the runtime reports
//! structurally, and return `None` for everything else, including output this
//! version of the SDK does not model. An absent answer means "not reported",
//! never "nothing happened" — a caller draws no number rather than a zero.

use crate::*;
use xai_grok_tools::types::output::{SearchReplaceOutput, ToolOutput};

/// Lines a settled edit reported changing.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash,
)]
pub struct ToolEditStat {
    pub added: usize,
    pub removed: usize,
}

impl ToolEditStat {
    /// Whether the edit reported no changed lines at all. True for a write
    /// that replaced a file with itself, and for an empty file created empty.
    pub fn is_empty(&self) -> bool {
        self.added == 0 && self.removed == 0
    }
}

impl ToolEvent {
    /// Lines added and removed, summed over every edit the call applied.
    ///
    /// Answers for a call that settled as an applied search/replace edit and
    /// carried its per-edit details — the shape the runtime's edit tools
    /// report. A call of another kind, one that failed, and one that applied
    /// without details all answer `None`.
    pub fn edit_stat(&self) -> Option<ToolEditStat> {
        let ToolOutput::SearchReplace(SearchReplaceOutput::EditsApplied(applied)) =
            parsed_output(self.raw_output.as_deref()?)?
        else {
            return None;
        };
        if applied.edits.details.is_empty() {
            return None;
        }
        let mut stat = ToolEditStat::default();
        for detail in &applied.edits.details {
            // Both sides empty with context around them is a blank line going
            // in: the file grew by a line that neither string spells. Without
            // context it is an empty file written empty, which changed no
            // lines and must not be counted as one.
            if detail.old_string.is_empty() && detail.new_string.is_empty() {
                if !detail.context_before.is_empty() || !detail.context_after.is_empty() {
                    stat.added += 1;
                }
                continue;
            }
            stat.removed += line_count(&detail.old_string);
            stat.added += line_count(&detail.new_string);
        }
        Some(stat)
    }

    /// The text the call produced, when it produced text.
    ///
    /// A command answers with what it printed, preferring the runtime's
    /// ANSI-stripped rendering and falling back to the raw bytes it captured;
    /// a tool whose whole result is preformatted text answers with that text.
    /// Everything else answers `None`, and so does a call that printed
    /// nothing.
    pub fn output_text(&self) -> Option<String> {
        let text = match parsed_output(self.raw_output.as_deref()?)? {
            ToolOutput::Bash(bash) => {
                if bash.output_for_prompt.is_empty() {
                    String::from_utf8_lossy(&bash.output).into_owned()
                } else {
                    bash.output_for_prompt
                }
            }
            ToolOutput::Text(text) => text.text,
            _ => return None,
        };
        (!text.is_empty()).then_some(text)
    }
}

/// The runtime's typed output, when this SDK can read the JSON it sent.
///
/// Output the runtime added since this SDK was built parses as nothing and is
/// reported as nothing; a Host is never handed a half-read result.
fn parsed_output(raw: &str) -> Option<ToolOutput> {
    serde_json::from_str(raw).ok()
}

/// How many lines one side of an edit spells. A string that ends without a
/// newline still spells its last line; an empty string spells none.
fn line_count(text: &str) -> usize {
    if text.is_empty() {
        0
    } else {
        text.split_inclusive('\n').count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use xai_grok_tools::types::output::{
        BashOutput, SearchReplaceEditContextInformation, SearchReplaceEditDetail,
        SearchReplaceEditsApplied, TextOutput,
    };

    fn event(output: Option<ToolOutput>) -> ToolEvent {
        ToolEvent {
            id: "call-1".into(),
            title: "src/main.rs".into(),
            kind: "edit".into(),
            status: "completed".into(),
            raw_input: None,
            raw_output: output
                .map(|output| serde_json::to_string(&output).expect("the runtime output encodes")),
        }
    }

    fn detail(old: &str, new: &str, context: bool) -> SearchReplaceEditDetail {
        SearchReplaceEditDetail {
            old_string: old.to_owned(),
            old_line: 1,
            new_string: new.to_owned(),
            new_line: 1,
            context_before: if context {
                "before\n".to_owned()
            } else {
                String::new()
            },
            context_after: if context {
                "after\n".to_owned()
            } else {
                String::new()
            },
            line_prefix: String::new(),
        }
    }

    fn edits(details: Vec<SearchReplaceEditDetail>) -> ToolOutput {
        ToolOutput::SearchReplace(SearchReplaceOutput::EditsApplied(
            SearchReplaceEditsApplied {
                old_string: String::new(),
                new_string: String::new(),
                tool_output_for_prompt: String::new(),
                tool_output_for_prompt_concise: None,
                absolute_path: PathBuf::from("/w/src/main.rs"),
                edits: SearchReplaceEditContextInformation { details },
                patch: None,
                unicode_normalized: false,
            },
        ))
    }

    fn bash(output: &str, for_prompt: &str) -> ToolOutput {
        ToolOutput::Bash(BashOutput {
            output: output.as_bytes().to_vec(),
            output_for_prompt: for_prompt.to_owned(),
            exit_code: 0,
            command: "ls".to_owned(),
            truncated: false,
            signal: None,
            timed_out: false,
            description: None,
            current_dir: "/w".to_owned(),
            output_file: String::new(),
            total_bytes: output.len(),
            output_delta: None,
            was_bare_echo: false,
        })
    }

    #[test]
    fn an_applied_edit_reports_the_lines_each_side_spells() {
        let stat = event(Some(edits(vec![
            detail("one\ntwo\n", "uno\n", false),
            detail("three", "tres\nquatro", false),
        ])))
        .edit_stat()
        .expect("an applied edit carries its details");
        assert_eq!(
            stat,
            ToolEditStat {
                added: 3,
                removed: 3
            }
        );
    }

    #[test]
    fn a_blank_line_going_in_counts_once_and_an_empty_write_counts_never() {
        let inserted = event(Some(edits(vec![detail("", "", true)])))
            .edit_stat()
            .expect("an applied edit carries its details");
        assert_eq!(
            inserted,
            ToolEditStat {
                added: 1,
                removed: 0
            }
        );

        let written_empty = event(Some(edits(vec![detail("", "", false)])))
            .edit_stat()
            .expect("an applied edit carries its details");
        assert!(written_empty.is_empty());
    }

    #[test]
    fn output_this_sdk_cannot_read_is_reported_as_nothing_rather_than_as_zero() {
        assert_eq!(event(None).edit_stat(), None);
        assert_eq!(event(None).output_text(), None);

        let mut unreadable = event(None);
        unreadable.raw_output = Some("{\"ToolTheSdkHasNeverSeen\":{}}".to_owned());
        assert_eq!(unreadable.edit_stat(), None);
        assert_eq!(unreadable.output_text(), None);

        // An edit that applied nothing structured is not a zero-line edit.
        assert_eq!(event(Some(edits(Vec::new()))).edit_stat(), None);
        // A command is not an edit, and an edit did not print anything.
        assert_eq!(event(Some(bash("hi\n", "hi\n"))).edit_stat(), None);
        assert_eq!(
            event(Some(edits(vec![detail("a", "b", false)]))).output_text(),
            None
        );
    }

    #[test]
    fn a_command_answers_with_what_it_printed() {
        assert_eq!(
            event(Some(bash("\u{1b}[31mred\u{1b}[0m\n", "red\n"))).output_text(),
            Some("red\n".to_owned()),
            "the runtime's own rendering is preferred over the raw bytes"
        );
        assert_eq!(
            event(Some(bash("raw\n", ""))).output_text(),
            Some("raw\n".to_owned()),
            "a command the runtime did not pre-render still answers"
        );
        assert_eq!(
            event(Some(bash("", ""))).output_text(),
            None,
            "a command that printed nothing reports nothing"
        );
        assert_eq!(
            event(Some(ToolOutput::Text(TextOutput {
                text: "3 memories".to_owned(),
                consumed_completion_task_id: None,
            })))
            .output_text(),
            Some("3 memories".to_owned())
        );
    }
}
