// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! A scriptable persistent kernel that speaks the line protocol
//! [`sophon_sdk::LocalKernelRuntime`] drives.
//!
//! It exists so the kernel contract can be proven against a *real* long-lived
//! child process rather than against a simulation: it holds in-memory state
//! across executions, serialises the part of that state it declares restorable,
//! declares the part it cannot, cooperates with an interrupt, and can be made
//! to die or to refuse a snapshot on demand. Every conformance and backend test
//! in this crate drives this program.
//!
//! Directives arrive on stdin, one per line; frames come back on both stdout
//! and stderr. See `LocalKernelRuntime` for the protocol.
//!
//! Fragment commands, one per line:
//!
//! | Command | Effect |
//! |---|---|
//! | `set <key> <value>` | Binds a restorable value. |
//! | `get <key>` | Writes `<key>=<value>` and a newline to stdout. |
//! | `echo <text>` | Writes `<text>` and a newline to stdout. |
//! | `warn <text>` | Writes `<text>` and a newline to stderr. |
//! | `sleep <ms>` | Sleeps, checking for an interrupt every few milliseconds. |
//! | `emit <n>` | Writes exactly `n` bytes to stdout. |
//! | `emit-err <n>` | Writes exactly `n` bytes to stderr. |
//! | `load <module>` | Records a restorable module name. |
//! | `hold-file` | Opens a file under the working root and keeps it open. |
//! | `unserialisable <kind> <value>` | Holds a value the kernel declares it cannot serialise. |
//! | `raise <class>` | Ends the fragment as a raise with that class. |
//! | `die <code>` | Ends the process, killing the session. |
//!
//! Arguments: `--uncooperative` ignores the interrupt, so a cancel can only be
//! served by killing the process; `--reject-restore` refuses every snapshot.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, Write},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

/// Frames are marked with a doubled ASCII record separator. Ordinary kernel
/// output never contains it, which is what lets the driver find a frame
/// boundary in a byte stream that is not line-aligned — an `emit` fragment
/// deliberately produces output with no trailing newline.
const MARK: &str = "\u{1e}\u{1e}";
const CHECKPOINT_HEADER: &str = "kernel-checkpoint 1";

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn on_interrupt(_: libc::c_int) {
    INTERRUPTED.store(true, Ordering::SeqCst);
}

#[cfg(unix)]
fn install_interrupt_handler(cooperative: bool) {
    // SAFETY: the handler only stores into a `static AtomicBool`, which is the
    // one thing a signal handler is allowed to do here.
    unsafe {
        if cooperative {
            libc::signal(
                libc::SIGINT,
                on_interrupt as *const () as libc::sighandler_t,
            );
        } else {
            libc::signal(libc::SIGINT, libc::SIG_IGN);
        }
    }
}

#[cfg(not(unix))]
fn install_interrupt_handler(_: bool) {}

#[derive(Default)]
struct State {
    bindings: BTreeMap<String, String>,
    modules: BTreeSet<String>,
    /// Values the kernel holds but declares it cannot serialise, by kind.
    unserialisable: BTreeMap<String, Vec<String>>,
    open_files: Vec<std::fs::File>,
}

enum Status {
    Ok,
    Raised(String),
    Cancelled,
    Rejected(String),
}

impl Status {
    fn token(&self) -> String {
        match self {
            Self::Ok => "ok".into(),
            Self::Raised(class) => format!("raised {class}"),
            Self::Cancelled => "cancelled".into(),
            Self::Rejected(reason) => format!("rejected {reason}"),
        }
    }
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let cooperative = !arguments.iter().any(|value| value == "--uncooperative");
    let reject_restore = arguments.iter().any(|value| value == "--reject-restore");
    install_interrupt_handler(cooperative);

    let mut state = State::default();
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    while let Some(line) = lines.next() {
        let Ok(line) = line else { break };
        let Some(directive) = line.strip_prefix(MARK) else {
            // Stray input outside a frame is a protocol error the driver would
            // otherwise hang on.
            frame(&Status::Rejected("unframed-input".into()));
            continue;
        };
        match directive {
            "exec" => {
                let body = read_body(&mut lines);
                let status = execute(&mut state, &body);
                frame(&status);
            }
            "checkpoint" => {
                write_stream(1, snapshot(&state).as_bytes());
                frame(&Status::Ok);
            }
            "restore" => {
                let body = read_body(&mut lines);
                let status = if reject_restore {
                    Status::Rejected("image-refused-snapshot".into())
                } else {
                    restore(&mut state, &body)
                };
                frame(&status);
            }
            "close" => {
                frame(&Status::Ok);
                return;
            }
            other => frame(&Status::Rejected(format!("unknown-directive-{other}"))),
        }
    }
}

fn read_body(lines: &mut std::io::Lines<std::io::StdinLock<'_>>) -> Vec<String> {
    let mut body = Vec::new();
    for line in lines.by_ref() {
        let Ok(line) = line else { break };
        if line == format!("{MARK}end") {
            break;
        }
        body.push(line);
    }
    body
}

fn execute(state: &mut State, body: &[String]) -> Status {
    for line in body {
        if INTERRUPTED.swap(false, Ordering::SeqCst) {
            return Status::Cancelled;
        }
        let (command, rest) = split(line);
        match command {
            "" => {}
            "set" => {
                let (key, value) = split(rest);
                state.bindings.insert(key.to_owned(), value.to_owned());
            }
            "get" => {
                let value = state.bindings.get(rest).cloned().unwrap_or_default();
                write_stream(1, format!("{rest}={value}\n").as_bytes());
            }
            "echo" => write_stream(1, format!("{rest}\n").as_bytes()),
            "warn" => write_stream(2, format!("{rest}\n").as_bytes()),
            "sleep" => {
                let total = rest.parse::<u64>().unwrap_or(0);
                let mut slept = 0;
                while slept < total {
                    if INTERRUPTED.swap(false, Ordering::SeqCst) {
                        return Status::Cancelled;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                    slept += 5;
                }
            }
            "emit" | "emit-err" => {
                let stream = if command == "emit" { 1 } else { 2 };
                emit(stream, rest.parse::<u64>().unwrap_or(0));
            }
            "load" => {
                state.modules.insert(rest.to_owned());
            }
            "hold-file" => {
                let path = std::env::current_dir()
                    .unwrap_or_default()
                    .join(format!("kernel-fixture-{}.hold", state.open_files.len()));
                if let Ok(file) = std::fs::File::create(path) {
                    state.open_files.push(file);
                }
            }
            "unserialisable" => {
                let (kind, value) = split(rest);
                state
                    .unserialisable
                    .entry(kind.to_owned())
                    .or_default()
                    .push(value.to_owned());
            }
            "raise" => return Status::Raised(rest.to_owned()),
            "die" => {
                let code = rest.parse::<i32>().unwrap_or(0);
                std::process::exit(code);
            }
            other => return Status::Raised(format!("unknown-command-{other}")),
        }
    }
    if INTERRUPTED.swap(false, Ordering::SeqCst) {
        return Status::Cancelled;
    }
    Status::Ok
}

/// The snapshot carries every binding and module, and *declares* — without
/// carrying — the open files and the values whose type it cannot serialise.
fn snapshot(state: &State) -> String {
    let mut payload = format!("{CHECKPOINT_HEADER}\n");
    for (key, value) in &state.bindings {
        payload.push_str(&format!("bind {key} {value}\n"));
    }
    for module in &state.modules {
        payload.push_str(&format!("module {module}\n"));
    }
    if !state.open_files.is_empty() {
        payload.push_str(&format!("lost open_file {}\n", state.open_files.len()));
    }
    for (kind, values) in &state.unserialisable {
        payload.push_str(&format!(
            "lost unserialisable_value {kind} {}\n",
            values.len()
        ));
    }
    payload
}

fn restore(state: &mut State, body: &[String]) -> Status {
    if body.first().map(String::as_str) != Some(CHECKPOINT_HEADER) {
        return Status::Rejected("malformed-payload".into());
    }
    let mut bindings = BTreeMap::new();
    let mut modules = BTreeSet::new();
    for line in &body[1..] {
        let (kind, rest) = split(line);
        match kind {
            "bind" => {
                let (key, value) = split(rest);
                bindings.insert(key.to_owned(), value.to_owned());
            }
            "module" => {
                modules.insert(rest.to_owned());
            }
            // A loss the snapshot declared is exactly what a restore does not
            // reconstruct. Reading it back as state would be the silent lie the
            // contract exists to prevent.
            "lost" | "" => {}
            _ => return Status::Rejected("malformed-payload".into()),
        }
    }
    state.bindings = bindings;
    state.modules = modules;
    state.unserialisable.clear();
    state.open_files.clear();
    Status::Ok
}

fn emit(stream: u8, count: u64) {
    let chunk = vec![b'x'; 1024];
    let mut remaining = count;
    while remaining >= 1024 {
        write_stream(stream, &chunk);
        remaining -= 1024;
    }
    if remaining > 0 {
        write_stream(stream, &chunk[..remaining as usize]);
    }
}

fn frame(status: &Status) {
    let frame = format!("{MARK}done {}\n", status.token());
    write_stream(2, frame.as_bytes());
    write_stream(1, frame.as_bytes());
}

fn write_stream(stream: u8, bytes: &[u8]) {
    if stream == 1 {
        let mut handle = std::io::stdout().lock();
        let _ = handle.write_all(bytes);
        let _ = handle.flush();
    } else {
        let mut handle = std::io::stderr().lock();
        let _ = handle.write_all(bytes);
        let _ = handle.flush();
    }
}

fn split(line: &str) -> (&str, &str) {
    match line.split_once(' ') {
        Some((head, rest)) => (head, rest),
        None => (line, ""),
    }
}
