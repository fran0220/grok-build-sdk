// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Durable custody of program executions: identity, receipts, declared bounds,
//! honest settlement and restart reconciliation.
//!
//! [`crate::prime::ProgramDriver`] is the Run reducer's dispatch seam: it hands
//! a typed intent to whatever executes it and takes a receipt back. It says
//! nothing about *how* an execution is owned across a crash, what happens when
//! a caller changes its mind, how much output a program is allowed to produce,
//! or how a secret reaches a process without being written down. This module
//! publishes those answers as one backend-neutral contract, in the same shape
//! as [`crate::artifact::ArtifactVault`]: a trait, validated newtypes, declared
//! bounds, a local reference implementation and a conformance suite that any
//! Host backend must pass.
//!
//! Four rules make the contract worth depending on.
//!
//! *Settlement is never fabricated.* Every terminal answer is an
//! [`ExitDisposition`], and only [`ExitDisposition::Exited`] with code zero is
//! success. A cancel settles as [`ExitDisposition::Cancelled`], an elapsed
//! deadline as [`ExitDisposition::TimedOut`], and a process that was running
//! when the Host died and is gone when the Host returns as
//! [`ExitDisposition::Interrupted`]. An execution whose fate cannot be
//! established stays [`ExecutionStatus::Uncertain`] until something explicitly
//! resolves it; it never decays into success.
//!
//! *Output is bounded and the bound is on the record.* Capture limits are
//! declared at launch through [`ProgramBounds`] and validated there. A program
//! that outruns its limit produces a [`CaptureRecord`] whose
//! [`CaptureRecord::truncated`] is true and whose `produced_bytes` exceeds
//! `captured_bytes`, so the loss is a fact a Host can show rather than a gap it
//! has to infer.
//!
//! *Secrets are unrepresentable in durable state.* A launch binds an
//! environment variable to a [`CredentialHandleName`] — a name, never a value.
//! The value is produced at spawn time by a caller-supplied
//! [`CredentialResolver`], lives only in the [`ResolvedCredential`] the backend
//! hands to the process, and is zeroed when that value is dropped. There is no
//! type in this module that can carry a secret into a receipt, a store row or a
//! log line.
//!
//! *Wall time is the caller's.* A backend owns a monotonic duration source so
//! it can enforce a deadline, and nothing else. Every wall instant in a receipt
//! is the `now_ms` a caller declared at launch plus measured elapsed time,
//! which is what keeps the conformance suite deterministic about ordering.

mod conformance;
mod local;

pub use conformance::{
    CONFORMANCE_CREDENTIAL_HANDLE, CONFORMANCE_CREDENTIAL_VARIABLE, CONFORMANCE_EXIT_CODE,
    CONFORMANCE_FLOOD_BYTES, CONFORMANCE_SECRET, CONFORMANCE_STDERR_MARK, CONFORMANCE_STDOUT_MARK,
    ProgramRuntimeHarness, ProgramScript, run_program_runtime_conformance,
};
pub use local::{
    ArtifactVaultOutputSink, LocalProgramRuntime, OsLivenessProbe, PROGRAM_RUNTIME_SCHEMA_MARKER,
    PROGRAM_RUNTIME_SCHEMA_VERSION,
};

use crate::artifact::{ArtifactDigest, ArtifactHandle};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Largest program path the contract accepts, in bytes.
pub const MAX_PROGRAM_PATH_BYTES: usize = 4096;
/// Largest number of arguments one launch may declare.
pub const MAX_PROGRAM_ARGUMENTS: usize = 256;
/// Largest single argument, in bytes.
pub const MAX_PROGRAM_ARGUMENT_BYTES: usize = 4096;
/// Largest number of environment bindings — literal and credential together —
/// one launch may declare.
pub const MAX_PROGRAM_ENVIRONMENT_ENTRIES: usize = 256;
/// Largest environment variable name, in bytes.
pub const MAX_PROGRAM_ENVIRONMENT_NAME_BYTES: usize = 256;
/// Largest literal environment value, in bytes.
pub const MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES: usize = 4096;
/// Largest number of credential handles one launch may attach.
pub const MAX_PROGRAM_CREDENTIAL_BINDINGS: usize = 16;
/// Largest capture bound a launch may declare for one stream, in bytes.
pub const MAX_PROGRAM_CAPTURE_BYTES: u64 = 4 * 1024 * 1024;
/// Largest deadline a launch may declare, in milliseconds.
pub const MAX_PROGRAM_DEADLINE_MS: u64 = 24 * 60 * 60 * 1000;
/// Largest execution identity, in bytes.
pub const MAX_PROGRAM_EXECUTION_ID_BYTES: usize = 128;
/// Largest bounded single-line label — credential handle names, owner tokens —
/// in bytes.
pub const MAX_PROGRAM_LABEL_BYTES: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum ProgramError {
    #[error("program execution input is invalid: {0}")]
    Validation(String),
    #[error("execution {0} is not known to this authority")]
    NotFound(ExecutionId),
    #[error("execution conflicts with durable state: {0}")]
    Conflict(String),
    #[error("execution {0} is running under another owner and cannot be waited on here")]
    Unowned(ExecutionId),
    #[error("program could not be spawned: {0}")]
    Spawn(String),
    #[error("program execution storage failed: {0}")]
    Storage(String),
    #[error("durable execution state is corrupt: {0}")]
    Corrupt(String),
}

pub(crate) fn validation(message: impl Into<String>) -> ProgramError {
    ProgramError::Validation(message.into())
}

pub(crate) fn corrupt(message: impl Into<String>) -> ProgramError {
    ProgramError::Corrupt(message.into())
}

fn validate_line(label: &str, value: &str, limit: usize) -> Result<(), ProgramError> {
    if value.is_empty() {
        return Err(validation(format!("{label} is empty")));
    }
    if value.len() > limit {
        return Err(validation(format!("{label} exceeds {limit} bytes")));
    }
    if value.chars().any(char::is_control) {
        return Err(validation(format!("{label} contains a control character")));
    }
    if value.trim() != value {
        return Err(validation(format!(
            "{label} has leading or trailing whitespace"
        )));
    }
    Ok(())
}

/// Length-prefixed canonical encoding of a field sequence.
///
/// Prefixing every field with its own length is what makes the digest
/// unambiguous: no combination of field contents can be re-partitioned into a
/// different sequence that encodes the same way, so two executions with
/// different arguments cannot share an arguments digest.
fn digest_of_fields(fields: &[&str]) -> ArtifactDigest {
    let mut encoded = String::new();
    for field in fields {
        encoded.push_str(&field.len().to_string());
        encoded.push(':');
        encoded.push_str(field);
        encoded.push('\n');
    }
    ArtifactDigest::of(encoded.as_bytes())
}

/// The durable identity of one execution, chosen by the caller.
///
/// It is caller-supplied rather than backend-generated on purpose: a Host that
/// crashes between deciding to run something and hearing that it ran can only
/// ask *did execution X happen* if it named X first.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ExecutionId(String);

impl ExecutionId {
    pub fn new(value: impl Into<String>) -> Result<Self, ProgramError> {
        let value = value.into();
        validate_line("execution id", &value, MAX_PROGRAM_EXECUTION_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ExecutionId {
    type Error = ProgramError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ExecutionId> for String {
    fn from(value: ExecutionId) -> Self {
        value.0
    }
}

impl std::fmt::Display for ExecutionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A bounded, single-line, non-secret name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProgramLabel(String);

impl ProgramLabel {
    pub fn new(value: impl Into<String>) -> Result<Self, ProgramError> {
        let value = value.into();
        validate_line("program label", &value, MAX_PROGRAM_LABEL_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProgramLabel {
    type Error = ProgramError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ProgramLabel> for String {
    fn from(value: ProgramLabel) -> Self {
        value.0
    }
}

impl std::fmt::Display for ProgramLabel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The absolute path of an executable.
///
/// Relative paths are refused because a relative program name is resolved
/// against a search path the receipt does not record, which would make the
/// receipt's claim about *what ran* unverifiable.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProgramPath(String);

impl ProgramPath {
    pub fn new(value: impl AsRef<Path>) -> Result<Self, ProgramError> {
        let path = value.as_ref();
        let text = path
            .to_str()
            .ok_or_else(|| validation("program path is not valid Unicode"))?
            .to_owned();
        validate_line("program path", &text, MAX_PROGRAM_PATH_BYTES)?;
        if !path.is_absolute() {
            return Err(validation("program path is not absolute"));
        }
        Ok(Self(text))
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProgramPath {
    type Error = ProgramError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ProgramPath> for String {
    fn from(value: ProgramPath) -> Self {
        value.0
    }
}

/// The opaque name of a credential a launch may attach.
///
/// This type is the whole of requirement four. It holds a *name* — the thing a
/// Host keychain or relay knows a secret by — and there is no constructor,
/// accessor or conversion anywhere in this module that turns it into secret
/// material. Receipts, store rows and logs carry this and only this, so
/// "the secret was never persisted" is a property of the type rather than a
/// discipline every backend has to remember.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CredentialHandleName(String);

impl CredentialHandleName {
    pub fn new(value: impl Into<String>) -> Result<Self, ProgramError> {
        let value = value.into();
        validate_line("credential handle name", &value, MAX_PROGRAM_LABEL_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CredentialHandleName {
    type Error = ProgramError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CredentialHandleName> for String {
    fn from(value: CredentialHandleName) -> Self {
        value.0
    }
}

impl std::fmt::Display for CredentialHandleName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Secret material, alive only between a resolver answering and a process being
/// spawned.
///
/// It implements neither `Serialize`, `Display` nor a revealing `Debug`, and it
/// zeroes its buffer on drop, so the only way the value leaves this type is the
/// one call a backend needs to put it in a child's environment.
pub struct ResolvedCredential(String);

impl ResolvedCredential {
    pub fn new(secret: impl Into<String>) -> Result<Self, ProgramError> {
        let secret = secret.into();
        if secret.is_empty() {
            return Err(validation("resolved credential is empty"));
        }
        if secret.len() > MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES {
            return Err(validation(format!(
                "resolved credential exceeds {MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES} bytes"
            )));
        }
        if secret.contains('\0') {
            return Err(validation("resolved credential contains a NUL byte"));
        }
        Ok(Self(secret))
    }

    /// The only exit from this type. Named for the single legitimate use so a
    /// call site that is doing anything else reads as wrong.
    pub fn expose_for_spawn(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ResolvedCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedCredential(<redacted>)")
    }
}

impl Drop for ResolvedCredential {
    fn drop(&mut self) {
        // A `String` that merely goes out of scope leaves its bytes in the
        // freed allocation. Replacing the contents with an equal-length filler
        // overwrites that allocation in place rather than reallocating, which
        // is the difference between a secret that is gone and one that is
        // merely unreferenced.
        let filler = "\0".repeat(self.0.len());
        self.0.replace_range(.., &filler);
    }
}

/// Turns a credential handle name into secret material at spawn time.
///
/// The resolver is supplied by the caller on every launch and is never stored,
/// so the authority that holds secrets stays outside this contract entirely. A
/// resolver that does not recognise a name must fail; a backend must not spawn
/// a process with the variable unset or empty, because a program that silently
/// runs unauthenticated is worse than one that does not run.
pub trait CredentialResolver: Send + Sync {
    fn resolve(&self, handle: &CredentialHandleName) -> Result<ResolvedCredential, ProgramError>;
}

/// What one environment variable is bound to.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentBinding {
    /// A value the launch declares outright. It is part of the environment
    /// digest and is stored verbatim.
    Literal(String),
    /// A value produced at spawn time by the caller's resolver. Only the handle
    /// name is part of the digest and the durable record.
    Credential(CredentialHandleName),
}

/// Declared limits an execution is held to.
///
/// Bounds are part of the launch rather than a backend policy so that a
/// receipt can report the limit that produced a truncation or a timeout. All
/// three are validated before a process exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramBounds {
    deadline_ms: u64,
    stdout_capture_bytes: u64,
    stderr_capture_bytes: u64,
}

impl ProgramBounds {
    pub fn new(
        deadline_ms: u64,
        stdout_capture_bytes: u64,
        stderr_capture_bytes: u64,
    ) -> Result<Self, ProgramError> {
        let bounds = Self {
            deadline_ms,
            stdout_capture_bytes,
            stderr_capture_bytes,
        };
        bounds.validate()?;
        Ok(bounds)
    }

    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }

    pub fn stdout_capture_bytes(&self) -> u64 {
        self.stdout_capture_bytes
    }

    pub fn stderr_capture_bytes(&self) -> u64 {
        self.stderr_capture_bytes
    }

    pub fn capture_bytes(&self, stream: ProgramStream) -> u64 {
        match stream {
            ProgramStream::Stdout => self.stdout_capture_bytes,
            ProgramStream::Stderr => self.stderr_capture_bytes,
        }
    }

    /// Re-checks bounds decoded from storage. A zero deadline is refused
    /// because an execution nothing will ever settle is the failure this
    /// contract exists to prevent, not a configuration.
    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.deadline_ms == 0 {
            return Err(validation("a launch must declare a non-zero deadline"));
        }
        if self.deadline_ms > MAX_PROGRAM_DEADLINE_MS {
            return Err(validation(format!(
                "declared deadline exceeds {MAX_PROGRAM_DEADLINE_MS} ms"
            )));
        }
        for (label, value) in [
            ("stdout", self.stdout_capture_bytes),
            ("stderr", self.stderr_capture_bytes),
        ] {
            if value > MAX_PROGRAM_CAPTURE_BYTES {
                return Err(validation(format!(
                    "declared {label} capture bound exceeds {MAX_PROGRAM_CAPTURE_BYTES} bytes"
                )));
            }
        }
        Ok(())
    }
}

/// Which captured stream a record describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramStream {
    Stdout,
    Stderr,
}

impl ProgramStream {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

/// Everything a launch declares about one execution.
///
/// The environment is closed: a backend clears the inherited environment and
/// supplies exactly these bindings, so the environment digest describes the
/// whole environment the process saw rather than a delta against whatever the
/// Host happened to be running with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgramLaunch {
    program: ProgramPath,
    arguments: Vec<String>,
    environment: BTreeMap<String, EnvironmentBinding>,
    working_root: PathBuf,
    bounds: ProgramBounds,
}

impl ProgramLaunch {
    pub fn new(
        program: ProgramPath,
        working_root: impl Into<PathBuf>,
        bounds: ProgramBounds,
    ) -> Result<Self, ProgramError> {
        let working_root = working_root.into();
        if !working_root.is_absolute() {
            return Err(validation("working root is not absolute"));
        }
        if working_root.to_str().is_none() {
            return Err(validation("working root is not valid Unicode"));
        }
        bounds.validate()?;
        Ok(Self {
            program,
            arguments: Vec::new(),
            environment: BTreeMap::new(),
            working_root,
            bounds,
        })
    }

    pub fn argument(mut self, value: impl Into<String>) -> Result<Self, ProgramError> {
        let value = value.into();
        if self.arguments.len() >= MAX_PROGRAM_ARGUMENTS {
            return Err(validation(format!(
                "a launch may declare at most {MAX_PROGRAM_ARGUMENTS} arguments"
            )));
        }
        if value.len() > MAX_PROGRAM_ARGUMENT_BYTES {
            return Err(validation(format!(
                "argument exceeds {MAX_PROGRAM_ARGUMENT_BYTES} bytes"
            )));
        }
        if value.contains('\0') {
            return Err(validation("argument contains a NUL byte"));
        }
        self.arguments.push(value);
        Ok(self)
    }

    pub fn environment(
        self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, ProgramError> {
        let value = value.into();
        if value.len() > MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES {
            return Err(validation(format!(
                "environment value exceeds {MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES} bytes"
            )));
        }
        if value.contains('\0') {
            return Err(validation("environment value contains a NUL byte"));
        }
        self.bind(name, EnvironmentBinding::Literal(value))
    }

    /// Binds a variable to a credential the resolver will produce at spawn
    /// time. Note that this takes a name and cannot be given a secret.
    pub fn credential(
        self,
        name: impl Into<String>,
        handle: CredentialHandleName,
    ) -> Result<Self, ProgramError> {
        if self.credential_handles().len() >= MAX_PROGRAM_CREDENTIAL_BINDINGS {
            return Err(validation(format!(
                "a launch may attach at most {MAX_PROGRAM_CREDENTIAL_BINDINGS} credential handles"
            )));
        }
        self.bind(name, EnvironmentBinding::Credential(handle))
    }

    fn bind(
        mut self,
        name: impl Into<String>,
        binding: EnvironmentBinding,
    ) -> Result<Self, ProgramError> {
        let name = name.into();
        validate_environment_name(&name)?;
        if self.environment.len() >= MAX_PROGRAM_ENVIRONMENT_ENTRIES {
            return Err(validation(format!(
                "a launch may declare at most {MAX_PROGRAM_ENVIRONMENT_ENTRIES} environment entries"
            )));
        }
        if self.environment.insert(name.clone(), binding).is_some() {
            return Err(validation(format!(
                "environment variable {name} is bound twice"
            )));
        }
        Ok(self)
    }

    pub fn program(&self) -> &ProgramPath {
        &self.program
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn environment_bindings(&self) -> &BTreeMap<String, EnvironmentBinding> {
        &self.environment
    }

    pub fn working_root(&self) -> &Path {
        &self.working_root
    }

    pub fn bounds(&self) -> ProgramBounds {
        self.bounds
    }

    /// The credential handle names this launch attaches, in binding order.
    pub fn credential_handles(&self) -> Vec<CredentialHandleName> {
        self.environment
            .values()
            .filter_map(|binding| match binding {
                EnvironmentBinding::Credential(handle) => Some(handle.clone()),
                EnvironmentBinding::Literal(_) => None,
            })
            .collect()
    }

    /// Identifies the argument vector without reproducing it.
    pub fn arguments_digest(&self) -> ArtifactDigest {
        let fields: Vec<&str> = self.arguments.iter().map(String::as_str).collect();
        digest_of_fields(&fields)
    }

    /// Identifies the environment the process will see.
    ///
    /// A credential binding contributes its variable name and its handle name
    /// and nothing else, so the digest is stable across credential rotation —
    /// which is exactly right: rotating a secret does not change what was run.
    pub fn environment_digest(&self) -> ArtifactDigest {
        let mut fields: Vec<String> = Vec::with_capacity(self.environment.len() * 3);
        for (name, binding) in &self.environment {
            match binding {
                EnvironmentBinding::Literal(value) => {
                    fields.push("literal".into());
                    fields.push(name.clone());
                    fields.push(value.clone());
                }
                EnvironmentBinding::Credential(handle) => {
                    fields.push("credential".into());
                    fields.push(name.clone());
                    fields.push(handle.as_str().to_owned());
                }
            }
        }
        let borrowed: Vec<&str> = fields.iter().map(String::as_str).collect();
        digest_of_fields(&borrowed)
    }

    /// Re-checks the whole declaration. Backends call this before any spawn
    /// effect so the refusal ordering belongs to the contract rather than to
    /// each backend.
    pub fn validate(&self) -> Result<(), ProgramError> {
        ProgramPath::new(self.program.as_path())?;
        self.bounds.validate()?;
        if self.arguments.len() > MAX_PROGRAM_ARGUMENTS {
            return Err(validation("a launch declares too many arguments"));
        }
        for argument in &self.arguments {
            if argument.len() > MAX_PROGRAM_ARGUMENT_BYTES || argument.contains('\0') {
                return Err(validation("a launch declares an invalid argument"));
            }
        }
        if self.environment.len() > MAX_PROGRAM_ENVIRONMENT_ENTRIES {
            return Err(validation("a launch declares too many environment entries"));
        }
        if self.credential_handles().len() > MAX_PROGRAM_CREDENTIAL_BINDINGS {
            return Err(validation("a launch attaches too many credential handles"));
        }
        for (name, binding) in &self.environment {
            validate_environment_name(name)?;
            match binding {
                EnvironmentBinding::Literal(value) => {
                    if value.len() > MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES || value.contains('\0') {
                        return Err(validation("a launch declares an invalid environment value"));
                    }
                }
                EnvironmentBinding::Credential(handle) => {
                    CredentialHandleName::new(handle.as_str())?;
                }
            }
        }
        if !self.working_root.is_absolute() {
            return Err(validation("working root is not absolute"));
        }
        if !self.working_root.is_dir() {
            return Err(validation("working root is not an existing directory"));
        }
        Ok(())
    }
}

fn validate_environment_name(name: &str) -> Result<(), ProgramError> {
    if name.is_empty() || name.len() > MAX_PROGRAM_ENVIRONMENT_NAME_BYTES {
        return Err(validation("environment variable name is empty or too long"));
    }
    let mut characters = name.chars();
    let first = characters.next().unwrap_or('0');
    if !(first.is_ascii_alphabetic() || first == '_')
        || characters.any(|value| !(value.is_ascii_alphanumeric() || value == '_'))
    {
        return Err(validation(
            "environment variable name is not an ASCII identifier",
        ));
    }
    Ok(())
}

/// How an execution ended.
///
/// Only [`ExitDisposition::Exited`] with code zero is success, and the
/// vocabulary deliberately has a distinct name for every honest way an
/// execution can stop being this Host's problem. There is no variant meaning
/// "probably fine".
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitDisposition {
    /// The process ran to completion and reported this status code.
    Exited { code: i32 },
    /// The process was terminated by a signal it did not ask for.
    Signalled { signal: i32 },
    /// A caller asked for the execution to stop and it did.
    Cancelled,
    /// The declared deadline elapsed and the execution was stopped.
    TimedOut,
    /// The execution was running when its owner died, and the process is gone.
    /// The work may have completed, partly completed or not started; the one
    /// thing that is known is that nobody observed it finish.
    Interrupted,
    /// No process was ever created.
    FailedToStart,
}

impl ExitDisposition {
    /// Success is exactly one thing.
    pub fn is_success(self) -> bool {
        self == Self::Exited { code: 0 }
    }

    /// The stable token used in receipt digests and durable rows.
    pub fn as_token(self) -> String {
        match self {
            Self::Exited { code } => format!("exited:{code}"),
            Self::Signalled { signal } => format!("signalled:{signal}"),
            Self::Cancelled => "cancelled".into(),
            Self::TimedOut => "timed_out".into(),
            Self::Interrupted => "interrupted".into(),
            Self::FailedToStart => "failed_to_start".into(),
        }
    }

    pub fn parse(value: &str) -> Result<Self, ProgramError> {
        if let Some(code) = value.strip_prefix("exited:") {
            return code
                .parse()
                .map(|code| Self::Exited { code })
                .map_err(|_| corrupt("stored exit disposition has an undecodable status code"));
        }
        if let Some(signal) = value.strip_prefix("signalled:") {
            return signal
                .parse()
                .map(|signal| Self::Signalled { signal })
                .map_err(|_| corrupt("stored exit disposition has an undecodable signal"));
        }
        match value {
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            "interrupted" => Ok(Self::Interrupted),
            "failed_to_start" => Ok(Self::FailedToStart),
            other => Err(corrupt(format!("unknown exit disposition {other:?}"))),
        }
    }
}

/// One captured stream, bound to the artifact that holds it.
///
/// `produced_bytes` is what the program wrote and `captured_bytes` is what was
/// kept. A backend must keep draining after the bound is reached so the two can
/// differ honestly; a backend that stops reading would deadlock the program and
/// report a produced count that is a lie.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureRecord {
    pub stream: ProgramStream,
    /// The content-addressed artifact holding the captured prefix.
    pub artifact: ArtifactHandle,
    /// Bytes stored in the artifact.
    pub captured_bytes: u64,
    /// Bytes the program wrote to the stream, whether kept or not.
    pub produced_bytes: u64,
    /// The bound in force for this stream at launch.
    pub declared_bound: u64,
    /// True exactly when `produced_bytes > captured_bytes`.
    pub truncated: bool,
}

impl CaptureRecord {
    pub(crate) fn validate(&self) -> Result<(), ProgramError> {
        if self.captured_bytes > self.produced_bytes {
            return Err(corrupt(
                "a capture record kept more bytes than were written",
            ));
        }
        if self.captured_bytes > self.declared_bound {
            return Err(corrupt("a capture record exceeds its own declared bound"));
        }
        if self.truncated != (self.produced_bytes > self.captured_bytes) {
            return Err(corrupt(
                "a capture record's truncation flag disagrees with its own counts",
            ));
        }
        Ok(())
    }

    fn digest_field(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            self.stream.as_str(),
            self.artifact.id(),
            self.captured_bytes,
            self.produced_bytes,
            self.declared_bound,
            self.truncated
        )
    }
}

/// The durable, digest-verified account of one settled execution.
///
/// A receipt is written once and never edited. Everything in it is either
/// derived from the launch or measured during the execution, and nothing in it
/// can hold secret material: credentials appear as the handle names that were
/// attached.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub execution: ExecutionId,
    pub program: ProgramPath,
    pub arguments_digest: ArtifactDigest,
    pub environment_digest: ArtifactDigest,
    pub working_root: PathBuf,
    /// The credential handles the launch attached, by name only.
    pub credential_handles: Vec<CredentialHandleName>,
    pub bounds: ProgramBounds,
    pub disposition: ExitDisposition,
    /// The wall instant the caller declared at launch.
    pub started_at_ms: u64,
    /// `started_at_ms` plus the elapsed time the backend measured.
    pub settled_at_ms: u64,
    pub stdout: Option<CaptureRecord>,
    pub stderr: Option<CaptureRecord>,
}

impl ExecutionReceipt {
    pub fn duration_ms(&self) -> u64 {
        self.settled_at_ms.saturating_sub(self.started_at_ms)
    }

    pub fn succeeded(&self) -> bool {
        self.disposition.is_success()
    }

    /// Whether either captured stream lost output to its bound.
    pub fn truncated(&self) -> bool {
        [self.stdout.as_ref(), self.stderr.as_ref()]
            .into_iter()
            .flatten()
            .any(|capture| capture.truncated)
    }

    /// The identity of this receipt's content.
    ///
    /// A store persists it alongside the receipt and recomputes it on read, so
    /// a row that was edited underneath the contract fails the read instead of
    /// presenting an altered account of what ran.
    pub fn digest(&self) -> ArtifactDigest {
        let mut fields: Vec<String> = vec![
            self.execution.as_str().to_owned(),
            self.program.as_str().to_owned(),
            self.arguments_digest.as_str().to_owned(),
            self.environment_digest.as_str().to_owned(),
            self.working_root.to_string_lossy().into_owned(),
        ];
        fields.push(self.credential_handles.len().to_string());
        for handle in &self.credential_handles {
            fields.push(handle.as_str().to_owned());
        }
        fields.push(self.bounds.deadline_ms.to_string());
        fields.push(self.bounds.stdout_capture_bytes.to_string());
        fields.push(self.bounds.stderr_capture_bytes.to_string());
        fields.push(self.disposition.as_token());
        fields.push(self.started_at_ms.to_string());
        fields.push(self.settled_at_ms.to_string());
        for capture in [self.stdout.as_ref(), self.stderr.as_ref()] {
            fields.push(
                capture
                    .map(CaptureRecord::digest_field)
                    .unwrap_or_else(|| "absent".into()),
            );
        }
        let borrowed: Vec<&str> = fields.iter().map(String::as_str).collect();
        digest_of_fields(&borrowed)
    }

    /// Re-checks a receipt decoded from storage against the digest stored with
    /// it, and against its own internal consistency.
    pub fn verify(&self, expected: &ArtifactDigest) -> Result<(), ProgramError> {
        self.bounds
            .validate()
            .map_err(|error| corrupt(error.to_string()))?;
        if self.settled_at_ms < self.started_at_ms {
            return Err(corrupt("a receipt settles before it starts"));
        }
        for capture in [self.stdout.as_ref(), self.stderr.as_ref()]
            .into_iter()
            .flatten()
        {
            capture.validate()?;
            if capture.declared_bound != self.bounds.capture_bytes(capture.stream) {
                return Err(corrupt(
                    "a capture record cites a bound the launch did not declare",
                ));
            }
        }
        if self
            .stdout
            .as_ref()
            .is_some_and(|capture| capture.stream != ProgramStream::Stdout)
            || self
                .stderr
                .as_ref()
                .is_some_and(|capture| capture.stream != ProgramStream::Stderr)
        {
            return Err(corrupt(
                "a receipt binds a capture record to the wrong stream",
            ));
        }
        if &self.digest() != expected {
            return Err(corrupt(
                "a stored receipt does not address to its own digest",
            ));
        }
        Ok(())
    }
}

/// The OS-level identity of a launched process, and who launched it.
///
/// A pid alone is not evidence: pids are reused, so a probe that only asks
/// "does pid 4211 exist" can mistake an unrelated process for the execution.
/// The owner token names the runtime instance that created the process, which
/// is what lets a reopened store tell *my process* from *a dead instance's
/// process* before it probes at all.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pid: u32,
    owner: ProgramLabel,
}

impl ProcessIdentity {
    pub fn new(pid: u32, owner: ProgramLabel) -> Result<Self, ProgramError> {
        if pid == 0 {
            return Err(validation("a process identity must name a real pid"));
        }
        Ok(Self { pid, owner })
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn owner(&self) -> &ProgramLabel {
        &self.owner
    }
}

/// What a liveness probe could establish.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Liveness {
    /// The process named by the identity is still running.
    Live,
    /// The process is definitively gone.
    Gone,
    /// The probe could not establish either. This is a real answer and must
    /// stay one: guessing here is how an interrupted execution becomes a
    /// fabricated success.
    Unknown,
}

/// Establishes whether a recorded process still exists.
///
/// This is a caller-supplied seam because the trustworthy answer is
/// platform- and deployment-specific: a pid check is enough on a machine that
/// has not rebooted, while a Host running executions in containers or under a
/// service manager has stronger evidence available. [`OsLivenessProbe`] is the
/// reference implementation and documents what it can and cannot prove.
pub trait LivenessProbe: Send + Sync {
    fn probe(&self, process: &ProcessIdentity) -> Result<Liveness, ProgramError>;
}

/// What the authority currently knows about one execution.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionStatus {
    /// Launched by this handle and not yet settled.
    Running {
        started_at_ms: u64,
        process: ProcessIdentity,
    },
    /// Durably recorded as running, but not by this handle: either another
    /// handle owns it or the owner died. Nothing about its fate is known until
    /// it is reconciled, and it will not become anything else on its own.
    Uncertain {
        started_at_ms: u64,
        process: ProcessIdentity,
    },
    /// Settled, with the append-only receipt that says how.
    Settled(Box<ExecutionReceipt>),
}

impl ExecutionStatus {
    pub fn receipt(&self) -> Option<&ExecutionReceipt> {
        match self {
            Self::Settled(receipt) => Some(receipt),
            _ => None,
        }
    }

    pub fn is_settled(&self) -> bool {
        matches!(self, Self::Settled(_))
    }
}

/// What a reconciliation established.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconcileOutcome {
    /// The process is still alive. Nothing was settled; ask again later.
    StillRunning,
    /// The execution now has a receipt — either one it already had, or an
    /// honest [`ExitDisposition::Interrupted`] settlement for an orphan.
    Settled(Box<ExecutionReceipt>),
    /// The probe could not establish the process's fate. The execution stays
    /// [`ExecutionStatus::Uncertain`].
    Uncertain,
}

/// Where captured output goes.
///
/// Capture binds to [`ArtifactHandle`] — a derived-identity value type with no
/// storage behaviour of its own — rather than to
/// [`crate::artifact::ArtifactVault`], and the storage step is this one-method
/// seam. That keeps the two contracts independent in the direction that
/// matters: a Host that runs programs is not forced to adopt a vault, a Host
/// that has a vault gets the binding for free through
/// [`ArtifactVaultOutputSink`], and neither contract has to change when the
/// other does. Sharing the handle vocabulary is what makes a receipt's output
/// reference resolvable by whatever custody the Host does run.
pub trait ProgramOutputSink: Send + Sync + 'static {
    /// Stores one captured stream and answers the handle that names it.
    ///
    /// A backend verifies the returned handle against the content it supplied,
    /// so a sink cannot bind an execution's output to bytes that are not that
    /// output.
    fn store(
        &self,
        execution: &ExecutionId,
        stream: ProgramStream,
        content: &[u8],
        now_ms: u64,
    ) -> Result<ArtifactHandle, ProgramError>;
}

/// Durable custody of program executions.
///
/// Implementations own process creation, transactions and physical layout. They
/// must persist and fail-closed verify a schema marker and version, refuse
/// stored state they cannot decode within the published bounds, and make every
/// method atomic against every other handle to the same authority — including
/// handles in other processes, which is what makes a restarted Host able to see
/// what the dead one had launched.
///
/// Implementations own a monotonic duration source and no wall clock. Every
/// wall instant that reaches a receipt is derived from the `now_ms` a caller
/// declared at launch.
pub trait ProgramRuntime: Send + Sync + 'static {
    /// Validates the launch, resolves its credential handles, spawns the
    /// process and durably records it as running before returning.
    ///
    /// Recording before returning is the whole point: a Host that crashes on
    /// the next instruction must still find the execution on restart. An
    /// identity that is already known — running or settled — is
    /// [`ProgramError::Conflict`] rather than a second process, so a retried
    /// launch after an unknown outcome cannot double-execute.
    fn launch(
        &self,
        execution: &ExecutionId,
        launch: &ProgramLaunch,
        credentials: &dyn CredentialResolver,
        now_ms: u64,
    ) -> Result<ProcessIdentity, ProgramError>;

    /// Blocks until the execution settles, and answers its receipt.
    ///
    /// The declared deadline is enforced here: an execution that outlives it is
    /// stopped and settles as [`ExitDisposition::TimedOut`]. Waiting on an
    /// already-settled execution replays its stored receipt unchanged, so a
    /// caller that crashed after settlement and before recording the answer can
    /// simply ask again. Waiting on an execution this handle did not launch is
    /// [`ProgramError::Unowned`]; that is a reconciliation, not a wait.
    fn wait(&self, execution: &ExecutionId) -> Result<ExecutionReceipt, ProgramError>;

    /// Asks a running execution to stop, settling it as
    /// [`ExitDisposition::Cancelled`].
    ///
    /// Idempotent, and never rewrites a settlement that already happened: an
    /// execution that exited on its own microseconds before the cancel keeps
    /// the disposition it actually had.
    fn cancel(&self, execution: &ExecutionId) -> Result<(), ProgramError>;

    /// What is known about one execution, or `None` for an identity this
    /// authority has never seen. Durable state that cannot be decoded within
    /// the published bounds is [`ProgramError::Corrupt`], never `None`.
    fn inspect(&self, execution: &ExecutionId) -> Result<Option<ExecutionStatus>, ProgramError>;

    /// Executions this authority durably believes are running but that this
    /// handle does not own, in identity order.
    ///
    /// After a restart this is exactly the crash-time backlog. It stays
    /// non-empty until each entry is reconciled.
    fn requiring_reconciliation(&self) -> Result<Vec<ExecutionId>, ProgramError>;

    /// Resolves an uncertain execution using the caller's liveness evidence.
    ///
    /// A live process answers [`ReconcileOutcome::StillRunning`] and settles
    /// nothing. A gone process settles as [`ExitDisposition::Interrupted`] —
    /// never as success, because nobody observed it finish. An inconclusive
    /// probe answers [`ReconcileOutcome::Uncertain`] and leaves the execution
    /// exactly as uncertain as it was.
    fn reconcile(
        &self,
        execution: &ExecutionId,
        liveness: &dyn LivenessProbe,
        now_ms: u64,
    ) -> Result<ReconcileOutcome, ProgramError>;

    /// The receipt of a settled execution, or `None` when it has not settled.
    fn receipt(&self, execution: &ExecutionId) -> Result<Option<ExecutionReceipt>, ProgramError> {
        Ok(self
            .inspect(execution)?
            .and_then(|status| status.receipt().cloned()))
    }
}
