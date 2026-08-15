// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Durable custody of *persistent* programmatic kernel sessions: identity that
//! outlives a process, executions that share state, checkpoints that declare
//! what they could not carry, and settlement that never fabricates success.
//!
//! [`crate::ProgramRuntime`] owns the durable custody of a *one-shot*
//! execution: a process is named before it is spawned, its bounds are declared
//! at launch, its output is captured to an [`ArtifactHandle`], and it settles
//! into exactly one [`crate::ExitDisposition`]. That contract assumes identity
//! and process lifetime are the same thing.
//!
//! A persistent kernel — an interpreter session, a notebook backend, a
//! scripting VM — deliberately separates them. Many executions run inside one
//! process, in order, sharing in-memory state; an execution can be cancelled
//! without the session ending; a session can be checkpointed and restored into
//! a *different* process; and the process can die while the session is still a
//! thing the Host has a name for. Modelling that as repeated
//! [`crate::ProgramRuntime`] launches loses the one property the kernel exists
//! for, so this is a sibling contract that shares vocabulary — [`ProgramPath`],
//! [`CaptureRecord`], [`ProgramOutputSink`], [`ProcessIdentity`],
//! [`LivenessProbe`] — rather than an extension of an existing one.
//!
//! Three exclusions are load-bearing.
//!
//! *This is not a second agent loop.* The kernel executes fragments a caller
//! hands it. It does not choose what to run, does not call a model, does not
//! iterate, and has no notion of a Turn. Nothing here may grow a method that
//! means "keep going".
//!
//! *Kernel state is not durable truth.* A checkpoint is evidence a Host may use
//! to shorten recovery; it is never the authority for a fact. Every durable
//! fact lives in the Run and in the artifact vault.
//!
//! *Loss is never silent.* Kernel state is either reconstructible from durable
//! inputs or explicitly declared lost with a typed reason
//! ([`NonRestorableFact`]) that travels with the checkpoint and comes back out
//! of [`KernelRestore::Restored`] by value. There is no representation of
//! "restored, probably complete".
//!
//! Two further rules are structural rather than advisory. A kernel process
//! receives no credential: there is no credential type in this module, no
//! method takes a [`crate::CredentialResolver`], and
//! [`KernelSpec::validate`] refuses the published reserved environment names
//! outright ([`KERNEL_RESERVED_ENVIRONMENT_NAMES`]). And a session runs
//! strictly one execution at a time, so a receipt's [`sequence`] is also the
//! order in which state was mutated.
//!
//! [`sequence`]: KernelExecutionReceipt::sequence

mod conformance;
mod local;

pub use conformance::{
    CONFORMANCE_KERNEL_ERROR_CLASS, CONFORMANCE_KERNEL_FLOOD_BYTES, CONFORMANCE_KERNEL_SECRET,
    CONFORMANCE_KERNEL_STATE_NAME, CONFORMANCE_KERNEL_STATE_VALUE, CONFORMANCE_KERNEL_STDERR_MARK,
    CONFORMANCE_KERNEL_STDOUT_MARK, KernelDamage, KernelRuntimeHarness, KernelScript,
    run_kernel_runtime_conformance,
};
pub use local::{
    KERNEL_FRAME_MARK, KERNEL_RUNTIME_SCHEMA_MARKER, KERNEL_RUNTIME_SCHEMA_VERSION,
    LOCAL_KERNEL_PROTOCOL, LocalKernelRuntime,
};

use crate::artifact::{ArtifactDigest, ArtifactHandle};
use crate::program::{
    CaptureRecord, LivenessProbe, MAX_PROGRAM_ARGUMENT_BYTES, MAX_PROGRAM_ARGUMENTS,
    MAX_PROGRAM_ENVIRONMENT_ENTRIES, MAX_PROGRAM_ENVIRONMENT_NAME_BYTES,
    MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES, ProcessIdentity, ProgramOutputSink, ProgramPath,
    ProgramStream, digest_of_fields,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

/// Largest session identity the contract accepts, in bytes.
pub const MAX_KERNEL_SESSION_ID_BYTES: usize = 128;
/// Largest execution identity the contract accepts, in bytes.
pub const MAX_KERNEL_EXECUTION_ID_BYTES: usize = 128;
/// Largest fragment one submission may carry, in bytes.
pub const MAX_KERNEL_SOURCE_BYTES: usize = 1024 * 1024;
/// Largest capture bound one execution may declare for one stream, in bytes.
pub const MAX_KERNEL_CAPTURE_BYTES: u64 = 4 * 1024 * 1024;
/// Largest per-execution deadline, in milliseconds.
pub const MAX_KERNEL_EXECUTION_DEADLINE_MS: u64 = 24 * 60 * 60 * 1000;
/// Largest idle deadline a session may declare, in milliseconds.
pub const MAX_KERNEL_IDLE_DEADLINE_MS: u64 = 24 * 60 * 60 * 1000;
/// Executions one session incarnation may accumulate before it must be closed
/// and reopened. Sequence numbers are dense and never reused.
pub const MAX_KERNEL_SESSION_EXECUTIONS: u64 = 100_000;
/// Largest number of restorable declarations one checkpoint may carry.
pub const MAX_KERNEL_RESTORABLE_FACTS: usize = 256;
/// Largest number of non-restorable declarations one checkpoint may carry.
pub const MAX_KERNEL_NON_RESTORABLE_FACTS: usize = 256;
/// Largest checkpoint payload, in bytes. Bounded so a checkpoint cannot become
/// a de facto state store.
pub const MAX_KERNEL_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;
/// Largest bounded single-line label — error classes, protocol names, ceiling
/// names, foreign-handle kinds — in bytes.
pub const MAX_KERNEL_LABEL_BYTES: usize = 256;

/// Environment variable names a kernel spec may never bind.
///
/// This is item three of the structural credential rule: a Host cannot smuggle
/// a secret into a kernel as a literal by naming it the thing a kernel library
/// would read. The list is published rather than Host-supplied so the property
/// is checkable by [`run_kernel_runtime_conformance`]; a Host that has more
/// names of its own adds them with [`KernelSpec::reserving`].
///
/// Names are compared ASCII-case-insensitively. Environment names are
/// case-sensitive on Unix and case-insensitive on Windows, so a case-sensitive
/// check would refuse `XAI_API_KEY` while admitting `xai_api_key` — which names
/// the same variable on one of the two platforms this SDK targets.
pub const KERNEL_RESERVED_ENVIRONMENT_NAMES: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "GROK_API_KEY",
    "GROK_BUILD_API_KEY",
    "GROK_BUILD_RELAY_BEARER",
    "GROK_BUILD_RELAY_TOKEN",
    "OPENAI_API_KEY",
    "XAI_API_KEY",
];

#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    #[error("kernel input is invalid: {0}")]
    Validation(String),
    #[error("kernel session {0} is not known to this authority")]
    SessionNotFound(KernelSessionId),
    #[error("kernel execution {0} is not known to this authority")]
    ExecutionNotFound(KernelExecutionId),
    #[error("kernel state conflicts with durable state: {0}")]
    Conflict(String),
    #[error("kernel session {0} is owned by another handle")]
    Unowned(KernelSessionId),
    #[error("kernel session {0} is not live")]
    NotLive(KernelSessionId),
    #[error("kernel could not be started: {0}")]
    Start(String),
    #[error("kernel storage failed: {0}")]
    Storage(String),
    #[error("durable kernel state is corrupt: {0}")]
    Corrupt(String),
}

pub(crate) fn validation(message: impl Into<String>) -> KernelError {
    KernelError::Validation(message.into())
}

pub(crate) fn corrupt(message: impl Into<String>) -> KernelError {
    KernelError::Corrupt(message.into())
}

fn validate_line(label: &str, value: &str, limit: usize) -> Result<(), KernelError> {
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

/// The durable identity of one kernel session, chosen by the caller.
///
/// Caller-supplied for the same reason [`crate::ExecutionId`] is: a Host that
/// crashes between deciding to start a kernel and hearing that it started can
/// only ask *does session X exist* if it named X first.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct KernelSessionId(String);

impl KernelSessionId {
    pub fn new(value: impl Into<String>) -> Result<Self, KernelError> {
        let value = value.into();
        validate_line("kernel session id", &value, MAX_KERNEL_SESSION_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for KernelSessionId {
    type Error = KernelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<KernelSessionId> for String {
    fn from(value: KernelSessionId) -> Self {
        value.0
    }
}

impl std::fmt::Display for KernelSessionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The identity of one execution *within* one session.
///
/// Scoped rather than global: two sessions may use the same execution name and
/// mean different things, and [`KernelExecutionKey`] is what a receipt
/// addresses.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct KernelExecutionId(String);

impl KernelExecutionId {
    pub fn new(value: impl Into<String>) -> Result<Self, KernelError> {
        let value = value.into();
        validate_line("kernel execution id", &value, MAX_KERNEL_EXECUTION_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for KernelExecutionId {
    type Error = KernelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<KernelExecutionId> for String {
    fn from(value: KernelExecutionId) -> Self {
        value.0
    }
}

impl std::fmt::Display for KernelExecutionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A bounded, single-line, non-secret name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct KernelLabel(String);

impl KernelLabel {
    pub fn new(value: impl Into<String>) -> Result<Self, KernelError> {
        let value = value.into();
        validate_line("kernel label", &value, MAX_KERNEL_LABEL_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for KernelLabel {
    type Error = KernelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<KernelLabel> for String {
    fn from(value: KernelLabel) -> Self {
        value.0
    }
}

impl std::fmt::Display for KernelLabel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Monotonic incarnation counter for one session identity.
///
/// A session identity survives process death; a *generation* does not. Every
/// successful [`KernelRuntime::open`] or [`KernelRuntime::restore`] mints the
/// next generation and never reuses a value, so a receipt or a checkpoint
/// always says which incarnation produced it. This is the same fencing
/// discipline as [`crate::ActivationFencingToken`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KernelGeneration(u64);

impl KernelGeneration {
    /// The first incarnation of a session identity.
    pub const FIRST: Self = Self(1);

    /// Minted by the authority alone. A backend must never issue a value it has
    /// issued before for the same session identity.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next incarnation, or `None` at the end of the counter.
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    pub(crate) fn validate(self) -> Result<(), KernelError> {
        if self.0 == 0 {
            return Err(validation("a kernel generation starts at one"));
        }
        Ok(())
    }
}

impl std::fmt::Display for KernelGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// The addressable identity of one execution: session, generation, execution.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KernelExecutionKey {
    session: KernelSessionId,
    generation: KernelGeneration,
    execution: KernelExecutionId,
}

impl KernelExecutionKey {
    pub fn new(
        session: KernelSessionId,
        generation: KernelGeneration,
        execution: KernelExecutionId,
    ) -> Result<Self, KernelError> {
        generation.validate()?;
        Ok(Self {
            session,
            generation,
            execution,
        })
    }

    pub fn session(&self) -> &KernelSessionId {
        &self.session
    }

    pub fn generation(&self) -> KernelGeneration {
        self.generation
    }

    pub fn execution(&self) -> &KernelExecutionId {
        &self.execution
    }

    pub fn validate(&self) -> Result<(), KernelError> {
        KernelSessionId::new(self.session.as_str())?;
        KernelExecutionId::new(self.execution.as_str())?;
        self.generation.validate()
    }
}

impl std::fmt::Display for KernelExecutionKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}@{}/{}",
            self.session, self.generation, self.execution
        )
    }
}

/// Limits the whole session incarnation is held to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelSessionBounds {
    idle_deadline_ms: u64,
    max_executions: u64,
    max_captured_bytes: u64,
}

impl KernelSessionBounds {
    pub fn new(
        idle_deadline_ms: u64,
        max_executions: u64,
        max_captured_bytes: u64,
    ) -> Result<Self, KernelError> {
        let bounds = Self {
            idle_deadline_ms,
            max_executions,
            max_captured_bytes,
        };
        bounds.validate()?;
        Ok(bounds)
    }

    /// The session settles as [`KernelDisposition::IdleExpired`] if no
    /// execution is submitted within this window.
    pub fn idle_deadline_ms(&self) -> u64 {
        self.idle_deadline_ms
    }

    pub fn max_executions(&self) -> u64 {
        self.max_executions
    }

    pub fn max_captured_bytes(&self) -> u64 {
        self.max_captured_bytes
    }

    /// Re-checks bounds decoded from storage. Zero is refused everywhere: a
    /// session nothing will ever settle, or that may accept no work at all, is
    /// the failure this contract exists to prevent rather than a configuration.
    pub fn validate(&self) -> Result<(), KernelError> {
        if self.idle_deadline_ms == 0 {
            return Err(validation(
                "a session must declare a non-zero idle deadline",
            ));
        }
        if self.idle_deadline_ms > MAX_KERNEL_IDLE_DEADLINE_MS {
            return Err(validation(format!(
                "declared idle deadline exceeds {MAX_KERNEL_IDLE_DEADLINE_MS} ms"
            )));
        }
        if self.max_executions == 0 || self.max_executions > MAX_KERNEL_SESSION_EXECUTIONS {
            return Err(validation(format!(
                "a session must declare between 1 and {MAX_KERNEL_SESSION_EXECUTIONS} executions"
            )));
        }
        if self.max_captured_bytes == 0 {
            return Err(validation(
                "a session must declare a non-zero captured-byte ceiling",
            ));
        }
        Ok(())
    }
}

/// Per-execution limits. Declared at submit so a receipt can report the limit
/// that produced a truncation or a timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelExecutionBounds {
    deadline_ms: u64,
    stdout_capture_bytes: u64,
    stderr_capture_bytes: u64,
}

impl KernelExecutionBounds {
    pub fn new(
        deadline_ms: u64,
        stdout_capture_bytes: u64,
        stderr_capture_bytes: u64,
    ) -> Result<Self, KernelError> {
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

    pub fn validate(&self) -> Result<(), KernelError> {
        if self.deadline_ms == 0 {
            return Err(validation("a submission must declare a non-zero deadline"));
        }
        if self.deadline_ms > MAX_KERNEL_EXECUTION_DEADLINE_MS {
            return Err(validation(format!(
                "declared deadline exceeds {MAX_KERNEL_EXECUTION_DEADLINE_MS} ms"
            )));
        }
        for (label, value) in [
            ("stdout", self.stdout_capture_bytes),
            ("stderr", self.stderr_capture_bytes),
        ] {
            if value > MAX_KERNEL_CAPTURE_BYTES {
                return Err(validation(format!(
                    "declared {label} capture bound exceeds {MAX_KERNEL_CAPTURE_BYTES} bytes"
                )));
            }
        }
        Ok(())
    }
}

/// A kernel image the Host is willing to run.
///
/// The program path and its argument vector are declared exactly as in
/// [`crate::ProgramLaunch`], absolute-path rule included, so a receipt's claim
/// about what ran stays verifiable. The environment holds literal bindings
/// only: there is no shape here in which a credential handle name could be
/// attached, and [`Self::validate`] refuses the reserved names outright.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KernelSpec {
    program: ProgramPath,
    protocol: KernelLabel,
    arguments: Vec<String>,
    environment: BTreeMap<String, String>,
    reserved: BTreeSet<String>,
    working_root: PathBuf,
    bounds: KernelSessionBounds,
}

impl KernelSpec {
    /// Declares an image. `protocol` is the kernel dialect the Host expects to
    /// speak, folded into [`Self::spec_digest`] so a Host cannot restore a
    /// checkpoint into an image that answers a different dialect.
    pub fn new(
        program: ProgramPath,
        protocol: KernelLabel,
        working_root: impl Into<PathBuf>,
        bounds: KernelSessionBounds,
    ) -> Result<Self, KernelError> {
        let working_root = working_root.into();
        if !working_root.is_absolute() {
            return Err(validation("kernel working root is not absolute"));
        }
        if working_root.to_str().is_none() {
            return Err(validation("kernel working root is not valid Unicode"));
        }
        bounds.validate()?;
        Ok(Self {
            program,
            protocol,
            arguments: Vec::new(),
            environment: BTreeMap::new(),
            reserved: BTreeSet::new(),
            working_root,
            bounds,
        })
    }

    pub fn argument(mut self, value: impl Into<String>) -> Result<Self, KernelError> {
        let value = value.into();
        if self.arguments.len() >= MAX_PROGRAM_ARGUMENTS {
            return Err(validation(format!(
                "a kernel spec may declare at most {MAX_PROGRAM_ARGUMENTS} arguments"
            )));
        }
        if value.len() > MAX_PROGRAM_ARGUMENT_BYTES {
            return Err(validation(format!(
                "kernel argument exceeds {MAX_PROGRAM_ARGUMENT_BYTES} bytes"
            )));
        }
        if value.contains('\0') {
            return Err(validation("kernel argument contains a NUL byte"));
        }
        self.arguments.push(value);
        Ok(self)
    }

    /// Binds one literal environment value. Note that this takes a `String` and
    /// there is no sibling that takes a credential handle.
    pub fn environment(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, KernelError> {
        let name = name.into();
        let value = value.into();
        validate_environment_name(&name)?;
        self.refuse_reserved(&name)?;
        if value.len() > MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES {
            return Err(validation(format!(
                "kernel environment value exceeds {MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES} bytes"
            )));
        }
        if value.contains('\0') {
            return Err(validation("kernel environment value contains a NUL byte"));
        }
        if self.environment.len() >= MAX_PROGRAM_ENVIRONMENT_ENTRIES {
            return Err(validation(format!(
                "a kernel spec may declare at most {MAX_PROGRAM_ENVIRONMENT_ENTRIES} environment entries"
            )));
        }
        if self.environment.insert(name.clone(), value).is_some() {
            return Err(validation(format!(
                "environment variable {name} is bound twice"
            )));
        }
        Ok(self)
    }

    /// Adds one Host-declared reserved environment name to the published set.
    ///
    /// A Host that routes provider access through variables this SDK does not
    /// know about closes the same hole for its own names.
    pub fn reserving(mut self, name: impl Into<String>) -> Result<Self, KernelError> {
        let name = name.into();
        validate_environment_name(&name)?;
        if self
            .environment
            .keys()
            .any(|bound| equal_name(bound, &name))
        {
            return Err(validation(format!(
                "environment variable {name} is already bound and cannot be reserved"
            )));
        }
        self.reserved.insert(name.to_ascii_uppercase());
        Ok(self)
    }

    pub fn program(&self) -> &ProgramPath {
        &self.program
    }

    pub fn protocol(&self) -> &KernelLabel {
        &self.protocol
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn environment_bindings(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    pub fn working_root(&self) -> &Path {
        &self.working_root
    }

    pub fn bounds(&self) -> KernelSessionBounds {
        self.bounds
    }

    /// Identifies the image without reproducing it, using the same
    /// length-prefixed canonical encoding as [`crate::ProgramLaunch`].
    ///
    /// The protocol label is folded in, so an image that answers a different
    /// dialect is a different digest and therefore a
    /// [`KernelRestore::SpecMismatch`] rather than a silent reinterpretation.
    pub fn spec_digest(&self) -> ArtifactDigest {
        let mut fields: Vec<String> = vec![
            "kernel-spec".into(),
            self.protocol.as_str().to_owned(),
            self.program.as_str().to_owned(),
            self.working_root.to_string_lossy().into_owned(),
            self.arguments.len().to_string(),
        ];
        fields.extend(self.arguments.iter().cloned());
        fields.push(self.environment.len().to_string());
        for (name, value) in &self.environment {
            fields.push(name.clone());
            fields.push(value.clone());
        }
        fields.push(self.bounds.idle_deadline_ms.to_string());
        fields.push(self.bounds.max_executions.to_string());
        fields.push(self.bounds.max_captured_bytes.to_string());
        let borrowed: Vec<&str> = fields.iter().map(String::as_str).collect();
        digest_of_fields(&borrowed)
    }

    /// Re-checks the whole declaration. Backends call this before creating a
    /// process, so the refusal ordering belongs to the contract rather than to
    /// each backend.
    pub fn validate(&self) -> Result<(), KernelError> {
        ProgramPath::new(self.program.as_path()).map_err(|error| validation(error.to_string()))?;
        KernelLabel::new(self.protocol.as_str())?;
        self.bounds.validate()?;
        if self.arguments.len() > MAX_PROGRAM_ARGUMENTS {
            return Err(validation("a kernel spec declares too many arguments"));
        }
        for argument in &self.arguments {
            if argument.len() > MAX_PROGRAM_ARGUMENT_BYTES || argument.contains('\0') {
                return Err(validation("a kernel spec declares an invalid argument"));
            }
        }
        if self.environment.len() > MAX_PROGRAM_ENVIRONMENT_ENTRIES {
            return Err(validation(
                "a kernel spec declares too many environment entries",
            ));
        }
        for (name, value) in &self.environment {
            validate_environment_name(name)?;
            self.refuse_reserved(name)?;
            if value.len() > MAX_PROGRAM_ENVIRONMENT_VALUE_BYTES || value.contains('\0') {
                return Err(validation(
                    "a kernel spec declares an invalid environment value",
                ));
            }
        }
        if !self.working_root.is_absolute() {
            return Err(validation("kernel working root is not absolute"));
        }
        if !self.working_root.is_dir() {
            return Err(validation(
                "kernel working root is not an existing directory",
            ));
        }
        Ok(())
    }

    fn refuse_reserved(&self, name: &str) -> Result<(), KernelError> {
        if KERNEL_RESERVED_ENVIRONMENT_NAMES
            .iter()
            .any(|reserved| equal_name(reserved, name))
            || self
                .reserved
                .iter()
                .any(|reserved| equal_name(reserved, name))
        {
            return Err(validation(format!(
                "environment variable {name} is reserved: a kernel receives no credential"
            )));
        }
        Ok(())
    }
}

fn equal_name(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn validate_environment_name(name: &str) -> Result<(), KernelError> {
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

/// One unit of work handed to a live session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KernelSubmission {
    source: String,
    bounds: KernelExecutionBounds,
}

impl KernelSubmission {
    /// The fragment is content, not a path, so
    /// [`KernelExecutionReceipt::source_digest`] addresses exactly what ran.
    pub fn new(
        source: impl Into<String>,
        bounds: KernelExecutionBounds,
    ) -> Result<Self, KernelError> {
        let submission = Self {
            source: source.into(),
            bounds,
        };
        submission.validate()?;
        Ok(submission)
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn bounds(&self) -> KernelExecutionBounds {
        self.bounds
    }

    /// Identifies the fragment without reproducing it. This is the only thing
    /// about a fragment a backend is expected to keep, which is why a secret in
    /// a fragment cannot reach durable state through a receipt.
    pub fn source_digest(&self) -> ArtifactDigest {
        digest_of_fields(&["kernel-source", &self.source])
    }

    pub fn validate(&self) -> Result<(), KernelError> {
        if self.source.is_empty() {
            return Err(validation("a submission carries no source"));
        }
        if self.source.len() > MAX_KERNEL_SOURCE_BYTES {
            return Err(validation(format!(
                "submission source exceeds {MAX_KERNEL_SOURCE_BYTES} bytes"
            )));
        }
        if self.source.contains('\0') {
            return Err(validation("submission source contains a NUL byte"));
        }
        self.bounds.validate()
    }
}

/// How one execution inside a session ended.
///
/// Deliberately narrower than [`crate::ExitDisposition`]: a kernel execution
/// does not exit a process, so `Signalled` and `FailedToStart` have no meaning
/// here, and [`Self::KernelDied`] is a case [`crate::ExitDisposition`] has no
/// name for.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelExecutionDisposition {
    /// The fragment ran to completion without raising.
    Completed,
    /// The fragment raised. `error_class` is a bounded, non-secret label the
    /// kernel reported; the detail is in the captured stderr artifact.
    Raised { error_class: KernelLabel },
    /// A caller asked for this execution to stop and the session survived.
    Cancelled,
    /// The declared deadline elapsed and the session survived.
    TimedOut,
    /// The kernel process ended while this execution was in flight. The work
    /// may have completed, partly completed or not started.
    ///
    /// A backend that can only cancel by killing the kernel settles a cancel
    /// here rather than as [`Self::Cancelled`]: the disposition has to say what
    /// actually happened.
    KernelDied,
    /// The execution was in flight when its owner died and the kernel is gone.
    Interrupted,
}

impl KernelExecutionDisposition {
    /// Success is exactly one thing.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed)
    }

    /// The stable token used in receipt digests and durable rows.
    pub fn as_token(&self) -> String {
        match self {
            Self::Completed => "completed".into(),
            Self::Raised { error_class } => format!("raised:{error_class}"),
            Self::Cancelled => "cancelled".into(),
            Self::TimedOut => "timed_out".into(),
            Self::KernelDied => "kernel_died".into(),
            Self::Interrupted => "interrupted".into(),
        }
    }

    pub fn parse(value: &str) -> Result<Self, KernelError> {
        if let Some(class) = value.strip_prefix("raised:") {
            return KernelLabel::new(class)
                .map(|error_class| Self::Raised { error_class })
                .map_err(|_| corrupt("stored kernel disposition has an undecodable error class"));
        }
        match value {
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            "kernel_died" => Ok(Self::KernelDied),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(corrupt(format!("unknown kernel disposition {other:?}"))),
        }
    }
}

/// How a whole session incarnation ended.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelDisposition {
    /// A caller closed it.
    Closed,
    /// The kernel process exited on its own.
    Exited { code: i32 },
    /// The idle deadline elapsed.
    IdleExpired,
    /// A declared session ceiling was reached.
    CeilingReached { ceiling: KernelLabel },
    /// The session was live when its owner died, and the process is gone.
    Interrupted,
    /// No kernel process was ever created.
    FailedToStart,
}

impl KernelDisposition {
    pub fn as_token(&self) -> String {
        match self {
            Self::Closed => "closed".into(),
            Self::Exited { code } => format!("exited:{code}"),
            Self::IdleExpired => "idle_expired".into(),
            Self::CeilingReached { ceiling } => format!("ceiling_reached:{ceiling}"),
            Self::Interrupted => "interrupted".into(),
            Self::FailedToStart => "failed_to_start".into(),
        }
    }

    pub fn parse(value: &str) -> Result<Self, KernelError> {
        if let Some(code) = value.strip_prefix("exited:") {
            return code
                .parse()
                .map(|code| Self::Exited { code })
                .map_err(|_| corrupt("stored session disposition has an undecodable status code"));
        }
        if let Some(ceiling) = value.strip_prefix("ceiling_reached:") {
            return KernelLabel::new(ceiling)
                .map(|ceiling| Self::CeilingReached { ceiling })
                .map_err(|_| corrupt("stored session disposition has an undecodable ceiling"));
        }
        match value {
            "closed" => Ok(Self::Closed),
            "idle_expired" => Ok(Self::IdleExpired),
            "interrupted" => Ok(Self::Interrupted),
            "failed_to_start" => Ok(Self::FailedToStart),
            other => Err(corrupt(format!("unknown session disposition {other:?}"))),
        }
    }
}

/// A category of state a checkpoint claims to carry.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestorableFact {
    /// Named top-level bindings, counted, not enumerated by value.
    Bindings { count: u64 },
    /// Loaded modules or packages.
    Modules { count: u64 },
    /// A Host-defined category the kernel image documents.
    Declared { kind: KernelLabel, count: u64 },
}

impl RestorableFact {
    pub fn count(&self) -> u64 {
        match self {
            Self::Bindings { count } | Self::Modules { count } | Self::Declared { count, .. } => {
                *count
            }
        }
    }

    pub fn as_token(&self) -> String {
        match self {
            Self::Bindings { count } => format!("bindings:{count}"),
            Self::Modules { count } => format!("modules:{count}"),
            Self::Declared { kind, count } => format!("declared:{kind}:{count}"),
        }
    }

    fn validate(&self) -> Result<(), KernelError> {
        if self.count() == 0 {
            return Err(validation(
                "a restorable declaration that carries nothing is not a declaration",
            ));
        }
        Ok(())
    }
}

/// A category of state a checkpoint declares is lost across restore.
///
/// This is the whole of the third rule. It is a typed value that travels with
/// the checkpoint and comes back out of [`KernelRuntime::restore`], so a Host
/// cannot consume a restored session without being handed the list of things
/// that did not come back.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NonRestorableFact {
    /// Open file descriptors held by the kernel.
    OpenFile { count: u64 },
    /// Live network connections.
    NetworkConnection { count: u64 },
    /// Child processes the kernel spawned.
    ChildProcess { count: u64 },
    /// Threads, coroutines or tasks that were running.
    ConcurrentTask { count: u64 },
    /// Handles onto external systems the kernel cannot serialise.
    ForeignHandle { kind: KernelLabel, count: u64 },
    /// Values whose type the kernel could not serialise.
    UnserialisableValue { kind: KernelLabel, count: u64 },
    /// Filesystem mutations made outside the working root, which a restore does
    /// not and must not undo.
    ExternalMutation { kind: KernelLabel },
}

impl NonRestorableFact {
    pub fn count(&self) -> u64 {
        match self {
            Self::OpenFile { count }
            | Self::NetworkConnection { count }
            | Self::ChildProcess { count }
            | Self::ConcurrentTask { count }
            | Self::ForeignHandle { count, .. }
            | Self::UnserialisableValue { count, .. } => *count,
            Self::ExternalMutation { .. } => 1,
        }
    }

    pub fn as_token(&self) -> String {
        match self {
            Self::OpenFile { count } => format!("open_file:{count}"),
            Self::NetworkConnection { count } => format!("network_connection:{count}"),
            Self::ChildProcess { count } => format!("child_process:{count}"),
            Self::ConcurrentTask { count } => format!("concurrent_task:{count}"),
            Self::ForeignHandle { kind, count } => format!("foreign_handle:{kind}:{count}"),
            Self::UnserialisableValue { kind, count } => {
                format!("unserialisable_value:{kind}:{count}")
            }
            Self::ExternalMutation { kind } => format!("external_mutation:{kind}"),
        }
    }

    fn validate(&self) -> Result<(), KernelError> {
        if self.count() == 0 {
            return Err(validation(
                "a non-restorable declaration that names nothing is not a declaration",
            ));
        }
        Ok(())
    }
}

/// A named, artifact-addressed snapshot of session state.
///
/// A checkpoint is *evidence*. It records what the kernel was able to
/// serialise, what it deliberately did not, and the incarnation it came from.
/// Nothing in the Host may treat it as the authority for a fact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelCheckpointRef {
    /// Content-addressed snapshot payload.
    pub artifact: ArtifactHandle,
    pub session: KernelSessionId,
    pub generation: KernelGeneration,
    /// The execution sequence this checkpoint was taken after; zero when it was
    /// taken before the incarnation ran anything.
    pub after_sequence: u64,
    /// The kernel image the snapshot was produced by. A restore into a
    /// different image is refused rather than attempted.
    pub spec_digest: ArtifactDigest,
    pub taken_at_ms: u64,
    /// What the kernel claims it captured, as typed declarations.
    pub restorable: Vec<RestorableFact>,
    /// What the kernel declares it could not capture.
    pub non_restorable: Vec<NonRestorableFact>,
}

impl KernelCheckpointRef {
    /// Re-checks a checkpoint decoded from storage.
    ///
    /// In particular it refuses a checkpoint that declares neither restorable
    /// nor non-restorable facts, because a snapshot that claims nothing is a
    /// snapshot whose losses were never enumerated.
    pub fn validate(&self) -> Result<(), KernelError> {
        self.generation.validate()?;
        if self.restorable.is_empty() && self.non_restorable.is_empty() {
            return Err(corrupt(
                "a checkpoint that declares neither restorable nor non-restorable state never enumerated its losses",
            ));
        }
        if self.restorable.len() > MAX_KERNEL_RESTORABLE_FACTS {
            return Err(corrupt(format!(
                "a checkpoint declares more than {MAX_KERNEL_RESTORABLE_FACTS} restorable facts"
            )));
        }
        if self.non_restorable.len() > MAX_KERNEL_NON_RESTORABLE_FACTS {
            return Err(corrupt(format!(
                "a checkpoint declares more than {MAX_KERNEL_NON_RESTORABLE_FACTS} non-restorable facts"
            )));
        }
        for fact in &self.restorable {
            fact.validate()
                .map_err(|error| corrupt(error.to_string()))?;
        }
        for fact in &self.non_restorable {
            fact.validate()
                .map_err(|error| corrupt(error.to_string()))?;
        }
        Ok(())
    }

    /// The identity of this checkpoint's declaration, which a Host persists
    /// beside it and recomputes on read.
    pub fn digest(&self) -> ArtifactDigest {
        let mut fields: Vec<String> = vec![
            "kernel-checkpoint".into(),
            self.artifact.id().as_str().to_owned(),
            self.artifact.digest().as_str().to_owned(),
            self.session.as_str().to_owned(),
            self.generation.get().to_string(),
            self.after_sequence.to_string(),
            self.spec_digest.as_str().to_owned(),
            self.taken_at_ms.to_string(),
            self.restorable.len().to_string(),
        ];
        fields.extend(self.restorable.iter().map(RestorableFact::as_token));
        fields.push(self.non_restorable.len().to_string());
        fields.extend(self.non_restorable.iter().map(NonRestorableFact::as_token));
        let borrowed: Vec<&str> = fields.iter().map(String::as_str).collect();
        digest_of_fields(&borrowed)
    }

    /// The reference the Run reducer's dispatch seam wants, given the size the
    /// Host stored and the producing Run.
    ///
    /// This is the conversion [`crate::PersistentKernelDriver`] is typed in:
    /// the checkpoint's artifact identity is the same identity a
    /// [`crate::run::ArtifactRef`] carries, so a Host does not re-address the
    /// bytes to cross the seam.
    pub fn as_artifact_ref(
        &self,
        size: u64,
        producer_run: &str,
    ) -> Result<crate::run::ArtifactRef, KernelError> {
        let reference = crate::run::ArtifactRef::new(
            self.artifact.digest().as_str(),
            "application/octet-stream",
            size,
            "kernel_checkpoint",
            producer_run,
        );
        reference
            .validate()
            .map_err(|error| validation(error.to_string()))?;
        Ok(reference)
    }
}

/// What a restore produced.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KernelRestore {
    /// A new incarnation is live. `lost` is the checkpoint's non-restorable
    /// declaration, handed back so the calling Host must receive it in order to
    /// receive the session at all. There is no accessor that yields a live
    /// session without it.
    Restored {
        session: KernelSessionId,
        generation: KernelGeneration,
        lost: Vec<NonRestorableFact>,
    },
    /// The checkpoint is well-formed but was produced by a different kernel
    /// image. Nothing was started.
    SpecMismatch {
        expected: ArtifactDigest,
        found: ArtifactDigest,
    },
    /// The kernel image refused the snapshot. Nothing was started; the Host
    /// must reconstruct from durable inputs instead.
    Rejected { reason: KernelLabel },
}

/// The durable, digest-verified account of one settled kernel execution.
/// Written once, never edited.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelExecutionReceipt {
    pub key: KernelExecutionKey,
    /// Dense position of this execution in its incarnation, from 1.
    pub sequence: u64,
    pub source_digest: ArtifactDigest,
    pub spec_digest: ArtifactDigest,
    pub bounds: KernelExecutionBounds,
    pub disposition: KernelExecutionDisposition,
    /// The wall instant the caller declared at submit.
    pub started_at_ms: u64,
    /// `started_at_ms` plus the elapsed time the backend measured.
    pub settled_at_ms: u64,
    pub stdout: Option<CaptureRecord>,
    pub stderr: Option<CaptureRecord>,
    /// The checkpoint taken immediately after this execution, when one was
    /// requested. Evidence only.
    pub checkpoint: Option<KernelCheckpointRef>,
}

impl KernelExecutionReceipt {
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
    pub fn digest(&self) -> ArtifactDigest {
        let mut fields: Vec<String> = vec![
            "kernel-execution".into(),
            self.key.session().as_str().to_owned(),
            self.key.generation().get().to_string(),
            self.key.execution().as_str().to_owned(),
            self.sequence.to_string(),
            self.source_digest.as_str().to_owned(),
            self.spec_digest.as_str().to_owned(),
            self.bounds.deadline_ms.to_string(),
            self.bounds.stdout_capture_bytes.to_string(),
            self.bounds.stderr_capture_bytes.to_string(),
            self.disposition.as_token(),
            self.started_at_ms.to_string(),
            self.settled_at_ms.to_string(),
        ];
        for capture in [self.stdout.as_ref(), self.stderr.as_ref()] {
            fields.push(
                capture
                    .map(capture_field)
                    .unwrap_or_else(|| "absent".into()),
            );
        }
        fields.push(
            self.checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.digest().as_str().to_owned())
                .unwrap_or_else(|| "absent".into()),
        );
        let borrowed: Vec<&str> = fields.iter().map(String::as_str).collect();
        digest_of_fields(&borrowed)
    }

    /// Re-checks a receipt decoded from storage against the digest stored with
    /// it, and against its own internal consistency.
    pub fn verify(&self, expected: &ArtifactDigest) -> Result<(), KernelError> {
        self.key
            .validate()
            .map_err(|error| corrupt(error.to_string()))?;
        self.bounds
            .validate()
            .map_err(|error| corrupt(error.to_string()))?;
        if self.sequence == 0 {
            return Err(corrupt("a kernel receipt has no position in its session"));
        }
        if self.settled_at_ms < self.started_at_ms {
            return Err(corrupt("a kernel receipt settles before it starts"));
        }
        for capture in [self.stdout.as_ref(), self.stderr.as_ref()]
            .into_iter()
            .flatten()
        {
            validate_capture(capture)?;
            if capture.declared_bound != self.bounds.capture_bytes(capture.stream) {
                return Err(corrupt(
                    "a capture record cites a bound the submission did not declare",
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
                "a kernel receipt binds a capture record to the wrong stream",
            ));
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
            if checkpoint.session != *self.key.session()
                || checkpoint.generation != self.key.generation()
            {
                return Err(corrupt(
                    "a kernel receipt cites a checkpoint from another incarnation",
                ));
            }
        }
        if &self.digest() != expected {
            return Err(corrupt(
                "a stored kernel receipt does not address to its own digest",
            ));
        }
        Ok(())
    }
}

fn capture_field(capture: &CaptureRecord) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        capture.stream.as_str(),
        capture.artifact.id(),
        capture.captured_bytes,
        capture.produced_bytes,
        capture.declared_bound,
        capture.truncated
    )
}

pub(crate) fn validate_capture(capture: &CaptureRecord) -> Result<(), KernelError> {
    if capture.captured_bytes > capture.produced_bytes {
        return Err(corrupt(
            "a capture record kept more bytes than were written",
        ));
    }
    if capture.captured_bytes > capture.declared_bound {
        return Err(corrupt("a capture record exceeds its own declared bound"));
    }
    if capture.truncated != (capture.produced_bytes > capture.captured_bytes) {
        return Err(corrupt(
            "a capture record's truncation flag disagrees with its own counts",
        ));
    }
    Ok(())
}

/// The durable, digest-verified account of one settled session incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelSessionReceipt {
    pub session: KernelSessionId,
    pub generation: KernelGeneration,
    pub spec_digest: ArtifactDigest,
    pub disposition: KernelDisposition,
    /// The wall instant the caller declared when the incarnation started.
    pub opened_at_ms: u64,
    pub settled_at_ms: u64,
    /// Executions this incarnation accepted, settled or not.
    pub executions: u64,
    /// Bytes this incarnation's executions captured in total.
    pub captured_bytes: u64,
}

impl KernelSessionReceipt {
    pub fn duration_ms(&self) -> u64 {
        self.settled_at_ms.saturating_sub(self.opened_at_ms)
    }

    pub fn digest(&self) -> ArtifactDigest {
        let fields = [
            "kernel-session".to_owned(),
            self.session.as_str().to_owned(),
            self.generation.get().to_string(),
            self.spec_digest.as_str().to_owned(),
            self.disposition.as_token(),
            self.opened_at_ms.to_string(),
            self.settled_at_ms.to_string(),
            self.executions.to_string(),
            self.captured_bytes.to_string(),
        ];
        let borrowed: Vec<&str> = fields.iter().map(String::as_str).collect();
        digest_of_fields(&borrowed)
    }

    pub fn verify(&self, expected: &ArtifactDigest) -> Result<(), KernelError> {
        self.generation
            .validate()
            .map_err(|error| corrupt(error.to_string()))?;
        if self.settled_at_ms < self.opened_at_ms {
            return Err(corrupt("a session receipt settles before it opens"));
        }
        if self.executions > MAX_KERNEL_SESSION_EXECUTIONS {
            return Err(corrupt(
                "a session receipt counts more executions than the contract allows",
            ));
        }
        if &self.digest() != expected {
            return Err(corrupt(
                "a stored session receipt does not address to its own digest",
            ));
        }
        Ok(())
    }
}

/// What the authority currently knows about one session.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KernelSessionStatus {
    /// Opened or restored by this handle and not yet settled.
    Live {
        generation: KernelGeneration,
        process: ProcessIdentity,
        opened_at_ms: u64,
        executions: u64,
    },
    /// Durably recorded as live, but not by this handle. Nothing about its fate
    /// is known until it is reconciled, and it never decays into success.
    Uncertain {
        generation: KernelGeneration,
        process: ProcessIdentity,
        opened_at_ms: u64,
        executions: u64,
    },
    /// Settled, with the append-only receipt that says how.
    Settled(Box<KernelSessionReceipt>),
}

impl KernelSessionStatus {
    pub fn receipt(&self) -> Option<&KernelSessionReceipt> {
        match self {
            Self::Settled(receipt) => Some(receipt),
            _ => None,
        }
    }

    pub fn generation(&self) -> KernelGeneration {
        match self {
            Self::Live { generation, .. } | Self::Uncertain { generation, .. } => *generation,
            Self::Settled(receipt) => receipt.generation,
        }
    }

    pub fn is_settled(&self) -> bool {
        matches!(self, Self::Settled(_))
    }
}

/// What the authority currently knows about one execution.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KernelExecutionStatus {
    /// Submitted by this handle and not yet settled.
    InFlight { sequence: u64, started_at_ms: u64 },
    /// Durably recorded as in flight, but not by this handle.
    Uncertain { sequence: u64, started_at_ms: u64 },
    /// Settled, with the append-only receipt that says how.
    Settled(Box<KernelExecutionReceipt>),
}

impl KernelExecutionStatus {
    pub fn receipt(&self) -> Option<&KernelExecutionReceipt> {
        match self {
            Self::Settled(receipt) => Some(receipt),
            _ => None,
        }
    }

    pub fn is_settled(&self) -> bool {
        matches!(self, Self::Settled(_))
    }
}

/// What a session reconciliation established.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KernelReconcileOutcome {
    /// The kernel process is still alive. Nothing was settled; ask again later.
    StillLive,
    /// The session now has a receipt — either one it already had, or an honest
    /// [`KernelDisposition::Interrupted`] settlement for an orphan. Every
    /// execution that was in flight settled with it, in the same transaction.
    Settled {
        session: Box<KernelSessionReceipt>,
        executions: Vec<KernelExecutionReceipt>,
    },
    /// The probe could not establish the process's fate. The session stays
    /// [`KernelSessionStatus::Uncertain`].
    Uncertain,
}

/// Durable custody of persistent kernel sessions.
///
/// Implementations own process creation, transactions and physical layout. They
/// persist and fail-closed verify a schema marker and version, refuse stored
/// state they cannot decode within the published bounds, and make every method
/// atomic against every other handle to the same authority — including handles
/// in other processes.
///
/// Implementations own a monotonic duration source and no wall clock. Every
/// wall instant that reaches a receipt is derived from a `now_ms` a caller
/// declared.
///
/// No method takes a credential resolver, and no type in this module can carry
/// secret material. That is the structural half of the rule that a kernel
/// process receives no provider credential and no relay bearer; a kernel that
/// needs a network capability gets it over MCP, from a process that already has
/// a credential boundary, which is a Host decision and appears nowhere here.
pub trait KernelRuntime: Send + Sync + 'static {
    /// Validates the spec, starts the kernel and durably records the session as
    /// live before returning.
    ///
    /// A session identity that is already known — live or settled — is
    /// [`KernelError::Conflict`], never a second process.
    fn open(
        &self,
        session: &KernelSessionId,
        spec: &KernelSpec,
        now_ms: u64,
    ) -> Result<KernelGeneration, KernelError>;

    /// Submits one fragment and durably records it as in flight before
    /// returning.
    ///
    /// A submission whose execution identity is already known in this
    /// incarnation is [`KernelError::Conflict`], so a retried submit after an
    /// unknown outcome cannot double-execute. A session that already has an
    /// execution in flight is [`KernelError::Conflict`] too: one incarnation
    /// runs one fragment at a time, which is what makes
    /// [`KernelExecutionReceipt::sequence`] mean the order state was mutated
    /// in.
    ///
    /// The sink is taken by `Arc` rather than by reference because settlement
    /// happens in [`Self::wait`], after this call has returned: a backend must
    /// be able to keep the sink it was told to use for this execution.
    fn submit(
        &self,
        key: &KernelExecutionKey,
        submission: &KernelSubmission,
        sink: &Arc<dyn ProgramOutputSink>,
        now_ms: u64,
    ) -> Result<(), KernelError>;

    /// Blocks until the execution settles and answers its receipt.
    ///
    /// The declared deadline is enforced here. Waiting on an already-settled
    /// execution replays its stored receipt unchanged. Waiting on an execution
    /// this handle did not submit is [`KernelError::Unowned`]; that is a
    /// reconciliation, not a wait.
    fn wait(&self, key: &KernelExecutionKey) -> Result<KernelExecutionReceipt, KernelError>;

    /// Asks one in-flight execution to stop, leaving the session live.
    ///
    /// Idempotent, and never rewrites a settlement that already happened. A
    /// backend whose kernel cannot abandon a fragment without dying settles the
    /// execution as [`KernelExecutionDisposition::KernelDied`] rather than as
    /// [`KernelExecutionDisposition::Cancelled`].
    fn cancel(&self, key: &KernelExecutionKey) -> Result<(), KernelError>;

    /// Ends the session, settling it and every in-flight execution together.
    fn close(
        &self,
        session: &KernelSessionId,
        now_ms: u64,
    ) -> Result<KernelSessionReceipt, KernelError>;

    /// Takes an evidence snapshot of a live session.
    fn checkpoint(
        &self,
        session: &KernelSessionId,
        sink: &Arc<dyn ProgramOutputSink>,
        now_ms: u64,
    ) -> Result<KernelCheckpointRef, KernelError>;

    /// Starts a new incarnation from a checkpoint. The new incarnation is
    /// recorded durably before this returns, exactly as [`Self::open`] does.
    ///
    /// There is no partial restore: a per-value failure is a
    /// [`NonRestorableFact::UnserialisableValue`] in the checkpoint's own
    /// declaration, which is where it belongs.
    fn restore(
        &self,
        session: &KernelSessionId,
        checkpoint: &KernelCheckpointRef,
        spec: &KernelSpec,
        now_ms: u64,
    ) -> Result<KernelRestore, KernelError>;

    /// What is known about one session, or `None` for an identity this
    /// authority has never seen. Durable state that cannot be decoded within
    /// the published bounds is [`KernelError::Corrupt`], never `None`.
    fn inspect_session(
        &self,
        session: &KernelSessionId,
    ) -> Result<Option<KernelSessionStatus>, KernelError>;

    fn inspect_execution(
        &self,
        key: &KernelExecutionKey,
    ) -> Result<Option<KernelExecutionStatus>, KernelError>;

    /// Sessions this authority durably believes are live but that this handle
    /// does not own, in identity order. After a restart this is exactly the
    /// crash-time backlog.
    fn requiring_reconciliation(&self) -> Result<Vec<KernelSessionId>, KernelError>;

    /// Resolves an uncertain session using the caller's liveness evidence.
    ///
    /// A live kernel answers [`KernelReconcileOutcome::StillLive`] and settles
    /// nothing. A gone kernel settles the session as
    /// [`KernelDisposition::Interrupted`] and every in-flight execution as
    /// [`KernelExecutionDisposition::Interrupted`] in the same transaction: a
    /// session receipt that settled while an execution receipt was still
    /// missing would let a Host conclude a fragment succeeded because nothing
    /// said otherwise. An inconclusive probe leaves everything uncertain.
    fn reconcile(
        &self,
        session: &KernelSessionId,
        liveness: &dyn LivenessProbe,
        now_ms: u64,
    ) -> Result<KernelReconcileOutcome, KernelError>;

    /// The receipt of a settled session, or `None` while it is live.
    fn session_receipt(
        &self,
        session: &KernelSessionId,
    ) -> Result<Option<KernelSessionReceipt>, KernelError> {
        Ok(self
            .inspect_session(session)?
            .and_then(|status| status.receipt().cloned()))
    }

    /// The receipt of a settled execution, or `None` while it is in flight.
    fn execution_receipt(
        &self,
        key: &KernelExecutionKey,
    ) -> Result<Option<KernelExecutionReceipt>, KernelError> {
        Ok(self
            .inspect_execution(key)?
            .and_then(|status| status.receipt().cloned()))
    }
}
