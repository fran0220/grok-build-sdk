// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Peer conversations: where a Turn's input came from, and the three
//! host-delegate-backed tools an agent uses to talk to another conversation.
//!
//! A conversation is an ordinary durable host conversation, not a subagent and
//! not a child of the caller. Nothing here couples two conversations'
//! lifecycles, waits for one another, or reduces a mailbox: an agent creates a
//! conversation, reads a bounded distillate of one, and sends one message to
//! one. Replies and completion notices are ordinary reverse sends by
//! convention.
//!
//! The SDK owns the contract — identity validation, argument and result
//! bounds, the declared tool surface, and the fail-closed checks that a
//! delegate's answer is about the conversation and delivery key that were
//! asked about. The Host owns every fact: which conversations exist, what a
//! project is, how a message is queued or started, and how a transcript is
//! distilled. The SDK never stores a conversation or reads a transcript.

use crate::*;

mod tools;

pub use tools::{
    CONVERSATION_TOOL_SERVER, ConversationToolCall, ConversationToolDescriptor,
    ConversationToolOutcome, conversation_tool_descriptors,
};
pub(crate) use tools::{conversation_tool_server, dispatch_tool_call};

/// Maximum byte length of a host conversation identifier.
pub const MAX_CONVERSATION_ID_BYTES: usize = 128;
/// Maximum byte length of a conversation display label.
pub const MAX_CONVERSATION_LABEL_BYTES: usize = 128;
/// Maximum byte length of a project (workspace) name.
pub const MAX_PROJECT_NAME_BYTES: usize = 128;
/// Maximum byte length of a requested conversation title.
pub const MAX_CONVERSATION_TITLE_BYTES: usize = 256;
/// Maximum byte length of one peer message body.
pub const MAX_PEER_MESSAGE_BYTES: usize = 16 * 1024;
/// Maximum byte length of a delivery idempotency key.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
/// Maximum byte length of a read digest, whatever the caller declared.
pub const MAX_CONVERSATION_DIGEST_BYTES: usize = 16 * 1024;
/// Maximum byte length of the free-text focus a read may state.
pub const MAX_DIGEST_FOCUS_BYTES: usize = 512;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConversationError {
    #[error(
        "conversation identifier '{0}' must be 1..={MAX_CONVERSATION_ID_BYTES} bytes of ASCII letters, digits, '-', '_', '.' or ':' and start with a letter or digit"
    )]
    InvalidConversationId(String),
    #[error(
        "project name '{0}' must be 1..={MAX_PROJECT_NAME_BYTES} bytes without control characters or surrounding whitespace"
    )]
    InvalidProjectName(String),
    #[error(
        "display label '{0}' must be 1..={MAX_CONVERSATION_LABEL_BYTES} bytes without control characters or surrounding whitespace"
    )]
    InvalidLabel(String),
    #[error(
        "conversation title '{0}' must be 1..={MAX_CONVERSATION_TITLE_BYTES} bytes without control characters or surrounding whitespace"
    )]
    InvalidTitle(String),
    #[error(
        "delivery key '{0}' must be 1..={MAX_IDEMPOTENCY_KEY_BYTES} bytes of ASCII letters, digits, '-', '_', '.' or ':' and start with a letter or digit"
    )]
    InvalidIdempotencyKey(String),
    #[error("a peer message must be 1..={MAX_PEER_MESSAGE_BYTES} bytes of non-blank text")]
    InvalidMessage,
    #[error(
        "a digest focus must be 1..={MAX_DIGEST_FOCUS_BYTES} bytes of non-blank text without control characters"
    )]
    InvalidFocus,
    #[error(
        "a read may declare 1..={MAX_CONVERSATION_DIGEST_BYTES} digest bytes; {requested} was requested"
    )]
    InvalidDigestBound { requested: u32 },
    #[error("the digest is {actual} bytes; the read declared at most {declared}")]
    DigestOverrun { declared: u32, actual: usize },
    #[error("the answer names conversation '{actual}'; '{expected}' was asked about")]
    ConversationMismatch {
        expected: ConversationId,
        actual: ConversationId,
    },
    #[error("the created conversation names project '{actual}'; '{expected}' was asked for")]
    ProjectMismatch {
        expected: ProjectName,
        actual: ProjectName,
    },
    #[error("the acceptance names delivery key '{actual}'; '{expected}' was sent")]
    IdempotencyKeyMismatch {
        expected: IdempotencyKey,
        actual: IdempotencyKey,
    },
    #[error("no conversation delegate is installed on this Runtime")]
    NoDelegate,
    #[error("the conversation delegate requires the Desktop profile")]
    RestrictedProfile,
    #[error("conversation '{0}' is unknown to the host")]
    UnknownConversation(ConversationId),
    #[error("the host could not answer: {0}")]
    Unavailable(String),
}

impl From<ConversationError> for Error {
    fn from(error: ConversationError) -> Self {
        match error {
            ConversationError::NoDelegate
            | ConversationError::RestrictedProfile
            | ConversationError::InvalidDigestBound { .. } => {
                Self::InvalidConfig(error.to_string())
            }
            other => Self::Operation(other.to_string()),
        }
    }
}

fn validate_opaque_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
}

fn validate_display_text(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value == value.trim()
        && !value.chars().any(char::is_control)
}

macro_rules! opaque_identifier {
    ($name:ident, $maximum:ident, $error:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Clone,
            Debug,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ConversationError> {
                let value = value.into();
                if validate_opaque_identifier(&value, $maximum) {
                    Ok(Self(value))
                } else {
                    Err(ConversationError::$error(value))
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = ConversationError;
            fn try_from(value: String) -> Result<Self, ConversationError> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

macro_rules! display_text {
    ($name:ident, $maximum:ident, $error:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Clone,
            Debug,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ConversationError> {
                let value = value.into();
                if validate_display_text(&value, $maximum) {
                    Ok(Self(value))
                } else {
                    Err(ConversationError::$error(value))
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = ConversationError;
            fn try_from(value: String) -> Result<Self, ConversationError> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

opaque_identifier!(
    ConversationId,
    MAX_CONVERSATION_ID_BYTES,
    InvalidConversationId,
    "An opaque Host-owned conversation identity. The SDK validates its shape and never interprets it."
);
opaque_identifier!(
    IdempotencyKey,
    MAX_IDEMPOTENCY_KEY_BYTES,
    InvalidIdempotencyKey,
    "A caller-chosen delivery key. Redelivering one key is the same delivery, never a second one."
);
display_text!(
    ProjectName,
    MAX_PROJECT_NAME_BYTES,
    InvalidProjectName,
    "A named Host project (workspace). The SDK carries the name and never resolves it to a path."
);
display_text!(
    ConversationLabel,
    MAX_CONVERSATION_LABEL_BYTES,
    InvalidLabel,
    "A human-facing label for a conversation, carried for display only."
);
display_text!(
    ConversationTitle,
    MAX_CONVERSATION_TITLE_BYTES,
    InvalidTitle,
    "A requested title for a conversation the Host is about to create."
);

/// One bounded peer message body. Interior newlines are kept; a blank body is
/// rejected, because an empty delivery is a bug rather than a message.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct PeerMessage(String);

impl PeerMessage {
    pub fn new(value: impl Into<String>) -> Result<Self, ConversationError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > MAX_PEER_MESSAGE_BYTES {
            return Err(ConversationError::InvalidMessage);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PeerMessage {
    type Error = ConversationError;
    fn try_from(value: String) -> Result<Self, ConversationError> {
        Self::new(value)
    }
}

impl From<PeerMessage> for String {
    fn from(value: PeerMessage) -> Self {
        value.0
    }
}

/// The bounded free-text steer a read may state. It is a hint to the Host's
/// distillation, never a query language the SDK interprets.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct DigestFocus(String);

impl DigestFocus {
    pub fn new(value: impl Into<String>) -> Result<Self, ConversationError> {
        let value = value.into();
        if value.trim().is_empty()
            || value.len() > MAX_DIGEST_FOCUS_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(ConversationError::InvalidFocus);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for DigestFocus {
    type Error = ConversationError;
    fn try_from(value: String) -> Result<Self, ConversationError> {
        Self::new(value)
    }
}

impl From<DigestFocus> for String {
    fn from(value: DigestFocus) -> Self {
        value.0
    }
}

/// Where a Turn's prompt came from.
///
/// This is additive: a prompt with no stated source is from the user, exactly
/// as before. It is deliberately *not* forward-tolerant — a source this build
/// does not know is a decode failure rather than a message silently attributed
/// to the user, because attribution is the whole point of the field.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputSource {
    /// A person typed or dictated this prompt into the conversation.
    #[default]
    User,
    /// Another conversation sent this prompt. The identity is the originating
    /// conversation; the label is display text and never an identity.
    Peer {
        conversation: ConversationId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<ConversationLabel>,
    },
}

impl InputSource {
    /// The originating conversation, or `None` for a user prompt.
    pub fn peer_conversation(&self) -> Option<&ConversationId> {
        match self {
            Self::User => None,
            Self::Peer { conversation, .. } => Some(conversation),
        }
    }

    pub fn label(&self) -> Option<&ConversationLabel> {
        match self {
            Self::User => None,
            Self::Peer { label, .. } => label.as_ref(),
        }
    }

    /// Used to keep a user Turn's durable ledger entry byte-identical to the
    /// entries written before this contract existed.
    pub fn is_user(&self) -> bool {
        matches!(self, Self::User)
    }
}

/// What the agent asked the Host to create.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationCreate {
    pub project: ProjectName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<ConversationTitle>,
}

impl ConversationCreate {
    pub fn new(project: ProjectName) -> Self {
        Self {
            project,
            title: None,
        }
    }

    pub fn title(mut self, value: ConversationTitle) -> Self {
        self.title = Some(value);
        self
    }
}

/// The conversation the Host created, in its own words.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationIdentity {
    pub conversation: ConversationId,
    pub project: ProjectName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<ConversationLabel>,
}

/// What the agent asked to have distilled. The raw transcript answering this
/// read never enters the calling Session's context; only the digest does.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationRead {
    pub conversation: ConversationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus: Option<DigestFocus>,
    /// The digest ceiling this read declares. It is validated against
    /// [`MAX_CONVERSATION_DIGEST_BYTES`] before the delegate is asked.
    #[serde(default = "default_digest_bytes")]
    pub max_digest_bytes: u32,
}

fn default_digest_bytes() -> u32 {
    MAX_CONVERSATION_DIGEST_BYTES as u32
}

impl ConversationRead {
    pub fn new(conversation: ConversationId) -> Self {
        Self {
            conversation,
            focus: None,
            max_digest_bytes: default_digest_bytes(),
        }
    }

    pub fn focus(mut self, value: DigestFocus) -> Self {
        self.focus = Some(value);
        self
    }

    pub fn max_digest_bytes(mut self, value: u32) -> Self {
        self.max_digest_bytes = value;
        self
    }

    pub fn validate(&self) -> Result<(), ConversationError> {
        if self.max_digest_bytes == 0
            || self.max_digest_bytes as usize > MAX_CONVERSATION_DIGEST_BYTES
        {
            return Err(ConversationError::InvalidDigestBound {
                requested: self.max_digest_bytes,
            });
        }
        Ok(())
    }
}

/// The Host's bounded distillate of a conversation. Truncation is a recorded
/// fact rather than a silently shortened answer.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationDigest {
    pub conversation: ConversationId,
    pub distillate: String,
    /// True when the Host had more to say than the declared bound allowed.
    pub truncated: bool,
    /// How many host-owned transcript messages the distillate covers.
    pub covered_messages: u32,
}

impl ConversationDigest {
    pub fn new(
        conversation: ConversationId,
        distillate: impl Into<String>,
        truncated: bool,
        covered_messages: u32,
    ) -> Self {
        Self {
            conversation,
            distillate: distillate.into(),
            truncated,
            covered_messages,
        }
    }
}

/// One peer delivery. `idempotency_key` makes redelivery safe: the same key to
/// the same conversation is the same delivery, whatever the transport did.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationSend {
    pub conversation: ConversationId,
    pub message: PeerMessage,
    pub idempotency_key: IdempotencyKey,
    /// Display label for the *sending* conversation, carried to the target's
    /// input source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<ConversationLabel>,
}

impl ConversationSend {
    pub fn new(
        conversation: ConversationId,
        message: PeerMessage,
        idempotency_key: IdempotencyKey,
    ) -> Self {
        Self {
            conversation,
            message,
            idempotency_key,
            label: None,
        }
    }

    pub fn label(mut self, value: ConversationLabel) -> Self {
        self.label = Some(value);
        self
    }
}

/// What the one send queue did with a delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationDelivery {
    /// The target was idle and this delivery started a Turn.
    StartedTurn,
    /// The target was running; this delivery is queued to Turn settlement.
    Queued,
    /// This delivery key was already accepted. Nothing was delivered twice.
    AlreadyAccepted,
}

/// The Host's acceptance of one delivery.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationAcceptance {
    pub conversation: ConversationId,
    pub delivery: ConversationDelivery,
    pub idempotency_key: IdempotencyKey,
}

impl ConversationAcceptance {
    pub fn new(
        conversation: ConversationId,
        delivery: ConversationDelivery,
        idempotency_key: IdempotencyKey,
    ) -> Self {
        Self {
            conversation,
            delivery,
            idempotency_key,
        }
    }

    /// True when this call, rather than an earlier one with the same key,
    /// admitted the message.
    pub fn is_first_delivery(&self) -> bool {
        !matches!(self.delivery, ConversationDelivery::AlreadyAccepted)
    }
}

/// The Host authority behind the three conversation tools.
///
/// The SDK declares the tools, validates their arguments and results, and
/// dispatches here. It never creates, stores, reads or queues a conversation
/// itself, and it never sees a raw transcript. `caller` is the Session that ran
/// the tool; the Host maps it to its own conversation identity.
#[async_trait::async_trait]
pub trait ConversationDelegate: Send + Sync + 'static {
    /// Runs the same host command path as user conversation creation in the
    /// named project, and answers with the created identity.
    async fn create(
        &self,
        caller: &SessionId,
        request: ConversationCreate,
    ) -> Result<ConversationIdentity, ConversationError>;

    /// Distills the target's host-owned transcript into a bounded digest. The
    /// distillation is entirely host-side.
    async fn read(
        &self,
        caller: &SessionId,
        request: ConversationRead,
    ) -> Result<ConversationDigest, ConversationError>;

    /// Delivers one peer message through the one send queue.
    async fn send(
        &self,
        caller: &SessionId,
        request: ConversationSend,
    ) -> Result<ConversationAcceptance, ConversationError>;
}

/// Validates a request, dispatches it, and checks that the answer is about the
/// thing that was asked about. A delegate cannot silently retarget a create, a
/// read, or a delivery.
pub(crate) async fn dispatch_create(
    delegate: &Arc<dyn ConversationDelegate>,
    caller: &SessionId,
    request: ConversationCreate,
) -> Result<ConversationIdentity, ConversationError> {
    let expected = request.project.clone();
    let identity = delegate.create(caller, request).await?;
    if identity.project != expected {
        return Err(ConversationError::ProjectMismatch {
            expected,
            actual: identity.project,
        });
    }
    Ok(identity)
}

pub(crate) async fn dispatch_read(
    delegate: &Arc<dyn ConversationDelegate>,
    caller: &SessionId,
    request: ConversationRead,
) -> Result<ConversationDigest, ConversationError> {
    request.validate()?;
    let expected = request.conversation.clone();
    let declared = request.max_digest_bytes;
    let digest = delegate.read(caller, request).await?;
    if digest.conversation != expected {
        return Err(ConversationError::ConversationMismatch {
            expected,
            actual: digest.conversation,
        });
    }
    if digest.distillate.len() > declared as usize {
        return Err(ConversationError::DigestOverrun {
            declared,
            actual: digest.distillate.len(),
        });
    }
    Ok(digest)
}

pub(crate) async fn dispatch_send(
    delegate: &Arc<dyn ConversationDelegate>,
    caller: &SessionId,
    request: ConversationSend,
) -> Result<ConversationAcceptance, ConversationError> {
    let expected_conversation = request.conversation.clone();
    let expected_key = request.idempotency_key.clone();
    let acceptance = delegate.send(caller, request).await?;
    if acceptance.conversation != expected_conversation {
        return Err(ConversationError::ConversationMismatch {
            expected: expected_conversation,
            actual: acceptance.conversation,
        });
    }
    if acceptance.idempotency_key != expected_key {
        return Err(ConversationError::IdempotencyKeyMismatch {
            expected: expected_key,
            actual: acceptance.idempotency_key,
        });
    }
    Ok(acceptance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validated_newtypes_reject_shapes_the_host_could_not_have_issued() {
        assert!(ConversationId::new("thread-01HZ.9:a_b").is_ok());
        for rejected in ["", "-leading", "has space", "new\nline"] {
            assert!(matches!(
                ConversationId::new(rejected),
                Err(ConversationError::InvalidConversationId(_))
            ));
        }
        assert!(matches!(
            ConversationId::new("x".repeat(MAX_CONVERSATION_ID_BYTES + 1)),
            Err(ConversationError::InvalidConversationId(_))
        ));

        assert!(ProjectName::new("Desktop Product").is_ok());
        for rejected in ["", " padded ", "control\u{7}"] {
            assert!(matches!(
                ProjectName::new(rejected),
                Err(ConversationError::InvalidProjectName(_))
            ));
        }

        assert!(PeerMessage::new("please review the plan").is_ok());
        assert!(matches!(
            PeerMessage::new("   \n "),
            Err(ConversationError::InvalidMessage)
        ));
        assert!(matches!(
            PeerMessage::new("x".repeat(MAX_PEER_MESSAGE_BYTES + 1)),
            Err(ConversationError::InvalidMessage)
        ));
        assert!(
            PeerMessage::new("first line\nsecond line").is_ok(),
            "a message body keeps its interior newlines"
        );
    }

    #[test]
    fn an_absent_input_source_is_the_user_and_an_unknown_one_fails_closed() {
        let absent: SessionLedgerEntry = serde_json::from_value(serde_json::json!({
            "turn_id": "turn-0",
            "prompt_digest": "sha256:abc",
            "runtime_prompt_index": 0,
            "state": { "state": "pending" }
        }))
        .expect("a ledger entry written before this contract still decodes");
        assert_eq!(absent.source, InputSource::User);

        let peer: InputSource = serde_json::from_value(serde_json::json!({
            "kind": "peer",
            "conversation": "thread-42",
            "label": "Release plan"
        }))
        .expect("a peer source round trips");
        assert_eq!(
            peer.peer_conversation().map(ConversationId::as_str),
            Some("thread-42")
        );
        assert_eq!(
            peer.label().map(ConversationLabel::as_str),
            Some("Release plan")
        );

        serde_json::from_value::<InputSource>(serde_json::json!({ "kind": "scheduler" }))
            .expect_err("an unknown source is never silently attributed to the user");
        serde_json::from_value::<InputSource>(serde_json::json!({
            "kind": "peer",
            "conversation": "not a valid identity"
        }))
        .expect_err("a peer source carries a validated conversation identity");
    }

    #[test]
    fn a_user_turn_ledger_entry_is_byte_identical_to_the_entries_written_before_this_contract() {
        let entry = SessionLedgerEntry {
            turn_id: "turn-0".into(),
            prompt_digest: "sha256:abc".into(),
            runtime_prompt_index: 0,
            state: LedgerTurnState::Pending,
            source: InputSource::User,
        };
        assert_eq!(
            serde_json::to_string(&entry).expect("entry"),
            r#"{"turn_id":"turn-0","prompt_digest":"sha256:abc","runtime_prompt_index":0,"state":{"state":"pending"}}"#
        );
    }

    #[test]
    fn a_read_declares_a_bound_the_contract_enforces_on_both_sides() {
        let conversation = ConversationId::new("thread-1").unwrap();
        assert!(
            ConversationRead::new(conversation.clone())
                .validate()
                .is_ok()
        );
        assert!(matches!(
            ConversationRead::new(conversation.clone())
                .max_digest_bytes(0)
                .validate(),
            Err(ConversationError::InvalidDigestBound { requested: 0 })
        ));
        assert!(matches!(
            ConversationRead::new(conversation)
                .max_digest_bytes(MAX_CONVERSATION_DIGEST_BYTES as u32 + 1)
                .validate(),
            Err(ConversationError::InvalidDigestBound { .. })
        ));
    }
}
