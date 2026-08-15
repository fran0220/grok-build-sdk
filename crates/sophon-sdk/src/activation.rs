// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Durable activation coordination for scheduled and autonomous work.
//!
//! A Host that supervises unattended work needs one durable answer to four
//! questions: what is due, who is allowed to execute it right now, is that
//! permission still valid, and has the outcome already been recorded. This
//! module publishes that answer as a backend-neutral contract so a supervisor
//! that crashes, restarts or is accidentally started twice can never execute
//! one work item twice.
//!
//! The unit is a *work item*: a Host-defined identity, a due time, and an
//! opaque payload the coordinator never interprets. A worker claims a due item
//! and receives a *fencing token* which is strictly monotonic per item and
//! never reused. Every later assertion about that item — renew, complete,
//! yield — carries the token, and the coordinator refuses any assertion whose
//! token is not the one it currently honours. Expiry, not liveness detection,
//! is what makes a crashed worker's item claimable again; the successor claim
//! advances the token, which is exactly what invalidates the crashed worker if
//! it ever wakes up and tries to finish.
//!
//! This is deliberately distinct from [`crate::run`]'s per-Run activation
//! lease, which fences one already-loaded Run's controller. The coordinator
//! here is the durable queue in front of that: it decides which work is due
//! and which supervisor may touch it at all, without loading anything.
//!
//! Time is always supplied by the caller. The coordinator has no clock, so its
//! behaviour is a pure function of the stored state and the instant a Host
//! declares — which is what makes the conformance suite deterministic.

mod conformance;
mod local;

pub use conformance::run_activation_coordinator_conformance;
pub use local::{
    ACTIVATION_STORE_SCHEMA_MARKER, ACTIVATION_STORE_SCHEMA_VERSION, LocalActivationCoordinator,
};

/// Largest work item identity the contract accepts, in bytes.
pub const MAX_ACTIVATION_ITEM_ID_BYTES: usize = 256;
/// Largest worker identity the contract accepts, in bytes.
pub const MAX_ACTIVATION_WORKER_ID_BYTES: usize = 256;
/// Largest opaque payload one work item may carry, in bytes.
pub const MAX_ACTIVATION_PAYLOAD_BYTES: usize = 64 * 1024;
/// Largest lease a claim or renewal may ask for, in milliseconds.
pub const MAX_ACTIVATION_LEASE_MS: u64 = 24 * 60 * 60 * 1000;
/// Largest number of work items one claim may take in a single sweep.
pub const MAX_ACTIVATION_CLAIM_BATCH: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum ActivationError {
    #[error("activation input is invalid: {0}")]
    Validation(String),
    #[error("stored activation state is corrupt: {0}")]
    Corrupt(String),
    #[error("activation storage failed: {0}")]
    Storage(String),
}

fn validate_identity(label: &str, value: &str, limit: usize) -> Result<(), ActivationError> {
    if value.is_empty() {
        return Err(ActivationError::Validation(format!("{label} is empty")));
    }
    if value.len() > limit {
        return Err(ActivationError::Validation(format!(
            "{label} exceeds {limit} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(ActivationError::Validation(format!(
            "{label} contains a control character"
        )));
    }
    if value.trim() != value {
        return Err(ActivationError::Validation(format!(
            "{label} has leading or trailing whitespace"
        )));
    }
    Ok(())
}

/// Host-defined stable identity of one unit of schedulable work.
///
/// The coordinator never parses this value; two wakes with the same identity
/// are the same work item, which is what makes duplicate scheduling a no-op
/// rather than a second execution.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActivationItemId(String);

impl ActivationItemId {
    pub fn new(value: impl Into<String>) -> Result<Self, ActivationError> {
        let value = value.into();
        validate_identity("work item id", &value, MAX_ACTIVATION_ITEM_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ActivationItemId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Identity of one supervisor instance competing for work items.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActivationWorkerId(String);

impl ActivationWorkerId {
    pub fn new(value: impl Into<String>) -> Result<Self, ActivationError> {
        let value = value.into();
        validate_identity("worker id", &value, MAX_ACTIVATION_WORKER_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ActivationWorkerId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Strictly monotonic permission to act on one work item.
///
/// A token is minted by a claim and is never reused for that item, so a token
/// lower than the item's current one identifies a worker that has already been
/// superseded — by expiry, by another worker, or by its own earlier release.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActivationFencingToken(u64);

impl ActivationFencingToken {
    /// Minted by the authority alone. A backend must never issue a value it has
    /// issued before for the same work item, and must persist the highest value
    /// it has issued so a restart continues the sequence rather than repeating
    /// it.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ActivationFencingToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Registration or rescheduling of one work item's due time and payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationWake {
    item_id: ActivationItemId,
    due_at_ms: u64,
    payload: Vec<u8>,
}

impl ActivationWake {
    pub fn new(item_id: ActivationItemId, due_at_ms: u64) -> Self {
        Self {
            item_id,
            due_at_ms,
            payload: Vec::new(),
        }
    }

    /// Attaches the opaque bytes handed back with every grant of this item.
    pub fn with_payload(mut self, payload: impl Into<Vec<u8>>) -> Result<Self, ActivationError> {
        let payload = payload.into();
        if payload.len() > MAX_ACTIVATION_PAYLOAD_BYTES {
            return Err(ActivationError::Validation(format!(
                "work item payload exceeds {MAX_ACTIVATION_PAYLOAD_BYTES} bytes"
            )));
        }
        self.payload = payload;
        Ok(self)
    }

    pub fn item_id(&self) -> &ActivationItemId {
        &self.item_id
    }

    pub fn due_at_ms(&self) -> u64 {
        self.due_at_ms
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// What a wake did to the durable queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationWakeOutcome {
    /// The identity was previously unknown and is now queued.
    Registered,
    /// A known item's due time or payload moved.
    Rescheduled,
    /// A duplicate wake: the item was already queued exactly this way.
    Unchanged,
    /// A worker holds an unexpired lease on this item, so its schedule is that
    /// worker's to settle. Reschedule after it releases.
    Held,
}

/// A worker's request to take due work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationClaimRequest {
    worker_id: ActivationWorkerId,
    now_ms: u64,
    lease_ms: u64,
    limit: usize,
}

impl ActivationClaimRequest {
    pub fn new(
        worker_id: ActivationWorkerId,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Self, ActivationError> {
        if lease_ms == 0 || lease_ms > MAX_ACTIVATION_LEASE_MS {
            return Err(ActivationError::Validation(format!(
                "activation lease must be between 1ms and {MAX_ACTIVATION_LEASE_MS}ms"
            )));
        }
        if limit == 0 || limit > MAX_ACTIVATION_CLAIM_BATCH {
            return Err(ActivationError::Validation(format!(
                "claim batch must be between 1 and {MAX_ACTIVATION_CLAIM_BATCH}"
            )));
        }
        now_ms.checked_add(lease_ms).ok_or_else(|| {
            ActivationError::Validation("lease expiry overflows the millisecond clock".into())
        })?;
        Ok(Self {
            worker_id,
            now_ms,
            lease_ms,
            limit,
        })
    }

    pub fn worker_id(&self) -> &ActivationWorkerId {
        &self.worker_id
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    pub fn lease_ms(&self) -> u64 {
        self.lease_ms
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub(crate) fn expires_at_ms(&self) -> u64 {
        self.now_ms.saturating_add(self.lease_ms)
    }
}

/// Exclusive, expiring permission to execute one work item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationGrant {
    item_id: ActivationItemId,
    worker_id: ActivationWorkerId,
    token: ActivationFencingToken,
    due_at_ms: u64,
    expires_at_ms: u64,
    attempt: u64,
    payload: Vec<u8>,
}

impl ActivationGrant {
    /// Built by the authority when it commits a claim.
    pub fn new(
        item_id: ActivationItemId,
        worker_id: ActivationWorkerId,
        token: ActivationFencingToken,
        due_at_ms: u64,
        expires_at_ms: u64,
        attempt: u64,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            item_id,
            worker_id,
            token,
            due_at_ms,
            expires_at_ms,
            attempt,
            payload,
        }
    }

    pub fn item_id(&self) -> &ActivationItemId {
        &self.item_id
    }

    pub fn worker_id(&self) -> &ActivationWorkerId {
        &self.worker_id
    }

    pub fn token(&self) -> ActivationFencingToken {
        self.token
    }

    /// The due time this grant answers, which may be well in the past when a
    /// supervisor is catching up after downtime.
    pub fn due_at_ms(&self) -> u64 {
        self.due_at_ms
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }

    /// How many times this item has been granted, counting this grant. A rising
    /// attempt with no completion is how a Host sees repeated crashes.
    pub fn attempt(&self) -> u64 {
        self.attempt
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// The fence to present on every later assertion about this item.
    pub fn handle(&self) -> ActivationHandle {
        ActivationHandle::new(self.item_id.clone(), self.worker_id.clone(), self.token)
    }
}

/// The fence a worker presents to renew or release work it believes it holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationHandle {
    item_id: ActivationItemId,
    worker_id: ActivationWorkerId,
    token: ActivationFencingToken,
}

impl ActivationHandle {
    /// Rebuilds a fence a worker already holds, which is what a supervisor does
    /// when it resumes from its own durable record of in-flight work.
    pub fn new(
        item_id: ActivationItemId,
        worker_id: ActivationWorkerId,
        token: ActivationFencingToken,
    ) -> Self {
        Self {
            item_id,
            worker_id,
            token,
        }
    }

    pub fn item_id(&self) -> &ActivationItemId {
        &self.item_id
    }

    pub fn worker_id(&self) -> &ActivationWorkerId {
        &self.worker_id
    }

    pub fn token(&self) -> ActivationFencingToken {
        self.token
    }
}

/// Answer to a claim aimed at one named work item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivationClaim {
    Granted(ActivationGrant),
    /// Another worker's lease is still live. This is the refusal that makes
    /// two supervisors safe to run at once.
    Held {
        worker_id: ActivationWorkerId,
        expires_at_ms: u64,
    },
    /// The item exists but its due time has not arrived.
    NotDue {
        due_at_ms: u64,
    },
    /// The item was completed and has no due time until a later wake.
    Settled,
    /// No such work item.
    Unknown,
}

/// Answer to a renewal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationRenewal {
    Renewed {
        expires_at_ms: u64,
    },
    /// The presented token is not the one being honoured — the lease expired,
    /// was taken over, or was already released. The worker must stop.
    Fenced,
}

/// How a worker gives a work item back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationDisposition {
    /// The work is done. The item keeps no due time until a later wake.
    Complete,
    /// The work is not done. The item becomes claimable again at `due_at_ms`.
    Yield { due_at_ms: u64 },
}

/// Answer to a release.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationSettlement {
    /// This release recorded the outcome.
    Settled,
    /// This exact token had already settled this item. Completion is idempotent
    /// by design: a retried release is an answer, never an error and never a
    /// second execution.
    AlreadySettled,
    /// The presented token is not the one being honoured. The outcome was not
    /// recorded and must not be treated as recorded.
    Fenced,
}

/// Lease currently recorded against a work item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationLeaseState {
    pub worker_id: ActivationWorkerId,
    pub token: ActivationFencingToken,
    pub expires_at_ms: u64,
}

/// Everything the coordinator durably knows about one work item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationItemState {
    pub item_id: ActivationItemId,
    /// `None` once the item has been completed and not woken since.
    pub due_at_ms: Option<u64>,
    pub payload: Vec<u8>,
    pub attempt: u64,
    /// The highest token ever minted for this item.
    pub fencing_token: ActivationFencingToken,
    pub lease: Option<ActivationLeaseState>,
    pub settled_at_ms: Option<u64>,
}

impl ActivationItemState {
    /// Whether a claim at `now_ms` would be granted, ignoring batch limits.
    pub fn is_claimable_at(&self, now_ms: u64) -> bool {
        self.due_at_ms.is_some_and(|due| due <= now_ms)
            && self
                .lease
                .as_ref()
                .is_none_or(|lease| lease.expires_at_ms <= now_ms)
    }
}

/// The durable authority over which work is due and who may execute it.
///
/// Implementations own transactions, durability and retention. They must
/// persist and fail-closed verify a schema marker and version, refuse stored
/// state they cannot decode within the published bounds, and make claim,
/// renew and release atomic against every other handle to the same authority —
/// including handles in other processes.
///
/// Every method takes the caller's instant. Implementations must not consult a
/// clock of their own; two Hosts that disagree about the time must still get
/// the same decision from the same stored state.
pub trait ActivationCoordinator: Send + Sync + 'static {
    /// Registers or reschedules one work item. Idempotent by identity: waking
    /// an item that is already queued exactly this way changes nothing.
    fn wake(
        &self,
        request: &ActivationWake,
        now_ms: u64,
    ) -> Result<ActivationWakeOutcome, ActivationError>;

    /// Takes up to `limit` due work items, in due order, oldest first, ties
    /// broken by identity. Items that are not yet due, that are settled, or
    /// that another worker holds under an unexpired lease are never returned.
    fn claim_due(
        &self,
        request: &ActivationClaimRequest,
    ) -> Result<Vec<ActivationGrant>, ActivationError>;

    /// Claims one named work item, answering why not when it is refused.
    fn claim_item(
        &self,
        item_id: &ActivationItemId,
        request: &ActivationClaimRequest,
    ) -> Result<ActivationClaim, ActivationError>;

    /// Extends a held lease by `lease_ms` from `now_ms`. A worker that cannot
    /// renew has lost the item and must abandon its work immediately.
    fn renew(
        &self,
        handle: &ActivationHandle,
        lease_ms: u64,
        now_ms: u64,
    ) -> Result<ActivationRenewal, ActivationError>;

    /// Records the outcome and gives the item back.
    fn release(
        &self,
        handle: &ActivationHandle,
        disposition: ActivationDisposition,
        now_ms: u64,
    ) -> Result<ActivationSettlement, ActivationError>;

    fn inspect(
        &self,
        item_id: &ActivationItemId,
    ) -> Result<Option<ActivationItemState>, ActivationError>;

    /// Drops items completed at or before `settled_before_ms` and answers how
    /// many were dropped. This is the only bound on the queue's growth; it also
    /// forgets the settlement those items were idempotent against, so a Host
    /// purges only work it will never hear about again.
    fn purge_settled(&self, settled_before_ms: u64) -> Result<usize, ActivationError>;
}
