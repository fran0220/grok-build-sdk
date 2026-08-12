use super::*;

mod capabilities;
mod command_loop;
mod evidence;
mod extensions;
mod journal;
mod rewind;
mod sessions;
mod shutdown;
mod startup;
mod turns;

#[derive(serde::Deserialize)]
pub(super) struct RewindPointsWire {
    rewind_points: Vec<RewindPointWire>,
}
#[derive(serde::Deserialize)]
pub(super) struct RewindPointWire {
    pub(super) prompt_index: u64,
    pub(super) created_at: String,
    pub(super) num_file_snapshots: u64,
    pub(super) has_file_changes: bool,
    pub(super) prompt_preview: Option<String>,
    pub(super) origin_prompt_digest: Option<String>,
}
#[derive(serde::Deserialize)]
struct RewindResultWire {
    success: bool,
    target_prompt_index: u64,
    mode: String,
    reverted_files: Vec<String>,
    #[serde(default)]
    clean_files: Vec<String>,
    conflicts: Vec<RewindConflictWire>,
    #[serde(default)]
    prompt_text: Option<String>,
    error: Option<String>,
}
#[derive(serde::Deserialize)]
struct RewindConflictWire {
    path: String,
    conflict_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct RewindIntent {
    operation_id: String,
    session_id: String,
    target_prompt_index: u64,
    target_turn_id: String,
    target_prompt_digest: String,
    recovery_turn_id: Option<String>,
    recovery_prompt_digest: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
enum RewindEvidence {
    Intent(RewindIntent),
    Receipt(ConversationRewindReceipt),
}

pub(super) fn native_rewind_already_applied(
    points: &[RewindPointWire],
    target_prompt_index: u64,
    ledger: &SessionLedger,
) -> Result<bool, Error> {
    for (expected, point) in points.iter().enumerate() {
        if point.prompt_index != expected as u64 {
            return Err(Error::Operation(
                "native Grok rewind points are not a contiguous prompt history".into(),
            ));
        }
        let expected_entry = ledger.entries.iter().find(|entry| {
            entry.runtime_prompt_index == point.prompt_index
                && !matches!(entry.state, LedgerTurnState::Discarded)
        });
        if expected_entry.is_some_and(|entry| {
            !entry.prompt_digest.starts_with("sha256-v2:")
                && Some(entry.prompt_digest.as_str()) != point.origin_prompt_digest.as_deref()
        }) {
            return Err(Error::Operation(
                "native Grok prompt prefix differs from the durable Turn ledger".into(),
            ));
        }
    }
    let prompt_count = points.len() as u64;
    if prompt_count < target_prompt_index {
        return Err(Error::Operation(
            "native Grok conversation is behind the pending rewind target".into(),
        ));
    }
    Ok(prompt_count == target_prompt_index)
}

#[derive(serde::Deserialize)]
struct UnloadWire {
    success: bool,
    drained: bool,
}

pub(super) fn close_outcome_releases_lease(outcome: Option<&str>) -> bool {
    matches!(outcome, Some("closed" | "notResident"))
}

pub(super) struct TurnReservation {
    pub(super) turns: Rc<RefCell<HashMap<String, String>>>,
    pub(super) turn_usages: TurnUsageMap,
    pub(super) session_id: String,
    pub(super) turn_id: String,
}

impl Drop for TurnReservation {
    fn drop(&mut self) {
        let mut turns = self.turns.borrow_mut();
        if turns
            .get(&self.session_id)
            .is_some_and(|active_turn| active_turn == &self.turn_id)
        {
            turns.remove(&self.session_id);
        }
        self.turn_usages
            .borrow_mut()
            .remove(&(self.session_id.clone(), self.turn_id.clone()));
    }
}

#[derive(Clone)]
struct ResidentSessionBinding {
    cwd: std::path::PathBuf,
    model: String,
    reasoning: Option<String>,
    harness_digest: Option<HarnessDigest>,
    capabilities: CapabilityResolution,
}

impl ResidentSessionBinding {
    fn new(
        config: &SessionConfig,
        effective_reasoning: Option<String>,
        harness_digest: Option<HarnessDigest>,
        capabilities: CapabilityResolution,
    ) -> Self {
        Self {
            cwd: config.cwd.clone(),
            model: config.model.clone(),
            reasoning: effective_reasoning,
            harness_digest,
            capabilities,
        }
    }
}

pub(super) struct JournalObservation {
    pub(super) oldest_retained_sequence: u64,
    pub(super) inclusive_end_sequence: u64,
    pub(super) retained_count: usize,
    pub(super) truncated: bool,
    pub(super) events: Vec<Event>,
}

pub(super) fn observe_journal_snapshot(
    retained: Option<&VecDeque<Event>>,
    inclusive_end_sequence: u64,
    after_sequence: u64,
) -> Result<JournalObservation, Error> {
    if after_sequence > inclusive_end_sequence {
        return Err(Error::Operation(
            "event cursor is beyond the session sequence".into(),
        ));
    }
    let oldest_retained_sequence = retained
        .and_then(|events| events.front())
        .map(|event| event.sequence)
        .unwrap_or(inclusive_end_sequence.saturating_add(1));
    let truncated = after_sequence.saturating_add(1) < oldest_retained_sequence;
    let events = retained
        .into_iter()
        .flatten()
        .filter(|event| truncated || event.sequence > after_sequence)
        .cloned()
        .collect();
    Ok(JournalObservation {
        oldest_retained_sequence,
        inclusive_end_sequence,
        retained_count: retained.map_or(0, VecDeque::len),
        truncated,
        events,
    })
}

struct PreparedHarnessTurn {
    prompt_digest: String,
    snapshot_digest: HarnessDigest,
    model: String,
    reasoning: Option<String>,
    after_sequence: u64,
}

pub(super) struct Core {
    agent: Rc<EmbeddedAgent>,
    session_state_authority: Option<Arc<ShellAuthority>>,
    session_state_store: Option<Arc<dyn crate::SessionStateStore>>,
    session_leases: RefCell<HashMap<String, Box<dyn crate::SessionStateLease>>>,
    events: mpsc::UnboundedSender<Event>,
    catalog: HashMap<String, crate::ModelSpec>,
    sequences: Rc<RefCell<HashMap<String, u64>>>,
    retained: Rc<RefCell<HashMap<String, VecDeque<Event>>>>,
    capacity: usize,
    options: RuntimeOptions,
    general_capabilities: crate::CapabilityLayer,
    resident: RefCell<HashSet<String>>,
    session_bindings: RefCell<HashMap<String, ResidentSessionBinding>>,
    mcp_bindings: Arc<McpBindingRegistry>,
    turns: Rc<RefCell<HashMap<String, String>>>,
    turn_usages: TurnUsageMap,
    prompt_tasks: RefCell<HashMap<String, tokio::task::AbortHandle>>,
    replay: Rc<RefCell<HashMap<String, ReplayMode>>>,
    evidence_store: Arc<dyn SessionEvidenceStore>,
    evidence_versions: RefCell<HashMap<SessionEvidenceKey, SessionEvidenceVersion>>,
    compaction_correlations: CompactionCorrelationMap,
}

pub(super) struct SessionLeaseAdmission<'a> {
    leases: &'a RefCell<HashMap<String, Box<dyn crate::SessionStateLease>>>,
    id: String,
    lease: Option<Box<dyn crate::SessionStateLease>>,
    state: LeaseAdmissionState,
}

#[derive(Clone, Copy)]
enum LeaseAdmissionState {
    Release,
    CommitResident,
    Quarantine,
}

impl<'a> SessionLeaseAdmission<'a> {
    pub(super) fn new(
        leases: &'a RefCell<HashMap<String, Box<dyn crate::SessionStateLease>>>,
        id: &SessionId,
        lease: Option<Box<dyn crate::SessionStateLease>>,
    ) -> Self {
        Self {
            leases,
            id: id.0.clone(),
            lease,
            state: LeaseAdmissionState::Release,
        }
    }

    pub(super) fn dispatch_uncertain(&mut self) {
        self.state = LeaseAdmissionState::Quarantine;
    }

    pub(super) fn release(&mut self) {
        self.state = LeaseAdmissionState::Release;
    }

    pub(super) fn commit_resident(&mut self) {
        self.state = LeaseAdmissionState::CommitResident;
    }
}

impl Drop for SessionLeaseAdmission<'_> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            match self.state {
                LeaseAdmissionState::Release => drop(lease),
                LeaseAdmissionState::CommitResident => {
                    self.leases.borrow_mut().insert(self.id.clone(), lease);
                }
                LeaseAdmissionState::Quarantine => quarantine_session_leases(vec![lease]),
            }
        }
    }
}

pub(super) fn quarantine_session_leases(leases: Vec<Box<dyn crate::SessionStateLease>>) {
    static QUARANTINE: std::sync::OnceLock<
        std::sync::Mutex<Vec<Box<dyn crate::SessionStateLease>>>,
    > = std::sync::OnceLock::new();
    QUARANTINE
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(leases);
}

impl Drop for Core {
    fn drop(&mut self) {
        let leases = self
            .session_leases
            .get_mut()
            .drain()
            .map(|(_, lease)| lease)
            .collect();
        quarantine_session_leases(leases);
    }
}
