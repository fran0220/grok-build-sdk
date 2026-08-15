use super::*;

impl Core {
    pub(super) fn require_resident(&self, id: &SessionId) -> Result<(), Error> {
        if self.resident.borrow().contains(&id.0) {
            Ok(())
        } else {
            Err(Error::Operation("session is not resident".into()))
        }
    }
    pub(super) fn observe_journal(
        &self,
        id: &SessionId,
        after_sequence: u64,
    ) -> Result<JournalObservation, Error> {
        let inclusive_end_sequence = self
            .sequences
            .borrow()
            .get(&id.0)
            .copied()
            .ok_or_else(|| Error::Operation("unknown session event journal".into()))?;
        let retained = self.retained.borrow();
        observe_journal_snapshot(retained.get(&id.0), inclusive_end_sequence, after_sequence)
    }
    pub(super) fn probe_session_replay(
        &self,
        id: &SessionId,
        after_sequence: u64,
    ) -> Result<SessionReplayProbe, Error> {
        self.require_resident(id)?;
        let binding = self
            .session_bindings
            .borrow()
            .get(&id.0)
            .cloned()
            .ok_or_else(|| Error::Operation("session binding is unavailable".into()))?;
        let journal = self.observe_journal(id, after_sequence)?;
        let ledger = self.load_ledger(id)?;
        Ok(SessionReplayProbe {
            binding: crate::SessionBinding {
                session_id: id.clone(),
                cwd: binding.cwd,
                model: binding.model,
                reasoning: binding.reasoning,
                harness_digest: binding.harness_digest,
            },
            requested_after_sequence: after_sequence,
            oldest_retained_sequence: journal.oldest_retained_sequence,
            inclusive_end_sequence: journal.inclusive_end_sequence,
            retained_count: journal.retained_count,
            truncated: journal.truncated,
            events: journal.events,
            ledger,
        })
    }
    pub(super) fn events_after(&self, id: &SessionId, sequence: u64) -> Result<Vec<Event>, Error> {
        let observation = self.observe_journal(id, sequence)?;
        if observation.truncated {
            return Err(Error::EventGap {
                requested: sequence,
                oldest_available: observation.oldest_retained_sequence,
                newest: observation.inclusive_end_sequence,
            });
        }
        Ok(observation.events)
    }
}
