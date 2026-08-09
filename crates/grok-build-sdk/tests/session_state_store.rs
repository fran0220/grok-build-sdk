use grok_build_sdk::{
    PreparedSessionStateCommit, PreparedSessionStateDelete, SessionStateCommit, SessionStateDelete,
    SessionStateDocument, SessionStateKey, SessionStateStore, SessionStateStoreError,
    SessionStateVersion,
};
use std::{collections::HashMap, sync::Mutex};

#[derive(Default)]
struct ExternalStore {
    rows: Mutex<HashMap<String, (u64, String, Vec<u8>)>>,
}

impl SessionStateStore for ExternalStore {
    fn load(
        &self,
        key: &SessionStateKey,
    ) -> Result<Option<SessionStateDocument>, SessionStateStoreError> {
        self.rows
            .lock()
            .unwrap()
            .get(key.session_identity())
            .cloned()
            .map(|(revision, digest, bytes)| {
                let version = SessionStateVersion::from_stored_parts(revision, digest)?;
                SessionStateDocument::from_stored(version, bytes)
            })
            .transpose()
    }

    fn compare_and_swap(
        &self,
        request: PreparedSessionStateCommit,
    ) -> Result<SessionStateCommit, SessionStateStoreError> {
        let mut rows = self.rows.lock().unwrap();
        let current = rows
            .get(request.key().session_identity())
            .map(|(revision, digest, _)| {
                SessionStateVersion::from_stored_parts(*revision, digest.clone())
            })
            .transpose()?;
        if current.as_ref() != request.expected() {
            return Ok(SessionStateCommit::Conflict);
        }
        let successor = request.successor().clone();
        rows.insert(
            request.key().session_identity().to_owned(),
            (
                successor.revision(),
                successor.digest().to_owned(),
                request.bytes().to_vec(),
            ),
        );
        Ok(SessionStateCommit::Committed(successor))
    }

    fn compare_and_delete(
        &self,
        request: PreparedSessionStateDelete,
    ) -> Result<SessionStateDelete, SessionStateStoreError> {
        let mut rows = self.rows.lock().unwrap();
        let current = rows
            .get(request.key().session_identity())
            .map(|(revision, digest, _)| {
                SessionStateVersion::from_stored_parts(*revision, digest.clone())
            })
            .transpose()?;
        if current.as_ref() != Some(request.expected()) {
            return Ok(SessionStateDelete::Conflict);
        }
        rows.remove(request.key().session_identity());
        Ok(SessionStateDelete::Deleted)
    }
}

#[test]
fn public_contract_is_implementable_outside_the_sdk_crate() {
    let store: Box<dyn SessionStateStore> = Box::new(ExternalStore::default());
    let key = SessionStateKey::new("external-host-session").unwrap();
    assert!(store.load(&key).unwrap().is_none());
}
