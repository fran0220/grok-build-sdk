use super::codec::{storage, validation};
use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::{path::PathBuf, sync::Arc};

struct FaultStore {
    inner: LocalSessionStateStore,
    calls: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
    object_unknown: Arc<AtomicBool>,
    manifest_unknown: Arc<AtomicBool>,
    delete_unknown: Arc<AtomicBool>,
}
impl SessionStateStore for FaultStore {
    fn inspect_slot(&self, key: &SessionKey) -> Result<SessionSlot, SessionStateStoreError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.inspect_slot(key)
    }
    fn load_object(
        &self,
        key: &SessionKey,
        generation: &SessionGeneration,
        id: &SessionObjectId,
    ) -> Result<Option<SessionObject>, SessionStateStoreError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.load_object(key, generation, id);
        if let Ok(Some(object)) = &result {
            self.reads
                .fetch_add(object.declared_size(), Ordering::Relaxed);
        }
        result
    }
    fn put_object(&self, object: SessionObject) -> Result<ObjectPut, SessionStateStoreError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.put_object(object)?;
        if self.object_unknown.swap(false, Ordering::AcqRel)
            && matches!(result, ObjectPut::Stored | ObjectPut::AlreadyPresent)
        {
            Ok(ObjectPut::CommitUnknown)
        } else {
            Ok(result)
        }
    }
    fn compare_and_swap_manifest(
        &self,
        request: PreparedManifestCas,
    ) -> Result<ManifestCas, SessionStateStoreError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.compare_and_swap_manifest(request)?;
        if self.manifest_unknown.swap(false, Ordering::AcqRel)
            && matches!(result, ManifestCas::Committed(_))
        {
            Ok(ManifestCas::CommitUnknown)
        } else {
            Ok(result)
        }
    }
    fn compare_and_delete(
        &self,
        request: PreparedSessionDelete,
    ) -> Result<SessionDelete, SessionStateStoreError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.compare_and_delete(request)?;
        if self.delete_unknown.swap(false, Ordering::AcqRel)
            && matches!(result, SessionDelete::Deleted(_))
        {
            Ok(SessionDelete::CommitUnknown)
        } else {
            Ok(result)
        }
    }
}
struct LocalFaultHarness {
    root: tempfile::TempDir,
    epoch: u64,
    calls: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
    faults: u64,
    object_unknown: Arc<AtomicBool>,
    manifest_unknown: Arc<AtomicBool>,
    delete_unknown: Arc<AtomicBool>,
}
impl LocalFaultHarness {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
            epoch: 0,
            calls: Arc::new(AtomicU64::new(0)),
            reads: Arc::new(AtomicU64::new(0)),
            faults: 0,
            object_unknown: Arc::new(AtomicBool::new(false)),
            manifest_unknown: Arc::new(AtomicBool::new(false)),
            delete_unknown: Arc::new(AtomicBool::new(false)),
        }
    }
    fn directory(&self) -> PathBuf {
        self.root.path().join(self.epoch.to_string())
    }
}
impl SessionStateFaultHarness for LocalFaultHarness {
    fn reset(&mut self) -> Result<(), SessionStateStoreError> {
        self.epoch += 1;
        self.calls.store(0, Ordering::Relaxed);
        self.reads.store(0, Ordering::Relaxed);
        self.faults = 0;
        self.object_unknown.store(false, Ordering::Relaxed);
        self.manifest_unknown.store(false, Ordering::Relaxed);
        self.delete_unknown.store(false, Ordering::Relaxed);
        Ok(())
    }
    fn open(&mut self) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError> {
        Ok(Arc::new(FaultStore {
            inner: LocalSessionStateStore::new(self.directory())?,
            calls: self.calls.clone(),
            reads: self.reads.clone(),
            object_unknown: self.object_unknown.clone(),
            manifest_unknown: self.manifest_unknown.clone(),
            delete_unknown: self.delete_unknown.clone(),
        }))
    }
    fn inject(
        &mut self,
        fault: SessionStateFault,
        key: &SessionKey,
        generation: &SessionGeneration,
        object: Option<&SessionObjectId>,
    ) -> Result<(), SessionStateStoreError> {
        self.faults += 1;
        match fault {
            SessionStateFault::AfterObjectPutBeforeAck => {
                self.object_unknown.store(true, Ordering::Release)
            }
            SessionStateFault::AfterManifestCommitBeforeAck => {
                self.manifest_unknown.store(true, Ordering::Release)
            }
            SessionStateFault::AfterDeleteBeforeAck => {
                self.delete_unknown.store(true, Ordering::Release)
            }
            mutation => {
                let id = object.ok_or_else(|| validation("object fault requires object ID"))?;
                let connection =
                    rusqlite::Connection::open(self.directory().join("native-session-log.sqlite3"))
                        .map_err(storage)?;
                let sql = match mutation {
                    SessionStateFault::CorruptObjectPayload => {
                        "UPDATE objects SET payload=X'00' WHERE session_identity=?1 AND generation=?2 AND id=?3"
                    }
                    SessionStateFault::RemoveObject => {
                        "DELETE FROM objects WHERE session_identity=?1 AND generation=?2 AND id=?3"
                    }
                    SessionStateFault::DeclareObjectOversize => {
                        "UPDATE objects SET size=67108865 WHERE session_identity=?1 AND generation=?2 AND id=?3"
                    }
                    _ => unreachable!(),
                };
                connection
                    .execute(sql, rusqlite::params![key.0, generation.0, id.0])
                    .map_err(storage)?;
            }
        }
        Ok(())
    }
    fn metrics(&self) -> SessionStateFaultMetrics {
        SessionStateFaultMetrics {
            backend_calls: self.calls.load(Ordering::Relaxed),
            payload_bytes_read: self.reads.load(Ordering::Relaxed),
            injected_faults: self.faults,
        }
    }
}

#[test]
fn local_conformance() {
    let d = tempfile::tempdir().unwrap();
    run_session_state_conformance(|_| Ok(Arc::new(LocalSessionStateStore::new(d.path())?)))
        .unwrap();
}
#[test]
fn local_session_lease_fences_independent_store_handles() {
    let d = tempfile::tempdir().unwrap();
    let first = LocalSessionStateStore::new(d.path()).unwrap();
    let second = LocalSessionStateStore::new(d.path()).unwrap();
    let key = SessionKey::new("shared-session").unwrap();
    let lease = first.acquire_session_lease(&key).unwrap();
    assert!(second.acquire_session_lease(&key).is_err());
    drop(lease);
    second.acquire_session_lease(&key).unwrap();
}
#[test]
fn local_fault_conformance() {
    run_session_state_fault_conformance(&mut LocalFaultHarness::new()).unwrap();
}
