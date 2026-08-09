use grok_build_sdk::{
    ManifestCas, ObjectPut, PreparedManifestCas, PreparedSessionDelete, SessionDelete,
    SessionGeneration, SessionKey, SessionObject, SessionObjectId, SessionSlot, SessionStateStore,
    SessionStateStoreError,
};

struct ExternalStore;
impl SessionStateStore for ExternalStore {
    fn inspect_slot(&self, _: &SessionKey) -> Result<SessionSlot, SessionStateStoreError> {
        Ok(SessionSlot::Vacant)
    }
    fn load_object(
        &self,
        _: &SessionKey,
        _: &SessionGeneration,
        _: &SessionObjectId,
    ) -> Result<Option<SessionObject>, SessionStateStoreError> {
        Ok(None)
    }
    fn put_object(&self, _: SessionObject) -> Result<ObjectPut, SessionStateStoreError> {
        Ok(ObjectPut::Stored)
    }
    fn compare_and_swap_manifest(
        &self,
        _: PreparedManifestCas,
    ) -> Result<ManifestCas, SessionStateStoreError> {
        Ok(ManifestCas::Conflict)
    }
    fn compare_and_delete(
        &self,
        _: PreparedSessionDelete,
    ) -> Result<SessionDelete, SessionStateStoreError> {
        Ok(SessionDelete::Conflict)
    }
}

#[test]
fn public_contract_is_implementable_outside_the_sdk_crate() {
    let store: Box<dyn SessionStateStore> = Box::new(ExternalStore);
    let key = SessionKey::new("external-host-session").unwrap();
    assert_eq!(store.inspect_slot(&key).unwrap(), SessionSlot::Vacant);
}
