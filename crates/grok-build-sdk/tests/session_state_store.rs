use grok_build_sdk::{
    ApiBackend, ManifestCas, ModelSpec, ObjectPut, PreparedManifestCas, PreparedSessionDelete,
    Runtime, RuntimeConfig, SessionDelete, SessionGeneration, SessionKey, SessionObject,
    SessionObjectId, SessionSlot, SessionStateStore, SessionStateStoreError,
};
use std::sync::Arc;

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

#[test]
fn runtime_builder_accepts_host_session_state_authority() {
    let config = RuntimeConfig {
        endpoint: "http://invalid.example".into(),
        api_key: "not-used".into(),
        grok_home: "/tmp/grok-build-sdk-contract-grok".into(),
        session_storage: "/tmp/grok-build-sdk-contract-sessions".into(),
        models: vec![ModelSpec {
            id: "contract-model".into(),
            context_window: 1024,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        }],
    };
    let store: Arc<dyn SessionStateStore> = Arc::new(ExternalStore);
    let _builder = Runtime::builder(config).session_state_store(store);
}
