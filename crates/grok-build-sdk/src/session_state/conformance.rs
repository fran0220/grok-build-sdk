use super::contracts::*;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConformanceOpen {
    Fresh,
    Concurrent,
    Reopen,
}

fn suite_error(message: impl Into<String>) -> SessionStateStoreError {
    SessionStateStoreError::Corrupt(format!("conformance: {}", message.into()))
}

/// Runs the backend-neutral contract against independent handles to one authority.
pub fn run_session_state_conformance<F>(mut open: F) -> Result<(), SessionStateStoreError>
where
    F: FnMut(ConformanceOpen) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError>,
{
    let store = open(ConformanceOpen::Fresh)?;
    let key = SessionKey::new("session-state-conformance")?;
    let generation = SessionGeneration::new("generation-1")?;
    let concurrent = open(ConformanceOpen::Concurrent)?;
    let lease = store.acquire_session_lease(&key)?;
    if concurrent.acquire_session_lease(&key).is_ok() {
        return Err(suite_error("concurrent Session admission was not fenced"));
    }
    drop(lease);
    drop(concurrent.acquire_session_lease(&key)?);
    if store.inspect_slot(&key)? != SessionSlot::Vacant {
        return Err(suite_error("fresh slot is not vacant"));
    }

    let orphan = SessionObject::checkpoint(
        key.clone(),
        generation.clone(),
        "orphan",
        b"orphan".to_vec(),
    )?;
    let orphan_id = orphan.id().clone();
    store.put_object(orphan.clone())?;
    if store.load_object(&key, &generation, orphan.id())? != Some(orphan) {
        return Err(suite_error("scoped object roundtrip"));
    }
    if store
        .load_object(&SessionKey::new("other-session")?, &generation, &orphan_id)?
        .is_some()
    {
        return Err(suite_error("object escaped session scope"));
    }

    let first =
        SessionObject::transcript(key.clone(), generation.clone(), None, 1, b"first".to_vec())?;
    store.put_object(first.clone())?;
    let first_request = PreparedManifestCas::new(
        key.clone(),
        None,
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(first.id().clone()),
            1,
            5,
        )?,
        std::slice::from_ref(&first),
    )?;
    let exact_first = first_request.successor().clone();
    if store.compare_and_swap_manifest(first_request)?
        != ManifestCas::Committed(exact_first.clone())
    {
        return Err(suite_error(
            "initial CAS did not return exact prepared successor",
        ));
    }

    let wrong_payload = SessionObject::rewind(
        key.clone(),
        generation.clone(),
        RewindKind::Merge,
        1,
        b"wrong-kind".to_vec(),
    )?;
    let wrong_publication = SessionObject::publish_checkpoint(
        key.clone(),
        generation.clone(),
        Some(first.id().clone()),
        2,
        b"invalid-reference".to_vec(),
        wrong_payload.id().clone(),
    )?;
    store.put_object(wrong_payload)?;
    store.put_object(wrong_publication.clone())?;
    let wrong_request = PreparedManifestCas::new(
        key.clone(),
        Some(exact_first.clone()),
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(wrong_publication.id().clone()),
            2,
            5 + b"invalid-reference".len() as u64,
        )?,
        std::slice::from_ref(&wrong_publication),
    )?;
    if store.compare_and_swap_manifest(wrong_request).is_ok() {
        return Err(suite_error("wrong-kind publication reference accepted"));
    }

    let checkpoint = SessionObject::checkpoint(
        key.clone(),
        generation.clone(),
        "named-checkpoint",
        b"checkpoint-payload".to_vec(),
    )?;
    let checkpoint_marker = b"checkpoint-marker".to_vec();
    let checkpoint_pub = SessionObject::publish_checkpoint(
        key.clone(),
        generation.clone(),
        Some(first.id().clone()),
        2,
        checkpoint_marker.clone(),
        checkpoint.id().clone(),
    )?;
    for object in [&checkpoint, &checkpoint_pub] {
        store.put_object(object.clone())?;
    }
    let cp_request = PreparedManifestCas::new(
        key.clone(),
        Some(exact_first.clone()),
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(checkpoint_pub.id().clone()),
            2,
            5 + checkpoint_marker.len() as u64,
        )?,
        std::slice::from_ref(&checkpoint_pub),
    )?;
    let exact_cp = cp_request.successor().clone();
    if store.compare_and_swap_manifest(cp_request)? != ManifestCas::Committed(exact_cp.clone()) {
        return Err(suite_error("checkpoint publication"));
    }
    match store
        .load_object(&key, &generation, checkpoint_pub.id())?
        .map(|o| o.kind)
    {
        Some(SessionObjectKind::CheckpointPublication {
            marker_bytes,
            checkpoint: reference,
            ..
        }) if marker_bytes == checkpoint_marker && reference == *checkpoint.id() => {}
        _ => {
            return Err(suite_error(
                "checkpoint marker/reference kind did not roundtrip",
            ));
        }
    }
    match store
        .load_object(&key, &generation, checkpoint.id())?
        .map(|o| o.kind)
    {
        Some(SessionObjectKind::Checkpoint { name, shell_bytes })
            if name == "named-checkpoint" && shell_bytes == b"checkpoint-payload" => {}
        _ => return Err(suite_error("checkpoint payload/name did not roundtrip")),
    }

    let rewind = SessionObject::rewind(
        key.clone(),
        generation.clone(),
        RewindKind::Truncate,
        7,
        b"rewind-payload".to_vec(),
    )?;
    let rewind_marker = b"rewind-marker".to_vec();
    let rewind_pub = SessionObject::publish_rewind(
        key.clone(),
        generation.clone(),
        Some(checkpoint_pub.id().clone()),
        3,
        rewind_marker.clone(),
        rewind.id().clone(),
    )?;
    for object in [&rewind, &rewind_pub] {
        store.put_object(object.clone())?;
    }
    let rw_request = PreparedManifestCas::new(
        key.clone(),
        Some(exact_cp.clone()),
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(rewind_pub.id().clone()),
            3,
            exact_cp.manifest().transcript_bytes() + rewind_marker.len() as u64,
        )?,
        std::slice::from_ref(&rewind_pub),
    )?;
    let exact_rw = rw_request.successor().clone();
    if store.compare_and_swap_manifest(rw_request)? != ManifestCas::Committed(exact_rw.clone()) {
        return Err(suite_error("rewind publication"));
    }
    match store
        .load_object(&key, &generation, rewind_pub.id())?
        .map(|o| o.kind)
    {
        Some(SessionObjectKind::RewindPublication {
            marker_bytes,
            operation,
            ..
        }) if marker_bytes == rewind_marker && operation == *rewind.id() => {}
        _ => {
            return Err(suite_error(
                "rewind marker/reference kind did not roundtrip",
            ));
        }
    }

    // Two genuinely independent handles race from exactly the same expected document.
    let left = open(ConformanceOpen::Concurrent)?;
    let right = open(ConformanceOpen::Concurrent)?;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let make = |bytes: &'static [u8]| -> Result<(SessionObject, PreparedManifestCas), SessionStateStoreError> {
        let object = SessionObject::transcript(key.clone(), generation.clone(), Some(rewind_pub.id().clone()), 4, bytes.to_vec())?;
        let request = PreparedManifestCas::new(key.clone(), Some(exact_rw.clone()),
            SessionManifest::new(key.clone(), generation.clone(), Some(object.id().clone()), 4, exact_rw.manifest().transcript_bytes() + bytes.len() as u64)?,
            std::slice::from_ref(&object))?;
        Ok((object, request))
    };
    let (lo, lr) = make(b"left")?;
    let (ro, rr) = make(b"right")?;
    left.put_object(lo)?;
    right.put_object(ro)?;
    let run =
        |s: Arc<dyn SessionStateStore>, b: Arc<std::sync::Barrier>, r: PreparedManifestCas| {
            std::thread::spawn(move || {
                b.wait();
                s.compare_and_swap_manifest(r)
            })
        };
    let left_thread = run(left, barrier.clone(), lr);
    let right_thread = run(right, barrier, rr);
    let a = left_thread
        .join()
        .map_err(|_| suite_error("race thread panicked"))??;
    let b = right_thread
        .join()
        .map_err(|_| suite_error("race thread panicked"))??;
    if usize::from(matches!(a, ManifestCas::Committed(_)))
        + usize::from(matches!(b, ManifestCas::Committed(_)))
        != 1
        || usize::from(a == ManifestCas::Conflict) + usize::from(b == ManifestCas::Conflict) != 1
    {
        return Err(suite_error("race was not one commit/one conflict"));
    }
    let winner = match store.inspect_slot(&key)? {
        SessionSlot::Live(x) => x,
        _ => return Err(suite_error("winner missing")),
    };
    let stale_object = SessionObject::transcript(
        key.clone(),
        generation.clone(),
        Some(rewind_pub.id().clone()),
        4,
        b"stale".to_vec(),
    )?;
    store.put_object(stale_object.clone())?;
    let stale = PreparedManifestCas::new(
        key.clone(),
        Some(exact_rw),
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(stale_object.id().clone()),
            4,
            5 + checkpoint_marker.len() as u64 + rewind_marker.len() as u64 + 5,
        )?,
        std::slice::from_ref(&stale_object),
    )?;
    if store.compare_and_swap_manifest(stale)? != ManifestCas::Conflict
        || store.inspect_slot(&key)? != SessionSlot::Live(winner.clone())
    {
        return Err(suite_error("stale writer changed winner"));
    }

    // Advance with individually bounded objects until cumulative transcript exceeds 64 MiB.
    let mut live = winner;
    for n in 0..65u8 {
        let payload = vec![n; 1024 * 1024];
        let object = SessionObject::transcript(
            key.clone(),
            generation.clone(),
            live.manifest().head().cloned(),
            live.manifest().segment_count() + 1,
            payload,
        )?;
        store.put_object(object.clone())?;
        let request = PreparedManifestCas::new(
            key.clone(),
            Some(live.clone()),
            SessionManifest::new(
                key.clone(),
                generation.clone(),
                Some(object.id().clone()),
                live.manifest().segment_count() + 1,
                live.manifest().transcript_bytes() + 1024 * 1024,
            )?,
            std::slice::from_ref(&object),
        )?;
        live = request.successor().clone();
        if store.compare_and_swap_manifest(request)? != ManifestCas::Committed(live.clone()) {
            return Err(suite_error("large cumulative transcript commit"));
        }
    }
    if live.manifest().transcript_bytes() <= MAX_SESSION_OBJECT_BYTES as u64 {
        return Err(suite_error(
            "cumulative transcript did not exceed object bound",
        ));
    }
    drop(store);
    let reopened = open(ConformanceOpen::Reopen)?;
    if reopened.inspect_slot(&key)? != SessionSlot::Live(live.clone()) {
        return Err(suite_error("restart full traversal"));
    }
    let delete = PreparedSessionDelete::new(key.clone(), live.clone())?;
    let intended_receipt = delete.receipt().clone();
    let receipt = match reopened.compare_and_delete(delete)? {
        SessionDelete::Deleted(x) if x == intended_receipt => x,
        _ => return Err(suite_error("CAS delete")),
    };
    if reopened.inspect_slot(&key)?
        != (SessionSlot::Tombstoned {
            receipt: receipt.clone(),
        })
    {
        return Err(suite_error("tombstone missing"));
    }
    drop(reopened);
    let reopened = open(ConformanceOpen::Reopen)?;
    if reopened.inspect_slot(&key)? != (SessionSlot::Tombstoned { receipt }) {
        return Err(suite_error("tombstone not permanent"));
    }
    let recreate = SessionObject::transcript(
        key.clone(),
        generation.clone(),
        None,
        1,
        b"recreate".to_vec(),
    )?;
    reopened.put_object(recreate.clone())?;
    let request = PreparedManifestCas::new(
        key.clone(),
        None,
        SessionManifest::new(key.clone(), generation, Some(recreate.id().clone()), 1, 8)?,
        std::slice::from_ref(&recreate),
    )?;
    if reopened.compare_and_swap_manifest(request)? != ManifestCas::Conflict {
        return Err(suite_error("tombstone allowed recreation"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionStateFault {
    AfterObjectPutBeforeAck,
    AfterManifestCommitBeforeAck,
    AfterDeleteBeforeAck,
    CorruptObjectPayload,
    RemoveObject,
    DeclareObjectOversize,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionStateFaultMetrics {
    pub backend_calls: u64,
    pub payload_bytes_read: u64,
    pub injected_faults: u64,
}
pub trait SessionStateFaultHarness {
    fn reset(&mut self) -> Result<(), SessionStateStoreError>;
    fn open(&mut self) -> Result<Arc<dyn SessionStateStore>, SessionStateStoreError>;
    fn inject(
        &mut self,
        fault: SessionStateFault,
        key: &SessionKey,
        generation: &SessionGeneration,
        object: Option<&SessionObjectId>,
    ) -> Result<(), SessionStateStoreError>;
    fn metrics(&self) -> SessionStateFaultMetrics;
}

/// Orchestrates real operations around typed backend fault injection points.
pub fn run_session_state_fault_conformance<H: SessionStateFaultHarness>(
    h: &mut H,
) -> Result<(), SessionStateStoreError> {
    let key = SessionKey::new("session-state-fault-conformance")?;
    let generation = SessionGeneration::new("generation-1")?;
    h.reset()?;
    let store = h.open()?;
    let object = SessionObject::transcript(
        key.clone(),
        generation.clone(),
        None,
        1,
        b"object-unknown".to_vec(),
    )?;
    h.inject(
        SessionStateFault::AfterObjectPutBeforeAck,
        &key,
        &generation,
        Some(object.id()),
    )?;
    let result = store.put_object(object.clone())?;
    if result != ObjectPut::CommitUnknown
        || !object_put_reconciled(
            &result,
            store.load_object(&key, &generation, object.id())?.as_ref(),
            &object,
        )
    {
        return Err(suite_error(
            "object CommitUnknown did not reconcile exact ID/bytes",
        ));
    }
    let request = PreparedManifestCas::new(
        key.clone(),
        None,
        SessionManifest::new(
            key.clone(),
            generation.clone(),
            Some(object.id().clone()),
            1,
            14,
        )?,
        std::slice::from_ref(&object),
    )?;
    let intended = request.successor().clone();
    h.inject(
        SessionStateFault::AfterManifestCommitBeforeAck,
        &key,
        &generation,
        None,
    )?;
    let result = store.compare_and_swap_manifest(request)?;
    let slot = store.inspect_slot(&key)?;
    if result != ManifestCas::CommitUnknown
        || !manifest_cas_reconciled(&result, &slot, &intended)
        || slot != SessionSlot::Live(intended.clone())
    {
        return Err(suite_error(
            "manifest CommitUnknown did not reconcile exact document",
        ));
    }
    h.inject(
        SessionStateFault::AfterDeleteBeforeAck,
        &key,
        &generation,
        None,
    )?;
    let result =
        store.compare_and_delete(PreparedSessionDelete::new(key.clone(), intended.clone())?)?;
    let slot = store.inspect_slot(&key)?;
    if result != SessionDelete::CommitUnknown || !delete_reconciled(&result, &slot, &intended) {
        return Err(suite_error(
            "delete CommitUnknown did not reconcile exact tombstone",
        ));
    }

    for fault in [
        SessionStateFault::RemoveObject,
        SessionStateFault::CorruptObjectPayload,
    ] {
        h.reset()?;
        let store = h.open()?;
        let object = SessionObject::transcript(
            key.clone(),
            generation.clone(),
            None,
            1,
            b"damage".to_vec(),
        )?;
        store.put_object(object.clone())?;
        let request = PreparedManifestCas::new(
            key.clone(),
            None,
            SessionManifest::new(
                key.clone(),
                generation.clone(),
                Some(object.id().clone()),
                1,
                6,
            )?,
            std::slice::from_ref(&object),
        )?;
        store.compare_and_swap_manifest(request)?;
        h.inject(fault, &key, &generation, Some(object.id()))?;
        if !matches!(
            store.inspect_slot(&key),
            Err(SessionStateStoreError::Corrupt(_))
        ) {
            return Err(suite_error("missing/corrupt object did not fail closed"));
        }
    }
    h.reset()?;
    let store = h.open()?;
    let object = SessionObject::transcript(
        key.clone(),
        generation.clone(),
        None,
        1,
        b"oversize".to_vec(),
    )?;
    store.put_object(object.clone())?;
    h.inject(
        SessionStateFault::DeclareObjectOversize,
        &key,
        &generation,
        Some(object.id()),
    )?;
    let before = h.metrics().payload_bytes_read;
    if !matches!(
        store.load_object(&key, &generation, object.id()),
        Err(SessionStateStoreError::Corrupt(_))
    ) || h.metrics().payload_bytes_read != before
    {
        return Err(suite_error("declared oversize fetched payload"));
    }
    if h.metrics().backend_calls == 0 || h.metrics().injected_faults == 0 {
        return Err(suite_error("fault metrics missing"));
    }
    Ok(())
}
