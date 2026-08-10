use super::*;

pub(super) type ShellAuthority =
    dyn xai_grok_shell::session::state_authority::NativeSessionStateAuthority;

pub(super) struct SessionStateAuthorityBridge {
    pub(super) store: Arc<dyn crate::SessionStateStore>,
}

struct SessionStateSessionBridge {
    store: Arc<dyn crate::SessionStateStore>,
    key: crate::SessionKey,
    generation: crate::SessionGeneration,
    identity: xai_grok_shell::session::state_authority::SessionIdentity,
    staged: std::sync::Mutex<Vec<Vec<u8>>>,
}

fn authority_error(
    error: impl ToString,
) -> xai_grok_shell::session::state_authority::AuthorityError {
    xai_grok_shell::session::state_authority::AuthorityError(error.to_string())
}

impl xai_grok_shell::session::state_authority::NativeSessionStateAuthority
    for SessionStateAuthorityBridge
{
    fn inspect(
        &self,
        session_identity: &str,
    ) -> Result<
        xai_grok_shell::session::state_authority::SessionInspection,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::SessionInspection;
        let key = crate::SessionKey::new(session_identity).map_err(authority_error)?;
        Ok(
            match self.store.inspect_slot(&key).map_err(authority_error)? {
                crate::SessionSlot::Vacant => SessionInspection::Vacant,
                crate::SessionSlot::Live(x) => SessionInspection::Live {
                    generation: x.manifest().generation().as_str().to_owned(),
                },
                crate::SessionSlot::Tombstoned { receipt } => SessionInspection::Tombstoned {
                    generation: receipt.generation().as_str().to_owned(),
                },
            },
        )
    }

    fn create(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        session.create_empty()?;
        Ok(Arc::new(session))
    }

    fn open(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        let current = session
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        if current.manifest().generation() != &session.generation {
            return Err(authority_error("session generation mismatch"));
        }
        Ok(Arc::new(session))
    }

    fn publish_fork(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
        records: Vec<xai_grok_shell::session::state_authority::ReplayRecord>,
    ) -> Result<
        Arc<dyn xai_grok_shell::session::state_authority::NativeSession>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let session = self.session(id)?;
        session.publish_prepared(records)?;
        Ok(Arc::new(session))
    }

    fn tombstone(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let session = self.session(id)?;
        let expected = session
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        if expected.manifest().generation() != &session.generation {
            return Err(authority_error("session generation mismatch"));
        }
        let request = crate::PreparedSessionDelete::new(session.key.clone(), expected.clone())
            .map_err(authority_error)?;
        let result = self
            .store
            .compare_and_delete(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&session.key)
            .map_err(authority_error)?;
        if crate::delete_reconciled(&result, &slot, &expected) {
            Ok(())
        } else {
            Err(authority_error(
                "delete conflicted or acknowledgement could not be reconciled",
            ))
        }
    }
}

impl SessionStateAuthorityBridge {
    fn session(
        &self,
        id: xai_grok_shell::session::state_authority::SessionIdentity,
    ) -> Result<SessionStateSessionBridge, xai_grok_shell::session::state_authority::AuthorityError>
    {
        Ok(SessionStateSessionBridge {
            store: self.store.clone(),
            key: crate::SessionKey::new(&id.identity).map_err(authority_error)?,
            generation: crate::SessionGeneration::new(&id.generation).map_err(authority_error)?,
            identity: id,
            staged: std::sync::Mutex::new(Vec::new()),
        })
    }
}

impl SessionStateSessionBridge {
    fn publish_prepared(
        &self,
        records: Vec<xai_grok_shell::session::state_authority::ReplayRecord>,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        use xai_grok_shell::session::state_authority::{ReplayRecord, RewindOperation};
        if self.current()?.is_some() {
            return Err(authority_error("fork target already exists"));
        }
        let mut previous = None;
        let mut sequence = 0u64;
        let mut objects = Vec::with_capacity(records.len());
        for record in records {
            match record {
                ReplayRecord::Update(bytes) => {
                    objects.extend(self.update_objects(&[bytes], &mut previous, &mut sequence)?);
                }
                ReplayRecord::Checkpoint {
                    name,
                    payload,
                    marker,
                } => {
                    let checkpoint = crate::SessionObject::checkpoint(
                        self.key.clone(),
                        self.generation.clone(),
                        name,
                        payload,
                    )
                    .map_err(authority_error)?;
                    self.put_exact(&checkpoint)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| authority_error("sequence overflow"))?;
                    let publication = crate::SessionObject::publish_checkpoint(
                        self.key.clone(),
                        self.generation.clone(),
                        previous.clone(),
                        sequence,
                        marker,
                        checkpoint.id().clone(),
                    )
                    .map_err(authority_error)?;
                    previous = Some(publication.id().clone());
                    objects.push(publication);
                }
                ReplayRecord::Rewind { operation, marker } => {
                    let (kind, index, payload) = match operation {
                        RewindOperation::AppendPoint { index, payload } => {
                            (crate::RewindKind::AppendPoint, index, payload)
                        }
                        RewindOperation::Truncate { index, payload } => {
                            (crate::RewindKind::Truncate, index, payload)
                        }
                        RewindOperation::Merge { index, payload } => {
                            (crate::RewindKind::Merge, index, payload)
                        }
                    };
                    let rewind = crate::SessionObject::rewind(
                        self.key.clone(),
                        self.generation.clone(),
                        kind,
                        index,
                        payload,
                    )
                    .map_err(authority_error)?;
                    self.put_exact(&rewind)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| authority_error("sequence overflow"))?;
                    let publication = crate::SessionObject::publish_rewind(
                        self.key.clone(),
                        self.generation.clone(),
                        previous.clone(),
                        sequence,
                        marker,
                        rewind.id().clone(),
                    )
                    .map_err(authority_error)?;
                    previous = Some(publication.id().clone());
                    objects.push(publication);
                }
            }
        }
        if objects.is_empty() {
            self.create_empty()
        } else {
            self.commit(objects).map(|_| ())
        }
    }

    fn create_empty(&self) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let manifest =
            crate::SessionManifest::new(self.key.clone(), self.generation.clone(), None, 0, 0)
                .map_err(authority_error)?;
        let request = crate::PreparedManifestCas::new(self.key.clone(), None, manifest, &[])
            .map_err(authority_error)?;
        let intended = request.successor().clone();
        let result = self
            .store
            .compare_and_swap_manifest(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?;
        if crate::manifest_cas_reconciled(&result, &slot, &intended) {
            Ok(())
        } else {
            Err(authority_error(
                "session already exists or creation acknowledgement could not be reconciled",
            ))
        }
    }

    fn current(
        &self,
    ) -> Result<
        Option<crate::LiveSessionDocument>,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        match self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?
        {
            crate::SessionSlot::Vacant => Ok(None),
            crate::SessionSlot::Live(x) => Ok(Some(x)),
            crate::SessionSlot::Tombstoned { .. } => Err(authority_error("session is tombstoned")),
        }
    }
    fn put_exact(
        &self,
        object: &crate::SessionObject,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        let result = self
            .store
            .put_object(object.clone())
            .map_err(authority_error)?;
        let loaded = if result == crate::ObjectPut::CommitUnknown {
            self.store
                .load_object(&self.key, &self.generation, object.id())
                .map_err(authority_error)?
        } else {
            None
        };
        if crate::object_put_reconciled(&result, loaded.as_ref(), object) {
            Ok(())
        } else {
            Err(authority_error(
                "object acknowledgement could not be reconciled exactly",
            ))
        }
    }
    fn commit(
        &self,
        objects: Vec<crate::SessionObject>,
    ) -> Result<crate::LiveSessionDocument, xai_grok_shell::session::state_authority::AuthorityError>
    {
        let expected = self.current()?;
        if let Some(x) = &expected {
            if x.manifest().generation() != &self.generation {
                return Err(authority_error("session generation mismatch"));
            }
        }
        for object in &objects {
            self.put_exact(object)?;
        }
        let head = objects
            .last()
            .map(|x| x.id().clone())
            .or_else(|| expected.as_ref().and_then(|x| x.manifest().head().cloned()));
        let count = expected
            .as_ref()
            .map_or(0, |x| x.manifest().segment_count())
            .checked_add(objects.len() as u64)
            .ok_or_else(|| authority_error("record count overflow"))?;
        let added = objects
            .iter()
            .map(|x| match x.kind() {
                crate::SessionObjectKind::TranscriptSegment { bytes, .. } => bytes.len() as u64,
                crate::SessionObjectKind::CheckpointPublication { marker_bytes, .. }
                | crate::SessionObjectKind::RewindPublication { marker_bytes, .. } => {
                    marker_bytes.len() as u64
                }
                _ => 0,
            })
            .sum::<u64>();
        let bytes = expected
            .as_ref()
            .map_or(0, |x| x.manifest().transcript_bytes())
            .checked_add(added)
            .ok_or_else(|| authority_error("transcript size overflow"))?;
        let manifest = crate::SessionManifest::new(
            self.key.clone(),
            self.generation.clone(),
            head,
            count,
            bytes,
        )
        .map_err(authority_error)?;
        let request =
            crate::PreparedManifestCas::new(self.key.clone(), expected, manifest, &objects)
                .map_err(authority_error)?;
        let intended = request.successor().clone();
        let result = self
            .store
            .compare_and_swap_manifest(request)
            .map_err(authority_error)?;
        let slot = self
            .store
            .inspect_slot(&self.key)
            .map_err(authority_error)?;
        if crate::manifest_cas_reconciled(&result, &slot, &intended) {
            Ok(intended)
        } else {
            Err(authority_error(
                "manifest conflicted or acknowledgement could not be reconciled exactly",
            ))
        }
    }
    fn update_objects(
        &self,
        updates: &[Vec<u8>],
        previous: &mut Option<crate::SessionObjectId>,
        sequence: &mut u64,
    ) -> Result<Vec<crate::SessionObject>, xai_grok_shell::session::state_authority::AuthorityError>
    {
        let mut out = Vec::new();
        for bytes in updates {
            if bytes.len() > crate::TARGET_TRANSCRIPT_SEGMENT_BYTES {
                return Err(authority_error("single update exceeds chunk limit"));
            }
            *sequence = sequence
                .checked_add(1)
                .ok_or_else(|| authority_error("sequence overflow"))?;
            let object = crate::SessionObject::transcript(
                self.key.clone(),
                self.generation.clone(),
                previous.clone(),
                *sequence,
                bytes.clone(),
            )
            .map_err(authority_error)?;
            *previous = Some(object.id().clone());
            out.push(object);
        }
        Ok(out)
    }
}

impl xai_grok_shell::session::state_authority::NativeSession for SessionStateSessionBridge {
    fn identity(&self) -> &xai_grok_shell::session::state_authority::SessionIdentity {
        &self.identity
    }
    fn stage_update(
        &self,
        bytes: Vec<u8>,
    ) -> Result<(), xai_grok_shell::session::state_authority::AuthorityError> {
        if bytes.len() > crate::TARGET_TRANSCRIPT_SEGMENT_BYTES {
            return Err(authority_error("single update exceeds chunk limit"));
        }
        self.staged.lock().map_err(authority_error)?.push(bytes);
        Ok(())
    }
    fn flush(
        &self,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        if !objects.is_empty() {
            self.commit(objects)?;
            staged.clear();
        }
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }
    fn replay_page(
        &self,
        cursor: Option<xai_grok_shell::session::state_authority::ReplayCursor>,
        max_records: usize,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayPage,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::{
            ReplayCursor, ReplayPage, ReplayRecord, RewindOperation,
        };
        if max_records == 0 || max_records > 4096 {
            return Err(authority_error("invalid replay page bound"));
        }
        let doc = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let start = cursor.map_or(1, |c| {
            if c.generation != self.identity.generation {
                u64::MAX
            } else {
                c.next_sequence
            }
        });
        if start == 0 || start > doc.manifest().segment_count() + 1 {
            return Err(authority_error("invalid replay cursor or cursor gap"));
        }
        let mut chain = Vec::new();
        let mut id = doc.manifest().head().cloned();
        while let Some(object_id) = id {
            if chain.len() >= 1_000_000 {
                return Err(authority_error("replay traversal limit exceeded"));
            }
            let object = self
                .store
                .load_object(&self.key, &self.generation, &object_id)
                .map_err(authority_error)?
                .ok_or_else(|| authority_error("missing replay object"))?;
            id = object.previous().cloned();
            chain.push(object);
        }
        chain.reverse();
        if chain.len() as u64 != doc.manifest().segment_count()
            || chain
                .iter()
                .enumerate()
                .any(|(i, x)| x.sequence() != Some(i as u64 + 1))
        {
            return Err(authority_error("corrupt replay chain or cursor gap"));
        }
        let mut records = Vec::new();
        for object in chain
            .into_iter()
            .skip((start - 1) as usize)
            .take(max_records)
        {
            records.push(match object.kind() {
                crate::SessionObjectKind::TranscriptSegment { bytes, .. } => {
                    ReplayRecord::Update(bytes.clone())
                }
                crate::SessionObjectKind::CheckpointPublication {
                    marker_bytes,
                    checkpoint,
                    ..
                } => {
                    let x = self
                        .store
                        .load_object(&self.key, &self.generation, checkpoint)
                        .map_err(authority_error)?
                        .ok_or_else(|| authority_error("missing checkpoint object"))?;
                    match x.kind() {
                        crate::SessionObjectKind::Checkpoint { name, shell_bytes } => {
                            ReplayRecord::Checkpoint {
                                name: name.clone(),
                                payload: shell_bytes.clone(),
                                marker: marker_bytes.clone(),
                            }
                        }
                        _ => return Err(authority_error("invalid checkpoint reference")),
                    }
                }
                crate::SessionObjectKind::RewindPublication {
                    marker_bytes,
                    operation,
                    ..
                } => {
                    let x = self
                        .store
                        .load_object(&self.key, &self.generation, operation)
                        .map_err(authority_error)?
                        .ok_or_else(|| authority_error("missing rewind object"))?;
                    match x.kind() {
                        crate::SessionObjectKind::RewindOperation {
                            kind,
                            index,
                            shell_bytes,
                        } => {
                            let op = match kind {
                                crate::RewindKind::AppendPoint => RewindOperation::AppendPoint {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                                crate::RewindKind::Truncate => RewindOperation::Truncate {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                                crate::RewindKind::Merge => RewindOperation::Merge {
                                    index: *index,
                                    payload: shell_bytes.clone(),
                                },
                            };
                            ReplayRecord::Rewind {
                                operation: op,
                                marker: marker_bytes.clone(),
                            }
                        }
                        _ => return Err(authority_error("invalid rewind reference")),
                    }
                }
                _ => return Err(authority_error("unpublished object in replay chain")),
            });
        }
        let next_sequence = start + records.len() as u64;
        Ok(ReplayPage {
            records,
            next: (next_sequence <= doc.manifest().segment_count()).then(|| ReplayCursor {
                generation: self.identity.generation.clone(),
                next_sequence,
            }),
        })
    }
    fn publish_checkpoint(
        &self,
        name: String,
        payload: Vec<u8>,
        marker: Vec<u8>,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let mut objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        let checkpoint = crate::SessionObject::checkpoint(
            self.key.clone(),
            self.generation.clone(),
            name,
            payload,
        )
        .map_err(authority_error)?;
        self.put_exact(&checkpoint)?;
        sequence += 1;
        let publication = crate::SessionObject::publish_checkpoint(
            self.key.clone(),
            self.generation.clone(),
            previous,
            sequence,
            marker,
            checkpoint.id().clone(),
        )
        .map_err(authority_error)?;
        objects.push(publication);
        self.commit(objects)?;
        staged.clear();
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }
    fn publish_rewind(
        &self,
        operation: xai_grok_shell::session::state_authority::RewindOperation,
        marker: Vec<u8>,
    ) -> Result<
        xai_grok_shell::session::state_authority::ReplayCursor,
        xai_grok_shell::session::state_authority::AuthorityError,
    > {
        use xai_grok_shell::session::state_authority::RewindOperation;
        let (kind, index, payload) = match operation {
            RewindOperation::AppendPoint { index, payload } => {
                (crate::RewindKind::AppendPoint, index, payload)
            }
            RewindOperation::Truncate { index, payload } => {
                (crate::RewindKind::Truncate, index, payload)
            }
            RewindOperation::Merge { index, payload } => (crate::RewindKind::Merge, index, payload),
        };
        let mut staged = self.staged.lock().map_err(authority_error)?;
        let current = self
            .current()?
            .ok_or_else(|| authority_error("session is not live"))?;
        let mut previous = current.manifest().head().cloned();
        let mut sequence = current.manifest().segment_count();
        let mut objects = self.update_objects(&staged, &mut previous, &mut sequence)?;
        let op = crate::SessionObject::rewind(
            self.key.clone(),
            self.generation.clone(),
            kind,
            index,
            payload,
        )
        .map_err(authority_error)?;
        self.put_exact(&op)?;
        sequence += 1;
        let publication = crate::SessionObject::publish_rewind(
            self.key.clone(),
            self.generation.clone(),
            previous,
            sequence,
            marker,
            op.id().clone(),
        )
        .map_err(authority_error)?;
        objects.push(publication);
        self.commit(objects)?;
        staged.clear();
        Ok(xai_grok_shell::session::state_authority::ReplayCursor {
            generation: self.identity.generation.clone(),
            next_sequence: sequence + 1,
        })
    }
}
