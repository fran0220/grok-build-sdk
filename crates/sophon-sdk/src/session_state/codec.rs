use super::contracts::*;
use sha2::{Digest as _, Sha256};

const MAGIC: &[u8] = b"sophon-sdk.session-log\0\x02";
pub(super) fn encode_object(
    k: &SessionKey,
    g: &SessionGeneration,
    kind: &SessionObjectKind,
) -> Result<Vec<u8>, SessionStateStoreError> {
    let mut b = MAGIC.to_vec();
    put_bytes(&mut b, k.0.as_bytes());
    put_bytes(&mut b, g.0.as_bytes());
    match kind {
        SessionObjectKind::TranscriptSegment {
            previous,
            sequence,
            bytes,
        } => {
            b.push(1);
            put_ref(&mut b, previous.as_ref());
            b.extend(sequence.to_be_bytes());
            put_bytes64(&mut b, bytes)
        }
        SessionObjectKind::Checkpoint { name, shell_bytes } => {
            b.push(2);
            put_bytes(&mut b, name.as_bytes());
            put_bytes64(&mut b, shell_bytes)
        }
        SessionObjectKind::RewindOperation {
            kind,
            index,
            shell_bytes,
        } => {
            b.push(match kind {
                RewindKind::AppendPoint => 3,
                RewindKind::Truncate => 4,
                RewindKind::Merge => 5,
            });
            b.extend(index.to_be_bytes());
            put_bytes64(&mut b, shell_bytes)
        }
        SessionObjectKind::CheckpointPublication {
            previous,
            sequence,
            marker_bytes,
            checkpoint,
        } => {
            b.push(6);
            put_ref(&mut b, previous.as_ref());
            b.extend(sequence.to_be_bytes());
            put_bytes64(&mut b, marker_bytes);
            put_ref(&mut b, Some(checkpoint))
        }
        SessionObjectKind::RewindPublication {
            previous,
            sequence,
            marker_bytes,
            operation,
        } => {
            b.push(7);
            put_ref(&mut b, previous.as_ref());
            b.extend(sequence.to_be_bytes());
            put_bytes64(&mut b, marker_bytes);
            put_ref(&mut b, Some(operation))
        }
        SessionObjectKind::CompactionPublication {
            previous,
            sequence,
            marker_bytes,
            checkpoint,
            record,
        } => {
            b.push(8);
            put_ref(&mut b, previous.as_ref());
            b.extend(sequence.to_be_bytes());
            put_bytes64(&mut b, marker_bytes);
            put_ref(&mut b, Some(checkpoint));
            put_bytes64(&mut b, &serde_json::to_vec(record).map_err(validation)?)
        }
    }
    if b.len() > MAX_SESSION_OBJECT_BYTES {
        return Err(validation("object exceeds 64 MiB"));
    }
    Ok(b)
}
pub(super) fn decode_object(
    b: &[u8],
) -> Result<(SessionKey, SessionGeneration, SessionObjectKind), SessionStateStoreError> {
    if b.len() > MAX_SESSION_OBJECT_BYTES || !b.starts_with(MAGIC) {
        return Err(corrupt("object marker/version mismatch"));
    }
    let mut p = MAGIC.len();
    let k = SessionKey::new(String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?)
        .map_err(as_corrupt)?;
    let g = SessionGeneration::new(String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?)
        .map_err(as_corrupt)?;
    let tag = take(b, &mut p, 1)?[0];
    let kind = match tag {
        1 => SessionObjectKind::TranscriptSegment {
            previous: get_ref(b, &mut p)?,
            sequence: get_u64(b, &mut p)?,
            bytes: get_bytes64(b, &mut p)?,
        },
        2 => SessionObjectKind::Checkpoint {
            name: String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?,
            shell_bytes: get_bytes64(b, &mut p)?,
        },
        3..=5 => SessionObjectKind::RewindOperation {
            kind: if tag == 3 {
                RewindKind::AppendPoint
            } else if tag == 4 {
                RewindKind::Truncate
            } else {
                RewindKind::Merge
            },
            index: get_u64(b, &mut p)?,
            shell_bytes: get_bytes64(b, &mut p)?,
        },
        6 | 7 => {
            let previous = get_ref(b, &mut p)?;
            let sequence = get_u64(b, &mut p)?;
            let marker_bytes = get_bytes64(b, &mut p)?;
            let reference =
                get_ref(b, &mut p)?.ok_or_else(|| corrupt("publication missing reference"))?;
            if tag == 6 {
                SessionObjectKind::CheckpointPublication {
                    previous,
                    sequence,
                    marker_bytes,
                    checkpoint: reference,
                }
            } else {
                SessionObjectKind::RewindPublication {
                    previous,
                    sequence,
                    marker_bytes,
                    operation: reference,
                }
            }
        }
        8 => {
            let previous = get_ref(b, &mut p)?;
            let sequence = get_u64(b, &mut p)?;
            let marker_bytes = get_bytes64(b, &mut p)?;
            let checkpoint =
                get_ref(b, &mut p)?.ok_or_else(|| corrupt("publication missing reference"))?;
            let record: crate::CompactionPublicationRecord =
                serde_json::from_slice(&get_bytes64(b, &mut p)?).map_err(corrupt)?;
            record.validate().map_err(corrupt)?;
            SessionObjectKind::CompactionPublication {
                previous,
                sequence,
                marker_bytes,
                checkpoint,
                record,
            }
        }
        _ => return Err(corrupt("invalid object kind")),
    };
    if p != b.len() {
        return Err(corrupt("trailing object bytes"));
    }
    Ok((k, g, kind))
}
pub(super) fn encode_manifest(
    k: &SessionKey,
    g: &SessionGeneration,
    h: Option<&SessionObjectId>,
    count: u64,
    bytes: u64,
    compaction_state: &CompactionManifestState,
) -> Result<Vec<u8>, SessionStateStoreError> {
    let mut b = MAGIC.to_vec();
    put_bytes(&mut b, k.0.as_bytes());
    put_bytes(&mut b, g.0.as_bytes());
    put_ref(&mut b, h);
    b.extend(count.to_be_bytes());
    b.extend(bytes.to_be_bytes());
    put_bytes64(
        &mut b,
        &serde_json::to_vec(compaction_state).map_err(validation)?,
    );
    if b.len() > MAX_SESSION_MANIFEST_BYTES {
        return Err(validation("manifest exceeds 64 KiB"));
    }
    Ok(b)
}
pub(super) fn decode_manifest(b: &[u8]) -> Result<SessionManifest, SessionStateStoreError> {
    if !b.starts_with(MAGIC) {
        return Err(corrupt("manifest marker/version mismatch"));
    }
    let mut p = MAGIC.len();
    let session = SessionKey::new(String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?)
        .map_err(as_corrupt)?;
    let generation =
        SessionGeneration::new(String::from_utf8(get_bytes(b, &mut p)?).map_err(corrupt)?)
            .map_err(as_corrupt)?;
    let head = get_ref(b, &mut p)?;
    let segment_count = get_u64(b, &mut p)?;
    let transcript_bytes = get_u64(b, &mut p)?;
    let compaction_state: CompactionManifestState =
        serde_json::from_slice(&get_bytes64(b, &mut p)?).map_err(corrupt)?;
    validate_compaction_state(&compaction_state).map_err(as_corrupt)?;
    if p != b.len() || head.is_none() && (segment_count != 0 || transcript_bytes != 0) {
        return Err(corrupt("invalid manifest"));
    }
    Ok(SessionManifest {
        session,
        generation,
        head,
        segment_count,
        transcript_bytes,
        compaction_state,
        bytes: b.to_vec(),
    })
}
pub(super) fn chain_parts(k: &SessionObjectKind) -> Option<(Option<&SessionObjectId>, u64)> {
    match k {
        SessionObjectKind::TranscriptSegment {
            previous, sequence, ..
        }
        | SessionObjectKind::CheckpointPublication {
            previous, sequence, ..
        }
        | SessionObjectKind::RewindPublication {
            previous, sequence, ..
        } => Some((previous.as_ref(), *sequence)),
        SessionObjectKind::CompactionPublication {
            previous, sequence, ..
        } => Some((previous.as_ref(), *sequence)),
        _ => None,
    }
}
pub(super) fn chain_bytes(k: &SessionObjectKind) -> u64 {
    match k {
        SessionObjectKind::TranscriptSegment { bytes, .. } => bytes.len() as u64,
        SessionObjectKind::CheckpointPublication { marker_bytes, .. }
        | SessionObjectKind::RewindPublication { marker_bytes, .. }
        | SessionObjectKind::CompactionPublication { marker_bytes, .. } => {
            marker_bytes.len() as u64
        }
        _ => 0,
    }
}
pub(super) fn validate_kind(k: &SessionObjectKind) -> Result<(), SessionStateStoreError> {
    if let Some((p, s)) = chain_parts(k)
        && (s == 0 || s == 1 && p.is_some() || s > 1 && p.is_none())
    {
        return Err(validation("chain previous/sequence mismatch"));
    }
    if let SessionObjectKind::Checkpoint { name, .. } = k {
        valid_text(name, 1024, "checkpoint name")?
    }
    if let SessionObjectKind::CompactionPublication { record, .. } = k {
        record.validate().map_err(validation)?;
    }
    Ok(())
}
pub(super) fn put_bytes(b: &mut Vec<u8>, v: &[u8]) {
    b.extend((v.len() as u32).to_be_bytes());
    b.extend(v)
}
pub(super) fn put_bytes64(b: &mut Vec<u8>, v: &[u8]) {
    b.extend((v.len() as u64).to_be_bytes());
    b.extend(v)
}
pub(super) fn get_bytes(b: &[u8], p: &mut usize) -> Result<Vec<u8>, SessionStateStoreError> {
    let n = u32::from_be_bytes(take(b, p, 4)?.try_into().unwrap()) as usize;
    Ok(take(b, p, n)?.to_vec())
}
pub(super) fn get_bytes64(b: &[u8], p: &mut usize) -> Result<Vec<u8>, SessionStateStoreError> {
    let n = usize::try_from(get_u64(b, p)?).map_err(|_| corrupt("length overflow"))?;
    Ok(take(b, p, n)?.to_vec())
}
pub(super) fn get_u64(b: &[u8], p: &mut usize) -> Result<u64, SessionStateStoreError> {
    Ok(u64::from_be_bytes(take(b, p, 8)?.try_into().unwrap()))
}
pub(super) fn put_ref(b: &mut Vec<u8>, r: Option<&SessionObjectId>) {
    match r {
        None => b.push(0),
        Some(r) => {
            b.push(1);
            b.extend(r.0.strip_prefix("sha256:").unwrap().as_bytes())
        }
    }
}
pub(super) fn get_ref(
    b: &[u8],
    p: &mut usize,
) -> Result<Option<SessionObjectId>, SessionStateStoreError> {
    match take(b, p, 1)?[0] {
        0 => Ok(None),
        1 => SessionObjectId::from_stored(format!(
            "sha256:{}",
            String::from_utf8(take(b, p, 64)?.to_vec()).map_err(corrupt)?
        ))
        .map(Some),
        _ => Err(corrupt("invalid reference tag")),
    }
}
pub(super) fn take<'a>(
    b: &'a [u8],
    p: &mut usize,
    n: usize,
) -> Result<&'a [u8], SessionStateStoreError> {
    let end = p.checked_add(n).ok_or_else(|| corrupt("length overflow"))?;
    let out = b
        .get(*p..end)
        .ok_or_else(|| corrupt("truncated encoding"))?;
    *p = end;
    Ok(out)
}
pub(super) fn valid_text(v: &str, max: usize, n: &str) -> Result<(), SessionStateStoreError> {
    if v.is_empty() || v.len() > max || v.contains('\0') {
        Err(validation(format!("invalid {n}")))
    } else {
        Ok(())
    }
}
pub(super) fn validate_digest(v: &str) -> Result<(), SessionStateStoreError> {
    if v.strip_prefix("sha256:").is_none_or(|h| {
        h.len() != 64
            || !h
                .bytes()
                .all(|x| x.is_ascii_digit() || (b'a'..=b'f').contains(&x))
    }) {
        Err(validation("invalid sha256 content ID"))
    } else {
        Ok(())
    }
}
pub(super) fn digest(b: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(b))
}
pub(super) fn validation(e: impl std::fmt::Display) -> SessionStateStoreError {
    SessionStateStoreError::Validation(e.to_string())
}
pub(super) fn corrupt(e: impl std::fmt::Display) -> SessionStateStoreError {
    SessionStateStoreError::Corrupt(e.to_string())
}
pub(super) fn storage(e: impl std::fmt::Display) -> SessionStateStoreError {
    SessionStateStoreError::Storage(e.to_string())
}
pub(super) fn as_corrupt(e: SessionStateStoreError) -> SessionStateStoreError {
    corrupt(e)
}

pub(super) fn validate_suffix(
    successor: &SessionManifest,
    expected: Option<&SessionManifest>,
    suffix: &[SessionObject],
) -> Result<(), SessionStateStoreError> {
    let mut prev = expected.and_then(|x| x.head.clone());
    let mut sequence = expected.map_or(0, |x| x.segment_count);
    let mut bytes = expected.map_or(0, |x| x.transcript_bytes);
    for o in suffix {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| validation("segment count overflow"))?;
        if o.session != successor.session
            || o.generation != successor.generation
            || o.previous() != prev.as_ref()
            || o.sequence() != Some(sequence)
        {
            return Err(validation("suffix does not extend expected head exactly"));
        }
        bytes = bytes
            .checked_add(chain_bytes(&o.kind))
            .ok_or_else(|| validation("transcript byte overflow"))?;
        prev = Some(o.id.clone())
    }
    if successor.head != prev
        || successor.segment_count != sequence
        || successor.transcript_bytes != bytes
    {
        return Err(validation("successor counters or head do not match suffix"));
    }
    Ok(())
}
