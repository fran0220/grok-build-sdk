use super::codec::*;
use super::contracts::*;
use rusqlite::{OptionalExtension as _, TransactionBehavior};
use sha2::{Digest as _, Sha256};
use std::{path::PathBuf, sync::Mutex};

pub struct LocalSessionStateStore {
    path: PathBuf,
    lease_root: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}

struct LocalSessionStateLease {
    _file: std::fs::File,
}
impl SessionStateLease for LocalSessionStateLease {}

impl LocalSessionStateStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, SessionStateStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(storage)?;
        let path = root.join("native-session-log.sqlite3");
        let legacy_path = root.join("native-session-state.sqlite3");
        let existed = path.exists();
        if !existed && legacy_path.exists() {
            return Err(corrupt(
                "legacy whole-snapshot session state requires explicit migration or discard",
            ));
        }
        let mut c = rusqlite::Connection::open(&path).map_err(storage)?;
        c.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        if existed {
            inspect_schema(&c)?
        } else {
            let tx = c.transaction().map_err(storage)?;
            tx.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY NOT NULL,value TEXT NOT NULL);CREATE TABLE objects(session_identity TEXT NOT NULL,generation TEXT NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(session_identity,generation,id));CREATE TABLE manifests(session_identity TEXT PRIMARY KEY NOT NULL,generation TEXT NOT NULL,revision INTEGER NOT NULL,digest TEXT NOT NULL,size INTEGER NOT NULL,payload BLOB NOT NULL);CREATE TABLE tombstones(session_identity TEXT PRIMARY KEY NOT NULL,generation TEXT NOT NULL,revision INTEGER NOT NULL,prior_digest TEXT NOT NULL);").map_err(storage)?;
            tx.execute(
                "INSERT INTO metadata VALUES('schema_marker',?1),('schema_version',?2)",
                rusqlite::params![
                    SESSION_LOG_SCHEMA_MARKER,
                    SESSION_LOG_SCHEMA_VERSION.to_string()
                ],
            )
            .map_err(storage)?;
            tx.commit().map_err(storage)?
        }
        c.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;")
            .map_err(storage)?;
        let lease_root = root.join("session-leases");
        std::fs::create_dir_all(&lease_root).map_err(storage)?;
        Ok(Self {
            path,
            lease_root,
            connection: Mutex::new(c),
        })
    }
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl SessionStateStore for LocalSessionStateStore {
    fn acquire_session_lease(
        &self,
        key: &SessionKey,
    ) -> Result<Box<dyn SessionStateLease>, SessionStateStoreError> {
        use fs2::FileExt as _;
        let digest = Sha256::digest(key.session_identity().as_bytes());
        let path = self.lease_root.join(format!("{digest:x}.lock"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(storage)?;
        file.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                validation("Session identity is owned by another Runtime")
            } else {
                storage(error)
            }
        })?;
        Ok(Box::new(LocalSessionStateLease { _file: file }))
    }

    fn inspect_slot(&self, key: &SessionKey) -> Result<SessionSlot, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(storage)?;
        let out = inspect_tx(&tx, key)?;
        tx.commit().map_err(storage)?;
        Ok(out)
    }
    fn load_object(
        &self,
        key: &SessionKey,
        generation: &SessionGeneration,
        id: &SessionObjectId,
    ) -> Result<Option<SessionObject>, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(storage)?;
        let out = load_tx(&tx, key, generation, id)?;
        tx.commit().map_err(storage)?;
        Ok(out)
    }
    fn put_object(&self, o: SessionObject) -> Result<ObjectPut, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c.transaction().map_err(storage)?;
        let n = tx
            .execute(
                "INSERT OR IGNORE INTO objects VALUES(?1,?2,?3,?4,?5)",
                rusqlite::params![
                    o.session.0,
                    o.generation.0,
                    o.id.0,
                    o.bytes.len() as u64,
                    o.bytes
                ],
            )
            .map_err(storage)?;
        if n == 0 && load_tx(&tx, &o.session, &o.generation, &o.id)?.as_ref() != Some(&o) {
            return Err(corrupt("content ID collision"));
        }
        match tx.commit() {
            Ok(()) => Ok(if n == 0 {
                ObjectPut::AlreadyPresent
            } else {
                ObjectPut::Stored
            }),
            Err(_) => Ok(ObjectPut::CommitUnknown),
        }
    }
    fn compare_and_swap_manifest(
        &self,
        r: PreparedManifestCas,
    ) -> Result<ManifestCas, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        if matches!(
            inspect_header(&tx, &r.key)?,
            Header::Tombstone | Header::Live(_)
        ) != r.expected.is_some()
            || !current_matches(&tx, &r)?
        {
            return Ok(ManifestCas::Conflict);
        }
        let objects = r
            .suffix
            .iter()
            .map(|id| {
                load_tx(&tx, &r.key, &r.successor.manifest.generation, id)?
                    .ok_or_else(|| corrupt("staged suffix object missing"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_suffix(
            &r.successor.manifest,
            r.expected.as_ref().map(|x| &x.manifest),
            &objects,
        )?;
        validate_references(&tx, &objects)?;
        let m = &r.successor.manifest;
        tx.execute("INSERT INTO manifests VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(session_identity) DO UPDATE SET generation=excluded.generation,revision=excluded.revision,digest=excluded.digest,size=excluded.size,payload=excluded.payload",rusqlite::params![r.key.0,m.generation.0,r.successor.version.revision,r.successor.version.digest,m.bytes.len() as u64,m.bytes]).map_err(storage)?;
        match tx.commit() {
            Ok(()) => Ok(ManifestCas::Committed(r.successor)),
            Err(_) => Ok(ManifestCas::CommitUnknown),
        }
    }
    fn compare_and_delete(
        &self,
        r: PreparedSessionDelete,
    ) -> Result<SessionDelete, SessionStateStoreError> {
        let mut c = self.connection.lock().map_err(storage)?;
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let current = load_live_document(&tx, &r.key, false)?;
        if current.as_ref() != Some(&r.expected) {
            return Ok(SessionDelete::Conflict);
        }
        let receipt = r.receipt;
        tx.execute(
            "DELETE FROM manifests WHERE session_identity=?1",
            [&r.key.0],
        )
        .map_err(storage)?;
        tx.execute(
            "INSERT INTO tombstones VALUES(?1,?2,?3,?4)",
            rusqlite::params![
                r.key.0,
                receipt.generation.0,
                receipt.revision,
                receipt.prior_digest
            ],
        )
        .map_err(storage)?;
        match tx.commit() {
            Ok(()) => Ok(SessionDelete::Deleted(receipt)),
            Err(_) => Ok(SessionDelete::CommitUnknown),
        }
    }
}

enum Header {
    Vacant,
    Tombstone,
    Live(()),
}
fn inspect_header(
    c: &rusqlite::Connection,
    key: &SessionKey,
) -> Result<Header, SessionStateStoreError> {
    if c.query_row(
        "SELECT 1 FROM tombstones WHERE session_identity=?1",
        [&key.0],
        |r| r.get::<_, i64>(0),
    )
    .optional()
    .map_err(storage)?
    .is_some()
    {
        return Ok(Header::Tombstone);
    }
    Ok(
        if c.query_row(
            "SELECT 1 FROM manifests WHERE session_identity=?1",
            [&key.0],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .map_err(storage)?
        .is_some()
        {
            Header::Live(())
        } else {
            Header::Vacant
        },
    )
}
fn inspect_tx(
    c: &rusqlite::Connection,
    key: &SessionKey,
) -> Result<SessionSlot, SessionStateStoreError> {
    if let Some((g, r, d)) = c
        .query_row(
            "SELECT generation,revision,prior_digest FROM tombstones WHERE session_identity=?1",
            [&key.0],
            |x| {
                Ok((
                    x.get::<_, String>(0)?,
                    x.get::<_, i64>(1)?,
                    x.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(storage)?
    {
        return Ok(SessionSlot::Tombstoned {
            receipt: DeleteReceipt::from_stored(
                SessionGeneration::new(g).map_err(as_corrupt)?,
                u64::try_from(r).map_err(corrupt)?,
                d,
            )?,
        });
    }
    Ok(match load_live_document(c, key, true)? {
        None => SessionSlot::Vacant,
        Some(x) => SessionSlot::Live(x),
    })
}
fn load_live_document(
    c: &rusqlite::Connection,
    key: &SessionKey,
    traverse: bool,
) -> Result<Option<LiveSessionDocument>, SessionStateStoreError> {
    let row: Option<(String, i64, String, i64, i64)> = c
        .query_row(
            "SELECT generation,revision,digest,size,length(payload) FROM manifests WHERE session_identity=?1",
            [&key.0],
            |x| {
                Ok((
                    x.get(0)?,
                    x.get(1)?,
                    x.get(2)?,
                    x.get(3)?,
                    x.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(storage)?;
    let Some((g, r, d, size, payload_size)) = row else {
        return Ok(None);
    };
    let bytes = bounded_blob(
        c,
        "manifests",
        "session_identity",
        &key.0,
        size,
        payload_size,
        MAX_SESSION_MANIFEST_BYTES,
    )?;
    let m = SessionManifest::from_stored(bytes)?;
    if m.session != *key || m.generation.0 != g {
        return Err(corrupt("stored manifest identity mismatch"));
    }
    let live = LiveSessionDocument::from_stored(
        ManifestVersion::from_stored(u64::try_from(r).map_err(corrupt)?, d)?,
        m,
    )?;
    if traverse {
        traverse_manifest(c, &live.manifest)?
    }
    Ok(Some(live))
}
fn load_tx(
    c: &rusqlite::Connection,
    key: &SessionKey,
    g: &SessionGeneration,
    id: &SessionObjectId,
) -> Result<Option<SessionObject>, SessionStateStoreError> {
    let sizes: Option<(i64, i64)> = c
        .query_row(
            "SELECT size,length(payload) FROM objects WHERE session_identity=?1 AND generation=?2 AND id=?3",
            rusqlite::params![key.0, g.0, id.0],
            |x| Ok((x.get(0)?, x.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    let Some((size, payload_size)) = sizes else {
        return Ok(None);
    };
    let bytes = bounded_object_blob(c, key, g, id, size, payload_size)?;
    let o = SessionObject::from_stored(id.clone(), u64::try_from(size).map_err(corrupt)?, bytes)?;
    if o.session != *key || o.generation != *g {
        return Err(corrupt("stored object scope mismatch"));
    }
    Ok(Some(o))
}
fn bounded_object_blob(
    c: &rusqlite::Connection,
    k: &SessionKey,
    g: &SessionGeneration,
    id: &SessionObjectId,
    size: i64,
    payload_size: i64,
) -> Result<Vec<u8>, SessionStateStoreError> {
    if size < 0 || payload_size != size || size as usize > MAX_SESSION_OBJECT_BYTES {
        return Err(corrupt("invalid object declared size"));
    }
    c.query_row(
        "SELECT payload FROM objects WHERE session_identity=?1 AND generation=?2 AND id=?3",
        rusqlite::params![k.0, g.0, id.0],
        |x| x.get(0),
    )
    .map_err(storage)
}
fn bounded_blob(
    c: &rusqlite::Connection,
    table: &str,
    column: &str,
    value: &str,
    size: i64,
    payload_size: i64,
    max: usize,
) -> Result<Vec<u8>, SessionStateStoreError> {
    if size < 0 || payload_size != size || size as usize > max {
        return Err(corrupt("invalid declared size"));
    }
    c.query_row(
        &format!("SELECT payload FROM {table} WHERE {column}=?1"),
        [value],
        |x| x.get(0),
    )
    .map_err(storage)
}
fn traverse_manifest(
    c: &rusqlite::Connection,
    m: &SessionManifest,
) -> Result<(), SessionStateStoreError> {
    let mut next = m.head.clone();
    let mut expected = m.segment_count;
    let mut bytes = 0u64;
    while let Some(id) = next {
        if expected == 0 {
            return Err(corrupt("chain longer than segment_count"));
        }
        let o = load_tx(c, &m.session, &m.generation, &id)?
            .ok_or_else(|| corrupt("chain object missing"))?;
        if o.sequence() != Some(expected) {
            return Err(corrupt("chain sequence mismatch"));
        }
        validate_references(c, std::slice::from_ref(&o))?;
        bytes = bytes
            .checked_add(chain_bytes(&o.kind))
            .ok_or_else(|| corrupt("transcript counter overflow"))?;
        next = o.previous().cloned();
        expected -= 1
    }
    if expected != 0 || bytes != m.transcript_bytes {
        return Err(corrupt("manifest counters do not match chain"));
    }
    Ok(())
}
fn validate_references(
    c: &rusqlite::Connection,
    objects: &[SessionObject],
) -> Result<(), SessionStateStoreError> {
    for o in objects {
        match &o.kind {
            SessionObjectKind::CheckpointPublication { checkpoint, .. } => {
                match load_tx(c, &o.session, &o.generation, checkpoint)?.map(|x| x.kind) {
                    Some(SessionObjectKind::Checkpoint { .. }) => {}
                    _ => {
                        return Err(corrupt(
                            "checkpoint publication has missing or wrong-kind reference",
                        ));
                    }
                }
            }
            SessionObjectKind::CompactionPublication {
                checkpoint, record, ..
            } => {
                let checkpoint =
                    load_tx(c, &o.session, &o.generation, checkpoint)?.ok_or_else(|| {
                        corrupt("compaction publication has a missing checkpoint reference")
                    })?;
                let SessionObjectKind::Checkpoint { shell_bytes, .. } = checkpoint.kind else {
                    return Err(corrupt(
                        "compaction publication has a wrong-kind checkpoint reference",
                    ));
                };
                let facts = crate::CompactionContentFacts::from_bytes(
                    crate::COMPACTION_CHECKPOINT_DIGEST_DOMAIN,
                    &shell_bytes,
                    record.checkpoint.item_count,
                );
                if facts != record.checkpoint {
                    return Err(corrupt(
                        "compaction checkpoint differs from its publication facts",
                    ));
                }
            }
            SessionObjectKind::RewindPublication { operation, .. } => {
                match load_tx(c, &o.session, &o.generation, operation)?.map(|x| x.kind) {
                    Some(SessionObjectKind::RewindOperation { .. }) => {}
                    _ => {
                        return Err(corrupt(
                            "rewind publication has missing or wrong-kind reference",
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
fn current_matches(
    c: &rusqlite::Connection,
    r: &PreparedManifestCas,
) -> Result<bool, SessionStateStoreError> {
    Ok(match &r.expected {
        None => matches!(inspect_header(c, &r.key)?, Header::Vacant),
        Some(e) => load_live_document(c, &r.key, false)?.as_ref() == Some(e),
    })
}

fn inspect_schema(c: &rusqlite::Connection) -> Result<(), SessionStateStoreError> {
    let metadata: Vec<(String, String)> = c
        .prepare("SELECT key,value FROM metadata ORDER BY key")
        .map_err(|_| corrupt("existing database has no current metadata"))?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(storage)?
        .collect::<Result<_, _>>()
        .map_err(storage)?;
    if metadata
        != [
            ("schema_marker".into(), SESSION_LOG_SCHEMA_MARKER.into()),
            (
                "schema_version".into(),
                SESSION_LOG_SCHEMA_VERSION.to_string(),
            ),
        ]
    {
        return Err(corrupt("schema marker/version mismatch"));
    }
    for (table, expected) in [
        (
            "metadata",
            vec![("key", "TEXT", 1, 1), ("value", "TEXT", 1, 0)],
        ),
        (
            "objects",
            vec![
                ("session_identity", "TEXT", 1, 1),
                ("generation", "TEXT", 1, 2),
                ("id", "TEXT", 1, 3),
                ("size", "INTEGER", 1, 0),
                ("payload", "BLOB", 1, 0),
            ],
        ),
        (
            "manifests",
            vec![
                ("session_identity", "TEXT", 1, 1),
                ("generation", "TEXT", 1, 0),
                ("revision", "INTEGER", 1, 0),
                ("digest", "TEXT", 1, 0),
                ("size", "INTEGER", 1, 0),
                ("payload", "BLOB", 1, 0),
            ],
        ),
        (
            "tombstones",
            vec![
                ("session_identity", "TEXT", 1, 1),
                ("generation", "TEXT", 1, 0),
                ("revision", "INTEGER", 1, 0),
                ("prior_digest", "TEXT", 1, 0),
            ],
        ),
    ] {
        let mut s = c
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(storage)?;
        let got: Vec<(String, String, i64, i64)> = s
            .query_map([], |r| Ok((r.get(1)?, r.get(2)?, r.get(3)?, r.get(5)?)))
            .map_err(storage)?
            .collect::<Result<_, _>>()
            .map_err(storage)?;
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(a, b, n, p)| (a.into(), b.into(), n, p))
            .collect();
        if got != expected {
            return Err(corrupt(format!("{table} table constraints mismatch")));
        }
    }
    Ok(())
}
