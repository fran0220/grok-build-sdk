// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! The artifact custody contract, exercised the way a Host exercises it:
//! through the public façade, against the reference implementation, and against
//! a backend that hides damage from the caller.

use grok_build_sdk::{
    ArtifactDamage, ArtifactDigest, ArtifactError, ArtifactHandle, ArtifactId, ArtifactIntegrity,
    ArtifactLabel, ArtifactMaterialization, ArtifactMediaType, ArtifactObservation,
    ArtifactProvenance, ArtifactProvenanceKind, ArtifactPut, ArtifactReceipt, ArtifactRecord,
    ArtifactRecovery, ArtifactRetention, ArtifactUsage, ArtifactVault, ArtifactVaultHarness,
    ArtifactWrite, ConformanceOpen, LocalArtifactVault, MAX_ARTIFACT_BYTES,
    MAX_ARTIFACT_LABEL_BYTES, MAX_ARTIFACT_OBSERVATION_INPUTS, run_artifact_vault_conformance,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const NOW: u64 = 1_700_000_000_000;
const CONTENT: &[u8] = b"a durable artifact produced by an iteration";
const OTHER: &[u8] = b"a different artifact entirely";

fn label(value: &str) -> ArtifactLabel {
    ArtifactLabel::new(value).expect("valid provenance label")
}

fn provenance(kind: ArtifactProvenanceKind, iteration: u64) -> ArtifactProvenance {
    ArtifactProvenance::produced(
        kind,
        label("run_artifact_custody"),
        iteration,
        label("iteration.write_output"),
        NOW,
    )
    .expect("valid provenance")
}

fn write_of(kind: ArtifactProvenanceKind) -> ArtifactWrite {
    ArtifactWrite::new(
        ArtifactMediaType::new("text/plain").expect("valid media type"),
        ArtifactRetention::WhileProducerLives,
        provenance(kind, 2),
    )
    .retain_until(NOW + 3_600_000)
}

fn vault(root: &Path) -> LocalArtifactVault {
    LocalArtifactVault::new(root).expect("a fresh artifact vault opens")
}

fn stored(vault: &LocalArtifactVault, content: &[u8]) -> ArtifactReceipt {
    vault
        .put(
            content,
            &write_of(ArtifactProvenanceKind::ProducedOutput),
            NOW,
        )
        .expect("content is storable")
}

fn record_of(vault: &LocalArtifactVault, id: &ArtifactId) -> ArtifactRecord {
    vault
        .inspect(id)
        .expect("inspect succeeds")
        .expect("the artifact is stored")
}

/// Replaces the bytes of a stored artifact without touching its record, which
/// is the only way to produce the damage the contract promises to report.
fn damage_payload(root: &Path, id: &ArtifactId, damage: ArtifactDamage) {
    let connection = rusqlite::Connection::open(root.join("artifact-vault.sqlite3"))
        .expect("the vault database opens");
    let payload: Vec<u8> = connection
        .query_row(
            "SELECT payload FROM artifacts WHERE artifact_id=?1",
            [id.as_str()],
            |row| row.get(0),
        )
        .expect("the artifact has stored bytes");
    let damaged = match damage {
        ArtifactDamage::TamperContent => {
            let mut bytes = payload;
            let last = bytes.len() - 1;
            bytes[last] ^= 0xff;
            bytes
        }
        _ => payload[..payload.len() / 2].to_vec(),
    };
    connection
        .execute(
            "UPDATE artifacts SET payload=?2 WHERE artifact_id=?1",
            rusqlite::params![id.as_str(), damaged],
        )
        .expect("the stored bytes are replaceable underneath the contract");
}

struct LocalVaultHarness {
    root: tempfile::TempDir,
    destination: tempfile::TempDir,
}

impl LocalVaultHarness {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
            destination: tempfile::tempdir().unwrap(),
        }
    }
}

impl ArtifactVaultHarness for LocalVaultHarness {
    fn open(&mut self, _: ConformanceOpen) -> Result<Arc<dyn ArtifactVault>, ArtifactError> {
        Ok(Arc::new(LocalArtifactVault::new(self.root.path())?) as Arc<dyn ArtifactVault>)
    }

    fn materialization_root(&mut self) -> Result<PathBuf, ArtifactError> {
        Ok(self.destination.path().to_owned())
    }

    fn damage(&mut self, id: &ArtifactId, damage: ArtifactDamage) -> Result<(), ArtifactError> {
        damage_payload(self.root.path(), id, damage);
        Ok(())
    }
}

#[test]
fn the_reference_artifact_vault_passes_the_public_conformance() {
    let mut harness = LocalVaultHarness::new();
    run_artifact_vault_conformance(&mut harness)
        .expect("the reference vault satisfies its own published contract");
}

/// (1) Identity is derived from content and can never be reassigned, so a
/// handle a Host persisted last week still names exactly one byte sequence.
#[test]
fn an_artifact_identity_is_the_digest_of_its_content_and_cannot_be_reassigned() {
    let directory = tempfile::tempdir().unwrap();
    let vault = vault(directory.path());

    let handle = ArtifactHandle::for_content(CONTENT);
    assert_eq!(
        handle.digest(),
        &ArtifactDigest::of(CONTENT),
        "a handle's digest is the SHA-256 of the content it names"
    );
    assert_eq!(
        handle.id().as_str(),
        format!("sha256-{}", ArtifactDigest::of(CONTENT)),
        "an identity names the algorithm that derived it"
    );
    assert_eq!(handle.id().digest(), *handle.digest());
    assert_ne!(ArtifactHandle::for_content(OTHER), handle);
    assert!(handle.addresses(CONTENT) && !handle.addresses(OTHER));

    assert!(
        ArtifactHandle::new(handle.id().clone(), ArtifactDigest::of(OTHER)).is_err(),
        "an id and a digest that disagree must not form a handle"
    );
    assert!(ArtifactId::parse("sha256-not-a-digest").is_err());
    assert!(ArtifactId::parse(ArtifactDigest::of(CONTENT).as_str()).is_err());
    assert!(ArtifactId::parse("md5-0123456789abcdef").is_err());
    assert!(ArtifactDigest::parse("../etc/passwd").is_err());
    assert!(ArtifactDigest::parse(format!("{:A>64}", "")).is_err());

    let receipt = stored(&vault, CONTENT);
    assert_eq!(receipt.handle, handle);
    assert_eq!(receipt.outcome, ArtifactPut::Stored);
    assert_eq!(vault.read(&handle, NOW).unwrap(), CONTENT);
    assert_eq!(
        vault.verify(handle.id()).unwrap(),
        ArtifactIntegrity::Intact
    );
}

/// (1, 5) A stored copy that no longer addresses to its identity is reported,
/// never served, however it was damaged.
#[test]
fn tampered_and_truncated_content_fails_closed_as_corrupt_rather_than_being_served() {
    for damage in [
        ArtifactDamage::TamperContent,
        ArtifactDamage::TruncateContent,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let vault = vault(directory.path());
        let handle = stored(&vault, CONTENT).handle;
        damage_payload(directory.path(), handle.id(), damage);

        assert_eq!(
            vault.verify(handle.id()).unwrap(),
            ArtifactIntegrity::Corrupt,
            "{damage:?} must be visible to a custody probe"
        );
        assert!(
            matches!(vault.read(&handle, NOW), Err(ArtifactError::Corrupt(_))),
            "{damage:?} must fail the read rather than serve damaged bytes"
        );
        assert!(
            vault
                .materialize(&handle, &directory.path().join("copy"), NOW)
                .is_err(),
            "{damage:?} must not reach a materialized copy"
        );
        assert!(
            !directory.path().join("copy").exists(),
            "a refused materialization must not leave a partial file"
        );
        assert!(
            vault.inspect(handle.id()).unwrap().is_some(),
            "a damaged artifact must not look absent"
        );
    }
}

/// (2) Provenance names the three producer coordinates a Run has, and comes
/// back verbatim for a Host to display.
#[test]
fn provenance_names_the_producing_run_iteration_and_operation() {
    let directory = tempfile::tempdir().unwrap();
    let vault = vault(directory.path());
    let handle = stored(&vault, CONTENT).handle;

    let record = record_of(&vault, handle.id());
    assert_eq!(
        record.provenance.kind(),
        ArtifactProvenanceKind::ProducedOutput
    );
    assert_eq!(
        record.provenance.producer_run().as_str(),
        "run_artifact_custody"
    );
    assert_eq!(record.provenance.iteration(), 2);
    assert_eq!(
        record.provenance.operation().as_str(),
        "iteration.write_output"
    );
    assert_eq!(record.provenance.recorded_at_ms(), NOW);
    assert!(record.provenance.observation().is_none());

    // Every kind other than an observation is expressible, and the vocabulary
    // is closed: there is no constructor that takes a Host-invented kind.
    for kind in [
        ArtifactProvenanceKind::ProducedOutput,
        ArtifactProvenanceKind::ConsumedInput,
        ArtifactProvenanceKind::OperationRecord,
    ] {
        assert!(
            ArtifactProvenance::produced(kind, label("run_x"), 1, label("op"), NOW).is_ok(),
            "{kind:?} must be expressible without an observation"
        );
    }
    assert!(
        ArtifactProvenance::produced(
            ArtifactProvenanceKind::InstrumentObservation,
            label("run_x"),
            1,
            label("op"),
            NOW,
        )
        .is_err(),
        "an observation kind without an observation must not be constructible"
    );
    assert!(ArtifactLabel::new("").is_err());
    assert!(ArtifactLabel::new(" leading").is_err());
    assert!(ArtifactLabel::new("line\nbreak").is_err());
    assert!(ArtifactLabel::new("x".repeat(MAX_ARTIFACT_LABEL_BYTES + 1)).is_err());
}

/// (2) An instrument observation is evidence about an execution, so it carries
/// the program, the inputs it ran against, and the revision under observation.
#[test]
fn an_instrument_observation_records_the_execution_its_inputs_and_the_revision_under_observation() {
    let directory = tempfile::tempdir().unwrap();
    let vault = vault(directory.path());
    let input = stored(&vault, CONTENT).handle;
    let second = stored(&vault, OTHER).handle;

    let observation = ArtifactObservation::new(label("iteration.program"), label("revision-91ab"))
        .input(input.id().clone())
        .unwrap()
        .input(second.id().clone())
        .unwrap();
    let capture = b"\x89PNG\r\n\x1a\n captured frame bytes".to_vec();
    let receipt = vault
        .put(
            &capture,
            &ArtifactWrite::new(
                ArtifactMediaType::new("image/png").unwrap(),
                ArtifactRetention::Durable,
                ArtifactProvenance::observed(
                    label("run_artifact_custody"),
                    9,
                    label("iteration.capture"),
                    NOW + 5,
                    observation,
                )
                .unwrap(),
            ),
            NOW + 5,
        )
        .unwrap();

    let record = record_of(&vault, receipt.handle.id());
    let observed = record
        .provenance
        .observation()
        .expect("the observation survives storage");
    assert_eq!(
        record.provenance.kind(),
        ArtifactProvenanceKind::InstrumentObservation
    );
    assert_eq!(observed.program().as_str(), "iteration.program");
    assert_eq!(observed.revision().as_str(), "revision-91ab");
    assert_eq!(
        observed.inputs(),
        [input.id().clone(), second.id().clone()],
        "an observation cites the artifacts the observed execution ran against"
    );
    assert_eq!(record.media_type.as_str(), "image/png");
    assert_eq!(record.retention, ArtifactRetention::Durable);

    let mut crowded = ArtifactObservation::new(label("p"), label("r"));
    for index in 0..MAX_ARTIFACT_OBSERVATION_INPUTS {
        crowded = crowded
            .input(ArtifactId::for_content(format!("input-{index}").as_bytes()))
            .unwrap();
    }
    assert!(
        crowded
            .input(ArtifactId::for_content(b"one too many"))
            .is_err(),
        "an observation must not cite unbounded inputs"
    );
}

/// (3) What an artifact is and how long a Host means to keep it are declared
/// once, bounded by the contract, and returned on inspect.
#[test]
fn size_media_type_and_retention_hints_are_bounded_at_write_and_returned_on_inspect() {
    let directory = tempfile::tempdir().unwrap();
    let vault = vault(directory.path());

    for retention in [
        ArtifactRetention::Transient,
        ArtifactRetention::WhileProducerLives,
        ArtifactRetention::Durable,
    ] {
        assert_eq!(
            ArtifactRetention::parse(retention.as_str()).unwrap(),
            retention,
            "a retention hint must round-trip through its stored spelling"
        );
    }
    assert!(ArtifactRetention::parse("forever").is_err());

    let receipt = vault
        .put(
            CONTENT,
            &ArtifactWrite::new(
                ArtifactMediaType::new("application/json").unwrap(),
                ArtifactRetention::Transient,
                provenance(ArtifactProvenanceKind::ProducedOutput, 4),
            )
            .retain_until(NOW + 60_000),
            NOW + 1,
        )
        .unwrap();
    let record = record_of(&vault, receipt.handle.id());
    assert_eq!(record.size, CONTENT.len() as u64);
    assert_eq!(record.media_type.as_str(), "application/json");
    assert_eq!(record.retention, ArtifactRetention::Transient);
    assert_eq!(record.retain_until_ms, Some(NOW + 60_000));
    assert_eq!(record.written_at_ms, NOW + 1);
    assert_eq!(record.recovered_at_ms, None);

    assert!(ArtifactMediaType::new("").is_err());
    assert!(ArtifactMediaType::new("text").is_err());
    assert!(ArtifactMediaType::new("text/plain/extra").is_err());
    assert!(ArtifactMediaType::new("text/ plain").is_err());
    assert!(ArtifactMediaType::new(format!("text/{}", "x".repeat(300))).is_err());
    assert_eq!(
        ArtifactMediaType::octet_stream().as_str(),
        "application/octet-stream"
    );

    let oversize = vec![b'z'; MAX_ARTIFACT_BYTES + 1];
    assert!(
        matches!(
            vault.put(
                &oversize,
                &write_of(ArtifactProvenanceKind::ProducedOutput),
                NOW
            ),
            Err(ArtifactError::Validation(_))
        ),
        "the byte bound belongs to the contract, not to a backend"
    );
    assert!(
        vault
            .inspect(&ArtifactId::for_content(&oversize))
            .unwrap()
            .is_none(),
        "a refused write must leave no record"
    );
}

/// (4) A handle is immutable: identical content is one artifact and never
/// re-dates or re-attributes the one already there, and content that does not
/// address to a declared identity is refused before any storage effect.
#[test]
fn re_writing_identical_content_is_idempotent_and_different_content_under_one_identity_is_refused()
{
    let directory = tempfile::tempdir().unwrap();
    let vault = vault(directory.path());
    let handle = stored(&vault, CONTENT).handle;
    let before = record_of(&vault, handle.id());

    let repeat = vault
        .put(
            CONTENT,
            &ArtifactWrite::new(
                ArtifactMediaType::new("application/octet-stream").unwrap(),
                ArtifactRetention::Durable,
                provenance(ArtifactProvenanceKind::OperationRecord, 77),
            ),
            NOW + 10_000,
        )
        .unwrap();
    assert_eq!(repeat.outcome, ArtifactPut::AlreadyPresent);
    assert_eq!(repeat.handle, handle);
    assert_eq!(
        record_of(&vault, handle.id()),
        before,
        "a second write of identical bytes must leave the stored record untouched"
    );

    let mismatched =
        write_of(ArtifactProvenanceKind::ProducedOutput).expect_identity(handle.id().clone());
    assert!(
        matches!(
            vault.put(OTHER, &mismatched, NOW + 11_000),
            Err(ArtifactError::Validation(_))
        ),
        "different content under an existing identity must be refused"
    );
    assert!(
        vault
            .inspect(&ArtifactId::for_content(OTHER))
            .unwrap()
            .is_none(),
        "a refused write must not store its content under another identity"
    );
    assert_eq!(vault.read(&handle, NOW + 12_000).unwrap(), CONTENT);

    let honest = write_of(ArtifactProvenanceKind::ProducedOutput)
        .expect_identity(ArtifactId::for_content(OTHER));
    assert!(
        vault.put(OTHER, &honest, NOW + 13_000).is_ok(),
        "a declared identity that matches the content is an ordinary write"
    );
}

/// (5) Absence and damage are different answers because a Host can re-supply
/// the bytes for one and cannot for the other.
#[test]
fn a_missing_artifact_and_a_corrupt_artifact_are_different_answers() {
    let directory = tempfile::tempdir().unwrap();
    let vault = vault(directory.path());
    let absent = ArtifactHandle::for_content(b"never written to this vault");

    assert!(vault.inspect(absent.id()).unwrap().is_none());
    assert!(vault.usage(absent.id()).unwrap().is_none());
    assert_eq!(
        vault.verify(absent.id()).unwrap(),
        ArtifactIntegrity::Missing
    );
    assert!(matches!(
        vault.read(&absent, NOW),
        Err(ArtifactError::Missing(id)) if id == *absent.id()
    ));
    assert!(matches!(
        vault.recover(absent.id(), b"never written to this vault", NOW),
        Err(ArtifactError::Missing(_))
    ));

    let handle = stored(&vault, CONTENT).handle;
    damage_payload(directory.path(), handle.id(), ArtifactDamage::TamperContent);
    assert_eq!(
        vault.verify(handle.id()).unwrap(),
        ArtifactIntegrity::Corrupt
    );
    assert!(matches!(
        vault.read(&handle, NOW),
        Err(ArtifactError::Corrupt(_))
    ));
}

/// (5) Recovery is explicit, preserves identity and never happens inside a read.
#[test]
fn a_corrupt_artifact_is_re_materialized_only_by_an_explicit_recovery_that_preserves_identity() {
    let directory = tempfile::tempdir().unwrap();
    let vault = vault(directory.path());
    let handle = stored(&vault, CONTENT).handle;
    let before = record_of(&vault, handle.id());

    assert_eq!(
        vault.recover(handle.id(), CONTENT, NOW + 1).unwrap(),
        ArtifactRecovery::AlreadyIntact,
        "an intact artifact must not be replaced"
    );
    assert_eq!(record_of(&vault, handle.id()), before);

    damage_payload(directory.path(), handle.id(), ArtifactDamage::TamperContent);
    assert!(
        matches!(vault.read(&handle, NOW + 2), Err(ArtifactError::Corrupt(_))),
        "a read must never quietly repair what it finds"
    );
    assert!(
        matches!(
            vault.recover(handle.id(), OTHER, NOW + 3),
            Err(ArtifactError::Validation(_))
        ),
        "recovery must not be able to change what an identity means"
    );
    assert_eq!(
        vault.verify(handle.id()).unwrap(),
        ArtifactIntegrity::Corrupt,
        "a refused recovery must leave the damaged artifact exactly as it was"
    );

    assert_eq!(
        vault.recover(handle.id(), CONTENT, NOW + 4).unwrap(),
        ArtifactRecovery::Restored
    );
    assert_eq!(vault.read(&handle, NOW + 5).unwrap(), CONTENT);
    let after = record_of(&vault, handle.id());
    assert_eq!(after.handle, before.handle);
    assert_eq!(after.provenance, before.provenance);
    assert_eq!(after.media_type, before.media_type);
    assert_eq!(after.written_at_ms, before.written_at_ms);
    assert_eq!(
        after.recovered_at_ms,
        Some(NOW + 4),
        "recovery is recorded rather than hidden"
    );
}

/// (6) Usage is durable accounting, not an in-memory counter, and a refused
/// read is not a use.
#[test]
fn read_and_materialization_counters_are_durable_and_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let handle = {
        let vault = vault(directory.path());
        let handle = stored(&vault, CONTENT).handle;
        assert_eq!(
            vault.usage(handle.id()).unwrap().unwrap(),
            ArtifactUsage::default(),
            "a freshly written artifact has been used zero times"
        );

        vault.read(&handle, NOW + 1).unwrap();
        vault.read(&handle, NOW + 2).unwrap();
        vault
            .materialize(&handle, &directory.path().join("copy"), NOW + 3)
            .unwrap();
        let usage = vault.usage(handle.id()).unwrap().unwrap();
        assert_eq!(usage.reads, 2);
        assert_eq!(usage.materializations, 1);
        assert_eq!(usage.bytes_served, 3 * CONTENT.len() as u64);
        assert_eq!(usage.last_used_at_ms, Some(NOW + 3));
        handle
    };

    let reopened = vault(directory.path());
    let usage = reopened.usage(handle.id()).unwrap().unwrap();
    assert_eq!(usage.reads, 2);
    assert_eq!(usage.materializations, 1);
    assert_eq!(usage.bytes_served, 3 * CONTENT.len() as u64);

    damage_payload(directory.path(), handle.id(), ArtifactDamage::TamperContent);
    assert!(reopened.read(&handle, NOW + 4).is_err());
    assert_eq!(
        reopened.usage(handle.id()).unwrap().unwrap(),
        usage,
        "a read that served nothing must not be counted as a use"
    );
}

/// (7) Two vaults on one root are one authority — the case a Host hits when a
/// second worker, a background service or a restarted process overlaps with the
/// first.
#[test]
fn two_vaults_on_one_root_observe_each_other_and_converge_on_one_artifact() {
    let directory = tempfile::tempdir().unwrap();
    let first = vault(directory.path());
    let second = vault(directory.path());

    let written = stored(&first, CONTENT).handle;
    assert_eq!(second.read(&written, NOW).unwrap(), CONTENT);
    let from_second = stored(&second, OTHER).handle;
    assert_eq!(first.read(&from_second, NOW).unwrap(), OTHER);

    let contended = b"content both workers decide to write at the same instant".to_vec();
    let outcomes: Vec<ArtifactPut> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|index| {
                let root = directory.path().to_owned();
                let contended = contended.clone();
                scope.spawn(move || {
                    let vault = LocalArtifactVault::new(&root).unwrap();
                    vault
                        .put(
                            &contended,
                            &write_of(ArtifactProvenanceKind::ProducedOutput),
                            NOW + index,
                        )
                        .unwrap()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .map(|receipt| {
                assert_eq!(receipt.handle, ArtifactHandle::for_content(&contended));
                receipt.outcome
            })
            .collect()
    });
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == ArtifactPut::Stored)
            .count(),
        1,
        "eight workers writing identical content must converge on one stored artifact"
    );

    let handle = ArtifactHandle::for_content(&contended);
    assert_eq!(first.read(&handle, NOW + 100).unwrap(), contended);
    assert_eq!(
        first.verify(handle.id()).unwrap(),
        ArtifactIntegrity::Intact,
        "contended writes must converge without corrupting the content"
    );
}

/// (7) A materialized copy is proven on disk, not merely written.
#[test]
fn materializing_to_a_path_produces_a_verified_copy() {
    let directory = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let vault = vault(directory.path());
    let handle = stored(&vault, CONTENT).handle;

    let path = destination.path().join("nested").join("artifact.bin");
    let ArtifactMaterialization {
        path: written,
        size,
        digest,
    } = vault.materialize(&handle, &path, NOW).unwrap();
    assert_eq!(written, path);
    assert_eq!(size, CONTENT.len() as u64);
    assert_eq!(&digest, handle.digest());
    assert_eq!(std::fs::read(&path).unwrap(), CONTENT);

    // A second worker materializes the same artifact over the same path and
    // still gets a verified copy rather than a merge of two writes.
    let concurrent = vault_from(directory.path());
    concurrent.materialize(&handle, &path, NOW + 1).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), CONTENT);

    assert!(
        vault
            .materialize(&handle, destination.path(), NOW + 2)
            .is_err(),
        "a directory is not a materialization destination"
    );
    assert_eq!(
        vault.usage(handle.id()).unwrap().unwrap().materializations,
        2,
        "only the materializations that produced a verified copy are counted"
    );
}

fn vault_from(root: &Path) -> LocalArtifactVault {
    LocalArtifactVault::new(root).expect("a second handle on one root opens")
}

/// A vault whose schema is not this contract's schema stops the Host rather
/// than presenting foreign rows as artifacts.
#[test]
fn the_vault_fails_closed_on_a_foreign_or_damaged_store() {
    let foreign = tempfile::tempdir().unwrap();
    let connection =
        rusqlite::Connection::open(foreign.path().join("artifact-vault.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
             INSERT INTO metadata(key,value) VALUES('schema_marker','another.product.artifacts');
             INSERT INTO metadata(key,value) VALUES('schema_version','1');",
        )
        .unwrap();
    drop(connection);
    assert!(
        matches!(
            LocalArtifactVault::new(foreign.path()),
            Err(ArtifactError::Corrupt(_))
        ),
        "a foreign schema marker must fail the open"
    );

    let unmarked = tempfile::tempdir().unwrap();
    let connection =
        rusqlite::Connection::open(unmarked.path().join("artifact-vault.sqlite3")).unwrap();
    connection
        .execute_batch("CREATE TABLE unrelated(x INTEGER);")
        .unwrap();
    drop(connection);
    assert!(
        matches!(
            LocalArtifactVault::new(unmarked.path()),
            Err(ArtifactError::Corrupt(_))
        ),
        "an existing store with no schema metadata must fail the open"
    );

    let damaged = tempfile::tempdir().unwrap();
    let vault = vault(damaged.path());
    let handle = stored(&vault, CONTENT).handle;
    drop(vault);
    let connection =
        rusqlite::Connection::open(damaged.path().join("artifact-vault.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE artifacts SET provenance='{\"kind\":\"produced_output\"}' WHERE artifact_id=?1",
            [handle.id().as_str()],
        )
        .unwrap();
    drop(connection);
    let reopened = vault_from(damaged.path());
    assert!(
        matches!(
            reopened.inspect(handle.id()),
            Err(ArtifactError::Corrupt(_))
        ),
        "an undecodable provenance record must fail the read rather than attribute the artifact to invented work"
    );
}

/// (8) A backend that answers "intact" for content it can no longer prove is
/// exactly the silent data loss this contract exists to prevent.
#[test]
fn a_backend_that_hides_damage_fails_the_artifact_vault_conformance() {
    /// Delegates every honest answer to the reference vault, but keeps its own
    /// pristine copy and serves it whenever the reference reports damage — so
    /// custody looks unbroken to the caller while the stored copy is gone.
    struct DamageHidingVault {
        inner: LocalArtifactVault,
        shadow: Arc<Mutex<HashMap<ArtifactId, Vec<u8>>>>,
    }

    impl ArtifactVault for DamageHidingVault {
        fn put(
            &self,
            content: &[u8],
            write: &ArtifactWrite,
            now_ms: u64,
        ) -> Result<ArtifactReceipt, ArtifactError> {
            let receipt = self.inner.put(content, write, now_ms)?;
            self.shadow
                .lock()
                .unwrap()
                .insert(receipt.handle.id().clone(), content.to_vec());
            Ok(receipt)
        }

        fn inspect(&self, id: &ArtifactId) -> Result<Option<ArtifactRecord>, ArtifactError> {
            self.inner.inspect(id)
        }

        fn read(&self, handle: &ArtifactHandle, now_ms: u64) -> Result<Vec<u8>, ArtifactError> {
            match self.inner.read(handle, now_ms) {
                Err(ArtifactError::Corrupt(_)) => Ok(self
                    .shadow
                    .lock()
                    .unwrap()
                    .get(handle.id())
                    .cloned()
                    .unwrap_or_default()),
                other => other,
            }
        }

        fn verify(&self, id: &ArtifactId) -> Result<ArtifactIntegrity, ArtifactError> {
            Ok(match self.inner.verify(id)? {
                ArtifactIntegrity::Corrupt => ArtifactIntegrity::Intact,
                honest => honest,
            })
        }

        fn recover(
            &self,
            id: &ArtifactId,
            content: &[u8],
            now_ms: u64,
        ) -> Result<ArtifactRecovery, ArtifactError> {
            self.inner.recover(id, content, now_ms)
        }

        fn materialize(
            &self,
            handle: &ArtifactHandle,
            destination: &Path,
            now_ms: u64,
        ) -> Result<ArtifactMaterialization, ArtifactError> {
            self.inner.materialize(handle, destination, now_ms)
        }
    }

    struct HidingHarness {
        root: tempfile::TempDir,
        destination: tempfile::TempDir,
        shadow: Arc<Mutex<HashMap<ArtifactId, Vec<u8>>>>,
    }

    impl ArtifactVaultHarness for HidingHarness {
        fn open(&mut self, _: ConformanceOpen) -> Result<Arc<dyn ArtifactVault>, ArtifactError> {
            Ok(Arc::new(DamageHidingVault {
                inner: LocalArtifactVault::new(self.root.path())?,
                shadow: Arc::clone(&self.shadow),
            }) as Arc<dyn ArtifactVault>)
        }

        fn materialization_root(&mut self) -> Result<PathBuf, ArtifactError> {
            Ok(self.destination.path().to_owned())
        }

        fn damage(&mut self, id: &ArtifactId, damage: ArtifactDamage) -> Result<(), ArtifactError> {
            damage_payload(self.root.path(), id, damage);
            Ok(())
        }
    }

    let mut harness = HidingHarness {
        root: tempfile::tempdir().unwrap(),
        destination: tempfile::tempdir().unwrap(),
        shadow: Arc::new(Mutex::new(HashMap::new())),
    };
    let error = run_artifact_vault_conformance(&mut harness)
        .expect_err("a backend that hides damage cannot pass");
    assert!(
        error.to_string().contains("conformance"),
        "the suite should name itself in the failure: {error}"
    );
}
