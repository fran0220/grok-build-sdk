// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use super::{
    ArtifactDigest, ArtifactError, ArtifactHandle, ArtifactId, ArtifactIntegrity, ArtifactLabel,
    ArtifactMediaType, ArtifactObservation, ArtifactProvenance, ArtifactProvenanceKind,
    ArtifactPut, ArtifactRecord, ArtifactRecovery, ArtifactRetention, ArtifactVault, ArtifactWrite,
    MAX_ARTIFACT_BYTES,
};
use crate::session_state::ConformanceOpen;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

type Vault = Arc<dyn ArtifactVault>;

/// A way to damage the physical copy of an artifact, underneath the contract.
///
/// Corruption is not reachable through the public API by design, so a backend
/// that claims to detect it has to be able to cause it. Both variants leave the
/// artifact's record intact and change only the bytes, which is exactly the
/// failure a Host must be told about rather than served.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactDamage {
    /// Replace the stored bytes with different bytes of the same length.
    TamperContent,
    /// Shorten the stored bytes.
    TruncateContent,
}

/// What the conformance suite needs from a backend beyond the contract itself.
///
/// A Host implements this once, against its own storage, to prove its vault has
/// the same semantics as [`super::LocalArtifactVault`].
pub trait ArtifactVaultHarness {
    /// Answers a handle onto one authority for each phase. `Fresh` and
    /// `Concurrent` must observe each other's writes; `Reopen` is taken after
    /// the earlier handles were dropped and stands in for a restart.
    fn open(&mut self, phase: ConformanceOpen) -> Result<Vault, ArtifactError>;

    /// A directory the suite may write materialized copies into. It must be
    /// outside the vault's own storage.
    fn materialization_root(&mut self) -> Result<PathBuf, ArtifactError>;

    /// Damages the stored bytes of an artifact that is known to exist, without
    /// going through the contract.
    fn damage(&mut self, id: &ArtifactId, damage: ArtifactDamage) -> Result<(), ArtifactError>;
}

fn suite_error(message: impl Into<String>) -> ArtifactError {
    ArtifactError::Corrupt(format!("conformance: {}", message.into()))
}

fn require(condition: bool, message: impl Into<String>) -> Result<(), ArtifactError> {
    if condition {
        Ok(())
    } else {
        Err(suite_error(message))
    }
}

fn expect<T>(value: Option<T>, message: &str) -> Result<T, ArtifactError> {
    value.ok_or_else(|| suite_error(message))
}

const BASE_MS: u64 = 1_700_000_000_000;
const ALPHA: &[u8] = b"conformance artifact alpha";
const BETA: &[u8] = b"conformance artifact beta, written by the other worker";
const SHARED: &[u8] = b"conformance artifact written by both workers at once";

fn label(value: &str) -> Result<ArtifactLabel, ArtifactError> {
    ArtifactLabel::new(value)
}

fn produced_write(
    kind: ArtifactProvenanceKind,
    operation: &str,
    iteration: u64,
) -> Result<ArtifactWrite, ArtifactError> {
    Ok(ArtifactWrite::new(
        ArtifactMediaType::new("text/plain")?,
        ArtifactRetention::WhileProducerLives,
        ArtifactProvenance::produced(
            kind,
            label("run_artifact_conformance")?,
            iteration,
            label(operation)?,
            BASE_MS,
        )?,
    )
    .retain_until(BASE_MS + 86_400_000))
}

/// Runs the backend-neutral artifact custody contract against independent
/// handles to one authority.
///
/// The contract this proves is the one a Host depends on when it shows a person
/// an artifact and hands the same artifact to another worker: identity is the
/// content and cannot be reassigned, a handle names one byte sequence forever,
/// provenance and declared hints come back exactly as written, a damaged copy
/// is reported rather than served and can only be repaired by an explicit
/// recovery that preserves identity, usage is durably counted, and two workers
/// on one authority see each other's writes and converge when they race.
pub fn run_artifact_vault_conformance<H: ArtifactVaultHarness>(
    harness: &mut H,
) -> Result<(), ArtifactError> {
    let vault = harness.open(ConformanceOpen::Fresh)?;
    let alpha = identity_is_the_content(&vault)?;
    let record = declarations_are_stored_verbatim(&vault, &alpha)?;
    provenance_records_its_producer(&vault, &alpha, &record)?;
    a_handle_names_one_byte_sequence(&vault, &alpha)?;
    absence_is_not_damage(&vault)?;
    usage_is_counted(&vault, &alpha, record.size)?;

    let concurrent = harness.open(ConformanceOpen::Concurrent)?;
    two_workers_share_one_authority(&vault, &concurrent, &alpha)?;
    let destination = harness.materialization_root()?.join("alpha.materialized");
    materialization_is_verified(&concurrent, &alpha, &destination)?;

    let refused = harness.materialization_root()?.join("never.materialized");
    damage_is_reported_and_recovery_is_explicit(&vault, harness, &alpha, &refused)?;

    let before_restart = expect(vault.inspect(alpha.id())?, "alpha vanished before restart")?;
    drop(concurrent);
    drop(vault);

    let reopened = harness.open(ConformanceOpen::Reopen)?;
    let after_restart = expect(
        reopened.inspect(alpha.id())?,
        "an artifact did not survive a restart",
    )?;
    require(
        after_restart == before_restart,
        "artifact custody did not survive a restart unchanged",
    )?;
    require(
        reopened.read(&alpha, BASE_MS + 9_000)? == ALPHA,
        "a recovered artifact did not read back after a restart",
    )?;
    require(
        expect(
            reopened.usage(alpha.id())?,
            "usage vanished across a restart",
        )?
        .reads
            == before_restart.usage.reads + 1,
        "usage accounting did not continue from its stored value after a restart",
    )?;
    Ok(())
}

/// An artifact is named by its content and by nothing else, so the same bytes
/// are one artifact and different bytes can never share an identity.
fn identity_is_the_content(vault: &Vault) -> Result<ArtifactHandle, ArtifactError> {
    let handle = ArtifactHandle::for_content(ALPHA);
    require(
        handle.id() == &ArtifactId::from_digest(&ArtifactDigest::of(ALPHA)),
        "an artifact id is not derived from the digest of its content",
    )?;
    require(
        handle.digest().as_str() == format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(ALPHA)),
        "an artifact digest is not the SHA-256 of its content",
    )?;
    require(
        ArtifactHandle::for_content(ALPHA) == handle && ArtifactHandle::for_content(BETA) != handle,
        "content addressing did not separate two different payloads",
    )?;
    require(
        ArtifactHandle::new(handle.id().clone(), ArtifactDigest::of(BETA)).is_err(),
        "a handle whose id and digest disagree was constructible",
    )?;

    require(
        vault.inspect(handle.id())?.is_none(),
        "a fresh authority already holds an artifact",
    )?;
    let receipt = vault.put(
        ALPHA,
        &produced_write(
            ArtifactProvenanceKind::ProducedOutput,
            "conformance.produce",
            3,
        )?,
        BASE_MS,
    )?;
    require(
        receipt.outcome == ArtifactPut::Stored && receipt.handle == handle,
        "the first write of an artifact did not store it under its own identity",
    )?;
    require(
        vault.read(&handle, BASE_MS + 1)? == ALPHA,
        "an artifact did not read back unchanged",
    )?;
    Ok(handle)
}

/// Size, media type and retention hints are declared once and returned exactly,
/// and the contract's byte bound is the contract's rather than the backend's.
fn declarations_are_stored_verbatim(
    vault: &Vault,
    alpha: &ArtifactHandle,
) -> Result<ArtifactRecord, ArtifactError> {
    let record = expect(
        vault.inspect(alpha.id())?,
        "a stored artifact did not inspect",
    )?;
    require(
        record.handle == *alpha && record.size == ALPHA.len() as u64,
        "an inspected artifact misreported its identity or size",
    )?;
    require(
        record.media_type.as_str() == "text/plain"
            && record.retention == ArtifactRetention::WhileProducerLives
            && record.retain_until_ms == Some(BASE_MS + 86_400_000),
        "declared media type or retention hints were not returned on inspect",
    )?;
    require(
        record.written_at_ms == BASE_MS && record.recovered_at_ms.is_none(),
        "a freshly written artifact did not record the instant it was written",
    )?;

    let oversize = vec![b'x'; MAX_ARTIFACT_BYTES + 1];
    require(
        vault
            .put(
                &oversize,
                &produced_write(
                    ArtifactProvenanceKind::ProducedOutput,
                    "conformance.oversize",
                    3,
                )?,
                BASE_MS,
            )
            .is_err(),
        "an artifact over the published byte bound was accepted",
    )?;
    require(
        vault
            .inspect(&ArtifactId::for_content(&oversize))?
            .is_none(),
        "a refused oversize write left a record behind",
    )?;
    Ok(record)
}

/// Provenance names the producer that a Host later shows, and an instrument
/// observation additionally names the execution it recorded.
fn provenance_records_its_producer(
    vault: &Vault,
    alpha: &ArtifactHandle,
    record: &ArtifactRecord,
) -> Result<(), ArtifactError> {
    let provenance = &record.provenance;
    require(
        provenance.kind() == ArtifactProvenanceKind::ProducedOutput
            && provenance.producer_run().as_str() == "run_artifact_conformance"
            && provenance.iteration() == 3
            && provenance.operation().as_str() == "conformance.produce"
            && provenance.recorded_at_ms() == BASE_MS
            && provenance.observation().is_none(),
        "an artifact did not return the provenance it was written with",
    )?;

    // An observation is meaningless without what it observed, and an ordinary
    // output must not pretend to be one.
    require(
        ArtifactProvenance::produced(
            ArtifactProvenanceKind::InstrumentObservation,
            label("run_artifact_conformance")?,
            4,
            label("conformance.observe")?,
            BASE_MS,
        )
        .is_err(),
        "an instrument observation was constructible without what it observed",
    )?;

    let observation =
        ArtifactObservation::new(label("conformance.program")?, label("revision-7f3c")?)
            .input(alpha.id().clone())?;
    let write = ArtifactWrite::new(
        ArtifactMediaType::new("image/png")?,
        ArtifactRetention::Durable,
        ArtifactProvenance::observed(
            label("run_artifact_conformance")?,
            4,
            label("conformance.observe")?,
            BASE_MS + 20,
            observation,
        )?,
    );
    let capture = b"conformance instrument capture bytes".to_vec();
    let receipt = vault.put(&capture, &write, BASE_MS + 20)?;
    let stored = expect(
        vault.inspect(receipt.handle.id())?,
        "an instrument observation did not inspect",
    )?;
    let observed = expect(
        stored.provenance.observation().cloned(),
        "an instrument observation lost what it observed",
    )?;
    require(
        stored.provenance.kind() == ArtifactProvenanceKind::InstrumentObservation
            && observed.program().as_str() == "conformance.program"
            && observed.revision().as_str() == "revision-7f3c"
            && observed.inputs() == [alpha.id().clone()]
            && stored.media_type.as_str() == "image/png"
            && stored.retention == ArtifactRetention::Durable,
        "an instrument observation did not return the execution, revision and inputs it recorded",
    )?;
    Ok(())
}

/// The bytes behind a handle never change: an identical write is idempotent and
/// leaves the record alone, and a write that declares an identity its content
/// does not have is refused.
fn a_handle_names_one_byte_sequence(
    vault: &Vault,
    alpha: &ArtifactHandle,
) -> Result<(), ArtifactError> {
    let before = expect(vault.inspect(alpha.id())?, "alpha vanished")?;
    let rewrite = ArtifactWrite::new(
        ArtifactMediaType::new("application/json")?,
        ArtifactRetention::Transient,
        ArtifactProvenance::produced(
            ArtifactProvenanceKind::OperationRecord,
            label("run_artifact_conformance_other")?,
            99,
            label("conformance.rewrite")?,
            BASE_MS + 500,
        )?,
    );
    let receipt = vault.put(ALPHA, &rewrite, BASE_MS + 500)?;
    require(
        receipt.outcome == ArtifactPut::AlreadyPresent && receipt.handle == *alpha,
        "re-writing identical content was not idempotent",
    )?;
    let after = expect(vault.inspect(alpha.id())?, "alpha vanished after a rewrite")?;
    require(
        after.media_type == before.media_type
            && after.retention == before.retention
            && after.provenance == before.provenance
            && after.written_at_ms == before.written_at_ms,
        "a second write of identical content re-labelled or re-attributed the stored artifact",
    )?;

    let mismatched = ArtifactWrite::new(
        ArtifactMediaType::new("text/plain")?,
        ArtifactRetention::Transient,
        ArtifactProvenance::produced(
            ArtifactProvenanceKind::ProducedOutput,
            label("run_artifact_conformance")?,
            5,
            label("conformance.mismatch")?,
            BASE_MS + 600,
        )?,
    )
    .expect_identity(alpha.id().clone());
    require(
        vault.put(BETA, &mismatched, BASE_MS + 600).is_err(),
        "different content was accepted under an existing declared identity",
    )?;
    require(
        vault.read(alpha, BASE_MS + 601)? == ALPHA,
        "a refused write changed the content behind an existing handle",
    )?;
    require(
        vault.inspect(&ArtifactId::for_content(BETA))?.is_none(),
        "a refused write stored its content under another identity anyway",
    )?;
    Ok(())
}

/// An identity that was never stored is answered as absent, never as damage and
/// never as empty content.
fn absence_is_not_damage(vault: &Vault) -> Result<(), ArtifactError> {
    let unknown = ArtifactHandle::for_content(b"conformance artifact that was never written");
    require(
        vault.inspect(unknown.id())?.is_none() && vault.usage(unknown.id())?.is_none(),
        "an authority answered for an artifact it never stored",
    )?;
    require(
        vault.verify(unknown.id())? == ArtifactIntegrity::Missing,
        "an unstored artifact was not reported missing",
    )?;
    require(
        matches!(
            vault.read(&unknown, BASE_MS + 700),
            Err(ArtifactError::Missing(_))
        ),
        "reading an unstored artifact was not answered as missing",
    )?;
    require(
        matches!(
            vault.recover(
                unknown.id(),
                b"conformance artifact that was never written",
                BASE_MS
            ),
            Err(ArtifactError::Missing(_))
        ),
        "recovery invented an artifact the authority had never accepted",
    )?;
    Ok(())
}

/// Reads and materializations are counted durably, and a refused read is not a
/// use.
fn usage_is_counted(vault: &Vault, alpha: &ArtifactHandle, size: u64) -> Result<(), ArtifactError> {
    let before = expect(
        vault.usage(alpha.id())?,
        "a stored artifact has no usage record",
    )?;
    vault.read(alpha, BASE_MS + 1_000)?;
    let after = expect(vault.usage(alpha.id())?, "usage vanished after a read")?;
    require(
        after.reads == before.reads + 1
            && after.bytes_served == before.bytes_served + size
            && after.last_used_at_ms == Some(BASE_MS + 1_000)
            && after.materializations == before.materializations,
        "a read was not counted as exactly one read",
    )?;
    require(
        expect(vault.inspect(alpha.id())?, "alpha vanished")?.usage == after,
        "usage reported by inspect disagrees with usage reported directly",
    )?;
    require(
        vault.verify(alpha.id())? == ArtifactIntegrity::Intact
            && expect(vault.usage(alpha.id())?, "usage vanished")? == after,
        "probing custody was counted as a use",
    )?;
    Ok(())
}

/// Two handles onto one authority are one authority: each observes the other's
/// writes, and two writers of the same content converge on one artifact.
fn two_workers_share_one_authority(
    vault: &Vault,
    concurrent: &Vault,
    alpha: &ArtifactHandle,
) -> Result<(), ArtifactError> {
    require(
        concurrent.read(alpha, BASE_MS + 2_000)? == ALPHA,
        "a concurrent handle did not observe an artifact the first handle wrote",
    )?;
    let beta = concurrent.put(
        BETA,
        &produced_write(
            ArtifactProvenanceKind::ConsumedInput,
            "conformance.import",
            6,
        )?,
        BASE_MS + 2_100,
    )?;
    require(
        beta.outcome == ArtifactPut::Stored,
        "a concurrent handle could not write",
    )?;
    require(
        vault.read(&beta.handle, BASE_MS + 2_200)? == BETA,
        "the first handle did not observe a concurrent handle's write",
    )?;

    let write = produced_write(
        ArtifactProvenanceKind::ProducedOutput,
        "conformance.race",
        7,
    )?;
    let first = vault.put(SHARED, &write, BASE_MS + 2_300)?;
    let second = concurrent.put(SHARED, &write, BASE_MS + 2_300)?;
    require(
        first.handle == second.handle && first.handle == ArtifactHandle::for_content(SHARED),
        "two writers of identical content produced two identities",
    )?;
    let outcomes = [first.outcome, second.outcome];
    require(
        outcomes == [ArtifactPut::Stored, ArtifactPut::AlreadyPresent],
        format!(
            "two writers of identical content answered {outcomes:?} rather than one store and one already-present"
        ),
    )?;
    require(
        concurrent.read(&first.handle, BASE_MS + 2_400)? == SHARED
            && vault.verify(first.handle.id())? == ArtifactIntegrity::Intact,
        "content written by two workers at once did not converge intact",
    )?;
    Ok(())
}

/// A materialized copy is a verified copy: the bytes on the path address to the
/// artifact's digest, and the materialization is counted.
fn materialization_is_verified(
    vault: &Vault,
    alpha: &ArtifactHandle,
    destination: &Path,
) -> Result<(), ArtifactError> {
    let before = expect(vault.usage(alpha.id())?, "alpha has no usage record")?;
    let materialized = vault.materialize(alpha, destination, BASE_MS + 3_000)?;
    require(
        materialized.path == destination
            && materialized.size == ALPHA.len() as u64
            && materialized.digest == *alpha.digest(),
        "a materialization misdescribed the copy it made",
    )?;
    let on_disk = std::fs::read(destination)
        .map_err(|error| suite_error(format!("a materialized copy is unreadable: {error}")))?;
    require(
        on_disk == ALPHA && ArtifactHandle::for_content(&on_disk) == *alpha,
        "a materialized copy is not the content it claims to be",
    )?;
    let after = expect(
        vault.usage(alpha.id())?,
        "usage vanished after materialization",
    )?;
    require(
        after.materializations == before.materializations + 1
            && after.reads == before.reads
            && after.last_used_at_ms == Some(BASE_MS + 3_000),
        "a materialization was not counted as exactly one materialization",
    )?;

    // Materializing over an existing copy replaces it whole; a partially
    // written destination is never observable.
    let repeat = vault.materialize(alpha, destination, BASE_MS + 3_100)?;
    require(
        repeat.digest == *alpha.digest()
            && std::fs::read(destination)
                .map(|bytes| bytes == ALPHA)
                .unwrap_or(false),
        "materializing over an existing copy did not leave a verified copy",
    )?;
    require(
        vault
            .materialize(
                &ArtifactHandle::for_content(b"conformance artifact never materialized"),
                &destination.with_extension("absent"),
                BASE_MS + 3_200,
            )
            .is_err(),
        "an artifact that was never stored was materialized",
    )?;
    Ok(())
}

/// Damage is reported rather than served, is a different answer from absence,
/// and is only repaired by an explicit recovery that cannot change what the
/// identity means.
fn damage_is_reported_and_recovery_is_explicit<H: ArtifactVaultHarness>(
    vault: &Vault,
    harness: &mut H,
    alpha: &ArtifactHandle,
    refused: &Path,
) -> Result<(), ArtifactError> {
    require(
        vault.recover(alpha.id(), ALPHA, BASE_MS + 4_000)? == ArtifactRecovery::AlreadyIntact,
        "recovering an intact artifact replaced content that was not damaged",
    )?;

    for damage in [
        ArtifactDamage::TamperContent,
        ArtifactDamage::TruncateContent,
    ] {
        harness.damage(alpha.id(), damage)?;
        let before = expect(
            vault.usage(alpha.id())?,
            "usage vanished when content was damaged",
        )?;
        require(
            vault.verify(alpha.id())? == ArtifactIntegrity::Corrupt,
            format!("{damage:?} was not reported as corruption"),
        )?;
        require(
            vault.inspect(alpha.id())?.is_some(),
            format!("{damage:?} made a damaged artifact look absent"),
        )?;
        match vault.read(alpha, BASE_MS + 4_100) {
            Err(ArtifactError::Corrupt(_)) => {}
            Err(other) => {
                return Err(suite_error(format!(
                    "reading damaged content failed as {other} rather than as corruption"
                )));
            }
            Ok(_) => {
                return Err(suite_error(format!(
                    "{damage:?} was served to a reader instead of being reported"
                )));
            }
        }
        require(
            vault.materialize(alpha, refused, BASE_MS + 4_150).is_err() && !refused.exists(),
            "damaged content was materialized",
        )?;
        require(
            expect(vault.usage(alpha.id())?, "usage vanished")? == before,
            "a refused read of damaged content was counted as a use",
        )?;
        require(
            vault.recover(alpha.id(), BETA, BASE_MS + 4_200).is_err(),
            "recovery accepted content that does not address to the artifact identity",
        )?;
        require(
            vault.verify(alpha.id())? == ArtifactIntegrity::Corrupt,
            "a refused recovery changed the stored artifact",
        )?;

        require(
            vault.recover(alpha.id(), ALPHA, BASE_MS + 4_300)? == ArtifactRecovery::Restored,
            "damaged content could not be recovered from a supplied source",
        )?;
        require(
            vault.verify(alpha.id())? == ArtifactIntegrity::Intact
                && vault.read(alpha, BASE_MS + 4_400)? == ALPHA,
            "a recovered artifact did not read back as the content its identity names",
        )?;
        let record = expect(vault.inspect(alpha.id())?, "alpha vanished after recovery")?;
        require(
            record.handle == *alpha
                && record.size == ALPHA.len() as u64
                && record.provenance.producer_run().as_str() == "run_artifact_conformance"
                && record.recovered_at_ms == Some(BASE_MS + 4_300),
            "recovery changed the identity, size or provenance of the artifact it repaired",
        )?;
    }
    Ok(())
}
