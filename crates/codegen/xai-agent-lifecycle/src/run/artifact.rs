use super::model::{RunError, valid_sha256};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub digest: String,
    pub size: u64,
    pub retention: String,
    pub pinned: bool,
    pub reachable: bool,
}

pub trait ArtifactStore: Send + Sync + 'static {
    fn put(&self, data: &[u8], retention: &str) -> Result<ArtifactMetadata, RunError>;
    fn get(&self, digest: &str, max_size: u64) -> Result<Vec<u8>, RunError>;
    fn set_reachability(&self, digest: &str, pinned: bool, reachable: bool)
    -> Result<(), RunError>;
}

#[derive(Clone, Debug)]
pub struct LocalArtifactStore {
    root: PathBuf,
    max_size: u64,
}

impl LocalArtifactStore {
    pub fn new(root: impl Into<PathBuf>, max_size: u64) -> Result<Self, RunError> {
        if max_size == 0 {
            return Err(RunError::Validation(
                "artifact maximum size must be non-zero".into(),
            ));
        }
        let root = root.into();
        fs::create_dir_all(&root).map_err(io_error)?;
        set_private_dir(&root)?;
        if fs::symlink_metadata(&root)
            .map_err(io_error)?
            .file_type()
            .is_symlink()
        {
            return Err(RunError::Integrity(
                "artifact root must not be a symbolic link".into(),
            ));
        }
        Ok(Self { root, max_size })
    }

    fn data_path(&self, digest: &str) -> PathBuf {
        self.root.join(format!("{digest}.blob"))
    }

    fn metadata_path(&self, digest: &str) -> PathBuf {
        self.root.join(format!("{digest}.json"))
    }
}

impl ArtifactStore for LocalArtifactStore {
    fn put(&self, data: &[u8], retention: &str) -> Result<ArtifactMetadata, RunError> {
        if data.len() as u64 > self.max_size || retention.len() > 128 {
            return Err(RunError::Validation(
                "artifact exceeds provider bounds".into(),
            ));
        }
        let digest = format!("{:x}", Sha256::digest(data));
        let data_path = self.data_path(&digest);
        if data_path.exists() {
            if self.get(&digest, self.max_size)? != data {
                return Err(RunError::Integrity("artifact digest collision".into()));
            }
        } else {
            atomic_write(&self.root, &data_path, data)?;
        }
        let metadata = ArtifactMetadata {
            digest: digest.clone(),
            size: data.len() as u64,
            retention: retention.into(),
            pinned: false,
            reachable: true,
        };
        let encoded =
            serde_json::to_vec(&metadata).map_err(|error| RunError::Storage(error.to_string()))?;
        atomic_write(&self.root, &self.metadata_path(&digest), &encoded)?;
        Ok(metadata)
    }

    fn get(&self, digest: &str, max_size: u64) -> Result<Vec<u8>, RunError> {
        if !valid_sha256(digest) {
            return Err(RunError::Validation("invalid artifact digest".into()));
        }
        let path = self.data_path(digest);
        let metadata = fs::symlink_metadata(&path).map_err(io_error)?;
        let limit = max_size.min(self.max_size);
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
            return Err(RunError::Integrity(
                "artifact path or size failed validation".into(),
            ));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(&path)
            .map_err(io_error)?
            .take(limit.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() as u64 > limit || format!("{:x}", Sha256::digest(&bytes)) != digest {
            return Err(RunError::Integrity(
                "artifact content failed digest validation".into(),
            ));
        }
        Ok(bytes)
    }

    fn set_reachability(
        &self,
        digest: &str,
        pinned: bool,
        reachable: bool,
    ) -> Result<(), RunError> {
        if !valid_sha256(digest) {
            return Err(RunError::Validation("invalid artifact digest".into()));
        }
        let metadata_path = self.metadata_path(digest);
        let file_metadata = fs::symlink_metadata(&metadata_path).map_err(io_error)?;
        if file_metadata.file_type().is_symlink() || file_metadata.len() > 64 * 1024 {
            return Err(RunError::Integrity(
                "artifact metadata path failed validation".into(),
            ));
        }
        let bytes = fs::read(&metadata_path).map_err(io_error)?;
        let mut metadata: ArtifactMetadata =
            serde_json::from_slice(&bytes).map_err(|error| RunError::Storage(error.to_string()))?;
        if metadata.digest != digest {
            return Err(RunError::Integrity(
                "artifact metadata digest mismatch".into(),
            ));
        }
        metadata.pinned = pinned;
        metadata.reachable = reachable;
        let encoded =
            serde_json::to_vec(&metadata).map_err(|error| RunError::Storage(error.to_string()))?;
        atomic_write(&self.root, &metadata_path, &encoded)
    }
}

fn atomic_write(root: &Path, target: &Path, bytes: &[u8]) -> Result<(), RunError> {
    let temporary = root.join(format!(".tmp-{}", uuid::Uuid::new_v4().simple()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(io_error)?;
    set_private_file(&temporary)?;
    let result = (|| {
        file.write_all(bytes).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        fs::rename(&temporary, target).map_err(io_error)?;
        File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(io_error)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn io_error(error: std::io::Error) -> RunError {
    RunError::Storage(error.to_string())
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<(), RunError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(io_error)
}

#[cfg(not(unix))]
fn set_private_dir(_: &Path) -> Result<(), RunError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<(), RunError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(io_error)
}

#[cfg(not(unix))]
fn set_private_file(_: &Path) -> Result<(), RunError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_addressing_checks_integrity_and_paths() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalArtifactStore::new(directory.path(), 1024).unwrap();
        let metadata = store.put(b"durable evidence", "run").unwrap();
        assert_eq!(
            store.get(&metadata.digest, 1024).unwrap(),
            b"durable evidence"
        );
        assert!(store.get("../etc/passwd", 1024).is_err());
        store
            .set_reachability(&metadata.digest, true, true)
            .unwrap();
    }
}
