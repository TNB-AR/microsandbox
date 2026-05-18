//! Host-side filesystem operations on a named volume.
//!
//! Unlike [`SandboxFs`](crate::sandbox::fs::SandboxFs) which goes through the
//! agent protocol, [`VolumeFs`] reads + writes a volume's bytes directly. For
//! the local backend that is `tokio::fs` against `volumes_dir/<name>/`; for
//! cloud (Phase 6) it routes through msb-cloud HTTP. Today every cloud op
//! returns [`MicrosandboxError::Unsupported`].
//!
//! `VolumeFs` is a single type per D6.4 — no public variants. It borrows the
//! parent volume's `Arc<dyn Backend>` + name and dispatches through the
//! [`VolumeBackend`](crate::backend::VolumeBackend) trait.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::backend::{Backend, BackendKind};
use crate::{
    MicrosandboxError, MicrosandboxResult,
    sandbox::fs::{FsEntry, FsMetadata},
};

//--------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------

/// Chunk size for streaming volume reads (64 KiB).
const STREAM_CHUNK_SIZE: usize = 64 * 1024;

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Filesystem operations on a volume.
///
/// Borrows the parent volume's `Arc<dyn Backend>` + name and dispatches every
/// op through the [`VolumeBackend`](crate::backend::VolumeBackend) trait.
/// Local routes to `tokio::fs`; cloud returns `Unsupported` until Phase 6.
pub struct VolumeFs<'a> {
    backend: Arc<dyn Backend>,
    name: &'a str,
}

/// A streaming reader for file data from a volume's host-side directory.
///
/// **Local backend only** — opened from a host path. Cloud streaming flows
/// through `VolumeFs::read_stream` will land alongside the cloud HTTP routes
/// in Phase 6.
pub struct VolumeFsReadStream {
    file: tokio::fs::File,
    buf: Vec<u8>,
}

/// A streaming writer for file data to a volume's host-side directory.
///
/// **Local backend only** — opened against a host path. Cloud streaming will
/// land alongside the cloud HTTP routes in Phase 6.
pub struct VolumeFsWriteSink {
    file: tokio::fs::File,
}

//--------------------------------------------------------------------------------------------------
// Methods: VolumeFs
//--------------------------------------------------------------------------------------------------

impl<'a> VolumeFs<'a> {
    /// Construct a volume FS handle for the named volume.
    ///
    /// Called by [`Volume::fs`](super::Volume::fs) and
    /// [`VolumeHandle::fs`](super::VolumeHandle::fs) — those are the public
    /// entry points; this constructor itself is crate-private.
    pub(crate) fn new(backend: Arc<dyn Backend>, name: &'a str) -> Self {
        Self { backend, name }
    }

    /// Public constructor for FFI shims that don't hold a [`Volume`](super::Volume) /
    /// [`VolumeHandle`](super::VolumeHandle) directly.
    ///
    /// Most callers should use [`Volume::fs`](super::Volume::fs) /
    /// [`VolumeHandle::fs`](super::VolumeHandle::fs); this is here for the
    /// language bindings that re-assemble a `VolumeFs` per FFI call.
    pub fn with_backend(backend: Arc<dyn Backend>, name: &'a str) -> Self {
        Self { backend, name }
    }

    //----------------------------------------------------------------------------------------------
    // Read Operations
    //----------------------------------------------------------------------------------------------

    /// Read an entire file into memory as raw bytes.
    pub async fn read(&self, path: &str) -> MicrosandboxResult<Bytes> {
        self.backend.volumes().fs_read(self.name, path).await
    }

    /// Read an entire file into memory as a UTF-8 string.
    pub async fn read_to_string(&self, path: &str) -> MicrosandboxResult<String> {
        self.backend
            .volumes()
            .fs_read_to_string(self.name, path)
            .await
    }

    /// Read a file with streaming. Returns a [`VolumeFsReadStream`] that
    /// yields chunks of bytes.
    ///
    /// **Local backend only** — cloud streaming routes through HTTP and
    /// hasn't shipped yet; returns [`MicrosandboxError::Unsupported`] on
    /// cloud.
    pub async fn read_stream(&self, path: &str) -> MicrosandboxResult<VolumeFsReadStream> {
        let full = self.require_local_path(path, "VolumeFs::read_stream")?;
        let file = tokio::fs::File::open(&full).await?;
        Ok(VolumeFsReadStream {
            file,
            buf: vec![0u8; STREAM_CHUNK_SIZE],
        })
    }

    //----------------------------------------------------------------------------------------------
    // Write Operations
    //----------------------------------------------------------------------------------------------

    /// Write data to a file, creating parent directories as needed.
    /// Overwrites if the file already exists.
    pub async fn write(&self, path: &str, data: impl AsRef<[u8]>) -> MicrosandboxResult<()> {
        let bytes = data.as_ref().to_vec();
        self.backend
            .volumes()
            .fs_write(self.name, path, bytes)
            .await
    }

    /// Write to a file with streaming. Returns a [`VolumeFsWriteSink`] that
    /// accepts chunks of bytes. Creates parent directories as needed.
    ///
    /// **Local backend only** — cloud streaming routes through HTTP and
    /// hasn't shipped yet; returns [`MicrosandboxError::Unsupported`] on
    /// cloud.
    pub async fn write_stream(&self, path: &str) -> MicrosandboxResult<VolumeFsWriteSink> {
        let full = self.require_local_path(path, "VolumeFs::write_stream")?;
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::File::create(&full).await?;
        Ok(VolumeFsWriteSink { file })
    }

    //----------------------------------------------------------------------------------------------
    // Directory + File Operations
    //----------------------------------------------------------------------------------------------

    /// List the immediate children of a directory (non-recursive).
    /// Each entry includes the path, kind, size, permissions, and modification time.
    pub async fn list(&self, path: &str) -> MicrosandboxResult<Vec<FsEntry>> {
        self.backend.volumes().fs_list(self.name, path).await
    }

    /// Create a directory (and parents).
    pub async fn mkdir(&self, path: &str) -> MicrosandboxResult<()> {
        self.backend.volumes().fs_mkdir(self.name, path).await
    }

    /// Remove a directory recursively.
    pub async fn remove_dir(&self, path: &str) -> MicrosandboxResult<()> {
        self.backend.volumes().fs_remove_dir(self.name, path).await
    }

    /// Delete a single file. Use [`remove_dir`](Self::remove_dir) for directories.
    pub async fn remove(&self, path: &str) -> MicrosandboxResult<()> {
        self.backend.volumes().fs_remove(self.name, path).await
    }

    /// Copy a file within the volume.
    pub async fn copy(&self, from: &str, to: &str) -> MicrosandboxResult<()> {
        self.backend.volumes().fs_copy(self.name, from, to).await
    }

    /// Rename/move a file or directory.
    pub async fn rename(&self, from: &str, to: &str) -> MicrosandboxResult<()> {
        self.backend.volumes().fs_rename(self.name, from, to).await
    }

    //----------------------------------------------------------------------------------------------
    // Metadata
    //----------------------------------------------------------------------------------------------

    /// Get file/directory metadata.
    pub async fn stat(&self, path: &str) -> MicrosandboxResult<FsMetadata> {
        self.backend.volumes().fs_stat(self.name, path).await
    }

    /// Check whether a file or directory exists at the given path.
    /// Returns `false` (not an error) if the path is absent.
    pub async fn exists(&self, path: &str) -> MicrosandboxResult<bool> {
        self.backend.volumes().fs_exists(self.name, path).await
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: VolumeFs (helpers)
//--------------------------------------------------------------------------------------------------

impl VolumeFs<'_> {
    /// Resolve a relative path against the local volume root. Errors with
    /// [`MicrosandboxError::Unsupported`] on a non-local backend.
    ///
    /// Used by streaming ops which need a direct host path and don't fit the
    /// boxed-future trait shape cleanly.
    fn require_local_path(&self, path: &str, feature: &str) -> MicrosandboxResult<PathBuf> {
        if self.backend.kind() != BackendKind::Local {
            return Err(MicrosandboxError::Unsupported {
                feature: feature.into(),
                available_when: "when cloud streaming routes ship".into(),
            });
        }
        let root = crate::backend::LocalBackend::ambient().volume_path(self.name);
        local::resolve_relative(&root, path)
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: VolumeFsReadStream
//--------------------------------------------------------------------------------------------------

impl VolumeFsReadStream {
    /// Receive the next chunk of file data.
    ///
    /// Returns `None` at EOF.
    pub async fn recv(&mut self) -> MicrosandboxResult<Option<Bytes>> {
        let n = self.file.read(&mut self.buf).await?;
        if n == 0 {
            Ok(None)
        } else {
            Ok(Some(Bytes::copy_from_slice(&self.buf[..n])))
        }
    }

    /// Read the remaining file data into a single `Bytes` buffer.
    pub async fn collect(mut self) -> MicrosandboxResult<Bytes> {
        let mut data = Vec::new();
        let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
        loop {
            let n = self.file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            data.extend_from_slice(&buf[..n]);
        }
        Ok(Bytes::from(data))
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: VolumeFsWriteSink
//--------------------------------------------------------------------------------------------------

impl VolumeFsWriteSink {
    /// Write a chunk of data to the file.
    pub async fn write(&mut self, data: impl AsRef<[u8]>) -> MicrosandboxResult<()> {
        self.file.write_all(data.as_ref()).await?;
        Ok(())
    }

    /// Flush and close the file.
    pub async fn close(mut self) -> MicrosandboxResult<()> {
        self.file.flush().await?;
        Ok(())
    }
}

//--------------------------------------------------------------------------------------------------
// Module: local (free fn impls called by LocalBackend's VolumeBackend impl)
//--------------------------------------------------------------------------------------------------

pub(crate) mod local {
    //! Local FS ops keyed by `(volume_name, rel_path)`.
    //!
    //! Lives in a sub-module so the `LocalBackend` trait impl in
    //! `backend/volume.rs` can call into one place. Internally resolves
    //! the path against the global `volumes_dir` config — moves into
    //! `LocalBackend` fields when the globals are hoisted.

    use std::path::{Path, PathBuf};

    use bytes::Bytes;

    use crate::{
        MicrosandboxError, MicrosandboxResult,
        sandbox::fs::{FsEntry, FsEntryKind, FsMetadata},
    };

    /// Resolve a relative path against the volume root, preventing path traversal.
    pub(crate) fn resolve_relative(root: &Path, path: &str) -> MicrosandboxResult<PathBuf> {
        // Strip leading slash for joining.
        let clean = path.strip_prefix('/').unwrap_or(path);

        let joined = root.join(clean);

        // Canonicalize what exists, then check prefix. If the path doesn't exist
        // yet (for writes), canonicalize the parent and verify.
        let canonical = if joined.exists() {
            joined
                .canonicalize()
                .map_err(|e| MicrosandboxError::SandboxFs(format!("resolve path: {e}")))?
        } else {
            // Find the deepest existing ancestor.
            let mut ancestor = joined.as_path();
            loop {
                if let Some(parent) = ancestor.parent() {
                    if parent.exists() {
                        let canon_parent = parent.canonicalize().map_err(|e| {
                            MicrosandboxError::SandboxFs(format!("resolve parent: {e}"))
                        })?;
                        // Reconstruct with remaining components.
                        let remainder = joined.strip_prefix(parent).unwrap_or(Path::new(""));
                        break canon_parent.join(remainder);
                    }
                    ancestor = parent;
                } else {
                    break joined.clone();
                }
            }
        };

        // Ensure the root itself is canonicalized for comparison.
        let canon_root = if root.exists() {
            root.canonicalize()
                .map_err(|e| MicrosandboxError::SandboxFs(format!("resolve root: {e}")))?
        } else {
            root.to_path_buf()
        };

        if !canonical.starts_with(&canon_root) {
            return Err(MicrosandboxError::SandboxFs(
                "path traversal outside volume root".into(),
            ));
        }

        Ok(canonical)
    }

    /// Volume root directory on the host for the named volume.
    fn volume_root(name: &str) -> PathBuf {
        crate::backend::LocalBackend::ambient().volume_path(name)
    }

    /// Resolve `(volume_name, path)` to a canonical host path.
    fn resolve(name: &str, path: &str) -> MicrosandboxResult<PathBuf> {
        resolve_relative(&volume_root(name), path)
    }

    pub(crate) async fn read(name: &str, path: &str) -> MicrosandboxResult<Bytes> {
        let full = resolve(name, path)?;
        let data = tokio::fs::read(&full).await?;
        Ok(Bytes::from(data))
    }

    pub(crate) async fn read_to_string(name: &str, path: &str) -> MicrosandboxResult<String> {
        let full = resolve(name, path)?;
        let data = tokio::fs::read_to_string(&full).await?;
        Ok(data)
    }

    pub(crate) async fn write(name: &str, path: &str, data: &[u8]) -> MicrosandboxResult<()> {
        let full = resolve(name, path)?;
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&full, data).await?;
        Ok(())
    }

    pub(crate) async fn list(name: &str, path: &str) -> MicrosandboxResult<Vec<FsEntry>> {
        let root = volume_root(name);
        let full = resolve_relative(&root, path)?;
        let mut dir = tokio::fs::read_dir(&full).await?;
        let mut entries = Vec::new();

        while let Some(entry) = dir.next_entry().await? {
            let entry_path = entry.path();
            let rel_path = entry_path.strip_prefix(&root).unwrap_or(&entry_path);

            match entry.metadata().await {
                Ok(meta) => {
                    entries.push(metadata_to_entry(
                        &format!("/{}", rel_path.display()),
                        &meta,
                    ));
                }
                Err(_) => {
                    entries.push(FsEntry {
                        path: format!("/{}", rel_path.display()),
                        kind: FsEntryKind::Other,
                        size: 0,
                        mode: 0,
                        modified: None,
                    });
                }
            }
        }

        Ok(entries)
    }

    pub(crate) async fn mkdir(name: &str, path: &str) -> MicrosandboxResult<()> {
        let full = resolve(name, path)?;
        tokio::fs::create_dir_all(&full).await?;
        Ok(())
    }

    pub(crate) async fn remove_dir(name: &str, path: &str) -> MicrosandboxResult<()> {
        let full = resolve(name, path)?;
        tokio::fs::remove_dir_all(&full).await?;
        Ok(())
    }

    pub(crate) async fn remove(name: &str, path: &str) -> MicrosandboxResult<()> {
        let full = resolve(name, path)?;
        tokio::fs::remove_file(&full).await?;
        Ok(())
    }

    pub(crate) async fn copy(name: &str, from: &str, to: &str) -> MicrosandboxResult<()> {
        let src = resolve(name, from)?;
        let dst = resolve(name, to)?;

        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::copy(&src, &dst).await?;
        Ok(())
    }

    pub(crate) async fn rename(name: &str, from: &str, to: &str) -> MicrosandboxResult<()> {
        let src = resolve(name, from)?;
        let dst = resolve(name, to)?;

        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::rename(&src, &dst).await?;
        Ok(())
    }

    pub(crate) async fn stat(name: &str, path: &str) -> MicrosandboxResult<FsMetadata> {
        let full = resolve(name, path)?;
        let meta = tokio::fs::symlink_metadata(&full).await?;
        Ok(std_metadata_to_fs(&meta))
    }

    pub(crate) async fn exists(name: &str, path: &str) -> MicrosandboxResult<bool> {
        let full = resolve(name, path)?;
        Ok(tokio::fs::try_exists(&full).await.unwrap_or(false))
    }

    //----------------------------------------------------------------------------------------------
    // Functions: helpers
    //----------------------------------------------------------------------------------------------

    fn std_kind(meta: &std::fs::Metadata) -> FsEntryKind {
        if meta.is_file() {
            FsEntryKind::File
        } else if meta.is_dir() {
            FsEntryKind::Directory
        } else if meta.is_symlink() {
            FsEntryKind::Symlink
        } else {
            FsEntryKind::Other
        }
    }

    fn std_modified(meta: &std::fs::Metadata) -> Option<chrono::DateTime<chrono::Utc>> {
        meta.modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| chrono::DateTime::from_timestamp(d.as_secs() as i64, 0).unwrap_or_default())
    }

    fn metadata_to_entry(path: &str, meta: &std::fs::Metadata) -> FsEntry {
        use std::os::unix::fs::MetadataExt;

        FsEntry {
            path: path.to_string(),
            kind: std_kind(meta),
            size: meta.len(),
            mode: meta.mode(),
            modified: std_modified(meta),
        }
    }

    fn std_created(meta: &std::fs::Metadata) -> Option<chrono::DateTime<chrono::Utc>> {
        meta.created()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| chrono::DateTime::from_timestamp(d.as_secs() as i64, 0).unwrap_or_default())
    }

    fn std_metadata_to_fs(meta: &std::fs::Metadata) -> FsMetadata {
        use std::os::unix::fs::MetadataExt;

        FsMetadata {
            kind: std_kind(meta),
            size: meta.len(),
            mode: meta.mode(),
            readonly: meta.permissions().readonly(),
            modified: std_modified(meta),
            created: std_created(meta),
        }
    }
}
