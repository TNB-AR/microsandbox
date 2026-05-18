//! Local backend implementation — wraps today's libkrun + agentd path.
//!
//! Currently a near-empty struct; serves as the ambient default while the
//! Backend trait + sub-traits are filled in. The migration plan moves the
//! contents of `crate::config::GlobalConfig` and `crate::db::init_global`
//! into this struct's fields as each call site is converted.

use std::path::PathBuf;
use std::sync::Arc;

use super::{Backend, BackendKind, SandboxBackend, VolumeBackend};

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Local-runtime backend: spawns microVMs via libkrun on the calling host.
///
/// Holds local-only state (DB pool, paths config, sandbox defaults, registry
/// config) — eventually absorbs the existing `GlobalConfig` singleton and
/// per-process `init_global` DB pool. For now, intentionally empty: existing
/// global call sites stay unchanged until each is migrated.
pub struct LocalBackend {
    // Fields land here as the migration progresses:
    //   db: Arc<DbPools>,
    //   config: Arc<LocalConfig>,
    //   ...
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl LocalBackend {
    /// Construct a `LocalBackend` that defers all setup to the existing
    /// process-wide globals (`crate::config::config()` / `crate::db::init_global`).
    ///
    /// This is what the ambient default resolves to when nothing else is set.
    /// Behaviourally a no-op wrapper today; concrete state moves in here as
    /// the refactor progresses.
    pub fn lazy() -> Self {
        Self {}
    }

    /// Host-side directory rooted at `volumes_dir/<name>` for a named volume.
    ///
    /// Non-trait helper used by [`VolumeFs`](crate::volume::VolumeFs) streaming
    /// methods and the local-FFI shims that need a path before any backend
    /// trait call. Reads from the process-wide config singleton today;
    /// absorbs into [`LocalBackend`] fields when the globals are hoisted.
    pub fn volume_path(&self, name: &str) -> PathBuf {
        crate::config::config().volumes_dir().join(name)
    }
}

//--------------------------------------------------------------------------------------------------
// Trait Implementations
//--------------------------------------------------------------------------------------------------

impl Backend for LocalBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Local
    }

    fn sandboxes(&self) -> &dyn SandboxBackend {
        self
    }

    fn volumes(&self) -> &dyn VolumeBackend {
        self
    }
}

impl Default for LocalBackend {
    fn default() -> Self {
        Self::lazy()
    }
}

impl From<LocalBackend> for Arc<dyn Backend> {
    fn from(backend: LocalBackend) -> Self {
        Arc::new(backend)
    }
}
