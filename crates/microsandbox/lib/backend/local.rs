//! Local backend implementation — wraps today's libkrun + agentd path.
//!
//! Currently a near-empty struct; serves as the ambient default while the
//! Backend trait + sub-traits are filled in. The migration plan moves the
//! contents of `crate::config::GlobalConfig` and `crate::db::init_global`
//! into this struct's fields as each call site is converted.

use std::sync::Arc;

use super::{Backend, BackendKind, SandboxBackend};

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
