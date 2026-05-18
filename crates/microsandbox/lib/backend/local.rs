//! Local backend implementation — wraps today's libkrun + agentd path.
//!
//! Holds local-only state (DB pool, paths config, sandbox defaults, registry
//! config) as fields on a single struct, replacing the per-process global
//! config + DB pool statics that lived in `crate::config` / `crate::db`. Two
//! construction paths:
//!
//! - [`LocalBackend::lazy`] — sync ambient default. Initialises its DB pool +
//!   config lazily on first access. Used by the ambient `default_backend()`
//!   when no explicit backend is installed.
//! - [`LocalBackend::builder`] — programmatic config. `.build().await`
//!   constructs eagerly with all DB pools + config resolved up front.
//!
//! Per D6.7 Layer 2a in the SDK local-cloud parity plan: this struct absorbs
//! the bulk of the old global config singleton plus the SQLite pool, so multiple
//! backends can hold different configurations for tests / migrations.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use microsandbox_db::pool::DbPools;
use microsandbox_migration::{Migrator, MigratorTrait};
use tokio::sync::OnceCell;

use super::{Backend, BackendKind, SandboxBackend, VolumeBackend};
use crate::config::{
    DEFAULT_CONNECT_TIMEOUT_SECS, DEFAULT_MAX_CONNECTIONS, DatabaseConfig, LocalConfig,
    PathsConfig, RegistriesConfig, SandboxDefaults, default_metrics_sample_interval,
    load_persisted_config_or_default,
};
use crate::{MicrosandboxError, MicrosandboxResult};

//--------------------------------------------------------------------------------------------------
// Statics
//--------------------------------------------------------------------------------------------------

/// Process-wide ambient `LocalBackend`. Lazily initialised by
/// [`LocalBackend::ambient`] on first access. Holds the "no explicit setup"
/// instance — the same `LocalBackend` the ambient `default_backend()` falls
/// back to when nothing else is installed.
///
/// Code paths that cannot thread a `&LocalBackend` through their callers
/// (serde defaults on `SandboxConfig`, CLI commands, FFI shims) consult this
/// cell. Code paths that come in through a trait method receive an explicit
/// `&LocalBackend` instead.
static AMBIENT: OnceLock<Arc<LocalBackend>> = OnceLock::new();

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Local-runtime backend: spawns microVMs via libkrun on the calling host.
///
/// Owns the persisted [`LocalConfig`] (paths, sandbox defaults, registry
/// settings, database tuning) and the SQLite [`DbPools`] for this instance.
/// Built via either [`LocalBackend::lazy`] (no-explicit-setup ambient
/// default, lazily initialised) or [`LocalBackend::builder`] (programmatic).
pub struct LocalBackend {
    config: Arc<LocalConfig>,
    db: OnceCell<DbPools>,
}

/// Fluent builder for [`LocalBackend`]. Construct via [`LocalBackend::builder`].
///
/// All fields are optional. `build().await` produces a `LocalBackend` whose
/// DB pool has already been opened and migrated.
#[derive(Default)]
pub struct LocalBackendBuilder {
    home: Option<PathBuf>,
    sandboxes_dir: Option<PathBuf>,
    volumes_dir: Option<PathBuf>,
    snapshots_dir: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    logs_dir: Option<PathBuf>,
    secrets_dir: Option<PathBuf>,
    max_connections: Option<u32>,
    connect_timeout_secs: Option<u64>,
    busy_timeout_secs: Option<u64>,
    default_cpus: Option<u8>,
    default_memory_mib: Option<u32>,
    log_level: Option<microsandbox_runtime::logging::LogLevel>,
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl LocalBackend {
    /// Construct a `LocalBackend` whose DB pool initialises on first access.
    ///
    /// The config is read from `~/.microsandbox/config.json` (honouring
    /// `MSB_CONFIG_PATH`) at construction; a missing file resolves to the
    /// hard-coded defaults. The DB pool is created (and migrations applied)
    /// on first call to [`Self::db`].
    ///
    /// This is the ambient default — the same instance every call to
    /// [`Self::ambient`] returns.
    pub fn lazy() -> Self {
        let config = Arc::new(load_persisted_config_or_default().unwrap_or_default());
        Self {
            config,
            db: OnceCell::new(),
        }
    }

    /// Eagerly construct a `LocalBackend` from `~/.microsandbox/config.json`,
    /// opening (and migrating) the DB pool up front.
    pub async fn new() -> MicrosandboxResult<Self> {
        let backend = Self::lazy();
        let _ = backend.db().await?;
        Ok(backend)
    }

    /// Start a builder for programmatic configuration.
    pub fn builder() -> LocalBackendBuilder {
        LocalBackendBuilder::default()
    }

    /// Return the process-wide ambient `LocalBackend`. Lazily constructed on
    /// first access via [`Self::lazy`]; subsequent calls return the same
    /// `Arc`.
    ///
    /// This is the bridge for code paths that cannot thread a
    /// `&LocalBackend` through their callers (serde defaults, FFI shims,
    /// CLI helpers).
    pub fn ambient() -> &'static Arc<LocalBackend> {
        AMBIENT.get_or_init(|| Arc::new(LocalBackend::lazy()))
    }

    /// Access the open DB pool, initialising it (and applying migrations) on
    /// first call.
    pub async fn db(&self) -> MicrosandboxResult<&DbPools> {
        self.db
            .get_or_try_init(|| async {
                let db_dir = self.config.home().join(microsandbox_utils::DB_SUBDIR);
                connect_and_migrate(&db_dir, &self.config.database).await
            })
            .await
    }

    /// Borrow this backend's [`LocalConfig`].
    pub fn config(&self) -> &LocalConfig {
        &self.config
    }

    /// Host-side directory rooted at `volumes_dir/<name>` for a named volume.
    ///
    /// Non-trait helper used by [`VolumeFs`](crate::volume::VolumeFs)
    /// streaming methods and FFI shims that need a path before any backend
    /// trait call.
    pub fn volume_path(&self, name: &str) -> PathBuf {
        self.config.volumes_dir().join(name)
    }

    /// Resolved sandboxes directory.
    pub fn sandboxes_dir(&self) -> PathBuf {
        self.config.sandboxes_dir()
    }

    /// Resolved volumes directory.
    pub fn volumes_dir(&self) -> PathBuf {
        self.config.volumes_dir()
    }

    /// Resolved snapshots directory.
    pub fn snapshots_dir(&self) -> PathBuf {
        self.config.snapshots_dir()
    }

    /// Resolved cache directory.
    pub fn cache_dir(&self) -> PathBuf {
        self.config.cache_dir()
    }

    /// Resolved logs directory.
    pub fn logs_dir(&self) -> PathBuf {
        self.config.logs_dir()
    }

    /// Resolved secrets directory.
    pub fn secrets_dir(&self) -> PathBuf {
        self.config.secrets_dir()
    }
}

impl LocalBackendBuilder {
    /// Override the home directory (default: `~/.microsandbox`).
    pub fn home(mut self, path: impl Into<PathBuf>) -> Self {
        self.home = Some(path.into());
        self
    }

    /// Override the sandboxes directory.
    pub fn sandboxes_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.sandboxes_dir = Some(path.into());
        self
    }

    /// Override the volumes directory.
    pub fn volumes_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.volumes_dir = Some(path.into());
        self
    }

    /// Override the snapshots directory.
    pub fn snapshots_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.snapshots_dir = Some(path.into());
        self
    }

    /// Override the cache directory.
    pub fn cache_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(path.into());
        self
    }

    /// Override the logs directory.
    pub fn logs_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.logs_dir = Some(path.into());
        self
    }

    /// Override the secrets directory.
    pub fn secrets_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.secrets_dir = Some(path.into());
        self
    }

    /// Override the DB max connections (default: 5).
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = Some(n);
        self
    }

    /// Override the DB connect timeout in seconds.
    pub fn connect_timeout_secs(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = Some(secs);
        self
    }

    /// Override SQLite's `busy_timeout` in seconds.
    pub fn busy_timeout_secs(mut self, secs: u64) -> Self {
        self.busy_timeout_secs = Some(secs);
        self
    }

    /// Override the default sandbox vCPU count.
    pub fn default_cpus(mut self, cpus: u8) -> Self {
        self.default_cpus = Some(cpus);
        self
    }

    /// Override the default sandbox guest memory (MiB).
    pub fn default_memory_mib(mut self, mib: u32) -> Self {
        self.default_memory_mib = Some(mib);
        self
    }

    /// Override the runtime log level applied to SDK-spawned sandboxes.
    pub fn log_level(mut self, level: microsandbox_runtime::logging::LogLevel) -> Self {
        self.log_level = Some(level);
        self
    }

    /// Build the `LocalBackend`. Opens the DB pool and applies migrations.
    pub async fn build(self) -> MicrosandboxResult<LocalBackend> {
        let config = self.into_local_config();
        let backend = LocalBackend {
            config: Arc::new(config),
            db: OnceCell::new(),
        };
        let _ = backend.db().await?;
        Ok(backend)
    }

    fn into_local_config(self) -> LocalConfig {
        let LocalBackendBuilder {
            home,
            sandboxes_dir,
            volumes_dir,
            snapshots_dir,
            cache_dir,
            logs_dir,
            secrets_dir,
            max_connections,
            connect_timeout_secs,
            busy_timeout_secs,
            default_cpus,
            default_memory_mib,
            log_level,
        } = self;

        LocalConfig {
            home,
            log_level,
            database: DatabaseConfig {
                url: None,
                max_connections: max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS),
                connect_timeout_secs: connect_timeout_secs.unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS),
                busy_timeout_secs: busy_timeout_secs
                    .unwrap_or(microsandbox_db::pool::DEFAULT_BUSY_TIMEOUT_SECS),
            },
            paths: PathsConfig {
                msb: None,
                libkrunfw: None,
                cache: cache_dir,
                sandboxes: sandboxes_dir,
                volumes: volumes_dir,
                snapshots: snapshots_dir,
                logs: logs_dir,
                secrets: secrets_dir,
            },
            sandbox_defaults: SandboxDefaults {
                cpus: default_cpus.unwrap_or(1),
                memory_mib: default_memory_mib.unwrap_or(512),
                shell: "/bin/sh".into(),
                workdir: None,
                metrics_sample_interval_ms: default_metrics_sample_interval(),
                disable_metrics_sample: false,
            },
            registries: RegistriesConfig::default(),
        }
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

    fn as_local(&self) -> Option<&LocalBackend> {
        Some(self)
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

//--------------------------------------------------------------------------------------------------
// Functions: Helpers
//--------------------------------------------------------------------------------------------------

/// Open both pools for `db_dir/msb.db` and run migrations on the writer.
///
/// The write pool connects first so WAL mode (persisted in the database
/// header) is set before the read pool opens.
async fn connect_and_migrate(
    db_dir: &Path,
    database: &DatabaseConfig,
) -> MicrosandboxResult<DbPools> {
    tokio::fs::create_dir_all(db_dir).await?;

    let db_path = db_dir.join(microsandbox_utils::DB_FILENAME);
    let pools = DbPools::open(
        &db_path,
        database.max_connections,
        Duration::from_secs(database.connect_timeout_secs),
        Duration::from_secs(database.busy_timeout_secs),
    )
    .await
    .map_err(|e| MicrosandboxError::Custom(format!("connect to {}: {e}", db_path.display())))?;

    Migrator::up(pools.write().inner(), None).await?;

    Ok(pools)
}

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};

    use super::*;

    #[tokio::test]
    async fn test_connect_and_migrate_creates_db_and_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let db_dir = tmp.path().join("db");
        let database = DatabaseConfig::default();

        let pools = connect_and_migrate(&db_dir, &database).await.unwrap();
        let conn = pools.read();

        // DB file should exist on disk.
        assert!(db_dir.join(microsandbox_utils::DB_FILENAME).exists());

        // All 12 tables should be present.
        let rows = conn
            .query_all(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'seaql_%' AND name != 'sqlite_sequence' ORDER BY name",
            ))
            .await
            .unwrap();

        let table_names: Vec<String> = rows
            .iter()
            .map(|r| r.try_get_by_index::<String>(0).unwrap())
            .collect();

        let expected = vec![
            "config",
            "image_ref",
            "layer",
            "manifest",
            "manifest_layer",
            "run",
            "sandbox",
            "sandbox_metric",
            "sandbox_rootfs",
            "snapshot_index",
            "volume",
        ];

        assert_eq!(table_names, expected);
    }

    #[tokio::test]
    async fn test_connect_and_migrate_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let db_dir = tmp.path().join("db");
        let database = DatabaseConfig::default();

        let pools = connect_and_migrate(&db_dir, &database).await.unwrap();

        // Running migrations again on the same DB should succeed.
        Migrator::up(pools.write().inner(), None).await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_and_migrate_recovers_from_partial_storage_migration() {
        let tmp = tempfile::tempdir().unwrap();
        let db_dir = tmp.path().join("db");
        tokio::fs::create_dir_all(&db_dir).await.unwrap();

        let db_path = db_dir.join(microsandbox_utils::DB_FILENAME);
        let db_url = format!("sqlite://{}?mode=rwc", db_path.display());

        let conn = Database::connect(&db_url).await.unwrap();

        conn.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_keys = ON;",
        ))
        .await
        .unwrap();

        // Apply only migrations 1 and 2 so migration 3 is still pending.
        Migrator::up(&conn, Some(2)).await.unwrap();

        // Simulate a half-applied migration 3.
        conn.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TABLE IF NOT EXISTS volume (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                quota_mib INTEGER,
                size_bytes BIGINT,
                labels TEXT,
                created_at DATETIME,
                updated_at DATETIME
            )",
        ))
        .await
        .unwrap();

        conn.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TABLE IF NOT EXISTS snapshot (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sandbox_id INTEGER,
                size_bytes BIGINT,
                description TEXT,
                created_at DATETIME,
                FOREIGN KEY (sandbox_id) REFERENCES sandbox(id) ON DELETE SET NULL
            )",
        ))
        .await
        .unwrap();

        conn.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE UNIQUE INDEX idx_snapshots_name_sandbox_unique ON snapshot (name, sandbox_id)",
        ))
        .await
        .unwrap();

        drop(conn);

        let database = DatabaseConfig::default();
        let recovered = connect_and_migrate(&db_dir, &database).await.unwrap();

        let migration_row_count = recovered
            .read()
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) FROM seaql_migrations WHERE version = 'm20260305_000003_create_storage_tables'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index::<i64>(0)
            .unwrap();
        assert_eq!(migration_row_count, 1);
    }
}
