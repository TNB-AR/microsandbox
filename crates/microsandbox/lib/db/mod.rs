//! Global database connection pool init and accessor.
//!
//! Opens both pools (read + write) for the configured database and runs
//! migrations. The backend is selected at runtime from the connection
//! URL: a `sqlite://` URL opens a local SQLite file (the default), a
//! `postgres://` URL connects to a PostgreSQL server.
//!
//! The URL and schema are resolved from the process configuration (see
//! [`crate::config::database_url`] and [`crate::config::database_schema`]
//! for the precedence ladders). The setters live alongside
//! [`crate::config::set_sdk_msb_path`] in the `config` module — they're
//! all process-level configuration, set once before the global pool
//! opens.
//!
//! Returns [`DbPools`] from `microsandbox-db`; callers pick `pools.read()`
//! (a [`DbReadConnection`]) or `pools.write()` (a [`DbWriteConnection`])
//! based on the operation. The type system blocks accidental writes
//! against the read pool.
//!
//! [`DbReadConnection`]: microsandbox_db::DbReadConnection
//! [`DbWriteConnection`]: microsandbox_db::DbWriteConnection

pub use microsandbox_db::entity;

use std::time::Duration;

use microsandbox_db::{Backend, DbPools, DbSettings, DbWriteConnection, validate_schema_name};
use microsandbox_migration::{Migrator, MigratorTrait};
use tokio::sync::OnceCell;

use crate::{MicrosandboxError, MicrosandboxResult};

//--------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------

/// Base value for the PostgreSQL migration advisory-lock key.
///
/// The ASCII bytes of `msbmig`. When a schema is configured, the key is
/// XORed with a 64-bit FNV-1a hash of the schema name so different
/// schemas serialise their migrations independently.
const MIGRATION_ADVISORY_LOCK_BASE: i64 = 0x6D73626D6967;

//--------------------------------------------------------------------------------------------------
// Statics
//--------------------------------------------------------------------------------------------------

static GLOBAL_POOL: OnceCell<DbPools> = OnceCell::const_new();

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

/// Initialize the global database pools for the configured database.
///
/// Migrations are applied automatically. Idempotent — repeat calls return
/// the existing pools. All tuning (max_connections, connect_timeout,
/// busy_timeout) is read from `~/.microsandbox/config.json`.
pub async fn init_global() -> MicrosandboxResult<&'static DbPools> {
    GLOBAL_POOL
        .get_or_try_init(|| async {
            let settings = resolve_db_settings();
            connect_and_migrate(&settings).await
        })
        .await
}

/// Get the global pools, or `None` if [`init_global`] has not run.
pub fn global() -> Option<&'static DbPools> {
    GLOBAL_POOL.get()
}

//--------------------------------------------------------------------------------------------------
// Functions: Helpers
//--------------------------------------------------------------------------------------------------

/// Build [`DbSettings`] from the resolved URL + schema + tuning from the
/// config.
fn resolve_db_settings() -> DbSettings {
    let cfg = crate::config::config();
    DbSettings {
        url: crate::config::database_url(),
        schema: crate::config::database_schema(),
        max_connections: cfg.database.max_connections,
        connect_timeout: Duration::from_secs(cfg.database.connect_timeout_secs),
        busy_timeout: Duration::from_secs(cfg.database.busy_timeout_secs),
    }
}

/// Open both pools for `settings` and run migrations.
async fn connect_and_migrate(settings: &DbSettings) -> MicrosandboxResult<DbPools> {
    let backend = Backend::from_url(&settings.url)
        .map_err(|e| MicrosandboxError::InvalidConfig(e.to_string()))?;

    let pools = DbPools::open(settings)
        .await
        .map_err(|e| MicrosandboxError::Custom(format!("connect to database: {e}")))?;

    run_migrations(backend, settings, &pools).await?;

    Ok(pools)
}

/// Apply pending migrations.
///
/// For PostgreSQL the migrator runs under a session advisory lock taken on
/// a dedicated single connection, so two processes initialising the same
/// database concurrently cannot race the `seaql_migrations` bookkeeping. A
/// session-scoped lock must be held on one connection for its whole
/// lifetime; the write pool is multi-connection, hence the dedicated
/// single-connection pool. The lock key incorporates the schema name (when
/// set) so different nodes do not serialise each other's migrations.
///
/// When a schema is configured, `CREATE SCHEMA IF NOT EXISTS` runs before
/// the migrator so a fresh node is self-configuring without a DBA step.
///
/// SQLite databases are per-file and per-host, so no lock or schema setup
/// is needed.
async fn run_migrations(
    backend: Backend,
    settings: &DbSettings,
    pools: &DbPools,
) -> MicrosandboxResult<()> {
    use sea_orm::ConnectionTrait;

    if backend != Backend::Postgres {
        Migrator::up(pools.write().inner(), None).await?;
        return Ok(());
    }

    let migration_conn = DbWriteConnection::open(&DbSettings {
        max_connections: 1,
        ..settings.clone()
    })
    .await
    .map_err(|e| MicrosandboxError::Custom(format!("open migration connection: {e}")))?;

    let lock_key = migration_advisory_lock_key(settings.schema.as_deref());

    migration_conn
        .execute_unprepared(&format!("SELECT pg_advisory_lock({lock_key})"))
        .await?;

    let result: MicrosandboxResult<()> = async {
        if let Some(schema) = settings.schema.as_deref() {
            // Schema is already validated by `DbPools::open` -> resolve_settings,
            // but re-validate defensively before splicing it into DDL.
            let validated = validate_schema_name(schema)
                .map_err(|e| MicrosandboxError::InvalidConfig(e.to_string()))?;
            migration_conn
                .execute_unprepared(&format!(r#"CREATE SCHEMA IF NOT EXISTS "{validated}""#))
                .await?;
        }
        Migrator::up(migration_conn.inner(), None).await?;
        Ok(())
    }
    .await;

    // Best-effort unlock; the lock is released anyway once `migration_conn`
    // is dropped and its connection closes.
    let _ = migration_conn
        .execute_unprepared(&format!("SELECT pg_advisory_unlock({lock_key})"))
        .await;

    result
}

/// Derive the per-schema migration advisory lock key.
///
/// `None` schema (default `public`) uses the base key. Named schemas XOR
/// the base with a deterministic 64-bit hash of the schema bytes so
/// different schemas serialise their migrations independently.
fn migration_advisory_lock_key(schema: Option<&str>) -> i64 {
    let Some(name) = schema else {
        return MIGRATION_ADVISORY_LOCK_BASE;
    };
    // FNV-1a 64-bit. Deterministic across processes/architectures.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in name.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    MIGRATION_ADVISORY_LOCK_BASE ^ (h as i64)
}

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::Path;

    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};

    use super::*;

    /// The 10 tables migrations create (excluding sea-orm bookkeeping).
    const EXPECTED_TABLES: &[&str] = &[
        "config",
        "image_ref",
        "layer",
        "manifest",
        "manifest_layer",
        "run",
        "sandbox",
        "sandbox_rootfs",
        "snapshot_index",
        "volume",
    ];

    /// Build SQLite [`DbSettings`] rooted at `home` (the DB lands at
    /// `{home}/db/msb.db`), mirroring the production layout.
    fn sqlite_settings(home: &Path) -> DbSettings {
        DbSettings {
            url: crate::config::default_sqlite_url(home),
            schema: None,
            max_connections: 5,
            connect_timeout: Duration::from_secs(30),
            busy_timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn test_connect_and_migrate_creates_db_and_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let settings = sqlite_settings(tmp.path());

        let pools = connect_and_migrate(&settings).await.unwrap();
        let conn = pools.read();

        // DB file should exist on disk.
        let db_path = tmp
            .path()
            .join(microsandbox_utils::DB_SUBDIR)
            .join(microsandbox_utils::DB_FILENAME);
        assert!(db_path.exists());

        // All 10 tables should be present.
        let rows = conn
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'seaql_%' AND name != 'sqlite_sequence' ORDER BY name",
            ))
            .await
            .unwrap();

        let table_names: Vec<String> = rows
            .iter()
            .map(|r| r.try_get_by_index::<String>(0).unwrap())
            .collect();

        assert_eq!(table_names, EXPECTED_TABLES);
    }

    #[tokio::test]
    async fn test_connect_and_migrate_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let settings = sqlite_settings(tmp.path());

        let pools = connect_and_migrate(&settings).await.unwrap();

        // Running migrations again on the same DB should succeed.
        Migrator::up(pools.write().inner(), None).await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_and_migrate_recovers_from_partial_storage_migration() {
        let tmp = tempfile::tempdir().unwrap();
        let settings = sqlite_settings(tmp.path());
        let db_dir = tmp.path().join(microsandbox_utils::DB_SUBDIR);
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

        // Simulate a half-applied migration 3: the storage tables and the first
        // snapshot index exist, but migration 3 itself was never recorded.
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

        let pending_before = conn
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) FROM seaql_migrations WHERE version = 'm20260305_000003_create_storage_tables'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index::<i64>(0)
            .unwrap();
        assert_eq!(pending_before, 0);

        drop(conn);

        let recovered = connect_and_migrate(&settings).await.unwrap();

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

        let legacy_index_count = recovered
            .read()
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index'
                   AND name IN (
                       'idx_snapshots_name_sandbox_unique',
                       'idx_snapshots_name_unique_no_sandbox'
                   )",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index::<i64>(0)
            .unwrap();
        assert_eq!(legacy_index_count, 0);

        let new_index_count = recovered
            .read()
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index'
                   AND name IN (
                       'idx_snapshot_index_name',
                       'idx_snapshot_index_parent',
                       'idx_snapshot_index_image'
                   )",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index::<i64>(0)
            .unwrap();
        assert_eq!(new_index_count, 3);
    }

    //----------------------------------------------------------------------------------------------
    // migration_advisory_lock_key
    //----------------------------------------------------------------------------------------------

    #[test]
    fn lock_key_default_for_no_schema() {
        assert_eq!(
            migration_advisory_lock_key(None),
            MIGRATION_ADVISORY_LOCK_BASE
        );
    }

    #[test]
    fn lock_key_differs_per_schema() {
        let a = migration_advisory_lock_key(Some("node_a"));
        let b = migration_advisory_lock_key(Some("node_b"));
        let base = migration_advisory_lock_key(None);
        assert_ne!(a, b);
        assert_ne!(a, base);
        assert_ne!(b, base);
    }

    #[test]
    fn lock_key_deterministic() {
        assert_eq!(
            migration_advisory_lock_key(Some("node_a")),
            migration_advisory_lock_key(Some("node_a"))
        );
    }

    //----------------------------------------------------------------------------------------------
    // PostgreSQL smoke tests (opt-in)
    //----------------------------------------------------------------------------------------------

    /// Connect + migrate against a real PostgreSQL server.
    ///
    /// Ignored by default since it needs a server. To run it:
    ///
    /// ```sh
    /// docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
    /// MSB_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
    ///   cargo test -p microsandbox test_connect_and_migrate_postgres -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires MSB_TEST_DATABASE_URL pointing at a PostgreSQL server"]
    async fn test_connect_and_migrate_postgres() {
        let url = std::env::var("MSB_TEST_DATABASE_URL")
            .expect("set MSB_TEST_DATABASE_URL to a postgres:// URL");
        assert_eq!(Backend::from_url(&url).unwrap(), Backend::Postgres);

        let settings = DbSettings {
            url,
            schema: None,
            max_connections: 5,
            connect_timeout: Duration::from_secs(30),
            busy_timeout: Duration::from_secs(5),
        };

        let pools = connect_and_migrate(&settings).await.unwrap();

        // All 10 tables should exist in the current schema. Use
        // `current_schema()` rather than hard-coding `public` so the test
        // also passes when run against a custom schema.
        let rows = pools
            .read()
            .query_all(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = current_schema() \
                   AND table_name NOT LIKE 'seaql_%' \
                 ORDER BY table_name",
            ))
            .await
            .unwrap();
        let table_names: Vec<String> = rows
            .iter()
            .map(|r| r.try_get_by_index::<String>(0).unwrap())
            .collect();
        assert_eq!(table_names, EXPECTED_TABLES);

        // Re-running migrations on the same database must be a no-op.
        connect_and_migrate(&settings).await.unwrap();
    }

    /// Connect + migrate against a real PostgreSQL server using a
    /// non-default schema.
    ///
    /// Verifies the schema is auto-created, that tables land in it (not
    /// `public`), and that the advisory-lock key differs from the default
    /// so concurrent runs against different schemas don't serialise.
    ///
    /// To run:
    /// ```sh
    /// MSB_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
    ///   cargo test -p microsandbox test_connect_and_migrate_postgres_with_schema \
    ///   -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "requires MSB_TEST_DATABASE_URL pointing at a PostgreSQL server"]
    async fn test_connect_and_migrate_postgres_with_schema() {
        let url = std::env::var("MSB_TEST_DATABASE_URL")
            .expect("set MSB_TEST_DATABASE_URL to a postgres:// URL");

        // Unique schema per run so repeated invocations don't collide.
        let schema = format!(
            "msb_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let settings = DbSettings {
            url: url.clone(),
            schema: Some(schema.clone()),
            max_connections: 5,
            connect_timeout: Duration::from_secs(30),
            busy_timeout: Duration::from_secs(5),
        };

        let pools = connect_and_migrate(&settings).await.unwrap();

        // Tables exist in *this* schema, not `public`.
        let rows = pools
            .read()
            .query_all(Statement::from_string(
                DatabaseBackend::Postgres,
                format!(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_schema = '{schema}' \
                       AND table_name NOT LIKE 'seaql_%' \
                     ORDER BY table_name"
                ),
            ))
            .await
            .unwrap();
        let table_names: Vec<String> = rows
            .iter()
            .map(|r| r.try_get_by_index::<String>(0).unwrap())
            .collect();
        assert_eq!(table_names, EXPECTED_TABLES);

        // search_path is pinned to our schema, so `current_schema()`
        // returns it.
        let current = pools
            .read()
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT current_schema()",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index::<String>(0)
            .unwrap();
        assert_eq!(current, schema);

        // Cleanup so repeated test runs against the same server don't
        // leak schemas.
        let cleanup_conn = Database::connect(&url).await.unwrap();
        cleanup_conn
            .execute_unprepared(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .await
            .unwrap();
    }
}
