//! Connection builders shared by every microsandbox process.
//!
//! Microsandbox can run against more than one SQL backend; the backend is
//! selected at runtime from the connection URL scheme (see
//! [`crate::backend::Backend`]). This module turns a [`DbSettings`] into
//! ready-to-use [`DbReadConnection`] / [`DbWriteConnection`] pools,
//! applying the per-backend connection setup in one place:
//!
//! - **SQLite** — PRAGMAs (WAL, busy timeout, foreign keys,
//!   `synchronous=NORMAL`) are applied to every connection. The write pool
//!   is a single connection because SQLite is single-writer system-wide;
//!   funnelling writes through one connection turns lock contention into a
//!   deterministic in-process queue.
//! - **PostgreSQL** — both pools are ordinary multi-connection pools; the
//!   server accepts concurrent writers natively. If the caller set
//!   [`DbSettings::schema`], the pool pins `search_path` to that schema on
//!   every connection (and microsandbox separately ensures the schema
//!   exists; see `connect_and_migrate` in the host crate).
//!
//! Internally the flat [`DbSettings`] is resolved into a
//! [`crate::backend::ResolvedDb`] so the per-backend builders take typed
//! structs — Postgres code can't reach `busy_timeout` and SQLite code
//! can't reach `schema` by construction.

use std::{str::FromStr, time::Duration};

use sea_orm::{DatabaseConnection, SqlxPostgresConnector, SqlxSqliteConnector};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

use crate::{
    backend::{
        Backend, DbError, PostgresSettings, ResolvedDb, SqliteSettings, validate_schema_name,
    },
    connection::{DbReadConnection, DbWriteConnection},
};

//--------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------

/// Default `busy_timeout` value used when a caller has no user-facing knob
/// to plumb (e.g. the in-VM runtime, where the host owns DB-tuning policy).
/// Applies to SQLite only; ignored by other backends.
pub const DEFAULT_BUSY_TIMEOUT_SECS: u64 = 5;

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Backend-independent inputs for opening database pools.
///
/// This is the flat surface used by every caller (host CLI, in-VM
/// supervisor, SDK). Backend-irrelevant fields are silently ignored at
/// resolve time (`busy_timeout` on Postgres, `schema` on SQLite), so a
/// global config carrying both is harmless on either backend.
#[derive(Debug, Clone)]
pub struct DbSettings {
    /// Connection URL. The scheme selects the backend:
    /// `sqlite://<path>` or `postgres://<host>/<database>`.
    pub url: String,

    /// PostgreSQL schema name to pin `search_path` to. Ignored when the
    /// resolved backend is SQLite (with a debug log). When `Some`, the
    /// host crate also runs `CREATE SCHEMA IF NOT EXISTS` so a fresh
    /// node is self-configuring.
    pub schema: Option<String>,

    /// Pool size. For SQLite this sizes the read pool only — the write
    /// pool is always a single connection. For PostgreSQL it sizes both
    /// the read and write pools.
    pub max_connections: u32,

    /// Timeout when acquiring a connection from the pool.
    pub connect_timeout: Duration,

    /// SQLite `busy_timeout`: how long SQLite spins internally on a
    /// contended lock before surfacing `SQLITE_BUSY` to the retry layer.
    /// Ignored by non-SQLite backends.
    pub busy_timeout: Duration,
}

/// A read pool and a write pool for the same database.
///
/// For SQLite the write pool is a dedicated single connection (see the
/// module docs); reads keep a wider pool — WAL mode lets readers run
/// concurrently. For PostgreSQL both pools are ordinary multi-connection
/// pools. Either way callers pick `read()` or `write()` and the type
/// system blocks accidental writes against the read pool.
#[derive(Debug)]
pub struct DbPools {
    read: DbReadConnection,
    write: DbWriteConnection,
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl DbPools {
    /// Open both pools for the database described by `settings`.
    ///
    /// For SQLite the write pool connects first so WAL mode (persisted in
    /// the database header) is set before the read pool opens.
    pub async fn open(settings: &DbSettings) -> Result<Self, DbError> {
        let write = DbWriteConnection::open(settings).await?;
        let read = DbReadConnection::open(settings).await?;
        Ok(Self { read, write })
    }

    /// Borrow the read pool.
    pub fn read(&self) -> &DbReadConnection {
        &self.read
    }

    /// Borrow the write pool (retries handled inside for SQLite).
    pub fn write(&self) -> &DbWriteConnection {
        &self.write
    }
}

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

/// Build the sea-orm connection backing a read pool for `settings`.
pub(crate) async fn build_read_pool(settings: &DbSettings) -> Result<DatabaseConnection, DbError> {
    match resolve_settings(settings)? {
        ResolvedDb::Sqlite(s) => {
            let max = s.max_connections;
            build_sqlite_pool(&s, max).await
        }
        ResolvedDb::Postgres(s) => {
            let max = s.max_connections;
            build_pg_pool(&s, max).await
        }
    }
}

/// Build the sea-orm connection backing a write pool for `settings`.
///
/// SQLite write pools are always a single connection; PostgreSQL write
/// pools are sized like read pools.
pub(crate) async fn build_write_pool(settings: &DbSettings) -> Result<DatabaseConnection, DbError> {
    match resolve_settings(settings)? {
        ResolvedDb::Sqlite(s) => build_sqlite_pool(&s, 1).await,
        ResolvedDb::Postgres(s) => {
            let max = s.max_connections;
            build_pg_pool(&s, max).await
        }
    }
}

/// Resolve the flat [`DbSettings`] into the typed [`ResolvedDb`] enum.
///
/// Validates `schema` against the PostgreSQL identifier rules when
/// applicable (Postgres + schema set); the field is dropped silently on
/// SQLite with a debug log (so a persisted `database.schema` in a global
/// config is harmless on SQLite hosts).
fn resolve_settings(settings: &DbSettings) -> Result<ResolvedDb, DbError> {
    match Backend::from_url(&settings.url)? {
        Backend::Sqlite => {
            if settings.schema.is_some() {
                tracing::debug!(
                    "ignoring `database.schema` for SQLite backend (SQLite has no schema namespaces)"
                );
            }
            Ok(ResolvedDb::Sqlite(SqliteSettings {
                url: settings.url.clone(),
                max_connections: settings.max_connections,
                connect_timeout: settings.connect_timeout,
                busy_timeout: settings.busy_timeout,
            }))
        }
        Backend::Postgres => {
            let schema = match &settings.schema {
                Some(name) => Some(validate_schema_name(name)?.to_string()),
                None => None,
            };
            Ok(ResolvedDb::Postgres(PostgresSettings {
                url: settings.url.clone(),
                schema,
                max_connections: settings.max_connections,
                connect_timeout: settings.connect_timeout,
            }))
        }
    }
}

/// Open a sqlx-backed SQLite pool wrapped as a sea-orm `DatabaseConnection`.
///
/// PRAGMAs are applied to every connection via `SqliteConnectOptions`, so
/// callers don't need to issue setup SQL. The parent directory of the
/// database file is created if missing — `create_if_missing` only creates
/// the file itself.
async fn build_sqlite_pool(
    settings: &SqliteSettings,
    max_connections: u32,
) -> Result<DatabaseConnection, DbError> {
    let connect_options = SqliteConnectOptions::from_str(&settings.url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(settings.busy_timeout)
        .foreign_keys(true)
        .synchronous(SqliteSynchronous::Normal);

    if let Some(parent) = connect_options.get_filename().parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(sqlx::Error::Io)?;
    }

    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections.max(1))
        .acquire_timeout(settings.connect_timeout)
        .connect_with(connect_options)
        .await?;

    Ok(SqlxSqliteConnector::from_sqlx_sqlite_pool(pool))
}

/// Open a sqlx-backed PostgreSQL pool wrapped as a sea-orm
/// `DatabaseConnection`.
///
/// When `settings.schema` is set, `search_path` is pinned to it on every
/// connection. sqlx's `PgConnectOptions::options` appends to the libpq
/// `options` string, and libpq applies multiple `-c name=value` settings
/// in order with last-wins semantics — so an explicit schema overrides
/// any `search_path` embedded in the URL's `options` parameter. An INFO
/// log makes the override visible.
async fn build_pg_pool(
    settings: &PostgresSettings,
    max_connections: u32,
) -> Result<DatabaseConnection, DbError> {
    let mut connect_options = PgConnectOptions::from_str(&settings.url)?;

    if let Some(schema) = &settings.schema {
        if url_embeds_search_path(&settings.url) {
            tracing::info!(
                schema = %schema,
                "explicit schema overrides `search_path` embedded in the database URL"
            );
        }
        connect_options = connect_options.options([("search_path", schema.as_str())]);
    }

    let pool = PgPoolOptions::new()
        .max_connections(max_connections.max(1))
        .acquire_timeout(settings.connect_timeout)
        .connect_with(connect_options)
        .await?;

    Ok(SqlxPostgresConnector::from_sqlx_postgres_pool(pool))
}

/// Best-effort detection of `search_path` embedded in a Postgres URL's
/// `options` query parameter. Used solely to decide whether to emit the
/// override INFO log — the actual override is libpq's last-wins behaviour.
fn url_embeds_search_path(url: &str) -> bool {
    url.contains("search_path")
}

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(url: &str, schema: Option<&str>) -> DbSettings {
        DbSettings {
            url: url.into(),
            schema: schema.map(str::to_string),
            max_connections: 5,
            connect_timeout: Duration::from_secs(30),
            busy_timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn resolves_sqlite_settings() {
        let r = resolve_settings(&settings("sqlite:///tmp/x.db", None)).unwrap();
        assert!(matches!(r, ResolvedDb::Sqlite(_)));
    }

    #[test]
    fn resolves_postgres_with_schema() {
        let r = resolve_settings(&settings("postgres://h/db", Some("node_a"))).unwrap();
        match r {
            ResolvedDb::Postgres(s) => assert_eq!(s.schema.as_deref(), Some("node_a")),
            _ => panic!("expected postgres"),
        }
    }

    #[test]
    fn resolves_postgres_without_schema() {
        let r = resolve_settings(&settings("postgres://h/db", None)).unwrap();
        match r {
            ResolvedDb::Postgres(s) => assert!(s.schema.is_none()),
            _ => panic!("expected postgres"),
        }
    }

    #[test]
    fn sqlite_silently_drops_schema() {
        // SQLite ignores schema setting; resolution succeeds.
        let r = resolve_settings(&settings("sqlite:///tmp/x.db", Some("any_name"))).unwrap();
        assert!(matches!(r, ResolvedDb::Sqlite(_)));
    }

    #[test]
    fn postgres_rejects_invalid_schema() {
        assert!(resolve_settings(&settings("postgres://h/db", Some("bad-name"))).is_err());
        assert!(resolve_settings(&settings("postgres://h/db", Some("1node"))).is_err());
    }

    #[test]
    fn detects_search_path_in_url() {
        assert!(url_embeds_search_path(
            "postgres://h/db?options=-csearch_path%3Dnode_a"
        ));
        assert!(url_embeds_search_path(
            "postgres://h/db?options=-c+search_path=node_a"
        ));
        assert!(!url_embeds_search_path("postgres://h/db"));
        assert!(!url_embeds_search_path(
            "postgres://h/db?options=-capplication_name=msb"
        ));
    }
}
