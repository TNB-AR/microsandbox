//! Database backend selection.
//!
//! Microsandbox can run against more than one SQL backend. Which one a
//! given connection targets is decided at runtime from the scheme of the
//! connection URL (`sqlite://...` vs `postgres://...`). [`Backend`] is the
//! single place that mapping lives — adding another SQL backend later is
//! one enum variant plus one arm in each `match` that branches on it.
//!
//! At the public surface, configuration is flat (`DbSettings` in
//! [`crate::pool`]); inside the crate we resolve into [`ResolvedDb`] so
//! pool and migration code is statically backend-aware — Postgres code
//! cannot reach `busy_timeout` and SQLite code cannot reach `schema`.

use std::{fmt, time::Duration};

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// A SQL backend microsandbox can run against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Embedded SQLite database stored as a local file.
    Sqlite,

    /// PostgreSQL server reached over the network.
    Postgres,
}

/// A database connection URL microsandbox cannot use.
#[derive(Debug, thiserror::Error)]
pub enum DbConfigError {
    /// The URL has no `scheme:` prefix.
    #[error("malformed database URL: `{0}` (expected e.g. `sqlite://...` or `postgres://...`)")]
    MalformedUrl(String),

    /// The URL scheme names a backend microsandbox does not support.
    #[error("unsupported database backend `{0}`: expected `sqlite` or `postgres`")]
    UnsupportedScheme(String),

    /// The configured schema name is not a valid PostgreSQL identifier.
    #[error("invalid schema name: {0}")]
    InvalidSchemaName(String),
}

/// An error opening database pools.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// The connection URL could not be classified.
    #[error(transparent)]
    Config(#[from] DbConfigError),

    /// The underlying driver failed to connect.
    #[error("database connection failed: {0}")]
    Connect(#[from] sqlx::Error),
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl Backend {
    /// Identify the backend from a connection URL by its scheme.
    ///
    /// `sqlite://...` maps to [`Backend::Sqlite`]; `postgres://...` and
    /// `postgresql://...` map to [`Backend::Postgres`]. Any other scheme is
    /// rejected with [`DbConfigError::UnsupportedScheme`].
    pub fn from_url(url: &str) -> Result<Self, DbConfigError> {
        let scheme = url
            .split_once(':')
            .map(|(scheme, _)| scheme)
            .filter(|scheme| !scheme.is_empty())
            .ok_or_else(|| DbConfigError::MalformedUrl(url.to_string()))?;

        match scheme {
            "sqlite" => Ok(Backend::Sqlite),
            "postgres" | "postgresql" => Ok(Backend::Postgres),
            other => Err(DbConfigError::UnsupportedScheme(other.to_string())),
        }
    }

    /// Whether this backend serialises writes through a single connection.
    ///
    /// SQLite is single-writer for the whole database file, so microsandbox
    /// funnels writes through one connection to turn lock contention into a
    /// deterministic in-process queue. Server backends accept concurrent
    /// writers natively and use ordinary multi-connection pools.
    pub fn wants_single_writer(self) -> bool {
        matches!(self, Backend::Sqlite)
    }
}

//--------------------------------------------------------------------------------------------------
// Trait Implementations
//--------------------------------------------------------------------------------------------------

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backend::Sqlite => write!(f, "sqlite"),
            Backend::Postgres => write!(f, "postgres"),
        }
    }
}

//--------------------------------------------------------------------------------------------------
// Resolved (typed) settings
//--------------------------------------------------------------------------------------------------

/// Per-backend typed settings, produced by resolving a flat
/// [`crate::pool::DbSettings`]. Pool and migration code take these by
/// reference so backend-irrelevant fields are unreachable at compile time.
#[derive(Debug, Clone)]
pub enum ResolvedDb {
    /// SQLite settings (no schema, has busy timeout).
    Sqlite(SqliteSettings),
    /// PostgreSQL settings (optional schema, no busy timeout).
    Postgres(PostgresSettings),
}

/// Inputs for opening a SQLite pool.
#[derive(Debug, Clone)]
pub struct SqliteSettings {
    /// `sqlite://...` URL.
    pub url: String,
    /// Pool size (1 for the dedicated write pool; `max_connections` for the read pool).
    pub max_connections: u32,
    /// Timeout when acquiring a connection from the pool.
    pub connect_timeout: Duration,
    /// SQLite `busy_timeout` PRAGMA.
    pub busy_timeout: Duration,
}

/// Inputs for opening a PostgreSQL pool.
#[derive(Debug, Clone)]
pub struct PostgresSettings {
    /// `postgres://...` URL.
    pub url: String,
    /// Optional schema. When `Some`, microsandbox creates it if missing and
    /// pins `search_path` to it. Already validated (see
    /// [`validate_schema_name`]).
    pub schema: Option<String>,
    /// Pool size — applies to both the read and write pools.
    pub max_connections: u32,
    /// Timeout when acquiring a connection from the pool.
    pub connect_timeout: Duration,
}

//--------------------------------------------------------------------------------------------------
// Schema validation
//--------------------------------------------------------------------------------------------------

/// PostgreSQL identifier limit in bytes.
pub const POSTGRES_IDENTIFIER_MAX_BYTES: usize = 63;

/// Validate a PostgreSQL schema name.
///
/// Rules: non-empty, ≤63 bytes, characters `[A-Za-z0-9_]` only, must not
/// start with a digit, must not start with `pg_` (reserved by PostgreSQL).
/// Schema names go into DDL (`CREATE SCHEMA IF NOT EXISTS "<name>"`), so
/// validation here is the defence against injection-shaped input.
pub fn validate_schema_name(name: &str) -> Result<&str, DbConfigError> {
    if name.is_empty() {
        return Err(DbConfigError::InvalidSchemaName(
            "schema name must not be empty".into(),
        ));
    }
    if name.len() > POSTGRES_IDENTIFIER_MAX_BYTES {
        return Err(DbConfigError::InvalidSchemaName(format!(
            "schema name `{name}` exceeds the {POSTGRES_IDENTIFIER_MAX_BYTES}-byte PostgreSQL identifier limit",
        )));
    }
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(DbConfigError::InvalidSchemaName(format!(
            "schema name `{name}` must not start with a digit",
        )));
    }
    if name.starts_with("pg_") {
        return Err(DbConfigError::InvalidSchemaName(format!(
            "schema name `{name}` is reserved (PostgreSQL reserves names starting with `pg_`)",
        )));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(DbConfigError::InvalidSchemaName(format!(
            "schema name `{name}` may only contain [A-Za-z0-9_]",
        )));
    }
    Ok(name)
}

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_sqlite_urls() {
        assert_eq!(
            Backend::from_url("sqlite:///home/user/.microsandbox/db/msb.db").unwrap(),
            Backend::Sqlite
        );
        assert!(Backend::Sqlite.wants_single_writer());
    }

    #[test]
    fn classifies_postgres_urls() {
        assert_eq!(
            Backend::from_url("postgres://user:pw@localhost:5432/msb").unwrap(),
            Backend::Postgres
        );
        assert_eq!(
            Backend::from_url("postgresql://localhost/msb").unwrap(),
            Backend::Postgres
        );
        assert!(!Backend::Postgres.wants_single_writer());
    }

    #[test]
    fn rejects_unknown_scheme() {
        assert!(matches!(
            Backend::from_url("mysql://localhost/msb"),
            Err(DbConfigError::UnsupportedScheme(scheme)) if scheme == "mysql"
        ));
    }

    #[test]
    fn rejects_url_without_scheme() {
        assert!(matches!(
            Backend::from_url("/just/a/path/msb.db"),
            Err(DbConfigError::MalformedUrl(_))
        ));
    }

    #[test]
    fn schema_validation_accepts_normal_names() {
        assert_eq!(validate_schema_name("node_a").unwrap(), "node_a");
        assert_eq!(validate_schema_name("Node1").unwrap(), "Node1");
        assert_eq!(validate_schema_name("a").unwrap(), "a");
        assert_eq!(validate_schema_name("_underscore").unwrap(), "_underscore");
    }

    #[test]
    fn schema_validation_rejects_empty() {
        assert!(matches!(
            validate_schema_name(""),
            Err(DbConfigError::InvalidSchemaName(_))
        ));
    }

    #[test]
    fn schema_validation_rejects_too_long() {
        let name = "a".repeat(POSTGRES_IDENTIFIER_MAX_BYTES + 1);
        assert!(matches!(
            validate_schema_name(&name),
            Err(DbConfigError::InvalidSchemaName(_))
        ));
    }

    #[test]
    fn schema_validation_rejects_leading_digit() {
        assert!(matches!(
            validate_schema_name("1node"),
            Err(DbConfigError::InvalidSchemaName(_))
        ));
    }

    #[test]
    fn schema_validation_rejects_pg_prefix() {
        assert!(matches!(
            validate_schema_name("pg_internal"),
            Err(DbConfigError::InvalidSchemaName(_))
        ));
    }

    #[test]
    fn schema_validation_rejects_special_chars() {
        for bad in &["node-a", "node a", "node;drop", "node.a", "node\"a"] {
            assert!(
                matches!(
                    validate_schema_name(bad),
                    Err(DbConfigError::InvalidSchemaName(_))
                ),
                "expected `{bad}` to be rejected"
            );
        }
    }
}
