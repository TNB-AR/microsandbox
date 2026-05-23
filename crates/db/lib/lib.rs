//! Shared database entity definitions and connection helpers for the
//! microsandbox project.
//!
//! Used by both `microsandbox` (host CLI) and `microsandbox-runtime`
//! (in-VM supervisor). The backend (SQLite or PostgreSQL) is selected at
//! runtime from the connection URL; the connection builders live here so
//! per-backend setup stays in one place.

#![warn(missing_docs)]

//--------------------------------------------------------------------------------------------------
// Exports
//--------------------------------------------------------------------------------------------------

pub mod backend;
pub mod connection;
#[allow(missing_docs)]
pub mod entity;
pub mod pool;
pub mod retry;

pub use backend::{
    Backend, DbConfigError, DbError, PostgresSettings, ResolvedDb, SqliteSettings,
    validate_schema_name,
};
pub use connection::{DbReadConnection, DbWriteConnection};
pub use pool::{DbPools, DbSettings};
