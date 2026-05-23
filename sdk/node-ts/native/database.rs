use napi_derive::napi;

/// Set the database connection URL (e.g. `postgres://...` or
/// `sqlite://...`). Call before any sandbox or database operation.
/// Set-once: subsequent calls are ignored.
#[napi]
pub fn set_database_url(url: String) {
    microsandbox::config::set_database_url(url);
}

/// Set the PostgreSQL schema (`search_path`) for this process. Call
/// before any sandbox or database operation. Set-once: subsequent calls
/// are ignored. Ignored on SQLite.
#[napi]
pub fn set_database_schema(name: String) {
    microsandbox::config::set_database_schema(name);
}
