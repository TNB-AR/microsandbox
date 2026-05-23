package microsandbox

import "github.com/superradcompany/microsandbox/sdk/go/internal/ffi"

// SetDatabaseURL sets the database connection URL for this process
// (e.g. `postgres://user:pw@host/db` or `sqlite:///path/to/db.db`).
//
// Must be called before any sandbox or database operation. Set-once on
// the Rust side: subsequent calls are ignored. Overrides the
// MSB_DATABASE_URL environment variable and the `database.url` field in
// `~/.microsandbox/config.json`.
//
// An empty url is a no-op.
func SetDatabaseURL(url string) {
	ffi.SetDatabaseURL(url)
}

// SetDatabaseSchema sets the PostgreSQL schema (`search_path`) for this
// process.
//
// Must be called before any sandbox or database operation. Set-once on
// the Rust side: subsequent calls are ignored. Overrides
// MSB_DATABASE_SCHEMA, `database.schema` in `~/.microsandbox/config.json`,
// and any `search_path` embedded in the URL's `options` parameter.
// microsandbox creates the schema if it does not already exist. Ignored
// on SQLite.
//
// An empty name is a no-op.
func SetDatabaseSchema(name string) {
	ffi.SetDatabaseSchema(name)
}
