import { napi } from "./internal/napi.js";

/**
 * Set the database connection URL for this process
 * (e.g. `postgres://user:pw@host/db` or `sqlite:///path/to/db.db`).
 *
 * Call before any sandbox or database operation. Set-once: subsequent
 * calls are ignored. Overrides the `MSB_DATABASE_URL` environment variable
 * and the `database.url` field in `~/.microsandbox/config.json`.
 */
export function setDatabaseUrl(url: string): void {
  napi.setDatabaseUrl(url);
}

/**
 * Set the PostgreSQL schema (`search_path`) for this process.
 *
 * Call before any sandbox or database operation. Set-once: subsequent
 * calls are ignored. Overrides `MSB_DATABASE_SCHEMA`, `database.schema`
 * in `~/.microsandbox/config.json`, and any `search_path` embedded in
 * the URL's `options` parameter. microsandbox creates the schema if it
 * does not already exist. Ignored on SQLite.
 */
export function setDatabaseSchema(name: string): void {
  napi.setDatabaseSchema(name);
}
