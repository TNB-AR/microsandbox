# Multi-Backend Database Support (SQLite + PostgreSQL)

## Context

Microsandbox was originally hard-wired to SQLite. The connection layer built
`SqliteConnectOptions` directly, the workspace `Cargo.toml` only enabled the
`sqlx-sqlite` driver, and the write path was shaped around SQLite's
single-writer model (`SQLITE_BUSY` retry loop, single-connection write pool).

This change adds **PostgreSQL** alongside SQLite, with an abstraction clean
enough to add more SQL backends later (MySQL etc.) without touching call
sites. It also lays the groundwork for **multi-node deployments**: a shared
Postgres server with one schema per node, so each node's state stays
host-local while a control plane can query across the fleet.

## Decisions (settled)

- **Runtime selection, single binary.** Both the SQLite and Postgres drivers
  are always compiled in. The backend is chosen at startup from the
  connection URL scheme (`sqlite://` vs `postgres://`). No Cargo feature
  gating.
- **Backend abstraction = a thin `Backend` enum + an internal `ResolvedDb`
  enum.** SeaORM already handles dialect-correct SQL. The closed set of
  non-portable concerns (connect options, pool topology, transient-error
  retry, advisory locking) is modelled best as enum variants — extensible by
  adding one variant + one match arm per concern. Adding a backend never
  touches queries, entities, or migrations.
- **Flat `DatabaseConfig` on disk and across SDK/CLI surfaces.** The URL
  scheme is the discriminator; the struct stays flat. No tagged enum on
  disk, no per-backend variants in `config.json`, no builders. The internal
  `ResolvedDb` is invisible to users — it exists only so pool/migration code
  is statically backend-aware (invalid field combinations unrepresentable).
- **Postgres applies to every DB-writing surface.** Host CLI **and** the
  in-VM supervisor connect to whatever backend the host configured. The
  supervisor is in the trusted domain; reaching an external Postgres is
  fine.
- **Schema-per-node is the multi-node model.** PostgreSQL schemas namespace
  the data per node. All queries use unqualified table names, so a
  per-connection `search_path` does the work — zero query / migration
  changes. A shared single schema would break host-local invariants (PIDs,
  artifact paths, the reaper, name uniqueness) and is rejected.
- **`--db-schema` (and siblings) is the single source of truth for schema.**
  If the URL also embeds `options=-c search_path=...`, the explicit setting
  **overrides** with an INFO log. If no explicit schema is set anywhere,
  microsandbox leaves `search_path` alone — the URL or role default applies
  (the "pre-create" pattern).
- **Schema is Postgres-only.** SQLite has no schema namespaces; setting one
  is a debug-logged no-op (so a persisted `database.schema` in a global
  config.json doesn't break SQLite invocations).
- **Auto-create the schema** (`CREATE SCHEMA IF NOT EXISTS`) so a fresh
  node is self-configuring with no DBA step. The role just needs `CREATE`
  on the database; `CREATE SCHEMA IF NOT EXISTS` is idempotent after that.
- **Programmatic API in every SDK.** Rust, Python, Node-TS, and Go all
  expose `set_database_url(url)` and `set_database_schema(name)` (with
  language-idiomatic casing). No `_override` suffix — matches the
  `set_config` / `set_sdk_msb_path` convention.

## Architecture

### `Backend` enum (public)

```rust
pub enum Backend { Sqlite, Postgres }
```

Selected from URL scheme via `Backend::from_url`. Adding MySQL = one variant
+ one match arm per concern.

### `DbSettings` (public, flat)

Backend-independent inputs:

```rust
pub struct DbSettings {
    pub url: String,
    pub schema: Option<String>,         // Postgres-only; ignored on SQLite
    pub max_connections: u32,
    pub connect_timeout: Duration,
    pub busy_timeout: Duration,         // SQLite-only; ignored on Postgres
}
```

### `ResolvedDb` (internal, typed)

After classification, `DbSettings` resolves once into:

```rust
pub(crate) enum ResolvedDb {
    Sqlite(SqliteSettings),    // url, max_connections, connect_timeout, busy_timeout
    Postgres(PostgresSettings),// url, schema, max_connections, connect_timeout
}
```

Pool builders and the migration runner consume `&SqliteSettings` /
`&PostgresSettings`. Postgres code physically cannot reference
`busy_timeout`; SQLite code physically cannot reference `schema`. The
compiler enumerates the work when a new backend is added.

### Pool topology

- **SQLite** — multi-connection read pool + single-connection write pool
  (single-writer to dodge `SQLITE_BUSY`); PRAGMAs (WAL, busy_timeout,
  foreign_keys, synchronous=NORMAL) on every connection.
- **Postgres** — both pools are ordinary multi-connection pools; server
  handles concurrent writers natively.

Both wrap `DatabaseConnection` in `DbReadConnection` / `DbWriteConnection`
so existing call sites and `ConnectionTrait` impls are unchanged.

## Configuration surface

### Four channels, two settings

| Source | URL | Schema (Postgres-only) |
|---|---|---|
| Programmatic | `set_database_url(url)` | `set_database_schema(name)` |
| CLI | `--database <URL>` | `--db-schema <NAME>` |
| Env | `MSB_DATABASE_URL` | `MSB_DATABASE_SCHEMA` |
| `config.json` | `database.url` | `database.schema` |

Programmatic API is exposed identically (language-idiomatic casing) in all
four SDKs:

| Language | URL | Schema |
|---|---|---|
| Rust | `microsandbox::config::set_database_url` | `microsandbox::config::set_database_schema` |
| Python | `microsandbox.set_database_url` | `microsandbox.set_database_schema` |
| Node-TS | `setDatabaseUrl` | `setDatabaseSchema` |
| Go | `microsandbox.SetDatabaseURL` | `microsandbox.SetDatabaseSchema` |

The Rust setters live in `config` alongside `set_sdk_msb_path` — they are
process-level configuration, set once before the global pool opens. The
non-Rust SDKs already flatten Rust's submodule structure to the package
root (e.g. Python's `microsandbox.set_runtime_msb_path` wraps Rust's
`config::set_sdk_msb_path`), so flat names match the existing
language-idiomatic convention.

### Precedence ladder (highest first, identical for URL and schema)

1. Programmatic — `set_database_url()` / `set_database_schema()`
2. CLI flag — `--database` / `--db-schema`
3. Env var — `MSB_DATABASE_URL` / `MSB_DATABASE_SCHEMA`
4. `config.json` — `database.url` / `database.schema`
5. Default — `sqlite://{home}/db/msb.db` for URL; for schema: URL-embedded
   `search_path` if present, else Postgres role / DB default.

### URL vs explicit-schema policy

- **Explicit schema set** (any tier) → wins. We strip any
  `search_path` from the URL's `options` and apply our own. Log an `INFO`
  describing the override so it is visible, not silent.
- **No explicit schema** → we touch nothing. Whatever the URL or role
  default specifies applies. If that schema doesn't exist, the operator is
  responsible for pre-creating it (the "pre-create" pattern).

### Schema name validation

- Allowed: `[A-Za-z0-9_]+`
- Max length: 63 bytes (Postgres identifier limit)
- Disallowed: leading digit, names beginning with `pg_` (reserved)
- Rejected at the entry points (`set_database_schema`, env, config, flag)
  with a clear `DbConfigError::InvalidSchemaName`.

## What does *not* change

- Every SQL query is dialect-agnostic (SeaORM query builder; the 3
  formerly-SQLite-only `execute_unprepared` sites in `snapshot/store.rs`
  rewritten to portable builders).
- Every entity declares only its table name — no schema qualification.
- `seaql_migrations` bookkeeping lands inside whatever schema
  `search_path` selects, so per-node migration tracking is automatic.
- The application layer is **schema-blind**. Schema lives in exactly two
  places: connection setup (`PgConnectOptions::options(search_path=...)`)
  and the startup path (`CREATE SCHEMA`, advisory-lock keying).

## In-VM supervisor

The host passes the resolved URL and schema to the supervisor through the
**environment**, not CLI args:

- `MSB_DATABASE_URL` — already wired
- `MSB_DATABASE_SCHEMA` — to wire alongside

`SandboxArgs` reads both via clap `env = "..."`. CLI args are visible in
`ps`/`/proc`; env vars are readable only by the owning user. Postgres URLs
can carry passwords, so env is the correct channel.

## Migration handling

- All seven migrations are SeaORM DSL — dialect-correct DDL emitted per
  backend. No DDL changes needed.
- Partial unique indexes (`CREATE UNIQUE INDEX ... WHERE ...`) work on
  both SQLite and Postgres.
- **PostgreSQL only:** `Migrator::up` runs inside a `pg_advisory_lock` so
  concurrent first-runs cannot race the `seaql_migrations` bookkeeping.
  The advisory-lock key is derived from the schema name (hash → i64) so
  different nodes (different schemas) do not serialise each other's
  migrations. A dedicated 1-connection pool ensures the session-scoped
  lock holds across all of the migrator's statements.

## Verification

1. `cargo check --workspace`, `cargo clippy` clean.
2. All existing tests pass; new unit tests for `Backend::from_url`,
   `resolve_database_url` precedence, `resolve_database_schema` precedence,
   schema-name validation.
3. `#[ignore]` Postgres smoke test: connect + migrate + assert tables
   exist in `current_schema()` (not hard-coded `public`).
4. `#[ignore]` Postgres schema smoke test: sets `--db-schema test_node`,
   verifies the schema is created and tables land in it.
5. `#[ignore]` integration test: boots 2–3 detached sandboxes against
   Postgres with a non-default schema, asserts `sandbox`/`run` rows by
   name, stops them, asserts `Terminated`/`Completed`/`exit_code 0`,
   removes them.
6. Manual end-to-end against a real Postgres server (16+).

## Critical files

- `crates/db/lib/backend.rs` — `Backend`, `DbConfigError`, `DbError`,
  `ResolvedDb`, `SqliteSettings`, `PostgresSettings`,
  `validate_schema_name`
- `crates/db/lib/pool.rs` — `DbSettings`, `DbPools::open`,
  `build_sqlite_pool`, `build_pg_pool` (strip URL search_path, apply ours)
- `crates/db/lib/connection.rs` — typed wrappers unchanged
- `crates/microsandbox/lib/config/mod.rs` — `DatabaseConfig.schema`;
  `set_database_url`, `set_database_schema`, `database_url`,
  `database_schema`, `resolve_database_url`, `resolve_database_schema`,
  `default_sqlite_url` (alongside `set_sdk_msb_path`)
- `crates/microsandbox/lib/db/mod.rs` — `init_global`, `global`,
  `resolve_db_settings` (reads `config::database_url` /
  `config::database_schema`), `MIGRATION_ADVISORY_LOCK_BASE` (now
  schema-derived per call), `connect_and_migrate`, `run_migrations`
  (CREATE SCHEMA on Postgres)
- `crates/cli/bin/main.rs` — `--database` and `--db-schema` global flags
- `crates/cli/lib/sandbox_cmd.rs` — `sandbox_db_url`, `sandbox_db_schema`
  (clap `env = ...`)
- `crates/microsandbox/lib/runtime/spawn.rs` — `cmd.env` for both
- `crates/runtime/lib/vm.rs` — `Config.sandbox_db_url`,
  `Config.sandbox_db_schema`, `connect_db`
- `crates/microsandbox/lib/snapshot/store.rs` — already rewritten
- `crates/migration/lib/...` — already commented and convention-noted
- `sdk/python/src/lib.rs` + `microsandbox/__init__.py` +
  `microsandbox/_microsandbox.pyi` — pyo3 bindings + re-export + types
- `sdk/node-ts/native/lib.rs` + `native/database.rs` (new) +
  `src/index.ts` — napi bindings + TS re-export
- `sdk/go/native/src/lib.rs` + `sdk/go/internal/ffi/ffi.go` +
  `sdk/go/database.go` (new) — cgo FFI + Go wrappers + cbindgen header

## Risks / call-outs

- Concurrent migrations on a shared Postgres are serialised by the
  per-schema advisory lock. Different schemas do not block each other.
- Postgres URLs carry passwords; passed to the supervisor via env not
  CLI; document not to put secrets in a world-readable `config.json`.
- Binary size grows by the Postgres driver + TLS/SCRAM code; accepted.
- Partial unique indexes treat NULLs as distinct on both backends —
  semantically identical, no behaviour change.

## Parked for later (not in this work)

- Nomad cluster (3 servers + 2 clients + autoscale): separate
  initiative. Verify nested-virt-capable instances first.
- `msbctl` / proper Nomad task-driver plugin: only after the crude
  `raw_exec` cluster proves the model.
- Other SQL backends (MySQL etc.): pattern documented, ~60–80 lines
  of routine wiring + backend-specific quirks (MySQL has no partial
  indexes, conflates schema and database).
