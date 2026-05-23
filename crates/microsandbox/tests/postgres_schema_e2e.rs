//! End-to-end: boot multiple sandboxes against PostgreSQL with a
//! non-default schema, then verify the rows landed in that schema.
//!
//! Requires KVM (or libkrun on macOS) **and** a reachable PostgreSQL
//! server. The test is `#[ignore]` so plain `cargo test --workspace`
//! skips it. Run via:
//!
//! ```sh
//! docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
//! MSB_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres \
//!     cargo nextest run -p microsandbox --test postgres_schema_e2e --run-ignored=only
//! ```
//!
//! Each run uses a unique schema name (nanosecond-timestamped) so
//! repeated invocations against the same server don't collide. The test
//! cleans up its sandboxes and drops the schema at the end.

use std::time::Duration;

use microsandbox::Sandbox;
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
use test_utils::msb_test;

const SANDBOX_NAMES: &[&str] = &[
    "msb-pg-schema-e2e-1",
    "msb-pg-schema-e2e-2",
    "msb-pg-schema-e2e-3",
];

#[msb_test(flavor = "multi_thread", worker_threads = 4)]
async fn boots_sandboxes_against_postgres_with_custom_schema() {
    let Ok(url) = std::env::var("MSB_TEST_DATABASE_URL") else {
        eprintln!("skipping: MSB_TEST_DATABASE_URL is not set");
        return;
    };

    // Per-run schema so concurrent or repeated runs don't collide.
    let schema = format!(
        "msb_e2e_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    // Configure microsandbox before any Sandbox/DB call. These are
    // process-global OnceLocks; this test file is its own integration
    // binary so it owns its process.
    microsandbox::config::set_database_url(url.clone());
    microsandbox::config::set_database_schema(schema.clone());

    // Boot N sandboxes. `.create()` brings them up detached. We hold the
    // returned `Sandbox` values so we can stop them deterministically at
    // teardown.
    let mut sandboxes: Vec<Sandbox> = Vec::new();
    for name in SANDBOX_NAMES {
        let sandbox = Sandbox::builder(*name)
            .image("mirror.gcr.io/library/alpine")
            .cpus(1)
            .memory(256)
            .replace()
            .create()
            .await
            .expect("create sandbox");
        sandboxes.push(sandbox);
    }

    // Verify rows landed in the custom schema. Connect with a separate
    // unconfigured connection so the assertions don't depend on the
    // process's pinned search_path.
    let verifier = Database::connect(&url).await.expect("verifier connect");

    let sandbox_count = scalar_count(
        &verifier,
        &format!(
            r#"SELECT COUNT(*) FROM "{schema}".sandbox WHERE name IN ({})"#,
            SANDBOX_NAMES
                .iter()
                .map(|n| format!("'{n}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
    .await;
    assert_eq!(
        sandbox_count,
        SANDBOX_NAMES.len() as i64,
        "expected {} sandbox rows in schema `{schema}`, got {sandbox_count}",
        SANDBOX_NAMES.len(),
    );

    // The in-VM supervisor inserts run rows. At least one per sandbox.
    let run_count = scalar_count(
        &verifier,
        &format!(r#"SELECT COUNT(*) FROM "{schema}".run"#),
    )
    .await;
    assert!(
        run_count >= SANDBOX_NAMES.len() as i64,
        "expected >= {} run rows in schema `{schema}`, got {run_count}",
        SANDBOX_NAMES.len(),
    );

    // Tear down sandboxes. Best-effort; cleanup is what matters.
    for sandbox in sandboxes {
        let _ = tokio::time::timeout(Duration::from_secs(30), sandbox.stop_and_wait()).await;
    }
    for name in SANDBOX_NAMES {
        if let Ok(mut h) = Sandbox::get(name).await {
            let _ = h.kill().await;
        }
        let _ = Sandbox::remove(name).await;
    }

    // Drop the schema so a re-run against the same server is clean.
    verifier
        .execute_unprepared(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
        .await
        .expect("drop schema");
}

async fn scalar_count(conn: &sea_orm::DatabaseConnection, sql: &str) -> i64 {
    conn.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        sql.to_string(),
    ))
    .await
    .expect("query")
    .expect("row")
    .try_get_by_index::<i64>(0)
    .expect("i64")
}
