//! Migrate the shared development database with the Rust migration runner.
//!
//! This is the `make db-migrate` implementation: the schema this port tracks
//! is the Rust corpus (`crates/dbos/migrations`), so the database it queries
//! against is migrated by that same runner. Nothing is compared, planned, or
//! decided here — [`runner::run`] is idempotent and a migrated database is a
//! no-op with a reported outcome.

use dbos::sysdb::migrations::runner;
use dbos::sysdb::DEFAULT_SCHEMA;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("DBOS_DATABASE_URL"))
        .expect("DATABASE_URL or DBOS_DATABASE_URL must be set");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect to the system database");
    match runner::run(&pool, DEFAULT_SCHEMA, true).await {
        Ok(outcome) => println!("migrate ok: {:?}", outcome),
        Err(error) => {
            eprintln!("migrate failed: {}", error);
            std::process::exit(1);
        }
    }
}
