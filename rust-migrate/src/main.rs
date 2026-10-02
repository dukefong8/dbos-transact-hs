//! Migrate the shared development database with the Rust migration runner.
//!
//! This is the `make db-migrate` implementation. The corpus under `migrations/`
//! is a verbatim copy of the Rust crate's (`dbos-transact-rust/crates/dbos`:
//! `migrations/*.sql`, `sysdb/migrations.rs`, `sysdb/migrations/runner.rs`), so
//! this crate still migrates a database with the same runner, numbering, and
//! placeholders the Rust corpus defines — but without a path dependency on
//! that checkout. Nothing is compared, planned, or decided here; `runner::run`
//! is idempotent and a migrated database is a no-op with a reported outcome.
//!
//! Refreshing the copy: re-copy the three pieces above and bump nothing — the
//! runner reads `SHARED_MIGRATIONS` from the copied `mod.rs`, which is what
//! `make db-migrate` reports against.

mod migrations;

use migrations::runner;
use migrations::DEFAULT_SCHEMA;

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
