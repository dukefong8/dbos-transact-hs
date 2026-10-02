# Use ihp-typed-sql with the Rust migration runner, without Haskell schema migrations (supersedes 0004)

DBOS Haskell implements all DBOS database transactions with `hasql` and the `ihp-typed-sql` `[typedSql| ... |]` quasiquoter (the Haskell analogue of sqlx `query!` macros), with explicit column lists so schema drift surfaces at compile time. The `hasql-th` dependency is removed. Haskell still implements no DBOS schema migration feature: the development, test, and compile-time databases are always migrated with the Rust migration runner (`crates/dbos/migrations`, ranges 1–47 + 100–108), and Haskell tests verify metadata compatibility rather than owning schema evolution.

The shared-schema contract this depends on — the wire format, the minimum common table/column set, and the Python/TypeScript 109–123 divergence from this port's 108 ceiling — is recorded in `docs/cross-language-schema-interop.md`.
