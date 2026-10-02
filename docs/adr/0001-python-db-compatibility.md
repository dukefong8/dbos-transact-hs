# Use Python DBOS Postgres schema as the Haskell compatibility boundary

The Haskell implementation will initially target exact compatibility with the live Python DBOS Postgres system schema. This means Haskell domain types may be richer than the database representation, but table names, column names, status strings, primary keys, foreign-key constraints, and migration-created indexes are treated as externally visible compatibility constraints so Haskell can interoperate with Python-created workflow data. Logical workflow links such as parent, fork, and child workflow references may use typed Haskell wrappers, but the database representation remains unconstrained text where Python uses unconstrained text.

Amended by 0007: the DDL source for development, test, and compile-time databases is the Rust migration runner (the shared cross-SDK schema corpus this port tracks). Python remains the row-format interoperability contract: status strings, serialization tags, and sentinel values must stay readable in both directions, verified by the hs-* fixture rows.

See `docs/cross-language-schema-interop.md` for the concrete wire format, the minimum common table/column set every corpus must create, and the Python/TypeScript 109–123 gap at this port's 108 ceiling.
