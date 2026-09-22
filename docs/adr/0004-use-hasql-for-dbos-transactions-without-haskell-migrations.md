# Use Hasql for DBOS transactions, without Haskell schema migrations (SUPERSEDED by 0007)

DBOS Haskell will implement all DBOS database transactions with `hasql`, `hasql-th`, and `postgresql-types`/`hasql-postgresql-types`. Haskell will not implement a DBOS schema migration feature: it must run against a schema migrated by Python DBOS and maintain 100% compatibility with that live schema, so Haskell tests should verify metadata compatibility rather than own schema evolution.
