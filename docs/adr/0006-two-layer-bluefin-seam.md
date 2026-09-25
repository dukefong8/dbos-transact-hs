# Bluefin capabilities live only at the external seam (supersedes 0002)

Effectful Haskell DBOS APIs are built in two layers. The internal layer is plain Haskell with no Bluefin imports: pure domain logic, typedSql database functions, explicit `LogAction` threading, and `Either` error returns, tested with io-sim and live Postgres. The external layer is thin Bluefin 0.7 scoped value-level capabilities (`Ask` handles with rank-2 `with...` handlers) over those internals, giving callers lifetime-like escape prevention. Bluefin modules may depend on internal modules, never the reverse; deleting the outer layer loses scope safety but no behavior.

Superseded 2026-09-25 by ADR-0012: Bluefin is removed entirely and the engine threads `Connection`/`Ctx` explicitly, so there is no external capability layer left to scope. The plain-Haskell-internals half of this decision stands as the whole design.
