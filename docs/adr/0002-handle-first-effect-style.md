# Use Bluefin scoped capabilities for DBOS system services

Effectful Haskell DBOS APIs use Bluefin 0.7 scoped value-level capabilities, not DBOS-specific `Handle` records or `Handle -> ... -> IO result` functions.

Functions that need external DBOS facts accept explicit Bluefin capabilities and run in `Eff es`. Capability handlers use `with...` names and introduce scoped evidence, for example `withWorkflowExecutionStore` and `withOperationCheckpointStore`. IO is performed through `IOE`, so the type surface shows both the DBOS capability and IO requirement.

Pure domain modeling and row parsing remain separate from effects. Python-migrated Postgres rows are parsed into ADTs at the boundary; effectful functions only retrieve rows/status/checkpoints through scoped capabilities.
