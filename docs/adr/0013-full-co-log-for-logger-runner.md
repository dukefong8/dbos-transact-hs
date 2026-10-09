# Full co-log for the LoggerT runner (RETIRED — superseded by 0015; amends the Rule 5 logging decision)

Status: retired 2026-09-30 by ADR-0015. `co-log` and `co-log-core` are no
longer cabal dependencies, no module imports either, and the `LoggerT` runner
below was never the shape that shipped — `SomeTracer` over contra-tracer with
FastLogger as the IO backend is. Kept as the record of why the `LoggerT`
shape was tried; nothing here describes current code.

`DBOS.Transact.Logger` now depends on full `co-log` (added to the cabal commons `build-depends` beside `co-log-core`), not just `co-log-core`. The reason is the `LoggerT` runner the engine adopts from the Playground shape: `runLogger :: LoggerT Colog.Message IO a -> IO a` runs a `Colog.Message` action against the bracketed `ioLogAction` stdout backend (`usingLoggerT (cmap fmtLogStr logger)`), with `fmtLogStr = (<> "\n") . Colog.fmtMessage`. `Colog.Message`, `Colog.Monad`, and `Colog.fmtMessage` exist only in full `co-log`; `co-log-core` has the actions and severities but neither the message type nor the transformer.

What this supersedes: the Rule 5 decision recorded in the port plan ("`co-log` message formatting goes") kept full `co-log` out and hand-rolled rendering instead — first a `DbosSeverity` parallel ADT with mapping functions, then a `WorkflowLogMsg` record with a `LogMsg` constructor class. Both are deleted: there is no message data type in the Logger module anymore. Severity is which helper you call (`logDebug`/`logInfo`/`logWarn`/`logError`, each stamping its tag onto `Text`); `Colog.Message` covers the one case that needs a real message value (the `LoggerT` runner). The engine seam is `LogAction m Text` throughout (`dbos_logger`, every dequeue/supervisor signature, `simInstance` over `simLogAction`).

What survives of the old rule: no ambient logger (Playground's global `installed` `IORef` stays out — `runLogger` brackets `ioLogAction` per run, nothing is installed); `co-log-core` actions remain the only other logging piece; `Text` is the rendering vocabulary. Recorded 2026-09-28.

Amendment 2026-09-28: `fast-logger` is out entirely — removed from the cabal commons `build-depends`, leaving `co-log-core` + `co-log`. The IO backend is plain `putStrLn` over `Text` (`ioLogAction`), timestamps come from the clock (`Data.Time.getZonedTime`, local ISO-8601, formerly fast-logger's one-second cache), and workflow-attributed lines are a `contramap` adapter (`WorkflowLog` in `DBOS.Transact.Workflow`) rather than a backend concern. Rationale: with no message data type left to build, fast-logger's builder vocabulary (`LogStr`/`ToLogStr`) had exactly one remaining use — the timed sink — and `Data.Time` covers it with one fewer dependency.
