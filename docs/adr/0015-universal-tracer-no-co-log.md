# Universal tracer over contra-tracer; co-log deleted (supersedes ADR-0013/0014)

`DBOS.Logger` (co-log `LogAction m Text`) is deleted. Its replacement is `DBOS.Tracer`'s `SomeTracer` GADT — one universal carrier over the Rank-N shape both backends already have:

```haskell
data SomeTracer m where
  SomeTracer :: (forall e. (LogEvent e, ToLogStr e, Typeable e) => Tracer m e) -> SomeTracer m
```

Exported function names carry no `Some` (they reuse the old spellings, so call sites read unchanged): `traceWith`/`nullTracer`/`simTracer` are universal (contra-tracer's narrow names are no longer re-exported; narrow use qualifies `Control.Tracer`), plus `ioTracer :: TimedFastLogger -> SomeTracer IO` and `projectTracer`. FastLogger is the IO backend (Rank-N over `ToLogStr`); io-sim's `traceM` is the sim backend (structured events recovered by type with `selectTraceEventsDynamic`). `co-log` and `co-log-core` leave the cabal file and the `make env` line.

Per-domain event ADTs (consolidated, not one-per-module): `EngineEvent` (instance/connection/client/registry), `WorkflowEvent` (step), `QueueEvent` (dequeue), `SysdbEvent` (retry/backend/notifier). Each has `LogEvent` (severity + line, span fields as `key=value`) + `ToLogStr`. The old `WorkflowLog` id-attribution adapter is gone: ids ride on event constructors (e.g. `DequeuedRowSkipped`).

Carrier map (struct audit vs the oracle — Rust holds no logger anywhere; tracing is ambient there, explicit here):
- Single SOURCE value per launch (`ioTracer <$> acquireFastBackend`); `Connection.connTracer` is the sole engine-half carrier (Rust's own "methods on the connection" doctrine); `Ctx` inherits at `newCtx` (`withTracer` overrides in tests).
- `DBOS.dbos_logger` and `Executor.tracer` deleted (the latter was write-only); readers use `executor.conn.connTracer`. `releaseTracer :: m ()` stays (lifecycle action, not data).
- `PostgresSystemDB.psdbLog` / `Notifier.log` (`SomeTracer IO`) stay: class/notifier methods receive only `db`/`notifier`, so no signature can carry it — same launch value via explicit constructor params.
- Client wires `nullTracer` (preserves its historical silence); `ClientConnected`/`ClientClosed` constructors are deferred, not emitted.
- Emission N/As: Rust `Drop`-warn (no finalizers; explicit shutdown is the discipline), already-launched no-op path (no backend exists there by Rule 5), `select.rs:256` (Select strategy undecided, item 2).

Two repo lessons: a concrete-typed collector cannot fill the carrier's Rank-N hole (MonoLocalBinds) — IO tests capture rendered `Text` via an explicitly-signed polymorphic emit, sim tests assert on types; and the test-suite sees only exposed modules, so tracer names travel through the `DBOS.Transact` facade. Recorded 2026-09-30.

## Addendum: FastLogger vs Rust tracing output (live comparison 2026-09-30)

A throwaway runner in `/tmp/trace-cmp` (path-dependency on the oracle crate + cached `tracing-subscriber`; the oracle checkout itself untouched, read-only rule kept) launched app `trace-cmp` at versions `9.9.9` then `1.0.0` against the shared test DB with `dbos=debug` fmt output (ANSI/timestamps stripped). Haskell side: the same scenario through `InstanceTest` launches. Normalized comparison (level + body + fields):

| Rust | Haskell | Verdict |
|---|---|---|
| `INFO DBOS launched` + 3 fields | `[Info] DBOS launched` + 3 fields | match |
| `INFO DBOS shut down` + app | `[Info] DBOS shut down` + app | match |
| `WARN` version-stale + `app_version`/`latest_version` | `[Warning]` same body + both fields | match (fields added after first diff) |
| `WARN no workflows are registered` | `[Warning]` same body | match |
| `DEBUG no workflows to recover` / `INFO re-enqueued … PENDING workflows=N` | `[Debug]`/`[Info]` same shapes | match |
| `INFO cancelled … still running … cancelled=N` | `[Info]` same, N live (`abortAll` returns the count) | match |
| `DEBUG the notifier stopped` | `[Debug]` same (pre-existing) | match |
| `DEBUG` notification-listener lines (3) | — | N/A by architecture: the port is polling-only (`Postgres.hs:3650`), no LISTEN loop |

Remaining delta is fmt-layer only: timestamp shape, Rust `"quoted"` vs Haskell bare field values, span/target decoration. Side note: the comparison registered versions `9.9.9`/`1.0.0` for throwaway app `trace-cmp` (no workflows, recovery scoped to its own executor — no shared rows touched).

## Addendum: event ADTs homed with their domains (2026-09-30)

The consolidated catalog above now lives per domain instead of in `DBOS.Tracer`: `EngineEvent` in `Transact.Recovery`, `SysdbEvent` in `SystemDB.Retry`, `WorkflowEvent` in `Transact.Step`, `QueueEvent` in `Transact.Dequeue`, `ManagementEvent` in `Transact.Management`. Placement follows the import graph, not the names: `Workflow.hs` and `Queue.hs` as owners would cycle (`Step`/`Instance` edges), so the emitter-adjacent leaf owns each type. `DBOS.Tracer` keeps the carrier, backends, and `LogEvent`/`LogSeverity`; `showText` moved to `DBOS.Prelude`. Facade export names are unchanged.
