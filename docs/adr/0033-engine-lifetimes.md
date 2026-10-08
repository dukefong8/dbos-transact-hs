# ADR-0033: Engine thread and connection ownership, lifetimes, and leak audit

Date: 2026-10-08. Ownership and lifetime analysis over `Executor`,
`WorkflowCtx`/`StepCtx`, `Connection`, the `SystemDB` seam,
`Postgres.Backend`, and the `Tasks` thread registry — with the leak
verdict and the decisions it forced.

## Context

Five fixes landed together (guard combinator, seam call form, three
resource-bracketing holes), and each turned on who owns what and what
outlives what. This ADR records the ownership graph, the lifetime
nesting, and the audit result so future changes inherit the invariants
instead of rediscovering them.

## Ownership

- `DBOS` owns the launched `Executor` (`dbos_executor :: StrictMVar m
  (Maybe (Executor m))`, at most one; `Instance.hs:91`), and the
  `dbos_lifecycle` lock serializing launch against shutdown
  (`Instance.hs:92`).
- `Executor` owns `Connection`, `Identity`, the workflow `Snapshot`,
  `Tasks`, and the `releaseTracer` action (`Instance.hs:103-111`). Its
  `datasources` field is *shared*, not owned — it aliases
  `dbos.dbos_datasources` (`Instance.hs:561`).
- `Connection` owns the `SomeSystemDB` existential plus settings
  (serializer, poll interval, instance id, tracer)
  (`Connection.hs:93-108`).
- `SomeSystemDB` wraps `PostgresSystemDB`, which owns `Pool.Pool`, the
  notifier, and the retry policy (`Backend.hs:991-1008`).
- `WorkflowCtx`/`StepCtx` and `WorkflowHandle` only *borrow* the
  connection (`Context.hs:361,375`). Contexts are per-execution values;
  handles may outlive the executor.

## Lifetime nesting

`withMVar lifecycle` → `acquireLoggerBackend` → `startExecutor` →
`acquireConnection` (`bracketOnError`, `Connection.hs:168`) →
`launchExecutor` (`prepare`, MVar install, supervisor spawn). Per call:
`withExecutor` (guard, not a bracket) → `withConnection`/`runSystemDB`
(unpack only) → `Pool.use` per session → `withRetry`. Shutdown: clear
MVar → `abortAll` → trace → `releaseTracer` → thaws →
`releaseConnection` → `releasePostgresSystemDB` (stop notifier, wait
flush, `Pool.release`). Shutdown kills tasks before closing the pool,
which is what keeps every borrow valid.

## Decisions

- **Production pools stay in manual acquire/release pairing.**
  `withPostgresSystemDB` (`Backend.hs:1079`) is the one true bracket and
  production never uses it — a bracket cannot span launch-to-shutdown.
  Release is guaranteed exactly where a bracket exists (verify failure,
  `Backend.hs:1048`; activate failure, `Connection.hs:168` and
  `Client.hs:180`; tests via `withResource`), and elsewhere release runs
  iff shutdown/`closeClient`/launch-cleanup runs. Known holes: unmasked
  async exceptions skipping shutdown, forgotten `closeClient`, double
  `closeClient` (Hasql.Pool release idempotency unverified), and
  `launchOn` connections (ownership transfers to the executor; no
  shutdown means no release).
- **Pool hold duration is minimal as-is.** The pool lives exactly
  executor/client lifetime; borrowing is already per-session via
  `Pool.use`, and idle connections are reaped by pool config
  (`acquisitionTimeout 10`, idleness/aging 1800, `Backend.hs:1036-1045`).
- **Caller context stays explicit.** `withConnection` maps the error
  channel only and never defaults the caller argument (`Connection.hs:75`):
  `Nothing` outside a workflow and the placed caller inside are different
  answers, and a default would conflate them.
- **`updateQueue`'s validate callback stays.** Rust's
  `update_queue` takes `validate` (`sysdb/mod.rs:1266`), so removing the
  parameter breaks oracle fidelity; the sole caller passes a no-op
  (`Queue.hs:351`).
- **Shutdown tears down release-last, not under `finally`.**
  `releaseConnection` runs after trace, tracer release, and both thaws, so
  a throwing close propagates with nothing skipped — and no new
  constraint is required, since the resources are independent.
- **Raw-channel consumers stay on `runSystemDB`.** `withConnection`
  promises the mapped engine channel, which does not fit consumers of
  the raw backend error: `driveGetEvent → adoptEventValue`
  (`Event.hs:173`), `initWorkflow → resolveEnqueueCollision`
  (`Client.hs:299`, whose callee takes raw `Error`, `Workflow.hs:545`),
  and all `try`-acquire/validation sites.

## Threads: daemon-only by design

`Tasks m` (`Workflow.hs:810`, `TaskState` at `:818`) is a supervisor
registry over io-classes `ThreadId m` — real GHC threads under `IO`,
simulated threads under `IOSim`; the `async` package is out, and the one
GHC-specific edge (matching `ThreadKilled` identity) is documented at
the import (`Workflow.hs:49`). Only two thread kinds are ever spawned,
both daemon-like and both via `spawnTracked`: the supervisor
(`Instance.hs:564`) and one poll worker per queue (`Dequeue.hs:491`,
self-unregistering `finally`). Workflow bodies, waits, and polls run on
the caller's thread. There is deliberately no fork/join shape — nothing
holds a joinable handle (`Workflow.hs:803-808`); the `live` count is a
join-substitute used solely by `abortAll`'s wait-for-quiet.

## Leak verdict

No thread leak in engine code: every spawn is masked from check through
registration (none escapes, none lands post-`closed`); every death
deregisters (normal exit, kill via try/`departed`/rethrow, and
fast-body-outruns-fork via the orphan entry); `abortAll` snapshots under
the same lock, kills all, and STM-waits for `live == 0`. Residual risks,
neither proven: hasql-pool's kill-safety while a connection is checked
out (pool exhaustion under shutdowns would be the symptom — worth one
probe), and indefinite uninterruptible work stalling `abortAll` (a hang,
not a leak; engine code holds no such section).
