# Universal tracer over contra-tracer; co-log retired (supersedes ADR-0013/0014)

Status: current. This is the decision that retired co-log — it is out of the
cabal file, out of `make env`, and out of every module's imports; nothing is
left to migrate.

`DBOS.Logger` (co-log `LogAction m Text`) is deleted. Its replacement is `DBOS.Transact.Logger`'s `SomeTracer` GADT — one universal carrier over the Rank-N shape both backends already have:

```haskell
data SomeTracer m where
  SomeTracer :: (forall e. (LogEvent e, ToLogStr e, Typeable e) => Tracer m e) -> SomeTracer m
```

Exported function names carry no `Some` (they reuse the old spellings, so call sites read unchanged): `traceWith`/`nullTracer`/`simTracer` are universal (contra-tracer's narrow names are no longer re-exported; narrow use qualifies `Control.Tracer`), plus `ioTracer :: TimedFastLogger -> SomeTracer IO` and `projectTracer`. FastLogger is the IO backend (Rank-N over `ToLogStr`); io-sim's `traceM` is the sim backend (structured events recovered by type with `selectTraceEventsDynamic`). `co-log` and `co-log-core` leave the cabal file and the `make env` line.

Per-domain event ADTs (consolidated, not one-per-module): `EngineEvent` (instance/connection/client/registry), `WorkflowEvent` (step), `QueueEvent` (dequeue), `SysdbEvent` (retry/backend/notifier). Each has `LogEvent` (severity + line, span fields as `key=value`) + `ToLogStr`. The old `WorkflowLog` id-attribution adapter is gone: ids ride on event constructors (e.g. `DequeuedRowSkipped`).

Carrier map (struct audit vs the oracle — Rust holds no logger anywhere; tracing is ambient there, explicit here):
- Single SOURCE value per launch (`ioTracer <$> acquireLoggerBackend`); `Connection.connTracer` is the sole engine-half carrier (Rust's own "methods on the connection" doctrine); `Ctx` inherits at `newCtx` (`withTracer` overrides in tests).
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

The consolidated catalog above now lives per domain instead of in `DBOS.Transact.Logger`: `EngineEvent` in `Transact.Recovery`, `SysdbEvent` in `SystemDB.Retry`, `WorkflowEvent` in `Transact.Step`, `QueueEvent` in `Transact.Dequeue`, `ManagementEvent` in `Transact.Management`. Placement follows the import graph, not the names: `Workflow.hs` and `Queue.hs` as owners would cycle (`Step`/`Instance` edges), so the emitter-adjacent leaf owns each type. `DBOS.Transact.Logger` keeps the carrier, backends, and `LogEvent`/`LogSeverity`; `showText` moved to `DBOS.Prelude`. Facade export names are unchanged.

## Addendum: line shape — constructor name and emitting thread (2026-10-01)

A rendered line now names its event: `renderLine` (a `LogEvent` method with a default) is `showSeverity <> " " <> eventName <> ": " <> renderEvent` — `[Debug] StepRunning: running step double (3)` — and every `ToLogStr` instance collapsed to `toLogStr = toLogStr . renderLine`, so the line is shaped in `DBOS.Transact.Logger` alone. `eventName` defaults to the leading word of `show`; every event ADT already derives `Show`, so a new constructor needs no clause and no new deriving. The constructor stands in for Rust's `target` in the fmt-layer delta above; level + body + fields still line up.

The FastLogger line carries the emitting thread: `2026-10-01T17:32:15-0400 ThreadId 2557 [Info] EngineShutdown: DBOS shut down app_name=…`. There is nothing in fast-logger to reuse: `TimedFastLogger`'s callback receives only the cached `FormattedTime` (`fast-logger-3.2.7` `System/Log/FastLogger.hs:76`), the package's one `myThreadId` picks a per-capability buffer rather than naming a thread (`System/Log/FastLogger/MultiLogger.hs:66`), and there is no `ToLogStr ThreadId` instance. So `fastLoggerTracer`'s emit reads the id itself through the Prelude's io-classes `myThreadId` — the customization the fast-logger haddock itself recommends — in the emitting thread, which for the engine is the forked workflow/worker/notifier thread an operator is actually debugging. The sim carrier says the same line, with neither time nor thread: both are backend decorations.

## Addendum: the thread id is cached per thread, like the time (2026-10-01)

Formatting the id per line (`show` + `Text.pack`, then the `LogStr`) measured at ~59 ns/line in this port's line shape (~63 ns under four concurrent emitters, min of 3×300k, GHC 9.12 `-O2`, aarch64), which is the single largest per-line cost now that the prose rides a `LogStr` builder. So the id joins the timestamp as pre-formatted state: `ThreadIdCache` is a `Map (ThreadId IO) LogStr` behind a strict `TVar`, keyed exactly by the thread, and the IO backend reads it back per line.

The `FormattedTime` pattern does not transfer literally. Time is global, so one background updater can keep one shared slot fresh; a thread's id is known only to the thread itself, so **the emitting thread is the updater** — its first line formats the id, every later line reads it back. A hit is ~9 ns at the full 256-entry table (~11 ns under four emitters) against ~59 ns per-line formatting; the memo is exact, so unlike the one-second time cache nothing can be stale.

The table is a `HashMap` behind a strict `TVar`, read with `readTVarIO` and a pure lookup — the hit path never enters STM. `hashable`'s `ThreadId` instance hashes the thread's numeric id through a primop (`fromThreadId`/`getThreadId`), so keying costs no rendering, and `Eq` still compares the thread, so a collision costs only a re-format. Measured hit paths at 256 entries (min of 3×300k): `HashMap` 9.0 ns single / 10.7 ns at four threads, `Data.Map` 14.8 / 17.6, and an STM container's floor — one `atomically (readTVar …)` before any container work — 25.0 / 28.2. That floor is why **stm-containers** was considered and dropped: its `lookup` is itself `STM (Maybe v)`, so it starts ~10 ns behind the current path and adds a HAMT walk and read-set bookkeeping on top; it targets contended read-modify-write transactions, and this cache writes once per thread.

Rejected alternatives, measured: the RTS **thread label** as the store (leak-free and lock-free, but `threadLabel` + `Text.pack` per line measured 144–265 ns — worse than formatting, and it would own the label namespace); a **weak-keyed table** (base gives no cheap per-line key: `myThreadId` allocates a fresh box each call, so `StableName` hashes are per-box, and a strong-keyed map retains the TSO, defeating the weak finalizer); **generics** for the constructor name (the `Show` prefix is free).

The table is capped at 256 and **resets** when full (amortized one reset per 256 inserts; the threads it held re-format once) rather than evicting, because liveness needs weak refs and an eviction order would be arbitrary. The cap is also the memory bound: an entry keeps its `ThreadId` reachable and a `ThreadId` points at the thread's TSO — 20,000 dead ids held measured ~21 MB (~1 KB each) — so the cache holds at most ~256 KB of finished threads, where an unbounded table would leak ~1 KB per workflow ever run. `LoggerBackend` bundles the logger and its cache so the tracer constructors stay pure (the Rank-N shape) and the cache takes the launch's lifetime; a 300-thread test emits past the cap and asserts every line names its own emitter, since cross-wiring ids is the failure the reset could cause.

## Addendum: the null path renders nothing (2026-10-01)

Clients and several launch paths wire `nullTracer`, so the silence is load-bearing: nothing may be formatted for an event nobody will read. Two facts make that true. Contra-tracer's null tracer is a *squelching* arrow — `nullTracer = Tracer Arrow.squelch` (`contra-tracer-0.2.1.1` `Control/Tracer.hs:213`) and `runTracerA (Squelching _) = arr (const ())` (`Control/Tracer/Arrow.hs:43`) discards the payload before the event is touched, so the event is not even forced to WHNF. And nothing in `DBOS.Transact.Logger` renders on the way in: every `runTracer` call site passes an event constructor, and `renderEvent`/`renderLine`/`toLogStr` are reachable only through `ToLogStr` (the FastLogger backend) or the sim carrier's `say`.

Measured per event (300k, min of 3, identical construction on both arms): `nullTracer` costs 5.2 ns for a 2-field event and 4.4 ns for an 8-field one — independent of the event's size, i.e. nothing is rendered — against 104 ns for the same 8-field event through a rendering tracer (before the `LogStr` and buffer work FastLogger adds). `LoggerTest` locks it in with bottoms: a bottom event and a bottom event field both pass through `runTracer nullTracer`, and a control tracer that touches the event throws on those same values, so the passing test means the null path never looked.
