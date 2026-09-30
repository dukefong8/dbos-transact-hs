# io-sim simulations for headless deterministic E2E mirrors

Goal: run the starter-acceptance flows (W1/W2/Q/E/M) without Postgres or
sockets — deterministic, instantaneous, reproducible — as Tasty tests, with
`Tasty`-golden snapshots of representative traces.

## 1. What io-sim is

A pure, discrete-event simulator for concurrent Haskell IO, from Well-Typed
with IOG/Quviq engineers ([announcement][iog-announce], [blog][wt-blog],
[repo][io-sim-repo], [hackage][io-sim-hackage]). It simulates GHC's runtime
— threads, STM, async exceptions, timeouts, delays — deterministically:
between time-dependent operations the simulation runs with "infinite CPU
speed"; only `threadDelay`/timeouts advance the virtual clock. The same code
runs in production (`IO`) and in tests (`IOSim s`) by generalising over the
`io-classes` interface; `IOSim` also supports the `base`/`exceptions`
hierarchies where they coincide.

Key properties for us:

- **Deterministic**: same seed program → same trace, bit-for-bit. Golden-testable.
- **Instantaneous**: years of simulated sleeps compress to milliseconds.
- **Observability**: full `SimTrace` — thread forks, STM commits/aborts/blocks,
  delays fired, `say` strings, `traceM` dynamics, timeouts.
- **Race exploration**: `IOSimPOR` replays alternative schedules
  (`exploreSimTrace`, schedule bound/branching/replay options) and shrinks
  well with QuickCheck.

## 2. API surface (hackage io-sim 1.9.1.0; 1.11 current)

Running:

- `runSim :: (forall s. IOSim s a) -> Either Failure a` — pure result.
- `runSimOrThrow` — for quick tests.
- `runSimStrictShutdown` — fails if non-main threads are still alive/blocked
  at main return. This is the shutdown-hygiene assertion our executor wants.
- `runSimTrace :: (forall s. IOSim s a) -> SimTrace a` — the general entry:
  result + full event trace; `traceResult`, `traceEvents` project from it.

Failure modes are typed: `FailureException`, `FailureDeadlock` (with labelled
thread ids — a deadlock *assertion*, not a hang), `FailureSloppyShutdown`,
`FailureEvaluation`, `FailureInternal`.

Tracing:

- `say :: String -> m ()` (`MonadSay`) — string events; `selectTraceEventsSay`,
  `selectTraceEventsSayWithTime :: SimTrace a -> [(Time, String)]`.
- `traceM :: Typeable a => a -> m ()` / `traceSTM` — typed dynamics;
  `selectTraceEventsDynamic[WithTime]` recovers them. Our leveled helpers
  already have the right shape (severity-tagged `Text` lines); emitting them
  through `say` in sim and through stdout in production is exactly
  the blog's `contra-tracer` pattern (typed, filterable, assertable).
- Pretty printers: `ppTrace`, `ppEvents`, `ppSimEvent`; 1.11 adds `ppSayTrace`.

Time: `newtype Time = Time DiffTime`; `setCurrentTime`, `unshareClock` for
multi-clock scenarios (not needed here).

QuickCheck bridge: `monadicIOSim`, `monadicIOSim_`, `runIOSimGen`, plus
`monadicIOSimPOR[_]` / `runIOSimPORGen` for race exploration. POR options:
schedule bound (default 100), branching (default 3), per-step time limit,
`withReplay` of a printed schedule control (reproduces a found race with
extra diagnostics).

Version notes:

- GHC 9.12 is in `tested-with` for 1.9.x–1.11.x (repo pins GHC 9.12 /
  base-4.21 — compatible). Recommend `io-sim ^>=1.11`, `io-classes ^>=1.11`.
  Both are already (unused) deps in `dbos-transact-hs.cabal`, and 1.11.0.0
  sits in the local hackage cache.
- 1.10 evaluated `say`/`traceM` args strictly (NF/WHNF) and threw inside the
  sim on failure; 1.11 reverted to lazy. Don't assert on evaluation-error
  events across versions.
- `IOSim` has no `MVar` by default (simulate with STM); it *does* have full
  `MonadAsync` (`async`, `cancel`, `waitCatch`, `withAsync`, STM variants).

## 3. Real-world patterns (primary sources)

**Elevator example** ([blog][wt-blog], [code][elevator-code]): the recipe —
generalise signatures over `MonadSTM/MonadAsync/MonadDelay/MonadSay`, keep
business logic identical, run the same function in `IO` (seconds) and
`IOSim` (instant), extract timestamped events, assert a timeliness property
(`violatesFourSecondRule`), then QuickCheck over inputs with shrinking to a
minimal schedule counterexample.

**ouroboros-network diffusion tests** (the production-scale proof —
`Test.Cardano.Network.Diffusion`): the pattern to copy is
`diffusionSimulationM` — the whole simulation is generic over `m` with the
tracer *injected as a parameter* (`Tracer m …`), so the identical topology
runs against `IOSim` in tests and `IO` in production. Tests do
`runSimTrace sim`, extract typed events (`traceTVar`, per-governor traces),
and assert transition order/coverage; `exploreSimTrace id sim` for race
coverage with replayable schedule controls. Recent commits show them
standardising on `dynamicTracer` in sim.

Lesson for us: parameterise the *tracer*, not just the monad. Our old
`LogAction m Text` seam pointed this way — since ADR-0015 it is the
`SomeTracer m` GADT, `traceM`-backed under sim.

## 4. What this means for dbos-transact-hs

### What simulates well today

`Supervisor` (poll rounds), `Executor` (spawn/track/shutdown), `Step`
replay, `Workflow` claim logic, `getEventBlocking`/`recvMessage` wait loops:
all concurrency + time, no intrinsic Postgres. These are precisely the
pieces our live-DB suite tests with real sleeps (300ms–2s per test) and
`tasty` parallelism flakes (cf. the fixture deadlock seen 2026-09-22).

### The two seams to build

1. **Monad generalisation (engine).** `Step`/`Workflow`/`Executor`/
   `Supervisor` currently take concrete `IO` (`threadDelay`, `getCurrentTime`,
   `Control.Concurrent.Async`, `STM` concrete `TVar`). Generalise to
   `(MonadAsync m, MonadDelay m, MonadSTM m, MonadCatch m, …)`, wall-clock via
   `MonadTime`. The `Transact` facade's Bluefin `Ask` stores already abstract
   *fetching*; they need the same treatment (or a record-of-functions DB
   handle — see below). Pure modules (`Codec`, parse/replay, `Types`) are
   untouched.
2. **A `SimDB` model (storage).** io-sim cannot run Hasql/Postgres. The sim
   needs an in-memory model of `workflow_status` / `operation_outputs` /
   `notifications` / `queues` implementing the *session contract*: PK
   conflicts (`DO NOTHING`/`DO UPDATE`), `SKIP LOCKED`-style claim
   single-winner semantics, `consumed` flags, `coalesce` version stamping.
   Back it with `TVar`s of `Map`s; give DB operations a simulated latency
   (`threadDelay` microseconds) so interleavings are interesting. This is the
   real work — and its boundary must be explicit: **the sim verifies engine
   logic against the model, never SQL semantics**. `SystemDBHasqlTest`,
   the migration-ceiling test, and cross-process recovery stay live-DB.

   Containers for the model (evaluated 2026-09-23):
   - `stm-containers`: **no**. Every operation is concrete-`STM`
     (`lookup :: key -> Map k v -> STM (Maybe v)`), so it cannot run under
     `IOSim s` (whose STM is a different type); its HAMT performance story
     is irrelevant to a sim. Plain `TVar (Map …)` under `MonadSTM m` is the
     compatible shape.
   - `strict-stm` (`io-classes:strict-stm` sublibrary): **yes, when the seam
     is built**. `StrictTVar m a` etc. are polymorphic over `MonadSTM m`
     (IO and IOSim alike), kill the lazy-thunk class of sim-vs-IO
     divergences, and `labelTVar`/`traceTVar` feed committed TVar changes
     straight into the trace for golden assertions.
   - `ixset-typed`: **optional**. Pure, works anywhere, GHC 9.12-tested,
     Well-Typed-maintained; gives type-safe multi-index queries
     (`getEQ`/`getRange`) for the model's secondary lookups
     (queue+status, name, destination+topic). Costs a new dep plus
     `Indexable` instances per row type — start with hand-rolled
     `Map`s per index (four tables, a few hundred lines) and graduate only
     if the index count grows.
3. **Deterministic identity/time.** `UUID.V4.nextRandom` and `getCurrentTime`
   must go through the sim: counter-based ids and `MonadTime` in sim,
   real ones in production. Our tests already mint fresh UUIDs per test —
   the sim equivalent is a per-run counter in the model.

### E2E-gate scenarios as sims (mapping)

- **W1 full run**: sim workflow, 2s steps complete in zero wall time;
  assert outcome + step rows + `steps_event` in the model.
- **W2 crash-resume**: `throwTo` the runner thread mid-sleep (async
  exception in sim, deterministic), re-run body, assert each step ran once —
  the exact scenario of `StarterTest`'s crash test, minus wall clock.
- **Q fan-out**: N workflows, limit 3, assert claim counts per sim-tick;
  `IOSimPOR` over the claim éclatement for the take-race class.
- **E blocking read**: publisher + reader threads; assert the reader wakes
  at the publish tick with `waited == publish - start` from trace times.
- **M approvals**: parked `recv` + `sendMessages`; assert take-once and
  bulk atomicity. `runSimStrictShutdown` asserts no stranded tasks.

### Golden tests

Render the extracted typed events (NOT the raw `SimTrace` — thread ids and
internal events are brittle) to canonical text and snapshot with `tasty-golden` (already a dependency).
One golden per scenario above, plus
`ppTrace` dumps on failure for diagnosis. POR-discovered schedules get
pinned via `withReplay` controls checked into the test.

### Explicit non-goals / caveats

- The model is not Postgres: `READ COMMITTED` re-evaluation, deadlock
  errors, advisory locking, and wire encoding are out of scope. Any SQL
  change still needs the live suite + gate.
- `say`/`traceM` strictness changed across 1.10/1.11 — pin `^>=1.11` and
  don't golden-test evaluation-error events.
- No `MVar`s in sim code paths; our engine already uses `TVar`/`STM`
  throughout — no change needed.
- `setCurrentTime`/`unshareClock` are available if a test needs clock skew;
  default single clock suffices for the gate mirrors.

## Sources

- [Well-Typed blog: Verifying and testing timeliness constraints with io-sim (2025-10)][wt-blog] + [example code][elevator-code]
- [IOG announcement: io-sim (2023-04)][iog-announce]
- [IntersectMBO/io-sim repo][io-sim-repo] (features, `IOSimPOR` how-to) and [hackage Control.Monad.IOSim][io-sim-hackage] (API pinned above)
- [ouroboros-network diffusion sim tests][ouroboros-diffusion] (`diffusionSimulationM` generic-monad + injected-tracer pattern; `exploreSimTrace` + replay)
- Local: `dbos-transact-hs.cabal` (unused `io-sim`/`io-classes` deps), `docs/starter-e2e-gate.md` (W1/W2/Q/E/M contracts to mirror), `docs/bluefin-research-context.md` §18 (Bluefin seam the sim must respect: plain-Haskell internals, capabilities only at the edge)

[wt-blog]: https://well-typed.com/blog/2025/10/an-introduction-to-io-sim/
[elevator-code]: https://github.com/well-typed/verifying-and-testing-with-iosim
[iog-announce]: https://engineering.iog.io/2023-04-14-io-sim-annoucement/
[io-sim-repo]: https://github.com/IntersectMBO/io-sim
[io-sim-hackage]: https://hackage.haskell.org/package/io-sim/docs/Control-Monad-IOSim.html
[ouroboros-diffusion]: https://github.com/IntersectMBO/ouroboros-network/tree/master/cardano-diffusion/tests
