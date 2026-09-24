# io-classes constraints for impure functions in the retry port

Question: the Rust retry module (`sysdb/retry.rs`) sleeps, logs, retries, and
draws entropy from a UUID; which of those effects should be constrained by
`io-classes` classes in the Haskell port, which by records-of-functions, and
which by plain `IO` with the effect injected as an argument — under this
repo's two-layer rule? Sources read directly (installed packages, repo
files, Rust oracle). Every claim cites `file:line`; probes are in
[Evidence](#evidence). No source files were changed.

Installed versions: `io-classes-1.11.0.0`, `io-sim-1.11.0.0` (both already
direct deps, `dbos-transact-hs.cabal:30-31`), GHC 9.12.4. Today they are used
only in tests: `test/DBOS/SimTest.hs:14-18` and `test/DBOS/SimDB.hs:16`;
`src/` has no `io-classes` import.

## Sources

- Oracle: `~/dev/dbos-transact-rust/crates/dbos/src/sysdb/retry.rs`,
  `sysdb/error.rs`, `sysdb/postgres.rs`
- Installed packages: `io-classes-1.11.0.0` and `io-sim-1.11.0.0` Hackage
  tarballs (extracted under `/var/folders/.../opencode/ioclasses/src/`) and
  live `ghci` probes through `cabal exec`
- Repo rules: `AGENTS.md`, `docs/adr/0006-two-layer-bluefin-seam.md`,
  `.lavish/rust-port-plan.html` §6 (lines 231–243)
- Repo prior art: `src/DBOS/Transact/Log.hs`, `Store.hs`, `Step.hs`,
  `Supervisor.hs`, `Workflow.hs`, `src/DBOS/SystemDB/Postgres.hs`,
  `test/DBOS/SimTest.hs`, `test/DBOS/SimDB.hs`
- Prior intent: `docs/io-sim-simulations.md` (read fully)

## 1. What the Rust retry module actually does

| Effect | Where | Detail |
|---|---|---|
| Retry loop / attempt counter | `retry.rs:102-122` | Plain recursion, no attempt limit (`retry.rs:83-85`); counter is pure state (`retry.rs:101,106`) |
| Classification (retry or not) | `retry.rs:69-79` (`should_retry`) | Pure match on `Error::Backend(e).kind` (`error.rs:304-317`: `Connection`/`Transient`/`Permanent`); no effect |
| Sleep | `retry.rs:118` | `tokio::time::sleep(delay).await` — relative delay, no wall-clock read anywhere in the file |
| Logging | `retry.rs:111-117` | `tracing::warn!(operation, attempt, delay_ms, error = %error, "system database operation failed; retrying")` |
| Entropy (jitter) | `retry.rs:133-138` | `uuid::Uuid::new_v4().as_u128() as u32` (`:134`), factor `0.5 + bits / (u32::MAX).next_up()` (`:136`), spread `[0.5, 1.5)` (`:125-132`) |
| Backoff growth | `retry.rs:100,119` | Pure arithmetic: start 1s, double, cap 60s (`retry.rs:57-65`) |
| Cancellation | `retry.rs:83-85` | None in-module: dropping the future stops the loop at the next await point |
| Concurrency | — | None: one sequential future; no spawn, no timeout wrapper |
| Identity rule | `retry.rs:25-30` | Nothing identifying *this attempt* may be minted inside the retried region — values differ between runs |

## 2. What io-classes 1.11.0.0 provides (exact methods and instances)

All classes below have an `IO` instance in `io-classes` and an `IOSim s`
instance in `io-sim-1.11.0.0:Control.Monad.IOSim.Types` (provenance strings
from the probes in [Evidence](#evidence)). Shorthand below: `Types.hs` =
`io-sim-1.11.0.0/src/Control/Monad/IOSim/Types.hs`; `MonadTimer.hs` etc. =
`io-classes-1.11.0.0/io-classes/Control/Monad/Class/<Module>.hs`; `SI.hs` =
`io-classes-1.11.0.0/si-timers/src/Control/Monad/Class/MonadTimer/SI.hs`.

| Class | Methods | IO instance | IOSim instance |
|---|---|---|---|
| `MonadDelay` (`MonadTimer.hs:23,29`) | `threadDelay :: Int -> m ()` | `MonadTimer.hs:44-45` | `Types.hs:722-726` (advances the virtual clock) |
| `MonadTimer` (`MonadTimer.hs:32,35,38`) | `registerDelay :: Int -> m (TVar m Bool)`, `timeout :: Int -> m a -> m (Maybe a)` | `MonadTimer.hs:48-51` | `Types.hs:754-764` |
| `MonadTime` (`MonadTime.hs:32,35`) | `getCurrentTime :: m UTCTime` | `MonadTime.hs:44-45` | `Types.hs:706-707` |
| `MonadMonotonicTimeNSec` (`MonadTime.hs:23,30`) | `getMonotonicTimeNSec :: m Word64` | `MonadTime.hs:41-42` | `Types.hs:696-701` |
| `MonadST` (`MonadST.hs:36-38`) | `stToIO :: ST (PrimState m) a -> m a` | `MonadST.hs:40-41` | `Types.hs:685-686` (`stToIO = liftST`) |
| `MonadUnique` (`MonadUnique.hs:21-24`) | `newUnique`, `hashUnique` | `MonadUnique.hs:37-40` (`Data.Unique` counter) | `Types.hs:635-637` (sim counter) |
| `MonadSay` (`MonadSay.hs:6-7`) | `say :: String -> m ()` | `MonadSay.hs:9-10` | `Types.hs:336-337` |
| `MonadThrow` / `MonadCatch` / `MonadMask` (`MonadThrow.hs:40,77,188`) | `throwIO`; `catch`/`try`/`bracket`; `mask`/`uninterruptibleMask` | `MonadThrow.hs:220,232,246` | `Types.hs:339,403,410` |
| `MonadFork` (`MonadFork.hs:41`) | `forkIO`, `throwTo`, `killThread`, `yield` | `MonadFork.hs:67-70` | `Types.hs:473-484` |
| `MonadAsync` (`MonadAsync.hs:54`) | `async`, `wait`/`waitCatch`/`poll`, `cancel`, `race`, `concurrently` | `MonadAsync.hs:358` | `Types.hs:651-675` |
| `MonadEventlog` (`MonadEventlog.hs:12-16`) | `traceEventIO`, `traceMarkerIO`, `flushEventLog` | `MonadEventlog.hs:33-34` | `Types.hs:794-798` (`traceEventIO = traceM . EventlogEvent`) |

What does **not** exist (all probed, see Evidence):

- **No randomness/entropy class.** The io-classes module list contains no
  `MonadRandom`; `:browse Control.Monad.Class.MonadRandom` fails.
- **`Control.Monad.Class.MonadDelay` is gone in 1.11** — the module does not
  resolve; `MonadDelay` now lives inside `Control.Monad.Class.MonadTimer`
  (`MonadTimer.hs:23`).
- **No `Control.Monad.Class.MonadTime.System`** — GHCi suggests
  `Control.Monad.Class.MonadTime.SI` instead (from the `si-timers`
  sublibrary, not a direct dep).
- **No `timeoutCancellable` anywhere in io-classes/io-sim 1.11** (zero grep
  hits); the nearest is `registerDelayCancellable` in the `si-timers`
  sublibrary (`si-timers/.../MonadTimer/SI.hs:141`). Neither class has a
  `wait` method — that is `MonadAsync.wait`.
- `MonadUnique` is a per-process **counter**, not entropy: the IO instance
  is `Data.Unique.newUnique` (`MonadUnique.hs:39`). It cannot supply jitter.

## 3. What the repo rules say

**Two layers (Rule 4).** `AGENTS.md:61`: "Two layers: plain-Haskell internals
hold all logic; Bluefin 0.9 `Ask`/`IOE` capabilities live only at the
external seam, never in internals." `AGENTS.md:68`: "plain-Haskell internals
(**no Bluefin imports**) hold all logic and tests". ADR-0006 says the same:
"The internal layer is plain Haskell with no Bluefin imports"
(`docs/adr/0006-two-layer-bluefin-seam.md:3`). The prohibition is **Bluefin
specifically**, not every effect framework — and plan Rule 4 goes further,
listing "`MonadAsync/MonadSTM` constraints for io-sim" as part of the
internal layer (`.lavish/rust-port-plan.html:242`). The plan's stated target
is "Built on plain `IO` + `io-classes` with `io-sim` tests"
(`rust-port-plan.html:74`). `AGENTS.md:69` records the current status
("`io-classes`/`io-sim` are unused deps") — a status note, not a ban.

**Rule 3 (errors).** `AGENTS.md:69`: "per-domain `Either` ADTs in the core
(`CodecError`, `StepError`, `WorkflowRunError`); base async exceptions
(`AsyncCancelled`) rethrown without recording at the edges."
`rust-port-plan.html:241`: "Cancellation (`AsyncCancelled`/`ThreadKilled`) is
rethrown without recording … No `MonadThrow`/`MonadCatch` abstraction and no
retry policy yet (`retry.rs` unported); `io-classes` stays an unused dep."
Prior art for the rethrow: `Supervisor.hs:44-49` and `Workflow.hs:90-95`.

**Rule 5 (logging).** `Log.hs:4-9`: "explicit 'LogAction', never ambient …
simulations instantiate it over @say@ and assert with
@selectTraceEventsSay@". `Log.hs:41-51` defines `nullLogAction` and
`withStdoutLogger`; `rust-port-plan.html:243` describes the sim half.

**Seam style.** `Store.hs:9`: "Adding a seam here means adding a record,
never a typeclass." `AGENTS.md:82`: "Typeclasses: concrete modules now; a
second real backend earns the Port pattern, test fakes use
records-of-functions." Plan Rule 1 (`rust-port-plan.html:234`) says the same
for DB fakes.

**Prior io-sim intent.** `docs/io-sim-simulations.md:111-118` already
proposes generalizing the engine to `(MonadAsync m, MonadDelay m, MonadSTM m,
MonadCatch m, …)`; `:148-151` says UUID/clock "must go through the sim:
counter-based ids and `MonadTime` in sim, real ones in production"; `:180`
pins `^>=1.11`.

**Current code facts.** Sleep is concrete IO today (`Step.hs:22,104`,
`Supervisor.hs:16,34`); `DbosDbError` is a `String` newtype
(`Postgres.hs:1079-1082`) thrown by `runDbOrFail` (`Postgres.hs:1109`), so
it carries no SQLSTATE/kind to classify on.

## 4. Per-effect decisions

| Effect (Rust) | io-classes class (or none) | Fits two-layer rule? | Recommendation |
|---|---|---|---|
| Sleep (`retry.rs:118`) | `MonadDelay.threadDelay :: Int -> m ()` (`MonadTimer.hs:29`); IOSim instance `Types.hs:722` | Yes — io-classes ≠ Bluefin (`AGENTS.md:61,68`); Rule 4 explicitly allows monad constraints in internals (`rust-port-plan.html:242`) | **Adopt** `MonadDelay` for the retry core. It is the one class both backends already satisfy, and it is what makes retry delays virtual-time deterministic. Keep `Millis` and the existing microsecond conversion (`Step.hs:123-124`). |
| Timeouts (`registerDelay`/`timeout`) | `MonadTimer` (`MonadTimer.hs:32-38`) | Yes | **Reject for retry**: `retry.rs` has no timeout; cancellation is the caller's (`retry.rs:83-85`). Defer `MonadTimer` until a caller needs it. `timeoutCancellable` does not exist in 1.11 (only `si-timers`' `registerDelayCancellable`, not a direct dep). |
| Wall clock | `MonadTime`/`MonadMonotonicTimeNSec` (`MonadTime.hs:23-35`) | Yes | **Reject for retry**: `retry.rs` never reads a clock; the sleep is relative. Relevant only if/when `sleepStep`'s `getPOSIXTime` (`Step.hs:120-121`) is generalized. |
| Entropy for jitter (`retry.rs:134`) | **None exists** (probe: no `MonadRandom`) | n/a | **Adopt a pure-argument shape**: `jitter :: Word32 -> Millis -> Millis` plus an injected `m Word32` source. Production: `uuid` v4 (already a dep; mirrors Rust). Sim: deterministic counter/list (mirrors `io-sim-simulations.md:148-151`). **Reject** `MonadUnique` (counter, not uniform, `MonadUnique.hs:39`) and `MonadST`+STRef as the default (see §5). |
| Logging (`retry.rs:111-117`) | `MonadSay` exists but is not the seam | Yes | **Adopt the existing `LogAction m DbosLogMsg` parameter** (`Log.hs:18`), per Rule 5; instantiate over `say` in sim (`rust-port-plan.html:243`, `io-sim-simulations.md:46-53`). No new class. |
| Retry loop + classification (`retry.rs:69-79,102-122`) | None needed — pure | n/a | **Adopt** as a plain recursive function in `m` over a Haskell `BackendErrorKind`-equivalent ADT (`error.rs:304-317`). `shouldRetry` stays a pure function, testable without IO. |
| Cancellation (`retry.rs:83-85`) | `MonadCatch`/`MonadThrow`/`MonadMask` (`MonadThrow.hs:40,77,188`); `AsyncCancelled` is re-exported by `MonadAsync.hs:14,45` | Textually yes, but plan Rule 3 records "no `MonadThrow`/`MonadCatch` abstraction" yet (`rust-port-plan.html:241`) | **Defer**: keep the retry core exception-free (work returns `Either`), and put `try @DbosDbError` + explicit async rethrow in the one concrete IO adapter, exactly as `Supervisor.hs:44-49`/`Workflow.hs:90-95` do. `MonadMask` is not needed at all — no bracket in retry. Revisit together with `io-sim-simulations.md:111-118`'s engine-wide generalization. |
| Concurrency | `MonadAsync`/`MonadFork` (`MonadAsync.hs:54`, `MonadFork.hs:41`) | Yes | **Reject for retry**: the Rust module is one sequential future with no spawn; cancellation is the caller's concern (Executor's `TVar [Async]`, `rust-port-plan.html:239`). |

**Prerequisite before any retry loop can classify:** `DbosDbError` must carry
the backend verdict (kind + SQLSTATE) the way `BackendError` does
(`error.rs:280-310`, classification at `postgres.rs:169-201`). Today it is
`newtype DbosDbError = DbosDbError String` (`Postgres.hs:1079-1082`) built
from `show Pool.UsageError` (`Postgres.hs:1109`), so the classification
evidence is discarded. Port order: widen the error type first, then retry.

## 5. Making `jitter` deterministic in IOSim

There is no entropy class to satisfy, so the choice is where the word comes
from:

1. **Pure `jitter` + injected `m Word32` (recommended).** The distribution is
   tested purely, mirroring Rust's own unit test
   (`retry.rs:262-271`); production supplies `uuid` v4 bits; the sim supplies
   a counter or a scripted list. This matches `Store.hs:9` ("a record, never
   a typeclass") and `AGENTS.md:82` (fakes are records/functions), without
   inventing a class io-classes deliberately does not have.
2. **`MonadST` + `STRef` counter.** It *works* — the probe shows
   `stToIO (newSTRef 0) :: IOSim s (STRef s Int)` typechecks, and `LiftST`
   is executed by the simulator (`Types.hs:685-686`,
   `Internal.hs:325-327`). But the state is invisible to the trace (no
   `LiftST` event), it forces `s` plumbing through the retry signature, and
   the package itself warns off mutable primitives ("don't try to use the
   @MVar@s as that will not work as expected", `Types.hs:675-683`). Reject
   as the default; acceptable inside a `SimDB`-style model that already owns
   `TVar`s (`SimDB.hs:30-36`).
3. **`MonadUnique`.** Reject for jitter: IO's `newUnique` is a process
   counter (`MonadUnique.hs:39`), and `hashUnique` is not uniform. It is the
   right class for identity, not for entropy (`io-sim-simulations.md:148`).
4. **Observability.** If the drawn word should be assertable, route it
   through the existing `LogAction`/`say` seam (`Log.hs:4-9`) or
   `MonadEventlog.traceEventIO` (`MonadEventlog.hs:16`, IOSim maps it to
   `traceM`, `Types.hs:794-798`) — not through a new class.

## 6. Where the seam goes if adopted

Retry core as a class-polymorphic function over `MonadDelay` plus an injected
entropy action, in a plain-Haskell internal module (no Bluefin, no Postgres
imports), e.g. `src/DBOS/Transact/Retry.hs`:

```haskell
withRetry ::
  MonadDelay m =>
  RetryPolicy ->
  LogAction m DbosLogMsg ->
  m Word32 ->                            -- entropy, injected
  m (Either RetryError a) ->             -- work, re-run per attempt
  m (Either RetryError a)
```

- The pure pieces stay pure: `jitter :: Word32 -> Millis -> Millis` and
  `shouldRetry :: RetryPolicy -> RetryError -> Bool`.
- Production instantiates at `IO`: entropy = `uuid` v4 bits, sleep =
  `MonadDelay IO`; the concrete adapter wraps DB actions that throw
  `DbosDbError`, converting to `Either` and rethrowing `AsyncCancelled`
  (the `Supervisor.hs:44-49` pattern).
- The Rust identity rule transfers as API documentation: the work action
  must capture ids from outside the loop (`retry.rs:25-30`); passing `work`
  as a value built by the caller makes that the natural shape.
- Do **not** add a `RetryEffects` record duplicating `threadDelay` — a record
  earns its keep only where no class exists, which here is entropy alone, and
  that is one function argument. Do not add a repo-local retry typeclass
  (plan Rule 1, `rust-port-plan.html:234`; `AGENTS.md:82`).

## Open questions

- Whether the retry loop should wait for the engine-wide io-sim
  generalization (`io-sim-simulations.md:111-118`) or land now as
  `MonadDelay`-polymorphic; plan Rule 3's "no retry policy yet"
  (`rust-port-plan.html:241`) needs updating either way.
- Whether `DbosLogMsg` should grow `attempt`/`delay` fields
  (`Log.hs:32-37`) or text-encode them as the Executor does
  (`Executor.hs:108`); Rust logs them as structured `tracing` fields
  (`retry.rs:111-117`).
- Whether to depend on `io-classes:si-timers` for `DiffTime`-typed delays and
  `registerDelayCancellable`; not needed for retry (millisecond delays,
  microsecond `threadDelay`), and it would be a new sublibrary dep.

## Evidence

All probes run from `/Users/duke/dev/dbos-transact-hs` with
`cabal exec -- ghci -ignore-dot-ghci` (the repo `.ghci` loads `test/Main.hs`,
so `-ignore-dot-ghci` keeps the probes clean).

1. Class methods and IO/IOSim instances (load-bearing):

```
$ cabal exec -- ghci -ignore-dot-ghci -v0 -e ':browse Control.Monad.Class.MonadTimer'
class Monad m => Control.Monad.Class.MonadTimer.MonadDelay m where
  Control.Monad.Class.MonadTimer.threadDelay :: Int -> m ()
class (Control.Monad.Class.MonadTimer.MonadDelay m,
       Control.Monad.Class.MonadSTM.Internal.MonadSTM m) =>
      Control.Monad.Class.MonadTimer.MonadTimer m where
  Control.Monad.Class.MonadTimer.registerDelay :: Int -> m (TVar m Bool)
  Control.Monad.Class.MonadTimer.timeout :: Int -> m a -> m (Maybe a)
```

   With `import Control.Monad.IOSim` first, `:info` showed the IOSim
   instances, e.g. `instance MonadDelay (IOSim s)` and
   `instance MonadTimer (IOSim s)` (both "Defined in
   ‘io-sim-1.11.0.0:Control.Monad.IOSim.Types’"), alongside the IO ones
   ("Defined in ‘Control.Monad.Class.MonadTimer’"). The full probe script
   (`probe.ghci`) covered `MonadST`, `MonadTime`, `MonadMonotonicTimeNSec`,
   `MonadUnique`, `MonadSay`, `MonadFork`, `MonadThrow`, `MonadCatch`,
   `MonadMask`, `MonadEventlog`, `MonadAsync` — every one has both `IO` and
   `IOSim s` instances.

2. No randomness class:

```
$ cabal exec -- ghci -ignore-dot-ghci -v0 -e ':browse Control.Monad.Class.MonadRandom'
<no location info>: error: [GHC-61948]
    Could not find module ‘Control.Monad.Class.MonadRandom’.
```

   The extracted io-classes 1.11.0.0 tree confirms it: the only
   `Control/Monad/Class/*` modules are `MonadAsync`, `MonadEventlog`,
   `MonadFork`, `MonadSay`, `MonadST`, `MonadSTM`, `MonadTest`,
   `MonadThrow`, `MonadTime`, `MonadTimer`, `MonadUnique`.

3. `Control.Monad.Class.MonadDelay` no longer exists in 1.11:

```
$ cabal exec -- ghci -ignore-dot-ghci -v0 -e ':browse Control.Monad.Class.MonadDelay'
error: [GHC-61948] Could not find module ‘Control.Monad.Class.MonadDelay’.
Perhaps you meant Control.Monad.Class.MonadSay ...
```

   Likewise `:browse Control.Monad.Class.MonadTime.System` fails; GHC
   suggests `Control.Monad.Class.MonadTime.SI`. `grep -rn timeoutCancellable`
   over both packages returns nothing.

4. `MonadST` + `STRef` works under IOSim (the STRef alternative in §5):

```
ghci> import Control.Monad.IOSim
ghci> import Control.Monad.Class.MonadST
ghci> import Data.STRef
ghci> :t stToIO (newSTRef 0) :: IOSim s (STRef s Int)
stToIO (newSTRef 0) :: IOSim s (STRef s Int)
```

5. Same polymorphic retry body runs in IO-shaped and sim-shaped code; the
   sim's virtual clock advances by exactly the jittered delays, and entropy
   is a pure input (`JitterProbe.hs`, written to the temp dir, not the repo):

```haskell
jitter :: Word32 -> Double -> Double
jitter bits backoff =
  backoff * (0.5 + fromIntegral bits / (fromIntegral (maxBound :: Word32) + 1))

retryLoop :: (MonadDelay m, MonadSay m) => [Word32] -> LogAction m String -> m Int
```

```
ghci> :t retryLoop :: (MonadDelay m, MonadSay m) => [Word32] -> LogAction m String -> m Int
  :: (MonadDelay m, MonadSay m) =>
     [Word32] -> LogAction m String -> m Int
ghci> mapM_ print (selectTraceEventsSayWithTime (runSimTrace sim))
(Time 0,"retry delay 0.5s")
(Time 0.5,"retry delay 2.9999999995343387s")
(Time 3.499999,"retry delay 2.9313225746154785s")
```

   The delays are the pure jitter of `[0, maxBound, 1e9]` over backoffs
   `1,2,4`; the log lines come out of `LogAction` over `say`, and the trace
   times are virtual (0 → 0.5 → 3.499999s), proving sleep + logging + entropy
   are all controllable without wall clock or OS randomness.

6. Version provenance: the `:info` output names the instances'
   packages/modules as `io-sim-1.11.0.0:Control.Monad.IOSim.Types` and
   `Control.Monad.Class.MonadTimer`; `cabal exec` reported the environment
   with `io-classes-1.11.0.0` / `io-sim-1.11.0.0` available.
