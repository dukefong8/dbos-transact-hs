# TODO: sim mirrors share the production concurrency path

Status: **step 1 done** (2026-10-01); steps 2-6 not started. Read `adr/0016-dual-stack-testing.md` (the structure), `adr/0020-sim-mirrors-share-the-concurrency-path.md` (the principle), then this file. Everything here is written to be executed by a session with none of the originating context.

## The test every step must pass

- **ADR-0020's criterion**: deleting a case's sim half removes no engine function call from the suite.
- **Flip guard**: after converting any case, temporarily flip one expectation and confirm *both* halves fail (a sim half that silently no-ops otherwise reports green). Revert.
- **House rules**: exactly one enabled `-- $>` toggle in `test/Main.hs` (today `DBOS.Transact.WorkflowTest.tests`, `test/Main.hs:83`); `tests` carries live trees only, `simTests` (`test/Main.hs:150-161`) the eval-only mirrors; no new dependency without the gate in `AGENTS.md`.
- **Never red**: one case migrated per commit-sized step, each green (`cabal build test:test && cabal test test`) before the next.

## Step 1 — The `Tasks` group sets the pattern (settled; implement as written)

Target: `test/DBOS/Transact/WorkflowTest.hs:2338-2404` — six cases become eleven leaves (5 × IO + 5 × IOSim + 1 IO-only).

Design, settled in review — do not re-derive:

- **Bodies return values; the runner owns the assertion.** `@?=` is `Assertion = IO ()` and cannot live in a monad-polymorphic body; the current cases already put it outside `runSimOrThrow`.
- **Capability style**: the single stack-specific operation is passed as a bare `ThreadId m -> m ()` — not a record (one-field ceremony), not a class. Bodies that do not await stay parameter-free (no `-Wunused-matches`).
  - IO half: `waitFinished` (staged at `WorkflowTest.hs:2328`) — **bound it** with `timeout` + failure, mirroring `ContextTest.waitFor` (`test/DBOS/Transact/ContextTest.hs:372`).
  - IOSim half: `simWaitDeparture _ = threadDelay 1000` — one sim tick; the scheduler runs the self-terminating child to completion during it.
- **Sim halves run `runSimOrThrow`**, not `runSimCase`: these bodies emit no tracer events, so there is no trace to print and no value in `runSimCase`'s second run. Nothing prints, so a plain `testGroup` is fine (`dependentTestGroup ... AllFinish` binds printing trees only).
- **Both halves run in-process inside the live tree** — what four of the six cases already do — so no `simTests` entry and no toggle change. ADR-0020 records the pattern; ADR-0016's rules are untouched.

Plumbing to add (signatures final; add `{-# LANGUAGE RankNTypes #-}`; the `simRun :: forall s. IOSim s a` binding must be explicit, and `body @(IOSim s)` must not be written inline):

```haskell
type TaskCase m = (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m, MonadDelay m)

bothStacks :: String -> (a -> IO ()) -> (forall m. TaskCase m => m a) -> TestTree
bothStacksAwaiting :: String -> (a -> IO ()) -> (forall m. TaskCase m => (ThreadId m -> m ()) -> m a) -> TestTree
dualGroup :: String -> (a -> IO ()) -> IO a -> (forall s. IOSim s a) -> TestTree
ioOnly :: String -> String -> IO () -> TestTree    -- reason rendered into the reported name
```

Cases (line numbers are today's):

| case | shape | notes |
|---|---|---|
| abortAll waits until every task has departed (`:2342`) | `bothStacks` | raise the child delays 1 s → **10 s**: under IO a >1 s deschedule between fork and sweep would let the children finish and break `@?= 2`; sim is immune — time advances only when nothing is runnable |
| a task that finished on its own is not left in the registry (`:2349`) | `bothStacksAwaiting` | its 1 ms tick is sim time today (deterministic); shared, the IO half needs the await — a fixed tick is a guess where the observation is exact |
| a task arriving after the sweep is aborted on arrival (`:2356`) | `bothStacks` | refusal is synchronous and identical on both stacks |
| an empty sweep returns at once (`:2365`) | `bothStacks` | first slice: smallest proof of the rank-2 plumbing |
| a spawn refused after abort fills its channel instead of hanging (`:2368`) | `ioOnly` | reason: "real preemption, not cooperation"; body unchanged |
| a task finishing before registration is not swept as aborted (`:2383`) | `bothStacksAwaiting` | **this is the case that failed**: under load it slept a fixed 1 ms, the spawned task had not departed, and the sweep correctly counted it — `FAIL … Exception: user error (dead tasks swept as aborted: 1)` (pasted 2026-10-01); the await removes the assumption. Keep the 200-iteration loop; the comment records that only the IO half can reach the orphan path (sim cannot preempt a forked child) |

Order: plumbing + "an empty sweep" → "abortAll waits" → "arriving after the sweep" → "finished on its own" → "finishing before registration" → case 5 to `ioOnly` → rewrite `tasksTests`' Haddock (it currently claims the group is IOSim-driven, now wrong).

Verification: `cabal test test --test-option='--pattern' --test-option='$3 == "Tasks"'` → exactly 11 leaves, all green; flip guard; repeat the pattern ~20× (cases 2 and 6 must never report a nonzero sweep count; group wall time stays in the tens of ms); watcher reload green.

**Done 2026-10-01**: 11 leaves, all green (~0.27 s for the group); flip guard fired on both halves when an expectation was flipped; 20 runs of the pattern, zero failures; full suite 585 (was 580); watcher `All good (77 modules)`. The old fixed-sleep sites are gone — case 2 and case 6 await the thread, and no run has reported a nonzero sweep count since.

## Step 2 — Convert `WorkflowTestSim`'s Mem-driven cases to shared bodies

Target: `test/DBOS/Transact/WorkflowTestSim.hs` (40 cases) and `test/DBOS/Transact/WorkflowTest.hs` (43 live cases; 38 are mirrored by name). Pattern: `ContextTest`/`ContextTestSim` — shared `scenario*` bodies over a `Fixture m`-style record (`ContextTest.hs:156,165+`), the live tree and the sim tree each calling them with their own fixture.

Do it in this order (closest to shared first):

1. Cases that already drive `MemSystemDB` through engine functions (`runWfSim`/`memLaunchOn`, `WorkflowTestSim.hs:1655-1675`) — lift the body into the live module, parameterize over the fixture, run both halves.
2. The four staged/re-encoded sites ADR-0020 lists: `WorkflowTestSim.hs:1251-1255` (queue-runner staging), `:1638-1651` (hand-emitted events), `:1464-1498` (test-side `forkIO`/`killThread` + `waitForRow`). Each is either converted to drive the engine or marked (step 5).
3. The un-mirrored tails: live-only "a recovery run replays completed steps" (`WorkflowTest.hs:137`), "an unregistered workflow is skipped" (`:194`), "a replayed parent reads the recorded outcome" (`:946`), "a foreign error is converted at the boundary" (`:1452`), "select reports the first workflow to settle" (`:1602`); sim-only "a budget cancels the workflow durably" (`WorkflowTestSim.hs:1516`). Mirroring recovery needs step 4 first (`MemSystemDB` delegates `reenqueueForRecovery` to the mock, `IOSim.hs:574`); select timing is wall-clock-bound and may end up marked.

Record per case in this file: converted / marked-with-reason / still open.

## Step 3 — Audit the remaining sim trees against the criterion

`ManagementTestSim`, `MessageTestSim`, `SleepTestSim`, `WaitTestSim`, `StepTestSim`, `HandleTestSim`, `ContextTestSim` (the last is already the model). For each: list the cases whose sim half re-encodes rather than drives (grep for test-side `atomically`/`forkIO`/`threadDelay` blocks that stand in for engine steps, and for hand-emitted tracer calls), and record the list here with file:line.

## Step 4 — Close `MemSystemDB` gaps that matter for concurrency

`test/DBOS/SystemDB/IOSim.hs:574-613` delegates ~40 methods to `MockSystemDB`'s canned answers. Implement, in rough priority: queue/dequeue claim + delayed transition (`reenqueueForRecovery`, `transitionDelayedWorkflows`, the queue methods), notifier wakeups (`sendMessage`/`recv`, so parked readers actually wake), schedules (`updateSchedule`, `setScheduleStatus`), messages/streams. Each implementation is what lets step 2 drive engine code instead of staging it. Keep `MockSystemDB` untouched for the existing suites.

## Step 5 — Marking sweep

Every case the simulator cannot run gets `ioOnly` with its reason in the reported name, and the reason is added to ADR-0020's IO-only running list. No case leaves the sim tree silently.

## Step 6 — Docs as each step lands

- ADR-0020's IO-only list grows in step 5.
- ADR-0016 gets an addendum pointer if the inline dual-stack shape spreads beyond `Tasks`.
- `AGENTS.md`'s port-gate section names the criterion once step 1 lands.
- CHANGELOG only when engine-visible behavior changes (none expected: these are test-tree and fixture changes; `src` should not move — if it does, the oracle leg of the three-leg gate applies).

## Open questions for the implementer

- Where the step-2 fixture record lives (live module, as `ContextTest` does, or a new shared module once three trees use it).
- Whether a converted body that *does* emit events switches its sim half to `runSimCase` + `printSimTrace` (then the tree needs `dependentTestGroup ... AllFinish`), or drops the say half entirely.
- Whether to adopt `exploreSimTrace`/POR for the fork→registration race (needs `ScheduleControl` bookkeeping + QuickCheck; ADR-0020 records it as an option, not a default).
