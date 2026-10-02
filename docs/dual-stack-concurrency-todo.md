# TODO: sim mirrors share the production concurrency path

Status 2026-10-01: **all steps done** — the Tasks pattern, 36 shared Workflow cases (order-identical leaves, zero per-side bodies), the audit of the other sim trees, the `MemSystemDB` gap closure with its own tests, the marking sweep (the symmetric `-- IO only:`/`-- Sim only:` markers), and the docs. Read `adr/0016-dual-stack-testing.md` (the structure), `adr/0020-sim-mirrors-share-the-concurrency-path.md` (the principle), then this file. Everything here is written to be executed by a session with none of the originating context.

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
- **Runners are split since 2026-10-01** (was: both halves in-process in
  the live tree): IO leaves run in `tasksTests` (live tree, `cabal test`
  verifies the IO backend), IOSim leaves run in `tasksSimTests`
  (`WorkflowTestSim`, tasty watcher verifies the Sim backend). Bodies and
  checks are shared — `checkNoMiscounts` lives in the live module and the
  sim tree judges by it. Reason: one runner per backend, so each backend
  is verified by its own loop instead of both riding the live eval.

Plumbing to add (signatures final; add `{-# LANGUAGE RankNTypes #-}`; the `simRun :: forall s. IOSim s a` binding must be explicit, and `body @(IOSim s)` must not be written inline):

```haskell
type TaskCase m = (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m, MonadDelay m)

bothStacks :: String -> (a -> IO ()) -> (forall m. TaskCase m => m a) -> TestTree
bothStacksAwaiting :: String -> (a -> IO ()) -> (forall m. TaskCase m => (ThreadId m -> m ()) -> m a) -> TestTree
dualGroup :: String -> (a -> IO ()) -> IO a -> (forall s. IOSim s a) -> TestTree
ioOnly :: String -> String -> IO () -> TestTree    -- reason rendered into the reported name
```
(The `ioOnly`/`simOnly` name-suffix helpers are gone again: names stay
plain with reason comments since the same-tree pass, above.)

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

**Done 2026-10-01**: 11 leaves, all green (~0.27 s for the group); flip guard fired on both halves when an expectation was flipped; 20 runs of the pattern, zero failures; full suite 585 (was 580); watcher `All good (77 modules)`.
**Split 2026-10-01**: 6 IO leaves live + 5 IOSim leaves in `WorkflowTestSim` (45 sim leaves total); bodies/checks shared unchanged; flip guard re-fired on both runners after the move; full suite 583 (live only — the 5 sim leaves now ride tasty, not `main`). One transient `Queues/worker concurrency bounds a dequeue` FAIL on the first full run, launched while the watcher eval was still in flight (two suite binaries on the shared DB — the overlap `AGENTS.md` warns about); `Queues` 5/5 green alone and full suite 583 green with the watcher confirmed idle. The old fixed-sleep sites are gone — case 2 and case 6 await the thread, and no run has reported a nonzero sweep count since.

## Step 2 — Convert `WorkflowTestSim`'s Mem-driven cases to shared bodies

Target: `test/DBOS/Transact/WorkflowTestSim.hs` (40 cases) and `test/DBOS/Transact/WorkflowTest.hs` (43 live cases; 38 are mirrored by name). Pattern: `ContextTest`/`ContextTestSim` — shared `scenario*` bodies over a `Fixture m`-style record (`ContextTest.hs:156,165+`), the live tree and the sim tree each calling them with their own fixture.

Do it in this order (closest to shared first):

1. Cases that already drive `MemSystemDB` through engine functions (`runWfSim`/`memLaunchOn`, `WorkflowTestSim.hs:1655-1675`) — lift the body into the live module, parameterize over the fixture, run both halves.
2. The four staged/re-encoded sites ADR-0020 lists: `WorkflowTestSim.hs:1251-1255` (queue-runner staging), `:1638-1651` (hand-emitted events), `:1464-1498` (test-side `forkIO`/`killThread` + `waitForRow`). Each is either converted to drive the engine or marked (step 5).
3. The un-mirrored tails: live-only "a recovery run replays completed steps" (`WorkflowTest.hs:137`), "an unregistered workflow is skipped" (`:194`), "a replayed parent reads the recorded outcome" (`:946`), "a foreign error is converted at the boundary" (`:1452`), "select reports the first workflow to settle" (`:1602`); sim-only "a budget cancels the workflow durably" (`WorkflowTestSim.hs:1516`). Mirroring recovery needs step 4 first (`MemSystemDB` delegates `reenqueueForRecovery` to the mock, `IOSim.hs:574`); select timing is wall-clock-bound and may end up marked.

Record per case in this file: converted / marked-with-reason / still open.

Converted 2026-10-01 (slice 1):
- "a registered workflow starts and records its result" — shared
  `scenarioRegisteredRecordsResult` over new `WfFixture` in
  `WorkflowTest.hs` (`wfNewDBOS`/`wfLaunch`/`wfFreshId`/`wfReadRow`);
  live via `liveWfFixture` (production `launchWithEnvironment`, per-test
  UUIDs, stronger row assertions now live too), sim via `simWfFixture`
  (fresh `MemSystemDB` per case, `memLaunchOn`, `sim-wf-double`).
  Live 54 green, sim 40 green, flip guard (`decoded+1`) failed both halves.

Converted 2026-10-01 (slice 2):
- "starting a taken id joins the existing run" — shared
  `scenarioJoinTakesId` returning `JoinOutcome` (both results, entry
  count, row status, both handle ids, first handle's PENDING read) over
  the same `WfFixture` (no fixture change needed); MVar gate + TVar
  counter unified on io-classes (`IORef` gone from the live case);
  result waits bounded by a shared 15 s `timeout` (virtual in sim).
  Sim gains handle-id + PENDING assertions (deterministic green); live
  keeps production launch + suite-backend row read.
  Live 54 green, sim 40 green, flip guard (`count+1`) failed both halves.

Converted 2026-10-01 (slice 3):
- "a fresh start is local and a join polls" — shared
  `scenarioFreshJoinPolls` returning the three provenance labels plus the
  decoded result; shared bounded `awaitSettled` hoisted to top level (the
  join scenario now calls it too). Sim keeps its label assertions verbatim;
  live keeps production launch.   Live 54 green, sim 40 green, flip guard
  (`n+1`) failed both halves.

Converted 2026-10-01 (slice 4):
- "awaiting a child is recorded as a step" — shared
  `scenarioAwaitRecorded` returning decoded result, parent steps, and the
  derived child id; `WfFixture` grows `wfListSteps` (live: suite-backend
  `listWorkflowSteps` with payloads; sim: `MemSystemDB` steps). Child +
  parent bodies shared verbatim (same `startChildWorkflow`/`awaitChild`
  engine calls). Both trees assert the start + `DBOS.getResult`
  checkpoint shapes. Live 54 green, sim 40 green, flip guard (`n+1`)
  failed both halves.

Converted 2026-10-01 (slice 5):
- "a recorded await of another workflow is refused" — shared
  `scenarioStaleAwaitRefused`; `WfFixture` grows `wfSystemDB` (the
  passed-in backend) so the scenario plants the stale await at step 1
  through `runSystemDB` while the forked parent parks on a gate after
  its start step. This replaces the sim's old pre-launch plant: both
  halves now run the same sequence (fork → gate → plant → release), and
  live keeps its async/gate shape. Sim asserts the events — child
  `WorkflowCompleted` then the parent's `WorkflowControlEnded` with the
  refusal detail — with no printing. Live 51 green, sim 51 green, flip
  guard (`stepId` expectation) failed both halves.

Converted 2026-10-01 (slice 6):
- "awaiting a child inside a step is covered by that step" — shared
  `scenarioAwaitInsideStep` (child start, await inside
  `runWorkflowStepWith "collect"`, step list) + `checkAwaitInsideStep`.
  The scenario needs `MonadAsync m` (`runWorkflowStepWith`'s constraint).
  Sim asserts the events: child `WorkflowCompleted`, `StepOutputRecorded
  "collect" 1`, parent `WorkflowCompleted`, no printing. Live 51 green,
  sim 51 green, flip guard (`n` expectation) failed both halves.

Converted 2026-10-01 (slice 7):
- "child starts and awaits keep their ids in build order" — shared
  `scenarioChildIdsInBuildOrder` (three starts via `mapM`, then the
  awaits in the same order, first-built child row read) +
  `checkChildIdsInBuildOrder` (sum, start/await table derived from the
  parent id, first child output). Sim asserts the events: three child
  `WorkflowCompleted` in build order, then the parent's, no printing.
  Live 51 green, sim 51 green, flip guard (`n` expectation) failed both
  halves.

Converted 2026-10-01 (slices 8-11, the child-id and select/race group):
- "runs claim their pairs of step ids adjacently" —
  `scenarioStepIdPairs`/`checkStepIdPairs` (interleaved start+await
  pairs; ids (0,1),(2,3),(4,5)). Sim trace: children -0, -2, -4 settle
  each before its own await, then the parent.
- "a select step races a step against a child's result" —
  `scenarioSelectStepRaces`/`checkSelectStepRaces` (pending step vs
  pending await, await arm wins; losing step leaves no row; history
  start/await/select). Sim trace: child completes, parent completes.
- "a control signal winning a select records no winner" —
  `scenarioControlSelect`/`checkControlSelect` (interrupted arm wins;
  `Interrupted` comes back; no rows; row PENDING). Sim trace:
  `StepControlEnded "interrupted" 0 1`, `WorkflowControlEnded` whose
  detail says left PENDING.
- "a losing step has its cancellation token fired" —
  `scenarioLosingTokenFired`/`checkLosingTokenFired` (fast step wins,
  loser's watcher observes the token fire). Sim trace:
  `StepOutputRecorded "fast" 1`, then the parent completes.
  All four: live 51 green, sim 51 green, flip guard failed both halves.

Converted 2026-10-01 (slices 12-16, cancellation and deadlines):
- "a cancelled child is an awaited cancellation in the parent" —
  `scenarioCancelledChildAwaited`/`checkCancelledChildAwaited` (child
  budget 300 ms vs 30 s sleep; parent ends on `AwaitedWorkflowCancelled`
  and records it; parent row `Error`, child row `Cancelled`). Sim trace:
  `WorkflowDeadlineCancelled` (child), `WorkflowFailed` (parent).
- "a child inherits its parent's deadline" —
  `scenarioDeadlineInherited`/`checkDeadlineInherited` (child copies the
  parent's instant; only the parent records the timeout). Sim trace:
  child completes, parent completes.
- "a child's own timeout replaces the inherited deadline" —
  `scenarioChildBudgetWins`/`checkChildBudgetWins` (child records its
  own 3600 s timeout and outlives the parent's 60 s deadline).
- "a child can decline the inherited deadline" —
  `scenarioDeclinedDeadline`/`checkDeclinedDeadline` (silent child
  inherits verbatim; `None` child carries neither deadline nor timeout).
- "a parent and its child hit an inherited deadline independently" —
  `scenarioCascadeDeadline`/`checkCascadeDeadline` (both cancelled; the
  interrupted await leaves only the child start; the live half now
  waits through `waitForWorkflow`, the engine function, instead of a
  row-polling stand-in). Sim trace: `WorkflowDeadlineCancelled` child
  then parent.
  All five: live 51 green, sim 51 green, flip guard failed both halves.

Converted 2026-10-01 (slices 17-19, child leaf refusals and detachment):
- "starting a child inside a step is refused, not recorded" —
  `scenarioChildInsideStepRefused`/`checkChildInsideStepRefused`
  (`InsideStep` comes back; no step rows). Sim trace: `WorkflowFailed`.
- "a child that fails differently is started through lift" —
  `scenarioLiftChildError`/`checkLiftChildError` (the child's own error
  channel crosses the boundary as itself; the parent reports its own
  `GaveUp`). The scenario registers through `registerDBOSWorkflowRef`
  (the IO-only `registerRefOf` alias is gone). Sim trace:
  `WorkflowFailed` child, `WorkflowCompleted` parent.
- "a child started and never awaited is still recorded" —
  `scenarioUnawaitedChild`/`checkUnawaitedChild`; `WfFixture` grows
  `wfChildren`. The detached child settles after the parent. Sim trace:
  `WorkflowCompleted` parent, `StepRunning`/`StepOutputRecorded` child,
  `WorkflowCompleted` child.
  All three: live 51 green, sim 51 green, flip guard failed both halves.

Converted 2026-10-01 (slices 21-22, roots and stale step positions):
- "a workflow started outside a workflow has no parent" —
  `scenarioRootNoParent`/`checkRootNoParent`. Sim trace:
  `WorkflowCompleted` root.
- "a start position holding a plain step is refused" —
  `scenarioPlainStepAtStart`/`checkPlainStepAtStart` (fork parent, gate
  before the first step id, plant a plain step through `wfSystemDB`,
  release; the refusal names the wanted child start and the found plain
  step, and the derived row never exists). Sim trace:
  `WorkflowControlEnded` with the refusal detail.
  Both: live 51 green, sim 51 green, flip guard failed both halves.

Converted 2026-10-01 (slice 35, the former blockers):
- "a parent starts a child under a derived id and replay adopts it" —
  `scenarioDerivedChildAdopted`/`checkDerivedChildAdopted`.
- "an assigned child id wins over the derived one" —
  `scenarioAssignedChildAdopted`/`checkAssignedChildAdopted`.
  Finding while converting: a child start detaches the child onto the
  executor and the child runs inline on both stacks, so the old live
  halves' crash-and-relaunch/recovery interlude ran a child that had
  already finished (and cost ~46 s a case chasing a settle that was
  already terminal). The shared cases now assert what the names say —
  the child runs under its derived/assigned id and the replay adopts the
  recorded id — with no relaunch; the `wfRelaunch`/`wfRecover` fixture
  ops were added, found unnecessary, and removed. Both: live 51 green,
  sim 51 green, flip guard failed both halves.

Converted 2026-10-01 (slices 23-24, rows and inputs):
- "a zero-argument workflow records no input" —
  `scenarioZeroNoInput`/`checkZeroNoInput` (row input stays null). Sim
  trace: `WorkflowCompleted`.
- "the row exists before the body starts" — `scenarioRowBeforeBody`/
  `checkRowBeforeBody` (the body reads its own row through the
  context's database). Sim trace: `WorkflowCompleted`.
  Both: live 51 green, sim 51 green, flip guard failed both halves.
  Still open: remaining 16 sim cases, 4 staged sites, un-mirrored tails.

Converted 2026-10-01 (slices 25-26, panics and unlaunched runs):
- "a panicking workflow leaves its row pending" — `scenarioPanic`/
  `checkPanic` (the body's exception escapes; row stays PENDING, no
  error column). Sim trace: `WorkflowPanicked`.
- "running before launch is refused" — `scenarioRunBeforeLaunch`/
  `checkRunBeforeLaunch` (`ErrorNotLaunched` names the call). Sim trace:
  empty, no events at all (nothing launched).
  Both: live 51 green, sim 51 green, flip guard failed both halves.
  Still open: remaining 14 sim cases, 4 staged sites, un-mirrored tails.

Converted 2026-10-01 (slices 27-28, error columns):
- "an application error round-trips as itself" —
  `scenarioAppErrorRoundtrip`/`checkAppErrorRoundtrip`. Sim trace:
  `WorkflowFailed`.
- "a database failure is not the workflow outcome" —
  `scenarioDbFailureNotOutcome`/`checkDbFailureNotOutcome` (backend error
  back as itself; row stays PENDING, no error column). Sim trace:
  `WorkflowControlEnded "system database error: connection reset by
  peer"`.
  Both: live 51 green, sim 51 green, flip guard failed both halves.
  Still open: remaining 12 sim cases, 4 staged sites, un-mirrored tails.

Converted 2026-10-01 (slices 29-33, steps, shutdown, futures, attributes):
- "a workflow records the steps it took" — `scenarioStepsTaken`. Sim
  trace: both steps' run/record pairs, then completion.
- "shutdown cancels a running workflow and leaves it pending" —
  `scenarioShutdownCancels` (gated run, `waitForRowShared` observation,
  `shutdown`, caller cancelled, row stays PENDING). Sim trace:
  `EngineCancelledRunning 1`, `EngineShutdown`.
- "dropping the future does not stop the workflow" — `scenarioDropFuture`
  (async run, row observed pending, caller cancelled, released, settles
  with 7). Sim trace: `WorkflowCompleted`.
- "a started workflow carries the attributes it was given" —
  `scenarioAttributes` (tenant on the parent's row; child inherits
  nothing). Sim trace: child, then parent completion.
- "a step error is recorded in its column" — `scenarioStepErrorRecorded`
  (step error back; column holds the shortfall). Sim trace:
  `StepErrorRecorded`, `WorkflowFailed`.
  All five: live 51 green, sim 51 green, flip guard failed all five on
  both halves. Shared helpers gained: `rowStatus`, `waitForRowShared`.
  Remaining sim bodies: 3 blocked (two step-4 recovery, one fixture op
  above) and the 2 sim-only cases; everything else is a marker.

Converted 2026-10-01 (slice 34, cross-instance refusal):
- "a child started through another instance is refused" —
  `scenarioWrongInstance`/`checkWrongInstance`; `WfFixture` grows
  `wfSecondInstance` (a second launchable instance with its own
  connection, registry, and suffixed identity over the same backend).
  The refusal names the call; the derived row and the parent's steps are
  never written (the scenario shuts the second instance down itself).
  Sim trace: `WorkflowFailed`, two `EngineShutdown` (both instances).
  Live 51 green, sim 51 green, flip guard failed both halves.

**Step 2 status 2026-10-01: done.** 36 cases are one line per tree over
shared scenarios, checks, and typed sim traces; the leaf order is
identical on both sides. What remains is intentional: the 2 sim-only
cases by design (budget, announcements), and the symmetric markers for
the 5 IO-only cases and the Tasks spawn-refusal leaf. Steps 3 (audit), 4
(Mem gaps), and 5 (marking) are done — see their sections.

Converted 2026-10-01 (slice 20, fan-out timing):
- "children started in a loop run concurrently" — `scenarioFanout`/
  `checkFanout`. One 400 ms child delay for both stacks; the timing
  assertion (`tookMs < 1200`) now runs in **both**, because sim's virtual
  clock advances by the delays: a serialized fan-out would show three
  delays there too. The old sim comment ("elapsed time cannot tell") was
  wrong about the virtual clock. Sim trace: three children, then parent.
  Live 51 green, sim 51 green, flip guard failed both halves.

Same tree 2026-10-01: both trees now carry the identical 51-leaf shape —
every gap marked symmetrically so the IO↔Sim pair diffs name-for-name.
The 5 live-only workflow cases run their real bodies under plain names
with an `-- IO only:` reason comment above each, and Sim carries `pure ()`
markers under the same plain names at the same positions; the 2 sim-only
cases are the mirror image (`-- Sim only:` comments). Names carry no
suffixes anywhere, so the pair diff is pure name-for-name. The reasons
live in the comments plus ADR-0020's running lists. Markers are slots,
not tests (no assertions, no flip guard).
Live 51 green, Sim 51 green, sequences compared identical. Converting a
case swaps its marker for the real body; totals stay 51 throughout.

Single-line cases 2026-10-01: the 34 converted cases are one line per
tree over fully shared scenario + shared pure check (`liveCase` /
`simCase` interpreters; checks follow ContextTest's pure-verdict
pattern). Sim imports the tree pieces and runs them with the sim fixture
under `runSimCase` — no setup or assertion is duplicated anymore. A
single imported `TestTree` cannot cover both backends (tasty leaves are
`IO`; sim execution is rank-2 with trace printing), so one line per side
plus the pair diff is the terminal shape. Flip guard on a shared check
fails both sides from one flip.

Poly tracer + typed sim events 2026-10-01: the tracer is a fixture
parameter (`SomeTracer m`) — FastLogger `ioTracer` over an explicitly
built connection on IO (`launchOn`; converted live cases no longer ride
the production launch — supervisor/prepare coverage rests on the
unconverted and IO-only-commented live cases), `simTracer` over `MemSystemDB`
on IOSim.

Shared fixture builder 2026-10-01: the single tree passes `SomeSystemDB`
and `SomeTracer` in. `mkWfFixture` (live module, imported by sim) builds
the connection, the launch, and the row reads over the passed backend
and tracer; per-side factories supply only atoms (config, identity, app
name, id scheme, id/entropy generators). Live passes Postgres +
FastLogger, sim passes `MemSystemDB` + the sim carrier
(`simIdentity`/`simGeneratedId`/`simEntropy` newly exported for it).
Behavior proven identical by the pair run, including the typed sim
traces. Live 51 / Sim 51 green, pair-diffed identical. Converted sim leaves assert typed events per type
(`selectTraceEventsDynamic`, one list per event ADT — it returns every
event of that type, not one per call) and print nothing; the watcher pane
stays quiet. Live 51 / Sim 51 green, pair-diffed identical.

## Step 3 — Audit the remaining sim trees against the criterion

**Done 2026-10-01.** Method: grep each tree for test-side `atomically`/
`forkIO`/`killThread`/poll loops and hand-emitted `runTracer` calls, then
read each hit. Findings:

- `ContextTestSim` — the model (shared `scenario*` + `Fixture`), one
  structural case hand-emits events for its typed-trace assertions
  (`ContextTestSim.hs:180-190`, `StepRunning`/`SysdbRetryAttempt`), the
  same allowed shape as the announcements case below.
- `SleepTestSim` — drives the real `sleepWorkflowStep`/`sleepPlain`
  over the mock; nothing staged.
- `WaitTestSim` — drives the real `selectWorkflow`; nothing staged.
- `StepTestSim` — drives the real `runWorkflowStep(With)`; the
  `atomically (modifyTVar attempts ...)` hits are step-body counters,
  not staging.
- `MessageTestSim` — drives the real `recv`; the one canned-answer case
  says so in its own name ("reads the mock's canned body, which is not
  JSON").
- `HandleTestSim` — drives `retrieveWorkflow`/`handleResult`; nothing
  staged.
- `ManagementTestSim` — engine-driven (`startWfRefSim`, `cancelWorkflows`,
  `resumeWorkflows`, `waitForWorkflow`); two cases read canned mock
  answers and say so (`ManagementTestSim.hs:140-143`); one structural
  case hand-emits seven management events (`ManagementTestSim.hs:341-347`)
  — same allowed shape as the announcements case.

No test-side scheduling stand-ins found in any of the seven. The two
hand-emitted structural cases stay sim-only with their typed trace
assertions (ADR-0016's structural announcements), and the canned-answer
cases keep their spoken caveats.

## Step 4 — Close `MemSystemDB` gaps that matter for concurrency

**Done 2026-10-01.** `MemSystemDB` now owns real state (TVars) for the
subsystems the engine touches, with the Postgres semantics mirrored from
the statements:

- Queues: `upsertQueue` (LeaveExisting keeps the stored record),
  `startQueuedWorkflows` (worker and concurrency budgets, candidates in
  priority/created order, claim flips to PENDING with executor, version,
  attempt bump, and deadline arming), `getQueuePartitions`,
  `startQueuedPartitionedWorkflows` (one head per pending-free
  partition), `getQueue`, `listQueues`, `updateQueue` (validated),
  `deleteQueue`. Rate limits are not modelled — Mem keeps no dequeue
  history — and `debounceDelayedWorkflow` still delegates to the mock.
- Recovery: `reenqueueForRecovery` (pending rows of the named executors
  at the named version → ENQUEUED on the recovery queue) and
  `transitionDelayedWorkflows` (due DELAYED rows → ENQUEUED, debounced
  keys released).
- Wakeups: `sendMessage`/`sendMessages` append to a messages TVar and a
  notifications TVar and record the caller's step; `recv` replays a
  recorded step, otherwise blocks on the TVar (a send genuinely wakes a
  parked reader) and records the timeout as an absence. `setEvent` /
  `getEvent` likewise, readers parking on the events TVar.
- Schedules (create/upsert/apply/get/list/update/setStatus/lastFired/
  delete, with `NotRegistered` on a missing row) and versions
  (create/list/latest-by-timestamp/update).
- Streams stay delegated: Postgres itself holds `writeStream`,
  `closeStream`, `readStreamValue`, and `getAllStreamEntries` as
  `undefined` (P7.4 deferred), so there is no semantics to mirror.

Tests: `SystemDB.IOSimTest` gains a `MemSystemDB (stateful)` group of 9
cases — claim budgets, delayed transition, recovery, send→recv wakeup,
recv timeout, event wakeup, schedule round-trip, latest version, awaited
settle — all green (72 in that group's suite). `MockSystemDB` untouched.
The pair stays green (live 51 / sim 51).

Still open from step 2's blockers: the two recovery-driven cases need a
sim *supervisor* (launchOn installs none), which is beyond the sysdb
methods; they stay recorded as blocked.

## Step 5 — Marking sweep

Every case the simulator cannot run keeps its plain name with an `-- IO only:` reason comment above it, and the reason is added to ADR-0020's IO-only running list. No case leaves the sim tree silently.

## Step 6 — Docs as each step lands

- ADR-0020's IO-only list grows in step 5.
- ADR-0016 gets an addendum pointer if the inline dual-stack shape spreads beyond `Tasks`.
- `AGENTS.md`'s port-gate section names the criterion once step 1 lands.
- CHANGELOG only when engine-visible behavior changes (none expected: these are test-tree and fixture changes; `src` should not move — if it does, the oracle leg of the three-leg gate applies).

## Gate run 2026-10-01 (all steps closed)

`cabal test test` — **594 green**; psql fixture check against
`$DBOS_DATABASE_URL` — the 7 expected rows present (workflow statuses
SUCCESS/PENDING, both operation outputs, the notification); watcher pair
live 51 / sim 51 green, leaf sequences order-identical. `src/` did not
move, so no oracle leg was owed. (Re-run after slice 35: same result,
594 green, pair 51/51.)

## Open questions for the implementer

- Where the step-2 fixture record lives (live module, as `ContextTest` does, or a new shared module once three trees use it).
- Whether a converted body that *does* emit events switches its sim half to `runSimCase` + `printSimTrace` (then the tree needs `dependentTestGroup ... AllFinish`), or drops the say half entirely.
- Whether to adopt `exploreSimTrace`/POR for the fork→registration race (needs `ScheduleControl` bookkeeping + QuickCheck; ADR-0020 records it as an option, not a default).
