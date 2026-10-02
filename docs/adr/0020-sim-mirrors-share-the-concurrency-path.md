# Sim mirrors share the production concurrency path

A `*Sim` mirror exists for one reason: to reproduce, deterministically, the concurrency behavior production gets from the RTS — MVar parks and wakes, TVar commits, `MonadAsync`/`MonadTimer`/`MonadFork` interleavings — under a scheduler that can be replayed. ADR-0016 decided to keep mirrors beside live trees; this ADR fixes what makes a mirror *valid*:

**A `*Sim` case is valid only if its sim half runs the same top-level engine functions as the live half.** The only permitted differences are the backend (`PostgresSystemDB` vs `MemSystemDB`/`MockSystemDB`) and the scheduler and clock (RTS vs io-sim). Anything else — a re-encoded call sequence, a staged effect, a hand-emitted event, a replaced timing assertion — tests the mock, not the engine, and reports green over concurrency code the simulator never ran.

## Permitted and forbidden divergence

| Divergence | Verdict |
|---|---|
| Backend: rows, answers, pools | Permitted — the seam's purpose |
| Scheduler and clock: threads, delays, wakes | Permitted — the seam's purpose |
| A wall-clock assertion replaced by a virtual-time one | Permitted, and said aloud in the case |
| A test-side re-implementation of an engine step ("what the queue runner would do") | Forbidden — drive the engine, or mark the case |
| Tracer events hand-emitted instead of produced | Forbidden — produce them, or mark the case |
| Test-side `forkIO`/`killThread`/poll loops standing in for engine scheduling | Forbidden — call the engine function that schedules |
| A case the sim cannot run, silently absent from the sim tree | Forbidden — mark it (below), never drop it |

## The observation rule

A per-stack capability may **observe**, never **stage**. The departure wait is the pattern to copy: IO observes the thread (`GHC.Conc.threadStatus` until `ThreadFinished`/`ThreadDied`, `test/DBOS/Transact/WorkflowTest.hs:2328`); IOSim needs nothing but a sim-time tick, because the scheduler advances time only when no thread is runnable, so a parent parked in a delay cannot resume before a self-terminating child has run to completion, departure commit included. Neither half stages an engine effect; both observe one.

The rule is not theoretical. The case that motivated it (the `Tasks` group's "a task finishing before registration is not swept as aborted", `WorkflowTest.hs:2383`) slept a fixed 1 ms before sweeping and failed under load with `Exception: user error (dead tasks swept as aborted: 1)`: the spawned task had not departed yet, so the sweep *correctly* counted it and the assertion misread a live task as a miscount. The engine was right; the harness had assumed timing.

## The mark rule

Some cases the simulator genuinely cannot run. io-sim's `Fork` inserts the child into the thread map, appends its id to the **runqueue**, and reschedules the *parent* (`Control/Monad/IOSim/Internal.hs:473-490`), so nothing preempts a forked child before its parent's next commit — preemption-dependent cases (e.g. the fork→registration window in `spawnTracked`, `src/DBOS/Transact/Workflow.hs:796-833`) are unreachable in sim. Such cases run IO-only, with the reason in a comment above the case and in the lists below — case names stay plain so the IO↔Sim pair diffs name-for-name.

## The acceptance criterion

Deleting a case's sim half must remove **no engine function call** from the suite. If it removes one, that half was re-encoding, not mirroring.

## The failure mode this fixes (audit 2026-10-01)

- `test/DBOS/Transact/WorkflowTestSim.hs:1251-1255`: "nothing runs it here, so the test stages what the queue runner would do and records its completion directly" — the runner's concurrency is untested in sim.
- `test/DBOS/Transact/WorkflowTestSim.hs:1638-1651`: announcement shapes are hand-emitted through the say-carrier because "races and faults the sim does not stage" — the producing engine paths are untested in sim.
- `MemSystemDB` delegates about forty methods — queues, schedules, streams, messages, versions — to `MockSystemDB`'s canned answers (`test/DBOS/SystemDB/IOSim.hs:574-613`), so those subsystems have no sim concurrency coverage at all. **Closed 2026-10-01 (step 4):** queues, recovery, delayed transitions, message/event wakeups, schedules, and versions are implemented over real Mem state with Postgres semantics; streams stay delegated because Postgres itself holds them `undefined` (P7.4). `SystemDB.IOSimTest` pins the new semantics in a `MemSystemDB (stateful)` group.
- Where the path *is* shared it works, and is the model to copy: `ContextTest`'s `Fixture m` plus `scenario*` bodies (`test/DBOS/Transact/ContextTest.hs:156,165+`) and the `Tasks` group's in-process `runSimOrThrow` bodies (`test/DBOS/Transact/WorkflowTest.hs:2338-2404`).

## Escape hatch, not adopted

`exploreSimTrace`/`controlSimTrace` (`Control.Monad.IOSim`) can force schedules the cooperative scheduler will not take — at the cost of `ScheduleControl` step bookkeeping and a QuickCheck dependency, which the dependency rule gates. Recorded as a standing option for one specific race, never the default.

## Consequences

- New mirrors are reviewed against the acceptance criterion; existing ones are audited by `docs/dual-stack-concurrency-todo.md`.
- A case that cannot meet the criterion is either converted (drive the engine) or marked IO-only with its reason; the list stays current.
- Mirror structure, the `simTests` registry, the one-`-- $>`-toggle rule, and the printing contract (`runSimCase` + `printSimTrace` for mirror trees; event-free inline halves may use `runSimOrThrow`) stand as ADR-0016 set them.

IO-only cases (running list):

- "a spawn refused after abort fills its channel instead of hanging" — real preemption, not cooperation (`test/DBOS/Transact/WorkflowTest.hs:2368`).
- "a recovery run replays completed steps after a body interruption" — crash-and-relaunch recovery sweep; `MemSystemDB` delegates `reenqueueForRecovery` to the canned mock.
- "an unregistered workflow is skipped and the rest recover" — same recovery sweep.
- "a replayed parent reads the recorded outcome rather than waiting again" — recorded-await replay across two launches; needs the recovery sweep.
- "a foreign error is converted at the boundary" — the body performs real IO (the foreign charge call), which the simulator cannot run.
- "select reports the first workflow to settle, not the first started" — first-to-settle timing is wall-clock-bound.

Recorded 2026-10-01; extended 2026-10-01 (same-tree marking).

Sim-only cases (running list):

- "a budget cancels the workflow durably" — not yet mirrored on IO; needs wall-clock budget/body scaling.
- "workflow announcements carry their counts and ids" — typed trace assertions live only in sim (structural, per ADR-0016).

## Addendum: the recovery interlude that ran nothing (2026-10-01)

Two cases ("a parent starts a child under a derived id and replay adopts
it", "an assigned child id wins over the derived one") carried a live
crash-and-relaunch interlude whose stated premise was "the child is
recorded but nothing runs it here". Converting them showed the premise
was wrong: a child start detaches the child onto the executor and the
child runs inline on both stacks, so the interlude waited ~46 s per case
on a settle that was already terminal. The shared cases now assert what
their names say — the child runs under its derived/assigned id and a
replay adopts the recorded id — with no relaunch. The lesson matches the
observation rule's: state what the engine actually did, not what the
harness assumed it would not do.

## Addendum: Tasks runners split (2026-10-01)

The `Tasks` group no longer runs both halves in-process in the live tree.
Bodies and checks are still shared (one body per case, `checkNoMiscounts`
judging both), but IO leaves run in `tasksTests` (live tree, IO backend
verified by `cabal test`) and IOSim leaves in `tasksSimTests`
(`WorkflowTestSim`, Sim backend verified by the tasty watcher). The
acceptance criterion is unchanged — deleting a sim leaf still removes no
engine function call — and the printing contract is untouched (these
bodies emit no events, so the sim leaves run `runSimOrThrow` with nothing
to print).
