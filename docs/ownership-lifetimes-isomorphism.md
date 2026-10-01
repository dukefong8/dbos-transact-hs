# Ownership, lifetimes, and the Haskell contract — 2026-09-25

**Question.** What compile-time invariants does Rust enforce via
ownership and lifetimes in the oracle, and are our Haskell solutions
isomorphic to them?

**Verdict.** No — and the non-isomorphism sorts into three buckets:
**(1) subsumed** (the GC/runtime gives the outcome free, nothing to
re-encode); **(2) re-encoded** (same invariant, different mechanism —
this is where the porting work is); **(3) genuinely weaker or absent**
(documented gaps, each with its trigger condition). A fourth section
records where Haskell is strictly *stronger*. Evidence below is
primary-source (`crates/dbos/src/...`, two survey passes, key passages
re-read directly); Haskell citations are `src/DBOS/...` by symbol.

## 1. Subsumed: lifetimes as liveness

The bulk of the oracle's lifetime annotations buy memory safety for
borrows — stack options borrowed into futures (`StartOptions<'a>`,
`Enqueue<'a>`, `Message<'a>`, `RunOptions<'a>`), unified borrows
(`new_row` tying row+id+queue into one `'a`), doubled borrows
(`&'a [&'a str]` wait sets), elided borrows (`&self`, `&str`
readers), `Cow<'static, str>` error fields, `Outcome<'_>` payload
borrows. The GC subsumes all of it: our options are strict `Text`
values, waits take `[WorkflowId]`, errors carry `Text`. No
use-after-free is expressible on either side; the Haskell versions
are simply owned throughout. Isomorphic *outcome*, absent mechanism —
nothing to do.

Two encode-before-own patterns survive as *ordering* discipline, not
lifetimes: `OwnedStart`/`OwnedEnqueue` snapshot everything owned
before the `'static` spawn boundary, and `encode_one`/`place_set_event`
encode payloads *before* the step id is spent so an unencodable value
never burns a slot. We follow both orderings (row built before
`initWorkflow`; `validateEnqueue` before `nextStepId`). One asymmetry:
ours encodes the body *output* after the run (unavoidable — the value
doesn't exist yet), which is safe only because `ToJSON` is total;
there is no unencodable-output failure to place.

## 2. Re-encoded I: variance markers and the error channel

`PhantomData<fn() -> (R, E)>` (handle), `fn(P) -> (R, E)` (ref),
`fn() -> C` (pending start), `fn() -> E` (branches) all serve one
purpose: name types the value doesn't hold *without* inheriting
their `Send`/`Sync`/`Drop` bounds. Haskell has no such bounds to
dodge, so the need vanishes — but the *decoupling* remains the point:
our `WorkflowRef` is unparameterized (JSON checked once at
registration, then erased) and `PendingStep` is identity-only. If
parameters return with typed IO, the guidance is the oracle's own:
phantom params with no stored value and no constraints (a `Proxy`
shape), never `PhantomData`-with-bounds equivalents.

The generic error channel (`Error<E>`, `Result<T, E>`, uninhabited
`EngineOnly`, blanket `From<E>`, `map_application` carrying engine
variants untouched through nested `MaxStepRetriesExceeded`,
`PendingStart`'s two-channel `C`-defaults-to-`E` with `lift`
re-declaring before the await) is absent *with* the untyped channel —
correctly so. When typed IO lands, the requirements transfer
verbatim: one `mapApp`-only traversal (engine cases pass through,
cancels stay cancels), a phantom channel defaulting to the child
error, conversion of the pending value before race registration —
never of the persisted payload.

## 3. Re-encoded II: `must_use` is convention plus warnings

`PendingStep`, `PendingStart`, `PendingWorkflow`, `map_error`/`lift`,
and the pure readers all carry `#[must_use]`, because "built and
dropped, it has still spent the id" — an un-awaited start burns 1–2
parent counter positions while creating nothing. GHC has no such
attribute, and Haskell is quieter, not louder, here: a discarded
`WorkflowM` action never runs at all. But the *counter* semantics
transfer exactly — our `nextStepId` also advances on construction,
so building-then-dropping a start still shifts every later step.
Discipline, enforced by review: bind or explicitly discard pending
values; `-Wunused-do-bind` covers the pure getters. Our id-density
tests (`ContextTest` triple-allocates, `CheckpointTest` asserts
claimed ids) are the executable half of this rule.

## 4. Re-encoded III: `Send`/`Sync`/`Unpin` and the moved future

GHC checks no `Send`: the shared heap plus GC gives thread-safe
sharing, and `IORef`/`STM` give safe mutation. What transfers is the
*reasoning the bounds forced*. The sharpest case is verified
verbatim (`checkpoint.rs:333-359`): `PendingStep::poll` re-checks
placement on **every** poll, because the value is `Send + Unpin` and
"a call polled once where it belongs and then moved would go on
running under the context it captured" — being polled is "the only
moment anything can tell where this call now stands." For our future
Pending layer this is a design constraint, not a poll loop yet:
re-validate `checkHere` on every resumption/bind, never only at
construction; wrappers must delegate to the inner action, never
unwrap it (the oracle's `Pin::new` delegation rule:
`workflow.rs:1421-1423,1495-1497,1534-1535`); and note the Haskell
twist — closures capture the *shared counter `IORef`*, so moving
threads preserves counting while the ambient `Ask` context changes
underneath, which is exactly why the check must read the *driving*
site, not the capture site. `Unpin`-as-contract becomes
"migratable by construction."

## 5. Re-encoded IV: RAII guards become brackets and explicit closes

Every `Drop` in the oracle is a cleanup that must survive panic,
cancellation, and early return — precisely what Haskell spells
`bracket`/`finally` (with `mask` where cancellation races):

| Oracle guard | Haskell form (have / need) |
|---|---|
| `TaskGuard` dropped however the task ends (verified `workflow.rs:150-186`); guard-before-spawn + abort-handle-insert as one `spawn_tracked` so a racing shutdown never misses both | **Need:** single `spawnTracked` combinator (`bracket_ arrived departed` around `forkIO`, acquired *before* the fork); never raw `forkIO` at call sites |
| `abort_all`: close, drain, abort, wait-to-quiet, rows stay `PENDING` | **Need:** `throwTo` + quiescence wait on a liveness counter, no timeout. Ours today (stop flag + join supervisor) covers only the supervisor, not spawned bodies — the task-ownership gap |
| `Subscription` drop deregisters | **Have:** explicit `unsubscribe`, bracketed (documented equivalent) |
| `Slot` drop releases tallies (poison → leave counts alone) | **Need** with partitioned dequeue (`finally releaseSlot` in the worker, never the supervisor) |
| `close`: notifier-stop → await → pool-close → join listener | **Have**, same order (`releasePostgresSystemDB`: stop, take task, release) |
| `Drop` warns-only (`Inner`, backend safety-net) | **Need:** explicit `shutdown`/`close` remain the only real teardown; never block in a finalizer |
| `PanicLog` (log id on unwind, only when panicking) | **Need:** `onException`/`catch` wrapper logging the id on defect death — nothing observes our unwinding today |
| Detached child-start task (drop detaches, work continues; killed-before-commit → retry, after → interrupted-but-recoverable) | **Need** with task ownership; today we don't spawn at all (documented) |
| `JoinHandle` drop detaches, never aborts ("dropping a handle stops watching, not the workflow") | **Moot today** (handles hold no task); when local awaits land, pin the semantics by test — drop cancels the *wait*, shutdown owns the *worker* |

## 6. Re-encoded V: locks, atomics, task-locals

| Oracle | Haskell | Verdict |
|---|---|---|
| `Tasks` std-`Mutex` triple, never across await | Future `MVar`/`TVar` triple, copy out before blocking | Same rule, TODO with ownership |
| Executor `RwLock` (reads) + tokio-`Mutex` lifecycle (across awaits) | `MVar` slot + `MVar ()` lifecycle held across launch/shutdown | Isomorphic (ours even matches the two-lock split) |
| Registry `RwLock`, freeze+snapshot under one section | `modifyMVar` freeze+copy | Isomorphic; STM would also serve |
| `Running`/`Queues` std-`Mutex`, never across DB awaits | Future `TVar (Map …)`, short atomic modifies | Same rule |
| Notify waiters map, sync+infallible subscribe-before-look | STM `TVar` registry, same ordering | Isomorphic |
| Notifier `pending` mutex, wake-only-opener | `TVar` map + wake-on-empty→nonempty | Isomorphic |
| Task/notifier `Mutex<Option<JoinHandle>>`, take-then-await outside | `MVar (Maybe (Async ()))`, same take-clear-join order | Isomorphic |
| `AtomicI32`/`AtomicU64` `Relaxed` (order nothing, dedupe only) | `atomicModifyIORef'` / `Data.Unique` (stronger ordering, same contract) | Stronger, harmless |
| `tokio::task_local!` + non-crossing spawn | Bluefin `Ask` + fork-snapshot pattern (tested) | Isomorphic, tested |
| `assert_send` lifecycle compile test | Proposed: fork-lifecycle-to-another-thread-and-await test | Re-encode as test |
| Hint flags (`delivering`/`pushing`) `Relaxed` | `TVar`/`IORef Bool`; correctness never depends on prompt visibility | Same rule |

## 7. Genuinely weaker (documented, with triggers)

- **`is_same_execution` pointer.** `Arc::ptr_eq` distinguishes a
  second instance and a recovery re-run sharing one workflow id; we
  compare id text. Trigger: two executors serving one id in one
  process, or a held id across a re-execution. Needs the executor
  pointer below the seam (task-ownership work) to close.
- **`#[non_exhaustive]` forward-compat.** Adding a Rust error variant
  is source-compatible for wildcard matchers; adding our constructor
  breaks exhaustive matches. Deliberate tradeoff for totality; keep
  closed ADTs, never promise exhaustiveness.
- **`WorkflowStatus::parse` shape.** Oracle returns `Option` (unknown
  → `None`); ours returns `Either` with a decode error (total-function
  rule). Same rejections, different channel.
- **Unwind observability.** No `PanicLog` equivalent; a defect dying
  in a forked body leaves no id-tagged trace (§5 row).
- **Handle status absence.** `Maybe` instead of `WorkflowNotFound` —
  deliberate (a single read has nothing to wait for), not a gap.

## 8. Strictly stronger (keep)

- **Pure placement.** Our `checkHere`/`placementAt` test without a
  database; the oracle's placement tests launch one.
- **Immutable snapshot.** `Snapshot` is frozen by construction; the
  oracle enforces freeze-discipline with a lock flag.
- **Total decoders.** `Either`, never `Option`-and-panic.
- **No `Arc`-clone accounting.** No `with_current`-style hot-path
  optimization debt: nothing to contend on.

**Bottom line for the next layers.** The Pending async layer must:
re-validate on every resumption; delegate, never unwrap; advance
counters on construction even when discarded; bracket arrival and
departure around every fork through one combinator; order close as
flush → pool → join; convert error channels on values, never on
persisted rows; and preserve — not repair — the two-commit select
window. Everything else the GC already proved.

## 9. Bluefin 0.10 verdict (asked 2026-09-25)

**Versions.** Resolved: `bluefin-0.10.0.0` + `bluefin-internal-0.10.1.0`
(`dist-newstyle/cache/plan.json`). Latest on Hackage is
`bluefin-0.10.1.0` (released 2026-09-25), whose delta is
`DslBuilderEff.runDslBuilderEffMappedArgs` and
`Capability.ThrowCatch` — nothing lifetime/scope. Cabal bound tightened
`>=0.9 && <0.11` → `>=0.10 && <0.11` to pin the canonical capability
API (`Bluefin.Capability.Ask`; 0.10 made `Ask` canonical and `Reader` a
synonym). No env bump needed for the gaps below.

**Already closed by Bluefin (evidence in the nvim side-by-side).**
- *Ambient-context escape.* Oracle: `Ctx::scope` is runtime-scoped
  ("restored on every poll and removed on every yield",
  `context.rs:310-311`). Ours: `WorkflowM` is
  `forall e1 e2. Ask WorkflowRuntime e2 -> IOE e1 -> Eff (e2 :& e1) a`
  — the `Ask` handle is rank-2 and cannot leave `runAsk`'s scope.
  Compile-time where the oracle is runtime.
- *Fork isolation.* Oracle: runtime (spawned task gets an empty
  task-local map; pinned by test). Ours: a forked thread has no `Ask`
  handle unless explicitly passed — the tested fork-snapshot pattern.
- *Attempt rebinding.* `Ask.local` = `Ctx::in_step_scope`'s rebind,
  with the same "nothing to unset" property by construction.
- *No escape hatch.* No `MonadUnliftIO` instance, so a scoped action
  cannot escape — the design property the oracle approximates with
  prose.

**Not closeable by any Bluefin version.**
- *Per-poll placement (`checkHere`).* Runtime in both languages. A
  pending call is a plain data value; and `WorkflowM`'s effect index is
  quantified inside the newtype (`forall e1 e2`), so a pending value
  cannot carry the scope witness in its type. The oracle itself keeps
  executor presence runtime (`Ctx::current() -> Option`,
  `executor() -> Option<&Arc<Executor>>`), so our `Maybe
  PostgresSystemDB` is faithful, not a gap.
- *Task ownership.* No `Bluefin.Async` module exists (module list
  verified); structured concurrency still needs our `spawnTracked`
  (`bracket_ arrived departed` around `forkIO`), per §5.
- *`must_use` counter semantics, pointer identity (`is_same_execution`),
  `non_exhaustive`, unwind observability.* Effect scoping has no say in
  any of them; `Bluefin.Exception.GeneralBracket`/`finally` can
  structure cleanup, not add forward-compat or handle equality.

**Verdict.** Bluefin 0.10 closes the ambient-scope class of invariants
— the strongest lifetime-related ones — and is already doing so; the
remaining gaps are either runtime checks in the oracle itself or need
a structured-concurrency combinator we must write. Updating past
0.10.0.0 buys nothing for them.

**2026-09-30 addendum.** ADR-0012 has since removed Bluefin and
`WorkflowM` altogether: the port threads `Connection`/`Ctx` explicitly,
so the ambient-escape class this section credits to `Ask` is closed by
construction (there is no ambient context at all). What Bluefin was
carrying that survives as a *future* question is scope branding —
cross-execution and cross-instance mixing — and the decided direction
is a plain phantom brand, not the dependency. See §11.

## 10. Workflow/step/child id & handle state matrix (2026-09-30)

One row per oracle mechanism that restricts what callers can build,
what the port has today, and which plan item closes the row. Oracle
citations are `crates/dbos/src/...`; port citations `src/DBOS/...` by
symbol or line. Statuses: **subsumed** (GC/runtime gives the outcome),
**closed-by-#n** (the plan item in `.lavish/rust-port-plan.html`),
**open** (no closing item yet), **N/A** (oracle is runtime too, or the
port is deliberately stronger).

### 10.1 What the oracle makes unrepresentable

| # | Mechanism | Illegal state prevented | Enforcement |
|---|---|---|---|
| R1 | `WorkflowRef<P, R, E>` (`registry.rs:204`, phantom `fn(P) -> (R, E)`, hand-written `Clone`) | wrong input/output/error types at a call site | compile-time |
| R2 | `run`/`start`/`run_with` take `&self` (`workflow.rs:823,936,840`) | dropping the ref while a run it spawned is outstanding | compile-time (borrow) |
| R3 | `PendingStep<'a, T, E>` (`checkpoint.rs:136`): id claimed at construction, `placement` re-checked every poll | a built call polled where it does not belong (`StepBuiltElsewhere`); moving it after a first poll | runtime, forced by private constructors |
| R4 | `PendingWorkflow`/`PendingRun` newtypes (`workflow.rs:1426,1516`) + `Branches::push(&PendingStep)` only (`select.rs:165`) | racing a whole run or a start | compile-time (signature) |
| R5 | `PendingStart<R, E, C = E>` (`workflow.rs:1446`, second `PhantomData<fn() -> C>`) | confusing the stored error channel with the reporting channel | compile-time |
| R6 | `WorkflowHandle<R, E>` (`handle.rs:29`), private `Provenance` (`Local(JoinHandle)` vs `Polling{fail_if_missing}`), `result(self)` consumes | double-await; polling a local task; forging provenance | compile-time (consume + privacy) |
| R7 | `StepPlacement` (`checkpoint.rs:421`) and `ChildResultPlacement` (`handle.rs:325`) private, built only by `of`/`taken`; the await re-checks the stored `child_workflow_id` | forged placements; adopting another workflow's outcome as this await's | runtime construction path |
| R8 | `#[must_use]` on every pending type | discarding a durable call | lint only (documented as spend-the-id) |
| R9 | `Ctx::current()`/`Ctx::scope`; `Arc::ptr_eq` connection compare; `Owner` split (`checkpoint.rs` `of`) | cross-instance step writes (`WrongInstance`) vs client degradation (`ClientConnection`); unlaunched use (`NotLaunched`) | runtime (the oracle itself is runtime here) |
| R10 | `parent_workflow_id`/`child_workflow_id` columns + detached child task | — (data, not types: child work outlives parent interest) | runtime |

### 10.2 The port today

| Rust | Haskell today | Status |
|---|---|---|
| R1 | `WorkflowRef m` erased (`Registry.hs:92`): JSON checked once at registration, then strings | `E` closes with the typed-error epic (#13–#16); `P`/`R` open; instance identity decided (#12) |
| R2 | refs are immutable values; `runWorkflowRef`/`startWorkflowRef` take them by value (`Workflow.hs:573,522`) | **subsumed** (§1) |
| R3 | `PendingStep m a` now carries its deferred run (`Checkpoint.hs`); `placeCall` claims at build and `pendingWorkflowStep`/`pendingAwait`/`drive*` split build from run; `checkHere` re-validates at drive | **landed 2026-09-30** (select core); per-poll re-check stays runtime (§9) |
| R4/R5 | `selectStep` takes `[SelectArm]` only — a start or a run can never be raced — and the pending layer backs it | **core landed 2026-09-30; macro dropped by decision** (typed core over syntax; the `<2` refusal is runtime and side-effect-free; guards and `else` arms are not expressible as arms, so there is nothing to refuse) |
| R6 | `WorkflowHandle m` carries both provenances (`Polling`/`Local`) and `awaitChild` records while `handleResult` is the ctx-less face | **Local landed 2026-09-30**: task channel + `LocalTaskOutcome`, `spawnLocal` seam; run path, child starts and top-level starts hand back `Local`; joins/enqueues/retrievals poll. E-typing and one-shot await open |
| R7 | `StepPlacement(..)` exported (`Transact.hs:144-149`); comparisons are workflow id + marker text; the await's child-id check and the child start's instance check are both in place | **landed 2026-09-30** (await check with #7; instance identity with ADR-0018) |
| R8 | no attribute; synchronous actions dropped silently | discipline + id-density tests (docs §3) |
| R9 | explicit `Ctx` (ADR-0012): ambient escape impossible (**stronger**); instance identity is runtime, per connection (ADR-0018) | brand direction §11 would lift it to types; runtime backstops stay |
| R10 | start claims the parent step id, checks the recorded launch, records via `InitWorkflowCaller` (`Workflow.hs:602-680`); fresh starts detach onto executor `Tasks` | launch half faithful; await half **closed-by-#7** |

## 11. Future enhancement: phantom brands (decided 2026-09-30)

- `Ctx s m`, `WorkflowRef s m`, `WorkflowHandle s m` with `s` introduced
  rank-2 by `withExecution`/`withInstance` at the external seam. A
  context or reference from another execution or instance then becomes a
  type error: R9's cross-instance half and the cross-execution half of
  `checkHere` move from runtime to compile time, with the existing
  runtime checks kept as backstops for text-identical ids.
- **Not Bluefin-the-dependency.** ADR-0012 removed it and §9's verdict
  stands: it closes ambient-scope escape (already structurally closed)
  and cannot close per-poll placement, task ownership (`spawnTracked`
  is ours), or `must_use`. If the seam ever returns to Bluefin, the
  brand can ride its handle parameter; the plain phantom works today.
- Handles: `Local` provenance landed 2026-09-30 (`Provenance` has
  both arms; the run path, `startChildWorkflow` and `startWorkflowRef`
  read the spawned task's channel directly, while joins, enqueues and
  retrievals poll). The one-shot await remains the `LinearTypes`
  candidate: Haskell handles stay reusable, so double-await is still
  representable — the deviation Rust's `result(self)` closes.
- Keep runtime whatever the oracle keeps runtime: per-poll placement,
  `NotLaunched`, `InsideStep`, handle-status absence.
- Not planned: `Tasks s m` scope brands — task ownership semantics are
  already right (detached starts, `abortAll` on shutdown).

## 12. Dated delta (2026-09-30)

- Moved since the 2026-09-25 draft: `Tasks`/`spawnTracked`/`abortAll`
  exist (`Workflow.hs:40-42`), fresh child starts detach, and the
  SystemDB seam provides `checkChildResult`/`recordChildResult`
  (`SystemDB.hs:137-138`, `:141`) for the recorded-await work.
- Landed after the map was drawn: `handle.rs` is complete.
  `Provenance` gains `Local` — a task channel filled while the task
  unwinds (`LocalTaskOutcome`: value / cancellation / panic), the
  spawner seam reshaped to `spawnLocal`; the run path,
  `startChildWorkflow` and `startWorkflowRef` hand back local handles,
  while joins, enqueues and retrievals stay polling.
  `handleResult`/`awaitChild` share `settleOutcome` over both
  provenances, with `ThreadKilled` → `Interrupted` and panics resumed
  into the waiter, as the oracle does. Pinned by the live+sim "a fresh
  start is local and a join polls"; full suite 611/611 (parallel).
- Landed after the map was drawn: the select core (`DBOS.Transact.Select`)
  + the pending layer (`PendingStep` carries its run; `placeCall`/
  `pendingWorkflowStep`/`pendingAwait`/`drive*`), with `selectStep`
  racing arms in source order and cancelling losers; a control-ended step
  now records no row (fidelity fix found by the control-winner case). The
  oracle's `select_step!` macro is deliberately not ported: the semantic
  fences (only pending steps race; position recorded, never the value) are
  types and core logic already, and the macro's remaining checks are
  syntax refusals with no Haskell counterpart — the `<2` case stays a
  runtime refusal that claims no id.
- Landed after the map was drawn: instance identity (ADR-0018) — the
  child start's `WrongInstance`/`NotLaunched` refusals, comparing the
  reference's bound connection against the running context.
- Landed after the map was drawn: the typed-error core (ADR-0019) —
  `ErrorOf e` with `Application e` (Rust's `Error::Application(E)`), the
  uninhabited `EngineOnly`, `Failure` (recorded envelope or control),
  and the tagged JSON round-trip (`encodeErrorText`/`decodeErrorText`),
  with the temporary `type Error = ErrorOf EngineOnly` alias standing in
  until the lift/foreign stage. Threaded through `Step`, `Checkpoint`,
  `Registry` (`WorkflowRef m e`), `Handle` (`WorkflowHandle m e`), and
  `Workflow`/`Instance`/`Client`; recorded step, await and workflow
  failures are now the serialized envelope (the DB holds
  `{"AwaitedWorkflowCancelled":{...}}`), and a polling read decodes the
  failure back into the caller's channel — the oracle's serde
  round-trip. A locally-run child cancelled by its own deadline reads as
  an awaited cancellation (the row's status), not as control. Stage 4 then
  landed lift (`children.rs:1894`) and foreign error (`workflows.rs:222`) on
  the typed API, deleted the migration alias, and renamed `ErrorOf` to
  `Error` (engine sites spell `Error EngineOnly`; the start's reported
  channel is a free type variable standing in for `PendingStart::lift`).
  Live 631/631, sim 40/40, oracle children+workflows green.
- Decisions folded into the plan: children.rs crosswalk + closure order;
  instance identity + `WrongInstance` (#12); full typed-error
  parameterization with serialized recorded payloads (#13–#15); the
  `awaitChild`/`handleResult` split (#7); select core + TH (#14);
  `-j1` gate policy; sim mirror per case; this document's brand
  direction, annotated now and re-annotated once #7–#15 land.
