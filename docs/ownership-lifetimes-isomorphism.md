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
