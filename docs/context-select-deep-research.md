# Context × Select: what, why, where, how — 2026-09-25

The two hardest modules in the oracle, interrogated as what / why /
where / how. Citations are `crates/dbos/src/...` with lines, verified
against the source (the two load-bearing claims — `Branches::push`
taking `&PendingStep` only, and the select id following the branches —
were re-read directly). Part III shows how the two modules interlock;
Part IV maps every finding onto the Haskell port: done, hard, decided.

## Part I — Context (`context.rs`, 820 lines)

### 1. The ambient task-local

- **What.** Durability metadata — which workflow this is, which step
  comes next — travels in `tokio::task_local! { static CURRENT: Ctx; }`
  (`context.rs:19-22`), "set while a workflow body runs, and read by
  everything the body calls." A workflow stays an ordinary `async fn`
  taking its own arguments and nothing else (`context.rs:3-5`).
- **Why.** Zero-arg durable signatures, and parity: "Python's
  `ContextVar`, TypeScript's `AsyncLocalStorage` and Java's
  thread-locals are the same mechanism" (`context.rs:5-6`). Go's
  parameter-threading was rejected on evidence: "its own starter
  application is the argument against copying it" (`context.rs:7-8`).
- **Where.** Set/unset by `Ctx::scope` (`context.rs:317`): "restored on
  every poll and removed on every yield, so it is present for the whole
  body including across `.await`" (`context.rs:310-311`).
- **How — and the spawn rule.** It does *not* cross `tokio::spawn`:
  "a spawned task is a new task with its own empty task-local map"
  (`context.rs:311-312`), because "silently adopting the parent's step
  counter would let two tasks allocate the same step id"
  (`context.rs:312-314`). Pinned as behavior, not left to discovery,
  by `the_context_does_not_cross_a_spawn` (`context.rs:787-803`).

### 2. `WorkflowState`: what outlives any one call

- **What.** `{workflow_id, deadline, next_step_id: AtomicI32}` —
  "the parts of a workflow that outlive any one call within it"
  (`context.rs:188-213`).
- **Why shared by clones.** "Cloning it does not make a second
  workflow — the clone shares the same step counter, which is the
  point" (`context.rs:26-28`); 8 threads × 100 ids come out distinct
  (`context.rs:598-620`). Ids are zero-based — "the first step in a
  workflow is step 0" (`context.rs:203`) — matching Go/TypeScript/Java
  ("a number Conductor renders and a user passes to `fork`",
  `context.rs:205-212`).
- **Where the deadline lives.** On the shared state, never on the
  per-scope `Ctx`: "anything held on the `Ctx` itself would be
  silently dropped at every step boundary" (`context.rs:672-674`).
- **How deadlines compose.** The deadline is an *instant the database
  holds*, "not the timeout its caller offered"
  (`context.rs:193-196`): a recovered workflow keeps its remainder,
  and an inheriting child inherits the remainder — "with no signal
  passing between them" (`context.rs:198-201`). Our port already does
  this (`resolveTimeoutDeadline`, row-stored deadline riding the
  execution).

### 3. `StepScope`: marker, status, cancellation

- **What.** "What a context inside a step body knows about that body"
  (`context.rs:54`): which body (marker, opaque, never persisted),
  what it may report (status), and the token that fires when the
  attempt is abandoned (`context.rs:64-88`).
- **Why one `Option` for marker+status.** "A context has both or
  neither… that invariant is worth more as a type than as a
  convention" (`context.rs:56-59`). Why the token is per-attempt: "a
  token cancelled by attempt one would arrive already-cancelled at
  attempt two" (`context.rs:77-79`). Why non-`Option`: "a step body
  always has one" (`context.rs:81-83`). Why receiving-only: "Nothing
  a body does with this cancels anything" (`context.rs:85-88`).
- **Where.** Bound by `in_step_scope`, which rebuilds the `Ctx`
  (executor + workflow clones, fresh scope) and re-scopes the
  task-local (`context.rs:358-376`). Our `withAttempt` is this shape.
- **How no unset guard is needed.** "The marker lives on the `Ctx`
  bound for this body alone, so it goes out of scope with the body
  however the body ends… A flag on the shared state would need a
  guard to clear it, and a guard could not make it correct"
  (`context.rs:353-357`). Sibling isolation is pinned by dedicated
  tests (`context.rs:711-784`).

### 4. Markers vs ids

- **What.** `StepMarker(u64)` is "which step body," never "which
  position": "a step id is an ordinal position, restarts from zero on
  every replay… where a marker is opaque, never persisted, and never
  compared across runs" (`context.rs:166-169`). One per *attempt*:
  a retry enters a new scope "with a fresh marker and the *same* id"
  (`context.rs:172-173`).
- **Why process-wide + `Relaxed`.** "Only ever compared for equality
  and a value distinct across the process is distinct within any one
  workflow… the counter orders nothing" (`context.rs:183-185`).
  Exactly our `Data.Unique` reading — same contract, no atomics.
- **Where it matters.** The leaf rule ("a step inside a step is a
  plain call", `context.rs:40-43`) and the cross-poll refusal: "Both
  places have the same workflow id, so comparing workflow identity
  cannot tell them apart" (`context.rs:343-346`). Our `checkHere`
  compares markers for the same reason.

### 5. `StepStatus`: attempt 1 of 1, honestly

- **What.** Zero-based `step_id`, one-based `current_attempt`,
  ceiling `max_attempts` (`context.rs:107-149`). The split is
  deliberate: ids address checkpoints (zero-based, like three of four
  SDKs); attempts match TypeScript's 1-based `attemptNum`
  (`context.rs:111-116`). `max_attempts` is always a number — "a plain
  step is honestly attempt 1 of 1" (`context.rs:120-124`).
- **Why read-only.** "Behind accessors… so there is no way to write
  one" — not against changing policy (impossible: the engine rebuilds
  its copy per attempt) but against "a settable `max_attempts` that
  silently changed nothing" (`context.rs:100-105`).
- **How retries work.** Same id, fresh marker, `current_attempt`
  moves; `max_attempts` is "a ceiling, not a promise" — a declined
  retry ends the step early (`context.rs:126-130`). Our
  `firstStepStatus`/`nextAttempt` is this.

### 6. The four free fns: answers with `None` conditions

`workflow_id` answers inside steps too (a step belongs to its
workflow) and is `Option` so helpers run inside and outside workflows
(`context.rs:393-405`). `step_id` is `None` in the workflow proper —
"between two steps a workflow is inside neither" — and reports the
*enclosing* id from a nested step, because that is "the only answer
with a checkpoint row behind it" (`context.rs:423-440`). `step_status`
adds which-attempt for last-attempt behavior, matched-not-unwrapped
so bodies stay testable outside workflows (`context.rs:456-480`).
`cancellation_token` is a bare token (never `Option`): outside a step
it never fires, "so a body that is also called outside a workflow
needs no second path" (`context.rs:532-534`) — but it only *receives*:
dropping the future stops ordinary async code, which is "the half
TypeScript cannot do"; the token covers `spawn_blocking` and foreign
cancel handles (`context.rs:513-520`). Firing is for timeouts,
preemptible-step preemption, and abandoned attempts — never for a
reached outcome (`context.rs:495-530`).

### 7. `is_same_execution`: identity, not id equality

- **What.** One pointer comparison: `Arc::ptr_eq` on the shared
  `WorkflowState` (`context.rs:296-301`).
- **Why strings lie.** "A workflow id names a row"; two sharers of one
  id share no counter: a second `DBOS` in the process (the
  `WrongInstance` case) and a recovery re-run whose counter restarted
  from zero (`context.rs:286-294}). **Where.** Wherever a held step id
  is honored — which is why our port, lacking the pointer, compares
  workflow-id text and documents the gap (audit §5).

### 8. `with_current` vs `current`: the contention argument

`current()` clones (three atomic refcount round-trips on words every
concurrent step shares); `with_current` borrows through one `FnOnce`
serving both arms (`context.rs:251-268`). The second accessor exists
because "a step's `poll` asks where it stands on every poll"
(`context.rs:258-260`) — and all four public reads take the borrow
path. Our port has no `Arc`s and no poll loop yet, so one reader
suffices; if per-poll checks ever contend, this is the precedent.

## Part II — Select (`select.rs`, macros, `wait.rs` arms)

### 1. The hazard: deterministic ids are not enough

- **What.** A raced pair of durable calls makes a *choice*, and "a
  choice a workflow makes has to be recorded" (`select.rs:1-5`).
- **Why ids don't save it.** "A step takes its id when it is built
  rather than at its first poll, [so] the ids under a `select!` are
  already deterministic — so it *looks* fixed. It is still not
  durable: nothing records which branch won"
  (`select.rs:9-12`; also `checkpoint.rs:91-95,112-115`,
  `lib.rs:126-131`). A `join!` needs nothing — "an all-wait decides
  nothing" (`select.rs:5-7`) — but a race that re-races on replay "may
  see the other branch answer first and take a path the first
  execution never took" (`select.rs:3-5`). The failure is silent,
  "which is why the macro over this refuses to be spelled like
  tokio's" (`select.rs:12-13`). A `timeout` is the same race against
  an unreplayed clock (`checkpoint.rs:117-118`).

### 2. `Branches`: the type that refuses

- **What.** Per branch: name + claimed id (`identities:
  Vec<(String, Option<i32>)>`), plus the unified error type as
  `PhantomData<fn() -> E>` — "this owns no error and must not inherit
  a `Send`/`Sync` restriction from one" (`select.rs:121-138`). Names
  are read while branches live, because losers drop at decision time
  and a stale-winner report must name a gone branch
  (`select.rs:130-132`).
- **Why `push` takes `&PendingStep` only.** "A `PendingStart` and a
  `PendingWorkflow` is **not** a `PendingStep`, so `Branches::push`
  cannot be handed one" (`select.rs:79-85`, verified at
  `select.rs:166`: `push<T>(&mut self, branch: &PendingStep<'_, T,
  E>)`): a start *creates* a workflow, and "whether a child exists at
  all would follow the timing of some other branch"
  (`select.rs:83-85`; `checkpoint.rs:57-61`,
  `workflow.rs:1400-1406`). Start outside; race what observes.
- **Where error agreement happens.** At `push`, at the build, before
  any await (`select.rs:123-128`): "`T` is free per call while `E` is
  fixed by the set." Arm-body `map_err` is too late — convert the
  call while still a value (`map_error`/`lift`,
  `checkpoint.rs:242-256,301-314`); what is recorded stays the error
  the call actually made (`checkpoint.rs:266-270`).

### 3. `check_select`: the select's id comes last

- **What.** `Racing` forces both arms: `Replay(usize)` — poll only
  the winner, which replays from its own step row — or
  `Fresh(Recording)`, where `Recording` links check to record by type
  (placement + pre-wait `started_at`, so durations cover waiting)
  (`select.rs:94-119`).
- **Where the select's id goes.** After every branch is built —
  "that ordering is the contract rather than a convenience"
  (verified at `select.rs:200-210`): branches take ids at
  construction, the select's follows via `StepPlacement::here()`
  (`select.rs:222-225`). First would shift every branch one slot past
  the replay's expectation. Inside a step / outside a workflow the
  placement records nothing and the race degrades to plain
  (`select.rs:217-221,283-285`).
- **How replay works.** Decode the winner index; a changed arity is
  `UnexpectedStep` ("a workflow that changed how many branches it
  races has changed what this position of its code means",
  `select.rs:233-257`). Only the winner is polled — losers were still
  *built* (ids stay spent) but never run (`dbos-macros/lib.rs:272-290;
  select.rs:493-528`).

### 4. `record_select`: position, not outcome — and the window

- **What.** "The position, not the branch's result… writing it again
  here would keep one outcome in two places" (`select.rs:266-273`).
  A decided race leaves *two rows in two commits*: branch outcome
  under its id, winner index under `DBOS.selectStep`
  (`select.rs:17-21`).
- **Where the window is.** Between the two commits: "a select with no
  row is a select that has not run, so a recovery races again"
  (`select.rs:20-21`). The select's row precedes any arm body, so an
  interrupted window ran no arm — but the recovery may take the other
  arm: "a charge that succeeded and recorded, raced against a
  timeout, and a second race that takes the timeout arm while the
  charge's row says the money moved" (`select.rs:23-28`). "There is
  no timing argument that makes this rare" — a losing sleep's wake
  time is long past at recovery, so it replays instantly and the race
  is a coin toss (`select.rs:30-33`).
- **Why both repairs were refused.** Infer-the-winner fails on the
  losing sleep ("a row does not mean a branch finished… Making this
  sound needs a bit on the call itself", `select.rs:40-47`); one
  transaction fails for lack of a single write (`record_step` vs
  `record_child_result` vs `record_sleep` vs in-creation child start,
  `select.rs:48-51`). "So the window stands, with its shape written
  down… the losing sleep is the test that says whether it worked"
  (`select.rs:53-54`). What *is* guaranteed: no step runs twice —
  the recorded branch replays (`select.rs:533-540,584-589`).

### 5. `control_error`: signals are not decisions

Takes a control signal out of the winner slot (`select.rs:299-317`,
via `failure.control()`): steps record nothing on cancellation /
interruption / DB failure, "so the workflow stays pending and is
recovered — and the race it won has to do the same"
(`select.rs:301-306`). Recording it would "pin every recovery to a
branch that never ran its body" — and signals arrive *fast*, "one
failed round trip ahead of any branch doing real work"
(`select.rs:304-306`). Application errors stay: the branch recorded
them, so the win is faithful (`select.rs:308-309`). Checked on fresh
and replay paths alike.

### 6. `select_step!`: why a proc macro

Per call site it writes what must differ per site: a local per
branch, a source-order poll loop, one arm (`select.rs:58-63`). A
function would need a sum type per arity (no anonymous sums); a
declarative macro "can neither invent an identifier nor count"
(`select.rs:65-70`; `dbos-macros/lib.rs:4-20`) — the proc macro
writes `__dbos_branch0…N` without arity limit (tested to 40
branches). Fixed source order, never tokio's randomized fairness:
"fairness is exactly the property a replay cannot reproduce"
(`dbos-macros/lib.rs:307-316`). Branches must be exactly one
*expression* (a block would defer builds to first poll, breaking
id order), one call each; guards/`else`/`biased`/`<2`/refutable
patterns are compile-time refusals (`lib.rs:212-241`). What may
race: any observing `PendingStep` (step, handle result, sleep,
event, wait, checkpointed management call); never a start/run
(§2). Expansion names `::dbos` absolutely (`extern crate self as
dbos`), so dependents may not rename the crate; the `__private`
shim (`Branches`, `Racing`, `Recording`, `check_select`,
`control_error`, `record_select`) is `pub`-by-necessity, documented
unstable (`lib.rs:24-26,148-156,261-281`).

### 7. `select_workflow!` / `join_workflows!`: handles, cheaper

Where every branch is a workflow's outcome, handles beat steps: one
wait settles the set — "one query per interval whatever N is" —
where N raced awaits are N pollers (`wait.rs:1-8,144-155,224-227`).
`select_workflow` checkpoints the winner's *id* under
`DBOS.selectWorkflow` (a choice); `join_workflows` records nothing
(an all-wait decides nothing — no constant exists for it)
(`wait.rs:84-110`; `sysdb/types.rs:2347-2353`). Only the winner is
awaited ("what keeps the step ids stable", `wait.rs:311-321`);
join results come back sequentially in source order so each
`DBOS.getResult` lands on its replay id (`wait.rs:443-449`).
Replay checks the recorded winner is still in the set
(`UnexpectedStep` otherwise); a replayed join re-asks rather than
returning anything recorded (`wait.rs:118-123,255-264`). Macros take
handle *variables* (an expression would evaluate twice — starting a
child twice, `wait.rs:353-361`) with an optional instance prefix
(macros have no ambient context, `wait.rs:163-166`); empty
`select_workflow` is refused *and recorded*, empty join returns at
once (`wait.rs:679-689,784-788`).

### 8. The prohibitions and their alternatives

| Forbidden (in a workflow body only) | Why | Reach for |
|---|---|---|
| `tokio::select!` over durable calls | Unrecorded decision; replay may diverge (§1) | `select_step!` |
| `PendingStart`/`PendingRun` as branches | Existence would follow poll timing | Start outside; race `handle.result()` |
| `timeout` around a durable call | Race against an unreplayed clock | Recorded deadlines: `get_event`/`recv` timeouts, `StartOptions::timeout`, inherited deadline |
| Any other race | — | Inside a step (its checkpoint covers how the answer was reached) |

Outside workflows and inside steps, all of tokio is available
(`checkpoint.rs:120-129`) — the ban is positional, enforced by
`check_here` per poll, except over `tokio::select!` itself, where
"the prose is what stands between a body and the mistake"
(`checkpoint.rs:62-65`).

## Part III — How context and select interlock

Three handoffs, each in one direction:

1. **Build: context mints, select orders.** Every branch takes its id
   from the ambient counter at construction; `check_select` takes the
   select's *after* all branches (`select.rs:200-210`). Our
   `nextStepId` + future `checkSelect` must preserve exactly this
   order — select-first shifts every branch past the replay.
2. **Poll: placement checks every time.** `PendingStep::poll`
   re-asks `check_here` on each poll because a `Send + Unpin` value
   polled once where it belongs can be moved
   (`checkpoint.rs:333-360` relayed via audit). Our `checkHere` is
   the decision without the async wrapper yet.
3. **Record: two rows, two commits, position-only.** Branch outcome
   under its name, winner index under `DBOS.selectStep`
   (`selectStepStepName` const already in `SystemDB.Types`) — never
   the result twice. A Haskell port must preserve the crash window
   *semantics* (recovery re-races; recorded branches replay; no step
   runs twice), not "fix" the window.

And the negative space both modules share: `join!` needs nothing
anywhere (all-waits decide nothing — our `waitForWorkflows` is
already this), while every *choice* — race winner, first-wait winner
— is a checkpoint.

## Part IV — Port implications (what maps, what is hard, what is decided)

**Maps cleanly (mostly done).** Markers → `Data.Unique`
(process-wide equality-only, same contract); scope discipline →
`withAttempt` (rebind, dies with the body); id/counter →
per-execution `IORef`; free fns → `current*` readers (same `None`
conditions); token → per-attempt `TVar Bool` (receiving end);
deadline-as-instant riding row + context → `resolveTimeoutDeadline`;
`check_here` table → `checkHere` (now marker-compared);
step-name consts → present, including `selectStepStepName` /
`selectStepName` ahead of the engine.

**Hard — the select port design space.**
- *Branch heterogeneity.* Rust leans on `PendingStep<T, E>` as the
  uniform branch type with per-branch `T` and unified `E` (fixed at
  `push`). Our bodies are `WorkflowM (Either Error a)` with one
  error type already — the agreement problem is smaller, but a
  branch set mixing steps, sleeps, events, waits, and handle-awaits
  still needs one combinator type; `PendingStep` (identity-only
  today) is the seed.
- *The macro.* A declarative macro cannot invent identifiers or
  count — the oracle's own argument for going procedural. Haskell
  options: Template Haskell (same power, same build-graph cost
  argument), or a combinator library (numbered slots via type-level
  Nats or CPS) that keeps positions stable by construction. The
  arity tests (10, 40 branches) are the acceptance bar either way.
- *Error agreement without `push`.* Our untyped channel dodges
  `map_error`/`lift` timing entirely — until typed IO lands, then
  this paragraph reactivates.
- *Async shape.* Everything here is `Future`-poll machinery
  (per-poll checks, biased selection, drop-before-record). Our
  `WorkflowM` is synchronous; a select port needs the Pending async
  layer first (task ownership work), with per-poll `checkHere` from
  day one — not as an optimization later.
- *The window.* Preserve it: branch row, then select row, select row
  before arm bodies; recovery re-races; the losing-sleep test
  (`select.rs:601-632` shape) is the acceptance test. Do not
  "repair" it per §II.4.
- *Prohibitions as types where possible.* `Branches::push` shows the
  move: make starts unrepresentable as branches (our
  `startChildWorkflow` returns `Either`, not a branch — already
  compatible). `timeout`-around-call and `select!`-over-calls can
  only be prose + review until the Pending layer types them.

**Decided (no work).** No task-local port needed — Bluefin `Ask` is
the ambient carrier, with the tested fork-snapshot pattern standing
in for the non-crossing rule. No `with_current` equivalent (no
`Arc`s to contend on). `__private`-style shim only if a macro needs
absolute paths. `select_workflow!`/`join_workflows!` reduce to
`waitForFirstWorkflow`/`waitForWorkflows` + `handleResult` until
durable awaits exist. Deadline bounds already ride options; no
timeout wrapper will be offered.

**Concrete gap list for `Transact.Select`.** `Racing`/`Recording`/
`Branches` types; `check_select` (id-after-branches) /
`record_select` (index-only, two-commit window); `control_error`
(control-vs-application split — needs the engine error taxonomy
first); `select_step!` equivalent; handle-race macros as
wait+`handleResult` compositions; compile-time refusals
(<2 branches, refutable bindings, biased order); the 12 oracle
select tests as behavioral acceptance (especially the window and
losing-sleep shapes).


