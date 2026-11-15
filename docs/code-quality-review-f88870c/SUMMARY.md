# Thermo-nuclear code quality review — `f88870c`

Scope: `git diff HEAD~1 HEAD` (61 files, +1225/−502; of which 20 `src/` files, +467/−291).
Review date: 2026-11-14. Branch `main`, tree clean apart from two untracked dotfiles.

## Verdict

**Not approved as-is.** The commit is behaviorally sound and fixes real defects — the
per-thread step-scope fix closes a genuine leaf-rule leak where a sibling step running on
another thread made this stack's durable calls plain, the fork walk and the send insert now
share one transaction, `sendBulk` records its step atomically instead of racing a synthetic
runner record, and `application_name` is now bound on every step write. The new regression
tests pin exactly the two behaviors that changed (sibling-thread visibility, drive-time
refusal).

But the change leaves two structural items that should not land, plus two smaller seams that
should be finished while the code is open. None of them is behavior work; all are deletions or
moves.

1. `Context` now keeps **two per-thread maps that encode one fact**, synchronized by two
   different restore mechanisms. This is the main blocker.
2. `Backend` still carries **two implementations of the same send pipeline** (`deliverTo` vs
   `sendBatchInTx`); the PR deleted the outside-transaction fork walk but stopped one cut
   short of unifying the expansion and parameter projection.
3. Queue registration now has **four copies** of the read-row → write-row → warn sequence, one
   of them a six-level nest inserted into an unrelated registration flow — and the pre-read they
   all share is a port-only race the oracle does not have, created because the collapsed
   `upsertQueue` type cannot return the row the warning needs.
4. `checkHere`'s `Maybe ambient` became **dead optionality**: all twelve call sites compute
   `here <- driveScope wctx` and pass `Just here`, and five files keep a now-unused
   `stepCtxBoundary` import from the sweep.

No file crossed the 1000-line threshold in this diff (the largest touched module is
`Context.hs`, 723 lines), and `Backend.hs`/`Statements.hs` are already far above it for a
fidelity reason (one Rust module ↔ one Haskell module), so the remedy there is internal
extraction, not a new module.

## Findings

### 1. Two maps, one fact: `stepDepthByThread` and `scopeByThread` (blocker)

`src/DBOS/Internal/Transact/Context.hs:234-248` declares two `StrictTVar (Map (ThreadId m) …)`
fields. `withStep` (`:595-619`) and `withInheritedStepScope` (`:632-651`) both bump the depth
map *and* insert the scope map inside one `atomically`, then restore them with two different
mechanisms: `unbump` (decrement, delete at zero) plus `unseat` (restore the previous scope),
versus `restorePrevious` (write both previous values back). The comments themselves document
the coupling ("Entries leave with their depth", "restored beside it").

The count is not independent information: `stepDepth` is read only by `insideAStep`
(`:435`), and its value is exactly "a scope is installed on this thread" because both
writers install both values in the same STM transaction — no observer can ever see one without
the other. A single `Map (ThreadId m) (StepScope m)` (presence = in-step) collapses
`stepDepth`, `insideAStep`, `unbump`, and one of the two restore blocks, and makes drift
impossible. The detail file has the exact replacement and the nesting table.

### 2. Two send pipelines in one module (blocker, high value)

`deliverTo` (`Backend.hs:2549-2618`) now inlines fork expansion and `SendMessagesParams`
assembly inside its transaction. `sendBatchInTx` (`:2654-2703`) already owns
`pairMessages`/`expandPaired`/`insertExpanded`, which do the same expansion and an almost
identical parameter projection, differing only in the `sendParamsStep` value. The PR deleted
the old outside-transaction `expandForks`/`descendantForks` pair — good — but left the
remaining two copies of the *inside-transaction* pipeline. A shared `Tx`-level batch function
(`deliverBatch messages fallbacks maybeStep`) plus one pure `sendParamsOf` collapses both:
`deliverTo` becomes `runTransaction … (deliverBatch …) >>= wake`, and `sendBatchInTx` becomes
the same call with `Nothing` for the step. This also puts the atomicity rule (walk, insert,
step record in one commit) in exactly one place.

### 3. Queue before/after: the pre-read is a port-only artifact (should-fix, and the ADR is now void)

`registerQueue` (`Queue.hs:347-376`) and `clientRegisterQueue` (`Client.hs:610-643`) now
pre-read with `getQueue`, upsert, and conditionally call `warnIfPartitioned`; `updateQueue`
(`Queue.hs:409-428`) and `clientUpdateQueue` already did the same. The engine registration path
is a six-level nest whose inner `case previous of Just …/Nothing …` exists only to fire a
tracing event.

The deeper problem is why the pre-reads exist. `warnIfPartitioned` needs the row *before* the
write, and `upsertQueue` returns `(Bool, QueueRecord)`, so the commit added a second read — one
the code itself documents as racy (`Queue.hs:360-364`). Rust has no such window: it reads
`before` from the row its own upsert locked and returns `UpsertedQueue { before, after }`
(`crates/dbos/src/queue.rs:753-760`; the update path at `:861`). That makes ADR-0044's
justification — "no caller reads a post-call `before` row" — false, and the collapsed type
worse than the oracle's on both axes: it carries a `Bool` no call site reads (all four discard
it) and omits the field every warning site now needs. The remedy is to restore
`UpsertedQueue`/`UpdatedQueue` (the repo's own one-type-one-type rule); the backend can populate
them from rows it already holds under the lock (`Backend.hs:3942/3952/3957`, `:4110-4120`) with
no new SQL, after which both pre-reads, both race paragraphs, the dead `Bool`, and the four-way
duplication all disappear together. If the pair is refused for another reason, the fallback is
one `upsertQueueWarned` helper in `Queue`: it removes the nest and three copies but keeps the
race. Either way ADR-0044 must be superseded or its evidence bullet rewritten.

### 4. `checkHere`'s ambient is now always `Just` (should-fix)

Twelve call sites across `Step`, `Sleep`, `Event`, `Patch`, `Handle`, and `Management` open
with `here <- driveScope wctx` and then pass `(Just here)`. The `Maybe` arm `(Outside, Nothing)`
is unreachable from every drive path, because a drive always has a `WorkflowCtx`. Keep the pure
checker (tests use it with explicit ambient values), but give the drive paths one monadic entry
point that reads `driveScope` itself: the `Maybe` leaves the seam and the twelve two-line
preambles collapse to one line each. `Outside` itself stays — it is the oracle's variant, reached
only through `placementHere`, which the pure tests exercise.

### 5. Sweep residue (minor, but free)

`Sleep.hs:32`, `Management.hs:119`, `Event.hs:23`, `Patch.hs:40`, and `Handle.hs:38` each still
import `stepCtxBoundary`, and in each file that name occurs exactly once — on the import line.
`-Wall` flags all five as unused imports. `Message.hs` separately duplicates the "inside a step
→ no caller, else take next step id" block between `sendWith` (`:94-100`) and `sendBulkWith`
(`:142-148`); one small `callerPosition` helper would own it.

## Positives worth keeping explicit

- `withStep` now masks the install and handler setup and runs only the body unmasked
  (`Context.hs:595-619`). That is the correct shape: a kill cannot land between the bump and
  the handler registration, while timeouts and cancellation still reach the body.
- The `MonadCatch → MonadMask` sweep is directionally right (the `mask` needs it), and the
  constraint changes propagate consistently through the transitive runner signatures.
- `sendBulk` no longer routes through `runStepWith`; the system-DB transaction both validates
  the caller position (`checkCallerStepUnfenced`) and records the step, so replay adopts and a
  crash cannot leave messages durable without the step row.
- The `replayStep` child-workflow-id guard removal is safe: `checkStep` rejects a position
  whose recorded name differs (`stepCheckToRecord`, `Backend.hs:1611-1627`), so a plain step
  cannot silently adopt a child-start row.
- The corrected wire name `DBOS.sendBulk` matches the oracle (`crates/dbos/tests/messages.rs`).
- `validateEnqueue` on the facade is not a boundary leak: both queue skill references tell app
  authors to call it before submitting, so the export has a documented consumer.

## Remediation sequence

1. Collapse the Context ambient state to one thread-keyed map; rerun the watcher sims
   (`ContextTestSim`, `CheckpointCases`) — the two new cases already pin the behavior.
2. Hoist the shared send batch pipeline in `Backend`; `sendInternal` and `sendBatchInTx` call
   it. No wire change; the existing message suite covers it.
3. Restore `UpsertedQueue`/`UpdatedQueue` and drop the four pre-reads (or, if refused, extract
   one `upsertQueueWarned` helper in `Queue`); supersede ADR-0044's evidence in the same commit.
4. Fold `driveScope` into the drive-side placement check; trim the five unused imports and the
   duplicated caller block in `Message`.
5. Full suite and durable read-back, per the repo gate.

Each step is independently green-able, so land them as separate commits rather than one more
omnibus.

## Detail files

- `01_context_ambient_scope.md` — the two-map analysis, invariant proof, proposed replacement.
- `02_backend_send_pipeline.md` — duplication measurements and the unified batch shape.
- `03_queue_before_after.md` — the four warning sites, the race the pre-read adds, and why
  ADR-0044 is now void.
- `04_drive_seam_and_residue.md` — `checkHere` dead optionality, the duplicated caller block,
  the five unused imports, and two changes that look like residue but are fixes.
  time-source inconsistency in `Statements.hs`.

## Verification status

Reviewed statically against the full `src/` and `test/` diff and the current files. I did not
rerun the suite: a demo server is live on port 8090 in this session, and the repo rule is to
stop it before any suite-binary run because its queue supervisor claims test fixtures. The
commit message reports watcher sims green and a full suite of 1105 with durable read-back;
oracle checks I ran read-only were the `DBOS.sendBulk` wire constant and the `checkStep`
name guard described above.
