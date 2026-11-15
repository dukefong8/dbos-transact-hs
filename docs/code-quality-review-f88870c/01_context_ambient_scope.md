# 01 — The two per-thread maps in `Context`

File: `src/DBOS/Internal/Transact/Context.hs` (723 lines, +126/−38 in this commit)

## What the commit added

The fix itself is right: the step scope that a pending drive reads as its ambient
context must be keyed by **call stack**, not by workflow, because a sibling step
running on another thread must not make this stack's durable calls plain. So
`WorkflowState` gained two thread-keyed fields
(`Context.hs:229-252`):

```haskell
stepDepthByThread :: StrictTVar m (Map.Map (ThreadId m) Int)
scopeByThread     :: StrictTVar m (Map.Map (ThreadId m) (StepScope m))
```

along with `newWorkflowState`'s two `newTVarIO Map.empty` calls (`:261-262`), a
reader `stepDepth` (`:319-323`), `insideAStep = (> 0) <$> stepDepth` (`:435`),
`driveScope`/`inStepScope` (`:443-457`), and the widened
`withStep`/`withInheritedStepScope` (`:595-651`).

## The problem

The two maps are not two facts. They are one fact stored twice, and the code
proves it twice over.

**First, they are written together, atomically, at every site.** There are
exactly four places either map is mutated, and each pair is inside a single
`atomically` block:

| site | depth write | scope write |
|---|---|---|
| `withStep` install (`:602-606`) | `insertWith (+) tid 1` | `Map.insert tid scope` |
| `withStep` `restore` (`:608-613`) | `unbump` | `unseat` |
| `withStep` `finish` (`:614-617`) | `unbump` | `unseat` |
| `withInheritedStepScope` install (`:638-643`) | `insertWith (+) tid 1` | `Map.insert tid scope` |
| `withInheritedStepScope` `restorePrevious` (`:644-650`) | restore/delete | restore/delete |

No observer can ever run between the depth write and the scope write: STM gives
them one commit. So for every thread id `tid` and every reachable state,

```
Map.member tid stepDepthByThread  ⇔  Map.lookup tid stepDepthByThread == Just n, n > 0
                                  ⇔  Map.member tid scopeByThread
```

**Second, the count has no reader that wants a count.** `stepDepth` is read in
exactly one place, `insideAStep` (`:435`), which immediately throws the integer
away and asks `> 0`. It is not exported. Its comment (`:312-318`) explains the
per-stack reasoning, not why a depth is needed rather than a presence. An `Int`
used as a `Bool`, maintained by arithmetic (`Map.insertWith (+)` on entry,
`Map.update (\n -> if n <= 1 then Nothing else Just (n - 1))` on exit), is the
kind of encoding that invites exactly the bug it is meant to prevent: two
counters that can drift.

**Third, one fact gets two different restore mechanisms.** `withStep` restores
the scope by remembering the previous value in a local (`previous`) and writing
it back through `unseat` (`maybe (Map.delete tid) (Map.insert tid) previous`),
while decrementing depth by arithmetic. `withInheritedStepScope` restores by
snapshotting *both* maps' previous values into a tuple and writing both back.
Two mechanisms, one invariant, and the comments have to explain the coupling
("Entries leave with their depth", `:247`; "restored beside it", `:245`). That
coupling is a maintenance contract no type enforces.

## The code-judo move

Keep the per-thread key (that is the fix) and delete the second map:

```haskell
-- | The step body running on each thread of this execution, if any. The
-- oracle's task-local @CURRENT@: a sibling step on another thread is
-- invisible here, a nested call on this stack sees the body it runs in, and
-- an entry leaves when its body returns, so a finished thread holds nothing.
stepScopeByThread :: StrictTVar m (Map.Map (ThreadId m) (StepScope m))
```

Then:

```haskell
inStepScope wctx = do
  tid <- myThreadId
  scopes <- readTVarIO wctx.wctxState.stepScopeByThread
  pure (StepCtx wctx . Just <$> Map.lookup tid scopes)

insideAStep wctx = Base.isJust <$> inStepScope wctx
```

`stepDepth` is deleted. `withStep`'s install saves the previous scope and its
single restore writes that scope back (delete at the bottom of the stack), so
`unbump` and `unseat` collapse into one `restore`:

```haskell
previous <- atomically $ do
  scopes <- readTVar wctx.wctxState.stepScopeByThread
  writeTVar wctx.wctxState.stepScopeByThread (Map.insert tid scope scopes)
  pure (Map.lookup tid scopes)
let restore = atomically $ do
      restored <- readTVar scope.scopeRestored
      unless restored $ do
        writeTVar scope.scopeCancellation True
        modifyTVar wctx.wctxState.stepScopeByThread (maybe (Map.delete tid) (Map.insert tid) previous)
```

Nesting keeps working exactly as it does now, because the stack is the call
stack and the previous value is what "one level up" means:

| state on `tid` | depth+today | scope map today | single map |
|---|---|---|---|
| outside any step | absent / 0 | absent | absent |
| one body | `1` | body A | `A` |
| nested body | `2` | body B | `B` (previous = `A`) |
| inner returns | `1` | body A | `A` |
| outer returns | absent | absent | absent |

`withInheritedStepScope`'s `restorePrevious` drops its depth half and its
`(Maybe Int, Maybe StepScope)` tuple becomes a `Maybe StepScope`.

Net: **one fewer `StrictTVar`, one fewer `newTVarIO` in `newWorkflowState`, one
fewer reader, one fewer restore mechanism, roughly 15 lines gone, and the
invariant becomes structural** — "in a step" *is* "a scope is installed", so the
two cannot disagree. This is the difference between a bug that was fixed and a
bug that can no longer be written.

## What not to do

- Do not keep the depth map "for future nesting rules". No consumer wants a
  number; when one does, it can ask for it then, against the map's presence
  plus the scope's own marker.
- Do not replace both with a single `Bool` that loses the scope. The scope is
  the payload the drive needs (`driveScope`); the presence is the predicate.
- Do not derive `insideAStep` from the *placement*. The placement is built once
  at call time and a `PlacementInsideStep` handed across threads must still be
  refused there (`checkHere`'s `sameStepBody`), which is a different question
  from "what is running on this call stack right now".

## Verification

The behavior is already pinned by the new tests, so this is a green-on-green
change:

- `test/DBOS/Transact/ContextTestSim.hs:166/179/183` — `insideAStep` false at a
  boundary, true inside `withStep`, and false again on a **sibling thread**
  while the parent body runs. The sibling case is the regression this commit
  exists for; it must stay green.
- `test/DBOS/Transact/ContextTest.hs:266/269` — the live twin.
- `withInheritedStepScope`'s carrier path is exercised through the timeout and
  preemption supervisor sims (the timed-body-stays-plain case the commit
  message names).

Run `make ghcid-wait` on the watcher (sims only) after the edit; the live
groups stay disabled, per the repo's TDD loop. No live-database gate is needed
beyond the usual suite run, because no wire or row changes.
