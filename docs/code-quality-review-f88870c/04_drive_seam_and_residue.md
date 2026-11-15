# 04 — The drive seam, the caller block, and sweep residue

Files: `src/DBOS/Internal/Transact/Checkpoint.hs`,
`Step.hs`, `Sleep.hs`, `Event.hs`, `Patch.hs`, `Handle.hs`, `Management.hs`,
`Message.hs`, `Statements.hs`, `Context.hs`.

## 1. `checkHere`'s ambient argument is now always `Just`

`checkHere` (`Checkpoint.hs:164-202`) takes `Maybe (StepCtx exec m)` and has an
`(Outside, Nothing) -> Right DurabilityPlain` arm (`:184`) plus a
`(ClientConnection, _)` arm (`:185`). Twelve live call sites open with the same
two lines and the same `Just`:

| file | `here <- driveScope wctx` | `checkHere … (Just here)` |
|---|---|---|
| `Step.hs` | 424 | 425 |
| `Sleep.hs` | 86 | 87 |
| `Event.hs` | 62, 189 | 63, 190 |
| `Patch.hs` | 129 | 130 |
| `Handle.hs` | 168 | 169 |
| `Management.hs` | 208, 258, 316, 403, 503, 549 | 209, 259, 317, 404, 504, 550 |

Every one of them computes `driveScope wctx` — `maybe (stepCtxBoundary wctx) id
<$> inStepScope wctx` — and immediately re-wraps it in `Just`. Nothing in the
live tree constructs a placement paired with `Nothing`: `placeCall`
(`:122-129`) always takes a `WorkflowCtx`, and a client call travels as
`ClientConnection`, which matches its arm whatever the ambient. So the `Maybe` is
dead optionality at the seam, and its cost is 24 lines of identical preamble plus
a binding name (`here`) that ten functions now have to keep straight.

Keep the pure checker — the tests use it with explicit ambient values
(`test/DBOS/Transact/CheckpointCases.hs:148, 156, 162-163, 171, 181`) — and add
one monadic drive-side entry:

```haskell
-- | 'checkHere' at the ambient this thread is actually standing in.
checkHereOn :: (MonadSTM m, MonadThread m)
            => WorkflowCtx exec m -> StepPlacement exec m -> Text
            -> m (Either (Error e) (StepDurability exec m))
checkHereOn wctx placement step = checkHere placement step . Just <$> driveScope wctx
```

Each site loses its `here` binding and becomes
`case checkHereOn wctx placement name of …`. `describePolled` and
`placementWhereabouts` keep their `Maybe` signatures: they describe a *refusal*,
and both arms are still meaningful there.

Note on fidelity: `Outside` stays. Rust's `StepPlacement::Outside` is reachable
from `StepPlacement::here()` when there is no task-local context
(`crates/dbos/src/checkpoint.rs:488`, `:540`), and the port's counterpart
`placementHere` (`:138-142`) is exercised by `CheckpointCases.hs:100`. It is only
the *drive* seam that can no longer produce it, because a drive always has a
`WorkflowCtx`. That asymmetry is worth one comment on `placementHere`, not a
type change.

## 2. `Message.hs`: the caller position is computed twice

`sendWith` (`:94-100`) and `sendBulkWith` (`:142-148`) contain the same block:

```haskell
stepped <- insideAStep wctx
caller <-
  if stepped
    then pure Nothing
    else do
      stepId' <- nextStepId wctx
      pure (Just (WorkflowId …, stepId'))
```

Six lines each, and the duplication hides itself behind two spellings of the same
value: `WorkflowId workflowText` in `sendWith`, `WorkflowId (workflowId wctx)` in
`sendBulkWith`. One helper on the same module owns the whole rule — "inside a step
the enclosing checkpoint stands for the send, so no id is spent":

```haskell
callerPosition :: (MonadSTM m, MonadThread m) => WorkflowCtx exec m
               -> m (Maybe (WorkflowId, Int))
callerPosition wctx = do
  stepped <- insideAStep wctx
  if stepped then pure Nothing else do
    stepId' <- nextStepId wctx
    pure (Just (WorkflowId (workflowId wctx), stepId'))
```

`recv` (`:174-181`) keeps its own use of `insideAStep` because it *refuses* rather
than degrading to plain — a different decision, and it should stay visibly
different.

## 3. Five unused `stepCtxBoundary` imports

The `insideAStep`/`driveScope` sweep left the old name imported in five modules.
In each file the identifier occurs exactly once — on the import line:

| file | import line | occurrences in file |
|---|---|---|
| `Sleep.hs` | 32 | 1 |
| `Management.hs` | 119 | 1 |
| `Event.hs` | 23 | 1 |
| `Patch.hs` | 40 | 1 |
| `Handle.hs` | 38 | 1 |

`-Wall` is on (`dbos-transact-hs.cabal:17-21`), so these are five unused-import
warnings the moment the module is rebuilt — free noise from this commit, free to
delete. `Step.hs`, `Checkpoint.hs`, and `Context.hs` genuinely use the name and
keep it.

## 4. Two things that look like residue but are not — leave them

- **`Statements.hs`'s `updated_at` changes are consistency fixes, not drift.**
  `clearQueueAssignmentSession` stopped taking a Haskell clock value and now
  writes `(extract(epoch from now()) * 1000)::bigint` (`:2423`), and
  `initWorkflowStatement` dropped `initParamUpdatedAt` so `created_at` and
  `updated_at` come from the same SQL `now()` (`:679`). SQL `now()` is already
  the convention for `updated_at` throughout the module (30-odd statements,
  `:569`, `:1853`, `:2685`, `:2790`, …); the diff removes the last two places a
  Haskell clock value was threaded in. That is the right direction, and it keeps
  `created_at`/`updated_at`/delay-base on one clock within the transaction.
- **`StepTiming <$> timestampNow <*> timestampNow`** in `deliverTo`
  (`Backend.hs:2552`) reads the clock twice for what is effectively the same
  instant. It is not sloppiness: Rust fixes exactly that pair before its
  transaction (`sysdb/postgres.rs:8329-8332`) so a retried send records the same
  coarse timing. Faithful; do not "fix" it.

## Verification

- `checkHereOn` is a pure wrapper insertion: no behavior text changes, and the
  drive-refusal message text is pinned by the existing `StepBuiltElsewhere`
  cases.
- `callerPosition` is covered by `MessageCases` (a send inside a step spends no
  id; a send at a boundary spends one) and by `DBOS.sendBulk`'s step-id
  assertions.
- Removing five imports cannot change behavior; the watcher reload is the check,
  and `make ghcid-wait` fails red on a warning-as-error build if one of them was
  in fact load-bearing.
