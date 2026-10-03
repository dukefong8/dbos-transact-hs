# Dual-stack transactional steps

THROWAWAY prototype (branch `proto/phantom-brands` only). Run: `sh run.sh`
(7 checks: IO run, sim run, 3 invariant agreements ×2 interpreters,
1 must-fail compile test). The IO run needs `DBOS_DATABASE_URL`; without
it the script runs sim + negatives and says so loudly.

## Claim

One widget workflow source against a `StepOps m` step table. Each field
is a step and each step is its own transaction: Postgres
BEGIN/body/COMMIT (ROLLBACK on throw) over a held raw connection under
IO, one `atomically` block under IOSim. A stock race, a write-then-throw
rollback, and an insufficient-stock refusal print identical invariant
lines either way.

## Shape (as requested)

```haskell
data StepOps m = StepOps
  { stepReserve :: Text -> Int -> m (Either Text OrderId)
  , stepBomb    :: Text -> Int -> m (Either Text OrderId)
  , stepTrace   :: forall a. Text -> a -> m a
  , ...
  }
```

The record travels through `WorkflowCtx`; `withStep` inherits it into
`StepCtx` implicitly; bodies retrieve it by field name (`stepOps`) —
no threading, no second parameter. The rank-2 `stepTrace` is the shape
in miniature: a step polymorphic in what it carries, audited inside
the scope whose effects it joins.

## Interpreter table

| Step | IO (Postgres) | IOSim (STM) |
|---|---|---|
| scope | held raw connection + explicit BEGIN/COMMIT/ROLLBACK (per-step pinning — no pool checkout exists, same as the real backend) | one `atomically` block |
| check+decrement+insert | single `UPDATE..WHERE..RETURNING`, then inserts | one STM transaction over three TVars |
| write-then-throw | ROLLBACK removes order + audit | `throwSTM` aborts the block |
| race (8×qty 3, stock 10) | real threads, losers see `stock < qty` | deterministic threads, same arithmetic |
| reads | single sessions | `readTVarIO` |

## What each stack verifies (and does not)

IO verifies real transactional behavior: rollback actually undoes,
concurrent losers actually fail, counts land. Sim verifies the
workflow's logic and invariants deterministically: same arithmetic,
same accounting, zero database. Neither verifies the other's
interpreter internals — the contract is the op semantics
("reserve atomically decrements iff sufficient, else touches
nothing"), kept coarse-grained on purpose.

Known asymmetry, documented not hidden: STM serializes; Postgres runs
read-committed, where safety comes from the single-statement
check-and-decrement, not the isolation level. A multi-statement
check-then-act would need `SELECT FOR UPDATE` on the IO side while the
sim side stayed correct — keep ops coarse-grained so the two cannot
drift. `runStoreTx`-style body-atomicity across *several* steps exists
on IO (held connection) but has no STM counterpart for `m`-typed
bodies; the unit both stacks share is one step.

## Scope note (no oversell)

This probe brands nothing: scope discipline (`withStep` for effects,
direct reads outside) is conventional here, and cross-stack separation
(one record per interpreter — `neg-step-cross-stack`) is the enforced
part. Step-scope refusal (`insideAStep`) and StepCtx-keyed invocation
live in the tree and the companion probe; they compose by running
these steps *inside* `withStep` bodies at integration time.

## Integration path (real tree)

Widget bodies move from `Tx m -> …` (hasql-concrete — the reason
`WidgetSim`'s fake errors on statements today) to `StepOps`-shaped
records: Postgres handler behind `runTransaction`'s held connection,
STM handler behind `atomically` in the sim backend. `WidgetTest` keeps
live coverage; `WidgetSim` gains real invariant teeth (race/rollback/
refusal against STM tables instead of statement-unsupported stubs).
`Tx(..)` stays exported for the backend internals; application bodies
stop naming it.

Open: per-domain step tables vs one shared table; op-level idempotency
keys reusing the step id; serializable isolation as a config knob;
pool-held vs raw-per-scope connections at production concurrency.
