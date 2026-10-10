---
title: Use Steps for External Operations
impact: HIGH
impactDescription: Steps enable recovery by checkpointing results
tags: step, external, api, checkpoint
---

## Use Steps for External Operations

Any function with side effects or non-determinism (API calls, filesystem, randomness, clocks, DB writes) must run inside `runStep`. The result is recorded in `dbos.operation_outputs`; on replay the body is skipped.

**Incorrect (external call in workflow body):**

```haskell
myBody () _wctx = do
  -- Not checkpointed: replay re-fetches and may diverge.
  resp <- fetch "https://api.example.com/data"
  pure (Right resp)
```

**Correct (external call in a step):**

```haskell
myBody () wctx = runExceptT $ do
  resp <- ExceptT (runStep wctx "fetchData" (\_ -> fetch "https://api.example.com/data"))
  pure resp
```

Rules:

- Inputs and outputs must be JSON-serializable (the durable value codec).
- Do not call, start, or enqueue workflows from within step bodies. Calling a step from another step folds it into the caller's execution (`runStep` degrades to a plain run; `runTxStep` refuses). See `step-nesting.md` for the full caller-by-callee matrix.
- DBOS must be launched before a step is called. Steps are only checkpointed when called from a workflow.
