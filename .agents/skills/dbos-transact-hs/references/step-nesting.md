---
title: Nest Steps and Transactions Inside the Right Bodies
impact: HIGH
impactDescription: Illegal runner combinations fail at compile time; legal ones carry durability guarantees
tags: step, nesting, transaction, workflow
---

## Nest Steps and Transactions Inside the Right Bodies

Every runner takes the context it records against: recording entries take `WorkflowCtx`, plain entries take `StepCtx`. A body only holds the narrower view, so illegal combinations do not compile.

| call ↓ / body → | workflow body | step body | tx body | outside workflow |
|---|---|---|---|---|
| `runStep` | recorded, exactly-once | ❌ does not compile | ❌ does not compile | — (no context) |
| `runNestedStep` | ❌ does not compile (use `runStep`) | plain, at-least-once | plain, at-least-once | — (no context) |
| `runTxStep` | recorded, exactly-once | ❌ does not compile | ❌ does not compile | — (no context) |
| `runTxOutside` | re-runs on replay — use `runTxStep` | re-runs per attempt — avoid | re-runs per attempt — avoid | single commit, no replay cover |
| start / enqueue / await | recorded | ❌ does not compile | ❌ does not compile | via `Executor`/`Client`, not contexts |

Guarantees: **exactly-once** means the outcome is deduplicated (replay adopts the stored result; the body itself may still run more than once before the record lands). **At-least-once** means the body re-runs with every enclosing attempt and recovery, with no deduplication. A bare transaction (`runTxOutside`) is all-or-nothing per run with no cross-crash record — inside a workflow that makes it at-least-once, so writes that must happen once belong in `runTxStep`.

The ❌ cells describe the direct shape (passing the body's own view). A workflow view closed over from an enclosing scope still compiles; the runners then degrade at runtime instead of recording (`runStep` runs plain, `runTxStep` and starts refuse). Write the direct shape — never rely on the degraded one.

**Correct (step inside a step):**

```haskell
outer <- ExceptT (runStep wctx "outer" $ \sctx -> do
  inner <- ExceptT (runNestedStep sctx "inner" (\_ -> pure (7 :: Int)))
  pure (inner + 1))
```

Rules:

- `runNestedStep` is the only entry that takes a step view: one step calling another. The inner call checkpoints nothing and takes no id.
- `runTxStep` needs the workflow view; it refuses inside step bodies. A transaction inside a transaction is a separate transaction, not a savepoint — keep nesting flat.
- `runTxOutside` is for code with no workflow: handlers, scripts, tests. In a workflow body it silently duplicates across recovery.

Difference from TS: TS has no nested-step entry (a step called from a step just runs) and per-ORM transaction packages; Haskell names both (`runNestedStep`, one `DataSource` seam). Rust has no transaction-step API.
