---
title: Child Workflows and Background Execution
impact: CRITICAL
impactDescription: Compose workflows without blocking the parent
tags: workflow, child, background
---

## Child Workflows and Background Execution

`startChildWorkflow` starts a registered child and returns a `WorkflowHandle` immediately; `awaitChild` blocks for its result. The child's id derives from the parent unless overridden.

```haskell
Right handle <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue input))
-- ... other steps ...
result <- awaitChild handle
```

Start-and-forget is starting without awaiting. Never start children from inside step bodies — the type system refuses nested checkpointing (`runTxStep` refuses; `runStep` degrades to a plain run).

Difference from TS: TS `startWorkflow` / `DBOS.startChildWorkflow`. Haskell threads `WorkflowCtx` and takes a `WorkflowRef` (from `registerWorkflowRef`), not a function reference.
