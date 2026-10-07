---
title: Workflow Timeouts
impact: CRITICAL
impactDescription: Bound whole-workflow execution time
tags: workflow, timeout
---

## Workflow Timeouts

`Timeout` on `RunOptions` / `StartOptions` bounds a workflow run. `timeoutBudget` extracts the duration; `resolveTimeoutDeadline` computes the wall-clock deadline from enqueue delay plus budget.

```haskell
startDBOSWorkflowRef exec ref (startOptionsDefault { startTimeout = timeoutSeconds 60 }) input
```

A timed-out workflow stops scheduling new steps; a running step finishes (steps have their own `StepOptions.timeout`). Use `sleepStep` + `getEvent` timeouts for business deadlines inside the body.

Difference from TS: TS `timeoutMS` on start options. Haskell is `Timeout` (`timeoutSeconds` / `Inherit`) on `RunOptions`/`StartOptions`.
