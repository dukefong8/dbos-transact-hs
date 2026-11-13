---
title: Workflow Timeouts
impact: CRITICAL
impactDescription: Bound whole-workflow execution time
tags: workflow, timeout
---

## Workflow Timeouts

`Timeout` on `RunOptions` / `StartOptions` bounds a workflow run. `Timeout` has three states: `Inherit` (the default, a child takes its parent's deadline), `None` (no limit), and `Explicit Duration` (a budget counted from now).

```haskell
startWorkflow exec ref (startOptionsDefault { startTimeout = Explicit (secondsDuration 60) }) input
```

A timed-out workflow stops scheduling new steps; a running step finishes (steps have their own `StepOptions.timeout`). Use `sleepStep` + `getEvent` timeouts for business deadlines inside the body.

Difference from TS: TS `timeoutMS` on start options. Haskell is `Timeout` (`Explicit` / `None` / `Inherit`) on `RunOptions`/`StartOptions`.
