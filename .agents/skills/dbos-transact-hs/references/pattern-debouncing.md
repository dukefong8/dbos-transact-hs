---
title: Debounce Workflows to Prevent Wasted Work
impact: MEDIUM
impactDescription: Prevents redundant workflow executions during rapid triggers
tags: pattern, debounce, delay, efficiency
---

## Debounce Workflows to Prevent Wasted Work

Use `DBOS.Transact.Debouncer` to delay workflow execution until some time has passed since the last trigger. This prevents wasted work when a workflow is triggered multiple times in quick succession.

```haskell
-- Register once, before launch: the debounced workflow itself.
debouncedRef <-
  registerWorkflowRef dbos (newWorkflowKey "processInput") processInput
    >>= either (die . show) pure

-- Every keystroke debounces; the workflow runs once, with the last input.
-- The options carry the queue, the timeout cap, and the acting application.
_ <- debounce dbos debouncedRef (debouncerNew {debouncerQueueName = Just "input-q"}) userId (secondsDuration 60) (Just (encodeWorkflowValue userInput))
```

Key behaviors (same as TS):

- The `key` groups executions debounced together (e.g. per user).
- The period delays execution by that amount from the last call.
- `debouncerTimeout` (`Maybe Duration`) caps the wait since the first trigger; `Nothing` waits indefinitely.
- `debouncerQueueName` (`Maybe Text`) runs the released workflow on a queue; `Nothing` runs it on the internal queue.
- When the workflow finally executes, it uses the **last** set of inputs.
- Once the period expires and the workflow is released, the next `debounce` call starts a new cycle.
- From inside a workflow body, `debounceInWorkflow` bounces with the caller's step, so the bounce commits atomically with its checkpoint.

Differences from TS: there is no `Debouncer` class holding the workflow function — the debounced workflow is a separately registered `WorkflowRef` passed to each call, because starting by bare name does not exist. The per-key gate is the deduplication key `<workflow>-<key>` (class-prefixed for instance workflows), bounced at the sysdb level rather than through a framework workflow.
