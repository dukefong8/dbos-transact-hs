---
title: Workflow Events
impact: MEDIUM
impactDescription: Publish and await durable workflow state
tags: communication, events
---

## Workflow Events

`setEvent` publishes a durable key on the calling workflow; `getEvent` waits for another workflow's key with a timeout. Events survive crashes and replay.

```haskell
_ <- setEvent wctx "progress" (1 :: Int)
v <- getEvent wctx otherWorkflowId "progress" (secondsDuration 30) :: IO (Either (Error EngineOnly) (Maybe Int))
```

`getEvent` returns `Nothing` on timeout. From outside a workflow use `getWorkflowEvent` (executor) or `clientGetEvent` (client).

Difference from TS: TS `DBOS.setEvent` / `handle.getEvent`. Haskell threads `WorkflowCtx` explicitly; no ambient handle.
