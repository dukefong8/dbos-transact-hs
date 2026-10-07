---
title: Idempotency Keys
impact: MEDIUM
impactDescription: Safe retries of workflow starts
tags: pattern, idempotency
---

## Idempotency Keys

Starting a workflow with an explicit id joins the already-running workflow instead of starting a second one. Use a business key (order id) as the workflow id.

```haskell
_ <- startDBOSWorkflowRef exec ref (startOptionsDefault { startWorkflowId = Just key }) (Just (encodeWorkflowValue input))
```

The same key through `enqueueDBOSWorkflow` or the client dedups. Keys are the primary defense against double-submit from HTTP handlers.

Difference from TS: TS `startWorkflow` with idempotency key option. Haskell is `startWorkflowId :: Maybe WorkflowId` on `StartOptions`.
