---
title: Enqueue Workflows from a Client
impact: MEDIUM
impactDescription: Start queued work from outside the app process
tags: client, enqueue, queue
---

## Enqueue Workflows from a Client

Enqueue by workflow name onto a queue. The owning application is the client's `applicationName`.

```haskell
_ <- enqueueClientWorkflow client "MyWorkflow" "email" (Just (encodeWorkflowValue input))
_ <- enqueueClientWorkflowWith client "MyWorkflow" (enqueueOptionsOn "email" (enqueueNew "email")) Nothing
```

`EnqueueOptions` carries the same `Enqueue` value as in-process enqueue (priority, delay, dedup). The returned `WorkflowHandle` supports `handleStatus` / `handleResult`.

Difference from TS: TS `client.enqueue(workflowName, queueName, ...)` / `enqueueOptions`. Haskell is `enqueueClientWorkflow` / `enqueueClientWorkflowWith` + `enqueueOptionsNew` / `enqueueOptionsOn`.
