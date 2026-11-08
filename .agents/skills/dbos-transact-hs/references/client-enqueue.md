---
title: Enqueue Workflows from a Client
impact: MEDIUM
impactDescription: Start queued work from outside the app process
tags: client, enqueue, queue
---

## Enqueue Workflows from a Client

Enqueue by workflow name onto a queue. The owning application is the client's `applicationName`.

```haskell
_ <- clientEnqueueWorkflow client (newQualifiedWorkflowName "MyWorkflow") "email" (Just (encodeWorkflowValue input))
_ <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "MyWorkflow") ((enqueueOptionsNew "email") {priority = Just 1}) Nothing
```

`EnqueueOptions` carries the queue-shaped fields flat beside the rest (priority, delay, dedup); an in-process start says the same things in a `QueueRequest` under `StartOptions.startQueue`. The returned `WorkflowHandle` supports `handleStatus` / `handleResult`.

Difference from TS: TS `client.enqueue(workflowName, queueName, ...)` / `enqueueOptions`. Haskell is `clientEnqueueWorkflow` / `clientEnqueueWorkflowWith` + `enqueueOptionsNew` with record updates.
