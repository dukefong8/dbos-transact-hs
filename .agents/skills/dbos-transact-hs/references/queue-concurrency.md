---
title: Queue Concurrency Limits
impact: HIGH
impactDescription: Bound how many queued workflows run at once
tags: queue, concurrency
---

## Queue Concurrency Limits

Set `workerConcurrency` on `QueueOptions` to bound simultaneous runs. The process also caps how many queues it drains concurrently (`configListenQueues`, max dispatch).

```haskell
q <- registerQueue dbos "email" (defaultQueueOptions { workerConcurrency = Just 10 }) NeverUpdate
```

`Nothing` means unbounded. Change limits at runtime with `updateQueue`; inspect with `listQueues` / `queue`.

Difference from TS: TS names are `workerConcurrency` / `globalConcurrency` depending on version. Haskell field is `workerConcurrency :: Maybe Int` on `QueueOptions`.
