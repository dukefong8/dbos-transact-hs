---
title: Queue Management
impact: HIGH
impactDescription: Inspect and change queues at runtime
tags: queue, management, update
---

## Queue Management

Queues are runtime objects, not code: list them, fetch one, update limits in place, or delete them.

```haskell
qs <- listQueues dbos
q  <- queue dbos "email"
_  <- updateQueue dbos "email" (defaultQueueChange { workerConcurrency = Set (Just 20) })
_  <- deleteQueue dbos "email"
```

`registerQueue` with `NeverUpdate` keeps stored limits when the queue exists; other `QueueConflict` policies replace them. Priority queues require `priorityEnabled = True` before enqueued priorities take effect.

Difference from TS: TS client methods are `registerQueue` / `retrieveQueue` / `deleteQueue` on `DBOS` or `DBOSClient`. Haskell splits app-side (`registerQueue`, `updateQueue`, `deleteQueue`, `listQueues`, `queue`) from client-side equivalents in `DBOS.Transact.Client`.
