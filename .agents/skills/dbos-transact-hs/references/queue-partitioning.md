---
title: Queue Partitioning
impact: HIGH
impactDescription: Isolate concurrency per partition key
tags: queue, partitioning
---

## Queue Partitioning

Partitioned queues isolate concurrency per partition key (e.g. per customer). Set the partition fields on `QueueOptions`; the enqueue entry supplies the key.

```haskell
q <- registerQueue dbos "per-customer"
  (defaultQueueOptions { partitionConcurrency = Just 4, partitionWorkerConcurrency = Just 1 })
  NeverUpdate
```

Enqueue with the partition key on the `Enqueue` value. A partitioned queue without a key on enqueue is refused.

Difference from TS: TS `partitionQueue` constructor was removed in 5.x; partitioning is now queue options. Haskell never had the constructor — options only.
