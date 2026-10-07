---
title: Queue Rate Limiting
impact: HIGH
impactDescription: Bound starts per second on a queue
tags: queue, rate-limit
---

## Queue Rate Limiting

Set `rateLimit` (and `partitionRateLimit` for partitioned queues) on `QueueOptions` to bound starts per second. Limits are enforced at dequeue; excess workflows wait for the next window.

```haskell
q <- registerQueue dbos "email" (defaultQueueOptions { rateLimit = Just (perSecond 10) }) NeverUpdate
```

Adjust at runtime with `updateQueue`. Rate limits compose with `workerConcurrency`: both must allow a start.

Difference from TS: TS takes `{ limit, period }` objects. Haskell takes the `RateLimit` value; same semantics.
