---
title: Queue Deduplication
impact: HIGH
impactDescription: Prevent duplicate queued work
tags: queue, deduplication, idempotency
---

## Queue Deduplication

Queue entries carry `QueueRequest { deduplicationId, duplicationPolicy, priority, delay }` (`EnqueueOptions` holds the same fields flat for a client enqueue). `DuplicationPolicy` decides collisions on the dedup key; starting the same workflow id twice joins the running workflow instead of duplicating it.

```haskell
let q = (queueRequestNew "email") { deduplicationId = Just "user-42", duplicationPolicy = ReturnExisting }
```

Rule: `ReturnExisting` requires a key — with no key there is no collision to resolve. Validate with `validateEnqueue` before submitting.

Difference from TS: TS exposes dedup via enqueue options on the queue call. Haskell models it as the `QueueRequest` value threaded through `StartOptions.startQueue`.
