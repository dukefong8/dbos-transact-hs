---
title: Queue Deduplication
impact: HIGH
impactDescription: Prevent duplicate queued work
tags: queue, deduplication, idempotency
---

## Queue Deduplication

Enqueue entries carry `Enqueue { deduplicationId, duplicationPolicy, priority, delay }`. `DuplicationPolicy` decides collisions on the dedup key; starting the same workflow id twice joins the running workflow instead of duplicating it.

```haskell
let q = (enqueueNew "email") { deduplicationId = Just "user-42", duplicationPolicy = ReturnExisting }
```

Rule: `ReturnExisting` requires a key — with no key there is no collision to resolve. Validate with `validateEnqueue` before submitting.

Difference from TS: TS exposes dedup via enqueue options on the queue call. Haskell models it as the `Enqueue` value threaded through `StartOptions.startQueue`.
