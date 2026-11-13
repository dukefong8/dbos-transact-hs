---
title: Set Queue Priority for Workflows
impact: MEDIUM
impactDescription: Urgent work waits behind a backlog
tags: queue, priority, ordering
---

## Set Queue Priority for Workflows

Set `priority` on the `QueueRequest` to order a queue: lower runs first, first in wins ties. Priorities run from 1 to 2147483647. Zero is refused. No priority means unprioritised, and unprioritised workflows run before any with one.

```haskell
let request = (queueRequestNew "tasks") { priority = Just 1 }
_ <- startChildWorkflow wctx ref (startOptionsDefault { startQueue = Just request }) input
```

The client spells the same limit flat on `EnqueueOptions`. A priority past what the column holds is refused before anything is written.

Difference from TS: TS takes `priority` on the enqueue call. Haskell carries it on `QueueRequest` (or flat on `EnqueueOptions` for a client enqueue), validated the same way.
