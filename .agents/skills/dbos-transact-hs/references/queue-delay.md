---
title: Queue Delay
impact: HIGH
impactDescription: Schedule queued work for later
tags: queue, delay
---

## Queue Delay

Set `delay` on the `Enqueue` value to hold a queued workflow until the delay elapses. The workflow idles in the queue; `dequeueDBOSWorkflows` skips it until due.

```haskell
let q = (enqueueNew "email") { delay = Just (secondsDuration 60) }
```

A delay with no queue is refused (`validateEnqueue`): nesting under a queue is what gives the delay meaning. Combine with `setWorkflowDelay` to move an already-queued workflow.

Difference from TS: TS takes `delaySeconds` on the enqueue call. Haskell takes `delay :: Maybe Duration` on `Enqueue`.
