---
title: Durable Sleep
impact: MEDIUM
impactDescription: Checkpointed waits that survive crashes
tags: pattern, sleep
---

## Durable Sleep

`sleepStep` suspends the workflow for a `Duration` and checkpoints the wake time. Replay adopts the recorded wake time instead of starting the clock again. Outside a workflow, `sleepPlain` waits without checkpointing.

```haskell
_ <- sleepStep wctx (secondsDuration 5)
```

A sleep inside a step body takes no step id (use the workflow context). Combine with `getEvent`/`recv` timeouts for deadline patterns.

Difference from TS: TS `DBOS.sleep(seconds)`. Haskell is `sleepStep :: WorkflowCtx exec m -> Duration -> m (...)` with `secondsDuration` / `millisDuration`.
