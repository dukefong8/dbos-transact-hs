---
title: Run Steps Concurrently
impact: HIGH
impactDescription: Sequential independent steps waste time; unrecorded concurrency breaks replay
tags: step, concurrency, select, await
---

## Run Steps Concurrently

Build pending values first, then drive or race them. A pending value claims its step id where it is built, so a replay rebuilds the same order. `pendingStep`, `pendingSleep`, `pendingSetEvent`, `pendingGetEvent`, and `pendingAwait` build them; awaiting the pending value runs it.

```haskell
pa <- pendingStep wctx "fetch-a" (\_ -> pure (Right "a"))
pb <- pendingStep wctx "fetch-b" (\_ -> pure (Right "b"))
result <- selectStep wctx [SelectArm "a" pa pure, SelectArm "b" pb pure]
```

`selectStep` races two or more arms and runs the winner's continuation. A recorded winner replays: only that branch is driven and nothing is recorded again. A control signal out of the winning branch ends the race with that error and records nothing. A pending value the race drops still spent its id and recorded nothing.

`awaitChild` waits for a child started with `startChildWorkflow` and records the wait as its own step. The handle must belong to this execution: a recorded row naming another workflow is refused, never adopted. Inside a step body the enclosing checkpoint stands for the wait.

Difference from Rust: Rust joins with `tokio::join!` or `try_join_all` and races with `select!`. Haskell builds pendings, then drives or races them with the same id-stability rule: the ids are taken at build time, in order, before anything runs.
