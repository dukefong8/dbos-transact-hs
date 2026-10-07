---
title: Set Step Timeouts and Honor Cancellation
impact: HIGH
impactDescription: Prevents hung external calls from stalling workflows
tags: step, timeout, cancellation
---

## Set Step Timeouts and Honor Cancellation

Set `timeout = Just d` in `StepOptions` to bound each attempt. A timed-out attempt fails and, with `maxAttempts > 1`, retries like any other failure. Cancelling a workflow does not interrupt a running step; the workflow stops at its next step.

**Incorrect (hung call, no timeout):**

```haskell
-- No timeout: a hung server stalls this step.
runStep wctx "fetchData" (\_ -> fetchData)
```

**Correct (timeout plus retry budget):**

```haskell
runStepWith
  (stepOptionsDefault { timeout = Just (secondsDuration 5), maxAttempts = 3, interval = secondsDuration 1 })
  wctx "fetchData" (\_ -> Right <$> fetchData)
```

Check `stepCtxCancellationToken` inside long step bodies to stop early when the workflow is cancelled. `timeout` only applies when the step runs inside a workflow; outside, steps run as plain calls with no timeout.

Difference from TS: TS passes `timeoutMS :: Int` and exposes `DBOS.stepStatus.timeoutSignal` / `cancelSignal` (`AbortSignal`). Haskell exposes `timeout :: Maybe Duration` and `stepCtxCancellationToken`; there is no abort-signal threading into `fetch` — poll the token cooperatively.
