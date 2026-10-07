---
title: Configure Step Retries for Transient Failures
impact: HIGH
impactDescription: Automatic retries handle transient failures without manual code
tags: step, retry, exponential-backoff, resilience, shouldRetry
---

## Configure Step Retries for Transient Failures

Use `runStepWith` with `StepOptions` for automatic retries with exponential backoff. Plain `runStep` uses `stepOptionsDefault` (`maxAttempts = 1`: no retry).

**Incorrect (manual retry loop):**

```haskell
fetchWithRetry wctx = go (0 :: Int)
  where
    go n = do
      r <- runStep wctx "fetchData" (\_ -> fetchData)
      case r of
        Right v -> pure (Right v)
        Left e | n < 2 -> do
          threadDelay (2 ^ n * 1000000)
          go (n + 1)
        Left e -> pure (Left e)
```

**Correct (built-in retries):**

```haskell
import DBOS.Transact (runStepWith, stepOptionsDefault, secondsDuration)

fetchWithRetry wctx =
  runStepWith
    (stepOptionsDefault { maxAttempts = 10, interval = secondsDuration 1, backoffRate = 2.0 })
    wctx "fetchData" (\_ -> Right <$> fetchData)
```

`StepOptions` fields: `maxAttempts` (total attempts including the first), `interval` (initial delay), `backoffRate` (multiplier), `maxInterval` (cap), `timeout` (per-attempt bound; a timed-out attempt retries like any failure — see `step-timeouts.md`), `preemptible`, `shouldRetry` (predicate; return False to rethrow immediately without further retries, e.g. skip 4xx).

If all attempts fail, the step records the failure and the workflow observes the same `Error`.

Difference from TS: TS uses `retriesAllowed` + `intervalSeconds` + `timeoutMS`. Haskell uses `maxAttempts` (default 1, not 3), `interval :: Duration` (`secondsDuration`/`millisDuration`), `timeout :: Maybe Duration`.
