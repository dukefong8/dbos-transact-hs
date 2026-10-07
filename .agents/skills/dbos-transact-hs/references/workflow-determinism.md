---
title: Keep Workflows Deterministic
impact: CRITICAL
impactDescription: Non-deterministic workflows cannot recover correctly
tags: workflow, determinism, recovery, reliability
---

## Keep Workflows Deterministic

A workflow body must invoke the same steps in the same order given the same inputs and step return values. Move randomness, clocks, external I/O, and file reads into steps.

**Incorrect (non-determinism in workflow):**

```haskell
myBody () wctx = do
  -- Random branch in the body: replay takes a different branch.
  choice <- randomIO :: IO Bool
  if choice then runStep wctx "a" (\_ -> stepOne)
            else runStep wctx "b" (\_ -> stepTwo)
```

**Correct (non-determinism in a step):**

```haskell
myBody () wctx = runExceptT $ do
  -- Checkpointed: replay reuses the recorded value.
  choice <- ExceptT (runStep wctx "generateChoice" (\_ -> randomIO :: IO Bool))
  ExceptT $ if choice then runStep wctx "a" (\_ -> stepOne)
                      else runStep wctx "b" (\_ -> stepTwo)
```

The step name is part of the durable identity: reordering or renaming steps changes replay. Never call, start, or enqueue workflows from inside a step body; never mutate globals from bodies. Steps called outside a workflow run as plain calls with no checkpoint.
