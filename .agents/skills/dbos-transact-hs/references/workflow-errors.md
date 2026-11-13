---
title: Give Workflows a Recordable Error Type
impact: HIGH
impactDescription: An error type that cannot cross a row cannot replay
tags: workflow, errors, DurableError
---

## Give Workflows a Recordable Error Type

Every workflow and step function returns its application error under the `DurableError` bound, which is `ToJSON` plus `FromJSON`. The engine records the error and replays it as the same typed error. Use `EngineOnly` (the uninhabited default) where no application error can occur.

```haskell
data CheckoutError = OutOfStock Text
  deriving stock (Eq, Show, Generic)

instance ToJSON CheckoutError
instance FromJSON CheckoutError

checkout :: Text -> m (Either (Error CheckoutError) Text)
```

A recorded error the build cannot read keeps the previous generic failure instead of failing the replay. A body that throws propagates: the failure is recorded at the workflow level, never as a step error row, so a recovery re-runs it rather than replaying a verdict it never reached.

Difference from Rust: Rust bounds the error with `thiserror` plus `Serialize` and `Deserialize`. Haskell bounds it with `ToJSON` and `FromJSON` under one synonym.
