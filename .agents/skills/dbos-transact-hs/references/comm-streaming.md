---
title: Stream Progress Out of a Workflow
impact: MEDIUM
impactDescription: Report progress and partial results while the workflow still runs
tags: communication, stream
---

## Stream Progress Out of a Workflow

A workflow appends typed values to a named stream with `writeStream` and seals it with `closeStream`. Any reader pulls the values in order while the workflow still runs. Use this for progress reporting and partial results.

```haskell
_ <- writeStream wctx "progress" ("chunk" :: Text)
_ <- closeStream wctx "progress"
reader <- readStream wctx wid "progress" defaultReadStreamOptions
values <- runExceptT (foldVectorStream (Foldl.generalize Foldl.list) (toVectorStream reader))
```

A write from the workflow body is its own step and replays skip it. A write from inside a step is plain and carries the enclosing step id, so a rerun of that step writes again. Nothing outside a workflow can write. A reader built in a body with `readStream` or `readStreamFrom` checkpoints every pull. A replay replays the recorded values and stops where the first run stopped. `readStreamFrom` resumes at an offset. A negative offset is refused.

Each pull returns `Right (Just value)`, `Right Nothing` when the stream is closed or ended, or `Left` on error. `ReadStreamOptions` carries `pollingInterval` and `timeout`, both `Maybe Duration`. A missing polling interval polls every second, and intervals under a millisecond are refused. A missing timeout waits as long as the producer runs. A set timeout bounds each gap between values and restarts on every delivery. On expiry the pull records `StreamTimeout` and returns it, ending the reader.

Outside a workflow body, reads never checkpoint. Use `readStreamOutside` on an executor connection, or `clientReadStream` and `clientReadStreamFrom` on a client. If the caller is cancelled, the reader ends with an error.

Difference from TS: TS reads with `for await` and throws `DBOSStreamTimeoutError` on expiry. Haskell pulls through `toVectorStream` and `foldVectorStream` and returns the `StreamTimeout` error as a value. The single-value read is `readStreamFrom` with an offset rather than a separate `readStreamOffset`.
