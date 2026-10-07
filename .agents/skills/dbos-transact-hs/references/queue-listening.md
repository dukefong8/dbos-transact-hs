---
title: Queue Listening
impact: HIGH
impactDescription: Control which queues this process drains
tags: queue, listening, shared-database
---

## Queue Listening

`configListenQueues` decides which queues a process dequeues from: `Nothing` drains all owned queues; `Just []` drains none (HTTP front-ends, external starters); `Just ["a","b"]` drains only those. Names not matching a queue at launch are picked up when registered.

```haskell
config <- configFromEnv "my-app"
let config' = config { configListenQueues = Just ["email"] }
```

Always set an app name plus `configListenQueues` when several applications share one system database, so processes do not steal each other's queued rows.

Difference from TS: TS field is `listenQueues: string[]`. Haskell is `configListenQueues :: Maybe [Text]`.
