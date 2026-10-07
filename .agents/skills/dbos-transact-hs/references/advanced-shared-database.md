---
title: Sharing One System Database
impact: LOW
impactDescription: Multiple applications on one Postgres schema set
tags: advanced, shared-database
---

## Sharing One System Database

`configAppName` owns rows: workflows, queues, versions. Processes only run their own application's workflows. Always set distinct names plus `configListenQueues` per process.

```haskell
config <- configFromEnv "my-app"   -- owns my-app rows
```

Clients act on behalf of an app via `applicationName`. Recovery only resumes the configured `configAppVersion`. Least-privilege roles set `configMigrate = False` and migrate out of band (`make db-migrate`).

Difference from TS: TS `applicationName` on `DBOSClient`. Haskell is `configAppName` / client `applicationName`, same semantics.
