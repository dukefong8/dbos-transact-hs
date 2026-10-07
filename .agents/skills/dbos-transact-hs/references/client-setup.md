---
title: Initialize Client for External Access
impact: MEDIUM
impactDescription: Enables external applications to interact with DBOS workflows
tags: client, external, setup
---

## Initialize Client for External Access

Use `Client` from outside a DBOS process (API servers, CLIs). It connects directly to the system database — no `launch` needed.

```haskell
cfg <- clientConfigFromEnv
Right client <- connectClient cfg
_ <- enqueueClientWorkflow client "MyWorkflow" "email" (Just (encodeWorkflowValue input))
_ <- closeClient client
```

Options: `clientConfigNew url`, `applicationName` (always set when sharing a database), `systemDatabaseSchemaName`, `observabilityQueryTimeoutMs`, polling concurrency. `Client` mirrors the executor surface: retrieve/status, send/getEvent, cancel/resume/fork, list, queue management, version management.

Difference from TS: TS `DBOSClient.create({ systemDatabaseUrl, applicationName, ... })`. Haskell is `clientConfigNew` / `clientConfigFromEnv` + `connectClient` / `closeClient`.
