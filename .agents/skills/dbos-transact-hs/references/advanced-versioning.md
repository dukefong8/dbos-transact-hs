---
title: Application Versioning
impact: LOW
impactDescription: Deploy new code without resuming old work by accident
tags: advanced, versioning
---

## Application Versioning

`configAppVersion` tags every workflow this process creates. Recovery only resumes workflows with its own version. Set it explicitly per deploy (`"0.1.0"` for new apps); leaving it `Nothing` derives behavior from the environment.

```haskell
let config' = config { configAppVersion = Just "0.1.0" }
```

Clients manage versions: `clientListApplicationVersions`, `clientLatestApplicationVersion`, `clientPromoteVersion` (rollback). Fork accepts an overriding version to move a workflow onto fixed code.

No patching API in this port: TS `DBOS.patch` / `enablePatching` has no Haskell equivalent. Version + fork is the upgrade path.
