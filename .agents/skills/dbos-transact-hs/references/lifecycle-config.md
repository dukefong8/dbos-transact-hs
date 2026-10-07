---
title: Configure and Launch DBOS Properly
impact: CRITICAL
impactDescription: Application won't function without proper setup
tags: configuration, launch, setup, initialization
---

## Configure and Launch DBOS Properly

Every Haskell application must build a `Config`, create `DBOS`, register workflows before `launch`, and `shutdown` on exit.

**Incorrect (missing configuration or launch):**

```haskell
-- No configuration or launch!
myWorkflowBody () wctx = runStep wctx "fetch" (\_ -> fetchData)
-- never launched: steps have no workflow id, recovery never runs
```

**Correct (configure and launch in main):**

```haskell
import DBOS.Transact (configFromEnv, launch, newDBOS, registerDBOSWorkflowRef, newWorkflowKey, shutdown)

main :: IO ()
main = do
  config <- configFromEnv "my-app"
  dbos   <- newDBOS config
  _ref   <- registerDBOSWorkflowRef dbos (newWorkflowKey "MyWorkflow") myWorkflowBody
  exec   <- launch dbos
  -- run workflows via exec ...
  shutdown dbos
```

`Config` fields (see `DBOS.Transact.Config`): `configAppName` (required, owns rows in a shared database), `configDatabaseUrl`, `configAppVersion` (set to `"0.1.0"` in new apps; recovery only resumes its own version), `configExecutorId`, `configSchema` (default `"dbos"`), `configMaxConnections` (default 10), `configMigrate` (default True; set False for least-privilege roles and migrate out of band with `make db-migrate`), `configUseListenNotify`, `configListenQueues` (`Just []` = dequeue nothing; Nothing = all owned queues), `configPollingConcurrency`, `configOutcomePollInterval`, `configNotificationCoalesce`.

Difference from TS: no `DBOS.setConfig` / `dbos-config.yaml`. Haskell builds `Config` explicitly (`configNew` / `configFromEnv`) and calls `newDBOS` + `launch`. `launch` throws on missing name, matching TS `DBOSInitializationError`.
