---
title: Queue Basics
impact: HIGH
impactDescription: Concurrency control for workflows
tags: queue, basics, register
---

## Queue Basics

Register a database-backed queue before use, then enqueue workflows by name. Registration is stored in the system database; do it after `launch` is allowed here (queues live in the DB, unlike workflow registration which must precede launch).

```haskell
q <- registerQueue dbos "email" defaultQueueOptions NeverUpdate
_ <- enqueueDBOSWorkflow dbos (newWorkflowKey "SendEmail") workflowId (Just (encodeWorkflowValue input)) "email"
```

`registerQueue dbos name options conflict`: `conflict` decides what happens when the queue already exists (`NeverUpdate` keeps the stored limits). `enqueueDBOSWorkflow` joins an existing workflow when the id matches instead of starting a duplicate.

Difference from TS: TS `new WorkflowQueue(name)` / `DBOS.registerQueue` class form does not exist here. Haskell uses `registerQueue` + `defaultQueueOptions` record updates.
