---
title: Workflow Introspection
impact: CRITICAL
impactDescription: Observe workflows from inside and outside
tags: workflow, introspection, listing
---

## Workflow Introspection

Inside: `workflowId` reads the current id. Outside: `retrieveWorkflow` fetches one record; `listWorkflows` / `fetchWorkflowStatuses` scan; `listWorkflowIdsByName` filters by name.

```haskell
wid <- workflowId wctx
Right statuses <- fetchWorkflowStatuses exec [wid]
Right records  <- listWorkflows exec workflowFilter
```

Handles: `handleStatus` / `handleResult` poll a `WorkflowHandle`; `pollingHandle` builds one from an id. `selectWorkflow` / `waitForWorkflow` / `waitForFirstWorkflow` / `waitForWorkflows` / `joinWorkflows` coordinate fan-out.

Difference from TS: TS `listWorkflows` / `listWorkflowSteps` / `handle.getStatus`. Haskell splits status fetch (`fetchWorkflowStatuses`), listing (`listWorkflows`), and step listing via system-DB checkpoints.
