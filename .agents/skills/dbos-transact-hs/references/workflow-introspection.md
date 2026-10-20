---
title: Workflow Introspection
impact: CRITICAL
impactDescription: Observe workflows from inside and outside
tags: workflow, introspection, listing
---

## Workflow Introspection

Inside: `workflowId` reads the current id. Outside: `retrieveWorkflow` fetches one record; `listWorkflows` / `fetchWorkflowStatuses` scan; `listWorkflowIdsByName` filters by name; `listWorkflowSteps` reads one workflow's steps in execution order with their outputs and errors.

```haskell
wid <- workflowId wctx
Right statuses <- fetchWorkflowStatuses dbos [wid]
Right records  <- listWorkflows dbos workflowFilter
Right steps    <- listWorkflowSteps dbos wid          -- [StepInfo], execution order
```

Handles: `handleStatus` / `handleResult` poll a `WorkflowHandle`; `pollingHandle` builds one from an id. `selectWorkflow` / `waitForWorkflow` / `waitForFirstWorkflow` / `waitForWorkflows` / `joinWorkflows` coordinate fan-out.

Difference from TS: TS `listWorkflows` / `listWorkflowSteps` / `handle.getStatus`. Haskell splits status fetch (`fetchWorkflowStatuses`), listing (`listWorkflows`), and step listing: `listWorkflowSteps` outside a workflow, `listWorkflowStepsInWorkflow` from a body (records itself as the step `DBOS.listWorkflowSteps`, so a replay reads the recorded snapshot), `clientListWorkflowSteps` from a client (records nothing). Every listing returns the typed `StepInfo` (`stepInfoName`, `stepInfoId`, `stepInfoOutcome`): `StepOutcomeNotLoaded` when `loadOutput = False`, `StepOutcomePending` for an unfinished row with no payload, `StepOutcomeSucceeded` with `PayloadAbsent`/`PayloadDecoded`/`PayloadUndecodable`, or `StepOutcomeFailed` with a portable or undecodable error. The Rust-shaped `StepRecord` row is internal to the engine and tests.
