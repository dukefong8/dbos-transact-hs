---
title: Cancel, Resume, Fork, and Rewind Workflows
impact: CRITICAL
impactDescription: Enables operational control over long-running workflows
tags: workflow, cancel, resume, fork, management
---

## Cancel, Resume, Fork, and Rewind Workflows

Use the `Executor`/`DBOS` management entries for operational control. Cancellation stops at the next step; resume restarts from the last completed step; fork copies inputs and step outputs up to a step into a new workflow id.

**Incorrect (no recovery path):**

```haskell
-- Workflow stuck or failed with no handle on it: no cancel, no resume, no fork.
```

**Correct (using cancel, resume, fork, and rewind):**

```haskell
-- Cancel: status becomes CANCELLED, removed from its queue.
_ <- cancelWorkflows dbos [workflowId] False
-- Resume from the last completed step.
_ <- resumeWorkflows dbos [workflowId] Nothing
-- Fork from a step into a new workflow id.
_ <- forkFrom dbos [workflowId] forkPoint forkOptions
-- Rewind in place from a step, under the same id.
_ <- rewindWorkflows dbos workflowId (defaultRewindOptions {rewindStartStep = stepId})
```

Notes:

- A running step is not interrupted; the workflow stops at the start of its next step. Child workflows are not cancelled by default in this port.
- Resume restarts cancelled or `MAX_RECOVERY_ATTEMPTS_EXCEEDED` workflows from the last completed step, and can also start an enqueued workflow immediately, bypassing its queue.
- Forking creates a new workflow with a new id. List steps first to find the step id, then fork from it (useful after downstream outages or bug fixes).

Difference from TS: TS `rewindWorkflow(workflowID, options)` re-executes in place under the same id; Haskell is `rewindWorkflow` (returns a handle), `rewindWorkflows` (returns `()`), and `rewindWorkflowsInWorkflow` (records the call as a step), all taking `RewindOptions`/`defaultRewindOptions`. TS `cancelWorkflow(workflowID, { cancelChildren })` maps to the explicit `Bool` argument on `cancelWorkflows` / `cancelWorkflowsInWorkflow`.
