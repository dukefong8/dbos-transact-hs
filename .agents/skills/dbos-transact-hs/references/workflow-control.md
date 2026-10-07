---
title: Cancel, Resume, and Fork Workflows
impact: CRITICAL
impactDescription: Enables operational control over long-running workflows
tags: workflow, cancel, resume, fork, management
---

## Cancel, Resume, and Fork Workflows

Use the `Executor`/`DBOS` management entries for operational control. Cancellation stops at the next step; resume restarts from the last completed step; fork copies inputs and step outputs up to a step into a new workflow id.

**Incorrect (no recovery path):**

```haskell
-- Workflow stuck or failed with no handle on it: no cancel, no resume, no fork.
```

**Correct (using cancel, resume, and fork):**

```haskell
-- Cancel: status becomes CANCELLED, removed from its queue.
_ <- cancelWorkflows exec [workflowId]
-- Resume from the last completed step.
_ <- resumeWorkflows exec [workflowId]
-- Fork from a step into a new workflow id.
_ <- forkFrom exec [workflowId] forkPoint forkOptions
```

Notes:

- A running step is not interrupted; the workflow stops at the start of its next step. Child workflows are not cancelled by default in this port.
- Resume restarts cancelled or `MAX_RECOVERY_ATTEMPTS_EXCEEDED` workflows from the last completed step, and can also start an enqueued workflow immediately, bypassing its queue.
- Forking creates a new workflow with a new id. List steps first to find the step id, then fork from it (useful after downstream outages or bug fixes).

Difference from TS: TS documents `DBOS.rewindWorkflow` (re-execute in place, same id). This port has no rewind entry — `forkWorkflows` / `forkFrom` are the available recovery copies. TS `cancelWorkflow(workflowID, { cancelChildren })` has no `cancelChildren` option here.
