# DBOS Transact Haskell

This context defines the domain language for the Haskell DBOS implementation. It describes DBOS concepts, not Haskell modules, database columns, or implementation strategy.

## Language

**Workflow Execution**:
A durable invocation of a registered workflow. It has one identity and one lifecycle, even when execution is resumed, recovered, queued, or awaited.
_Avoid_: workflow row, job, task

**Workflow Status**:
The lifecycle state of a workflow execution. A status says whether the execution is active, terminal, queued, delayed, cancelled, or unrecoverable.
_Avoid_: state, phase

**Workflow Operation**:
A deterministic operation performed inside a workflow execution. Operations are ordered within the workflow so recovery can compare the current execution against prior history.
_Avoid_: step when referring to all operation kinds

**Operation Checkpoint**:
The durable record of a workflow operation's observed result. A checkpoint may represent a successful result, an error, a child workflow link, or an awaited workflow result.
_Avoid_: operation output, step output, log entry

**Serialized Workflow Value**:
A workflow value represented in DBOS' durable interchange format before it is decoded into an application value. It includes both the stored payload and the serialization format needed to interpret it.
_Avoid_: JSON blob, raw text, payload

**Child Workflow**:
A workflow execution started from another workflow execution. The child has its own lifecycle, while the parent records a link to it as part of its operation history.
_Avoid_: subtask, nested workflow

**Recovery Attempt**:
An attempt to execute a workflow after it already has durable state. Recovery attempts are counted so workflows can be moved to an unrecoverable state after too many failed attempts.
_Avoid_: retry when referring to whole-workflow recovery
