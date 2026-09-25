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

**System Database**:
The database holding the durable state of one or more applications: workflow executions and their status, operation checkpoints, events, messages, queues, and schedules. Executors sharing it can recover each other's work.
_Avoid_: DB, datastore, store

**Executor**:
A process identity among the executors sharing the system database. Rows written by a process are stamped with its executor id, which is what makes them owned and recoverable by another executor later.
_Avoid_: worker, host

**Application**:
A named group of executors sharing the system database. Rows are stamped with the application name so applications sharing one database own disjoint subsets of rows, while unstamped rows belong to every application.
_Avoid_: tenant, service

**Engine**:
The part of a DBOS application that executes workflow executions, driving their operations, recovery, queues, and waits against the system database. Callers reach a workflow through the engine rather than the database.
_Avoid_: runtime, scheduler, core

**Wait**:
A blocking read that returns once the awaited state settles — a workflow result, an event, or a message. A wait re-reads the system database on the polling interval, so it is correct with polling alone.
_Avoid_: poll, sleep, block

**Polling Interval**:
How often a wait re-reads the database while nothing has woken it. Every blocking read is correct on polling alone; a wakeup only ever shortens the interval.
_Avoid_: timeout when referring to the re-read cadence

**Step Status**:
What a step body may read about the attempt it runs as: which step it is, which attempt is running, and how many attempts the policy allows. Attempts share one step id; the count moves between them.
_Avoid_: retry state, attempt counter

**Workflow Reference**:
What registration returns and what a call site holds: the identity a workflow registered under, without the body. Starting or running through a reference names the workflow by that identity.
_Avoid_: handle when referring to the registration; a handle names a running workflow by id

**Workflow Timeout**:
How long a whole workflow may take: a budget counted from now, a refusal to inherit a parent's deadline, or silence — in which case a child takes its parent's deadline and a root runs unbounded. A budget becomes a wall-clock deadline stored on the row, so it survives a crash; cancelling on expiry, never failing.
_Avoid_: timeout when referring to the re-read cadence (that is the polling interval), step timeout when referring to one attempt
