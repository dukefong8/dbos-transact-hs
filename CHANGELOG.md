# Changelog

## Unreleased

- Rewind replays a finished workflow in place under its own id (`rewindWorkflow`/`rewindWorkflows` plus `rewindWorkflowsInWorkflow`, `clientRewindWorkflows`): steps, event history, consumed-or-parked messages, the completed payload, and stream closes from the cut on are discarded in one transaction guarded on the status read, then the row is re-enqueued. Only terminal rows rewind (`WorkflowNotRewindable`/`RewindInterrupted`); the executor drops datasource checkpoints first while the client skips them, as Python documents. The caller contract is typed (`CallerStep`: verified replay/fresh/refusal shared by both backends) with the oracle's should-record gate — recordable failures commit a failure step, control failures condemn — and engine-layer validation reports `InvalidArgument` while the backends keep `InvalidInput`. The client hands back the polled handle, the announcement carries the cut and queue, and the options split into engine (`RewindOptions`) and sysdb (`SysRewindOptions`) halves as the oracle does. A lost status race records its failure through a separate session. Needs migrations 115–121 (notification `consumed_by_function_id`); the ceiling pin moves 114→121.
- Datasource checkpoints clear on every non-kill completion — direct, queued, supervised, and inline-child — before the outcome commits, as Python specifies; kills and control ends keep theirs for recovery to adopt. The clearance rides the task spawner, so a child inherits its parent's channel with no body-level signature change. The widget exactly-once instrument reads an append-only commit log instead of the table.
- A send replays through the shared `CallerStep` check like every other named call: a step already recorded under the send's own name is the replay, while a position holding another name (the workflow's code changed between runs) is refused with `UnexpectedStep` instead of delivered. The sysdb send reports that refusal as a value and condemns, so the caller's transaction rolls back with it; the sim backend reaches the same verdict through the extracted `verifyCallerStep`.

- Client sends join caller-owned system transactions (`withClientTransaction` runner plus `clientSendMessagesInTx`/`clientSendMessageInTx` wrappers): caller writes and sends commit together or not at all, validated up front, fork-expanded inside the transaction. The client carries its transaction runner from connect time; raw batch pieces stay Backend-internal, as Python keeps its connection-level send private.

- `transactionConfigDefault` runs `Serializable` like the Python datasource default; an explicit level still reaches the database verbatim. Bare `BEGIN` is gone from the runner (callers wanting the database default say `ReadCommitted`).
- Transactional steps retry connection-class failures as well as the SQLSTATE serialization class; the checkpoint dedup converges a retried ambiguous commit to adoption.

- Stream pulls page (100 values per blocking read, each still recorded and delivered one pull at a time) and own one step id across wait slices, so a replay finds the row where the pull recorded it; the sim backend takes the same final look after a terminal producer as Postgres.
- The stream close sentinel is stored JSON-encoded (`"__DBOS_STREAM_CLOSED__"` quoted, `portable_json`), matching what Rust and Python write and detect — the old build stored it bare. Rows written by the old build keep the bare form.
- Stream write offset races retry 16 times (`STREAM_OFFSET_ATTEMPTS`), not 8.

- `TRACE_LEVEL` (`debug`/`info`/`warning`/`warn`/`error`, case-insensitive) sets the FastLogger backend's floor: an event below it is dropped before any formatting or thread-id lookup — the null path for a level nobody asked for. Unset, blank or unrecognised keeps logging everything.
- Tracer lines name their event's constructor (`[Debug] StepRunning: …`); FastLogger lines also carry the emitting `ThreadId`, pre-formatted once per thread in a capped hash-map cache (fast-logger has no thread support to reuse; per-line `show`+`pack` measured ~59 ns against a ~9–11 ns cache hit).
- Merged `DBOS.SystemDB.{Hasql,Queries}` into `DBOS.SystemDB.Postgres` (typedSql sessions + pool runners, one module).
- Fixed replayed `sleepStep` waiting out the recorded remainder (`reads` on the bare stored text, not its `show`).
- Scoped generated message ids per recipient (`fallback::destination`), reusing `messageUUIDForSend` in the batch insert.
- `dequeuePass` releases (instead of parking) claims it skips: unregistered names and unreadable rows return to `ENQUEUED`.
- `launchOnWithQueues` installs the listen-filtered executor into the instance rather than returning a filtered copy of an unfiltered install: the dequeue sweep and a later `launchExecutor` see the listen set, so a filtered launch stops polling every queue row in the database.
- `runWorkflow` replays recorded `SUCCESS`/`ERROR` outcomes and loses foreign-owned claims instead of re-running or stealing them.
- Infrastructure failures (`DbosDbError`) and all `AsyncException`s escape `runWorkflow` without recording an `ERROR` outcome.
- `takeNotificationSession` tolerates a lost take race (`ON CONFLICT DO NOTHING`); the loser reads back the winner's record.
- `updateWorkflowOutcome` records the running executor's id instead of `'local'`.
- `fetchWorkflowStatuses` skips rows with unknown statuses instead of failing the batch.
- `Executor` task tracking is race-safe; `shutdownExecutor` drains late arrivals; the supervisor re-checks close per queue and logs (not swallows) per-queue failures.
- Deferred, recorded: single-row decode failures still throw (`fetchWorkflowStatus`, `fetchOperationCheckpoint`); `enqueueWorkflow` carries no inputs/version (Phase 7); blocking reads have no backoff retry; the `OperationCheckpointStore` seam is `IO`-locked (no io-sim instantiation); the sleep output tag (`portable_json`) is unread cross-language.

## 0.1.0.0

- Replaced the initial scaffold with the `DBOS.*` module tree.
- Added the `DBOS.Transact` public facade for workflow execution parsing, operation checkpoint parsing/replay, notification value types, and Bluefin-scoped store capabilities.
- Added the `DBOS.SystemDB` Hasql-backed facade for Python-compatible `workflow_status`, `operation_outputs`, and `notifications` access.
- Added Tasty coverage for schema records, row parsing, scoped capability replay, live Hasql reads/writes, and the mirrored sync simple workflow.
