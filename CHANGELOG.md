# Changelog

## Unreleased

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
