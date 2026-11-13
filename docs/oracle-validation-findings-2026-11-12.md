# Oracle validation findings (2026-11-12)

Line-level audit of the port's internal engine against the pinned oracles:

- **Rust 0.6.0** (`git tag v0.6.0`, archived read-only; primary code oracle)
- **TypeScript 5.4** (`git tag v5.4`; datasource and client surfaces where Rust is silent)
- **Python 3.4.0** (`git tag 3.4.0`; datasource and client surfaces where Rust is silent)

The port's `AGENTS.md` was previously pinned to Rust v0.5.0. This audit uses v0.6.0,
so findings marked "0.6 delta" are changes the port has not adopted yet.

## Reproducibility

The full suite was run twice, back to back, against the same live database:

| run | result | duration | workflow_status rows | notifications rows |
|---|---|---|---|---|
| 1 | `All 1098 tests passed` | 64.00 s | 679 | 49 |
| 2 | `All 1098 tests passed` | 63.29 s | 679 | 49 |

The second run's durable-state read-back used its own start timestamp:

```sql
select 'workflow_status', count(*) from dbos.workflow_status
where created_at >= 1794535655917
union all
select 'notifications', count(*) from dbos.notifications
where created_at_epoch_ms >= 1794535655917;
-- workflow_status|679
-- notifications|49
```

Status mix from the same window: `SUCCESS 311, PENDING 182, ENQUEUED 129, ERROR 28,
CANCELLED 24, DELAYED 4, MAX_RECOVERY_ATTEMPTS_EXCEEDED 1`.

Other gates: watcher green (`All 102 tests passed`, `All good (129 modules)`);
`make neg` green (5 negatives fail with the accepted classes, 5 witnesses build).

## Findings

Severity: **BUG** = behavioral divergence from the governing oracle; **DEVIATION** =
shape or API difference without an ADR; **DOC-GAP** = behavior undocumented;
**TEST-GAP** = the suite does not exercise the behavior; **OK** = verified parity.

### Transactional steps and datasource

| # | Sev | Finding | Haskell | Oracle | Status |
|---|---|---|---|---|---|
| T1 | BUG | A recorded body failure commits the body's partial application writes. The error checkpoint is inserted into the still-open transaction, then the transaction COMMITs, so rows written before the body returned `Left` are durable. Both oracles roll the body back and record the error in a new transaction. | `Datasource.hs:302-318`, `:164` | Python 3.4 `_datasource.py:73-78, 904-964`; TS 5.4 `packages/postgres-datasource/index.ts:190-208, 138-147` | OPEN |
| T2 | BUG | Completion cleanup deletes `transaction_completion` rows with no ownership guard. A superseded execution that finishes normally can wipe the new owner's checkpoints, causing duplicate writes. The seam has no owner argument. It also clears every registered datasource, not only the ones the workflow used. | `Workflow.hs:421-423, 480-484, 490`; `Datasource.hs:573-582`; `Datasource/Postgres.hs:268-269` | Python 3.4 `_datasource.py:748-760`, `_workflow_commands.py:179-196`; TS 5.4 `workflow_management.ts:144-167` | OPEN |
| T3 | BUG | Rewind deletes datasource checkpoints before validating options and before the active-status refusal. Rewinding a running workflow destroys its checkpoints and then refuses; `rewindStartStep = -1` wipes every row and then refuses. | `Instance.hs:760-768`; `Management.hs:470-475`; `Statements.hs:1344-1351` | Python 3.4 `_workflow_commands.py:161-176, 246-253`; TS 5.4 `workflow_management.ts:79-93, 104-129` | OPEN |
| T4 | BUG | The default transaction step name is the literal `"transaction"`, not the call site. Two unnamed transactions at adjacent slots both record the same name, defeating the reorder guard the ADR addendum exists to provide. `HasCallStack` is constrained but never read. | `Datasource.hs:245, 115-124, 261, 383` | Python 3.4 `_datasource.py:91-98`; TS 5.4 `datasource.ts:207` | OPEN |
| T5 | DEVIATION | Cross-kind slot reorders are unguarded: a transaction at a slot never consults `operation_outputs`, and a step at a slot never consults `transaction_completion`. Step-to-transaction and transaction-to-step swaps run silently. | `Datasource.hs:250-265`; `Step.hs:187-195` | Python 3.4 `_core.py:2488-2519, 2550-2555`; TS 5.4 `dbos-executor.ts:1253-1258`, `datasource.ts:171-181` | OPEN (T6 should close it) |
| T6 | BUG | Transactional steps are absent from `operation_outputs`, hence from `listWorkflowSteps`, and leave step-id gaps. A finished workflow retains no record of its transactions. A fork copies `operation_outputs` only, so a transaction below the fork point re-executes and duplicates application writes. | `Datasource.hs:251, 573-582`; `Statements.hs:1758-1780`; `Types.hs:1638-1644`; `Step.hs:187-195` | Python 3.4 `_core.py:2455-2486`, `_datasource.py:560-573`, `_sys_db.py:2425`; TS 5.4 `datasource.ts:171-181`, `dbos-executor.ts:1263-1274`, `workflow_management.ts:56-72` | OPEN |
| T7 | DOC-GAP | `step_name` is a Haskell-only column in `transaction_completion`. A TS-provisioned datasource schema (no such column) makes every checkpoint insert fail with `42703`. The ADR records the divergence; the cross-language schema doc and the demo schema comment do not. | `Datasource/Postgres.hs:210-219`; `demo-apps/dbos-hs-widget-store/schema.sql:32-44`; ADR-0021:125-137 | Python 3.4 `_schemas/datasource_database.py:14-30`; TS 5.4 `datasource.ts:362-372` | Documented in `cross-language-schema-interop.md` (Known divergences) and both demo schemas |
| T8 | DOC-GAP | `runTxOutside` retries ambiguous commits with no checkpoint to deduplicate, so a lost-COMMIT-ack retry re-runs the body and double-applies. Python 3.4 has the same broad retry; TS 5.4 retries only `40001`. The facade says only "same retry loop, nothing checkpointed". | `Datasource.hs:505-529, 473-475` | Python 3.4 `_datasource.py:574`; TS 5.4 `packages/postgres-datasource/index.ts:210-221` | Documented on `runTxOutside` (retry hazard note) |
| T9 | TEST-GAP | The sim fake cannot model rollback: `dsWithTransaction` returns `Right <$> action fakeTx` and checkpoint writes land in the STM map immediately. Ownership-loss rollback is therefore unverified by the watcher. | `test/DBOS/Transact/DatasourceCases.hs:162-168, 169-193, 359-370` | ADR-0020 | OPEN (needed for T1) |

### Workflow lifecycle, recovery, completion, management

| # | Sev | Finding | Haskell | Oracle | Status |
|---|---|---|---|---|---|
| L1 | BUG | Same root cause as T6, lifecycle view: `listWorkflowSteps` omits every transactional step; completion cleanup leaves no record of them. | see T6 | see T6 | OPEN |
| L2 | BUG | Fork re-executes transactional steps below the fork point, duplicating application writes. | `Statements.hs:1758-1780`; `Datasource.hs:251`; `Types.hs:1638-1644` | Python 3.4 `_sys_db.py:2425`; TS 5.4 `workflow_management.ts:56-72` | OPEN (T6) |
| L3 | BUG | Same root cause as T2, lifecycle view: unguarded, all-datasource completion clear. | see T2 | see T2 | OPEN |
| L4 | BUG | Same root cause as T3, lifecycle view: rewind deletes before the rewindability check. | see T3 | see T3 | OPEN |
| L5 | BUG | A dequeued execution that lost its claim polls for the winner while holding the queue's worker-concurrency slot. Rust drops the slot first; under `worker_concurrency` this can deadlock. | `Workflow.hs:494-509` vs `:358-365, 523-525`; `Dequeue.hs:531-543` | Rust 0.6 `workflow.rs:2450-2468, 2479-2488` | OPEN |
| L6 | DEVIATION (refuted on re-read: verified parity, no change) | The 0.5 fork-from API (`ForkLastFailure`, `ForkLastStep`, `ForkStepNamed`) is retained; the finding read the 0.6 engine path (`Fork { start_step }`, `management.rs:243-266`) as a removal, but 0.6 keeps `ForkPoint` with all four variants (`sysdb/types.rs:2948`) for `fork_from`, and the port mirrors both paths (`Fork`/`forkWorkflows` for the engine call, `ForkPoint`/`forkFrom` for the Conductor-style call). No superset, no break. | `Types.hs:1709-1714`; `Backend.hs:3351`; `Instance.hs:741-752` | Rust 0.6 `management.rs:243-266`; commit `a568709` (#92) | CLOSED 2026-11-13 (no deviation) |
| L7 | DOC-GAP | Schedules are deferred; the facade has no `Schedule*` export. | ADR-0036 | Rust 0.6 `schedule.rs:548-880` | Documented in the `DBOS.Transact` facade (`Deferred` section) |
| L8 | BUG | `setWorkflowDelay` and `updateWorkflowAttributes` accept a caller and ignore it. The engine wraps them in `runStepWith`, which checks, runs, then records, so the write is unfenced and not atomic with its checkpoint. A superseded execution can still move the delay or rewrite attributes. | `Backend.hs:3151, 3168`; `Statements.hs:2391-2398`; `Management.hs:580-591, 640-655`; `Step.hs:398-452` | Rust 0.6 `sysdb/mod.rs:280-330`, `postgres.rs:4296-4311, 1232-1285` | OPEN |

### Steps, sleep, events, messages

| # | Sev | Finding | Haskell | Oracle | Status |
|---|---|---|---|---|---|
| S1 | BUG (0.6) | Recorded step error replay ignores the stored `serialization` tag. An unreadable tag is mis-taxonomised as a text decode/`StepFailed`; a native-tag error can be mis-decoded. The output path passes the tag; the error path does not. | `Step.hs:249-252` | Rust 0.6 `checkpoint.rs:456-478` | OPEN |
| S2 | BUG (0.6) | An unencodable step result is not recorded; the exception escapes and the id is spent. Rust 0.6 records the encoding refusal as the step's error and does not re-run the body. | `Step.hs:201-216, 421-438`; `Serialization.hs:166-171` | Rust 0.6 `step.rs:530-555` | OPEN |
| S3 | BUG | `send`/`sendBulk` do not force the payload encoding before taking the step id. A throwing `ToJSON` spends the id and escapes; Rust encodes the whole batch first and reports a refusal with no id spent. | `Message.hs:74-77, 93, 97, 117-122, 130` | Rust 0.6 `message.rs:155-173, 207-231, 465-512, 584-650` | OPEN |
| S4 | BUG | The remaining-wait conversion overflows and truncates. A wake more than ~292 years away makes `milliseconds * 1000` wrap; a negative value makes `threadDelay` return immediately; sub-millisecond truncation wakes up to 1 ms early. | `Sleep.hs:104-108, 120-121` | Rust 0.6 `sleep.rs:116-124` | OPEN |
| S5 | BUG | The workflow-wide `stepDepth` makes concurrent durable calls plain. A sibling step sees depth 1 and runs uncheckpointed; a concurrent `send`/`sleepStep` goes plain; a concurrent `recv`/`setEvent` returns `InsideStep`. Rust uses per-call-stack `in_step`. | `Context.hs:214-223, 287-288, 397-398, 536`; `Step.hs:179-187`; `Message.hs:88-94` | Rust 0.6 `context.rs:448-456, 470-494` | FIXED 2026-11-13 |
| S6 | BUG | Pending drives never consult the drive-time step scope; both `StepBuiltElsewhere` directions are bypassed. The `here` passed to `checkHere` is always the build-time boundary. | `Step.hs:388`; `Sleep.hs:85`; `Event.hs:62`; `Handle.hs:168`; `Patch.hs:129`; `Event.hs:183-185`; `Checkpoint.hs:165-167, 177-181` | Rust 0.6 `checkpoint.rs:387-414, 713-760`; `step.rs:1364-1390, 1439-1480` | FIXED 2026-11-13 |
| S7 | BUG | A body exception seen after the timeout arm wins is recorded as `StepTimeout`; the exception is discarded. Rust propagates the panic and records nothing. | `Step.hs:519-522, 530-534` | Rust 0.6 `step.rs:704-740` | OPEN |
| S8 | BUG | `sendBulk` commits the messages and the step record in two transactions. A crash between them leaves the batch durable with no step row; recovery re-runs the body and sends the batch again. Rust records the step last in the same transaction. | `Message.hs:123-143`; `Step.hs:426-438`; `Backend.hs:2617-2626` | Rust 0.6 `postgres.rs:8450-8466, 8275-8470` | FIXED 2026-11-13 |
| S9 | BUG | Send-to-forks recipient set is computed outside the send transaction, so a fork created between the walk and the insert never receives the message. Rust walks the forks inside the transaction. | `Backend.hs:2543, 2573, 2618` | Rust 0.6 `postgres.rs:8355-8362` | FIXED 2026-11-13 |
| S10 | DEVIATION (0.6) | Missing `SetEventOptions`/`set_event_with` and `SendOptions.serialization`; the port always writes `portable_json`. | `Message.hs:51-54`; `Event.hs:32-36`; `Config.hs:63-64` | Rust 0.6 `message.rs:827-839`, `event.rs:76-127`; Python 3.4 `_dbos.py:2195-2200`; TS 5.4 `dbos.ts:309-315` | Documented in the `DBOS.Transact` facade (`Deferred` section; single portable codec per ADR-0038) |
| S11 | DEVIATION | The `PendingStep` surface is incomplete: no pending `send`/`recv`, no free pending `getEvent`, no public await/join. | `Message.hs:62-178`; `Transact.hs:141-154, 227-233`; `Select.hs:216-220, 256-259`; `Event.hs:150-172` | Rust 0.6 `message.rs:122-322`, `event.rs:177-202`, `checkpoint.rs:195-330`, `step.rs:240-284` | Documented in the `DBOS.Transact` facade (`Deferred` section; eager calls checkpoint the same rows) |
| S12 | DEVIATION | A recorded child-workflow checkpoint is refused as a step replay (`StepUnexpectedChildWorkflow` unused). Rust ignores `child_workflow_id` and decodes output/error. | `Step.hs:247-248` | Rust 0.6 `step.rs:403-411` | FIXED 2026-11-13 |
| S13 | BUG | `operation_outputs.application_name` is left NULL on six step-writing paths (recv, send, setEvent, getEvent, management-call caller step, rewind failure record). Rust always binds it. | `Statements.hs:2123, 2222, 2316, 2381, 1518`; `Backend.hs:3417` | Rust 0.6 `postgres.rs:2642` | FIXED 2026-11-13 |
| S14 | DEVIATION | `workflow_status.updated_at` is bound from the application clock at init while `created_at` comes from `now()`. Under clock skew `updated_at < created_at` is possible. | `Backend.hs:1400, 2981, 3065`; `Statements.hs:679` | Rust 0.6 `postgres.rs:3413-3414, 3330-3345` | FIXED 2026-11-13 |

### Queues and dequeue

| # | Sev | Finding | Haskell | Oracle | Status |
|---|---|---|---|---|---|
| Q1 | BUG | Client `updateQueue` skips the in-lock merged validation (no-op validator), so two concurrent client updates can store a limit pair neither asked for. | `Client.hs:678`; engine `Queue.hs:394-405` | Rust 0.6 `queue.rs:615-619, 784-832`, `postgres.rs:7430-7458` | OPEN |
| Q2 | BUG | Client `updateQueue` still refuses a partition-touching update on a legacy `partition_queue` row; Rust 0.6 and the engine path accept it. | `Client.hs:658-670`; `Queue.hs:385-389`; dead import `Queue.hs:50`; `ClientTest.hs:1508-1535` | Rust 0.6 `types.rs:2386-2395`, `postgres.rs:2035-2038, 7477-7487`, `queue.rs:308-316` | OPEN |
| Q3 | BUG | Both surfaces judge the merged options against the stale pre-read row before the locked read, so a concurrent operator's change can cause a spurious refusal Rust would not make. | `Queue.hs:380-393`; `Client.hs:653-676` | Rust 0.6 `postgres.rs:7430-7458` | OPEN |
| Q4 | DEVIATION | An empty `QueueChange` writes the row and runs validation instead of doing nothing. A forced `Set True` makes `isQueueUpdateEmpty` unreachable. | `Queue.hs:226-231`; `Types.hs:1129-1136`; `Backend.hs:4100` | Rust 0.6 `queue.rs:1490-1492`, `postgres.rs:7443-7452`; Python 3.4 `_sys_db.py:6639-6642` | OPEN |
| Q5 | DEVIATION | `clientListQueues` does not filter the engine's internal queue; the engine's `listQueues` does. | `Client.hs:596-600` vs `Queue.hs:354-357` | Rust 0.6 `queue.rs:772-781`, `client.rs:413-415` | OPEN |
| Q6 | DEVIATION | `QueueRecord` keeps two legacy fields Rust 0.6 dropped (`priority_enabled`, `partition_queue`). Inert; documented in ADR-0030. | `Types.hs:993-1006, 1010-1016` | Rust 0.6 `types.rs:2355-2381`, `postgres.rs:2035-2038` | Documented in ADR-0030 (legacy columns kept, always `True`) |
| Q7 | DOC-GAP | A peer-contended partition is skipped with no log at all; Rust emits a debug line naming the partition. | `Dequeue.hs:324-331` | Rust 0.6 `dequeue.rs:546-553` | FIXED 2026-11-13 (`ContendedPartitionSkipped` debug event at the skip) |
| Q8 | DOC-GAP | Refusal order differs for a multiply-invalid queue config (rate -> partition_rate -> partition_concurrency -> partition_worker vs Rust's rate -> partition_concurrency -> partition_worker -> partition_rate). Cosmetic. | `Queue.hs:266-281` | Rust 0.6 `queue.rs:398-538` | FIXED 2026-11-13 (partition-rate check moved after the partition bound checks, matching the oracle order) |
| Q9 | DEVIATION | Missing Rust's operator warnings: over-limit `PENDING` counts, partitioning a queue with an unkeyed backlog, contended partition skipped. | queue/dequeue paths | Rust 0.6 `dequeue.rs`, `postgres.rs` | Partially fixed 2026-11-13: contended-partition skip now emits `ContendedPartitionSkipped`, and newly-partitioning updates emit `QueuePartitioned` (both engine and client paths) — over-limit `PENDING` counts stay absent by design, since the port bounds concurrency from local tallies without the per-partition count query. Registration gap closed 2026-11-14: both registration paths pre-read the row and warn via `warnIfPartitioned` (ADR-0044 note); sim asserts the event, live asserts the partitioned row |

### System database types, schema, queries

| # | Sev | Finding | Haskell | Oracle | Status |
|---|---|---|---|---|---|
| Y1 | DEVIATION | `UpsertedQueue` and `UpdatedQueue` have no Haskell counterparts; the before/after pair is collapsed to a `Bool`+after tuple and an after-only return with before demoted to a validation callback. | `Class.hs:135, 142`; `Queue.hs:337` | Rust 0.6 `types.rs:2144-2178`, `mod.rs:1331, 1505` | ADR-0044 |
| Y2 | DEVIATION | `WorkflowRecord` carries an extra `creatorXid` field that Rust's record omits, and it is serialized into the checkpoint. | `Types.hs:599, 2580, 2629` | Rust 0.6 `types.rs:545-698` | ADR-0045 |
| Y3 | DEVIATION | Checkpointed listing JSON is not the oracle's shape: Haskell field names, ISO-8601 `Timestamp`, integer-millisecond `Duration`. Intra-port replay is exact; cross-SDK checkpoint replay is impossible. | `Types.hs:2538-2545, 2547-2554, 2556-2648, 2648-2684` | Rust 0.6 `types.rs:55, 545-698, 1537-1583`; `cross-language-schema-interop.md:151-156` | Documented in `cross-language-schema-interop.md` (listing-checkpoint shape section) |
| Y4 | DEVIATION | Extra types/methods with no oracle counterpart: `SysRewindOptions` (0.6 collapsed to an alias), `DeleteCaller`, `NotificationRow`, `clearQueueAssignment`; `StepCaller` flattens `WorkflowOwner` and drops the codec. | `Types.hs:1813, 1923, 249, 1934`; `Class.hs:67`; `Statements.hs:2404`; `Backend.hs:3162` | Rust 0.6 `types.rs:3069, 2700-2714`; `management.rs:174`; `sysdb/mod.rs:45-1740` | ADR-0045 |
| Y5 | DOC-GAP | Stale comments: the schema note still calls `creator_xid` and `consumed_by_function_id` inert; the migration comment says 123/121; `AppScope` says "Mirrors Rust Applications" where 0.6 uses `AppScope`. | `Types.hs:1288`; schema notes; migration comments | Rust 0.6 `types.rs:1348` | FIXED 2026-11-13 |

## Fix queue

Order chosen by data-integrity impact, then correctness, then parity/bookkeeping.

1. T1 partial-write rollback (with T9 sim rollback fidelity)
2. T2/L3 owner-guarded, used-datasource-scoped completion clear
3. T3/L4 rewind pre-validation
4. L8 caller-fenced delay/attribute management
5. T6/L1/L2/T5 transactional steps in `operation_outputs` plus replay and fork parity
6. L5 release the queue slot before adopting `AlreadyFinished`
7. S8 sendBulk single-transaction record
8. S3 send encoding before id allocation
9. S5 per-call-stack step depth
10. S6 drive-time step scope
11. S7 timeout race propagates the body exception
12. S1 error replay serialization tag
13. S2 unencodable result recorded as the step error
14. T4 call-site default transaction name
15. Q1/Q2/Q3/Q5 client queue update parity
16. Q4 empty queue change is a no-op
17. S9 send-to-forks recipient walk inside the transaction
18. S13 application_name on every step writer
19. S14 DB-clock `updated_at`
20. S4 sleep overflow/rounding
21. Y1/Y2/Y4 type-shape parity or ADR records
22. Y3/Y5 listing shape and stale docs
23. T7 cross-language datasource schema warning
24. T8 runTxOutside retry documentation
25. L6/L7/S10/S11/S12 deferred API documentation or implementation

No commit is made until the user invokes `/review`.

## Fix status (2026-11-13)

Full suite after the fixes: **All 1099 tests passed (63.57 s)**, plus the watcher sims
(102/102) and `make neg` green. No commit yet.

Re-gated 2026-11-13 on the final tree (S12, Q6-Q9, facade Deferred section):
**All 1103 tests passed (63.20 s)**; read-back over the run window shows
694 `workflow_status` rows and 52 `notifications` rows with a sane status
mix (`SUCCESS` 308, `PENDING` 200, `ENQUEUED` 128, plus the terminal
states). No commit yet.

### Fixed

| ID | Fix | Verification |
|---|---|---|
| T1 | A body that returns `Left` now rolls the attempt back (stashed outcome + `TxBodyRollback`) and records the error in a second transaction, matching Python/TS. | New live test `a returned body failure rolls its writes back`; suite green. |
| T2/L3 | Completion cleanup runs inside the datasource transaction and asks the sysdb whether the execution still owns the workflow; a lost owner rolls the delete back. | Callback seam `dsDeleteCheckpoints ... (m Bool)`; suite green. |
| T3/L4 | Rewind validates the start step, existence, and active status before touching checkpoints. | `rewind refusals name their cause and write nothing` passes; suite green. |
| T4 | Unnamed transactions default to the call site (`transaction at file:line`), not the literal `"transaction"`. | `defaultTransactionName`; suite green. |
| T5/T6/L1/L2 | Transactional steps now write an `operation_outputs` row after the datasource commit; replay checks it first, `listWorkflowSteps` sees it, and a fork copies/replays it. Cross-kind slot swaps are caught by the same name check. | Sim commit/replay traces; suite green. |
| L5 | A superseded execution releases its queue slot before polling the winner. | `AlreadyFinished` path; suite green. |
| L8 | Delay/attribute management calls carry the caller; the backend checks the **calling** workflow's owner (null or matching passes, a different owner is `WorkflowConflict`). The target row is not owner-fenced. | Live management tests green. |
| Q1/Q2/Q3/Q5 | Client queue update uses the in-lock merged validator, no longer refuses legacy partition rows, no longer pre-judges a stale row, and filters the internal queue from its list. | Queue sims and client tests green. |
| Q4 | An empty `QueueChange` is a no-op (legacy flag only set for materialized updates). | Queue sims green. |
| S1 | Recorded error replay honours the serialization tag; an unknown tag is `UnknownSerialization`. | Suite green. |
| S2 | A step result that cannot be encoded is forced before recording and recorded as the step's `ErrorSerialization`; the exception no longer escapes. | Suite green. |
| S3 | `send`/`sendBulk` force payload encoding before taking the step id; a throwing `ToJSON` returns a serialization error without spending the id. | Suite green; `send` gained `MonadCatch`. |
| S4 | Sleep wait clamps/ceils the nanosecond-to-microsecond conversion; huge or negative waits no longer wrap or wake early. | Suite green. |
| S7 | A body exception that lands at the timeout boundary propagates instead of being recorded as `StepTimeout`. | Suite green. |
| S8 | `sendBulk` takes its step id and passes the caller into `sendMessages`, so the batch insert and the `DBOS.sendBulk` step record commit in the same system-database transaction; a replay meets its recorded step and resends nothing. Inside a step it sends plainly like a single send. | New sim case `a replayed batch sends nothing again`; message sims 19/19 green (live gate blocked: system DB unreachable, OrbStack docker daemon wedged). |
| S5 | Step depth is keyed by thread: `insideAStep` reads the calling thread's count, so a sibling step on another thread no longer makes this stack's calls plain, refused, or misrecorded. Same-thread captured-parent calls still see the running body, matching the oracle's per-stack `in_step`. | New sim case `a concurrent call sees outside-step while a step runs`; all 376 sims green (live gate still blocked: system DB unreachable). |
| S6 | Pending builds bind the running body with its marker and pending drives read the calling thread's scope, so a recorded call carried into a step is refused and a same-body call still goes plain — the oracle's ambient read on both sides. | New sim case `a recorded pending driven inside a step is refused`; all 377 sims green (live gate still blocked: system DB unreachable). |
| S9 | `deliverTo` walks `descendantForksTx` inside the `send_messages` transaction and commits the walk, the insert, and the step record together; a fork committed before the transaction starts is walked with it. The outside-transaction walk is deleted, and the missing-workflow error names the pre-fan-out destinations as the oracle does. | Fan-out sims green, all 377 sims green (live gate still blocked: system DB unreachable; no deterministic sim repro exists at this seam — the sim backend was already single-atomic). |
| S13 | The six null step writers (recv, send, set event, get event, management caller step, rewind failure record) now bind the backend's application name through new params fields; no class-interface change was needed. | New live test `recorded message steps bind the backend's application name` passes; direct read-back shows the send and recv rows carrying the probe name. |
| S14 | `initWorkflowStatement` binds `updated_at` from the database clock like `created_at` instead of the application clock; the dead app-clock param field is removed. Follow-up sweep found the same defect in `clearQueueAssignmentSession` and bound it the same way. | Full live suite green (1101/1101); read-back over the run window shows 0 rows with `updated_at < created_at` and 282 with them equal. |

### Remaining (documented, not fixed)

| ID | Why it remains |
|---|---|
| Y1/Y2/Y3/Y4/Y5 | Type-shape and listing-format deviations (`UpsertedQueue`/`UpdatedQueue`, `creatorXid`, JSON key/encoding, extra types, stale comments). |
| T7/T8 | The Haskell-only `step_name` column and `runTxOutside` retry hazard are documented here; a cross-language schema warning and a retry note are still owed. |
| L6/L7/S10/S11/S12 | 0.5 fork-from superset, deferred schedules, missing 0.6 send/event serialization options, missing pending send/recv/await, and the child-workflow replay refusal. |
| Q6-Q9 | Legacy queue columns (ADR-0030), contended-skip + newly-partitioned warnings added, refusal order aligned; over-limit `PENDING` counts stay absent by design. |

## Review follow-up (2026-11-13)

The thermo-nuclear review of the branch raised these follow-ups. Status after the follow-up
fixes: `cabal test all` passed 1099/1099 in 63.47 s, the watcher passed 102/102 sims, and
`make neg` is green. No commit yet.

- **Async cancellation: fixed.** The encode paths now use `trySync` from `Context.hs`, which
  re-raises `AsyncException` instead of turning a shutdown into `ErrorSerialization`. The
  sanctioned `AsyncException` import moved from `Workflow.hs` to `Context.hs`; `AGENTS.md`
  records it.
- **Management caller fence: partially fixed.** The caller check now requires an existing
  caller row whose `owner_xid` equals the caller token; a missing row or a null owner is
  `WorkflowConflict`. The check and the target write are still two `runSession` calls; the
  atomic SQL form (an `EXISTS` on the same table inside the target update) hung the
  missing-target tagger test and was reverted. The TOCTOU remains open.
- **Public API breaks: accepted and documented.** `send`/`sendWith` now require
  `MonadCatch`; `DataSource.dsDeleteCheckpoints` takes an `m Bool` owner callback;
  `SystemDB.setWorkflowDelay`/`updateWorkflowAttributes` take `Maybe StepCaller`; the
  single-library merge removes the `dbos-transact-internals` package (ADR-0043). App
  workflows run in `IO`/`IOSim`, which have `MonadCatch`; custom `DataSource`/`SystemDB`
  implementations and generic `send` callers need a source update.
- **Unnamed transaction names: legacy fallback fixed; new-row sensitivity open.** Replay
  accepts a legacy `"transaction"` row against the new call-site name, so existing
  checkpoints keep replaying. New rows still embed `file:line`; a replay after a line move
  or a different checkout path can still fail the name check. An explicit `txName` is the
  stable choice.
- **Transaction journal conflict: fixed.** A `WorkflowConflict` from the outer `checkStep`
  now refuses the transaction instead of running the body. The post-commit journal write
  still tolerates `WorkflowConflict` for the live datasource fixtures that run without a
  `workflow_status` row; that write never runs the body, so it is not an ownership bypass.
- **Remaining review items: open.** The transaction journal re-encodes after the app commit
  and does not retry a transient `recordStep`; `validateMergedQueue` now rejects non-config
  errors; the legacy `partition_queue` flag is computed from the pre-read row; thrown
  transaction bodies are not recorded; the comments in `Management.hs` and the unused
  `queueIsLegacyPartitioned` import are fixed; completion cleanup runs one owner read per
  registered datasource; the rewind pre-check TOCTOU remains.
