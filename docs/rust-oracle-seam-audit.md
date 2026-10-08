# Rust oracle seam audit — 2026-09-25

Every Haskell module seam diffed name-for-name against the Rust oracle
(`~/dev/dbos-transact-rust`, pinned v0.5.0 semantics). Tables list the
oracle corpus per module — operations, types, classes/traits — with the
Haskell status beside each item. §8 consolidates the gaps, ranked.

Method: five parallel surveys over the primary source (one per corpus
slice: sysdb core, postgres backend, engine foundations, workflow core,
collaboration surface), each reading the Rust files fully and returning
inventories with line numbers; the Haskell side was read directly
(export lists, class definition, backend instance). Rust citations are
`crates/dbos/src/...` with lines; Haskell citations are `src/DBOS/...`
by symbol.

Legend: ✓ ported (same semantics) · ◐ partial (present, narrowed) ·
✗ missing · ⌀ deferred by recorded decision · → renamed (spelling
deviation, behavior same). "Engine" below means `DBOS.Transact.*`;
"class" means `DBOS.SystemDB` (`src/DBOS/SystemDB.hs`).

Corpus map (oracle → port):

| Rust (`crates/dbos/src`) | Haskell (`src/DBOS`) |
|---|---|
| `sysdb/mod.rs` (`trait SystemDatabase`) | `SystemDB` class |
| `sysdb/types.rs` | `SystemDB.Types` |
| `sysdb/error.rs` | `SystemDB.Error` |
| `sysdb/retry.rs` | `SystemDB.Retry` |
| `sysdb/notify.rs` | `SystemDB.Notify` |
| `sysdb/postgres.rs` | `SystemDB.Postgres` (+ `Postgres.Statements` seam) |
| `sysdb/postgres/notifier.rs` | `SystemDB.Postgres.Notifier` |
| `sysdb/postgres/listener.rs` | ⌀ none (polling-only, ADR-0010) |
| `sysdb/migrations` | ⌀ none (Haskell never migrates, ADR-0004; `rust-migrate/` drives) |
| `checkpoint.rs` | `Transact.Checkpoint` |
| `client.rs` | `Transact.Client` |
| `config.rs` | `Transact.Config` |
| `connection.rs` | `Transact.Connection` (see the verification note; the old ⌀ folded row predates the refactor) |
| `context.rs` | `Transact.Context` |
| `dequeue.rs` | `Transact.Dequeue` |
| `error.rs` | `Transact.Error` |
| `event.rs` | `Transact.Event` |
| `handle.rs` | `Transact.Handle` |
| `identity.rs` | `Transact.Identity` |
| `instance.rs` | `Transact.Instance` (`DBOS`) |
| `lib.rs` | `Transact` facade (+ `SystemDB` facade) |
| `management.rs` | `Transact.Management` |
| `message.rs` | `Transact.Message` |
| `queue.rs` | `Transact.Queue` |
| `recovery.rs` | `Transact.Recovery` |
| `registry.rs` | `Transact.Registry` |
| `select.rs` + `select_step!` macro | ✗ none (`Transact.Select` not created) |
| `serialization.rs` | `Transact.Codec` |
| `sleep.rs` | `Transact.Sleep` |
| `step.rs` | `Transact.Step` |
| `wait.rs` (+ `select_workflow!`, `join_workflows!`) | `Transact.Wait` (waits only, no macros) |
| `workflow.rs` | `Transact.Workflow` |
| `demo-apps/dbos-rust-starter` | `app/Main.hs` (L3 rewire pending) |
| `demo-apps/dbos-rust-widget-store` | ⌀ none (future reference app) |

## 1. Trait seam — `SystemDatabase`, 61/61 in the class

Oracle: `sysdb/mod.rs:92`, 60 required + 1 provided (`check_child_result`).
Haskell class (`SystemDB.hs:72-134`) carries all 61, with the same
provided-method shape (`checkChildResult` defaults to `checkStep` under
`getResultStepName`, `SystemDB.hs:132-133`). Snake_case folds to
camelCase; `&str` → `Text` (`WorkflowId` for ids); `Result<_, Error>` →
`m (Either Error _)`; the `Option<(&str, i32)>` caller becomes
`Maybe (WorkflowId, Int)`; waits add `MonadDelay`/`MonadTime`.

| # | Trait method (`mod.rs`) | Class | Backend (`Postgres.hs`) |
|---|---|---|---|
| 1 | `init_workflow` | ✓ | ✓ |
| 2 | `get_workflow` | ✓ | ✓ |
| 3 | `list_workflows` | ✓ | ✓ |
| 4 | `get_workflow_children` | ✓ | ✓ |
| 5 | `record_workflow_outcome` | ✓ | ✓ |
| 6 | `await_workflow_result` | ✓ | ✓ |
| 7 | `await_first_workflow_id` | ✓ | ✓ |
| 8 | `await_workflow_ids` | ✓ | ✓ |
| 9 | `set_workflow_delay` | ✓ | ✓ |
| 10 | `clear_queue_assignment` | ✓ | ✓ |
| 11 | `update_workflow_attributes` | ✓ | ✓ |
| 12 | `reenqueue_for_recovery` | ✓ | ✓ |
| 13 | `transition_delayed_workflows` | ✓ | ✓ |
| 14 | `cancel_workflows` | ✓ | ✓ |
| 15 | `resume_workflows` | ✓ | ✓ |
| 16 | `delete_workflows` | ✓ | ✓ |
| 17 | `fork_workflows` | ✓ | ✓ |
| 18 | `fork_from` | ✓ | ✓ |
| 19 | `send_message` | ✓ | ✓ |
| 20 | `send_messages` | ✓ | ✓ |
| 21 | `recv` | ✓ | ✓ |
| 22 | `write_stream` | ✓ (class) | ✗ `undefined` (`Postgres.hs:3030`) |
| 23 | `close_stream` | ✓ (class) | ✗ `undefined` (`Postgres.hs:3031`) |
| 24 | `close` | ✓ | ✓ |
| 25 | `check_step` | ✓ | ✓ |
| 26 | `record_step` | ✓ | ✓ |
| 27 | `list_workflow_steps` | ✓ | ✓ |
| 28 | `record_sleep` | ✓ | ✓ |
| 29 | `set_event` | ✓ | ✓ |
| 30 | `get_event` | ✓ | ✓ |
| 31 | `get_all_notifications` | ✓ | ✓ |
| 32 | `get_all_events` | ✓ | ✓ |
| 33 | `read_stream_value` | ✓ (class) | ✗ `undefined` (`Postgres.hs:3171`) |
| 34 | `get_all_stream_entries` | ✓ (class) | ✗ `undefined` (`Postgres.hs:3172`) |
| 35 | `create_application_version` | ✓ | ✓ |
| 36 | `list_application_versions` | ✓ | ✓ |
| 37 | `get_latest_application_version` | ✓ | ✓ |
| 38 | `update_application_version_timestamp` | ✓ | ✓ |
| 39 | `upsert_queue` | ✓ | ✓ |
| 40 | `start_queued_workflows` | ✓ | ✓ |
| 41 | `get_queue_partitions` | ✓ | ✓ |
| 42 | `start_queued_partitioned_workflows` | ✓ | ✓ |
| 43 | `get_queue` | ✓ | ✓ |
| 44 | `list_queues` | ✓ | ✓ |
| 45 | `update_queue` | ✓ | ✓ |
| 46 | `debounce_delayed_workflow` | ✓ | ✓ |
| 47 | `get_deduplication_key_holder` | ✓ | ✓ |
| 48 | `delete_queue` | ✓ | ✓ |
| 49 | `create_schedule` | ✓ | ✓ |
| 50 | `upsert_schedule` | ✓ | ✓ |
| 51 | `apply_schedules` | ✓ | ✓ |
| 52 | `get_schedule` | ✓ | ✓ |
| 53 | `list_schedules` | ✓ | ✓ |
| 54 | `update_schedule` | ✓ | ✓ |
| 55 | `set_schedule_status` | ✓ | ✓ |
| 56 | `update_schedule_last_fired_at` | ✓ | ✓ |
| 57 | `delete_schedule` | ✓ | ✓ |
| 58 | `rename_application` | ✓ | ✓ |
| 59 | `record_child_workflow` | ✓ | ✓ |
| 60 | `check_child_result` (provided) | ✓ (provided) | ✓ (inherited) |
| 61 | `record_child_result` | ✓ | ✓ |

Backend coverage: 57/61 live, 4 stream methods `undefined` by
deferral. Shared constants ported: `DEFAULT_SCHEMA` (fixed `dbos`
schema), `INTERNAL_QUEUE` → `internalQueueName`, `NULL_TOPIC` →
`nullTopicSentinel`, `PARTITIONED_DEQUEUE_SWEEP_CAP` →
`dequeueSweepCap`. `STREAM_CLOSED` → `streamClosedSentinel` (type-level
only; no stream engine reads it).

## 2. Domain types — `types.rs` (30 structs, 15 enums)

All 30 structs and all 15 enums are present in `SystemDB.Types`
(structs §§2.1–2.2 of the survey map one-to-one onto the export list:
`Timestamp` … `ScheduleFilter`). `pub mod step_names` (28 constants)
is fully ported as `*StepName` values, including `selectWorkflow` /
`selectStep` ahead of any select engine. Free functions ported:
`duration_from_ms/secs`, `is_valid_application_name`. Methods
ported: `Timestamp` full set (`addTimeout` = `checked_add`,
`durationSince`, ISO-8601 both ways, system-time both ways, `timestampNow`
on `MonadTime`), `WorkflowStatus` (`parseWorkflowStatus` returns
`Either` with `WorkflowStatusDecodeError` rather than `Option` —
total-function deviation), `NewWorkflow` (`newWorkflow`,
`validateNewWorkflow`, `initialStatus`), `Outcome` (`outcomeStatus`,
`outcomeColumns`), `Change` (`changeSet`, `changeIsLeave`,
`defaultChange`), `QueueUpdate` (`applyQueueUpdate`,
`isQueueUpdateEmpty`, `defaultQueueUpdate`), `ResolvedLimits`
(`resolvedIsPartitioned`), `QueueRecord` (all three),
`NewQueue` (`newQueue`, same limitless 1s-poll defaults),
`Fork` (`forkNew`, `forkValidate`), `ForkOptions`
(`forkOptionsValidate`), `ScheduleStatus`, `NewSchedule`,
`ScheduleUpdate`, `validateAttributes`, `WorkflowDelay`
(`resolveWorkflowDelay`), `Submission` (`claimsOwnership`),
`RenameFrom` (`renameFromApplication`), `DEFAULT_RENAME_BATCH_SIZE`
(`defaultRenameBatchSize`).

Type-level gaps and renames:

| Oracle item | Status | Note |
|---|---|---|
| `Message<'a>` (`message.rs:667`) | → `SendMessage` | Renamed; carries the already-encoded value instead of generic `T` |
| `OnExistingQueue::{Update,Leave}` | → `UpdateExisting`/`LeaveExisting` | Renamed variants |
| `ForkPoint` (`management.rs`) | → `ForkPoint` with `Fork`-prefixed ctors | `Beginning` has no spelling (only `ForkLastFailure`/`ForkLastStep`/`ForkStep`/`ForkStepNamed`); singular `fork_with` with a chosen id has no path (`Fork.forkForkedId` exists only on the bulk struct) |
| `ForkOptions` | ◐ | All fields except `forked_id`; extra `forkOptionsReplacementChildren` with no oracle counterpart (verify against a newer oracle before L2 close) |
| `ResumeOptions{queue}` | ✗ type | Expressiveness kept inline (`Maybe Text` queue param); no named type |
| `Children::{Skip,Include}` | ✗ type | `Bool` (`includeChildren`) at every call site |
| `QueueOptions` validation | ✓ (`validateQueueOptions`) | Lives in `Transact.Queue`, not beside the type; `DEFAULT_POLLING_INTERVAL` inlined as `secondsDuration 1` (no named const) |
| `update_queue` validator | ◐ open question | Class takes the check closure; our one engine call site passes `(\_ _ -> Right ())` (`Queue.hs:300`) — confirm what the oracle's own caller enforces before L2 close |
| `WorkflowFilter`/`NewWorkflow` fields | ✓ spot-checked | 15-field filter and ~25-field row constructor present; full field audit not repeated here |
| `Timestamp::now` pre-1970 clamp | ◐ | `timestampNow` is raw `MonadTime`; the oracle clamps pre-epoch to epoch (trivia) |

## 3. Errors, retry, wakeups

`sysdb/error.rs` (15 variants): 15/15 ported in `SystemDB.Error`
(`Backend`, `Malformed`, `ConflictingWorkflow`, `ConcurrentRecv`,
`InvalidInput`, `QueueDeduplicated`, `WorkflowCancelled`,
`UnexpectedStep`, `StepAlreadyRecorded`, `NoForkPoint`,
`NonExistentWorkflow`, `AlreadyRegistered`, `NotRegistered`,
`RegisteredByAnother`, plus `MaxRecoveryAttemptsExceeded` under the
documented `Error` prefix — the one constructor collision in the file).
`BackendError` (3 fields) and `BackendErrorKind` (3 variants) verbatim;
`invalidInput`, `renderError`, `renderBackendError`, `Exception`
instance all present.

`retry.rs`: `RetryPolicy` (3 fields, same defaults) ✓; `withRetry`,
`shouldRetry`, `jitter`, `uuidEntropy`, `defaultRetryPolicy` ✓
(`should_retry`/`jitter` are private in Rust, public here — promoted
for the `PostgresTest`/`RetryTest` contract).

`notify.rs`: channels, `messageKey`/`eventKey`/`streamKey`/`keyFor`,
`Registry` (`subscribe`, `subscribeExclusive`, `wake`, `wakeAll`),
`Subscription` (`notified`) ✓, plus `unsubscribe` — Haskell has no
`Drop` hook, so the explicit call is the documented equivalent.

`notifier.rs`: `coalesceInterval`, `pushBatch`, `notifierNew`,
`enable`, `isPushing`, `signal`, `run`, `stop`, `flush` ✓ (14 + 5
watcher tests).

`listener.rs`: ⌀ entire file missing by decision (ADR-0010,
polling-only; `new`/`is_delivering`/`poll_interval`/`run`, the 1s/60s
intervals, the self-test). Nothing in the engine depends on delivery
proof; waits poll at 1s unconditionally.

## 4. Postgres backend — `postgres.rs`

Inherent surface: `Settings` (6 fields) ✓, `Config` + `new` (10
connections, listen-notify on, migrate on — migrate/ensure-database
not honored: verify-only connect, ADR-0004/0010) ✓, `connect` ✓
(verify-only), `fromPool` ✓, `activatePostgresSystemDB` ≈
`start_notifications` (notifier only, no listener/Cockroach branch) ✓,
`releasePostgresSystemDB` ≈ `close` ✓. `is_pushing`/`is_delivering`
live in the Notifier. Schema fixed to `dbos` (no `Tables` render
parameter). `run_transactional_step` has no single counterpart:
check-then-record runs inside each write transaction per ADR-0011.
Polling semaphore is STM (`Postgres.hs`, `pollingLimit`), same
half-the-pool rule. `PORTABLE_JSON` never appears: rows record
`rust_serde`, matching `Config.serializer`.

## 5. Engine modules

### `config.rs` → `Transact.Config` ✓

13/13 fields, `configNew`/`configFromEnv` (URL-only env read),
`validateConfig`, `outcomePollInterval`,
`defaultOutcomePollInterval`, `serializerName`/`rust_serde`,
`databaseUrlEnv`. `validate`/`outcome_poll_interval` resolvers are
public here (used by instance/dequeue), `pub(crate)` there.

### `identity.rs` → `Transact.Identity` ✓

5 env consts, `Environment`, `Identity`, `readEnvironment`, `resolve`,
`validateAppName`, `defaultExecutorId` (`local`). `Environment`/
`Identity` are public here vs `pub(crate)` there — promoted for
`launchWithEnvironment` test isolation.

### `error.rs` (engine) → `Transact.Error` ◐ (16/23 + 2 extras)

| Oracle variant | Haskell |
|---|---|
| `NotLaunched{operation}` | ✓ `ErrorNotLaunched` |
| `AlreadyLaunched{operation}` | ✓ `ErrorAlreadyLaunched` |
| `NotInWorkflow{operation}` | ✓ `NotInWorkflow` |
| `InsideStep{operation}` | ✓ `InsideStep` |
| `WrongInstance{operation}` | ✓ `WrongInstance` |
| `Config(String)` | ✓ `ErrorConfig` |
| `InvalidArgument{operation,detail}` | ✓ `InvalidArgument` |
| `AlreadyRegistered{key}` | ✓ `ErrorAlreadyRegistered` |
| `Serialization{what,message,source}` | ◐ `ErrorSerialization{what,message}` (no `source`) |
| `Deserialization{what,message,source}` | ◐ `ErrorDeserialization{what,message}` (no `source`) |
| `SystemDatabase(sysdb::Error)` | ✓ `ErrorSystemDatabase` |
| `StepFailed{step,message}` | ✓ `StepFailed` |
| `WorkflowFailed{workflow_id,message}` | ✓ `ErrorWorkflowFailed` |
| `StepBuiltElsewhere{step,built,polled}` | ✓ `StepBuiltElsewhere` |
| `Application(E)` | ✗ (no application error channel — typed-IO phase) |
| `NotRegistered{key}` | ✗ |
| `WorkflowNotFound{workflow_id}` | ✗ |
| `Interrupted{workflow_id}` | ✗ (shutdown aborts; rows stay `PENDING`) |
| `WorkflowCancelled{workflow_id}` | ✗ engine-level (sysdb `WorkflowCancelled` exists) |
| `AwaitedWorkflowCancelled{workflow_id}` | ✗ |
| `MaxRecoveryAttemptsExceeded{workflow_id,recovery_attempts}` | ✗ engine-level (sysdb variant exists) |
| `StepTimeout{step,timeout}` | ✗ (no step timeouts — retry wiring) |
| `MaxStepRetriesExceeded{step,attempts,errors}` | ✗ (no retries — retry wiring) |
| — | extra `ErrorWorkflowNotRegistered{key}` (registry level, no counterpart) |
| — | extra `ErrorWorkflowClaimLost{workflowId}` (claim level, no counterpart) |
| `EngineOnly` / `DurableError` / `Result` / `map_application` / `control` / `lift` | ✗ (all fall with the untyped channel) |

### `serialization.rs` → `Transact.Codec` ✓

`encode`/`decode` (absent-as-null, named halves, `()` handling) plus
`encodeUnit` and `encodeAttributes` (attribute JSON plane). Oracle-checked
2026-09-25: no divergence.

### `context.rs` → `Transact.Context` ✓ (with adaptations)

Free fns: `workflow_id` → `currentWorkflowId` ✓,
`step_id` → `currentStepId` ✓ (incl. enclosing-step rule),
`step_status` → `currentStepStatus` ✓, `cancellation_token` →
`cancellationToken` ✓ (`TVar Bool` fire-and-poll for
`CancellationToken`). `StepStatus` + 3 readers ✓ (`firstStepStatus`
for the test-only `first`). `in_step_scope` → `withAttempt` ✓ (sync;
fresh marker + same-id status + fresh flag, rebind-never-mutate).
`next_step_id` ✓, `deadline` → `currentDeadline` ✓, `in_step` →
`inStep` ✓, `scope` → `runWorkflowM` family ✓ (never crosses
`forkIO`/`async` — proved by test). Marker: `StepMarker` over
`Data.Unique`, process-wide like `NEXT_STEP_MARKER` ✓.
Missing: `is_same_execution` pointer (identity is workflow-id text —
same reason `WrongInstance` has no comparison), `executor()` pointer
(`currentEngine` returns backend/config/identity instead), `step_marker`
reader (markers are mint-only), `with_current` borrow (no `Arc`
refcounts to save).

### `checkpoint.rs` → `Transact.Checkpoint` ◐

`StepPlacement` all 4 variants ✓ (`InsideStep` →
`PlacementInsideStep`, documented collision); `here`/`at` →
`placementHere`/`placementAt` ✓; `check_here` → `checkHere` ✓ with
the oracle refusal table incl. sibling-step wording;
`whereabouts`/`describe`/`inside_a_workflow`/`step_id` ✓;
`StepDurability` ✓ (ctors prefixed — one module, one `Recorded`).
`PendingStep` exists as identity-only (name + placement +
`pendingStepId`); its runners are absent: `new`/`placed`,
`map_error`/`lift`, `Future` impl, `StepPlacement::taken`,
`ambient_connection`, `next_step_id`, `step`/`executor` readers,
`check`/`record` (per-caller by design — SystemDB owns the writes),
`revive` (replayed errors arrive as `StepFailed` text).

### `connection.rs` → folded ⌀

No module. The hidden backend in the `WorkflowM` runtime is the
executor's stand-in; `Owner` (application vs client) has no counterpart,
which is also why `WrongInstance` and `ClientConnection`-vs-instance
distinctions never arise.

### `instance.rs` (`DBOS`) → `Transact.Instance` ◐

`new`/`config`/`is_launched`/`launch`/`shutdown` ✓
(`launchWithEnvironment` extra, for test isolation),
`register_workflow` → `registerWorkflow` (+ `Ref` variant) ✓,
`run`/`enqueue`/`retrieve`/`dequeue` drivers ✓, management wrappers
✓. Missing: `executor_id`/`app_version`/`app_id` accessors (3 trivial
methods, no test needs them yet). `Tasks`/`runtime`/`spawn_tracked`
absent — task ownership is open L2 work; `launch` spawns the
supervisor directly and `shutdown` joins it.

### `registry.rs` → `Transact.Registry` ◐

`WorkflowKey` + `new`/`instance`/`from_row`/`renderWorkflowKey` ✓.
`WorkflowRef` is unparameterized (registry + key; no `DBOS` handle, so
capturing one cannot pin an instance) with `refKey`/`refName`/
`registerWorkflowRef`; the typed `register_workflow`/
`register_workflow_as`/reset distinction collapses to one erased
registration (types checked once at the boundary, as the oracle
intends). Snapshot/freeze/thaw ✓.

### `workflow.rs` → `Transact.Workflow` ◐

`RunOptions`/`StartOptions`/`Enqueue`/`DuplicationPolicy`/`Timeout`
(full fields, defaults, `runOptionsToStartOptions`,
`validateEnqueue`, `storedPriority`, `enqueueNew`,
`childWorkflowId`, `resolveTimeoutDeadline`, `timeoutBudget`) ✓;
`resolveEnqueueCollision` ≈ `init_or_join`'s join arm ✓;
`new_row` folded into row constructors ✓; `encode_attributes` lives
in `Codec` ✓. `startWorkflowRef` / `runWorkflowRef` (single-init
execute-or-adopt core `runRegisteredWorkflowWithRow`) and
`startChildWorkflow` (derived ids, replay adoption, joined-dedup
mapping-only write, `InsideStep` refusal) cover `start`/`start_with`/
`run`/`run_with` observably, minus the async layer: no
`PendingWorkflow`/`PendingStart`/`PendingRun`, no `lift`, no
`step_id` readers, no `spawn_execution`, no `Connection::adopt`
(`handleResult` polls and decodes inline), no deadline watcher
(`run_until_deadline`/`cancel_at_deadline` — the deadline rides the
row and the context, nothing enforces it yet).

### `handle.rs` → `Transact.Handle` ◐ (polling only)

`workflow_id` ✓; `status` returns `Maybe WorkflowStatus` where the
oracle reports `WorkflowNotFound` (single read has nothing to wait
for — deliberate); `handleResult` polls and adopts the recorded
outcome (maps `Cancelled`/`Parked` faithfully) but has no local-task
await (`Provenance.Local` absent) and records no `DBOS.getResult`
checkpoint on the parent. `ChildResultPlacement`/`settle`/`interpret`
absent with it.

### `step.rs` → `Transact.Step` ◐ (defaults only)

`runStep` ≈ `step()` with default options; `sleepStepName`
contract kept. `StepOptions` (7 fields), `ShouldRetry`,
`step_with`, timeouts, `preemptible`, backoff, and the retry loop are
all absent — retry wiring is open L2 work, and the legacy pool-based
`runStep`/`sleepStep` beside it belong to the starter seam.

### `sleep.rs` → `Transact.Sleep` ✓

Record-wake-then-wait-remainder, `completed_at`-as-wake-time,
plain call outside workflows/inside steps. (`PendingStep` wrapper
replaced by direct `WorkflowM` — same observable shape.)

### `event.rs` → `Transact.Event` ◐

Free `set_event`/`get_event` ✓ (two-step read checkpoint included).
Missing: `DBOS::get_event` method form (with `WrongInstance` guard)
and `Client::get_event`.

### `message.rs` → `Transact.Message` ◐

Free `send`/`recv` ✓ (two-step recv checkpoint, `InsideStep`
refusal, exclusive-subscription semantics in the backend). Missing:
`send_with`/`send_bulk`/`send_bulk_with` (+ `Message`/`SendOptions`/
`SendBulkOptions`/`Forks` types — `SendMessage` covers the single
shape), all four `DBOS::send*` method forms, all four `Client::send*`
forms.

### `queue.rs` → `Transact.Queue` ✓ (engine side)

`Queue`/`QueueOptions`/`QueueChange`/`QueueConflict`,
`defaultQueueOptions`/`defaultQueueChange`, `queueFromRecord`,
`queueIsPartitioned`, option/update translation, `validateQueueOptions`,
and all five ops (`registerQueue`, `queue`, `listQueues`,
`updateQueue`, `deleteQueue`) ✓. Accessors are record fields, not
methods — same answers. `registerQueue` resolves
`UpdateIfLatestVersion` against the latest version ✓.

### `dequeue.rs` → `Transact.Dequeue` ✓ (closed 2026-09-25)

`dequeuePass` + `superviseForever` cover `spawn`'s job (called from
`launch`; the supervisor loop lives in the module rather than as a
spawned task). Closed 2026-09-25: `Running`/`Slot` local tallies,
`workerBudget`, all three `pollOnce` shapes (batched partitioned sweep
included), `55P03` contention skip, backoff/scaleback/jitter,
`shuffled` partition walk, queue-set refresh with keep-on-error and the
internal-queue warning, and concurrent tracked dispatch. Recorded
deviations: `Slot` release is explicit (`finally` in the spawned task,
since Haskell has no `Drop`), and `pollOnce` reports claimed ids so the
one-shot `dequeuePass` driver can return them.

### `recovery.rs` → `Transact.Recovery` ✓

`reenqueueForRecovery` — one call, same shape.

### `wait.rs` → `Transact.Wait` ◐ (DBOS waits only)

`waitForWorkflow`/`waitForFirstWorkflow`/`waitForWorkflows` cover the
`await_*` outcomes. Missing: free `select_workflow`/`join_workflows`,
both `Client` forms, both `DBOS` select/join method forms, and the
`select_workflow!`/`join_workflows!` macros.

### `management.rs` → `Transact.Management` + `Instance` ◐

Bulk `cancel`/`resume`/`delete`/`fork` + `forkFrom` + `retrieveWorkflow`
✓ (bulk-first, matching the oracle's primitive orientation). Missing:
singular wrappers, `Children` (bool inline), `ForkFrom` (inline
`ForkPoint`), `ResumeOptions` (inline `Maybe Text`), `set_workflow_delay`,
`update_workflow_attributes`, `list_workflows`, `list_workflow_steps`,
all 14 `Client` forms, and the checkpointed `PendingStep` shape
(ours are plain `IO` — checkpointed management is Select-adjacent
future work).

### `client.rs` → `Transact.Client` ◐

`ClientConfig` (9/9 fields), `new`/`from_env`/`validate`/
`outcome_poll_interval`, `connect` (eager, verify-only), `close`,
`app_name`, `enqueue`/`enqueue_with` (every `EnqueueOptions` field
honored incl. dedup join), `retrieve_workflow`,
`workflow_status` ✓. Missing: 5 queue ops, 4 version ops
(`list/latest/set_latest*`), 4 `send*`, `get_event`,
`select/join_workflows`, all 14 management forms.

### `select.rs` → ✗ absent

`Racing`/`Recording`/`Branches`, `check_select`/`record_select`/
`control_error`, and `select_step!` — no module, no macro story.
Nothing in the starter needs it (plan Q3); it gates checkpointed
management and durable races.

### `lib.rs` → facades ✓-shaped

`Transact` + `SystemDB` re-export the port's surface; no `__private`
equivalent needed (no macro expansions to feed); no
`CancellationToken` re-export (`TVar Bool` instead).

## 6. Macros, migrations, demo apps

- `select_step!` (proc macro, `dbos-macros`), `select_workflow!`,
  `join_workflows!` (`macro_rules!` in `wait.rs`): ✗ no Haskell
  counterpart of any kind. Blocks durable races and checkpointed
  management awaits.
- `sysdb/migrations`: ⌀ by decision — `rust-migrate/` drives, Haskell
  verifies the ceiling (108).
- `demo-apps/dbos-rust-starter` → `app/Main.hs`: ◐ builds against the
  legacy seam and is currently red on class-seam fallout (L3 rewire
  pending; tracked in `p76-tdd-plan.md`).
- `demo-apps/dbos-rust-widget-store`: ✗ unscoped second reference app
  (child workflows, typed IO, app tables — Phase 8/future).

## 7. Consolidated gaps, ranked

P0 — L2 completion (in plan order):
1. `Transact.Select` (whole module) + macro story for `select_step!`.
2. Full `Management`: singulars, `set_workflow_delay`,
   `update_workflow_attributes`, `list_workflows`,
   `list_workflow_steps`, `Client` forms, checkpointed shape.
3. Step retry wiring: `StepOptions`/`ShouldRetry`/backoff/timeout/
   `preemptible`/retry loop; engine `MaxStepRetriesExceeded`/
   `StepTimeout` arrive with it.
4. Task ownership: `Tasks`/`spawn_tracked`/`spawn_execution`,
   local-task handle await, deadline enforcement watcher. (`Tasks`/
   `spawnTracked`/`abortAll` landed 2026-09-25; handle await and the
   deadline watcher remain.)
5. ~~Queue backoff/local running counts (`Running`/`Slot`,
   contention/jitter, partition-sweep shapes).~~ CLOSED 2026-09-25:
   `Running`/`Slot`/`workerBudget`, all three poll shapes, contention
   skip, backoff/scaleback/jitter, shuffle, queue-set refresh, and
   concurrent dispatch all landed (see the `dequeue.rs` section and the
   plan's dequeue delta).
6. `updateQueue` validator question (§2 table) — resolve before close.
7. `DBOS` executor accessors (`executor_id`/`app_version`/`app_id`;
   trivial).
8. `DBOS::get_event` + `Client::get_event`; message bulk/with/forms
   + `DBOS`/`Client` send forms + `Message`/`SendOptions` types.
9. Wait free + `Client` forms.
10. `Client` queue/version/management/event/message/select ops.
11. Engine error channel completion (`NotRegistered`,
    `WorkflowNotFound`, `Interrupted`, `AwaitedWorkflowCancelled`,
    engine `MaxRecoveryAttemptsExceeded`, `WorkflowCancelled`) with the
    typed-IO phase (`Application(E)`, `EngineOnly`, `DurableError`,
    `map_application`/`control`/`lift`, `PendingStart::lift`).
12. `ForkOptions.forked_id` + `ForkFrom::Beginning` spellings.

Deferred by decision (not gaps to close blindly): listener, streams
(4 backend `undefined`s), migrations, `Owner`/executor-pointer
comparisons (`WrongInstance`, same-execution), `step_marker` reader,
`with_current` borrow, private retry/jitter visibility (already
promoted — keep).

L3: starter rewire, which unblocks the legacy deletion (`Store`,
`SimDB`, `OperationCheckpoint*`, old `Executor`/`Supervisor`, old
pool block, `WorkflowExecution*` rows, `WorkflowExecutionParse`).

## 8. Naming deviations catalog (behavior same)

`SystemDB`/`PostgresSystemDB` for `SystemDatabase` (repo rule);
`PlacementInsideStep`, `DurabilityRecorded`/`DurabilityPlain`,
`ErrorConfig`, `ErrorWorkflow{NotRegistered,ClaimLost,Failed}`,
`ErrorMaxRecoveryAttemptsExceeded` (constructor-prefix collisions);
`SendMessage` for `Message`, `Fork{LastFailure,LastStep,Step,…}`
ctors, `ForkPoint` for `ForkFrom`, `UpdateExisting`/`LeaveExisting`
for `Update`/`Leave`; record-dot fields for accessor methods
(`Queue`, `Executor`); `Either` errors for `Result`; `TVar Bool`
for `CancellationToken`; `Maybe WorkflowStatus` for
`WorkflowNotFound` on handle status; sync drivers for the `Pending*`
`Future` layer; `WorkflowStatusDecodeError`/`Either` for `parse`
`None`; `WorkflowRef` was unparameterized at audit time and is now
`WorkflowRef m` (post-refactor, see the verification note).

Counts: trait 61/61 in the class (57 live in the backend, 4 streams
`undefined`); `types.rs` 30 structs + 15 enums + 28 step-names +
3 fns present; sysdb errors 15/15; engine error 16/23 (+2 extras);
`Config` 13/13; `ClientConfig` 9/9.


## 9. Full verification — 2026-09-25 (post-refactor)

Re-run of every claim above against the code and the oracle, after the
`m`-polymorphism refactor (ADR-0012) and the `DBOS.Prelude` switch.

Method: mechanical name-level diffs (Rust `pub fn`/`pub struct`/`pub enum`
per module vs Haskell export lists and top-level definitions; trait and
class method lists; enum constructors; record fields), plus the three
project gates and the two matching Rust oracle suites.

| Claim | Result |
|---|---|
| Trait seam 61/61 (60 required + `checkChildResult` provided) | ✓ verified, no missing/extra |
| `types.rs` 30 structs + 15 enums | ✓ 15/15 enums, 30/30 structs (`Message` → `SendMessage`, §8) |
| sysdb errors 15/15 (+2 extras `Backend`/`Malformed`) | ✓ verified |
| engine error 16/23 (+2 extras) | ✓ verified; missing 7 are the recorded typed-IO phase gaps (`Application`, `NotRegistered`, `WorkflowNotFound`, `Interrupted`, `AwaitedWorkflowCancelled`, `StepTimeout`, `MaxStepRetriesExceeded`) |
| `Config` 13/13, `ClientConfig` 9/9 | ✓ verified (`config`-prefixed fields) |
| Backend 57 live + 4 stream `undefined` | ✓ 4 `undefined` confirmed in `Postgres.hs` |
| Per-module name diff | ✓ no new regressions; the missing names are exactly the ranked gaps: `step`/`step_with`/`should_retry` (§7.3), management singulars (§7.2), message `send_with`/`send_bulk*` (§7.8), wait macros (deferred), client queue/version ops (§7.10), `Select` (§7.1) |

Gates: `make db-migrate` 108→108; `cabal test` 303/303; psql mirror 7/7
prescribed rows; ghciwatch walk of all 21 test groups green (Types 52,
Error 18, Retry 8, Postgres 135, Notify, Notifier 5, Identity 8, Config 3,
Context 19, Sim 1, Registry 10, Checkpoint 9, Step 2, Event 1, Message 1,
Workflow 10, Queue 4, Instance 3, Handle 2, Client 5, Starter 21). Oracle:
`cargo test -p dbos --test recovery` 3/3 and `--test queues` 35/35
(read-only, v0.5.0), including
`worker_concurrency_bounds_what_one_process_runs_at_once`, the contract the
Haskell dequeue test now mirrors.

Drift since the audit (recorded, not regressions):

- `connection.rs` is a real Haskell module now (`Transact.Connection`:
  `SomeSystemDB`/`runSystemDB`, `Owner`, `Connection`, `ExecutionIdentity`,
  `newConnection`, `forApplication`); the audit-time ⌀ folded row described
  the deleted `WorkflowM` runtime.
- `WorkflowM` and Bluefin are gone; the engine is polymorphic in `m`
  (`Registry m`, `Snapshot m`, `WorkflowRef m`, `Client m`,
  `WorkflowHandle m`, `DBOS m`), `launch`/`connectClient` stay IO at the
  Postgres edge (ADR-0012).
- New own-seam modules with no Rust counterpart: `DBOS.Prelude` (io-classes
  unqualified, `NoImplicitPrelude` global; `async`/`stm`/`exceptions` deps
  removed, `time` kept for timestamp parsing only) and the P7.7 seed
  `DBOS.SystemDB.IOSim` (60 stub methods, `simConnection`/`simDBOS`).
- `Data.Unique` and `UUID.V4` are no longer engine dependencies: execution
  identity is a per-connection counter and generated ids/entropy are
  injected `m` actions.
