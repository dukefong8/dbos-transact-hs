# P7.6 TDD Plan — engine on the SystemDB seam (recorded 2026-09-24)

Goal: the starter demo (`app/Main.hs`) running on the new engine, with the
old seam deleted. Traced top-down from the starter; executed bottom-up.
Every layer's gate — ghcid.txt green on its group AND a full `cabal test`
green AND psql evidence — must pass before the next layer begins. No
shortcuts, no workarounds: a method that cannot be faithful stays
`undefined` with a NOTE instead of being faked.

## The graph, traced from `app/Main.hs` down

`app/Main.hs` (Warp routes, 4 workflow bodies) needs, by call site:

- launch: `acquirePool`, `launchExecutor`, `registerQueue`, `superviseForever`
- workflows tab: `tryStartWorkflow`, `spawnWorkflow`, `getEvent` (+ crash: the crash line)
- queues tab: `enqueueWorkflow`, `fetchQueueWorkerConcurrency`,
  `fetchWorkflowStatuses`, `updateQueueWorkerConcurrency`, `sleepStep` (body)
- events tab: `runStep`, `sleepStep`, `setEvent`, `getEvent`,
  `getEventBlocking`
- messages tab: `recvMessage`, `sendMessage`, `sendMessages`,
  `listWorkflowIdsByName`, `decodeWorkflowValue`
- shutdown: `shutdownExecutor`, `releasePool`

Those resolve into three strata:

- **Engine API** (`DBOS.Transact`): `Executor` (launch/shutdown/spawn),
  `Registry` (register/empty), `WorkflowBody`, `runStep`/`sleepStep`,
  `superviseForever`.
- **Old seam (to delete)**: `Store.hs` (`EventStore`/`StepStore` records of
  functions), `SimDB`, and the old pool-function block at the bottom of
  `Postgres.hs` (`acquirePool`, `releasePool`, `tryStartWorkflow`,
  `enqueueWorkflow`, `postgresStepStore`, `getEventBlocking`, `recvMessage`,
  `registerQueue`, `fetchQueueWorkerConcurrency`,
  `updateQueueWorkerConcurrency`, `listWorkflowIdsByName`,
  `fetchWorkflowStatuses`, `sendMessage`, `sendMessages`, `setEvent`,
  `getEvent`).
- **New seam (done, 220/220)**: the `SystemDB` class over
  `ReaderT PostgresSystemDB IO`, with known gaps: `getAllNotifications`,
  `getAllEvents`, `recordChildWorkflow`, `recordChildResult`, `close`, the
  notifier flush-loop spawn, and the refused caller forms.

The oracle's engine seam (`crates/dbos/src`): `DBOS::new`/`launch`/
`shutdown` (`instance.rs`), workflow registry + ref (`registry.rs`),
steps (`step.rs`, `sleep.rs`), events (`event.rs`), messages
(`message.rs`), queues (`queue.rs`), dequeue (`dequeue.rs`), recovery
(`recovery.rs`), waits (`wait.rs`), management (`management.rs`).

## Layer 1 — close the new seam (bottom)

One method at a time, red-first in the existing `PostgresTest` group:

1. `getAllNotifications`, `getAllEvents` (the two remaining reads).
2. `recordChildWorkflow`, `recordChildResult` (the child link; the refused
   `initWorkflow`-caller form is unlocked by the first).
3. `close`: stop the notifier (flush loop) *before* closing the pool, then
   `Pool.release`. Spawn the flush loop (`enable` + `run`) in
   `acquirePostgresSystemDB`/`fromPool` so `close` has something to stop;
   bracket it in `withPostgresSystemDB`.
4. The caller forms: implement `run_transactional_step` semantics for the
   refused callers (debounce, create/upsert/get/list/update/setStatus lane
   of schedules, initWorkflow caller) — check-then-record inside the write
   transaction, replay returns the stored output without re-running.

Gate L1: `All 105+n tests passed` in ghcid.txt, `cabal test` 220+n,
psql shows the child/debounce/schedule caller rows.

## Layer 2 — rewrite the engine on the class (middle) — DECIDED 2026-09-24 (Q1)

Rewrite every `DBOS.Transact.*` submodule from scratch as a 1-to-1 port of
the Rust oracle module, programmed against `SystemDB
(ReaderT PostgresSystemDB IO)` — no pool threading, no `Handle` records
(ADR-0009: the class is the seam). `DBOS.Transact` re-exports them for the
starter. `Store.hs`, `SimDB`, and the old pool-function block die in the
rewrite (no shim). The mapping:

| Rust (`crates/dbos/src`) | New Haskell (`src/DBOS/Transact`) |
|---|---|
| `checkpoint.rs` | `Transact.Checkpoint` |
| `client.rs` | `Transact.Client` |
| `config.rs` | `Transact.Config` |
| `connection.rs` | folded into the backend seam (no counterpart; record if it needs one) |
| `context.rs` | `Transact.Context` |
| `dequeue.rs` | `Transact.Dequeue` |
| `error.rs` | `Transact.Error` |
| `event.rs` | `Transact.Event` |
| `handle.rs` | `Transact.Handle` |
| `identity.rs` | `Transact.Identity` |
| `instance.rs` | `Transact.Instance` |
| `lib.rs` | `Transact` (facade) |
| `management.rs` | `Transact.Management` |
| `message.rs` | `Transact.Message` |
| `queue.rs` | `Transact.Queue` |
| `recovery.rs` | `Transact.Recovery` |
| `registry.rs` | `Transact.Registry` (rewrite) |
| `select.rs` | `Transact.Select` (only what the starter needs; the macro has no starter call site) |
| `serialization.rs` | `Transact.Codec` (existing port stands unless it diverges) |
| `sleep.rs` | `Transact.Sleep` |
| `step.rs` | `Transact.Step` (rewrite) |
| `wait.rs` | `Transact.Wait` |
| `workflow.rs` | `Transact.Workflow` |

Unpark `StarterTest` one group at a time (cabal `other-modules` +
`test/Main.hs` toggle), each group green before the next. Streams stay
deferred through L4 (DECIDED Q3: no starter route touches them). Engine duties:
recovery on launch (`transitionDelayedWorkflows` + `reenqueueForRecovery`),
the queue sweep (`startQueuedWorkflows`), id-tracked tasks (fixes the
open-join gap in docs/starter-e2e-gate.md: re-POSTing an id must join, not
duplicate).

Gate L2: parked suites back, `cabal test` green, psql mirror of
§End-to-End in AGENTS.md.

## Layer 3 — rewire the starter (top)

`app/Main.hs` onto the Layer-2 engine, same HTTP surface and same
`app/page.html` byte-for-byte. The call-site mapping is mechanical:
`acquirePool`→`acquirePostgresSystemDB`, `tryStartWorkflow`→`initWorkflow`,
`spawnWorkflow`→engine run, `superviseForever`→engine supervisor,
`enqueueWorkflow`→queued `initWorkflow`, `postgresStepStore`/`runStep`/
`sleepStep`→class steps, `setEvent`/`getEvent`/`getEventBlocking`→class,
`recvMessage`→`recv`, `sendMessage(s)`→class, `registerQueue`→`upsertQueue`,
`updateQueueWorkerConcurrency`→`updateQueue`,
`fetchQueueWorkerConcurrency`→`getQueue`,
`fetchWorkflowStatuses`→`getWorkflow`,
`listWorkflowIdsByName`→`listWorkflows`, `shutdownExecutor`+`releasePool`→
`close`. Durations stay the shortened E2E values.

Gate L3: `cabal build exe:dbos-hs-starter` clean, and the repo's
`StarterTest` mirror green against the live database.

## Layer 4 — side-by-side E2E (final gate)

Haskell starter on `:8081` (tmux `Haskell:4`), Rust oracle on `:8080`
(tmux `Haskell:5`), fresh databases, same W1/W2/Q/E/M flows from
docs/starter-e2e-gate.md driven with fresh ids through both apps' own
endpoints. Evidence: chrome-devtools-axi console log in the browser
(index page, timeline, polls, toasts) and the curl CLI lines in the tmux
panes, identical behavior both sides. The known gaps stay ledgered until
closed: `set_event` checkpoint rows (function ids 0–5 vs 1–3) and any
re-opened open-join case.

---

## Module map — every Rust module and its Haskell counterpart, by phase

One Rust module maps to exactly one Haskell module (HARD RULES); facades
and the `Statements` seam are the port's own and re-export, never redefine.

### Phases 0–6 — engine v1 on the Store seam (done; P7.6 L2 deletes the seam)

| Rust (`crates/dbos/src`) | Haskell (`src/DBOS`) |
|---|---|
| `checkpoint.rs` | `Transact.OperationCheckpointTypes` / `OperationCheckpointParse` / `OperationCheckpointReplay` |
| `serialization.rs` | `Transact.Codec` |
| `error.rs` (engine half) | `Transact` error ADTs (`StepError`, `WorkflowRunError`) over shared `SystemDB.Error` |
| `workflow.rs` | `Transact.Workflow` |
| `registry.rs` (v1) | `Transact.Registry` |
| `instance.rs` Executor (v1) | `Transact.Executor` |
| `step.rs` / `sleep.rs` / `context.rs` (engine halves) | `Transact.Step` |
| `recovery.rs` (recovery sweep) | `Transact.Supervisor` |
| `config.rs` / `identity.rs` | `SystemDB.Postgres` `Settings`/`Config` + starter env reads |
| `demo-apps/dbos-rust-starter` | `app/Main.hs` + `app/page.html` |
| (pre-trait hasql SystemDB fns) | `SystemDB.Postgres` old pool-function block (L2 deletes) |

### P7.0–P7.2 — class seam, domain types, skeleton, reads + waits (done)

| Rust | Haskell |
|---|---|
| `sysdb/types.rs` | `SystemDB.Types` |
| `sysdb/error.rs` | `SystemDB.Error` |
| `sysdb/retry.rs` | `SystemDB.Retry` |
| `sysdb/mod.rs` (`NULL_TOPIC`, `trait SystemDatabase`) | `SystemDB` class + `nullTopicSentinel` |
| `sysdb/postgres.rs` skeleton | `SystemDB.Postgres` (`Settings`, `Config`, `PostgresSystemDB`, `fromPool`, verify) |

### P7.3 — workflow writes (done): `initWorkflow`, `recordWorkflowOutcome`,
`setWorkflowDelay`, `clearQueueAssignment`, `updateWorkflowAttributes`,
`reenqueueForRecovery`, `transitionDelayedWorkflows`, `cancelWorkflows`,
`resumeWorkflows`, `deleteWorkflows`, `forkWorkflows`, `forkFrom`,
`renameApplication` (+ `listWorkflowSteps`).

### P7.4 — steps, events, messages (done; streams deferred):
`checkStep`, `recordStep`, `recordSleep`, `setEvent`, `getEvent`,
`sendMessage`, `sendMessages`, `recv`, `send_to_forks`.

### P7.5 — versions, queues, schedules, wakeups (done, 220/220)

| Rust | Haskell |
|---|---|
| `application_versions` fns | `VersionInfo` + 4 methods |
| `queue.rs` + postgres queue fns | `QueueRecord`/`NewQueue`/`QueueUpdate`/`ResolvedLimits` + 10 methods |
| schedule fns | `ScheduleRecord`/`NewSchedule`/`ScheduleUpdate` + 9 methods |
| `sysdb/notify.rs` | `SystemDB.Notify` |
| `sysdb/postgres/notifier.rs` | `SystemDB.Postgres.Notifier` |
| `sysdb/postgres/listener.rs` | NOT PORTED (receive side stays polling-only) |

### P7.6 — engine on the class (this plan)

| Rust | Haskell |
|---|---|
| `instance.rs` / `registry.rs` / `handle.rs` / `workflow.rs` / `step.rs` / `sleep.rs` / `context.rs` / `event.rs` / `message.rs` / `queue.rs` / `dequeue.rs` / `recovery.rs` / `wait.rs` / `management.rs` / `client.rs` / `checkpoint.rs` / `error.rs` / `identity.rs` / `config.rs` / `select.rs` | from-scratch 1-to-1 rewrite per the L2 table above (DECIDED Q1); `Store.hs`, `SimDB`, and the old pool-function block deleted in the rewrite |
| starter | `app/Main.hs` rewired, same routes and page |

### P7.7 — `IOSimSystemDB` (port's own seam, no Rust counterpart). Phase 8
(widget-store: child workflows, typed IO, app error channel, app tables,
dispatch loop, stable identity) is future work.

## Autonomous execution constraints for the L4 gate

- **Loop**: AGENTS.md TDD loop verbatim — `make dev` owns `ghcid.txt`;
  read it after every reload; exactly one group enabled; one public test at
  a time; `cabal test` only when the watcher is idle; every test owns its
  rows; psql against `$DBOS_DATABASE_URL` after every green `cabal test`.
- **Fidelity**: HARD RULES verbatim. No `hasql-dynamic-statements`; static
  SQL with CASE/null-guards. No new dependency without the hoogle check.
  Glossary language from CONTEXT.md in test names. Plan deltas folded back
  into `.lavish/rust-port-plan.html` with dated notes after each layer.
- **Layer gates** (from the plan above): L1 = 105+n watcher green +
  `cabal test` 220+n + caller/child rows in psql; L2 = parked suites back
  + `cabal test` + psql mirror + Store/SimDB/old-block deleted (grep must
  show no `StepStore`, no `SimDB`, no `tryStartWorkflow pool`); L3 =
  `cabal build exe:dbos-hs-starter` + StarterTest mirror green; L4 = the
  W1/W2/Q/E/M side-by-side below.
- **L4 setup** (DECIDED Q5: fresh database per app, behavior compared,
  never rows): Haskell starter on `:8081` (fresh `dbos` database, `make
  db-migrate`, tmux `Haskell:4`), Rust oracle on `:8080` (fresh
  `dbos_rust_starter`, tmux `Haskell:5`), fresh workflow/queue ids every
  run. Drive: `POST /workflow/<id>` → poll `GET /last_step/<id>`;
  `POST /crash` → relaunch → poll; `POST /queue/enqueue` →
  `GET /queue/status`; `POST /events/start|read`, `GET /events/status`;
  `POST /messages/start|respond|respond-all`, `GET /messages/status`.
  Evidence: chrome-devtools-axi console log (page, timeline, polls,
  toasts) and the curl lines in the tmux panes, both sides behaving
  identically per docs/starter-e2e-gate.md. Ledgered gaps stay open:
  `set_event` checkpoint-row shape and any re-opened open-join case.

---

## L2 continuation notes (updated 2026-09-24)

L2 is **in progress**, not complete. The current verified gate is
`cabal test`: **271/271**, following `make db-migrate` (`108→108`), plus a
read-only `psql` check against `$DBOS_DATABASE_URL`. The suite covers the
class-backed DBOS launch, typed workflow start + replay adoption, one-pass
queue execution, durable step/sleep, events, and send/recv. L2 still has
substantial work: concurrent task tracking/cancellation, `WorkflowRef` and
child workflow handling, scheduler/management/client/select modules, the
StarterTest mirror, and removal of the v1 pool/Store path have not landed.

### Implemented in the current continuation

- `Transact.Config`: the 13-field Rust shape, env constructor, validation,
  outcome polling default, and `Serializer.RustSerde` / `rust_serde` name.
- `Transact.Identity`: environment snapshot and cloud/config precedence.
- `Transact.Context`: `WorkflowM` keeps `Ctx` private to the body; a hidden
  engine runtime carries the operation-id counter and optional backend. User
  selected the **hidden backend Ask** seam (2026-09-24): engine interpreters
  use `liftSystemDB`, client workflow bodies get no Postgres handle.
- `Transact.Registry`: triple identity, empty-string normalization, atomic
  snapshot/freeze/thaw, and typed JSON erasure; legacy registry functions
  remain temporarily for the v1 executor.
- `Transact.Instance`: `DBOS` lifecycle, identity resolution, verified
  Postgres acquire/activate, app-version registration, startup recovery,
  registry snapshot, automatic dequeue supervisor, orderly stop; tests use
  `launchWithEnvironment` to avoid process-environment cross-talk.
- Class-backed engine surfaces: `Transact.Step`, `Sleep`, `Workflow`,
  `Event`, `Message`, `Queue`, `Dequeue`, `Recovery`, and `Wait`. Queue tests
  exercise registration/update/list/delete, enqueuing and supervisor
  execution; the public workflow runner records output and adopts duplicate
  runs by awaiting the settled result.
- The facade now exports the engine `Transact.Error`; SystemDB errors remain
  available from `DBOS.SystemDB`. Constructor-prefix deviations include
  `ErrorConfig` and `ErrorWorkflow{NotRegistered,ClaimLost,Failed}` where
  constructor/type or in-flight v1 names collide.

### Remaining L2 order

1. Finish engine fidelity: correct queue backoff/local running counts and
   task ownership; retry/error replay wiring. DONE since: cooperative
   cancellation tokens, `StepStatus`/`StepScope`/`StepMarker`/deadline on
   `Ctx`; `WorkflowRef` with ref start/run and child start/result.
2. Port the remaining Rust modules one-to-one: `Select`, `Management` (full
   singular/bulk/list/delay/attribute surfaces), and the full
   `Wait`/`Queue`/`Dequeue`/`Recovery` surfaces. `Sleep` now has its own
   module; the existing Store-era Step and Workflow code is still present
   alongside the new class path.
3. Delete `Store.hs`, `SimDB`, `OperationCheckpointParse` /
   `OperationCheckpointReplay` / `OperationCheckpointTypes` as their
   one-module `Checkpoint` replacement lands, the old `Executor` and
   `Supervisor` modules, and the old `Postgres.hs` pool-function block.
   BLOCKED on L3 (verified 2026-09-25): `app/Main.hs` still runs the old
   seam (`postgresStepStore`, `runStep`/`sleepStep`, `tryStartWorkflow`,
   `spawnWorkflow`, `OperationId`/`OperationName`), and
   `exe:dbos-hs-starter` is already red on the pre-existing class-seam
   fallout (13 missing `DBOS.SystemDB` pool names) — deletion lands with
   the starter rewire, not before.
4. Unpark and port `StarterTest` group by group. Only after the L2 gate passes
   proceed to L3 app rewiring and L4 side-by-side E2E.

### L2 delta 2026-09-25 — Checkpoint, Handle, Client land

- `Transact.Checkpoint` (new, 9 tests): `StepPlacement` (`Outside`,
  `PlacementInsideStep`, `ClientConnection`, `Recorded`), `StepDurability`,
  `PendingStep`, `placementAt`/`placementHere`/`placementStepId`,
  `checkHere` with the oracle's refusal table (including the sibling-step
  "different step" wording), `describePlacement`/`placementWhereabouts`/
  `insideAWorkflow`. Deviations: `PlacementInsideStep` (facade already
  exports `Error.InsideStep`), `DurabilityRecorded`/`DurabilityPlain` (one
  module cannot hold two `Recorded`). Placement compared workflow id text
  until the Context completion below gave it the real marker: `checkHere`
  now compares `StepMarker` equality exactly like the oracle (see the
  Context delta). Same-execution pointer comparison stays workflow-id
  text — the executor pointer lives below the seam. DB-backed
  `check`/`record` stay per-caller (SystemDB owns them), as in the oracle.
- `Transact.Handle` (new, 2 tests): polling `WorkflowHandle`
  (`workflow_id`, backend, resolved poll interval, `Provenance`), `status`
  (absence is a value), `result` (adopts the recorded outcome; local-task
  await and durable parent-side `DBOS.getResult` are NOTE'd future work).
  `Instance.retrieveWorkflow` mints faithless polling handles
  (`fail_if_missing=False`), mirroring `DBOS::retrieve_workflow`.
- `Transact.Client` (new, 5 tests): `ClientConfig` (all 9 Rust fields),
  `clientConfigNew`/`clientConfigFromEnv`/`validateClientConfig`,
  `connectClient` (eager, verifies via acquire — never migrates),
  `closeClient`, `enqueueClientWorkflow`, `retrieveClientWorkflow`,
  `workflowStatusClient`. `Transact.Workflow` gains `Enqueue`/
  `DuplicationPolicy`/`enqueueNew`/`validateEnqueue`/`storedPriority`;
  `enqueueClientWorkflowWith` + `EnqueueOptions` honor every field
  (dedup join resolves the holder via `getDeduplicationKeyHolder`;
  `ReturnExisting` without a key, dedup+partition, and priority 0/overflow
  are refused before any write, as in the oracle).
- `Transact.Error` grows `NotInWorkflow`, `WrongInstance`,
  `InvalidArgument`, `StepBuiltElsewhere` with the oracle's `Display`
  shapes (kept verbatim — no collisions, so no prefixes).
- Lesson (faithful behavior, shared-DB flake): a client enqueue that
  records no version is claimed only by the latest registered version —
  same predicate as Rust `version_predicate`. The shared test DB holds a
  newer nameless version (`v-df1a4d23`), so unversioned client rows wait
  forever; the ClientTest pins `app_version` (exact match, immune to
  `latest` flips, parallel-safe). Two orphan `ENQUEUED` rows
  (`0795f7c6…`, `0940ffad…`) predate the pin — they sit on dead queues no
  sweep reads.
- Infra: OrbStack's dockerd wedged mid-session (port held, daemon mute);
  quit/reopen + `docker start postgres`, `make db-migrate` still
  `108→108`. A DB-blocked watcher eval never resolves on its own (libpq
  has no connect timeout here) — kill the GHCi child (not the watcher) and
  the pending reload fires. Watcher + DB healthy at close.

### L2 delta 2026-09-25 — Context + Workflow complete

- `Transact.Context` (17 tests): `StepStatus` (zero-based id, one-based
  attempt, cap; fields private behind readers, as the oracle's are
  `pub(crate)` behind `pub` methods), `firstStepStatus`/`nextAttempt`,
  `StepMarker` (`Data.Unique`, process-wide exactly like `NEXT_STEP_MARKER`)
  with `Eq`/`Show`, `StepScope` (marker+status travel as one),
  `withAttempt` (rebind, never mutate — scope dies with the body),
  per-attempt `TVar Bool` cancellation tokens (fire-and-poll receiving end,
  same contract as `CancellationToken`), `currentStepStatus`/
  `currentDeadline`/`inStep`, `runWorkflowMEngine` + `currentEngine` (the
  launched executor in pieces: backend, config, identity — bodies that
  start children read it instead of capturing a handle). Adaptations:
  executor pointer stays below the seam (no `WrongInstance` comparison),
  `runWorkflowStep` now scopes bodies via `withAttempt`. `Ctx` gains a
  third field (`ctxDeadline`); all constructors updated.
- `Transact.Workflow` (6 tests): `Timeout` (`Inherit`/`None`/`Explicit`)
  with `timeoutBudget`/`resolveTimeoutDeadline` (queued budgets leave the
  deadline null for the claim; inherited deadlines pass as instants),
  `RunOptions`/`StartOptions` (+ defaults, `runOptionsToStartOptions`),
  `childWorkflowId` (`{parent}-{step}` derivation, chosen wins),
  `resolveEnqueueCollision` (shared client/reference dedup settlement),
  `startWorkflowRef` / `runWorkflowRef` (+ `startDBOSWorkflowRef` /
  `runDBOSWorkflowRef` instance drivers), `startChildWorkflow` (ambient
  parentage, replay adopts the recorded child, joined dedup writes only
  the mapping, `InsideStep` inside a step). `runRegisteredWorkflowWithRow`
  is the shared execute-or-adopt core. Deviations: `run*`/`start*` option
  fields carry prefixes (facade owns the bare `EnqueueOptions` spellings).
- `Transact.Registry` (10 tests): `WorkflowRef` (registry + key, no `DBOS`
  handle — capturing one cannot pin an instance here), `refKey`/`refName`,
  `registerWorkflowRef`.
- Real bug found by the new tests: `runWorkflowRef` as start-then-run
  double-init could never execute — every init mints its own `owner_xid`,
  so the second init always reads a foreign owner and polls a PENDING row
  nothing runs. Fixed as single-init via the shared core (same lesson
  applies to any future caller: record and decide in one upsert). The
  `start` half stays record-only (no spawn — task ownership is still the
  open L2 item); started rows run via recovery, which the test exercises.
- Verification: `cabal test` 295/295, migrate `108→108`, psql mirror
  green (prescribed 7 rows + parent/child rows with the `parent_workflow_id`
  link and the parent step-0 `child_workflow_id` checkpoint).
- Watcher lesson (sharpens the earlier infra note): with
  `--no-interrupt-reloads` a hung eval wedges ALL reloads — later edits
  queue behind it silently (`ghcid.txt` untouched, old child idle). After
  killing a stuck child, `touch` a watched file if the watcher sits
  childless. Pane diagnosis: `tmux capture-pane -p -t Haskell:3.1` shows
  eval lines; `tasty` captures stdout so progress never streams — stderr
  (`hPutStrLn stderr`) streams live; remove breadcrumbs after.
- `CONTEXT.md` gains Step Status, Workflow Reference, Workflow Timeout.
- Still open for L2: `Select`, full `Management`, queue backoff/local
  counts, task ownership, retry wiring, legacy deletion, `StarterTest`.

### Cleanup 2026-09-25 — Codec, OperationCheckpoint*, WorkflowExecution*

- `WorkflowExecutionTypes` was a re-export shim: all 8 value types
  (`Serialization`, `SerializedWorkflowValue`, `WorkflowId`, …) are
  defined in `SystemDB.Types`. Shrunk to the 3 legacy row types
  (`WorkflowOutcome`, `WorkflowExecution`, `WorkflowExecutionRow`);
  19 import sites now point at `DBOS.SystemDB.Types` directly (one Rust
  type, one Haskell home again). No file deleted: the legacy trio is
  load-bearing for the starter path (`Executor`/`Workflow.runWorkflow`,
  pool decode, facade type exports).
- Facade drops the dead Bluefin store helpers
  (`withWorkflowExecutionStore`, `getWorkflowExecution`,
  `withOperationCheckpointStore`, `checkOperationExecution`,
  both store aliases, `OperationExecutionCheckError`) — zero users outside
  parked suites; sheds the Bluefin/Colog/`HasCallStack` imports. Legacy
  type + pure-function exports stay (`OperationId`, `parse…`, …).
  The two duplicate `SystemDB.Types` import blocks are merged.
- `Codec`: doc fix (`rust_serde`, not `json`) + oracle check (no
  divergence: same tag, absent-as-null, named halves).
- Legacy headers on all five modules (status, replaced-by, do-not-extend,
  deletion pointer). Gate: `cabal build lib:test` + `cabal test` 295/295
  + psql mirror green. `exe:dbos-hs-starter` stays red on pre-existing
  grounds only (verified: all 13 errors are `DBOS.SystemDB` pool names
  from the class seam; the app uses none of the pruned facade names).

### Audit + research 2026-09-25 — `rust-oracle-seam-audit.md`, `context-select-deep-research.md`

- Full name-for-name seam audit (5 oracle surveys + direct Haskell
  inventory): trait 61/61, backend 57 live + 4 streams `undefined`,
  `types.rs` complete, sysdb errors 15/15, engine error 16/23.
- Ranked gap list is audit §7; it supersedes older gap prose in this
  plan where they differ (notably: `Select` needs a macro-strategy
  decision — Template Haskell vs combinator library — before porting;
  `updateQueue` validator and `forkOptionsReplacementChildren` are
  verify-before-close items, not asserted gaps).
- The research doc records the mechanism rationale (what/why/where/how
  with oracle line citations) for context + select, plus the Haskell
  port-implication split (done / hard / decided) — read it before
  starting `Transact.Select`.

### Refactor delta 2026-09-25 — Bluefin/`WorkflowM` removed, explicit threading (ADR-0012)

- Decision (user, supersedes the Layer 2 shape above): delete `WorkflowM`
  and every Bluefin import; port the Rust context seam faithfully with
  explicit threading — `Ctx`/`Connection`/`Executor` indexed by `m`, every
  `SystemDB` method taking its backend first (`class Monad m => SystemDB db
  m`), `io-classes` + strict TVars for concurrency. Layer 2's "`WorkflowM`
  keeps `Ctx` private" bullet is superseded; bodies are now
  `argument -> Ctx IO -> IO result`.
- New seam, ported from `connection.rs`/`instance.rs`/`context.rs`:
  `Connection m` (`SomeSystemDB m` existential + `runSystemDB`, `Owner =
  OwnerApplication | OwnerClient`, `newConnection`/`forApplication`/
  `closeConnection`; `forClient` in `Client.hs`), `Ctx m` (`WorkflowState m`
  with `StrictTVar` step/marker counters and execution identity, `StepScope
  m`, readers, `withAttempt` rebind, `withSystemDB`), `Executor m {conn,
  identity, workflows, listen_queues, tasks}`, `Tasks m`
  (`spawnTracked`/`abortAll` over `MonadFork`/`MonadMask`/`MonadSTM`).
  Facade pins `IO`; the library stays polymorphic so `IOSim` runs the Tasks
  tests. Stage gates: class carrier 295/295, `Connection.hs`, Tasks (4
  IOSim tests) 299/299, consumer sweep + test migration 301/301.
- Recorded deviations: `Ctx` holds `Connection` + resolved `Identity` (not
  `Executor`) because `context.rs` ↔ `instance.rs` cannot cycle across
  Haskell modules; `forClient` lives in `Client`; `StepMarker = StepMarker
  Int` from a per-workflow counter (no `MonadIO` under `IOSim`, and every
  comparison also checks the workflow id); `DBOS.Transact` no longer
  exports `BackendErrorKind (..)` (its `Connection` constructor collides
  with `Connection`'s — reachable via `DBOS.SystemDB` only).
- Deleted: `WorkflowM`, the `bluefin`/`bluefin-internal` dependency,
  `test/DBOS/TransactTest.hs`, `test/DBOS/SystemDBHasqlTest.hs` (both
  Bluefin importers; parked and superseded). The stub backend for tests is
  `StubDB` in `ContextTest` (exports `stubConnection`/`testIdentity`/
  `testCtx`/`ctxOver`).
- Verification: `cabal test` 301/301, migrate `108→108`, psql mirror green
  (prescribed 7 rows + fresh `hs-l2-step-*` rows with their step-0
  operation outputs).
- Still open for L2: `Select` (macro-strategy decision first), full
  `Management`, queue backoff/local counts, retry wiring, legacy deletion
  (L3, blocked on the starter rewire), `StarterTest` port.

### Dequeue delta 2026-09-25 — local running counts, concurrent dispatch, backoff (cabal test 302/302, migrate 108→108, psql mirror green)

- `Transact.Dequeue` is now the faithful `dequeue.rs` port: `Running`
  tallies keyed by queue and queue-and-partition (Rust's `Key`), `Slot`
  claim/release (Rust's `Drop` becomes an explicit release run from the
  spawned task's `finally`), `workerBudget` over `ResolvedLimits`, the
  three `pollOnce` shapes (unpartitioned claim, batched partitioned sweep
  for `partition_concurrency == 1` with no shared limits, per-partition
  walk otherwise), `55P03` contention detection by `backendSqlState`,
  Fisher-Yates `shuffled` seeded from a UUID word, and the supervisor that
  transitions delayed workflows, rebuilds the queue set (keep-on-error,
  internal-queue row warned once and ignored), and spawns one tracked
  worker per queue whose interval is clamped to the row's own floor,
  doubles under contention, scales back by 0.9 when clean, and is jittered
  into [0.95, 1.05).
- Dispatch is concurrent: claimed rows are read in one `listWorkflows`
  round trip, walked in claim order (priority), and each spawned as a
  tracked task holding its slot, so worker concurrency is enforced locally
  without a database round trip. `dequeuePass` keeps its one-shot shape
  (fresh tally, one poll per queue, returns claimed ids) for the
  `dequeueDBOSWorkflows` driver.
- `Workflow` gains `maxRecoveryAttempts = 100` (workflow.rs) threaded into
  every run-path init, and `spawnRegisteredWorkflowWithRow`, which checks
  the row's own `PENDING` status before spawning and treats a parked row
  as a skip. `executeRegisteredWorkflow` is the extracted body+outcome
  core shared by run and spawn.
- Test (red first): "a queue's worker concurrency runs that many at once
  in one process" — `worker_concurrency = 2`, three gated bodies, asserts
  the peak concurrent bodies is exactly 2 and all three finish; it failed
  on the serial dispatcher before the port. Verification: `cabal test`
  302/302, watcher 4/4 on the queue group, psql shows the three SUCCESS
  rows and the queue's `worker_concurrency = 2` row.
- Still open for L2: step retry wiring (`StepOptions`/`ShouldRetry`/
  timeouts/preemptible), `Select` (macro strategy decision), full
  `Management`, remaining `Wait`/`Queue`/`Client` method forms, legacy
  deletion (L3).

### Process constraints carried forward

- Only `*Test` modules belong in the test suite's `other-modules`.
- LANGUAGE pragmas go above the `module` header.
- Exactly one `-- $>` group is active per watcher reload; switch both sides
  of the toggle. Read `ghcid.txt` immediately after each reload.
- `cabal test` only when the watcher is idle; after each successful full run,
  query the rows it exercised on `$DBOS_DATABASE_URL`.
- `make db-migrate` before build/test; keep ceiling 108.
