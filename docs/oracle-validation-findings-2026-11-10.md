# Oracle validation findings — implementation backlog

Date: 2026-11-10. Status: **open, unverified fixes**. No source files were changed to produce
this document.

## Implementation status and corrected fix directions (2026-11-10)

The findings are being landed in `docs/oracle-validation-fix-todo.md`. Review of this
document corrected several fix directions; where this section and a finding disagree,
this section wins:

- **F33** bounds raw transaction connections with a permit and runs the owner check
  before the app transaction commits. It compares `owner_xid` with `ownerToken wctx`,
  not `executor_id`, and the check is a separate system-database read, so "before
  commit" is not one atomic transaction across a split `APP_DATABASE_URL`.
- **F26** normalizes the text before `iso8601ParseM` rather than adding a stricter
  pre-check. Rust 0.3.55 accepts lowercase `t`/`z`, a space, and any single separator
  byte, and requires `±HH:MM`; the port now mirrors that. A leap second on a valid
  leap date stays a recorded deviation, not a silent difference.
- **F09** generates fork ids inside the sysdb method, before the retried transaction,
  not at the Management entry point. The crash window closes only when the work and
  the caller step share one transaction.
- **F08** is fixed with an idempotent per-scope restore, not a `MonadMask` cascade:
  the exception handler covers the depth increment and a `scopeRestored` flag stops a
  post-restore kill from decrementing twice.
- **F10** uses the `producers` `Tasks` registry as the worker registry. There is no
  second map; `spawnTracked` registers a worker and its departure hook removes it.
- **F18** puts the connection instance into `ExecutionIdentity` first, then wires
  `isSameExecution` into `checkHere` and `sameStepBody`.
- **F27** decided ADR-0038 option (b): keep the wire, record the typed cross-SDK
  replay limitation in the ADR, and pin the `name: message` fallback.
- **F05** keeps two required `WorkflowRecord` keys and three required `StepRecord`
  keys (`workflowId`, `stepId`, `stepName`).
- **F16**'s recordable examples are `InvalidInput`, `QueueDeduplicated`, and
  `ConflictingWorkflow`; `UnexpectedStep` is control (`shouldRecord = False`).
- **F37f** ports the named 64-bit `Int` fields to `Int32` as Rust spells them:
  priority, step id, rate-limit limit, stream offset/step id, and fork/rewind
  start steps, including the statement parameters they feed. Queue concurrency
  stays `Int` on the public options but is refused past `maxBound :: Int32`
  before the INT4 bind. An engine step id is the one remaining `Int`: it is
  monotone, engine-allocated, and its typed-SQL INT4 write fails loudly rather
  than truncating.
- The batch table's `F38` reference does not exist. F21, F25, F34, and F35 batch
  assignments are superseded by the per-finding statuses in the TODO.

## What this is

A module-by-module behavioral validation of the Haskell DBOS port against the Rust oracle,
`~/dev/dbos-transact-rust/crates/dbos/src` at `dbfb104` (`0.7.0-dev`, PRs #136–#161 already
ported). Twelve parallel source audits were run; every High finding below was re-read directly in
both trees, and overlapping findings were deduplicated. Each entry carries the evidence and a
concrete fix direction so an implementation agent can pick it up without repeating the
investigation.

**Legend.** `R:` = `crates/dbos/src/…`; `H:` = `src/DBOS/Internal/DBOS/…`; `test/` paths are
relative to `dbos-transact-hs`. Classes: **B** behavior gap, **T** Postgres
transaction/atomicity, **Rec** recovery, **A** async safety (deadlock/race/liveness-latency),
**L** laziness, **I** weakened type invariant.

**Severity.** High = durable wrong state, data corruption, or stuck workflow; Medium = wrong
behavior under a reachable condition or a significant latency/robustness regression; Low = edge
case, wire/interop, log shape, or latent structural drift.

## Repo rules that bind every fix (AGENTS.md)

- **Rust fidelity.** One Rust module → one Haskell module; one Rust type → one Haskell type by
  name. A split or rename needs an explicit ADR in `docs/adr/`. Keep the `SystemDB` spelling.
  Rust is the code oracle; Python/`dbos-agent-skills` are the spec oracle. Record any
  intentional divergence with a dated note.
- **Types before terms.** Land signatures/ADTs/smart constructors first; let `ghcid.txt` name
  every broken call site. Make illegal states unrepresentable — do not fix an invariant with a
  boolean where the oracle uses an enum.
- **Identifier sweeps** are word-boundary, counted, and verified; never touch string literals
  (`"DBOS.*"` wire names are data). After any sweep `grep -rn '"DBOS\.'` and compare against
  `sysdb/types.rs`.
- **No schema migrations in Haskell.** SQL changes go in `Statements.hs` / `Backend.hs` only;
  the schema is migrated by `make db-migrate` (Rust runner, ceiling 125; it was 114 when these findings were written).
- **Facade discipline.** Apps/tests/demos import through `DBOS.Transact` only. Every name
  imported into the facade stays exported.
- **TDD loop.** After **every** edit run `make ghcid-wait` (never sleep-poll). The watcher runs
  **sims only** (`*Sim.tests`, `SystemDBIOSim.tests`, `SimTest.tests` stay `-- $>`-enabled).
  One public Tasty test at a time; smallest passing implementation; `cabal test` full suite
  (~1000 tests, parallel, `-N4`); standalone binary for `--pattern` selection. Sim mirrors must
  drive the real engine functions (ADR-0020) — no staged effects, no hand-emitted events.
- **Three-leg gate** (ADR-0016) for each ported module: watcher sim group green with
  announcement lines; standalone `--pattern '$2 == "<Group>"'` green with FastLogger lines;
  `cargo test -p dbos --test <suite>` read-only green plus a structural trace comparison.
- **End-to-end proof.** After a green suite run, `psql` against `$DBOS_SYSTEM_DATABASE_URL`
  (not the default `dbos` DB) must show hundreds of recent `workflow_status` rows with a sane
  status mix; `operation_outputs` corroborated by volume. Demos: restart the server after
  rebuild, stop it before suite runs.
- **Commits wait for `/review`.** Do not commit these fixes unprompted.
- **No ad-hoc harness scripts in the repo.** Throwaway drivers live outside the workspace.

## Suggested fix batches (dependency order)

| Batch | Findings | Why grouped |
|---|---|---|
| **1 — retry/SQLSTATE** | F02, F03, F11, F29, F37(part) | One coherent change to `Retry.hs`, `Backend.serverKind`, `Dequeue.isContention`/`Polled`, and both queue-sweep call sites. Land together or the dequeue loop and the retry loop will disagree. |
| **2 — recovery ownership** | F01, F21 (claim), F25 | Small SQL fix first; it is the highest-confidence correctness bug and needs no API change. |
| **3 — validation & replay** | F04, F05, F06, F12, F15, F31, F32 | Pure guards/codecs; independent of the concurrency work. Update the test pins that currently enshrine the wrong behavior. |
| **4 — transactions & atomicity** | F09, F17, F20, F33, F34, F36 | Largest batch: `runCallerStep`/`caller` plumbing, one-transaction management methods, `FOR UPDATE`, post-write owner resolve. Needs careful dual-stack (live + sim) tests. |
| **5 — concurrency** | F07, F08, F10, F19, F22, F35, F38, F30 | Independent per module; each needs a sim trace pin. |
| **6 — types/clock/invariants** | F13, F14, F16, F18, F23, F24, F26, F27, F28 | Mostly `Types.hs` / small pure functions. |
| **7 — low-severity sweep** | F19(part), F21(part), F33–F37 remainder | Bundle log/plan/latency cleanups. |

Each entry below has a `Batch:` field. Fix in batch order within a cluster; do not refactor
while red.

---

## Index

| ID | Sev | Class | Batch | Module | One-line |
|---|---|---|---|---|---|
| F01 | High | Rec/A | 2 | `Statements.reenqueueForRecoverySession` | Recovery re-enqueue never clears `owner_xid` |
| F02 | High | B/A | 1 | `SystemDB/Retry.hs` | Two retry schedules collapsed; no `retry_conflicts` switch |
| F03 | High | B | 1 | `Backend.serverKind` | Whole SQLSTATE class `40` → `Transient` |
| F04 | High | B | 3 | `Workflow.validateEnqueue` | Refuses `deduplication_id` + `partition_key` together |
| F05 | High | B/I | 3 | `FromJSON WorkflowRecord`/`StepRecord` | All keys required; no `serde(default)` equivalent |
| F06 | High | B | 3 | `Types.validateNewWorkflow` | Omits `config_name` empty-string refusal |
| F07 | High | B/A | 5 | `Step.hs` preemptible | Cancellation only checked between attempts |
| F08 | High | A | 5 | `Context.withStep` | Unmasked shared `stepDepth` window → uncheckpointed steps |
| F09 | High | T/Rec | 4 | `Backend.forkWorkflows`, `Management.*` | Management work + step checkpoint in separate commits; fork ids minted inside body |
| F10 | High | A | 5 | `Dequeue.superviseForever` | Blind `writeTVar workers kept` lost update → queue starves |
| F11 | Med | B/A | 1 | `Dequeue.isContention` | Only `55P03`; misses `40001` and inverts backoff |
| F12 | Med | B/I | 3 | `Workflow.runWorkflowRef` | Never calls `validateStart` |
| F13 | Med | B/A | 6 | deadline SQL | `now()+timeout` without `numeric` saturating clamp |
| F14 | Med | B | 6 | `Types.queueResolvedLimits` | Legacy `partition_queue` re-scoping Rust deleted in #88 |
| F15 | Med | B/Rec | 3 | `Workflow.shouldRecordStartError` | Omits `InvalidInput` |
| F16 | Med | I | 6 | `Select.controlError` | `SystemDatabase {} -> True` wildcard bypasses `controlOf` |
| F17 | Med | T | 4 | `Queue.hs` → `Backend.updateQueue` | No-op validator passed; validation outside the row lock |
| F18 | Med | I | 6 | `Checkpoint.checkHere` | Compares workflow-id text; `isSameExecution` is dead code |
| F19 | Med | A | 5 | `releasePostgresSystemDB` | `Pool.release` is a reset, not a close; no `PoolClosed → Permanent` |
| F20 | Med | A | 5 | `Backend.sendInternal` | No local wake on `send` → ≥1 s `recv` latency |
| F21 | Med | T | 4 | several sysdb methods | Split critical sections (resume, versions, upsert_queue, executor claim, recv timeout, checkpoint_sleep) |
| F22 | Med | A/B | 5 | `race`/`timeout` sites | Rust's `biased select!` ordering lost |
| F23 | Med | B | 6 | `Types.durationIsZero` | Floors to ms, refusing positive sub-ms durations |
| F24 | Med | I | 6 | `Types.FromJSON Duration` / `durationToMs` | Accepts negative JSON; no lower bound |
| F25 | Med | B | 6 | `Types.databaseTimeNow`/`Reaches` | No `saturating_*`; `Word64` subtraction wraps |
| F26 | Med | B | 6 | `Types.timestampFromIso8601` | Rfc3339 divergence |
| F27 | Low–Med | B/I | 6 | `Error.hs` codec | Portable error record shape diverges from Rust |
| F28 | Low | L | 5 | `Event.driveSetEvent` | Step id claimed before the value encode is forced |
| F29 | Low | L | 1 | `Retry.withRetry` | Lazy `attempt + 1` thunk chain under a filtered tracer |
| F30 | Low | L | 5 | `Workflow.TaskState.live`, `Notify.Waiter` | Lazy `Int` accumulators in WHNF-only records |
| F31 | Low | B | 3 | `Step.replayStep` | NULL-output step refuses instead of decoding `"null"` |
| F32 | Low | B | 3 | `Workflow.adoptRecordedFailure` | Missing `WorkflowNotFound`; `adoptSuperseded` TOCTOU |
| F33 | Low | T/A | 4 | `Datasource` | No in-transaction `_still_owns`; raw connection per attempt |
| F34 | Low | A | 5 | `Instance.shutdownWith` | Untracked teardown `forkIO` + `takeMVar` can hang |
| F35 | Low | B | 4 | `Workflow.initOrJoin` | Joins on caller's dedup key instead of the error's |
| F36 | Low | B/A | 4 | misc | `recv` timeout split; `selectWorkflow` replay; `dispatch` order; `register_queue` read-back |
| F37 | Low | B/A | 7 | misc | Index-defeating ORDER BY; host-clock `updated_at`; extra `FOR NO KEY UPDATE`; idempotency; log shape |

---

# Detailed findings

## F01 — Recovery re-enqueue never clears `owner_xid`

- **Sev/Class/Batch:** High · Rec, A, B · 2 · Confidence **High** (found independently by the SQL and recovery audits).
- **Oracle (R):** `sysdb/postgres.rs:4390-4403` — `reenqueue_for_recovery` sets `status='ENQUEUED'`, `started_at_epoch_ms=NULL`, **`owner_xid = NULL`**, `updated_at=now`, `queue_name=COALESCE(NULLIF(queue_name,''),$1)`. Comment at `:4383-4385`: clearing the token makes a still-writing dead run's *next write* refused by `check_owner_on`; the dequeue claim then writes its own token.
- **Port (H):** `SystemDB/Postgres/Statements.hs:2300-2311` — identical statement **without** `owner_xid = null`. Instance `SystemDB/Postgres/Backend.hs:2957-2965` (`reenqueueForRecovery`) adds no second write. `grep -n 'owner_xid = null' Statements.hs` → 7 hits (550, 1529, 1770, 1792, 1819, 1848, 1881), none at 2300; sibling sweeps (`cancelBatch`, `deadLetter`, `resume`) all clear it. `checkOwnerTx` (`Statements.hs:1369-1374`) checks only CANCELLED + the owner token.
- **Failure scenario:** startup recovery re-enqueues a dead executor's PENDING rows. The dead `owner_xid` survives until a later dequeue claim overwrites it. With a shared default executor id (`"local"`), a rolling restart, or a not-fully-dead prior process, the superseded execution still holds a valid token, passes `checkOwnerTx`, and keeps checkpointing; the new claim races it. Exactly-once/recovery correctness break.
- **Root cause:** isolated omission of one SET clause; the Haskell comment even narrates only `started_at_epoch_ms`.
- **Fix:** add `owner_xid = null,` to `reenqueueForRecoverySession`; update its Haddock to state the ownership invalidation; add the same clause to the mem/mock mirror (`test/DBOS/SystemDB/IOSim.hs`) if it models the sweep.
- **Tests:** a live `*Test` case that plants a PENDING row with a non-null `owner_xid`, runs the recovery sweep, and asserts the token is NULL and a subsequent `recordStep` under the old token is refused; twin it in `*Sim`/mem. Recovery group in the watcher must stay green.
- **Related:** F09 (atomicity), F21 (executor claim). No ADR covers this; the seam audit marks the query "✓ same shape" — update `docs/rust-oracle-seam-audit.md` when fixed.

## F02 — Two retry schedules collapsed into one; no conflict-replay switch

- **Sev/Class/Batch:** High · B, A (latency) · 1 · Confidence **High**.
- **Oracle (R):** `sysdb/retry.rs:44-76` — `RetryPolicy { initial_backoff=1s, max_backoff=60s, retry_connection_errors, retry_conflicts, conflict_initial_backoff=50ms, conflict_max_backoff=2s }`. `:84-89` `without_conflict_replay()`. `:113-114` two independent counters; `:130-145` Connection uses `jitter(backoff)` doubling to 60s, Conflict uses `jitter(conflict_backoff)` doubling to 2s, `loud = conflict_backoff >= conflict_max_backoff` (DEBUG until ceiling, WARN after). Used at `postgres.rs:6743` and `:7140` for both dequeues: *"Conflicts are not replayed here … the caller's answer to losing a race … is the right one."*
- **Port (H):** `SystemDB/Retry.hs:86-100` — 3 fields only (`initialBackoff=1s`, `maxBackoff=60s`, `retryConnectionErrors`). `:107-114` `shouldRetry` hardcodes `Transient -> True` ("whatever the policy says"). `:136-151` `go backoff attempt` with one counter for every retriable kind. `Backend.hs:3732`/`3815` both queue sweeps use `defaultRetryPolicy`. Whole-tree grep: no `retry_conflicts` / `without_conflict_replay`.
- **Failure scenario:** a `40P01`/`40001` conflict waits 0.5–1.5 s and doubles to **60 s** instead of 25–75 ms → 2 s; a prior connection burst pins later conflicts at 60 s. Under sustained checkpoint contention Rust clears in tens of ms, the port stalls a workflow for seconds-to-minutes. Dequeues cannot obtain a conflict as `Left` (see F11).
- **Fix:** extend `RetryPolicy` with `retryPolicyRetryConflicts :: Bool`, `retryPolicyConflictInitialBackoff :: Duration` (50 ms), `retryPolicyConflictMaxBackoff :: Duration` (2 s); add `withoutConflictReplay :: RetryPolicy -> RetryPolicy`. Split `go` into two counters keyed on `backendKind` (`Transient` = conflict, `Connection` = connection); keep `shouldRetry` returning `False` for conflicts when the flag is off. Split the log event into conflict-DEBUG / loud-WARN (see F37). Thread a `withoutConflictReplay defaultRetryPolicy` into both dequeue sweep call sites (`Backend.hs:3732`, `:3815`) and any other caller that owns its own answer to a lost race. `BackendErrorKind` already distinguishes `Transient` (wire tag `"Conflict"`) from `Connection` — no new type needed.
- **Tests:** `test/DBOS/Transact/RetryTest.hs` (`:50-53` currently pins `Transient -> True` — update); add pure cases for both schedules, the shared-counter bug, the flag, and DEBUG/WARN; IOSim timing cases for jitter bands. Add a dequeue case proving a `40001` reaches the poll loop.
- **Related:** F03, F11, F29, F37. ADR-0008 (`io-classes` retry seam) still holds; keep pure `shouldRetry`/`jitter` pure.

## F03 — Whole SQLSTATE class `40` classified `Transient`

- **Sev/Class/Batch:** High · B · 1 · Confidence **High**.
- **Oracle (R):** `sysdb/postgres.rs:242-244` `if matches!(code, "40001" | "40P01") { return Conflict; }`; the final class match (`:273-285`) has **no `"40"` arm** → everything else falls to `Permanent`. Rationale at `:167-173`: `40002` (integrity constraint at commit) is a refusal; `40003` (statement completion unknown, CockroachDB) *"the transaction may have committed and a replay could apply it twice."*
- **Port (H):** `SystemDB/Postgres/Backend.hs:898-904` `serverKind … case Text.take 2 code of "40" -> Transient`. Feeds `classifyUsageError` → `withRetry` (`Retry.hs:111` `Transient -> True`).
- **Failure scenario:** a `40003`/`40002` is retried forever by the unbounded `withRetry`; on `40003` the write may already be committed → double-apply. The port's own docs (`Backend.hs:862`, `Datasource.hs:438`, ADR-0021:25) describe whole-class-40 — those notes are wrong against the current oracle.
- **Fix:** replace the `"40" -> Transient` arm with exact-code checks `code == "40001" || code == "40P01"`; everything else in class 40 → `Permanent`. Correct the three doc notes. Keep `08P01` severity split as the documented deviation (hasql carries no severity).
- **Tests:** `test/DBOS/Transact/*` unit table for `serverKind` with `40001`, `40P01`, `40002`, `40003`, `40P02`; two live refusal probes already exist for #153 (malformed bind → `Permanent` + `08P01`, statement timeout → `Permanent` + `57014`) — extend the table. Oracle leg: `cargo test -p dbos --lib` `classify_sqlstate` tests read-only.
- **Related:** F02, F11. PR #153 was ported recently; re-check the whole carve-out table while here.

## F04 — `validateEnqueue` refuses `deduplication_id` + `partition_key`

- **Sev/Class/Batch:** High · B · 3 · Confidence **High**.
- **Oracle (R):** `workflow.rs:553-598` (`QueueRequest::validate`) refuses only *empty* `deduplication_id`/`partition_key`, `ReturnExisting` without a key, priority range, and a bad delay — **no "both set" arm**. Doc `workflow.rs:439-440`: a dedup id beside a partition key *"is held across the whole queue, not within this partition."* Integration test `crates/dbos/tests/queues.rs:1888-1935` (`a_deduplication_id_is_held_across_partitions`) sets both and expects success; `tests/client.rs:678-712` pins the same for `Client::enqueue_with`.
- **Port (H):** `Transact/Workflow.hs:537-540` — `| Just _ <- queue.deduplicationId, Just _ <- queue.partitionKey = refuse "… cannot both be set …"`. `test/DBOS/Transact/ClientTest.hs:398-404` pins this refusal; `docs/p76-tdd-plan.md:332` records the stale 2026-09 belief "refused … as in the oracle".
- **Failure scenario:** a start or client enqueue onto a partitioned queue with a dedup key returns `InvalidArgument` and writes no row; Rust/Python write the row and hold the key across all partitions. No Haskell caller can reproduce a partitioned+dedup backlog.
- **Fix:** delete the third guard in `validateEnqueue`; verify the dedup-key uniqueness SQL already spans partitions (`Statements.hs` `ON CONFLICT` / dedup index). Update `ClientTest.hs:398-404` to assert success + cross-partition key holding; add a live + sim scenario mirroring `a_deduplication_id_is_held_across_partitions`. Correct `docs/p76-tdd-plan.md:332`.
- **Related:** F12, F35. Watch wire literals — this is a guard change, not a string sweep.

## F05 — `FromJSON WorkflowRecord` / `StepRecord` require every key

- **Sev/Class/Batch:** High · B, I · 3 · Confidence **High**.
- **Oracle (R):** `sysdb/types.rs:525-539` documents *"every field here carries `#[serde(default)]`"* bar the two NOT NULL columns, because a checkpoint is written on one deployment and replayed on the next; without it a new column makes an older checkpoint fail with `Error::Malformed` on the replay path, and the workflow can never get past that step. `#[serde(default)]` on `:544-687` (`WorkflowRecord`) and `:1530-1568` (`StepRecord`).
- **Port (H):** `SystemDB/Types.hs:2534-2575` (`FromJSON WorkflowRecord`) and `:2591-2602` (`FromJSON StepRecord`) use `o .: "…"` for all ~37 / 9 fields — no `.!=`, no `.:?`. Aeson's `(.:)` fails on a missing key even at `Maybe a`. The port already added `workflowRecordCreatorXid`, which Rust does not have, so a build bump will break live replays.
- **Callers:** `Transact/Management.hs:821-829` (`listWorkflowsInWorkflow` checkpoints `[WorkflowRecord]`), `:807-816` (`(Bool,[StepRecord])`); replay decodes via `Transact/Serialization.hs:238-242`.
- **Failure scenario:** deploy a build that adds a record field → every checkpoint from the prior build fails with `key "…" not found` → `Malformed` on replay → workflow stuck at that step forever.
- **Fix:** give every non-key field a default in `FromJSON`: `Maybe` fields `o .:? "…"` (defaulting to `Nothing`), numeric/list/boolean fields `o .:? "…" .!= <zero/False/[]>`, and the two NOT NULL keys keep `.:`. Prefer a small helper (`fieldWithDefault`) so the change is uniform and auditable. Keep `ToJSON` unchanged. Add a round-trip test that decodes a JSON object with an unknown extra key and with several keys omitted.
- **Constraint:** this is a *decoder* change only — do not add a schema migration, do not rename wire keys. If a default cannot be chosen for a field, that is a design question: record it with a dated note rather than guessing.

## F06 — `validateNewWorkflow` omits `config_name`

- **Sev/Class/Batch:** High · B · 3 · Confidence **High** (code), Medium (reachability: `NewWorkflow` is directly constructible).
- **Oracle (R):** `sysdb/types.rs:853-870` — the "must be absent rather than empty" loop includes `("config_name", &self.config_name)`.
- **Port (H):** `SystemDB/Types.hs:405-414` — `optionalFields` lists `class_name`, `queue_name`, `deduplication_id`, `queue_partition_key`, `schedule_name`, `input`, `serialization`, `application_version`; **`config_name` missing**. `newWorkflowConfigName = Just ""` validates `Right ()` and is written as `''` (`Statements.hs:277`). Callers: `Backend.hs:1532, 2747, 2831`; `Client.hs:416`.
- **Failure scenario:** breaks the "one spelling of absence" invariant (#136) that dequeue/registry resolution depends on; a nameless/empty config reaches the row where Rust refuses before any write.
- **Fix:** add `("config_name", new.newWorkflowConfigName)` to `optionalFields` in the oracle's order (after `class_name`). Add a unit case pinning `Just ""` → `InvalidInput "config_name" "must be absent rather than empty"`.
- **Related:** the field-name mapping `newWorkflowAppVersion` ↔ Rust `application_version` is a naming deviation, not a behavior bug — leave it.

## F07 — Preemptible steps never observe cancellation during the body

- **Sev/Class/Batch:** High · B, A (liveness) · 5 · Confidence **High**.
- **Oracle (R):** `step.rs:669-729` `supervised` builds `cancelled = observe_cancellation(ctx)` and races it *concurrently* in `tokio::select! { biased; outcome = attempt => …, () = cancelled => { token.cancel(); Err(WorkflowCancelled) }, () = deadline => … }`. `observe_cancellation` (`:743-776`) loops on `await_workflow_result(ctx.workflow_id(), outcome_poll_interval, true)` for the step's whole life; `:650-661` gives each attempt a `CancellationToken` + drop guard. Doc `:86-91`: "a periodic status read per running step".
- **Port (H):** `Transact/Step.hs:459-470` `attemptLoop` polls once per attempt: `preempted <- if options.preemptible then checkCancelled workflowId' else pure False`; `checkCancelled` (`:519-523`) is a single `getWorkflow`. `rest` (`:471-489`) races only `options.timeout`. The exported `raceCancel`/`tokenCancelled`/`stepCtxCancellationToken` (`Context.hs:409-423`) have **no call sites in the step path**; `awaitWorkflowResult` (`Handle.hs:232`, `Wait.hs:153`, `Workflow.hs:236`) is not wired to steps.
- **Failure scenario:** a 30-minute `preemptible` step cancelled at t=1 min. Rust fires the per-attempt token within one poll interval, drops the attempt, records nothing, row stays PENDING. Haskell runs the full 30 minutes, never fires the token, side effects continue; only the final `recordStep` notices CANCELLED via `checkOwnerTx`.
- **Fix:** in `rest`, when `options.preemptible`, race the body against a cancellation observer (an `awaitWorkflowResult`-shaped poll at `connOutcomePollInterval`) as well as the timeout; on the cancellation arm fire the step's cancellation token, drop/cancel the body task, and return the `WorkflowCancelled` control error **without** recording. Keep the existing pre-attempt `checkCancelled` as a cheap fast path. Use a *biased* ordering that prefers a completed body (see F22). Reuse the existing `Context` token seam rather than adding a new mechanism.
- **Tests:** live + sim case: a long preemptible step with no timeout, cancelled externally; assert the token fires, the body stops early, no step row is recorded, and recovery resumes. Add a sim trace pin for the announcement. Oracle leg `cargo test -p dbos --test step` / cancellation tests read-only.

## F08 — `withStep` unmasked shared `stepDepth` window

- **Sev/Class/Batch:** High · A (race), B (durability) · 5 · Confidence Medium-High (code certain; needs a kill in a narrow window).
- **Oracle (R):** `context.rs:455-456, 476-494` — `in_step_scope` rebinds a **per-task** `Ctx` via `CURRENT.scope(scoped, body).await`; `Ctx::in_step()` reads that task's own `step`. No shared counter, no teardown window. Rust comment `:471-475`: no shared flag needs a guard to clear.
- **Port (H):** `Transact/Context.hs:214-219` shared `stepDepthRef :: StrictTVar m Int`; `insideAStep` (`:391-392`). `withStep` (`:520-527`):
  ```haskell
  atomically (modifyTVar wctx.wctxState.stepDepthRef (+ 1))          -- 524: unmasked
  outcome <- body (StepCtx wctx (Just scope)) `onException`           -- 525
               atomically (writeTVar scope.scopeCancellation True
                           >> modifyTVar wctx.wctxState.stepDepthRef (subtract 1))
  atomically (modifyTVar wctx.wctxState.stepDepthRef (subtract 1))    -- 526: unmasked
  ```
  Takes `MonadCatch`, **not `MonadMask`**; no `mask`, no `finally`. `docs/scoped-workflow-capabilities.md:179,191` claims it restores under `finally` — the doc is wrong. `detachChildWrite` (`Workflow.hs:1237`) does use `mask`.
- **Failure scenario:** an async exception (shutdown/cancel, `Select.hs:308` `race` loser kill) lands (i) between the increment and `onException` installation, or (ii) after `body` returns and before the decrement. `stepDepth` stays ≥ 1 for that `WorkflowCtx`; every later durable call reads `insideAStep == True` and takes the `DurabilityPlain` branch (`Step.hs:174-179` — no `checkStep`, no `recordStep`, no id). On crash+recovery those steps re-execute their side effects. Note `onException`'s own `atomically` cleanup is also unmasked, so a second kill mid-transaction rolls the decrement back.
- **Fix:** take `MonadMask` and wrap increment → body → decrement in one `mask $ \restore -> …` with `finally`-equivalent cleanup (both success and exception paths), or follow Rust and replace the shared counter with a per-task scope value (larger refactor — needs an ADR). At minimum: mask the increment, install the cleanup handler before the increment, and use `onException`/`finally` under the mask. Update the lying comments in `Context.hs:515-517` and `docs/scoped-workflow-capabilities.md`.
- **Tests:** a dual-stack `Context` case killing the body at the exact window (use `IOSim` scheduling) and asserting depth returns to 0; a step case proving a later step still records after an aborted one. Do **not** fix by merely clamping depth on read — that hides the invariant loss.
- **Related:** F22, F30, F34.

## F09 — Management work and its step checkpoint commit separately; fork ids minted inside the body

- **Sev/Class/Batch:** High · T, Rec, B · 4 · Confidence **High** (found by three audits).
- **Oracle (R):** `management.rs:651+` (`fork_workflows`, `cancel_workflow`, `resume_workflow`, `rewind_workflow`) are `run_transactional_step` methods — work + the caller's step checkpoint in **one** transaction (`postgres.rs:965-1011`: owner check, `check_step_on`, work, `record_step_output`). `postgres.rs:4810-4846` generates fork ids **outside** the retry with the explicit warning *"a fresh id on the second attempt would not recognise the first attempt's write, so a lost commit acknowledgement would fork every workflow twice"* (`:4826-4828`). Used by `cancel_workflows` (`:4556`), `resume_workflows` (`:4646`), `delete_workflows` (`:4771`), `fork_batch` (`:1532`).
- **Port (H):** sysdb methods bind and discard the caller: `Backend.hs:2986, 3035, 3113, 3125` (`_caller`). `Backend.hs:3113-3123` `forkWorkflows` mints ids **inside** the body (`UUID.V4.nextRandom`) and `runFork` (`:2366`) commits them alone; `forkFrom` (`:3125`) same; `rewindWorkflow` splits `rewindTx` (`:3181`) from `recordStepSession` (`:3194`). `Transact/Management.hs:153-157, 186-190, 259-269, 321-325` wraps in ordinary `runStepWith` → `Step.hs:185-218` does `checkStep` / body / `recordStep` as **three** separate sysdb calls. `Class.hs:79-94` carries the `Maybe (WorkflowId, Int)` caller that the Postgres backend discards. Schedules are the exception: `Backend.hs:3937-4033` uses `runCallerStep`.
- **Failure scenario:** crash (or lost commit ack) between the fork transaction and the engine's checkpoint → fork rows durable, no step recorded. Recovery's `checkStep` finds nothing, `runStepWith` re-runs the body, `forkWorkflows` mints **new** UUIDs, `forkTx` inserts a second set of forks. cancel/resume converge (status-guarded) but rewind/fork duplicate durable workflow trees. Also the existence-check ↔ update split (F21).
- **Fix:** (1) generate fork ids **outside** the retried region, at the `Management` entry point (before `runStepWith`), and pass them down — stable across a replay; (2) make the sysdb management methods honor their `caller` and run work + `check_step` + `record_step_output` inside one `runTransaction`, mirroring `runCallerStep`/`run_transactional_step`; (3) update the `SystemDB` class, Postgres backend, mem/mock (`test/DBOS/SystemDB/IOSim.hs`) and all engine/facade call sites in one sweep; keep the class method shapes one-to-one with the Rust trait. (4) Delete the now-redundant engine-level `runStepWith` wrap for these methods (or keep it as the `PendingStep` face only).
- **Constraint:** a split of `registry.rs`-style seams needs an ADR — do not split a Rust module here; this is one method-shape change across existing modules. Keep the `DBOS.forkWorkflow` step name literal (see #154).
- **Tests:** a crash-and-resume case (fork a supervisor, kill between the fork commit and the checkpoint, relaunch, assert no duplicate fork rows and one fork count), live + sim + psql read-back of `forked_from`. Oracle leg: `cargo test -p dbos --test management` / `--test recovery` read-only. Update the existing management scenario pins if their step counts change.
- **Related:** F21, F01, F32. ADR-0011 covers only `set_event`/`get_event`/`init_workflow`; this extends the same discipline — record the decision with a dated note.

## F10 — `superviseForever` lost update resurrects a dead worker

- **Sev/Class/Batch:** High · A (race/liveness) · 5 · Confidence **High**.
- **Oracle (R):** `dequeue.rs:177` `workers: HashMap<String, JoinHandle<()>>` **local to the single supervisor**; `:197` `workers.retain(|_, h| !h.is_finished())`; `:215` insert. No concurrent writer.
- **Port (H):** `Transact/Dequeue.hs:632-633` `workers <- newTVarIO Map.empty` **shared with workers**. `:646-649`: `current <- readTVarIO workers; let kept = Map.filterWithKey …; atomically (writeTVar workers kept)` — a blind write. Worker `:656-658`: `finally (pollQueue …) (atomically (modifyTVar workers (Map.delete name)))`. Supervisor `:663-665`: `Just tid -> atomically (modifyTVar workers (Map.insert name tid))`.
- **Interleavings:** (a) supervisor reads `{Q: tid1}`; Q's `finally` deletes Q; supervisor blind-writes the stale `kept={Q: tid1}` back; `unless (Map.member Q kept)` skips the respawn and every later loop repeats → **no living worker for Q, forever**, rows stay ENQUEUED. (b) the `finally` delete lands before the supervisor's `Map.insert`, so a dead `ThreadId` is inserted. (c) a name-keyed delete removes a *replacement* worker's entry.
- **Fix:** make the workers registry supervisor-local (a pure `Map` threaded through `loop`, as Rust does), or hold a single `atomically` transaction that reads-and-writes the map in one commit and have workers report liveness by `ThreadId` (not by queue name). Reap by `isFinished`/thread liveness, not by name. Never `writeTVar` a value computed from a `readTVarIO` outside the transaction.
- **Tests:** a sim case with a queue deleted and re-registered within one supervisor tick; assert exactly one living poller and that the queue still drains. Add a trace pin for spawn/reap. Oracle leg `cargo test -p dbos --test queues` read-only.
- **Related:** F36 (dispatch order), F22.

## F11 — `isContention` misses `40001` and inverts the backoff decision

- **Sev/Class/Batch:** Medium · B, A, I · 1 · Confidence **High**.
- **Oracle (R):** `dequeue.rs:848-852` `Contention::SerializationFailure::backs_off() == true`, `LockNotAvailable` false (comment: a 55P03 peer commits in milliseconds, backing off only leaves work unclaimed; 40001 contenders keep colliding, so back off to spread them). `:859-868` `contention()` matches `55P03 => LockNotAvailable`, `40001 => SerializationFailure`. `:354-360` uses `is_some_and(backs_off)`; `Polled.contention: Option<Contention>` (`:384-389`).
- **Port (H):** `Transact/Dequeue.hs:383-385` `isContention (Backend backend) = backend.backendSqlState == Just "55P03"`. `:219` `polledContention :: Bool`. `:616-620` `if contended then min (×2) ceiling else max (×0.9) floorInterval`. Whole-tree grep: no `40001`/`SerializationFailure` in `src/`.
- **Failure scenario:** sustained `40001` under REPEATABLE READ / CockroachDB: Rust backs off toward 120 s; Haskell logs `DequeueFailed` (WARN) and **scales back toward the 1 s floor** — a contention storm. A `55P03`: Rust keeps the floor cadence; Haskell doubles toward 120 s, leaving work unclaimed. The two-variant enum collapsed to `Bool` (weakened invariant), and F02's `withRetry` swallows `40001` before it can reach here.
- **Fix:** port `Contention` as an ADT (`ContentionLockNotAvailable | ContentionSerializationFailure`) or a verdict type with a smart constructor; carry `Maybe Contention` in `Polled`; implement `backsOff` exhaustively with no wildcard; map `40001` and `55P03`. Combine with F02 so the dequeue sees the conflict at all.
- **Tests:** unit cases for both codes and the no-backoff/backoff decision; a sim case for a contention burst asserting interval direction. Update `Dequeue.hs` trace expectations.
- **Related:** F02, F03, F22.

## F12 — `runWorkflowRef` never calls `validateStart`

- **Sev/Class/Batch:** Medium · B, I · 3 · Confidence **High**.
- **Oracle (R):** `workflow.rs:1045-1051` `run_with` → `place(&options)`; `place` (`:1116-1127`) runs `validate_start(options.queue.as_ref(), options.timeout.budget(), options.workflow_id, options.workflow_id_reuse_policy)?` **before** `StepPlacement::of` takes a step id.
- **Port (H):** `Transact/Workflow.hs:1185-1196` `runWorkflowRef` builds `new` and calls `runRegisteredWorkflowWithRow` (`:139-146`) directly. `validateStart` (`:579`) is called only at `:722` (`validateEnqueueOptions`), `:1008` (`startWorkflow`), `:1421` (`startChildWorkflowWith`). `runRegisteredWorkflowWithRow` does not validate either. `Backend.hs:1488-1489` refuses `WorkflowIdReject` only when a conflicting row already exists.
- **Failure scenario:** `runWorkflowRef ref (runOptionsDefault { runWorkflowIdReusePolicy = WorkflowIdReject })` → Rust refuses `InvalidArgument` with no row; Haskell generates a UUID and **runs the workflow**. An empty `runWorkflowId` or a zero `Explicit` timeout becomes an insert-time `InvalidInput`/`SystemDatabase` refusal (`fromSystemDB` at `:147`) or writes a zero budget — wrong channel, late, and after a step id is spent.
- **Fix:** call `validateStart` at the top of `runWorkflowRef` (with `Nothing` queue, `timeoutBudget options.runTimeout`, the offered id, and `options.runWorkflowIdReusePolicy`) before building `new`; map its `Either` into the function's `Error e` channel. Also audit `runRegisteredWorkflowWithRow`'s other callers for the same omission.
- **Tests:** unit cases for `Reject`-without-id, empty id, zero explicit timeout on `runWorkflowRef`; assert no DB write (spy the sysdb mock in sim). Add a live case if the mock cannot prove it.
- **Related:** F04, F15, F35.

## F13 — Deadline arithmetic lacks the saturating clamp

- **Sev/Class/Batch:** Medium · B, A (liveness) · 6 · Confidence **High**.
- **Oracle (R):** `sysdb/postgres.rs:1969-1977` `now_ms_plus_sql(operand)` — `CASE WHEN operand IS NULL THEN NULL ELSE LEAST(GREATEST(now()::numeric + operand::numeric, i64::MIN::numeric), i64::MAX::numeric)::bigint END`. Used at `:6974` (claim), `:7284` (partition flip), `:3397` (init deadline). Comment `:1951-1956`: plain `+` raises `bigint out of range`, the claim rolls back, the row stays ENQUEUED, *"nothing on the queue ever starts."*
- **Port (H):** `SystemDB/Postgres/Statements.hs:2710-2713` (`dequeueClaimStatement`), `:2802-2805` (`partitionSweepFlipStatement`), `:664-666` (`initWorkflowStatement`) use plain `now()*1000 + timeout`. The same file **does** clamp other adds (`:659` init delay, `:668` init debounce deadline, `:2846` debounce bounce, `:307` `set_workflow_delay`) — inconsistent, not a design decision. `Types.hs:2220` `durationToMs` guards only the upper bound.
- **Failure scenario:** a row with `workflow_timeout_ms` near `i64::MAX` is dequeued → `22003 bigint out of range`, claim rolls back, row stays ENQUEUED, every poll re-fails identically → queue never drains.
- **Fix:** add one shared SQL helper (a `Text` fragment builder, or a `[typedSql| |]` expression) mirroring `now_ms_plus_sql` in `numeric` with `LEAST/GREATEST` and use it at all three sites; also route the existing hand-clamped sites through it so there is one spelling. Keep the `dbos` schema pin (ADR-0007). No migration.
- **Tests:** a live case planting a near-max timeout via raw SQL, dequeuing, and asserting the workflow starts with a clamped deadline; unit/SQL text pin comparing the fragment to `postgres.rs:1969`.
- **Related:** F24 (negative durations), F23.

## F14 — `queueResolvedLimits` enforces legacy `partition_queue` semantics Rust deleted in #88

- **Sev/Class/Batch:** Medium · B · 6 · Confidence **High**.
- **Oracle (R):** `sysdb/mod.rs:1362-1363` *"Each limit applies at the scope its column names, and the stored `partition_queue` flag plays **no part**"*; `sysdb/types.rs:2385-2395` `is_partitioned()` = `has_partition_limit(...)`. Rust commit `5921c20` (#88) removed `ResolvedLimits`, `resolved_limits()`, `is_legacy_partitioned()`, the re-scoping, and `update_queue`'s refusal of a legacy row.
- **Port (H):** `SystemDB/Types.hs:1014-1066` — `queueIsLegacyPartitioned` + `queueResolvedLimits` **moves** queue-wide limits into the partition fields and clears the queue-wide ones. Consumed by `Queue.hs:150-162` (`queueFromRecord`), `Dequeue.hs:232,308`. `Queue.hs:361-379` refuses partition-touching updates on a legacy row; `:217` writes `queueUpdatePartitionQueue` explicitly.
- **Failure scenario:** a row with `partition_queue=true` and only `concurrency=5`. Rust: unpartitioned, one fleet-wide claim. Haskell: partitioned with `partition_concurrency=5`, `pollOnce` takes the partition walk, rows with no partition key are **never claimed** — the stranding #88 removed. Operators also cannot add a per-partition limit to such a row, where Rust allows it.
- **Fix:** delete `ResolvedLimits`/`queueResolvedLimits`/`queueIsLegacyPartitioned` and the legacy refusal; make `resolvedIsPartitioned` read `has_partition_limit` from the row's partition columns directly, mirroring `types.rs:2385-2395`. This is a **type deletion** — land the new ADT shape first and let `ghcid.txt` enumerate all call sites. Sweep `\bqueueResolvedLimits\b` word-boundary and count per file. Update `Queue.hs`, `Dequeue.hs`, `Types.hs`, `test/` pins, and `docs/adr/0030` if it describes the old behavior.
- **Related:** F17 (update_queue validator), F11.

## F15 — `shouldRecordStartError` / `recordedStartErrorNames` omit `InvalidInput`

- **Sev/Class/Batch:** Medium · B, Rec · 3 · Confidence **High**.
- **Oracle (R):** `workflow.rs:1926-1933` `should_record_start_error` matches **4** variants (`QueueDeduplicated | ConflictingWorkflow | WorkflowIdInUse | InvalidInput`); `:1942-1947` `RECORDED_START_ERRORS` has the same 4 names; `:1950-1957` `is_recorded_start_error` feeds `Parent::check_child` (`:2003-2051`). Doc `:1910-1916`: `InvalidInput` stays in the list *"so a record an earlier version wrote still reads back as a start error."* Unit test `:2679-2716` pins it.
- **Port (H):** `Transact/Workflow.hs:878-890` — 3 names, `_ -> False`. Used at `:925` (`not (isRecordedStartError value) -> UnexpectedStep`) and `:1281` (whether `recordStartError` writes the refusal). `SystemDB/Error.hs:96` already serializes `{"InvalidInput":…}`, so the shape is available but unrecognized. `test/DBOS/Transact/WorkflowCases.hs:273-284` pins the 3-name list.
- **Failure scenario:** a Rust/older/Python executor records a child-start `InvalidInput` refusal at the parent's step; a Haskell executor recovering that parent returns `UnexpectedStep` (control error, records nothing, re-drives recovery) instead of replaying the recorded refusal. Conversely a Haskell-refused `InvalidInput` start is returned without a step row, so the start is re-attempted on replay.
- **Fix:** add `"InvalidInput"` to `recordedStartErrorNames` and an `InvalidInput {} -> True` arm to `shouldRecordStartError`; keep the oracle's variant order. Update `WorkflowCases.hs:273-284`. Add a mixed-version replay case (plant a recorded `{"InvalidInput":…}` row under a child-start step name, fork/replay, assert the same refusal).
- **Related:** F05, F12, F31.

## F16 — `Select.controlError` wildcard bypasses `controlOf`

- **Sev/Class/Batch:** Medium · I, B · 6 · Confidence **High**.
- **Oracle (R):** `error.rs:773-800` `Error::control()` = `WorkflowCancelled | Interrupted | SystemDatabase(e) if !e.should_record()`; *"A typed refusal … is the opposite case: an answer the call gave, which its step records and every replay gives back."* `select.rs:333-337` uses it.
- **Port (H):** `Transact/Error.hs:306-311` ports it correctly as `controlOf` (`SystemDatabase err | not (shouldRecord err) -> Just …`). But `Transact/Select.hs:208-217` defines its own `isControl` with `SystemDatabase {} -> True` (no guard), used at `:275`, `:294`. `controlOf` is used in `Step.hs:516`, `Handle.hs:228`, `Error.hs:466` — `Select.hs` is the sole bypass.
- **Failure scenario:** a raced step fails with a **recordable** typed refusal (`UnexpectedStep`, `InvalidInput`, `QueueDeduplicated`). Rust records the winning branch, the workflow fails, replay reproduces it. Haskell ends the race with `Left err`, records no select step, workflow stays PENDING and is re-raced on every recovery — a race that never settles.
- **Fix:** delete `Select.hs`'s local `isControl` and call `TransactError.controlOf`. Add a test pinning a recordable refusal settling the race (the first-wins test already exists — extend it).
- **Related:** F31, F36.

## F17 — `update_queue` validation outside the transaction; no-op validator

- **Sev/Class/Batch:** Medium · T (TOCTOU) · 4 · Confidence **High**.
- **Oracle (R):** `queue.rs:615-619` *"The read, the merge, the validation and the write are one transaction … two operators changing different limits at the same instant cannot store a pair neither asked for."* `:798-810` builds a `validate` closure running `validate_fields` on the **merged** row → `SysdbError::InvalidInput`; `:829-832` passes it into `update_queue(name, &update, &validate)`.
- **Port (H):** `Transact/Queue.hs:356` `getQueue` (separate session), `:381-383` `desired` + `validateQueueOptions`, `:387` `SystemDB.updateQueue db queueName update (\_ _ -> Right ())`. The seam `SystemDB/Postgres/Backend.hs:3853-3866` **does** lock `queueByNameForUpdateStatement` and call `validate record (applyQueueUpdate update record)` inside the transaction — but the closure is a no-op. `docs/rust-oracle-seam-audit.md:178` already flags this as an open question.
- **Failure scenario:** operators A and B concurrently set `worker_concurrency=10` and `concurrency=2`; both read the same row, validate their own merge, both write; the stored pair `(2,10)` is incoherent. Rust's second writer re-validates against what A wrote inside the lock and refuses.
- **Fix:** pass a real validator from `Queue.hs` (the same `validateQueueOptions` logic applied to the merged `QueueRecord`) into `SystemDB.updateQueue`; keep the caller-side check as a fast path. Keep the validator's shape one-to-one with the class method. Add a concurrent-update test (two sessions, one must be refused).
- **Related:** F14, F21.

## F18 — `checkHere` compares workflow-id text; `isSameExecution` is dead code

- **Sev/Class/Batch:** Medium · I · 6 · Confidence Low-Medium (code certain; reachability constrained by the phantom `exec` brand).
- **Oracle (R):** `checkpoint.rs:724-730` `if here.is_same_execution(ctx) && here.step_marker().is_none()`; comment: *"By execution identity rather than by workflow id … Matching strings would let the run write through the executor it captured while the workflow around it belongs to the other."* `context.rs:388-390` `is_same_execution` = `Arc::ptr_eq(&self.workflow, &other.workflow)`.
- **Port (H):** `Transact/Checkpoint.hs:165-167` `| workflowId here.stepCtxWorkflow == workflowId wctx …`. `Transact/Context.hs:277-279` defines `isSameExecution` (comparing `wctxState.executionIdentity`), exported at `:52`; whole-tree grep shows **zero call sites in `src/`** (only `test/DBOS/Transact/ContextTest.hs:317,333`). `Connection.hs:134` starts `connExecutionCounter` at 0 **per Connection**, so two connections both mint `ExecutionIdentity 0`; `Eq (WorkflowCtx)` (`:190-192`) compares only that Int, not `connInstanceId`.
- **Failure scenario:** a `Recorded` placement smuggled from execution A into a different execution of the *same workflow id* (recovery re-run, second instance) passes `checkHere`; B writes under A's claimed step id. Rust returns `StepBuiltElsewhere`. The design docs (`docs/scoped-workflow-capabilities.md:189`) assert a runtime backstop exists — it does not; the guarantee rests solely on the type brand.
- **Fix:** (1) wire `isSameExecution` into `checkHere` (and `sameStepBody`, `Checkpoint.hs:196-200`); (2) strengthen `ExecutionIdentity` so it cannot collide across connections — include `connInstanceId` or make the counter instance-wide (compare `Connection.hs:134`, `Context.hs:277-279`); (3) add a runtime refusal case. If the brand makes the path provably unreachable, document that explicitly with a dated note instead of claiming a backstop.
- **Constraint:** ADR-0012/0025/0026 (phantom brand / ambient context) stay in force; this is defence-in-depth, not a replacement.
- **Related:** F08, F05.

## F19 — `close` uses `Pool.release`; no `PoolClosed → Permanent`

- **Sev/Class/Batch:** Medium · A (liveness) · 5 · Confidence **High** (code), Low-Medium (post-release hasql error unverified).
- **Oracle (R):** `sysdb/postgres.rs:5572-5600` — `notifier.stop(); await task; pool.close().await; await listener`. `:221-225` `sqlx::Error::PoolClosed => Permanent` with the comment: *"Calling it a connection error makes any operation issued after shutdown hang instead of returning"*; `mod.rs` docs rely on `close()` to fail every in-flight poll permanently.
- **Port (H):** `SystemDB/Postgres/Backend.hs:1131-1139` `releasePostgresSystemDB` ends with `Pool.release env.psdbPool`. hasql-pool 1.5.1.1 `Hasql/Pool.hs:164-171` documents `release` as *"Release all the idle connections … **The pool remains usable after this action**"* (it is a reset). `classifyUsageError` (`:842-857`) has no closed-pool arm; `BackendErrorKind` (`Error.hs:83-86`) has no such constructor; no `psdbClosed` field (`:1029-1042`). `awaitWorkflowResult` (`:2875-2890`), `getEvent`, `recv` are unbounded poll loops all wrapped in `withRetry`.
- **Failure scenario:** after shutdown, parked waits keep polling a live pool; a post-close sysdb call succeeds or is classified `Connection` and retried forever. Rust ends all waits via `PoolClosed → Permanent`.
- **Fix:** add a `psdbClosed :: StrictTVar IO Bool` (or equivalent) set **before** `Pool.release`; make `runSession`/`runPolling` check it and return a permanent `Backend` error; classify that error `Permanent`. Then `Pool.release` is a correct final teardown. Alternatively use a real pool close if the installed hasql-pool exposes one. Verify the installed driver's API before choosing.
- **Tests:** a live shutdown case asserting a post-close `awaitWorkflowResult` returns promptly; a unit case for the new classification. Update `docs/adr/0010` (polling-only) with the new close semantics.
- **Related:** F34, F20.

## F20 — No local wake on `send` → ≥1 s `recv` latency

- **Sev/Class/Batch:** Medium · A (latency) · 5 · Confidence **High**.
- **Oracle (R):** `sysdb/postgres/notifier.rs:118-129` `signal` wakes the local registry; `listener.rs:209-234` maps migration-1's trigger NOTIFY on `NOTIFICATIONS_CHANNEL` to `registry.wake`. `client.rs:502-555` asserts a 50 s `get_event` is **woken** (`elapsed < 20s`). `SHORT_INTERVAL=1s`/`LONG_INTERVAL=60s` only cap `subscription.notified()`.
- **Port (H):** `Backend.hs:1022` (listener deliberately absent, ADR-0010); `Config.hs:69` `configUseListenNotify` inert. `signal` is called only for `streamsChannel` (`:3369`) and `eventsChannel` (`:3494`), **never `notificationsChannel`**; `SystemDB/Notify.hs:7-9` says `send` "needs nothing from this module" — true only with a listener. `pollForMessage` (`:2613-2629`) waits `min remaining getEventPollInterval` with `getEventPollInterval = secondsDuration 1` (`:1709`); `waitForWakeup` (`:2707-2709`) only times out on `notified subscription` — **no wake source exists for that key**.
- **Failure scenario:** a `send` at t=100 ms into a `recv` with a 2 s timeout: Rust wakes in milliseconds; Haskell sits out the 1 s poll until t≈1 s. Cross-process Haskell readers always pay the full interval.
- **Fix:** call `signal`/`wake` for the message key on the `send`/`deliverTo` path (`Backend.hs:2381-2478`), reusing `SystemDB/Notify.hs`'s existing wake seam; keep the 1 s poll as a backstop. Do **not** reintroduce LISTEN — ADR-0010 stands. Add a local-wake timing test (sim with virtual time) mirroring `client.rs:502-555`.
- **Related:** F19, F37.

## F21 — Split critical sections across several sysdb methods

- **Sev/Class/Batch:** Medium · T · 4 · Confidence **High** (structure), Medium (observable).
- **Oracle (R):** one transaction each for `resume_workflows` (`postgres.rs:4645-4702`: existence SELECT + UPDATE … RETURNING + caller step; comment *"a workflow deleted between the two cannot make this report a resume that did not happen"*), `create_application_version` (`:6351-6412`: claim → INSERT … ON CONFLICT → **post-write** `resolve_owning_application` → commit), `promote_application_version` (`:6478-6537`), `upsert_queue` (`:6554-6655`: INSERT loop → **`SELECT … FOR UPDATE`** → owner resolve → UPDATE), `record_step_on` (`:2605-2613, 2664-2679`: INSERT … RETURNING + `UPDATE … SET executor_id` share the tx opened by `check_owner_on`), `recv` timeout (`:5353-5395` + `:2647-2661` `RETURNING completed_at_epoch_ms` → `StepNondeterminism`), `checkpoint_sleep` (`:2837-2971`: DB clock read **inside** the owner-locked tx).
- **Port (H):**
  - `Backend.hs:3042` existence SELECT and `:3052` resume UPDATE are two `runSession`s with independent `withRetry`; `_caller` ignored (`:3035`).
  - `:3643-3659` `createApplicationVersion` = three `runSession`s; `:3666-3678` `promoteApplicationVersion` = two; `:3679-3703` `upsertQueue` = one session + one tx whose `queueOwnerStatement` (`Statements.hs:2413-2418`) is a plain SELECT **without `FOR UPDATE`**. Post-write owner re-resolve and the delete-and-retry loop are gone.
  - `:3429-3446` `recordStep` commits the step then issues `claimExecutorSession` (`Statements.hs:829-835`) in a **separate** `runSession`; all six composite `Tx` helpers (`setEventTx` `:2131`, `getEventRecordTx` `:2216`, `recvTx` `:1959`, `sendMessagesTx` `:2061`, `writeStream` `:3354`, `recordCallerOutcome` `:1410`) call `recordStepStatement` with **no claim at all** and discard its `Maybe Int64` (`_`/`void`) — GHC `-Wunused-do-bind` warnings at `Statements.hs:1959,2131,2216` confirm.
  - `Statements.hs:1950-1964` `recvTx` always `Tx.condemn >> pure RecvMissed`; `Backend.hs:3276-3300` opens a **second** `runTransaction` to record the absence; the `StepNondeterminism` verdict is dropped.
  - `Backend.hs:1633-1705` `checkpointSleep` reads the DB clock in a standalone `databaseTimeSession` before `checkStep` and `recordStep`.
- **Failure scenarios:** a row deleted between the existence SELECT and the UPDATE (or a crash between the UPDATE and the engine checkpoint) → a resume reported that did not happen / replay re-runs it. Two named apps concurrently registering the same new version: both read "no holder", both claim 0 rows, one INSERT wins, **both return Ok**; Rust returns `RegisteredByAnother` for the loser. A claim session failing permanently after a committed step → `runStep` discards the value while replay reads the recorded output (Rust's documented hazard). A `recv` timeout releases the owner lock between tx1/tx2, so a rival's record is silently accepted instead of `StepNondeterminism`.
- **Fix:** in one batch, (1) compose each listed method into a single `runTransaction`/`runTransactionAt` with the same statement order and guards as Rust; (2) add `FOR UPDATE` to `queueOwnerStatement`; (3) move `claimExecutorSession` into `recordStep`'s transaction and add it to all composite `Tx` helpers; (4) compare `recordStepStatement`'s `RETURNING` value and raise `StepNondeterminism` where Rust does; (5) fold the DB-clock read into the owner-locked sleep transaction. Update the `SystemDB` class, Postgres backend, mem/mock mirrors, and all callers in the same sweep. Watch `Statements.hs` parameter renumbering when moving statements.
- **Constraint:** ADR-0011 already sanctions `hasql-transaction` for multi-statement methods — extend it, record the extension with a dated note. No schema migrations.
- **Tests:** live + sim twins for each; psql read-back for executor id / version ownership / sleep wake; oracle legs `cargo test -p dbos --test sysdb`, `--test clock`, `--test management` read-only.
- **Related:** F09, F01, F13, F33.

## F22 — Rust's `biased select!` ordering lost

- **Sev/Class/Batch:** Medium · A, B · 5 · Confidence **High** (bias), Medium (tie window).
- **Oracle (R):** `step.rs:698-706` `tokio::select! { biased; outcome = attempt => outcome, () = cancelled => …, () = deadline => … }` — a completed attempt keeps its outcome at a tie; `rt/mod.rs:161,176-180` `timeout_at` polls the future before expiry. `workflow.rs:2266-2281` repeats for the deadline race (biased, body first). `instance.rs` shutdown supervisor likewise.
- **Port (H):** `Step.hs:478-489` `race (threadDelay (durationMicros limit)) (wait task)` — **timer is the left (biased) arm** in io-classes (`waitEitherSTM` uses `orElse`), so a body finishing exactly at the deadline is checkpointed as `StepTimeout` (a durable failure). `cancel task` also **waits** for the body (`uninterruptibleCancel`), whereas Rust drops the future immediately. `Workflow.hs:367-376` uses `MonadTimer.timeout` = `System.Timeout.timeout` (unbiased, `timeout 0 = return Nothing`, kill is a swallowable `Timeout` exception). The shutdown supervisor's unbiased `race` can run one extra `transitionDelayedWorkflows` pass after `stopping=True` (`Instance.hs`; `foundations-b` #4).
- **Fix:** order every race so the body/attempt is the preferred branch; use an absolute deadline with a poll-first check rather than `timeout 0`; avoid waiting for the loser's unwinding when the oracle drops the future. Where io-classes cannot express bias, add an explicit "already finished?" STM check before firing the timer arm. Audit `Select.hs`'s races too.
- **Tests:** sim cases for step-timeout tie (body completes exactly at the limit → `OutcomeOutput`), workflow with a past deadline (body still runs and records SUCCESS), shutdown with `stopping=True` (no extra delayed transition). Add trace pins.
- **Related:** F07, F08, F34.

## F23 — `durationIsZero` floors to milliseconds

- **Sev/Class/Batch:** Medium · B · 6 · Confidence **High**.
- **Oracle (R):** `sysdb/types.rs:233-240` `validate_positive_duration` uses `Duration::is_zero()` (exact `secs==0 && nanos==0`); called from `NewWorkflow::validate` (`:873-881`), `DebounceRequest::validate` (`:2262`), `ForkOptions::validate` (`:3015-3017`), `IdleTransactionTimeout::validate` (`:1993`). A positive sub-ms duration is non-zero → accepted, `duration_to_ms` → `Ok(0)`.
- **Port (H):** `SystemDB/Types.hs:2445-2446` `durationIsZero duration = durationAsMillis duration == 0`; `durationAsMillis` (`:2251-2252`) is `floor (secs*1000)`. Used at `Types.hs:426, 1581, 1804`, `Queue.hs:291,297`, `Debouncer.hs:514`, `Config.hs:136`, `Client.hs:168`, `Workflow.hs:564`.
- **Failure scenario:** `durationFromSecs 0.0005` is refused with "must be a positive, non-zero duration" / "cannot be zero" before any write, where Rust stores 0 ms. Affects queue rate-limit period and config polling interval too.
- **Fix:** represent `Duration` so zero is exact (compare the underlying seconds value to 0, not its ms floor); keep `durationAsMillis` for storage only. Add unit cases for 0.0005 s, 0 s, and -0 handling.

## F24 — `FromJSON Duration` accepts negatives; `durationToMs` has no lower bound

- **Sev/Class/Batch:** Medium · I · 6 · Confidence **High** (code), Medium (needs corrupt input).
- **Oracle (R):** `types.rs:251-256` narrows `Duration::as_millis()` (a `u128`, always ≥ 0); serde rejects negative secs/nanos.
- **Port (H):** `SystemDB/Types.hs:2487-2488` `parseJSON = withScientific "Duration" (\ms -> pure (Duration (fromIntegral (floor ms :: Integer) / 1000)))` — accepts any number despite the "Non-negative by construction" doc at `:2096-2100`. `durationToMs` (`:2220-2225`) guards only `ms > maxBound::Int64`; `addTimeout` (`:2373-2378`) checks only the upper bound.
- **Failure scenario:** a checkpoint/JSON `-5` decodes into `workflowRecordTimeout` → `Duration (-0.005)`; `durationToMs` returns `Right (-5)` and a negative `BIGINT` is written. `addTimeout`'s `fromInteger total` can wrap below `minBound::Int64`.
- **Fix:** make the `Duration` constructor opaque behind a smart constructor that refuses negatives (types-before-terms: land the newtype/opaque shape first, let `ghcid.txt` name every construction site); enforce non-negativity in `FromJSON` and in `durationToMs`/`addTimeout`. Do not just add a `max 0` — that silently changes a corrupt value into a valid one.
- **Related:** F13, F23, F25.

## F25 — `DatabaseTime` arithmetic lacks Rust's saturating guards

- **Sev/Class/Batch:** Medium · B · 6 · Confidence **High** (code), Low (reachability).
- **Oracle (R):** `types.rs:1806-1818` `reaches` uses `saturating_sub` + `u64::try_from(..).unwrap_or(0)`; `now` uses `saturating_duration_since`, `i64::try_from(..).unwrap_or(i64::MAX)`, `saturating_add`.
- **Port (H):** `SystemDB/Types.hs:2134-2136` `databaseTimeReaches` = `clock.databaseTimeReadAt + fromIntegral (max 0 (…)) * 1000000` — plain subtraction (`i64::MIN - i64::MAX` wraps to 1) and `Word64` addition. `:2142-2146` `databaseTimeNow` computes `fromIntegral ((now - clock.databaseTimeReadAt) \`div\` 1000000) :: Int64` on `Word64` (wraps if `now < readAt`), then plain `Int64 +`.
- **Failure scenario:** a `DatabaseTime`/`WakeTime` carried into a process/IOSim timeline whose monotonic origin is behind `databaseTimeReadAt` → elapsed wraps to ~1.8e19 ns → ~1.8e16 ms → `epoch_ms + elapsedMs` overflows to a garbage (possibly negative) timestamp, so a deadline wait fires at the wrong time.
- **Fix:** port the saturating helpers (`saturatingSub`, clamp before `fromIntegral`, `min`/`max` on the epoch-ms sum) and add unit cases for extremes and clock skew. Cross-check `test/DBOS/Transact/ClockTest.hs` (skewed pools) still passes.
- **Related:** F13, F23.

## F26 — `timestampFromIso8601` diverges from Rfc3339

- **Sev/Class/Batch:** Medium · B · 6 · Confidence **High** (probed against `time 0.3.55` and the project's `time 1.14`).
- **Oracle (R):** `types.rs:165-172` `OffsetDateTime::parse(text, &Rfc3339)`.
- **Port (H):** `SystemDB/Types.hs:2406-2425` `validClockFields` + `iso8601ParseM @UTCTime` / `@ZonedTime` (`FromJSON Timestamp` at `:2478-2482`, `Backend.hs:2180`). `validClockFields` returns `True` (accept) whenever its fixed-width parse fails, so it does not close the gaps.
- **Probed divergences:** Rust accepts / Haskell rejects: `2026-08-12t00:00:00Z`, `2026-08-12T00:00:00z`, `2026-08-12 00:00:00Z` (the `time` source explicitly notes rusqlite writes space separators). Rust rejects / Haskell accepts and shifts by an hour: `2026-08-12T00:00:00+01`. Matching cases (verified): `+0130`, `24:00:00Z`, `:60`, month 13, bare `Z`, `.000000000000Z`, `+01:30`.
- **Failure scenario:** a `last_fired_at` / JSON instant written by another SDK with a space separator or lowercase `z` is readable by Rust but fails decode in Haskell; an hour-only offset Rust rejects is accepted and shifted.
- **Fix:** add a strict Rfc3339-shape pre-check (uppercase `T`/`Z` or space separator per Rfc3339, offset requires `±HH:MM` or `±HHMM`, not `±HH`) before `iso8601ParseM`; keep the existing range checks. Add a table-driven unit test with all probed cases.
- **Constraint:** this is a *decoder* used on cross-SDK rows — do not change what the port writes without checking replay compatibility.

## F27 — Portable error record shape diverges from Rust

- **Sev/Class/Batch:** Low–Medium · B, I · 6 · Confidence **High**.
- **Oracle (R):** `serialization.rs:800-890` `to_portable` → `PortableError { name, message, code, data }`; `DBOS_ERRORS = "dbos::"` (`:853`); `variant()` (`:870-880`) produces `{"dbos::StepTimeout": …}` with the variant's **untagged, snake_case fields** in `data`. `rebuild()` (`:883-905`) strips the prefix and reconstructs `{"Variant": fields}`. Test pins at `:1348, 1360, 1382, 1466`.
- **Port (H):** `Transact/Error.hs:503-540` `encodeErrorText` writes `{"name": <bare constructor tag>, "message": …, "data": toJSON err}` where `toJSON err` is a **tagged envelope with one key** (`:227-262`, camelCase field keys such as `workflowId`, `recoveryAttempts`). `decodeErrorText` (`:536+`) expects a tagged `data`; Rust's untagged `data` fails `fromJSON` → falls back to `name: message`.
- **Failure scenario:** cross-SDK typed error replay fails both ways: a Rust reader of a Haskell engine error gets `Error::Portable`; a Haskell reader of `dbos::StepTimeout` loses the typed constructor. ADR-0038 documents the port's shape (`{name, message, data}` with the structured envelope in `data`) and claims Python/TS can read the record — true at the human `name`/`message` level, not for typed reconstruction.
- **Fix direction (decision needed):** either (a) align to Rust — `name = "dbos::" <> tag`, `data` = the untagged variant fields with snake_case keys, `rebuild` by reconstructing the tagged shape — and keep a legacy decoder for existing Haskell-written rows; or (b) keep the current shape but record the typed-replay incompatibility as an explicit ADR with a dated note. Do **not** change the wire casually: recorded errors are durable replay data. Check `docs/adr/0023` §4's "Aeson keys keep their exact spelling" claim while here.
- **Related:** F05, F15, F31, F36 (`selectWorkflow` replay).

## F28 — `setEvent` claims the step id before forcing the encode

- **Sev/Class/Batch:** Low · L · 5 · Confidence **High**.
- **Oracle (R):** `event.rs:135-150` `place_set_event(value, …)` calls `encode(value, &encoding, "event value")?` **before** placement/connection capture; the `PendingStep` future is built from the already-encoded value.
- **Port (H):** `Transact/Event.hs:41-59` `pendingSetEvent` calls `placeCall wctx` (claims the id) and `driveSetEvent` then binds `let encoded = encodeWorkflowValue value` — a thunk forced later.
- **Failure scenario:** a value whose `ToJSON` throws (a bottom) spends a step id and the exception surfaces at a later, wrong place; Rust reports the encode failure before any id is taken.
- **Fix:** force the encoding (e.g. `evaluate`/strict `let !encoded`) at placement time, before `placeCall`, and report a codec failure on the engine channel. Add a unit case with a throwing `ToJSON`.
- **Related:** F29, F30.

## F29 — Lazy `attempt` thunk chain in `withRetry`

- **Sev/Class/Batch:** Low · L · 1 · Confidence Medium.
- **Oracle (R):** `retry.rs:115-121` `let mut attempt: u32 = 0; attempt += 1;` — eager.
- **Port (H):** `SystemDB/Retry.hs:136-151` `go backoff attempt` threads `nextAttempt = attempt + 1` lazily; the field is forced only by `renderEvent` (`showText attempt`, `:58`). `fastLoggerTracer` drops sub-floor events with `pure ()` **before** rendering (`Transact/Logger.hs:250`); `nullTracer` (`:280`) discards entirely. `SysdbRetryAttempt` fields are lazy.
- **Failure scenario:** with `TRACE_LEVEL=ERROR` (WARNING filtered) or a null tracer during a long outage, `(((0+1)+1)+…)` grows one thunk per retry and is forced in one O(n) step if a later level-permitting event renders.
- **Fix:** make `attempt` strict (`BangPatterns` or `seq` in `go`), or force it before constructing the event; give `SysdbRetryAttempt`'s numeric field a strict annotation per the deriving-strategy convention. Combine with F02's retry rewrite.

## F30 — Lazy `Int` accumulators in WHNF-only records

- **Sev/Class/Batch:** Low · L · 5 · Confidence Medium-High.
- **Oracle (R):** `workflow.rs:85-107` `arrived`/`departed` do `state.live += 1` / `saturating_sub(1)` eagerly under a `Mutex`; `notify.rs:139-166` `broadcast::Sender::send(())` is O(1) state.
- **Port (H):** `Transact/Workflow.hs:1550-1557` `TaskState { live :: Int }` has no bang; `:1599` `writeTVar … st { live = st.live + 1 }`; `:1714-1717` `live = max 0 (st.live - 1)`. `StrictTVar` forces only WHNF of the `TaskState` constructor (`strict-stm`), so the field stays an unevaluated chain. `SystemDB/Notify.hs:101-104` `Waiter` has no strict fields; `:156-165` `wake`/`wakeAll` use `waiter {waiterGeneration = waiter.waiterGeneration + 1}`; `:172-182` `notified` is the only forcing point.
- **Failure scenario:** a long-running executor with many short workflows accumulates `((((0+1)-1)+1)-1)…`; the first `waitAll`/`abortAll`/`waitAllUntil` read forces the whole chain inside STM — a long pause or stack overflow at shutdown. A hot notify key woken ~10⁵ times during a 60 s backoff builds a 10⁵-deep chain.
- **Fix:** add strict field annotations (`!Int`) to `TaskState.live`, `Waiter.waiterGeneration`, and any other counter in a `StrictTVar`/`Map`; use `modifyTVar'`-style strict updates. Keep explicit `stock`/`newtype` deriving strategies (AGENTS.md).
- **Related:** F08, F29.

## F31 — NULL-output step replay refuses instead of decoding `"null"`

- **Sev/Class/Batch:** Low · B · 3 · Confidence Medium.
- **Oracle (R):** `step.rs:403-411` `decode_tagged(recorded.output.as_deref(), tag, …, "step result")`; `serialization.rs:454-457` `decode(…)` uses `value.unwrap_or("null")`, so a NULL output column decodes as JSON `null` (`Ok(())`, `Ok(None)`).
- **Port (H):** `Transact/Step.hs:248-249` `stepRecordOutput == Nothing -> Left (StepFailed name ("recorded step … has no output"))`, before `decodeWorkflowValue` (which itself defaults an absent payload to `"null"`, `Serialization.hs:230`).
- **Fix:** on `Nothing` output + `Nothing` error, decode `"null"` through the same codec as Rust (i.e. treat absence as `Just (SerializedWorkflowValue "null" …)` or call `decodeWorkflowValue` with a null payload). Keep the `StepFailed` case only when both output and error are absent **and** the row is incomplete by the port's own invariants. Add a cross-SDK replay unit case.
- **Related:** F05, F15, F27.

## F32 — `adoptRecordedFailure` lacks `WorkflowNotFound`; `adoptSuperseded` TOCTOU

- **Sev/Class/Batch:** Low · B · 3 · Confidence Medium.
- **Oracle (R):** `workflow.rs:2497-2518` `Connection::adopt` maps `sysdb::Error::NonExistentWorkflow` → `Error::WorkflowNotFound { workflow_id }`; `:2479-2488` `adopt_superseded` drops the slot then calls `adopt` directly (no pre-read).
- **Port (H):** `Transact/Workflow.hs:235-240` `Left err -> FailureControl (fromSystemDB err)`; whole-tree grep shows **no `WorkflowNotFound` variant in `src/`**. `:281-287` `adoptSuperseded` first does `SystemDB.getWorkflow` and short-circuits on `Cancelled`.
- **Fix:** add the engine error constructor (`WorkflowNotFound { workflowId :: Text }`) with render/JSON/control arms, map `NonExistentWorkflow` through `fromExecution`, and remove the extra `getWorkflow` from `adoptSuperseded` (or move it into the same transaction). Add unit + live cases for a row deleted mid-wait and a cancelled-resumed handoff.
- **Related:** F09, F16, F27.

## F33 — Transactional step: no in-transaction `_still_owns`; raw connection per attempt

- **Sev/Class/Batch:** Low · T, A · 4 · Confidence Medium.
- **Oracle (spec):** ADR-0021 / Python `_datasource.py:843-933` place `_still_owns` **inside** `with session.begin():` so a mid-body hand-off rolls the checkpoint back.
- **Port (H):** `Transact/Datasource.hs:289-329` `attemptTransaction` commits unconditionally; `checkOwner` (`:358-368`) runs **only** in `adoptTransaction` (`:340`), i.e. only on `TxConflict`. `Transact/Datasource/Postgres.hs:243-292` `transactionAttempt` opens each attempt with a raw `Connection.acquire` (`:258-272`), not `Pool.use`, so concurrent steps can exceed `maxConnections` (pool `size` at `:82-91`) → `53300`. `BEGIN … ISOLATION LEVEL` (`:115-124`) and the `ON CONFLICT → TxConflict → adopt` loop are correct.
- **Fix:** run the ownership check inside the transaction before commit (rollback on loss); acquire connections through the pool (`Pool.use`) so the bound holds. Add a sim/live hand-off case.
- **Constraint:** ADR-0021 is the port's own seam (no Rust module counterpart) — keep the facade surface unchanged.

## F34 — `shutdownWith` teardown `forkIO` can leave its box empty

- **Sev/Class/Batch:** Low · A (liveness) · 5 · Confidence Medium.
- **Oracle (R):** `instance.rs:593-600` `crate::rt::spawn(async move { dbos.tear_down(options).await }).await`; `Ok(())` / `Err(join) if join.is_panic() => resume_unwind` / `Err(_) => {}` — a runtime-shutdown-cancelled teardown is swallowed and `shutdown_with` returns. No intermediate box.
- **Port (H):** `Transact/Instance.hs:283-286` `box <- newEmptyMVar; _ <- forkIO (do result <- try (tearDownIfLaunched …); putMVar box result); outcome <- takeMVar box; either throwIO pure outcome`. `try` covers only `tearDownIfLaunched`, **not `putMVar box`**; no `mask`; the thread is untracked (`spawnTracked` exists). `:281-282` claims the box always fills.
- **Fix:** use `spawnTracked`/`withAsync` and read the task result directly (no MVar box), or mask the whole `try … putMVar` region; swallow only shutdown-cancellation, rethrow panics as Rust does. Add a shutdown-during-teardown live case.
- **Related:** F08, F19, F22.

## F35 — `initOrJoin` joins on the caller's dedup key

- **Sev/Class/Batch:** Low · B · 3 · Confidence Medium (not reachable through current public validators).
- **Oracle (R):** `workflow.rs:890-915` matches `QueueDeduplicated { queue_name, deduplication_id, … } if policy == ReturnExisting`, reads the holder with the **error's** queue/key, and maps every other error through `Error::from_execution`; PR #156 also switched `record_start_error` (`:2112`) and `record_child_workflow` (`:2152`) to `from_execution`.
- **Port (H):** `Transact/Workflow.hs:862-872` requires `(ReturnExisting, Just key, QueueDeduplicated {})` and uses `shape.name`/`key` from the caller's `QueueRequest`; `:976` `recordStartError` returns `fromSystemDB recordErr` (which additionally lifts `UnknownSerialization` to a top-level variant Rust keeps as `SystemDatabase(UnknownSerialization)`); `:147` uses `fromSystemDB` on an init error.
- **Fix:** join using the error's queue/key; switch the two record paths to `fromExecution`-equivalent mapping. Add a unit case with a normalized/deduped key.
- **Related:** F04, F12, F27.

## F36 — Lower-severity channel/path gaps (bundle)

- **Sev/Class/Batch:** Low · B, T · 4 · Confidence High–Medium.
- **`recv` timeout split / verdict dropped:** `R: postgres.rs:5353-5395, 2647-2661`; `H: Statements.hs:1950-1964`, `Backend.hs:3276-3300` — see F21. Fix with the transaction batch.
- **`selectWorkflow` replay:** `R: wait.rs:718-724` `revive(error, recorded.serialization, …)`; `H: Wait.hs:70-73` hardcodes `InvalidArgument "select_workflow" "no workflow ids to wait for"` and ignores `stepRecordSerialization`. Fix by decoding the recorded error through `decodeErrorText`; add a mixed-version replay case.
- **`dispatch` order:** `R: dequeue.rs:747-772` checks `qualified_name()`/registry **before** admission (an unregistered row "never parked"); `H: Dequeue.hs:489-523` decides admission/park first → a repeatedly crashing unregistered workflow gets dead-lettered instead of staying PENDING. Fix the order.
- **`register_queue` read-back:** `R: queue.rs:751-760` uses `upserted.after`; `H: Queue.hs:316-319` re-reads via `queueRecord` (`:413-420`) → a peer update between commit and read-back makes the receipt report limits this registration did not write; extra round trip. Fix by returning the transaction's row.
- **Minor channels:** `queue.rs:772-781` filters `INTERNAL_QUEUE` from `listQueues`; `Queue.hs:328-333` / `Backend.hs:3845-3852` do not. `wait.rs:769-787` refuses an empty first-wait with engine `InvalidArgument{operation:"select_workflow"}`; `Wait.hs:156-161` delegates to sysdb `InvalidInput "workflow_ids"` (`Backend.hs:2893`). `select.rs:243-248` `decode_tagged` vs `Select.hs:140-155` converting a decode failure to `UnexpectedStep`. `dequeue.rs:685-693` `by_id.remove(id)` vs `Dequeue.hs:475-476` `Map.fromList` + `Map.lookup` (a duplicate id dispatches twice).
- **`SleepKind` codec:** `R: postgres.rs:1891-1897, 2936-2953, 5788` `SleepKind::Durable(&dyn StepCodec)`; `H: Backend.hs:1620` is a bare ADT and `recordRefusal` (`:1672-1679`) writes portable JSON. This is the ADR-0038 boundary — self-replay is exact; do **not** "fix" it without an ADR (see Deviations).
- **Tests:** add one case per channel in the relevant `*Test`/`*Sim` tree; keep wire literals unchanged.

## F37 — Low-severity structural/log sweep (bundle)

- **Sev/Class/Batch:** Low · B, A · 7 · Confidence High (structure), Low–Medium (impact).
- **`list_workflows` index:** `R: postgres.rs:3895-3905` plain `ORDER BY created_at`; `H: Statements.hs:426-429` wraps the key in `CASE WHEN $31 …` → defeats `workflow_status_created_at_index` for `ORDER BY … LIMIT`. Fix by building two SQL variants (ASC/DESC) instead of a CASE expression; no schema change.
- **Extra `SELECT … FOR NO KEY UPDATE`:** `R: postgres.rs:3995-4007, 4449-4459` single guarded UPDATE on the pool; `H: Backend.hs:2860-2869, 2974-2978, 3092` wrap `checkOwnerTx` (`Statements.hs:1379`) + UPDATE in a transaction → doubled round trips, longer lock. Results stay faithful; measure before changing.
- **`queues.updated_at` host clock:** `R: postgres.rs:6583, 7511` use `now()`; `H: Backend.hs:3688, 3854` use `timestampNow` (host) bound as `$14`/`$13` (`Statements.hs:2448/2479, 2562/2582`). Latent (the column is not selected) but cross-SDK-visible. Fix by stamping `(extract(epoch from now())*1000)::bigint` in SQL.
- **`activatePostgresSystemDB` non-idempotent:** `R: postgres.rs:669-682` guards with `is_none_or(|task| task.is_finished())`; `H: Backend.hs:1120-1125` unconditionally `enable`/`async`/`swapMVar`, leaving two flush loops and an untracked thread; `releasePostgresSystemDB` waits only the stored task. Fix with the guard + cancel/await of a previous task.
- **Launch bracket:** `R: instance.rs:487-495` thaws the registry on a failed `Executor::start`; `H: Instance.hs:235-241` leaves `freezeDataSourceRegistry` and `acquireLoggerBackend` (`Logger.hs:206-211`) outside the `onException` cleanup → a thrown acquisition leaves the registry permanently frozen (`AlreadyLaunched`). Fix the bracket; add a case that throws from acquisition.
- **Log shape:** `R: retry.rs:141,148-164` DEBUG-until-ceiling for conflicts, WARN for connections; `H: Retry.hs:48` always WARNING (F02). `R: instance.rs:480-483` warns on an already-launched `launch`; `H: Instance.hs:228-229` is silent (`EngineEvent` has no constructor). `R: workflow.rs:143-158` aborts only unfinished tasks; `H: Workflow.hs:1633-1639` labels every registered task → `EngineWorkflowsCut` inflated. `R: instance.rs:296-301` logs `workflow_ids`; `H: Instance.hs:345-346` `EngineCancelledRunning` carries only a count. `R: config.rs:184-188` `env::var(...).unwrap_or_default()` treats non-UTF-8 as unset; `H: Config.hs:116-119` `lookupEnv` + `Text.pack` yields surrogate-laden text that passes `validateConfig` (`Identity.hs:77-79`, `Config.hs:51-54` too). `R: types.rs:649,1543,2063,2869,2900` uses `i32` for `priority`/`step_id`/`RateLimit.limit`/`StreamRecord.offset`/`Fork.start_step`; `H: Types.hs:603,1445,729,1529,1676,1930-1945` uses `Int` (64-bit) — a `step_id = 3_000_000_000` reaches an `INT4` bind. Consider `Int32` with a boundary refusal (types-before-terms).
- **Fix order:** bundle these after the correctness batches; each is independent.

---

## Documented deviations — do NOT "fix"

These are intentional and recorded. An implementation agent must not change them while closing
the findings above (they are listed so a fix does not accidentally erase a decision):

- **ADR-0010** polling-only LISTEN/NOTIFY, fixed `dbos` schema, verify-only connect. `listener.rs` is deliberately unported. *F20 is the wakeup/latency consequence of this deviation and **is** a fix.*
- **ADR-0011** `hasql-transaction` instead of composed `BEGIN`/`COMMIT`; **ADR-0004/0007** no Haskell migrations, `ihp-typed-sql` pinned identifiers.
- **ADR-0034** single-value stream read (`readStreamValue`) omitted; **ADR-0036** schedules/cron/scheduler deferred; **ADR-0030** queue-options TS alignment; **ADR-0027/0028** debouncer seam.
- **ADR-0038/0039** portable-JSON-only payloads, non-finite floats, byte-identical `rust_serde` deliberately gone, `{name, message, data}` error record. F27 is the interop *consequence* — it needs an ADR decision, not a silent wire change.
- **ADR-0019** typed error channel; **ADR-0041** per-domain engine-error fragments; **ADR-0023** camelCase fields; **ADR-0024** constructor prefixes; **ADR-0042** dedup-id respelling.
- **ADR-0012** explicit ctx threading; **ADR-0021** datasource seam; **ADR-0035** transactional-step inside-step refusal; **ADR-0033** lifetimes; **ADR-0020** sim mirrors; **ADR-0018** instance identity in refs; **ADR-0025/0026** ambient context closed.
- **#140** selectable runtimes (concurrency rides io-classes), **#148** NUL-byte serializer refusal (N/A by vacuity, pinned by `SerializationTest`), **#153** `08P01` severity split (hasql has no severity), **#155** `Arc` cycle (no instance pinned by refs), **#145** standalone sleep clock reading, **#139** scheduler-loop legs.
- `WorkflowRecord.workflowRecordCreatorXid` is a port-local extra field (the column exists in migration 420, Rust keeps it only on `WorkflowInitResult`) — F05 must account for it, not delete it.

## Coverage gaps in this audit (not "no bugs")

- **`skip 10b`:** `Debouncer.hs`, `Message.hs`, `Stream.hs`, `Management.hs` (debounce/message/stream bodies), and `Client.hs` were **not** audited as a dedicated module slice. Overlapping findings (F04, F09, F15, F27, F36) came from the workflow/dequeue/transaction audits, but those modules' own divergences are unreported. Re-run that slice before calling the port validated.
- Rust integration tests were read as behavioral evidence, not executed. No project source files were edited for this audit.
- Several Low findings (F33–F37) come from a single audit each and were not independently re-verified; they are grouped rather than detailed.
- No hard deadlock was confirmed. The async findings are races, lost wakeups, and ordering/latency inversions.

## Evidence provenance

Full per-slice audit reports (with complete snippets, search logs, and confidence notes) were
written by the audit agents to `/tmp/opencode/audit-*.md` (scratch space, outside the repo —
they may expire). This document embeds the load-bearing evidence; re-read the reports if a
line number has drifted after a sweep.
