# Oracle validation fix TODO

Tracking the 2026-11-10 oracle validation findings plus the two review findings
added after the audit. Status values: `open`, `doing`, `green` (watcher and
targeted tests), `full` (full suite and psql read-back), `deferred`.

The source list is `docs/oracle-validation-findings-2026-11-10.md`. The review
corrections to that document (F33 connection bound, F26 parser grammar, F09 id
placement, F08 mask shape, F10 liveness source, F18 ordering, F27 decision,
F05 key count, batch metadata) are tracked as `D01`-`D09` and land before the
code units that depend on them.

## Gates

- Every edit: `make ghcid-wait` (sims only, watcher already running).
- Every code unit: targeted live group through the standalone binary, then
  `cabal test` when the unit touches live paths.
- Every database unit: psql read-back against `$DBOS_SYSTEM_DATABASE_URL`.
- Oracle legs read-only from `~/dev/dbos-transact-rust` where the unit ports
  behavior.
- No commits until `/review`.

## Units and status

### U0. Documentation corrections (D01-D09)

| ID | Finding | Status |
|---|---|---|
| D01 | F33 fix says `Pool.use`; replace with a connection permit and say "before commit", not "inside the transaction" | full |
| D02 | F33 fix must compare `owner_xid`, not `executor_id` | full |
| D03 | F26 fix must normalize before parsing; add lowercase/space/separator/leap-second cases | full |
| D04 | F09 fix belongs in the sysdb method before the retried transaction, not the Management entry point | full |
| D05 | F08 fix must mask the bracket and run the body under `restore`; note the constraint ripple | full |
| D06 | F10 fix needs a real liveness source (`Tasks` registry or death channel) | full |
| D07 | F18 fix must strengthen `ExecutionIdentity` before wiring `isSameExecution` | full |
| D08 | F27 needs a decision; default to keeping the wire and amending ADR-0038 | full |
| D09 | F05 key count: `WorkflowRecord` two, `StepRecord` three; batch metadata contradictions (F21, F25, F34, F35, F38) | full |
| D10 | F16's recordable example: `UnexpectedStep` is control (`shouldRecord = False`); use `InvalidInput`/`QueueDeduplicated`/`ConflictingWorkflow` | full |

### U1. Small guards and decoders

| ID | Finding | Status |
|---|---|---|
| F01 | Recovery re-enqueue clears `owner_xid` | full |
| F04 | Allow `deduplication_id` plus `partition_key` | full |
| F06 | `validateNewWorkflow` includes `config_name` | full |
| F12 | `runWorkflowRef` calls `validateStart` | full |
| F15 | `InvalidInput` is a recorded start error | full |
| F16 | `Select.controlError` uses `TransactError.controlOf` | full |
| F31 | NULL-output step replays as JSON `null` | full |
| F05 | `FromJSON` defaults every non-key field | full |
| F32 | `WorkflowNotFound` and `adoptSuperseded` without the pre-read | full |

### U2. Retry, SQLSTATE, contention, laziness

| ID | Finding | Status |
|---|---|---|
| F02 | Two retry schedules and `retry_conflicts` | full |
| F03 | Class 40 exact codes only | full |
| F11 | `Contention` ADT and correct backoff direction | full |
| F29 | Strict retry attempt | full |
| F37a | Conflict DEBUG / connection WARN log split | full |

### U3. Types, clock, codecs

| ID | Finding | Status |
|---|---|---|
| F23 | Exact zero for `Duration` | full |
| F24 | Non-negative `Duration` at JSON and storage bounds | full |
| F25 | Saturating `DatabaseTime` arithmetic | full |
| F26 | Rfc3339 parity (normalize, offsets, leap seconds) | full |
| F13 | Saturating deadline SQL at the three sites | full |
| F27 | Portable error record decision and ADR note | full |
| F28 | `setEvent` encodes before claiming the id | full |
| F30 | Strict counters in `TaskState` and `Waiter` | full |

### U4. Execution identity and datasource

| ID | Finding | Status |
|---|---|---|
| F18 | `isSameExecution` wired after identity strengthening | full |
| N1 | `checkOwner` compares `owner_xid` to `ownerToken wctx` | full |
| N2 | Adopt path checks the recorded transaction step name | full |
| F33 | Transactional step ownership check before commit and a connection permit | full |

### U5. Queue semantics

| ID | Finding | Status |
|---|---|---|
| F14 | Delete `ResolvedLimits` and legacy re-scoping | full |
| F17 | Real `updateQueue` validator inside the lock | full |
| F35 | Join on the error's dedup key | full |
| F36a | Internal queue filtered from `listQueues` | full |

### U6. Concurrency

| ID | Finding | Status |
|---|---|---|
| F07 | Preemptible cancellation observer per attempt | full |
| F08 | Masked `withStep` bracket | full |
| F10 | Worker registry has no lost update | full |
| F19 | Closed pool is permanent | full |
| F20 | Local wake on send | full |
| F22 | Body-first race bias | full |
| F34 | `shutdownWith` cannot leave the box empty | full |
| F36b | `dispatch` checks the registry before admission | full |
| F36c | `selectWorkflow` replays the recorded error | full |

### U7. Management and transactions

| ID | Finding | Status |
|---|---|---|
| F09 | Management work and caller step in one transaction; stable fork ids | full |
| F21 | Single transactions for resume, versions, upsert queue, executor claim, recv timeout, sleep clock | full |

### U8. Low sweep

| ID | Finding | Status |
|---|---|---|
| F36d | `register_queue` returns the written row | full |
| F36e | Empty first-wait refusal channel | full |
| F37b | `list_workflows` index-friendly ORDER BY | full |
| F37c | Queue `updated_at` from the database clock | full |
| F37d | Activation idempotence and launch bracket | full |
| F37e | Log-shape gaps (`launch`, cut counts) | full |
| F37f | `Int32` for priority, step id, rate limit, offset, fork start | full |

## Progress

2026-11-10: U1, U2, U4 landed. `cabal test` green (1076 tests, 62.9 s) with
`DBOS_SYSTEM_DATABASE_URL` and `APP_DATABASE_URL` on the local Postgres.
psql read-back for the run window: 665 `workflow_status` rows (306 SUCCESS,
171 PENDING, 132 ENQUEUED, 28 ERROR, 23 CANCELLED, 4 DELAYED, 1 parked) and
49 notifications. The units marked `full` are covered by the suite; F01, F04,
F06, F12, F16, F31, F05 and F18 also have the sim/live pins named in the
findings.

A second full run passed (1076 tests, 62.5 s). A later run of the U3/U5
units reached 1085 tests in 61.2 s. F13's typedSql initially omitted the
null-operand guard, which set every no-timeout workflow's deadline to the
i64 floor and hung the live suite; the guard is in place now. The first run failed one
timing-sensitive live case; the rerun was green, matching the two known flakes
in AGENTS.md. `make ghcid-wait` now also fails on tasty's
`N out of M tests failed` line (Makefile).

Independent review (`opencode-go/muse-spark-1.3-contributor`) found no
correctness bug in the fixed paths. The three reviewed risks were resolved
on 2026-11-11:
- `ensureOwned` distinguishes an unreadable owner fence from a lost race:
  the attempt rolls back and returns the system-database error, so a
  transient blip cannot surface as a spurious `missingWinner`.
- The app datasource's pool and raw-transaction path are each sized at
  `maxConnections`. They cannot share one budget: a transaction body may
  start or await a nested execution that transacts too, and a half-width
  raw path deadlocks the parent on the child (the widget and outbox
  fixtures at `maxConnections = 2` reproduce it). The deployment's server
  budget must cover `2 * max_connections` plus nesting headroom; the
  acquisition comment records this.
- A NULL `owner_xid` on a PENDING/ENQUEUED row refuses. Preflight audit
  before rollout (rows written by older builds or another SDK):

  ```sql
  select workflow_uuid, status, application_version
  from dbos.workflow_status
  where owner_xid is null and status in ('PENDING','ENQUEUED')
  order by created_at desc
  limit 100;
  ```

  A non-empty result means the redeploy would refuse those executions' next
  step write; cancel them or let them finish first.

Open: none of the tracked findings. The residual risks and structural cleanups are listed below. F09 is full: fork,
fork-from, cancel, resume, rewind, and delete all take a `StepCaller`
(execution, step, owner token); the owner check, the step check, the work,
and the caller's record run in one transaction; fork ids are minted before
the retried region; a recordable refusal is checkpointed and replayed;
rewind's post-rollback failure record is fenced too. The mem backend
compares the token. Schedules remain on the unfenced caller
path because that surface is deferred (ADR-0036), not because it was
skipped. Debounce is live, and on 2026-11-11 it took the Rust 0.6.0
`StepCaller` fence (ADR-0028 amendment), which corrects the earlier note
that attributed its unfenced caller to ADR-0036. F09's `run_transactional_step` (a system-database transaction whose
caller check, work, and step record commit together) is not the
application-facing `runTxStep`/`runTxOutside` of ADR-0021, which run app code
against the application datasource; F09 does not cover `runTxStep`. `StepRunning`/
`StepReplaying`/`StepOutputRecorded` are no longer emitted for these calls
(Rust's `run_transactional_step` emits none either); the management logs
fire on replay too, as Rust's do. F21's
transaction batch landed: resume, application-version create/promote, and
upsert_queue are one transaction each (insert-first with `FOR UPDATE` and a
post-write owner resolve), every step record now carries its executor claim
in the same commit, `recv` records its deadline absence in the take's
transaction with the stored-completion comparison that yields
`StepNondeterminism`, and `checkpoint_sleep` reads the database clock from
the owner check's locked row and checks, decides, records, and adopts in
that same transaction. F19's closed-handle boundary is in: a per-handle
`psdbClosed` flag makes every later session a permanent refusal, and
`releaseConnection` closes only an owned backend, so the shared suite handle
and peers' pools stay open. F37e's cancelled-running id list,
already-launched warning, and cut-count refinement are in, as is the F37f
`Int32` port. The cut count subtracts tasks whose actions returned before the
sweep looked (recorded by the departure), so a finished task is never labelled
cancelled; the sim and live task trees pin it. F37f ports `priority` (`NewWorkflow`, `WorkflowRecord`,
`storedPriority`), `RateLimit.limit`, `StepRecord.stepId`, `StreamRecord`
offset and step id, `Fork.startStep`/`ForkPoint.Step`, `RewindOptions.startStep`,
and the sysdb fork/rewind/stream/queue statement parameters to `Int32`. Queue
concurrency stays `Int` on the public options but is refused past `maxBound ::
Int32` before the INT4 bind, so no INT4 column can truncate a user value
silently. The document
corrections D01-D10 are recorded in the findings addendum. F22's step-timeout, workflow-deadline, and
select races are body-first: the body/attempt/earlier branch is the left arm
of every `race`. A step timer arm polls its already-forked task once after
it fires, so a body that finished at the deadline keeps its outcome instead
of being recorded as a timeout; the workflow deadline keeps the body in the
caller's thread (no extra task), so a shutdown kill takes the body with the
run. The shutdown supervisor's `sleepUnlessStopping` checks the flag before
sleeping and puts the stop ahead of the timer, and `Select`'s nested race
puts the earlier branch on the left — both were audited against the oracle's
biased `select!` and need no change. F07's preemptible observer now waits on
`awaitWorkflowResult` at the connection's outcome interval, as the oracle's
`observe_cancellation` does: a cancellation fires it, while a terminal
outcome that is not a cancellation, or a refusal the retry policy will not
retry, parks it with a warning and lets the attempt finish on its own terms.
The wait reads the four settled-status columns rather than the whole row, so
the poll is the oracle's weight; a step with no timeout now has a sim/live
pin that only the observer can end. F10 now uses the `Tasks` registry as the worker registry; the change
shifted two timing-sensitive trace pins (a debouncer generated id and an
interop row status), both relaxed to the behavior under test. F08 is fixed
with an idempotent per-scope restore rather than a `MonadMask` cascade: the
exception handler covers the depth increment, and a `scopeRestored` flag
stops a post-restore kill from decrementing twice. A kill test in
`ContextTestSim` pins the depth return; one management trace moved with the
new STM scheduling. F14's behavior is fixed (the
legacy flag no longer re-scopes and the update refusal is gone), and
`ResolvedLimits` is deleted: every caller reads the record's fields at the
scope its column names. F24's boundary guards are in, and the `Duration`
constructor is now opaque — the type is built only by its validating smart
constructors, with `durationSeconds`/`durationMicros`/`scaleDuration` as the
arithmetic seams, so a negative span cannot be built at all. F19 was first attempted with a backend-wide flag, which broke live tests
reusing the shared suite handle; it was then completed with a per-handle
`psdbClosed` plus `Connection.connOwnsBackend`, so only an acquiring
connection closes its backend. D10 records that F16's `UnexpectedStep`
example is wrong: `shouldRecord` says it is control, so the recordable
example must be `InvalidInput`/`QueueDeduplicated`/`ConflictingWorkflow`.

2026-11-11: the residual sweep closed F14 (`ResolvedLimits` deleted),
F24 (`Duration` opaque), F22 (body-first races with a post-timer poll on
the step paths; supervisor and `Select` audited), `ensureOwned`'s
transient-read distinction, and the app-datasource connection-budget note.
`cabal test` green at 1098 tests (62.8 s); `cabal build all` green with no
redundant-constraint warnings; psql read-back 668 `workflow_status` rows
(308 SUCCESS, 173 PENDING, 130 ENQUEUED, 28 ERROR, 24 CANCELLED, 4 DELAYED,
1 MAX_RECOVERY_ATTEMPTS_EXCEEDED) and 49 notifications. The same sweep also
aligned `migrationCeiling` with the vendored corpus (123 → 125; the test pin
and `make db-migrate` were already at 125) and amended ADR-0010. F07's last
residual closed after that run: the preemptible observer now waits on
`awaitWorkflowResult`, and its new no-timeout sim/live pin took the suite to
1095. The Rust 0.6.0 decision then fenced debounce: `debounceDelayedWorkflow`
takes `Maybe StepCaller`, the bounce checks the owner before the step read,
and the live and mem stale-caller refusal pins took the suite to 1097.
Schedules keep the unfenced helper because that surface is still deferred.
A review of the same tree then caught the preemptible observer's park: it
used `atomically retry`, which GHC's RTS kills with
`BlockedIndefinitelyOnSTM` in IO, and a timer-based park replaced it. The
observer leak on the body-wins branch is fixed too: both helpers now cancel
the observer, since `race` only cancels the losing wait. The
`checkpointSleep` clock pairing is fixed as well: the clock is read in an
outer select over the locking subquery (as the oracle does), and the
monotonic reading is taken after the transaction returns rather than before
it starts, so a durable sleep or `recv` deadline no longer wakes early under
contention or retry. Finding #4 corrected the cross-language ceiling table
(Python 132, TypeScript 130). Finding #5 closed the last unchecked
`Double`-to-`Duration` path: `scaleDuration` now returns `Maybe Duration`
through `durationFromSecs`, and the jitter and backoff sites keep their old
result as the total fallback.

The connection-budget change first shared one `maxConnections` between the
session pool and the raw-transaction path. That deadlocked the widget and
outbox fixtures: at `maxConnections = 2` the raw path got one permit, and a
transaction body that starts or awaits a nested transaction held the only
permit while the child waited for one. The split was reverted (each path is
sized at `maxConnections`), and the deployment note records the `2 *` server
budget. The first `withAsync`-wrapped workflow deadline race was reverted
for the same class of reason: a killed caller must take its body with it,
and the plain `race` over the body in the caller's thread does that.

## Decision log

`.agent/oracle-validation-fixes.tsv` (local, not committed).
