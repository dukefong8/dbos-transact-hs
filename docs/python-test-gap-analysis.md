# Python test-suite gap analysis

Date: 2026-10-18. Tree: branch off `720ce04`. Status: **scouting record with
the landed slices logged inline** — R1, the close-sentinel half of R2 (live-only),
L1 (both halves), L2, L3's engine-path and claiming halves, and the listing
id-address case have landed; gap 4 and R4 are verified already covered rather
than added. ADR-0037's two type-level tightenings (the validated rename target,
one stream-close recognizer) landed alongside R2. The suite is 814 leaves; the
management pair is 51 live / 37 sim.

The port treats Python as its behavior oracle (`AGENTS.md`: "Rust is the API and
ADT design oracle; Python is the behavior spec oracle"). This note scouts the
Python test suite through the `dbos-python` skill's own priority table
(`~/dev/dbos-agent-skills/skills/dbos-python/AGENTS.md`) and records where the
Haskell suite covers the same behavior and where it does not. It is the index
the follow-up test slices work from; the open items are the ones owned by the
rewind / duplication-policy / lifecycle slices.

## Method

- **Lens.** The skill's priority order — Lifecycle (CRITICAL), Workflow
  (CRITICAL), Step (HIGH), Queue (HIGH), Communication (MEDIUM), Pattern
  (MEDIUM), Testing (LOW-MEDIUM), Client (MEDIUM), Advanced (LOW).
- **Python corpus.** `~/dev/dbos-transact-py/tests/` — 1,140 test functions
  across ~55 files, 44,674 lines, one backend per run (Postgres; SQLite /
  Cockroach variants behind skips).
- **Haskell corpus.** `test/` — 814 Tasty tests (611 named decls). Every
  behavior runs **twice** over the *same* engine: a live half (`*Test`) and an
  IOSim half (`*TestSim`) sharing `*Cases` (ADR-0020). Counts are therefore
  ~2x the distinct behaviors, and the comparison below is by *behavior*.
- **Scope.** Only surfaces the port implements are counted as gaps. Language-
  runtime features (asyncio, SQLAlchemy engine kwargs, Kafka, CockroachDB,
  SQLite, OTLP, class decorators, FastAPI/Flask lifecycle) are out of scope by
  design and listed at the end.

## Priority-lens gap matrix

| # | Category | Python (tests) | Haskell coverage | Verdict |
|---|---|---|---|---|
| 1 | Lifecycle | `test_config` 67, `test_schema_migration` 32, `test_application_name` 17, singleton/package/cli/enterprise/cockroach ≈ 137 | `ConfigTest` 3, `InstanceTest` 13, `IdentityTest` 8, `RegistryTest` 10, `LoggerTest` 22, `PostgresTest` claim/version/rename cases, the five new ownership cases (L1–L3) | Ownership side now pinned end to end (owner stamps, a fork's inherited owner, a nameless client's row claimed by its runner, read scoping, peer-skipping claims); destroy/reset/migrations deferred (below). Config bulk is Python-specific. |
| 2 | Workflow | `test_dbos` 139, introspection 61, failures 35, rewind 33, duplication 29, management 28, attributes 23, classdecorators 53 ≈ 480 | `WorkflowTest` 13 (+14 sim), `ManagementTest` 51 live (+37 sim), 10 rewind scenarios, `ClientTest` 45 | Broad but shallower: rewind / attributes breadth; duplication policy covered; patching absent. |
| 3 | Step | `test_datasource` 40, retry/timeout ~20 | `StepTest` 11 (+12), `StepRetryTest` 19 (+1), `DatasourceTest` 25 (+2), `CheckpointTest` 13 (+2), `SleepTest` 7 (+1) ≈ 93 | Good. Retry / timeout / preemption / nested / capture all covered. |
| 4 | Queue | `test_queue` 104, `test_concurrency` 21, primitives 26, debouncer 23 ≈ 151 | `QueueTest` 41 (+5), `DebouncerTest` 5 (+5), sysdb `PostgresTest` | Good on mechanics; thin on debouncer breadth and delay. Schedules own slice (deferred). |
| 5 | Communication | `test_streaming` 57, `test_interop` 3, events/messages ≈ 60+ | `StreamCases` ~19, `EventCases` 8, `MessageTest` 19 (+1), `NotifyTest` 14, `NotifierTest` 6 | Stream-heavy gap: notifier/LISTEN-NOTIFY, offsets, cross-batch. Interop = 0. |
| 6 | Pattern | `test_scheduler` 39, `test_debouncer` 23, sleep/delayed/idempotency | sleep ✓, delayed ✓, debouncing ✓ | Schedules = 0 at engine level (deferred). |
| 7 | Testing | `conftest.py` 546 lines, reset-per-test | `DualStack`, `IOSimTracer`, `Mock/MemSystemDB` | Haskell stronger: dual-stack + ownership-by-UUID instead of truncate. |
| 8 | Client | `test_client` 55 + `test_pgsql_client` 14 = 69 | `ClientTest` 45 | Moderate; several Python client knobs are by-design absent. |
| 9 | Advanced | async 55, kafka 48, telemetry 27, otel 5, classdecorators 53, serialization 15, auth 8 | `SerializationTest` 5; rest absent | Mostly out of scope by design. In-scope-ish: patching, interop. |

Haskell counts are leaf counts from the scouting pass and are indicative; the
Management pair was re-verified while landing R1/L1 (51 live / 37 sim, the
step-owner, read-scope and claim-scope cases included).

New management and client cases are **live-only by decision (2026-10-18)**:
those slices skip their sim half, so a new case there gets one leaf in the live
module and none in the sim tree.

## Ranked gaps, with status

1. **Schedules at the engine level — DEFERRED (`docs/adr/0036-schedules-deferred.md`).**
   Python `test_scheduler.py` has 39 tests (cron parse/fire, backfill, pause /
   resume, trigger, version selection, error replay, sysdb downtime). The port's
   sysdb layer is complete and tested (`createSchedule` … `deleteSchedule`,
   `applySchedules` in `IOSimTest`/`PostgresTest`), but nothing exposes it: no
   engine form of Rust's `create_schedule`/`apply_schedules`, no cron loop.
   Rust speaks (v0.5.0 has the surface), so this is an **unimplemented epic**,
   not a test gap. It is recorded here and left to its own slice.
2. **Workflow patching — open.** Python `test_patch.py` (`patch`,
   `patch_async`). No Haskell counterpart; Rust's API shape is the oracle.
3. **Rewind semantics — R1 and R2 landed; R2's replay half and R3 are blocked
   on an oracle disagreement (2026-10-18).** 10 Haskell scenarios vs 33 Python
   tests. **R1 landed** (partition key), **R2 landed live-only** (a cut above
   the close keeps its sentinel), **R4 verified covered**, and **R2b/R3
   blocked**: Python's rewind-stream tests assume a stream write re-executes on
   replay, while Rust's `write_stream` is a `PendingStep` that replays instead
   (`crates/dbos/src/stream.rs:84`), so the port follows Rust.
4. **Duplication policy — COVERED (verified 2026-10-18).** The scouting estimate
   was wrong: `QueueCases.scenarioDedup` pins the default `Reject` collision and
   `scenarioJoin` pins `ReturnExisting` joining plus starting fresh once the
   holder completes; `ClientTest` pins the missing-dedup-id refusal and the
   in-transaction `ReturnExisting` / `ConflictingWorkflow` refusals; and
   `WorkflowCases` pins the child-replay join. No new scenario needed — D1–D5
   below now say where each already lives.
5. **Workflow attributes — open.** ~4 Haskell assertions vs 23 Python tests
   (validation, concurrent merge, list filters, after-rewind). Mapped in the
   management slice.
6. **Lifecycle ownership — LANDED (2026-10-18).** L1 (row and step halves),
   L2, and L3's engine-path and claiming halves are in; details in the backlog
   below. All the new cases are live-only, per the 2026-10-18 decision. The row
   owner, a fork's inherited owner, a nameless client's row claimed by its
   runner, read scoping (unset / named / any, id-address vs prefix) and claims
   that skip a peer (dequeue and recovery) are each pinned at the layer that
   owns the rule.
   Deferred with that slice:
   `destroy` (deliberate contract, `Instance.hs:214-237`), `reset_system_database`
   (test-infra only), rename / custom-schema migrations, and the Python-only
   `dbos rename-application` CLI wrapper (the sysdb-level `rename_application`
   *is* ported and tested — `PostgresTest` "Renames", four cases — and its
   target gained the validated `ApplicationName`, ADR-0037). See TODO.md
   "G6" notes.
7. **Cross-language interop — open.** Python `test_interop.py`; no end-to-end
   envelope / status / error round-trip against another SDK. The boundary is
   enforced structurally (Rust corpus, `TypesTest`, `cross-language-schema-interop.md`).
8. **Observability query timeout — deferred.** Python-only; Rust's listing
   statements carry no timeout either (TODO.md G6).
9. **Streaming breadth — open.** Notifier / LISTEN-NOTIFY (the port polls by
   design), `stream_read` offsets, cross-batch, large payloads.

## Concrete scenario backlog (gaps 3, 4, 6)

Owner slices for the follow-up TDD work. Each scenario is dual-stack (live +
sim) via the shared `*Cases` module unless marked otherwise.

### Gap 3 — rewind (`ManagementCases.hs`)

- **R1 — rewind onto a queue with a partition key — DONE 2026-10-18.**
  `scenarioRewindQueueVersion` now passes `rewindQueuePartitionKey = Just "pk-1"`
  and `checkRewindQueueVersion` asserts the row carries it, on both stacks.
  Mirrors `test_rewind_onto_a_queue_with_a_partition_key`.
- **R2 — the close sentinel's lifetime — DONE (live-only) 2026-10-18.** The
  rewind deletes the stream-close sentinel only when the cut reaches its step
  (`Statements.hs:1284`, `function_id >= $2`; the Mem backend mirrors it at
  `IOSim.hs:1774-1778`). `scenarioRewindKeepStreamClose` /
  `checkRewindKeepStreamClose` write one entry, close the stream, and rewind
  with the cut *above* the close: the sentinel stands with the entry. The
  existing cut-at-or-below cases (`checkRewindUndoCut`,
  `checkRewindKeepBelowCut`) already cover the other half — the sentinel goes
  while the entries stay. Mirrors the third cut in
  `test_rewind_reopens_a_closed_stream`.
  - Follow-up 2026-10-18 (ADR-0037): the sentinel pair has one recognizer now —
    `isStreamClosed value serialization` in `Types`, ported name-for-name from
    Rust's `is_stream_closed` (`sysdb/mod.rs:49`) — read by the pull reader and
    the recorded-output replay in `Stream.hs`, the write step name in
    `Backend.hs`, and the sim mirror's write step name and rewind predicate.
    The SQL delete still binds the two constants as parameters, so the pair is
    now compared in exactly one place. Pinned by a TypesTest case: the pair must
    match, and the sentinel under another label is a producer's value, not a
    close.
- **R2b/R3 — the replay half — BLOCKED on the oracle disagreement.** Python's
  stream tests assert the replayed run *appends* (`test_rewind_keeps_stream_entries`
  reads `["a1","b1","a2","b2"]`) and that a partway reader re-reads the
  reopened entries (`test_rewind_reopens_a_stream_a_reader_is_partway_through`).
  Both hold only if a stream write re-executes on replay; Rust's
  `write_stream` returns a `PendingStep`
  (`crates/dbos/src/stream.rs:84-102`) and the port checkpoints it the same
  way, so a replay reads back the recorded write and appends nothing. Rust wins
  by default (`AGENTS.md`), so no test is written against Python's shape; if
  the port ever moves off the checkpoint, these two are the tests to add.
- **R4 — rewind input-validation breadth — COVERED (verified 2026-10-18).**
  `scenarioRewindRefusals` covers a missing id, a negative cut, an empty queue
  name, and an active workflow. Python's `test_rewind_input_validation` adds
  only `start_step = 0` refused at the *sysdb* entry (`rewind_workflow(wf, 0)`
  → "must be >= 1"), which the port spells `InvalidInput` at that same layer
  (`sysRewindValidate`); both engines read a cut of 0 as "from the beginning",
  so the two do not disagree. No new scenario.

### Gap 4 — duplication policy (`QueueCases.hs` / `WorkflowCases.hs` / `ClientTest.hs`)

**Covered — verified 2026-10-18; no new scenario needed.**

- **D1 — `ReturnExisting` returns the holder's handle.** `QueueCases.scenarioJoin`
  (the second handle resolves to the holder's row and both read the one run).
- **D2 — `ReturnExisting` rejects without a dedup id.** `ClientTest` (`noKey`
  refusal; `Workflow.hs:446-449`).
- **D3 — `ReturnExisting` starts fresh after the holder completes.**
  `QueueCases.scenarioJoin` (`third == thirdId`).
- **D4 — default `Reject` refuses a duplicate dedup key.** `QueueCases.scenarioDedup`
  (`checkDedup`); the explicit-id case is `ClientTest`'s `ConflictingWorkflow`
  refusal.
- **D5 — `ReturnExisting` in a child workflow replay.** `WorkflowCases:1994`; the
  in-transaction refusal is `ClientTest` "return-existing is refused before
  anything runs".

### Gap 6 — lifecycle ownership (`InstanceTest.hs` / `ManagementCases.hs`)

**Landed 2026-10-18: L1 (row and step halves), L2, and L3's engine-path and
claiming halves.** Every new case is live-only: each needs a second
application's row, and the sim fixture's store keys rows by id with no owner
column, so a `*Sim` twin would have to fake one (the 2026-10-18 live-only
decision).

- **L1 — forks inherit the owning application — DONE 2026-10-18 (row + step
  halves).** `ManagementCases.scenarioForkInheritsOwner` asserts both the source
  row and the fork's row carry `mfIdentity.identityAppName`, on both stacks. The
  step-row half is `ManagementTest` "a workflow's step rows carry the owner and a
  fork's copies carry it too", live-only: it runs a workflow with one step, forks
  from above it (`ForkStep 1`), and reads `operation_outputs.application_name`
  raw (`stepOwners`, a Hasql statement in the test). No ported step type carries
  the column — Rust's `STEP_COLUMNS` omits it too
  (`crates/dbos/src/sysdb/postgres.rs:2464`) — yet the value matters because the
  column is part of the cross-language schema another SDK's `step_owners` reads.
  Mirrors `test_steps_carry_owner_and_forks_inherit_it`.
- **L2 — a nameless client writes unclaimed rows; the runner claims them — DONE
  2026-10-18 (live-only).** `ClientTest` "a nameless client writes unclaimed rows,
  which the app claims when it runs them": a client whose config carries no
  application name enqueues onto an unpolled queue (the row stays unclaimed) and
  onto a queue the app polls, where the driven dequeue runs it and stamps the
  owner. The stamp is `application_name = coalesce(application_name, $1)` in the
  dequeue claim (`Statements.hs:2400-2403`). Mirrors
  `test_client_without_identity_writes_unclaimed_rows`.
- **L3 — reads scope to the application (+ unclaimed); claims skip a peer — DONE
  2026-10-18 (sysdb + engine + claiming).** `PostgresTest` "scopes applications:
  unset, named and any" (`:991-1011`) pins the unset / named / any trio, and "an
  id is an address while a prefix stays a search" pins the two rules that case
  could not isolate: an id-keyed read reaches a peer's row while a prefix stays a
  search, and naming an application still narrows an id-keyed read. The engine
  path is `ManagementTest` "an identity read reaches a peer's row while an unset
  list stays scoped" (two launched applications; the peer's row is reached by id
  and carries its owner, is excluded from the unset list, and is narrowed out by
  `Named`), and the claiming half is "a claim skips another application's queued
  rows" (the peer's rows on a shared registered queue name and on the internal
  queue are never among the ids our driven dequeue claims, while our own work on
  both runs). Recovery's application scope is the extended `PostgresTest` "a
  recovery sweep enqueues only the dead executor's own rows": a peer's row at the
  same executor and version is left `Pending`, while the owned and unclaimed rows
  are recovered. Mirrors `test_reads_surface_owner_and_ids_stay_global` and
  `test_claiming_skips_another_applications_workflows`.

Drive-by 2026-10-18: `scenarioAttributes` and `scenarioAttributesInWorkflow`
both searched the literal tier `"gold"`, and the live management fixture's
backend carries no application name (the name rides the `Connection`, not the
`SomeSystemDB`), so an attribute search over the shared database is unscoped and
the two cases could see each other's rows under a parallel eval. Both build the
tier from the fixture's suffix now. No behavior changed.

## Out of scope by design

async / asyncio, Kafka, CockroachDB, SQLite, OTel / telemetry, enterprise/auth,
class-decorator and instance-method workflows, SQLAlchemy engine kwargs,
FastAPI/Flask lifecycle, and LISTEN/NOTIFY (the port polls; `configUseListenNotify`
is deliberately inert, `Config.hs:70-71`).

## References

- `docs/adr/0036-schedules-deferred.md` — the deferred epic.
- `docs/adr/0037-validated-application-name-and-stream-close-recognizer.md` —
  the rename target's validated name and the single close recognizer (2026-10-18).
- `docs/cross-language-schema-interop.md` — the shared `dbos` schema contract.
- `TODO.md` — "Constraint-audit actions" G6 notes (destroy / reset / rename /
  migrations deferrals).
- `~/dev/dbos-transact-py/tests/` — behavior oracle.
- `~/dev/dbos-agent-skills/skills/dbos-python/AGENTS.md` — the priority lens.
