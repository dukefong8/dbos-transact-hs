# Agent-skill API review: TS Incorrect/Correct vs our tree

Source: `~/dev/dbos-agent-skills/skills/dbos-typescript/` (`SKILL.md` +
`references/`, v2.0.0, Sept 2026). Read 2026-10-03: all CRITICAL (lifecycle,
workflow) and HIGH (step, queue) references, plus `pattern-idempotency.md`
(MEDIUM, requested). Each rule below is the skill's Incorrect vs its Correct,
then the tree's verdict: **type-impossible** (misuse is a compile error),
**refused** (runtime `Either` before any write/counter move),
**degraded** (runs plain, nothing recorded, counter unmoved), or **open**
(misuse compiles and runs — a hole or an accepted limitation).

Slices 1–3 (depth backstop, child-start refusal, `WorkflowCtx`/`StepCtx`)
are the reason most workflow-op rows already read "refused/degraded".

## 1. Lifecycle (CRITICAL)

| TS rule (Incorrect → Correct) | Tree verdict | Evidence |
|---|---|---|
| Run without configure+launch → `setConfig` + `launch()` first | refused (`ErrorNotLaunched`) | `Instance.hs:463-468`, used by run/start/enqueue/register-queue paths |
| Register after launch → register before `launch()` | refused (`ErrorAlreadyLaunched`) | `Registry.hs:187-188`; freeze at `snapshotRegistry :194-198` |
| Register queues / create schedules before launch → after | enforced by construction | `registerQueue` requires executor: `Queue.hs:259` (queues genuinely post-launch, unlike workflows) |
| Double launch → (idempotent in TS) | idempotent (`Right ()`) | `Instance.hs:177-178`; failed start thaws: `:188-190` |

No delta: runtime `Either` at every boundary is the right weight here. A
launch typestate (`LaunchedDBOS`) was considered and rejected — it would
churn every test seam (`launchOn`, direct `ctxOver` fixtures) for no new
safety, since every boundary already fails fast with a matchable constructor.

## 2. Workflow ops from steps (CRITICAL, `workflow-constraints.md`)

TS Incorrect: `startWorkflow` / `recv` inside a step. Correct: from the
workflow body only.

| Op | In-step (handed ctx) | Captured parent (depth>0) |
|---|---|---|
| `startChildWorkflow` | refused (`InsideStep`) — slice 2 | refused — slice 2 |
| `setEvent` | refused (`InsideStep`) | refused (via `takenPlacement` depth check — slice 1) |
| `recv` | refused (`InsideStep`) | refused (`InsideStep "recv"`, pinned `scenarioCapturedRecv`) — slice 4 |
| `send` / `sendWith` | plain (no id) | plain, no id (caller dropped via `insideAStep`, pinned `scenarioCapturedSend`) — slice 4 |
| `sendBulk` | plain (step-wrap degrades) | plain (transitively via `placeCall` — slice 1). No change |
| `getEvent` (eager) | plain (no ids) | plain, no ids (caller dropped via `insideAStep`, pinned `scenarioCapturedRead`) — slice 5 |
| `getEvent` (pending) | plain | plain (via `takenPlacement` — slice 1). No change |
| `awaitChild` | plain (unrecorded settle) | plain. No change (read-only wait, no ids) |
| `sleep` | plain | plain (via `placeCall` — slice 1). No change |
| `selectStep` | fresh/non-recorded | same (via `placeCall` — slice 1) + `<2` arms `ErrorConfig`. No change |
| `runTxStep` | refused (`InsideStep`) | refused (`InsideStep "transaction"`, pinned `scenarioCaptureRefused`) — slice 5 |
| `runStep` (nested) | plain (leaf rule) | plain (via `placeCall` — slice 1). Matches TS "step-from-step nests" |
| `enqueueWorkflow` / `startWorkflowRef` on captured `DBOS`/`Executor`/`Client` | n/a (no `Ctx`) — the ctx form is a type error here; the instance forms stay callable on captured values: outside-only by discipline (**open, accepted** — below) | ctx form refused (`InsideStep`, pinned); instance forms unguarded (same accepted limitation) |

The ctx-routed half is closed: the only start/enqueue entry a body reaches
without a captured value is `startChildWorkflow :: WorkflowCtx exec m -> …`
— including the in-workflow enqueue via `startQueue` (`Workflow.hs:655-745`),
which records the parent step under the child's bare name plus
`child_workflow_id` and replays (`scenarioEnqueuedChildReplays`, 2026-10-07).
From a step body the ctx form is a type error; through a captured parent it
is an `InsideStep` refusal — both pinned (`scenarioChildInsideStepRefused`,
`scenarioCaptureChildRefused`, live+sim). What remains is the
captured-instance escape: `enqueueWorkflow :: DBOS m` / `startWorkflowRef ::
Executor m` stay for handlers, clients, and tests, and a step body can close
over those values. Depth lives in `WorkflowState`, reachable only through a
ctx, so no instance-surface signature can see it — the recorded rewire
("signatures take `WorkflowCtx`") is already satisfied where it can be and
impossible where it cannot. Verdict: **open, accepted limitation** (the
explicitness price); the only true closure is ambient execution state
(ADR-0026 revival) or nothing. Decision 2026-10-07 (R1): accept and record;
ADR-0026 closed, not revived. (`currentConnection`, named in the 2026-10-03
(ADR-0026 revival) or nothing. (`currentConnection`, named in the 2026-10-03
draft, no longer exists in `src`.)

## 3. Determinism / globals (CRITICAL)

TS Incorrect: `Math.random()` / `Date.now()` / fetch / file reads / raw
queries in the workflow body. Correct: inside steps (checkpointed replay
uses the saved value).

Tree verdict: **accepted limitation — unenforceable statically.** Bodies
are `(argument -> Ctx m -> m …)` with capability classes; when `m ~ IO`,
`getCurrentTime` / `randomIO` / `IORef` writes all typecheck. No Haskell
effect system short of an `IO`-free workflow monad can forbid ambient
`IO`, and the engine itself needs it — the Rust oracle is in the same
position. Mitigations, not enforcement: blessed helpers scoped to
`WorkflowCtx` where they exist (`generatedWorkflowId`,
`timestampNow` under IOSim virtual time), registration-time `FromJSON` /
`ToJSON` constraints (serializable boundary in both directions), and this
documentation. No delta.

## 4. Steps (HIGH, `step-basics.md` + `step-retries.md` + `step-timeouts.md`)

| TS rule (Incorrect → Correct) | Tree verdict |
|---|---|
| External call in workflow → in a step | doc-level (see §3); the step API itself is the blessed path |
| Manual retry loop → `retriesAllowed`/`maxAttempts`/backoff | enforced by shape: `max_attempts` (default 1 = no retry, matching TS default `false`), `interval`/`backoff_rate`/`max_interval`; `stepBackoff` (`Step.hs:293-301`). Manual retry inside a body is correctness-harmless (one checkpoint for the whole body) |
| Retry 4xx forever → `shouldRetry` predicate | `should_retry :: Maybe (ShouldRetry e)`, pure (`Error e -> Bool`) — **stronger than TS**: a pure predicate cannot throw, so the TS footgun (predicate error becomes step failure) is type-impossible |
| `shouldRetry` ignored when retries off | by construction (no retry → predicate never consulted) |
| Exhaustion → `DBOSMaxStepRetriesError` | `MaxStepRetriesExceeded` (`Step.hs:488-498`); control errors end uncheckpointed, app failures recorded |
| Hung call, no timeout → `timeoutMS` per attempt | per-attempt timeout fires the token then kills (`Step.hs:457-472`); result of an overrunning step discarded — cooperative, as in TS |
| Signal ignored → honor `timeoutSignal`/`cancelSignal` | **gap (ergonomics, not correctness)**: the only signal is a polled `StrictTVar Bool` via `cancellationToken` (never-firing outside a step — same shape as TS's always-present `cancelSignal`). Bodies must hand-poll. Fix: `raceCancel` combinator (slice 6) so honoring cancellation is one call, not a polling loop |
| Inputs/outputs JSON → (required) | **type-impossible otherwise**: `FromJSON`/`ToJSON` on every typed entry point (`Step.hs:172,312-329,356-357`; `Registry.hs:122,138`). Mismatches are runtime `CodecTypeMismatch`, structurally matched — the TS `instanceof`-replay footgun (`pattern-idempotency.md:60`) cannot exist here since errors are ADTs, never classes |
| `startWorkflow` on a step throws → wrap in a workflow | **type-impossible, stronger than TS**: no step refs exist — only `WorkflowRef` (`Registry.hs:99-102`); passing "a step" as a workflow is a compile error (there is no such term) |
| Step outside workflow runs plain (TS 5.0) → | leaf rule: in-step/captured calls run plain, counter unmoved (slice 1) |

## 5. Transactions (HIGH, `step-transactions.md`)

TS Incorrect: raw `pool.query` in a workflow (not checkpointed). Correct:
`dataSource.runTransaction` (exactly-once, checkpointed).

Tree: `runTxStep` is the bracketed path with `transaction_completion`
exactly-once (`Datasource/Postgres.hs:171-200`, check-outside / body+record
in one tx / `ON CONFLICT DO NOTHING` adopt-winner). Two raw paths exist
alongside it:

- `runAppSession :: AppDataSource -> Session a -> IO …` — legit callers
  (test schema setup/verification reads, demo-app `Run.hs` setup,
  `Demo.Http` handler reads) mean it stays exported. TS has the same
  property (drizzle/knex clients are directly usable). Verdict:
  **doc-level, at parity** — strengthened docs ("never inside a workflow
  body"), slice 6.
- `Tx(..)` constructor export — only the backend constructs it
  (`Postgres.hs:280`); the WidgetSim fake also constructs it
  (`WidgetSim.hs:88,113`), and forging a `Tx` grants nothing without the
  pool. Verdict: **no change** (hiding it breaks the sim fake for zero
  safety; the enforcement point is the pool, not the handle).

`isolationLevel` config exists (`TransactionConfig`, default read
committed). Datasources need the DBOS schema (`transaction_completion`);
matches `step-transactions.md:68`.

## 6. Queues (HIGH)

| TS rule | Tree verdict |
|---|---|
| No limits → `workerConcurrency` / `globalConcurrency` | validated at construction (`validateQueueOptions`, `Queue.hs:200-232`), reserved name refused first (`:347-356`); `updateQueue` re-validates merged options. Illegal configs fail fast with `ErrorConfig` before any DB touch — hard enough; proofs-at-type would be theater |
| `workerConcurrency ≤ globalConcurrency` | rejected by `ordered` check (`:214,228-232`), incl. partition pairs |
| Dedup: no dedup → `deduplicationID`; active while DELAYED/ENQUEUED/PENDING, released on completion | `DuplicationPolicy = Reject \| ReturnExisting` (default `Reject`), `QueueDeduplicated` structural error, holder lookup + adopt (`Workflow.hs:532-547`). `ReturnExisting` without key rejected (`validateEnqueue :398-401`) — but **at call time, not construction**: `enqueueNew` builds the illegal shape freely. Optional hardening: smart constructors; deferred — every call site validates, so misuse fails fast with `ErrorConfig` |
| Singleton (`return-existing`) discards colliding args, resolves with original's result | matches holder-handle semantics |

## 7. Idempotency (MEDIUM, `pattern-idempotency.md`)

TS Incorrect: no workflow id (double charge). Correct: caller-named id;
same id joins (`return-existing` default); `reject` opt-in throws
`DBOSWorkflowIDInUseError`, matched structurally.

Tree: caller-named or generated; **children derive deterministically**
(`parentId <> "-" <> show stepId`, `Workflow.hs:521-526`) so replay
re-derives and adopts. Plain start with an existing id **joins**
(`Workflow.hs:585-589` — "joining rather than erroring is what makes a
retried request idempotent"), which is TS's default. No `reject`
opt-in: same-id-different-shape collides structurally
(`ConflictingWorkflow`, `Postgres.hs:1345-1353`), so silent
double-execution with different semantics is already refused. Verdict: at
parity with TS's default; a `reject` policy would be additive API, not a
hole. No delta.

## 8. Deltas

Implemented (slices 4–6): `insideAStep` + depth guards on `send` /
`recv` (slice 4), `getEvent` / `runTxStep` (slice 5), each with
live+sim RED tests; `raceCancel` + `runAppSession` docs (slice 6).
Deferred: a typed start wrapper (taking the input value with `ToJSON`
instead of `Maybe SerializedWorkflowValue`); the ctx form already takes
`WorkflowCtx`, so no rewire churn blocks it — still deferred as a papercut.
manual-encode footgun fails fast today (matchable `Codec` errors), so
this is a papercut, not a hole. Rewire retired 2026-10-07: the ctx form already takes `WorkflowCtx` and the in-workflow enqueue ships via `startQueue` (§2); the remaining instance-surface escape is an accepted limitation pending an ADR-0026 decision (`start-enqueue-escape-todo.md` R1). Accepted limitations: §3 determinism/globals,
no `StepCtx` overload (§2). Accepted limitations: §3 determinism/globals,
§5 raw-pool doc-level, §6 dedup-construction.
