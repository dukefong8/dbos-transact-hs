# TODO — scoped workflow capabilities + step tables

Branch `proto/phantom-brands` (cut from main-line work; main untouched).
Date: 2026-10-04. Gate state: full suite **650/650**, psql mirror green,
migration ceiling 114, permanent probes 10/10 (`make probes`).
Design record: `.lavish/rust-port-plan.html` (Phase 10, Rule 9, TODO 12–16),
`docs/scoped-workflow-capabilities.md` (§1–§10),
`docs/agent-skill-api-review.md` (per-rule verdicts),
`proto/deterministic-steps/README.md`, `proto/dual-store/README.md`.

## Constraint-audit actions (2026-10-16)

Audit of the Python skill's Lifecycle/CRITICAL, Workflow/CRITICAL,
Step/HIGH, Queue/HIGH constraints against the port; tables in
`.lavish/dbos-python-constraint-audit.html`. Tree audited: `ad906e0` +
the 19-file uncommitted stack. Fix in this order, red-first where a pin
holds the wrong behavior:

- [x] B1 queue over-validation: drop `partitionRate`'s cross-check in
      `validateQueueOptions` (`Queue.hs:252, 271-277`); Rust compares
      concurrency only (`queue.rs:455-461`). Repoint the pins at
      `QueueCases.hs:270` and `:349` to acceptance.
      Done 2026-10-16, uncommitted — plus the missing fourth bound
      (`partition_worker_concurrency <= concurrency`, both oracles refuse;
      new `incoherentTable` row pins it).
- [x] B2 both concurrency spellings: refuse `concurrency` +
      `globalConcurrency` both-set with Python's message
      (`_queue.py:148-151`); flip `scenarioConcurrencySpellings`
      (`QueueCases.hs:414-441`).
      Done 2026-10-16, uncommitted — including the `queueOptionsAfterChange`
      canonicalization (merged options name one spelling) and the
      change-level both-set refusal, with the `scenarioConcurrencyUpdate`
      migration case.
- [x] B3 empty first-wait mirror: `MemSystemDB.awaitFirstWorkflowId []`
      refuses like live (`Backend.hs:2596`) instead of hanging
      (`IOSim.hs:1250`); annotate the mock placeholder pin
      (`IOSim.hs:186`, `IOSimTest.hs:161`); add the shared
      executor-level `waitForFirstWorkflow` empty-set case (also
      closes G5's wait coverage).
      Done 2026-10-16, uncommitted — three shared executor-level cases
      (first-empty refusal on the `SystemDatabase`/`InvalidInput` channel,
      all-empty satisfied, settled-member winner) in `WaitCases`, live+sim.
- [x] G1 `Duration` abstract from `DBOS.Transact` (`Transact.hs:188`);
      keep `millisDuration`/`secondsDuration`, internal `(..)` stays.
      Done 2026-10-16, uncommitted — the constructor is no longer exported
      from the facade; no consumer used it raw.
- [x] G5 executor-level wait coverage: shared first-wait winner,
      empty-set refusal (paired with B3), and wait-all empty-is-satisfied
      cases. Done 2026-10-16, uncommitted — five shared
      `WaitExecFixture` cases (first empty refused on the
      `SystemDatabase`/`InvalidInput` channel, all-empty satisfied,
      settled-member first winner, all-settled completes, single wait
      reports the settled outcome), live+sim.
- [ ] Notes (dated, no behavior): G4 `_dbos_` prefix
      (`_queue.py:58`) vs exact-name-only here (`Queue.hs:366`, Rust
      `sysdb/mod.rs:21`); G6 observability query timeout, graceful
      destroy (`Instance.hs:214`), client schedules/rename exposure,
      `set_latest_application_version` writer; G7 datasource comment
      "levels both oracles expose" (`Datasource.hs:84-89`) and
      ADR-0021's stale `ReadCommitted` default (code: `Serializable`).
- [ ] G3 config: `configUseListenNotify`/`configMigrate`
      (`Config.hs:70-71`) are read nowhere — wire or mark deliberately
      inert (polling-only port; migrations out-of-band).
- [x] G2 client slices: queue CRUD + `set_*`, `wait_first`,
      `update_workflow_attributes`, `set_workflow_delay`,
      `enqueue`/`enqueue_in_transaction`, `list_queued_workflows`,
      `read_stream`; client `register_queue` defaults `AlwaysUpdate`
      and refuses `UpdateIfLatestVersion` with `ErrorConfig`
      (`queue.rs:640-660`). Done 2026-10-16, uncommitted —
      `clientRegisterQueue`/`RetrieveQueue`/`UpdateQueue`/`DeleteQueue`
      (reusing the engine's guards + validators), `clientWaitFirst`
      (Python's non-empty/no-duplicate refusal), `clientUpdateWorkflowAttributes`,
      `clientSetWorkflowDelay`, `clientListQueuedWorkflows`,
      `clientReadStream`/`clientReadStreamFrom` (the shared reader
      generalized to a `ClientConnection` placement via
      `readStreamOutside`), `clientEnqueueWorkflowInTx` (+ Postgres
      `enqueueInTx`). Remaining G6 items (schedules/rename exposure,
      `set_latest_application_version` writer) stay with their notes.
- [ ] Stack-review follow-ups: R1 empty fork input returns `Ok` with no
      validation on every layer now (engine early-`Right`, sim null guards
      matching the Postgres guards; shared refusal scenario pins both
      empty verdicts plus the two zero-count announcements in the sim
      trace) — done 2026-10-16, uncommitted; R2 comment narrowed to
      launched-instance entries (client forks call the backends directly)
      — done 2026-10-16, uncommitted; R3 `coghcid.sh` moves out
      of the repo root.
- [ ] Close-out: full `cabal test all` + direct psql mirror against
      `$DBOS_DATABASE_URL` with the watcher idle; then `/review`.

## Locked decisions (do not relitigate without new evidence)

1. Phantom brands over Bluefin-the-dependency (`Eff` is `Env -> IO a`,
   kills the IOSim dual stack); Bluefin patterns only.
2. Mechanism before surface: depth counter, then the `Ctx` split, then
   the engine rewire.
3. Calls degrade to plain, starts refuse (`InsideStep`) — oracle-faithful.
4. `withAttempt` restores via `onException` + explicit success unbump
   (keeps `MonadCatch`, no `MonadMask` cascade; residual window leaks safe).
5. One shared `insideAStep` predicate (scope field OR depth) read by every
   guard: `placeCall`, `takenPlacement`, `startChildWorkflow`, `send`,
   `recv`, eager `getEvent`, `runTxStep`.
6. `WorkflowCtx`/`StepCtx` run parallel-track until the rewire; `Ctx`
   stays until the last module flips.
7. App-level StepCtx-keyed domain records; engine core stays ops-free.
   Registration closes over tables, helpers take them explicitly, step
   bodies capture + receive `StepCtx`. Per-workflow tables (no shared
   kitchen-sink record); runners never see them.
8. Runners take `Executor m` (Phase B); scope binder keeps narrow
   `Connection`/`Identity` (launch-free for unit tests).
9. `withWorkflow` takes `WorkflowId` (+ `workflowIdText` projector);
   smart-constructor hiding rejected (45-site churn for one blank check).
10. Enforcement ledger: compile-time where types reach; runtime refusal
    with matchable ADTs where capture defeats types; documented
    discipline where Haskell cannot reach (determinism, raw pool).
11. Probes before tree; throwaway packages under `proto/`, never merged.
12. TDD/watcher process per AGENTS.md: one eval pair, `make db-migrate`
    first, no `cabal test` overlapping evals, psql verify per gate.

## Phase A — design consolidation (THIS FILE + plan doc)

- [x] A1: fold deltas (plan HTML Phase 10 / Rule 9 / TODO 12–16 / sidebar).
- [x] A2: record decisions 1–11 (this section).
- [x] A3: widget table sketch (`docs/widget-step-tables.md`):
      `CheckoutSteps`/`DispatchSteps` (the actual workflow pair), `setStatus`
      repeated per the ≤2 rule, STM handlers with split-race fixes,
      failing variant, live contract specified for C-phase.

## Phase B — Executor runner slice — DONE (ac51207)

- [x] B1–B5 landed: `datasources` field; launch paths return `Executor`;
      three runners take it (`ErrorNotLaunched` unconstructible there);
      facade exports `Executor` abstract; every test file and both demo
      apps thread the executor; the run-before-launch case retargeted to
      retrieve (compile-time half recorded).
- [x] B6 gate: 644/644, psql 3/3, sim trees re-run one pair at a time
      (WorkflowSim 52/52, ManagementSim 12/12, WidgetSim 3/3).
- Remainder (NOT in slice): enqueue/queue/management/dequeue/getters/
  `pendingGetEvent` stay on `DBOS`+`requireExecutor`.

## Phase C — engine rewire (module order fixed)

Step → Handle → Select/Event/Sleep → Workflow → Management/Datasource/rest.
Per module: signatures → call sites → live+sim → gate. `Ctx` stays until
the last module flips.

- [x] C1a scoped step runners (7384031): `runStepScoped`
      (workflow view, allocates, hands `StepCtx`) and `runNestedStep`
      (step view, plain, no allocation possible); context seam readers
      `stepCtxAt`/`stepCtxTracer`/`workflowCtxInner`; live+sim tests with
      exact trace asserts; 646/646.
- [x] C2a scoped pending pair (0bc1852): `pendingStepWithScoped`/
      `pendingStepScoped`/`driveStepWithScoped`;
      `awaitChildScoped`/`pendingAwaitScoped`; live+sim tests (StepSim
      pins the drive trace: `StepOutputRecorded` only).
- [x] C2b scoped sleep/event/select (0adf014): `sleepStepScoped`/
      `pendingSleepScoped`, `setEventScoped`/`pendingSetEventScoped`/
      `getEventScoped`/`pendingGetEventScoped`, `selectStepScoped`; the
      shared `scenarioScopedSelect` races two pending steps (rows
      `[(1,"fast"),(2,"DBOS.selectStep")]`, sim trace pinned); WfFixture
      gains `wfConn`/`wfIdentity` for scope-building scenarios; 649/649.
- [x] C2c `PendingStep`/`SelectArm`/`Winner` carry `exec` (814b6cb):
      scoped producers tie the brand to their view, so a cross-execution
      arm is a compile error (verified with a temporary in-tree negative:
      "Couldn't match type 'exec1' with 'exec'"); old producers keep the
      parameter call-site-quantified for unconverted call sites.
- [x] C4a scoped registry bodies (34c406f): `ErasedWorkflow` is a rank-2
      newtype; the run path builds the scope with `withWorkflow` and
      installs the task spawner; `registerTypedWorkflowScoped` /
      `registerWorkflowRefScoped` (+ Instance twins) register converted
      bodies; old entries wrap with an explicit downgrade so converted and
      unconverted bodies share one registry. `WorkflowCtx`/`StepCtx` pin
      `(exec :: Type)`. Mem backend's `recordSleep` now composes
      check+record like Postgres.
- [x] C4b scoped entries for the rest of the body surface (429e8c8):
      `startChildWorkflowScoped`, `sendScoped`/`sendWithScoped`/
      `sendBulkScoped`/`sendBulkWithScoped`/`recvScoped`,
      `runTransactionScoped`, six management `...Scoped` wrappers. Two
      bodies converted as the pattern (the capture-refusal body now uses
      `withStep` + `startChildWorkflowScoped` and still refuses).
- [x] C4c bulk (2455671): every registered body across ClientTest,
      DeadlinesTest, EventTest, InstanceTest, ManagementTest, QueueTest,
      RegistryTest, WidgetTest, WorkflowTest and the sim trees takes
      `WorkflowCtx` and registers through the scoped entries. Each body
      keeps one explicit `let ctx = workflowCtxInner wctx` downgrade;
      local body-forwarding helpers are retyped rank-2; sim bodies that
      capture state/refs move to top-level helpers (forall s in argument
      position, forall exec in result position — MonoLocalBinds cannot
      generalize let-bound bodies to rank-2). Gates: Workflow pair 54/54,
      ManagementSim 12/12, Widget pair 4/7, full suite 650/650, psql 3/3.
- [x] C1c step-body flip (fa5768f): StepRetryTest/StepTest/EventTest/
      MessageTest/WidgetSim/WidgetTest/DatasourceTest step bodies take
      `StepCtx`; runner call sites go through runStep(With)Scoped;
      nested shapes use `runNestedStep`; the Datasource fixture is
      `dsFixtureRun` (scope runner) instead of a Ctx builder. In-step
      downgrades for refusal tests stay explicit via `stepCtxInner`.
      Gates: full suite 650/650, DatasourceSim 11/11, WidgetSim 7/7.
- [x] C5b registration + twin-swap (4eb040f, 2785fee): every body's
      alias deleted and calls moved to the scoped twins; the old
      registration entries (`registerTypedWorkflow`, `registerWorkflowRef`,
      `registerDBOSWorkflow`, `registerDBOSWorkflowRef`) and their facade
      exports deleted; demo apps converted and registered scoped. Gates:
      650/650, sim trees individually green, demo apps load clean.
- [x] C5c raw start/enqueue paths (50d8eb1): the facade no longer exports
      enqueueWorkflow/startWorkflowRef/runWorkflowRef/startChildWorkflow/
      runRegisteredWorkflow*; starts and enqueues reach application code
      only through the Executor/DBOS wrappers or the WorkflowCtx scoped
      entries. WorkflowTest's two in-step negatives moved to the captured
      shape; the direct in-step shape has no facade term.
- [x] C5c probes: permanent `probes/` pairs + `make probes` (four
      negatives matched to their error class, four witness twins clean):
      child start, allocation, nested step, cross-exec race.
- [x] C5c inner accessors (superseded twice): `workflowCtxInner`/
      `stepCtxInner` LEFT the facade in the C5a trim, and the C5 close
      (106e872) deleted them outright — views own their fields now, so
      there is no inner context to reach. The backstop tests construct
      illegal shapes through the view vocabulary itself
      (`stepCtxBoundary`, `stepCtxWorkflow`, captured parents), and the
      exec-brand probes pin the shapes that must not typecheck.
- [x] C5a op-pair promotion: every `*Scoped` operation takes the plain
      oracle name (`send`, `recv`, `setEvent`, `getEvent`, `runTxStep`,
      `runStep(With)`, `sleepStep`, `awaitChild`,
      `selectStep`, `startChildWorkflow`, the pendings, the six management
      `...InWorkflow`s, the `register*` entries); the raw-context workers
      are inlined or module-private (`selectStepOn`, `runTxStepWith`,
      the `drive*` drivers). Direct-shape backstop tests move to the
      captured shape with identical verdicts — except place-first ops,
      where capture now degrades to plain per the new `checkHere` arm
      (deviation 6); the transaction direct-shape case deletes
      (compile-time by construction, captured twin keeps G4). New:
      `stepCtxWorkflow` projector (same brand; backstop still applies).
      Gates: full suite 650/650, every converted sim re-run pair-green,
      probes 8/8.
- [x] C5a reader twins — decided minimal (338bb97 follow-up): exactly one
      twin, `stepCtxCancellationToken`, the `stepCtxX` mirror of the raw
      stem. Rationale: it is the only reader with app-body demand (token
      polling is contract-level); observation/scaffolding readers
      (`stepId`/`stepStatus`/`withSystemDB`/`workflowId`, 7 test-only
      sites) keep the documented downgrade; engine-only readers need
      nothing. Rejected: full twin set (shallow), `stepCtxCancelToken`
      (verb-stem collision with the firing function), `cancellingToken`
      (near-miss with the raw reader), typeclass overloading. 4 body
      sites swapped; StepTest's 2 workflow-scope checks stay downgraded
      by design (documented asymmetry). Gates: 650/650, probes 8/8.
- [x] C5b remainder (finished 2026-10-04, 106e872): `data Ctx` deleted —
      `WorkflowCtx`/`StepCtx` own every field (conn/identity/state/spawner/
      tracer; scope), `withStep` holds the bump/restore/cancel trio, the
      ten `workflowCtxInner` downgradelets are gone with the accessor, the
      `currentConnection` hole is closed (`workflowConnection` is
      engine-internal and takes a view), and the exec-brand `-fno-code`
      probe pair landed (`neg-exec-escape`/`w-exec-escape`, probes 10/10).
      Engines and tests build views; `newWorkflowCtx` replaces `newCtx` as
      the documented fixture seam.
- [x] C2 Handle — executed as C2a + C5a: awaits over scoped views
      (`awaitChild`/`pendingAwait` take the plain oracle names).
- [x] C3 Select/Event/Sleep — executed as C2b + C5a: scoped arms/reads/
      sleeps under the plain names.
- [x] C4 Workflow execute path — executed as C4a/b/c + C5c + C5a:
      `startChildWorkflow` takes `WorkflowCtx` (the typed surface is the
      `WorkflowRef m e`-taking entry); enqueue/start are `WorkflowCtx`-only
      with no `StepCtx` overload.
- [x] C5 Management/Datasource/rest + final `Ctx` removal (106e872): the
      six management `...InWorkflow`s, `runTxStep`, waits, selects,
      events, messages, sleeps, steps, handles, and checkpoints all take
      views; `stepCtxBoundary` is the boundary-shaped attempt view drives
      hand where a step view is expected. Gates: full suite 650/650,
      probes 10/10, migrate 114→114, psql mirror green.

## Phase D — StepOps widget pilot (`docs/widget-step-tables.md`)

- [x] D0 sketch: `CheckoutSteps`/`DispatchSteps` (StepCtx-keyed, `OrderId`
      boundary, `setStatus` repeated per the ≤2 rule), STM handlers
      (single-`atomically`, split-race fixes), failing variant, status
      codes, live contract specified for C-phase.
- [x] D1 tables + STM handlers + failing variant + direct handler tests
      (ca71aac): mint sequence, oversell race (one winner, stock 0),
      failing third call aborts whole, status codes `(1,0)` matching the
      live assertions; tables built inside the scope continuation so the
      phantom unifies; watcher Widget pair green (live 4, sim 7).
- [x] D2 engine integration (1f9d630 sim + live half): the
      `runTransactionScoped` bridge hands bodies `(StepCtx, Tx)`;
      `WidgetSim` bodies spend `stmCheckoutSteps`/`stmDispatchSteps`,
      `Tx`-ignoring fakes and `failingWidgetDs` retired; live bodies spend
      `pgCheckoutSteps`/`pgDispatchSteps` closing over the held `Tx`; the mixed
      live+canned case (`failingPgCheckoutSteps` refuses the paid mark)
      asserts no `order_id`, order `(1,0,3)`, inventory `4`, PENDING row
      with no dispatch child. Gates: full suite 651/651, Widget pair 5/7.
- [ ] D3 follow-up domains after the pilot proves the pattern (intentionally
      open: next-domain work, not this epic's gate).

## Standing gates (every slice)

1. `make db-migrate` first (idempotent, verify ceiling).
2. Watcher: exactly one eval pair per edit; read `ghcid.txt` after reload.
3. `cabal test` only with the watcher idle (shared-DB `40P01` otherwise).
4. psql mirror against `$DBOS_DATABASE_URL` (NOT the default `dbos` DB
   unless the env says so).
5. Read-only Rust oracle spot-checks (`~/dev/dbos-transact-rust`).
6. One commit per slice with gate numbers in the message.

## Probe index (reference only, never merge)

| Probe | Runner | Claim | Gate |
|---|---|---|---|
| `proto/scope-brands/run.sh` | 19/19 | brands + backstop + pool pinning | compile-fail negatives + runtime marks |
| `proto/deterministic-steps/run.sh` | 12/12 | Ops carrier + journal replay, IO vs IOSim | agreement diff + exploding-handler replay + 3 negatives |
| `proto/dual-store/run.sh` | 7/7 | Postgres-txn vs STM step table | byte-identical invariant lines + 1 negative |

## Known asymmetries (documented, not hidden)

- STM serializes; Postgres is read-committed — op safety comes from
  single-statement check-and-decrement; ops stay coarse-grained.
- Body-atomicity across several steps exists on IO (held connection) but
  has no STM counterpart for `m`-typed bodies; the shared unit is one step.
- Brands track executions, not lexical nesting; the depth counter tracks
  dynamic step scope. Both layers stay.
- Record-access conventions landed 2026-10-04: dot-first reads (preference
  order in `AGENTS.md`), `RecordWildCards` a cabal default, pass-through
  accessors deleted, `StepStatus` oracle-shaped; public fields are
  camelCase (ADR-0023) while SQL/Aeson/config keys and log prose keep
  Rust spelling.
- `hasql` pinned to the repo's 1.10 range in probes (2.x moved `acquire`).
