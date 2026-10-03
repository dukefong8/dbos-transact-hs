# TODO — scoped workflow capabilities + step tables

Branch `proto/phantom-brands` (cut from main-line work; main untouched).
Date: 2026-10-03. Gate state: full suite **644/644**, psql mirror green,
migration ceiling 114, both probes green (12/12, 7/7).
Design record: `.lavish/rust-port-plan.html` (Phase 10, Rule 9, TODO 12–16),
`docs/scoped-workflow-capabilities.md` (§1–§10),
`docs/agent-skill-api-review.md` (per-rule verdicts),
`proto/deterministic-steps/README.md`, `proto/dual-store/README.md`.

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
   `recv`, eager `getEvent`, `runTransaction`.
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
- [ ] A3: per-domain table sketch for widget (checkout vs restock fields,
      shared-field convention: repeat or embed, no inheritance).

## Phase B — Executor runner slice

- [ ] B1 `Instance.hs`: `Executor` gains `datasources`; `launchOn` +
      `launch`/`launchWithEnvironment`/`startExecutor` return `Executor`
      (double launch hands back the live one).
- [ ] B2 `runDBOSWorkflow`, `runDBOSWorkflowRef`,
      `startDBOSWorkflowRef` take `Executor m`; drop `requireExecutor`
      boilerplate; checkpoint clearing via `executor.datasources`.
- [ ] B3 facade exports `Executor` (abstract).
- [ ] B4 `WfFixture.wfLaunch` retyped; scenarios bind exec (covers
      live+sim); sweep Event/Queue/Widget/WidgetSim/Instance/Client/
      Deadlines/Handle/Management launch-match + threading fixes.
- [ ] B5 demo-apps `Handler.hs` ×2 by hand + standalone typecheck
      (outside cabal gates).
- [ ] B6 gate: watcher pair green, full `cabal test`, psql mirror, commit.
- Remainder (NOT in slice): enqueue/queue/management/dequeue/getters/
  `pendingGetEvent` stay on `DBOS`+`requireExecutor`.

## Phase C — engine rewire (module order fixed)

Step → Handle → Select/Event/Sleep → Workflow → Management/Datasource/rest.
Per module: signatures → call sites → live+sim → gate. `Ctx` stays until
the last module flips.

- [ ] C1 Step: runners accept `StepCtx`, degrade-by-construction;
      `PendingStep exec` introduced here; consumers adapt in their slices.
- [ ] C2 Handle: awaits over scoped views.
- [ ] C3 Select/Event/Sleep: scoped arms/reads/sleeps.
- [ ] C4 Workflow execute path: `startChildWorkflow` takes `WorkflowCtx`;
      enqueue/start `WorkflowCtx`-only (no `StepCtx` overload — closes the
      `currentConnection` hole); typed start wrapper lands here.
- [ ] C5 Management/Datasource/rest; final `Ctx` removal; full gate.

## Phase D — StepOps widget pilot

- [ ] D1 `CheckoutOps`/`RestockOps` tables (StepCtx-keyed fields).
- [ ] D2 Postgres handler behind the held connection; STM handler in sim.
- [ ] D3 retire `WidgetSim` statement-unsupported stubs; race/rollback/
      refusal teeth + one mixed live+canned test.
- [ ] D4 follow-up domains after the pilot proves the pattern.

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
- `RunOptions.runWorkflowId` still bare `Text` (plan TODO 16).
- `hasql` pinned to the repo's 1.10 range in probes (2.x moved `acquire`).
