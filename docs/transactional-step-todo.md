# Transactional step (`DBOS.Transact.Datasource`) — build tracker

Oracle: Python `dbos/_datasource.py:run_tx_step`, gate `tests/test_datasource.py`.
ADR: `docs/adr/0021-transactional-step-datasource.md`.
Parity evaluation: `docs/transactional-step-parity.md`.

## Done

- [x] ADR-0021 (seam, semantics, error/event/schema/Sim rules, staging, gate map)
- [x] Types + `TransactionEvent` + `transactionConfigDefault` + pure test (green)
- [x] P1 engine-logic prototype 11/11 (`/tmp/opencode/ds-p1/Proto.hs`, throwaway)
- [x] Watcher pair on Datasource IO/Sim; `runTxStep`/`registerTransaction`
      signatures staged as loud stubs; 5+5 ported cases compile, fail red at seam
- [x] `RecordedOutcome` stays text-shaped: decode paths ignore serde tags
      (`Serialization.hs:61`), so no per-row tag needed in v1
- [x] Locked-precheck retry (`checkWithRetry`: transient reads back off with
      the oracle's 1ms×1.5/2s constants, attempts restart per phase)
- [x] Outside-workflow entry (`runTxOutside`: same retry loop,
      nothing checkpointed, nothing announced, silent trace)
- [x] Delete-checkpoints op (`dsDeleteCheckpoints`: range delete, re-run
      after full clear)
- [x] Completion clearing + datasource registry: `DBOS.Transact.Datasource.Registry`
      leaf (`registerDataSource`/`freeze`/`thaw`/`clearDatasourceCheckpoints`);
      frozen at launch (registration after launch refused — the
      created-before-launch rule), thawed on failed launch/shutdown;
      `runWorkflow`/`runWorkflowRef` clear the finished workflow's
      checkpoints best-effort. Live test: duplicate refused, rows 1→0.
      Follow-up (recorded): child-workflow completions and `startWorkflowRef`
      fire-and-forget starts do not clear yet
- [x] Outside-workflow entry (`runTxOutside`: same retry loop,
      nothing checkpointed, nothing announced, silent trace)
- [x] Ownership check: executor-gated adoption (`checkOwner` — missing or
      unowned row adopts; foreign-executor row stops without recording as
      control). `MemSystemDB` stamps deterministic owners like live stamps
      UUIDs, so both stacks rule identically (Mock stays ownerless/canned)
- [x] Body-failure record channel: bodies report failure as a value
      (`Tx m -> m (Either (Error e) a)`, the `runStepWith`
      precedent — no `Exception` plumbing); a held error-checkpoint rolls
      back and adopts; panics propagate unrecorded
- [x] Facade complete (`-- * Transactional steps` group, incl. binding:
      `AppDataSource`, `acquire/release/verify`, `runAppSession`,
      `toDataSource`, `beginSql`); `BackendErrorKind` stays behind
      `DBOS.SystemDB` — re-exporting it from `DBOS.Transact` collides
      with the `Connection` type (recorded, not retried)
- [x] Full `cabal test test` 606/606 (`/tmp/dbos-test5.log`); psql: two
      expected `ds-own-*` PENDING rows (unique per run, foreign executor,
      untouched by sweeps), zero `ds_probe_*` leftovers
- [x] Postgres app-pool binding, verify-only (no DDL): per-attempt raw
      connections with explicit `BEGIN`/`COMMIT`/`ROLLBACK` (never
      `runTransactionAt`); `txStatement` runs `Session.statement` on the
      held connection. Live bracket tests green (commit leaves rows,
      throw rolls back, verify refuses, `beginSql` pure); scratch tables
      cleaned, psql clean
- [x] Full `cabal test test` 610/610 (`/tmp/dbos-test8.log`); one
      unrelated flaky queue case passed in isolation and on the rerun
- [x] Snoop-oracle event mapping (2026-10-02, `/tmp/ds-snoop*.log` + live
      `test_datasource.py` gate 11 passed): error-column recording →
      `TransactionErrorRecorded`; conflict replay (`_replay_conflicting_step`)
      → `TransactionConflictAdopted` (distinct from plain replay);
      ownership rethrow (`DBOSWorkflowConflictIDError`) →
      `TransactionOwnershipLost` (Warning); retry span event already covered.
      All three asserted by `selectTraceEventsDynamic` in the Sim tree.
      (Snoop caveat: worker-thread bodies escape pysnooper; deep internals
      were captured via direct `_check_execution`/`_still_owns` calls plus
      row reads instead.)
- [x] Python gate green 2026-10-02: `uv run --frozen pytest
      tests/test_datasource.py -k "<6 mapped sync tests>"` → **11 passed,
      1 skipped** (sqlite+pg params). Needed `uv sync --frozen --extra otel`
      first (otel is optional in pyproject; the venv lacked it). Oracle
      behavior also verified by direct read (`_datasource.py:run_tx_step`),
      unchanged by the upstream merge.
- [x] Lavish dated note (2026-10-02 transactional-step delta folded into
      `.lavish/rust-port-plan.html`)
- [x] Widget checkout/dispatch composition (2026-10-02): `WidgetSim`
      (sim-only, in `simTests`) composes the real engine —
      `runTxStep` checkout (create → reserve → payment-id event →
      `recv` → dispatch or compensate), a spawned `DispatchOrderWorkflow`
      (3 durable sleep ticks), and a TVar app store. Oracle-matched
      assertions: paid `inventory 5→4`, order `(0,3)→(1,0)` (PENDING→
      DISPATCHED via PAID); refused `inventory 5→5`, order `(-1,3)`
      (CANCELLED). Oracle probe `widget_probe.py` (2026-10-02) confirms
      those ladders.
- [x] Widget live variant + crash-resume (2026-10-02): `WidgetTest`
      (live, 4 cases) runs the same checkout/dispatch over real app tables
      in a per-case schema and the real Postgres binding. To isolate the
      checkpoint table, the binding gained the oracle's `schemaName`
      parameter (`acquireAppDataSourceIn`; schema validated and quoted).
      Cases: paid → inventory 4, order (1,1,0); refused → inventory 5,
      order (1,-1,3); crash while parked on `recv` → relaunch replays
      create/reserve with no duplicate order and identical checkpoint
      rows, then pays to dispatch; crash mid-dispatch → relaunch resumes
      so exactly three ticks land (progress 0, not negative). psql: zero
      `widget_%` schema leftovers; 16 checkout + 12 dispatch SUCCESS rows
      across runs.

## Case ↔ Python gate map

| Haskell scenario | Python `test_datasource.py` |
|---|---|
| commit/replay | `:332` records_and_replays |
| error replay (seeded + recorded) | `:379` errors (both halves) |
| serialization retry | `:414` retries_on_serialization_error |
| duplicate-wins adopt | `:662` conflicts_when_duplicate_execution_wins |
| in-step refusal | misuse rejection (`:298` shape) |
| ownership-moved rethrow | `:767` rolls_back_once_ownership_moves (executor-gated) |
| completion clears | `:806` (deferred with `deleteCheckpoints`) |
