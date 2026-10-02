# Transactional steps close over one commit: `DBOS.Transact.Datasource` (port's own seam)

TypeScript's `dataSource.runTransaction` (`dbos-transact-ts/packages/knex-datasource/index.ts:invokeTransactionFunction`)
and Python's `SQLAlchemyDatasource.run_tx_step` (`dbos-transact-py/dbos/_datasource.py:843`)
are the oracles; Go's `RunAsTransaction` and Java's `@TransactionalStep` agree.
Rust has no counterpart yet — `demo-apps/dbos-rust-widget-store/src/store.rs:1-14` says so
plainly, and its inventory writes are at-least-once across two commits. The Haskell
port therefore cannot map this 1:1 onto a Rust module; it is the port's own seam,
in the same class as the `[typedSql| ... |]` session modules (not a Rust module
counterpart, so no split to record — but the absence of an oracle module is recorded
here instead).

Decided 2026-10-02 ("one commit, not two"): a transactional step commits the
application writes and the step's checkpoint in a single database transaction on a
separate application pool, replaying the recorded outcome without re-running the
body — branch-for-branch against Python `run_tx_step:871-933`:

- Pre-check (`_check_execution_with_retry`) → replay recorded output/error as itself.
- `with session.begin():` holds user writes + `session.flush()` + `_record_result`
  + `_still_owns` ownership check — one commit. The port spells this
  `dsWithTransaction` over `hasql-transaction` (`Tx.Transaction`, ADR-0011), with
  hand-written `Statement`s via `Tx.statement` (typedSql `Session`s do not compose).
- `_StepAlreadyRecorded` stays internal → adopt the winner (`_replay_conflicting_step`)
  or raise the ownership conflict unrecorded when this execution lost.
- Retriable errors (PG `40001`, class `40` → `Transient`) loop with backoff
  (`MonadDelay`, so IOSim drives it); the conflict error is rethrown, never recorded.
- In workflow the whole body runs under the step-id allocator (Python wraps `_body`
  in `run_step`); outside a workflow it runs plainly with no guarantees. In-step
  calls are refused (`InsideStep "transaction"`, the `Event.hs:54` shape).

Decision, in parts:

- New module `DBOS.Transact.Datasource` (plain Haskell, no Bluefin): `IsolationLevel`
  (`ReadUncommitted|ReadCommitted|RepeatableRead|Serializable`), `TransactionConfig`
  (`txName`, `txIsolation` — no `readOnly` in v1), `RecordedOutcome`
  (`RecordedOutput|RecordedError`), opaque `Tx m` (explicit where TS uses ALS),
  `DataSource m` as a record-of-functions (`dsCheck/dsWithTransaction/dsRecordOutput/dsRecordError`
  — test fakes are records, per the typeclass rule), `runTransaction` /
  `registerTransaction`, and `TransactionEvent` homed here (Rule 5: emitter-adjacent
  leaf; `LogEvent`+`ToLogStr`, `Warning` only for the contention retry).
- Error mapping (ADR-0019 channel): body failure → `Application e` via `application`;
  refusal → `InsideStep`; ownership conflict and exhausted transport → `ErrorSystemDatabase`
  (control, never recorded); no new variant, no `StepError` reuse (`StepError` has no
  live mapping — the live code maps onto `Error e` directly).
- Serde reuses `encodeWorkflowValue`/`decodeWorkflowValue` (Aeson ≡ SuperJSON).
- Schema is verify-only: the `transaction_completion` table is checked, never migrated
  (ADR-0004 guardrail; mirrors TS `runMigrations:false`). Default isolation
  `ReadCommitted`; the sweep ladder (`RepeatableRead`/`Serializable`) is copied only
  if an app-pool claim ever needs it.
- Deferred: `readOnly` fast-path, `pendingTransaction` for `select_step!` races,
  `deleteCheckpoints` for rewind/completion. Each earns its own case when a
  widget-track test needs it.

Prototypes (2026-10-02): P1 (`Proto.hs`, runghc, no DB) validated the loop semantics
11/11 — replay-before-run, conflict adopt, backoff retry, refusal writes nothing,
recorded-error replay. P2 (standalone hasql build) was abandoned as too slow to
compile; its questions were answered from the Python oracle + ADR-0011 recon instead.

Consequences:

- Staging, each ending green: (1) this ADR; (2) types + `TransactionEvent` + one pure
  test; (3) `runTransaction` engine + live/sim mirror over a fake `DataSource`;
  (4) Postgres app-pool binding + `Statements`-style `Tx` sessions;
  (5) facade re-exports; widget composition stays a follow-up.
- Gate: `tests/test_datasource.py` (records/replays `:332`, errors `:379`,
  serialization retry `:414`, duplicate-wins `:662`, ownership rollback `:767`,
  completion clears `:806`) stays green read-only throughout; the Haskell mirror
  cases map 1:1 onto it.
- Facade (`DBOS.Transact`) re-exports the new names under a `-- * Transactional steps`
  group and `TransactionEvent` in the tracing group; it never redefines them.

## Addendum: interface comparison (codebase-design, 2026-10-02)

Three shapes were designed in parallel and judged on depth (leverage per
interface unit), seam reality (one adapter = hypothetical), the deletion
test, and correctness (exactly-once must be achievable, not just drawable):

- **Record seam (kept).** The `DataSource` record + `runTransaction` stands:
  6 fields, one loop, STM fake now, live binding next. `dsRecordError` was
  fixed to take `Tx` and return `Bool` like `dsRecordOutput` — an error
  checkpoint outside the commit violated the module's own invariant.
- **Class-homed checkpoints (rejected).** Checkpoint-only `SystemDB` methods
  cannot receive app SQL without a driver type (violates the class's
  driver-free rule), so app writes would commit separately from the
  checkpoint: provably at-least-once, with the crash window owned by neither
  backend. It also forces same-database deployment against all four oracles'
  independent pools. Fails the deletion test (moves rows, not responsibility)
  and correctness both.
- **Fragments + native runner (deferred).** Sound but doubles the loop and
  the test surface for one skeleton; kept as the fallback if the live
  binding below proves unworkable.
- **Live binding shape (decided):** `dsWithTransaction` holds dedicated
  connections with explicit `BEGIN`/`COMMIT`/`ROLLBACK` (never
  `runTransactionAt`, which only accepts closed `Tx.Transaction` bodies);
  `txStatement` runs `Session.statement` on the held connection with the IO
  body sequenced between statements — the Haskell spelling of Python's
  `with session.begin()`. Connection management lives inside the adapter;
  the interface does not grow.

Recorded 2026-10-02.
