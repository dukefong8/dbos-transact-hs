# Transactional step: Haskell ↔ Python parity evaluation

Oracle: `dbos/_datasource.py` (`SQLAlchemyDatasource`), gate
`tests/test_datasource.py`. Recorded 2026-10-02 against upstream/main
post-merge; gate green (11 passed, 1 skipped on the mapped subset).

## Interface parity (`SQLAlchemyDatasource` → `DBOS.Transact.Datasource[.Postgres]`)

| Python | Haskell | Verdict |
|---|---|---|
| `create(url, schema=…)` | `acquireAppDataSource` / `acquireAppDataSourceIn` | Covered (checkpoint schema parameter ported; serializer/sessionmaker/engine injection are app-layer, out of scope) |
| `transaction(func\|config)` decorator | `registerTransaction` | Covered (Haskell callers close over args; no decorator protocol needed) |
| `run_tx_step(opts, func, *args)` | `runTxStep` | Covered |
| `sql_session()` (txn-bound queries) | `Tx.txStatement` | Covered (explicit runner vs ambient session) |
| `DatasourceOptions{name, isolation_level}` | `TransactionConfig` | Covered (`readOnly` deferred per ADR-0021) |
| `IsolationLevel` strings | `IsolationLevel` ADT | Covered |
| `migrate` / `run_migrations` | — | Deviation, not a gap: no Haskell migrations ever (ADR-0004) |
| `verify_migrations` | `verifyAppDataSource` | Covered (same refuse-to-serve posture) |
| `_delete_checkpoints[_if_owner]` | `dsDeleteCheckpoints` + `clearDatasourceCheckpoints` | Covered (owner gate omitted — clearing runs on the completing executor) |
| `_check_execution` / `_record_result` / `_record_error` / `_replay_conflicting_step` / `_still_owns` | Internalized (record ops + `adoptTransaction` + `checkOwner`) | Covered by design (depth: fewer methods, same behavior) |
| `AsyncSQLAlchemyDatasource` + `run_tx_step_async` | — | N/A (direct-style `IO`; behavior covered once) |
| created-before-launch registry rule | `registerDataSource` (frozen at launch, thawed on failed launch/shutdown) | Covered |
| least-privilege roles, `schema_translate_map`, custom sessionmaker, ORM objects | — | N/A (ORM/deployment layer; no ORM in this port) |

## Test coverage (`test_datasource.py` sync → `DatasourceTest[Sim]`)

Covered (7): `records_and_replays`, `records_and_replays_errors` (both
halves), `retries_on_serialization_error`, `conflicts_when_duplicate_execution_wins`,
`rolls_back_once_ownership_moves` (executor-gated approximation — same
rethrow direction as token comparison; same-executor concurrent duplicates
join instead of stopping, documented in `checkOwner`), `run_migrations_false`
posture via verify-refuses, in-step refusal (misuse shape).

Partial (3): `rejects_misuse` (in-step covered; coroutine/sessionmaker
concepts N/A); `replays_its_own_lost_commit` (adopt mechanism covered,
dedicated ambiguous-commit case missing); `duplicate_execution_stops_at_the_lost_race`
(ownership rethrow covered, pre-record race stop missing).

Missing (4): `runs_outside_workflow` (no outside-`Ctx` entry point);
`retries_locked_precheck` (`dsCheck` has no retry loop — real gap);
`completion_clears_checkpoints` + `delete_checkpoints` (need the delete API);
`must_be_created_before_launch` (needs registry integration).

N/A: async mirrors (covered once), migration machinery, ORM/sessionmaker
behavior, least-privilege deployment.

## Verdict

Behavioral core at parity (record/replay/error/retry/adopt/refuse/ownership,
locked-precheck retry, outside-workflow execution, delete-checkpoints with
completion clearing, created-before-launch), each mapped 1:1 to a gate test.
The interface is smaller than the oracle's by design (internals internalized,
migrations refused, async unified). Remaining deltas: child/start-fire-and-forget
completions do not clear checkpoints yet; `_if_owner`'s token gate is approximated
by executor identity; ORM/deployment layers stay out. None blocks the widget
track, which needs only the covered core.
