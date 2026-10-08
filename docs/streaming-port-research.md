# Streaming port research: Rust oracle, current Haskell behavior, hasql-cursor-query + foldl leverage

Placement: `docs/` holds research notes flat at top level (e.g. `monadreader-seam-research.md`,
`sometracer-removal-research.md`, `bluefin-research-context.md`); there is no `docs/research/`
directory. This note follows that convention.

## 1. Oracle finding: Rust does not stream query results

The question presupposes a Rust streaming pattern to port. There is none. All
`crates/dbos/src/sysdb/postgres.rs` reads materialize:

- 23 `fetch_all` sites (full `Vec` in memory), 29 `fetch_optional`/`fetch_one` sites
  (counted 2026-10-08; `postgres.rs` is 7232 lines).
- Driver is sqlx 0.9 (`crates/dbos/Cargo.toml:43`). No `tokio-postgres`, no `RowStream`,
  no `try_next`/`StreamExt`/`BoxStream` anywhere in `sysdb/`, `recovery.rs`, `queue.rs`,
  or `management.rs` (grep for `cursor|RowStream|impl Stream|pin_mut|next().await|fetch|DECLARE`
  hits only DBOS workflow *streams*, the `write_stream`/`read_stream_value` feature, and
  sqlx `fetch_*`).
- Unbounded-looking reads are bounded by protocol, not by cursor:
  - `list_workflows` (`postgres.rs:3017-3226`): `QueryBuilder` + `LIMIT`/`OFFSET` bound from
    `filter.limit`/`filter.offset`, `ORDER BY created_at` (`:3204-3215`), ends
    `q.build().fetch_all(&mut *conn)` (`:3215`).
  - `list_workflow_steps` (`postgres.rs:4615-4659`): same shape, `ORDER BY function_id`,
    `LIMIT`/`OFFSET`, `fetch_all` (`:4644`).
  - `get_all_notifications` (`:4947-4980`), `get_all_events` (`:4982-...`): plain
    `fetch_all(pool)` with `ORDER BY`.
  - `rename_application_in_batches` (`:2539-2640`): key-range batching over `workflow_uuid`
    (`SELECT DISTINCT ... LIMIT 1 OFFSET batch_size-1` bound at `:2586`, range updates at
    `:2591-2600`).
  - Dequeue sweep capped by `PARTITIONED_DEQUEUE_SWEEP_CAP: u32 = 8192` (`sysdb/mod.rs:42`).
- Terminology trap: "streams" in the oracle means the workflow-streams feature
  (`write_stream` at `:4390`, `read_stream_value` with `STREAM_OFFSET_ATTEMPTS = 16` offset
  races at `:4426`), not result-set streaming. A "streaming port" faithful to v0.5.0 would
  port *pagination*, which is already ported.

## 2. Current Haskell behavior: faithful materialization

- `listWorkflowsSession :: WorkflowListParams -> Session [WorkflowRowRaw]`
  (`src/DBOS/Internal/DBOS/SystemDB/Postgres/Statements.hs:355-357`), decoder
  `Decoders.rowList workflowRowDecoder` (`:447-448`).
- `listStepsSession :: Text -> Bool -> Maybe Int64 -> Maybe Int64 -> Session [StepRowRaw]`
  (`Statements.hs:977-979`), decoder `Decoders.rowList (...)` (`:999-1010`).
- Class methods return lists: `listWorkflows :: ... -> m (Either Error [WorkflowRecord])`
  (`src/DBOS/Internal/DBOS/SystemDB/Class.hs:44`),
  `listSteps :: ... -> m (Either Error [StepRecord])` (`Class.hs:72`); same for
  `getAllNotifications`/`getAllEvents`/`getAllStreamEntries` (`Class.hs:77-81`).
- Execution is `runSession env operation session = withRetry ... (Pool.use env.psdbPool session)`
  (`src/DBOS/Internal/DBOS/SystemDB/Postgres/Backend.hs:1114-1120`); pool acquired in
  `acquirePostgresSystemDB` (`Backend.hs:1034-1049`). Haskell `Vec<T>`-as-`[T]` matches the
  oracle exactly. There is no memory-behavior divergence to fix.

## 3. hasql-cursor-query 0.4.5.4 (Hackage + linked source)

Exact API, verbatim from Haddock/source:

- `cursorQuery :: ByteString -> Params params -> ReducingDecoder result -> BatchSize -> CursorQuery params result`
- `reducingDecoder :: Row row -> Fold row reduction -> ReducingDecoder reduction`
  (`Fold` is `Control.Foldl`'s; `ReducingDecoder`'s `Applicative` pairs row decoders with a
  strict pair, so wide rows compose — no 16-column cap issue at this layer).
- `batchSize_10 | batchSize_100 | batchSize_1000 | batchSize_10000`.
- `Hasql.CursorQuery.Sessions.cursorQuery :: params -> CursorQuery params result -> Session result`
  — "During the execution it establishes a Read transaction with the ReadCommitted isolation
  level."
- `Hasql.CursorQuery.Transactions.cursorQuery :: params -> CursorQuery params result -> Transaction result`
  (for use inside existing `hasql-transaction` bodies); plus a `CursorTransaction` runner.
- Internals (`Private/CursorTransactions.hs`): `declareCursor` with the SQL template + encoded
  params, then `fetchAndFoldCursor` FETCHes batches until an empty batch (`D.null` check) and
  folds with the unwrapped `Fold step enter exit`. No explicit `CLOSE` is issued; the cursor
  dies with its transaction (Postgres non-`WITH HOLD` semantics), and the `Sessions` runner
  owns that transaction — lifecycle/finalizers come for free.
- Version/deps: 0.4.5.4 (uploaded 2026-08-06); `hasql >=1.10 && <1.11 || >=2.0 && <2.1`
  (compatible with this repo's `hasql >=1.10 && <1.11`), `foldl >1 && <2`,
  `hasql-cursor-transaction >=0.6 && <0.7`, `hasql-transaction >=1.1 && <1.3`,
  `base >=4.11 && <5` (repo builds on GHC 9.12.4 — in range).
- typedSql interop: none directly. `cursorQuery` needs the decomposed triple
  `(ByteString SQL, Params encoder, Row decoder)`; `[typedSql| |]` yields opaque
  `Session`/`Statement` values (`sqlQueryTypedSession`/`sqlQueryTypedStatement`). A cursor
  spec must hand-write the triple while reusing the SQL text (e.g. `listWorkflowsSql :: Text`
  at `Statements.hs:359-400`) with `Hasql.Encoders`/`Hasql.Decoders` — exactly the ADR-0011
  pattern already used for `Transaction` bodies and wide rows.

## 4. foldl 1.4.18 (Hackage)

- `data Fold a b = forall x. Fold (x -> a -> x) x (x -> b)` — strict left folds "that stream
  in constant memory", combined `Applicative`-style in one pass.
- `FoldM m a b` (`FoldM (x -> a -> m x) (m x) (x -> m b)`) for effectful accumulation;
  `generalize :: Monad m => Fold a b -> FoldM m a b`.
- Combinators that matter here: `length`/`genericLength`, `list`/`revList`, `vector`/`vectorM`,
  `mconcat`/`foldMap`, `mapM_`/`sink`, `premap`, `purely`/`impurely`.
- Footprint: base, bytestring, comonad, containers (<0.9), contravariant, hashable, primitive,
  profunctors, random, semigroupoids, semigroups, text (<2.2), transformers,
  unordered-containers, vector (<0.14). Moot in practice: `hasql-cursor-query` already depends
  on `foldl`, so it arrives transitively; depending on it directly only matters for writing
  custom `Fold`s.

## 5. Fit with repo constraints

- One-Rust-module-to-one-Haskell-module (`AGENTS.md`): `postgres.rs` maps to
  `Backend`+`Statements`, where `Statements.hs` is already declared "the port's own seam, not
  a Rust module counterpart" (`Statements.hs:8-12`). Cursor specs would live in `Statements.hs`
  beside the existing hand-written `Statement.preparable` sites — no new module needed, no ADR
  for the seam location.
- No new Haskell migrations (`docs/adr/0007-typed-sql-with-rust-migrations.md`, `Backend.hs:1030-1033`):
  satisfied trivially — `DECLARE`/`FETCH` need no schema.
- Class seam (`docs/adr/0009-systemdb-class-reader-backend.md` as amended; `Class.hs:26-40`):
  every method takes `db` first and returns `m (Either Error ...)` per Rule 3. A streaming
  method (e.g. `foldSteps :: db -> ... -> Fold StepRecord acc -> m (Either Error acc)`) changes
  the class shared with the `IOSim` backend — that *does* need an ADR, and per ADR-0020
  (`docs/adr/0016-dual-stack-testing.md:34-36`, `docs/adr/0020-...`) the sim backend must
  interpret the fold over its in-memory rows while engine code calls the same top-level
  function in both legs. Folding to `[T]` keeps the class unchanged but buys nothing over
  `rowList`.
- Retry/errors: `runSession`'s `withRetry` + `classifyUsageError` (`Backend.hs:1114-1120`)
  covers a cursor `Session` unchanged; a mid-fetch failure restarts the whole cursor read,
  matching Rust's per-method `with_retry` restart semantics. Decode failures abort mid-fold
  exactly as they abort `rowList` today.

## 6. Verdict and recommended shape

- **Verdict: do not port streaming.** There is no oracle streaming to be faithful to; adding
  cursors invents behavior the three-leg gate (ADR-0016: oracle leg `cargo test -p dbos`)
  cannot verify, and `foldl` without `hasql-cursor-query` changes nothing (rows already
  materialize). The only legitimate trigger is a demonstrated unbounded-result problem
  (e.g. limit-less `list_workflows` over a huge table), not parity.
- **If that trigger arrives**, the shape is:
  1. Hand-write `(sqlBytes, Params, Row)` triples in `Statements.hs`, reusing the existing SQL
     texts; build `reducingDecoder rowDecoder fold` with `foldl` folds.
  2. Run via `Sessions.cursorQuery` under `Pool.use` inside the existing `runSession`
     (keeps `withRetry` + `Error` channel); inside multi-statement methods use
     `Transactions.cursorQuery`.
  3. Add a `fold*` method to the `SystemDB` class + an `IOSim` interpretation, recorded in an
     ADR; mirror live/sim per ADR-0020.
- **Open questions**: (a) Which operation actually overflows — needs production evidence, none
  exists today. (b) Batch size choice (`batchSize_1000` is the plausible default; FETCH count
  vs row width tradeoff unmeasured). (c) Whether `Transactions.cursorQuery` nests correctly
  under this repo's `transactionNoRetry ReadCommitted Write` bodies — untested. (d) `ihp-typed-sql`
  1.5.0's Hackage build reports currently fail — irrelevant here since cursor specs bypass
  typedSql, but worth noting for the dependency story.

## Sources

- Oracle (read-only, `~/dev/dbos-transact-rust`, v0.5.0 semantics):
  `crates/dbos/src/sysdb/postgres.rs` — `list_workflows` :3017-3226, `list_workflow_steps`
  :4615-4659, `get_all_notifications` :4947-4980, `rename_application_in_batches` :2539-2640,
  `start_queued_workflows` :5408+, `write_stream` :4390, `STREAM_OFFSET_ATTEMPTS` :1635;
  `crates/dbos/src/sysdb/mod.rs:42` (`PARTITIONED_DEQUEUE_SWEEP_CAP`), `:763` (stream docs);
  `crates/dbos/Cargo.toml:43` (sqlx 0.9).
- Haskell: `src/DBOS/Internal/DBOS/SystemDB/Postgres/Statements.hs:344-448`
  (listWorkflows), `:977-1010` (listSteps), `:8-20` (seam/SQL preference rule);
  `src/DBOS/Internal/DBOS/SystemDB/Postgres/Backend.hs:1034-1049` (pool),
  `:1114-1120` (runSession), `:2367+` (`SystemDB` instance, e.g. `:2478`, `:2801`);
  `src/DBOS/Internal/DBOS/SystemDB/Class.hs:41-109`; `dbos-transact-hs.cabal` (hasql bounds);
  GHC 9.12.4 (`ghc --version`).
- Constraints: `AGENTS.md`; `docs/adr/0007-typed-sql-with-rust-migrations.md`;
  `docs/adr/0009-systemdb-class-reader-backend.md`; `docs/adr/0011-hasql-transaction-for-multi-statement-methods.md`;
  `docs/adr/0016-dual-stack-testing.md`; `docs/cross-language-schema-interop.md`.
- Libraries: https://hackage.haskell.org/package/hasql-cursor-query (0.4.5.4, deps incl.
  `hasql >=1.10 && <1.11 || >=2.0 && <2.1`); module docs
  `.../docs/Hasql-CursorQuery.html` (`cursorQuery`, `reducingDecoder`, `BatchSize`),
  `.../docs/Hasql-CursorQuery-Sessions.html` (Read-txn statement),
  `.../docs/Hasql-CursorQuery-Transactions.html`,
  `.../docs/Hasql-CursorQuery-CursorTransactions.html`; source
  `.../src/library/Hasql/CursorQuery/Private/CursorQuery.hs` (exact `CursorQuery`/`ReducingDecoder`
  definitions), `.../Private/Sessions.hs`, `.../Private/Transactions.hs`,
  `.../Private/CursorTransactions.hs` (`fetchAndFoldCursor` FETCH-until-empty loop);
  https://hackage.haskell.org/package/hasql-cursor-transaction (0.6.6.2, underlying
  `declareCursor`/`fetchBatch`); https://hackage.haskell.org/package/foldl (1.4.18,
  `Fold`/`FoldM`/combinator docs in `.../docs/Control-Foldl.html`);
  https://hackage.haskell.org/package/ihp-typed-sql (1.5.0, quasiquoter yields opaque sessions).
