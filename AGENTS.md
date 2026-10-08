# AGENTS.md

Repo guide for DBOS Haskell.

## Structure

- `src/DBOS/Transact.hs` is the public DBOS transaction facade.
- `src/DBOS/<Domain>/*` holds internal parsers, replay logic, and types.
- `src/DBOS/Transact/Serialization.hs` (Aeson JSON, mirrors Rust `serialization.rs`) and `src/DBOS/Transact/Logger.hs` (explicit `SomeTracer` carrier, backends, and the app-facing `log*` seam, no ambient tracer) are plain-Haskell internals with no Bluefin imports.
- `src/DBOS/SystemDB/Postgres.hs` holds `[typedSql| ... |]` sessions over an explicit pool via `ihp-typed-sql` (the `hasql-th` dependency is removed).
- `test/DBOS/<DomainTest>.hs` holds Tasty specs.
- Sim trees live beside them: `test/DBOS/<Domain>Sim.hs` (eval-only mirrors over simulated data, never in `defaultMain`), `test/DBOS/IOSimTracer.hs` (the `simTracer` carrier — structured event plus its said line — `runSimCase`, stderr `printSimTrace`), `test/DBOS/SystemDB/IOSim.hs` (the `MockSystemDB`/`MemSystemDB` backends), and `test/DBOS/Transact/<Domain>SimData.hs` (per-domain mock constructors, dup'd across domains on purpose).
- `rust-migrate/` is a standalone Cargo crate driving the Rust migration runner (`make db-migrate`); it is not part of any workspace, and it vendors the corpus (`migrations/*.sql` + `src/migrations/{mod,runner}.rs`, copied from `dbos-transact-rust/crates/dbos`) so the only path deps are its own files.
- `docs/` holds durable engineering notes, workflow guidance, research context, and ADRs (`cross-language-schema-interop.md` = the shared `dbos` schema contract).
- `docs/adr/` records architectural decisions.
- `demo-apps/` holds the two worked examples (starter, widget store) and `Demo.Http`; their code panels are compile-time source extractions (`Starter.CodePanel.panel` with `addDependentFile`, tag needles in `Starter/Assets.hs`) — a panel always shows the real `.hs` source, never a hand-written copy, and a rename must update the tag needles in the same sweep.
- `.lavish/rust-port-plan.html` is the living port plan; fold each phase's delta back into it (self-recursive loop) and mark edits with dated notes. (The former `probes/` compile-probe gate was removed 2026-10-06; its policy history lives in `docs/invariant-gates.md`.)
- `CONTEXT.md` is the glossary for domain language only.
- `.agents/skills/dbos-transact-hs/SKILL.md` is the client-app skill: how to build an application on the `DBOS.Transact` facade (registration, step bodies, transactional steps, testing). Port-development rules live here; that skill serves app authors.

## HARD RULES — Rust fidelity (module + type one-to-one)

- **One Rust module maps to exactly one Haskell module.** `sysdb/types.rs` → `DBOS.SystemDB.Types`, `sysdb/error.rs` → `DBOS.SystemDB.Error`, `sysdb/retry.rs` → `DBOS.SystemDB.Retry`. Do NOT split a Rust module across several Haskell modules.
- **One Rust type maps to exactly one Haskell type, by name.** Types, constructors, and fields keep the Rust DBOS domain name verbatim, spelled per Haskell convention (PascalCase for types and constructors, camelCase for fields and values) — no `SystemDb` prefixes, no renames for taste, no field splitting. Collisions with existing names are resolved as documented deviations (constructor prefixes), not by renaming the ported type. Snake_case spellings carried over from Rust (`workflow_id`, `worker_concurrency`) are the deviation this replaces; the public-field camelCase sweep records the conversion per slice.
- **A split or a rename requires an explicit ADR** in `docs/adr/` recording why it was unavoidable. (No module split is in force today: the short-lived `DBOS.SystemDB.Time` leaf was merged back into `Types` once the cycle it broke was removed. Constructor-prefix collisions like `ForkStep` and `ErrorMaxRecoveryAttemptsExceeded` are the deviation style instead.)
- Facades (`DBOS.SystemDB`, `DBOS.Transact`) and `[typedSql| ... |]` session modules are the port's own seams, not Rust module counterparts; they may re-export (`module Types`) but never redefine ported types.
- **Keep the established `SystemDB` spelling in Haskell module, type, and class names.** Use `DBOS.SystemDB.*`, `SystemDB`, and `PostgresSystemDB` — never `SystemDatabase` — for Haskell artifacts. References to Rust's `trait SystemDatabase` keep the Rust spelling.

## Naming and Refactor Conventions

- **Sweeps are word-boundary, counted, and verified.** Replace `\bname\b` per file, print per-file counts, and re-grep the whole tree for leftovers before building. Substring traps met in practice: `runTransactionOutside`/`runTransactionAt` are different functions (system-DB internals, never swept with the step API), `forkWorkflowStepName` parses as `<op>Workflow` + `StepName` rather than the runner family, and `WorkflowStep` also hides inside `nextWorkflowStepId` and `listWorkflowSteps`.
- **Identifier sweeps never touch string literals.** Wire names are data, not names: the `WorkflowStep` → `Step` sweep silently corrupted `listStepsStepName = "DBOS.listSteps"` where the oracle constant is `LIST_WORKFLOW_STEPS = "DBOS.listWorkflowSteps"`. After any sweep, `grep -rn '"DBOS\.'` and compare against `sysdb/types.rs`.
- **Approved names and argument order**: `Tx` abbreviates `Transaction` in the transactional-step API — `runTxStep`, `runTxOutside`, `Tx`, `txName`, `txIsolation`; runners take the workflow context last (`runTxStep ds txConfig wctx body`, `runTxOutside ds txConfig body`). The step family is `runStep`/`runStepWith`/`pendingStep`/`pendingStepWith`/`sleepStep`/`runNestedStep`; engine internals are `driveStepWith`/`replayStep`.
- **Event ADTs stay full-word per owner** (`WorkflowEvent` with `Step*`, `TransactionEvent` with `Transaction*`): `renderLine` renders the constructor via `show`, so renaming one changes observable log `kind`s. Update living docs; leave dated captured traces as evidence.
- **Facade discipline**: apps, tests, and demos import through `DBOS.Transact` only, and every name imported into the facade must stay exported — a trimmed export block breaks consumers, and GHCi hides it until `cabal build all` / `cabal test all`. When facade and internal names disagree, the facade defines the client surface.
- **Post-sweep checks**: `cabal build all` does not compile test suites — finish with `cabal test all` (or watch the enabled pair) before calling a sweep done. A test-local helper that collides with a newly public runner (`runStep`) silently shadows the import and can become self-recursive; qualify the library call (`import DBOS.Transact qualified as Transact`) or rename the helper.
- **Editor coordination**: a save from an editor buffer held open across agent edits reverts sweeps; after the agent edits an open file, reload (`:e!`) before saving. When both sides need the same file, agree on the owner first.

## TDD Loop

1. Run or watch `make dev` first — it is the single watcher and it owns `ghcid.txt`.
2. **MUST: after every reload, read `ghcid.txt` immediately — never sleep more than 5 seconds first.** `ghciwatch` reloads in a few seconds and rewrites `ghcid.txt` with that reload's whole result: compile errors and warnings, `All good (N modules)`, and the tasty eval output (progress lines and results — sim announcement traces print to the watcher's stderr via `printSimTrace`, so read the tmux pane, not the file, for those). While a reload is in flight the file still holds the previous reload, so re-read until the content changes; long waits hide both the compiler error and the tasty result (`tail ghcid.txt` is enough). Do not pipe, redirect, or `tee` the watcher — ghciwatch owns the file and a second writer corrupts it. Read the file, not the tmux pane.
   - Sim trees that `printSimTrace` must use `dependentTestGroup ... AllFinish`: tasty runs cases in parallel by default and parallel stderr writers interleave mid-character on the pane; `AllFinish` keeps every case running on a failure, in order.
3. **MUST: exactly one IO/Sim pair is enabled before and during every edit**, via the `-- $>` / `--- $>` toggles in `test/Main.hs` (paired alias form, e.g. `-- $> tasty WorkflowTest.tests` + `-- $> tasty WorkflowSim.tests`) — a reload that runs no tests verifies nothing, and the pair diff is compared every reload. The watcher's eval must never overlap the live-DB suite: two suite binaries deadlock on the shared fixture rows (reproduced `40P01`).
4. Use `ghci -e ':hoogle ...'` and `ghci -e ':browse ...'` before adding any new dependency.
5. Write one public Tasty test at a time.
6. Implement the smallest code that passes that test.
7. Run `cabal test all` only when the watcher is idle — the full suite and a watcher eval must not run at the same time. `cabal.project` pins this package to `test-options: --num-threads 1` (the default parallel run stalls against the shared database) and disables `ihp-typed-sql`'s tests (its AUTO_DB spec needs a local `postgres` server keg; a libpq-only install cannot run it). Tasty flags on the `cabal` command line are unusable — they also reach the dependencies' hspec suites and fail them.
8. Tests share one live database and run in parallel (tasty default): every test must own its rows — fresh UUIDs, unique workflow/queue names, per-test executor ids. A sweep only ever matches its launching executor's id.

Use Neovim LSP document symbols to inspect module structure and exported surfaces before changing a module layout.

## End-to-End Verification

For DB-backed behavior, a green `cabal test` is not the final gate. After every successful `cabal test`, run a direct `psql` query and verify the rows the tests are expected to create or read. **Query the database the tests actually used: `$DBOS_DATABASE_URL`** (the environment's value decides — the local `dbos` database is only the default).

```sql
select 'workflow_status' as table_name, workflow_uuid as id, status, name
from dbos.workflow_status
where workflow_uuid in ('hs-wf-1', 'hs-op-wf', 'hs-notification-wf', 'hs-simple-wf')
union all
select 'operation_outputs', workflow_uuid || ':' || function_id::text, null, function_name
from dbos.operation_outputs
where workflow_uuid in ('hs-op-wf', 'hs-simple-wf')
union all
select 'notifications', message_uuid, null, topic
from dbos.notifications
where message_uuid = 'hs-message-1'
order by table_name, id;
```

Expected rows include the mirrored simple workflow status row (`hs-simple-wf`, `SUCCESS`, `TryConcExec.testConcWorkflow`) and its operation output row (`hs-simple-wf:1`, `TryConcExec.testConcStep`). Adjust the IDs only when the test fixture intentionally changes.

Then check the Rust oracle: `~/dev/dbos-transact-rust` is the behavioral reference. Run matching suites from that directory (e.g. `cargo test -p dbos --test recovery` for crash-and-resume, `--test queues` for the fan-out), read-only, pinned to v0.5.0 semantics — never copy code, never chase main.

For the demos, restart the server after every rebuild (`PORT=8090 cabal run exe:demo-apps`) and stop it before any `cabal test` (its queue supervisor claims test fixtures). Drive the browser with `chrome-devtools-axi open/snapshot`; the storefront auto-refreshes and stale refs are rejected, so click by text via `eval` (`[...document.querySelectorAll('button')].find(...)`). Verify in psql: `widget_store.orders` (`order_status`, `progress_remaining`), `widget_store.transaction_completion` (transactional-step rows), `dbos.operation_outputs` (sleeps/steps), and `dbos.workflow_status` (`CheckoutWorkflow`, `DispatchOrderWorkflow`).

## Three-Leg Port Gate (ADR-0016)

When porting each module, all three legs must pass: (1) watcher eval on the domain's `*Sim.tests` — green with announcement lines inline; (2) `cabal test test --test-option='--pattern' --test-option='$2 == "<Group>"'` — green with matching FastLogger lines on stdout (tasty `$n` fields are 1-indexed; `$0 ~ /.../` does not parse); (3) `cargo test -p dbos --test <suite>` read-only — green, plus a structural trace comparison against the oracle's `tracing::info!` call sites (Rust integration tests install no collector, so the comparison is format-string-structural, cited by file and line). Live trees (`*Test`) ship in `defaultMain`; sim trees (`*Sim`) are eval-only — never add a `*Sim.tests` to `main`.

The database must always be migrated with the Rust runner first: run `make db-migrate` (idempotent; verifies `114→114`) before build/test. The migration-ceiling test pins version 114; a higher value means the Rust corpus moved and pinned queries must be re-verified.

## Guardrails

- **Commits wait for the user's `/review`** (standing rule): do not commit unprompted; a slice lands only when the user invokes `/review` with the gates green.
- **Database URLs: `DBOS_DATABASE_URL` is the system database, `DATABASE_URL` is the application's own datasource.** The two are read separately (`DBOS.SystemDB.Postgres.configFromEnv` vs `DBOS.Transact.Config.appDatabaseUrlFromEnv`); a single-database deployment sets both to the same URL, and the app pool falls back to the system URL when `DATABASE_URL` is unset.
- **MUST: NEVER store e2e or other ad-hoc harness scripts in the project folder.** Throwaway runners, crash/restart loops, Chrome/E2E drivers, live side-by-side comparison scripts, fuzz drivers, and snoop harnesses live outside the repo. Keep them under the operator's own scratch space (for example `~/.local/share/…` or a `scratch/` directory outside the workspace) and check them in only when a script is a durable, reviewed part of the build or test story (e.g. `Makefile` targets and the in-repo test suite). A `.sh`/`.py` file that exists only to poke a running demo or drive a one-off investigation does not belong in the tree.
- Keep tests on public behavior, not implementation details.
- Do not refactor while red.
- Do not add schema migrations in Haskell.
- Keep Python DBOS schema compatibility as the boundary.
- Two layers: plain-Haskell internals hold all logic; Bluefin 0.9 `Ask`/`IOE` capabilities live only at the external seam, never in internals. Do not add DBOS-specific `Handle` records.
- Use the existing tmux `make env` / `ghciwatch` pane; do not start duplicate watchers. `ghciwatch` owns `ghcid.txt`; never add a `tee` or redirect to it.

## Haskell Design Conventions (`~/dev/haskell-design-system`)

Soft conventions: they apply only where the Rust oracle and the plan rules (`.lavish/rust-port-plan.html` §6) are silent. The oracle wins on behavior; the plan wins on architecture.

- Two layers (ADR-0006): plain-Haskell internals (no Bluefin imports) hold all logic and tests; thin Bluefin capabilities live only at the external seam. Bluefin may depend inward, never outward.
- Errors (Rule 3): per-domain `Either` ADTs in the core (`CodecError`, `StepError`, `WorkflowRunError`, the shared `DBOS.SystemDB.Error`); base async exceptions (`AsyncCancelled`) rethrown without recording at the edges. Impure internals constrain effects with `io-classes` where timing must be simulated (ADR-0008: `MonadDelay` in `DBOS.SystemDB.Retry`, IO in production, IOSim in tests); no unified `DbosError`, no `WorkflowCtx` record — the plan §6 records what was predicted vs built.
- Base funnel: `DBOS.Prelude` re-exports base through short aliases (`import Control.Monad as Monad`, `import Data.Maybe as Base`, alongside the io-classes aliases) — `import DBOS.Prelude` is the only base import a module needs. Never import a funneled base module per-file; when new base names are needed, extend the Prelude with another alias re-export instead. Always prefer io-classes over the `async`/`stm`/exception packages — no `Control.Concurrent.Async`, `Control.Concurrent.STM`, or `Control.Exception` imports; the single sanctioned exception is `AsyncException` identity in `Workflow.hs` (the abort channel matches `ThreadKilled`, which io-classes 1.11 does not expose — noted at the import). Demo apps are client code on the facade and keep explicit `Prelude`, outside the funnel.
- Tracing (Rule 5): explicit `SomeTracer m` (contra-tracer GADT, universal over event types), never ambient; per-domain event ADTs homed with their owners (`EngineEvent` in `Recovery`, `SysdbEvent` in `Retry`, `WorkflowEvent` in `Step`, `QueueEvent` in `Dequeue`, `ManagementEvent` in `Management`) with `LogEvent`+`ToLogStr`; a line is severity + constructor name + prose (`renderLine`, the name from `show`), and the IO backend prefixes FastLogger's time and the emitting `ThreadId` (pre-formatted once per thread in a capped cache, `LoggerBackend`'s second field) and renders only events at or above its `TRACE_LEVEL` floor (read once at acquisition; below-floor events are dropped before formatting); emission only through `runTracer`; `showText` in the Prelude. FastLogger Rank-N backend on IO (stderr — stdout carries results), `traceM` on IOSim (the sim carrier also says each rendered line, for `printSimTrace`); sim trees print via the test-owned carrier (`printSimTrace` to pane stderr, never into `ghcid.txt`); co-log is out (ADR-0015).
- Sim mirrors (ADR-0020): a `*Sim` case must drive the same engine functions as its live half — only the backend and the scheduler/clock may differ. Staged effects, re-encoded call sequences, hand-emitted events, and test-side `forkIO`/`killThread`/poll stand-ins are defects; cases the simulator cannot run (preemption-dependent) are marked IO-only with an `-- IO only:` comment above the case (plain name; the reason also lives in ADR-0020's running list), never dropped. Build plan: `docs/dual-stack-concurrency-todo.md`.
- Deriving: every clause carries an explicit `stock`/`newtype` strategy.
- Records (`NoFieldSelectors` + `OverloadedRecordDot`, both in cabal `default-extensions`).
  Field access, in this order — stop at the first that fits:
  1. **Record dot** — the default. Requires the field in scope (import it,
     e.g. `WorkflowCtx (wctxConn)`) and a record type concrete enough for
     `HasField`; it works fine under the rank-2 `forall exec.` binders, but
     not for a field whose *own* type is rank-n (dot would need impredicative
     instantiation):
     `first.wctxState.executionIdentity == second.wctxState.executionIdentity` (`isSameExecution`),
     `wctx.wctxConn.connInstanceId` (`Checkpoint`).
     Dot sections lift reads: `(.rowWorkflowInputs) =<< fetched`,
     `maybe "null" (.serializedText) input`.
  2. **Plain record pattern** — for rank-n fields, or when destructuring once
     and using several fields:
     `let DataSource {dsWithTransaction = withTx} = ds` (`Datasource`),
     `Right (Just WorkflowRecord {workflowRecordStatus = status}) -> ...`.
     Rank-n fields only expose through patterns:
     `spawnLocal (TaskSpawner spawn) = spawn`, `let ErasedWorkflow body = workflow`.
     Newtype/sum destructuring likewise: `let WorkflowId destination = ...`,
     `case ... of Just (WorkflowSucceeded stored) -> ...`.
     RecordWildCards destructures a whole record (`InitWorkflowParams {..} = params`,
     `Statements.hs`) and can rebuild it in one spread — override one field, the
     rest come from the in-scope bind: `let MkA {..} = myA in MkA {c = 13, ..}`.
     It is a cabal default (and a `.ghci` `:set`, like the other record
     extensions); the spread reads plain in-scope variables, so it sidesteps
     duplicate-field ambiguity rather than resolving it.
  3. **Record pattern with a type annotation or TypeApplication** — when the
     type cannot be inferred at the pattern. GHC 9.12 rejects a type
     application on a record constructor (`R @Int{f = 1}` is a parse error), so
     annotate the scrutinee or the field instead:
     `case (found :: Maybe WorkflowRecord) of ...`,
     `(R {f = x :: Int})`, `Right (numbers :: [Int]) -> ...` (`WorkflowTest`).
     An annotation can also pin an *update's* target:
     `(myA :: A) {c = 13}` — but on a field name shared under
     `DuplicateRecordFields` this is the type-directed disambiguation GHC
     deprecates (`-Wambiguous-fields` fires by default). The warning is a
     future-GHC deprecation, not an error: when the type-directed
     disambiguation is intended, it is safe to silence per file with
     `{-# OPTIONS_GHC -Wno-ambiguous-fields #-}`; otherwise prefer the
     rule-2 spread or the rule-4 qualified field.
     TypeApplications are only for non-record constructors: `go (Just @Int x) = ...`.
  4. **Module-qualified constructor (and qualified fields in construction, update,
     and patterns)** — last resort for duplicate-field ambiguity:
     `SystemDBError.WorkflowCancelled {workflowId = workflowId wctx'}` and
     `TransactError.StepTimeout {step = name, timeout = limit}` (`Step.hs`),
     `Types.QueueRecord {Types.queueRecordName = name}`; qualified fields also
     update and match (`myA {M.c = 13}` / `MkA {M.c = x}`) and are the fix
     `-Wambiguous-fields` names. The records need not live in separate
     modules: one module may declare several whose field names collide
     (DuplicateRecordFields permits it), and the consumer imports that one
     module once per type under an alias —
     `import qualified Mod as Foo (Foo (..))` /
     `import qualified Mod as Bar (Bar (..))` (or post-qualified:
     `import Mod qualified as Foo (Foo (..))`) — then names fields per type:
     `Foo.Foo {Foo.x = 1, Foo.y = 2}`, `b {Bar.x = 9}`, `Bar.Bar {Bar.x = v}`.
     Dot reads cannot be
     qualified (`r.M.f` is a parse error), so a qualified read goes through
     2 or 3, never a dot chain.
- DO update and construct with record syntax: `row { rowWorkflowId = wid }`, `SerializedWorkflowValue { serializedText = t, ... }`.
- DO add a per-file `{-# LANGUAGE OverloadedRecordDot #-}` wherever dot syntax is used (ghci loads don't inherit cabal defaults; `.ghci` keeps it `:seti`).
- DON'T call bare field selectors as functions (`rowWorkflowId row`) — they don't exist under `NoFieldSelectors`.
- DON'T reach for optics (`optics`/`aeson-optics`/`optics-th`) for reads or simple updates — optics is reserved for deep nested updates only. No such case exists, so the deps stay out (`aeson-optics` is additionally unusable: capped at `base<4.20`, incompatible with GHC 9.12).
- `.ghci` discipline: `:set` iff cabal enables it, else `:seti` (a `:set -XNoFieldSelectors` once broke every ghci load while cabal stayed green).
- Exports: explicit export lists, grouped by concept (see `DBOS.Transact`).
- Typeclasses: concrete modules now; a second real backend earns the Port pattern, test fakes use records-of-functions.
- Recorded deviations: member-import style is kept (not qualified-everything); ported sum-constructor names stand as ported from Rust (constructor-suffix rule ignored).
