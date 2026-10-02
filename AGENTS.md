# AGENTS.md

Repo guide for DBOS Haskell.

## Structure

- `src/DBOS/Transact.hs` is the public DBOS transaction facade.
- `src/DBOS/<Domain>/*` holds internal parsers, replay logic, and types.
- `src/DBOS/Transact/Serialization.hs` (Aeson JSON, mirrors Rust `serialization.rs`) and `src/DBOS/Tracer.hs` (explicit `SomeTracer`, no ambient tracer) are plain-Haskell internals with no Bluefin imports.
- `src/DBOS/SystemDB/Postgres.hs` holds `[typedSql| ... |]` sessions over an explicit pool via `ihp-typed-sql` (the `hasql-th` dependency is removed).
- `test/DBOS/<DomainTest>.hs` holds Tasty specs.
- Sim trees live beside them: `test/DBOS/<Domain>Sim.hs` (eval-only mirrors over simulated data, never in `defaultMain`), `test/DBOS/IOSimTracer.hs` (the `simTracer` carrier — structured event plus its said line — `runSimCase`, stderr `printSimTrace`), `test/DBOS/SystemDB/IOSim.hs` (the `MockSystemDB`/`MemSystemDB` backends), and `test/DBOS/Transact/<Domain>SimData.hs` (per-domain mock constructors, dup'd across domains on purpose).
- `rust-migrate/` is a standalone Cargo crate driving the public Rust migration runner (`make db-migrate`); it is not part of any workspace.
- `docs/` holds durable engineering notes, workflow guidance, research context, and ADRs.
- `docs/adr/` records architectural decisions.
- `.lavish/rust-port-plan.html` is the living port plan; fold each phase's delta back into it (self-recursive loop) and mark edits with dated notes.
- `CONTEXT.md` is the glossary for domain language only.

## HARD RULES — Rust fidelity (module + type one-to-one)

- **One Rust module maps to exactly one Haskell module.** `sysdb/types.rs` → `DBOS.SystemDB.Types`, `sysdb/error.rs` → `DBOS.SystemDB.Error`, `sysdb/retry.rs` → `DBOS.SystemDB.Retry`. Do NOT split a Rust module across several Haskell modules.
- **One Rust type maps to exactly one Haskell type, by name.** Types, constructors, and fields keep the Rust spelling verbatim — no `SystemDb` prefixes, no renames for taste, no field splitting. Collisions with existing names are resolved as documented deviations (constructor prefixes), not by renaming the ported type.
- **A split or a rename requires an explicit ADR** in `docs/adr/` recording why it was unavoidable. (No module split is in force today: the short-lived `DBOS.SystemDB.Time` leaf was merged back into `Types` once the cycle it broke was removed. Constructor-prefix collisions like `ForkStep` and `ErrorMaxRecoveryAttemptsExceeded` are the deviation style instead.)
- Facades (`DBOS.SystemDB`, `DBOS.Transact`) and `[typedSql| ... |]` session modules are the port's own seams, not Rust module counterparts; they may re-export (`module Types`) but never redefine ported types.
- **Keep the established `SystemDB` spelling in Haskell module, type, and class names.** Use `DBOS.SystemDB.*`, `SystemDB`, and `PostgresSystemDB` — never `SystemDatabase` — for Haskell artifacts. References to Rust's `trait SystemDatabase` keep the Rust spelling.

## TDD Loop

1. Run or watch `make dev` first — it is the single watcher and it owns `ghcid.txt`.
2. **MUST: after every reload, read `ghcid.txt` immediately — never sleep more than 5 seconds first.** `ghciwatch` reloads in a few seconds and rewrites `ghcid.txt` with that reload's whole result: compile errors and warnings, `All good (N modules)`, and the tasty eval output (progress lines and results — sim announcement traces print to the watcher's stderr via `printSimTrace`, so read the tmux pane, not the file, for those). While a reload is in flight the file still holds the previous reload, so re-read until the content changes; long waits hide both the compiler error and the tasty result (`tail ghcid.txt` is enough). Do not pipe, redirect, or `tee` the watcher — ghciwatch owns the file and a second writer corrupts it. Read the file, not the tmux pane.
   - Sim trees that `printSimTrace` must use `dependentTestGroup ... AllFinish`: tasty runs cases in parallel by default and parallel stderr writers interleave mid-character on the pane; `AllFinish` keeps every case running on a failure, in order.
3. **MUST: exactly one IO/Sim pair is enabled before and during every edit**, via the `-- $>` / `--- $>` toggles in `test/Main.hs` (paired alias form, e.g. `-- $> tasty WorkflowTest.tests` + `-- $> tasty WorkflowSim.tests`) — a reload that runs no tests verifies nothing, and the pair diff is compared every reload. The watcher's eval must never overlap the live-DB suite: two suite binaries deadlock on the shared fixture rows (reproduced `40P01`).
4. Use `ghci -e ':hoogle ...'` and `ghci -e ':browse ...'` before adding any new dependency.
5. Write one public Tasty test at a time.
6. Implement the smallest code that passes that test.
7. Run `cabal test` only when the watcher is idle — the full suite and a watcher eval must not run at the same time.
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

## Three-Leg Port Gate (ADR-0016)

When porting each module, all three legs must pass: (1) watcher eval on the domain's `*Sim.tests` — green with announcement lines inline; (2) `cabal test test --test-option='--pattern' --test-option='$2 == "<Group>"'` — green with matching FastLogger lines on stdout (tasty `$n` fields are 1-indexed; `$0 ~ /.../` does not parse); (3) `cargo test -p dbos --test <suite>` read-only — green, plus a structural trace comparison against the oracle's `tracing::info!` call sites (Rust integration tests install no collector, so the comparison is format-string-structural, cited by file and line). Live trees (`*Test`) ship in `defaultMain`; sim trees (`*Sim`) are eval-only — never add a `*Sim.tests` to `main`.

The database must always be migrated with the Rust runner first: run `make db-migrate` (idempotent; verifies `108→108`) before build/test. The migration-ceiling test pins version 108; a higher value means the Rust corpus moved and pinned queries must be re-verified.

## Guardrails

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
- Tracing (Rule 5): explicit `SomeTracer m` (contra-tracer GADT, universal over event types), never ambient; per-domain event ADTs homed with their owners (`EngineEvent` in `Recovery`, `SysdbEvent` in `Retry`, `WorkflowEvent` in `Step`, `QueueEvent` in `Dequeue`, `ManagementEvent` in `Management`) with `LogEvent`+`ToLogStr`; a line is severity + constructor name + prose (`renderLine`, the name from `show`), and the IO backend prefixes FastLogger's time and the emitting `ThreadId` (pre-formatted once per thread in a capped cache, `LoggerBackend`'s second field) and renders only events at or above its `TRACE_LEVEL` floor (read once at acquisition; below-floor events are dropped before formatting); emission only through `runTracer`; `showText` in the Prelude. FastLogger Rank-N backend on IO (stderr — stdout carries results), `traceM` on IOSim (the sim carrier also says each rendered line, for `printSimTrace`); sim trees print via the test-owned carrier (`printSimTrace` to pane stderr, never into `ghcid.txt`); co-log is out (ADR-0015).
- Sim mirrors (ADR-0020): a `*Sim` case must drive the same engine functions as its live half — only the backend and the scheduler/clock may differ. Staged effects, re-encoded call sequences, hand-emitted events, and test-side `forkIO`/`killThread`/poll stand-ins are defects; cases the simulator cannot run (preemption-dependent) are marked IO-only with an `-- IO only:` comment above the case (plain name; the reason also lives in ADR-0020's running list), never dropped. Build plan: `docs/dual-stack-concurrency-todo.md`.
- Deriving: every clause carries an explicit `stock`/`newtype` strategy.
- Records (`NoFieldSelectors` + `OverloadedRecordDot`, both in cabal `default-extensions`):
  - DO read with record-dot: `row.rowWorkflowStatus`, `message.logMessage`.
  - DO lift reads with dot sections: `(.rowWorkflowInputs) =<< fetched`, `maybe "null" (.serializedText) input`.
  - DO update and construct with record syntax: `row { rowWorkflowId = wid }`, `SerializedWorkflowValue { serializedText = t, ... }`.
  - DO destructure with patterns: `let WorkflowId destination = ...`, `case ... of Just (WorkflowSucceeded stored) -> ...`.
  - DO add a per-file `{-# LANGUAGE OverloadedRecordDot #-}` wherever dot syntax is used (ghci loads don't inherit cabal defaults; `.ghci` keeps it `:seti`).
  - DON'T call bare field selectors as functions (`rowWorkflowId row`) — they don't exist under `NoFieldSelectors`.
  - DON'T reach for optics (`optics`/`aeson-optics`/`optics-th`) for reads or simple updates — optics is reserved for deep nested updates only. No such case exists, so the deps stay out (`aeson-optics` is additionally unusable: capped at `base<4.20`, incompatible with GHC 9.12).
- `.ghci` discipline: `:set` iff cabal enables it, else `:seti` (a `:set -XNoFieldSelectors` once broke every ghci load while cabal stayed green).
- Exports: explicit export lists, grouped by concept (see `DBOS.Transact`).
- Typeclasses: concrete modules now; a second real backend earns the Port pattern, test fakes use records-of-functions.
- Recorded deviations: member-import style is kept (not qualified-everything); ported sum-constructor names stand as ported from Rust (constructor-suffix rule ignored).
