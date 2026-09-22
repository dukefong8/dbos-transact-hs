# AGENTS.md

Repo guide for DBOS Haskell.

## Structure

- `src/DBOS/Transact.hs` is the public DBOS transaction facade.
- `src/DBOS/<Domain>/*` holds internal parsers, replay logic, and types.
- `src/DBOS/Transact/Codec.hs` (Aeson JSON, mirrors Rust `serialization.rs`) and `src/DBOS/Transact/Log.hs` (explicit `LogAction`, no ambient logger) are plain-Haskell internals with no Bluefin imports.
- `src/DBOS/SystemDB/Postgres.hs` holds `[typedSql| ... |]` sessions over an explicit pool via `ihp-typed-sql` (the `hasql-th` dependency is removed).
- `test/DBOS/<DomainTest>.hs` holds Tasty specs.
- `rust-migrate/` is a standalone Cargo crate driving the public Rust migration runner (`make db-migrate`); it is not part of any workspace.
- `docs/` holds durable engineering notes, workflow guidance, research context, and ADRs.
- `docs/adr/` records architectural decisions.
- `.lavish/rust-port-plan.html` is the living port plan; fold each phase's delta back into it (self-recursive loop) and mark edits with dated notes.
- `CONTEXT.md` is the glossary for domain language only.

## TDD Loop

1. Run or watch `make dev` first.
2. Monitor `.ghcid.txt` and fix compiler errors before widening the slice.
3. Use `ghci -e ':hoogle ...'` and `ghci -e ':browse ...'` before adding any new dependency.
4. Write one public Tasty test at a time.
5. Implement the smallest code that passes that test.
6. Run `cabal test` after each green compiler cycle.
7. Tests share one live database and run in parallel (tasty default): every test must own its rows — fresh UUIDs, unique workflow/queue names, per-test executor ids. A sweep only ever matches its launching executor's id.

Use Neovim LSP document symbols to inspect module structure and exported surfaces before changing a module layout.

## End-to-End Verification

For DB-backed behavior, a green `cabal test` is not the final gate. After every successful `cabal test`, run a direct `psql` query against the local Postgres `dbos` database and verify the rows the tests are expected to create or read.

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

Then check the Rust oracle: `~/dev/dbos-transact-rust` is the behavioral reference. Run matching suites from that directory (e.g. `cargo test -p dbos --test recovery` for crash-and-resume, `--test queues` for the fan-out), read-only, pinned to v0.5.0 semantics — never copy code, never chase main. The starter acceptance mirror lives in `test/DBOS/StarterTest.hs`: steps-once, crash-and-resume, events, messages, queues, all as public-behavior tests against the live database.

The database must always be migrated with the Rust runner first: run `make db-migrate` (idempotent; verifies `108→108`) before build/test. The migration-ceiling test pins version 108; a higher value means the Rust corpus moved and pinned queries must be re-verified.

## Guardrails

- Keep tests on public behavior, not implementation details.
- Do not refactor while red.
- Do not add schema migrations in Haskell.
- Keep Python DBOS schema compatibility as the boundary.
- Two layers: plain-Haskell internals hold all logic; Bluefin 0.9 `Ask`/`IOE` capabilities live only at the external seam, never in internals. Do not add DBOS-specific `Handle` records.
- Use the existing tmux `make env` / `ghciwatch` pane; do not start duplicate watchers.

## Haskell Design Conventions (`~/dev/haskell-design-system`)

Soft conventions: they apply only where the Rust oracle and the plan rules (`.lavish/rust-port-plan.html` §6) are silent. The oracle wins on behavior; the plan wins on architecture.

- Two layers (ADR-0006): plain-Haskell internals (no Bluefin imports) hold all logic and tests; thin Bluefin capabilities live only at the external seam. Bluefin may depend inward, never outward.
- Errors (Rule 3): per-domain `Either` ADTs in the core (`CodecError`, `StepError`, `WorkflowRunError`); base async exceptions (`AsyncCancelled`) rethrown without recording at the edges. (`io-classes`/`io-sim` are unused deps; no unified `DbosError`, no `WorkflowCtx` record — the plan §6 records what was predicted vs built.)
- Logging (Rule 5): explicit `LogAction m DbosLogMsg`, never ambient; `co-log-core` + `fast-logger` stay, `co-log` message formatting is out.
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
