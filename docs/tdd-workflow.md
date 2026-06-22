# TDD Workflow

This project uses vertical-slice TDD for DBOS Haskell. Tests describe behavior through public interfaces, not internal implementation details.

## Required Tool Loop

1. Start compiler feedback first:

   ```sh
   make dev
   ```

   `make dev` writes live compiler diagnostics to `.ghcid.txt`. During implementation, monitor this file continuously and fix compiler errors before spending time on the full test suite.

   If a dev environment is already running, use `tmux` to find the existing `make env` / `ghciwatch` pane and restart it there. Do not start duplicate compiler-watch loops in new panes.

2. Run the full Tasty suite after the compiler loop is green:

   ```sh
   cabal test
   ```

   Use `cabal test` as the integration check for the current vertical slice.

3. For DB-backed behavior, verify the live Postgres rows after `cabal test`.

   Use `psql` against the local `dbos` database and query the workflow, operation, and notification rows that the test slice should create or read. The current mirrored simple-workflow gate checks `dbos.workflow_status`, `dbos.operation_outputs`, and `dbos.notifications` for the `hs-*` fixture IDs documented in `AGENTS.md`.

4. Mirror from Python pytest only for the current minimum TDD slice. Do not port a whole pytest file in one pass. Pick the smallest Python behavior needed for the current Haskell public interface, write one Tasty test for it, implement it, then repeat.

## Package Rule

Before adding a new package dependency, search existing installed/package-environment capabilities from GHCi:

```sh
ghci -e ':hoogle <name-or-type>'
ghci -e ':browse <Module.Name>'
```

Adding an already-present hidden package to the Cabal file is fine when the package is already in the project environment or dependency set. New package additions require the GHCi search first and should be justified by a behavior in the current slice.

## Slice Rules

- Read `CONTEXT.md` and relevant ADRs before writing tests or code.
- Respect the current architectural decisions:
  - Python-migrated Postgres schema is the compatibility boundary.
  - DBOS transactions use `hasql`, `hasql-th`, and Postgres type codecs.
  - Haskell does not implement schema migrations.
  - DB rows and serialized values are parsed into domain ADTs at the boundary.
  - Effectful DBOS APIs use Bluefin 0.7 scoped capabilities with `IOE`; do not add DBOS-specific `Handle` records.
- One behavior test at a time.
- One minimal implementation at a time.
- Never refactor while red.
- Fix `.ghcid.txt` compiler errors before expanding tests.
- Run `cabal test` after each green compiler cycle.
- For DB-backed slices, run the `psql` row verification gate after `cabal test`.

## Test Shape

Tasty tests should verify behavior through the public Haskell interface for the slice. Prefer integration-style tests that exercise real row parsing, Bluefin-scoped capability APIs, and live-schema compatibility where that is the behavior under test.

Avoid tests that only assert implementation shape, private helper behavior, or internal call order. If a test would fail after an internal refactor while behavior remains correct, it is testing the wrong thing.

## Current Verified Slice

The committed implementation covers the first Python-compatible workflow execution, operation checkpoint, notification, and simple sync workflow replay slice:

1. Decode live Python-compatible workflow status strings into the Haskell `WorkflowStatus` ADT, rejecting unknown statuses.
2. Parse `workflow_status`, `operation_outputs`, and `notifications` rows into domain types at the boundary.
3. Replay operation checkpoints through `DBOS.Transact` Bluefin-scoped store capabilities.
4. Use `DBOS.SystemDB` Hasql functions to read/write the live Python DBOS tables used by the tests.
5. Verify the mirrored simple workflow with `cabal test` and the follow-up `psql` row gate.

## Per-Cycle Checklist

- The test describes one observable behavior.
- The test name uses glossary language.
- The test crosses the public interface planned for the slice.
- The implementation is the smallest code that can pass this test.
- `.ghcid.txt` is green before running or trusting `cabal test`.
- No new dependency was added without `ghci -e ':hoogle ...'` and `ghci -e ':browse ...'`.
