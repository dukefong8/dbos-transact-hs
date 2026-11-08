---
name: verify
description: Drive dbos-transact-hs end to end — standalone suite binary against live Postgres plus Postgres state proof. Use to prove behavior of the Haskell DBOS port, its slices, or library changes before review.
---

# Verify dbos-transact-hs

This skill proves library behavior: the full test suite (live Postgres +
sim mirrors) via a standalone suite binary, with durable state read back
independently. Follow it literally; a proof that drives an internal setter
instead of the suite plus a Postgres read-back is not a proof.

Key facts (all observed in-repo, Oct 2026):

- System database URL is `$DBOS_SYSTEM_DATABASE_URL`
  (`postgresql://postgres:pgpasswd@127.0.0.1:5432/postgres` here).
  App datasource falls back to it when `APP_DATABASE_URL` is unset.
  `DATABASE_URL` is **not** ours: `ihp-typed-sql` reads it at compile time
  (the `typedSql` describe in `src/` needs it set when building).
- Migrations run through the Rust runner: `make db-migrate` (idempotent;
  record the `from→to` versions). The authority on the expected version is
  the migration-ceiling probe in `test/DBOS/SystemDB/PostgresTest.hs`
  (currently `version >= 121`). Never add schema migrations in Haskell.
- `make widget-db` bootstraps the `widget_store` schema, which the
  `dbos-demos` package needs at compile time for `typedSql` describe —
  run it before building, even though no demo is driven.
- `cabal build` / `cabal test` resolve since the hasql-2 migration
  (main package and `dbos-demos` both pin `hasql >=2.0.1 && <2.1`,
  matching sibling `ihp-typed-sql`). Verification usually builds the
  suite with standalone `ghc --make` (exact command in Launch) and runs
  the binary directly — the faster loop for `--pattern` selection.
  Never "fix" version pins during a verification run — record it, don't
  paper over it.
- The watcher (`make dev`) runs sim tests only and never touches the live
  database, so a suite-binary run composes freely alongside it — no
  idling, no fixture deadlock.
- **Never store harness scripts in the repo** (standing `AGENTS.md`
  guardrail). Drive with inline commands only. Evidence goes to
  `/tmp/dbos-verify-<RUN_ID>/`, never the tree.

## Launch

Reach a usable state from an empty checkout. Every step is required, in order:

1. `make db-migrate` — migrates the system database with the Rust runner
   (idempotent; record the `from→to` versions it reports). The authority
   on the expected version is the migration-ceiling probe in
   `test/DBOS/SystemDB/PostgresTest.hs` (currently `version >= 121`); if
   the runner reports a version below that floor, stop and report — the
   Rust corpus moved or the wrong database was migrated.
2. `make widget-db` — bootstraps the `widget_store` app schema for the
   compile-time `typedSql` describe. Uses `$DATABASE_URL` falling back to
   `$DBOS_SYSTEM_DATABASE_URL`.
3. Build the suite binary (standalone, since `cabal build` does not
   resolve — see Key facts). `$DBOS_SYSTEM_DATABASE_URL` (and `DATABASE_URL`)
   must be set for the `typedSql` describe:

   ```bash
   ghc --make -outputdir /tmp/dbuild -isrc:test:src/DBOS/Internal \
     -XGHC2024 -XDuplicateRecordFields -XNoFieldSelectors \
     -XNoImplicitPrelude -XOverloadedLabels -XOverloadedRecordDot \
     -XRecordWildCards -threaded -rtsopts test/Main.hs -o /tmp/dbos-test
   ```

   A non-zero exit stops the run — read the output, never proceed to a
   test run on a failed build.
4. Run the full suite and save the output:

   ```bash
   /usr/bin/time -l /tmp/dbos-test +RTS -M4G -RTS > /tmp/dbos-verify-<RUN_ID>/suite.log 2>&1
   ```

   Expect `All N tests passed` (862/862 at ~160MB peak as of Oct 2026).
   The binary runs the `tests` tree: live backends plus the DB-free
   `SystemDBIOSim`/`CheckpointSim` groups. The dual-stack `*Sim` mirrors
   are watcher-only by design (see the `simTests` list in `test/Main.hs`)
   and were covered by the sim eval — do not expect them in this count.
   Unlike `cabal`, tasty flags *are* allowed on the binary (e.g.
   `--pattern '$2 == "Workflow registry"' selects one group) because no
   dependency suites are in the process.

Do not start a second watcher (`make dev` / `ghciwatch` owns `ghcid.txt`;
never `tee` into it). Build artifacts live under `/tmp` (`/tmp/dbuild`,
`/tmp/dbos-test`) so the tree stays clean; remove them when the run ends.

## Doctor

One read-only check, run first whenever anything looks off. All must hold:

```bash
psql "$DBOS_SYSTEM_DATABASE_URL" -tAc "select version from dbos.dbos_migrations order by 1 desc limit 1"
```

Expect a migration version at or above the ceiling-probe floor in
`PostgresTest.hs`. If the database is unreachable or below floor, confirm
`DBOS_SYSTEM_DATABASE_URL` points at the database `make db-migrate` migrated
before running anything.

## Drive

One harness: the suite binary from Launch step 4. It exercises
registration, steps, events, messages, waits, queues, outbox, recovery,
and every system-database method against live Postgres — the same engine
functions the sim trees mirror in the watcher. Library behavior is driven
by the suite, never by ad-hoc `ghc -e` pokes at internals.

## TDD loop: watcher vs full suite

Two different signals for two different moments — this is the rule from
`AGENTS.md`, kept here so verification reads it cold.

**During edits: the watcher (`make dev`) owns the fast truth.**
`ghciwatch` reloads on every save and rewrites `ghcid.txt` with that
reload's whole result: compile errors and warnings, `All good (N
modules)`, and the tasty output of the enabled sim groups. After every
reload, read `ghcid.txt` immediately — never sleep more than 5 seconds
first, and re-read until the content changes, because while a reload is in
flight the file still holds the previous reload (`tail ghcid.txt` is
enough). Sim announcement traces print to the watcher's stderr, so read
the tmux pane — not the file — for those. Never pipe, redirect, or `tee`
into `ghcid.txt`; never start a second watcher (use the existing tmux
`make env` / `ghciwatch` pane). The watcher runs sims only: a live-only
test is verified through the suite binary, never by enabling its group.

**Trust `ghcid.txt` only when the watcher is alive.** No `ghciwatch`
process (`pgrep -fl ghciwatch` empty) or a stale mtime means the file
proves nothing — start `make dev` first.

**For the gate: the suite binary, freely alongside the watcher.** The
watcher eval holds no fixture rows, so the two never deadlock. After
green, run the psql end-to-end query (see Evidence) — a green suite
without the rows is not a pass.

## Evidence

Proof standards:

- Exercise the suite, not internal setters: every behavior is pinned by a
  named Tasty case.
- Verify durable side effects in Postgres alongside the green suite:
  `dbos.workflow_status` (workflow rows), `dbos.operation_outputs`
  (checkpoints), `widget_store.orders` (`order_status`,
  `progress_remaining`), `widget_store.transaction_completion`
  (transactional-step rows).
- After a green suite run, prove the suite wrote durable state: count
  recent rows (the suite owns its rows per test with fresh UUIDs, so no
  fixed ids exist to look up — recency is the proof):

  ```bash
  NOW_MS=$(python3 -c "import time; print(int(time.time()*1000))")
  psql "$DBOS_SYSTEM_DATABASE_URL" -tAc \
    "select 'workflow_status', count(*) from dbos.workflow_status \
       where created_at >= $((NOW_MS - 3600000)) \
     union all select 'notifications', count(*) from dbos.notifications \
       where created_at_epoch_ms >= $((NOW_MS - 3600000))"
  ```

  Expect hundreds of `workflow_status` rows with a sane status mix
  (hundreds `SUCCESS`, plus the terminal states the suite exercises).
  A green suite with no recent rows is not a pass.
- Record the migration `from→to` versions from the Launch step with every
  artifact.

Write every artifact under `/tmp/dbos-verify-<RUN_ID>/` (build log, suite
output, psql outputs). A cleanup that eats the proof fails this skill.

## Cleanup

Kill only what this run started; never kill by process name. This skill
starts no servers: remove `/tmp/dbuild` and `/tmp/dbos-test` when the run
ends, confirm `/tmp/dbos-verify-<RUN_ID>/` still holds the proof
artifacts, and leave the watcher alone. After every failed iteration,
re-run Doctor before driving again — a wedged database state fails the
run, not the proof.

## Helpers

This skill ships no helper scripts, by repo rule: throwaway runners and
drivers live outside the repo and are never checked in (only durable,
reviewed build/test story like `Makefile` targets belongs in the tree).
Every command above is inline and self-contained; anything a reader would
have to reverse-engineer would not be a helper.
