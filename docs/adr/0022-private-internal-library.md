# ADR-0022: Private internal library and peer-only engine imports

Date: 2026-10-04. Branch: `proto/phantom-brands`. Supersedes nothing;
implements the facade rule recorded in the C5a cleanup (facade for
clients only; peers import peers directly).

## Context

`DBOS.Transact` re-exported engine internals (`Ctx`, `newCtx`,
`withAttempt`, allocators, placement machinery, inner accessors) because
the test suite — a separate Cabal stanza — can only import exposed
modules. Engine peers already imported each other directly, but tests
(and any future peer) could only reach internals through the client
facade, doubling its interface and inviting app code onto engine
seams. Cabal visibility, not file layout, decides what a stanza can
import: `other-modules` are invisible outside the library stanza.

## Decision

1. New private internal library `dbos-transact-internals`
   (`visibility: private`, same `src/`, partitioned module lists, shared
   `commons`). The public library keeps the four stable names
   (`DBOS.Prelude`, `DBOS.SystemDB`, `DBOS.SystemDB.Postgres`,
   `DBOS.Transact`) and depends on internals. Downstream packages see
   exactly what they saw before; the test suite (and only it, inside
   this package) additionally depends on internals and imports engine
   names from their defining peer modules. The demo-apps executable
   deliberately does NOT depend on internals, so a build break there
   proves the facade no longer suffices for clients.

2. Three splits, all behavior-neutral, all client names preserved:
   - `DBOS.SystemDB.Postgres` implementation moves to
     `DBOS.SystemDB.Postgres.Backend`; the public name is a pure
     re-export shim. Forced: internal `Connection`/`Client` build the
     concrete backend, a library-level import cycle that macOS
     dynamic TH-loading (`typedSql` splices) refuses to link.
   - The `SystemDB` class moves to `DBOS.SystemDB.Class`; the public
     name re-exports it with the domain modules. Forced: same link
     failure, one symbol later (the class dictionary). The class keeps
     the established `SystemDB` spelling, so the Rust `trait
     SystemDatabase` reference is unchanged.
   - `showText` (the only `DBOS.Prelude` definition) moves to
     `DBOS.Prelude.Base`; the public name re-exports it. Same link
     reason.
   Each split is recorded here rather than in three ADRs because all
   three serve this one decision.

3. Facade trim policy: a name stays on `DBOS.Transact` iff demos use it
   or a reasonable app body/runner could (ops, views, binders, errors,
   handles, launch/config, codecs, tracing, all domain types).
   Engine machinery and test scaffolding (`Ctx`, builders, `withAttempt`,
   allocators, placement checks, inner accessors, connection/task/
   registry lifecycles, env-var plumbing) leave the facade; tests
   import them from peers. The defining-module rule from AGENTS.md
   still holds: these are session/seam modules, never Rust-module
   counterparts, and no ported type is renamed.

## Amendment: nested source roots + demos as a separate package

Same day, after the exe link failure. Two corrections to the above:

1. "Same `src/`" does not survive GHC `--make`: with one shared source
   dir, compiling a shim closure-compiles every import-reachable peer
   source into the *main* unit, the shim `.hi` attributes peer symbols
   to the main unit, and the exe link fails with `inplace_DBOS…`
   undefined symbols (the archive only holds listed modules). Separate
   top-level `facade/` fixed it, but the tree split hurt navigation.
   Final layout keeps one tree with nested roots: the three shims stay
   at `src/DBOS/…`, peer sources move unchanged (same module names, no
   rename, no ADR needed for names) to `src/DBOS/Internal/DBOS/…`, the
   internals library takes `hs-source-dirs: src/DBOS/Internal` and the
   public library `src`. Peers are invisible to the main unit's import
   search, so no double-compile (main-unit build dir holds exactly the
   three shim objects) and the exe links.
2. `DBOS.Prelude` stays internal-only (no public shim; demos use the
   standard `Prelude` plus `io-classes` effects). Demos leave the
   package entirely (`demo-apps/dbos-demos.cabal`, `packages: demo-apps`
   in `cabal.project`, depending on the public library only), so client
   purity is structural — a peer import from demo code fails exactly as
   it would for an external user. The TH `schema.sql` path in
   `WidgetStore.Store` is relative to the new package root. The watcher
   (`make dev`) loads tests only now: `-isrc:test:src/DBOS/Internal`,
   `--watch src --watch test` (`src` covers the nested root).

## Consequences

- `cabal build all` is the gate for layering: any new peer→facade
  import of a peer-defined name fails the build only by review; the
  macOS link makes public→internal the only legal direction.
- `make probes` builds the library first (stale registration otherwise
  fails every probe with hidden-package errors).
- Test files import engine names from `DBOS.Transact.Context`,
  `.Checkpoint`, `.Connection`, etc. directly; the import lists are the
  live documentation of which seam each test exercises.
