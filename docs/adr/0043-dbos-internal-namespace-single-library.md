# ADR-0043: `DBOS.Internal.*` module namespace and the single library

Date: 2026-11-12.

## Context

The internals lived as a second, private library (`library
dbos-transact-internals`, `hs-source-dirs: src`) beside the public facade
library in the same package, with files under `src/DBOS/Internal/DBOS/**` and
module names `DBOS.Prelude`, `DBOS.SystemDB.*`, `DBOS.Transact.*`. Two
failure modes came with that shape:

- The extra `DBOS` directory level under `src/DBOS/Internal/` existed only to
  spell the module names; the file tree duplicated the namespace.
- The two libraries shared one search path: the public library's `-i src` saw
  `src/DBOS/Internal/**` and home-compiled all 38 internals modules into
  itself (`[41 of 41]`), while cabal's archive step packages only the modules
  a component declares. The public archive therefore held 4 members and none
  of the internal symbols, and every cross-package link (`exe:demo-apps`)
  failed with `ld: symbol(s) not found for architecture arm64` on the
  internals symbols; `-Wmissing-home-modules` surfaced the same shape as a
  warning. The split is unsound for this package — not a naming mistake, and
  not fixable by search-path tricks while the facades and the internals share
  `src/`.

## Decision

1. Flatten the tree: `src/DBOS/Internal/DBOS/**` → `src/DBOS/Internal/**`.
2. Namespace the internal modules as `DBOS.Internal.*`
   (`DBOS.Internal.Prelude`, `DBOS.Internal.SystemDB.*`,
   `DBOS.Internal.Transact.*`), chosen by the operator over the `Internal.*`
   top-level spelling. A word-boundary sweep renamed 743 identifier
   occurrences across 127 files (module headers, imports, module-name
   comments; the `negative/` witnesses included). Identifier sweeps never
   touch string literals: the 157 `"DBOS."` wire-name literals were outside
   the sweep pattern and verified unchanged after.
3. Merge `dbos-transact-internals` into the single `library` component: one
   stanza, `hs-source-dirs: src`, 41 exposed modules (3 facades + 38
   internals). The test suite depends on the package and imports internals
   directly, so they stay exposed; demo-apps import only the facades by
   convention.

## Consequences

- The Rust one-module-to-one-module mapping is unchanged: every former
  `DBOS.SystemDB.Types` is now exactly `DBOS.Internal.SystemDB.Types`, and so
  on — a namespace move, not a split or a rename of a ported type, so no
  deviation from the hard rules is introduced. AGENTS.md's mapping examples
  and the base-funnel import name follow the new spellings.
- The public archive now holds 42 members with the full internals symbol set
  (644 `DBOS.Internal.Transact.Workflow` symbols measured); `cabal build all`
  links the demos and the test suite with zero `-Wmissing-home-modules`
  warnings, and the live suite runs green (1098/1098, 2026-11-12, after
  `make db-migrate` verified 125→125).
- hie.yaml's `./src → lib` cradle mapping became correct with the merge: one
  component owns everything under `src/`, which is what the mapping asserts.
- Declaring internals in the same component is what makes the archive
  complete. A new module under `src/DBOS/Internal/**` must be listed in
  `exposed-modules` (or `other-modules`), or the next cross-package link
  fails as before — the `-Wmissing-home-modules` warning is the early signal.
