# ADR-0024: `AnyApplication` constructor-prefix deviation

Date: 2026-10-05. Status: accepted.

## Context

`DBOS.SystemDB.Types.Applications` ports Rust `Applications` (`sysdb/types.rs:844`)
one-to-one, including the `Any` case ("Every application, said deliberately").
`Prelude` funnels `Data.Semigroup`'s `Any` monoid into every module, so any
module importing both `DBOS.Prelude` and the Types surface unqualified gets
`Ambiguous occurrence ‘Any’` (first hit: `Postgres/Backend.hs` queue scoping).

## Decision

Constructor-prefix deviation (the `ForkStep` / `ErrorMaxRecoveryAttemptsExceeded`
style): the case is `AnyApplication` in Haskell. The oracle word stays, the
domain scope disambiguates, and no hiding list or qualified call site is needed.

- Rust spelling is unchanged (`Applications::Any`); the deviation is Haskell-only.
- `Applications` is a filter DSL, never serialized: no SQL, Aeson, or config key
  changes. `Unset` and `Named` keep their ported names.

## Consequences

- One word-boundary sweep over `src/`, `test/` (definition, three `Backend.hs`
  patterns, one `IOSim.hs` pattern, four `PostgresTest.hs` seeds, doc mentions).
- Future oracle diffs must map `Any` ↔ `AnyApplication`.
