# ADR-0040: `QualifiedWorkflowName` lives in its own leaf module

Date: 2026-10-28. Status: superseded by ADR-0041 (the key merges back into
`Registry` once the `Registry → Error` edge is removed; leaf deleted).

Note 2026-11-08 (Rust #136): the oracle renamed `WorkflowKey` to
`QualifiedWorkflowName`; this record now uses the current name throughout.
At the time of writing (#133) the type was still `WorkflowKey`.

## Context

Rust #133 (`99677cc`) makes `Error::AlreadyRegistered` and
`Error::NotRegistered` carry the `QualifiedWorkflowName` rather than its display
string, so a caller can match on its parts. The port must do the same:
the engine `Error` constructors `AlreadyRegistered`/`NotRegistered`
currently carry `Text`.

## Decision

`QualifiedWorkflowName` (the type, its smart constructors, `renderQualifiedWorkflowName`,
`qualifiedWorkflowNameFromRow`, and its JSON instances) moves from
`DBOS.Transact.Registry` to a new leaf module
`DBOS.Transact.QualifiedWorkflowName`. `Registry` re-exports it; `Transact.Error`
imports the leaf. The facade gains only `methodQualifiedWorkflowName`.

## Why not keep it in `Registry`

`Registry` imports `Transact.Error` broadly (failures, refusals,
`renderTransactError`, `failureOf`) and cannot loosen that import, so
`Error` importing `QualifiedWorkflowName` back from `Registry` is a genuine import
cycle and GHC rejects it. Rust tolerates the corresponding cycle between
`registry.rs` and `error.rs`; Haskell does not, and the choice is between
a new leaf and leaving the errors on `Text` against the oracle.

A leaf in `DBOS.SystemDB.Types` was considered and rejected: that module
mirrors `sysdb/types.rs`, while `QualifiedWorkflowName` mirrors `registry.rs` —
misplacing it there would be a worse deviation than the split.

## Consequences

- This is a recorded deviation from the one-Rust-module-to-one-Haskell-module
  rule: `registry.rs` now maps to `Registry` plus the `QualifiedWorkflowName` leaf.
  The leaf holds only what `registry.rs` defines for the key; registration,
  references, and the snapshot stay in `Registry`.
- `AlreadyRegistered`/`NotRegistered` render and JSON arms now go through
  the key (`renderQualifiedWorkflowName` for prose; `name`/`class_name`/`config_name`
  on the wire, matching Rust's derived serde shape). Recorded errors
  carrying these variants decode back to the key, not to text.
