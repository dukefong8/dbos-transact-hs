# ADR-0040: `WorkflowKey` lives in its own leaf module

Date: 2026-10-28. Status: superseded by ADR-0041 (the key merges back into
`Registry` once the `Registry → Error` edge is removed; leaf deleted).

## Context

Rust #133 (`99677cc`) makes `Error::AlreadyRegistered` and
`Error::NotRegistered` carry the `WorkflowKey` rather than its display
string, so a caller can match on its parts. The port must do the same:
the engine `Error` constructors `AlreadyRegistered`/`NotRegistered`
currently carry `Text`.

## Decision

`WorkflowKey` (the type, its smart constructors, `renderWorkflowKey`,
`workflowKeyFromRow`, and its JSON instances) moves from
`DBOS.Transact.Registry` to a new leaf module
`DBOS.Transact.WorkflowKey`. `Registry` re-exports it; `Transact.Error`
imports the leaf. The facade gains only `methodWorkflowKey`.

## Why not keep it in `Registry`

`Registry` imports `Transact.Error` broadly (failures, refusals,
`renderTransactError`, `failureOf`) and cannot loosen that import, so
`Error` importing `WorkflowKey` back from `Registry` is a genuine import
cycle and GHC rejects it. Rust tolerates the corresponding cycle between
`registry.rs` and `error.rs`; Haskell does not, and the choice is between
a new leaf and leaving the errors on `Text` against the oracle.

A leaf in `DBOS.SystemDB.Types` was considered and rejected: that module
mirrors `sysdb/types.rs`, while `WorkflowKey` mirrors `registry.rs` —
misplacing it there would be a worse deviation than the split.

## Consequences

- This is a recorded deviation from the one-Rust-module-to-one-Haskell-module
  rule: `registry.rs` now maps to `Registry` plus the `WorkflowKey` leaf.
  The leaf holds only what `registry.rs` defines for the key; registration,
  references, and the snapshot stay in `Registry`.
- `AlreadyRegistered`/`NotRegistered` render and JSON arms now go through
  the key (`renderWorkflowKey` for prose; `name`/`class_name`/`config_name`
  on the wire, matching Rust's derived serde shape). Recorded errors
  carrying these variants decode back to the key, not to text.
