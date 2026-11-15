# ADR-0046: Queue upsert/update before-after pair (supersedes ADR-0044)

Date: 2026-11-15 (thermo-nuclear code quality review of `f88870c`, finding 03).

## Context

ADR-0044 collapsed Rust's `UpsertedQueue { before, after }` and
`UpdatedQueue { before, after }` (`sysdb/types.rs:2144-2178`) into
`(Bool, QueueRecord)` and `QueueRecord`, justified by: no caller reads a
post-call `before` row.

The `f88870c` registration partition warning made that statement false.
The warning is a caller reading the post-call `before` row, and the
collapsed type forced both registration paths to reconstruct it with a
`getQueue` pre-read that races a concurrent peer's write. The pre-read
also cost a second round trip on every registration, and the `Bool` no
call site reads remained as dead weight. The repo's own one-type-one-type
rule was violated for a reason that no longer holds.

## Decision

Supersede ADR-0044: restore the oracle pair, populated from rows the
write already holds under its lock, with no new SQL.

- `SystemDB.Types` gains `UpsertedQueue { upsertedBefore, upsertedAfter }`
  with `upsertedCreated` (mirroring `created()`), and
  `UpdatedQueue { updatedBefore, updatedAfter }`.
- `SystemDB.Class` returns the pairs from `upsertQueue`/`updateQueue`.
  The lock-scoped `validateMergedQueue` judgment ADR-0044 defended is
  unchanged: validation still runs under the update's row lock, and only
  the return trip is added.
- `PostgresSystemDB` builds both from locked reads it already performs:
  `Nothing`/`Just record` at the insert/find branches, the update's
  locked row and written row, and `before == after` on the empty-update
  no-op.
- All four engine/client call sites read the warning transition off the
  write's own result. All four `getQueue` pre-reads are deleted with
  their six-level nest and race paragraphs.
- The update paths need no stored row at all: `queueChangeToUpdate` is a
  pure copy of the change, because `applyQueueUpdate` already derives the
  legacy partition flag from the merged limits under the write's own
  lock — the same derivation the oracle performs in `sysdb`, where the
  caller likewise builds its update without a read (`queue.rs`, update
  path). A missing row is refused by the write itself and mapped in
  `queueUpdateError` to the exact not-registered `ErrorConfig` text the
  pre-read used to produce, so the error surface is byte-identical.

## Consequences

- No wire or row change: the pairs never leave the process, and the
  stored rows are untouched.
- One rule, one place: the before/after transition a warning needs is
  read off the write that produced it, never reconstructed racily
  beside it.
- ADR-0044 stays in the tree as history; its evidence bullet ("no
  caller reads a post-call `before` row") is contradicted by the
  registration warning and must not be cited for future collapses.
