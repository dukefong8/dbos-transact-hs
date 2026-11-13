# ADR-0044: Queue upsert/update before-after collapse (no UpsertedQueue/UpdatedQueue)

Date: 2026-11-13 (oracle-validation findings Y1, Rust 0.6).

## Context

Rust's sysdb returns a before-and-after pair for queue writes
(`sysdb/types.rs:2144-2178`):

- `UpsertedQueue { before: Option<QueueRecord>, after: QueueRecord }`,
  with `created()` defined as `before.is_none()`;
- `UpdatedQueue { before: QueueRecord, after: QueueRecord }`,
  handed to the caller beside the row it replaced so the caller can judge
  the transition (for example, whether the update partitioned the queue).

The port collapses both pairs (`Class.hs`, `upsertQueue` /
`updateQueue`):

- `upsertQueue` returns `(Bool, QueueRecord)` — created flag plus the
  row as committed;
- `updateQueue` returns the row as committed, with the before row demoted
  to the validation callback's first argument
  (`QueueRecord -> QueueRecord -> Either Error ()`).

The one-type-one-type rule would ask for `UpsertedQueue` /
`UpdatedQueue` counterparts carrying the stored `before` row out of the
call.

## Decision

Keep the collapse and record it as a deviation rather than adding the
types: no caller reads a post-call `before` row, so the types would be
structure nothing consumes.

Evidence, per call site:

- Engine `registerQueue` (`Queue.hs`) and client `clientRegisterQueue`
  (`Client.hs`) use only the created flag and the `after` row
  (`Right (_, record)`). The created flag is exactly the oracle's
  `created()` (`before.is_none()`), so the upsert collapse preserves
  every bit its callers consume.
- Engine `updateQueue` and `clientUpdateQueue` use only the `after` row.
  The `before` row is still read under the update's own row lock and
  handed to the shared `validateMergedQueue` alongside the merged row —
  the same validation the oracle runs with `&before, &after` — so the
  lock-scoped judgment the pair exists for is intact. What is dropped is
  only post-call access to `before`, which no path takes.
- The sim and mock backends (`IOSim.hs` mock and mem implementations)
  return the same collapsed shapes, so sims exercise the same contract
  the live backend implements.

## Consequences

- No wire or row change: the collapsed values never leave the process,
  and the stored rows are untouched.
- Revisit trigger: if a caller ever needs the replaced row after the
  call returns (for example, to judge whether a registration partitioned
  the queue, as the oracle's docstring suggests), add `UpsertedQueue` /
  `UpdatedQueue` to `SystemDB.Types` mirroring the oracle field names,
  return them from the two class methods, and migrate the four call
  sites plus the sim/mock backends. The validator already receives
  before and after, so only the return trip needs work.
- 2026-11-14 note: the registration half of the trigger fired (review
  finding: registration paths missed the partition warning). Taken the
  pre-read path instead of the seam surgery: both registration call
  sites read the row with `getQueue` ahead of the upsert and judge the
  transition with the existing `warnIfPartitioned`, exactly as the update
  paths already do. The read races a concurrent peer's write, but the
  consequence is warning-only either way — the same standing the update
  path's pre-read has — while the seam change would have touched the
  class, both backends, both sim/mock backends, and all four call sites
  for no behavioral gain beyond the race. The full `UpsertedQueue` /
  `UpdatedQueue` migration stays the fallback if a caller ever needs the
  replaced row for more than a warning.
- Y1 in `docs/oracle-validation-findings-2026-11-12.md` is closed by
  this record.
