# ADR-0030: Queue-options TS 5.x alignment

Date: 2026-10-08. Closes the "queue option drift" finding in the TS parity
audit: `QueueOptions` is a Rust-mapped ported type, so the renames here
need this ADR (HARD RULES).

## Context

The queue-options surface drifted from both SDK oracles while following
Rust:

- `priorityEnabled :: Bool` exists as an option. TypeScript 5.0 removed it
  (`REMOVED_QUEUE_PARAMS` in `wfqueue.ts`: "every queue dispatches in
  priority order, so the option can be deleted"); Python's
  `register_queue` has no such parameter either, and hardcodes
  `"priority_enabled": True` on write ("Legacy columns, still read by
  other SDKs. Every queue is a priority queue now." — `dbos/_sys_db.py`).
  Verified in this tree: the claim sweeps order by
  `priority asc, created_at asc` unconditionally — `priority_enabled` is
  written to the row but never read by any dequeue path. Deletion is
  behavior-preserving.
- `concurrency :: Maybe Int` is the only global-limit name. Both oracles
  renamed it to `globalConcurrency` and retained `concurrency` as a
  deprecated alias, explicit-wins precedence (`global ?? legacy` in
  `wfqueue.ts` `recordFromParams` and `dbos/_dbos.py`).
- `pollingInterval :: Duration` keeps its name and unit by decision (grill
  round 1, Q3): Python polls in seconds (`polling_interval_sec: float =
  1.0`), so an ms number would move away from Python and Rust. Only TS's
  `minPollingIntervalMs` name drifts. The DB wire
  (`polling_interval_sec`, fractional seconds) is untouched regardless.

## Decision

- Delete `priorityEnabled` from `QueueOptions`, `QueueChange` (and their
  defaults), `Queue` (receipt), and the option→record/update mappings.
  Register and update always persist `True`, as Python does, so cross-SDK
  rows stay identical. `QueueRecord`, the `queues` columns, and the sysdb
  statements keep the column (Rust-mapped; no migration).
- Add `globalConcurrency :: Maybe Int` to `QueueOptions` and `QueueChange`
  (+ defaults); keep `concurrency` as a documented deprecated alias
  (explicit wins). A `{-# DEPRECATED #-}` pragma was probed and rejected:
  one pragma fires on every record sharing the field name, including the
  receipt's live `concurrency` (which mirrors TS `Queue.concurrency`).
  Effective limit is `globalConcurrency <|> concurrency`, on both the
  register and update paths; validation (`positive`, `ordered` against
  partition limits) applies to the effective value.
- `pollingInterval` is unchanged (name and `Duration` unit).
- Skill `queue-*` refs and the parity audit table move in the same sweep.

## Consequences

- One breaking rename-by-deletion (`priorityEnabled`) plus one additive
  field: callers setting `priorityEnabled` fail to compile with a named
  field error; callers on `concurrency` get a deprecation warning, not an
  error. In-repo call sites migrate in the sweep (sweeps are
  word-boundary, counted, verified).
- The `update_if_latest_version` / `always_update` paths write
  `priority_enabled = True` unconditionally; a queue row previously
  registered with `False` flips to `True` on its next update — a no-op
  semantically (nothing reads the column in any SDK's dequeue path).
- TDD: extend the queue-registry dual-stack suite (defaults, legacy
  re-scope, reservations) with the new option shape, the deprecated
  alias + precedence, and the always-`True` persistence, live + sim.
