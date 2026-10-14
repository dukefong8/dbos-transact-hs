# ADR-0036: Schedules, Cron, and Scheduler Loop Deferred

Date: 2026-10-14

## Context

Rust ships a full scheduling surface (`schedule.rs`: ten `DBOS` operations
plus ten `Client` mirrors, `ScheduleSpec` validation, backfill and trigger
semantics; `cron.rs`: dialect parsing, firing ids, DST rules; `scheduler.rs`:
poll/reconcile/run loop with catch-up, jitter, and single-writer
`last_fired_at`). Python documents the same surface for application
developers and external admin tools.

The Haskell port holds only the storage seam: `ScheduleRecord`,
`NewSchedule`, `ScheduleUpdate`, `ScheduleFilter` (`SystemDB/Types.hs`),
the eight class methods (`SystemDB/Class.hs`: `createSchedule` through
`deleteSchedule`), and row-level tests. No Transact-level type, operation,
cron parser, or loop exists, and no Transact-level schedule test can exist
until an operation does.

## Decision

Defer the schedules slice (types and validation, management plus client
operations, cron dialect, scheduler loop) behind all other Rust-parity
work: rewind (A1), client gaps (A3), management and queue semantics
(A4, A5), and streams/wait/select/event gaps (A6). The seam stays; the
surface waits.

## Consequences

- Applications needing periodic work keep polling with sleeps or driving
  cadence from outside; nothing in the port advertises schedules.
- The deferred slice needs a design note before code (cron dialect scope,
  backfill/trigger in-workflow refusals, loop ownership), like the notes
  that preceded the stream and client-transaction ports.
- Revisit when Rust parity elsewhere is green; the seam's row-level tests
  (`TypesTest`, `IOSimTest`, `ManagementSimData`) pin the storage contract
  meanwhile.
