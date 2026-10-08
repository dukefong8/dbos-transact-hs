# ADR-0028: Debouncer port (TypeScript-aligned seam) — corrected to the sysdb bounce

Date: 2026-10-07. Corrects the decision below (same day): the message-based
design it records was a misread of the oracle, caught in review. The
mechanism is the sysdb bounce, as the primitives' own names always said.

## Context

The Python queue-patterns demo needs debouncing, and Rust owns only the
sysdb-level bounce primitives (`DebounceRequest`, `debounce_delayed_workflow`
in `sysdb/`), not a public `Debouncer`. The public shape therefore follows
the TypeScript `Debouncer` (`src/debouncer.ts`, mirrored by Python's
`dbos/_debouncer.py`): `Debouncer(workflow, timeout, queue)` plus
`debounce(key, period, inputs)`.

## Decision

New module `DBOS.Transact.Debouncer` (plain-Haskell internals, re-exported
from the `DBOS.Transact` facade as `Debouncer (..)`, `debouncerNew`,
`debounce`, `debounceInWorkflow`):

- The mechanism is the oracle's sysdb bounce, not a framework workflow.
  Each call builds a `DebounceRequest` (name/class/config from the
  referenced workflow, the queue, the deduplication key, `now + period`,
  the encoded inputs) and runs `debounceDelayedWorkflow`:
  - `Debounced wid` answers a handle to the extended row;
  - `DebounceHeld holder` classifies by `classifyBounce` (the oracle's
    four-way rule: non-debounced, wrong name/class, or foreign application
    raises `QueueDeduplicated`; a same-name debounced holder that flipped
    out of DELAYED mid-bounce retries);
  - `DebounceUnheld` enqueues a fresh debounced row (DELAYED, marked
    debounced, deadline stamped, no inherited deadline), and a lost
    enqueue race loops back to the bounce, as the oracle's does.
- `debounceInWorkflow` bounces with the caller's `(workflow, step), so the
  bounce commits atomically with its step checkpoint (the oracle's
  `call_txn_as_step`); the fresh-enqueue leg starts a child whose id
  derives from the parent and the bounce's step, so a replay re-enqueues
  the same id.
- The deduplication key is `<workflow>-<key>`, class-prefixed for instance
  workflows: free functions read as the Python oracle writes them,
  instance workflows carry their class as the TypeScript oracle's
  `Class.workflow-key` does. The period guard (`InvalidArgument` on a
  non-positive period) mirrors the oracle's throw.
- The fresh-enqueue leg needed three fields the shared `Enqueue` lacked,
  mirroring TypeScript's `EnqueueOptions` (`isDebounced`,
  `debounceDeadlineEpochMS`, and the acting application, which the
  oracle's own comment says only the debounce names): `Enqueue` gains
  `isDebounced`, `debounceTimeout`, and `applicationName`, mapped onto the
  row by both start paths (`debounceCreation` caps the delay at the
  timeout's deadline, as the oracle's executor does). Plain enqueues
  contribute nothing beyond their delay, so existing behavior is unchanged.
- Coverage is a five-scenario dual-stack suite (`DebouncerCases` over
  `DebouncerTest`/`DebouncerTestSim`): first-bounce DELAYED row, second
  bounce coalescing (same id, extended delay, last inputs win, one run),
  foreign-holder refusal, in-workflow step recording, and deadline capping.

## Superseded design (kept as evidence)

The original decision built the Python oracle's *message-based* debouncer:
a framework `debouncerWorkflow` per user workflow, inputs ferried on
`DEBOUNCER_TOPIC`, collection until one quiet period passes. That design
is real — it is what an early `dbos/_debouncer.py` did — but the current
oracles bounce at the sysdb level, and the message machinery (framework
registration, `registerDebouncer`, `debouncerTopic`, ack events) is gone:
`debouncerNew` is now a plain value (queue/timeout/application), and the
queue-patterns demo debounces its user workflow directly. The handle-id
bug that review found in the message design (every caller answered its
own minted id, so concurrent callers fanned out instead of joining) is
structurally absent: every path answers the bounced or enqueued row's id.

## Consequences

- `demo-apps/dbos-hs-queue-patterns/` keeps its debouncer tab with no
  framework registration; its durable footprint is one debounced row per
  key with last-inputs-wins, as the oracle's is.
- Facade grows by four names (`Debouncer (..)`, `debouncerNew`,
  `debounce`, `debounceInWorkflow`); all other debounce primitives stay
  internal.
- Full suite green (715 tests, including the 5 new live debouncer cases;
  the 5 sim cases run eval-only in the watcher pair).

## Observations (not this slice's scope)

- The supervisor's `transitionDelayedWorkflows` sweep is application-scoped
  but not queue-scoped: a due DELAYED row moves to ENQUEUED (releasing a
  debounced key) on every queue, listened or not. The timeout-cap case
  keeps its row undue for exactly this reason.
