# ADR-0035: Transactional steps refuse inside-step calls

Date: 2026-10-14

## Context

Python runs a datasource transaction called from inside a step as a plain,
uncheckpointed transaction: its context reports workflow membership only
outside steps (`is_workflow()` is false in steps), so the checkpoint path is
skipped silently and a step retry re-runs the body. Nested transactions
behave the same way (a savepoint-ish nested run, unrecorded).

The Haskell runner refuses both with `InsideStep "transaction"`: a
transaction inside a step would checkpoint under the enclosing step's id,
and recovery could neither adopt nor re-run it correctly. `send` inside a
step stays plain by the leaf rule, but a send moves no checkpoint of its
own; a transaction exists to checkpoint, so plainness would silently drop
its reason for being. The only nesting the port proves is separate
transactions outside workflows (IO-only independent-commit case).

## Decision

Keep the refusal. A call that reaches `runTxStep` through a handed context
or a captured parent whose scope predates the running body fails before
anything is written or any id moves (`scenarioCaptureRefused`).

## Consequences

- Python code that calls transactional functions from step bodies works
  (unreliably) there and hard-errors here: a porting hazard to call out in
  migration notes, not a behavior to match.
- If a plain-in-step transaction is ever needed, add an explicit entry
  point that cannot checkpoint (mirroring `runTxOutside`), rather than
  overloading `runTxStep` — the refusal then guards the durable path while
  the plain path stays honest about offering no exactly-once.
