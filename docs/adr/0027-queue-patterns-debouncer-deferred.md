# ADR-0027: Queue-patterns debouncer deferred

Date: 2026-10-07

## Context

The Python queue-patterns demo (`dbos-demo-apps/python/queue-patterns/main.py`)
demos three patterns: **fair queueing**, **rate limits**, and **debouncing**
(`Debouncer.create(debouncer_workflow, queue="debouncer-queue")` with a
per-tenant debounce key and 5s period).

The Haskell port (`demo-apps/dbos-hs-queue-patterns/`) covers fair queueing
(partitioned queue + concurrency-manager workflow + `Enqueue.partitionKey`)
and rate limiting (`QueueOptions.rateLimit`), both of which the `DBOS.Transact`
facade exposes.

## Decision

Defer the debouncer. The engine holds debounce primitives internally
(`DebounceRequest`, `debounceDelayedWorkflow`, debounce columns on
`NewWorkflow`/`QueueRecord`), but the public `DBOS.Transact` facade exposes
no debouncer API — no `Debouncer.create` equivalent, no `debounce` call —
so a demo cannot use it without reaching into the private
`dbos-transact-internals` library, which `demo-apps` exists to forbid
(its cabal package may depend only on the public library).

The demo's debouncer tab renders this deferral note instead of a form.

## Consequences

- To port the debouncer later, first expose a facade-level debounce API
  (mirroring the oracle's `DebounceRequest` semantics), then add the
  `debouncer-queue` registration, the `debouncer_workflow`, and the submit
  path that debounces per tenant key.
- Fair-queue and rate-limit coverage is unaffected.
