# DBOS Haskell Queue Patterns

The Python queue-patterns (`~/dev/dbos-demo-apps/python/queue-patterns/main.py`)
rebuilt on the Haskell queue engine and the IHP web stack. All three patterns
are ported:

- **Fair queueing**: the handler enqueues a concurrency-manager workflow onto
  `partitioned-queue` with the tenant as its `Enqueue.partitionKey` (the
  Python `SetEnqueueOptions`); the manager publishes the tenant as an event
  (the facade exposes no row reads, so the list cannot read
  `queue_partition_key` directly), enqueues the real work on
  `concurrency-queue` (concurrency 5), and awaits it.
- **Rate limiting**: `rate-limited-queue` with `RateLimit 2 / 10s`; the
  workflow is the same 5s durable sleep.
- **Debouncing**: the new `DBOS.Transact.Debouncer` seam (ADR-0028),
  message-based like the Python `Debouncer`: first call per tenant key wins
  the internal-queue slot, later calls replace its inputs, and the workflow
  fires 5s after inputs go quiet with the last input.

## Run

```sh
PORT=8090 cabal run exe:demo-apps
# http://localhost:8090/queue-patterns/
```

## HTTP surface

`POST /workflows/fair_queue?tenant_id=…` (or urlencoded `tenant_id`),
`POST /workflows/rate_limited_queue`,
`GET /workflows?tab=fair-queue|rate-limited|debouncer` listing
`{workflow_id, workflow_status, tenant_id}`. A request carrying `HX-Request`
gets the rendered rows (polled every 2s) instead of JSON.
