# DBOS Haskell Transactional Outbox

The Python transactional-outbox (`~/dev/dbos-demo-apps/python/transactional-outbox`)
rebuilt on the Haskell transactional-step datasource and the IHP web stack.
Both Python patterns are ported:

1. **Atomic workflow** (`atomic_workflow.py`): one `place_order_workflow`
   inserts the order, publishes its id, sends the notification step, and
   marks it sent. A crash anywhere recovers to the same SENT state.
2. **Transactional enqueue** (`transactional_enqueue.py`): the handler
   inserts the order **and** calls the `dbos.enqueue_workflow` PL/pgSQL
   function in the same `runTxOutside` transaction (via
   `Outbox.Store.enqueueNotificationStatement`, ihp-typed-sql), so the
   `send_notification_workflow` is durably scheduled iff the order commits.
   No outbox table, no poller.

## Fidelity notes

- Step names (`insert_order`, `send_order_notification`,
  `update_notification_status`) match the oracle's function names, so the
  tracer and checkpoint sequence read the same.
- The enqueued consumer takes a `NotifyEnvelope` (`positionalArgs`) rather
  than bare positional arguments: the Python framework unwraps the envelope
  the SQL function writes, while the Haskell engine reads the stored input
  as-is. Same atomicity, one documented seam.
- Tracer comparison: `TRACE_LEVEL=debug` emits
  `TransactionRunning/TransactionOutputRecorded` and
  `StepRunning/StepOutputRecorded` pairs per durable unit, where the Python
  demo's pysnooper labels (`db:insert_order`, `step:send_order_notification`,
  `workflow:send_notification_workflow`) trace per line.

## Run

```sh
psql "$DATABASE_URL" -f demo-apps/dbos-hs-outbox/schema.sql  # compile-time typedSql describe
PORT=8090 cabal run exe:demo-apps
# http://localhost:8090/outbox/
```

## HTTP surface

Same URLs as the Python demo — `POST /orders`, `GET /orders` — plus
`POST /enqueue-orders` for the second variant, each with the Python JSON
body. A request carrying the `HX-Request` header gets the rendered orders
fragment (polled every 2s) instead of JSON.

## Database layout

| table | owner |
| --- | --- |
| `outbox_store.orders` | the app (`schema.sql`) |
| `outbox_store.transaction_completion` | the transactional-step engine |
| `dbos.*` | the system database, migrated by the Rust runner (`make db-migrate`) |
