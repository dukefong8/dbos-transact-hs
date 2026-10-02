# DBOS Haskell Widget Store

The Rust widget store port (`~/dev/dbos-transact-rust/demo-apps/dbos-rust-widget-store`)
rebuilt on the Haskell transactional-step datasource and the IHP web stack:

- **ihp-hsx + htmx v4** — views are `[hsx| ... |]` with the htmx v4 attribute
  allowlist from the Playground/hs template, rendered with lucid2 and swapped
  as server fragments; **ihp-router** serves the routes.
- **ihp-typed-sql** — every application query is a typed `Statement` in
  `WidgetStore.Store`. The same statement runs two ways: inside a workflow
  step through the transactional-step engine's held connection (the
  application write and the step checkpoint share one commit), and through
  the datasource pool for handler reads.
- **The look** is the Rust page's: same Tailwind palettes, custom CSS, cards,
  badges, progress bar and crash button. The Alpine state machine becomes
  server-rendered fragments driven by htmx.

## Run

```sh
make widget-db          # schema.sql -> $DBOS_DATABASE_URL (compile-time typedSql describe)
cabal run demo-apps
# http://localhost:8080
```

The app also creates its own schema at startup (the embedded `schema.sql`),
the way the Rust port's `create_schema` does; `make widget-db` exists because
typedSql PREPAREs every statement against a live database at build time.

Press **Buy Now**, confirm the payment, and watch the ten dispatch ticks.
Press **Crash the Application** at any point and start the app again: `launch`
recovers the abandoned workflow and the finished steps replay from their
checkpoints.

## Database layout

| table | owner |
| --- | --- |
| `widget_store.orders`, `widget_store.products` | the app (`schema.sql`) |
| `widget_store.transaction_completion` | the transactional-step engine (checkpoint column shape shared with every SDK) |
| `dbos.*` | the system database, migrated by the Rust runner (`make db-migrate`) |

## HTTP surface

Same as the Rust port — `GET /`, `/product`, `/orders`, `/order/{id}`,
`POST /restock`, `POST /checkout/{idempotency_key}`,
`POST /payment_webhook/{payment_id}/{payment_status}`,
`POST /crash_application` — plus htmx: a request carrying the `HX-Request`
header gets the rendered fragment instead of JSON/text.

## Crash and recovery

Both ports run side by side, each under a restart loop so the crash button
shows the whole recovery story live (a production process manager plays the
loop's role): the Haskell app tails its production tracer, the Python app its
engine log. A crash mid-dispatch is recovered the same way by both — the
pending workflow is re-enqueued at startup, its finished steps replay, and the
rest reaches DISPATCHED with the inventory decremented exactly once.

(Operators drive this with throwaway scripts kept outside the repo.)



## Local IHP checkout note

The lucid2 hsx renderer lives in the `ihp-hsx-lucid2` sublibrary of the local
`~/dev/ihp/ihp-hsx` checkout. So this app can depend on it from Cabal, that
sublibrary was made `visibility: public` (and the package's `cabal-version`
raised to 3.0, the spec that has the field). The change is local to the IHP
checkout and uncommitted there.
