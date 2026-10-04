# Widget store port — tracker

Porting `~/dev/dbos-transact-rust/demo-apps/dbos-rust-widget-store` into
`demo-apps/dbos-hs-widget-store` on the transactional-step datasource and the
Playground/hs web stack (ihp-hsx + htmx v4 + ihp-router + ihp-typed-sql).

## Decisions

- **Web stack**: hsx with the htmx v4 attribute allowlist (the Playground's
  `Htmx.QQ`), rendered with **lucid2** (the user's renderer of choice);
  ihp-router `[routes|]`; interactions are htmx fragments (server-rendered)
  rather than the Rust page's Alpine state machine. The Tailwind palettes,
  custom CSS, cards, badges and progress bar are copied from the Rust page.
- **Renderer dependency**: the lucid2 hsx modules live in the
  `ihp-hsx-lucid2` sublibrary of the local `~/dev/ihp/ihp-hsx` checkout. It
  was private, so `ihp-hsx.cabal` gained `visibility: public` on that
  sublibrary and its `cabal-version` was raised 2.2 → 3.0 (the spec that has
  the field). Uncommitted, local to the IHP checkout.
- **typedSql + transactional step**: `WidgetStore.Store` exposes plain
  `Statement`s. Workflow steps run them through `Tx.txStatement` inside
  `runTxStep` (one commit with the checkpoint); handlers run the same
  statements through `runAppSession`. typedSql's compile-time describe needs
  the `widget_store` schema, so `make widget-db` applies `schema.sql` before
  the first build; the app also creates the schema at startup (Rust's
  `create_schema`).
- **Checkpoint schema**: `widget_store` (app DB), not `dbos` — the app's
  tables and its checkpoints live together, away from the system schema.
- **HTTP surface**: identical to the Rust port; htmx fragment responses are
  selected by the `HX-Request` header on the same URLs.
- **Watcher**: `.ghci` now `:load`s the widget-store `Main` (replacing
  `test/Main.hs`), `make dev` watches `demo-apps`, and `make env` installs
  `ihp-hsx ihp-router lucid2 wai warp http-types unix`.

## Status — all gates green (2026-10-02)

- [x] `app/` → `demo-apps/dbos-hs-starter/` (starter executable moved).
- [x] `cabal.project`: local `ihp-hsx` added (Playground template shape).
- [x] `schema.sql` + `make widget-db`; schema applied to the dev DB.
- [x] `WidgetStore.Store` typedSql statements + row converters.
- [x] `WidgetStore.Workflows` checkout + dispatch (transactional steps).
- [x] `WidgetStore.View` / `Htmx` / `App` / `Http` / `Handler` / `Route` /
      `Main`, mirroring the Playground/hs Todo app's web-stack split.
- [x] `dbos-hs-widget-store` cabal executable.
- [x] Watcher green on the widget-store load target
      (`.ghci` loads `demo-apps/dbos-hs-widget-store/Main.hs`;
      `make dev` watches `demo-apps`; ghcid.txt: **All good (45 modules)**).
- [x] Live gate: buy → pay → dispatch. Orders (2,1,0), inventory 99,
      tracer `TransactionRunning/TransactionOutputRecorded` per named step.
- [x] Crash/resume live gate. Crash mid-dispatch → relaunch →
      `EngineRecovered: re-enqueued workflows a previous run left PENDING
      workflows=1`, `TransactionReplaying update_order_progress (1), (3)`,
      remaining ticks run → DISPATCHED, progress 0, one decrement per order.
- [x] Python live run + snoop comparison, side by side in one tmux window
      (Haskell 8080 | Python 8000, both under restart loops):
      Python `Recovering 1 workflows` and the snooped
      `create_order`/`reserve_inventory`/`update_order_status`/
      `update_order_progress` calls line up with the Haskell tracer's
      `create_order`/`reserve_inventory`/`mark_order_paid`/
      `update_order_progress` events.
- [x] UI verified in Chrome DevTools: Buy → payment panel → Confirm →
      status panel self-polls to DISPATCHED; Chrome inspection caught and
      fixed a duplicated `[hsx|` opener rendering literally.
- [x] README / plan deltas (`README.md`, this file,
      `.lavish/rust-port-plan.html` dated delta).

## Live evidence

- Haskell tracer (stderr, FastLogger, tee'd so it can be tailed):
  `/tmp/dbos-hs-tracer.log` — `EngineRecovered`, `TransactionRunning` /
  `TransactionOutputRecorded` / `TransactionReplaying` per step,
  `SleepUntilWake`, `WorkflowCompleted`.
- Python app + engine log (tee'd): `/tmp/dbos-py.log` —
  `Recovering N workflows from application version ...`, `DBOS launched!`.
- Python lifecycle events: `/tmp/dbos-py-lifecycle.jsonl`; state sampler:
  `/tmp/dbos-py-states.jsonl`; pysnooper frames per transaction body:
  `/tmp/dbos-py-snoop-<step>.log`.
- Phase markers from both scenarios: `/tmp/dbos-markers.jsonl`.
- Rows: `widget_store.orders` (DISPATCHED, progress 0),
  `widget_store.products.inventory`, `widget_store.transaction_completion`
  (13 rows, kept); Python `orders`, `dbos.workflow_status`,
  `dbos.datasource_outputs` (peak 9, dropped on completion).
- Screenshots from the Chrome flow: `/tmp/widget-hs-{1-store,2-payment,3-dispatched}.png`.

## Lifecycle evaluation (2026-10-02)

The Python run was instrumented to emit one lifecycle event per engine seam
(launch, recovery scan/start, workflow start/complete, step call/return, the
outer step checkpoint, the datasource pre-check and its in-transaction write,
the durable messaging set, `sleep`); the Haskell run drove the UI through a
browser with the same phases (Buy → payment → Confirm → two ticks → Crash →
restart → DISPATCHED). The driver scripts were throwaway and live outside the
repo; the comparison checked 27 phase/database assertions and the report is
`docs/widget-store-lifecycle-eval.md` — **27/27 passed**.

The mirror table in the report lines up, phase for phase:

| phase | Python lifecycle event | Haskell tracer |
| --- | --- | --- |
| recovery scan | `recovery.scan` (`get_pending_workflows`) | `EngineRecovered workflows=N` |
| checkout steps | `step.call create_order/reserve_inventory/update_order_status` | `TransactionRunning create_order/reserve_inventory/mark_order_paid` |
| commit | `checkpoint.record` (same tx) | `TransactionOutputRecorded` |
| dispatch | `step.call update_order_progress`, `sleep.call` | `TransactionRunning update_order_progress`, `SleepUntilWake` |
| replay after crash | `step.replay` (outer checkpoint; ids 1–5 incl. sleeps) | `TransactionReplaying update_order_progress (1), (3)` |
| completion | `workflow.complete` | `WorkflowCompleted` |

Two documented storage differences, no behavioral difference: the checkpoint
table name (`widget_store.transaction_completion` vs
`dbos.datasource_outputs`), and Python dropping a workflow's checkpoint rows
on completion while the port keeps them (clearing started workflows is the
recorded follow-up).

### Browser-run evidence

- The UI run extracted the server-rendered idempotency key from the Buy
  button, bought and paid through the panels, crashed at progress 8 (two
  ticks landed), and saw the recovered page reach DISPATCHED.
- Console: only the Tailwind CDN production warning plus the
  `ERR_CONNECTION_REFUSED` / htmx fetch errors during the crash window
  (expected — polling resumes after the restart loop).
- Chrome inspection also caught the duplicated `[hsx|` opener that rendered
  literally; fixed and re-verified.



## Environment

- Haskell app: `$DBOS_DATABASE_URL`, schema `widget_store` in the shared DB.
- Python app: `widget_store_py` database (own system schema), port 8000;
  seeded `public.products`/`public.orders`.
