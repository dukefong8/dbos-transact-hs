-- The outbox demo's own schema, mirroring the Python transactional-outbox
-- orders table (atomic_workflow.py / transactional_enqueue.py).
--
-- The demo app creates this same schema at startup from the embedded copy
-- (Outbox.Store), so bootstrapping it with psql only exists for the
-- compile-time typedSql describe: the quasiquoter connects to $DATABASE_URL
-- and PREPAREs every query at build time, so the tables must exist before
-- the first `cabal build` of the outbox demo.
CREATE SCHEMA IF NOT EXISTS outbox_store;

CREATE TABLE IF NOT EXISTS outbox_store.orders (
    order_id SERIAL PRIMARY KEY,
    customer TEXT NOT NULL,
    item TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    notification_status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

-- The transactional-step checkpoint table. The step's name, output and
-- error are recorded in the same transaction as the application writes,
-- and the name is checked on replay so a reordered or renamed transaction
-- is refused (DBOSUnexpectedStepError) rather than replayed.
--
-- `step_name` is Haskell-only (ADR-0021): TypeScript's
-- `transaction_completion` has no such column, and Python records
-- transactions in its own `datasource_outputs` table instead. Do not
-- point this app at a schema another SDK provisioned: every checkpoint
-- insert binds `step_name` and fails with undefined-column `42703` where
-- the column is missing.
CREATE TABLE IF NOT EXISTS outbox_store.transaction_completion (
    workflow_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    error TEXT,
    PRIMARY KEY (workflow_id, function_num)
);
