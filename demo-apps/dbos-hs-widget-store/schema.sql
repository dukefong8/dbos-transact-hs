-- The widget store's own schema, shared with the Python, TypeScript, Go and
-- Rust ports (dbos-rust-widget-store/src/store.rs, create_schema).
--
-- The demo app creates this same schema at startup from the embedded copy of
-- this file (WidgetStore.Store), so `make widget-db` only exists to bootstrap
-- the compile-time typedSql describe: the quasiquoter connects to
-- $DATABASE_URL and PREPAREs every query at build time, so the tables must
-- exist before the first `cabal build` of the widget store.

CREATE SCHEMA IF NOT EXISTS widget_store;

CREATE TABLE IF NOT EXISTS widget_store.orders (
    order_id SERIAL PRIMARY KEY,
    order_status INTEGER NOT NULL,
    last_update_time TIMESTAMP DEFAULT now() NOT NULL,
    progress_remaining INTEGER DEFAULT 10 NOT NULL
);

CREATE TABLE IF NOT EXISTS widget_store.products (
    product_id SERIAL PRIMARY KEY,
    product VARCHAR(255) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    inventory INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL
);

INSERT INTO widget_store.products (product_id, product, description, inventory, price)
VALUES (1, 'Premium Quality Widget',
        'Enhance your productivity with our top-rated widgets!', 100, 99.99)
ON CONFLICT (product_id) DO NOTHING;

-- The transactional-step checkpoint table. Its shape is fixed by the oracles
-- (Python SQLAlchemyDatasource, TypeScript KnexDataSource): the step's output
-- and error are recorded in the same transaction as the application writes.
CREATE TABLE IF NOT EXISTS widget_store.transaction_completion (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    error TEXT,
    PRIMARY KEY (workflow_id, function_num)
);
