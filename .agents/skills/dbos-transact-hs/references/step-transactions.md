---
title: Use Transactional Steps for Database Operations
impact: HIGH
impactDescription: Transactions provide exactly-once database execution within workflows
tags: step, transaction, database, datasource
---

## Use Transactional Steps for Database Operations

Use `runTxStep` for application-database writes. The application write and the checkpoint row (`<schema>.transaction_completion`) commit in one transaction, so the step is exactly-once across crashes and retries. Never use plain `runStep` for writes that must be atomic with their checkpoint.

**Incorrect (raw query in workflow body):**

```haskell
myBody () _wctx = do
  -- Not checkpointed, not atomic: replay double-inserts.
  runAppSession app (\tx -> insertOrder tx)
```

**Correct (transactional step):**

```haskell
orderId <- ExceptT (runTxStep ds TransactionConfig { txName = Just "create_order", txIsolation = Just Serializable } wctx (\sctx tx ->
  Right <$> insertOrder sctx tx))
```

Setup (before `launch`):

```haskell
app <- acquireAppDataSourceInFromEnv "my_schema" config.configDatabaseUrl 5
runAppSession app createSchemaSession >>= either die pure
verifyAppDataSource app >>= either die pure
let ds = toDataSource app
registerDBOSDataSource dbos ds >>= either die pure
```

Rules: name the step the way the oracle names it (`txName = Just "create_order"`); pick `Serializable` when the guard depends on it (transient serialization failures retry with backoff). `runTxStep` requires a registered `DataSource`, refuses inside a step body, and replays recorded outcomes without running the body. Outside workflows use `runTxOutside ds txConfig body`.

Difference from TS/Rust: TS uses per-ORM datasource packages (`KnexDataSource.runTransaction`) with `transaction_completion` installed via `initializeDBOSSchema`. Haskell uses one `DataSource` seam (`acquireAppDataSourceInFromEnv`, `toDataSource`, `runTxStep`/`runTxOutside`) over `Tx`, per ADR-0021. Keep checkpoint payloads plain (`Int`, `Text`); unwrap newtypes at the transaction edge.
