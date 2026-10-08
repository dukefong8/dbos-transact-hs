---
name: dbos-transact-hs
description: Haskell port of the DBOS durable-workflow SDK. Use when writing Haskell code with DBOS.Transact, creating workflows and steps, using queues, using Client from external applications, or building applications that need to be resilient to failures.
license: MIT
metadata:
  author: dbos
  version: "2.0.0"
  organization: DBOS
  date: October 2026
  abstract: Guide for building fault-tolerant Haskell applications with DBOS.Transact durable workflows. Covers configuration, workflows, steps, transactional steps, queues, communication, and testing.
---

# DBOS Haskell Best Practices

Guide for building reliable, fault-tolerant Haskell applications with `DBOS.Transact` durable workflows. This skill is for applications *using* the library; the repo's `AGENTS.md` governs work *on* the port itself.

Source of truth: `src/DBOS/Transact.hs` — the client API **is** its export list. Import only from `DBOS.Transact` in app code; anything else is engine-internal (private `dbos-transact-internals` lib).

## When to Apply

Reference these guidelines when:

- Adding DBOS to existing Haskell code
- Creating workflows and steps
- Using queues for concurrency control
- Implementing workflow communication (events, messages)
- Configuring and launching DBOS applications
- Using `Client` from external applications
- Testing DBOS applications

## Rule Categories by Priority

| Priority | Category | Impact | Prefix |
|----------|----------|--------|--------|
| 1 | Lifecycle | CRITICAL | `lifecycle-` |
| 2 | Workflow | CRITICAL | `workflow-` |
| 3 | Step | HIGH | `step-` |
| 4 | Queue | HIGH | `queue-` |
| 5 | Communication | MEDIUM | `comm-` |
| 6 | Pattern | MEDIUM | `pattern-` |
| 7 | Testing | LOW-MEDIUM | `test-` |
| 8 | Client | MEDIUM | `client-` |
| 9 | Advanced | LOW | `advanced-` |

## Critical Rules

### DBOS Configuration and Launch

A DBOS application MUST build a `Config`, create `DBOS`, register workflows before `launch`, and `shutdown` on exit:

```haskell
import DBOS.Transact (configFromEnv, launch, newDBOS, newWorkflowKey, registerWorkflowRef, shutdown)

main :: IO ()
main = do
  config <- configFromEnv "my-app"          -- DBOS_* environment variables
  dbos   <- newDBOS config
  _ref   <- registerWorkflowRef dbos (newWorkflowKey "MyWorkflow") myBody
  exec   <- launch dbos
  -- run workflows via exec ...
  shutdown dbos
```

With an application database (required for `runTxStep`):

```haskell
app <- acquireAppDataSourceInFromEnv "my_schema" config.configDatabaseUrl 5
runAppSession app createSchemaSession >>= either die pure
verifyAppDataSource app >>= either die pure
let ds = toDataSource app
registerDataSource dbos ds >>= either die pure   -- BEFORE launch: recovery starts inside it
```

When creating a new application, set `configAppVersion` to `Just "0.1.0"`. Recovery only resumes its own version. `configExecutorId` isolates processes; `configListenQueues = Just []` makes a process dequeue nothing (see `references/queue-listening.md` and `references/lifecycle-config.md`).

### Workflow and Step Structure

Workflows are comprised of steps. Any function performing complex operations or accessing external services must run as a step using `runStep`:

```haskell
myBody () wctx = runExceptT $ do
  resp <- ExceptT (runStep wctx "fetchData" (\_ -> fetch "https://api.example.com/data"))
  ExceptT (sleepStep wctx (secondsDuration 5))                -- durable sleep
  ExceptT (setEvent wctx "progress" (1 :: Int))               -- durable event
  answer <- ExceptT (recv wctx (Just (Topic "approval")) (secondsDuration 60))
  ExceptT (startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue input)))
  pure answer
```

Let the first failure end the workflow (`ExceptT`, the oracle's `?`). The step name is part of the durable identity; reordering or renaming steps changes replay.

### Transactional Steps (app writes + checkpoint, one commit)

Use `runTxStep` for application-database writes — never plain `runStep` for writes that must be atomic with their checkpoint:

```haskell
orderId <- ExceptT (runTxStep ds TransactionConfig { txName = Just "create_order", txIsolation = Just Serializable } wctx (\sctx tx ->
  Right <$> insertOrder sctx tx))
```

The body receives `StepCtx` and the held `Tx`; the checkpoint row commits with the writes, so app tables are exactly-once across crashes and retries. `runTxStep` requires a registered `DataSource`, refuses inside a step, and has an outside-workflow form `runTxOutside`. See `references/step-transactions.md`.

Define one record of step functions per workflow, keyed by `StepCtx`, plus builders closing over the held `Tx` (live) or an STM store (tests) — the step-table seam that lets live (Postgres) and simulated (STM) trees drive the same bodies. Keep checkpoint payloads plain (`Int`, `Text`); unwrap newtypes at the transaction edge.

### Key Constraints

- Do NOT call, start, or enqueue workflows from within steps
- Do NOT use uncontrolled concurrency to start workflows — use `startWorkflow` or queues
- Workflows MUST be deterministic — non-deterministic operations go in steps
- Do NOT mutate globals from workflows or steps
- Register workflows and datasources BEFORE `launch` — registering after is refused
- Steps are only durable when called from a workflow; outside a workflow they run as plain calls
- Never import engine internals in app code; the facade is the supported surface

## How to Use

Read individual rule files for detailed explanations and examples:

```
references/lifecycle-config.md
references/workflow-determinism.md
references/step-transactions.md
references/queue-concurrency.md
references/comm-messages.md
references/client-setup.md
```

If multiple applications share one system database, always set distinct app names plus `configListenQueues` and see `references/advanced-shared-database.md`.

## References

- Worked examples: `demo-apps/dbos-hs-starter/` (queues, events, messages, durable waits) and `demo-apps/dbos-hs-widget-store/` (transactional steps with an application datasource)
- Deeper reading: `docs/widget-step-tables.md`, `docs/adr/0021-transactional-step-datasource.md`, `docs/cross-language-schema-interop.md`
- TS oracle skill: `~/dev/dbos-agent-skills/skills/dbos-typescript/` (behavioral reference; Haskell behavior wins on differences noted per ref)
