---
name: dbos-transact-hs
description: Use when building or changing a Haskell application on the dbos-transact-hs client facade (DBOS.Transact) — registering durable workflows and queues, writing step bodies (runStep/runStepWith, sleepStep, events, messages, child workflows), transactional steps over an app DataSource (runTxStep), launching an Executor, and testing apps live or in IOSim. Covers the facade-only import rule, step-body idioms, checkpoint semantics, and the demo apps as worked examples.
---

# Building DBOS apps in Haskell

`dbos-transact-hs` is a DBOS runtime: registered workflows are durable, replay
from checkpoints after a crash, and run through steps that are recorded exactly
once. This skill is for applications *using* the library; the repo's `AGENTS.md`
governs work *on* the port itself.

## Source of truth

- `src/DBOS/Transact.hs` — the client API **is** its export list. Import only
  from `DBOS.Transact` in app code; anything else is engine-internal.
- Worked examples: `demo-apps/dbos-hs-starter/` (queues, events, messages,
  durable waits) and `demo-apps/dbos-hs-widget-store/` (transactional steps
  with an application datasource).
- Deeper reading: `docs/widget-step-tables.md`,
  `docs/adr/0021-transactional-step-datasource.md`,
  `docs/cross-language-schema-interop.md`.

## Mental model

- **Workflows are registered before launch** and started by name or through a
  `WorkflowRef`. A body is an inline function
  `() -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) a)`.
- **Steps are the durability boundary.** `runStep wctx "name" body` records the
  result; on replay the body is skipped. Bodies must be deterministic — real
  side effects belong in steps under stable names (the step name is part of the
  durable identity; reordering or renaming steps changes replay).
- **Checkpoints**: ordinary steps write `dbos.operation_outputs`; transactional
  steps write the app schema's `.transaction_completion` row in the same
  transaction as the application write.
- **`WorkflowCtx` vs `StepCtx`**: a workflow view allocates step ids and starts
  children; a step view cannot (the type system refuses nested checkpointing).
  Calling a runner inside a step body degrades to a plain run (`runStep`) or is
  refused (`runTxStep`).

## App skeleton

```haskell
start = do
  config <- configFromEnv "my-app"                 -- DBOS_* environment variables
  dbos   <- newDBOS config
  ref    <- registerDBOSWorkflowRef dbos (newWorkflowKey "MyWorkflow") myBody
  exec   <- launch dbos                             -- or launchWithEnvironment for isolated identity
  ...
  pure (application, shutdown dbos)
```

With an application database (required for `runTxStep`):

```haskell
app <- acquireAppDataSourceInFromEnv "my_schema" config.configDatabaseUrl 5
runAppSession app createSchemaSession >>= either die pure
verifyAppDataSource app >>= either die pure
let ds = toDataSource app
registerDBOSDataSource dbos ds >>= either die pure   -- BEFORE launch: recovery starts inside it
```

Config knobs the demos set: `configAppVersion` (recovery only resumes its own
version, so give the app its own version string), `configExecutorId`, and
`configListenQueues = Just []` for an app that must not drain other apps'
queued rows in a shared database.

## Writing a body

Use `ExceptT` and let the first failure end the workflow (the oracles' `?`):

```haskell
myBody () wctx = runExceptT $ do
  ExceptT (runStep wctx "charge" (\_ -> chargeCard))
  ExceptT (sleepStep wctx (secondsDuration 5))                -- durable sleep
  ExceptT (setEvent wctx "progress" (1 :: Int))               -- durable event
  answer <- ExceptT (recv wctx (Just (Topic "approval")) timeout)
  ExceptT (startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue input)))
  pure answer
```

- Retries/timeouts: `runStepWith options wctx name body` with `StepOptions`
  (`maxAttempts`, `interval`/`backoffRate`, `timeout`, `preemptible`,
  `shouldRetry`).
- Races: `pendingStep`/`pendingStepWith` build the call (claiming its id), then
  `selectStep` races the arms in source order.
- Messages: `send`/`sendWith`/`sendBulk`/`sendBulkWith`, `recv` for an
  exclusive receiver (returns `Maybe Text`; `Nothing` on timeout).
- Child workflows: `startChildWorkflow` (start and forget) or `awaitChild` with
  a `WorkflowHandle`.
- Queues: `registerQueue dbos name (defaultQueueOptions { workerConcurrency = … }) NeverUpdate`,
  then `enqueueDBOSWorkflow dbos (newWorkflowKey name) workflowId input queueName`.
- Starting from HTTP handlers: hold the `WorkflowRef`s from registration and use
  `startDBOSWorkflowRef exec ref (startOptionsDefault { startWorkflowId = Just key }) input`.
  The idempotency key joins an already-running workflow instead of starting a
  second one.

## Transactional steps (app writes + checkpoint, one commit)

```haskell
orderId <- ExceptT (runTxStep ds (namedStep "create_order") wctx (\sctx tx ->
  Right . OrderId <$> (mySteps tx).createOrder sctx))
```

- The body receives `StepCtx` and the held `Tx`; run statements through it (or
  through a step table). The checkpoint row commits with the writes, so
  application tables are exactly-once across crashes and retries.
- `TransactionConfig { txName, txIsolation }`: name the step the way the oracle
  names it (`"create_order"`), and pick `Serializable` when the guard depends on
  it — transient serialization failures retry with backoff.
- `runTxStep` requires a registered `DataSource`, refuses inside a step (a
  nested transaction would checkpoint under the wrong id), and has an
  outside-workflow form: `runTxOutside ds txConfig body`.
- Replay returns recorded outcomes without running the body; recorded failures
  decode back to the same `Error`.

## Step tables (recommended seam for multi-step app work)

Define one record of step functions per workflow, keyed by `StepCtx`, plus
builders that close over the held `Tx` (live) or an STM store (tests), and pass
the builder into the body:

```haskell
data CheckoutSteps exec m = CheckoutSteps
  { coCreate :: StepCtx exec m -> m OrderId
  , coReserve :: StepCtx exec m -> m Bool
  }
pgCheckoutSteps :: Tx IO -> CheckoutSteps exec IO
```

This is what lets the live (Postgres) and simulated (STM) trees drive the same
bodies. Keep checkpoint payloads plain (`Int`, `Text`); unwrap newtypes at the
transaction edge.

## Testing an app

- Tasty; every test owns its rows — fresh UUIDs, unique workflow/queue names,
  per-test executor ids.
- Live: launch on the real database, assert the app tables *and*
  `dbos.workflow_status`/`operation_outputs`; cover replay (crash then relaunch)
  and refusal paths.
- IOSim: run the same bodies over STM-backed handlers and assert exact event
  traces.
- Tests import through `DBOS.Transact` only — hidden internals are not part of
  the app contract.

## Verification recipe

- `make db-migrate` (system schema), then `cabal build all`, then
  `cabal test all`.
- Evidence in psql: `dbos.workflow_status`, `dbos.operation_outputs` (step
  rows), `<schema>.transaction_completion` (transactional steps), plus the
  app's own tables.
- Demos: `PORT=8090 cabal run exe:demo-apps`, drive with
  `chrome-devtools-axi` (click by text via `eval`; refs go stale on
  auto-refresh), and stop the server before running tests.

## Pitfalls

- Registering datasources or workflows after launch is refused
  (created-before-launch rule); do it before `launch`.
- Using `runStep` for an app write that must be atomic with its checkpoint is
  wrong — that is what `runTxStep` exists for.
- Step bodies must be deterministic: no wall-clock reads or random values
  outside steps (use the deadline/timeout helpers, which are durable).
- Never import `DBOS.Internal.*`; the facade is the supported surface, and the
  engine may move beneath it.
