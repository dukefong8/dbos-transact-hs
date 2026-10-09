{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The widget store over the live Postgres binding — the live half of the
-- dual-stack mirror. The shared scenarios, fixture, and checks live in
-- "DBOS.Transact.WidgetCases"; this module owns the live interpretation: a
-- per-case schema, the real datasource and step handlers, the production
-- launch, and the SQL observations.
module DBOS.Transact.WidgetTest (tests) where

import Data.Int (Int32, Int64)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import DBOS.Transact
  ( AppDataSource,
    Config (..),
    DBOS,
    Environment (..),
    Executor,
    Tx (..),
    WorkflowId (..),
    acquireAppDataSourceInFromEnv,
    configFromEnv,
    handleStatus,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDataSource,
    registerWorkflowRef,
    releaseAppDataSource,
    retrieveWorkflow,
    runAppSession,
    shutdown,
    toDataSource)
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Context (firstStepStatus, nextWorkflowMarker, withStep, withWorkflow)
import DBOS.Transact.WidgetCases
  ( CheckoutSteps (..),
    lostAckOnce,
    DispatchSteps (..),
    OrderId (..),
    WidgetFixture (..),
    checkCannedPaidWriteRefused,
    checkCrashMidDispatch,
    captureCheckoutThread,
    captureDispatchThread,
    waitThread,
    checkCrashWhileWaiting,
    checkLostAck,
    checkTableCreate,
    checkTableFailing,
    checkTableReserveRace,
    checkTableStatusCodes,
    checkPaidCheckout,
    checkRefusedPayment,
    checkoutBody,
    dispatchBody,
    scenarioCannedPaidWriteRefused,
    scenarioCrashMidDispatch,
    scenarioCrashWhileWaiting,
    scenarioKilledMidDispatch,
    scenarioKilledWhileWaiting,
    scenarioLostAck,
    scenarioTableCreate,
    scenarioTableFailing,
    scenarioTableReserveRace,
    scenarioTableStatusCodes,
    scenarioPaidCheckout,
    scenarioRefusedPayment,
  )
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.ContextTest (connOver)
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Test.Tasty (TestTree, testGroup)

-- * App tables, per case

-- | The storefront's tables and the statements every workflow and
-- assertion runs against them. Built once per fixture over a unique
-- schema, so cases own their rows.
data WidgetTables = WidgetTables
  { wtSchema :: Text,
    wtReserve :: Statement.Statement () Int64,
    wtUndoReserve :: Statement.Statement () (),
    wtCreateOrder :: Statement.Statement () Int32,
    wtSetStatus :: Statement.Statement (Int32, Int32) (),
    wtTick :: Statement.Statement Int32 Int32,
    wtInventory :: Statement.Statement () Int32,
    wtOrders :: Statement.Statement () [(Int32, Int32, Int32)],
    wtCheckpoints :: Statement.Statement Text Int64,
    wtSetInventory :: Statement.Statement Int32 (),
    wtCommits :: Statement.Statement () [(Text, Text, Int64)]
  }

widgetTables :: Text -> WidgetTables
widgetTables schema =
  WidgetTables
    { wtSchema = schema,
      wtReserve =
        stmt
          ("UPDATE " <> q <> ".products SET inventory = inventory - 1 WHERE product_id = 1 AND inventory > 0")
          Encoders.noParams
          Decoders.rowsAffected,
      wtUndoReserve =
        stmt
          ("UPDATE " <> q <> ".products SET inventory = inventory + 1 WHERE product_id = 1")
          Encoders.noParams
          Decoders.noResult,
      wtCreateOrder =
        stmt
          ("INSERT INTO " <> q <> ".orders (order_status) VALUES (0) RETURNING order_id")
          Encoders.noParams
          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int4))),
      wtSetStatus =
        stmt
          ("UPDATE " <> q <> ".orders SET order_status = $2 WHERE order_id = $1")
          (contramap fst (Encoders.param (Encoders.nonNullable Encoders.int4)) <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.int4)))
          Decoders.noResult,
      wtTick =
        stmt
          ("UPDATE " <> q <> ".orders SET progress_remaining = progress_remaining - 1 WHERE order_id = $1 RETURNING progress_remaining")
          (Encoders.param (Encoders.nonNullable Encoders.int4))
          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int4))),
      wtInventory =
        stmt
          ("SELECT inventory FROM " <> q <> ".products WHERE product_id = 1")
          Encoders.noParams
          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int4))),
      wtOrders =
        stmt
          ("SELECT order_id, order_status, progress_remaining FROM " <> q <> ".orders ORDER BY order_id")
          Encoders.noParams
          ( Decoders.rowList
              ( (,,)
                  <$> Decoders.column (Decoders.nonNullable Decoders.int4)
                  <*> Decoders.column (Decoders.nonNullable Decoders.int4)
                  <*> Decoders.column (Decoders.nonNullable Decoders.int4)
              )
          ),
      wtCheckpoints =
        stmt
          ("SELECT COUNT(*) FROM " <> q <> ".transaction_completion WHERE workflow_id = $1")
          (Encoders.param (Encoders.nonNullable Encoders.text))
          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8))),
      wtSetInventory =
        stmt
          ("UPDATE " <> q <> ".products SET inventory = $1 WHERE product_id = 1")
          (Encoders.param (Encoders.nonNullable Encoders.int4))
          Decoders.noResult,
      wtCommits =
        stmt
          ("SELECT workflow_id, step_name, COUNT(*) FROM " <> q <> ".transaction_completion GROUP BY workflow_id, step_name")
          Encoders.noParams
          ( Decoders.rowList
              ( (,,)
                  <$> Decoders.column (Decoders.nonNullable Decoders.text)
                  <*> Decoders.column (Decoders.nonNullable Decoders.text)
                  <*> Decoders.column (Decoders.nonNullable Decoders.int8)
              )
          )
    }
  where
    q = quoteIdent schema

stmt :: Text -> Encoders.Params p -> Decoders.Result r -> Statement.Statement p r
stmt = Statement.preparable

-- | A test-owned schema name, quoted for interpolation. Unique per case.
quoteIdent :: Text -> Text
quoteIdent name = "\"" <> Text.replace "\"" "\"\"" name <> "\""

createWidgetSchema :: AppDataSource -> WidgetTables -> IO ()
createWidgetSchema app tables = do
  created <-
    runAppSession app $
      Session.script $
        mconcat
          [ "CREATE SCHEMA " <> q <> "; ",
            "CREATE TABLE " <> q <> ".orders (order_id SERIAL PRIMARY KEY, order_status INTEGER NOT NULL, progress_remaining INTEGER NOT NULL DEFAULT 3); ",
            "CREATE TABLE " <> q <> ".products (product_id SERIAL PRIMARY KEY, inventory INTEGER NOT NULL); ",
            "CREATE TABLE " <> q <> ".transaction_completion (workflow_id TEXT NOT NULL, step_name TEXT NOT NULL, function_num INT NOT NULL, output TEXT, error TEXT, PRIMARY KEY (workflow_id, function_num)); ",
            "INSERT INTO " <> q <> ".products (product_id, inventory) VALUES (1, 5);"
          ]
  either (fail . show) pure created
  where
    q = quoteIdent tables.wtSchema

dropWidgetSchema :: AppDataSource -> WidgetTables -> IO ()
dropWidgetSchema app tables = do
  dropped <- runAppSession app (Session.script ("DROP SCHEMA IF EXISTS " <> quoteIdent tables.wtSchema <> " CASCADE"))
  either (fail . show) pure dropped

-- * Step tables (live interpretation)

-- | PG handlers behind the held connection: each op runs on the step's own
-- 'Tx', so the application writes share the step's commit.
pgCheckoutSteps :: WidgetTables -> Tx IO -> CheckoutSteps exec IO
pgCheckoutSteps tables (Tx run) =
  CheckoutSteps
    { coCreate = \_ -> OrderId . fromIntegral <$> run tables.wtCreateOrder (),
      coReserve = \_ -> (> 0) <$> run tables.wtReserve (),
      coUndo = \_ -> run tables.wtUndoReserve (),
      coSetStatus = \_ (OrderId oid) status -> run tables.wtSetStatus (fromIntegral oid, fromIntegral status)
    }

pgDispatchSteps :: WidgetTables -> Tx IO -> DispatchSteps exec IO
pgDispatchSteps tables (Tx run) =
  DispatchSteps
    { doTick = \_ (OrderId oid) -> do
        remaining <- run tables.wtTick (fromIntegral oid)
        when (remaining <= 0) $
          run tables.wtSetStatus (fromIntegral oid, 1),
      doSetStatus = \_ (OrderId oid) status -> run tables.wtSetStatus (fromIntegral oid, fromIntegral status)
    }

-- | The canned paid-write refusal: the paid mark throws instead of
-- committing, so the checkout stops before the dispatch child.
failingPgCheckoutSteps :: WidgetTables -> Tx IO -> CheckoutSteps exec IO
failingPgCheckoutSteps tables tx =
  (pgCheckoutSteps tables tx)
    { coSetStatus = \s (OrderId oid) status ->
        if status == 2
          then throwIO (userError "mark_order_paid refused")
          else (pgCheckoutSteps tables tx).coSetStatus s (OrderId oid) status
    }

-- * The live fixture

-- | One live interpretation of the shared fixture: a per-case schema, the
-- real datasource, both checkout registrations, the production launch, and
-- SQL observations. The bracket owns the schema and the pool.
withWidgetFixture :: (WidgetFixture IO -> IO b) -> IO b
withWidgetFixture body = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.take 12 (Text.filter (/= '-') (Text.pack (UUID.toString fresh)))
      schema = "widget_" <> suffix
      appName = "hs-widget-" <> suffix
      appVersion = "hs-widget-version-" <> suffix
      executorId = "hs-widget-executor-" <> suffix
  config0 <- configFromEnv appName
  let config =
        config0
          { configAppVersion = Just appVersion,
            configExecutorId = Just executorId,
            -- A widget case never drains queues; listening to all (the
            -- default) would sweep other tests' queue fixtures.
            configListenQueues = Just []
          }
  -- The app datasource reads the app's own database: @APP_DATABASE_URL@
  -- when set, else the system URL.
  app <- acquireAppDataSourceInFromEnv schema config0.configDatabaseUrl 2
  tables <- pure (widgetTables schema)
  createWidgetSchema app tables
  dbos <- newDBOS config
  let ds = toDataSource app
  _ <- registerDataSource dbos ds >>= either (fail . show) pure
  checkoutTid <- newTVarIO Nothing
  dispatchTid <- newTVarIO Nothing
  dispatchRef <- registerWorkflowRef dbos (newWorkflowKey "DispatchOrderWorkflow") (dispatchBody ds (\tx -> captureDispatchThread dispatchTid (pgDispatchSteps tables tx))) >>= either (fail . show) pure
  checkoutRef <- registerWorkflowRef dbos (newWorkflowKey "CheckoutWorkflow") (checkoutBody ds (\tx -> captureCheckoutThread checkoutTid (pgCheckoutSteps tables tx)) dispatchRef) >>= either (fail . show) pure
  failingRef <- registerWorkflowRef dbos (newWorkflowKey "CheckoutCannedFailWorkflow") (checkoutBody ds (\tx -> failingPgCheckoutSteps tables tx) dispatchRef) >>= either (fail . show) pure
  acked <- newTVarIO 0
  let lostDs = lostAckOnce acked ds
  lostRef <- registerWorkflowRef dbos (newWorkflowKey "CheckoutLostAckWorkflow") (checkoutBody lostDs (\tx -> pgCheckoutSteps tables tx) dispatchRef) >>= either (fail . show) pure
  postgresConfig <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB postgresConfig nullTracer
  Postgres.activatePostgresSystemDB backend
  conn <- connOver backend nullTracer
  let tableIdentity =
        Identity
          { identityAppName = "widget-tables",
            identityAppVersion = "0.0.0",
            identityExecutorId = "widget-tables",
            identityAppId = ""
          }
      wf =
        WidgetFixture
          { wfDataSource = ds,
            wfDBOS = dbos,
            wfMkCheckout = \tx -> pgCheckoutSteps tables tx,
            wfMkDispatch = \tx -> pgDispatchSteps tables tx,
            wfMkFailingCheckout = \tx -> failingPgCheckoutSteps tables tx,
            wfCheckoutRef = checkoutRef,
            wfFailingCheckoutRef = failingRef,
            wfLostAckCheckoutRef = lostRef,
            wfLoseNextAck = atomically (writeTVar acked 1),
            wfCheckoutThread = waitThread "checkout" checkoutTid,
            wfDispatchThread = waitThread "dispatch" dispatchTid,
            wfLaunch = launchWidget dbos,
            wfRelaunch = launchWidget dbos,
            wfFreshWorkflowId = pure (WorkflowId ("hs-widget-" <> schema)),
            wfSetInventory = \n -> runAppSession app (Session.statement (fromIntegral (n :: Int) :: Int32) tables.wtSetInventory) >>= either (fail . show) pure,
            wfWithTableStep = \tableBody -> withWorkflow conn tableIdentity (WorkflowId "widget-tables") Nothing (\wctx -> nextWorkflowMarker wctx >>= \marker -> withStep wctx marker (firstStepStatus 0) tableBody),
            wfReadInventory = readInventory app tables,
            wfReadOrders = readOrders app tables,
            wfReadStepCommits = readCommits app tables,
            wfReadStatus = \wid -> do
              found <- retrieveWorkflow dbos wid
              case found of
                Left _ -> pure Nothing
                Right wfHandle -> either (const Nothing) id <$> handleStatus wfHandle
          }
  body wf `finally` do
    shutdown dbos
    dropWidgetSchema app tables
    releaseAppDataSource app
    Postgres.releasePostgresSystemDB backend

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

launchWidget :: DBOS IO -> IO (Executor IO)
launchWidget dbos = launchWithEnvironment dbos isolatedEnvironment >>= either (fail . show) pure

readInventory :: AppDataSource -> WidgetTables -> IO Int
readInventory app tables = do
  value <- runAppSession app (Session.statement () tables.wtInventory) >>= either (fail . show) pure
  pure (fromIntegral value)

readOrders :: AppDataSource -> WidgetTables -> IO [(Int, Int, Int)]
readOrders app tables = do
  rows <- runAppSession app (Session.statement () tables.wtOrders) >>= either (fail . show) pure
  pure [(fromIntegral o, fromIntegral s, fromIntegral p) | (o, s, p) <- rows]

-- | Commits per workflow text id and step name — the exactly-once instrument
-- on the live side (the sim fake's map is the mirror).
readCommits :: AppDataSource -> WidgetTables -> IO (Map Text (Map Text Int))
readCommits app tables = do
  rows <- runAppSession app (Session.statement () tables.wtCommits) >>= either (fail . show) pure
  pure (Map.fromListWith (Map.unionWith (+)) [(wid, Map.singleton name (fromIntegral n)) | (wid, name, n) <- rows])

-- * Cases

tests :: TestTree
tests =
  testGroup
    "Widget store (live)"
    [ liveCaseWith withWidgetFixture "a paid checkout dispatches the order and keeps inventory down" scenarioPaidCheckout checkPaidCheckout,
      liveCaseWith withWidgetFixture "a refused payment restores inventory and cancels the order" scenarioRefusedPayment checkRefusedPayment,
      liveCaseWith withWidgetFixture "a refused paid write stops the checkout instead of dispatching" scenarioCannedPaidWriteRefused checkCannedPaidWriteRefused,
      liveCaseWith withWidgetFixture "a crash while waiting for payment replays the reserved steps" scenarioCrashWhileWaiting checkCrashWhileWaiting,
      liveCaseWith withWidgetFixture "a crash mid-dispatch resumes the remaining ticks" scenarioCrashMidDispatch checkCrashMidDispatch,
      liveCaseWith withWidgetFixture "the create op mints ids in order" scenarioTableCreate checkTableCreate,
      liveCaseWith withWidgetFixture "the reserve op never oversells under a race" scenarioTableReserveRace checkTableReserveRace,
      liveCaseWith withWidgetFixture "the failing table's third call aborts whole" scenarioTableFailing checkTableFailing,
      liveCaseWith withWidgetFixture "the table status codes match the live assertions" scenarioTableStatusCodes checkTableStatusCodes,
      liveCaseWith withWidgetFixture "a lost acknowledgement replays the committed step instead of re-running it" scenarioLostAck checkLostAck,
      liveCaseWith withWidgetFixture "a killed checkout replays the reserved steps" scenarioKilledWhileWaiting checkCrashWhileWaiting,
      liveCaseWith withWidgetFixture "a killed dispatch resumes the remaining ticks" scenarioKilledMidDispatch checkCrashMidDispatch
    ]
