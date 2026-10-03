{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The widget store over the live Postgres binding: real app tables in a
-- per-case schema and the real transactional-step engine. Mirrors the
-- Python widget-store oracle (probe 2026-10-02): a paid order walks
-- @PENDING(0) -> PAID(2) -> DISPATCHED(1)@ with inventory @5 -> 4@; a
-- refused payment walks @PENDING(0) -> CANCELLED(-1)@ with inventory
-- restored. The crash cases are IO-only (crash-and-relaunch recovery
-- sweep, per ADR-0020): a checkout interrupted while parked on @recv@
-- replays its recorded steps without duplicating the order, and a
-- dispatch interrupted mid-tick resumes so exactly three ticks land.
module DBOS.Transact.WidgetTest (tests) where

import DBOS.Prelude
import Control.Monad.Except (ExceptT (..), runExceptT)
import Control.Monad (when)
import Data.Functor.Contravariant (contramap)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Transact
  ( AppDataSource,
    Config (..),
    Ctx,
    DBOS,
    DataSource,
    EngineOnly,
    Environment (..),
    Error (..),
    StartOptions (..),
    Topic (..),
    TransactionConfig (..),
    Tx (..),
    WorkflowId (..),
    WorkflowRef,
    acquireAppDataSourceIn,
    acquireAppDataSourceInFromEnv,
    configFromEnv,
    encodeWorkflowValue,
    getWorkflowEvent,
    launchWithEnvironment,
    millisDuration,
    newDBOS,
    newWorkflowKey,
    recv,
    registerDBOSDataSource,
    registerDBOSWorkflowRef,
    releaseAppDataSource,
    runAppSession,
    runTransaction,
    sendWorkflowMessage,
    setEvent,
    shutdown,
    sleepWorkflowStep,
    startChildWorkflow,
    startDBOSWorkflowRef,
    startOptionsDefault,
    toDataSource,
  )
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase)

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
    wtCheckpoints :: Statement.Statement Text Int64
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
          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))
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

-- * The workflows

widgetConfig :: TransactionConfig
widgetConfig = TransactionConfig {txName = Just "widget_step", txIsolation = Nothing}

-- | One app transaction at the engine's engine-only channel.
widgetStep :: DataSource IO -> Ctx IO -> (Tx IO -> IO ()) -> IO (Either (Error EngineOnly) ())
widgetStep ds ctx action = runTransaction ds ctx widgetConfig (\tx -> Right <$> action tx)

createOrderTx :: WidgetTables -> Tx IO -> IO Int
createOrderTx tables (Tx run) = fromIntegral <$> run tables.wtCreateOrder ()

reserveInventoryTx :: WidgetTables -> Tx IO -> IO Bool
reserveInventoryTx tables (Tx run) = do
  affected <- run tables.wtReserve ()
  pure (affected > 0)

undoReserveTx :: WidgetTables -> Tx IO -> IO ()
undoReserveTx tables (Tx run) = run tables.wtUndoReserve ()

setStatusTx :: WidgetTables -> Int -> Int -> Tx IO -> IO ()
setStatusTx tables orderId status (Tx run) =
  run tables.wtSetStatus (fromIntegral orderId, fromIntegral status)

tickOrderTx :: WidgetTables -> Int -> Tx IO -> IO ()
tickOrderTx tables orderId (Tx run) = do
  remaining <- run tables.wtTick (fromIntegral orderId)
  when (remaining <= 0) $
    run tables.wtSetStatus (fromIntegral orderId, 1)

-- | The checkout workflow: create, reserve, publish the payment id, wait
-- for the payment, then dispatch or compensate. Mirrors the oracle's
-- @checkout_workflow@.
checkoutBody :: DataSource IO -> WorkflowRef IO EngineOnly -> WidgetTables -> () -> Ctx IO -> IO (Either (Error EngineOnly) Text)
checkoutBody ds dispatchRef tables () ctx = runExceptT $ do
  orderId <- ExceptT (runTransaction ds ctx widgetConfig (\tx -> Right <$> createOrderTx tables tx))
  onShelf <- ExceptT (runTransaction ds ctx widgetConfig (\tx -> Right <$> reserveInventoryTx tables tx))
  if not onShelf
    then do
      _ <- ExceptT (widgetStep ds ctx (\tx -> setStatusTx tables orderId (-1) tx))
      _ <- ExceptT (setEvent ctx "payment_id" (Nothing :: Maybe Text))
      pure "no-inventory"
    else do
      _ <- ExceptT (setEvent ctx "payment_id" (Just (Text.pack (show orderId))))
      ExceptT (recv ctx (Just (Topic "payment_status")) (millisDuration 30000) :: IO (Either (Error EngineOnly) (Maybe Text))) >>= \case
        Just status | status == "paid" -> do
          _ <- ExceptT (widgetStep ds ctx (\tx -> setStatusTx tables orderId 2 tx))
          _ <- ExceptT (startChildWorkflow ctx dispatchRef startOptionsDefault (Just (encodeWorkflowValue orderId)))
          _ <- ExceptT (setEvent ctx "order_id" (Text.pack (show orderId)))
          pure "paid"
        _ -> do
          _ <- ExceptT (widgetStep ds ctx (\tx -> undoReserveTx tables tx))
          _ <- ExceptT (widgetStep ds ctx (\tx -> setStatusTx tables orderId (-1) tx))
          _ <- ExceptT (setEvent ctx "order_id" (Text.pack (show orderId)))
          pure "cancelled"

-- | The dispatch workflow: three one-second ticks, the oracle's durable
-- sleep loop.
dispatchBody :: DataSource IO -> WidgetTables -> Int -> Ctx IO -> IO (Either (Error EngineOnly) Text)
dispatchBody ds tables orderId ctx = go (3 :: Int)
  where
    go 0 = pure (Right "dispatched")
    go n = do
      slept <- sleepWorkflowStep ctx (millisDuration 1000)
      case slept of
        Left err -> pure (Left err)
        Right () -> do
          _ <- widgetStep ds ctx (\tx -> tickOrderTx tables orderId tx)
          go (n - 1)

-- * Fixture

data WidgetFixture = WidgetFixture
  { wfDbos :: DBOS IO,
    wfApp :: AppDataSource,
    wfTables :: WidgetTables,
    wfCheckout :: WorkflowRef IO EngineOnly,
    wfSchema :: Text
  }

acquireWidgetFixture :: IO WidgetFixture
acquireWidgetFixture = do
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
  -- The app datasource reads the app's own database: @DATABASE_URL@ when
  -- set, else the system URL.
  app <- acquireAppDataSourceInFromEnv schema config0.configDatabaseUrl 2
  tables <- pure (widgetTables schema)
  createWidgetSchema app tables
  dbos <- newDBOS config
  let ds = toDataSource app
  _ <- registerDBOSDataSource dbos ds >>= either (fail . show) pure
  dispatchRef <-
    registerDBOSWorkflowRef dbos (newWorkflowKey "DispatchOrderWorkflow") (dispatchBody ds tables)
      >>= either (fail . show) pure
  checkoutRef <-
    registerDBOSWorkflowRef dbos (newWorkflowKey "CheckoutWorkflow") (checkoutBody ds dispatchRef tables)
      >>= either (fail . show) pure
  pure
    WidgetFixture
      { wfDbos = dbos,
        wfApp = app,
        wfTables = tables,
        wfCheckout = checkoutRef,
        wfSchema = schema
      }

releaseWidgetFixture :: WidgetFixture -> IO ()
releaseWidgetFixture wf = do
  shutdown wf.wfDbos
  dropWidgetSchema wf.wfApp wf.wfTables
  releaseAppDataSource wf.wfApp

withWidgetFixture :: (WidgetFixture -> IO a) -> IO a
withWidgetFixture = bracket acquireWidgetFixture releaseWidgetFixture

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

launchWidget :: WidgetFixture -> IO ()
launchWidget wf = do
  launched <- launchWithEnvironment wf.wfDbos isolatedEnvironment
  either (fail . show) pure launched

-- * Observation helpers

pollFor :: Text -> Int -> IO Bool -> IO Bool
pollFor what attempts act = go attempts
  where
    go 0 = do
      _ <- fail ("timed out waiting for " <> Text.unpack what) :: IO ()
      pure False
    go n = do
      ok <- act
      if ok
        then pure True
        else threadDelay 100000 >> go (n - 1)

waitEvent :: WidgetFixture -> Text -> Text -> IO ()
waitEvent wf widText key = do
  _ <-
    pollFor ("event " <> key) 100 $ do
      found <- getWorkflowEvent wf.wfDbos (WorkflowId widText) key (millisDuration 0)
      pure (case found of Right (Just _) -> True; _ -> False)
  pure ()

readInventory :: WidgetFixture -> IO Int
readInventory wf = do
  value <- runAppSession wf.wfApp (Session.statement () wf.wfTables.wtInventory) >>= either (fail . show) pure
  pure (fromIntegral value)

readOrders :: WidgetFixture -> IO [(Int, Int, Int)]
readOrders wf = do
  rows <- runAppSession wf.wfApp (Session.statement () wf.wfTables.wtOrders) >>= either (fail . show) pure
  pure [(fromIntegral o, fromIntegral s, fromIntegral p) | (o, s, p) <- rows]

readCheckpoints :: WidgetFixture -> Text -> IO Int
readCheckpoints wf widText = do
  value <- runAppSession wf.wfApp (Session.statement widText wf.wfTables.wtCheckpoints) >>= either (fail . show) pure
  pure (fromIntegral value)

waitDispatched :: WidgetFixture -> IO ()
waitDispatched wf = do
  _ <-
    pollFor "the order to dispatch" 150 $ do
      orders <- readOrders wf
      pure (any (\(_, status, _) -> status == 1) orders)
  pure ()

-- * Cases

tests :: TestTree
tests =
  testGroup
    "Widget store (live)"
    [ testCase "a paid checkout over app tables dispatches the order" $ withWidgetFixture $ \wf -> do
        launchWidget wf
        let widText = "hs-widget-paid-" <> wf.wfSchema
        _ <- startDBOSWorkflowRef wf.wfDbos wf.wfCheckout (startOptionsDefault {startWorkflowId = Just widText}) Nothing
        waitEvent wf widText "payment_id"
        _ <- sendWorkflowMessage wf.wfDbos (WorkflowId widText) (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
        waitEvent wf widText "order_id"
        waitDispatched wf
        inventory <- readInventory wf
        orders <- readOrders wf
        checkpoints <- readCheckpoints wf widText
        assertEqual "inventory is down by one" 4 inventory
        assertEqual "the order is dispatched with no progress left" [(1, 1, 0)] orders
        assertBool "the datasource recorded checkpoints" (checkpoints > 0),
      testCase "a refused payment restores inventory and cancels the order" $ withWidgetFixture $ \wf -> do
        launchWidget wf
        let widText = "hs-widget-refused-" <> wf.wfSchema
        _ <- startDBOSWorkflowRef wf.wfDbos wf.wfCheckout (startOptionsDefault {startWorkflowId = Just widText}) Nothing
        waitEvent wf widText "payment_id"
        _ <- sendWorkflowMessage wf.wfDbos (WorkflowId widText) (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("failed" :: Text))
        waitEvent wf widText "order_id"
        inventory <- readInventory wf
        orders <- readOrders wf
        assertEqual "inventory is restored" 5 inventory
        assertEqual "the order is cancelled" [(1, -1, 3)] orders,
      -- IO only: crash-and-relaunch recovery sweep (ADR-0020).
      testCase "a crash while waiting for payment replays the reserved steps" $ withWidgetFixture $ \wf -> do
        launchWidget wf
        let widText = "hs-widget-crash-wait-" <> wf.wfSchema
        _ <- startDBOSWorkflowRef wf.wfDbos wf.wfCheckout (startOptionsDefault {startWorkflowId = Just widText}) Nothing
        waitEvent wf widText "payment_id"
        before <- (,,) <$> readInventory wf <*> readOrders wf <*> readCheckpoints wf widText
        assertEqual "reserved before the crash" (4, [(1, 0, 3)]) (let (i, o, _) = before in (i, o))
        shutdown wf.wfDbos
        launchWidget wf
        threadDelay 1000000
        afterRecovery <- (,,) <$> readInventory wf <*> readOrders wf <*> readCheckpoints wf widText
        assertEqual "the replay did not duplicate the order or the reservation" before afterRecovery
        _ <- sendWorkflowMessage wf.wfDbos (WorkflowId widText) (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
        waitEvent wf widText "order_id"
        waitDispatched wf
        inventory <- readInventory wf
        orders <- readOrders wf
        assertEqual "inventory is down by one" 4 inventory
        assertEqual "the order is dispatched" [(1, 1, 0)] orders,
      -- IO only: crash-and-relaunch recovery sweep (ADR-0020).
      testCase "a crash mid-dispatch resumes the remaining ticks" $ withWidgetFixture $ \wf -> do
        launchWidget wf
        let widText = "hs-widget-crash-dispatch-" <> wf.wfSchema
        _ <- startDBOSWorkflowRef wf.wfDbos wf.wfCheckout (startOptionsDefault {startWorkflowId = Just widText}) Nothing
        waitEvent wf widText "payment_id"
        _ <- sendWorkflowMessage wf.wfDbos (WorkflowId widText) (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
        waitEvent wf widText "order_id"
        _ <-
          pollFor "the first dispatch tick" 100 $ do
            orders <- readOrders wf
            pure (any (\(_, status, progress) -> status == 2 && progress <= 2) orders)
        shutdown wf.wfDbos
        launchWidget wf
        waitDispatched wf
        inventory <- readInventory wf
        orders <- readOrders wf
        assertEqual "inventory is down by one" 4 inventory
        assertEqual "exactly three ticks landed; the recorded one replayed" [(1, 1, 0)] orders
    ]
