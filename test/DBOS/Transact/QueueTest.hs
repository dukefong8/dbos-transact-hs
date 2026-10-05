{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Queue engine types and the resolution boundary for rows written by
-- older SDKs.
module DBOS.Transact.QueueTest (tests) where

import Data.Text (Text)
import Data.Text qualified as Text
import Data.Map.Strict qualified as Map
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import DBOS.SystemDB (AwaitedOutcome (..), Change (..), NewQueue (..), OnExistingQueue (..), QueueName (..), SystemDB (getQueue, upsertQueue), WorkflowInitResult (..), WorkflowRecord (..), WorkflowStatus (..), getWorkflow, internalQueueName, newQueue, secondsDuration)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (CodecError, Config (..), DBOS, WorkflowCtx, Executor, DuplicationPolicy (..), EngineOnly, Enqueue (..), Environment (..), Error (..), Queue (..), QueueChange (..), QueueConflict (..), QueueOptions (..), Serialization (..), SerializedWorkflowValue (..),
 StartOptions (..), WorkflowId (..),
 WorkflowKey,
 WorkflowRef,
 WorkflowHandle (..), configFromEnv, decodeWorkflowValue, defaultQueueOptions, encodeWorkflowValue, enqueueDBOSWorkflow, enqueueNew, handleResult, handleStatus,
 launchWithEnvironment,
 newDBOS, newWorkflowKey, nullTracer, registerDBOSWorkflowRef, registerDBOSWorkflow, registerQueue, renderTransactError, retrieveWorkflow, shutdown, startDBOSWorkflowRef, startOptionsDefault, updateQueue, waitForWorkflow)
import DBOS.Transact.Queue (defaultQueueChange)
import DBOS.Transact.QueueCases
  ( QueueFixture (..),
    checkBadEnqueue,
    checkCountedPartitioned,
    checkDedup,
    checkDelayed,
    checkJoin,
    checkListenInternal,
    checkListenNarrow,
    checkListenNone,
    checkPartitioned,
    checkPeerQueue,
    checkPriority,
    checkUpdateHonoured,
    checkWorkerConcurrency,
    checkCrud,
    checkDeadlineStamped,
    checkEqualLimits,
    checkGhostQueue,
    checkIncoherent,
    checkInheritedDeadline,
    checkInternalRow,
    checkLateQueue,
    checkLegacyRescope,
    checkLegacyUpdateRefused,
    checkNoDeadlineYet,
    checkPartitionLimits,
    checkPartitionRow,
    checkQueueDefaults,
    checkRateLimit,
    checkReregister,
    checkReserved,
    checkSentinel,
    checkUnhonourable,
    checkUnlaunched,
    checkUpdateCoherent,
    scenarioBadEnqueue,
    scenarioCountedPartitioned,
    scenarioDedup,
    scenarioDelayed,
    scenarioJoin,
    scenarioListenInternal,
    scenarioListenNarrow,
    scenarioListenNone,
    scenarioPartitioned,
    scenarioPeerQueue,
    scenarioPriority,
    scenarioUpdateHonoured,
    scenarioWorkerConcurrency,
    scenarioCrud,
    scenarioDeadlineStamped,
    scenarioEqualLimits,
    scenarioGhostQueue,
    scenarioIncoherent,
    scenarioInheritedDeadline,
    scenarioInternalRow,
    scenarioLateQueue,
    scenarioLegacyRescope,
    scenarioLegacyUpdateRefused,
    scenarioNoDeadlineYet,
    scenarioPartitionLimits,
    scenarioPartitionRow,
    scenarioQueueDefaults,
    scenarioRateLimit,
    scenarioReregister,
    scenarioReserved,
    scenarioSentinel,
    scenarioUnhonourable,
    scenarioUnlaunched,
    scenarioUpdateCoherent,
  )
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

-- | Launch over the isolated environment and hand back the executor:
-- the one-call form of @launchWithEnvironment@ plus unwrap.
launchQueueExec :: DBOS IO -> Environment -> IO (Executor IO)
launchQueueExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

-- | One fixture per leaf: a fresh instance over a fresh identity (bracketed
-- around the leaf), launched on demand per scenario, with raw queue-row
-- access over the suite backend so stored-row assertions hold. The listen
-- filter is derived from the leaf's suffix, so filtered supervisors and
-- their scenarios agree on queue names by construction.
mkQueueFixture :: Postgres.PostgresSystemDB -> (Text -> Maybe [Text]) -> (QueueFixture IO -> IO a) -> IO a
mkQueueFixture backend listenOf run = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      appName = "hs-l2-queue-" <> Text.take 12 suffix
  config0 <- configFromEnv appName
  let config =
        config0
          { configAppVersion = Just ("v-" <> suffix),
            configExecutorId = Just ("exec-" <> suffix),
            configListenQueues = listenOf suffix
          }
  bracket (newDBOS config) shutdown $ \dbos ->
    run
      QueueFixture
        { qfSuffix = suffix,
          qfAppName = appName,
          qfDBOS = dbos,
          qfLaunch = launchQueueExec dbos isolatedEnvironment,
          qfShutdown = shutdown dbos,
          qfUpsertQueue = \new onExisting -> upsertQueue backend new onExisting,
          qfReadQueueRow = \name -> do
            found <- getQueue backend name
            case found of
              Left err -> fail (show err)
              Right record -> pure record,
          qfReadWorkflowRow = \wid -> do
            found <- getWorkflow backend wid
            case found of
              Left err -> fail (show err)
              Right record -> pure record
        }

-- | One framed leaf over the per-leaf fixture. Most cases drain every
-- queue; filtered supervisors pass their derivation.
leaf :: IO Postgres.PostgresSystemDB -> (Text -> Maybe [Text]) -> String -> (QueueFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
leaf getBackend listenOf name scen judge = liveCaseWith (\run -> getBackend >>= \backend -> mkQueueFixture backend listenOf run) name scen judge

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Workflow queues"
    [ leaf getBackend (const Nothing) "queue options default to no limits and poll once a second" scenarioQueueDefaults checkQueueDefaults,
      leaf getBackend (const Nothing) "a legacy-partitioned row re-scopes its limits" scenarioLegacyRescope checkLegacyRescope,
      leaf getBackend (const Nothing) "a registered queue updates, lists and deletes through the instance" scenarioCrud checkCrud,
      -- IO only: the fixture rewrites the row through raw SQL into the
      -- pre-109 shape (input moved into the status column, payload-table
      -- row dropped), and MemSystemDB keeps no separate workflow_input
      -- table to rewrite.
      testCase "a queued workflow with a legacy input runs with it" $ do
        fresh <- UUID.V4.nextRandom
        backend <- getBackend
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-queue-legacy-" <> Text.take 12 suffix
            queueName = "hs-l2-legacy-" <> Text.take 12 suffix
            version = "hs-l2-version-" <> suffix
            workflowText = "hs-l2-enqueue-legacy-" <> suffix
            workflowId = WorkflowId workflowText
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          let key = newWorkflowKey "doubles"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right (input * 2))
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          enqueued <- enqueueDBOSWorkflow dbos key workflowId (Just (encodeWorkflowValue (21 :: Int))) queueName
          case enqueued of
            Left err -> fail (show err)
            Right result -> result.initResultStatus @?= Enqueued
          -- Make the row legacy: move its input into the status column and
          -- drop the payload-table row, as a pre-109 writer left it.
          moved <-
            Postgres.runSession backend "fixture" $
              Session.statement workflowText $
                Statement.preparable
                  "update dbos.workflow_status set inputs = (select inputs from dbos.workflow_input where workflow_uuid = $1) where workflow_uuid = $1"
                  (Encoders.param (Encoders.nonNullable Encoders.text))
                  Decoders.rowsAffected
          case moved of
            Left err -> fail (show err)
            Right 1 -> pure ()
            Right n -> fail ("expected one moved row, got: " <> show n)
          dropped <-
            Postgres.runSession backend "fixture" $
              Session.statement workflowText $
                Statement.preparable
                  "delete from dbos.workflow_input where workflow_uuid = $1"
                  (Encoders.param (Encoders.nonNullable Encoders.text))
                  Decoders.rowsAffected
          case dropped of
            Left err -> fail (show err)
            Right 1 -> pure ()
            Right n -> fail ("expected one dropped row, got: " <> show n)
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          waited <- waitForWorkflow dbos workflowId
          case waited of
            Left err -> fail (show err)
            Right (AwaitedSucceeded (Just output) serialization) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the legacy input reaches the body" (Right 42) decoded
            Right other -> fail (show other),
      leaf getBackend (const Nothing) "a queue's worker concurrency runs that many at once in one process" scenarioWorkerConcurrency checkWorkerConcurrency,
      leaf getBackend ((\s -> Just ["hs-l2-listen-fast-" <> Text.take 12 s])) "listen queues narrow what this process dequeues" scenarioListenNarrow checkListenNarrow,
      leaf getBackend ((const (Just []))) "an empty listen set dequeues from no registered queue" scenarioListenNone checkListenNone,
      leaf getBackend ((\s -> Just ["hs-l2-listen-other-" <> Text.take 12 s])) "listen queues never exclude the internal queue" scenarioListenInternal checkListenInternal,
      leaf getBackend (const Nothing) "the internal queue name is reserved" scenarioReserved checkReserved,
      leaf getBackend (const Nothing) "registering before launch is refused" scenarioUnlaunched checkUnlaunched,
      leaf getBackend (const Nothing) "incoherent limits are refused before they reach the row" scenarioIncoherent checkIncoherent,
      leaf getBackend (const Nothing) "an update cannot leave a queue incoherent" scenarioUpdateCoherent checkUpdateCoherent,
      leaf getBackend (const Nothing) "a dequeue stamps the deadline an enqueue left open" scenarioDeadlineStamped checkDeadlineStamped,
      leaf getBackend (const Nothing) "an explicit timeout on a queued workflow records no deadline yet" scenarioNoDeadlineYet checkNoDeadlineYet,
      leaf getBackend (const Nothing) "a partition key is recorded on the row" scenarioPartitionRow checkPartitionRow,
      leaf getBackend (const Nothing) "an unprioritised workflow stores the sentinel" scenarioSentinel checkSentinel,
      leaf getBackend (const Nothing) "an incoherent enqueue is refused" scenarioBadEnqueue checkBadEnqueue,
      leaf getBackend (const Nothing) "a delayed enqueue waits before it is dequeued" scenarioDelayed checkDelayed,
      leaf getBackend (const Nothing) "a deduplication id admits one waiting workflow" scenarioDedup checkDedup,
      leaf getBackend (const Nothing) "return existing joins the workflow holding the key" scenarioJoin checkJoin,
      leaf getBackend (const Nothing) "priority orders the backlog lower first" scenarioPriority checkPriority,
      leaf getBackend (const Nothing) "updating a queue changes what a running worker honours" scenarioUpdateHonoured checkUpdateHonoured,
      leaf getBackend (const Nothing) "a partitioned queue runs one workflow per key at a time" scenarioPartitioned checkPartitioned,
      leaf getBackend (const Nothing) "a counted partitioned queue runs its limit per key" scenarioCountedPartitioned checkCountedPartitioned,
      leaf getBackend (const Nothing) "a stored row cannot redefine the internal queue" scenarioInternalRow checkInternalRow,
      leaf getBackend (const Nothing) "another application's queue is not dequeued from" scenarioPeerQueue checkPeerQueue,
      leaf getBackend (const Nothing) "an unhonourable queue configuration is refused" scenarioUnhonourable checkUnhonourable,
      leaf getBackend (const Nothing) "a per-process limit may equal the fleet limit" scenarioEqualLimits checkEqualLimits,
      leaf getBackend (const Nothing) "adding a per-partition limit to a legacy row is refused" scenarioLegacyUpdateRefused checkLegacyUpdateRefused,
      leaf getBackend (const Nothing) "a queue registered after launch is dequeued from" scenarioLateQueue checkLateQueue,
      leaf getBackend (const Nothing) "a queue this process never registered is dequeued from" scenarioGhostQueue checkGhostQueue,
      leaf getBackend (const Nothing) "an inherited deadline reaches a queued child" scenarioInheritedDeadline checkInheritedDeadline,
      leaf getBackend (const Nothing) "a queue carries a rate limit and priority ordering" scenarioRateLimit checkRateLimit,
      leaf getBackend (const Nothing) "per-partition limits partition a queue" scenarioPartitionLimits checkPartitionLimits,
      leaf getBackend (const Nothing) "re-registering updates the stored limits" scenarioReregister checkReregister
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
retrieveWf :: DBOS IO -> WorkflowId -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
retrieveWf = retrieveWorkflow

resultWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

-- | Polls a condition until it holds or the budget runs out.
pollUntil :: Int -> IO Bool -> IO Bool
pollUntil remaining check
  | remaining <= 0 = check
  | otherwise = do
      ok <- check
      if ok
        then pure True
        else threadDelay 100000 >> pollUntil (remaining - 100000) check

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | Register a body at the engine-only channel: the polymorphic
-- registration cannot infer the JSON types from a local binding.
registerRefOf :: DBOS IO -> WorkflowKey -> (forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)) -> IO (Either (Error EngineOnly) (WorkflowRef IO EngineOnly))
registerRefOf = registerDBOSWorkflowRef

-- | Register a @Text -> Text@ body at the engine-only channel.
registerTextRefOf :: DBOS IO -> WorkflowKey -> (forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)) -> IO (Either (Error EngineOnly) (WorkflowRef IO EngineOnly))
registerTextRefOf = registerDBOSWorkflowRef

-- | Register a @() -> Text@ body at the engine-only channel.
registerUnitTextRefOf :: DBOS IO -> WorkflowKey -> (forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)) -> IO (Either (Error EngineOnly) (WorkflowRef IO EngineOnly))
registerUnitTextRefOf = registerDBOSWorkflowRef
