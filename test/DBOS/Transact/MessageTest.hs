{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Message send/receive behavior through the workflow API and live
-- SystemDB. Scenarios and checks live in 'DBOS.Transact.MessageCases'
-- and run here over Postgres rows (and in 'DBOS.Transact.MessageTestSim'
-- over the in-memory backend, whose notifier genuinely wakes receivers).
module DBOS.Transact.MessageTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (ForkOptions (..), ForkPoint (..), NewWorkflow (..), Outcome (..), Submission (..), WorkflowId (..), newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (Identity (..), nullTracer)
import DBOS.Transact.Connection (nextExecutionIdentity)
import DBOS.Transact.Context (newWorkflowCtx, newWorkflowState)
import DBOS.Transact.ContextTest (connOver)
import DBOS.Transact.MessageCases
  ( MessageFixture (..),
    checkBulkEmpty,
    checkBulkSend,
    checkCapturedRecv,
    checkCapturedSend,
    checkFanOut,
    checkReplayTakes,
    checkSendDeliveredOnce,
    checkStepSend,
    checkThirdParty,
    checkTopics,
    scenarioBulkEmpty,
    scenarioBulkSend,
    scenarioCapturedRecv,
    scenarioCapturedSend,
    scenarioFanOut,
    scenarioReplayTakes,
    scenarioSendDeliveredOnce,
    scenarioStepSend,
    scenarioThirdParty,
    scenarioTopics,
  )
import Test.Tasty (TestTree, testGroup, withResource)

-- | The application identity the scoped message cases install.
messageTestIdentity :: Identity
messageTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | One fixture per leaf over the suite backend: labeled sender and
-- destination rows per scenario, scopes over plain connections. Each
-- workflow holds one scope per case, so consecutive sends and receives
-- advance the same step counter; replays run fresh scopes that restart
-- it.
mkMessageFixture :: Postgres.PostgresSystemDB -> IO (MessageFixture IO)
mkMessageFixture backend = do
  states <- newTVarIO Map.empty
  let stateFor widText = do
        found <- Map.lookup widText <$> readTVarIO states
        case found of
          Just st -> pure st
          Nothing -> do
            conn <- connOver backend nullTracer
            identity <- nextExecutionIdentity conn
            st <- newWorkflowState widText Nothing identity
            atomically (modifyTVar states (Map.insert widText st))
            pure st
  pure
    MessageFixture
      { mfFreshPair = \label -> do
          freshId <- UUID.V4.nextRandom
          let prefix = "hs-l2-message-" <> label <> "-" <> Text.pack (UUID.toString freshId)
              sourceText = prefix <> "-source"
              destinationText = prefix <> "-destination"
              create workflowText workflowName =
                SystemDB.initWorkflow backend ((newWorkflow workflowText) {newWorkflowName = Just workflowName}) Nothing Fresh Nothing
          sourceCreated <- create sourceText "L2MessageSource"
          destinationCreated <- create destinationText "L2MessageDestination"
          case (sourceCreated, destinationCreated) of
            (Right _, Right _) -> pure (WorkflowId sourceText, WorkflowId destinationText)
            (Left err, _) -> fail (show err)
            (_, Left err) -> fail (show err),
        mfCtx = \wid action -> do
          let WorkflowId widText = wid
          st <- stateFor widText
          conn <- connOver backend nullTracer
          ctx <- newWorkflowCtx conn messageTestIdentity st
          action ctx,
        mfFreshCtx = \wid action -> do
          let WorkflowId widText = wid
          conn <- connOver backend nullTracer
          identity <- nextExecutionIdentity conn
          st <- newWorkflowState widText Nothing identity
          ctx <- newWorkflowCtx conn messageTestIdentity st
          action ctx,
        mfNotifyCount = \wid -> do
          found <- SystemDB.getAllNotifications backend wid
          pure (either (const 0) length found),
        mfForkFrom = \wid -> do
          forked <-
            SystemDB.forkFrom
              backend
              [wid]
              (ForkStep 0)
              ForkOptions
                { forkOptionsApplicationVersion = Nothing,
                  forkOptionsQueueName = Nothing,
                  forkOptionsQueuePartitionKey = Nothing,
                  forkOptionsTimeout = Nothing,
                  forkOptionsReplacementChildren = []
                }
              Nothing
          case forked of
            Left err -> fail (show err)
            Right [fork] -> pure fork
            Right other -> fail ("expected one fork, got: " <> show (length other)),
        mfSettle = \wid ->
          SystemDB.recordWorkflowOutcome backend wid (OutcomeOutput (Just "null")) >>= either (fail . show) (const (pure ())),
        mfCheckStep = \wid name step ->
          SystemDB.checkStep backend wid step name >>= either (fail . show) pure
      }

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    let leaf :: String -> (MessageFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        leaf name scen check = liveCase (mkMessageFixture =<< getBackend) name scen check
     in testGroup
          "Workflow messages"
          [ leaf "a workflow send is delivered once and recv replays" scenarioSendDeliveredOnce checkSendDeliveredOnce,
            leaf "a send may fan out to the destination's forks" scenarioFanOut checkFanOut,
            leaf "a message from another workflow reaches its destination" scenarioThirdParty checkThirdParty,
            leaf "topics do not cross and absence is a value" scenarioTopics checkTopics,
            leaf "a replay takes the recorded message and sends once" scenarioReplayTakes checkReplayTakes,
            leaf "a step may send but may not receive" scenarioStepSend checkStepSend,
            leaf "a send through a captured parent is plain and moves no id" scenarioCapturedSend checkCapturedSend,
            leaf "a recv through a captured parent is refused" scenarioCapturedRecv checkCapturedRecv,
            leaf "a batch delivers every message and checkpoints once" scenarioBulkSend checkBulkSend,
            leaf "an empty bulk send still takes its step" scenarioBulkEmpty checkBulkEmpty
          ]
