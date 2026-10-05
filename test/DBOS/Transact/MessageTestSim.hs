{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.MessageTest' mirrored under IOSim over the in-memory
-- backend, whose notifier genuinely wakes receivers: sends deliver,
-- receives take, replays read their recordings, and the fan-out reaches
-- forks. Scenarios and checks are shared; this module owns the sim
-- factory and the sim-only extra — the typed 'WorkflowEvent' record,
-- which only the bulk-send step announces (single sends and receives
-- checkpoint without a runner, exactly as the oracle's silent
-- @message.rs@).
module DBOS.Transact.MessageTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import DBOS.SystemDB (ForkOptions (..), ForkPoint (..), NewWorkflow (..), Outcome (..), Submission (..), WorkflowId (..), newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB, simIdentity)
import DBOS.Transact (WorkflowEvent (..))
import DBOS.Transact.Connection (nextExecutionIdentity)
import DBOS.Transact.Context (newWorkflowCtx, newWorkflowState)
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
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool)

-- | One fixture per leaf over a fresh in-memory database: labeled sender
-- and destination rows per scenario, every send and receive sharing the
-- same store in the same simulation so delivery is genuine. Each workflow
-- holds one scope per case; replays run fresh scopes that restart it.
simMessageFixture :: forall s. IOSim s (MessageFixture (IOSim s))
simMessageFixture = do
  mem <- newMemDB
  states <- newTVarIO Map.empty
  let stateFor widText = do
        found <- Map.lookup widText <$> readTVarIO states
        case found of
          Just st -> pure st
          Nothing -> do
            conn <- memConnectionOn mem simTracer
            identity <- nextExecutionIdentity conn
            st <- newWorkflowState widText Nothing identity
            atomically (modifyTVar states (Map.insert widText st))
            pure st
  pure
    MessageFixture
      { mfFreshPair = \label -> do
          let sourceText = "sim-message-" <> label <> "-source"
              destinationText = "sim-message-" <> label <> "-destination"
              create workflowText workflowName =
                SystemDB.initWorkflow mem ((newWorkflow workflowText) {newWorkflowName = Just workflowName}) Nothing Fresh Nothing
          sourceCreated <- create sourceText "SimMessageSource"
          destinationCreated <- create destinationText "SimMessageDestination"
          case (sourceCreated, destinationCreated) of
            (Right _, Right _) -> pure (WorkflowId sourceText, WorkflowId destinationText)
            (Left err, _) -> error (show err)
            (_, Left err) -> error (show err),
        mfCtx = \wid action -> do
          let WorkflowId widText = wid
          st <- stateFor widText
          conn <- memConnectionOn mem simTracer
          ctx <- newWorkflowCtx conn simIdentity st
          action ctx,
        mfFreshCtx = \wid action -> do
          let WorkflowId widText = wid
          conn <- memConnectionOn mem simTracer
          identity <- nextExecutionIdentity conn
          st <- newWorkflowState widText Nothing identity
          ctx <- newWorkflowCtx conn simIdentity st
          action ctx,
        mfNotifyCount = \wid -> do
          found <- SystemDB.getAllNotifications mem wid
          pure (either (const 0) length found),
        mfForkFrom = \wid -> do
          forked <-
            SystemDB.forkFrom
              mem
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
            Left err -> error (show err)
            Right [fork] -> pure fork
            Right other -> error ("expected one fork, got: " <> show (length other)),
        mfSettle = \wid ->
          SystemDB.recordWorkflowOutcome mem wid (OutcomeOutput (Just "null")) >>= either (error . show) (const (pure ())),
        mfCheckStep = \wid name step ->
          SystemDB.checkStep mem wid step name >>= either (error . show) pure
      }

tests :: TestTree
tests =
  testGroup
    "Workflow messages (Sim)"
    [ simCase simMessageFixture "a workflow send is delivered once and recv replays" scenarioSendDeliveredOnce checkSendDeliveredOnce traceMessageSilent,
      simCase simMessageFixture "a send may fan out to the destination's forks" scenarioFanOut checkFanOut traceMessageSilent,
      simCase simMessageFixture "a message from another workflow reaches its destination" scenarioThirdParty checkThirdParty traceMessageSilent,
      simCase simMessageFixture "topics do not cross and absence is a value" scenarioTopics checkTopics traceMessageSilent,
      simCase simMessageFixture "a replay takes the recorded message and sends once" scenarioReplayTakes checkReplayTakes traceMessageSilent,
      simCase simMessageFixture "a step may send but may not receive" scenarioStepSend checkStepSend traceStepSend,
      simCase simMessageFixture "a send through a captured parent is plain and moves no id" scenarioCapturedSend checkCapturedSend traceMessageSilent,
      simCase simMessageFixture "a recv through a captured parent is refused" scenarioCapturedRecv checkCapturedRecv traceMessageSilent,
      simCase simMessageFixture "a batch delivers every message and checkpoints once" scenarioBulkSend checkBulkSend traceBulkSend,
      simCase simMessageFixture "an empty bulk send still takes its step" scenarioBulkEmpty checkBulkEmpty traceBulkEmpty
    ]

-- * Typed-event assertions (sim-only)

-- | Single sends and receives checkpoint without a runner and stay
-- silent, exactly as the oracle's untraced message paths.
traceMessageSilent :: SimTrace a -> IO ()
traceMessageSilent tr =
  assertBool "no workflow event may fire outside a bulk send" (null (selectTraceEventsDynamic tr :: [WorkflowEvent]))

-- | The probe step announces its start and its recorded output.
traceStepSend :: SimTrace a -> IO ()
traceStepSend tr =
  assertBool "the probe step must announce" (selectTraceEventsDynamic tr == [StepRunning "probe" 0, StepOutputRecorded "probe" 0])

-- | The batch checkpoints once under its bulk step.
traceBulkSend :: SimTrace a -> IO ()
traceBulkSend tr =
  assertBool "the batch must checkpoint once" (selectTraceEventsDynamic tr == [StepOutputRecorded "DBOS.sendBulk" 0])

-- | The empty batch still takes its step.
traceBulkEmpty :: SimTrace a -> IO ()
traceBulkEmpty tr =
  assertBool "the empty batch must take its step" (selectTraceEventsDynamic tr == [StepOutputRecorded "DBOS.sendBulk" 0])
