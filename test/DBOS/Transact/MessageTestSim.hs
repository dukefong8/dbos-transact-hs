{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.MessageTest' mirrored under IOSim over the in-memory
-- backend, whose notifier genuinely wakes receivers: sends deliver,
-- receives take, replays read their recordings, and the fan-out reaches
-- forks. Scenarios and checks are shared; this module owns the sim
-- factory and the sim-only extra — the typed 'WorkflowEvent' record,
-- which no message path announces: sends, bulk sends, and receives all
-- checkpoint without a runner, exactly as the oracle's silent @message.rs@.
module DBOS.Transact.MessageTestSim (tests) where

import DBOS.Internal.Transact.Step (WorkflowEvent (..))
import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Internal.Prelude
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import DBOS.SystemDB (ForkOptions (..), ForkPoint (..), NewWorkflow (..), NotificationRecord (notificationRecordMessageUuid), Outcome (..), WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowOwner (..), WorkflowRecord (workflowRecordCreatorXid), newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (MemSystemDB, memConnectionOn, newMemDB, newMemDBSkewed, simIdentity)
import DBOS.Internal.Transact.Connection (nextExecutionIdentity)
import DBOS.Internal.Transact.Context (newWorkflowCtx, newWorkflowState)
import DBOS.Transact.MessageCases
  ( MessageFixture (..),
    checkBulkEmpty,
    checkBatchNotifyUnique,
    checkBulkSend,
    checkBulkReplay,
    checkCapturedRecv,
    checkCapturedSend,
    checkFanOut,
    checkKeyedFanOut,
    checkResendDeduped,
    checkScopedReceive,
    checkReplayTakes,
    checkSendDeliveredOnce,
    checkStepSend,
    checkRecvTimeoutSteps,
    checkRecvTimed,
    checkSendNameMismatch,
    checkThirdParty,
    checkTopics,
    scenarioBulkEmpty,
    scenarioBatchNotifyUnique,
    scenarioBulkSend,
    scenarioBulkReplay,
    scenarioCapturedRecv,
    scenarioCapturedSend,
    scenarioFanOut,
    scenarioKeyedFanOut,
    scenarioResendDeduped,
    scenarioScopedReceive,
    scenarioReplayTakes,
    scenarioSendDeliveredOnce,
    scenarioStepSend,
    scenarioRecvTimeoutSteps,
    scenarioRecvTimed,
    scenarioSendNameMismatch,
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
simMessageFixture = newMemDB >>= simMessageFixtureOn

simMessageFixtureSkewed :: forall s. Int64 -> IOSim s (MessageFixture (IOSim s))
simMessageFixtureSkewed skewMs = newMemDBSkewed skewMs >>= simMessageFixtureOn

simMessageFixtureOn :: forall s. MemSystemDB s -> IOSim s (MessageFixture (IOSim s))
simMessageFixtureOn mem = do
  states <- newTVarIO Map.empty
  let -- The context holds the row's own creator token, as an execution built
      -- from the init result would: anything else is refused at the first
      -- fenced write.
      ownerFor widText = do
        fetched <- SystemDB.getWorkflow mem (WorkflowId widText)
        case fetched of
          Right (Just row) -> case row.workflowRecordCreatorXid of
            Just token -> pure (WorkflowOwner {ownerWorkflowId = WorkflowId widText, ownerToken = token})
            Nothing -> error "message fixture row has no creator token"
          _ -> error "message fixture built a context for a row that was never initialized"
      stateFor widText = do
        found <- Map.lookup widText <$> readTVarIO states
        case found of
          Just st -> pure st
          Nothing -> do
            conn <- memConnectionOn mem simTracer
            identity <- nextExecutionIdentity conn
            owner <- ownerFor widText
            st <- newWorkflowState widText Nothing owner identity
            atomically (modifyTVar states (Map.insert widText st))
            pure st
  pure
    MessageFixture
      { mfFreshPair = \label -> do
          let sourceText = "sim-message-" <> label <> "-source"
              destinationText = "sim-message-" <> label <> "-destination"
              create workflowText workflowName =
                SystemDB.initWorkflow mem ((newWorkflow workflowText) {newWorkflowName = Just workflowName}) Nothing WorkflowIdReturnExisting
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
          owner <- ownerFor widText
          st <- newWorkflowState widText Nothing owner identity
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
                { forkOptionsAppVersion = Nothing,
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
        mfSettle = \wid@(WorkflowId widText) -> do
          owner <- ownerFor widText
          SystemDB.recordWorkflowOutcome mem wid (OutcomeOutput (Just "null")) owner.ownerToken >>= either (error . show) (const (pure ())),
        mfCheckStep = \wid name step ->
          SystemDB.checkStep mem wid step name >>= either (error . show) pure,
        mfNotifyIds = \wid -> do
          found <- SystemDB.getAllNotifications mem wid
          pure (either (const []) (map (.notificationRecordMessageUuid)) found)
      }

tests :: TestTree
tests =
  testGroup
    "Workflow messages (Sim)"
    [ simCase simMessageFixture "a workflow send is delivered once and recv replays" scenarioSendDeliveredOnce checkSendDeliveredOnce traceMessageSilent,
      simCase simMessageFixture "a send may fan out to the destination's forks" scenarioFanOut checkFanOut traceMessageSilent,
      simCase simMessageFixture "a keyed send fans out like an unkeyed one" scenarioKeyedFanOut checkKeyedFanOut traceMessageSilent,
      simCase simMessageFixture "a resend under one key delivers once" scenarioResendDeduped checkResendDeduped traceMessageSilent,
      simCase simMessageFixture "a message addressed elsewhere stays unreadable" scenarioScopedReceive checkScopedReceive traceMessageSilent,
      simCase simMessageFixture "a message from another workflow reaches its destination" scenarioThirdParty checkThirdParty traceMessageSilent,
      simCase simMessageFixture "topics do not cross and absence is a value" scenarioTopics checkTopics traceMessageSilent,
      simCase simMessageFixture "a replay takes the recorded message and sends once" scenarioReplayTakes checkReplayTakes traceMessageSilent,
      simCase simMessageFixture "a step may send but may not receive" scenarioStepSend checkStepSend traceStepSend,
      simCase simMessageFixture "a timed-out recv records its deadline step beside it" scenarioRecvTimeoutSteps checkRecvTimeoutSteps traceMessageSilent,
      simCase (simMessageFixtureSkewed 3600000) "a recv timeout waits its duration with the database clock an hour ahead" scenarioRecvTimed checkRecvTimed traceMessageSilent,
      simCase (simMessageFixtureSkewed (-3600000)) "a recv timeout waits its duration with the database clock an hour behind" scenarioRecvTimed checkRecvTimed traceMessageSilent,
      simCase simMessageFixture "a send from a renamed position is refused" scenarioSendNameMismatch checkSendNameMismatch traceStepSend,
      simCase simMessageFixture "a send through a captured parent is plain and moves no id" scenarioCapturedSend checkCapturedSend traceMessageSilent,
      simCase simMessageFixture "a recv through a captured parent is refused" scenarioCapturedRecv checkCapturedRecv traceMessageSilent,
      simCase simMessageFixture "a batch delivers every message and checkpoints once" scenarioBulkSend checkBulkSend traceBulkSend,
      simCase simMessageFixture "an empty bulk send still takes its step" scenarioBulkEmpty checkBulkEmpty traceBulkEmpty,
      simCase simMessageFixture "a replayed batch sends nothing again" scenarioBulkReplay checkBulkReplay traceBulkSend,
      simCase simMessageFixture "one batch mints a distinct notification id per delivery" scenarioBatchNotifyUnique checkBatchNotifyUnique traceBulkSend
    ]

-- * Typed-event assertions (sim-only)

-- | Single sends, bulk sends, and receives checkpoint without a runner and
-- stay silent, exactly as the oracle's untraced message paths: the batch's
-- step record commits in the same system-database transaction as its
-- messages, with no runner around it.
traceMessageSilent :: SimTrace a -> IO ()
traceMessageSilent tr =
  assertBool "no workflow event may fire on a message path" (null (selectTraceEventsDynamic tr :: [WorkflowEvent]))

-- | The probe step announces its start and its recorded output.
traceStepSend :: SimTrace a -> IO ()
traceStepSend tr =
  assertBool "the probe step must announce" (selectTraceEventsDynamic tr == [StepRunning "probe" 0, StepOutputRecorded "probe" 0])

-- | The batch checkpoints once under its bulk step, silently: the step
-- record is written by the send transaction itself, with no runner to
-- announce it. The assertion stays a distinct name so the batch cases keep
-- their own trace hook.
traceBulkSend :: SimTrace a -> IO ()
traceBulkSend tr =
  assertBool "the batch must checkpoint silently" (null (selectTraceEventsDynamic tr :: [WorkflowEvent]))

-- | The empty batch still takes its step, silently for the same reason.
traceBulkEmpty :: SimTrace a -> IO ()
traceBulkEmpty tr =
  assertBool "the empty batch must take its step silently" (null (selectTraceEventsDynamic tr :: [WorkflowEvent]))
