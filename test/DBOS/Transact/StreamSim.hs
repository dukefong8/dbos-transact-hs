{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RankNTypes #-}

-- | Stream behavior over IOSim plus deterministic adapter and mock-backend
-- coverage. Eval-only; 'Main.tests' runs the live tree instead.
module DBOS.Transact.StreamSim (tests) where

import Control.Foldl qualified as Foldl
import Control.Monad.IOSim (IOSim, SimTrace, runSimOrThrow, selectTraceEventsDynamic)
import Data.Vector.Fusion.Stream.Monadic qualified as Vector
import DBOS.DualStack (simCase)
import DBOS.Prelude
import DBOS.SystemDB
  ( EncodedValue (..),
    StreamRead (..),
    StreamRecord (..),
    SystemDB (..),
    WorkflowId (..),
    WorkflowStatus (..),
    WrittenBy (..)
  )
import DBOS.SystemDB.IOSim (MockSystemDB (..))
import DBOS.Transact qualified as Transact
import DBOS.Transact.EventTestSim (simEventFixture)
import DBOS.Transact.Recovery (EngineEvent)
import DBOS.Transact.Step (WorkflowEvent (..))
import DBOS.Transact.StreamCases
  ( checkCloseStopsStream,
    checkCrossPages,
    checkStreamDecodeFailure,
    checkStreamReader,
    checkStreamReaderError,
    checkTimeoutAfterValue,
    checkSingleStreamValue,
    checkCancelledProducer,
    checkWriteInsideStep,
    checkReplayRead,
    checkTimeoutReplay,
    checkJoinedReaders,
    checkDroppedRead,
    checkMissingWorkflowReplay,
    checkInvalidStreamRead,
    checkCancelledReader,
    checkUnclosedStream,
    checkWaitingReadStepId,
    checkWrittenStream,
    scenarioCloseStopsStream,
    scenarioCrossPages,
    scenarioStreamDecodeFailure,
    scenarioStreamReader,
    scenarioStreamReaderError,
    scenarioTimeoutAfterValue,
    scenarioSingleStreamValue,
    scenarioCancelledProducer,
    scenarioWriteInsideStep,
    scenarioReplayRead,
    scenarioTimeoutReplay,
    scenarioJoinedReaders,
    scenarioDroppedRead,
    scenarioMissingWorkflowReplay,
    scenarioInvalidStreamRead,
    scenarioCancelledReader,
    scenarioUnclosedStream,
    scenarioWaitingReadStepId,
    scenarioWrittenStream
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Workflow streams (Sim)"
    [ simCase simEventFixture "a written stream reads in order and ends at its close" scenarioWrittenStream checkWrittenStream traceSilent,
      simCase simEventFixture "an unclosed stream ends when its producer finishes" scenarioUnclosedStream checkUnclosedStream traceSilent,
      simCase simEventFixture "a read stops at the close marker" scenarioCloseStopsStream checkCloseStopsStream traceSilent,
      simCase simEventFixture "reads cross pages after all values are written" scenarioCrossPages checkCrossPages traceSilent,
      simCase simEventFixture "a reader times out waiting for its next value" scenarioTimeoutAfterValue checkTimeoutAfterValue traceSilent,
      simCase simEventFixture "single-offset reads yield values then report the stream end" scenarioSingleStreamValue checkSingleStreamValue traceSingleStream,
      simCase simEventFixture "a cancelled producer reader drains and ends" scenarioCancelledProducer checkCancelledProducer traceSilent,
      simCase simEventFixture "a step may write and close a stream" scenarioWriteInsideStep checkWriteInsideStep traceStepWrites,
      simCase simEventFixture "a workflow read replays its recorded values" scenarioReplayRead checkReplayRead traceSilent,
      simCase simEventFixture "a timed out workflow read replays as a timeout" scenarioTimeoutReplay checkTimeoutReplay traceSilent,
      simCase simEventFixture "two readers in one workflow both read" scenarioJoinedReaders checkJoinedReaders traceSilent,
      simCase simEventFixture "dropping a reader after a value loses no later value" scenarioDroppedRead checkDroppedRead traceSilent,
      simCase simEventFixture "a missing workflow read replays as missing" scenarioMissingWorkflowReplay checkMissingWorkflowReplay traceSilent,
      simCase simEventFixture "invalid stream reader arguments are refused" scenarioInvalidStreamRead checkInvalidStreamRead traceSilent,
      simCase simEventFixture "a waiting workflow reader stops when its workflow is cancelled" scenarioCancelledReader checkCancelledReader traceSilent,
      simCase simEventFixture "a waiting pull owns one step id" scenarioWaitingReadStepId checkWaitingReadStepId traceAfterStep,
      simCase simEventFixture "a workflow reads a durable stream through FoldM" scenarioStreamReader checkStreamReader traceSilent,
      simCase simEventFixture "a stream read error stays an error through the vector adapter" scenarioStreamReaderError checkStreamReaderError traceSilent,
      simCase simEventFixture "a decode failure advances to the next stream value" scenarioStreamDecodeFailure checkStreamDecodeFailure traceSilent,
      testCase "writeStream succeeds against the mock SystemDB" $
        run (writeStream backend (WorkflowId "wf-1") 0 "key" "\"v\"" Nothing Workflow) @?= Right (),
      testCase "closeStream succeeds against the mock SystemDB" $
        run (closeStream backend (WorkflowId "wf-1") 0 "key" Workflow) @?= Right (),
      testCase "readStreamValues returns values and producer status" $
        run (readStreamValues backend (WorkflowId "wf-1") "key" 0 1) @?= Right (StreamRead Pending [EncodedValue "mock-stream" (Just "rust_serde")]),
      testCase "getAllStreamEntries returns mock entries" $ do
        let result = run (getAllStreamEntries backend (WorkflowId "wf-1"))
        case result of
          Right entries -> map (.streamKey) entries @?= ["mock-key"]
          Left err -> fail (show err),
      testCase "foldVectorStream folds yielded values and skips skipped states" $ do
        let source = Vector.Stream step 0 :: Vector.Stream IO Int
            step n
              | n == 0 = pure (Vector.Skip 1)
              | n > 3 = pure Vector.Done
              | otherwise = pure (Vector.Yield n (n + 1))
            monadicFold = Foldl.FoldM (\acc value -> pure (acc + value)) (pure 0) pure
        Transact.foldVectorStream monadicFold source >>= (@?= 6)
    ]

backend :: MockSystemDB
backend = MockSystemDB

run :: (forall s. IOSim s a) -> a
run = runSimOrThrow

traceSilent :: forall a. SimTrace a -> IO ()
traceSilent simTrace = do
  selectTraceEventsDynamic simTrace @?= ([] :: [WorkflowEvent])
  selectTraceEventsDynamic simTrace @?= ([] :: [EngineEvent])

traceStepWrites :: forall a. SimTrace a -> IO ()
traceStepWrites simTrace = do
  selectTraceEventsDynamic simTrace @?= [StepRunning "produce" 0, StepOutputRecorded "produce" 0]
  selectTraceEventsDynamic simTrace @?= ([] :: [EngineEvent])

traceAfterStep :: forall a. SimTrace a -> IO ()
traceAfterStep simTrace = do
  selectTraceEventsDynamic simTrace @?= [StepRunning "after" 2, StepOutputRecorded "after" 2]
  selectTraceEventsDynamic simTrace @?= ([] :: [EngineEvent])

traceSingleStream :: forall a. SimTrace a -> IO ()
traceSingleStream simTrace = do
  selectTraceEventsDynamic simTrace @?=
    [ StepRunning "initial" 0,
      StepOutputRecorded "initial" 0,
      StepReplaying "initial" 0,
      StepRunning "append" 1,
      StepOutputRecorded "append" 1
    ]
  selectTraceEventsDynamic simTrace @?= ([] :: [EngineEvent])
