{-# LANGUAGE OverloadedStrings #-}

-- | Workflow-facing stream behavior over a live Postgres SystemDB.
module DBOS.Transact.StreamTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import DBOS.SystemDB.Postgres (PostgresSystemDB)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.SystemDB.PostgresTest qualified as PostgresTest
import DBOS.Transact.EventTest (liveEventFixture)
import DBOS.Transact.Logger (acquireLoggerBackend, ioTracer, nullTracer)
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
import Test.Tasty (TestTree, testGroup, withResource)

acquireSuiteBackend :: IO PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    withResource acquireLoggerBackend snd $ \getLogger ->
      testGroup
        "Workflow streams"
        [ liveCase (fixture getBackend getLogger) "a written stream reads in order and ends at its close" scenarioWrittenStream checkWrittenStream,
          liveCase (fixture getBackend getLogger) "an unclosed stream ends when its producer finishes" scenarioUnclosedStream checkUnclosedStream,
          liveCase (fixture getBackend getLogger) "a read stops at the close marker" scenarioCloseStopsStream checkCloseStopsStream,
          liveCase (fixture getBackend getLogger) "reads cross pages after all values are written" scenarioCrossPages checkCrossPages,
          liveCase (fixture getBackend getLogger) "a reader times out waiting for its next value" scenarioTimeoutAfterValue checkTimeoutAfterValue,
          liveCase (fixture getBackend getLogger) "single-offset reads yield values then report the stream end" scenarioSingleStreamValue checkSingleStreamValue,
          liveCase (fixture getBackend getLogger) "a cancelled producer reader drains and ends" scenarioCancelledProducer checkCancelledProducer,
          liveCase (fixture getBackend getLogger) "a step may write and close a stream" scenarioWriteInsideStep checkWriteInsideStep,
          liveCase (fixture getBackend getLogger) "a workflow read replays its recorded values" scenarioReplayRead checkReplayRead,
          liveCase (fixture getBackend getLogger) "a timed out workflow read replays as a timeout" scenarioTimeoutReplay checkTimeoutReplay,
          liveCase (fixture getBackend getLogger) "two readers in one workflow both read" scenarioJoinedReaders checkJoinedReaders,
          liveCase (fixture getBackend getLogger) "dropping a reader after a value loses no later value" scenarioDroppedRead checkDroppedRead,
          liveCase (fixture getBackend getLogger) "a missing workflow read replays as missing" scenarioMissingWorkflowReplay checkMissingWorkflowReplay,
          liveCase (fixture getBackend getLogger) "invalid stream reader arguments are refused" scenarioInvalidStreamRead checkInvalidStreamRead,
          liveCase (fixture getBackend getLogger) "a waiting workflow reader stops when its workflow is cancelled" scenarioCancelledReader checkCancelledReader,
          liveCase (fixture getBackend getLogger) "a waiting pull owns one step id" scenarioWaitingReadStepId checkWaitingReadStepId,
          PostgresTest.streamTests,
          liveCase (fixture getBackend getLogger) "a workflow reads a durable stream through FoldM" scenarioStreamReader checkStreamReader,
          liveCase (fixture getBackend getLogger) "a stream read error stays an error through the vector adapter" scenarioStreamReaderError checkStreamReaderError,
          liveCase (fixture getBackend getLogger) "a decode failure advances to the next stream value" scenarioStreamDecodeFailure checkStreamDecodeFailure
        ]
  where
    fixture getBackend getLogger = liveEventFixture getBackend (ioTracer . fst <$> getLogger)
