{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal step runner (Rule 4: plain Haskell, no Bluefin imports).
-- Mirrors @step.rs@ for plain steps: run the body once and record its
-- output, or replay the recorded outcome without running the body.
-- Recorded failures come back as 'StepRecordedError'; child-workflow and
-- awaited checkpoints never occur for plain steps and are reported as
-- 'StepUnexpectedCheckpoint'. A throwing body propagates: the failure is
-- recorded at the workflow level by 'runWorkflow', never as a step error
-- row — step error rows only ever replay values 'recordOperationError'
-- wrote, so a step that throws re-runs on recovery by design.
module DBOS.Transact.Step
  ( StepError (..),
    runStep,
    sleepStep,
  )
where

import Control.Concurrent (threadDelay)
import DBOS.SystemDB.Postgres
  ( Pool,
    fetchOperationCheckpoint,
    recordOperationOutput,
    recordSleep,
  )
import DBOS.Transact.OperationCheckpointTypes
  ( OperationCheckpoint (..),
    OperationCheckpointResult (..),
    OperationId,
    OperationName (..),
  )
import DBOS.Transact.WorkflowExecutionTypes
  ( Millis (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    WorkflowId,
  )
import Data.Int (Int64)
import Data.Text (pack, unpack)
import Data.Time.Clock.POSIX (getPOSIXTime)

data StepError
  = StepRecordedError SerializedWorkflowValue
  | StepUnexpectedCheckpoint OperationCheckpoint
  deriving stock (Eq, Show)

runStep ::
  Pool ->
  WorkflowId ->
  OperationId ->
  OperationName ->
  IO SerializedWorkflowValue ->
  IO (Either StepError SerializedWorkflowValue)
runStep pool workflowId operationId operationName body = do
  checkpoint <- fetchOperationCheckpoint pool workflowId operationId
  case checkpoint of
    Nothing -> do
      output <- body
      recordOperationOutput pool workflowId operationId operationName output
      pure (Right output)
    Just recorded -> pure (replayRecorded recorded)
  where
    replayRecorded recorded =
      case recorded.checkpointResult of
        CheckpointOutput output -> Right output
        CheckpointError err -> Left (StepRecordedError err)
        _ -> Left (StepUnexpectedCheckpoint recorded)

-- | Durable sleep: record the wake time, wait until it, and on replay wait
-- only what is left of the original wait. Mirrors @sleep.rs@: the recorded
-- wake time is the step output and @completed_at@ is stamped at the wake
-- time, so an hour's sleep reads as an hour rather than an instant. The step
-- name is fixed (@DBOS.sleep@), as in the oracle.
sleepStep ::
  Pool ->
  WorkflowId ->
  OperationId ->
  Millis ->
  IO ()
sleepStep pool workflowId operationId (Millis durationMs) = do
  checkpoint <- fetchOperationCheckpoint pool workflowId operationId
  case checkpoint of
    Just recorded ->
      case recorded.checkpointResult of
        CheckpointOutput output -> waitUntilRecorded output
        _ -> pure ()
    Nothing -> do
      now <- currentTimeMillis
      let started = Millis now
          wakeAt = Millis (now + durationMs)
      recordSleep
        pool
        workflowId
        operationId
        sleepOperationName
        (SerializedWorkflowValue (pack (show (now + durationMs))) (Just (Serialization "portable_json")))
        started
        wakeAt
      threadDelay (millisToMicros (max 0 durationMs))

-- | The step name the oracle records every durable sleep under.
sleepOperationName :: OperationName
sleepOperationName = OperationName "DBOS.sleep"

-- | A replayed sleep waits until the instant the first run recorded, which
-- may already have passed — then it waits not at all. The stored text is a
-- bare integer (@show@ on @Text@ would add quotes and never parse).
waitUntilRecorded :: SerializedWorkflowValue -> IO ()
waitUntilRecorded output = do
  now <- currentTimeMillis
  case reads (unpack output.serializedText) of
    [(wakeAt, _)] -> threadDelay (millisToMicros (max 0 (wakeAt - now)))
    _ -> pure ()

currentTimeMillis :: IO Int64
currentTimeMillis = round . (* 1000) <$> getPOSIXTime

millisToMicros :: Int64 -> Int
millisToMicros ms = fromIntegral (ms * 1000)
