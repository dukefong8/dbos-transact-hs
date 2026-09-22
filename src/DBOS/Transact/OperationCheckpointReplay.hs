{-# LANGUAGE OverloadedRecordDot #-}

module DBOS.Transact.OperationCheckpointReplay
  ( replayOperationCheckpoint,
  )
where

import DBOS.Transact.OperationCheckpointTypes
  ( OperationCheckpoint (..),
    OperationCheckpointReplay (..),
    OperationCheckpointReplayError (..),
    OperationName,
  )

replayOperationCheckpoint ::
  OperationName ->
  Maybe OperationCheckpoint ->
  Either OperationCheckpointReplayError OperationCheckpointReplay
replayOperationCheckpoint _ Nothing =
  Right RunOperation
replayOperationCheckpoint expectedName (Just checkpoint)
  | expectedName == checkpoint.checkpointOperationName =
      Right (ReplayOperation (checkpoint.checkpointResult))
  | otherwise =
      Left
        ( UnexpectedOperationName
            (checkpoint.checkpointOperationId)
            expectedName
            (checkpoint.checkpointOperationName)
        )
