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
  | expectedName == checkpointOperationName checkpoint =
      Right (ReplayOperation (checkpointResult checkpoint))
  | otherwise =
      Left
        ( UnexpectedOperationName
            (checkpointOperationId checkpoint)
            expectedName
            (checkpointOperationName checkpoint)
        )
