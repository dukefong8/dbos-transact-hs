{-# LANGUAGE OverloadedRecordDot #-}

-- | Legacy checkpoint replay for the starter seam: rerun the operation on
-- no row, adopt the recorded result on a name match, refuse a renamed
-- step. See "DBOS.Transact.OperationCheckpointTypes" for the seam status;
-- do not extend.
module DBOS.Transact.OperationCheckpointReplay
  ( replayOperationCheckpoint,
  )
where

import DBOS.Prelude
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
