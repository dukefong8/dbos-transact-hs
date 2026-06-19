{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

module DbosTransact.Error
  ( DBOSError(..)
  , isTerminalStatus
  ) where

import Data.Text (Text)

-- | Public DBOS error vocabulary used by the pure core and interpreters.
data DBOSError
  = WorkflowAlreadyExists Text
  | WorkflowNotFound Text
  | WorkflowNameCollision Text
  | WorkflowExecutionError Text
  | StepOutsideWorkflow
  | UnexpectedStepError Text
  | SerializationError Text
  | DatabaseError Text
  | NotImplemented Text
  deriving stock (Eq, Show)

isTerminalStatus :: Text -> Bool
isTerminalStatus status = status `elem` ["SUCCESS", "ERROR", "CANCELLED"]
