{-# LANGUAGE OverloadedStrings #-}

module DBOS.Transact.WorkflowExecutionStatus
  ( WorkflowStatus (..),
    WorkflowStatusDecodeError (..),
    parseWorkflowStatus,
  )
where

import Data.Text (Text)

data WorkflowStatus
  = Pending
  | Success
  | Error
  | MaxRecoveryAttemptsExceeded
  | Cancelled
  | Enqueued
  | Delayed
  deriving stock (Eq, Show)

newtype WorkflowStatusDecodeError
  = UnknownWorkflowStatus Text
  deriving stock (Eq, Show)

parseWorkflowStatus :: Text -> Either WorkflowStatusDecodeError WorkflowStatus
parseWorkflowStatus raw =
  case raw of
    "PENDING" -> Right Pending
    "SUCCESS" -> Right Success
    "ERROR" -> Right Error
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED" -> Right MaxRecoveryAttemptsExceeded
    "CANCELLED" -> Right Cancelled
    "ENQUEUED" -> Right Enqueued
    "DELAYED" -> Right Delayed
    other -> Left (UnknownWorkflowStatus other)
