{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module DbosTransact.Step
  ( StepName
  , stepName
  , StepOptions(..)
  , defaultStepOptions
  , IsoLevel(..)
  , StepOutcome(..)
  , step
  , runAsTxn
  , sleep
  ) where

import Bluefin.Eff (Eff, type (<:), type (:&))
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import DbosTransact.Effects (TransactionScope, WorkflowScope)
import DbosTransact.Error (DBOSError)

newtype StepName = StepName Text
  deriving stock (Eq, Ord, Show)

stepName :: Text -> StepName
stepName = StepName

data IsoLevel
  = IsoDefault
  | IsoReadCommitted
  | IsoRepeatableRead
  | IsoSerializable
  deriving stock (Eq, Show)

data StepOptions = StepOptions
  { stepNameOption    :: Maybe StepName
  , stepMaxRetries    :: Int
  , stepBackoffFactor :: Double
  , stepBaseInterval  :: NominalDiffTime
  , stepMaxInterval   :: NominalDiffTime
  , stepIsoLevel      :: Maybe IsoLevel
  }
  deriving stock (Eq, Show)

defaultStepOptions :: StepOptions
defaultStepOptions = StepOptions
  { stepNameOption = Nothing
  , stepMaxRetries = 0
  , stepBackoffFactor = 2.0
  , stepBaseInterval = 0.1
  , stepMaxInterval = 5.0
  , stepIsoLevel = Nothing
  }

data StepOutcome a = StepOutcome
  { stepResult :: Maybe a
  , stepError  :: Maybe DBOSError
  }
  deriving stock (Eq, Show)

step
  :: forall stmt wf tx es output.
     (wf <: es, tx <: es, ToJSON output, FromJSON output)
  => WorkflowScope wf
  -> TransactionScope stmt tx
  -> StepOptions
  -> Eff es output
  -> Eff es output
step _ _ _ _ = error "DbosTransact.Step.step: not implemented"

runAsTxn
  :: forall stmt wf es output.
     (wf <: es, ToJSON output, FromJSON output)
  => WorkflowScope wf
  -> StepOptions
  -> (forall tx. TransactionScope stmt tx -> Eff (tx :& es) output)
  -> Eff es output
runAsTxn _ _ _ = error "DbosTransact.Step.runAsTxn: not implemented"

sleep :: forall wf es. (wf <: es) => WorkflowScope wf -> NominalDiffTime -> Eff es NominalDiffTime
sleep _ _ = error "DbosTransact.Step.sleep: not implemented"
