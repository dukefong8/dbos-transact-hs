{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE UndecidableInstances #-}

module DbosTransact.Effects
  ( -- * Effect handles
    DBOS (..)          -- constructor exported for handler definition only
  , WorkflowScope (..)
  , TransactionScope (..)

  -- * Bootstrapping
  , withDBOS

  -- * DBOS runtime operations
  , launch
  , shutdown

  -- * Workflow-scoped introspection
  , currentWorkflowId
  , currentStepId
  ) where

import Bluefin.Compound
  ( Handle
  , OneWayCoercibleHandle (MkOneWayCoercibleHandle)
  , OneWayCoercible (oneWayCoercibleImpl)
  , gOneWayCoercible
  , mapHandle
  , oneWayCoercibleTrustMe
  , useImpl
  , useImplIn
  )
import Bluefin.Eff (Eff, type (<:), type (:&))
import Data.Text (Text)
import Bluefin.IO (IOE)
import Data.Time (NominalDiffTime)
import DbosTransact.Config (DBOSConfig)
import GHC.Generics (Generic)

------------------------------------------------------------
-- DBOS — top-level runtime handle
------------------------------------------------------------

data DBOS e = MkDBOS
  { launchImpl   :: Eff e ()
  , shutdownImpl :: NominalDiffTime -> Eff e ()
  }
  deriving stock (Generic)
  deriving (Handle) via OneWayCoercibleHandle DBOS

instance (e <: es) => OneWayCoercible (DBOS e) (DBOS es) where
  oneWayCoercibleImpl = gOneWayCoercible

------------------------------------------------------------
-- WorkflowScope — per-workflow scoped handle
------------------------------------------------------------

data WorkflowScope e = MkWorkflowScope
  { currentWorkflowIdImpl :: Eff e Text
  , currentStepIdImpl     :: Eff e Int
  }
  deriving stock (Generic)
  deriving (Handle) via OneWayCoercibleHandle WorkflowScope

instance (e <: es) => OneWayCoercible (WorkflowScope e) (WorkflowScope es) where
  oneWayCoercibleImpl = gOneWayCoercible

------------------------------------------------------------
-- TransactionScope — per-transaction scoped handle
------------------------------------------------------------

data TransactionScope stmt e = MkTransactionScope
  { queryTxImpl   :: forall params result. stmt params result -> params -> Eff e result
  , commandTxImpl :: forall params. stmt params () -> params -> Eff e ()
  }
  deriving (Handle) via OneWayCoercibleHandle (TransactionScope stmt)

instance (e <: es) => OneWayCoercible (TransactionScope stmt e) (TransactionScope stmt es) where
  oneWayCoercibleImpl = oneWayCoercibleTrustMe $ \h ->
    MkTransactionScope
      { queryTxImpl   = \s p -> useImpl (queryTxImpl h s p)
      , commandTxImpl = \s p -> useImpl (commandTxImpl h s p)
      }

------------------------------------------------------------
-- Public operations
------------------------------------------------------------

-- | Bootstrap the DBOS runtime.  The caller is responsible for
--   calling 'runEff' at the top level; this handler only introduces
--   the 'DBOS' effect tag.
withDBOS
  :: forall io es a
  .  DBOSConfig
  -> IOE io
  -> (forall e. DBOS e -> Eff (e :& es) a)
  -> Eff es a
withDBOS _config _io k =
  useImplIn
    k
    (MkDBOS
      { launchImpl   = error "DbosTransact.Effects.launch: not implemented"
      , shutdownImpl = \_ -> error "DbosTransact.Effects.shutdown: not implemented"
      })

-- | Start the DBOS runtime event loop.
launch :: (e <: es) => DBOS e -> Eff es ()
launch e = launchImpl (mapHandle e)

-- | Gracefully shut down the DBOS runtime.
shutdown :: (e <: es) => DBOS e -> NominalDiffTime -> Eff es ()
shutdown e = shutdownImpl (mapHandle e)

-- | Read the current workflow ID.
currentWorkflowId :: (e <: es) => WorkflowScope e -> Eff es Text
currentWorkflowId ws = currentWorkflowIdImpl (mapHandle ws)

-- | Read the current step counter.
currentStepId :: (e <: es) => WorkflowScope e -> Eff es Int
currentStepId ws = currentStepIdImpl (mapHandle ws)
