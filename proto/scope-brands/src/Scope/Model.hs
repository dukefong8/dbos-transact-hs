{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | THROWAWAY prototype model (not production code).
--
-- Analogues of the §11 phantom-brand direction, at real io-classes
-- constraints: 'inst' is the instance scope (one per 'withInstance'
-- region), 'exec' the execution scope (one per 'withExecution' region).
-- Constructors are hidden; the export list is the privacy boundary, the
-- way 'Ctx''s private constructor is in the real tree.
module Scope.Model
  ( -- * Scoped values (constructors hidden)
    DBOS
  , Connection
  , Registry
  , WRef
  , WHandle
  , WCtx
  , SCtx
  , Pending
    -- * Region binders (the only place a scope variable is introduced)
  , withInstance
  , withExecution
    -- * Operations
  , register
  , mintHandle
  , nextStepId
  , runStep
  , startChild
  , awaitChild
  , placeAwait
  , drive
    -- * Readers (note the split spellings: no overloaded-field games)
  , ctxWorkflowId
  , sctxWorkflowId
  , handleId
  ) where

import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadMVar.Strict
  ( StrictMVar
  , modifyMVar_
  , newMVar
  )
import Control.Concurrent.Class.MonadSTM.Strict
  ( MonadSTM
  , StrictTVar
  , atomically
  , newTVarIO
  , readTVar
  , writeTVar
  )
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as Text

-- | Instance-scoped connection. The analogue of 'Connection', carrying the
-- instance tag instead of a comparable 'connInstanceId' text.
data Connection inst m = MkConn
  { connTag :: Text
  , connState :: StrictTVar m Int
  }

-- | Instance-scoped registry.
data Registry inst m = MkReg
  { regBodies :: StrictMVar m (Map Text Text)
  }

-- | The launched-instance bundle. One 'inst' per 'withInstance' region.
data DBOS inst m = MkDBOS
  { dbConn :: Connection inst m
  , dbReg :: Registry inst m
  , dbExecCount :: StrictTVar m Int
  , dbName :: Text
  }

-- | A registered workflow. Durable: 'inst'-scoped (same instance) but
-- execution-free, so a parent may hold a ref across runs.
data WRef inst m e = MkRef
  { refReg :: Registry inst m
  , refKey :: Text
  }

-- | A running-or-finished workflow, by id. Same scoping story as the ref:
-- anchored to the instance, usable from any of its executions.
data WHandle inst m e = MkHandle
  { hConn :: Connection inst m
  , hId :: Text
  }

-- | The per-execution workflow context. Owns the step counter: 'nextStepId'
-- takes this type and no other, so id allocation belongs to the workflow.
data WCtx inst exec m = MkWCtx
  { wConn :: Connection inst m
  , wId :: Text
  , wNextStep :: StrictTVar m Int
  , wExecToken :: Int
  }

-- | The narrowed per-attempt view handed to step bodies. Wraps the parent
-- but exposes no counter: the only accessor the export list offers reads
-- the id. (Deliberate: see Residual_Capture for what this does and does
-- not prevent.)
data SCtx inst exec m = MkSCtx
  { sCtx :: WCtx inst exec m
  }

-- | A placed-but-undriven await, carrying the building execution's token
-- so a drive from another execution is both ill-typed (the 'exec'
-- parameter) and, at runtime, refused (the token backstop).
data Pending inst exec m a = MkPending
  { pToken :: Int
  , pLabel :: Text
  , pRun :: m a
  }

-- | Bind the instance scope. The analogue of the §11 @withInstance@: every
-- value inside shares one rigid @inst@, and none of it can be named
-- outside (the continuation's result cannot mention @inst@).
withInstance
  :: (MonadSTM m, MonadMVar m)
  => Text -> (forall inst. DBOS inst m -> m a) -> m a
withInstance name use = do
  st <- newTVarIO 0
  bodies <- newMVar Map.empty
  count <- newTVarIO 0
  use (MkDBOS (MkConn ("conn-" <> name) st) (MkReg bodies) count name)

-- | Bind the execution scope inside an instance region. Mints the
-- per-execution step counter and the runtime token the 'Pending'
-- backstop compares.
withExecution
  :: MonadSTM m
  => DBOS inst m -> Text -> (forall exec. WCtx inst exec m -> m a) -> m a
withExecution dbos wid use = do
  token <- atomically $ do
    n <- readTVar (dbExecCount dbos)
    writeTVar (dbExecCount dbos) (n + 1)
    pure n
  counter <- newTVarIO 0
  use (MkWCtx (dbConn dbos) wid counter token)

-- | Register under this instance's registry.
register :: MonadMVar m => DBOS inst m -> Text -> m (WRef inst m e)
register dbos key = do
  modifyMVar_ (regBodies (dbReg dbos)) (pure . Map.insert key ("body:" <> key))
  pure (MkRef (dbReg dbos) key)

-- | The scope-introduction gate at the boundary: a bare id becomes an
-- instance-anchored handle. The analogue of @retrieveWorkflow@.
mintHandle :: Applicative m => DBOS inst m -> Text -> m (WHandle inst m e)
mintHandle dbos wid = pure (MkHandle (dbConn dbos) wid)

-- | Allocate the next step id. Takes 'WCtx' and no other type: the
-- workflow owns its counter.
nextStepId :: MonadSTM m => WCtx inst exec m -> m Int
nextStepId wctx = atomically $ do
  n <- readTVar (wNextStep wctx)
  writeTVar (wNextStep wctx) (n + 1)
  pure n

-- | Run a step body under the narrowed view. The engine hands the body
-- only the 'SCtx'; the parent 'WCtx' is not in the body's named scope
-- (though Haskell closures can still capture it — see Residual_Capture).
runStep :: MonadSTM m => WCtx inst exec m -> Text -> (SCtx inst exec m -> m a) -> m a
runStep wctx _label body = body (MkSCtx wctx)

-- | Start a child: same instance (by type), id derived parent-step
-- (recovery-stable), counter advanced (position claimed at build).
startChild
  :: MonadSTM m
  => WCtx inst exec m -> WRef inst m e -> Text -> m (WHandle inst m e)
startChild wctx _ref _opts = do
  step <- nextStepId wctx
  let child = wId wctx <> "-" <> Text.pack (show step)
  pure (MkHandle (wConn wctx) child)

-- | Stub outcome read (the DB is not what this prototype is testing).
awaitChild :: MonadSTM m => WCtx inst exec m -> WHandle inst m e -> m Text
awaitChild _wctx h = pure ("outcome:" <> hId h)

-- | Claim the await's position now; drive it later.
placeAwait
  :: MonadSTM m => WCtx inst exec m -> WHandle inst m e -> m (Pending inst exec m Text)
placeAwait wctx h =
  pure (MkPending (wExecToken wctx) ("await:" <> hId h) (awaitChild wctx h))

-- | Drive a placed await. The type already demands the building
-- execution; the token backstop refuses a smuggled one at runtime
-- (the analogue of per-poll placement checks).
drive :: MonadSTM m => WCtx inst exec m -> Pending inst exec m a -> m (Either Text a)
drive wctx p
  | wExecToken wctx == pToken p = Right <$> pRun p
  | otherwise = pure (Left "StepBuiltElsewhere: pending driven in another execution")

-- Readers. Split spellings on purpose (the tree's collision-deviation
-- practice): one overloaded name for two ctx types is exactly the
-- ambiguity this design removes.
ctxWorkflowId :: WCtx inst exec m -> Text
ctxWorkflowId = wId

sctxWorkflowId :: SCtx inst exec m -> Text
sctxWorkflowId = wId . sCtx

handleId :: WHandle inst m e -> Text
handleId = hId
