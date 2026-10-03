{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | THROWAWAY prototype model (not production code).
--
-- Phase 2 adds the scope-depth backstop behind the 'WCtx'/'SCtx' split:
-- 'inst' is the instance scope (one per 'withInstance' region), 'exec'
-- the execution scope (one per 'withExecution' region), and a depth
-- counter in the shared per-execution state refuses allocations that
-- arrive while a step body is running — including through a captured
-- parent context. Constructors are hidden; the export list is the privacy
-- boundary, the way 'Ctx''s private constructor is in the real tree.
--
-- Simplifications vs the real tree (documented, not hidden): the step
-- status carries a fixed id (the real tree threads the allocated id in);
-- 'placeAwait' does not degrade inside steps (real 'awaitChild' degrades
-- to unrecorded via the leaf rule — out of focus here); outcomes are stub
-- text (no DB in this prototype).
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
  , StepStatus (..)
  , StepScope
    -- * Region binders (the only place a scope variable is introduced)
  , withInstance
  , withExecution
    -- * Operations
  , register
  , mintHandle
  , nextStepId
  , placeCall
  , withAttempt
  , startChild
  , awaitChild
  , placeAwait
  , drive
    -- * Readers (note the split spellings: no overloaded-field games)
  , ctxWorkflowId
  , sctxWorkflowId
  , handleId
  , stepStatusOf
  , stepMarkerOf
  , cancelStep
  , stepCancelled
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
  , readTVarIO
  , writeTVar
  )
import Control.Monad.Class.MonadThrow qualified as MThrow
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

-- | The per-execution workflow context. Owns the step counter, the marker
-- counter, and the scope-depth counter: 'nextStepId' and 'placeCall' take
-- this type and no other, so id allocation belongs to the workflow — and
-- the depth counter lets allocation refuse while a step body runs, even
-- through a captured parent value.
data WCtx inst exec m = MkWCtx
  { wConn :: Connection inst m
  , wId :: Text
  , wNextStep :: StrictTVar m Int
  , wNextMarker :: StrictTVar m Int
  , wDepth :: StrictTVar m Int
  , wExecToken :: Int
  }

-- | What a step body may read about its own attempt. Fixed id here; the
-- real tree threads the allocated step id in (see module header).
data StepStatus = MkStatus
  { statusStep :: Int
  , statusAttempt :: Int
  }
  deriving stock (Eq, Show)

-- | One attempt's scope: marker (per attempt, never persisted), status,
-- and a fresh cancellation token. Constructor hidden: attempts are minted
-- only by 'withAttempt'.
data StepScope m = MkScope
  { scopeMarker :: Int
  , scopeStatus :: StepStatus
  , scopeToken :: StrictTVar m Bool
  }

-- | The narrowed per-attempt view handed to step bodies. No counter, no
-- placement: the only allocation-adjacent accessor the export list offers
-- reads the id. (Deliberate: the capture shape still compiles — see the
-- backstop exe — but the handed shape cannot allocate.)
data SCtx inst exec m = MkSCtx
  { sCtx :: WCtx inst exec m
  , sScope :: StepScope m
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
-- per-execution step counter, marker counter, scope depth, and the
-- runtime token the 'Pending' backstop compares.
withExecution
  :: MonadSTM m
  => DBOS inst m -> Text -> (forall exec. WCtx inst exec m -> m a) -> m a
withExecution dbos wid use = do
  token <- atomically $ do
    n <- readTVar (dbExecCount dbos)
    writeTVar (dbExecCount dbos) (n + 1)
    pure n
  counter <- newTVarIO 0
  markers <- newTVarIO 0
  depth <- newTVarIO 0
  use (MkWCtx (dbConn dbos) wid counter markers depth token)

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
-- workflow owns its counter. (Depth is checked by 'placeCall', the only
-- engine path that allocates positions; this is the raw counter beneath
-- it, the way the real tree's counter sits beneath placement.)
nextStepId :: MonadSTM m => WCtx inst exec m -> m Int
nextStepId wctx = atomically $ do
  n <- readTVar (wNextStep wctx)
  writeTVar (wNextStep wctx) (n + 1)
  pure n

-- | Claim a replay position for a named call, refusing while a step body
-- is running. The depth lives in the shared per-execution state, so a
-- captured parent value sees the bumped depth exactly like the handed
-- view does: this is the runtime backstop behind the type split, and the
-- analogue of the real tree's @InsideStep@ refusal.
placeCall :: MonadSTM m => WCtx inst exec m -> Text -> m (Either Text Int)
placeCall wctx op = do
  depth <- readTVarIO (wDepth wctx)
  if depth > 0
    then pure (Left ("InsideStep: allocating " <> op <> " inside a step body"))
    else Right <$> nextStepId wctx

-- | Run one attempt under a bumped depth with a fresh marker and token.
-- 'finally' restores the depth however the body ends (value, refusal, or
-- thrown exception), so a dead attempt never locks its workflow out of
-- allocating again. The engine hands the body only the 'SCtx'.
withAttempt
  :: (MonadSTM m, MThrow.MonadMask m)
  => WCtx inst exec m -> Text -> (SCtx inst exec m -> m a) -> m a
withAttempt wctx _label body = do
  marker <- atomically $ do
    n <- readTVar (wNextMarker wctx)
    writeTVar (wNextMarker wctx) (n + 1)
    pure n
  token <- newTVarIO False
  let enter = atomically $ do
        d <- readTVar (wDepth wctx)
        writeTVar (wDepth wctx) (d + 1)
      leave = atomically $ do
        d <- readTVar (wDepth wctx)
        writeTVar (wDepth wctx) (d - 1)
      scope = MkScope marker (MkStatus 0 1) token
  MThrow.finally (enter >> body (MkSCtx wctx scope)) leave

-- | Start a child: same instance (by type), position claimed through the
-- depth-checked 'placeCall' (so capture-shape starts are refused at
-- runtime), id derived parent-step (recovery-stable).
startChild
  :: MonadSTM m
  => WCtx inst exec m -> WRef inst m e -> Text -> m (Either Text (WHandle inst m e))
startChild wctx _ref _opts = do
  placed <- placeCall wctx "startChild"
  case placed of
    Left err -> pure (Left err)
    Right step -> do
      let child = wId wctx <> "-" <> Text.pack (show step)
      pure (Right (MkHandle (wConn wctx) child))

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

-- | What the attempt may read about itself.
stepStatusOf :: SCtx inst exec m -> StepStatus
stepStatusOf = scopeStatus . sScope

-- | Which attempt-body this is (per attempt, never persisted).
stepMarkerOf :: SCtx inst exec m -> Int
stepMarkerOf = scopeMarker . sScope

-- | Fire this attempt's own token. It starts unfired per attempt; nothing
-- here touches any other attempt's.
cancelStep :: MonadSTM m => SCtx inst exec m -> m ()
cancelStep sctx = atomically (writeTVar (scopeToken (sScope sctx)) True)

-- | Whether this attempt's token has fired.
stepCancelled :: MonadSTM m => SCtx inst exec m -> m Bool
stepCancelled sctx = readTVarIO (scopeToken (sScope sctx))
