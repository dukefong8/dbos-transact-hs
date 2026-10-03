{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | THROWAWAY prototype model (not production code).
--
-- Phase 3: @inst@ dropped (decision: the DBOS object is singleton-like
-- per process, so cross-instance mixing stays a runtime refusal per the
-- oracle). What remains is execution scoping (@exec@), the
-- WorkflowCtx/StepCtx split, and the scope-depth backstop — all over an
-- explicitly passed, unscoped 'DBOS' value. Constructors are hidden; the
-- export list is the privacy boundary.
--
-- Simplifications vs the real tree (documented, not hidden): the step
-- status carries a fixed id (the real tree threads the allocated id in);
-- 'placeAwait' does not degrade inside steps (real 'awaitChild' degrades
-- to unrecorded via the leaf rule — out of focus here); outcomes are stub
-- text (no DB in this prototype); connection/registry ids derive from the
-- given name (the real tree mints a UUID per connection — two
-- @newDBOS \"a\"@ values would share an id here, so tests use distinct
-- names).
module Scope.Model
  ( -- * Values (constructors hidden)
    DBOS
  , Connection
  , Registry
  , WRef
  , WHandle
  , WorkflowCtx
  , StepCtx
  , Pending
  , StepStatus (..)
  , StepScope
  , Pool
  , PinnedConn
  , newPool
  , checkout
  , releasePin
  , useIn
  , rawAcquire
  , rawRelease
  , poolHighWater
    -- * Region binder (the only place a scope variable is introduced)
  , newDBOS
  , withWorkflow
    -- * Operations
  , register
  , mintHandle
  , nextStepId
  , placeCall
  , withStep
  , startChild
  , awaitChild
  , placeAwait
  , drive
    -- * Readers
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
  , retry
  , writeTVar
  )
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as Text

-- NOTE on naming: the value types kept their short prototype spellings
-- (WorkflowCtx/StepCtx/WRef/WHandle); only the binders were renamed to
-- value-named form (withDBOS/withWorkflow/withStep). Reader spellings
-- predate that pass and are unchanged here.
data Connection m = MkConn
  { connTag :: Text
  , connState :: StrictTVar m Int
  }

-- | Instance registry. Carries the instance id the connection was opened
-- with, which is what 'startChild' compares for the runtime cross-
-- instance refusal (the analogue of ADR-0018's bound registry).
data Registry m = MkReg
  { regBodies :: StrictMVar m (Map Text Text)
  , regInstance :: Text
  }

-- | The launched-instance bundle. Deliberately unscoped: at most one
-- lives per process in production, and tests name theirs distinctly.
data DBOS m = MkDBOS
  { dbConn :: Connection m
  , dbReg :: Registry m
  , dbExecCount :: StrictTVar m Int
  , dbName :: Text
  }

-- | A registered workflow. Unscoped (durable across executions); the
-- instance it belongs to is the runtime 'regInstance' above.
data WRef m e = MkRef
  { refReg :: Registry m
  , refKey :: Text
  }

-- | A running-or-finished workflow, by id. Unscoped, like the ref.
data WHandle m e = MkHandle
  { hConn :: Connection m
  , hId :: Text
  }

-- | The per-execution workflow context. Owns the step counter, the marker
-- counter, and the scope-depth counter: 'nextStepId' and 'placeCall' take
-- this type and no other, so id allocation belongs to the workflow — and
-- the depth counter lets allocation refuse while a step body runs, even
-- through a captured parent value.
data WorkflowCtx exec m = MkWorkflowCtx
  { wConn :: Connection m
  , wId :: Text
  , wNextStep :: StrictTVar m Int
  , wNextMarker :: StrictTVar m Int
  , wDepth :: StrictTVar m Int
  , wExecToken :: Int
  }

-- | What a step body may read about the attempt it runs as.
data StepStatus = MkStatus
  { statusStep :: Int
  , statusAttempt :: Int
  }
  deriving stock (Eq, Show)

-- | One attempt's scope: marker (per attempt, never persisted), status,
-- and a fresh cancellation token. Constructor hidden: attempts are minted
-- only by 'withStep'.
data StepScope m = MkScope
  { scopeMarker :: Int
  , scopeStatus :: StepStatus
  , scopeToken :: StrictTVar m Bool
  }

-- | The narrowed per-attempt view handed to step bodies. No counter, no
-- placement: the only accessor the export list offers reads the id.
data StepCtx exec m = MkStepCtx
  { sCtx :: WorkflowCtx exec m
  , sScope :: StepScope m
  }

-- | A placed-but-undriven await, carrying the building execution's token
-- so a drive from another execution is both ill-typed (the 'exec'
-- parameter) and, at runtime, refused (the token backstop).
data Pending exec m a = MkPending
  { pToken :: Int
  , pLabel :: Text
  , pRun :: m a
  }

-- | Build an instance bundle. Unscoped by decision: cross-instance
-- confusion is the runtime 'WrongInstance' refusal (oracle parity), not a
-- type error. Ids derive from the name; use distinct names per object.
newDBOS
  :: (MonadSTM m, MonadMVar m)
  => Text -> m (DBOS m)
newDBOS name = do
  st <- newTVarIO 0
  bodies <- newMVar Map.empty
  count <- newTVarIO 0
  let tag = "conn-" <> name
  pure (MkDBOS (MkConn tag st) (MkReg bodies tag) count name)

-- | Bind the execution scope for one workflow run. Mints the
-- per-execution step counter, marker counter, scope depth, and the
-- runtime token the 'Pending' backstop compares. Each call gets a fresh
-- rigid 'exec': values from one run cannot be driven in another.
withWorkflow
  :: MonadSTM m
  => DBOS m -> Text -> (forall exec. WorkflowCtx exec m -> m a) -> m a
withWorkflow dbos wid use = do
  token <- atomically $ do
    n <- readTVar (dbExecCount dbos)
    writeTVar (dbExecCount dbos) (n + 1)
    pure n
  counter <- newTVarIO 0
  markers <- newTVarIO 0
  depth <- newTVarIO 0
  use (MkWorkflowCtx (dbConn dbos) wid counter markers depth token)

-- | Register under this instance's registry.
register :: MonadMVar m => DBOS m -> Text -> m (WRef m e)
register dbos key = do
  modifyMVar_ (regBodies (dbReg dbos)) (pure . Map.insert key ("body:" <> key))
  pure (MkRef (dbReg dbos) key)

-- | The scope-introduction gate at the boundary: a bare id becomes a
-- handle on this instance's connection. The analogue of
-- @retrieveWorkflow@.
mintHandle :: Applicative m => DBOS m -> Text -> m (WHandle m e)
mintHandle dbos wid = pure (MkHandle (dbConn dbos) wid)

-- | Allocate the next step id. Takes 'WorkflowCtx' and no other type: the
-- workflow owns its counter. (Depth is checked by 'placeCall', the only
-- engine path that allocates positions; this is the raw counter beneath
-- it, the way the real tree's counter sits beneath placement.)
nextStepId :: MonadSTM m => WorkflowCtx exec m -> m Int
nextStepId wctx = atomically $ do
  n <- readTVar (wNextStep wctx)
  writeTVar (wNextStep wctx) (n + 1)
  pure n

-- | Claim a replay position for a named call, refusing while a step body
-- is running. The depth lives in the shared per-execution state, so a
-- captured parent value sees the bumped depth exactly like the handed
-- view does: this is the runtime backstop behind the type split, and the
-- analogue of the real tree's @InsideStep@ refusal.
placeCall :: MonadSTM m => WorkflowCtx exec m -> Text -> m (Either Text Int)
placeCall wctx op = do
  depth <- readTVarIO (wDepth wctx)
  if depth > 0
    then pure (Left ("InsideStep: allocating " <> op <> " inside a step body"))
    else Right <$> nextStepId wctx

-- | Run one attempt under a bumped depth with a fresh marker and token.
-- 'finally' restores the depth however the body ends (value, refusal, or
-- thrown exception), so a dead attempt never locks its workflow out of
-- allocating again. The engine hands the body only the 'StepCtx'.
withStep
  :: (MonadSTM m, MThrow.MonadMask m)
  => WorkflowCtx exec m -> Text -> (StepCtx exec m -> m a) -> m a
withStep wctx _label body = do
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
  MThrow.finally (enter >> body (MkStepCtx wctx scope)) leave

-- | Start a child: position claimed through the depth-checked 'placeCall'
-- (so capture-shape starts are refused at runtime), id derived
-- parent-step (recovery-stable). A ref from another instance is refused
-- before anything allocates, per the oracle's placement order
-- (ADR-0018, runtime by decision).
startChild
  :: MonadSTM m
  => WorkflowCtx exec m -> WRef m e -> Text -> m (Either Text (WHandle m e))
startChild wctx ref _opts = do
  let refInstance = regInstance (refReg ref)
      ctxInstance = connTag (wConn wctx)
  if refInstance /= ctxInstance
    then pure (Left ("WrongInstance: ref from " <> refInstance <> " started under " <> ctxInstance))
    else do
      placed <- placeCall wctx "startChild"
      case placed of
        Left err -> pure (Left err)
        Right step -> do
          let child = wId wctx <> "-" <> Text.pack (show step)
          pure (Right (MkHandle (wConn wctx) child))

-- | Stub outcome read (the DB is not what this prototype is testing).
awaitChild :: MonadSTM m => WorkflowCtx exec m -> WHandle m e -> m Text
awaitChild _wctx h = pure ("outcome:" <> hId h)

-- | Claim the await's position now; drive it later.
placeAwait
  :: MonadSTM m => WorkflowCtx exec m -> WHandle m e -> m (Pending exec m Text)
placeAwait wctx h =
  pure (MkPending (wExecToken wctx) ("await:" <> hId h) (awaitChild wctx h))

-- | Drive a placed await. The type already demands the building
-- execution; the token backstop refuses a smuggled one at runtime
-- (the analogue of per-poll placement checks).
drive :: MonadSTM m => WorkflowCtx exec m -> Pending exec m a -> m (Either Text a)
drive wctx p
  | wExecToken wctx == pToken p = Right <$> pRun p
  | otherwise = pure (Left "StepBuiltElsewhere: pending driven in another execution")

-- Readers.
ctxWorkflowId :: WorkflowCtx exec m -> Text
ctxWorkflowId = wId

sctxWorkflowId :: StepCtx exec m -> Text
sctxWorkflowId = wId . sCtx

handleId :: WHandle m e -> Text
handleId = hId

-- | What the attempt may read about itself.
stepStatusOf :: StepCtx exec m -> StepStatus
stepStatusOf = scopeStatus . sScope

-- | Which attempt-body this is (per attempt, never persisted).
stepMarkerOf :: StepCtx exec m -> Int
stepMarkerOf = scopeMarker . sScope

-- | Fire this attempt's own token. It starts unfired per attempt; nothing
-- here touches any other attempt's.
cancelStep :: MonadSTM m => StepCtx exec m -> m ()
cancelStep sctx = atomically (writeTVar (scopeToken (sScope sctx)) True)

-- | Whether this attempt's token has fired.
stepCancelled :: MonadSTM m => StepCtx exec m -> m Bool
stepCancelled sctx = readTVarIO (scopeToken (sScope sctx))

-- * Pinned pool connections (phase 3: connection affinity, scoped)
--
-- A transactional step needs every statement of one attempt on one
-- physical connection, which a pool will not promise per statement
-- ('Pool.use' may hand out a different connection each time). The answer
-- is pinning: check one connection out for the attempt's lifetime and
-- return it after. The pin is exec-branded (a pin from another execution
-- is ill-typed) and generation-counted (use after release is a runtime
-- refusal), and checkout is refused inside a step body (same uniform
-- depth == 0 rule as 'placeCall': nested pins would double pool
-- pressure the way nested allocations would shift replay slots).
--
-- The blocking take is STM 'retry', so under IOSim a starved checkout
-- waits in virtual time deterministically; the real tree layers its
-- acquisition timeout on top of the same semantics.

-- | Pool state: available ids, currently held count, high-water mark,
-- current generation per id (-1 = released), next generation, and the
-- out-of-band raw-acquire count. One TVar: checkout/release compose
-- atomically with the depth read.
data PoolState = MkPoolState
  { psAvail :: [Int]
  , psHeld :: Int
  , psHigh :: Int
  , psGen :: Map Int Int
  , psNextGen :: Int
  , psRaw :: Int
  }

-- | A bounded pool of fake connections. Capacity is fixed: checkout past
-- it waits (STM 'retry'), it never opens more.
data Pool m = MkPool
  { poolCap :: Int
  , poolState :: StrictTVar m PoolState
  }

-- | A pinned connection: its id plus the generation that proves it is
-- still held. Exec-branded so a pin cannot be used from another
-- execution; the generation so a released pin cannot be reused.
data PinnedConn exec = MkPinned
  { pinnedConn :: Int
  , pinnedGen :: Int
  }

-- | A pool with the given capacity, all connections available.
newPool :: MonadSTM m => Int -> m (Pool m)
newPool cap = do
  st <- newTVarIO (MkPoolState [1 .. cap] 0 0 Map.empty 0 0)
  pure (MkPool cap st)

-- | Check one connection out for this execution. Refused inside a step
-- body (uniform depth rule); waits while the pool is empty. The pin it
-- returns is bound to this execution by type and to this checkout by
-- generation.
checkout :: MonadSTM m => Pool m -> WorkflowCtx exec m -> m (Either Text (PinnedConn exec))
checkout pool wctx = atomically $ do
  depth <- readTVar (wDepth wctx)
  if depth > 0
    then pure (Left ("InsideStep: pinning a connection inside a step body"))
    else do
      st <- readTVar (poolState pool)
      case psAvail st of
        [] -> retry
        (c : cs) -> do
          let g = psNextGen st
          writeTVar (poolState pool) $
            st
              { psAvail = cs,
                psHeld = psHeld st + 1,
                psHigh = max (psHigh st) (psHeld st + 1),
                psGen = Map.insert c g (psGen st),
                psNextGen = g + 1
              }
          pure (Right (MkPinned c g))

-- | Return a pin. Invalidates its generation first, so a later use of the
-- same value is refused rather than silently landing on a recycled id.
releasePin :: MonadSTM m => PinnedConn exec -> Pool m -> m ()
releasePin (MkPinned c _) pool = atomically $ do
  st <- readTVar (poolState pool)
  writeTVar (poolState pool) $
    st
      { psAvail = c : psAvail st,
        psHeld = psHeld st - 1,
        psGen = Map.insert c (-1) (psGen st)
      }

-- | Use a pinned connection under this execution's context. The context
-- is what ties the use to the execution at the type level; the generation
-- is what refuses a use after release at runtime.
useIn :: MonadSTM m => WorkflowCtx exec m -> PinnedConn exec -> Pool m -> m (Either Text Int)
useIn _wctx (MkPinned c g) pool = atomically $ do
  st <- readTVar (poolState pool)
  case Map.lookup c (psGen st) of
    Just g' | g' == g -> pure (Right c)
    _ -> pure (Left "use-after-release: pinned connection no longer held")

-- | Open a connection past the pool, the way today's per-attempt raw
-- 'Connection.acquire' bypasses pool limits. Shares the high-water
-- accounting so the contrast is measurable: raw opens never wait and the
-- high-water mark shows it.
rawAcquire :: MonadSTM m => Pool m -> m Int
rawAcquire pool = atomically $ do
  st <- readTVar (poolState pool)
  let n = psRaw st + 1
  writeTVar (poolState pool) $
    st
      { psHeld = psHeld st + 1,
        psHigh = max (psHigh st) (psHeld st + 1),
        psRaw = n
      }
  pure (-n)

-- | Give a raw connection back (bookkeeping only).
rawRelease :: MonadSTM m => Pool m -> m ()
rawRelease pool = atomically $ do
  st <- readTVar (poolState pool)
  writeTVar (poolState pool) (st {psHeld = psHeld st - 1})

-- | Highest simultaneously-held count so far: the observable the cap
-- assertion reads.
poolHighWater :: MonadSTM m => Pool m -> m Int
poolHighWater pool = psHigh <$> readTVarIO (poolState pool)
