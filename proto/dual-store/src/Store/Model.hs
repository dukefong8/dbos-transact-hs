-- | THROWAWAY model: dual-stack transactional steps as a record.
--
-- One workflow source against a 'StepOps' record. The record travels
-- through 'WorkflowCtx'; 'withStep' inherits it into 'StepCtx'
-- implicitly, and step bodies retrieve it by field name ('stepOps').
-- Each step is its own transaction on both stacks (Postgres
-- BEGIN/COMMIT/ROLLBACK under IO, 'atomically' under IOSim):
-- atomicity unit = one step invocation.
module Store.Model
  ( StepOps (..)
  , WorkflowCtx (..)
  , StepCtx (..)
  , OrderId (..)
  , withWorkflow
  , withStep
  , placeOrder
  , raceOrders
  , probeInsufficient
  , probeRollback
  ) where

import Control.Concurrent.Class.MonadMVar.Strict (MonadMVar, newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException)
import Control.Monad (forM_, replicateM)
import Control.Monad.Class.MonadFork (MonadFork, forkIO)
import Control.Monad.Class.MonadThrow (MonadCatch, try)
import Data.Either (isLeft)
import Data.Functor (void)
import Data.Text (Text)

newtype OrderId = OrderId Int
  deriving stock (Eq, Show)

-- | The step table: every transactional capability the workflow may
-- use. Handlers differ per interpreter (Postgres vs STM); the workflow
-- below never names either. The rank-2 'stepTrace' is a step
-- polymorphic in what it carries, tapped inside the scope it audits.
data StepOps m = StepOps
  { stepReserve    :: Text -> Int -> m (Either Text OrderId)
  , stepBomb       :: Text -> Int -> m (Either Text OrderId)
  , stepTrace      :: forall a. Text -> a -> m a
  , stepStock      :: Text -> m Int
  , stepOrderCount :: m Int
  , stepAuditCount :: m Int
  }

-- | One execution's workflow view: the installed step table plus a
-- name. Carries, never spends — invocation happens in steps.
data WorkflowCtx m = WorkflowCtx
  { wfSteps :: StepOps m
  , wfName  :: Text
  }

-- | One step's narrowed view: the same table, inherited implicitly by
-- 'withStep', plus the step name. Bodies retrieve the table by field
-- name ('stepOps') — no threading, no second parameter.
data StepCtx m = StepCtx
  { stepOps  :: StepOps m
  , stepName :: Text
  }

-- | Run a body under one execution with a step table installed.
withWorkflow :: StepOps m -> Text -> (WorkflowCtx m -> m a) -> m a
withWorkflow ops name body = body (WorkflowCtx ops name)

-- | Run one step under the narrowed view. The table moves along
-- implicitly — call sites name the step, never the record.
withStep :: WorkflowCtx m -> Text -> (StepCtx m -> m a) -> m a
withStep wctx name body = body (StepCtx (wfSteps wctx) name)

-- | One order flow: audit the attempt, then reserve. Two steps, two
-- scopes — a loser's audit row commits while its order never exists,
-- which is exactly the attempt-vs-effect distinction.
placeOrder :: Monad m => WorkflowCtx m -> Text -> Int -> m (Either Text OrderId)
placeOrder wctx item qty = do
  _ <- withStep wctx "trace-attempt" $ \s ->
    stepTrace (stepOps s) ("attempt:" <> item) qty
  withStep wctx "reserve" $ \s ->
    stepReserve (stepOps s) item qty

-- | N placers racing one stock, same source under both interpreters.
-- One context shared across threads: it is plain data, nothing to lock.
raceOrders :: (Monad m, MonadFork m, MonadMVar m) => WorkflowCtx m -> Int -> Text -> Int -> m [Either Text OrderId]
raceOrders wctx n item qty = do
  boxes <- replicateM n newEmptyMVar
  forM_ boxes $ \box -> void (forkIO (placeOrder wctx item qty >>= putMVar box))
  mapM takeMVar boxes

-- | Refusal probe: absurd quantity leaves every table untouched except
-- the attempt audit. Returns (refused, stock, orders, audits).
probeInsufficient :: Monad m => WorkflowCtx m -> Text -> Int -> m (Bool, Int, Int, Int)
probeInsufficient wctx item qty = do
  let ops = wfSteps wctx
  outcome <- placeOrder wctx item qty
  stock <- stepStock ops item
  orders <- stepOrderCount ops
  audits <- stepAuditCount ops
  pure (isLeft outcome, stock, orders, audits)

-- | Rollback probe: a step that writes then throws must vanish whole —
-- order, audit, everything. Returns (threw, stock, orders, audits).
probeRollback :: forall m. (Monad m, MonadCatch m) => WorkflowCtx m -> Text -> Int -> m (Bool, Int, Int, Int)
probeRollback wctx item qty = do
  let ops = wfSteps wctx
  outcome <- try (withStep wctx "bomb" $ \s -> stepBomb (stepOps s) item qty) :: m (Either SomeException (Either Text OrderId))
  stock <- stepStock ops item
  orders <- stepOrderCount ops
  audits <- stepAuditCount ops
  pure (isLeft outcome, stock, orders, audits)
