{-# LANGUAGE ScopedTypeVariables #-}

-- | The widget store's per-workflow step tables — the record-of-step-functions
-- seam the dual-stack tests use (test/DBOS/Transact/WidgetTest.hs,
-- WidgetSim.hs; docs/widget-step-tables.md §1): one record per workflow,
-- keyed by the attempt's 'StepCtx', with Postgres handlers closing over the
-- step's held 'Tx' so the application write and the step checkpoint share
-- one commit. The store shape is untouched; only the workflow bodies spend
-- tables.
module WidgetStore.Steps
  ( OrderId (..),
    CheckoutSteps (..),
    DispatchSteps (..),
    pgCheckoutSteps,
    pgDispatchSteps,
  )
where

import DBOS.Transact (StepCtx, Tx (..))
import IHP.TypedSql.Id (Id' (..))
import Prelude
import WidgetStore.Store

-- | Order ids cross the steps boundary as a newtype; PG columns stay 'Int'
-- (no table migration).
newtype OrderId = OrderId Int
  deriving stock (Eq, Show)

-- | The checkout's transactional capabilities: one step per field. The
-- 'StepCtx' parameter is the capability that scopes each call — the live
-- handlers close over the held 'Tx' and do not read it.
data CheckoutSteps exec m = CheckoutSteps
  { coCreate :: StepCtx exec m -> m OrderId,
    coReserve :: StepCtx exec m -> m Bool,
    coUndo :: StepCtx exec m -> m (),
    coSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }

-- | The dispatch's capabilities. 'coSetStatus' is repeated per the
-- share-by-two rule (records are cheap; embed at three).
data DispatchSteps exec m = DispatchSteps
  { doTick :: StepCtx exec m -> OrderId -> m (),
    doSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }

-- | PG handlers behind the held connection: each op runs on the step's own
-- 'Tx', so the application writes share the step's commit.
pgCheckoutSteps :: Tx IO -> CheckoutSteps exec IO
pgCheckoutSteps (Tx run) =
  CheckoutSteps
    { coCreate = \_ -> do
        Id key <- run createOrderStatement ()
        pure (OrderId key),
      coReserve = \_ -> (> 0) <$> run reserveInventoryStatement (),
      coUndo = \_ -> run undoReserveInventoryStatement (),
      coSetStatus = \_ (OrderId oid) status -> run (setOrderStatusStatement status (Id oid)) ()
    }

pgDispatchSteps :: Tx IO -> DispatchSteps exec IO
pgDispatchSteps (Tx run) =
  DispatchSteps
    { doTick = \_ (OrderId oid) -> do
        remaining <- run (updateOrderProgressStatement (Id oid)) ()
        case remaining of
          [0] -> run (setOrderStatusStatement orderStatusDispatched (Id oid)) ()
          _ -> pure (),
      doSetStatus = \_ (OrderId oid) status -> run (setOrderStatusStatement status (Id oid)) ()
    }
