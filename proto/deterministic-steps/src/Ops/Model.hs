-- | THROWAWAY model: deterministic workflows via record-of-functions steps.
--
-- The workflow below is written once, polymorphic over @m@, against an
-- 'Ops' record carried by the scopes themselves: 'WorkflowCtx' is the
-- intake (the runner installs the handler record at construction) and
-- 'StepCtx' is the use point (bodies read it off the view). Every
-- operation additionally demands the step view per call, so even holding
-- the record at workflow scope, invoking one there is a compile error.
-- The same workflow instantiates under IO (real handlers) and under
-- IOSim (canned handlers), which is what keeps bodies off ambient IO.
-- Step results record into a per-execution journal on first run and
-- replay from it afterwards, so recovery never re-executes an effect.
module Ops.Model
  ( WorkflowCtx (..)
  , StepCtx (..)
  , Ops (..)
  , Receipt (..)
  , Journal
  , newJournal
  , withWorkflow
  , withFreshWorkflow
  , withStep
  , runStepOp
  , bump
  , readCounter
  , explodingOps
  , readInvoice
  , orderWorkflow
  ) where

import Control.Concurrent.Class.MonadSTM.Strict
  ( MonadSTM
  , StrictTVar
  , atomically
  , modifyTVar
  , newTVarIO
  , readTVar
  , readTVarIO
  , writeTVar
  )
import Data.Dynamic (Dynamic, fromDynamic, toDyn)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Typeable (Typeable)

-- | Recorded step results by step name: the execution's durable memory.
-- A real engine rows this in the system database; the shape (name-keyed,
-- hit-before-run) is what this probe is about.
type Journal = Map Text Dynamic

-- | The operations record: every non-deterministic capability a step may
-- use, each demanding the step view per call. This is the Bluefin handle
-- shape with @m@ instead of @Eff@: the record is first-class (two
-- handlers in one process, counting wrappers, per-test canned values),
-- and the 'StepCtx' parameter is the capability that scopes each call
-- to a step. Handlers are execution-independent, so one record serves
-- every execution: @(forall exec. Ops exec m)@.
data Ops exec m = Ops
  { opReadInvoice :: StepCtx exec m -> m Int
  , opFetchPrice  :: StepCtx exec m -> Text -> m Int
  , opRandomCents :: StepCtx exec m -> m Int
  , opNow         :: StepCtx exec m -> m Int
  }

-- | One execution's workflow view: its name, its journal, and the
-- installed handler record. Bodies may hold the record (to thread into
-- helpers that run steps) but never invoke it — invocation demands a
-- 'StepCtx', which only a step body holds.
data WorkflowCtx exec m = WorkflowCtx
  { wfName    :: Text
  , wfJournal :: StrictTVar m Journal
  , wfOps     :: Ops exec m
  }

-- | One attempt's narrowed view: the step name, the same journal, and
-- the same record — the capability and the authority arrive together.
data StepCtx exec m = StepCtx
  { stepName    :: Text
  , stepJournal :: StrictTVar m Journal
  , stepOps     :: Ops exec m
  }

-- | What the demo workflow returns.
data Receipt = Receipt
  { receiptInvoice :: Int
  , receiptPrice   :: Int
  , receiptTotal   :: Int
  , receiptLane    :: Text
  , receiptCents   :: Int
  , receiptAt      :: Int
  } deriving stock (Eq, Show)

-- | A handler that must never run: the replay run uses it, so any call
-- is a probe failure (surfaces as an exception, failing the run).
explodingOps :: Ops exec m
explodingOps = Ops
  { opReadInvoice = \_ -> error "replay invoked the invoice handler"
  , opFetchPrice  = \_ _ -> error "replay invoked the price handler"
  , opRandomCents = \_ -> error "replay invoked the randomness handler"
  , opNow         = \_ -> error "replay invoked the clock handler"
  }

newJournal :: MonadSTM m => m (StrictTVar m Journal)
newJournal = newTVarIO Map.empty

-- | Run a body under one execution over an explicit journal, with an
-- execution-independent handler record installed into the scope.
-- Rank-2 throughout: neither the scope nor the record's brand escapes,
-- so one run's values never leak into another's.
withWorkflow :: MonadSTM m => StrictTVar m Journal -> (forall exec. Ops exec m) -> Text -> (forall exec. WorkflowCtx exec m -> m a) -> m a
withWorkflow journal ops name body = body (WorkflowCtx name journal ops)

-- | 'withWorkflow' over a fresh journal: two runs share nothing.
withFreshWorkflow :: MonadSTM m => (forall exec. Ops exec m) -> Text -> (forall exec. WorkflowCtx exec m -> m a) -> m a
withFreshWorkflow ops name body = newJournal >>= \journal -> withWorkflow journal ops name body

-- | Run one step under the narrowed view, moving the journal and the
-- record into it. No depth counter here: the scope split itself is the
-- guard — only a 'StepCtx' reaches the body.
withStep :: MonadSTM m => WorkflowCtx exec m -> Text -> (StepCtx exec m -> m a) -> m a
withStep wctx name body = body (StepCtx name (wfJournal wctx) (wfOps wctx))

-- | Replay-or-run: a recorded slot returns its value without running the
-- action; otherwise the action runs once and its value is recorded. This
-- is the whole determinism payoff — recovery observes the same values
-- the original execution did, whatever the handlers would return now.
runStepOp :: (MonadSTM m, Typeable a) => StepCtx exec m -> m a -> m a
runStepOp sctx action = do
  recorded <- atomically $ do
    journal <- readTVar (stepJournal sctx)
    case Map.lookup (stepName sctx) journal of
      Nothing -> pure Nothing
      Just stored -> case fromDynamic stored of
        Nothing -> error ("journal type changed for step " <> Text.unpack (stepName sctx))
        Just value -> pure (Just value)
  case recorded of
    Just value -> pure value
    Nothing -> do
      value <- action
      atomically $ do
        journal <- readTVar (stepJournal sctx)
        writeTVar (stepJournal sctx) (Map.insert (stepName sctx) (toDyn value) journal)
      pure value

bump :: MonadSTM m => StrictTVar m Int -> m ()
bump counter = atomically (modifyTVar counter (+ 1))

readCounter :: MonadSTM m => StrictTVar m Int -> m Int
readCounter = readTVarIO

-- | An application-level helper: takes the record explicitly (threaded
-- from the workflow's view) and runs its own step. The realistic
-- structuring — helpers compose steps, the engine scopes them.
readInvoice :: MonadSTM m => Ops exec m -> WorkflowCtx exec m -> m Int
readInvoice ops wctx = withStep wctx "read-invoice" $ \s ->
  runStepOp s (opReadInvoice ops s)

-- | The demo workflow, written once against the carried record. Pure
-- orchestration: branching on recorded step results is deterministic
-- given the same journal, which is exactly the skill's Correct shape
-- (choice from a checkpointed value, never from ambient
-- non-determinism). The invoice arrives via a helper (explicit
-- threading from the workflow view); the rest reads the step view.
orderWorkflow :: MonadSTM m => WorkflowCtx exec m -> m Receipt
orderWorkflow wctx = do
  invoice <- readInvoice (wfOps wctx) wctx
  price <- withStep wctx "fetch-price" $ \s ->
    runStepOp s (opFetchPrice (stepOps s) s "widget")
  let total = invoice + price
      lane = if total > 5000 then "express" else "standard"
  (cents, at) <- withStep wctx lane $ \s -> runStepOp s $ do
    cents <- opRandomCents (stepOps s) s
    at <- opNow (stepOps s) s
    pure (cents, at)
  pure (Receipt invoice price total lane cents at)
