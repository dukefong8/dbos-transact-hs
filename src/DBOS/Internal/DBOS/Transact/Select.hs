{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A durable race over steps. Mirrors Rust @select.rs@: the race claims a
-- step id of its own — after every branch has claimed theirs — reads
-- whether it already has a recorded winner, and writes which branch won
-- once it has one. A branch's result is already recorded under the
-- branch's own step id, so the race records the position and never the
-- value.
--
-- Branches disagree about what they return, which is the whole reason a
-- race exists; a list cannot hold that, so each arm carries its
-- continuation existentially and 'selectStep' keeps the result typed for
-- the caller.
--
-- The oracle ships this surface as @select_step!@, a procedural macro
-- whose only job is syntax: it invents per-branch names, matches the
-- winning index back to a slot, and refuses shapes a function cannot
-- (fewer than two arms, guards, an @else@). Those refusals guard misuse
-- that would silently change what a replay decides — but the semantic
-- fences here are types, not syntax, and they are already in place: only a
-- 'PendingStep' can be an arm, so a start or a whole run cannot be raced,
-- and the arm's continuation decides what a branch's failure means.
--
-- This port deliberately does not ship a macro (deviation, recorded in the
-- plan): the combinator is the API; the fewer-than-two refusal is the one
-- check the macro owned and it happens at run time, before any id is
-- claimed; and guards or @else@ arms have no counterpart to refuse because
-- they are not expressible as arms at all.
module DBOS.Transact.Select
  ( -- * The race's own checkpoint
    Racing (..),
    Recording (..),
    Branches (..),
    newBranches,
    pushBranch,
    checkSelect,
    recordSelect,
    controlError,

    -- * Racing
    SelectArm (..),
    Winner (..),
    selectStep,
  )
where

import DBOS.Prelude
import Control.Monad.Class.MonadAsync (race)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Applicative ((<|>))
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Outcome (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), StepTiming (..), Timestamp, WorkflowId (..), selectStepStepName, timestampNow)
import DBOS.Transact.Checkpoint (PendingStep (..), StepPlacement (..), pendingStepId, placeCall)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Context (WorkflowCtx, workflowConnection, workflowCtxId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Serialization (CodecError, decodeWorkflowValue, encodeWorkflowValue)

-- | What 'checkSelect' found: a winner already recorded, or a race still
-- to run. Two states rather than an 'Option' beside a loose index, so the
-- caller must handle both.
data Racing exec m
  = -- | This select already ran. Drive **only** this branch, which then
    -- replays from its own step row without running. Carries the select's
    -- own step id so a stale-winner refusal names where it refused.
    Replay Int Int
  | -- | No checkpoint yet. Race the branches, then hand the winner to
    -- 'recordSelect'.
    Fresh (Recording exec m)
  deriving stock (Show)

-- | A checkpoint that has been claimed and not yet written: the placement
-- it claimed, and the instant from *before* the wait, so the recorded
-- duration covers the waiting.
data Recording exec m = Recording
  { recordingPlacement :: StepPlacement exec m,
    recordingStartedAt :: Timestamp
  }
  deriving stock (Show)

-- | The branches of one race: what each is called and what id it claimed,
-- in build order. Read while the branches are still alive, because the
-- losers are dropped as soon as the race is decided and a stale-winner
-- refusal has to be able to name a branch that is gone. Only a
-- 'PendingStep' is pushed, which is what keeps a start or a whole run out
-- of a race.
newtype Branches = Branches {branchIdentities :: [(Text, Maybe Int)]}

-- | An empty set; every race pushes at least two.
newBranches :: Branches
newBranches = Branches []

-- | Records what this branch is called and which id it claimed. Mirrors
-- Rust @Branches::push@.
pushBranch :: PendingStep exec m a -> Branches -> Branches
pushBranch branch (Branches identities) =
  Branches (identities <> [(branch.name, pendingStepId branch)])

-- | The branches this run built, as the stale-winner refusal names them:
-- @0: charge (step 3), 1: timeout (step 4)@.
summarize :: Branches -> Text
summarize (Branches identities) =
  Text.intercalate ", " $
    zipWith
      ( \at (branchName, stepId) ->
          showText at <> ": " <> branchName <> case stepId of
            Just step -> " (step " <> showText step <> ")"
            Nothing -> " (uncheckpointed)"
      )
      [0 :: Int ..]
      identities

-- | How many branches this race has, which is the set a recorded winner is
-- checked against.
branchCount :: Branches -> Int
branchCount (Branches identities) = length identities

-- | Claims this select's step id and asks whether it already has a winner.
-- Called after every branch is built: the branches take their ids as they
-- are constructed and the select's own id follows them, so a select that
-- took its id first would leave every branch one slot higher than the
-- replay expects. Holds a recorded winner to the branches that exist now.
checkSelect :: (MonadSTM m, MonadTime m) => WorkflowCtx exec m -> Branches -> m (Either (TransactError.Error TransactError.EngineOnly) (Racing exec m))
checkSelect wctx branches = do
  placement <- placeCall wctx
  startedAt <- timestampNow
  case placement of
    Recorded wctx' stepId' -> do
      let conn = workflowConnection wctx'
          parent = WorkflowId (workflowCtxId wctx')
      checked <- runSystemDB conn.connSysdb (\db -> SystemDB.checkStep db parent stepId' selectStepStepName)
      case checked of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right Nothing -> pure (Right (Fresh (Recording placement startedAt)))
        Right (Just recorded) ->
          case decodeWorkflowValue "the branch that won a select" (Just (SerializedWorkflowValue (fromMaybe "" recorded.stepRecordOutput) (Serialization <$> recorded.stepRecordSerialization))) :: Either CodecError Int of
            Left _ ->
              pure
                ( Left
                    ( TransactError.ErrorSystemDatabase
                        ( SystemDBError.UnexpectedStep
                            { workflowId = workflowCtxId wctx',
                              stepId = stepId',
                              expected = "a recorded select winner",
                              recorded = "a select row with no decodable winner"
                            }
                        )
                    )
                )
            Right winner
              | winner >= branchCount branches ->
                  pure
                    ( Left
                        ( TransactError.ErrorSystemDatabase
                            ( SystemDBError.UnexpectedStep
                                { workflowId = workflowCtxId wctx',
                                  stepId = stepId',
                                  expected =
                                    "a select over " <> showText (branchCount branches) <> " branches — " <> summarize branches,
                                  recorded = "a select won by branch " <> showText winner <> ", which no longer exists"
                                }
                            )
                        )
                    )
              | otherwise -> pure (Right (Replay winner stepId'))
    _ -> pure (Right (Fresh (Recording placement startedAt)))

-- | Writes which branch won, once the race has one. Where nothing is
-- checkpointed — outside a workflow, or inside a step body — this writes
-- nothing and the race was a plain one.
recordSelect :: (MonadSTM m, MonadTime m) => Recording exec m -> Int -> m (Either (TransactError.Error TransactError.EngineOnly) ())
recordSelect recording winner = case recording.recordingPlacement of
  Recorded wctx stepId' -> do
    completedAt <- timestampNow
    let conn = workflowConnection wctx
        encoded = encodeWorkflowValue winner
        serialization = case encoded.serializedSerialization of
          Nothing -> Nothing
          Just (Serialization serializationName') -> Just serializationName'
        timing = Just (StepTiming recording.recordingStartedAt completedAt)
    written <-
      runSystemDB conn.connSysdb $ \db ->
        SystemDB.recordStep
          db
          (WorkflowId (workflowCtxId wctx))
          stepId'
          selectStepStepName
          (OutcomeOutput (Just encoded.serializedText))
          (serialization <|> Just (serializerName conn.connSerializer))
          timing
    pure (either (Left . TransactError.ErrorSystemDatabase) Right written)
  _ -> pure (Right ())

-- | Takes a control signal out of the winning branch's slot, if that is
-- what it holds. A control signal is not the race's decision: the branch
-- recorded nothing and the workflow stays pending, so recording that
-- branch as the winner would pin every recovery to a branch that never ran
-- its body. An application error is left where it is — the branch recorded
-- it under its own id, so recording it as the winner is faithful.
controlError :: Either (TransactError.Error TransactError.EngineOnly) a -> Maybe (TransactError.Error TransactError.EngineOnly)
controlError outcome = case outcome of
  Left err | isControl err -> Just err
  _ -> Nothing
  where
    isControl err = case err of
      TransactError.Interrupted {} -> True
      TransactError.ErrorSystemDatabase {} -> True
      _ -> False

-- | One arm of a race: the pending branch that claimed a step, and what to
-- run with its outcome if it wins. The result type is existential because
-- branches disagree about what they return.
data SelectArm exec m r = forall a. SelectArm
  { armName :: Text,
    armPending :: PendingStep exec m (Either (TransactError.Error TransactError.EngineOnly) a),
    armContinue :: Either (TransactError.Error TransactError.EngineOnly) a -> m (Either (TransactError.Error TransactError.EngineOnly) r)
  }

-- | The winner of a race: which branch, its outcome, and the continuation
-- the arm declared — packaged together so a list of heterogeneous arms can
-- be raced as one typed value.
data Winner exec m r = forall a. Winner
  { winnerIndex :: Int,
    winnerOutcome :: Either (TransactError.Error TransactError.EngineOnly) a,
    winnerContinue :: Either (TransactError.Error TransactError.EngineOnly) a -> m (Either (TransactError.Error TransactError.EngineOnly) r)
  }

-- | Races the arms and runs the winner's continuation, recording which
-- branch won first. A recorded winner from an earlier run replays: only
-- that branch is driven, and nothing is recorded again. A control signal
-- out of the winning branch ends the race with that error and records
-- nothing.
--
-- Branches must already be built — every arm's 'armPending' comes from
-- 'DBOS.Transact.Step.pendingWorkflowStep' or
-- 'DBOS.Transact.Handle.pendingAwait' — so every branch has claimed its id
-- before the race's own id follows them.
-- | 'selectStep' over the scoped workflow view: the race's id and the
-- recorded winner are claimed through the workflow context, and the arms
-- are pendings built through the same view.
selectStep ::
  (MonadAsync m, MonadTime m) =>
  WorkflowCtx exec m ->
  [SelectArm exec m r] ->
  m (Either (TransactError.Error TransactError.EngineOnly) r)
selectStep wctx arms = selectStepOn wctx arms

-- | The context-level race: the scoped entry hands its view straight
-- through; this runs the race over it.
selectStepOn ::
  forall exec m r.
  (MonadAsync m, MonadTime m) =>
  WorkflowCtx exec m ->
  [SelectArm exec m r] ->
  m (Either (TransactError.Error TransactError.EngineOnly) r)
selectStepOn _ [] =
  pure (Left (TransactError.ErrorConfig "selectStep races two or more durable steps; one branch is not a race"))
selectStepOn _ [_] =
  pure (Left (TransactError.ErrorConfig "selectStep races two or more durable steps; one branch is not a race"))
selectStepOn wctx arms = do
  let branches = foldl (\bs arm -> case arm of SelectArm _ pending _ -> pushBranch pending bs) newBranches arms
  checked <- checkSelect wctx branches
  case checked of
    Left err -> pure (Left err)
    Right (Replay winner replayedStep) -> case drop winner arms of
      (arm : _) -> case arm of
        SelectArm _ pending cont -> do
          outcome <- pending.pendingRun
          case controlError outcome of
            Just err -> pure (Left err)
            Nothing -> cont outcome
      [] ->
        pure
          ( Left
              ( TransactError.ErrorSystemDatabase
                  ( SystemDBError.UnexpectedStep
                      { workflowId = workflowCtxId wctx,
                        stepId = replayedStep,
                        expected = "a branch a recorded select can replay",
                        recorded = "a select won by branch " <> showText winner <> ", which no longer exists"
                      }
                  )
              )
          )
    Right (Fresh recording) -> do
      raced <- raceArms 0 arms
      case raced of
        Winner at outcome cont -> case controlError outcome of
          Just err -> pure (Left err)
          Nothing -> do
            recorded <- recordSelect recording at
            case recorded of
              Left err -> pure (Left err)
              Right () -> cont outcome
-- | Races every arm in source order: earlier branches win ties, and the
-- losers are cancelled at their next suspension point — which fires a
-- losing step's cancellation token, as dropping one does in the oracle.
raceArms :: (MonadAsync m) => Int -> [SelectArm exec m r] -> m (Winner exec m r)
raceArms startIndex arms = case arms of
  [arm] -> participant startIndex arm
  (arm : rest@(_ : _)) -> do
    raced <- race (participant startIndex arm) (raceArms (startIndex + 1) rest)
    pure (either id id raced)
  -- The public entry refuses fewer than two, so this is unreachable.
  [] -> error "selectStep raced no branches"

participant :: (Monad m) => Int -> SelectArm exec m r -> m (Winner exec m r)
participant at (SelectArm _ pending cont) = do
  outcome <- pending.pendingRun
  pure (Winner at outcome cont)
