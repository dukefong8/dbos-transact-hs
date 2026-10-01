{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Where a durable call stands, and what that means for its replay.
-- Mirrors Rust @checkpoint.rs@: several calls are steps the caller never
-- wrote, each taking a step id from the execution and recording its answer
-- under it. The placement decision is shared here; each caller owns its own
-- write.
module DBOS.Transact.Checkpoint
  ( -- * Placement
    StepPlacement (..),
    StepDurability (..),
    PendingStep (..),
    placementAt,
    placementHere,
    placeCall,
    takenPlacement,
    placementStepId,
    pendingStepId,
    checkHere,
    describePlacement,
    placementWhereabouts,
    insideAWorkflow,
  )
where

import DBOS.Prelude
import Data.Text (Text)
import DBOS.Transact.Context (Ctx, StepMarker, currentConnection, inStep, nextStepId, stepId, stepMarker, workflowId)
import DBOS.Transact.Connection (Connection (..), Owner (..))
import DBOS.Transact.Error (Error (..))

-- | Where a durable call stands: which of the execution's step ids it
-- occupies, if any. One type for both user steps and library steps.
data StepPlacement m
  = -- | Not inside a workflow. Nothing is recorded.
    Outside
  | -- | Inside a step body, a plain call by the leaf rule. Rust spells this
    -- @InsideStep@; the @Placement@ prefix is the collision deviation,
    -- because 'DBOS.Transact.Error' already owns @InsideStep@.
    PlacementInsideStep (Ctx m)
  | -- | Reached through a client's connection: no counter to agree with,
    -- so the undurable version of the call.
    ClientConnection
  | -- | Inside a workflow at a step boundary: this call is a step.
    Recorded (Ctx m) Int
  deriving stock (Eq, Show)

-- | What 'checkHere' answers for a call it accepts. Constructors carry the
-- @Durability@ prefix: Rust spells both this and 'StepPlacement' @Recorded@,
-- and one module cannot hold the name twice.
data StepDurability m
  = -- | Durable: record it under this workflow and this id.
    DurabilityRecorded (Ctx m) Int
  | -- | Undurable, and rightly so: it claimed no id.
    DurabilityPlain
  deriving stock (Eq, Show)

-- | A durable call that has taken its step id and has not run. The identity
-- is what the placement machinery can read — the name a refusal reports and
-- the id a replay checks — and 'pendingRun' is the deferred call itself,
-- driven when the pending value is awaited or raced. 'Nothing' is a build
-- that failed before it reached the counter and claims no position anywhere.
data PendingStep m a = PendingStep
  { name :: Text,
    placement :: Maybe (StepPlacement m),
    pendingRun :: m a
  }

-- | 'here' for a caller already holding the context. Inside a step body the
-- call is plain; at a step boundary it records under the allocated id.
placementAt :: Ctx m -> Int -> StepPlacement m
placementAt ctx stepId' =
  case stepId ctx of
    Just _ -> PlacementInsideStep ctx
    Nothing -> Recorded ctx stepId'

-- | Where a call served by the given connection stands, with the ambient
-- context reconciled against it. Mirrors @StepPlacement::taken@: inside a
-- step the call is plain whoever serves it, so that check comes before the
-- connection comparison and there is nothing for the halves to disagree
-- about; a client connection degrades to the undurable call; another
-- application's connection is refused, because the record would land where
-- the workflow that allocated the id cannot see it. The comparison is of
-- instance identities, which is what the two halves would disagree about.
-- Callers check their own launch first, so a call to an unlaunched
-- instance moves no counter.
takenPlacement :: MonadSTM m => Connection m -> Text -> Ctx m -> m (Either (Error e) (StepPlacement m))
takenPlacement conn operation ctx
  | inStep ctx = pure (Right (PlacementInsideStep ctx))
  | conn.connInstanceId == (currentConnection ctx).connInstanceId = do
      stepId' <- nextStepId ctx
      pure (Right (Recorded ctx stepId'))
  | otherwise = pure $ case conn.connOwner of
      OwnerClient -> Right ClientConnection
      OwnerApplication -> Left (WrongInstance {operation = operation})

-- | Where a call being *built* stands: the id is claimed here, before
-- anything the call does can fail, because the position of the call in the
-- workflow has to be the same on the run as it was on the run. Mirrors
-- the allocation half of Rust @StepPlacement::here@; 'placementHere' is the
-- reader for a placement that already knows its id.
placeCall :: MonadSTM m => Ctx m -> m (StepPlacement m)
placeCall ctx
  | inStep ctx = pure (PlacementInsideStep ctx)
  | otherwise = do
      stepId' <- nextStepId ctx
      pure (Recorded ctx stepId')

-- | Where a call stands given the ambient context, with the id its caller
-- already allocated. Outside a workflow there is no counter to draw from.
placementHere :: Maybe (Ctx m) -> Int -> StepPlacement m
placementHere ambient stepId' =
  case ambient of
    Nothing -> Outside
    Just ctx -> placementAt ctx stepId'

-- | The id this call claimed, or 'Nothing' where it claimed none.
placementStepId :: StepPlacement m -> Maybe Int
placementStepId placement =
  case placement of
    Recorded _ stepId' -> Just stepId'
    Outside -> Nothing
    PlacementInsideStep _ -> Nothing
    ClientConnection -> Nothing

-- | The id this pending call claimed when built, or 'Nothing' if none.
pendingStepId :: PendingStep m a -> Maybe Int
pendingStepId pending =
  case pending.placement of
    Just placement -> placementStepId placement
    Nothing -> Nothing

-- | Whether a call built here may be polled where the ambient context is in
-- scope, and what polling it there means. An id is a claim on one position
-- in one workflow, so a call carried somewhere that cannot honour it is
-- refused rather than run.
checkHere :: StepPlacement m -> Text -> Maybe (Ctx m) -> Either (Error e) (StepDurability m)
checkHere placement step ambient =
  case (placement, ambient) of
    (Recorded ctx stepId', Just here)
      | workflowId here == workflowId ctx && stepId here == Nothing ->
          Right (DurabilityRecorded ctx stepId')
    (PlacementInsideStep ctx, Just here)
      | sameStepBody ctx here ->
          Right DurabilityPlain
    (Outside, Nothing) -> Right DurabilityPlain
    (ClientConnection, _) -> Right DurabilityPlain
    _ ->
      Left
        ( StepBuiltElsewhere
            { step = step,
              built = placementWhereabouts placement,
              polled = describePolled placement ambient
            }
        )
  where
    -- Compared by marker alone: a marker is unique within its workflow, so
    -- equal markers are the same body. The match keeps two absent markers
    -- from matching as the workflow proper twice.
    sameStepBody context here =
      case (stepMarker context, stepMarker here) of
        (Just outer, Just inner) ->
          outer == inner && workflowId here == workflowId context
        _ -> False

-- | How to describe the place a call is standing, given the context there.
describePlacement :: Maybe (Ctx m) -> Text
describePlacement ambient =
  case ambient of
    Nothing -> "outside a workflow"
    Just ctx -> case stepId ctx of
      Just _ -> "inside a step of workflow " <> workflowId ctx
      Nothing -> "in workflow " <> workflowId ctx

-- | How to describe where a placement was built.
placementWhereabouts :: StepPlacement m -> Text
placementWhereabouts placement =
  case placement of
    Recorded ctx _ -> describePlacement (Just ctx)
    PlacementInsideStep ctx -> describePlacement (Just ctx)
    ClientConnection -> "on a client's connection"
    Outside -> describePlacement Nothing

-- | How to describe where a refused call was polled. Two different step
-- bodies of one workflow describe identically, so the marker is what told
-- them apart and the message says so.
describePolled :: StepPlacement m -> Maybe (Ctx m) -> Text
describePolled placement ambient =
  case (placement, ambient) of
    (PlacementInsideStep _, Just here) -> case stepMarker here of
      Just _ -> "inside a different step of workflow " <> workflowId here
      Nothing -> describePlacement ambient
    _ -> describePlacement ambient

-- | Whether there is a surrounding workflow: true wherever a cancelled
-- awaited workflow must be distinguished from this caller being cancelled.
insideAWorkflow :: StepPlacement m -> Bool
insideAWorkflow placement =
  case placement of
    Outside -> False
    _ -> True
