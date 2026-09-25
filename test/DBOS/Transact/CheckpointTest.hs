{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @checkpoint.rs@ placement seam.
module DBOS.Transact.CheckpointTest (tests) where

import DBOS.Prelude
import DBOS.Transact
  ( Error (..),
    PendingStep (..),
    StepDurability (..),
    StepPlacement (..),
    checkHere,
    describePlacement,
    firstStepStatus,
    insideAWorkflow,
    nextStepMarker,
    pendingStepId,
    placementAt,
    placementHere,
    placementStepId,
    placementWhereabouts,
    withAttempt,
  )
import DBOS.Transact.ContextTest (testCtx)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Checkpoint placement"
    [ testCase "outside a workflow takes no id and records nothing" $ do
        let placement = placementHere Nothing 0
        placement @?= Outside
        placementStepId placement @?= Nothing
        pendingStepId (PendingStep "DBOS.sleep" (Just placement)) @?= Nothing,
      testCase "at a step boundary the call records under the allocated id" $ do
        ctx <- testCtx
        let placement = placementAt ctx 0
        placement @?= Recorded ctx 0
        placementStepId placement @?= Just 0
        pendingStepId (PendingStep "checkout" (Just placement)) @?= Just 0,
      testCase "inside a step body the call is plain by the leaf rule" $ do
        ctx <- testCtx
        marker <- nextStepMarker ctx
        withAttempt ctx marker (firstStepStatus 3) $ \stepped -> do
          let placement = placementAt stepped 1
          placement @?= PlacementInsideStep stepped
          placementStepId placement @?= Nothing,
      testCase "a recorded call polled at its boundary stays durable" $ do
        ctx <- testCtx
        checkHere (Recorded ctx 0) "checkout" (Just ctx)
          @?= Right (DurabilityRecorded ctx 0),
      testCase "a recorded call carried into a step is refused" $ do
        ctx <- testCtx
        marker <- nextStepMarker ctx
        withAttempt ctx marker (firstStepStatus 0) $ \stepped ->
          checkHere (Recorded ctx 0) "checkout" (Just stepped)
            @?= Left
              ( StepBuiltElsewhere
                  { step = "checkout",
                    built = "in workflow wf-1",
                    polled = "inside a step of workflow wf-1"
                  }
              ),
      testCase "a client's call stays plain wherever it is driven" $ do
        ctx <- testCtx
        checkHere ClientConnection "DBOS.cancel" Nothing @?= Right DurabilityPlain
        checkHere ClientConnection "DBOS.cancel" (Just ctx) @?= Right DurabilityPlain,
      testCase "an in-step call polled in its own body stays plain" $ do
        ctx <- testCtx
        marker <- nextStepMarker ctx
        withAttempt ctx marker (firstStepStatus 3) $ \stepped ->
          checkHere (PlacementInsideStep stepped) "checkout" (Just stepped)
            @?= Right DurabilityPlain,
      testCase "an in-step call carried to a sibling body is refused" $ do
        ctx <- testCtx
        firstMarker <- nextStepMarker ctx
        secondMarker <- nextStepMarker ctx
        withAttempt ctx firstMarker (firstStepStatus 3) $ \first ->
          withAttempt ctx secondMarker (firstStepStatus 3) $ \second ->
            checkHere (PlacementInsideStep first) "checkout" (Just second)
              @?= Left
                ( StepBuiltElsewhere
                    { step = "checkout",
                      built = "inside a step of workflow wf-1",
                      polled = "inside a different step of workflow wf-1"
                    }
                ),
      testCase "only outside has no workflow around it" $ do
        ctx <- testCtx
        insideAWorkflow Outside @?= False
        insideAWorkflow ClientConnection @?= True
        insideAWorkflow (Recorded ctx 0) @?= True
        describePlacement Nothing @?= "outside a workflow"
        describePlacement (Just ctx) @?= "in workflow wf-1"
        placementWhereabouts ClientConnection @?= "on a client's connection"
    ]
