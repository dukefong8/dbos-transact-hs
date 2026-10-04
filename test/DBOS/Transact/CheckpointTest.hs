{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @checkpoint.rs@ placement seam.
module DBOS.Transact.CheckpointTest (tests) where

import DBOS.Prelude
import DBOS.Transact
  ( EngineOnly, Error (..),
    Identity (..),
    LogEvent (..),
    PendingStep (..),
    Serializer (..),
    SomeTracer (..),
    StepDurability (..),
    StepPlacement (..),
    StepStatus,
    Timestamp (..),
    acquireLoggerBackend,
    firstStepStatus,
    ioTracer,
    nullTracer,
    secondsDuration,
  )
import DBOS.Transact.Context
  ( stepCtxBoundary,
    withStep,
    nextWorkflowMarker,
    stepStatusCurrentAttempt,
    stepStatusId,
    stepStatusMaxAttempts
  )
import DBOS.Transact.Checkpoint
  ( checkHere,
    describePlacement,
    insideAWorkflow,
    pendingStepId,
    placeCall,
    placementAt,
    placementHere,
    placementStepId,
    placementWhereabouts,
    takenPlacement
  )
import DBOS.Transact.Connection
  ( Connection (..),
    Owner (..),
    SomeSystemDB (..),
    newConnection,
    uuidWorkflowId
  )
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (testCase, (@?=))

-- | One backend for the whole group: contexts build real connections
-- over it, though placement checks never reach the database.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Checkpoint placement"
      [ testCase "outside a workflow takes no id and records nothing" $ do
        let placement = placementHere Nothing 0
        placement @?= Outside
        placementStepId placement @?= Nothing
        pendingStepId (PendingStep "DBOS.sleep" (Just placement) (pure () :: IO ())) @?= Nothing,
      testCase "at a step boundary the call records under the allocated id" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        let placement = placementAt (stepCtxBoundary ctx) 0
        placement @?= Recorded ctx 0
        placementStepId placement @?= Just 0
        pendingStepId (PendingStep "checkout" (Just placement) (pure () :: IO ())) @?= Just 0,
      testCase "a call built through a captured parent while a step body runs is plain" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        marker <- nextWorkflowMarker ctx
        withStep ctx marker (firstStepStatus 0) $ \_sctx -> do
          placement <- placeCall ctx
          placement @?= PlacementInsideStep (stepCtxBoundary ctx),
      testCase "a taken placement through a captured parent under another connection is plain" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        otherId <- uuidWorkflowId
        otherConn <-
          newConnection
            (SomeSystemDB backend)
            RustSerde
            (Just "test-app")
            (secondsDuration 1)
            OwnerApplication
            otherId
            uuidWorkflowId
            uuidEntropy
            nullTracer
        marker <- nextWorkflowMarker ctx
        withStep ctx marker (firstStepStatus 0) $ \_sctx -> do
          placed <- takenPlacement otherConn "get_event" ctx :: IO (Either (Error EngineOnly) (StepPlacement () IO))
          placed @?= Right (PlacementInsideStep (stepCtxBoundary ctx)),
      testCase "inside a step body the call is plain by the leaf rule" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        marker <- nextWorkflowMarker ctx
        withStep ctx marker (firstStepStatus 3) $ \sctx -> do
          let placement = placementAt sctx 1
          placement @?= PlacementInsideStep sctx
          placementStepId placement @?= Nothing,
      testCase "a recorded call polled at its boundary stays durable" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        (checkHere (Recorded ctx 0) "checkout" (Just (stepCtxBoundary ctx)) :: Either (Error EngineOnly) (StepDurability () IO))
          @?= Right (DurabilityRecorded ctx 0),
      testCase "a recorded call carried into a step is refused" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        marker <- nextWorkflowMarker ctx
        withStep ctx marker (firstStepStatus 0) $ \sctx ->
          (checkHere (Recorded ctx 0) "checkout" (Just sctx) :: Either (Error EngineOnly) (StepDurability () IO))
            @?= Left
              ( StepBuiltElsewhere
                  { step = "checkout",
                    built = "in workflow wf-1",
                    polled = "inside a step of workflow wf-1"
                  }
              ),
      testCase "a client's call stays plain wherever it is driven" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        (checkHere ClientConnection "DBOS.cancel" Nothing :: Either (Error EngineOnly) (StepDurability () IO)) @?= Right DurabilityPlain
        (checkHere ClientConnection "DBOS.cancel" (Just (stepCtxBoundary ctx)) :: Either (Error EngineOnly) (StepDurability () IO)) @?= Right DurabilityPlain,
      testCase "an in-step call polled in its own body stays plain" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        marker <- nextWorkflowMarker ctx
        withStep ctx marker (firstStepStatus 3) $ \sctx ->
          (checkHere (PlacementInsideStep sctx) "checkout" (Just sctx) :: Either (Error EngineOnly) (StepDurability () IO))
            @?= Right DurabilityPlain,
      testCase "an in-step call carried to a sibling body is refused" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        firstMarker <- nextWorkflowMarker ctx
        secondMarker <- nextWorkflowMarker ctx
        withStep ctx firstMarker (firstStepStatus 3) $ \first ->
          withStep ctx secondMarker (firstStepStatus 3) $ \second ->
            (checkHere (PlacementInsideStep first) "checkout" (Just second) :: Either (Error EngineOnly) (StepDurability () IO))
              @?= Left
                ( StepBuiltElsewhere
                    { step = "checkout",
                      built = "inside a step of workflow wf-1",
                      polled = "inside a different step of workflow wf-1"
                    }
                ),
      testCase "only outside has no workflow around it" $ do
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        insideAWorkflow Outside @?= False
        insideAWorkflow ClientConnection @?= True
        insideAWorkflow (Recorded ctx 0) @?= True
        describePlacement Nothing @?= "outside a workflow"
        describePlacement (Just (stepCtxBoundary ctx)) @?= "in workflow wf-1"
        placementWhereabouts ClientConnection @?= "on a client's connection"
    ]
