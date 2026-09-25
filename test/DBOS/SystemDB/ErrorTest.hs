{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.ErrorTest
  ( tests,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (BackendError (..), BackendErrorKind (..), Error (..), renderBackendError, renderError)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "SystemDB Error"
    [ testCase "renders a backend failure with its SQLSTATE" $
        renderError (Backend (BackendError "connection reset" (Just "08006") Connection))
          @?= "system database error: connection reset (08006)",
      testCase "renders a backend failure without one" $
        renderError (Backend (BackendError "boom" Nothing Permanent))
          @?= "system database error: boom",
      testCase "renders an unreadable stored value" $
        renderError (Malformed "status XYZ")
          @?= "unexpected value in the system database: status XYZ",
      testCase "renders a reused workflow id" $
        renderError (ConflictingWorkflow {workflowId = "wf", detail = "different function"})
          @?= "workflow wf already exists: different function",
      testCase "renders a second waiter with and without a topic" $ do
        renderError (ConcurrentRecv {workflowId = "wf", topic = Just "t"})
          @?= "workflow wf is already receiving on topic t"
        renderError (ConcurrentRecv {workflowId = "wf", topic = Nothing})
          @?= "workflow wf is already receiving",
      testCase "renders a rejected input" $
        renderError (InvalidInput {field = "name", detail = "must not be empty"})
          @?= "invalid name: must not be empty",
      testCase "renders a deduplicated enqueue" $
        renderError (QueueDeduplicated {workflowId = "wf", queueName = "q", deduplicationId = "k"})
          @?= "workflow wf (queue: q, deduplication id: k) is already enqueued",
      testCase "renders a cancelled workflow" $
        renderError (WorkflowCancelled {workflowId = "wf"})
          @?= "workflow wf is cancelled",
      testCase "renders a renamed step" $
        renderError (UnexpectedStep {workflowId = "wf", stepId = 2, expected = "b", recorded = "a"})
          @?= "workflow wf step 2 was recorded as \"a\", but \"b\" was expected",
      testCase "renders a step another execution recorded" $
        renderError (StepAlreadyRecorded {workflowId = "wf", stepId = 2})
          @?= "workflow wf step 2 was already recorded by another execution",
      testCase "renders a missing fork point with and without a name" $ do
        renderError (NoForkPoint {workflowIds = ["a", "b"], stepName = Just "s"})
          @?= "no step named s in workflows a, b"
        renderError (NoForkPoint {workflowIds = ["a", "b"], stepName = Nothing})
          @?= "no steps in workflows a, b",
      testCase "renders unknown workflow ids" $
        renderError (NonExistentWorkflow {workflowIds = ["a"]})
          @?= "no such workflow: a",
      testCase "renders an exhausted recovery budget" $
        renderError (ErrorMaxRecoveryAttemptsExceeded {workflowId = "wf", limit = 5})
          @?= "workflow wf exceeded 5 recovery attempts",
      testCase "renders a taken name" $
        renderError (AlreadyRegistered {kind = "Schedule", name = "s"})
          @?= "Schedule \"s\" is already registered",
      testCase "renders a missing name" $
        renderError (NotRegistered {kind = "Schedule", name = "s"})
          @?= "Schedule \"s\" is not registered",
      testCase "renders a name held by another application" $
        renderError (RegisteredByAnother {kind = "Queue", name = "q", holder = "app-a", claimant = Nothing})
          @?= "Queue \"q\" is already registered by application \"app-a\" in this system database, and queue names must be unique across the applications sharing one",
      testCase "renders the remedy when the claimant is named" $
        renderError (RegisteredByAnother {kind = "Queue", name = "q", holder = "app-a", claimant = Just "app-b"})
          @?= "Queue \"q\" is already registered by application \"app-a\" in this system database, and queue names must be unique across the applications sharing one: either give \"app-b\" a different queue name, or, if \"app-a\" was renamed to \"app-b\", move its rows first",
      testCase "renders a backend error directly" $ do
        renderBackendError (BackendError "boom" (Just "CODE") Transient)
          @?= "boom (CODE)"
        renderBackendError (BackendError "boom" Nothing Transient)
          @?= "boom"
    ]
