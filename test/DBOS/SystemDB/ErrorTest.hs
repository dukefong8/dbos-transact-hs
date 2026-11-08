{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.ErrorTest
  ( tests,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (BackendError (..), BackendErrorKind (..), Error (..), defaultRetryPolicy, renderBackendError, renderError, shouldRecord, shouldRetry)
import DBOS.SystemDB.Error (decodePortableError, encodePortableError)
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
      testCase "renders an unreadable serialization tag" $
        renderError (UnknownSerialization {name = "pickle"})
          @?= "serialization pickle is not available",
      testCase "an unknown tag is control, never recorded" $
        shouldRecord (UnknownSerialization {name = "pickle"}) @?= False,
      testCase "a portable error record round trips" $
        decodePortableError (encodePortableError (UnexpectedStep {workflowId = "wf", stepId = 2, expected = "b", recorded = "a"}))
          @?= Right (UnexpectedStep {workflowId = "wf", stepId = 2, expected = "b", recorded = "a"}),
      testCase "renders a reused workflow id" $
        renderError (ConflictingWorkflow {workflowId = "wf", detail = "different function"})
          @?= "workflow wf already exists: different function",
      testCase "renders a workflow id that is already in use" $ do
        renderError (WorkflowIdInUse {workflowId = "wf", status = "SUCCESS", existingName = Just "charge"})
          @?= "workflow id wf is already in use, by workflow charge (SUCCESS)"
        renderError (WorkflowIdInUse {workflowId = "wf", status = "ENQUEUED", existingName = Nothing})
          @?= "workflow id wf is already in use, by a workflow (ENQUEUED)",
      testCase "a workflow id in use is an answer, recorded and never retried" $ do
        shouldRecord (WorkflowIdInUse {workflowId = "wf", status = "SUCCESS", existingName = Nothing}) @?= True
        shouldRetry defaultRetryPolicy (WorkflowIdInUse {workflowId = "wf", status = "SUCCESS", existingName = Nothing}) @?= False
        decodePortableError (encodePortableError (WorkflowIdInUse {workflowId = "wf", status = "SUCCESS", existingName = Just "charge"}))
          @?= Right (WorkflowIdInUse {workflowId = "wf", status = "SUCCESS", existingName = Just "charge"}),
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
      testCase "renders a step the same execution recorded twice" $
        renderError (StepNondeterminism {workflowId = "wf", stepId = 2})
          @?= "step 2 of workflow wf was recorded twice by the same execution with different results; check that the workflow is deterministic",
      testCase "a nondeterministic step is control, never recorded" $
        shouldRecord (StepNondeterminism {workflowId = "wf", stepId = 2}) @?= False,
      testCase "a nondeterministic step round trips" $
        decodePortableError (encodePortableError (StepNondeterminism {workflowId = "wf", stepId = 2}))
          @?= Right (StepNondeterminism {workflowId = "wf", stepId = 2}),
      testCase "renders an execution that lost its workflow" $
        renderError (WorkflowConflict {workflowId = "wf"})
          @?= "workflow wf is no longer owned by this execution",
      testCase "a lost workflow is control, never recorded" $
        shouldRecord (WorkflowConflict {workflowId = "wf"}) @?= False,
      testCase "a lost workflow round trips" $
        decodePortableError (encodePortableError (WorkflowConflict {workflowId = "wf"}))
          @?= Right (WorkflowConflict {workflowId = "wf"}),
      testCase "renders a missing fork point with and without a name" $ do
        renderError (NoForkPoint {workflowIds = ["a", "b"], stepName = Just "s"})
          @?= "no step named s in workflows a, b"
        renderError (NoForkPoint {workflowIds = ["a", "b"], stepName = Nothing})
          @?= "no steps in workflows a, b",
      testCase "renders unknown workflow ids" $
        renderError (NonExistentWorkflow {workflowIds = ["a"]})
          @?= "no such workflow: a",
      testCase "renders a workflow that cannot be rewound" $
        renderError (WorkflowNotRewindable {workflowId = "wf", status = "ENQUEUED"})
          @?= "cannot rewind workflow wf (ENQUEUED): it is running or waiting to run, so cancel it first",
      testCase "renders a rewind that lost its status guard" $
        renderError (RewindInterrupted {workflowId = "wf", status = "SUCCESS"})
          @?= "workflow wf changed status from SUCCESS while being rewound; retry the rewind",
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
