{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the typed error channel (@error.rs@): the
-- round trip the generic parameter exists for, the blanket application
-- lift, the engine lift into any channel, and the control classification.
module DBOS.Transact.ErrorTest (tests) where

import DBOS.Internal.Prelude
import Data.Aeson (FromJSON (..), Result (..), ToJSON (..), fromJSON, object, withObject, (.:), (.=))
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
  (
    EngineOnly,
    Error (..),
    QualifiedWorkflowName,
    newQualifiedWorkflowName,
    methodQualifiedWorkflowName,
  )
import DBOS.Internal.Transact.Error (Failure (..), controlOf, decodeErrorText, encodeErrorText, failureOf, fromExecution, fromSystemDB, liftEngine)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | An application's own error, with the two derives the recorded column
-- needs and nothing else — the oracle's @CheckoutError@ shape.
data CardDeclined = CardDeclined {attempts :: Int}
  deriving stock (Eq, Show)

instance ToJSON CardDeclined where
  toJSON declined = object ["attempts" .= declined.attempts]

instance FromJSON CardDeclined where
  parseJSON = withObject "CardDeclined" (\fields -> CardDeclined <$> fields .: "attempts")

tests :: TestTree
tests =
  testGroup
    "Typed errors"
    [ testCase "an application error round trips whole" $ do
        let err = ErrorApplication (CardDeclined {attempts = 3})
        decodeErrorText (encodeErrorText err) @?= Right err,
      testCase "the blanket conversion lifts an application error" $ do
        ErrorApplication (CardDeclined {attempts = 1}) @?= (ErrorApplication (CardDeclined {attempts = 1}) :: Error CardDeclined),
      testCase "an engine error lifts into any channel" $ do
        let engine = NotLaunched {operation = "run a workflow"} :: Error EngineOnly
        liftEngine engine @?= (NotLaunched {operation = "run a workflow"} :: Error CardDeclined)
        displayException (liftEngine engine :: Error CardDeclined) @?= "cannot run a workflow before DBOS is launched",
      testCase "a parked await renders its budget" $ do
        let parked = ErrorMaxRecoveryAttemptsExceeded {workflowId = "wf", recoveryAttempts = 5} :: Error CardDeclined
        displayException parked @?= "the workflow wf exceeded 5 recovery attempts"
        decodeErrorText (encodeErrorText parked) @?= Right parked,
      testCase "only control errors report themselves as control" $ do
        let cancelled = SystemDatabase (SystemDB.WorkflowCancelled {workflowId = "wf-1"}) :: Error CardDeclined
            interrupted = Interrupted {workflowId = "wf-1"} :: Error CardDeclined
            failed = ErrorApplication (CardDeclined {attempts = 1})
        assertBool "a cancellation is control" (controlOf cancelled /= Nothing)
        assertBool "an interruption is control" (controlOf interrupted /= Nothing)
        assertBool "an unknown serialization is not control" (controlOf (UnknownSerialization {name = "pickle"}) == Nothing)
        failureOf (UnknownSerialization {name = "pickle"} :: Error CardDeclined)
          @?= FailureRecorded (encodeErrorText (UnknownSerialization {name = "pickle"} :: Error CardDeclined))
        controlOf failed @?= Nothing,
      testCase "a Rust-shaped portable error keeps its name and message" $ do
        -- ADR-0038: typed cross-SDK error replay is not a goal; a Rust
        -- record's untagged data is reported as its name and message.
        let rust =
              "{\"name\":\"dbos::StepTimeout\",\"message\":\"the step s exceeded its 1000ms timeout\",\"data\":{\"step\":\"s\",\"timeout\":{\"secs\":1,\"nanos\":0}}}"
        (decodeErrorText rust :: Either Text (Error CardDeclined))
          @?= Left "dbos::StepTimeout: the step s exceeded its 1000ms timeout",
      testCase "an unknown tag lifts to the same engine variant" $ do
        fromSystemDB (SystemDB.UnknownSerialization {name = "pickle"})
          @?= (UnknownSerialization {name = "pickle"} :: Error CardDeclined)
        fromSystemDB (SystemDB.Malformed "bad")
          @?= (SystemDatabase (SystemDB.Malformed "bad") :: Error CardDeclined),
      testCase "an execution sees cancellation at the top level" $ do
        fromExecution (SystemDB.WorkflowCancelled {workflowId = "wf-1"})
          @?= (WorkflowCancelled {workflowId = "wf-1"} :: Error CardDeclined)
        fromSystemDB (SystemDB.WorkflowCancelled {workflowId = "wf-1"})
          @?= (WorkflowCancelled {workflowId = "wf-1"} :: Error CardDeclined)
        fromExecution (SystemDB.WorkflowConflict {workflowId = "wf-1"})
          @?= (SystemDatabase (SystemDB.WorkflowConflict {workflowId = "wf-1"}) :: Error CardDeclined)
        displayException (WorkflowCancelled {workflowId = "wf-1"} :: Error CardDeclined) @?= "the workflow wf-1 was cancelled"
        assertBool "a top-level cancellation is control" (controlOf (WorkflowCancelled {workflowId = "wf-1"} :: Error CardDeclined) /= Nothing),
      testCase "a deleted adopted workflow is a recorded not-found" $ do
        let err = WorkflowNotFound {workflowId = "wf-gone"} :: Error CardDeclined
        displayException err @?= "no workflow exists with id wf-gone"
        controlOf err @?= Nothing,
      testCase "re-targeting recurses into the nested retry errors" $ do
        let err = MaxStepRetriesExceeded {step = "s", attempts = 2, errors = [ErrorApplication (CardDeclined {attempts = 1})]} :: Error CardDeclined
        fmap (const "boom") err
          @?= (MaxStepRetriesExceeded {step = "s", attempts = 2, errors = [ErrorApplication "boom"]} :: Error String),
      testCase "the renamed constructors record under their bare tags" $ do
        -- Engine-only variants that never reach a recorded row (the
        -- system-database failure is control) are refused by decode, so
        -- only the decodable renames round-trip here.
        let renamed :: [Error CardDeclined]
            renamed =
              [ NotLaunched {operation = "op"},
                AlreadyLaunched {operation = "op"},
                AlreadyRegistered {workflow = newQualifiedWorkflowName "k"},
                Deserialization {what = "in", message = "bad"},
                UnknownSerialization {name = "pickle"},
                NotRegistered {workflow = newQualifiedWorkflowName "k"},
                WorkflowClaimLost {workflowId = "wf"},
                WorkflowFailed {workflowId = "wf", message = "bad"},
                WorkflowNotFound {workflowId = "wf-gone"}
              ]
        mapM_ (\err -> decodeErrorText (encodeErrorText err) @?= Right err) renamed,
      testCase "a name with missing optional fields decodes like Rust" $ do
        -- Rust's derived Deserialize treats absent Option fields as None;
        -- aeson's .: does not, so the name instance uses .:? instead.
        (fromJSON (object ["name" .= ("x" :: Text)]) :: Result QualifiedWorkflowName)
          @?= Success (newQualifiedWorkflowName "x"),
      testCase "a registration error recorded as key still decodes" $ do
        -- Errors are checkpointed, so the field keeps its pre-rename spelling
        -- as an alias and one recorded before the rename still replays.
        decodeErrorText "{\"NotRegistered\":{\"key\":{\"name\":\"f\",\"class_name\":\"C\",\"config_name\":null}}}"
          @?= Right (NotRegistered {workflow = methodQualifiedWorkflowName "f" "C"} :: Error CardDeclined)
        decodeErrorText "{\"AlreadyRegistered\":{\"key\":{\"name\":\"f\",\"class_name\":null,\"config_name\":null}}}"
          @?= Right (AlreadyRegistered {workflow = newQualifiedWorkflowName "f"} :: Error CardDeclined),
      testCase "patch nondeterminism round-trips under its own tag" $ do
        let err = PatchNondeterminism {workflowId = "wf", patch = "v2", stepId = 0} :: Error CardDeclined
        decodeErrorText (encodeErrorText err) @?= Right err
    ]
