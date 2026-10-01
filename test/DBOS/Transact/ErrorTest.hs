{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the typed error channel (@error.rs@): the
-- round trip the generic parameter exists for, the blanket application
-- lift, the engine lift into any channel, and the control classification.
module DBOS.Transact.ErrorTest (tests) where

import DBOS.Prelude
import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
  (
    EngineOnly,
    Error (..),
    application,
    controlOf,
    decodeErrorText,
    encodeErrorText,
    liftEngine,
    mapApplication,
    renderTransactError,
  )
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
        let err = application (CardDeclined {attempts = 3})
        decodeErrorText (encodeErrorText err) @?= Right err,
      testCase "the blanket conversion lifts an application error" $ do
        application (CardDeclined {attempts = 1}) @?= (Application (CardDeclined {attempts = 1}) :: Error CardDeclined),
      testCase "an engine error lifts into any channel" $ do
        let engine = ErrorNotLaunched {operation = "run a workflow"} :: Error EngineOnly
        liftEngine engine @?= (ErrorNotLaunched {operation = "run a workflow"} :: Error CardDeclined)
        renderTransactError (liftEngine engine :: Error CardDeclined) @?= "cannot run a workflow before DBOS is launched",
      testCase "only control errors report themselves as control" $ do
        let cancelled = ErrorSystemDatabase (SystemDB.WorkflowCancelled {workflowId = "wf-1"}) :: Error CardDeclined
            interrupted = Interrupted {workflowId = "wf-1"} :: Error CardDeclined
            failed = application (CardDeclined {attempts = 1})
        assertBool "a cancellation is control" (controlOf cancelled /= Nothing)
        assertBool "an interruption is control" (controlOf interrupted /= Nothing)
        controlOf failed @?= Nothing,
      testCase "re-targeting recurses into the nested retry errors" $ do
        let err = MaxStepRetriesExceeded {step = "s", attempts = 2, errors = [Application (CardDeclined {attempts = 1})]} :: Error CardDeclined
        mapApplication (const "boom") err
          @?= MaxStepRetriesExceeded {step = "s", attempts = 2, errors = [Application "boom"]}
    ]
