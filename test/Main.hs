module Main (main) where

import Test.Tasty (defaultMain, testGroup)
import qualified DbosTransact.Compat.IdempotencySpec as IdempotencySpec
import qualified DbosTransact.Compat.StepSpec as StepSpec
import qualified DbosTransact.Compat.WorkflowSpec as WorkflowSpec
import qualified DbosTransact.Compat.ChaosSpec as ChaosSpec
import qualified DbosTransact.Core.TransitionSpec as TransitionSpec
import qualified DbosTransact.Golden.SchemaSpec as SchemaSpec

main :: IO ()
main = defaultMain $ testGroup "dbos-transact-hs"
  [ WorkflowSpec.tests
  , StepSpec.tests
  , IdempotencySpec.tests
  , SchemaSpec.tests
  , TransitionSpec.tests
  , ChaosSpec.tests
  ]
