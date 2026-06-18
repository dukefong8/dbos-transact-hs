module Main (main) where

import DbosTransact (projectName)
import Hedgehog (Property, forAll, property, (===))
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Data.Text (pack)
import Prelude
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.Hedgehog (testProperty)
import Test.Tasty.HUnit ((@?=), testCase)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
  testGroup
    "dbos-transact-hs"
    [ testCase "project name is set" $
        projectName @?= pack "dbos-transact-hs",
      testProperty "reverse . reverse = id" reverseTwice
    ]

reverseTwice :: Property
reverseTwice = property $ do
  xs <- forAll $ Gen.list (Range.linear 0 100) (Gen.int (Range.linear (-1000) 1000))
  reverse (reverse xs) === xs
