module Main (main) where

import qualified DBOS.SchemaTest as Schema
import qualified DBOS.SystemDBHasqlTest as SystemDBHasql
import qualified DBOS.SystemDBTest as SystemDB
import qualified DBOS.TransactTest as Transact
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main =
  defaultMain $
    testGroup
      "dbos-transact-hs"
      [ Transact.tests,
        SystemDB.tests,
        SystemDBHasql.tests,
        Schema.tests
      ]
