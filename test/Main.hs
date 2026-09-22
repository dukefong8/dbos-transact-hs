module Main (main) where

import DBOS.CodecTest qualified as Codec
import DBOS.LogTest qualified as Log
import DBOS.SchemaTest qualified as Schema
import DBOS.StarterTest qualified as Starter
import DBOS.SystemDBHasqlTest qualified as SystemDBHasql
import DBOS.SystemDBTest qualified as SystemDB
import DBOS.TransactTest qualified as Transact
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main =
  defaultMain $
    testGroup
      "dbos-transact-hs"
      [ Transact.tests,
        Codec.tests,
        Log.tests,
        Starter.tests,
        SystemDB.tests,
        SystemDBHasql.tests,
        Schema.tests
      ]
