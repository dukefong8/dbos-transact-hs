-- | Everything a handler needs: the launched instance, the application pool
-- and the atomic-workflow reference. Split out so 'Outbox.Handler' and the
-- route dispatch can both see it without a cycle.
{-# LANGUAGE OverloadedStrings #-}
module Outbox.App (OutboxApp (..), outboxAppName, outboxVersion) where

import DBOS.Transact (AppDataSource, DBOS, EngineOnly, Executor, WorkflowRef)
import Data.Text (Text)
import Prelude

data OutboxApp = OutboxApp
  { obDbos       :: DBOS IO,
    obExec       :: Executor IO,
    obApp        :: AppDataSource,
    obPlaceOrder :: WorkflowRef IO EngineOnly
  }

-- | The version this build runs as. Recovery only resumes workflows stamped
-- with the running executor's own version, and the system database's version
-- registry admits one application name per version.
outboxVersion :: Text
outboxVersion = "hs-outbox-0.1.0"

-- | The application name new rows are stamped with. The transactional-enqueue
-- variant writes its notification row by hand, so it must stamp what the
-- engine stamps automatically — an unstamped row is invisible to every
-- version- and application-scoped sweep.
outboxAppName :: Text
outboxAppName = "dbos-hs-outbox"
