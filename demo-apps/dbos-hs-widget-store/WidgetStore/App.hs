-- | Everything a handler needs: the launched instance, the application pool
-- and the checkout reference. Split out of 'WidgetStore.Route' so
-- 'WidgetStore.Handler' and the route dispatch can both see it without a
-- cycle (the Todo app keeps its pool in the argument the same way).
module WidgetStore.App (WidgetApp (..)) where

import DBOS.Prelude
import DBOS.Transact (AppDataSource, DBOS, EngineOnly, Executor, WorkflowRef)

data WidgetApp = WidgetApp
  { waDbos     :: DBOS IO,
    waExec     :: Executor IO,
    waApp      :: AppDataSource,
    waCheckout :: WorkflowRef IO EngineOnly
  }
