{-# LANGUAGE OverloadedStrings #-}

-- | Everything a handler needs: the launched instance and the events tab's
-- current order id (the workflows tab's task id travels in the URL, the way
-- the original page kept it in @?id=@).
module Starter.App (StarterApp (..)) where

import DBOS.Prelude
import DBOS.Transact (DBOS, WorkflowId)
import Starter.Workflows (StarterRefs)

data StarterApp = StarterApp
  { staDbos :: DBOS IO,
    staOrderId :: StrictTVar IO (Maybe WorkflowId),
    staRefs :: StarterRefs
  }
