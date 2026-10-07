{-# LANGUAGE OverloadedStrings #-}

-- | Message mock data for sim trees: the body mock receives answer with.
-- Owned here (not in the backend) so trees correlate through the same
-- value the backend serves. Duplicated across domains on purpose.
module DBOS.Transact.MessageSimData (mockMessageBody) where

import DBOS.Prelude

-- | The message body mock receives answer with.
mockMessageBody :: Text
mockMessageBody = "mock-message"
