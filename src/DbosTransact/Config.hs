{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

module DbosTransact.Config
  ( DBOSConfig(..)
  , defaultDBOSConfig
  ) where

import Data.Text (Text)

-- | Runtime configuration shared by the safe and compatibility APIs.
data DBOSConfig = DBOSConfig
  { dbosApplicationName :: Text
  , dbosApplicationVersion :: Maybe Text
  , dbosExecutorId :: Maybe Text
  , dbosDatabaseUrl :: Maybe Text
  , dbosDatabaseSchema :: Text
  }
  deriving stock (Eq, Show)

defaultDBOSConfig :: DBOSConfig
defaultDBOSConfig = DBOSConfig
  { dbosApplicationName = "dbos-transact-hs"
  , dbosApplicationVersion = Nothing
  , dbosExecutorId = Nothing
  , dbosDatabaseUrl = Nothing
  , dbosDatabaseSchema = "dbos"
  }
