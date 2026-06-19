{-# LANGUAGE DerivingStrategies #-}

module DbosTransact.Client
  ( Client(..)
  , newClient
  ) where

import DbosTransact.Config (DBOSConfig)

newtype Client = Client DBOSConfig
  deriving stock (Eq, Show)

newClient :: DBOSConfig -> IO Client
newClient = error "DbosTransact.Client.newClient: not implemented"
