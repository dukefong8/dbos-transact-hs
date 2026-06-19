{-# LANGUAGE DerivingStrategies #-}

module DbosTransact.Queue
  ( QueueName
  , queueName
  , QueueOptions(..)
  , defaultQueueOptions
  ) where

import Data.Text (Text)

newtype QueueName = QueueName Text
  deriving stock (Eq, Ord, Show)

queueName :: Text -> QueueName
queueName = QueueName

data QueueOptions = QueueOptions
  { queueConcurrency :: Maybe Int
  , queueRateLimit :: Maybe Int
  }
  deriving stock (Eq, Show)

defaultQueueOptions :: QueueOptions
defaultQueueOptions = QueueOptions
  { queueConcurrency = Nothing
  , queueRateLimit = Nothing
  }
