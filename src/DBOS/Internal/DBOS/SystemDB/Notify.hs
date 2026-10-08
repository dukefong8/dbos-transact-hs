{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Who is waiting for what, so a write can wake them. Mirrors Rust
-- @sysdb::notify@.
--
-- The wait loops themselves are what deliver — look, wait a bounded
-- interval, look again — so a wakeup is only ever a hint to look again,
-- never the value. This registry has one job: let something that already
-- knows a row was written cut a waiter's interval short. Nothing here polls
-- and nothing here touches the database.
--
-- Subscribe before looking: a waiter that looks first and subscribes second
-- misses anything written in between, which is why 'subscribe' is
-- synchronous and infallible — there is no await between deciding to wait
-- and being able to be woken.
--
-- Keys are flat and prefixed (@m::@, @e::@, @s::@ over one map). The prefix
-- is load-bearing: an event and a stream on the same workflow and key are
-- different things to wait for, and without it a @set_event@ would wake a
-- stream reader. A listener builds a key by concatenating the prefix onto
-- the payload exactly as it arrived and never splits it, because both halves
-- are caller-supplied strings that may themselves contain @::@.
--
-- Prefixing does not make keys injective, and that is fine: the pairs
-- @(\"a\", \"b::c\")@ and @(\"a::b\", \"c\")@ both render @m::a::b::c@, so a
-- wake for one wakes the other. A wakeup is only a hint, so the woken waiter
-- looks, finds nothing, and waits again — one wasted query, no wrong answer.
--
-- Rust's @Subscription@ deregisters on @Drop@; Haskell has no such hook, so
-- 'unsubscribe' is the explicit equivalent and every wait brackets on it.
module DBOS.SystemDB.Notify
  ( notificationsChannel,
    eventsChannel,
    streamsChannel,
    messageKey,
    eventKey,
    streamKey,
    keyFor,
    Registry (..),
    Waiter (..),
    Subscription (..),
    newRegistry,
    subscribe,
    subscribeExclusive,
    unsubscribe,
    wake,
    wakeAll,
    notified,
  )
where

import DBOS.Prelude
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import DBOS.SystemDB.Types (nullTopicSentinel)

-- | The channel a message notification arrives on, published by migration 1's
-- trigger. Haskell spelling of Rust @NOTIFICATIONS_CHANNEL@.
notificationsChannel :: Text
notificationsChannel = "dbos_notifications_channel"

-- | The channel an event notification arrives on. No trigger feeds it
-- (migration 44 removes the one there was); the notifier pushes instead.
eventsChannel :: Text
eventsChannel = "dbos_workflow_events_channel"

-- | The channel a stream notification arrives on, fed by the notifier too.
streamsChannel :: Text
streamsChannel = "dbos_streams_channel"

-- | The key a @recv@ waits on. The topic is resolved to the sentinel here
-- rather than by the caller, because it has to match what a notification
-- carries, and the stored row never holds @NULL@.
messageKey :: Text -> Maybe Text -> Text
messageKey destinationId topic = "m::" <> destinationId <> "::" <> fromMaybe nullTopicSentinel topic

-- | The key a @get_event@ waits on.
eventKey :: Text -> Text -> Text
eventKey workflowId key = "e::" <> workflowId <> "::" <> key

-- | The key a stream reader's loop waits on. The one key with no caller yet:
-- the subscription belongs to the engine's @read_stream@, which lands with
-- the streams port.
streamKey :: Text -> Text -> Text
streamKey workflowId key = "s::" <> workflowId <> "::" <> key

-- | The key a notification names, or 'Nothing' for a channel this does not
-- listen on. The payload is prepended whole and never split, which is what
-- makes this agree with the three functions above for every input.
keyFor :: Text -> Text -> Maybe Text
keyFor channel payload
  | channel == notificationsChannel = Just ("m::" <> payload)
  | channel == eventsChannel = Just ("e::" <> payload)
  | channel == streamsChannel = Just ("s::" <> payload)
  | otherwise = Nothing

-- | One key's state: how many wakeups it has carried, and how many
-- subscriptions are on it. Rust keeps a broadcast channel per key; the
-- generation counter is the same one-slot buffer — a wake that lands before
-- the wait is still seen, and two wakes before a wait collapse into one.
data Waiter = Waiter
  { waiterGeneration :: Int,
    waiterSubscribers :: Int
  }
  deriving stock (Eq, Show)

-- | Who is waiting for what. One flat map rather than one per kind of wait:
-- the keys are already disjoint by prefix, and a wake only ever needs to
-- match one string.
data Registry = Registry
  { waiters :: StrictTVar IO (Map Text Waiter)
  }

-- | A registration, held for as long as a caller is waiting. The @receiver@
-- is the generation this subscription last saw, so each of several waiters
-- on one key is woken independently.
data Subscription = Subscription
  { key :: Text,
    registry :: Registry,
    receiver :: StrictTVar IO Int
  }

newRegistry :: IO Registry
newRegistry = Registry <$> newTVarIO Map.empty

-- | Registers interest in @key@. Synchronous and infallible on purpose: a
-- caller subscribes, then looks at the database, and anything landing in
-- between wakes it. Several callers may wait on one key, and each is woken.
subscribe :: Registry -> Text -> IO Subscription
subscribe registry key = atomically $ do
  waiters' <- readTVar registry.waiters
  let waiter = Map.findWithDefault (Waiter 0 0) key waiters'
  writeTVar registry.waiters (Map.insert key waiter {waiterSubscribers = waiter.waiterSubscribers + 1} waiters')
  receiver <- newTVar waiter.waiterGeneration
  pure Subscription {key, registry, receiver}

-- | Registers /sole/ interest in @key@, or 'Nothing' if something is already
-- waiting on it. For @recv@, where two waiters are a bug rather than a
-- pattern: one message can only go to one of them, so the loser would wait
-- out its timeout and report that nothing arrived. This is a per-process
-- guard and cannot be more than that: two receivers in different processes
-- are arbitrated at the database by the @consumed = FALSE@ predicate.
subscribeExclusive :: Registry -> Text -> IO (Maybe Subscription)
subscribeExclusive registry key = atomically $ do
  waiters' <- readTVar registry.waiters
  if Map.member key waiters'
    then pure Nothing
    else do
      writeTVar registry.waiters (Map.insert key (Waiter 0 1) waiters')
      receiver <- newTVar 0
      pure (Just Subscription {key, registry, receiver})

-- | Deregisters, so the registry never carries state for a caller that has
-- gone. The explicit Haskell equivalent of Rust's @Drop@: the last
-- subscription on a key takes its registration with it.
unsubscribe :: Subscription -> IO ()
unsubscribe subscription = atomically $ do
  waiters' <- readTVar subscription.registry.waiters
  case Map.lookup subscription.key waiters' of
    Nothing -> pure ()
    Just waiter
      | waiter.waiterSubscribers <= 1 ->
          writeTVar subscription.registry.waiters (Map.delete subscription.key waiters')
      | otherwise ->
          writeTVar
            subscription.registry.waiters
            (Map.insert subscription.key waiter {waiterSubscribers = waiter.waiterSubscribers - 1} waiters')

-- | Wakes everything waiting on @key@. A wake for nobody is ordinary rather
-- than an error: a listener sees every process's notifications, and almost
-- none of them are this one's callers'.
wake :: Registry -> Text -> IO ()
wake registry key =
  atomically (modifyTVar registry.waiters (Map.adjust (\waiter -> waiter {waiterGeneration = waiter.waiterGeneration + 1}) key))

-- | Wakes everything registered, whatever it is waiting for. For the one
-- case where a wakeup source knows it has /missed/ wakeups but not which: a
-- listener whose connection dropped and came back saw nothing in between.
wakeAll :: Registry -> IO ()
wakeAll registry =
  atomically (modifyTVar registry.waiters (Map.map (\waiter -> waiter {waiterGeneration = waiter.waiterGeneration + 1})))

-- | Waits until something is written on this key. No timeout of its own,
-- deliberately: the caller owns the deadline and re-checks the database on
-- every return, because a wakeup is a hint that something changed and never
-- the change itself.
notified :: Subscription -> IO ()
notified subscription = atomically $ do
  waiters' <- readTVar subscription.registry.waiters
  case Map.lookup subscription.key waiters' of
    -- Unreachable while this subscription holds a receiver; a vanished key
    -- reads as a wakeup rather than a stall, like Rust's closed channel.
    Nothing -> pure ()
    Just waiter -> do
      seen <- readTVar subscription.receiver
      if waiter.waiterGeneration /= seen
        then writeTVar subscription.receiver waiter.waiterGeneration
        else retry
