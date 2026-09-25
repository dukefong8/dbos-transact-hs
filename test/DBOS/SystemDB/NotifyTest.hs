{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.NotifyTest (tests) where

import DBOS.Prelude
import Control.Monad (void)
import DBOS.SystemDB
  ( Registry,
    Registry (..),
    Subscription,
    eventsChannel,
    eventKey,
    keyFor,
    messageKey,
    nullTopicSentinel,
    newRegistry,
    notificationsChannel,
    notified,
    streamKey,
    streamsChannel,
    subscribe,
    subscribeExclusive,
    unsubscribe,
    wake,
    wakeAll,
  )

import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | The @sysdb::notify@ registry: keys a waiter and a listener build
-- separately and have to agree on, and the wakeups that shorten a wait.
-- Every case mirrors its Rust test name.
tests :: TestTree
tests =
  testGroup
    "Notify"
    [ testGroup
        "Keys"
        [ testCase "a key is prefixed by what is being waited for" $ do
            eventKey "wf-1" "k" @?= "e::wf-1::k"
            streamKey "wf-1" "k" @?= "s::wf-1::k"
            assertBool "an event and a stream are different waits" (eventKey "wf-1" "k" /= streamKey "wf-1" "k"),
          testCase "a message key resolves an absent topic to the sentinel" $ do
            messageKey "wf-1" (Just "orders") @?= "m::wf-1::orders"
            messageKey "wf-1" Nothing @?= "m::wf-1::" <> nullTopicSentinel,
          testCase "a listener reaches a waiter by concatenation never by parsing" $ do
            let wire destination key = destination <> "::" <> key
                pairs =
                  [ ("wf-1", "progress"),
                    ("wf::with::colons", "k"),
                    ("wf-1", "key::with::colons"),
                    ("::leading", "trailing::")
                  ]
            mapM_
              ( \(destination, key) -> do
                  let payload = wire destination key
                  keyFor eventsChannel payload @?= Just (eventKey destination key)
                  keyFor streamsChannel payload @?= Just (streamKey destination key)
                  keyFor notificationsChannel payload @?= Just (messageKey destination (Just key))
              )
              pairs
            let sentinelPayload = wire "wf-1" nullTopicSentinel
            messageKey "wf-1" Nothing @?= "m::" <> sentinelPayload
            keyFor notificationsChannel sentinelPayload @?= Just (messageKey "wf-1" Nothing),
          testCase "an unknown channel names no key" $ do
            keyFor "some_other_channel" "wf-1::k" @?= Nothing
            keyFor "" "wf-1::k" @?= Nothing
        ],
      testGroup
        "Registry"
        [ testCase "colliding keys only cost a spurious wakeup" $ do
            registry <- newRegistry
            subscription <- subscribe registry (messageKey "a" (Just "b::c"))
            messageKey "a" (Just "b::c") @?= messageKey "a::b" (Just "c")
            wake registry (messageKey "a::b" (Just "c"))
            waitFor (notified subscription),
          testCase "a second exclusive waiter is refused until the first is gone" $ do
            registry <- newRegistry
            let key = messageKey "wf-1" (Just "orders")
            first <- subscribeExclusive registry key
            assertBool "nothing was waiting" (maybe False (const True) first)
            second <- subscribeExclusive registry key
            refusedIsNothing second
            case first of
              Nothing -> fail "expected the first exclusive waiter"
              Just subscription -> unsubscribe subscription
            registered registry >>= (@?= 0)
            again <- subscribeExclusive registry key
            assertBool "the topic is free again once its receiver has gone" (maybe False (const True) again),
          testCase "exclusivity covers one key and no more" $ do
            registry <- newRegistry
            _orders <- exclusive registry (messageKey "wf-1" (Just "orders"))
            _refunds <- exclusive registry (messageKey "wf-1" (Just "refunds"))
            _elsewhere <- exclusive registry (messageKey "wf-2" (Just "orders"))
            _event <- subscribe registry (eventKey "wf-1" "orders")
            registered registry >>= (@?= 4),
          testCase "an exclusive waiter and a shared one do not share a key" $ do
            registry <- newRegistry
            let key = messageKey "wf-1" Nothing
            shared <- subscribe registry key
            refused <- subscribeExclusive registry key
            refusedIsNothing refused
            unsubscribe shared
            exclusive' <- subscribeExclusive registry key
            assertBool "free again" (maybe False (const True) exclusive')
            _joined <- subscribe registry key
            registered registry >>= (@?= 1),
          testCase "a subscriber is woken and deregisters when dropped" $ do
            registry <- newRegistry
            subscription <- subscribe registry (eventKey "wf-1" "progress")
            registered registry >>= (@?= 1)
            wake registry (eventKey "wf-1" "progress")
            waitFor (notified subscription)
            unsubscribe subscription
            registered registry >>= (@?= 0),
          testCase "every waiter on one key is woken" $ do
            registry <- newRegistry
            let key = streamKey "wf-1" "log"
            first <- subscribe registry key
            second <- subscribe registry key
            registered registry >>= (@?= 1)
            wake registry key
            waitFor (notified first)
            waitFor (notified second)
            unsubscribe first
            registered registry >>= (@?= 1)
            wake registry key
            waitFor (notified second),
          testCase "a parked waiter is woken by a later write" $ do
            registry <- newRegistry
            subscription <- subscribe registry (eventKey "wf-1" "k")
            done <- newEmptyTMVarIO
            void (forkIO (notified subscription >> atomically (putTMVar done ())))
            threadDelay 50000
            wake registry (eventKey "wf-1" "k")
            waitFor (atomically (takeTMVar done)),
          testCase "a wakeup between subscribing and waiting is not missed" $ do
            registry <- newRegistry
            subscription <- subscribe registry (eventKey "wf-1" "k")
            wake registry (eventKey "wf-1" "k")
            waitFor (notified subscription),
          testCase "a gap wakes every waiter whatever it waits for" $ do
            registry <- newRegistry
            onEvent <- subscribe registry (eventKey "wf-1" "progress")
            onStream <- subscribe registry (streamKey "wf-2" "log")
            onMessage <- subscribe registry (messageKey "wf-3" Nothing)
            wakeAll registry
            waitFor (notified onEvent)
            waitFor (notified onStream)
            waitFor (notified onMessage),
          testCase "a wake for nobody is not an error" $ do
            registry <- newRegistry
            wake registry (eventKey "someone-else" "key")
            wakeAll registry
            registered registry >>= (@?= 0)
        ]
    ]

-- | How many keys the registry carries. Tests only; nothing branches on it.
registered :: Registry -> IO Int
registered (Registry waitersVar) = Map.size <$> readTVarIO waitersVar

-- | An exclusive subscription that must succeed.
exclusive :: Registry -> Text -> IO Subscription
exclusive registry key = do
  subscription <- subscribeExclusive registry key
  case subscription of
    Just found -> pure found
    Nothing -> fail "expected the key to be free"

-- | A refusal: the second receiver on one topic must not be admitted.
refusedIsNothing :: Maybe Subscription -> IO ()
refusedIsNothing (Just _) = fail "a second receiver on one topic must be refused"
refusedIsNothing Nothing = pure ()

-- | A waiter that must be woken, not left waiting.
waitFor :: IO a -> IO a
waitFor action = do
  result <- timeout 5000000 action
  case result of
    Just value -> pure value
    Nothing -> fail "the waiter was never woken"
