{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

-- | Shared transactional-outbox scenarios: Variant A (the atomic workflow:
-- insert-order transaction plus notification steps) and Variant B (the
-- transactional enqueue: order insert plus notification enqueue in one
-- outside transaction), each under all-or-nothing commit and rollback, plus
-- notification redrive after a mid-send failure. One body per case, judged
-- by one pure check on each stack, over the shared 'OutboxFixture'. The
-- live tree ('DBOS.Transact.OutboxTest') runs them over Postgres with real
-- SQL statements; the sim tree ('DBOS.Transact.OutboxTestSim') over staged
-- TVar maps with the same engine calls (real 'runTxOutside' / 'runTxStep' /
-- 'runWorkflow' / 'resumeWorkflow' on both stacks — only the statement
-- layer differs, the WidgetSim precedent).
module DBOS.Transact.OutboxCases
  ( TxFault (..),
    Envelope (..),
    OutboxFixture (..),
    atomicBody,
    notifyBody,
    placeEnqueued,
    findNotification,
    waitClaimed,
    driveQueue,
    matchCustomer,
    readRowShared,
    scenarioAtomicCommit,
    checkAtomicCommit,
    scenarioAtomicRollback,
    checkAtomicRollback,
    scenarioEnqueueCommit,
    checkEnqueueCommit,
    scenarioEnqueueRollbackApp,
    checkEnqueueRollbackApp,
    scenarioEnqueueRollbackDb,
    checkEnqueueRollbackDb,
    scenarioAtomicRedrive,
    checkAtomicRedrive,
    scenarioEnqueueRedrive,
    checkEnqueueRedrive,
  )
where

import Control.Applicative ((<|>))
import Data.Aeson (Value (..), decodeStrict)
import Data.Aeson qualified as Aeson
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Text qualified as Text
import Data.Text.Encoding (encodeUtf8)
import DBOS.Prelude
import DBOS.SystemDB (AwaitedOutcome (..), WorkflowFilter (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), defaultWorkflowFilter)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error (BackendError)
import DBOS.Transact
import DBOS.Transact.Connection (SomeSystemDB, runSystemDB)
import DBOS.Transact.Instance (dequeueWorkflows)
import GHC.Stack (HasCallStack)

-- | What the next instrumented transaction body does instead of its work:
-- the fault is consumed on read (one-shot), so a resume runs clean.
data TxFault
  = TxOk
  | TxThrowApp
  | TxThrowDb
  deriving stock (Eq, Show)

-- | Domain operations each backend implements: the live tree with real SQL
-- through the 'Tx' handle, the sim tree with staged TVar writes that the
-- fake 'withTx' commits or discards. Scenarios only see these ops plus the
-- real engine entries, never SQL or maps directly.
data OutboxFixture m = OutboxFixture
  { obDBOS               :: DBOS m,
    obExecutor           :: Executor m,
    obDataSource         :: DataSource m,
    obAtomicRef          :: WorkflowRef m EngineOnly,
    obNotifyRef          :: WorkflowRef m EngineOnly,
    obFreshTag           :: Text -> m Text,
    obFreshWid           :: Text -> m WorkflowId,
    obTxInsert           :: Tx m -> Text -> Text -> Int -> m Int,
    obTxEnqueue          :: Tx m -> Int -> Text -> m WorkflowId,
    obTxMarkSent         :: Tx m -> Int -> m (),
    obNoteSent           :: m (),
    obArmTxThrow         :: m (),
    obArmTxDbError       :: m (),
    obArmSendFail        :: Int -> m (),
    obConsumeSendFail    :: m Bool,
    obConsumeTxFault     :: m TxFault,
    obRunAtomic          :: WorkflowId -> Text -> Text -> Int -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)),
    obPlaceEnqueued      :: Text -> Text -> Int -> m (Either BackendError (Int, WorkflowId)),
    obOrderIds           :: Text -> m [Int],
    obFindNotification   :: Text -> m (Maybe WorkflowId),
    obAwaitNotification  :: WorkflowId -> m (Maybe WorkflowStatus),
    obResumeNotification :: WorkflowId -> m (Maybe WorkflowStatus),
    obResumeAtomic       :: WorkflowId -> m (Maybe WorkflowStatus),
    obOrderStatus        :: Int -> m (Maybe Text),
    obOrderCount         :: Text -> m Int,
    obSentCount          :: m Int,
    obReadRow            :: WorkflowId -> m (Maybe WorkflowRecord),
    obSystemDB           :: SomeSystemDB m
  }

txInsertCfg :: TransactionConfig
txInsertCfg = TransactionConfig {txName = Just "insert_order", txIsolation = Just Serializable}

txMarkCfg :: TransactionConfig
txMarkCfg = TransactionConfig {txName = Just "update_notification_status", txIsolation = Just Serializable}

-- | Variant A body (shared): insert-order transaction, notification send
-- step, mark-sent transaction. Returns the order id.
atomicBody ::
  forall m exec.
  (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  DataSource m ->
  (Tx m -> Text -> Text -> Int -> m Int) ->
  (Tx m -> Int -> m ()) ->
  m () ->
  m Bool ->
  (Text, Text, Int) ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Int)
atomicBody ds insertOp markOp noteSent takeFail (cust, item, qty) wctx = do
  placed <- runTxStep ds txInsertCfg wctx (\sctx tx -> Right <$> insertOp tx cust item qty)
  case placed of
    Left err -> pure (Left err)
    Right oid -> do
      flight <- runStep wctx "send_notification" (\_ -> do
        failed <- takeFail
        if failed
          then throwIO (userError "outbox probe: send fails")
          else noteSent >> pure oid)
      case flight of
        Left err -> pure (Left err)
        Right _ -> runTxStep ds txMarkCfg wctx (\sctx tx -> markOp tx oid >> pure (Right oid))

-- | A notification envelope accepting both input shapes: the bare tuple
-- the engine records for direct runs, and the @positionalArgs@ object the
-- SQL enqueue path writes (mirroring the demo's @NotifyEnvelope@).
newtype Envelope = Envelope
  { envelopeArgs :: (Int, Text, Text)
  }

instance Aeson.FromJSON Envelope where
  parseJSON v = (Envelope <$> Aeson.parseJSON v) <|> Aeson.withObject "Envelope" (\o -> Envelope <$> o Aeson..: "positionalArgs") v

-- | The enqueued notification body (shared): send step, then mark-sent
-- transaction. The envelope arrives as recorded inputs.
notifyBody ::
  forall m exec.
  (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  DataSource m ->
  (Tx m -> Int -> m ()) ->
  m () ->
  m Bool ->
  Envelope ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) ())
notifyBody ds markOp noteSent takeFail (Envelope args) wctx = do
  let (oid, _cust, _item) = args
  flight <- runStep wctx "send_notification" (\_ -> do
    failed <- takeFail
    if failed
      then throwIO (userError "outbox probe: send fails")
      else noteSent >> pure ())
  case flight of
    Left err -> pure (Left err)
    Right _ -> runTxStep ds txMarkCfg wctx (\sctx tx -> markOp tx oid >> pure (Right ()))

-- | Variant B placement (shared): order insert plus notification enqueue in
-- one outside transaction — both commit or neither does.
placeEnqueued ::
  forall m.
  (MonadDelay m, MonadCatch m) =>
  DataSource m ->
  (Tx m -> Text -> Text -> Int -> m Int) ->
  (Tx m -> Int -> Text -> m WorkflowId) ->
  Text ->
  Text ->
  Int ->
  m (Either BackendError (Int, WorkflowId))
placeEnqueued ds insertOp enqueueOp cust item qty =
  runTxOutside ds txInsertCfg (\tx -> do
    oid <- insertOp tx cust item qty
    wid <- enqueueOp tx oid cust
    pure (oid, wid))

-- | The notification enqueued for the tagged customer, if any.
-- Lists the notification workflows and matches recorded inputs. No status
-- filter: the supervisor may already have claimed and run it.
findNotification :: (MonadThrow m, MonadMVar m) => DBOS m -> Text -> m (Maybe WorkflowId)
findNotification dbos tag = do
  listed <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterNames = ["send_notification_workflow"]})
  case listed of
    Left err   -> throwIO (userError (show err))
    Right rows -> pure (go rows)
  where
    go []           = Nothing
    go (row : rest) = if matchCustomer tag row then Just row.workflowRecordId else go rest

-- | Drives the executor's listened queues once, returning what was claimed.
-- Throws on a database failure; an empty claim is fine (the supervisor may
-- have taken the row first — claims are atomic, outcomes identical).
driveQueue :: (MonadMVar m, MonadFork m, MonadMask m, MonadTimer m, MonadTime m) => DBOS m -> m [WorkflowId]
driveQueue dbos = do
  driven <- dequeueWorkflows dbos
  case driven of
    Left err -> throwIO (userError (show err))
    Right wids -> pure wids

-- | Waits until a queued workflow leaves ENQUEUED (claimed by a worker),
-- then holds one quiescence interval and re-reads: the panic path that
-- follows a claim finishes in microseconds, while a still-running body
-- would move the row again. Bounded; throws on timeout instead of hanging
-- the suite.
waitClaimed :: (MonadDelay m, MonadThrow m) => OutboxFixture m -> WorkflowId -> m (Maybe WorkflowStatus)
waitClaimed fx wid = go (200 :: Int)
  where
    statusOf row = case row of
      Just r  -> Just r.workflowRecordStatus
      Nothing -> Nothing
    go 0 = throwIO (userError "the queued workflow was never claimed")
    go n = do
      row <- fx.obReadRow wid
      case statusOf row of
        Just Enqueued -> threadDelay 50000 >> go (n - 1)
        _             -> threadDelay 500000 >> statusOf <$> fx.obReadRow wid

-- | Whether a listed workflow row is the notification enqueued for the
-- tagged customer: the recorded inputs carry @[orderId, customer, item]@.
matchCustomer :: Text -> WorkflowRecord -> Bool
matchCustomer tag record = case record.workflowRecordInput of
  Nothing -> False
  Just raw -> case decodeStrict (encodeUtf8 raw) of
    Just (Object obj) -> case KeyMap.lookup "positionalArgs" obj of
      Just (Array arr) -> case foldr (:) [] arr of
        [_, String cust, _] -> cust == tag
        _ -> False
      _ -> False
    _ -> False

-- | A workflow row read that throws on database failure (engine errors
-- throw via 'MonadThrow'), shared by both trees.
readRowShared :: (MonadThrow m) => SomeSystemDB m -> WorkflowId -> m (Maybe WorkflowRecord)
readRowShared sysdb wid = do
  found <- runSystemDB sysdb (\db -> SystemDB.getWorkflow db wid)
  case found of
    Left err  -> throwIO (userError (show err))
    Right row -> pure row

-- | Variant A commits atomically: the workflow succeeds, one order row
-- reads SENT, one completed send. Returns the count, status, row status,
-- and completions.
scenarioAtomicCommit ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  OutboxFixture m ->
  m (Int, Maybe Text, Maybe WorkflowStatus, Int)
scenarioAtomicCommit fx = do
  tag <- fx.obFreshTag "atomic-commit"
  wid <- fx.obFreshWid "atomic-commit"
  ran <- fx.obRunAtomic wid tag "Widget A" 2
  oid <- case ran of
    Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
      Right n  -> pure n
      Left err -> throwIO (userError (show err))
    other -> throwIO (userError ("expected the order id, got: " <> show other))
  count <- fx.obOrderCount tag
  status <- fx.obOrderStatus oid
  row <- fx.obReadRow wid
  sent <- fx.obSentCount
  pure (count, status, fmap (.workflowRecordStatus) row, sent)

-- | The atomic commit records one SENT order and one completed send.
checkAtomicCommit :: (Int, Maybe Text, Maybe WorkflowStatus, Int) -> Either String ()
checkAtomicCommit (count, status, row, sent)
  | count /= 1 = Left ("expected one order row, got: " <> show count)
  | status /= Just "SENT" = Left ("expected the order SENT, got: " <> show status)
  | row /= Just Success = Left ("expected the workflow SUCCESS, got: " <> show row)
  | sent /= 1 = Left ("expected one completed send, got: " <> show sent)
  | otherwise = Right ()

-- | Variant A rolls back on an app throw after the insert: the run panics,
-- the row stays PENDING, no order row exists and nothing sent; a resume
-- then runs clean to one SENT order and one send. Returns the first row,
-- count, status, completions, then the resumed row, count, status,
-- completions.
scenarioAtomicRollback ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  OutboxFixture m ->
  m (Maybe WorkflowStatus, Int, Maybe Text, Int, Maybe WorkflowStatus, Int, Maybe Text, Int)
scenarioAtomicRollback fx = do
  tag <- fx.obFreshTag "atomic-rollback"
  wid <- fx.obFreshWid "atomic-rollback"
  fx.obArmTxThrow
  first <- try (fx.obRunAtomic wid tag "Widget A" 2)
  row1 <- fx.obReadRow wid
  count1 <- fx.obOrderCount tag
  sent1 <- fx.obSentCount
  status1 <- case count1 of
    0 -> pure Nothing
    _ -> throwIO (userError "expected no order row after the rolled-back insert")
  _ <- case first of
    Left (_ :: SomeException) -> pure ()
    Right ran                 -> throwIO (userError ("expected the run to panic, got: " <> show ran))
  _ <- fx.obResumeNotification wid
  row2 <- fx.obReadRow wid
  count2 <- fx.obOrderCount tag
  status2 <- do
    oids <- fx.obOrderIds tag
    case oids of
      [oid] -> fx.obOrderStatus oid
      other -> throwIO (userError ("expected exactly one order row after resume, got: " <> show other))
  sent2 <- fx.obSentCount
  pure (fmap (.workflowRecordStatus) row1, count1, status1, sent1, fmap (.workflowRecordStatus) row2, count2, status2, sent2)

-- | The rolled-back atomic run leaves PENDING with no rows; the resume
-- completes to one SENT order and one send.
checkAtomicRollback :: (Maybe WorkflowStatus, Int, Maybe Text, Int, Maybe WorkflowStatus, Int, Maybe Text, Int) -> Either String ()
checkAtomicRollback (row1, count1, status1, sent1, row2, count2, status2, sent2)
  | row1 /= Just Pending = Left ("expected the panicked row PENDING, got: " <> show row1)
  | count1 /= 0 = Left ("expected no order row, got: " <> show count1)
  | status1 /= Nothing = Left ("expected no status, got: " <> show status1)
  | sent1 /= 0 = Left ("expected no completed send, got: " <> show sent1)
  | row2 /= Just Success = Left ("expected the resumed workflow SUCCESS, got: " <> show row2)
  | count2 /= 1 = Left ("expected one order row after resume, got: " <> show count2)
  | status2 /= Just "SENT" = Left ("expected the order SENT after resume, got: " <> show status2)
  | sent2 /= 1 = Left ("expected one completed send after resume, got: " <> show sent2)
  | otherwise = Right ()

-- | Variant B commits atomically: one order row, one found enqueue, and the
-- driven notification succeeds to SENT with one send. Returns the count,
-- whether the enqueue was found, the row status, order status, completions.
scenarioEnqueueCommit ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  OutboxFixture m ->
  m (Int, Bool, Maybe WorkflowStatus, Maybe Text, Int)
scenarioEnqueueCommit fx = do
  tag <- fx.obFreshTag "enqueue-commit"
  placed <- fx.obPlaceEnqueued tag "Widget B" 1
  (oid, wid) <- case placed of
    Right pair -> pure pair
    Left err   -> throwIO (userError ("expected the placement to commit, got: " <> show err))
  found <- fx.obFindNotification tag
  rowStatus <- fx.obAwaitNotification wid
  count <- fx.obOrderCount tag
  row <- fx.obReadRow wid
  status <- fx.obOrderStatus oid
  sent <- fx.obSentCount
  _ <- case rowStatus of
    Just Success -> pure ()
    other        -> throwIO (userError ("expected the awaited notification SUCCESS, got: " <> show other))
  pure (count, found == Just wid, fmap (.workflowRecordStatus) row, status, sent)

-- | The enqueued placement commits both halves and the notification sends once.
checkEnqueueCommit :: (Int, Bool, Maybe WorkflowStatus, Maybe Text, Int) -> Either String ()
checkEnqueueCommit (count, found, row, status, sent)
  | count /= 1 = Left ("expected one order row, got: " <> show count)
  | not found = Left "expected to find the enqueued notification by customer"
  | row /= Just Success = Left ("expected the notification SUCCESS, got: " <> show row)
  | status /= Just "SENT" = Left ("expected the order SENT, got: " <> show status)
  | sent /= 1 = Left ("expected one completed send, got: " <> show sent)
  | otherwise = Right ()

-- | Variant B rolls back on an app throw after both writes: the placement
-- fails, no order row and no enqueue row exist, nothing sent; placing again
-- (fault consumed) commits and the replacement notification runs to SENT.
-- Returns the first error presence, count, found, sent, then the
-- replacement count, status, completions, row status.
scenarioEnqueueRollbackApp ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  OutboxFixture m ->
  m (Bool, Int, Bool, Int, Int, Maybe Text, Int, Maybe WorkflowStatus)
scenarioEnqueueRollbackApp fx = do
  tag <- fx.obFreshTag "enqueue-rollback-app"
  fx.obArmTxThrow
  placed <- try (fx.obPlaceEnqueued tag "Widget B" 1)
  failed <- case placed of
    Left (_ :: SomeException) -> pure True
    Right pair                -> throwIO (userError ("expected the placement to roll back, got: " <> show pair))
  count1 <- fx.obOrderCount tag
  found1 <- fx.obFindNotification tag
  sent1 <- fx.obSentCount
  placed2 <- fx.obPlaceEnqueued tag "Widget B" 1
  (oid2, wid2) <- case placed2 of
    Right pair -> pure pair
    Left err   -> throwIO (userError ("expected the replacement to commit, got: " <> show err))
  rowStatus2 <- fx.obAwaitNotification wid2
  count2 <- fx.obOrderCount tag
  status2 <- fx.obOrderStatus oid2
  sent2 <- fx.obSentCount
  pure (failed, count1, found1 /= Nothing, sent1, count2, status2, sent2, rowStatus2)

-- | The rolled-back placement leaves no order and no enqueue; the
-- replacement runs to one SENT order and one send.
checkEnqueueRollbackApp :: (Bool, Int, Bool, Int, Int, Maybe Text, Int, Maybe WorkflowStatus) -> Either String ()
checkEnqueueRollbackApp (failed, count1, found1, sent1, count2, status2, sent2, rowStatus2)
  | not failed = Left "expected the placement to fail"
  | count1 /= 0 = Left ("expected no order row, got: " <> show count1)
  | found1 = Left "expected no enqueue row"
  | sent1 /= 0 = Left ("expected no completed send, got: " <> show sent1)
  | count2 /= 1 = Left ("expected one replacement order, got: " <> show count2)
  | status2 /= Just "SENT" = Left ("expected the replacement SENT, got: " <> show status2)
  | sent2 /= 1 = Left ("expected one completed send, got: " <> show sent2)
  | rowStatus2 /= Just Success = Left ("expected the replacement SUCCESS, got: " <> show rowStatus2)
  | otherwise = Right ()

-- | Variant B rolls back on a database error after the insert: same
-- all-or-nothing shape through the engine-error path, with the replacement
-- driven to SENT.
scenarioEnqueueRollbackDb ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  OutboxFixture m ->
  m (Bool, Int, Bool, Int, Int, Maybe Text, Int, Maybe WorkflowStatus)
scenarioEnqueueRollbackDb fx = do
  tag <- fx.obFreshTag "enqueue-rollback-db"
  fx.obArmTxDbError
  placed <- fx.obPlaceEnqueued tag "Widget B" 1
  failed <- case placed of
    Left _     -> pure True
    Right pair -> throwIO (userError ("expected the placement to roll back, got: " <> show pair))
  count1 <- fx.obOrderCount tag
  found1 <- fx.obFindNotification tag
  sent1 <- fx.obSentCount
  placed2 <- fx.obPlaceEnqueued tag "Widget B" 1
  (oid2, wid2) <- case placed2 of
    Right pair -> pure pair
    Left err   -> throwIO (userError ("expected the replacement to commit, got: " <> show err))
  rowStatus2 <- fx.obAwaitNotification wid2
  count2 <- fx.obOrderCount tag
  status2 <- fx.obOrderStatus oid2
  sent2 <- fx.obSentCount
  pure (failed, count1, found1 /= Nothing, sent1, count2, status2, sent2, rowStatus2)

-- | The database-error placement likewise leaves nothing behind, and the
-- replacement runs to SENT.
checkEnqueueRollbackDb :: (Bool, Int, Bool, Int, Int, Maybe Text, Int, Maybe WorkflowStatus) -> Either String ()
checkEnqueueRollbackDb (failed, count1, found1, sent1, count2, status2, sent2, rowStatus2)
  | not failed = Left "expected the placement to fail"
  | count1 /= 0 = Left ("expected no order row, got: " <> show count1)
  | found1 = Left "expected no enqueue row"
  | sent1 /= 0 = Left ("expected no completed send, got: " <> show sent1)
  | count2 /= 1 = Left ("expected one replacement order, got: " <> show count2)
  | status2 /= Just "SENT" = Left ("expected the replacement SENT, got: " <> show status2)
  | sent2 /= 1 = Left ("expected one completed send, got: " <> show sent2)
  | rowStatus2 /= Just Success = Left ("expected the replacement SUCCESS, got: " <> show rowStatus2)
  | otherwise = Right ()

-- | Variant A redrives a failed send: the first run panics mid-notification
-- with the insert already recorded (one PENDING-notification order, no
-- send); the resume adopts the insert and completes to one SENT order and
-- one send. Returns the first row, count, status, completions, then the
-- resumed row, count, status, completions.
scenarioAtomicRedrive ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  OutboxFixture m ->
  m (Maybe WorkflowStatus, Int, Maybe Text, Int, Maybe WorkflowStatus, Int, Maybe Text, Int)
scenarioAtomicRedrive fx = do
  tag <- fx.obFreshTag "atomic-redrive"
  wid <- fx.obFreshWid "atomic-redrive"
  fx.obArmSendFail 1
  first <- try (fx.obRunAtomic wid tag "Widget A" 2)
  row1 <- fx.obReadRow wid
  count1 <- fx.obOrderCount tag
  status1 <- do
    oids <- fx.obOrderIds tag
    case oids of
      [oid] -> fx.obOrderStatus oid
      _     -> pure Nothing
  sent1 <- fx.obSentCount
  _ <- case first of
    Left (_ :: SomeException) -> pure ()
    Right ran                 -> throwIO (userError ("expected the send to panic, got: " <> show ran))
  _ <- fx.obResumeAtomic wid
  row2 <- fx.obReadRow wid
  count2 <- fx.obOrderCount tag
  status2 <- do
    oids <- fx.obOrderIds tag
    case oids of
      [oid] -> fx.obOrderStatus oid
      other -> throwIO (userError ("expected exactly one order row after resume, got: " <> show other))
  sent2 <- fx.obSentCount
  pure (fmap (.workflowRecordStatus) row1, count1, status1, sent1, fmap (.workflowRecordStatus) row2, count2, status2, sent2)

-- | The failed send leaves PENDING with the insert recorded and no send;
-- the resume completes exactly once.
checkAtomicRedrive :: (Maybe WorkflowStatus, Int, Maybe Text, Int, Maybe WorkflowStatus, Int, Maybe Text, Int) -> Either String ()
checkAtomicRedrive (row1, count1, status1, sent1, row2, count2, status2, sent2)
  | row1 /= Just Pending = Left ("expected the panicked row PENDING, got: " <> show row1)
  | count1 /= 1 = Left ("expected the recorded insert, got: " <> show count1)
  | status1 /= Just "PENDING" = Left ("expected the order PENDING-notification, got: " <> show status1)
  | sent1 /= 0 = Left ("expected no completed send, got: " <> show sent1)
  | row2 /= Just Success = Left ("expected the resumed workflow SUCCESS, got: " <> show row2)
  | count2 /= 1 = Left ("expected still one order row, got: " <> show count2)
  | status2 /= Just "SENT" = Left ("expected the order SENT after resume, got: " <> show status2)
  | sent2 /= 1 = Left ("expected one completed send after resume, got: " <> show sent2)
  | otherwise = Right ()

-- | Variant B redrives a failed notification: place, run to a mid-send
-- panic (order PENDING-notification, no send), resume to one SENT order
-- and one send. Returns the first row, count, status, completions, then
-- the resumed row, count, status, completions.
scenarioEnqueueRedrive ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack) =>
  OutboxFixture m ->
  m (Maybe WorkflowStatus, Int, Maybe Text, Int, Maybe WorkflowStatus, Int, Maybe Text, Int)
scenarioEnqueueRedrive fx = do
  tag <- fx.obFreshTag "enqueue-redrive"
  placed <- fx.obPlaceEnqueued tag "Widget B" 1
  (oid, wid) <- case placed of
    Right pair -> pure pair
    Left err   -> throwIO (userError ("expected the placement to commit, got: " <> show err))
  fx.obArmSendFail 1
  claimed <- fx.obFindNotification tag
  _ <- case claimed of
    Just found | found == wid -> pure ()
    other                     -> throwIO (userError ("expected to find the enqueued notification, got: " <> show other))
  _ <- driveQueue fx.obDBOS
  row1 <- waitClaimed fx wid
  status1 <- fx.obOrderStatus oid
  sent1 <- fx.obSentCount
  _ <- case row1 of
    Just Pending -> pure ()
    other        -> throwIO (userError ("expected the panicked row PENDING, got: " <> show other))
  count1 <- fx.obOrderCount tag
  _ <- fx.obResumeNotification wid
  row2 <- fx.obReadRow wid
  status2 <- fx.obOrderStatus oid
  count2 <- fx.obOrderCount tag
  sent2 <- fx.obSentCount
  pure (row1, count1, status1, sent1, fmap (.workflowRecordStatus) row2, count2, status2, sent2)

-- | The failed notification leaves PENDING with no send; the resume
-- completes exactly once with no duplicate order.
checkEnqueueRedrive :: (Maybe WorkflowStatus, Int, Maybe Text, Int, Maybe WorkflowStatus, Int, Maybe Text, Int) -> Either String ()
checkEnqueueRedrive (row1, count1, status1, sent1, row2, count2, status2, sent2)
  | row1 /= Just Pending = Left ("expected the failed notification PENDING (dispatched panic records nothing), got: " <> show row1)
  | count1 /= 1 = Left ("expected the placed order, got: " <> show count1)
  | status1 /= Just "PENDING" = Left ("expected the order PENDING-notification, got: " <> show status1)
  | sent1 /= 0 = Left ("expected no completed send, got: " <> show sent1)
  | row2 /= Just Success = Left ("expected the resumed workflow SUCCESS, got: " <> show row2)
  | count2 /= 1 = Left ("expected still one order row, got: " <> show count2)
  | status2 /= Just "SENT" = Left ("expected the order SENT after resume, got: " <> show status2)
  | sent2 /= 1 = Left ("expected one completed send after resume, got: " <> show sent2)
  | otherwise = Right ()
