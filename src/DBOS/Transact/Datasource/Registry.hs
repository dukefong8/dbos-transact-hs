{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DBOS.Transact.Datasource.Registry
  ( DataSourceRegistry,
    newDataSourceRegistry,
    registerDataSource,
    freezeDataSourceRegistry,
    thawDataSourceRegistry,
    snapshotDatasources,
    clearDatasourceCheckpoints,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadMVar.Strict (StrictMVar, modifyMVar, newMVar, readMVar)
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.Text (Text)
import DBOS.SystemDB.Types (WorkflowId (..))
import DBOS.SystemDB.Error (BackendError)
import DBOS.Transact.Datasource (DataSource (..))
import DBOS.Transact.Error qualified as TransactError

-- | The per-instance datasource list, frozen at launch like the workflow
-- registry beside it. A registration after launch is refused — this is
-- the oracle's created-before-launch rule — and a failed launch or
-- shutdown thaws it again.
data DataSourceRegistry m = DataSourceRegistry
  { dsrSources :: StrictMVar m [DataSource m],
    dsrFrozen :: StrictMVar m Bool
  }

newDataSourceRegistry :: MonadMVar m => m (DataSourceRegistry m)
newDataSourceRegistry = DataSourceRegistry <$> newMVar [] <*> newMVar False

-- | Register one datasource unless launch has frozen the registry or its
-- name is taken.
registerDataSource :: MonadMVar m => DataSourceRegistry m -> DataSource m -> m (Either (TransactError.Error TransactError.EngineOnly) ())
registerDataSource registry source = do
  frozen <- readMVar registry.dsrFrozen
  if frozen
    then pure (Left (TransactError.ErrorAlreadyLaunched "register_datasource"))
    else
      modifyMVar registry.dsrSources $ \sources ->
        case filter ((== source.dsName) . (.dsName)) sources of
          _ : _ -> pure (sources, Left (TransactError.ErrorAlreadyRegistered ("datasource " <> source.dsName)))
          [] -> pure (source : sources, Right ())

-- | The registered datasources, oldest first.
snapshotDatasources :: MonadMVar m => DataSourceRegistry m -> m [DataSource m]
snapshotDatasources registry = reverse <$> readMVar registry.dsrSources

-- | Freeze registrations at launch, so a datasource created afterwards is
-- refused rather than silently unused.
freezeDataSourceRegistry :: MonadMVar m => DataSourceRegistry m -> m ()
freezeDataSourceRegistry registry = modifyMVar_ registry.dsrFrozen (const (pure True))

-- | Reopen registration after a failed launch or a shutdown.
thawDataSourceRegistry :: MonadMVar m => DataSourceRegistry m -> m ()
thawDataSourceRegistry registry = modifyMVar_ registry.dsrFrozen (const (pure False))

-- | Clear a finished workflow's checkpoints from every registered
-- datasource, best effort and silent: a leftover row is harmless, since a
-- later replay adopts from it or re-runs.
clearDatasourceCheckpoints :: forall m. (MonadMVar m, MThrow.MonadCatch m) => DataSourceRegistry m -> WorkflowId -> m ()
clearDatasourceCheckpoints registry wid = do
  sources <- snapshotDatasources registry
  mapM_
    ( \source -> do
        _ <- MThrow.try (source.dsDeleteCheckpoints wid 0) :: m (Either SomeException (Either BackendError ()))
        pure ()
    )
    sources
