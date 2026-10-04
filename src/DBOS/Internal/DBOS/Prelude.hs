-- | The port's own prelude: 'Prelude' plus the @io-classes@ surface the
-- engine programs against, so modules enable @NoImplicitPrelude@ and take
-- everything from here unqualified.
--
-- The io-classes modules replace their @base@ counterparts: exceptions and
-- bracketing come from 'Control.Monad.Class.MonadThrow' (never
-- @Control.Exception@), threads from 'Control.Monad.Class.MonadFork',
-- async from 'Control.Monad.Class.MonadAsync', MVars from the strict
-- 'Control.Concurrent.Class.MonadMVar.Strict', TVars from the strict
-- 'Control.Concurrent.Class.MonadSTM.Strict', and time/delay from
-- 'Control.Monad.Class.MonadTime' and 'Control.Monad.Class.MonadTimer'.
-- One vocabulary runs under @IO@ and under @IOSim@ alike.
--
-- Conflict policy: names the io-classes modules share with 'Prelude' are
-- hidden here, in the 'Prelude' import — the io-classes imports re-export
-- their full surface. The @async@, @stm@, @exceptions@ and @time@ packages
-- are then not imported anywhere else: their types arrive through these
-- re-exports ('SomeException', 'Exception', 'AsyncCancelled',
-- 'NominalDiffTime', 'UTCTime', strict @TVar@/@MVar@), so a module cannot
-- reach the base API behind the port's back.
module DBOS.Prelude
  ( module Prelude,
    module MonadThrow,
    module MonadFork,
    module MonadAsync,
    module MonadTime,
    module MonadTimer,
    module MonadMVar,
    module MonadSTM,
    showText,
  )
where

import Control.Concurrent.Class.MonadMVar.Strict as MonadMVar
import Control.Concurrent.Class.MonadSTM.Strict as MonadSTM
import Control.Monad.Class.MonadAsync as MonadAsync
import Control.Monad.Class.MonadFork as MonadFork
import Control.Monad.Class.MonadThrow as MonadThrow
import Control.Monad.Class.MonadTime as MonadTime
import Control.Monad.Class.MonadTimer as MonadTimer
import Data.Text (Text, pack)
import Prelude hiding ()

-- | Render any 'Show' value as 'Text'. Shared here so every domain-event
-- renderer uses one spelling.
showText :: Show a => a -> Text
showText = pack . show
