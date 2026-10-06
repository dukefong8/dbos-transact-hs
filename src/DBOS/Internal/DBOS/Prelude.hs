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
  ( module Base,
    module Monad,
    showText,
  )
where

import Control.Applicative as Base
import Control.Arrow as Base hiding (first, second)
import Data.Bifunctor as Base
import Data.Bits as Base
import Data.Bool as Base
import Data.ByteString as Base (ByteString)
import Data.Char as Base
import Data.Coerce as Base
import Data.Data as Base
import Data.Dynamic as Base
import Data.Either as Base
import Data.Fixed as Base
import Data.Foldable as Base hiding (toList)
import Data.Function as Base
import Data.Functor as Base hiding (unzip)
import Data.Functor.Compose as Base
import Data.Functor.Contravariant as Base
import Data.Functor.Contravariant.Divisible as Base
import Data.Maybe as Base
import Data.Monoid as Base hiding (Alt, (<>))
import Data.Semigroup as Base hiding (First (..), Last (..))
import Data.String as Base
import Data.Text (Text, pack)
import Data.Text as Base (Text)
import Data.Text.Encoding as Base (encodeUtf8)
import Data.Traversable as Base
import Data.Tuple as Base
import Data.Word as Base
import Debug.Trace as Base
import Prelude as Base hiding (Read, all, and, any, elem, foldl, foldl1, foldr, foldr1, mapM, mapM_, maximum, minimum, notElem, or, product, sequence, sequence_, sum, (.))
import Text.Read as Base (Read (..), readEither, readMaybe)

import Control.Concurrent.Class.MonadMVar.Strict as Monad
import Control.Concurrent.Class.MonadSTM.Strict as Monad
import Control.Monad as Base hiding (fail, forM, forM_, mapM, mapM_, msum, sequence, sequence_)
import Control.Monad.Class.MonadAsync as Monad
import Control.Monad.Class.MonadFork as Monad
import Control.Monad.Class.MonadThrow as Monad
import Control.Monad.Class.MonadTime as Monad
import Control.Monad.Class.MonadTimer as Monad
import Control.Monad.Except as Monad (Except, ExceptT (ExceptT), mapExcept, mapExceptT, runExcept, runExceptT, withExcept, withExceptT)

-- | Render any 'Show' value as 'Text'. Shared here so every domain-event
-- renderer uses one spelling.
showText :: Show a => a -> Text
showText = pack . show
