-- | The Postgres system-database backend: public client surface.
-- The implementation lives in 'DBOS.SystemDB.Postgres.Backend' (a peer
-- of the other engine modules); this module only re-exports it, so
-- engine peers import the implementation directly and clients keep the
-- stable name.
module DBOS.SystemDB.Postgres
  ( module DBOS.SystemDB.Postgres.Backend,
  )
where

import DBOS.SystemDB.Postgres.Backend
