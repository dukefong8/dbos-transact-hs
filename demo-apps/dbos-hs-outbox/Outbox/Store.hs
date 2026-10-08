{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

-- Orphans by design: the PrimaryKey instances for the app's own tables
-- belong beside those tables, not in the type family's module.
{-# OPTIONS_GHC -Wno-orphans #-}

-- | The outbox demo's application table and the typedSql statements every
-- transactional step and handler runs against it (the Python
-- transactional-outbox's @orders@ table, now with ihp-typed-sql).
--
-- Every statement is a plain 'Statement.Statement' with its parameters baked
-- in, so the same value runs two ways: through the transactional-step
-- engine's held connection inside a workflow step (@Tx.txStatement@ — the
-- application write and the step checkpoint share one commit), and through
-- the datasource pool in a handler (@runAppSession . Session.statement ()@).
-- The transactional-enqueue variant's raw @dbos.enqueue_workflow@ call is a
-- statement too, so the order row and the enqueued notification workflow
-- commit (or roll back) together.
module Outbox.Store
  ( -- * App rows
    OutboxOrder (..),
    OrderRow,
    decodeOrderRow,

    -- * Schema bootstrap (app-owned; the demo creates its own tables)
    schemaSql,
    createSchemaSession,

    -- * Statements
    insertOrderStatement,
    listOrdersStatement,
    setNotificationStatusStatement,
    enqueueNotificationStatement,
  )
where

import Control.Monad (void)
import Data.Aeson (ToJSON (..), object, (.=))
import Data.Aeson qualified as Aeson
import Data.Text (Text)
import Data.Text qualified as Text
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import IHP.TypedSql.Hasql (sqlExecTypedStatement, sqlQueryTypedStatement, typedSql)
import IHP.TypedSql.Id (Id' (..), PrimaryKey)
import IHP.TypedSql.RowType (SqlRow)
import Language.Haskell.TH.Syntax (addDependentFile, lift, runIO)
import Prelude

-- | The app's claim on its own primary key, what IHP's generated project
-- code would declare (the Python schema uses @SERIAL@, so the key is 'Int').
-- Orphans by design: the instances belong beside the app's own tables.
type instance PrimaryKey "orders" = Int

-- * Rows

-- | An order, as the orders list displays it (the Python @orders@ table).
type OrderRow =
  SqlRow
    '[ '("order_id", Id' "orders"),
       '("customer", Text),
       '("item", Text),
       '("quantity", Int),
       '("notification_status", Text),
       '("created_at", Maybe Text)
     ]

-- | The order, as the storefront displays it.
data OutboxOrder = OutboxOrder
  { outboxOrderId     :: Int,
    outboxCustomer    :: Text,
    outboxItem        :: Text,
    outboxQuantity    :: Int,
    outboxNotifStatus :: Text,
    outboxCreatedAt   :: Text
  }
  deriving stock (Eq, Show)

-- | The Python JSON keys, spelled the same so the UI vocabulary matches.
instance ToJSON OutboxOrder where
  toJSON o =
    object
      [ "order_id" .= o.outboxOrderId,
        "customer" .= o.outboxCustomer,
        "item" .= o.outboxItem,
        "quantity" .= o.outboxQuantity,
        "notification_status" .= o.outboxNotifStatus,
        "created_at" .= o.outboxCreatedAt
      ]

decodeOrderRow :: OrderRow -> OutboxOrder
decodeOrderRow row =
  OutboxOrder
    { outboxOrderId = let Id key = row.order_id in key,
      outboxCustomer = row.customer,
      outboxItem = row.item,
      outboxNotifStatus = row.notification_status,
      outboxQuantity = row.quantity,
      outboxCreatedAt = maybe "" id row.created_at
    }

-- * Schema bootstrap

-- | The app-owned DDL, embedded at compile time from @schema.sql@ so the demo
-- can create its own tables the way the widget store does.
schemaSql :: Text
schemaSql = Text.pack $(do
  sql <- runIO (readFile "dbos-hs-outbox/schema.sql")
  addDependentFile "dbos-hs-outbox/schema.sql"
  lift sql)

-- | Create the demo schema idempotently. Run once at startup, before launch.
createSchemaSession :: Session.Session ()
createSchemaSession = Session.script schemaSql

-- * Statements

-- | @INSERT INTO orders (customer, item, quantity) ... RETURNING order_id@.
insertOrderStatement :: Text -> Text -> Int -> Statement.Statement () (Id' "orders")
insertOrderStatement customer item quantity = sqlQueryTypedStatement [typedSql|
  insert into outbox_store.orders (customer, item, quantity) values (${customer}, ${item}, ${quantity})
  returning order_id
|]

-- | All orders, newest first (the Python @list_orders@).
listOrdersStatement :: Statement.Statement () [OrderRow]
listOrdersStatement = sqlQueryTypedStatement [typedSql|
  select order_id, customer, item, quantity, notification_status,
         created_at::text as created_at
    from outbox_store.orders
   order by order_id desc
|]

-- | @UPDATE orders SET notification_status = ... WHERE order_id = ...@.
setNotificationStatusStatement :: Text -> Id' "orders" -> Statement.Statement () ()
setNotificationStatusStatement status orderId = void $ sqlExecTypedStatement [typedSql|
  update outbox_store.orders
     set notification_status = ${status}
   where order_id = ${orderId}
|]

-- | The classic transactional outbox as one statement: instead of writing to
-- an outbox table that a poller scans, call DBOS's @dbos.enqueue_workflow@
-- PL/pgSQL function. Run through the held 'Tx' in the same body as the order
-- insert, the order row and the enqueued workflow commit (or roll back)
-- together, so the notification is durably scheduled if and only if the order
-- is created. Arguments travel as JSON (each positional argument is a JSON
-- value); the @::json@ casts are needed because the parameters arrive
-- untyped. Returns the enqueued workflow's id.
enqueueNotificationStatement ::
  Text -> Text -> Text -> Text -> Aeson.Value -> Aeson.Value -> Aeson.Value -> Statement.Statement () (Maybe Text)
enqueueNotificationStatement appVersion appName workflowName queueName orderIdJson customerJson itemJson =
  sqlQueryTypedStatement [typedSql|
  select dbos.enqueue_workflow(workflow_name => ${workflowName}, queue_name => ${queueName}, positional_args => array[${orderIdJson}::json, ${customerJson}::json, ${itemJson}::json], app_version => ${appVersion}, application_name => ${appName}) as workflow_id
|]
