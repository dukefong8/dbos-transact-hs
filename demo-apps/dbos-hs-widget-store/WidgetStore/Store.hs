{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

-- Orphans by design: the PrimaryKey instances for the app's own tables
-- belong beside those tables, not in the type family's module.
{-# OPTIONS_GHC -Wno-orphans #-}

-- | The widget store's application tables and the typedSql statements every
-- workflow step and handler runs against them (the Rust port's @store.rs@,
-- now with ihp-typed-sql).
--
-- Every statement is a plain 'Statement.Statement' with its parameters baked
-- in, so the same value runs two ways: through the transactional-step
-- engine's held connection inside a workflow step (@Tx.txStatement@ — the
-- application write and the step checkpoint share one commit), and through
-- the datasource pool in a handler (@runAppSession . Session.statement ()@).
-- The compile-time typedSql describe connects to @$DATABASE_URL@ (the
-- quasiquoter's own variable, not the datasource's @APP_DATABASE_URL@), so
-- @make widget-db@ (schema.sql) must have run before the first build.
--
-- typedSql names the shapes: primary keys decode as @Id' table@ (the type
-- family instances below are the app's claim on its own tables), @int4@
-- columns decode as 'Int', and a cast expression (the @price::double
-- precision@ and @last_update_time::text@ the Rust port also performs)
-- describes as nullable, so those fields are 'Maybe' and defaulted in the
-- row converters.
module WidgetStore.Store
  ( -- * Constants
    widgetId,
    orderStatusCancelled,
    orderStatusPending,
    orderStatusDispatched,
    orderStatusPaid,
    dispatchTicks,

    -- * App rows
    Product (..),
    Order (..),
    ProductRow,
    OrderRow,
    decodeProductRow,
    decodeOrderRow,

    -- * Schema bootstrap (app-owned; the demo creates its own tables)
    schemaSql,
    createSchemaSession,

    -- * Statements
    createOrderStatement,
    reserveInventoryStatement,
    undoReserveInventoryStatement,
    setOrderStatusStatement,
    updateOrderProgressStatement,
    productStatement,
    ordersStatement,
    orderStatement,
    restockStatement,
  )
where

import Control.Monad (void)
import Data.Aeson (ToJSON (..), object, (.=))
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Prelude
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import IHP.TypedSql.Hasql (sqlExecTypedStatement, sqlQueryTypedStatement, typedSql)
import IHP.TypedSql.Id (Id' (..), PrimaryKey)
import IHP.TypedSql.RowType (SqlRow)
import Language.Haskell.TH.Syntax (addDependentFile, lift, runIO)

-- | The app's claim on its own primary keys, what IHP's generated project
-- code would declare (the Rust schema uses @SERIAL@, so the key is 'Int').
-- Orphans by design: the instances belong beside the app's own tables.
type instance PrimaryKey "orders" = Int

type instance PrimaryKey "products" = Int

-- * Constants (store.rs)

-- | The only product the store sells.
widgetId :: Int
widgetId = 1

-- | Order statuses, shared by number with the Python, TypeScript, Go and Java
-- widget stores: the frontend and the schema are the same in all five, so the
-- numbers are a wire format.
orderStatusCancelled :: Int
orderStatusCancelled = -1

orderStatusPending :: Int
orderStatusPending = 0

orderStatusDispatched :: Int
orderStatusDispatched = 1

orderStatusPaid :: Int
orderStatusPaid = 2

-- | How many progress ticks a dispatched order takes to arrive.
dispatchTicks :: Int
dispatchTicks = 10

-- * Rows

-- | A typedSql multi-column select decodes as a 'SqlRow' with the selected
-- column names as fields.
type ProductRow =
  SqlRow
    '[ '("product_id", Id' "products"),
       '("product", Maybe Text),
       '("description", Text),
       '("inventory", Int),
       '("price", Maybe Double)
     ]

type OrderRow =
  SqlRow
    '[ '("order_id", Id' "orders"),
       '("order_status", Int),
       '("last_update_time", Maybe Text),
       '("progress_remaining", Int)
     ]

-- | The product, as the storefront displays it (store.rs @Product@).
data Product = Product
  { productId          :: Int,
    productName        :: Text,
    productDescription :: Text,
    productInventory   :: Int,
    productPrice       :: Double
  }
  deriving stock (Eq, Show)

-- | An order, as the orders list displays it (store.rs @Order@).
data Order = Order
  { orderId                :: Int,
    orderStatus            :: Int,
    orderLastUpdateTime    :: Text,
    orderProgressRemaining :: Int
  }
  deriving stock (Eq, Show)

-- | The Rust JSON keys, spelled the same for every port's frontend.
instance ToJSON Product where
  toJSON p =
    object
      [ "product_id" .= p.productId,
        "product" .= p.productName,
        "description" .= p.productDescription,
        "inventory" .= p.productInventory,
        "price" .= p.productPrice
      ]

instance ToJSON Order where
  toJSON order =
    object
      [ "order_id" .= order.orderId,
        "order_status" .= order.orderStatus,
        "last_update_time" .= order.orderLastUpdateTime,
        "progress_remaining" .= order.orderProgressRemaining
      ]

decodeProductRow :: ProductRow -> Product
decodeProductRow row =
  Product
    { productId = let Id key = row.product_id in key,
      productName = fromMaybe "" row.product,
      productDescription = row.description,
      productInventory = row.inventory,
      productPrice = fromMaybe 0 row.price
    }

decodeOrderRow :: OrderRow -> Order
decodeOrderRow row =
  Order
    { orderId = let Id key = row.order_id in key,
      orderStatus = row.order_status,
      orderLastUpdateTime = fromMaybe "" row.last_update_time,
      orderProgressRemaining = row.progress_remaining
    }

-- * Schema bootstrap

-- | The app-owned DDL, embedded at compile time from @schema.sql@ so the demo
-- can create its own tables the way the Rust port's @create_schema@ does.
-- @make widget-db@ applies the same file for the compile-time typedSql
-- describe.
schemaSql :: Text
schemaSql = Text.pack $(do
  sql <- runIO (readFile "dbos-hs-widget-store/schema.sql")
  addDependentFile "dbos-hs-widget-store/schema.sql"
  lift sql)

-- | Create the demo schema idempotently. Run once at startup, before launch.
createSchemaSession :: Session.Session ()
createSchemaSession = Session.script (schemaSql <> migrationSql)

-- | Bring a table created before @step_name@ existed up to date. Additive and
-- idempotent, the way the oracles' data-source migrations are.
migrationSql :: Text
migrationSql =
  "ALTER TABLE IF EXISTS widget_store.transaction_completion "
    <> "ADD COLUMN IF NOT EXISTS step_name TEXT NOT NULL DEFAULT '';"

-- * Statements (store.rs queries)

-- | @INSERT INTO orders (order_status) VALUES (0) RETURNING order_id@.
createOrderStatement :: Statement.Statement () (Id' "orders")
createOrderStatement = sqlQueryTypedStatement [typedSql|
  insert into widget_store.orders (order_status) values (0)
  returning order_id
|]

-- | @UPDATE products SET inventory = inventory - 1 WHERE product_id = 1 AND inventory > 0@.
-- The affected-row count is the reservation's yes/no, so two checkouts racing
-- for the last widget both run it and exactly one updates a row.
reserveInventoryStatement :: Statement.Statement () Int64
reserveInventoryStatement = sqlExecTypedStatement [typedSql|
  update widget_store.products set inventory = inventory - 1
   where product_id = 1 and inventory > 0
|]

undoReserveInventoryStatement :: Statement.Statement () ()
undoReserveInventoryStatement = void $ sqlExecTypedStatement [typedSql|
  update widget_store.products set inventory = inventory + 1
   where product_id = 1
|]

setOrderStatusStatement :: Int -> Id' "orders" -> Statement.Statement () ()
setOrderStatusStatement status orderId = void $ sqlExecTypedStatement [typedSql|
  update widget_store.orders
     set order_status = ${status}, last_update_time = now()
   where order_id = ${orderId}
|]

-- | Ticks one unit off an order's progress. The transaction body marks the
-- order dispatched when the returned count reaches zero, in the same commit.
updateOrderProgressStatement :: Id' "orders" -> Statement.Statement () [Int]
updateOrderProgressStatement orderId = sqlQueryTypedStatement [typedSql|
  update widget_store.orders
     set progress_remaining = greatest(progress_remaining - 1, 0),
         last_update_time = now()
   where order_id = ${orderId}
  returning progress_remaining
|]

productStatement :: Statement.Statement () (Maybe ProductRow)
productStatement = sqlQueryTypedStatement [typedSql|
  select product_id, product::text as product, description, inventory,
         price::double precision as price
    from widget_store.products
   order by product_id
   limit 1
|]

ordersStatement :: Statement.Statement () [OrderRow]
ordersStatement = sqlQueryTypedStatement [typedSql|
  select order_id, order_status, last_update_time::text as last_update_time,
         progress_remaining
    from widget_store.orders
   order by order_id
|]

orderStatement :: Id' "orders" -> Statement.Statement () (Maybe OrderRow)
orderStatement orderId = sqlQueryTypedStatement [typedSql|
  select order_id, order_status, last_update_time::text as last_update_time,
         progress_remaining
    from widget_store.orders
   where order_id = ${orderId}
|]

restockStatement :: Statement.Statement () ()
restockStatement = void $ sqlExecTypedStatement [typedSql|
  update widget_store.products set inventory = 100
|]
