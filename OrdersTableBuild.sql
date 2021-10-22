use warehouse COMPUTE_WH;
use database FIVETRAN_DB;

--To create the table
CREATE OR REPLACE TABLE FIVETRAN_DB.PROD.ORDER_FACT
    (
     DATE DATE NULL,
     ITEMUUID VARCHAR(255) NULL,
     OrderUUID VARCHAR(255) NULL,
     QTY_SOLD SMALLINT NULL,
     PRICE DECIMAL(10, 2) NULL,
     DISCOUNT NUMBER NULL,
     LOCATIONID VARCHAR(50) NUll,
     SOURCE VARCHAR(100) NULL
        )
As

--Inventory Table Build
-- /// NOTES ///
-- Per Aras who designed most of the Yandy architecture
-- Orders is the main meta for customer purchases.
-- Orders_prods is the line items for orders in a particular order. (Customer may buy many)
--
-- Best way to connect on variant/option level:
-- orders_prods.option_id  ->  product_options.prod_option_id
--
-- If you just need main product level
-- orders_prods.prod_id -> products.prod_id
-- orders_prods.prod_id -> products.site_specific.prod_id


-- !! Need to confirm timezones of each data that's landed to ensure proper time zone offset.
WITH RECURSIVE
---- /// Lightspeed /// ----
   -- Assigns row numbers to to get values over time and identify when more than value occurs in a day.
    ctelightspeedorders AS (Select ORDER_HISTORY.id,
                                   ORDER_HISTORY.updated_time,
                                   ORDER_HISTORY.shop_id,
                                   ORDER_HISTORY.ordered_date,
                                   ORDER_HISTORY.received_date,
                                   ORDER_HISTORY.arrival_date,
                                   ORDER_HISTORY.ship_cost,
                                   ORDER_HISTORY.discount,
                                   ORDER_HISTORY.total_discount,
                                   ORDER_HISTORY.total_quantity,
                                   ROW_NUMBER() OVER (PARTITION BY ORDER_HISTORY.ID ORDER BY ORDER_HISTORY.UPDATED_TIME desc) as OrderRowNumber
                            from FIVETRAN_DB.LIGHT_SPEED_RETAIL.ORDER_HISTORY)
   -- Assigns row numbers to to get values over time
   , cteLightspeedorderlines AS (Select ORDER_LINE_HISTORY.id,
                                        ORDER_LINE_HISTORY.updated_time,
                                        ORDER_LINE_HISTORY.order_id,
                                        ORDER_LINE_HISTORY.item_id,
                                        ORDER_LINE_HISTORY.total,
                                        ORDER_LINE_HISTORY.price,
                                        ORDER_LINE_HISTORY.original_price,
                                        ORDER_LINE_HISTORY.quantity,
                                        ROW_NUMBER() OVER (PARTITION BY ORDER_LINE_HISTORY.ID, ORDER_LINE_HISTORY.ORDER_ID ORDER BY ORDER_LINE_HISTORY.UPDATED_TIME desc) as OrderlineRownumber
                                 from FIVETRAN_DB.LIGHT_SPEED_RETAIL.ORDER_LINE_HISTORY)
   -- this table combines the two above and then filters to only rownumber 1 to get the Most recent value in a given day.
   , ctelightspeedcombined as (Select ctelightspeedorders.id,
                                      cteLightspeedorders.shop_id,
                                      ctelightspeedorders.ordered_date, --Table also has Received and arrival date that may be useful later.
                                      cteLightspeedorderlines.item_id,
                                      cteLightspeedorderlines.price,
                                      cteLightspeedorderlines.original_price,
                                      cteLightspeedorders.discount,
                                      cteLightspeedorderlines.quantity,
                                      cteLightspeedorderlines.total
                               from ctelightspeedorders
                                        join cteLightspeedorderlines
                                             on ctelightspeedorders.ID = cteLightspeedorderlines.ID
                                                 and ctelightspeedorders.OrderRowNumber = 1
                                                 and cteLightspeedorderlines.OrderlineRownumber = 1)
---- /// YANDY /// ----
   -- Assigns row numbers to multiple inventory moves per day
   , cteyandyorders as (Select o.ORDER_ID,
                               op.ORDERS_PRODS_ID,
                               o.ORDER_DATE,
                               op.BACKORDER_DATE,
                               op.PROD_ID,
                               op.OPTION_ID,
                               op.quantity,
                               op.discount_percent,
                               op.tax,
                               op.total_prod_price,
                               o.order_status,
                               op.SITE_ID, -- site id 1 = Yandy, SITE_ID = 2 then 'Playboy'
                               op.RETURNED
                        from FIVETRAN_DB.POSTGRES_PUBLIC.ORDERS O
                                 join FIVETRAN_DB.POSTGRES_PUBLIC.ORDERS_PRODS OP
                                      on O.ORDER_ID = OP.ORDERS_PRODS_ID
                        where O.ORDER_DATE >= '2020-01-01'
                          and o.ORDER_STATUS in (1, 2, 3))--this filters out all orders that are active and not cancelled


---//// WHERE IT ALL COMBINES ////---
-- the intent here is to take all the relevant cte's above and combine them through unions to make one conjoined inventory table.
-- This methodology will be followed for all tables.


-- Yandy
Select to_date(ORDER_DATE)                                                   as Date,
       concat('Yandy', '/', to_varchar(PROD_ID), '/', to_varchar(OPTION_ID)) as ItemUUID,   --UUID for Item Dim Table
       to_varchar(ORDER_ID)                                                  as OrderUUID,
       QUANTITY                                                              as QTY_SOLD,
       TOTAL_PROD_PRICE                                                      as Price,
       DISCOUNT_PERCENT                                                      as Discount_Amount,
       SITE_ID                                                               as LocationID, --couldn't find shop values in yandy data, need to ask Aras.
       'Yandy'                                                               as Source
from cteyandyorders

union all

-- Lightspeed
Select to_date(ORDERED_DATE)              as SoldDate,
       concat('Lightspeed', '/', to_varchar(ITEM_ID)) as ItemUUID,   --UUID for Item dim Table
       to_varchar(ID)                     as OrderUUID,  --Order ID
       QUANTITY                           as QTY_SOLD,
       PRICE                              as Price,
       DISCOUNT                           as Discount_Amount,
       SHOP_ID                            as LocationID, --couldn't find shop values in yandy data, need to ask Aras.
       'Lightspeed'                 as Source
from ctelightspeedcombined;

