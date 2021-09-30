use warehouse COMPUTE_WH;
use database FIVETRAN_DB;

--To create the table
CREATE OR REPLACE TABLE FIVETRAN_DB.public.INVENTORY_FACT
    (
     DATE DATE NOT NULL,
     ITEMID VARCHAR(255) NULL,
     UUID VARCHAR(255) NULL,
     QTY_SOLD SMALLINT NULL,
     PRICE DECIMAL(6, 2) NULL,
     DISCOUNT DECIMAL(6, 2) NULL,
     LOCATIONID VARCHAR(50) NUll,
     SOURCE VARCHAR(100) NULL
        )
As

--Inventory Table Build
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
   , cteyandyorders as (select returned, --boolean true or false
                               order_id,
                               order_date,
                               backorder_date,
                               prod_id,
                               option_id,
                               quantity,
                               discount_percent,
                               tax,
                               total_prod_price,
                               SITE_ID
                        from FIVETRAN_DB.POSTGRES_PUBLIC.ORDERS_PRODS)
--    , cteYandyInventoryHistoryPrep AS ()
--    , cteyandyinventorycombined as ()


---//// WHERE IT ALL COMBINES ////---
-- the intent here is to take all the relevant cte's above and combine them through unions to make one conjoined inventory table.
-- This methodology will be followed for all tables.


-- First attempt at Yandy Inventory table, need to add cost next from FIFO ledger
Select to_date(ORDER_DATE)                                     as Date,
       CONCAT(to_varchar(PROD_ID), '/', to_varchar(OPTION_ID)) as ItemID,
       ORDER_ID                                                as UUID,
       QUANTITY                                                as QTY_SOLD,
       TOTAL_PROD_PRICE                                        as Price,
       DISCOUNT_PERCENT                                        as Discount_Amount,
       null                                                    as LocationID, --couldn't find shop values in yandy data, need to ask Aras.
       'Yandy'                                                 as Source
from cteyandyorders

union all

-- Lightspeed Completed Inventory Fact Details, this is missing cost.
Select to_date(ORDERED_DATE) as SoldDate,
       to_varchar(ITEM_ID)   as ItemID,
       ID                    as UUID,       --Order ID
       QUANTITY              as QTY_SOLD,
       PRICE                 as Price,
       DISCOUNT              as Discount_Amount,
       SHOP_ID               as LocationID, --couldn't find shop values in yandy data, need to ask Aras.
       'LoversLightspeed'    as Source
from ctelightspeedcombined;




