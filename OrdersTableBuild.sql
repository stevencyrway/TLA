use warehouse COMPUTE_WH;
use database PROD_TLA_DW

--To create the table
CREATE OR REPLACE TABLE PROD_TLA_DW.public.INVENTORY_FACT
(
    DATE       DATE          NOT NULL,
    SKU        VARCHAR(255)  NULL,
    UUID       VARCHAR(255)  NULL,
    QOH        SMALLINT      NULL,
    BACKORDER  SMALLINT      NULL,
    COST       DECIMAL(6, 2) NULL,
    ITEMID     VARCHAR(50)   NULL,
    LOCATIONID VARCHAR(50)   Null,
    CATEGORYID VARCHAR(100)  NULL,
    SOURCE     VARCHAR(100)  NULL
)

As

--Inventory Table Build
-- !! Need to confirm timezones of each data that's landed to ensure proper time zone offset.
WITH RECURSIVE
---- /// Lightspeed /// ----
   -- Assigns row numbers to to get values over time and identify when more than value occurs in a day.
    ctelightspeedItem AS (Select id,
                                 CATEGORY_ID,
                                 to_date(UPDATED_TIME)                                          as ItemDate,
                                 CUSTOM_SKU,
                                 AVG_COST,
                                 ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                          FROM FIVETRAN_DB.LIGHT_SPEED_RETAIL.ITEM_HISTORY)
   -- Assigns row numbers to to get values over time
   , cteLightspeedInventory AS (Select to_date(UPDATED_TIME)                                                                                   as InventoryDate,
                                       item_id,
                                       shop_id,
                                       qoh,
                                       backorder,
                                       ROW_NUMBER() OVER (PARTITION BY ID, ITEM_ID, SHOP_ID, to_date(UPDATED_TIME) ORDER BY UPDATED_TIME DESC) as RowNumber
                                FROM FIVETRAN_DB.LIGHT_SPEED_RETAIL.ITEM_SHOP_HISTORY)
   -- this table combines the two above and then filters to only rownumber 1 to get the Most recent value in a given day.
   , ctelightspeedcombined as (Select item_id,
                                      shop_id,
                                      qoh,
                                      backorder,
                                      cteLightspeedInventory.rownumber as RN,
                                      id,
                                      category_id,
                                      InventoryDate                    as Date,
                                      custom_sku,
                                      AVG_COST as Cost
                               from cteLightspeedInventory
                                        left outer join ctelightspeedItem
                                                        on cteLightspeedInventory.ITEM_ID = ctelightspeedItem.ID
                               where ctelightspeedItem.RowNumber = 1
                                 and cteLightspeedInventory.RowNumber = 1)
---- /// YANDY /// ----
   -- Assigns row numbers to multiple inventory moves per day
   , cteyandycategoryjoin as (Select PROD_ID,
                                     PRODUCTS.ptype,
                                     PRODUCTS.n2,
                                     LOCATION,
                                     concat(PTYPE, '_', N2) as CategoryID
                              from FIVETRAN_DB.POSTGRES_PUBLIC.PRODUCTS)
   , cteYandyInventoryHistoryPrep AS (Select im.oldinv,
                                             to_date(im.changetime)                                                                  as InventoryDate,
                                             im.option_id,
                                             im.newinv                                                                               as QOH,
                                             ROW_NUMBER() OVER (PARTITION BY OPTION_ID,to_date(CHANGETIME) ORDER BY CHANGETIME DESC) as InventoryRowNumber
                                      from FIVETRAN_DB.POSTGRES_PUBLIC.INVENTORY_MOVE IM)
   , cteYandyProducts AS (select po.PROD_ID as UUID,
                                 PROD_ID,
                                 PROD_OPTION_ID,
                                 po.option_sku
                          from FIVETRAN_DB.POSTGRES_PUBLIC.product_options po)
   , cteyandyavgcost as (select FIFO_LEDGER.prod_option_id,
                                avg(FIFO_LEDGER.cost_price) as Cost
                         from FIVETRAN_DB.POSTGRES_PUBLIC.FIFO_LEDGER
                         group by PROD_OPTION_ID)
   , cteyandyinventorycombined as (Select inventorydate,
                                          option_id,
                                          qoh,
                                          InventoryRowNumber,
                                          uuid,
                                          cteYandyProducts.prod_id,
                                          cteYandyProducts.prod_option_id,
                                          CategoryID,
                                          option_sku,
                                          cteyandyavgcost.Cost
                                   from cteYandyInventoryHistoryPrep
                                            join cteYandyProducts on PROD_OPTION_ID = OPTION_ID
                                            join cteyandyavgcost on cteyandyavgcost.PROD_OPTION_ID = cteYandyProducts.PROD_OPTION_ID
                                            join cteyandycategoryjoin on cteYandyProducts.PROD_ID = cteyandycategoryjoin.PROD_ID
                                   where InventoryRowNumber = 1)


---//// WHERE IT ALL COMBINES ////---
-- the intent here is to take all the relevant cte's above and combine them through unions to make one conjoined inventory table.
-- This methodology will be followed for all tables.

-- First attempt at Yandy Inventory table, need to add cost next from FIFO ledger
Select inventorydate            as Date,
       option_sku               as SKU,
       concat('Yandy', to_varchar(UUID)) as UUID,
       null as QTY_SOLD,
       null as Price,
       null as Discount_Amount
       null                     as LocationID, --couldn't find shop values in yandy data, need to ask Aras.
       CategoryID               as CategoryID,
       'Yandy'                  as Source
from cteyandyinventorycombined

union all

-- Lightspeed Completed Inventory Fact Details, this is missing cost.
Select null          as SoldDate,
       option_sku               as SKU,
       concat('Yandy', to_varchar(UUID)) as UUID,
       null as QTY_SOLD,
       null as Price,
       null as Discount_Amount,
       null                     as LocationID, --couldn't find shop values in yandy data, need to ask Aras.
       CategoryID               as CategoryID,
       'LoversLightspeed'       as Source
from ctelightspeedcombined;


select
       returned,
       order_date,
       backorder_date,
       prod_id,
       net_price,
       option_sku,
       option_color,
       amazon_id,
       option_style,
       order_id,
       tax,
       option_size,
       misship,
       option_id,

       prod_name,
       brand_code,
       prod_price,
       total_prod_price,
       returned_date,
       reviewed,
       prod_sku,
       discount_group_id,
       to_dropship,
       discount_percent,
       quantity,
       wholesale_price,
from FIVETRAN_DB.POSTGRES_PUBLIC.ORDERS_PRODS