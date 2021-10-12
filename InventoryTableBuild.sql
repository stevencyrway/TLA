use warehouse COMPUTE_WH;
use database FIVETRAN_DB;

--To create the table
CREATE OR REPLACE TABLE FIVETRAN_DB.PROD.INVENTORY_FACT
    (
     DATE DATE NOT NULL,
     SKU VARCHAR(255) NULL,
     ITEMUUID VARCHAR(255) NULL,
     QOH SMALLINT NULL,
     BACKORDER SMALLINT NULL,
     COST DECIMAL(6, 2) NULL,
     LOCATIONID VARCHAR(50) Null,
     CATEGORYID VARCHAR(100) NULL,
     SOURCE VARCHAR(100) NULL
        )
As
    --Inventory Table Build
-- !! Need to confirm timezones of each data that's landed to ensure proper time zone offset.
WITH RECURSIVE
---- /// Lightspeed /// ----
-- Assigns row numbers to to get values over time and identify when more than value occurs in a day.
    ctelightspeedcombined as (Select  VALUELASTUPDATEDDATE,
                                      insertdate,
                                      sku,
                                      itemuuid,
                                      qoh,
                                      backorder,
                                      cost,
                                      locationid,
                                      categoryid
                               from FIVETRAN_DB.PROD.LIGHTSPEED_INVENTORY_HISTORY)
---- /// YANDY /// ----
   -- Assigns row numbers to multiple inventory moves per day
   , cteyandyproductname as (select PROD_ID, PROD_NAME
                             from FIVETRAN_DB.POSTGRES_PUBLIC.ORDERS_PRODS
                             group by PROD_ID, PROD_NAME)
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
                         from FIVETRAN_DB.POSTGRES_PUBLIC.FIFO_LEDGER ---Need to add date to this calculation. this table contains historical costs received.
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
                                          PROD_NAME,
                                          cteyandyavgcost.Cost
                                   from cteYandyInventoryHistoryPrep
                                            join cteYandyProducts on PROD_OPTION_ID = OPTION_ID
                                            join cteyandyavgcost
                                                 on cteyandyavgcost.PROD_OPTION_ID = cteYandyProducts.PROD_OPTION_ID
                                            join cteyandycategoryjoin
                                                 on cteYandyProducts.PROD_ID = cteyandycategoryjoin.PROD_ID
                                            join cteyandyproductname
                                                 on cteYandyProducts.PROD_ID = cteyandyproductname.PROD_ID ---Need to confirm this join doesn't duplicate anything still 09/29/2021
                                   where InventoryRowNumber = 1)


---//// WHERE IT ALL COMBINES ////---
-- the intent here is to take all the relevant cte's above and combine them through unions to make one conjoined inventory table.
-- This methodology will be followed for all tables.
--
-- -- Yandy
-- Select inventorydate                                                              as Date,
--        option_sku                                                                 as SKU,
--        concat('Yandy', '/', to_varchar(PROD_ID), '/', to_varchar(PROD_OPTION_ID)) as ItemUUID,
--        QOH,
--        Null                                                                       as Backorder,
--        Cost,
--        null                                                                       as LocationID, --couldn't find shop values in yandy data, need to ask Aras.
--        CategoryID                                                                 as CategoryID,
--        'Yandy'                                                                    as Source
-- from cteyandyinventorycombined
--
-- union all

-- Lightspeed
Select INSERTDATE                    as Date,
       SKU                                as SKU,
       concat('Lightspeed', '/', ITEMUUID) as ItemUUID,
       QOH,
       Backorder,
       Cost                               as Cost, --Need to add this in upstream table
       LOCATIONID                         as LocationID,
       to_varchar(categoryid)             as CategoryID,
       'Lightspeed'                       as Source
from ctelightspeedcombined;

