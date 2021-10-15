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
   , cteyandyavgcost as (Select prod_option_id,
                                last_day(to_date(to_timestamp(STAMPED)))                                              as MonthEnding,
                                avg(cost_price)                                                                       as MonthlyCostAvg,
                                Lead(last_day(to_date(to_timestamp(STAMPED))))
                                     OVER (ORDER BY last_day(to_date(to_timestamp(STAMPED))) desc, FL.prod_option_id desc)  as PreviousMonthEnding
                         from FIVETRAN_DB.POSTGRES_PUBLIC.FIFO_LEDGER FL
                         group by last_day(to_date(to_timestamp(STAMPED))), prod_option_id)
   , cteyandyinvcostLeadDayAdd as (Select prod_option_id,
                                          Monthending,
                                          MonthlyCostAvg,
                                          case
                                              when PreviousMonthEnding >= MonthEnding then PreviousMonthEnding
                                              when PreviousMonthEnding < MonthEnding
                                                  then dateadd(day, 1, PreviousMonthEnding) end as StartMonth
                                   from cteyandyavgcost)
   , cteyandyinventorycombined as (Select inventorydate,
                                          option_id,
                                          qoh,
                                          InventoryRowNumber,
                                          uuid,
                                          cteYandyProducts.prod_id,
                                          cteYandyProducts.prod_option_id,
                                          CategoryID,
                                          option_sku,
                                        pss.PROD_NAME,
                                          cteyandyinvcostLeadDayAdd.MonthlyCostAvg
                                   from cteYandyInventoryHistoryPrep
                                            join cteYandyProducts on PROD_OPTION_ID = OPTION_ID
                                            join cteyandyinvcostLeadDayAdd
                                                 on cteyandyinvcostLeadDayAdd.PROD_OPTION_ID = cteYandyProducts.PROD_OPTION_ID
                                                and InventoryDate <= cteyandyinvcostLeadDayAdd.MonthEnding
                                                and inventorydate >= cteyandyinvcostLeadDayAdd.StartMonth
                                            join cteyandycategoryjoin
                                                 on cteYandyProducts.PROD_ID = cteyandycategoryjoin.PROD_ID
                                          LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.products_site_specific pss
                                             ON cteYandyProducts.prod_id = pss.prod_id AND pss.site_id = 1
                                   where InventoryRowNumber = 1)

---//// WHERE IT ALL COMBINES ////---
-- the intent here is to take all the relevant cte's above and combine them through unions to make one conjoined inventory table.
-- This methodology will be followed for all tables.

-- Yandy

Select inventorydate                                                              as Date,
       option_sku                                                                 as SKU,
       concat('Yandy', '/', to_varchar(PROD_ID), '/', to_varchar(PROD_OPTION_ID)) as ItemUUID,
       QOH,
       Null                                                                       as Backorder,
       MonthlyCostAvg                                                             as Cost,
       null                                                                       as LocationID, --couldn't find shop values in yandy data, need to ask Aras.
       CategoryID                                                                 as CategoryID,
       'Yandy'                                                                    as Source
from cteyandyinventorycombined

union all

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

