--To create the table
-- CREATE OR REPLACE TABLE FIVETRAN_DB.PROD.LIGHTSPEED_INVENTORY_HISTORY
--     (
--      ROWUUID VARCHAR(255) NOT NULL PRIMARY KEY,
--      INSERTDATE DATE NOT NULL,
--      VALUELASTUPDATEDDATE VARCHAR(255) NULL,
--      SKU VARCHAR(255) NULL,
--      ITEMUUID VARCHAR(255) NULL,
--      QOH SMALLINT NULL,
--      BACKORDER SMALLINT NULL,
--      COST DECIMAL(10, 2) NULL,
--      LOCATIONID VARCHAR(50) Null,
--      CATEGORYID VARCHAR(150) NULL
--         )
--
-- as
Insert INTO FIVETRAN_DB.PROD.LIGHTSPEED_INVENTORY_HISTORY (ROWUUID, INSERTDATE, VALUELASTUPDATEDDATE, SKU, ITEMUUID, QOH, BACKORDER, COST, LOCATIONID, CATEGORYID)

WITH RECURSIVE
     ctelightspeedItem AS (Select id,
                                 CATEGORY_ID,
                                 to_date(UPDATED_TIME)                                          as ItemDate,
                                 CUSTOM_SKU,
                                 AVG_COST,
                                 DESCRIPTION,
                                 ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                          FROM FIVETRAN_DB.LIGHT_SPEED_RETAIL.ITEM_HISTORY)
   -- Assigns row numbers to to get values over time
   , cteLightspeedInventory AS (Select to_date(UPDATED_TIME)                                                                                   as InventoryDate,
                                       id,
                                       item_id,
                                       shop_id,
                                       qoh,
                                       backorder,
                                       ROW_NUMBER() OVER (PARTITION BY ID, ITEM_ID, SHOP_ID, to_date(UPDATED_TIME) ORDER BY UPDATED_TIME DESC) as RowNumber
                                FROM FIVETRAN_DB.LIGHT_SPEED_RETAIL.ITEM_SHOP_HISTORY)
   -- this table combines the two above and then filters to only rownumber 1 to get the Most recent value in a given day.
   , ctelightspeedcombined as (Select concat(to_date(current_timestamp),'/',to_varchar(SHOP_ID),'/',to_varchar(ITEM_ID)) as rowuuid,
                                      item_id as ITEMUUID,
                                      shop_id as LOCATIONID,
                                      qoh,
                                      backorder,
                                      cteLightspeedInventory.rownumber as RN,
                                      category_id as CATEGORYID,
                                      InventoryDate                    as ValueLastUpdatedDate,
                                      to_date(current_timestamp)        as InsertDate,
                                      custom_sku as SKU,
                                      AVG_COST as Cost,
                                      DESCRIPTION
                               from cteLightspeedInventory
                                        left outer join ctelightspeedItem
                                                        on cteLightspeedInventory.ITEM_ID = ctelightspeedItem.ID
                               where ctelightspeedItem.RowNumber = 1
                                 and cteLightspeedInventory.RowNumber = 1)

Select ROWUUID,
       InsertDate,
       valuelastupdateddate,
       SKU,
       ITEMUUID,
       QOH,
       BACKORDER,
       COST,
       LOCATIONID,
       CATEGORYID
from ctelightspeedcombined;