use warehouse COMPUTE_WH;
use database FIVETRAN_DB;

--Inventory Table Build
WITH RECURSIVE
     ctelightspeedItem AS (Select id,
                                  CATEGORY_ID,
                                 to_date(UPDATED_TIME) as ItemDate,
                                  CUSTOM_SKU,
                                  ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                           FROM LIGHT_SPEED_RETAIL.ITEM_HISTORY)
   --Assigns row numbers to to get values over time
   , cteLightspeedInventory AS (Select to_date(UPDATED_TIME) as InventoryDate,
                             item_id,
                             shop_id,
                             qoh,
                             backorder,
                             ROW_NUMBER() OVER (PARTITION BY ID, ITEM_ID, SHOP_ID, to_date(UPDATED_TIME) ORDER BY UPDATED_TIME DESC) as RowNumber
                      FROM LIGHT_SPEED_RETAIL.ITEM_SHOP_HISTORY)
   , ctelightspeedcombined as (Select item_id,
                                      shop_id,
                                      qoh,
                                      backorder,
                                      cteLightspeedInventory.rownumber as RN,
                                      id,
                                      category_id,
                                      InventoryDate as Date,
                                      custom_sku
                               from cteLightspeedInventory
                                        left outer join ctelightspeedItem
                                                        on cteLightspeedInventory.ITEM_ID = ctelightspeedItem.ID
                                                            where ctelightspeedItem.RowNumber = 1
                                                            and cteLightspeedInventory.RowNumber = 1
)
   --Preps shopify by joining product and product variant to get all details
   , cteShopifyinventory AS (Select 'Shopify'              as Source,
                                    P.product_type         as Category1,
                                    NULL                   as Category2,
                                    NULL                   as Category3,
                                    TO_DATE(il.UPDATED_AT) as Date,
                                    II.COST                as default_cost,
                                    PV.Sku,
                                    P.status,
                                    IL.available           as qoh,
                                    NULL                   as backorder,
                                    NULL                   as reorder_point,
                                    NULL                   as reorder_level,
                                    'Shopify Ecom'         as ShopName,
                                    NULL                   as TIME_ZONE,
                                    1                      as RN
                             from LOVERS_SHOPIFY.PRODUCT as P
                                      join LOVERS_SHOPIFY.PRODUCT_VARIANT as PV
                                           on P.ID = PV.PRODUCT_ID
                                      join LOVERS_SHOPIFY.INVENTORY_ITEM as II
                                           on pv.INVENTORY_ITEM_ID = ii.ID
                                      join LOVERS_SHOPIFY.INVENTORY_LEVEL IL
                                           on PV.INVENTORY_ITEM_ID = IL.INVENTORY_ITEM_ID)
   --Assigns row numbers to multiple inventory moves per day
   , cteYandyInventoryHistoryPrep AS (Select im.oldinv,
                                         to_date(im.changetime) as InventoryDate,
                                         im.option_id,
                                         im.newinv as QOH,
                                             ROW_NUMBER() OVER (PARTITION BY OPTION_ID,to_date(CHANGETIME) ORDER BY CHANGETIME DESC) as RowNumber
                                  from POSTGRES_PUBLIC.INVENTORY_MOVE IM)
   ---!! neeed clarity !! Not sure which price we should be using from product options in yandy, there exists catalog price, map price, option price and more.
   , cteYandyProducts AS (select po.PROD_ID as UUID,
                                 PROD_ID,
                                 PROD_OPTION_ID,
                                 po.option_sku,
                                 po.option_price,
                                 null       as Cost
                          from POSTGRES_PUBLIC.product_options po)
   , cteyandyinventorycombined as (Select oldinv,
                                          inventorydate,
                                          option_id,
                                          qoh,
                                          rownumber,
                                          uuid,
                                          prod_id,
                                          prod_option_id,
                                          option_sku,
                                          option_price,
                                          cost
                                   from cteYandyInventoryHistoryPrep
                                            join cteYandyProducts on PROD_OPTION_ID = OPTION_ID
                                   where RowNumber = 1)

--Lightspeed Completed Inventory Fact Details
Select item_id,
       shop_id,
       qoh,
       backorder,
       id,
       category_id,
       Date,
       custom_sku
from ctelightspeedcombined
where RN = 1
and ITEM_ID = 2407



---Need Yandy location data for location id
Select YIHP.oldinv,
       YIHP.InventoryDate,
       YIHP.option_id,
       YIHP.newinv,
       po.OPTION_INV,
       po.CATALOG_PRICE,
       po.ON_ORDER,
       po.BACKORDERED
from cteYandyInventoryHistoryPrep YIHP
         join POSTGRES_PUBLIC.PRODUCT_OPTIONS PO
              on po.PROD_OPTION_ID = YIHP.OPTION_ID
                  and YIHP.RowNumber = 1;








