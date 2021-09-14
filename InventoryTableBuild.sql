use warehouse COMPUTE_WH;

--Inventory Table Build
WITH RECURSIVE cteItem AS (Select id,
                                  CATEGORY_ID,
                                  UPDATED_TIME,
                                  CUSTOM_SKU,
                                  ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                           FROM LIGHT_SPEED_RETAIL.ITEM_HISTORY
                           group by ID, UPDATED_TIME, CATEGORY_ID, CUSTOM_SKU)
   --Assigns row numbers to to get values over time
   , cteLightspeedInventory AS (Select to_date(UPDATED_TIME),
                             item_id,
                             shop_id,
                             qoh,
                             backorder,
                             ROW_NUMBER() OVER (PARTITION BY ID, ITEM_ID, SHOP_ID, to_date(UPDATED_TIME) ORDER BY UPDATED_TIME DESC) as RowNumber
                      FROM LIGHT_SPEED_RETAIL.ITEM_SHOP_HISTORY
                      group by ID, ITEM_ID, SHOP_ID, UPDATED_TIME, QOH, BACKORDER)
   --Preps shopify by joining product and product variant to get all details
   , cteShopifyinventory AS (Select 'Shopify'              as Source,
                                    P.product_type         as Category1,
                                    NULL                   as Category2,
                                    NULL                   as Category3,
                                    TO_DATE(il.UPDATED_AT) as Date,
                                    YEAR(il.UPDATED_AT)    as Year,
                                    MONTH(il.UPDATED_AT)   as Month,
                                    WEEKISO(il.UPDATED_AT) as Week,
                                    P.title                as ItemDescription,
                                    PV.option_1            as attribute_1,
                                    PV.option_2            as attribute_2,
                                    PV.option_3            as attribute_3,
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
                                         im.newinv,
                                             ROW_NUMBER() OVER (PARTITION BY OPTION_ID,to_date(CHANGETIME) ORDER BY CHANGETIME DESC) as RowNumber
                                  from POSTGRES_PUBLIC.INVENTORY_MOVE IM)


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




select po.prod_option_id,
       po.la_sku,
       po.option_style,
       po.upc,
       po.catalog_price,
       po.map_price, --maybe only necessary if catalog doesn't match map
       po.on_order,
       po.fnsku,
       po.option_inv,
       po.backordered,
       po.option_sku,
       po.option_price

from POSTGRES_PUBLIC.product_options po
where po.ON_ORDER is not null
and po.ON_ORDER >= 1


