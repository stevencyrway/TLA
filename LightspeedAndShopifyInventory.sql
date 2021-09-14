use warehouse COMPUTE_WH;
use database FIVETRAN_DB;

--Lightspeed & Shopify Inventory combined.
WITH RECURSIVE cteItem AS (Select id,
                                  CATEGORY_ID,
                                  UPDATED_TIME,
                                  TO_DATE(UPDATED_TIME)                                          as Date,
                                  year(UPDATED_TIME)                                             as Year,
                                  MONTH(UPDATED_TIME)                                            as Month,
                                  weekiso(UPDATED_TIME)                                          as Week,
                                  description,
                                  attribute_1,
                                  attribute_2,
                                  attribute_3,
                                  default_cost,
                                  avg_cost,
                                  CUSTOM_SKU,
                                  MANUFACTURER_SKU,
                                  publish_to_ecom,
                                  ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                           FROM LIGHT_SPEED_RETAIL.ITEM_HISTORY
                           group by ID, UPDATED_TIME, CATEGORY_ID, DESCRIPTION, ATTRIBUTE_1, ATTRIBUTE_2, ATTRIBUTE_3,
                                    DEFAULT_COST, AVG_COST, CUSTOM_SKU, MANUFACTURER_SKU, PUBLISH_TO_ECOM)
   , cteItemShop AS (Select id,
                            UPDATED_TIME,
                            TO_DATE(UPDATED_TIME)                                                            as Date,
                            YEAR(UPDATED_TIME)                                                               as Year,
                            MONTH(UPDATED_TIME)                                                              as Month,
                            weekiso(UPDATED_TIME)                                                            as Week,
                            item_id,
                            shop_id,
                            qoh,
                            backorder,
                            component_qoh,
                            component_backorder,
                            reorder_point,
                            reorder_level,
                            ROW_NUMBER() OVER (PARTITION BY ID, ITEM_ID, SHOP_ID ORDER BY UPDATED_TIME DESC) as RowNumber
                     FROM LIGHT_SPEED_RETAIL.ITEM_SHOP_HISTORY
                     group by ID, ITEM_ID, SHOP_ID, UPDATED_TIME, QOH, BACKORDER, COMPONENT_QOH, COMPONENT_BACKORDER,
                              REORDER_POINT, REORDER_LEVEL)
   , cteShop AS (Select ID,
                        UPDATED_TIME,
                        NAME,
                        TIME_ZONE,
                        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                 FROM LIGHT_SPEED_RETAIL.SHOP_HISTORY
                 group by ID, UPDATED_TIME, NAME, TIME_ZONE)
   , ctecategory as (select id,
                            name,
                            FULL_PATH_NAME,
                            ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                     from LIGHT_SPEED_RETAIL.CATEGORY_HISTORY
                     group by ID, UPDATED_TIME, name, FULL_PATH_NAME)
   , cteShopify AS (Select 'Shopify'              as Source,
                           P.id,
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


select 'Lightspeed'                                      as Source,
       cteItem.ID,
       TRIM(SPLIT_PART(ctecategory.NAME, '/', 1), ' $.') as Category1,
       TRIM(SPLIT_PART(ctecategory.NAME, '/', 2), ' $.') as Category2,
       TRIM(SPLIT_PART(ctecategory.NAME, '/', 3), ' $.') as Category3,
--        max(cteItemShop.UPDATED_TIME)as MaxItemTimeValue,
       cteItemShop.Date,
       cteItemShop.Year,
       cteItemShop.Month,
       cteItemShop.Week,
       cteItem.description                               as ItemDescription,
       cteItem.attribute_1,
       cteItem.attribute_2,
       cteItem.attribute_3,
       cteItem.default_cost,
       cteItem.CUSTOM_SKU                                as Sku,
       NULL                                              as Status,
       cteItemShop.qoh,
       cteItemShop.backorder,
       cteItemShop.reorder_point,
       cteItemShop.reorder_level,
       cteshop.NAME                                      as ShopName,
       cteshop.TIME_ZONE,
       cteItemShop.RowNumber                             as RN
FROM cteItemShop
         join cteItem
              on cteItem.ID = cteItemShop.ITEM_ID
                  and cteItem.RowNumber = 1
         join ctecategory
              on cteItem.CATEGORY_ID = ctecategory.ID
                  and ctecategory.RowNumber = 1
         join cteShop
              on cteItemShop.SHOP_ID = cteshop.ID
                  and cteShop.RowNumber = 1
UNION ALL

Select source,
       id,
       category1,
       category2,
       category3,
       date,
       year,
       month,
       week,
       itemdescription,
       attribute_1,
       attribute_2,
       attribute_3,
       default_cost,
       sku,
       status,
       qoh,
       backorder,
       reorder_point,
       reorder_level,
       shopname,
       time_zone,
       rn
from cteShopify

-- Group by ID,
--          Category1,
--          Category2,
--          Category3,
--          cteItemShop.Year,
--          cteItemShop.Month,
--          cteItemShop.Week,
--          cteItemShop.Date,
--          ItemDescription,
--          cteItem.attribute_1,
--          cteItem.attribute_2,
--          cteItem.attribute_3,
--          cteItem.default_cost,
--          cteItem.avg_cost,
--          Sku,
--          status,
--          cteItemShop.qoh,
--          cteItemShop.backorder,
--          cteItemShop.component_qoh,
--          cteItemShop.component_backorder,
--          cteItemShop.reorder_point,
--          cteItemShop.reorder_level,
--          ShopName,
--          cteshop.TIME_ZONE,
--          RN;
-- full outer join join cteShopify
--     on cteItem.CUSTOM_SKU = cteShopify.SKU
-- where cteItem.RowNumber = 1;





---- for shopify need to combine sku into lightspeed custom sku column, title into description and options into attribute, add source column for shopify vs lightspeed.



