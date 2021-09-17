use warehouse COMPUTE_WH;
-- use database PROD_TLA_DW

--To create the table
-- CREATE OR REPLACE TABLE PROD_TLA_DW.public.ITEM_DIM
-- (
--     SKU         VARCHAR(255) NULL,
--     UUID        VARCHAR(255) NOT NULL,
--     DESCRIPTION VARCHAR(255) NULL,
--     ATTRIBUTE1  VARCHAR(255) NULL,
--     ATTRIBUTE2  VARCHAR(255) NULL,
--     ATTRIBUTE3  VARCHAR(255) NULL,
--     SOURCE      VARCHAR(100) NULL
-- )
--
-- As

--Inventory Table Build
-- !! Need to confirm timezones of each data that's landed to ensure proper time zone offset.
WITH RECURSIVE
---- /// Lightspeed /// ----
   -- Assigns row numbers to to get values over time and identify when more than value occurs in a day.
    ctelightspeedItem AS (Select ID,
                                 CUSTOM_SKU,
                                 MANUFACTURER_ID                                                as ManufacturerID,
                                 DEFAULT_VENDOR_ID                                              as VendorID,
                                 to_varchar(CATEGORY_ID) as CategoryID,
                                 DESCRIPTION,
                                 ATTRIBUTE_1,
                                 ATTRIBUTE_2,
                                 ATTRIBUTE_3,
                                 ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                          FROM FIVETRAN_DB.LIGHT_SPEED_RETAIL.ITEM_HISTORY)
---- /// YANDY /// ----
   -- Assigns row numbers to multiple inventory moves per day
   , cteyandycategoryjoin as (Select PROD_ID,
                                     concat(PTYPE, '_', N2) as CategoryID
                              from FIVETRAN_DB.POSTGRES_PUBLIC.PRODUCTS)
   , cteYandyProducts AS (select concat(to_varchar(po.PROD_ID), '/', to_varchar(po.PROD_OPTION_ID)) as UUID,
                                 PROD_ID,
                                 PROD_OPTION_ID,
                                 po.option_sku,
                                 OPTION_COLOR,
                                 OPTION_SIZE,
                                 OPTION_STYLE,
                                 SIZES_TABLE_ID
                          from FIVETRAN_DB.POSTGRES_PUBLIC.product_options po)
   , cteyandyProductSizes as (select SIZE_TYPE_ID, SIZE_TYPE, SIZE_NAME, SIZE_VALUE
                              from FIVETRAN_DB.POSTGRES_PUBLIC.SIZES_TABLE)
   , cteyandyinventorycombined as (Select uuid,
                                          cteYandyProducts.prod_id,
                                          prod_option_id,
                                          option_sku,
                                          option_color,
                                          option_size,
                                          option_style,
                                          sizes_table_id,
                                          categoryid,
                                          size_type_id,
                                          size_type,
                                          size_name,
                                          size_value
                                   from cteYandyProducts
                                            join cteyandycategoryjoin
                                                 on cteYandyProducts.PROD_ID = cteyandycategoryjoin.PROD_ID
                                            left outer join cteyandyProductSizes on SIZES_TABLE_ID = SIZE_TYPE_ID)


---//// WHERE IT ALL COMBINES ////---
-- the intent here is to take all the relevant cte's above and combine them through unions to make one conjoined inventory table.
-- This methodology will be followed for all tables.
-- Yandy Item's
Select NUll       as Product,
       OPTION_SKU as SKU,
       UUID       as UUID,
       null       as ManufacturerID,
       null       as VendorID,
       CategoryID as CategoryID,
       null       as Description,
       Null       as Color,
       SIZE_VALUE as Size,
       SIZE_NAME  as Attribute3,
       'Yandy'    as Source
from cteyandyinventorycombined

union all
-- Lightspeed Item's
Select null           as Product,
       CUSTOM_SKU     as SKU,
       null           as UUID,
       ManufacturerID as ManufacturerID,
       VendorID       as VendorID,
       CategoryID   as CategoryID,
       DESCRIPTION    as Description,
       Attribute_1    as Color,
       Attribute_2    as Size,
       Attribute_3    as Attribute3,
       'Lightspeed'   as Source
from ctelightspeedItem

