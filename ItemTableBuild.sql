use warehouse COMPUTE_WH;
-- use database PROD_TLA_DW

-- --To create the table
-- CREATE OR REPLACE TABLE FIVETRAN_DB.PROD.ITEM_DIM
--     (SKU VARCHAR(100) NULL,
--      UUID VARCHAR(255) NOT NULL,
--      BRANDNAME VARCHAR(255) NULL,
--      VENDORID VARCHAR(255) NULL,
--      CATEGORYID VARCHAR(100) NULL,
--      DESCRIPTION VARCHAR(255) NULL,
--      COLOR VARCHAR(100) NULL,
--      SIZE VARCHAR(255) NULL,
--      ATTRIBUTE3 VARCHAR(255) NULL,
--      SOURCE VARCHAR(30) NULL
--         )
-- AS
--Inventory Table Build
-- !! Need to confirm timezones of each data that's landed to ensure proper time zone offset.
WITH RECURSIVE
---- /// Lightspeed /// ----
   -- Assigns row numbers to to get values over time and identify when more than value occurs in a day.
    ctelightspeedItem AS (Select ID,
                                 concat('Lightspeed', '/', to_varchar(ID))                      as UUID,
                                 CUSTOM_SKU,
                                 to_varchar(MANUFACTURER_ID)                                    as ManufacturerID,
                                 DEFAULT_VENDOR_ID                                              as VendorID,
                                 to_varchar(CATEGORY_ID)                                        as CategoryID,
                                 DESCRIPTION,
                                 ATTRIBUTE_1,
                                 ATTRIBUTE_2,
                                 ATTRIBUTE_3,
                                 ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as ItemRowNumber
                          FROM FIVETRAN_DB.LIGHT_SPEED_RETAIL.ITEM_HISTORY)
   , ctelightspeedvendor as (select id,
                                    updated_time,
                                    name                                                           as VendorName,
                                    ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as VendorRowNumber
                             from FIVETRAN_DB.LIGHT_SPEED_RETAIL.VENDOR_HISTORY)
   , ctelightspeedbrand as (select id,
                                   updated_time,
                                   name                                                           as BrandName,
                                   ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as BrandRowNumber
                            from FIVETRAN_DB.LIGHT_SPEED_RETAIL.MANUFACTURER_HISTORY)
   , ctelightspeedcombined as (Select ctelightspeedItem.id,
                                      uuid,
                                      custom_sku,
                                      manufacturerid,
                                      vendorid,
                                      categoryid,
                                      description,
                                      attribute_1,
                                      attribute_2,
                                      attribute_3,
                                      VendorName,
                                      BrandName,
                                      ItemRowNumber,
                                      vendorrownumber,
                                      BrandRowNumber
                               from ctelightspeedItem
                                        join ctelightspeedvendor on VendorID = ctelightspeedvendor.ID
                                        join ctelightspeedbrand on ManufacturerID = ctelightspeedbrand.ID
                                   and ctelightspeedvendor.VendorRowNumber = 1
                                   and ctelightspeedItem.ItemRowNumber = 1
                                   and ctelightspeedbrand.BrandRowNumber = 1
)
---- /// YANDY /// ----
   -- Assigns row numbers to multiple inventory moves per day
   , cteyandyproductname as (select PROD_ID, PROD_NAME
                             from FIVETRAN_DB.POSTGRES_PUBLIC.ORDERS_PRODS
                             group by PROD_ID, PROD_NAME)
   , cteyandybrands as (Select BRAND_CODE, BRAND_NAME, PROD_ID, PROD_SKU
                        from POSTGRES_PUBLIC.BRANDS
                                 join POSTGRES_PUBLIC.PRODUCTS on BRAND_ID = PROD_BRAND)
   , cteyandycategoryjoin as (Select PROD_ID,
                                     concat(PTYPE, '_', N2) as CategoryID
                              from FIVETRAN_DB.POSTGRES_PUBLIC.PRODUCTS)
   , cteYandyProducts AS (select concat('Yandy', '/', to_varchar(po.PROD_ID), '/',
                                        to_varchar(po.PROD_OPTION_ID)) as UUID,
                                 po.PROD_ID,
                                 PROD_OPTION_ID,
                                 po.option_sku,
                                 OPTION_COLOR,
                                 BRAND_NAME,
                                 BRAND_CODE,
                                 OPTION_SIZE,
                                 OPTION_STYLE,
                                 SIZES_TABLE_ID,
                                 PROD_SKU,
                                 PN.PROD_NAME                          as ProductName
                          from FIVETRAN_DB.POSTGRES_PUBLIC.product_options po
                                   join cteyandybrands B on po.PROD_ID = b.PROD_ID
                                   join cteyandyproductname PN on po.PROD_ID = PN.PROD_ID
                              and IS_AMAZON_PRODUCT is null)
   , cteyandyProductSizes as (select SIZE_TYPE_ID, SIZE_TYPE
                              from FIVETRAN_DB.POSTGRES_PUBLIC.SIZES_TABLE
                              group by SIZE_TYPE_ID, SIZE_TYPE)
   , cteyandyProductcolors as (select prod_color_id, color_name, prod_id
                               from POSTGRES_PUBLIC.PRODUCT_COLORS)
   , cteyandyinventorycombined as (Select uuid,
                                          cteYandyProducts.prod_id,
                                          prod_option_id,
                                          concat(PROD_SKU, '/', OPTION_SKU) as SKU,
                                          ProductName,
                                          BRAND_NAME,
                                          option_size,
                                          option_style,
                                          sizes_table_id,
                                          categoryid,
                                          COLOR_NAME,
                                          SIZE_TYPE
                                   from cteYandyProducts
                                            join cteyandycategoryjoin
                                                 on cteYandyProducts.PROD_ID = cteyandycategoryjoin.PROD_ID
                                            join cteyandyProductcolors
                                                 on cteYandyProducts.OPTION_COLOR = cteyandyProductcolors.PROD_COLOR_ID
                                            join cteyandyProductSizes on SIZES_TABLE_ID = SIZE_TYPE_ID)
---//// WHERE IT ALL COMBINES ////---
-- the intent here is to take all the relevant cte's above and combine them through unions to make one conjoined inventory table.
-- This methodology will be followed for all tables.
-- Yandy Item's

select count(po.PROD_ID),
       count(distinct po.PROD_ID),
       count(po.PROD_OPTION_ID),
       count(distinct po.PROD_OPTION_ID)
from FIVETRAN_DB.POSTGRES_PUBLIC.product_options po
         left outer join cteyandybrands B on po.PROD_ID = b.PROD_ID
         left outer join cteyandyproductname PN on po.PROD_ID = PN.PROD_ID
where IS_AMAZON_PRODUCT is null



Select * from FIVETRAN_DB.POSTGRES_PUBLIC.PRODUCTS_SITE_SPECIFIC

select * from fivetran_db.POSTGRES_PUBLIC.PRODUCT_OPTIONS



-- Select sku         as SKU,
--        UUID        as UUID,
--        BRAND_NAME  as BrandID,
--        null        as VendorID, --couldn't find any source in Yandy for this.
--        CategoryID  as CategoryID,
--        ProductName as Description,
--        COLOR_NAME  as Color,
--        SIZE_TYPE   as Size,
--        null        as Attribute3,
--        'Yandy'     as Source
-- from cteyandyinventorycombined

-- union all
--
-- -- Lightspeed Item's
-- Select CUSTOM_SKU   as SKU,
--        UUID         as UUID,
--        BrandName    as BrandID,
--        VendorName   as VendorID,
--        CategoryID   as CategoryID,
--        DESCRIPTION  as Description,
--        Attribute_1  as Color,
--        Attribute_2  as Size,
--        Attribute_3  as Attribute3,
--        'Lightspeed' as Source
-- from ctelightspeedcombined;