use warehouse COMPUTE_WH;


select id, FULL_PATH_NAME, NODE_DEPTH,
                            SPLIT_PART(TRIM(ctecategory.NAME, ' $.'), '/', 1) as Category1,
                            TRIM(SPLIT_PART(ctecategory.NAME, '/', 2), ' $.') as Category2,
                            TRIM(SPLIT_PART(ctecategory.NAME, '/', 3), ' $.') as Category3,
                            TRIM(SPLIT_PART(ctecategory.NAME, '/', 4), ' $.') as Category4,
                            TRIM(SPLIT_PART(ctecategory.NAME, '/', 5), ' $.') as Category5,
                            ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                     from LIGHT_SPEED_RETAIL.CATEGORY_HISTORY ctecategory
-- where NODE_DEPTH = 0
                     group by ID, UPDATED_TIME, NAME, FULL_PATH_NAME, NODE_DEPTH;


Select distinct(CATEGORY_ID), FULL_PATH_NAME, NODE_DEPTH,
                            SPLIT_PART(TRIM(ctecategory.NAME, ' $.'), '/', 1) as Category1,
                            TRIM(SPLIT_PART(ctecategory.NAME, '/', 2), ' $.') as Category2,
                            TRIM(SPLIT_PART(ctecategory.NAME, '/', 3), ' $.') as Category3,
                            TRIM(SPLIT_PART(ctecategory.NAME, '/', 4), ' $.') as Category4,
                            TRIM(SPLIT_PART(ctecategory.NAME, '/', 5), ' $.') as Category5
                           FROM LIGHT_SPEED_RETAIL.ITEM_HISTORY
join LIGHT_SPEED_RETAIL.CATEGORY_HISTORY ctecategory
on ITEM_HISTORY.CATEGORY_ID = ctecategory.ID;




Select distinct(PTYPE), n2 from POSTGRES_PUBLIC.PRODUCTS;

Select distinct(PRODUCT_TYPE)
 from LOVERS_SHOPIFY.PRODUCT as P
                             join LOVERS_SHOPIFY.PRODUCT_VARIANT as PV
                                  on P.ID = PV.PRODUCT_ID
                    where STATUS = 'active'
                    and LENGTH(TRIM(SKU)) > 0


Select * from FIVETRAN_DB.LOVERS_SHOPIFY.PRODUCT_TAG