use warehouse COMPUTE_WH;
-- use database PROD_TLA_DW

--To create the table
CREATE OR REPLACE TABLE FIVETRAN_DB.PROD.LOCATION_DIM
    (
     LocationID VARCHAR(100) NULL,
     LocationName VARCHAR(255) NOT NULL,
     Category VARCHAR(255) NULL,
     SOURCE VARCHAR(30) NULL
        )
AS
    --Inventory Table Build
-- !! Need to confirm timezones of each data that's landed to ensure proper time zone offset.
WITH RECURSIVE
---- /// Lightspeed /// ----
   -- Assigns row numbers to to get values over time and identify when more than value occurs in a day.
    ctelightspeedlocation AS (select id,
                                 updated_time,
                                 name,
                                 time_zone,
                                 ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_TIME DESC) as RowNumber
                          FROM FIVETRAN_DB.LIGHT_SPEED_RETAIL.SHOP_HISTORY)
---- /// YANDY /// ----
   -- I haven't found a location table that has data in Yandy.
--    , cteyandybrands as ()
--    , cteyandyinventorycombined as ()


---//// WHERE IT ALL COMBINES ////---
-- the intent here is to take all the relevant cte's above and combine them through unions to make one conjoined inventory table.
-- This methodology will be followed for all tables.
-- Yandy Item's
Select null    as LocationID,
       null    as LocationName,
       null    as Timezone,
       'Yandy' as Source
from cteyandyinventorycombined

union all

-- Lightspeed Item's
Select ID           as LocationID,
       NAME         as LocationName,
       TIME_ZONE    as Timezone,
       'Lightspeed' as Source
from ctelightspeedlocation;




