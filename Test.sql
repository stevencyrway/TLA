use warehouse COMPUTE_WH;
Select date,
       year,
       month,
       month_name,
       day_of_mon,
       day_of_week,
       week_of_year,
       day_of_year,
       sku,
       itemuuid,
       qoh,
       backorder,
       cost,
       categoryid,
       source,
       brandname,
       description,
       color,
       size,
       attribute3,
       georegion
from FIVETRAN_DB.PROD.inventoryview
where DATE >= '2020-01-01'