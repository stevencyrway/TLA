use warehouse COMPUTE_WH;

Select weekending,
       year,
       month,
       month_name,
       week_of_year,
       itemuuid,
       orderuuid,
       qtysold,
       salesdollars,
       avgpercentdiscount,
       source,
       sku,
       uuid,
       brandname,
       vendorid,
       categoryid,
       description,
       color,
       size,
       attribute3
from FIVETRAN_DB.PROD.SALESVIEW
where YEAR >= 2020