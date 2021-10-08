use warehouse COMPUTE_WH;

create or replace view FIVETRAN_DB.prod.InventoryView as

---concat('Lightspeed', '/', ITEM_ID)  as UUID,
---concat('Yandy','/',to_varchar(PROD_ID), '/', to_varchar(PROD_OPTION_ID)) as UUID,

---testing to get it populate inventory for all dates
WITH RECURSIVE
---- /// Tableau View /// Assigns Previous date value for comparative join to date table.
--                        Allows inventory value to populate for applicable dates since last update in source.
    cteinventoryDateLead as (Select *,
                                    Lead(if.DATE) OVER (ORDER BY if.DATE desc) as PreviousRecordDate
                             from FIVETRAN_DB.prod.INVENTORY_FACT IF)
   , cteinventoryLeadDayAdd as (Select date,
                                       sku,
                                       ITEMUUID,
                                       qoh,
                                       backorder,
                                       cost,
                                       locationid,
                                       categoryid,
                                       source,
                                       previousrecorddate,
                                       dateadd(day, 1, PreviousRecordDate) as StartRecordDate
                                from cteinventoryDateLead)

---- /// Final Table for Tableau /// ----

Select DD.date,
       DD.year,
       DD.month,
       DD.month_name,
       DD.day_of_mon,
       DD.day_of_week,
       DD.week_of_year,
       DD.day_of_year,
       LDA.sku,
       LDA.itemuuid,
       LDA.qoh,
       LDA.backorder,
       LDA.cost,
       LDA.categoryid,
       LDA.source,
       ID.brandname,
       ID.description,
       ID.color,
       ID.size,
       ID.attribute3,
       LD.category as GeoRegion
from FIVETRAN_DB.prod.DATE_DIM DD
         join cteinventoryLeadDayAdd LDA
              on DD.DATE >= LDA.StartRecordDate
                  and DD.Date <= lda.DATE
         LEFT OUTER join FIVETRAN_DB.PROD.ITEM_DIM ID
              on lda.ITEMUUID = id.UUID
         LEFT OUTER join FIVETRAN_DB.prod.LOCATION_DIM LD
              on lda.LOCATIONID = ld.LOCATIONID

