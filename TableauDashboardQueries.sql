use warehouse COMPUTE_WH;

---concat('Lightspeed', '/', ITEM_ID)  as UUID,
---concat('Yandy','/',to_varchar(PROD_ID), '/', to_varchar(PROD_OPTION_ID)) as UUID,

---testing to get it populate inventory for all dates
WITH RECURSIVE
---- /// Tableau View /// Assigns Previous date value for comparative join to date table.
--                        Allows inventory value to populate for applicable dates since last update in source.
cteinventoryDateLead as (Select *,
                                Lead(if.DATE) OVER (ORDER BY if.DATE desc) as PreviousRecordDate
                         from FIVETRAN_DB.prod.INVENTORY_FACT IF
                         where if.date >= '2021-09-01'
                           and if.ITEMUUID = 'Lightspeed/3862'
                            and LOCATIONID = '35')
        ,
cteinventoryLeadDayAdd as (Select date,
                                  sku,
                                  ITEMUUID,
                                  qoh,
                                  backorder,
                                  cost,
                                  locationid,
                                  categoryid,
                                  source,
                                  previousrecorddate,
                                  dateadd(day,1,PreviousRecordDate) as StartRecordDate
                           from cteinventoryDateLead)

---- /// Final Table for Tableau /// ----

Select *
from FIVETRAN_DB.prod.DATE_DIMENSION DD
        join cteinventoryLeadDayAdd LDA
                         on DD.DATE >= LDA.StartRecordDate
                        and DD.Date <= lda.DATE
        join FIVETRAN_DB.PROD.ITEM_DIM ID
        on lda.ITEMUUID = id.UUID;

