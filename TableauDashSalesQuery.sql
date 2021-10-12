use warehouse COMPUTE_WH;

With recursive Rowcount as (
Select count(ORDERUUID) from FIVETRAN_DB.PROD.ORDER_FACT)

select * from Rowcount;

select * from FIVETRAN_DB.PROD.ORDER_FACT
where DATE is null;

Select count(ORDERUUID)
from FIVETRAN_DB.prod.DATE_DIM DD
         join FIVETRAN_DB.PROD.ORDER_FACT ORF
              on dd.DATE = orf.DATE;
--          LEFT OUTER join FIVETRAN_DB.PROD.ITEM_DIM ID
--               on ORF.ITEMUUID = id.UUID


Select count(*)
from FIVETRAN_DB.prod.DATE_DIM DD
         join FIVETRAN_DB.PROD.ORDER_FACT ORF
              on dd.DATE = orf.DATE
         LEFT OUTER join FIVETRAN_DB.PROD.ITEM_DIM ID
              on ORF.ITEMUUID = id.UUID
--          LEFT OUTER join FIVETRAN_DB.prod.LOCATION_DIM LD
--               on ORF.LOCATIONID = ld.LOCATIONID



Select ORDER_HISTORY.id,
                                   ORDER_HISTORY.updated_time,
                                   ORDER_HISTORY.shop_id,
                                   ORDER_HISTORY.ordered_date,
                                   ORDER_HISTORY.received_date,
                                   ORDER_HISTORY.arrival_date,
                                   ORDER_HISTORY.ship_cost,
                                   ORDER_HISTORY.discount,
                                   ORDER_HISTORY.total_discount,
                                   ORDER_HISTORY.total_quantity,
                                   ROW_NUMBER() OVER (PARTITION BY ORDER_HISTORY.ID ORDER BY ORDER_HISTORY.UPDATED_TIME desc) as OrderRowNumber
                            from FIVETRAN_DB.LIGHT_SPEED_RETAIL.ORDER_HISTORY
where ORDERED_DATE is null


select count(*) from FIVETRAN_DB.LIGHT_SPEED_RETAIL.ORDER_HISTORY
where ORDERED_DATE is null

