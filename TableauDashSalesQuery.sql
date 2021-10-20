use warehouse COMPUTE_WH;

Select count(ORDERUUID)
from FIVETRAN_DB.prod.DATE_DIM DD
         join FIVETRAN_DB.PROD.ORDER_FACT ORF
              on dd.DATE = orf.DATE
         left outer join FIVETRAN_DB.PROD.ITEM_DIM ID
              on ORF.ITEMUUID = id.UUID;

-- count of order items with item join, for some reason itemuuid is not joining well. some items in orders that aren't in products.
select count(distinct itemuuid), orf.source from fivetran_db.prod.ORDER_FACT ORF
-- join FIVETRAN_DB.PROD.ITEM_DI3M ID
-- on ORF.ITEMUUID = id.UUID
group by orf.SOURCE;


--count of items
select count(distinct UUID), source from FIVETRAN_DB.PROD.ITEM_DIM ID
group by id.SOURCE

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
                            from FIVETRAN_DB.LIGHT_SPEED_RETAIL.ORDER_HISTORY;

select orf.ITEMUUID, id.uuid, orf.source
from fivetran_db.prod.ORDER_FACT ORF
left outer join FIVETRAN_DB.PROD.ITEM_DIM ID
on ORF.ITEMUUID = id.UUID
where DATE >= '2018-01-02'
and id.UUID is null

select * from FIVETRAN_DB.PROD.ITEM_DIM
where UUID =  'Yandy/1/0'


Select * from FIVETRAN_DB.PROD.ORDER_FACT
where ITEMUUID = 'Lightspeed/10362'



count(item)
