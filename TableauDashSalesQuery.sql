use warehouse COMPUTE_WH;

create or replace view FIVETRAN_DB.prod.SalesView as

Select last_day(DD.Date, 'week') as WeekEnding,
       DD.year,
       DD.month,
       DD.month_name,
       DD.week_of_year,
       ORF.itemuuid,
       ORF.orderuuid,
       sum(ORF.qty_sold) as QtySold,
       sum(ORF.price) as SalesDollars,
       AVG(ORF.discount) as AvgPercentDiscount,
       ORF.source,
       ID.sku,
       ID.uuid,
       ID.brandname,
       ID.vendorid,
       ID.categoryid,
       ID.description,
       ID.color,
       ID.size,
       ID.attribute3
from FIVETRAN_DB.prod.DATE_DIM DD
         join FIVETRAN_DB.PROD.ORDER_FACT ORF
              on dd.DATE = orf.DATE
         left outer join FIVETRAN_DB.PROD.ITEM_DIM ID
                         on ORF.ITEMUUID = id.UUID
Group by DD.date, DD.year, DD.month, DD.month_name, DD.week_of_year, ORF.orderuuid, ORF.itemuuid, ID.uuid, ID.sku, ORF.locationid, ORF.source, ID.source, ID.brandname, ID.vendorid,
         ID.categoryid, ID.description, ID.color, ID.size, ID.attribute3
