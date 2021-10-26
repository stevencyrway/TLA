use warehouse COMPUTE_WH;

Select
       ORF.itemuuid,
       sum(ORF.qty_sold) as QtySold,
       sum(ORF.price) as SalesDollars,
       AVG(ORF.discount) as AvgPercentDiscount,
       ORF.source
from FIVETRAN_DB.prod.DATE_DIM DD
         join FIVETRAN_DB.PROD.ORDER_FACT ORF
              on dd.DATE = orf.DATE
         left outer join FIVETRAN_DB.PROD.ITEM_DIM ID
                         on ORF.ITEMUUID = id.UUID
where orf.DISCOUNT > 0
Group by  ORF.itemuuid,  ORF.source



select source, ITEMUUID, AVGPERCENTDISCOUNT from FIVETRAN_DB.PROD.SALESVIEW
where SOURCE = 'Lightspeed'
and date
group by source, ITEMUUID, AVGPERCENTDISCOUNT


Select ORDER_HISTORY.id,
                                   ORDER_HISTORY.updated_time,
                                   ORDER_HISTORY.shop_id,
                                   ORDER_HISTORY.ordered_date,
                                   ORDER_HISTORY.received_date,
                                   ORDER_HISTORY.arrival_date,
                                   ORDER_HISTORY.ship_cost,
                                   (ORDER_HISTORY.discount * 100),
                                   (ORDER_HISTORY.total_discount),
                                   ORDER_HISTORY.total_quantity,
                                   ROW_NUMBER() OVER (PARTITION BY ORDER_HISTORY.ID ORDER BY ORDER_HISTORY.UPDATED_TIME desc) as OrderRowNumber
                            from FIVETRAN_DB.LIGHT_SPEED_RETAIL.ORDER_HISTORY
where TOTAL_DISCOUNT > 0