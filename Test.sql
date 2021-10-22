use warehouse COMPUTE_WH;

Select o.ORDER_ID,
       op.ORDERS_PRODS_ID,
       o.ORDER_DATE,
       op.BACKORDER_DATE,
       op.PROD_ID,
       op.OPTION_ID,
       op.quantity,
       op.discount_percent,
       op.tax,
       op.total_prod_price,
       o.order_status,
       case
           when op.SITE_ID = 1 then 'Yandy'
           when op.SITE_ID = 2 then 'Playboy'
       end as order_site,
       op.RETURNED
from FIVETRAN_DB.POSTGRES_PUBLIC.ORDERS O
         join FIVETRAN_DB.POSTGRES_PUBLIC.ORDERS_PRODS OP
              on O.ORDER_ID = OP.ORDERS_PRODS_ID
where O.ORDER_DATE >= '2020-01-01'
  and o.ORDER_STATUS in (1, 2, 3); --this filters out all orders that are active and not cancelled


-- Friday, October 22, 2021
-- 11:02 AM
--
-- Order status, are the ones for items solds
-- 1
-- 2
-- 3
--
-- Status 4 is  cancelled
-- status
--
-- Innerjoin orders prods
--
-- Orders_prods_prodid
--
-- Isreturned = false
--
--
-- Siteid = 2  is playboy or pleasure for all
-- Site id 1 = 1 yandy
--
-- Print active = true
--
--
-- Bo_schedule in product options will show amount on backorder
--
--
-- Print active = false is backorder
--
-- Onway column
--
-- Information on back orders, order sheets shows backorder info or purchase orders.