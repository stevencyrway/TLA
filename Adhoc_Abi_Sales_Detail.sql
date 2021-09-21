use warehouse COMPUTE_WH;
-- These two CTE's are views in the source system at yandy, the logic is replicated here for simplicity and the fact we don't have replication of those views.
WITH RECURSIVE cte_optionview as (
SELECT br.brand_name,
    br.brand_code,
    p.prod_id,
    p.prod_sku,
    po.option_sku,
    color.name AS color,
    size.name AS size,
    style.name AS style,
    po.option_inv,
    po.prod_option_id,
    pss.prod_price,
    po.la_upc,
    pss.prod_active,
    pss.prod_description,
    p.wholesale_price,
    color.variant_id AS option_color,
    size.variant_order,
    po.discontinued,
    po.la_sku,
    po.option_active,
    po.asin_size,
    po.asin_color,
    br.brand_id,
    po.asin_brand AS brand2,
    size.variant_id AS option_size,
    style.variant_id AS option_style,
    pss.prod_name,
    po.asin,
    po.option_wholesale,
    po.option_price,
    pss.discount_percent,
    pss.discount_group_id,
    po.wmarked,
    po.dropshippable,
    po.pleaser_inv,
    po.sizes_table_id,
    size.general_size,
    po.last_certify,
    po.wmarked_tmp,
    p.discontinued AS prod_discontinued,
    pss.date_added,
    po.offsite_inv,
    po.bin_capacity,
    po.is_club,
    po.accy,
    po.exclusive,
    po.ell_color_code,
    br.discount_percent AS brand_discount,
    po.bin_current,
    po.sale_speed,
    po.bo_schedule,
    po.on_way,
    pss.original_price,
    p.ptype,
    po.extra_upc,
    po.on_order,
    p.n2,
    po.wholesale_price2,
    po.catalog_price,
    po.catalog_discount,
    po.vendor_discontinued,
    po.closeout,
    po.spd_rank,
    po.fba_inventory,
    po.oos_until,
    po.inventory_status,
    ps.scs_id,
    po.inception
   FROM FIVETRAN_DB.POSTGRES_PUBLIC.product_options po
     JOIN FIVETRAN_DB.POSTGRES_PUBLIC.products p ON p.prod_id = po.prod_id
     LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.products_site_specific pss ON p.prod_id = pss.prod_id AND pss.site_id = 1
     LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.product_shopify ps ON ps.id = po.prod_option_id AND ps.type::text = 'variant'::text AND ps.site_id = 1
     JOIN FIVETRAN_DB.POSTGRES_PUBLIC.brands br ON br.brand_id = p.prod_brand
     LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.option_variants size ON size.variant_id = po.option_size
     LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.option_variants style ON style.variant_id = po.option_style
     LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.option_variants color ON color.variant_id = po.option_color
  ORDER BY color.name, size.variant_order)
-- Sales Details
Select ov.brand_name,
       ov.ptype,
       ov.brand_code || '-' || ov.prod_sku                   AS PRODUCT_SKU,
       ov.prod_id,
       month(to_date(to_timestamp(f.STAMPED))) as Month,
       year(to_date(to_timestamp(f.STAMPED))) as Year,
       SUM(f.quantity)                                       as total_units,
       SUM(f.quantity * f.price)                             as total_revenue,
       SUM(f.quantity * f.cost_price)                        as total_COGS,
       SUM(f.quantity * f.price - f.quantity * f.cost_price) as total_GP
from FIVETRAN_DB.POSTGRES_PUBLIC.fifo_transactions f
         INNER JOIN FIVETRAN_DB.POSTGRES_PUBLIC.orders o ON o.order_id = f.order_id
         INNER JOIN cte_optionview ov on ov.prod_option_id = f.prod_option_id
WHERE o.order_status IN (1, 2, 3)
  AND f.reason_code = 'SALE'
  AND to_timestamp(f.stamped) between '2021-01-01 00:00:00' AND '2021-01-31 23:59:59'
group by year, month, ov.brand_name, ov.ptype, PRODUCT_SKU, ov.prod_id;