use warehouse COMPUTE_WH

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
  ORDER BY color.name, size.variant_order) ,
     cte_prodinfo as (SELECT br.brand_code,
    p.prod_sku,
    pi.image,
    pi.thumbnail,
    pss.prod_name,
    pss.prod_active,
    p.prod_id,
    p.prod_brand,
    pss.prod_description,
    p.wholesale_price,
    pss.prod_price,
    pss.original_price,
    pss.show_adjust,
    pss.discount_percent,
    pss.discount_group_id,
    u.url,
    p.made_in_usa,
    pss.prod_new,
    pss.prod_coming_soon,
    pi.hi_rez,
    p.product_set,
    v2.name,
    p.in_stock,
    pss.meta_title,
    pss.notify_off,
    p.exclusive,
    p.limited_edition,
    pss.plus_size,
    p.sexy_rating,
    pss.meta_keywords,
    p.size_chart_id,
    pi.option_color,
    p.pla_filter,
    p.discontinued,
    pss.date_added,
    pss.intro_text,
    br.brand_name,
    p.ptype,
    p.n2,
    pi.has_logo,
    pss.prod_fabric,
    p.google_feed,
    pss.meta_description,
    pss.secret_keywords,
    p.usp,
    p.sales_rank,
    pss.prod_pending
   FROM FIVETRAN_DB.POSTGRES_PUBLIC.products p
     JOIN FIVETRAN_DB.POSTGRES_PUBLIC.brands br ON br.brand_id = p.prod_brand
     LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.product_images pi ON pi.prod_id = p.prod_id AND pi.main_image = true AND pi.site_id = 1
     LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.urls u ON u.associated_id = p.prod_id AND u.type = 1 AND u.site_id = 1
     LEFT JOIN FIVETRAN_DB.POSTGRES_PUBLIC.option_variants v2 ON v2.variant_id = pi.option_color
     JOIN FIVETRAN_DB.POSTGRES_PUBLIC.products_site_specific pss ON pss.prod_id = p.prod_id AND pss.site_id = 1)

-- Sales Summary
Select ov.ptype,
       SUM(f.quantity)                                       as total_units,
       SUM(f.quantity * f.price)                             as total_revenue,
       SUM(f.quantity * f.cost_price)                        as total_COGS,
       SUM(f.quantity * f.price - f.quantity * f.cost_price) as total_GP,
       month(to_date(to_timestamp(f.STAMPED))) as Month,
       year(to_date(to_timestamp(f.STAMPED))) as Year
from FIVETRAN_DB.POSTGRES_PUBLIC.fifo_transactions f
         INNER JOIN FIVETRAN_DB.POSTGRES_PUBLIC.orders o ON o.order_id = f.order_id
         INNER JOIN cte_optionview ov on ov.prod_option_id = f.prod_option_id
WHERE o.order_status IN (1, 2, 3)
  AND f.reason_code = 'SALE'
  AND to_timestamp(f.stamped) between '2020-01-01 00:00:00' AND '2021-01-31 23:59:59'
group by ov.ptype, year, month;

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

-- -- Cost per unit
select p.prod_id, p.brand_code || '-' || p.prod_sku, sum(x.we)/sum(x.q) as cost
from cte_prodinfo p
inner join FIVETRAN_DB.POSTGRES_PUBLIC.product_options po on po.prod_id=p.prod_id
inner join (select prod_option_id, sum(quantity_remaining*cost_price) we, sum(quantity_remaining) q
from FIVETRAN_DB.POSTGRES_PUBLIC.fifo_ledger where quantity_remaining > 0 group by prod_option_id) as x on x.prod_option_id=po.prod_option_id
where p.prod_active=true
group by p.prod_id, p.brand_code, p.prod_sku




