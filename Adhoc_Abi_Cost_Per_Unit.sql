use warehouse COMPUTE_WH;
-- These two CTE's are views in the source system at yandy, the logic is replicated here for simplicity and the fact we don't have replication of those views.
WITH RECURSIVE
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

-- Cost per unit
select p.prod_id, p.brand_code || '-' || p.prod_sku, sum(x.we)/sum(x.q) as cost
from cte_prodinfo p
inner join FIVETRAN_DB.POSTGRES_PUBLIC.product_options po on po.prod_id=p.prod_id
inner join (select prod_option_id, sum(quantity_remaining*cost_price) we, sum(quantity_remaining) q
from FIVETRAN_DB.POSTGRES_PUBLIC.fifo_ledger where quantity_remaining > 0 group by prod_option_id) as x on x.prod_option_id=po.prod_option_id
where p.prod_active=true
group by p.prod_id, p.brand_code, p.prod_sku
