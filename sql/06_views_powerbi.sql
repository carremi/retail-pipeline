-- =============================================================
-- 06_views_powerbi.sql
-- Wide views for Power BI / Tableau / Looker.
-- =============================================================

CREATE OR REPLACE VIEW core.v_sales_wide AS
SELECT
    f.sale_id,
    f.order_dt_utc,
    f.date_id,
    d.year, d.quarter, d.month, d.month_name, d.day_name, d.is_weekend,

    p.platform_code,
    p.platform_name,
    p.platform_type,

    f.sku_canonical,
    pr.product_name,
    pr.category,

    f.platform_order_id,
    f.platform_line_id,
    f.platform_sku,

    f.quantity,
    f.unit_price_local,
    f.discount_local,
    f.tax_local,
    f.line_total_local,
    f.currency,
    f.unit_price_usd,
    f.line_total_usd,

    f.order_status,
    f.customer_email,
    f.loaded_at
FROM core.fact_sales f
JOIN core.dim_platform p USING (platform_id)
JOIN core.dim_date     d USING (date_id)
LEFT JOIN core.dim_product pr USING (product_id);


-- Convenience view: revenue by month + platform + category
CREATE OR REPLACE VIEW core.v_revenue_monthly AS
SELECT
    d.year,
    d.month,
    d.month_name,
    p.platform_name,
    COALESCE(pr.category, 'Sin categoría') AS category,
    COUNT(*)                                 AS line_items,
    SUM(f.quantity)                          AS units,
    ROUND(SUM(f.line_total_usd)::numeric, 2) AS revenue_usd
FROM core.fact_sales f
JOIN core.dim_platform p USING (platform_id)
JOIN core.dim_date     d USING (date_id)
LEFT JOIN core.dim_product pr USING (product_id)
WHERE f.order_status = 'paid'
GROUP BY 1, 2, 3, 4, 5;
