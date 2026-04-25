"""Load core.fact_sales from staging + reference mappings.

Strategy:
- All work happens inside Postgres via a single INSERT...SELECT with ON CONFLICT.
- This is faster and more atomic than fetching to pandas.
- Handles unmapped SKUs by leaving product_id NULL (left join).
"""
from sqlalchemy import text

from retail_pipeline.utils.db import get_engine
from retail_pipeline.utils.logging_setup import get_logger

log = get_logger("loader.fact_sales")


UPSERT_SQL = """
WITH unioned AS (
    SELECT * FROM staging.shopify_sales
    UNION ALL SELECT * FROM staging.mercadolibre_sales
    UNION ALL SELECT * FROM staging.amazon_sales
    UNION ALL SELECT * FROM staging.tiendanube_sales
    UNION ALL SELECT * FROM staging.pos_sales
),
enriched AS (
    SELECT
        u.source_platform,
        u.platform_order_id,
        u.platform_line_id,
        u.platform_sku,
        u.order_dt_utc,
        u.quantity,
        u.unit_price_local,
        u.discount_local,
        u.tax_local,
        u.line_total_local,
        u.currency,
        u.unit_price_usd,
        u.line_total_usd,
        u.order_status,
        u.customer_email,

        dp.platform_id,
        m.sku_canonical,
        prod.product_id
    FROM unioned u
    JOIN core.dim_platform dp
      ON dp.platform_code = u.source_platform
    LEFT JOIN reference.sku_mapping m
      ON  m.source_platform = u.source_platform
      AND lower(trim(m.platform_sku)) = lower(trim(u.platform_sku))
    LEFT JOIN core.dim_product prod
      ON prod.sku_canonical = m.sku_canonical
)
INSERT INTO core.fact_sales (
    platform_id, product_id, date_id,
    source_platform, platform_order_id, platform_line_id,
    platform_sku, sku_canonical,
    order_dt_utc, quantity,
    unit_price_local, discount_local, tax_local, line_total_local,
    currency, unit_price_usd, line_total_usd,
    order_status, customer_email
)
SELECT
    e.platform_id,
    e.product_id,
    (e.order_dt_utc AT TIME ZONE 'UTC')::date AS date_id,
    e.source_platform,
    e.platform_order_id,
    e.platform_line_id,
    e.platform_sku,
    e.sku_canonical,
    e.order_dt_utc,
    e.quantity,
    e.unit_price_local,
    e.discount_local,
    e.tax_local,
    e.line_total_local,
    e.currency,
    e.unit_price_usd,
    e.line_total_usd,
    e.order_status,
    e.customer_email
FROM enriched e
ON CONFLICT (source_platform, platform_order_id, platform_line_id) DO UPDATE SET
    platform_id      = EXCLUDED.platform_id,
    product_id       = EXCLUDED.product_id,
    date_id          = EXCLUDED.date_id,
    platform_sku     = EXCLUDED.platform_sku,
    sku_canonical    = EXCLUDED.sku_canonical,
    order_dt_utc     = EXCLUDED.order_dt_utc,
    quantity         = EXCLUDED.quantity,
    unit_price_local = EXCLUDED.unit_price_local,
    discount_local   = EXCLUDED.discount_local,
    tax_local        = EXCLUDED.tax_local,
    line_total_local = EXCLUDED.line_total_local,
    currency         = EXCLUDED.currency,
    unit_price_usd   = EXCLUDED.unit_price_usd,
    line_total_usd   = EXCLUDED.line_total_usd,
    order_status     = EXCLUDED.order_status,
    customer_email   = EXCLUDED.customer_email,
    loaded_at        = NOW()
"""


def load() -> int:
    engine = get_engine()
    log.info("Loading core.fact_sales from staging + mappings...")

    with engine.begin() as conn:
        result = conn.execute(text(UPSERT_SQL))
        n = result.rowcount

    log.info(f"[OK] fact_sales: {n} rows upserted")
    return n


if __name__ == "__main__":
    load()
