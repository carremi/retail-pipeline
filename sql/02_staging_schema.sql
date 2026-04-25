-- =============================================================
-- 02_staging_schema.sql
-- Staging layer: cleaned, typed, but still platform-by-platform.
-- All tables share the SAME canonical schema.
-- Idempotent: safe to re-run.
-- =============================================================

CREATE TABLE IF NOT EXISTS staging.shopify_sales (
    staging_id          BIGSERIAL PRIMARY KEY,
    source_platform     TEXT        NOT NULL,
    platform_order_id   TEXT        NOT NULL,
    platform_line_id    TEXT        NOT NULL,
    order_dt_utc        TIMESTAMPTZ NOT NULL,
    platform_sku        TEXT,
    product_name        TEXT,
    quantity            INTEGER     NOT NULL,
    unit_price_local    NUMERIC(12, 4),
    discount_local      NUMERIC(12, 4),
    tax_local           NUMERIC(12, 4),
    line_total_local    NUMERIC(12, 4),
    currency            VARCHAR(3),
    unit_price_usd      NUMERIC(12, 4),
    line_total_usd      NUMERIC(12, 4),
    order_status        TEXT,
    customer_email      TEXT,
    raw_id              BIGINT,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_platform, platform_order_id, platform_line_id)
);
CREATE INDEX IF NOT EXISTS ix_stg_shopify_dt  ON staging.shopify_sales(order_dt_utc);
CREATE INDEX IF NOT EXISTS ix_stg_shopify_sku ON staging.shopify_sales(platform_sku);

CREATE TABLE IF NOT EXISTS staging.mercadolibre_sales (LIKE staging.shopify_sales INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.amazon_sales       (LIKE staging.shopify_sales INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.tiendanube_sales   (LIKE staging.shopify_sales INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.pos_sales          (LIKE staging.shopify_sales INCLUDING ALL);
