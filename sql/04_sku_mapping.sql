-- =============================================================
-- 04_sku_mapping.sql
-- Master SKU mapping: per-platform SKU -> canonical SKU.
-- =============================================================

CREATE TABLE IF NOT EXISTS reference.sku_mapping (
    source_platform     TEXT NOT NULL,
    platform_sku        TEXT NOT NULL,
    sku_canonical       TEXT NOT NULL,
    product_name        TEXT,
    category            TEXT,
    base_price_usd      NUMERIC(12, 2),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_platform, platform_sku)
);

CREATE INDEX IF NOT EXISTS ix_sku_map_canonical
    ON reference.sku_mapping(sku_canonical);

-- Optional: a "products" master table to dimension by canonical SKU.
CREATE TABLE IF NOT EXISTS reference.products (
    sku_canonical   TEXT PRIMARY KEY,
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,
    base_price_usd  NUMERIC(12, 2) NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
