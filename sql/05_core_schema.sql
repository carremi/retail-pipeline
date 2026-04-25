-- =============================================================
-- 05_core_schema.sql
-- Core layer: unified dimensional model for BI consumption.
-- Idempotent: safe to re-run.
-- =============================================================

-- -------------------------------------------------------------
-- Dimensions
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.dim_platform (
    platform_id      SMALLSERIAL PRIMARY KEY,
    platform_code    TEXT UNIQUE NOT NULL,
    platform_name    TEXT NOT NULL,
    platform_type    TEXT NOT NULL,            -- ecommerce | marketplace | pos
    is_active        BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO core.dim_platform (platform_code, platform_name, platform_type) VALUES
    ('shopify',      'Shopify',       'ecommerce'),
    ('mercadolibre', 'MercadoLibre',  'marketplace'),
    ('amazon',       'Amazon',        'marketplace'),
    ('tiendanube',   'Tiendanube',    'marketplace'),
    ('pos',          'Tienda Física', 'pos')
ON CONFLICT (platform_code) DO NOTHING;


CREATE TABLE IF NOT EXISTS core.dim_product (
    product_id       SERIAL PRIMARY KEY,
    sku_canonical    TEXT UNIQUE NOT NULL,
    product_name     TEXT NOT NULL,
    category         TEXT NOT NULL,
    base_price_usd   NUMERIC(12, 2) NOT NULL,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_dim_product_category ON core.dim_product(category);


-- dim_date: pre-built calendar table. Star schemas always have one.
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_id          DATE PRIMARY KEY,
    year             INTEGER NOT NULL,
    quarter          INTEGER NOT NULL,
    month            INTEGER NOT NULL,
    month_name       TEXT NOT NULL,
    day              INTEGER NOT NULL,
    day_of_week      INTEGER NOT NULL,         -- 1=Monday ... 7=Sunday
    day_name         TEXT NOT NULL,
    week_of_year     INTEGER NOT NULL,
    is_weekend       BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dim_date_year_month ON core.dim_date(year, month);


-- -------------------------------------------------------------
-- Fact table
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.fact_sales (
    sale_id              BIGSERIAL PRIMARY KEY,

    -- Foreign keys to dimensions
    platform_id          SMALLINT     NOT NULL REFERENCES core.dim_platform(platform_id),
    product_id           INTEGER      REFERENCES core.dim_product(product_id),  -- nullable: unmapped SKUs
    date_id              DATE         NOT NULL REFERENCES core.dim_date(date_id),

    -- Natural keys (for traceability)
    source_platform      TEXT         NOT NULL,
    platform_order_id    TEXT         NOT NULL,
    platform_line_id     TEXT         NOT NULL,
    platform_sku         TEXT,
    sku_canonical        TEXT,                                                  -- nullable: unmapped

    -- Time
    order_dt_utc         TIMESTAMPTZ  NOT NULL,

    -- Measures
    quantity             INTEGER      NOT NULL,
    unit_price_local     NUMERIC(12, 4),
    discount_local       NUMERIC(12, 4),
    tax_local            NUMERIC(12, 4),
    line_total_local     NUMERIC(12, 4),
    currency             VARCHAR(3),
    unit_price_usd       NUMERIC(12, 4),
    line_total_usd       NUMERIC(12, 4),

    -- Status & customer
    order_status         TEXT,
    customer_email       TEXT,

    -- Audit
    loaded_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    UNIQUE (source_platform, platform_order_id, platform_line_id)
);

CREATE INDEX IF NOT EXISTS ix_fact_date     ON core.fact_sales(date_id);
CREATE INDEX IF NOT EXISTS ix_fact_platform ON core.fact_sales(platform_id);
CREATE INDEX IF NOT EXISTS ix_fact_product  ON core.fact_sales(product_id);
CREATE INDEX IF NOT EXISTS ix_fact_status   ON core.fact_sales(order_status);
