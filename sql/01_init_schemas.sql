-- =============================================================
-- 01_init_schemas.sql
-- Create schemas and operational tables for the pipeline.
-- Idempotent: safe to run multiple times.
-- =============================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ops;

-- =============================================================
-- RAW layer: one table per source, stores original payloads
-- =============================================================

-- Shopify: full order JSON per row
CREATE TABLE IF NOT EXISTS raw.shopify_orders (
    raw_id          BIGSERIAL PRIMARY KEY,
    order_id        BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_file     TEXT
);
CREATE INDEX IF NOT EXISTS ix_raw_shopify_order_id ON raw.shopify_orders(order_id);
CREATE INDEX IF NOT EXISTS ix_raw_shopify_ingested ON raw.shopify_orders(ingested_at);

-- MercadoLibre
CREATE TABLE IF NOT EXISTS raw.mercadolibre_orders (
    raw_id          BIGSERIAL PRIMARY KEY,
    order_id        BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_file     TEXT
);
CREATE INDEX IF NOT EXISTS ix_raw_ml_order_id ON raw.mercadolibre_orders(order_id);

-- Amazon: one row per line item (TSV is row-oriented)
CREATE TABLE IF NOT EXISTS raw.amazon_shipments (
    raw_id              BIGSERIAL PRIMARY KEY,
    amazon_order_id     TEXT,
    merchant_order_id   TEXT,
    purchase_date       TEXT,             -- keep as text until staging
    last_updated_date   TEXT,
    order_status        TEXT,
    sku                 TEXT,
    product_name        TEXT,
    quantity_purchased  TEXT,             -- can be negative, keep as text
    currency            TEXT,
    item_price          TEXT,             -- may have thousand separators
    item_tax            TEXT,
    shipping_price      TEXT,
    buyer_email         TEXT,
    ship_city           TEXT,
    ship_state          TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_file         TEXT
);
CREATE INDEX IF NOT EXISTS ix_raw_amazon_order ON raw.amazon_shipments(amazon_order_id);

-- Tiendanube: Spanish columns kept verbatim
CREATE TABLE IF NOT EXISTS raw.tiendanube_ventas (
    raw_id          BIGSERIAL PRIMARY KEY,
    n_orden         TEXT,
    fecha           TEXT,
    estado_pago     TEXT,
    estado_envio    TEXT,
    cliente         TEXT,
    email           TEXT,
    sku             TEXT,
    producto        TEXT,
    cantidad        TEXT,
    precio_unit     TEXT,
    descuento       TEXT,
    subtotal        TEXT,
    moneda          TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_file     TEXT
);

-- POS: we'll read directly from pos_source.ventas and snapshot into raw.pos_ventas
CREATE TABLE IF NOT EXISTS raw.pos_ventas (
    raw_id          BIGSERIAL PRIMARY KEY,
    venta_id        BIGINT NOT NULL,
    fecha_venta     TIMESTAMP,
    tienda_id       INTEGER,
    cajero          TEXT,
    sku             TEXT,
    producto        TEXT,
    cantidad        INTEGER,
    precio_unit     NUMERIC(12, 2),
    descuento       NUMERIC(12, 2),
    total_linea     NUMERIC(12, 2),
    medio_pago      TEXT,
    cliente_doc     TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_raw_pos_venta ON raw.pos_ventas(venta_id);

-- =============================================================
-- OPS layer: pipeline state and quality tables
-- =============================================================

-- Checkpoints: remember the last watermark per source for incremental loads
CREATE TABLE IF NOT EXISTS ops.etl_checkpoints (
    source_name     TEXT PRIMARY KEY,
    last_watermark  TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rows_ingested   BIGINT NOT NULL DEFAULT 0
);

-- Rejected rows: anything that failed validation lands here with a reason
CREATE TABLE IF NOT EXISTS ops.rejected_rows (
    rejected_id     BIGSERIAL PRIMARY KEY,
    source_name     TEXT NOT NULL,
    raw_id          BIGINT,
    reason          TEXT NOT NULL,
    payload         JSONB,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_rejected_source ON ops.rejected_rows(source_name);

-- Unmapped SKUs: when a platform SKU doesn't match the master, log it here
CREATE TABLE IF NOT EXISTS ops.unmapped_skus (
    unmapped_id     BIGSERIAL PRIMARY KEY,
    source_name     TEXT NOT NULL,
    platform_sku    TEXT NOT NULL,
    occurrences     INTEGER NOT NULL DEFAULT 1,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved        BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (source_name, platform_sku)
);

-- Pipeline run log
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running',    -- running | success | failed
    sources_summary JSONB,
    error_message   TEXT
);
