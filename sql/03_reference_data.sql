-- =============================================================
-- 03_reference_data.sql
-- Reference tables: FX rates, status mapping, etc.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS reference;

-- FX rates: one row per (currency, date)
CREATE TABLE IF NOT EXISTS reference.fx_rates (
    currency     VARCHAR(3) NOT NULL,
    rate_date    DATE       NOT NULL,
    rate_to_usd  NUMERIC(12, 6) NOT NULL,
    PRIMARY KEY (currency, rate_date)
);

-- Status mapping: each platform's status -> our canonical status
CREATE TABLE IF NOT EXISTS reference.status_mapping (
    source_platform  TEXT NOT NULL,
    raw_status       TEXT NOT NULL,
    canonical_status TEXT NOT NULL,
    PRIMARY KEY (source_platform, raw_status)
);

-- Seed canonical statuses we'll use:
-- paid | cancelled | refunded | partially_refunded | pending
INSERT INTO reference.status_mapping (source_platform, raw_status, canonical_status) VALUES
    ('shopify',      'paid',                  'paid'),
    ('shopify',      'pending',               'pending'),
    ('shopify',      'refunded',              'refunded'),
    ('shopify',      'partially_refunded',    'partially_refunded'),
    ('mercadolibre', 'paid',                  'paid'),
    ('mercadolibre', 'cancelled',             'cancelled'),
    ('mercadolibre', 'refunded',              'refunded'),
    ('amazon',       'Shipped',               'paid'),
    ('amazon',       'Pending',               'pending'),
    ('amazon',       'Cancelled',             'cancelled'),
    ('amazon',       'Refunded',              'refunded'),
    ('tiendanube',   'Pagado',                'paid'),
    ('tiendanube',   'Pendiente de pago',     'pending'),
    ('tiendanube',   'Cancelado',             'cancelled'),
    ('tiendanube',   'Reembolsado',           'refunded'),
    ('pos',          'EFECTIVO',              'paid'),
    ('pos',          'TARJETA',               'paid'),
    ('pos',          'YAPE',                  'paid'),
    ('pos',          'PLIN',                  'paid')
ON CONFLICT DO NOTHING;
