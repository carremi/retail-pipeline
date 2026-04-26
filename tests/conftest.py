"""Shared fixtures for the test suite."""
from datetime import date, datetime, timezone

import pandas as pd
import pytest


@pytest.fixture
def dirty_skus() -> pd.Series:
    """A series with all the typical SKU dirtiness we expect."""
    return pd.Series([
        "SHP-POL001-BLK-M",         # clean
        " SHP-POL001-BLK-M",        # leading space
        "SHP-POL001-BLK-M ",        # trailing space
        "SHP-POL001  BLK-M",        # internal double space
        "shp-pol001-blk-m",         # lowercase (we don't change case)
        "  SHP-POL001-BLK-M  ",     # both
        None,                        # null
        "",                          # empty -> NA
    ])


@pytest.fixture
def messy_money() -> pd.Series:
    """Numeric values in various dirty formats commonly seen in source files."""
    return pd.Series([
        "19.90",
        " 19.90 ",
        "$19.90",
        "1,299.90",   # thousand separator
        "USD 49.90",
        "  ",
        None,
        "abc",        # garbage
    ])


@pytest.fixture
def shopify_iso_dates() -> pd.Series:
    """ISO 8601 timestamps with timezone offsets, like Shopify returns."""
    return pd.Series([
        "2024-03-15T10:30:00-05:00",   # Lima time
        "2024-03-15T15:30:00+00:00",   # same instant in UTC
        "2024-12-31T23:59:59-05:00",
        None,
        "not a date",
    ])


@pytest.fixture
def lima_naive_dates() -> pd.Series:
    """Naive timestamps as a POS would store them (assumed Lima time)."""
    return pd.Series([
        "2024-03-15 10:30:00",
        "2024-03-15 23:59:59",
        None,
    ])


@pytest.fixture
def fx_table() -> pd.DataFrame:
    """A minimal fx_rates table covering March 2024."""
    return pd.DataFrame([
        {"currency": "USD", "rate_date": date(2024, 3, 15), "rate_to_usd": 1.0},
        {"currency": "PEN", "rate_date": date(2024, 3, 15), "rate_to_usd": 0.2667},
    ])


@pytest.fixture
def status_map() -> pd.DataFrame:
    """A minimal status_mapping covering shopify."""
    return pd.DataFrame([
        {"source_platform": "shopify", "raw_status": "paid",     "canonical_status": "paid"},
        {"source_platform": "shopify", "raw_status": "refunded", "canonical_status": "refunded"},
    ])


@pytest.fixture
def valid_sales_record() -> dict:
    """A clean staging row that should pass all validators."""
    return {
        "source_platform":   "shopify",
        "platform_order_id": "1000001",
        "platform_line_id":  "101",
        "order_dt_utc":      datetime(2024, 3, 15, 15, 30, tzinfo=timezone.utc),
        "platform_sku":      "SHP-POL001-BLK-M",
        "product_name":      "Polo Basico Negro M",
        "quantity":          2,
        "unit_price_local":  "19.90",
        "discount_local":    "0.00",
        "tax_local":         None,
        "line_total_local":  "39.80",
        "currency":          "USD",
        "unit_price_usd":    "19.90",
        "line_total_usd":    "39.80",
        "order_status":      "paid",
        "customer_email":    "ana@example.com",
        "raw_id":            42,
    }
