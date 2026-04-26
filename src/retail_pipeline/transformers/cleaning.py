"""Reusable cleaning functions for normalizers.

These are the building blocks the per-platform normalizers compose.
Each function is small, pure, and easily testable.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy.types import BigInteger, Integer, Numeric, String, TIMESTAMP


# ---------------------------------------------------------------------------
# SKU cleaning
# ---------------------------------------------------------------------------

def clean_sku(series: pd.Series) -> pd.Series:
    """Strip whitespace, collapse internal whitespace, uppercase if ambiguous.

    Strategy: trim outer space and collapse multiple spaces. We do NOT change
    case, because case can be meaningful in some platforms — case mismatches
    will be handled in the SKU mapper with a case-insensitive join.
    """
    cleaned = (
        series.astype("string")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    # Empty strings -> NA
    return cleaned.mask(cleaned == "", pd.NA)


# ---------------------------------------------------------------------------
# Numeric parsing (handles "1,299.90", "  19.90 ", "1.299,90", etc.)
# ---------------------------------------------------------------------------

def parse_money(series: pd.Series, decimal_sep: str = ".") -> pd.Series:
    """Parse strings/numbers into nullable Float64.

    Handles: thousand separators, surrounding whitespace, currency symbols,
    European decimal commas if ``decimal_sep=','``.
    """
    s = series.astype("string").str.strip()
    # Drop currency symbols and spaces inside the number
    s = s.str.replace(r"[^\d\.,\-]", "", regex=True)

    if decimal_sep == ",":
        # European: "1.299,90" -> "1299.90"
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    else:
        # US/Latin English: "1,299.90" -> "1299.90"
        s = s.str.replace(",", "", regex=False)

    return pd.to_numeric(s, errors="coerce").astype("Float64")


def parse_int(series: pd.Series) -> pd.Series:
    """Parse to nullable Int64 (preserves NA, supports negatives)."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def parse_iso_to_utc(series: pd.Series) -> pd.Series:
    """Parse ISO-8601 strings (with offset) and convert to UTC."""
    return pd.to_datetime(series, format="ISO8601", utc=True, errors="coerce")


def parse_naive_with_tz_to_utc(series: pd.Series, tz: str) -> pd.Series:
    """Parse naive datetimes (no offset), localize to ``tz``, convert to UTC."""
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.tz_localize(tz, ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")


def parse_dmy_with_tz_to_utc(series: pd.Series, tz: str) -> pd.Series:
    """Parse 'DD/MM/YYYY' (Tiendanube style), localize to ``tz``, convert to UTC."""
    parsed = pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")
    return parsed.dt.tz_localize(tz, ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")


# ---------------------------------------------------------------------------
# FX conversion
# ---------------------------------------------------------------------------

def attach_fx_rate(df: pd.DataFrame, fx_df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'rate_to_usd' column based on (currency, date_only).

    Falls back to the most recent rate available for that currency
    if the exact date is missing (weekends, holidays).
    """
    df = df.copy()
    df["rate_date"] = df["order_dt_utc"].dt.tz_convert("UTC").dt.date

    fx = fx_df.rename(columns={"rate_date": "rate_date_fx"})

    # Exact match first
    merged = df.merge(
        fx,
        left_on=["currency", "rate_date"],
        right_on=["currency", "rate_date_fx"],
        how="left",
    )

    # Backfill missing rates with most recent available per currency
    if merged["rate_to_usd"].isna().any():
        # Build a sorted lookup per currency
        fx_sorted = fx.sort_values(["currency", "rate_date_fx"])
        latest_per_currency = fx_sorted.groupby("currency").tail(1)[["currency", "rate_to_usd"]]
        latest_map = dict(zip(latest_per_currency["currency"], latest_per_currency["rate_to_usd"]))
        merged["rate_to_usd"] = merged["rate_to_usd"].fillna(merged["currency"].map(latest_map))

    merged = merged.drop(columns=["rate_date", "rate_date_fx"])
    return merged


def to_usd(local_amount: pd.Series, rate_to_usd: pd.Series) -> pd.Series:
    """Convert local amounts to USD using rate_to_usd (= USD per 1 unit local)."""
    return (local_amount * rate_to_usd).round(4)


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------

def map_status(df: pd.DataFrame, status_map_df: pd.DataFrame, platform: str) -> pd.Series:
    """Return canonical status using reference.status_mapping for ``platform``."""
    relevant = status_map_df[status_map_df["source_platform"] == platform]
    lookup = dict(zip(relevant["raw_status"], relevant["canonical_status"]))
    # Default to "unknown" if a status isn't in the mapping table
    return df["raw_status"].map(lookup).fillna("unknown")


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def dedupe_by_key(df: pd.DataFrame, key_cols: list[str], keep: str = "last") -> pd.DataFrame:
    """Drop duplicates on ``key_cols``. ``keep='last'`` keeps the freshest row."""
    before = len(df)
    df = df.drop_duplicates(subset=key_cols, keep=keep)
    after = len(df)
    if before != after:
        # Log via standard logging if a logger is configured upstream
        import logging
        logging.getLogger("transformer").info(
            f"Dropped {before - after} duplicates on keys {key_cols}"
        )
    return df


# ---------------------------------------------------------------------------
# Type coercion before DB write (defensive)
# ---------------------------------------------------------------------------

def coerce_dtypes_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """Force expected dtypes before to_sql to avoid VARCHAR-cast surprises.

    SQLAlchemy 2.0 inspects pandas dtypes and emits explicit casts. If a column
    is full of pd.NA with object dtype, it gets cast to VARCHAR, which clashes
    with a NUMERIC column on the DB side. Calling this just before to_sql
    fixes the inference.
    """
    df = df.copy()

    numeric_cols = [
        "unit_price_local",
        "discount_local",
        "tax_local",
        "line_total_local",
        "unit_price_usd",
        "line_total_usd",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Float64")

    int_cols = ["quantity", "raw_id"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    text_cols = [
        "source_platform", "platform_order_id", "platform_line_id",
        "platform_sku", "product_name", "currency", "order_status",
        "customer_email",
    ]
    for c in text_cols:
        if c in df.columns:
            df[c] = df[c].astype("string")

    return df


# ---------------------------------------------------------------------------
# Explicit SQL dtype map for to_sql writes into staging.*_sales tables
# ---------------------------------------------------------------------------

STAGING_DTYPE_MAP = {
    "source_platform":   String(),
    "platform_order_id": String(),
    "platform_line_id":  String(),
    "order_dt_utc":      TIMESTAMP(timezone=True),
    "platform_sku":      String(),
    "product_name":      String(),
    "quantity":          Integer(),
    "unit_price_local":  Numeric(12, 4),
    "discount_local":    Numeric(12, 4),
    "tax_local":         Numeric(12, 4),
    "line_total_local":  Numeric(12, 4),
    "currency":          String(3),
    "unit_price_usd":    Numeric(12, 4),
    "line_total_usd":    Numeric(12, 4),
    "order_status":      String(),
    "customer_email":    String(),
    "raw_id":            BigInteger(),
}
