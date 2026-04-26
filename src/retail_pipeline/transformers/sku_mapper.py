"""SKU mapper: enriches staging rows with canonical SKU, name and category.

Strategy:
- Read the master mapping into pandas, lower-case the platform_sku for matching
- Lower-case the staging.platform_sku for matching too (case-insensitive)
- Left-merge: matched rows get sku_canonical; unmatched stay NULL
- Unmatched rows are reported to ops.unmapped_skus for the commercial team
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from retail_pipeline.utils.db import get_engine
from retail_pipeline.utils.logging_setup import get_logger

log = get_logger("sku_mapper")


def load_mapping() -> pd.DataFrame:
    """Read reference.sku_mapping with normalized join keys."""
    engine = get_engine()
    df = pd.read_sql(
        """
        SELECT source_platform,
               platform_sku,
               sku_canonical,
               product_name AS canonical_product_name,
               category
        FROM reference.sku_mapping
        """,
        engine,
    )
    # Normalized join key: trim + lower
    df["join_key"] = df["platform_sku"].str.strip().str.lower()
    return df[["source_platform", "join_key", "sku_canonical",
               "canonical_product_name", "category"]]


def attach_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Add sku_canonical, canonical_product_name, category columns to a sales df.

    Expects df with columns: source_platform, platform_sku.
    """
    if df.empty:
        df = df.copy()
        for c in ["sku_canonical", "canonical_product_name", "category"]:
            df[c] = pd.NA
        return df

    mapping = load_mapping()

    df = df.copy()
    df["join_key"] = df["platform_sku"].str.strip().str.lower()

    merged = df.merge(
        mapping,
        on=["source_platform", "join_key"],
        how="left",
    )
    merged = merged.drop(columns=["join_key"])
    return merged


def report_unmapped(df: pd.DataFrame) -> int:
    """Detect rows with no canonical SKU and upsert them into ops.unmapped_skus.

    Expects df with columns: source_platform, platform_sku, sku_canonical.
    Returns the number of distinct (platform, sku) pairs unmapped.
    """
    unmapped = df[df["sku_canonical"].isna()].copy()
    if unmapped.empty:
        log.info("No unmapped SKUs detected.")
        return 0

    counts = (
        unmapped.groupby(["source_platform", "platform_sku"])
        .size()
        .reset_index(name="occurrences")
    )

    engine = get_engine()
    rows = counts.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO ops.unmapped_skus
                    (source_name, platform_sku, occurrences)
                VALUES (:source_platform, :platform_sku, :occurrences)
                ON CONFLICT (source_name, platform_sku) DO UPDATE SET
                    occurrences   = ops.unmapped_skus.occurrences + EXCLUDED.occurrences,
                    last_seen_at  = NOW()
            """),
            rows,
        )

    log.warning(
        f"Detected {len(counts)} unmapped (platform, sku) pairs covering "
        f"{len(unmapped)} rows. Logged to ops.unmapped_skus."
    )
    return len(counts)
