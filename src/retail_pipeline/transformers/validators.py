"""Pydantic validation models and a runner over staging tables.

Strategy:
- Define a strict schema with business rules (prices, quantities, dates, etc.)
- Validate every row from staging.* before it enters core.fact_sales
- Failed rows are inserted into ops.rejected_rows with the error message
- Valid rows continue downstream untouched
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import text

from retail_pipeline.utils.db import get_engine
from retail_pipeline.utils.logging_setup import get_logger

log = get_logger("validators")

VALID_CURRENCIES = {"USD", "PEN"}
VALID_STATUSES = {
    "paid", "pending", "cancelled", "refunded",
    "partially_refunded", "unknown",
}




def _to_json_safe(record: dict) -> str:
    """Serialize a pandas record to JSON, replacing NaN/NaT/numpy types with None.

    Standard json.dumps emits NaN/Infinity tokens, which Postgres' JSONB rejects.
    Pandas also returns numpy scalars and Timestamps that need string coercion.
    """
    import math
    safe: dict = {}
    for k, v in record.items():
        if v is None:
            safe[k] = None
        elif isinstance(v, float) and math.isnan(v):
            safe[k] = None
        elif pd.isna(v):  # catches pd.NA, pd.NaT
            safe[k] = None
        else:
            safe[k] = v
    return json.dumps(safe, default=str)

class SalesLineItem(BaseModel):
    """Validation contract for a single staging sales row."""

    model_config = ConfigDict(strict=False, str_strip_whitespace=True)

    source_platform:   str
    platform_order_id: str
    platform_line_id:  str
    order_dt_utc:      datetime
    platform_sku:      Optional[str] = None
    product_name:      Optional[str] = None
    quantity:          int
    unit_price_local:  Optional[Decimal] = None
    discount_local:    Optional[Decimal] = None
    tax_local:         Optional[Decimal] = None
    line_total_local:  Optional[Decimal] = None
    currency:          str = Field(min_length=3, max_length=3)
    unit_price_usd:    Optional[Decimal] = None
    line_total_usd:    Optional[Decimal] = None
    order_status:      str
    customer_email:    Optional[str] = None
    raw_id:            Optional[int] = None

    # ----- Business rules -----

    @field_validator("currency")
    @classmethod
    def currency_in_whitelist(cls, v: str) -> str:
        v = v.upper()
        if v not in VALID_CURRENCIES:
            raise ValueError(f"unsupported currency '{v}'")
        return v

    @field_validator("order_status")
    @classmethod
    def status_in_whitelist(cls, v: str) -> str:
        if v not in VALID_STATUSES:
            raise ValueError(f"unknown order_status '{v}'")
        return v

    @field_validator("quantity")
    @classmethod
    def quantity_not_zero(cls, v: int) -> int:
        if v == 0:
            raise ValueError("quantity cannot be zero")
        return v

    @field_validator("unit_price_local", "unit_price_usd")
    @classmethod
    def unit_price_non_negative(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        if v is not None and v < 0:
            raise ValueError(f"unit price must be non-negative, got {v}")
        return v

    @field_validator("order_dt_utc")
    @classmethod
    def order_dt_not_in_future(cls, v: datetime) -> datetime:
        # Allow up to 1 day in the future to tolerate small clock skew.
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        if v > now + timedelta(days=1):
            raise ValueError(f"order_dt_utc {v.isoformat()} is in the future")
        return v


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

STAGING_TABLES = [
    "staging.shopify_sales",
    "staging.mercadolibre_sales",
    "staging.amazon_sales",
    "staging.tiendanube_sales",
    "staging.pos_sales",
]


def validate_all() -> dict[str, int]:
    """Validate every row in every staging table.

    Returns a dict with counts: total / valid / rejected.
    Rejected rows are inserted into ops.rejected_rows with their error.
    """
    engine = get_engine()
    total, valid_count, rejected = 0, 0, []

    for tbl in STAGING_TABLES:
        df = pd.read_sql(f"SELECT * FROM {tbl}", engine)
        log.info(f"Validating {tbl}: {len(df)} rows")
        total += len(df)

        for record in df.to_dict(orient="records"):
            try:
                SalesLineItem(**record)
                valid_count += 1
            except ValidationError as e:
                # Pull a compact reason: first error's location + message
                first_err = e.errors()[0]
                reason = f"{'.'.join(str(p) for p in first_err['loc'])}: {first_err['msg']}"
                rejected.append({
                    "source_name": record.get("source_platform", "unknown"),
                    "raw_id":      record.get("raw_id"),
                    "reason":      reason[:500],
                    "payload":     _to_json_safe(record),
                })

    if rejected:
        log.warning(f"Inserting {len(rejected)} rejected rows into ops.rejected_rows")
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO ops.rejected_rows
                        (source_name, raw_id, reason, payload)
                    VALUES (:source_name, :raw_id, :reason, CAST(:payload AS JSONB))
                """),
                rejected,
            )
    else:
        log.info("No validation errors detected.")

    summary = {"total": total, "valid": valid_count, "rejected": len(rejected)}
    log.info(f"Validation summary: {summary}")
    return summary


if __name__ == "__main__":
    validate_all()
