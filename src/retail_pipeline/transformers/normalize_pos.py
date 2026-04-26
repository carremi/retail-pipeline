"""Normalize raw.pos_ventas into staging.pos_sales.

POS quirks handled:
- Timestamps stored as TIMESTAMP (no tz) -> treat as America/Lima -> UTC
- Currency is PEN
- venta_id is unique per transaction line, so it IS the line_id
- Payment method maps to canonical 'paid' status
"""
from __future__ import annotations

import pandas as pd

from retail_pipeline.transformers.base_normalizer import BaseNormalizer


class PosNormalizer(BaseNormalizer):
    PLATFORM = "pos"
    RAW_TABLE = "raw.pos_ventas"
    RAW_COLUMNS = (
        "raw_id, venta_id, fecha_venta, tienda_id, cajero, "
        "sku, producto, cantidad, precio_unit, descuento, "
        "total_linea, medio_pago, cliente_doc"
    )
    STAGING_TABLE = "staging.pos_sales"
    DATE_PARSER = "naive"
    DATE_TZ = "America/Lima"

    def extract_fields(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        df = raw_df.copy()

        # POS doesn't have a separate "order"; each venta_id is a single line.
        df["platform_order_id"] = df["venta_id"].astype("string")
        df["platform_line_id"] = df["venta_id"].astype("string")
        df["platform_sku"] = df["sku"]
        df["product_name"] = df["producto"].astype("string")
        df["quantity"] = df["cantidad"]
        df["unit_price_local"] = df["precio_unit"]
        df["discount_local"] = df["descuento"]
        df["line_total_local"] = df["total_linea"]
        df["tax_local"] = pd.NA
        df["currency"] = "PEN"
        df["customer_email"] = pd.NA  # POS doesn't capture email
        df["raw_status"] = df["medio_pago"].astype("string")
        df["order_dt_raw"] = df["fecha_venta"]

        return df


def run() -> int:
    return PosNormalizer().run()


if __name__ == "__main__":
    run()
