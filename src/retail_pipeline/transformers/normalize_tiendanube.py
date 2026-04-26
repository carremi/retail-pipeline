"""Normalize raw.tiendanube_ventas into staging.tiendanube_sales.

Tiendanube quirks handled:
- Date arrives as 'DD/MM/YYYY' string in Lima local time, no time component
- Currency PEN -> needs FX conversion
- Spanish status values mapped via reference.status_mapping
- One row per item (already line-level in raw, no explosion needed)
- No native line_id -> synthesize from n_orden + sku + row position
"""
from __future__ import annotations

import pandas as pd

from retail_pipeline.transformers.base_normalizer import BaseNormalizer


class TiendanubeNormalizer(BaseNormalizer):
    PLATFORM = "tiendanube"
    RAW_TABLE = "raw.tiendanube_ventas"
    RAW_COLUMNS = (
        "raw_id, n_orden, fecha, estado_pago, estado_envio, "
        "cliente, email, sku, producto, cantidad, precio_unit, "
        "descuento, subtotal, moneda"
    )
    STAGING_TABLE = "staging.tiendanube_sales"
    DATE_PARSER = "dmy"
    DATE_TZ = "America/Lima"

    def extract_fields(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        df = raw_df.copy()

        # Synthesize line_id
        df["line_pos"] = df.groupby("n_orden").cumcount() + 1
        df["platform_line_id"] = (
            df["n_orden"].astype("string") + "-" + df["line_pos"].astype("string")
        )

        df["platform_order_id"] = df["n_orden"].astype("string")
        df["platform_sku"] = df["sku"]
        df["product_name"] = df["producto"].astype("string")
        df["quantity"] = df["cantidad"]
        df["unit_price_local"] = df["precio_unit"]
        df["discount_local"] = df["descuento"]
        df["line_total_local"] = df["subtotal"]
        df["tax_local"] = pd.NA
        df["currency"] = df["moneda"].fillna("PEN").astype("string")
        df["customer_email"] = df["email"].astype("string")
        df["raw_status"] = df["estado_pago"]
        df["order_dt_raw"] = df["fecha"]

        return df


def run() -> int:
    return TiendanubeNormalizer().run()


if __name__ == "__main__":
    run()
