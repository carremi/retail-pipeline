"""Normalize raw.mercadolibre_orders into staging.mercadolibre_sales.

MercadoLibre quirks handled:
- Currency PEN -> needs FX conversion to USD
- Sale fee (commission) lives separately; we ignore it for gross sales
- Items nested under 'order_items' with the actual SKU at item.seller_sku
"""
from __future__ import annotations

import json

import pandas as pd

from retail_pipeline.transformers.base_normalizer import BaseNormalizer


class MercadoLibreNormalizer(BaseNormalizer):
    PLATFORM = "mercadolibre"
    RAW_TABLE = "raw.mercadolibre_orders"
    RAW_COLUMNS = "raw_id, order_id, payload, ingested_at"
    STAGING_TABLE = "staging.mercadolibre_sales"
    DATE_PARSER = "iso"

    def extract_fields(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        records = []
        for _, row in raw_df.iterrows():
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)

            order_id = str(payload["id"])
            created_at = payload["date_created"]
            currency = payload.get("currency_id", "PEN")
            status = payload.get("status")
            buyer = payload.get("buyer") or {}
            email = buyer.get("email")

            for idx, li in enumerate(payload.get("order_items", []), start=1):
                item = li.get("item") or {}
                qty = li.get("quantity", 0)
                unit = li.get("unit_price", 0) or 0
                line_total = round(unit * qty, 2)

                records.append({
                    "raw_id":            row["raw_id"],
                    "platform_order_id": order_id,
                    "platform_line_id":  f"{order_id}-{idx}",
                    "order_dt_raw":      created_at,
                    "platform_sku":      item.get("seller_sku"),
                    "product_name":      item.get("title"),
                    "quantity":          qty,
                    "unit_price_local":  unit,
                    "discount_local":    0,
                    "tax_local":         pd.NA,
                    "line_total_local":  line_total,
                    "currency":          currency,
                    "raw_status":        status,
                    "customer_email":    email,
                })
        return pd.DataFrame.from_records(records)


def run() -> int:
    return MercadoLibreNormalizer().run()


if __name__ == "__main__":
    run()
