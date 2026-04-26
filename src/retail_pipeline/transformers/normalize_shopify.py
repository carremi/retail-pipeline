"""Normalize raw.shopify_orders into staging.shopify_sales.

Shopify quirks handled:
- Nested line_items inside each order
- Prices as strings ("19.90")
- ISO 8601 timestamps with timezone
- Currency: USD (no FX conversion needed but we set USD columns anyway)
- Status: map financial_status -> canonical
"""
from __future__ import annotations

import json

import pandas as pd

from retail_pipeline.transformers.base_normalizer import BaseNormalizer


class ShopifyNormalizer(BaseNormalizer):
    PLATFORM = "shopify"
    RAW_TABLE = "raw.shopify_orders"
    RAW_COLUMNS = "raw_id, order_id, payload, ingested_at"
    STAGING_TABLE = "staging.shopify_sales"
    DATE_PARSER = "iso"

    def extract_fields(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Flatten Shopify orders: 1 row per line_item."""
        records = []
        for _, row in raw_df.iterrows():
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)

            order_id = str(payload["id"])
            created_at = payload["created_at"]
            currency = payload.get("currency", "USD")
            status = payload.get("financial_status")
            customer = payload.get("customer") or {}
            email = customer.get("email")

            for li in payload.get("line_items", []):
                records.append({
                    "raw_id":            row["raw_id"],
                    "platform_order_id": order_id,
                    "platform_line_id":  str(li["id"]),
                    "order_dt_raw":      created_at,
                    "platform_sku":      li.get("sku"),
                    "product_name":      li.get("title"),
                    "quantity":          li.get("quantity"),
                    "unit_price_local":  li.get("price"),
                    "discount_local":    li.get("total_discount"),
                    "line_total_local":  li.get("line_total"),
                    "tax_local":         None,  # Shopify reports tax at order-level only
                    "currency":          currency,
                    "raw_status":        status,
                    "customer_email":    email,
                })
        return pd.DataFrame.from_records(records)


def run() -> int:
    return ShopifyNormalizer().run()


if __name__ == "__main__":
    run()
