"""Normalize raw.amazon_shipments into staging.amazon_sales.

Amazon quirks handled:
- All raw columns arrive as TEXT (preserved verbatim from TSV)
- quantity_purchased may be negative (return) -> keep sign
- item_price may have thousand separators ("1,299.90")
- No native line_id -> synthesize from amazon_order_id + sku + position
- Currency is USD; tax is reported separately
"""
from __future__ import annotations

import pandas as pd

from retail_pipeline.transformers import cleaning as C
from retail_pipeline.transformers.base_normalizer import BaseNormalizer


class AmazonNormalizer(BaseNormalizer):
    PLATFORM = "amazon"
    RAW_TABLE = "raw.amazon_shipments"
    RAW_COLUMNS = (
        "raw_id, amazon_order_id, merchant_order_id, "
        "purchase_date, last_updated_date, order_status, "
        "sku, product_name, quantity_purchased, currency, "
        "item_price, item_tax, shipping_price, "
        "buyer_email, ship_city, ship_state"
    )
    STAGING_TABLE = "staging.amazon_sales"
    DATE_PARSER = "iso"

    def extract_fields(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        df = raw_df.copy()

        # Synthesize a line_id: order_id + row position within that order
        df["line_pos"] = df.groupby("amazon_order_id").cumcount() + 1
        df["platform_line_id"] = (
            df["amazon_order_id"].astype("string") + "-" + df["line_pos"].astype("string")
        )

        df["platform_order_id"] = df["amazon_order_id"].astype("string")
        df["platform_sku"] = df["sku"]
        df["quantity"] = df["quantity_purchased"]
        df["unit_price_local"] = df["item_price"]
        df["tax_local"] = C.parse_money(df["item_tax"])
        df["discount_local"] = pd.NA
        df["currency"] = df["currency"].fillna("USD").astype("string")
        df["customer_email"] = df["buyer_email"].astype("string")
        df["raw_status"] = df["order_status"]
        df["order_dt_raw"] = df["purchase_date"]

        return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to compute line_total after parsing unit_price and quantity."""
        df = super().clean(df)
        # Compute line_total_local: unit_price * quantity (preserve sign for returns)
        df["line_total_local"] = (df["unit_price_local"] * df["quantity"]).round(4)
        return df


def run() -> int:
    return AmazonNormalizer().run()


if __name__ == "__main__":
    run()
