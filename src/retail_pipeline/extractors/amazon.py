"""Amazon extractor: reads TSV report into raw.amazon_shipments.

Strategy: keep ALL columns as text in raw. Type coercion happens in staging.
This way, malformed values (negative quantities, comma-separated thousands,
etc.) don't break ingestion.
"""
from pathlib import Path

import pandas as pd

from retail_pipeline.extractors.base import BaseExtractor
from retail_pipeline.utils.config import config


# Map TSV column names (with hyphens) to raw table column names (with underscores)
COLUMN_MAP = {
    "amazon-order-id":     "amazon_order_id",
    "merchant-order-id":   "merchant_order_id",
    "purchase-date":       "purchase_date",
    "last-updated-date":   "last_updated_date",
    "order-status":        "order_status",
    "sku":                 "sku",
    "product-name":        "product_name",
    "quantity-purchased":  "quantity_purchased",
    "currency":            "currency",
    "item-price":          "item_price",
    "item-tax":            "item_tax",
    "shipping-price":      "shipping_price",
    "buyer-email":         "buyer_email",
    "ship-city":           "ship_city",
    "ship-state":          "ship_state",
}


class AmazonExtractor(BaseExtractor):
    source_name = "amazon"

    def __init__(self, engine=None, source_file: Path | None = None):
        super().__init__(engine)
        self.source_file = source_file or (config.DROPS_DIR / "amazon_fulfilled_shipments.tsv")

    def extract(self) -> int:
        if not self.source_file.exists():
            self.log.warning(f"Source file not found: {self.source_file}")
            return 0

        self.log.info(f"Reading {self.source_file}")

        # Read all columns as string to preserve raw values verbatim.
        # keep_default_na=False stops pandas from converting "" to NaN.
        df = pd.read_csv(
            self.source_file,
            sep="\t",
            dtype=str,
            keep_default_na=False,
            na_values=[""],
        )

        self.log.info(f"Parsed {len(df)} rows from TSV")
        if df.empty:
            return 0

        # Rename columns to match raw table
        df = df.rename(columns=COLUMN_MAP)
        df["source_file"] = self.source_file.name

        # Replace pandas NaN with None for proper SQL NULLs
        df = df.where(pd.notna(df), None)

        records = df.to_dict(orient="records")

        with self.engine.begin() as conn:
            from sqlalchemy import text
            conn.execute(
                text("""
                    INSERT INTO raw.amazon_shipments
                    (amazon_order_id, merchant_order_id, purchase_date, last_updated_date,
                     order_status, sku, product_name, quantity_purchased, currency,
                     item_price, item_tax, shipping_price, buyer_email, ship_city,
                     ship_state, source_file)
                    VALUES
                    (:amazon_order_id, :merchant_order_id, :purchase_date, :last_updated_date,
                     :order_status, :sku, :product_name, :quantity_purchased, :currency,
                     :item_price, :item_tax, :shipping_price, :buyer_email, :ship_city,
                     :ship_state, :source_file)
                """),
                records,
            )

        return len(records)


def run() -> int:
    return AmazonExtractor().run()


if __name__ == "__main__":
    run()
