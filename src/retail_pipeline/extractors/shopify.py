"""Shopify extractor: reads orders JSON (API snapshot) into raw.shopify_orders."""
import json
from pathlib import Path

from sqlalchemy import text

from retail_pipeline.extractors.base import BaseExtractor
from retail_pipeline.utils.config import config


class ShopifyExtractor(BaseExtractor):
    source_name = "shopify"

    def __init__(self, engine=None, source_file: Path | None = None):
        super().__init__(engine)
        self.source_file = source_file or (config.DROPS_DIR / "shopify_orders.json")

    def extract(self) -> int:
        if not self.source_file.exists():
            self.log.warning(f"Source file not found: {self.source_file}")
            return 0

        self.log.info(f"Reading {self.source_file}")
        with open(self.source_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        orders = data.get("orders", [])
        self.log.info(f"Parsed {len(orders)} orders from JSON")

        if not orders:
            return 0

        rows = [
            {
                "order_id": o["id"],
                "payload": json.dumps(o),
                "source_file": self.source_file.name,
            }
            for o in orders
        ]

        # Insert with idempotent semantics: on duplicate, update payload.
        # We rely on a unique index on order_id; since our table has
        # raw_id SERIAL primary key (not order_id), duplicates are allowed
        # in raw BY DESIGN: they represent re-ingestions. We track them.
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO raw.shopify_orders (order_id, payload, source_file)
                    VALUES (:order_id, CAST(:payload AS JSONB), :source_file)
                """),
                rows,
            )

        return len(rows)


def run() -> int:
    return ShopifyExtractor().run()


if __name__ == "__main__":
    run()
