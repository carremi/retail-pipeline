"""MercadoLibre extractor: reads orders JSON into raw.mercadolibre_orders."""
import json
from pathlib import Path

from sqlalchemy import text

from retail_pipeline.extractors.base import BaseExtractor
from retail_pipeline.utils.config import config


class MercadoLibreExtractor(BaseExtractor):
    source_name = "mercadolibre"

    def __init__(self, engine=None, source_file: Path | None = None):
        super().__init__(engine)
        self.source_file = source_file or (config.DROPS_DIR / "mercadolibre_orders.json")

    def extract(self) -> int:
        if not self.source_file.exists():
            self.log.warning(f"Source file not found: {self.source_file}")
            return 0

        self.log.info(f"Reading {self.source_file}")
        with open(self.source_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # MercadoLibre uses "results" as the wrapper key, not "orders"
        orders = data.get("results", [])
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

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO raw.mercadolibre_orders (order_id, payload, source_file)
                    VALUES (:order_id, CAST(:payload AS JSONB), :source_file)
                """),
                rows,
            )

        return len(rows)


def run() -> int:
    return MercadoLibreExtractor().run()


if __name__ == "__main__":
    run()
