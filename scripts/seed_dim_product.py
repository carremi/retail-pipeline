"""Seed core.dim_product from the catalog."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "simulators"))

from sqlalchemy import text  # noqa: E402

from _catalog import CATALOG  # noqa: E402
from retail_pipeline.utils.db import get_engine  # noqa: E402


def main():
    engine = get_engine()
    rows = [
        {
            "sku_canonical":  p.sku_canonical,
            "product_name":   p.name,
            "category":       p.category,
            "base_price_usd": p.base_price_usd,
        }
        for p in CATALOG
    ]
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO core.dim_product
                    (sku_canonical, product_name, category, base_price_usd)
                VALUES (:sku_canonical, :product_name, :category, :base_price_usd)
                ON CONFLICT (sku_canonical) DO UPDATE SET
                    product_name   = EXCLUDED.product_name,
                    category       = EXCLUDED.category,
                    base_price_usd = EXCLUDED.base_price_usd
            """),
            rows,
        )
    print(f"[dim_product] {len(rows)} products upserted")


if __name__ == "__main__":
    main()
