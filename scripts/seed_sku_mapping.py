"""Seed reference.products and reference.sku_mapping from the simulator catalog.

In real life, this data would be maintained in an Excel sheet by the
commercial team. Here we generate it from CATALOG to keep things in sync.
"""
import sys
from pathlib import Path

# Allow importing the simulator catalog
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "simulators"))

from sqlalchemy import text

from _catalog import CATALOG
from retail_pipeline.utils.db import get_engine

PLATFORMS = ["shopify", "mercadolibre", "amazon", "tiendanube", "pos"]


def main():
    engine = get_engine()

    products = [
        {
            "sku_canonical": p.sku_canonical,
            "product_name":  p.name,
            "category":      p.category,
            "base_price_usd": p.base_price_usd,
        }
        for p in CATALOG
    ]

    mappings = []
    for p in CATALOG:
        for platform in PLATFORMS:
            sku_field = f"sku_{platform}"
            mappings.append({
                "source_platform": platform,
                "platform_sku":    getattr(p, sku_field),
                "sku_canonical":   p.sku_canonical,
                "product_name":    p.name,
                "category":        p.category,
                "base_price_usd":  p.base_price_usd,
            })

    with engine.begin() as conn:
        # Upsert products
        conn.execute(
            text("""
                INSERT INTO reference.products
                    (sku_canonical, product_name, category, base_price_usd)
                VALUES (:sku_canonical, :product_name, :category, :base_price_usd)
                ON CONFLICT (sku_canonical) DO UPDATE SET
                    product_name   = EXCLUDED.product_name,
                    category       = EXCLUDED.category,
                    base_price_usd = EXCLUDED.base_price_usd
            """),
            products,
        )

        # Upsert mappings
        conn.execute(
            text("""
                INSERT INTO reference.sku_mapping
                    (source_platform, platform_sku, sku_canonical,
                     product_name, category, base_price_usd)
                VALUES (:source_platform, :platform_sku, :sku_canonical,
                        :product_name, :category, :base_price_usd)
                ON CONFLICT (source_platform, platform_sku) DO UPDATE SET
                    sku_canonical  = EXCLUDED.sku_canonical,
                    product_name   = EXCLUDED.product_name,
                    category       = EXCLUDED.category,
                    base_price_usd = EXCLUDED.base_price_usd
            """),
            mappings,
        )

    print(f"[seed] {len(products)} products, {len(mappings)} mappings written.")


if __name__ == "__main__":
    main()
