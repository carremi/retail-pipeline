"""Run a single normalizer by platform name."""
import sys

from retail_pipeline.transformers import (
    normalize_amazon,
    normalize_mercadolibre,
    normalize_pos,
    normalize_shopify,
    normalize_tiendanube,
)

NORMALIZERS = {
    "shopify":      normalize_shopify.run,
    "mercadolibre": normalize_mercadolibre.run,
    "amazon":       normalize_amazon.run,
    "tiendanube":   normalize_tiendanube.run,
    "pos":          normalize_pos.run,
}


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <platform>")
        print(f"Available: {', '.join(NORMALIZERS)}")
        sys.exit(1)

    name = sys.argv[1]
    if name not in NORMALIZERS:
        print(f"Unknown: {name}")
        sys.exit(1)

    n = NORMALIZERS[name]()
    print(f"\n[OK] {name}: {n} rows in staging")


if __name__ == "__main__":
    main()
