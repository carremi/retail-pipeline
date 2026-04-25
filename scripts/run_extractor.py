"""Run a single extractor by name.

Usage:
    python scripts/run_extractor.py shopify
    python scripts/run_extractor.py pos
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from retail_pipeline.extractors import (  # noqa: E402
    amazon,
    mercadolibre,
    pos,
    shopify,
    tiendanube,
)

EXTRACTORS = {
    "shopify":      shopify.run,
    "mercadolibre": mercadolibre.run,
    "amazon":       amazon.run,
    "tiendanube":   tiendanube.run,
    "pos":          pos.run,
}


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <source>")
        print(f"Available: {', '.join(EXTRACTORS)}")
        sys.exit(1)

    source = sys.argv[1]
    if source not in EXTRACTORS:
        print(f"Unknown source: {source}. Available: {', '.join(EXTRACTORS)}")
        sys.exit(1)

    n = EXTRACTORS[source]()
    print(f"\n[OK] {source}: {n} rows ingested")


if __name__ == "__main__":
    main()
