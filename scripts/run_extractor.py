"""Run a single extractor by name.

Usage:
    python scripts/run_extractor.py shopify
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from retail_pipeline.extractors import shopify  # noqa: E402

EXTRACTORS = {
    "shopify": shopify.run,
    # future: "mercadolibre": mercadolibre.run, etc.
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
