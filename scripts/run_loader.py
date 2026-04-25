"""Run the fact_sales loader."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from retail_pipeline.loaders import fact_sales  # noqa: E402


def main():
    n = fact_sales.load()
    print(f"\n[OK] fact_sales: {n} rows upserted")


if __name__ == "__main__":
    main()
