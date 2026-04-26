"""Run all normalizers concurrently."""
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from retail_pipeline.transformers import (
    normalize_amazon,
    normalize_mercadolibre,
    normalize_pos,
    normalize_shopify,
    normalize_tiendanube,
)
from retail_pipeline.utils.logging_setup import get_logger

log = get_logger("orchestrator.normalize")

NORMALIZERS = {
    "shopify":      normalize_shopify.run,
    "mercadolibre": normalize_mercadolibre.run,
    "amazon":       normalize_amazon.run,
    "tiendanube":   normalize_tiendanube.run,
    "pos":          normalize_pos.run,
}


def main():
    log.info("=== Running all normalizers in parallel ===")
    results: dict[str, int | str] = {}

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fn): name for name, fn in NORMALIZERS.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                log.error(f"[{name}] FAILED: {e}")
                results[name] = f"FAILED: {e}"

    log.info("=== Normalization summary ===")
    for name, val in results.items():
        log.info(f"  {name:15s} -> {val}")

    failed = [k for k, v in results.items() if isinstance(v, str) and v.startswith("FAILED")]
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
