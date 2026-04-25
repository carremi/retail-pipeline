"""Run all extractors concurrently.

Each extractor is independent (different source, different raw table),
so they can safely run in parallel. We use threads (not processes) because
the bottleneck is I/O (reading files, network to Postgres), not CPU.
"""
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from retail_pipeline.extractors import (  # noqa: E402
    amazon,
    mercadolibre,
    pos,
    shopify,
    tiendanube,
)
from retail_pipeline.utils.logging_setup import get_logger  # noqa: E402

log = get_logger("orchestrator.extract")

EXTRACTORS = {
    "shopify":      shopify.run,
    "mercadolibre": mercadolibre.run,
    "amazon":       amazon.run,
    "tiendanube":   tiendanube.run,
    "pos":          pos.run,
}


def main():
    log.info("=== Running all extractors in parallel ===")
    results: dict[str, int | str] = {}

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fn): name for name, fn in EXTRACTORS.items()}

        for fut in as_completed(futures):
            name = futures[fut]
            try:
                n = fut.result()
                results[name] = n
            except Exception as e:
                log.error(f"[{name}] FAILED: {e}")
                results[name] = f"FAILED: {e}"

    log.info("=== Extraction summary ===")
    for name, val in results.items():
        log.info(f"  {name:15s} -> {val}")

    failed = [k for k, v in results.items() if isinstance(v, str) and v.startswith("FAILED")]
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
