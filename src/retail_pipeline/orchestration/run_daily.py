"""Daily pipeline runner.

Orchestrates the full flow:
    extractors -> normalizers -> validators -> loader

Each stage logs its outcome. The full run is recorded in ops.pipeline_runs
so we can audit history (started_at, finished_at, status, summary, errors).
"""
from __future__ import annotations

import json
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from sqlalchemy import text

from retail_pipeline.extractors import (
    amazon as ex_amazon,
    mercadolibre as ex_ml,
    pos as ex_pos,
    shopify as ex_shopify,
    tiendanube as ex_tn,
)
from retail_pipeline.transformers import (
    normalize_amazon,
    normalize_mercadolibre,
    normalize_pos,
    normalize_shopify,
    normalize_tiendanube,
)
from retail_pipeline.transformers.validators import validate_all
from retail_pipeline.loaders import fact_sales
from retail_pipeline.utils.db import get_engine
from retail_pipeline.utils.logging_setup import get_logger

log = get_logger("pipeline.daily")

EXTRACTORS = {
    "shopify":      ex_shopify.run,
    "mercadolibre": ex_ml.run,
    "amazon":       ex_amazon.run,
    "tiendanube":   ex_tn.run,
    "pos":          ex_pos.run,
}

NORMALIZERS = {
    "shopify":      normalize_shopify.run,
    "mercadolibre": normalize_mercadolibre.run,
    "amazon":       normalize_amazon.run,
    "tiendanube":   normalize_tiendanube.run,
    "pos":          normalize_pos.run,
}


def _run_parallel(stage_name: str, fns: dict[str, callable]) -> dict[str, int | str]:
    """Run a dict of named callables in parallel threads. Return their results."""
    log.info(f"=== Stage: {stage_name} ===")
    results: dict[str, int | str] = {}
    with ThreadPoolExecutor(max_workers=len(fns)) as pool:
        futures = {pool.submit(fn): name for name, fn in fns.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                log.error(f"[{stage_name}.{name}] FAILED: {e}")
                results[name] = f"FAILED: {e}"
    return results


def _start_run() -> int:
    """Insert a row in ops.pipeline_runs and return its run_id."""
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                INSERT INTO ops.pipeline_runs (status)
                VALUES ('running')
                RETURNING run_id
            """)
        ).fetchone()
    return row[0]


def _finish_run(run_id: int, status: str, summary: dict, error: str | None = None):
    """Mark the run as finished with status and summary."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE ops.pipeline_runs
                SET finished_at     = NOW(),
                    status          = :status,
                    sources_summary = CAST(:summary AS JSONB),
                    error_message   = :error
                WHERE run_id = :run_id
            """),
            {
                "run_id":  run_id,
                "status":  status,
                "summary": json.dumps(summary, default=str),
                "error":   error,
            },
        )


def main() -> int:
    started = time.monotonic()
    started_at = datetime.now(timezone.utc)
    log.info(f"========== Pipeline started: {started_at.isoformat()} ==========")
    run_id = _start_run()
    log.info(f"Run ID: {run_id}")

    summary: dict = {"run_id": run_id, "started_at": started_at.isoformat()}

    try:
        # 1. Extract
        ex_results = _run_parallel("EXTRACT", EXTRACTORS)
        summary["extract"] = ex_results
        if any(isinstance(v, str) for v in ex_results.values()):
            raise RuntimeError(f"Extraction failed: {ex_results}")

        # 2. Normalize
        nm_results = _run_parallel("NORMALIZE", NORMALIZERS)
        summary["normalize"] = nm_results
        if any(isinstance(v, str) for v in nm_results.values()):
            raise RuntimeError(f"Normalization failed: {nm_results}")

        # 3. Validate
        log.info("=== Stage: VALIDATE ===")
        val_summary = validate_all()
        summary["validate"] = val_summary

        # 4. Load to core.fact_sales
        log.info("=== Stage: LOAD ===")
        n_loaded = fact_sales.load()
        summary["load"] = {"fact_sales_rows_upserted": n_loaded}

        elapsed = round(time.monotonic() - started, 2)
        summary["elapsed_seconds"] = elapsed
        log.info(f"========== Pipeline finished OK in {elapsed}s ==========")
        _finish_run(run_id, "success", summary)
        _print_report(summary)
        return 0

    except Exception as e:
        elapsed = round(time.monotonic() - started, 2)
        summary["elapsed_seconds"] = elapsed
        tb = traceback.format_exc()
        log.exception("Pipeline FAILED")
        _finish_run(run_id, "failed", summary, error=tb)
        _print_report(summary, ok=False)
        return 1


def _print_report(summary: dict, ok: bool = True) -> None:
    """Print a human-friendly summary at the end."""
    print("\n" + "=" * 60)
    print(f"PIPELINE RUN {summary.get('run_id')} — {'OK' if ok else 'FAILED'}")
    print(f"Elapsed: {summary.get('elapsed_seconds')}s")
    print("=" * 60)

    for stage in ("extract", "normalize"):
        if stage in summary:
            print(f"\n[{stage.upper()}]")
            for src, val in summary[stage].items():
                print(f"  {src:15s} -> {val}")

    if "validate" in summary:
        v = summary["validate"]
        print(f"\n[VALIDATE]")
        print(f"  total: {v['total']}, valid: {v['valid']}, rejected: {v['rejected']}")

    if "load" in summary:
        print(f"\n[LOAD]")
        for k, v in summary["load"].items():
            print(f"  {k}: {v}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    sys.exit(main())
