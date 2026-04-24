"""Base class for all platform extractors."""
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from retail_pipeline.utils.db import get_engine
from retail_pipeline.utils.logging_setup import get_logger


class BaseExtractor(ABC):
    """Contract for platform extractors.

    Subclasses implement `extract()` which must load rows into raw.*
    and return the count of rows ingested.
    """

    source_name: str  # overridden in subclass, e.g. "shopify"

    def __init__(self, engine: Engine | None = None):
        self.engine = engine or get_engine()
        self.log = get_logger(f"extractor.{self.source_name}")

    @abstractmethod
    def extract(self) -> int:
        """Run extraction. Return number of rows inserted into raw.*"""
        ...

    def update_checkpoint(self, watermark: datetime, rows_ingested: int) -> None:
        """Upsert the checkpoint for this source."""
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO ops.etl_checkpoints (source_name, last_watermark, rows_ingested)
                    VALUES (:src, :wm, :rows)
                    ON CONFLICT (source_name) DO UPDATE
                    SET last_watermark = EXCLUDED.last_watermark,
                        rows_ingested  = ops.etl_checkpoints.rows_ingested + EXCLUDED.rows_ingested,
                        updated_at     = NOW()
                """),
                {"src": self.source_name, "wm": watermark, "rows": rows_ingested},
            )

    def run(self) -> int:
        """Entry point with logging and error capture."""
        self.log.info(f"--- Starting extraction: {self.source_name} ---")
        started = datetime.now(timezone.utc)
        try:
            n = self.extract()
            self.log.info(f"[OK] {self.source_name}: {n} rows ingested")
            self.update_checkpoint(started, n)
            return n
        except Exception as e:
            self.log.exception(f"[FAIL] {self.source_name}: {e}")
            raise
