"""POS extractor: snapshots pos_source.ventas into raw.pos_ventas.

Since both schemas live in the same Postgres instance, we do a
SQL-only INSERT...SELECT, which is dramatically faster than fetching
to Python and re-inserting.

Incremental strategy: read the checkpoint and only pull rows
newer than last_watermark.
"""
from datetime import datetime, timezone

from sqlalchemy import text

from retail_pipeline.extractors.base import BaseExtractor


class PosExtractor(BaseExtractor):
    source_name = "pos"

    def _get_last_watermark(self) -> datetime | None:
        """Return the last processed fecha_venta, or None if first run."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT last_watermark
                    FROM ops.etl_checkpoints
                    WHERE source_name = :src
                """),
                {"src": self.source_name},
            ).fetchone()
        return row[0] if row else None

    def extract(self) -> int:
        watermark = self._get_last_watermark()
        self.log.info(f"Last watermark: {watermark}")

        # Build the INSERT...SELECT. POS uses naive timestamps (no TZ);
        # we treat them as Lima time. For comparison we cast both sides.
        if watermark is None:
            where_clause = ""
            params = {}
        else:
            # Compare against the watermark interpreted as Lima local time
            where_clause = "WHERE v.fecha_venta > (:wm AT TIME ZONE 'America/Lima')"
            params = {"wm": watermark}

        sql = f"""
            INSERT INTO raw.pos_ventas
            (venta_id, fecha_venta, tienda_id, cajero, sku, producto,
             cantidad, precio_unit, descuento, total_linea, medio_pago, cliente_doc)
            SELECT
                v.venta_id, v.fecha_venta, v.tienda_id, v.cajero, v.sku, v.producto,
                v.cantidad, v.precio_unit, v.descuento, v.total_linea,
                v.medio_pago, v.cliente_doc
            FROM pos_source.ventas v
            {where_clause}
            ORDER BY v.fecha_venta
        """

        with self.engine.begin() as conn:
            result = conn.execute(text(sql), params)
            n = result.rowcount

        return n

    def run(self) -> int:
        """Override to set checkpoint to the max fecha_venta we just snapshotted."""
        self.log.info(f"--- Starting extraction: {self.source_name} ---")
        try:
            n = self.extract()
            self.log.info(f"[OK] {self.source_name}: {n} rows ingested")

            # Update checkpoint to the latest fecha_venta we have in raw,
            # not to "now" — incremental loads need the data watermark.
            with self.engine.connect() as conn:
                max_fecha = conn.execute(
                    text("SELECT MAX(fecha_venta) FROM raw.pos_ventas")
                ).scalar()

            if max_fecha is not None:
                # Treat naive timestamp as Lima time, convert to UTC for storage
                watermark_utc = max_fecha.replace(tzinfo=None)
                # We store as TIMESTAMPTZ; pass with explicit Lima offset
                from zoneinfo import ZoneInfo
                lima = ZoneInfo("America/Lima")
                watermark_aware = watermark_utc.replace(tzinfo=lima).astimezone(timezone.utc)
                self.update_checkpoint(watermark_aware, n)
            return n
        except Exception as e:
            self.log.exception(f"[FAIL] {self.source_name}: {e}")
            raise


def run() -> int:
    return PosExtractor().run()


if __name__ == "__main__":
    run()
