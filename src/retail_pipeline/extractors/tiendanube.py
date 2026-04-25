"""Tiendanube extractor: reads .xlsx into raw.tiendanube_ventas.

The Excel uses Spanish column names with accents. We map them to ASCII
snake_case for the raw table.
"""
from pathlib import Path

import pandas as pd

from retail_pipeline.extractors.base import BaseExtractor
from retail_pipeline.utils.config import config


COLUMN_MAP = {
    "N° de orden":       "n_orden",
    "Fecha":             "fecha",
    "Estado del pago":   "estado_pago",
    "Estado del envío":  "estado_envio",
    "Cliente":           "cliente",
    "Email":             "email",
    "SKU":               "sku",
    "Producto":          "producto",
    "Cantidad":          "cantidad",
    "Precio unitario":   "precio_unit",
    "Descuento":         "descuento",
    "Subtotal":          "subtotal",
    "Moneda":            "moneda",
}


class TiendanubeExtractor(BaseExtractor):
    source_name = "tiendanube"

    def __init__(self, engine=None, source_file: Path | None = None):
        super().__init__(engine)
        self.source_file = source_file or (config.DROPS_DIR / "tiendanube_ventas.xlsx")

    def extract(self) -> int:
        if not self.source_file.exists():
            self.log.warning(f"Source file not found: {self.source_file}")
            return 0

        self.log.info(f"Reading {self.source_file}")

        # Read everything as string to preserve raw formatting (dates as DD/MM/YYYY etc.)
        df = pd.read_excel(
            self.source_file,
            sheet_name="Ventas",
            dtype=str,
            engine="openpyxl",
        )

        self.log.info(f"Parsed {len(df)} rows from Excel")
        if df.empty:
            return 0

        # Rename Spanish columns to snake_case
        df = df.rename(columns=COLUMN_MAP)
        df["source_file"] = self.source_file.name

        # Convert NaN to None for SQL NULLs
        df = df.where(pd.notna(df), None)

        records = df.to_dict(orient="records")

        with self.engine.begin() as conn:
            from sqlalchemy import text
            conn.execute(
                text("""
                    INSERT INTO raw.tiendanube_ventas
                    (n_orden, fecha, estado_pago, estado_envio, cliente, email,
                     sku, producto, cantidad, precio_unit, descuento, subtotal,
                     moneda, source_file)
                    VALUES
                    (:n_orden, :fecha, :estado_pago, :estado_envio, :cliente, :email,
                     :sku, :producto, :cantidad, :precio_unit, :descuento, :subtotal,
                     :moneda, :source_file)
                """),
                records,
            )

        return len(records)


def run() -> int:
    return TiendanubeExtractor().run()


if __name__ == "__main__":
    run()
