"""Simulate a POS (point-of-sale) system writing directly to a Postgres table.

This mimics a physical-store POS that maintains its own database.
The pipeline will read from this table (not from a file).

Key quirks:
- Timestamps in server's local timezone (Lima) stored without TZ info
- Amounts in PEN
- Schema 'pos_source' simulates the POS being a separate database/system
- Includes nulls and some edge cases
"""
import random
import sys
from datetime import timedelta
from pathlib import Path

from faker import Faker
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from retail_pipeline.utils.config import config  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent))
from _catalog import get_products_for_platform  # noqa: E402
from _dirty import maybe_none, random_date_in_window  # noqa: E402

fake = Faker("es_MX")
Faker.seed(46)
random.seed(46)

PEN_PER_USD = 3.75


def setup_schema(engine):
    """Create the pos_source schema and its tables."""
    ddl = """
    CREATE SCHEMA IF NOT EXISTS pos_source;

    DROP TABLE IF EXISTS pos_source.ventas CASCADE;

    CREATE TABLE pos_source.ventas (
        venta_id      BIGINT PRIMARY KEY,
        fecha_venta   TIMESTAMP,  -- no timezone, local Lima time
        tienda_id     INTEGER,
        cajero        VARCHAR(100),
        sku           VARCHAR(50),
        producto      VARCHAR(200),
        cantidad      INTEGER,
        precio_unit   NUMERIC(10, 2),
        descuento     NUMERIC(10, 2),
        total_linea   NUMERIC(10, 2),
        medio_pago    VARCHAR(20),
        cliente_doc   VARCHAR(20)
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def generate_rows(n_transactions: int, products: list[dict]) -> list[dict]:
    rows = []
    stores = [1, 2, 3]
    cashiers = ["Ana Lopez", "Carlos Ruiz", "Maria Diaz", "Jorge Flores", "Luisa Tapia"]

    for tx_id in range(1, n_transactions + 1):
        fecha = random_date_in_window(30)
        # POS stores naive datetime (no TZ)
        fecha_naive = fecha.replace(tzinfo=None)

        n_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 20, 7, 3])[0]
        chosen = random.sample(products, k=min(n_items, len(products)))

        for p in chosen:
            qty = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
            unit_pen = round(p["base_price_usd"] * PEN_PER_USD * random.uniform(0.95, 1.02), 2)
            disc = round(unit_pen * qty * random.choice([0, 0, 0, 0.1]), 2)
            total = round(unit_pen * qty - disc, 2)

            rows.append({
                "venta_id": 4000000 + len(rows) + 1,
                "fecha_venta": fecha_naive + timedelta(seconds=len(rows)),
                "tienda_id": random.choice(stores),
                "cajero": random.choice(cashiers),
                "sku": p["sku"],  # POS tends to be cleanest
                "producto": p["name"],
                "cantidad": qty,
                "precio_unit": unit_pen,
                "descuento": disc,
                "total_linea": total,
                "medio_pago": random.choice(["EFECTIVO", "TARJETA", "YAPE", "PLIN"]),
                "cliente_doc": maybe_none(fake.bothify(text="########"), probability=0.4),
            })

    return rows


def main(n_transactions: int = 250):
    products = get_products_for_platform("pos")
    engine = create_engine(config.pg_url)

    print(f"[pos] Setting up pos_source schema...")
    setup_schema(engine)

    rows = generate_rows(n_transactions, products)

    print(f"[pos] Inserting {len(rows)} rows into pos_source.ventas...")
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pos_source.ventas
                (venta_id, fecha_venta, tienda_id, cajero, sku, producto,
                 cantidad, precio_unit, descuento, total_linea, medio_pago, cliente_doc)
                VALUES
                (:venta_id, :fecha_venta, :tienda_id, :cajero, :sku, :producto,
                 :cantidad, :precio_unit, :descuento, :total_linea, :medio_pago, :cliente_doc)
            """),
            rows,
        )

    print(f"[pos] Done. {len(rows)} rows in pos_source.ventas")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 250
    main(n)
