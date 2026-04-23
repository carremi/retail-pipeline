"""Simulate a Tiendanube export: Excel .xlsx with Spanish column names.

Key quirks:
- Column names in Spanish with accents
- Dates as 'DD/MM/YYYY' strings (not ISO)
- Amounts as floats with commas as decimal separator occasionally
- Mixed states: 'Pagado', 'Cancelado', 'Pendiente de pago'
- Multi-row orders: one row per item
"""
import random
import sys
from datetime import timedelta
from pathlib import Path

from faker import Faker
from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).parent))
from _catalog import get_products_for_platform
from _dirty import maybe_none, maybe_dirty_sku, random_date_in_window, should_duplicate

fake = Faker("es_AR")
Faker.seed(45)
random.seed(45)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "drops"

PEN_PER_USD = 3.75


def main(n_orders: int = 120):
    products = get_products_for_platform("tiendanube")

    wb = Workbook()
    ws = wb.active
    ws.title = "Ventas"

    headers = [
        "N° de orden",
        "Fecha",
        "Estado del pago",
        "Estado del envío",
        "Cliente",
        "Email",
        "SKU",
        "Producto",
        "Cantidad",
        "Precio unitario",
        "Descuento",
        "Subtotal",
        "Moneda",
    ]
    ws.append(headers)

    row_count = 0
    for order_idx in range(1, n_orders + 1):
        order_id = 3000000 + order_idx
        created_at = random_date_in_window(30)
        date_str = created_at.strftime("%d/%m/%Y")

        payment_status = random.choices(
            ["Pagado", "Pendiente de pago", "Cancelado", "Reembolsado"],
            weights=[82, 8, 5, 5],
        )[0]
        shipping_status = random.choice(["Entregado", "En preparación", "Enviado", "Cancelado"])

        customer = fake.name()
        email = maybe_none(fake.email(), probability=0.12)

        n_items = random.choices([1, 2, 3], weights=[70, 25, 5])[0]
        chosen = random.sample(products, k=min(n_items, len(products)))

        for p in chosen:
            qty = random.choices([1, 2], weights=[88, 12])[0]
            unit_pen = round(p["base_price_usd"] * PEN_PER_USD * random.uniform(0.97, 1.07), 2)
            discount = round(unit_pen * qty * random.choice([0, 0, 0, 0.05, 0.15]), 2)
            subtotal = round(unit_pen * qty - discount, 2)

            ws.append([
                order_id,
                date_str,
                payment_status,
                shipping_status,
                customer,
                email,
                maybe_dirty_sku(p["sku"]),
                p["name"],
                qty,
                unit_pen,
                discount,
                subtotal,
                "PEN",
            ])
            row_count += 1

            # Occasional duplicate row
            if should_duplicate(0.02):
                ws.append([
                    order_id, date_str, payment_status, shipping_status, customer,
                    email, p["sku"], p["name"], qty, unit_pen, discount, subtotal, "PEN",
                ])
                row_count += 1

    output_file = OUTPUT_DIR / "tiendanube_ventas.xlsx"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_file)

    print(f"[tiendanube] {row_count} rows written to {output_file}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    main(n)
