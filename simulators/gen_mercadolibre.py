"""Simulate MercadoLibre orders API response.

Key quirks:
- Prices in local currency (PEN) — pipeline needs FX conversion
- Commission deducted from seller earnings
- Shipping included as a separate concept
- Status strings in Spanish
"""
import json
import random
import sys
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from faker import Faker

sys.path.insert(0, str(Path(__file__).parent))
from _catalog import get_products_for_platform
from _dirty import maybe_none, maybe_dirty_sku, random_date_in_window, should_duplicate

fake = Faker("es_MX")
Faker.seed(43)
random.seed(43)

LIMA_TZ = ZoneInfo("America/Lima")
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "drops"

PEN_PER_USD = 3.75  # Approximate FX rate


def generate_order(order_id: int, products: list[dict]) -> dict:
    created_at = random_date_in_window(30).astimezone(LIMA_TZ)
    n_items = random.choices([1, 2], weights=[80, 20])[0]
    chosen = random.sample(products, k=min(n_items, len(products)))

    items = []
    total_pen = 0.0
    for p in chosen:
        qty = random.choices([1, 2], weights=[90, 10])[0]
        unit_pen = round(p["base_price_usd"] * PEN_PER_USD * random.uniform(0.98, 1.08), 2)
        line_total = round(unit_pen * qty, 2)
        total_pen += line_total

        items.append({
            "item": {
                "id": f"MLP{random.randint(100000, 999999)}",
                "seller_sku": maybe_dirty_sku(p["sku"]),
                "title": p["name"],
                "category_id": p["category"],
            },
            "quantity": qty,
            "unit_price": unit_pen,
            "full_unit_price": unit_pen,
            "sale_fee": round(unit_pen * qty * 0.13, 2),  # MercadoLibre commission ~13%
        })

    status = random.choices(
        ["paid", "cancelled", "refunded"],
        weights=[88, 7, 5],
    )[0]

    shipping_cost = round(random.choice([0, 0, 10, 15, 20]) * 1.0, 2)

    return {
        "id": 2000000 + order_id,
        "date_created": created_at.isoformat(),
        "date_closed": (created_at + timedelta(hours=random.randint(1, 48))).isoformat(),
        "status": status,
        "currency_id": "PEN",
        "total_amount": round(total_pen + shipping_cost, 2),
        "paid_amount": round(total_pen + shipping_cost, 2) if status == "paid" else 0,
        "buyer": {
            "id": random.randint(10000, 99999),
            "nickname": fake.user_name(),
            "email": maybe_none(fake.email(), probability=0.10),
        },
        "order_items": items,
        "shipping": {
            "cost": shipping_cost,
            "status": "delivered" if status == "paid" else "cancelled",
        },
    }


def main(n_orders: int = 150):
    products = get_products_for_platform("mercadolibre")
    orders = []
    for i in range(1, n_orders + 1):
        order = generate_order(i, products)
        orders.append(order)
        if should_duplicate(0.03):
            orders.append(order)

    output_file = OUTPUT_DIR / "mercadolibre_orders.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({"results": orders}, f, indent=2, ensure_ascii=False)

    print(f"[mercadolibre] {len(orders)} orders written to {output_file}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 150
    main(n)
