"""Simulate a Shopify API response and save it as JSON.

Shopify returns line items nested inside orders, with:
- ISO 8601 timestamps in store timezone (America/Lima in our case)
- Prices as strings (Shopify API style)
- Tax and discount broken out per line
- Financial status: paid, refunded, partially_refunded
"""
import json
import random
import sys
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from faker import Faker

# Make sibling modules importable when running as a script
sys.path.insert(0, str(Path(__file__).parent))
from _catalog import get_products_for_platform
from _dirty import maybe_none, maybe_dirty_sku, random_date_in_window, should_duplicate

fake = Faker("es_MX")
Faker.seed(42)
random.seed(42)

LIMA_TZ = ZoneInfo("America/Lima")
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "drops"


def generate_order(order_number: int, products: list[dict]) -> dict:
    created_at = random_date_in_window(30).astimezone(LIMA_TZ)
    n_items = random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
    chosen = random.sample(products, k=min(n_items, len(products)))

    line_items = []
    subtotal = 0.0
    for i, p in enumerate(chosen, start=1):
        qty = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
        unit_price = p["base_price_usd"] * random.uniform(0.95, 1.05)
        discount = round(unit_price * qty * random.choice([0, 0, 0, 0.1, 0.2]), 2)
        line_total = round(unit_price * qty - discount, 2)
        subtotal += line_total

        line_items.append({
            "id": order_number * 100 + i,
            "sku": maybe_dirty_sku(p["sku"]),
            "title": p["name"],
            "quantity": qty,
            "price": f"{unit_price:.2f}",
            "total_discount": f"{discount:.2f}",
            "line_total": f"{line_total:.2f}",
        })

    tax = round(subtotal * 0.18, 2)
    total = round(subtotal + tax, 2)

    financial_status = random.choices(
        ["paid", "refunded", "partially_refunded", "pending"],
        weights=[85, 5, 5, 5],
    )[0]

    return {
        "id": 1000000 + order_number,
        "order_number": order_number,
        "created_at": created_at.isoformat(),
        "updated_at": (created_at + timedelta(hours=random.randint(0, 72))).isoformat(),
        "currency": "USD",
        "financial_status": financial_status,
        "customer": {
            "id": random.randint(10000, 99999),
            "email": maybe_none(fake.email(), probability=0.08),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
        },
        "subtotal_price": f"{subtotal:.2f}",
        "total_tax": f"{tax:.2f}",
        "total_price": f"{total:.2f}",
        "line_items": line_items,
    }


def main(n_orders: int = 200):
    products = get_products_for_platform("shopify")
    orders = []
    for i in range(1, n_orders + 1):
        order = generate_order(i, products)
        orders.append(order)
        # Occasional duplicate (API retries happen)
        if should_duplicate(0.02):
            orders.append(order)

    output_file = OUTPUT_DIR / "shopify_orders.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({"orders": orders}, f, indent=2, ensure_ascii=False)

    print(f"[shopify] {len(orders)} orders written to {output_file}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    main(n)
