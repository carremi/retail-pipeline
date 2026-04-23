"""Simulate Amazon Seller Central Fulfilled Shipments report (CSV, TSV-like).

Key quirks:
- Tab-separated (not comma)
- Date column as 'YYYY-MM-DDTHH:MM:SS+00:00' (ISO with offset)
- Amounts in USD but as strings with comma as thousand sep occasionally
- Column names in snake_case lowercase
- Returns appear as separate rows with negative quantity
"""
import csv
import random
import sys
from datetime import timedelta
from pathlib import Path

from faker import Faker

sys.path.insert(0, str(Path(__file__).parent))
from _catalog import get_products_for_platform
from _dirty import maybe_none, maybe_dirty_sku, random_date_in_window, should_duplicate

fake = Faker("en_US")
Faker.seed(44)
random.seed(44)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "drops"


def generate_row(order_idx: int, products: list[dict]) -> dict:
    p = random.choice(products)
    created_at = random_date_in_window(30)

    is_return = random.random() < 0.05
    qty = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
    if is_return:
        qty = -qty

    unit_price = round(p["base_price_usd"] * random.uniform(0.95, 1.10), 2)
    item_price = round(unit_price * abs(qty), 2)
    item_tax = round(item_price * 0.08, 2)

    # Occasional thousand separator mess
    item_price_str = f"{item_price:,.2f}" if item_price > 1000 else f"{item_price:.2f}"

    return {
        "amazon-order-id": f"111-{random.randint(1000000, 9999999)}-{random.randint(1000000, 9999999)}",
        "merchant-order-id": f"MRC{order_idx:06d}",
        "purchase-date": created_at.isoformat(),
        "last-updated-date": (created_at + timedelta(days=random.randint(0, 3))).isoformat(),
        "order-status": random.choices(
            ["Shipped", "Pending", "Cancelled", "Refunded"],
            weights=[85, 5, 5, 5],
        )[0],
        "sku": maybe_dirty_sku(p["sku"]),
        "product-name": p["name"],
        "quantity-purchased": qty,
        "currency": "USD",
        "item-price": item_price_str,
        "item-tax": f"{item_tax:.2f}",
        "shipping-price": f"{random.choice([0, 0, 5.99, 7.99, 9.99]):.2f}",
        "buyer-email": maybe_none(fake.email(), probability=0.15),
        "ship-city": fake.city(),
        "ship-state": fake.state_abbr(),
    }


def main(n_rows: int = 180):
    products = get_products_for_platform("amazon")
    rows = []
    for i in range(1, n_rows + 1):
        row = generate_row(i, products)
        rows.append(row)
        if should_duplicate(0.02):
            rows.append(row)

    output_file = OUTPUT_DIR / "amazon_fulfilled_shipments.tsv"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(rows[0].keys())
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"[amazon] {len(rows)} rows written to {output_file}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 180
    main(n)
