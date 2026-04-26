"""Seed reference.fx_rates with synthetic but realistic rates for the past 90 days."""
import random
from datetime import date, timedelta

from sqlalchemy import text

from retail_pipeline.utils.db import get_engine


def main():
    random.seed(99)
    engine = get_engine()

    today = date.today()
    rows = []

    # USD is always 1.0
    for i in range(90):
        d = today - timedelta(days=i)
        rows.append({"currency": "USD", "rate_date": d, "rate_to_usd": 1.0})

    # PEN around 3.75, with small daily noise
    base = 3.75
    for i in range(90):
        d = today - timedelta(days=i)
        # rate_to_usd = how many USD per 1 PEN -> 1/3.75 ~= 0.2667
        pen_per_usd = base + random.uniform(-0.05, 0.05)
        rate_to_usd = round(1.0 / pen_per_usd, 6)
        rows.append({"currency": "PEN", "rate_date": d, "rate_to_usd": rate_to_usd})

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO reference.fx_rates (currency, rate_date, rate_to_usd)
                VALUES (:currency, :rate_date, :rate_to_usd)
                ON CONFLICT (currency, rate_date) DO UPDATE
                SET rate_to_usd = EXCLUDED.rate_to_usd
            """),
            rows,
        )
    print(f"[fx] Seeded {len(rows)} FX rate rows")


if __name__ == "__main__":
    main()
