"""Seed core.dim_date with 5 years of dates."""
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sqlalchemy import text  # noqa: E402

from retail_pipeline.utils.db import get_engine  # noqa: E402

SPANISH_MONTHS = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                  "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
SPANISH_DAYS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]


def main():
    engine = get_engine()
    start = date(2024, 1, 1)
    end   = date(2028, 12, 31)

    rows = []
    d = start
    while d <= end:
        rows.append({
            "date_id":      d,
            "year":         d.year,
            "quarter":      (d.month - 1) // 3 + 1,
            "month":        d.month,
            "month_name":   SPANISH_MONTHS[d.month],
            "day":          d.day,
            "day_of_week":  d.isoweekday(),
            "day_name":     SPANISH_DAYS[d.isoweekday() - 1],
            "week_of_year": d.isocalendar()[1],
            "is_weekend":   d.isoweekday() >= 6,
        })
        d += timedelta(days=1)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO core.dim_date
                    (date_id, year, quarter, month, month_name, day,
                     day_of_week, day_name, week_of_year, is_weekend)
                VALUES
                    (:date_id, :year, :quarter, :month, :month_name, :day,
                     :day_of_week, :day_name, :week_of_year, :is_weekend)
                ON CONFLICT (date_id) DO NOTHING
            """),
            rows,
        )

    print(f"[dim_date] {len(rows)} rows inserted (or already present)")


if __name__ == "__main__":
    main()
