"""Base normalizer: common ETL flow for all platform normalizers.

Subclasses only need to define:
- PLATFORM: str             (e.g. "shopify")
- RAW_TABLE: str            (e.g. "raw.shopify_orders")
- RAW_COLUMNS: str          (SQL column list for the SELECT)
- STAGING_TABLE: str        (e.g. "staging.shopify_sales")
- extract_fields(raw_df) -> pd.DataFrame   (platform-specific field extraction)

Optional overrides:
- DATE_PARSER: str          ("iso", "naive", "dmy") — how to parse dates
- DATE_TZ: str              (timezone for naive/dmy parsers, e.g. "America/Lima")
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd
from sqlalchemy import text

from retail_pipeline.transformers import cleaning as C
from retail_pipeline.utils.db import get_engine
from retail_pipeline.utils.logging_setup import get_logger


# Columns expected in the staging output, in order.
STAGING_COLUMNS = [
    "source_platform", "platform_order_id", "platform_line_id",
    "order_dt_utc", "platform_sku", "product_name", "quantity",
    "unit_price_local", "discount_local", "tax_local", "line_total_local",
    "currency", "unit_price_usd", "line_total_usd",
    "order_status", "customer_email", "raw_id",
]


class BaseNormalizer(ABC):
    """Common normalization pipeline: load -> extract -> clean -> FX -> status -> dedup -> write."""

    PLATFORM: str
    RAW_TABLE: str
    RAW_COLUMNS: str
    STAGING_TABLE: str
    DATE_PARSER: str = "iso"
    DATE_TZ: str = "UTC"

    def __init__(self):
        self.engine = get_engine()
        self.log = get_logger(f"normalize.{self.PLATFORM}")

    @abstractmethod
    def extract_fields(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw rows into a DataFrame with the staging column contract.

        Must produce columns: raw_id, platform_order_id, platform_line_id,
        order_dt_raw, platform_sku, product_name, quantity, unit_price_local,
        discount_local, tax_local, line_total_local, currency, raw_status,
        customer_email.
        """
        ...

    def load_raw(self) -> pd.DataFrame:
        self.log.info(f"Loading {self.RAW_TABLE}...")
        df = pd.read_sql(f"SELECT {self.RAW_COLUMNS} FROM {self.RAW_TABLE}", self.engine)
        self.log.info(f"Loaded {len(df)} raw rows")
        return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df["platform_sku"] = C.clean_sku(df["platform_sku"])
        df["unit_price_local"] = C.parse_money(df["unit_price_local"])
        df["discount_local"] = C.parse_money(df["discount_local"])
        df["line_total_local"] = C.parse_money(df["line_total_local"])
        df["quantity"] = C.parse_int(df["quantity"])
        df["source_platform"] = self.PLATFORM

        if self.DATE_PARSER == "iso":
            df["order_dt_utc"] = C.parse_iso_to_utc(df["order_dt_raw"])
        elif self.DATE_PARSER == "naive":
            df["order_dt_utc"] = C.parse_naive_with_tz_to_utc(df["order_dt_raw"], tz=self.DATE_TZ)
        elif self.DATE_PARSER == "dmy":
            df["order_dt_utc"] = C.parse_dmy_with_tz_to_utc(df["order_dt_raw"], tz=self.DATE_TZ)

        return df

    def apply_fx(self, df: pd.DataFrame) -> pd.DataFrame:
        fx_df = pd.read_sql(
            "SELECT currency, rate_date, rate_to_usd FROM reference.fx_rates",
            self.engine,
        )
        df = C.attach_fx_rate(df, fx_df)
        df["unit_price_usd"] = C.to_usd(df["unit_price_local"], df["rate_to_usd"])
        df["line_total_usd"] = C.to_usd(df["line_total_local"], df["rate_to_usd"])
        return df

    def apply_status(self, df: pd.DataFrame) -> pd.DataFrame:
        status_map = pd.read_sql(
            "SELECT source_platform, raw_status, canonical_status FROM reference.status_mapping",
            self.engine,
        )
        df["order_status"] = C.map_status(df, status_map, self.PLATFORM)
        return df

    def dedup_and_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("raw_id")
        df = C.dedupe_by_key(df, ["platform_order_id", "platform_line_id"], keep="last")

        before = len(df)
        df = df.dropna(subset=["order_dt_utc", "quantity"])
        self.log.info(f"Dropped {before - len(df)} rows with missing dt/quantity")
        return df

    def write_staging(self, df: pd.DataFrame) -> int:
        final = df[STAGING_COLUMNS].copy()
        final = C.coerce_dtypes_for_db(final)

        self.log.info(f"Writing {len(final)} rows to {self.STAGING_TABLE}")
        schema, table = self.STAGING_TABLE.split(".")

        with self.engine.begin() as conn:
            conn.execute(text(f"TRUNCATE {self.STAGING_TABLE} RESTART IDENTITY"))
            final.to_sql(
                table, conn, schema=schema,
                if_exists="append", index=False, method="multi", chunksize=500,
                dtype=C.STAGING_DTYPE_MAP,
            )
        return len(final)

    def normalize(self) -> int:
        raw_df = self.load_raw()
        if raw_df.empty:
            return 0

        df = self.extract_fields(raw_df)
        self.log.info(f"Extracted {len(df)} line items")

        df = self.clean(df)
        df = self.apply_fx(df)
        df = self.apply_status(df)
        df = self.dedup_and_filter(df)
        return self.write_staging(df)

    def run(self) -> int:
        self.log.info(f"--- Normalizing {self.PLATFORM} ---")
        n = self.normalize()
        self.log.info(f"[OK] {self.PLATFORM}: {n} rows in staging")
        return n
