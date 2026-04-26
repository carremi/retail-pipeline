"""Quick exploration: read all staging tables, apply mapping, report stats."""
import pandas as pd

from retail_pipeline.transformers.sku_mapper import attach_canonical, report_unmapped
from retail_pipeline.utils.db import get_engine


def main():
    engine = get_engine()

    # Union all 5 staging tables
    sql = """
        SELECT source_platform, platform_sku FROM staging.shopify_sales
        UNION ALL SELECT source_platform, platform_sku FROM staging.mercadolibre_sales
        UNION ALL SELECT source_platform, platform_sku FROM staging.amazon_sales
        UNION ALL SELECT source_platform, platform_sku FROM staging.tiendanube_sales
        UNION ALL SELECT source_platform, platform_sku FROM staging.pos_sales
    """
    df = pd.read_sql(sql, engine)
    print(f"Total staging rows: {len(df)}")

    enriched = attach_canonical(df)

    matched   = enriched["sku_canonical"].notna().sum()
    unmatched = enriched["sku_canonical"].isna().sum()
    print(f"  Matched:   {matched}")
    print(f"  Unmatched: {unmatched}")

    if unmatched > 0:
        print("\nUnmatched (platform, sku) pairs:")
        bad = (
            enriched[enriched["sku_canonical"].isna()]
            .groupby(["source_platform", "platform_sku"])
            .size()
            .reset_index(name="occurrences")
            .sort_values("occurrences", ascending=False)
        )
        print(bad.to_string(index=False))

        report_unmapped(enriched)


if __name__ == "__main__":
    main()
