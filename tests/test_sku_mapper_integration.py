"""Integration tests for sku_mapper — requires a running Postgres with seeded data."""
import pandas as pd
import pytest

from retail_pipeline.transformers.sku_mapper import attach_canonical


pytestmark = pytest.mark.integration


class TestAttachCanonical:
    def test_known_sku_gets_canonical(self):
        df = pd.DataFrame([
            {"source_platform": "shopify", "platform_sku": "SHP-POL001-BLK-M"},
        ])
        enriched = attach_canonical(df)
        assert enriched["sku_canonical"].iloc[0] == "POL-001"
        assert enriched["category"].iloc[0] == "Polos"

    def test_dirty_sku_still_matches_via_normalization(self):
        """Trim + case-insensitive join must catch this."""
        df = pd.DataFrame([
            {"source_platform": "shopify", "platform_sku": " shp-pol001-blk-m "},
        ])
        enriched = attach_canonical(df)
        assert enriched["sku_canonical"].iloc[0] == "POL-001"

    def test_unknown_sku_yields_null_canonical(self):
        df = pd.DataFrame([
            {"source_platform": "shopify", "platform_sku": "DOES-NOT-EXIST"},
        ])
        enriched = attach_canonical(df)
        assert pd.isna(enriched["sku_canonical"].iloc[0])

    def test_wrong_platform_does_not_match(self):
        """Same SKU string on the wrong platform must NOT match."""
        df = pd.DataFrame([
            # SHP-* SKUs only exist for shopify
            {"source_platform": "amazon", "platform_sku": "SHP-POL001-BLK-M"},
        ])
        enriched = attach_canonical(df)
        assert pd.isna(enriched["sku_canonical"].iloc[0])
