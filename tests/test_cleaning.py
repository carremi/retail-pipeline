"""Tests for transformers.cleaning — the pure functions used by all normalizers."""
import pandas as pd
import pytest

from retail_pipeline.transformers import cleaning as C


# ---------------------------------------------------------------------------
# clean_sku
# ---------------------------------------------------------------------------

class TestCleanSku:
    def test_strips_leading_and_trailing_spaces(self, dirty_skus):
        result = C.clean_sku(dirty_skus)
        # All non-null values should have no surrounding whitespace
        non_null = result.dropna()
        assert all(s == s.strip() for s in non_null), \
            f"Found leading/trailing whitespace: {non_null.tolist()}"

    def test_collapses_internal_whitespace(self):
        s = pd.Series(["A  B  C", "X   Y"])
        result = C.clean_sku(s)
        assert result.tolist() == ["A B C", "X Y"]

    def test_preserves_case(self):
        """Case is meaningful in some platforms; we don't normalize it here."""
        s = pd.Series(["abc", "ABC", "AbC"])
        result = C.clean_sku(s)
        assert result.tolist() == ["abc", "ABC", "AbC"]

    def test_empty_string_becomes_na(self):
        s = pd.Series(["valid", "", "  "])
        result = C.clean_sku(s)
        assert result.iloc[0] == "valid"
        assert pd.isna(result.iloc[1])
        # "  " becomes "" after strip -> NA
        assert pd.isna(result.iloc[2])

    def test_none_stays_none(self):
        s = pd.Series([None, "valid", None])
        result = C.clean_sku(s)
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == "valid"
        assert pd.isna(result.iloc[2])


# ---------------------------------------------------------------------------
# parse_money
# ---------------------------------------------------------------------------

class TestParseMoney:
    def test_parses_simple_decimal(self):
        s = pd.Series(["19.90", "100.00", "0.50"])
        result = C.parse_money(s)
        assert result.tolist() == [19.90, 100.0, 0.50]

    def test_strips_currency_symbols(self):
        s = pd.Series(["$19.90", "USD 49.90"])
        result = C.parse_money(s)
        assert result.tolist() == [19.90, 49.90]

    def test_handles_thousand_separator(self):
        """1,299.90 should be parsed as 1299.90, not 1.299 or NaN."""
        s = pd.Series(["1,299.90", "12,345.67"])
        result = C.parse_money(s)
        assert result.tolist() == [1299.90, 12345.67]

    def test_pure_garbage_becomes_na(self):
        """Strings with no extractable number should become NA."""
        s = pd.Series(["abc", "xyz", "$$$"])
        result = C.parse_money(s)
        assert result.isna().all()

    def test_mixed_text_extracts_the_number(self):
        """parse_money is permissive: it strips non-numeric chars and
        parses what's left. This matches real-world data where prices
        sometimes carry units, codes, etc."""
        s = pd.Series(["xyz123", "USD49.90", "$19.90"])
        result = C.parse_money(s)
        assert result.tolist() == [123.0, 49.90, 19.90]

    def test_ambiguous_punctuation_yields_na(self):
        """Strings like 'S/. 19.90' have leading punctuation that confuses
        the numeric parser; they end up as NA. This is acceptable: real
        cleaning would happen via a per-source pre-processor, not parse_money."""
        s = pd.Series(["S/. 19.90"])
        result = C.parse_money(s)
        assert result.isna().all()

    def test_preserves_negative_values(self):
        """Negative line totals (returns) must NOT be turned into NaN."""
        s = pd.Series(["-19.90", "-1,000.00"])
        result = C.parse_money(s)
        assert result.tolist() == [-19.90, -1000.0]


# ---------------------------------------------------------------------------
# parse_int
# ---------------------------------------------------------------------------

class TestParseInt:
    def test_parses_integers(self):
        s = pd.Series(["1", "2", "100"])
        result = C.parse_int(s)
        assert result.tolist() == [1, 2, 100]

    def test_negative_quantities_preserved(self):
        """Returns: quantity_purchased = -2 must stay -2."""
        s = pd.Series(["-1", "-3"])
        result = C.parse_int(s)
        assert result.tolist() == [-1, -3]

    def test_garbage_becomes_na(self):
        """Non-numeric strings and None should become NA."""
        s = pd.Series(["abc", None, ""])
        result = C.parse_int(s)
        assert result.isna().all()

    def test_clean_integer_strings_are_parsed(self):
        s = pd.Series(["1", "42", "-3"])
        result = C.parse_int(s)
        assert result.tolist() == [1, 42, -3]


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

class TestDateParsing:
    def test_iso_with_offset_to_utc(self, shopify_iso_dates):
        result = C.parse_iso_to_utc(shopify_iso_dates)
        # First two entries represent the same UTC instant
        assert result.iloc[0] == result.iloc[1]
        assert str(result.iloc[0].tz) == "UTC"

    def test_invalid_dates_become_nat(self, shopify_iso_dates):
        result = C.parse_iso_to_utc(shopify_iso_dates)
        assert pd.isna(result.iloc[3])  # None -> NaT
        assert pd.isna(result.iloc[4])  # "not a date" -> NaT

    def test_naive_lima_to_utc_adds_5_hours(self, lima_naive_dates):
        """Lima is UTC-5, so 10:30 Lima -> 15:30 UTC."""
        result = C.parse_naive_with_tz_to_utc(lima_naive_dates, tz="America/Lima")
        # First date: 2024-03-15 10:30 Lima -> 15:30 UTC
        assert result.iloc[0].hour == 15
        assert result.iloc[0].minute == 30
        assert str(result.iloc[0].tz) == "UTC"

    def test_dmy_format(self):
        """Tiendanube's DD/MM/YYYY in Lima -> UTC."""
        s = pd.Series(["15/03/2024", "31/12/2024"])
        result = C.parse_dmy_with_tz_to_utc(s, tz="America/Lima")
        # 2024-03-15 00:00 Lima -> 2024-03-15 05:00 UTC
        assert result.iloc[0].day == 15
        assert result.iloc[0].hour == 5


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestDedupe:
    def test_drops_duplicates_keeping_last(self):
        df = pd.DataFrame([
            {"order_id": "A", "line_id": "1", "raw_id": 1, "value": "old"},
            {"order_id": "A", "line_id": "1", "raw_id": 5, "value": "new"},
            {"order_id": "B", "line_id": "1", "raw_id": 2, "value": "single"},
        ])
        result = C.dedupe_by_key(df, ["order_id", "line_id"], keep="last")
        assert len(result) == 2
        # 'new' wins (last)
        a_rows = result[result["order_id"] == "A"]
        assert a_rows.iloc[0]["value"] == "new"

    def test_no_duplicates_returns_unchanged(self):
        df = pd.DataFrame([
            {"order_id": "A", "line_id": "1"},
            {"order_id": "B", "line_id": "1"},
        ])
        result = C.dedupe_by_key(df, ["order_id", "line_id"])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# FX conversion
# ---------------------------------------------------------------------------

class TestFxConversion:
    def test_to_usd_with_rate_one_returns_same_value(self):
        local = pd.Series([19.90, 100.0])
        rate = pd.Series([1.0, 1.0])
        result = C.to_usd(local, rate)
        assert result.tolist() == [19.9, 100.0]

    def test_to_usd_with_pen_rate(self):
        """75 PEN * 0.2667 USD/PEN = 20.0025"""
        local = pd.Series([75.0])
        rate = pd.Series([0.2667])
        result = C.to_usd(local, rate)
        assert abs(result.iloc[0] - 20.0025) < 0.001

    def test_to_usd_rounds_to_4_decimals(self):
        local = pd.Series([10.123456])
        rate = pd.Series([1.0])
        result = C.to_usd(local, rate)
        assert result.iloc[0] == 10.1235  # rounded
