"""Tests for the Pydantic validators."""
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from retail_pipeline.transformers.validators import SalesLineItem


class TestSalesLineItem:
    def test_valid_record_passes(self, valid_sales_record):
        item = SalesLineItem(**valid_sales_record)
        assert item.quantity == 2
        assert item.currency == "USD"

    def test_currency_lowercase_is_uppercased(self, valid_sales_record):
        valid_sales_record["currency"] = "usd"
        item = SalesLineItem(**valid_sales_record)
        assert item.currency == "USD"

    def test_unsupported_currency_rejected(self, valid_sales_record):
        valid_sales_record["currency"] = "EUR"
        with pytest.raises(ValidationError, match="unsupported currency"):
            SalesLineItem(**valid_sales_record)

    def test_zero_quantity_rejected(self, valid_sales_record):
        valid_sales_record["quantity"] = 0
        with pytest.raises(ValidationError, match="quantity cannot be zero"):
            SalesLineItem(**valid_sales_record)

    def test_negative_quantity_allowed(self, valid_sales_record):
        """Returns are encoded as negative quantities; this must pass."""
        valid_sales_record["quantity"] = -1
        item = SalesLineItem(**valid_sales_record)
        assert item.quantity == -1

    def test_negative_unit_price_rejected(self, valid_sales_record):
        valid_sales_record["unit_price_local"] = "-19.90"
        with pytest.raises(ValidationError, match="non-negative"):
            SalesLineItem(**valid_sales_record)

    def test_unknown_status_rejected(self, valid_sales_record):
        valid_sales_record["order_status"] = "Pagado"  # Spanish, not canonical
        with pytest.raises(ValidationError, match="unknown order_status"):
            SalesLineItem(**valid_sales_record)

    def test_far_future_date_rejected(self, valid_sales_record):
        future = datetime.now(timezone.utc) + timedelta(days=30)
        valid_sales_record["order_dt_utc"] = future
        with pytest.raises(ValidationError, match="future"):
            SalesLineItem(**valid_sales_record)

    def test_slightly_future_date_tolerated(self, valid_sales_record):
        """Allow up to 1 day of clock skew."""
        slight_future = datetime.now(timezone.utc) + timedelta(hours=12)
        valid_sales_record["order_dt_utc"] = slight_future
        item = SalesLineItem(**valid_sales_record)
        assert item.order_dt_utc == slight_future

    def test_optional_fields_can_be_none(self, valid_sales_record):
        valid_sales_record["customer_email"] = None
        valid_sales_record["tax_local"] = None
        valid_sales_record["platform_sku"] = None
        item = SalesLineItem(**valid_sales_record)
        assert item.customer_email is None
        assert item.tax_local is None
