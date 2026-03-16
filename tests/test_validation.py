from __future__ import annotations

"""Unit tests for normalization and validation helpers """

from decimal import Decimal

from pricemonitor.parsers.normalization import clean_text, normalize_price, normalize_url
from pricemonitor.services.validation import validate_product_records


def test_normalization_helpers_handle_messy_values() -> None:
    """Whitespace, prices, and URLs should normalize into stable business-friendly values."""

    assert clean_text("  Demo   Product  ") == "Demo Product"
    assert normalize_price("€1,299.99") == Decimal("1299.99")
    assert (
        normalize_url(
            " /catalogue/item_1/index.html#reviews ",
            base_url="https://books.toscrape.com/",
        )
        == "https://books.toscrape.com/catalogue/item_1/index.html"
    )


def test_validate_product_records_tracks_valid_and_invalid_rows() -> None:
    """Only valid records should survive normalization and business validation."""

    raw_records = [
        {
            "external_id": " SKU-1 ",
            "product_name": "  Valid   Product ",
            "brand": None,
            "category": "  Accessories ",
            "product_url": " /items/sku-1 ",
            "product_url_base": "https://example.com/catalog/",
            "image_url": " /images/sku-1.jpg ",
            "image_url_base": "https://example.com/catalog/",
            "currency": None,
            "listed_price": " €1,299.99 ",
            "sale_price": None,
            "availability": " In stock ",
        },
        {
            "external_id": "SKU-2",
            "product_name": "   ",
            "product_url": "https://example.com/items/sku-2",
            "listed_price": "19.99",
            "availability": "In stock",
        },
        {
            "external_id": "SKU-3",
            "product_name": "Bad Price Product",
            "product_url": "https://example.com/items/sku-3",
            "listed_price": "-5.00",
            "availability": "In stock",
        },
    ]

    summary = validate_product_records(raw_records, source_name="site_a")

    assert summary.total_records == 3
    assert summary.valid_count == 1
    assert summary.invalid_count == 2

    valid_record = summary.valid_records[0]
    assert valid_record.product_name == "Valid Product"
    assert valid_record.category == "Accessories"
    assert valid_record.currency == "EUR"
    assert valid_record.listed_price == Decimal("1299.99")
    assert valid_record.product_url == "https://example.com/items/sku-1"
    assert valid_record.image_url == "https://example.com/images/sku-1.jpg"

    error_messages = ["; ".join(record.errors) for record in summary.invalid_records]
    assert any("empty product_name" in message for message in error_messages)
    assert any("negative listed_price" in message for message in error_messages)
