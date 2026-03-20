from __future__ import annotations

"""Unit tests for change detection between consecutive scrape runs."""

from datetime import datetime, timezone
from decimal import Decimal

from pricemonitor.models.db_models import ProductSnapshot
from pricemonitor.services.change_detection import detect_price_changes


def _build_snapshot(
    *,
    snapshot_id: int,
    scrape_run_id: int,
    external_id: str,
    product_name: str,
    listed_price: str,
) -> ProductSnapshot:
    """Build lightweight snapshot objects for pure change-detection tests."""

    return ProductSnapshot(
        id=snapshot_id,
        scrape_run_id=scrape_run_id,
        source_name="site_a",
        external_id=external_id,
        product_name=product_name,
        brand=None,
        category="Books",
        product_url=f"https://example.com/{external_id}",
        currency="USD",
        listed_price=Decimal(listed_price),
        sale_price=None,
        availability="in_stock",
        payload={},
        scraped_at=datetime(2026, 3, 20, tzinfo=timezone.utc),
    )


def test_detect_price_changes_between_runs() -> None:
    """A changed price should generate one event with absolute and percentage differences."""

    previous_snapshots = [
        _build_snapshot(
            snapshot_id=1,
            scrape_run_id=10,
            external_id="SKU-1",
            product_name="Tracked Product",
            listed_price="20.00",
        )
    ]
    current_snapshots = [
        _build_snapshot(
            snapshot_id=2,
            scrape_run_id=11,
            external_id="SKU-1",
            product_name="Tracked Product",
            listed_price="25.00",
        )
    ]

    events = detect_price_changes(
        source_name="site_a",
        scrape_run_id=11,
        current_snapshots=current_snapshots,
        previous_snapshots=previous_snapshots,
        changed_at=datetime(2026, 3, 20, 1, 0, tzinfo=timezone.utc),
    )

    assert len(events) == 1
    event = events[0]
    assert event.external_id == "SKU-1"
    assert event.previous_price == Decimal("20.00")
    assert event.current_price == Decimal("25.00")
    assert event.absolute_difference == Decimal("5.00")
    assert event.percentage_difference == Decimal("25.00")


def test_detect_price_changes_ignores_new_products_and_unchanged_prices() -> None:
    """Only products with a previous baseline and a real price movement should emit events."""

    previous_snapshots = [
        _build_snapshot(
            snapshot_id=1,
            scrape_run_id=10,
            external_id="SKU-1",
            product_name="Stable Product",
            listed_price="19.99",
        )
    ]
    current_snapshots = [
        _build_snapshot(
            snapshot_id=2,
            scrape_run_id=11,
            external_id="SKU-1",
            product_name="Stable Product",
            listed_price="19.99",
        ),
        _build_snapshot(
            snapshot_id=3,
            scrape_run_id=11,
            external_id="SKU-2",
            product_name="New Product",
            listed_price="9.99",
        ),
    ]

    events = detect_price_changes(
        source_name="site_a",
        scrape_run_id=11,
        current_snapshots=current_snapshots,
        previous_snapshots=previous_snapshots,
    )

    assert events == []
