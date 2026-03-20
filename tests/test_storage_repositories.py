from __future__ import annotations

"""Tests for repository lifecycle helpers, change-event persistence, and raw-page archives."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pricemonitor.models.schemas import ArchivedPageRecord, ProductRecord, PriceChangeEventCreate
from pricemonitor.storage.database import create_engine_from_url, create_session_factory, init_db
from pricemonitor.storage.repositories import (
    ProductSnapshotRepository,
    RawPageArchiveRepository,
    ScrapeRunRepository,
    PriceChangeEventRepository,
)


def test_scrape_run_repository_lifecycle_and_queries(tmp_path: Path) -> None:
    """Scrape run helpers should create, complete, fail, and list runs consistently."""

    db_path = tmp_path / "storage.db"
    engine = create_engine_from_url(f"sqlite:///{db_path.as_posix()}")
    init_db(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        repo = ScrapeRunRepository(session)

        completed_run = repo.create_scrape_run(
            "site_a",
            started_at=datetime(2026, 3, 20, tzinfo=timezone.utc),
        )
        session.commit()

        repo.complete_scrape_run(
            completed_run.id,
            records_fetched=3,
            records_inserted=2,
            finished_at=datetime(2026, 3, 20, 0, 5, tzinfo=timezone.utc),
        )
        session.commit()

        failed_run = repo.create_scrape_run(
            "site_b",
            started_at=datetime(2026, 3, 20, 1, 0, tzinfo=timezone.utc),
        )
        session.commit()

        repo.fail_scrape_run(
            failed_run.id,
            error_message="boom",
            finished_at=datetime(2026, 3, 20, 1, 1, tzinfo=timezone.utc),
        )
        session.commit()

        recent_runs = repo.list_recent(limit=5)
        site_a_runs = repo.list_recent(source_name="site_a", limit=5)
        previous_success = repo.get_previous_successful_run(
            source_name="site_b",
            before_scrape_run_id=failed_run.id,
        )

    assert len(recent_runs) == 2
    assert recent_runs[0].status == "failed"
    assert recent_runs[1].status == "succeeded"

    assert len(site_a_runs) == 1
    assert site_a_runs[0].records_fetched == 3
    assert site_a_runs[0].records_inserted == 2


def test_product_snapshot_repository_inserts_validated_products(tmp_path: Path) -> None:
    """Snapshot repository should build clean database rows from validated product records."""

    db_path = tmp_path / "snapshots.db"
    engine = create_engine_from_url(f"sqlite:///{db_path.as_posix()}")
    init_db(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        run_repo = ScrapeRunRepository(session)
        snapshot_repo = ProductSnapshotRepository(session)

        scrape_run = run_repo.create_scrape_run("site_a")
        session.commit()

        inserted_count = snapshot_repo.insert_product_snapshots(
            scrape_run_id=scrape_run.id,
            source_name="site_a",
            products=[
                ProductRecord(
                    external_id="SKU-1",
                    product_name="Stored Product",
                    brand="ExampleBrand",
                    category="Accessories",
                    product_url="https://example.com/items/sku-1",
                    image_url="https://example.com/images/sku-1.jpg",
                    currency="USD",
                    listed_price=Decimal("19.99"),
                    sale_price=None,
                    availability="in_stock",
                )
            ],
            scraped_at=datetime(2026, 3, 20, tzinfo=timezone.utc),
        )
        session.commit()

        stored = snapshot_repo.list_latest_for_source("site_a")
        run_snapshots = snapshot_repo.list_for_scrape_run(scrape_run.id)

    assert inserted_count == 1
    assert len(stored) == 1
    assert len(run_snapshots) == 1
    assert stored[0].external_id == "SKU-1"
    assert stored[0].product_name == "Stored Product"


def test_raw_page_archive_repository_writes_html_and_manifest(tmp_path: Path) -> None:
    """Raw-page archive support should write readable files plus a manifest."""

    raw_repo = RawPageArchiveRepository(tmp_path / "raw-pages")

    written_files = raw_repo.archive_pages(
        source_name="site_a",
        scrape_run_id=7,
        pages=[
            ArchivedPageRecord(
                page_type="listing",
                page_url="https://example.com/catalog/page-1.html",
                content="<html>listing</html>",
            ),
            ArchivedPageRecord(
                page_type="detail",
                page_url="https://example.com/catalog/item-1.html",
                content="<html>detail</html>",
            ),
        ],
    )

    run_dir = tmp_path / "raw-pages" / "site_a" / "run_7"
    manifest_path = run_dir / "manifest.json"

    assert len(written_files) == 2
    assert all(path.exists() for path in written_files)
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(manifest) == 2
    assert manifest[0]["page_type"] == "listing"
    assert manifest[1]["page_type"] == "detail"

def test_price_change_event_repository_inserts_detected_changes(tmp_path: Path) -> None:
    """Change-event repository should persist computed price differences for later inspection."""

    db_path = tmp_path / "changes.db"
    engine = create_engine_from_url(f"sqlite:///{db_path.as_posix()}")
    init_db(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        run_repo = ScrapeRunRepository(session)
        snapshot_repo = ProductSnapshotRepository(session)
        change_repo = PriceChangeEventRepository(session)

        previous_run = run_repo.create_scrape_run("site_a")
        session.commit()
        snapshot_repo.insert_product_snapshots(
            scrape_run_id=previous_run.id,
            source_name="site_a",
            products=[
                ProductRecord(
                    external_id="SKU-1",
                    product_name="Tracked Product",
                    brand=None,
                    category="Accessories",
                    product_url="https://example.com/items/sku-1",
                    image_url=None,
                    currency="USD",
                    listed_price=Decimal("20.00"),
                    sale_price=None,
                    availability="in_stock",
                )
            ],
            scraped_at=datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc),
        )
        run_repo.complete_scrape_run(previous_run.id, records_fetched=1, records_inserted=1)
        session.commit()

        current_run = run_repo.create_scrape_run("site_a")
        session.commit()
        snapshot_repo.insert_product_snapshots(
            scrape_run_id=current_run.id,
            source_name="site_a",
            products=[
                ProductRecord(
                    external_id="SKU-1",
                    product_name="Tracked Product",
                    brand=None,
                    category="Accessories",
                    product_url="https://example.com/items/sku-1",
                    image_url=None,
                    currency="USD",
                    listed_price=Decimal("25.00"),
                    sale_price=None,
                    availability="in_stock",
                )
            ],
            scraped_at=datetime(2026, 3, 20, 1, 0, tzinfo=timezone.utc),
        )
        session.commit()

        previous_snapshots = snapshot_repo.list_for_scrape_run(previous_run.id)
        current_snapshots = snapshot_repo.list_for_scrape_run(current_run.id)

        inserted_count = change_repo.insert_price_change_events(
            [
                PriceChangeEventCreate(
                    source_name="site_a",
                    external_id="SKU-1",
                    scrape_run_id=current_run.id,
                    previous_snapshot_id=previous_snapshots[0].id,
                    current_snapshot_id=current_snapshots[0].id,
                    product_name="Tracked Product",
                    currency="USD",
                    previous_price=Decimal("20.00"),
                    current_price=Decimal("25.00"),
                    absolute_difference=Decimal("5.00"),
                    percentage_difference=Decimal("25.00"),
                    changed_at=datetime(2026, 3, 20, 1, 0, tzinfo=timezone.utc),
                )
            ]
        )
        session.commit()

        stored_events = change_repo.list_latest_for_source("site_a")

    assert inserted_count == 1
    assert len(stored_events) == 1
    assert stored_events[0].external_id == "SKU-1"
    assert stored_events[0].absolute_difference == Decimal("5.00")
    assert stored_events[0].percentage_difference == Decimal("25.00")