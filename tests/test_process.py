from __future__ import annotations

"""Tests for the processed-data service and CLI flow."""

import csv
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pricemonitor.main import main
from pricemonitor.models.schemas import PriceChangeEventCreate, ProductRecord
from pricemonitor.services.process import ProcessService
from pricemonitor.storage.database import create_engine_from_url, create_session_factory, create_test_schema
from pricemonitor.storage.repositories import (
    PriceChangeEventRepository,
    ProductSnapshotRepository,
    ScrapeRunRepository,
)


def write_process_test_config(root: Path) -> Path:
    """Create a minimal config tree for process-only tests."""

    configs_dir = root / "configs"
    sources_dir = configs_dir / "sources"
    configs_dir.mkdir(parents=True, exist_ok=True)
    sources_dir.mkdir(parents=True, exist_ok=True)

    (configs_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "app:",
                "  name: Price Monitor ETL",
                "  environment: test",
                "database:",
                f"  url: sqlite:///{(root / 'processed.db').as_posix()}",
                "logging:",
                "  level: INFO",
                "  file: logs/test.log",
                "directories:",
                "  raw: data/raw",
                "  processed: data/processed",
                "  exports: data/exports",
                "  logs: logs",
            ]
        ),
        encoding="utf-8",
    )

    (sources_dir / "site_a.yaml").write_text(
        "\n".join(
            [
                "name: site_a",
                "enabled: true",
                "base_url: https://example.com",
                "scraper: site_a",
                "fetcher: http",
                "timeout_seconds: 10",
            ]
        ),
        encoding="utf-8",
    )

    return configs_dir / "settings.yaml"


def seed_process_data(session) -> None:
    """Insert runs, snapshots, and a change event so processing has real material to curate."""

    run_repo = ScrapeRunRepository(session)
    snapshot_repo = ProductSnapshotRepository(session)
    change_repo = PriceChangeEventRepository(session)

    previous_run = run_repo.create_scrape_run(
        "site_a",
        started_at=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc),
    )
    session.commit()

    snapshot_repo.insert_product_snapshots(
        scrape_run_id=previous_run.id,
        source_name="site_a",
        products=[
            ProductRecord(
                external_id="SKU-1",
                product_name="Processed Product",
                brand="ExampleBrand",
                category="Accessories",
                product_url="https://example.com/items/sku-1",
                image_url="https://example.com/images/sku-1.jpg",
                currency="USD",
                listed_price=Decimal("20.00"),
                sale_price=None,
                availability="in_stock",
            )
        ],
        scraped_at=datetime(2026, 3, 21, 10, 1, tzinfo=timezone.utc),
    )
    run_repo.complete_scrape_run(
        previous_run.id,
        records_fetched=1,
        records_inserted=1,
        finished_at=datetime(2026, 3, 21, 10, 2, tzinfo=timezone.utc),
    )
    session.commit()

    current_run = run_repo.create_scrape_run(
        "site_a",
        started_at=datetime(2026, 3, 21, 11, 0, tzinfo=timezone.utc),
    )
    session.commit()

    snapshot_repo.insert_product_snapshots(
        scrape_run_id=current_run.id,
        source_name="site_a",
        products=[
            ProductRecord(
                external_id="SKU-1",
                product_name="Processed Product",
                brand="ExampleBrand",
                category="Accessories",
                product_url="https://example.com/items/sku-1",
                image_url="https://example.com/images/sku-1.jpg",
                currency="USD",
                listed_price=Decimal("25.00"),
                sale_price=Decimal("24.00"),
                availability="in_stock",
            )
        ],
        scraped_at=datetime(2026, 3, 21, 11, 1, tzinfo=timezone.utc),
    )
    run_repo.complete_scrape_run(
        current_run.id,
        records_fetched=1,
        records_inserted=1,
        finished_at=datetime(2026, 3, 21, 11, 2, tzinfo=timezone.utc),
    )
    session.commit()

    previous_snapshots = snapshot_repo.list_for_scrape_run(previous_run.id)
    current_snapshots = snapshot_repo.list_for_scrape_run(current_run.id)

    change_repo.insert_price_change_events(
        [
            PriceChangeEventCreate(
                source_name="site_a",
                external_id="SKU-1",
                scrape_run_id=current_run.id,
                previous_snapshot_id=previous_snapshots[0].id,
                current_snapshot_id=current_snapshots[0].id,
                product_name="Processed Product",
                currency="USD",
                previous_price=Decimal("20.00"),
                current_price=Decimal("24.00"),
                absolute_difference=Decimal("4.00"),
                percentage_difference=Decimal("20.00"),
                changed_at=datetime(2026, 3, 21, 11, 2, tzinfo=timezone.utc),
            )
        ]
    )
    session.commit()


def test_process_service_writes_curated_outputs(tmp_path: Path) -> None:
    """The processed-data service should write enriched CSV and JSON datasets."""

    db_path = tmp_path / "service.db"
    engine = create_engine_from_url(f"sqlite:///{db_path.as_posix()}")
    create_test_schema(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        seed_process_data(session)

        process_service = ProcessService(
            processed_dir=tmp_path / "processed",
            scrape_run_repo=ScrapeRunRepository(session),
            snapshot_repo=ProductSnapshotRepository(session),
            change_event_repo=PriceChangeEventRepository(session),
        )
        report = process_service.process_source_data("site_a", recent_limit=25)

    counts = report.counts()
    assert counts["latest_products_processed"] == 1
    assert counts["price_changes_processed"] == 1
    assert counts["run_summary_processed"] == 2

    latest_products_csv = tmp_path / "processed" / "site_a" / "latest_products_processed.csv"
    price_changes_json = tmp_path / "processed" / "site_a" / "price_changes_processed.json"
    run_summary_json = tmp_path / "processed" / "site_a" / "run_summary_processed.json"

    assert latest_products_csv.exists()
    assert price_changes_json.exists()
    assert run_summary_json.exists()

    with latest_products_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["effective_price"] == "24.00"
    assert rows[0]["is_discounted"] == "True"
    assert rows[0]["availability_group"] == "available"

    change_rows = json.loads(price_changes_json.read_text(encoding="utf-8"))
    assert len(change_rows) == 1
    assert change_rows[0]["change_direction"] == "increase"
    assert change_rows[0]["change_bucket"] == "15%+"
    assert change_rows[0]["change_magnitude"] == "major"

    run_rows = json.loads(run_summary_json.read_text(encoding="utf-8"))
    assert len(run_rows) == 2
    assert run_rows[0]["success_flag"] is True
    assert run_rows[0]["insert_rate"] == 1.0
    assert run_rows[0]["validity_rate"] == 1.0


def test_process_cli_generates_processed_outputs(tmp_path: Path, capsys) -> None:
    """The CLI should generate the expected processed files from stored DB records."""

    config_path = write_process_test_config(tmp_path)
    db_path = tmp_path / "processed.db"

    engine = create_engine_from_url(f"sqlite:///{db_path.as_posix()}")
    create_test_schema(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        seed_process_data(session)

    assert main(["--config", str(config_path), "process", "--source", "site_a", "--limit", "10"]) == 0
    output = capsys.readouterr().out

    assert "Process completed for site_a: latest_products=1 price_changes=1 run_summary=2" in output

    processed_dir = tmp_path / "data" / "processed" / "site_a"
    assert (processed_dir / "latest_products_processed.csv").exists()
    assert (processed_dir / "latest_products_processed.json").exists()
    assert (processed_dir / "price_changes_processed.csv").exists()
    assert (processed_dir / "price_changes_processed.json").exists()
    assert (processed_dir / "run_summary_processed.csv").exists()
    assert (processed_dir / "run_summary_processed.json").exists()
