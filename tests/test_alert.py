from __future__ import annotations

"""Tests for stakeholder-facing alerts built on top of processed datasets."""

import csv
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pricemonitor.main import main
from pricemonitor.models.schemas import PriceChangeEventCreate, ProductRecord
from pricemonitor.services.alert import AlertService
from pricemonitor.services.process import ProcessService
from pricemonitor.storage.database import create_engine_from_url, create_session_factory, create_test_schema
from pricemonitor.storage.repositories import (
    PriceChangeEventRepository,
    ProductSnapshotRepository,
    ScrapeRunRepository,
)


def write_alert_test_config(root: Path) -> Path:
    """Create a minimal config tree for alert-only tests."""

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
                f"  url: sqlite:///{(root / 'alerts.db').as_posix()}",
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


def seed_alert_data(session) -> None:
    """Insert runs, snapshots, and change events so alert generation has meaningful inputs."""

    run_repo = ScrapeRunRepository(session)
    snapshot_repo = ProductSnapshotRepository(session)
    change_repo = PriceChangeEventRepository(session)

    previous_run = run_repo.create_scrape_run(
        "site_a",
        started_at=datetime(2026, 3, 22, 10, 0, tzinfo=timezone.utc),
    )
    session.commit()

    snapshot_repo.insert_product_snapshots(
        scrape_run_id=previous_run.id,
        source_name="site_a",
        products=[
            ProductRecord(
                external_id="SKU-1",
                product_name="Tracked Drop Product",
                brand="ExampleBrand",
                category="Accessories",
                product_url="https://example.com/items/sku-1",
                image_url="https://example.com/images/sku-1.jpg",
                currency="USD",
                listed_price=Decimal("20.00"),
                sale_price=None,
                availability="in_stock",
            ),
            ProductRecord(
                external_id="SKU-3",
                product_name="Tracked Increase Product",
                brand="ExampleBrand",
                category="Accessories",
                product_url="https://example.com/items/sku-3",
                image_url="https://example.com/images/sku-3.jpg",
                currency="USD",
                listed_price=Decimal("10.00"),
                sale_price=None,
                availability="in_stock",
            ),
        ],
        scraped_at=datetime(2026, 3, 22, 10, 1, tzinfo=timezone.utc),
    )
    run_repo.complete_scrape_run(
        previous_run.id,
        records_fetched=2,
        records_inserted=2,
        finished_at=datetime(2026, 3, 22, 10, 2, tzinfo=timezone.utc),
    )
    session.commit()

    current_run = run_repo.create_scrape_run(
        "site_a",
        started_at=datetime(2026, 3, 22, 11, 0, tzinfo=timezone.utc),
    )
    session.commit()

    snapshot_repo.insert_product_snapshots(
        scrape_run_id=current_run.id,
        source_name="site_a",
        products=[
            ProductRecord(
                external_id="SKU-1",
                product_name="Tracked Drop Product",
                brand="ExampleBrand",
                category="Accessories",
                product_url="https://example.com/items/sku-1",
                image_url="https://example.com/images/sku-1.jpg",
                currency="USD",
                listed_price=Decimal("18.00"),
                sale_price=None,
                availability="in_stock",
            ),
            ProductRecord(
                external_id="SKU-2",
                product_name="Brand New Product",
                brand="ExampleBrand",
                category="Accessories",
                product_url="https://example.com/items/sku-2",
                image_url="https://example.com/images/sku-2.jpg",
                currency="USD",
                listed_price=Decimal("30.00"),
                sale_price=None,
                availability="in_stock",
            ),
            ProductRecord(
                external_id="SKU-3",
                product_name="Tracked Increase Product",
                brand="ExampleBrand",
                category="Accessories",
                product_url="https://example.com/items/sku-3",
                image_url="https://example.com/images/sku-3.jpg",
                currency="USD",
                listed_price=Decimal("12.00"),
                sale_price=None,
                availability="in_stock",
            ),
        ],
        scraped_at=datetime(2026, 3, 22, 11, 1, tzinfo=timezone.utc),
    )
    run_repo.complete_scrape_run(
        current_run.id,
        records_fetched=3,
        records_inserted=3,
        finished_at=datetime(2026, 3, 22, 11, 2, tzinfo=timezone.utc),
    )
    session.commit()

    previous_snapshots = {
        snapshot.external_id: snapshot
        for snapshot in snapshot_repo.list_for_scrape_run(previous_run.id)
    }
    current_snapshots = {
        snapshot.external_id: snapshot
        for snapshot in snapshot_repo.list_for_scrape_run(current_run.id)
    }

    change_repo.insert_price_change_events(
        [
            PriceChangeEventCreate(
                source_name="site_a",
                external_id="SKU-1",
                scrape_run_id=current_run.id,
                previous_snapshot_id=previous_snapshots["SKU-1"].id,
                current_snapshot_id=current_snapshots["SKU-1"].id,
                product_name="Tracked Drop Product",
                currency="USD",
                previous_price=Decimal("20.00"),
                current_price=Decimal("18.00"),
                absolute_difference=Decimal("2.00"),
                percentage_difference=Decimal("10.00"),
                changed_at=datetime(2026, 3, 22, 11, 2, tzinfo=timezone.utc),
            ),
            PriceChangeEventCreate(
                source_name="site_a",
                external_id="SKU-3",
                scrape_run_id=current_run.id,
                previous_snapshot_id=previous_snapshots["SKU-3"].id,
                current_snapshot_id=current_snapshots["SKU-3"].id,
                product_name="Tracked Increase Product",
                currency="USD",
                previous_price=Decimal("10.00"),
                current_price=Decimal("12.00"),
                absolute_difference=Decimal("2.00"),
                percentage_difference=Decimal("20.00"),
                changed_at=datetime(2026, 3, 22, 11, 2, tzinfo=timezone.utc),
            ),
        ]
    )
    session.commit()


def test_alert_service_writes_summary_and_reports(tmp_path: Path) -> None:
    """The alert service should create summary artifacts plus focused CSV reports."""

    db_path = tmp_path / "service.db"
    engine = create_engine_from_url(f"sqlite:///{db_path.as_posix()}")
    create_test_schema(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        seed_alert_data(session)

        process_service = ProcessService(
            processed_dir=tmp_path / "processed",
            scrape_run_repo=ScrapeRunRepository(session),
            snapshot_repo=ProductSnapshotRepository(session),
            change_event_repo=PriceChangeEventRepository(session),
        )
        process_service.process_source_data("site_a", recent_limit=25)

        alert_service = AlertService(
            processed_dir=tmp_path / "processed",
            exports_dir=tmp_path / "exports",
            scrape_run_repo=ScrapeRunRepository(session),
            snapshot_repo=ProductSnapshotRepository(session),
        )
        report = alert_service.generate_source_alerts(
            "site_a",
            recent_limit=10,
            major_threshold_pct=Decimal("15.00"),
        )

    counts = report.counts()
    assert counts["top_price_changes"] == 2
    assert counts["price_drops"] == 1
    assert counts["major_increases"] == 1
    assert counts["new_products"] == 1

    alerts_dir = tmp_path / "exports" / "site_a" / "alerts"
    summary_json = alerts_dir / "alerts_summary.json"
    summary_txt = alerts_dir / "alerts_summary.txt"
    top_changes_csv = alerts_dir / "top_price_changes.csv"
    price_drops_csv = alerts_dir / "price_drops.csv"
    major_increases_csv = alerts_dir / "major_increases.csv"
    new_products_csv = alerts_dir / "new_products.csv"

    assert summary_json.exists()
    assert summary_txt.exists()
    assert top_changes_csv.exists()
    assert price_drops_csv.exists()
    assert major_increases_csv.exists()
    assert new_products_csv.exists()

    summary = json.loads(summary_json.read_text(encoding="utf-8"))
    assert summary["source_name"] == "site_a"
    assert summary["baseline_mode"] is False
    assert summary["latest_successful_run_id"] == 2
    assert summary["new_products_count"] == 1

    with top_changes_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert rows[0]["external_id"] == "SKU-3"
    assert rows[0]["percentage_difference"] == "20.00"

    with price_drops_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["external_id"] == "SKU-1"

    with major_increases_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["external_id"] == "SKU-3"

    with new_products_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["external_id"] == "SKU-2"

    summary_text = summary_txt.read_text(encoding="utf-8")
    assert "Alerts for site_a" in summary_text
    assert "New products exported: 1" in summary_text


def test_alert_cli_generates_files_from_processed_inputs(tmp_path: Path, capsys) -> None:
    """The alert CLI should generate files after the processed layer exists."""

    config_path = write_alert_test_config(tmp_path)
    db_path = tmp_path / "alerts.db"

    engine = create_engine_from_url(f"sqlite:///{db_path.as_posix()}")
    create_test_schema(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        seed_alert_data(session)

    assert main(["--config", str(config_path), "process", "--source", "site_a", "--limit", "10"]) == 0
    capsys.readouterr()

    assert (
        main(
            [
                "--config",
                str(config_path),
                "alert",
                "--source",
                "site_a",
                "--limit",
                "10",
                "--major-threshold",
                "15",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out

    assert (
        "Alert completed for site_a: top_price_changes=2 price_drops=1 "
        "major_increases=1 new_products=1"
    ) in output

    alerts_dir = tmp_path / "data" / "exports" / "site_a" / "alerts"
    assert (alerts_dir / "alerts_summary.json").exists()
    assert (alerts_dir / "alerts_summary.txt").exists()
    assert (alerts_dir / "top_price_changes.csv").exists()
    assert (alerts_dir / "price_drops.csv").exists()
    assert (alerts_dir / "major_increases.csv").exists()
    assert (alerts_dir / "new_products.csv").exists()
