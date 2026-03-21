from __future__ import annotations

"""Tests for business-facing exports that are built from processed datasets."""

import csv
import json
from pathlib import Path

from pricemonitor.main import main
from pricemonitor.services.export import ExportService


def write_export_test_config(root: Path) -> Path:
    """Create a minimal config tree for export-only tests."""

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
                f"  url: sqlite:///{(root / 'exports.db').as_posix()}",
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


def seed_processed_files(processed_root: Path) -> None:
    """Write processed JSON inputs that the export layer can turn into client-facing outputs."""

    source_dir = processed_root / "site_a"
    source_dir.mkdir(parents=True, exist_ok=True)

    latest_products_rows = [
        {
            "source_name": "site_a",
            "external_id": "SKU-1",
            "product_name": "Exported Product",
            "brand": "ExampleBrand",
            "category": "Accessories",
            "product_url": "https://example.com/items/sku-1",
            "image_url": "https://example.com/images/sku-1.jpg",
            "currency": "USD",
            "listed_price": "25.00",
            "sale_price": "24.00",
            "effective_price": "24.00",
            "is_discounted": True,
            "availability": "in_stock",
            "availability_group": "available",
            "scraped_at": "2026-03-21T11:01:00+00:00",
            "scrape_date": "2026-03-21",
            "scrape_run_id": 2,
        }
    ]
    price_changes_rows = [
        {
            "source_name": "site_a",
            "external_id": "SKU-1",
            "product_name": "Exported Product",
            "currency": "USD",
            "previous_price": "20.00",
            "current_price": "24.00",
            "absolute_difference": "4.00",
            "percentage_difference": "20.00",
            "change_direction": "increase",
            "change_bucket": "15%+",
            "change_magnitude": "major",
            "is_price_increase": True,
            "is_price_decrease": False,
            "changed_at": "2026-03-21T11:02:00+00:00",
            "changed_date": "2026-03-21",
            "scrape_run_id": 2,
        }
    ]
    run_summary_rows = [
        {
            "scrape_run_id": 2,
            "source_name": "site_a",
            "status": "succeeded",
            "success_flag": True,
            "started_at": "2026-03-21T11:00:00+00:00",
            "finished_at": "2026-03-21T11:02:00+00:00",
            "duration_seconds": 120.0,
            "records_fetched": 1,
            "records_inserted": 1,
            "insert_rate": 1.0,
            "validity_rate": 1.0,
            "error_message": None,
        },
        {
            "scrape_run_id": 1,
            "source_name": "site_a",
            "status": "succeeded",
            "success_flag": True,
            "started_at": "2026-03-21T10:00:00+00:00",
            "finished_at": "2026-03-21T10:02:00+00:00",
            "duration_seconds": 120.0,
            "records_fetched": 1,
            "records_inserted": 1,
            "insert_rate": 1.0,
            "validity_rate": 1.0,
            "error_message": None,
        },
    ]

    (source_dir / "latest_products_processed.json").write_text(
        json.dumps(latest_products_rows, indent=2),
        encoding="utf-8",
    )
    (source_dir / "price_changes_processed.json").write_text(
        json.dumps(price_changes_rows, indent=2),
        encoding="utf-8",
    )
    (source_dir / "run_summary_processed.json").write_text(
        json.dumps(run_summary_rows, indent=2),
        encoding="utf-8",
    )


def test_export_service_writes_csv_and_json_outputs(tmp_path: Path) -> None:
    """The export service should build business-facing files from processed inputs."""

    seed_processed_files(tmp_path / "processed")

    export_service = ExportService(
        processed_dir=tmp_path / "processed",
        exports_dir=tmp_path / "exports",
    )
    report = export_service.export_source_report("site_a", recent_limit=25)

    counts = report.counts()
    assert counts["latest_products"] == 1
    assert counts["price_changes"] == 1
    assert counts["run_summary"] == 2

    latest_products_csv = tmp_path / "exports" / "site_a" / "latest_products.csv"
    price_changes_json = tmp_path / "exports" / "site_a" / "price_changes.json"
    run_summary_json = tmp_path / "exports" / "site_a" / "run_summary.json"

    assert latest_products_csv.exists()
    assert price_changes_json.exists()
    assert run_summary_json.exists()

    with latest_products_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["external_id"] == "SKU-1"
    assert rows[0]["effective_price"] == "24.00"
    assert rows[0]["availability_group"] == "available"

    change_rows = json.loads(price_changes_json.read_text(encoding="utf-8"))
    assert len(change_rows) == 1
    assert change_rows[0]["change_direction"] == "increase"
    assert change_rows[0]["change_bucket"] == "15%+"

    run_rows = json.loads(run_summary_json.read_text(encoding="utf-8"))
    assert len(run_rows) == 2
    assert run_rows[0]["success_flag"] is True


def test_export_cli_generates_business_outputs(tmp_path: Path, capsys) -> None:
    """The CLI should generate export files from the processed layer."""

    config_path = write_export_test_config(tmp_path)
    seed_processed_files(tmp_path / "data" / "processed")

    assert main(["--config", str(config_path), "export", "--source", "site_a", "--limit", "10"]) == 0
    output = capsys.readouterr().out

    assert "Export completed for site_a: latest_products=1 price_changes=1 run_summary=2" in output

    export_dir = tmp_path / "data" / "exports" / "site_a"
    assert (export_dir / "latest_products.csv").exists()
    assert (export_dir / "latest_products.json").exists()
    assert (export_dir / "price_changes.csv").exists()
    assert (export_dir / "price_changes.json").exists()
    assert (export_dir / "run_summary.csv").exists()
    assert (export_dir / "run_summary.json").exists()
