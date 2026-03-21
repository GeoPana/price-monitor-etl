from __future__ import annotations

"""Tests for the read-only FastAPI layer."""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from pricemonitor.api.app import create_app
from pricemonitor.models.schemas import PriceChangeEventCreate, ProductRecord
from pricemonitor.services.alert import AlertService
from pricemonitor.services.export import ExportService
from pricemonitor.services.process import ProcessService
from pricemonitor.storage.database import create_engine_from_url, create_session_factory, create_test_schema
from pricemonitor.storage.repositories import (
    PriceChangeEventRepository,
    ProductSnapshotRepository,
    ScrapeRunRepository,
)


def write_api_test_config(root: Path) -> Path:
    """Create a minimal config tree for API tests."""

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
                f"  url: sqlite:///{(root / 'api.db').as_posix()}",
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
                "base_url: https://example.com/site-a",
                "scraper: site_a",
                "fetcher: http",
                "timeout_seconds: 10",
            ]
        ),
        encoding="utf-8",
    )

    (sources_dir / "site_b.yaml").write_text(
        "\n".join(
            [
                "name: site_b",
                "enabled: true",
                "base_url: https://example.com/site-b",
                "scraper: site_b",
                "fetcher: browser",
                "timeout_seconds: 10",
            ]
        ),
        encoding="utf-8",
    )

    return configs_dir / "settings.yaml"


def seed_api_data(session) -> None:
    """Insert runs, snapshots, and change events so the API has meaningful data to expose."""

    run_repo = ScrapeRunRepository(session)
    snapshot_repo = ProductSnapshotRepository(session)
    change_repo = PriceChangeEventRepository(session)

    previous_run = run_repo.create_scrape_run(
        "site_a",
        started_at=datetime(2026, 3, 23, 10, 0, tzinfo=timezone.utc),
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
        scraped_at=datetime(2026, 3, 23, 10, 1, tzinfo=timezone.utc),
    )
    run_repo.complete_scrape_run(
        previous_run.id,
        records_fetched=2,
        records_inserted=2,
        finished_at=datetime(2026, 3, 23, 10, 2, tzinfo=timezone.utc),
    )
    session.commit()

    current_run = run_repo.create_scrape_run(
        "site_a",
        started_at=datetime(2026, 3, 23, 11, 0, tzinfo=timezone.utc),
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
        scraped_at=datetime(2026, 3, 23, 11, 1, tzinfo=timezone.utc),
    )
    run_repo.complete_scrape_run(
        current_run.id,
        records_fetched=3,
        records_inserted=3,
        finished_at=datetime(2026, 3, 23, 11, 2, tzinfo=timezone.utc),
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
                changed_at=datetime(2026, 3, 23, 11, 2, tzinfo=timezone.utc),
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
                changed_at=datetime(2026, 3, 23, 11, 2, tzinfo=timezone.utc),
            ),
        ]
    )
    session.commit()


def generate_api_outputs(root: Path, session) -> None:
    """Generate the processed, exported, and alert datasets that the API exposes."""

    process_service = ProcessService(
        processed_dir=root / "data" / "processed",
        scrape_run_repo=ScrapeRunRepository(session),
        snapshot_repo=ProductSnapshotRepository(session),
        change_event_repo=PriceChangeEventRepository(session),
    )
    process_service.process_source_data("site_a", recent_limit=25)

    export_service = ExportService(
        processed_dir=root / "data" / "processed",
        exports_dir=root / "data" / "exports",
    )
    export_service.export_source_report("site_a", recent_limit=25)

    alert_service = AlertService(
        processed_dir=root / "data" / "processed",
        exports_dir=root / "data" / "exports",
        scrape_run_repo=ScrapeRunRepository(session),
        snapshot_repo=ProductSnapshotRepository(session),
    )
    alert_service.generate_source_alerts(
        "site_a",
        recent_limit=25,
        major_threshold_pct=Decimal("15.00"),
    )


def test_api_health_and_sources_smoke(tmp_path: Path) -> None:
    """The API should expose health metadata and configured sources."""

    config_path = write_api_test_config(tmp_path)
    app = create_app(config_path)

    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200

        payload = response.json()
        assert payload["status"] == "ok"
        assert payload["app_name"] == "Price Monitor ETL"
        assert payload["environment"] == "test"
        assert payload["configured_sources"] == 2

        response = client.get("/sources")
        assert response.status_code == 200

        sources = response.json()
        assert len(sources) == 2
        assert {source["name"] for source in sources} == {"site_a", "site_b"}


def test_api_read_endpoints_return_expected_data(tmp_path: Path) -> None:
    """The API should expose run history, latest products, price changes, and alerts."""

    config_path = write_api_test_config(tmp_path)
    db_path = tmp_path / "api.db"
    engine = create_engine_from_url(f"sqlite:///{db_path.as_posix()}")
    create_test_schema(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        seed_api_data(session)
        generate_api_outputs(tmp_path, session)

    app = create_app(config_path)
    with TestClient(app) as client:
        response = client.get("/runs", params={"source": "site_a", "limit": 10})
        assert response.status_code == 200
        runs = response.json()
        assert len(runs) == 2
        assert runs[0]["scrape_run_id"] == 2
        assert runs[0]["success_flag"] is True

        response = client.get("/products/latest", params={"source": "site_a", "limit": 10})
        assert response.status_code == 200
        products = response.json()
        assert len(products) == 3
        assert {product["external_id"] for product in products} == {"SKU-1", "SKU-2", "SKU-3"}

        response = client.get("/price-changes", params={"source": "site_a", "limit": 10})
        assert response.status_code == 200
        price_changes = response.json()
        assert len(price_changes) == 2
        assert price_changes[0]["external_id"] == "SKU-3"
        assert price_changes[0]["change_direction"] == "increase"

        response = client.get("/alerts/summary", params={"source": "site_a"})
        assert response.status_code == 200
        summary = response.json()
        assert summary["source_name"] == "site_a"
        assert summary["baseline_mode"] is False
        assert summary["new_products_count"] == 1

        response = client.get("/alerts/top-price-changes", params={"source": "site_a", "limit": 10})
        assert response.status_code == 200
        top_changes = response.json()
        assert len(top_changes) == 2
        assert top_changes[0]["external_id"] == "SKU-3"

        response = client.get("/alerts/new-products", params={"source": "site_a", "limit": 10})
        assert response.status_code == 200
        new_products = response.json()
        assert len(new_products) == 1
        assert new_products[0]["external_id"] == "SKU-2"


def test_api_returns_not_found_for_unknown_source(tmp_path: Path) -> None:
    """Unknown sources should fail with a clean 404 instead of leaking internals."""

    config_path = write_api_test_config(tmp_path)
    app = create_app(config_path)

    with TestClient(app) as client:
        response = client.get("/products/latest", params={"source": "missing_source"})
        assert response.status_code == 404
        assert "Unknown source" in response.json()["detail"]
