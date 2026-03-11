from __future__ import annotations

"""Smoke tests covering the CLI happy path with a temporary SQLite database."""

from pathlib import Path

from sqlalchemy import create_engine, text

from pricemonitor.main import main


def write_test_config(root: Path) -> Path:
    """Create a minimal test configuration tree under a temporary directory."""

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
                f"  url: sqlite:///{(root / 'test.db').as_posix()}",
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
                "scraper: dummy_site_a",
                "fetcher: http",
                "timeout_seconds: 10",
                "sample_products:",
                "  - external_id: SKU-1",
                "    product_name: Test Shoe",
                "    brand: TestBrand",
                "    category: Footwear",
                "    product_url: https://example.com/p/sku-1",
                "    currency: USD",
                "    listed_price: '100.00'",
                "    sale_price: '90.00'",
                "    availability: in_stock",
                "  - external_id: SKU-2",
                "    product_name: Test Jacket",
                "    brand: TestBrand",
                "    category: Apparel",
                "    product_url: https://example.com/p/sku-2",
                "    currency: USD",
                "    listed_price: '120.00'",
                "    sale_price:",
                "    availability: out_of_stock",
            ]
        ),
        encoding="utf-8",
    )

    return configs_dir / "settings.yaml"


def test_init_db_and_scrape_smoke(tmp_path: Path) -> None:
    """Verify schema creation and a single dummy scrape end to end."""

    config_path = write_test_config(tmp_path)
    db_path = tmp_path / "test.db"

    assert main(["--config", str(config_path), "init-db"]) == 0
    assert db_path.exists()

    assert main(["--config", str(config_path), "scrape", "--source", "site_a"]) == 0

    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    with engine.connect() as connection:
        scrape_runs_count = connection.execute(text("SELECT COUNT(*) FROM scrape_runs")).scalar_one()
        snapshots_count = connection.execute(text("SELECT COUNT(*) FROM product_snapshots")).scalar_one()

    assert scrape_runs_count == 1
    assert snapshots_count == 2


def test_show_config_smoke(tmp_path: Path, capsys) -> None:
    """Verify that the CLI prints the resolved configuration."""

    config_path = write_test_config(tmp_path)

    assert main(["--config", str(config_path), "show-config"]) == 0
    output = capsys.readouterr().out

    assert "Price Monitor ETL" in output
    assert "site_a" in output
    assert "sqlite" in output
