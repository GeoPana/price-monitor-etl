from __future__ import annotations

"""Tests for end-to-end pipeline orchestration."""

import json
from pathlib import Path
from textwrap import dedent

from sqlalchemy import create_engine, text

from pricemonitor.fetchers.base import FetchResponse
from pricemonitor.fetchers.browser_fetcher import BrowserFetcher
from pricemonitor.fetchers.http_fetcher import HttpFetcher
from pricemonitor.main import main

SITE_A_LISTING_HTML = dedent(
    """\
    <html>
      <body>
        <section>
          <article class="product_pod">
            <div class="image_container">
              <a href="catalogue/test-book-one_1/index.html">
                <img src="media/cache/test-book-one-thumb.jpg" alt="Test Book One">
              </a>
            </div>
            <h3>
              <a href="catalogue/test-book-one_1/index.html" title="Test Book One">
                Test Book One
              </a>
            </h3>
            <div class="product_price">
              <p class="price_color">&pound;10.00</p>
              <p class="instock availability">In stock</p>
            </div>
          </article>
        </section>
      </body>
    </html>
    """
)

SITE_A_DETAIL_HTML = dedent(
    """\
    <html>
      <body>
        <ul class="breadcrumb">
          <li><a href="/">Home</a></li>
          <li><a href="/catalogue/category/books_1/index.html">Books</a></li>
          <li><a href="/catalogue/category/books/travel_2/index.html">Travel</a></li>
          <li class="active">Test Book One</li>
        </ul>
        <div class="product_main">
          <h1>Test Book One</h1>
          <p class="price_color">&pound;10.00</p>
          <p class="instock availability">In stock (20 available)</p>
        </div>
        <div class="item active">
          <img src="../../media/cache/test-book-one-large.jpg" alt="Test Book One">
        </div>
        <table class="table table-striped">
          <tr><th>UPC</th><td>UPC-BOOK-1</td></tr>
          <tr><th>Price (incl. tax)</th><td>&pound;10.00</td></tr>
          <tr><th>Availability</th><td>In stock (20 available)</td></tr>
        </table>
      </body>
    </html>
    """
)

SITE_B_RENDERED_HTML = dedent(
    """\
    <html>
      <body>
        <div class="col-md-9">
          <div class="row">
            <div class="col-md-4 col-xl-4 col-lg-4">
              <div class="thumbnail">
                <img class="img-responsive" src="/images/test-laptop-1.jpg" alt="Laptop One">
                <div class="caption">
                  <h4 class="price">$999.99</h4>
                  <h4>
                    <a class="title" href="/test-sites/e-commerce/ajax/product/101" title="Laptop One">
                      Laptop One
                    </a>
                  </h4>
                  <p class="description">Solid productivity laptop</p>
                </div>
                <div class="ratings">
                  <p class="pull-right">5 reviews</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </body>
    </html>
    """
)


def write_test_config(root: Path) -> Path:
    """Create a temp config tree for pipeline tests."""

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
                f"  url: sqlite:///{(root / 'pipeline.db').as_posix()}",
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
                "base_url: https://books.toscrape.com/",
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
                "base_url: https://webscraper.io/test-sites/e-commerce/ajax/computers/laptops",
                "scraper: site_b",
                "fetcher: browser",
                "timeout_seconds: 10",
                "browser_headless: true",
                "browser_wait_for_selector: div.thumbnail",
            ]
        ),
        encoding="utf-8",
    )

    return configs_dir / "settings.yaml"


def stub_fetchers(monkeypatch) -> None:
    """Replace network access with deterministic HTML fixtures."""

    pages = {
        "https://books.toscrape.com/": SITE_A_LISTING_HTML,
        "https://books.toscrape.com/catalogue/test-book-one_1/index.html": SITE_A_DETAIL_HTML,
        "https://webscraper.io/test-sites/e-commerce/ajax/computers/laptops": SITE_B_RENDERED_HTML,
    }

    def fake_fetch(self, url: str) -> FetchResponse:
        html = pages.get(url)
        if html is None:
            raise AssertionError(f"Unexpected URL fetched in test: {url}")
        return FetchResponse(
            url=url,
            status_code=200,
            text=html,
            content_type="text/html",
        )

    monkeypatch.setattr(HttpFetcher, "fetch", fake_fetch)
    monkeypatch.setattr(BrowserFetcher, "fetch", fake_fetch)


def test_run_pipeline_for_all_sources_smoke(tmp_path: Path, monkeypatch, capsys) -> None:
    """The `run` command should orchestrate scrape and export for all enabled sources."""

    config_path = write_test_config(tmp_path)
    db_path = tmp_path / "pipeline.db"
    stub_fetchers(monkeypatch)

    assert main(["--config", str(config_path), "init-db"]) == 0
    capsys.readouterr()

    assert (
        main(
            [
                "--config",
                str(config_path),
                "run",
                "--source",
                "all",
                "--limit",
                "1",
                "--report-limit",
                "10",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out

    assert "Scrape completed for site_a" in output
    assert "Export completed for site_a: latest_products=1 price_changes=0 run_summary=1" in output
    assert "Scrape completed for site_b" in output
    assert "Export completed for site_b: latest_products=1 price_changes=0 run_summary=1" in output
    assert "All enabled pipeline runs completed successfully: site_a, site_b" in output

    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    with engine.connect() as connection:
        scrape_runs_count = connection.execute(text("SELECT COUNT(*) FROM scrape_runs")).scalar_one()
        snapshots_count = connection.execute(text("SELECT COUNT(*) FROM product_snapshots")).scalar_one()
        change_count = connection.execute(text("SELECT COUNT(*) FROM price_change_events")).scalar_one()

    assert scrape_runs_count == 2
    assert snapshots_count == 2
    assert change_count == 0

    site_a_export_dir = tmp_path / "data" / "exports" / "site_a"
    site_b_export_dir = tmp_path / "data" / "exports" / "site_b"

    assert (site_a_export_dir / "latest_products.csv").exists()
    assert (site_a_export_dir / "price_changes.json").exists()
    assert (site_a_export_dir / "run_summary.csv").exists()

    assert (site_b_export_dir / "latest_products.csv").exists()
    assert (site_b_export_dir / "price_changes.json").exists()
    assert (site_b_export_dir / "run_summary.csv").exists()

    site_a_run_summary = json.loads((site_a_export_dir / "run_summary.json").read_text(encoding="utf-8"))
    assert len(site_a_run_summary) == 1
    assert site_a_run_summary[0]["source_name"] == "site_a"
