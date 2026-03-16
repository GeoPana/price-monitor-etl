from __future__ import annotations

"""Smoke tests covering the CLI happy path with a temporary SQLite database."""

import json
from pathlib import Path
from textwrap import dedent

from sqlalchemy import create_engine, text

from pricemonitor.fetchers.base import FetchResponse
from pricemonitor.fetchers.http_fetcher import HttpFetcher
from pricemonitor.main import main


LISTING_HTML = dedent(
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
              <p class="price_color">£10.00</p>
              <p class="instock availability">In stock</p>
            </div>
          </article>

          <article class="product_pod">
            <div class="image_container">
              <a href="catalogue/test-book-two_2/index.html">
                <img src="media/cache/test-book-two-thumb.jpg" alt="Test Book Two">
              </a>
            </div>
            <h3>
              <a href="catalogue/test-book-two_2/index.html" title="Test Book Two">
                Test Book Two
              </a>
            </h3>
            <div class="product_price">
              <p class="price_color">£22.50</p>
              <p class="instock availability">Out of stock</p>
            </div>
          </article>
        </section>
      </body>
    </html>
    """
)


DETAIL_ONE_HTML = dedent(
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
          <p class="price_color">£10.00</p>
          <p class="instock availability">In stock (20 available)</p>
        </div>
        <div class="item active">
          <img src="../../media/cache/test-book-one-large.jpg" alt="Test Book One">
        </div>
        <table class="table table-striped">
          <tr><th>UPC</th><td>UPC-BOOK-1</td></tr>
          <tr><th>Price (incl. tax)</th><td>£10.00</td></tr>
          <tr><th>Availability</th><td>In stock (20 available)</td></tr>
        </table>
      </body>
    </html>
    """
)

DETAIL_TWO_HTML = dedent(
    """\
    <html>
      <body>
        <ul class="breadcrumb">
          <li><a href="/">Home</a></li>
          <li><a href="/catalogue/category/books_1/index.html">Books</a></li>
          <li><a href="/catalogue/category/books/poetry_23/index.html">Poetry</a></li>
          <li class="active">Test Book Two</li>
        </ul>
        <div class="product_main">
          <h1>Test Book Two</h1>
          <p class="price_color">£22.50</p>
          <p class="instock availability">Out of stock</p>
        </div>
        <div class="item active">
          <img src="../../media/cache/test-book-two-large.jpg" alt="Test Book Two">
        </div>
        <table class="table table-striped">
          <tr><th>UPC</th><td>UPC-BOOK-2</td></tr>
          <tr><th>Price (incl. tax)</th><td>£22.50</td></tr>
          <tr><th>Availability</th><td>Out of stock</td></tr>
        </table>
      </body>
    </html>
    """
)


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
                "base_url: https://books.toscrape.com/",
                "scraper: site_a",
                "fetcher: http",
                "timeout_seconds: 10",
            ]
        ),
        encoding="utf-8",
    )

    return configs_dir / "settings.yaml"


def stub_site_a_fetcher(monkeypatch) -> None:
    """Replace outbound HTTP calls with deterministic HTML fixtures."""

    pages = {
        "https://books.toscrape.com/": LISTING_HTML,
        "https://books.toscrape.com/catalogue/test-book-one_1/index.html": DETAIL_ONE_HTML,
        "https://books.toscrape.com/catalogue/test-book-two_2/index.html": DETAIL_TWO_HTML,
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


def test_init_db_and_scrape_smoke(tmp_path: Path, monkeypatch) -> None:
    """Verify schema creation and a single real-scraper flow end to end."""

    config_path = write_test_config(tmp_path)
    db_path = tmp_path / "test.db"
    stub_site_a_fetcher(monkeypatch)

    assert main(["--config", str(config_path), "init-db"]) == 0
    assert db_path.exists()

    assert main(["--config", str(config_path), "scrape", "--source", "site_a"]) == 0

    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    with engine.connect() as connection:
        scrape_runs_count = connection.execute(text("SELECT COUNT(*) FROM scrape_runs")).scalar_one()
        snapshots_count = connection.execute(text("SELECT COUNT(*) FROM product_snapshots")).scalar_one()
        snapshots = connection.execute(
            text(
                """
                SELECT external_id, product_name, category, availability, payload
                FROM product_snapshots
                ORDER BY id
                """
            )
        ).mappings().all()

    assert scrape_runs_count == 1
    assert snapshots_count == 2

    assert snapshots[0]["external_id"] == "UPC-BOOK-1"
    assert snapshots[0]["product_name"] == "Test Book One"
    assert snapshots[0]["category"] == "Travel"
    assert snapshots[0]["availability"] == "in_stock"

    assert snapshots[1]["external_id"] == "UPC-BOOK-2"
    assert snapshots[1]["category"] == "Poetry"
    assert snapshots[1]["availability"] == "out_of_stock"

    first_payload = snapshots[0]["payload"]
    if isinstance(first_payload, str):
        first_payload = json.loads(first_payload)

    assert first_payload["image_url"].endswith("test-book-one-large.jpg")


def test_show_config_smoke(tmp_path: Path, capsys) -> None:
    """Verify that the CLI prints the resolved configuration."""

    config_path = write_test_config(tmp_path)

    assert main(["--config", str(config_path), "show-config"]) == 0
    output = capsys.readouterr().out

    assert "Price Monitor ETL" in output
    assert "site_a" in output
    assert "sqlite" in output
