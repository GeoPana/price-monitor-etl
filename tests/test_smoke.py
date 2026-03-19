from __future__ import annotations

"""Smoke tests covering the CLI happy path with a temporary SQLite database."""

import json
from pathlib import Path
from textwrap import dedent

from sqlalchemy import create_engine, text

from pricemonitor.fetchers.base import FetchResponse
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
              <a href="catalogue/test-book-one_1/index.html" title="  Test   Book One  ">
                Test Book One
              </a>
            </h3>
            <div class="product_price">
              <p class="price_color"> Ã‚Â£10.00 </p>
              <p class="instock availability"> In stock </p>
            </div>
          </article>

          <article class="product_pod">
            <div class="image_container">
              <a href="catalogue/test-book-two_2/index.html">
                <img src="media/cache/test-book-two-thumb.jpg" alt="Test Book Two">
              </a>
            </div>
            <h3>
              <a href="catalogue/test-book-two_2/index.html" title="   ">
              </a>
            </h3>
            <div class="product_price">
              <p class="price_color"> Ã‚Â£-22.50 </p>
              <p class="instock availability"> Out of stock </p>
            </div>
          </article>
        </section>
      </body>
    </html>
    """
)


SITE_A_DETAIL_ONE_HTML = dedent(
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
          <h1>  Test Book One  </h1>
          <p class="price_color"> Ã‚Â£10.00 </p>
          <p class="instock availability"> In stock (20 available) </p>
        </div>
        <div class="item active">
          <img src="../../media/cache/test-book-one-large.jpg" alt="Test Book One">
        </div>
        <table class="table table-striped">
          <tr><th>UPC</th><td>UPC-BOOK-1</td></tr>
          <tr><th>Price (incl. tax)</th><td>Ã‚Â£10.00</td></tr>
          <tr><th>Availability</th><td>In stock (20 available)</td></tr>
        </table>
      </body>
    </html>
    """
)

SITE_A_DETAIL_TWO_HTML = dedent(
    """\
    <html>
      <body>
        <ul class="breadcrumb">
          <li><a href="/">Home</a></li>
          <li><a href="/catalogue/category/books_1/index.html">Books</a></li>
          <li><a href="/catalogue/category/books/poetry_23/index.html">Poetry</a></li>
          <li class="active"></li>
        </ul>
        <div class="product_main">
          <h1>   </h1>
          <p class="price_color"> Ã‚Â£-22.50 </p>
          <p class="instock availability"> Out of stock </p>
        </div>
        <div class="item active">
          <img src="../../media/cache/test-book-two-large.jpg" alt="Test Book Two">
        </div>
        <table class="table table-striped">
          <tr><th>UPC</th><td>UPC-BOOK-2</td></tr>
          <tr><th>Price (incl. tax)</th><td>Ã‚Â£-22.50</td></tr>
          <tr><th>Availability</th><td>Out of stock</td></tr>
        </table>
      </body>
    </html>
    """
)

SITE_B_LISTING_HTML = dedent(
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
                    <a class="title" href="/test-sites/e-commerce/static/product/101" title="  Laptop One  ">
                      Laptop One
                    </a>
                  </h4>
                  <p class="description">  Solid productivity laptop  </p>
                </div>
                <div class="ratings">
                  <p class="pull-right">5 reviews</p>
                </div>
              </div>
            </div>

            <div class="col-md-4 col-xl-4 col-lg-4">
              <div class="thumbnail">
                <img class="img-responsive" src="/images/test-laptop-2.jpg" alt="Laptop Two">
                <div class="caption">
                  <h4 class="price">$1,299.00</h4>
                  <h4>
                    <a class="title" href="/test-sites/e-commerce/static/product/102" title="Laptop Two">
                      Laptop Two
                    </a>
                  </h4>
                  <p class="description">  Higher-end model  </p>
                </div>
                <div class="ratings">
                  <p class="pull-right">12 reviews</p>
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

    (sources_dir / "site_b.yaml").write_text(
        "\n".join(
            [
                "name: site_b",
                "enabled: true",
                "base_url: https://webscraper.io/test-sites/e-commerce/static/computers/laptops",
                "scraper: site_b",
                "fetcher: http",
                "timeout_seconds: 10",
            ]
        ),
        encoding="utf-8",
    )
    
    return configs_dir / "settings.yaml"


def stub_fetchers(monkeypatch) -> None:
    """Replace outbound HTTP calls with deterministic HTML fixtures."""

    pages = {
        "https://books.toscrape.com/": SITE_A_LISTING_HTML,
        "https://books.toscrape.com/catalogue/test-book-one_1/index.html": SITE_A_DETAIL_ONE_HTML,
        "https://books.toscrape.com/catalogue/test-book-two_2/index.html": SITE_A_DETAIL_TWO_HTML,
        "https://webscraper.io/test-sites/e-commerce/static/computers/laptops": SITE_B_LISTING_HTML,
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


def test_init_db_and_scrape_site_a_smoke(tmp_path: Path, monkeypatch, capsys) -> None:
    """Verify schema creation and a single real-scraper flow end to end."""

    config_path = write_test_config(tmp_path)
    db_path = tmp_path / "test.db"
    stub_fetchers(monkeypatch)

    assert main(["--config", str(config_path), "init-db"]) == 0
    assert db_path.exists()
    capsys.readouterr()

    assert main(["--config", str(config_path), "scrape", "--source", "site_a"]) == 0
    scrape_output = capsys.readouterr().out

    assert "fetched=2 valid=1 invalid=1 inserted=1 archived=3" in scrape_output

    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    with engine.connect() as connection:
        scrape_runs_count = connection.execute(text("SELECT COUNT(*) FROM scrape_runs")).scalar_one()
        snapshots_count = connection.execute(text("SELECT COUNT(*) FROM product_snapshots")).scalar_one()
        latest_run = connection.execute(
            text(
                """
                SELECT records_fetched, records_inserted
                FROM scrape_runs
                ORDER BY id DESC
                LIMIT 1
                """
            )
        ).mappings().one()
        
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
    assert snapshots_count == 1

    assert latest_run["records_fetched"] == 2
    assert latest_run["records_inserted"] == 1

    assert snapshots[0]["external_id"] == "UPC-BOOK-1"
    assert snapshots[0]["product_name"] == "Test Book One"
    assert snapshots[0]["category"] == "Travel"
    assert snapshots[0]["availability"] == "in_stock"

    first_payload = snapshots[0]["payload"]
    if isinstance(first_payload, str):
        first_payload = json.loads(first_payload)

    assert first_payload["image_url"].endswith("test-book-one-large.jpg")

    raw_run_dir = tmp_path / "data" / "raw" / "site_a" / "run_1"
    assert raw_run_dir.exists()

    archived_html = sorted(raw_run_dir.glob("*.html"))
    assert len(archived_html) == 3

    manifest = json.loads((raw_run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest) == 3
    assert manifest[0]["page_type"] == "listing"
    assert manifest[1]["page_type"] == "detail"

def test_scrape_site_b_smoke(tmp_path: Path, monkeypatch, capsys) -> None:
    """Verify the second source reuses the same validation and storage pipeline."""

    config_path = write_test_config(tmp_path)
    db_path = tmp_path / "test.db"
    stub_fetchers(monkeypatch)

    assert main(["--config", str(config_path), "init-db"]) == 0
    capsys.readouterr()

    assert main(["--config", str(config_path), "scrape", "--source", "site_b"]) == 0
    scrape_output = capsys.readouterr().out

    assert "fetched=2 valid=2 invalid=0 inserted=2 archived=1" in scrape_output

    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    with engine.connect() as connection:
        snapshots = connection.execute(
            text(
                """
                SELECT source_name, external_id, product_name, category, availability, payload
                FROM product_snapshots
                ORDER BY id
                """
            )
        ).mappings().all()

    assert len(snapshots) == 2
    assert snapshots[0]["source_name"] == "site_b"
    assert snapshots[0]["external_id"] == "101"
    assert snapshots[0]["product_name"] == "Laptop One"
    assert snapshots[0]["category"] == "Laptops"
    assert snapshots[0]["availability"] == "unknown"

    first_payload = snapshots[0]["payload"]
    if isinstance(first_payload, str):
        first_payload = json.loads(first_payload)

    assert first_payload["image_url"].endswith("/images/test-laptop-1.jpg")


def test_scrape_all_sources_smoke(tmp_path: Path, monkeypatch, capsys) -> None:
    """Verify the CLI can run all enabled sources in sequence."""

    config_path = write_test_config(tmp_path)
    db_path = tmp_path / "test.db"
    stub_fetchers(monkeypatch)

    assert main(["--config", str(config_path), "init-db"]) == 0
    capsys.readouterr()

    assert main(["--config", str(config_path), "scrape", "--source", "all"]) == 0
    scrape_output = capsys.readouterr().out

    assert "Scrape completed for site_a" in scrape_output
    assert "Scrape completed for site_b" in scrape_output
    assert "All enabled scrapes completed successfully: site_a, site_b" in scrape_output

    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    with engine.connect() as connection:
        scrape_runs_count = connection.execute(text("SELECT COUNT(*) FROM scrape_runs")).scalar_one()
        snapshots_count = connection.execute(text("SELECT COUNT(*) FROM product_snapshots")).scalar_one()

    assert scrape_runs_count == 2
    assert snapshots_count == 3
   
    
def test_show_config_smoke(tmp_path: Path, capsys) -> None:
    """Verify that the CLI prints the resolved configuration."""

    config_path = write_test_config(tmp_path)

    assert main(["--config", str(config_path), "show-config"]) == 0
    output = capsys.readouterr().out

    assert "Price Monitor ETL" in output
    assert "site_a" in output
    assert "site_b" in output
    assert "sqlite" in output
