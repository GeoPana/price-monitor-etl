from __future__ import annotations

"""Unit tests for selecting HTTP vs browser fetchers from source configuration."""

from pricemonitor.config import SourceSettings
from pricemonitor.fetchers.browser_fetcher import BrowserFetcher
from pricemonitor.fetchers.factory import create_fetcher
from pricemonitor.fetchers.http_fetcher import HttpFetcher


def test_create_fetcher_returns_http_fetcher_for_static_source() -> None:
    """Static sources should keep using the lightweight requests-based fetcher."""

    source_settings = SourceSettings(
        name="site_a",
        enabled=True,
        base_url="https://books.toscrape.com/",
        scraper="site_a",
        fetcher="http",
        timeout_seconds=10,
    )

    fetcher = create_fetcher(source_settings)

    assert isinstance(fetcher, HttpFetcher)
    assert fetcher.timeout_seconds == 10


def test_create_fetcher_returns_browser_fetcher_for_dynamic_source() -> None:
    """Dynamic sources should opt into the Playwright-backed browser fetcher."""

    source_settings = SourceSettings(
        name="site_b",
        enabled=True,
        base_url="https://webscraper.io/test-sites/e-commerce/ajax/computers/laptops",
        scraper="site_b",
        fetcher="browser",
        timeout_seconds=15,
        browser_headless=True,
        browser_wait_for_selector="div.thumbnail",
    )

    fetcher = create_fetcher(source_settings)

    assert isinstance(fetcher, BrowserFetcher)
    assert fetcher.timeout_seconds == 15
    assert fetcher.headless is True
    assert fetcher.wait_for_selector == "div.thumbnail"
