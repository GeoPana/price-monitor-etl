from __future__ import annotations

"""Registry for resolving configured source names to scraper implementations."""

from pricemonitor.config import SourceSettings
from pricemonitor.scrapers.base import BaseScraper
from pricemonitor.scrapers.site_a import SiteAScraper
from pricemonitor.scrapers.site_b import SiteBScraper

SCRAPER_REGISTRY: dict[str, type[BaseScraper]] = {
    "site_a": SiteAScraper,
    "site_b": SiteBScraper,
}

def get_scraper(source_name: str, source_settings: SourceSettings) -> BaseScraper:
    """Resolve a scraper by explicit source name first, then by configured scraper id."""

    scraper_cls = SCRAPER_REGISTRY.get(source_name) or SCRAPER_REGISTRY.get(source_settings.scraper)
    if scraper_cls is None:
        raise ValueError(f"No scraper registered for source '{source_name}'.")
    return scraper_cls(source_settings)
