from __future__ import annotations

"""Demo scraper that turns configured sample data into product records."""

from pricemonitor.models.schemas import ProductRecord
from pricemonitor.scrapers.base import BaseScraper


class DummySiteAScraper(BaseScraper):
    """Scraper used for local smoke tests and initial pipeline wiring."""

    def scrape(self, limit: int | None = None) -> list[ProductRecord]:
        sample_products = self.source_settings.sample_products
        if limit is not None:
            sample_products = sample_products[:limit]

        return [ProductRecord.model_validate(item) for item in sample_products]
