from __future__ import annotations

"""Second real scraper using the Web Scraper test e-commerce site."""

import logging
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from pricemonitor.config import SourceSettings
from pricemonitor.fetchers.http_fetcher import HttpFetcher
from pricemonitor.models.schemas import ArchivedPageRecord, ProductRecord
from pricemonitor.scrapers.base import BaseScraper
from pricemonitor.services.validation import validate_product_records

logger = logging.getLogger(__name__)


class SiteBScraper(BaseScraper):
    """Scrape a different card layout while reusing the same validation pipeline."""

    def __init__(self, source_settings: SourceSettings) -> None:
        super().__init__(source_settings)
        self.fetcher = HttpFetcher(timeout_seconds=source_settings.timeout_seconds)

    def scrape(self, limit: int | None = None) -> list[ProductRecord]:
        # Reset per-run state so repeated calls stay deterministic.
        self.last_archived_pages = []
        self.last_scrape_stats = {
            "raw_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
        }

        response = self.fetcher.fetch(self.source_settings.base_url)
        self._remember_raw_page(
            page_type="listing",
            page_url=response.url,
            content=response.text,
        )

        soup = BeautifulSoup(response.text, "html.parser")
        cards = soup.select("div.thumbnail")
        if not cards:
            raise ValueError(f"No product cards found at {response.url}")

        raw_records: list[dict[str, str | None]] = []
        for card in cards:
            raw_records.append(self._extract_listing_card(card, response.url))
            if limit is not None and len(raw_records) >= limit:
                break

        summary = validate_product_records(
            raw_records,
            source_name=self.source_settings.name,
            logger_=logger,
        )
        self.last_scrape_stats = {
            "raw_records": summary.total_records,
            "valid_records": summary.valid_count,
            "invalid_records": summary.invalid_count,
        }
        return summary.valid_records

    def _extract_listing_card(self, card: Tag, page_url: str) -> dict[str, str | None]:
        """Extract raw values from the Web Scraper test-site card layout."""

        product_link = card.select_one("a.title")
        if product_link is None or not product_link.get("href"):
            raise ValueError("Product card is missing a title link.")

        image_node = card.select_one("img.img-responsive")
        image_src = image_node.get("src") if image_node is not None else None

        description_node = card.select_one("p.description")
        reviews_node = card.select_one("p.pull-right")
        category = self._derive_category(page_url)

        return {
            "external_id": self._derive_external_id(str(product_link["href"])),
            "product_name": product_link.get("title") or self._get_text(product_link),
            "brand": None,
            "category": category,
            "product_url": str(product_link["href"]),
            "product_url_base": page_url,
            "image_url": image_src,
            "image_url_base": page_url,
            "currency": None,
            "listed_price": self._get_text(card.select_one("h4.price")),
            "sale_price": None,
            # This source does not expose stock status, so we keep it explicit.
            "availability": "unknown",
            # The validation layer ignores extra keys, but this keeps raw context in payload.
            "review_summary": self._get_text(reviews_node),
            "description": self._get_text(description_node),
        }

    def _remember_raw_page(self, *, page_type: str, page_url: str, content: str) -> None:
        """Capture fetched page content so it can be archived after the run completes."""

        self.last_archived_pages.append(
            ArchivedPageRecord(
                page_type=page_type,
                page_url=page_url,
                content=content,
            )
        )

    def _derive_external_id(self, href: str) -> str:
        """Use the final path segment as a stable per-product identifier."""

        return href.rstrip("/").split("/")[-1]

    def _derive_category(self, page_url: str) -> str | None:
        """Infer a coarse category from the configured listing URL."""

        parts = [part for part in page_url.split("/") if part]
        if not parts:
            return None
        return parts[-1].replace("-", " ").title()

    def _get_text(self, node: Tag | None) -> str:
        if node is None:
            return ""
        return node.get_text(" ", strip=True)
