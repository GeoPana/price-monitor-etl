from __future__ import annotations

"""Real static-site scraper that extracts raw HTML values and delegates cleanup/validation for Books to Scrape."""

import logging
from decimal import Decimal
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from pricemonitor.config import SourceSettings
from pricemonitor.fetchers.http_fetcher import HttpFetcher
from pricemonitor.models.schemas import ArchivedPageRecord, ProductRecord
from pricemonitor.scrapers.base import BaseScraper
from pricemonitor.services.validation import validate_product_records

logger = logging.getLogger(__name__)


class SiteAScraper(BaseScraper):
    """Scrape listing pages, extract raw fields, and validate products downstream."""

    def __init__(self, source_settings: SourceSettings) -> None:
        super().__init__(source_settings)
        self.fetcher = HttpFetcher(timeout_seconds=source_settings.timeout_seconds)

    def scrape(self, limit: int | None = None) -> list[ProductRecord]:
        # Reset run-local state so repeated scraper instances do not leak previous results.
        self.last_archived_pages = []
        self.last_scrape_stats = {
            "raw_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
        }

        raw_records: list[dict[str, str | None]] = []
        next_url: str | None = self.source_settings.base_url
        visited_pages: set[str] = set()

        while next_url is not None:
            if next_url in visited_pages:
                break
            visited_pages.add(next_url)

            listing_response = self.fetcher.fetch(next_url)
            self._remember_raw_page(
                page_type="listing",
                page_url=listing_response.url,
                content=listing_response.text,
            )

            listing_soup = BeautifulSoup(listing_response.text, "html.parser")
            product_cards = listing_soup.select("article.product_pod")

            if not product_cards and not raw_records:
                raise ValueError(f"No product cards found at {listing_response.url}")

            for card in product_cards:
                listing_data = self._extract_listing_card(card, listing_response.url)
                detail_url = urljoin(listing_response.url, str(listing_data["product_url"]))
                detail_data = self._fetch_detail_page(detail_url)

                raw_records.append(self._merge_product_data(listing_data, detail_data))

                if limit is not None and len(raw_records) >= limit:
                    break
            
            if limit is not None and len(raw_records) >= limit:
                break

            next_url = self._extract_next_page_url(listing_soup, listing_response.url)

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

    def _extract_listing_card(self, card: Tag, page_url: str) -> dict[str, str | Decimal | None]:
        """Extract raw listing values exactly as seen in the HTML."""

        product_link = card.select_one("h3 a")
        if product_link is None or not product_link.get("href"):
            raise ValueError("Product card is missing a product link.")

        image_node = card.select_one("div.image_container img")
        image_src = image_node.get("src") if image_node is not None else None

        return {
            "product_name": product_link.get("title") or self._get_text(product_link),
            "product_url": str(product_link["href"]),
            "product_url_base": page_url,
            "image_url": image_src,
            "image_url_base": page_url,
            "listed_price": self._get_text(card.select_one("p.price_color")),
            "sale_price": None,
            "availability": self._get_text(card.select_one("p.instock.availability")),
            "brand": None,
            "category": None,
            "currency": None,
            "external_id": None,
        }

    def _fetch_detail_page(self, product_url: str) -> dict[str, str | Decimal | None]:
        """Extract raw detail-page values without applying business normalization yet."""

        detail_response = self.fetcher.fetch(product_url)
        self._remember_raw_page(
            page_type="detail",
            page_url=detail_response.url,
            content=detail_response.text,
        )

        detail_soup = BeautifulSoup(detail_response.text, "html.parser")
        table_values = self._parse_product_table(detail_soup)

        price_text = (
            table_values.get("Price (incl. tax)")
            or table_values.get("Price (excl. tax)")
            or self._get_text(detail_soup.select_one("p.price_color"))
        )
        image_node = detail_soup.select_one("div.item.active img")
        image_src = image_node.get("src") if image_node is not None else None

        return {
            "external_id": table_values.get("UPC"),
            "product_name": self._get_text(detail_soup.select_one("div.product_main h1")),
            "category": self._extract_category(detail_soup),
            "product_url": detail_response.url,
            "product_url_base": detail_response.url,
            "image_url": image_src,
            "image_url_base": detail_response.url,
            "listed_price": price_text,
            "sale_price": None,
            "availability": table_values.get("Availability")
            or self._get_text(detail_soup.select_one("p.instock.availability")),
            "currency": None,
            "brand": None,
        }

    def _merge_product_data(
        self,
        listing_data: dict[str, str | None],
        detail_data: dict[str, str | None],
    ) -> dict[str, str | None]:
        """Prefer detail-page values, but keep listing-page fallbacks when needed."""

        merged = dict(listing_data)
        for key, value in detail_data.items():
            if value not in (None, ""):
                merged[key] = value
        return merged
    
    def _remember_raw_page(self, *, page_type: str, page_url: str, content: str) -> None:
        """Capture fetched page content so it can be archived after the run completes."""

        self.last_archived_pages.append(
            ArchivedPageRecord(
                page_type=page_type,
                page_url=page_url,
                content=content,
            )
        )
    
    def _parse_product_table(self, soup: BeautifulSoup) -> dict[str, str]:
        values: dict[str, str] = {}
        for row in soup.select("table.table.table-striped tr"):
            header = self._get_text(row.find("th"))
            value = self._get_text(row.find("td"))
            if header:
                values[header] = value
        return values

    def _extract_category(self, soup: BeautifulSoup) -> str | None:
        breadcrumb_links = soup.select("ul.breadcrumb li a")
        if len(breadcrumb_links) >= 3:
            return self._get_text(breadcrumb_links[2])
        return None

    def _extract_next_page_url(self, soup: BeautifulSoup, page_url: str) -> str | None:
        next_link = soup.select_one("li.next a")
        if next_link is None or not next_link.get("href"):
            return None
        return urljoin(page_url, str(next_link["href"]))

    def _get_text(self, node: Tag | None) -> str:
        if node is None:
            return ""
        return node.get_text(" ", strip=True)

