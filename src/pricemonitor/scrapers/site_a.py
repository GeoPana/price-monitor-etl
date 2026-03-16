from __future__ import annotations

"""Real static-site scraper for Books to Scrape."""

import re
from decimal import Decimal
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from pricemonitor.config import SourceSettings
from pricemonitor.fetchers.http_fetcher import HttpFetcher
from pricemonitor.models.schemas import ProductRecord
from pricemonitor.scrapers.base import BaseScraper


class SiteAScraper(BaseScraper):
    """Scrape listing pages and enrich products from detail pages."""

    def __init__(self, source_settings: SourceSettings) -> None:
        super().__init__(source_settings)
        self.fetcher = HttpFetcher(timeout_seconds=source_settings.timeout_seconds)

    def scrape(self, limit: int | None = None) -> list[ProductRecord]:
        products: list[ProductRecord] = []
        next_url: str | None = self.source_settings.base_url
        visited_pages: set[str] = set()

        while next_url is not None:
            if next_url in visited_pages:
                break
            visited_pages.add(next_url)

            listing_response = self.fetcher.fetch(next_url)
            listing_soup = BeautifulSoup(listing_response.text, "html.parser")
            product_cards = listing_soup.select("article.product_pod")

            if not product_cards and not products:
                raise ValueError(f"No product cards found at {listing_response.url}")

            for card in product_cards:
                listing_data = self._parse_listing_card(card, listing_response.url)
                detail_data = self._fetch_detail_page(listing_data["product_url"])

                products.append(
                    ProductRecord(
                        external_id=str(detail_data["external_id"]),
                        product_name=str(detail_data["product_name"] or listing_data["product_name"]),
                        brand=None,
                        category=self._as_optional_str(detail_data["category"]),
                        product_url=str(listing_data["product_url"]),
                        image_url=self._as_optional_str(
                            detail_data["image_url"] or listing_data["image_url"]
                        ),
                        currency=str(detail_data["currency"]),
                        listed_price=Decimal(str(detail_data["listed_price"])),
                        sale_price=None,
                        availability=str(detail_data["availability"] or listing_data["availability"]),
                    )
                )

                if limit is not None and len(products) >= limit:
                    return products

            next_url = self._extract_next_page_url(listing_soup, listing_response.url)

        return products

    def _parse_listing_card(self, card: Tag, page_url: str) -> dict[str, str | Decimal | None]:
        product_link = card.select_one("h3 a")
        if product_link is None or not product_link.get("href"):
            raise ValueError("Product card is missing a product link.")

        image_node = card.select_one("div.image_container img")
        image_src = image_node.get("src") if image_node is not None else None
        price_text = self._get_text(card.select_one("p.price_color"))

        return {
            "product_name": product_link.get("title") or self._get_text(product_link),
            "product_url": urljoin(page_url, str(product_link["href"])),
            "image_url": urljoin(page_url, image_src) if image_src else None,
            "listed_price": self._parse_price(price_text),
            "availability": self._normalize_availability(
                self._get_text(card.select_one("p.instock.availability"))
            ),
        }

    def _fetch_detail_page(self, product_url: str) -> dict[str, str | Decimal | None]:
        detail_response = self.fetcher.fetch(product_url)
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
            "external_id": table_values.get("UPC") or self._url_slug(product_url),
            "product_name": self._get_text(detail_soup.select_one("div.product_main h1")),
            "category": self._extract_category(detail_soup),
            "image_url": urljoin(detail_response.url, image_src) if image_src else None,
            "listed_price": self._parse_price(price_text),
            "currency": self._parse_currency(price_text),
            "availability": self._normalize_availability(
                table_values.get("Availability")
                or self._get_text(detail_soup.select_one("p.instock.availability"))
            ),
        }

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

    def _parse_currency(self, price_text: str | None) -> str:
        text = (price_text or "").strip()
        if text.startswith("£"):
            return "GBP"
        if text.startswith("$"):
            return "USD"
        if text.startswith("€"):
            return "EUR"
        return "USD"

    def _parse_price(self, price_text: str | None) -> Decimal:
        if not price_text:
            raise ValueError("Price text is missing.")

        match = re.search(r"([0-9]+(?:\.[0-9]{2})?)", price_text.replace(",", ""))
        if match is None:
            raise ValueError(f"Could not parse price from: {price_text}")

        return Decimal(match.group(1))

    def _normalize_availability(self, text: str | None) -> str:
        if not text:
            return "unknown"

        normalized = " ".join(text.lower().split())
        if "out of stock" in normalized or "unavailable" in normalized:
            return "out_of_stock"
        if "in stock" in normalized or "available" in normalized:
            return "in_stock"
        return normalized.replace(" ", "_")

    def _url_slug(self, product_url: str) -> str:
        path_parts = [
            part for part in urlparse(product_url).path.split("/") if part and part != "index.html"
        ]
        return path_parts[-1]

    def _get_text(self, node: Tag | None) -> str:
        if node is None:
            return ""
        return node.get_text(" ", strip=True)

    def _as_optional_str(self, value: str | Decimal | None) -> str | None:
        if value is None:
            return None
        return str(value)
