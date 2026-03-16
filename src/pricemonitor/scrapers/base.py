from __future__ import annotations

"""Base classes for converting source content into product records."""

from abc import ABC, abstractmethod

from pricemonitor.config import SourceSettings
from pricemonitor.models.schemas import ProductRecord


class BaseScraper(ABC):
    """Base interface implemented by all source-specific scrapers."""

    def __init__(self, source_settings: SourceSettings) -> None:
        self.source_settings = source_settings
        # Keeps these counters in-memory so the CLI can report raw vs valid records.
        self.last_scrape_stats: dict[str, int] = {
            "raw_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
        }

    @abstractmethod
    def scrape(self, limit: int | None = None) -> list[ProductRecord]:
        raise NotImplementedError
