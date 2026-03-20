from __future__ import annotations

"""Validation schemas shared across scraping and persistence layers."""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

ScrapeRunStatus = Literal["running", "succeeded", "failed"]
RawPageType = Literal["listing", "detail"]


class ProductRecord(BaseModel):
    """Validated product data emitted by scrapers before persistence."""

    model_config = ConfigDict(str_strip_whitespace=True)

    external_id: str
    product_name: str
    brand: str | None = None
    category: str | None = None
    product_url: str
    image_url: str | None = None
    currency: str = Field(min_length=3, max_length=3)
    listed_price: Decimal
    sale_price: Decimal | None = None
    availability: str = "unknown"

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()

    @field_validator("listed_price", "sale_price")
    @classmethod
    def validate_price(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and value < 0:
            raise ValueError("Price cannot be negative.")
        return value


class ArchivedPageRecord(BaseModel):
    """Raw page content captured during scraping for audit and replay."""

    page_type: RawPageType
    page_url: str
    content: str

class PriceChangeEventCreate(BaseModel):
    """Persistable price-change event detected between two consecutive runs."""

    source_name: str
    external_id: str
    scrape_run_id: int
    previous_snapshot_id: int
    current_snapshot_id: int
    product_name: str
    currency: str = Field(min_length=3, max_length=3)
    previous_price: Decimal
    current_price: Decimal
    absolute_difference: Decimal
    percentage_difference: Decimal | None = None
    changed_at: datetime

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()

    @field_validator("previous_price", "current_price", "absolute_difference", "percentage_difference")
    @classmethod
    def validate_non_negative(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and value < 0:
            raise ValueError("Change detection values cannot be negative.")
        return value

class ScrapeRunCreate(BaseModel):
    """Payload for creating a new scrape run record."""

    source_name: str
    status: ScrapeRunStatus = "running"
    started_at: datetime


class ScrapeRunUpdate(BaseModel):
    """Payload for updating scrape run status after execution."""

    status: ScrapeRunStatus
    finished_at: datetime
    records_fetched: int = 0
    records_inserted: int = 0
    error_message: str | None = None


class ProductSnapshotCreate(ProductRecord):
    """Persistable product snapshot enriched with scrape metadata."""

    source_name: str
    scrape_run_id: int
    scraped_at: datetime
