from __future__ import annotations

"""Pydantic response models for the read-only HTTP API."""

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    """Basic service health payload."""

    model_config = ConfigDict(extra="forbid")

    status: str
    app_name: str
    environment: str
    version: str
    configured_sources: int


class SourceResponse(BaseModel):
    """Configured source metadata exposed via the API."""

    model_config = ConfigDict(extra="forbid")

    name: str
    enabled: bool
    base_url: str
    scraper: str
    fetcher: str


class RunResponse(BaseModel):
    """API shape for recent pipeline runs."""

    model_config = ConfigDict(extra="forbid")

    scrape_run_id: int
    source_name: str
    status: str
    success_flag: bool
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    records_fetched: int
    records_inserted: int
    insert_rate: float | None = None
    validity_rate: float | None = None
    error_message: str | None = None


class LatestProductResponse(BaseModel):
    """Client-facing latest-product record returned by the API."""

    model_config = ConfigDict(extra="forbid")

    source_name: str
    external_id: str
    product_name: str
    brand: str | None = None
    category: str | None = None
    product_url: str
    image_url: str | None = None
    currency: str
    is_discounted: bool
    listed_price: Decimal
    sale_price: Decimal | None = None
    effective_price: Decimal
    availability: str
    availability_group: str
    scraped_at: datetime


class PriceChangeResponse(BaseModel):
    """Client-facing price-change record returned by the API."""

    model_config = ConfigDict(extra="forbid")

    source_name: str
    external_id: str
    product_name: str
    currency: str
    previous_price: Decimal
    current_price: Decimal
    absolute_difference: Decimal
    percentage_difference: Decimal | None = None
    change_direction: str
    change_bucket: str
    change_magnitude: str
    changed_at: datetime


class AlertSummaryResponse(BaseModel):
    """Summary of generated alerts for one source."""

    model_config = ConfigDict(extra="forbid")

    source_name: str
    generated_at: datetime
    latest_successful_run_id: int | None = None
    previous_successful_run_id: int | None = None
    baseline_mode: bool
    major_increase_threshold_pct: Decimal
    total_current_products: int
    total_price_changes: int
    top_price_changes_count: int
    price_drops_count: int
    major_increases_count: int
    new_products_count: int


class AlertPriceChangeRowResponse(BaseModel):
    """Alert-report row for ranked price-change CSV outputs."""

    model_config = ConfigDict(extra="forbid")

    source_name: str
    external_id: str
    product_name: str
    currency: str
    previous_price: Decimal
    current_price: Decimal
    absolute_difference: Decimal
    percentage_difference: Decimal | None = None
    change_direction: str
    change_bucket: str
    change_magnitude: str
    changed_date: date


class NewProductAlertResponse(BaseModel):
    """Alert-report row for newly seen products."""

    model_config = ConfigDict(extra="forbid")

    source_name: str
    external_id: str
    product_name: str
    category: str | None = None
    product_url: str
    currency: str
    effective_price: Decimal
    is_discounted: bool
    availability_group: str
    scrape_date: date
    scrape_run_id: int
