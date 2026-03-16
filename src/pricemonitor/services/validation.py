from __future__ import annotations

"""Validation service that turns raw extracted data into clean business records."""

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urlsplit

from pydantic import ValidationError

from pricemonitor.models.schemas import ProductRecord
from pricemonitor.parsers.normalization import (
    clean_text,
    normalize_availability,
    normalize_currency,
    normalize_price,
    normalize_url,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class InvalidProductRecord:
    """Container for a rejected raw record and the reasons it was rejected."""

    row_number: int
    errors: list[str]
    raw_record: dict[str, Any]


@dataclass(slots=True)
class ValidationSummary:
    """Summary returned after normalizing and validating a batch of raw records."""

    valid_records: list[ProductRecord] = field(default_factory=list)
    invalid_records: list[InvalidProductRecord] = field(default_factory=list)

    @property
    def total_records(self) -> int:
        return self.valid_count + self.invalid_count

    @property
    def valid_count(self) -> int:
        return len(self.valid_records)

    @property
    def invalid_count(self) -> int:
        return len(self.invalid_records)


def validate_product_records(
    raw_records: Iterable[dict[str, Any]],
    *,
    source_name: str | None = None,
    logger_: logging.Logger | None = None,
) -> ValidationSummary:
    """Normalize raw records, validate business rules, and log rejected rows."""

    active_logger = logger_ or logger
    summary = ValidationSummary()

    for row_number, raw_record in enumerate(raw_records, start=1):
        normalized = _normalize_raw_record(raw_record)
        errors = _collect_validation_errors(normalized)

        if errors:
            summary.invalid_records.append(
                InvalidProductRecord(
                    row_number=row_number,
                    errors=errors,
                    raw_record=raw_record,
                )
            )
            active_logger.warning(
                "Rejected %s record #%s: %s",
                source_name or "product",
                row_number,
                "; ".join(errors),
            )
            continue

        try:
            summary.valid_records.append(ProductRecord.model_validate(normalized))
        except ValidationError as exc:
            error_messages = [detail["msg"] for detail in exc.errors()]
            summary.invalid_records.append(
                InvalidProductRecord(
                    row_number=row_number,
                    errors=error_messages,
                    raw_record=raw_record,
                )
            )
            active_logger.warning(
                "Rejected %s record #%s during schema validation: %s",
                source_name or "product",
                row_number,
                "; ".join(error_messages),
            )

    active_logger.info(
        "Validation summary for %s: total=%s valid=%s invalid=%s",
        source_name or "source",
        summary.total_records,
        summary.valid_count,
        summary.invalid_count,
    )
    return summary


def _normalize_raw_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    """Apply text, price, URL, and availability normalization to a raw record."""

    product_url = normalize_url(
        raw_record.get("product_url"),
        raw_record.get("product_url_base"),
    )
    image_url = normalize_url(
        raw_record.get("image_url"),
        raw_record.get("image_url_base"),
    )
    listed_price = normalize_price(raw_record.get("listed_price"))
    sale_price = normalize_price(raw_record.get("sale_price"))

    external_id = clean_text(raw_record.get("external_id"))
    if external_id is None:
        external_id = _derive_external_id(product_url)

    return {
        "external_id": external_id,
        "product_name": clean_text(raw_record.get("product_name")),
        "brand": clean_text(raw_record.get("brand")),
        "category": clean_text(raw_record.get("category")),
        "product_url": product_url,
        "image_url": image_url,
        "currency": normalize_currency(
            raw_record.get("currency"),
            price_text=raw_record.get("listed_price"),
        ),
        "listed_price": listed_price,
        "sale_price": sale_price,
        "availability": normalize_availability(raw_record.get("availability")),
    }


def _collect_validation_errors(normalized_record: dict[str, Any]) -> list[str]:
    """Apply business validation rules after normalization."""

    errors: list[str] = []

    if not normalized_record["external_id"]:
        errors.append("missing external_id")

    if not normalized_record["product_name"]:
        errors.append("empty product_name")

    if not normalized_record["product_url"]:
        errors.append("missing product_url")

    listed_price = normalized_record["listed_price"]
    if listed_price is None:
        errors.append("missing listed_price")
    elif listed_price < 0:
        errors.append("negative listed_price")

    sale_price = normalized_record["sale_price"]
    if sale_price is not None and sale_price < 0:
        errors.append("negative sale_price")

    return errors


def _derive_external_id(product_url: str | None) -> str | None:
    """Use the URL slug as a fallback external id when the source does not expose one."""

    if not product_url:
        return None

    path_parts = [
        part
        for part in urlsplit(product_url).path.split("/")
        if part and part != "index.html"
    ]
    if not path_parts:
        return None

    return path_parts[-1]
