from __future__ import annotations

"""Build curated, analytics-ready processed datasets from operational database records."""

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pricemonitor.models.db_models import PriceChangeEvent, ProductSnapshot, ScrapeRun
from pricemonitor.storage.repositories import (
    PriceChangeEventRepository,
    ProductSnapshotRepository,
    ScrapeRunRepository,
)

LATEST_PRODUCTS_PROCESSED_FIELDS = [
    "source_name",
    "external_id",
    "product_name",
    "brand",
    "category",
    "product_url",
    "image_url",
    "currency",
    "listed_price",
    "sale_price",
    "effective_price",
    "is_discounted",
    "availability",
    "availability_group",
    "scraped_at",
    "scrape_date",
    "scrape_run_id",
]

PRICE_CHANGES_PROCESSED_FIELDS = [
    "source_name",
    "external_id",
    "product_name",
    "currency",
    "previous_price",
    "current_price",
    "absolute_difference",
    "percentage_difference",
    "change_direction",
    "change_bucket",
    "change_magnitude",
    "is_price_increase",
    "is_price_decrease",
    "changed_at",
    "changed_date",
    "scrape_run_id",
]

RUN_SUMMARY_PROCESSED_FIELDS = [
    "scrape_run_id",
    "source_name",
    "status",
    "success_flag",
    "started_at",
    "finished_at",
    "duration_seconds",
    "records_fetched",
    "records_inserted",
    "insert_rate",
    "validity_rate",
    "error_message",
]


@dataclass(slots=True)
class ProcessArtifact:
    """Metadata about one processed dataset written to disk."""

    name: str
    csv_path: Path
    json_path: Path
    row_count: int


@dataclass(slots=True)
class ProcessReport:
    """Summary of processed datasets produced for a source."""

    source_name: str
    processed_dir: Path
    artifacts: list[ProcessArtifact]

    def counts(self) -> dict[str, int]:
        return {artifact.name: artifact.row_count for artifact in self.artifacts}


class ProcessService:
    """Create stable processed datasets that sit between the DB and final exports."""

    def __init__(
        self,
        *,
        processed_dir: Path,
        scrape_run_repo: ScrapeRunRepository,
        snapshot_repo: ProductSnapshotRepository,
        change_event_repo: PriceChangeEventRepository,
    ) -> None:
        self.processed_dir = Path(processed_dir)
        self.scrape_run_repo = scrape_run_repo
        self.snapshot_repo = snapshot_repo
        self.change_event_repo = change_event_repo

    def process_source_data(self, source_name: str, *, recent_limit: int = 50) -> ProcessReport:
        """Write all processed datasets for one source."""

        source_processed_dir = self.processed_dir / source_name
        source_processed_dir.mkdir(parents=True, exist_ok=True)

        latest_products_rows = self._build_latest_products_rows(source_name)
        price_changes_rows = self._build_price_changes_rows(source_name, recent_limit=recent_limit)
        run_summary_rows = self._build_run_summary_rows(source_name, recent_limit=recent_limit)

        artifacts = [
            self._write_dataset(
                processed_dir=source_processed_dir,
                file_stem="latest_products_processed",
                fieldnames=LATEST_PRODUCTS_PROCESSED_FIELDS,
                rows=latest_products_rows,
            ),
            self._write_dataset(
                processed_dir=source_processed_dir,
                file_stem="price_changes_processed",
                fieldnames=PRICE_CHANGES_PROCESSED_FIELDS,
                rows=price_changes_rows,
            ),
            self._write_dataset(
                processed_dir=source_processed_dir,
                file_stem="run_summary_processed",
                fieldnames=RUN_SUMMARY_PROCESSED_FIELDS,
                rows=run_summary_rows,
            ),
        ]

        return ProcessReport(
            source_name=source_name,
            processed_dir=source_processed_dir,
            artifacts=artifacts,
        )

    def _build_latest_products_rows(self, source_name: str) -> list[dict[str, Any]]:
        """Build the curated latest-product dataset from the newest snapshot per product."""

        snapshots = self.snapshot_repo.list_current_catalog_for_source(source_name)
        rows: list[dict[str, Any]] = []

        for snapshot in snapshots:
            payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
            effective_price = self._effective_price(snapshot)

            rows.append(
                {
                    "source_name": snapshot.source_name,
                    "external_id": snapshot.external_id,
                    "product_name": snapshot.product_name,
                    "brand": snapshot.brand,
                    "category": snapshot.category,
                    "product_url": snapshot.product_url,
                    "image_url": payload.get("image_url"),
                    "currency": snapshot.currency,
                    "listed_price": snapshot.listed_price,
                    "sale_price": snapshot.sale_price,
                    "effective_price": effective_price,
                    "is_discounted": self._is_discounted(snapshot),
                    "availability": snapshot.availability,
                    "availability_group": self._availability_group(snapshot.availability),
                    "scraped_at": snapshot.scraped_at,
                    "scrape_date": snapshot.scraped_at.date().isoformat(),
                    "scrape_run_id": snapshot.scrape_run_id,
                }
            )

        return rows

    def _build_price_changes_rows(self, source_name: str, *, recent_limit: int) -> list[dict[str, Any]]:
        """Build a richer change dataset with direction and magnitude fields."""

        events = self.change_event_repo.list_latest_for_source(source_name, limit=recent_limit)
        rows: list[dict[str, Any]] = []

        for event in events:
            direction = self._change_direction(event)
            rows.append(
                {
                    "source_name": event.source_name,
                    "external_id": event.external_id,
                    "product_name": event.product_name,
                    "currency": event.currency,
                    "previous_price": event.previous_price,
                    "current_price": event.current_price,
                    "absolute_difference": event.absolute_difference,
                    "percentage_difference": event.percentage_difference,
                    "change_direction": direction,
                    "change_bucket": self._change_bucket(event.percentage_difference),
                    "change_magnitude": self._change_magnitude(event.percentage_difference),
                    "is_price_increase": direction == "increase",
                    "is_price_decrease": direction == "decrease",
                    "changed_at": event.changed_at,
                    "changed_date": event.changed_at.date().isoformat(),
                    "scrape_run_id": event.scrape_run_id,
                }
            )

        return rows

    def _build_run_summary_rows(self, source_name: str, *, recent_limit: int) -> list[dict[str, Any]]:
        """Build an operational dataset enriched with run-level ratios."""

        runs = self.scrape_run_repo.list_recent(source_name=source_name, limit=recent_limit)
        rows: list[dict[str, Any]] = []

        for scrape_run in runs:
            insert_rate = self._ratio(scrape_run.records_inserted, scrape_run.records_fetched)

            rows.append(
                {
                    "scrape_run_id": scrape_run.id,
                    "source_name": scrape_run.source_name,
                    "status": scrape_run.status,
                    "success_flag": scrape_run.status == "succeeded",
                    "started_at": scrape_run.started_at,
                    "finished_at": scrape_run.finished_at,
                    "duration_seconds": self._duration_seconds(scrape_run),
                    "records_fetched": scrape_run.records_fetched,
                    "records_inserted": scrape_run.records_inserted,
                    "insert_rate": insert_rate,
                    # Today, inserted records are the validated records that made it into storage.
                    "validity_rate": insert_rate,
                    "error_message": scrape_run.error_message,
                }
            )

        return rows

    def _write_dataset(
        self,
        *,
        processed_dir: Path,
        file_stem: str,
        fieldnames: list[str],
        rows: list[dict[str, Any]],
    ) -> ProcessArtifact:
        """Write both CSV and JSON forms of the same processed dataset."""

        csv_path = processed_dir / f"{file_stem}.csv"
        json_path = processed_dir / f"{file_stem}.json"

        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        field: self._serialize_value(row.get(field), for_csv=True)
                        for field in fieldnames
                    }
                )

        json_rows = [
            {
                field: self._serialize_value(row.get(field), for_csv=False)
                for field in fieldnames
            }
            for row in rows
        ]
        json_path.write_text(
            json.dumps(json_rows, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return ProcessArtifact(
            name=file_stem,
            csv_path=csv_path,
            json_path=json_path,
            row_count=len(rows),
        )

    def _effective_price(self, snapshot: ProductSnapshot) -> Decimal:
        """Use sale price when present so analytics reflect what a customer would pay."""

        if snapshot.sale_price is not None:
            return snapshot.sale_price
        return snapshot.listed_price

    def _is_discounted(self, snapshot: ProductSnapshot) -> bool:
        """Flag products where the sale price beats the listed price."""

        return snapshot.sale_price is not None and snapshot.sale_price < snapshot.listed_price

    def _availability_group(self, availability: str) -> str:
        """Collapse source-specific availability values into a small analytics-friendly set."""

        normalized = availability.strip().lower()
        if normalized == "in_stock":
            return "available"
        if normalized == "out_of_stock":
            return "unavailable"
        return "unknown"

    def _change_direction(self, event: PriceChangeEvent) -> str:
        """Normalize price movement into a readable direction label."""

        if event.current_price > event.previous_price:
            return "increase"
        if event.current_price < event.previous_price:
            return "decrease"
        return "unchanged"

    def _change_bucket(self, percentage_difference: Decimal | None) -> str:
        """Bucket price movement into stable percentage bands."""

        if percentage_difference is None:
            return "unknown"
        if percentage_difference < Decimal("5.00"):
            return "0-5%"
        if percentage_difference < Decimal("15.00"):
            return "5-15%"
        return "15%+"

    def _change_magnitude(self, percentage_difference: Decimal | None) -> str:
        """Translate percentage movement into a simple qualitative label."""

        if percentage_difference is None:
            return "unknown"
        if percentage_difference < Decimal("5.00"):
            return "minor"
        if percentage_difference < Decimal("15.00"):
            return "moderate"
        return "major"

    def _duration_seconds(self, scrape_run: ScrapeRun) -> float | None:
        """Compute runtime only when the run has both endpoints."""

        if scrape_run.finished_at is None:
            return None
        return round((scrape_run.finished_at - scrape_run.started_at).total_seconds(), 2)

    def _ratio(self, numerator: int, denominator: int) -> float | None:
        """Return a rounded ratio, or None when the denominator is zero."""

        if denominator == 0:
            return None
        return round(numerator / denominator, 4)

    def _serialize_value(self, value: Any, *, for_csv: bool) -> Any:
        """Convert database-native values into JSON/CSV-safe values."""

        if value is None:
            return "" if for_csv else None
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value
