from __future__ import annotations

"""Build business-facing CSV and JSON exports from stored scrape data."""

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

LATEST_PRODUCTS_FIELDS = [
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
    "availability",
    "scraped_at",
    "scrape_run_id",
]

PRICE_CHANGES_FIELDS = [
    "source_name",
    "external_id",
    "product_name",
    "currency",
    "previous_price",
    "current_price",
    "absolute_difference",
    "percentage_difference",
    "changed_at",
    "scrape_run_id",
]

RUN_SUMMARY_FIELDS = [
    "scrape_run_id",
    "source_name",
    "status",
    "started_at",
    "finished_at",
    "duration_seconds",
    "records_fetched",
    "records_inserted",
    "error_message",
]


@dataclass(slots=True)
class ExportArtifact:
    """Metadata about one exported dataset."""

    name: str
    csv_path: Path
    json_path: Path
    row_count: int


@dataclass(slots=True)
class ExportReport:
    """Summary of all export files produced for one source."""

    source_name: str
    export_dir: Path
    artifacts: list[ExportArtifact]

    def counts(self) -> dict[str, int]:
        return {artifact.name: artifact.row_count for artifact in self.artifacts}


class ExportService:
    """Create client-facing exports from stored scrape and change-detection data."""

    def __init__(
        self,
        *,
        exports_dir: Path,
        scrape_run_repo: ScrapeRunRepository,
        snapshot_repo: ProductSnapshotRepository,
        change_event_repo: PriceChangeEventRepository,
    ) -> None:
        self.exports_dir = Path(exports_dir)
        self.scrape_run_repo = scrape_run_repo
        self.snapshot_repo = snapshot_repo
        self.change_event_repo = change_event_repo

    def export_source_report(self, source_name: str, *, recent_limit: int = 50) -> ExportReport:
        """Write all business-facing exports for a single source."""

        export_dir = self.exports_dir / source_name
        export_dir.mkdir(parents=True, exist_ok=True)

        latest_products_rows = self._build_latest_products_rows(source_name)
        price_changes_rows = self._build_price_changes_rows(source_name, recent_limit=recent_limit)
        run_summary_rows = self._build_run_summary_rows(source_name, recent_limit=recent_limit)

        artifacts = [
            self._write_dataset(
                export_dir=export_dir,
                file_stem="latest_products",
                fieldnames=LATEST_PRODUCTS_FIELDS,
                rows=latest_products_rows,
            ),
            self._write_dataset(
                export_dir=export_dir,
                file_stem="price_changes",
                fieldnames=PRICE_CHANGES_FIELDS,
                rows=price_changes_rows,
            ),
            self._write_dataset(
                export_dir=export_dir,
                file_stem="run_summary",
                fieldnames=RUN_SUMMARY_FIELDS,
                rows=run_summary_rows,
            ),
        ]

        return ExportReport(
            source_name=source_name,
            export_dir=export_dir,
            artifacts=artifacts,
        )

    def _build_latest_products_rows(self, source_name: str) -> list[dict[str, Any]]:
        """Export the latest known snapshot for each product in a source."""

        snapshots = self.snapshot_repo.list_current_catalog_for_source(source_name)
        rows: list[dict[str, Any]] = []

        for snapshot in snapshots:
            payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
            effective_price = snapshot.sale_price if snapshot.sale_price is not None else snapshot.listed_price

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
                    "availability": snapshot.availability,
                    "scraped_at": snapshot.scraped_at,
                    "scrape_run_id": snapshot.scrape_run_id,
                }
            )

        return rows

    def _build_price_changes_rows(self, source_name: str, *, recent_limit: int) -> list[dict[str, Any]]:
        """Export recent price change events in reverse-chronological order."""

        events = self.change_event_repo.list_latest_for_source(source_name, limit=recent_limit)
        rows: list[dict[str, Any]] = []

        for event in events:
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
                    "changed_at": event.changed_at,
                    "scrape_run_id": event.scrape_run_id,
                }
            )

        return rows

    def _build_run_summary_rows(self, source_name: str, *, recent_limit: int) -> list[dict[str, Any]]:
        """Export recent run outcomes for operational reporting."""

        runs = self.scrape_run_repo.list_recent(source_name=source_name, limit=recent_limit)
        rows: list[dict[str, Any]] = []

        for scrape_run in runs:
            rows.append(
                {
                    "scrape_run_id": scrape_run.id,
                    "source_name": scrape_run.source_name,
                    "status": scrape_run.status,
                    "started_at": scrape_run.started_at,
                    "finished_at": scrape_run.finished_at,
                    "duration_seconds": self._duration_seconds(scrape_run),
                    "records_fetched": scrape_run.records_fetched,
                    "records_inserted": scrape_run.records_inserted,
                    "error_message": scrape_run.error_message,
                }
            )

        return rows

    def _write_dataset(
        self,
        *,
        export_dir: Path,
        file_stem: str,
        fieldnames: list[str],
        rows: list[dict[str, Any]],
    ) -> ExportArtifact:
        """Write both CSV and JSON versions of the same export dataset."""

        csv_path = export_dir / f"{file_stem}.csv"
        json_path = export_dir / f"{file_stem}.json"

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

        return ExportArtifact(
            name=file_stem,
            csv_path=csv_path,
            json_path=json_path,
            row_count=len(rows),
        )

    def _duration_seconds(self, scrape_run: ScrapeRun) -> float | None:
        """Compute run duration only when the run has a finish timestamp."""

        if scrape_run.finished_at is None:
            return None
        return round((scrape_run.finished_at - scrape_run.started_at).total_seconds(), 2)

    def _serialize_value(self, value: Any, *, for_csv: bool) -> Any:
        """Convert database-native values into export-safe representations."""

        if value is None:
            return "" if for_csv else None
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value
