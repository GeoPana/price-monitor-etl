from __future__ import annotations

"""Build business-facing exports from the processed data layer, not directly from the DB."""

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


LATEST_PRODUCTS_EXPORT_FIELDS = [
    "source_name",
    "external_id",
    "product_name",
    "brand",
    "category",
    "product_url",
    "image_url",
    "currency",
    "is_discounted",
    "listed_price",
    "sale_price",
    "effective_price",
    "availability",
    "availability_group",
    "scraped_at",
]

PRICE_CHANGES_EXPORT_FIELDS = [
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
    "changed_at",
]

RUN_SUMMARY_EXPORT_FIELDS = [
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
    """Create client-facing outputs from the curated processed datasets."""

    def __init__(self, *, processed_dir: Path, exports_dir: Path) -> None:
        self.processed_dir = Path(processed_dir)
        self.exports_dir = Path(exports_dir)

    def export_source_report(self, source_name: str, *, recent_limit: int = 50) -> ExportReport:
        """Write all business-facing exports for a single source."""

        export_dir = self.exports_dir / source_name
        export_dir.mkdir(parents=True, exist_ok=True)

        latest_products_rows = self._select_fields(
            self._load_processed_dataset(source_name, "latest_products_processed"),
            LATEST_PRODUCTS_EXPORT_FIELDS,
        )
        price_changes_rows = self._select_fields(
            self._load_processed_dataset(source_name, "price_changes_processed")[:recent_limit],
            PRICE_CHANGES_EXPORT_FIELDS,
        )
        run_summary_rows = self._select_fields(
            self._load_processed_dataset(source_name, "run_summary_processed")[:recent_limit],
            RUN_SUMMARY_EXPORT_FIELDS,
        )

        artifacts = [
            self._write_dataset(
                export_dir=export_dir,
                file_stem="latest_products",
                fieldnames=LATEST_PRODUCTS_EXPORT_FIELDS,
                rows=latest_products_rows,
            ),
            self._write_dataset(
                export_dir=export_dir,
                file_stem="price_changes",
                fieldnames=PRICE_CHANGES_EXPORT_FIELDS,
                rows=price_changes_rows,
            ),
            self._write_dataset(
                export_dir=export_dir,
                file_stem="run_summary",
                fieldnames=RUN_SUMMARY_EXPORT_FIELDS,
                rows=run_summary_rows,
            ),
        ]

        return ExportReport(
            source_name=source_name,
            export_dir=export_dir,
            artifacts=artifacts,
        )

    def _load_processed_dataset(self, source_name: str, file_stem: str) -> list[dict[str, Any]]:
        """Read one processed dataset from disk and fail with a clear message if it is missing."""

        json_path = self.processed_dir / source_name / f"{file_stem}.json"
        if not json_path.exists():
            raise FileNotFoundError(
                f"Processed dataset not found: {json_path}. "
                f"Run `pricemonitor process --source {source_name}` or "
                f"`pricemonitor run --source {source_name}` first."
            )

        rows = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError(f"Expected a list in processed dataset: {json_path}")

        return rows

    def _select_fields(
        self,
        rows: list[dict[str, Any]],
        fieldnames: list[str],
    ) -> list[dict[str, Any]]:
        """Project processed rows into the smaller client-facing export shape."""

        return [{field: row.get(field) for field in fieldnames} for row in rows]

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
    
    def _serialize_value(self, value: Any, *, for_csv: bool) -> Any:
        """Convert export values into CSV/JSON-safe representations."""

        if value is None:
            return "" if for_csv else None
        return value
