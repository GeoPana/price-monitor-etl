from __future__ import annotations

"""Generate stakeholder-ready alert outputs from the processed data layer."""

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pricemonitor.storage.repositories import ProductSnapshotRepository, ScrapeRunRepository

TOP_PRICE_CHANGES_FIELDS = [
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
    "changed_date",
]

PRICE_DROPS_FIELDS = TOP_PRICE_CHANGES_FIELDS

MAJOR_INCREASES_FIELDS = TOP_PRICE_CHANGES_FIELDS

NEW_PRODUCTS_FIELDS = [
    "source_name",
    "external_id",
    "product_name",
    "category",
    "product_url",
    "currency",
    "effective_price",
    "is_discounted",
    "availability_group",
    "scrape_date",
    "scrape_run_id",
]


@dataclass(slots=True)
class AlertArtifact:
    """Metadata about one alert artifact written to disk."""

    name: str
    path: Path
    row_count: int | None = None


@dataclass(slots=True)
class AlertReport:
    """Summary of all alert artifacts produced for a source."""

    source_name: str
    alerts_dir: Path
    artifacts: list[AlertArtifact]
    summary: dict[str, Any]

    def counts(self) -> dict[str, int]:
        return {
            "top_price_changes": int(self.summary.get("top_price_changes_count", 0)),
            "price_drops": int(self.summary.get("price_drops_count", 0)),
            "major_increases": int(self.summary.get("major_increases_count", 0)),
            "new_products": int(self.summary.get("new_products_count", 0)),
        }


class AlertService:
    """Create stakeholder-facing alert outputs from processed datasets."""

    def __init__(
        self,
        *,
        processed_dir: Path,
        exports_dir: Path,
        scrape_run_repo: ScrapeRunRepository,
        snapshot_repo: ProductSnapshotRepository,
    ) -> None:
        self.processed_dir = Path(processed_dir)
        self.exports_dir = Path(exports_dir)
        self.scrape_run_repo = scrape_run_repo
        self.snapshot_repo = snapshot_repo

    def generate_source_alerts(
        self,
        source_name: str,
        *,
        recent_limit: int = 20,
        major_threshold_pct: Decimal = Decimal("15.00"),
    ) -> AlertReport:
        """Build alert outputs for one source from processed data plus run comparison metadata."""

        alerts_dir = self.exports_dir / source_name / "alerts"
        alerts_dir.mkdir(parents=True, exist_ok=True)

        latest_products_rows = self._load_processed_dataset(source_name, "latest_products_processed")
        price_changes_rows = self._load_processed_dataset(source_name, "price_changes_processed")
        run_summary_rows = self._load_processed_dataset(source_name, "run_summary_processed")

        sorted_price_changes = sorted(
            price_changes_rows,
            key=lambda row: self._as_decimal(row.get("percentage_difference")),
            reverse=True,
        )
        top_price_changes_rows = self._select_fields(
            sorted_price_changes[:recent_limit],
            TOP_PRICE_CHANGES_FIELDS,
        )
        price_drops_rows = self._select_fields(
            [
                row
                for row in sorted_price_changes
                if str(row.get("change_direction", "")).lower() == "decrease"
            ][:recent_limit],
            PRICE_DROPS_FIELDS,
        )
        major_increases_rows = self._select_fields(
            [
                row
                for row in sorted_price_changes
                if str(row.get("change_direction", "")).lower() == "increase"
                and self._as_decimal(row.get("percentage_difference")) >= major_threshold_pct
            ][:recent_limit],
            MAJOR_INCREASES_FIELDS,
        )

        latest_successful_run_id = self._latest_successful_run_id(run_summary_rows)
        previous_successful_run_id = self._previous_successful_run_id(
            source_name=source_name,
            latest_successful_run_id=latest_successful_run_id,
        )
        new_product_external_ids = self._detect_new_product_external_ids(
            latest_successful_run_id=latest_successful_run_id,
            previous_successful_run_id=previous_successful_run_id,
        )
        new_products_rows = self._build_new_products_rows(
            latest_products_rows=latest_products_rows,
            new_product_external_ids=new_product_external_ids,
            recent_limit=recent_limit,
        )

        summary = {
            "source_name": source_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "latest_successful_run_id": latest_successful_run_id,
            "previous_successful_run_id": previous_successful_run_id,
            # On the first successful run, we intentionally avoid alerting everything as "new".
            "baseline_mode": previous_successful_run_id is None,
            "major_increase_threshold_pct": str(major_threshold_pct),
            "total_current_products": len(latest_products_rows),
            "total_price_changes": len(price_changes_rows),
            "top_price_changes_count": len(top_price_changes_rows),
            "price_drops_count": len(price_drops_rows),
            "major_increases_count": len(major_increases_rows),
            "new_products_count": len(new_products_rows),
        }

        artifacts = [
            self._write_csv_dataset(
                alerts_dir=alerts_dir,
                file_name="top_price_changes.csv",
                fieldnames=TOP_PRICE_CHANGES_FIELDS,
                rows=top_price_changes_rows,
            ),
            self._write_csv_dataset(
                alerts_dir=alerts_dir,
                file_name="price_drops.csv",
                fieldnames=PRICE_DROPS_FIELDS,
                rows=price_drops_rows,
            ),
            self._write_csv_dataset(
                alerts_dir=alerts_dir,
                file_name="major_increases.csv",
                fieldnames=MAJOR_INCREASES_FIELDS,
                rows=major_increases_rows,
            ),
            self._write_csv_dataset(
                alerts_dir=alerts_dir,
                file_name="new_products.csv",
                fieldnames=NEW_PRODUCTS_FIELDS,
                rows=new_products_rows,
            ),
            self._write_json_artifact(
                alerts_dir=alerts_dir,
                file_name="alerts_summary.json",
                payload=summary,
            ),
            self._write_text_artifact(
                alerts_dir=alerts_dir,
                file_name="alerts_summary.txt",
                content=self._render_summary_text(summary),
            ),
        ]

        return AlertReport(
            source_name=source_name,
            alerts_dir=alerts_dir,
            artifacts=artifacts,
            summary=summary,
        )

    def _load_processed_dataset(self, source_name: str, file_stem: str) -> list[dict[str, Any]]:
        """Read one processed dataset from disk and fail clearly if it is missing."""

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

    def _latest_successful_run_id(self, run_summary_rows: list[dict[str, Any]]) -> int | None:
        """Find the latest successful run from the processed run summary file."""

        for row in run_summary_rows:
            success_flag = row.get("success_flag")
            status = str(row.get("status", "")).lower()
            if success_flag is True or status == "succeeded":
                scrape_run_id = row.get("scrape_run_id")
                if scrape_run_id is not None:
                    return int(scrape_run_id)
        return None

    def _previous_successful_run_id(
        self,
        *,
        source_name: str,
        latest_successful_run_id: int | None,
    ) -> int | None:
        """Resolve the previous successful run id so we can detect newly seen products."""

        if latest_successful_run_id is None:
            return None

        previous_run = self.scrape_run_repo.get_previous_successful_run(
            source_name=source_name,
            before_scrape_run_id=latest_successful_run_id,
        )
        if previous_run is None:
            return None
        return previous_run.id

    def _detect_new_product_external_ids(
        self,
        *,
        latest_successful_run_id: int | None,
        previous_successful_run_id: int | None,
    ) -> set[str]:
        """Compare the latest run to the previous successful run and return newly seen product ids."""

        if latest_successful_run_id is None or previous_successful_run_id is None:
            return set()

        latest_ids = {
            snapshot.external_id
            for snapshot in self.snapshot_repo.list_for_scrape_run(latest_successful_run_id)
        }
        previous_ids = {
            snapshot.external_id
            for snapshot in self.snapshot_repo.list_for_scrape_run(previous_successful_run_id)
        }
        return latest_ids - previous_ids

    def _build_new_products_rows(
        self,
        *,
        latest_products_rows: list[dict[str, Any]],
        new_product_external_ids: set[str],
        recent_limit: int,
    ) -> list[dict[str, Any]]:
        """Project processed latest-product rows into a new-products report."""

        latest_by_external_id = {
            str(row.get("external_id")): row for row in latest_products_rows if row.get("external_id")
        }
        rows = [
            latest_by_external_id[external_id]
            for external_id in new_product_external_ids
            if external_id in latest_by_external_id
        ]
        rows.sort(key=lambda row: (str(row.get("product_name", "")), str(row.get("external_id", ""))))
        return self._select_fields(rows[:recent_limit], NEW_PRODUCTS_FIELDS)

    def _select_fields(
        self,
        rows: list[dict[str, Any]],
        fieldnames: list[str],
    ) -> list[dict[str, Any]]:
        """Project rows into the exact output shape expected by an artifact."""

        return [{field: row.get(field) for field in fieldnames} for row in rows]

    def _write_csv_dataset(
        self,
        *,
        alerts_dir: Path,
        file_name: str,
        fieldnames: list[str],
        rows: list[dict[str, Any]],
    ) -> AlertArtifact:
        """Write a CSV alert artifact with stable headers."""

        file_path = alerts_dir / file_name
        with file_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        field: self._serialize_value(row.get(field), for_csv=True)
                        for field in fieldnames
                    }
                )

        return AlertArtifact(
            name=file_name,
            path=file_path,
            row_count=len(rows),
        )

    def _write_json_artifact(
        self,
        *,
        alerts_dir: Path,
        file_name: str,
        payload: dict[str, Any],
    ) -> AlertArtifact:
        """Write the summary JSON artifact."""

        file_path = alerts_dir / file_name
        file_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return AlertArtifact(name=file_name, path=file_path, row_count=None)

    def _write_text_artifact(
        self,
        *,
        alerts_dir: Path,
        file_name: str,
        content: str,
    ) -> AlertArtifact:
        """Write the summary text artifact for easy sharing or copy/paste."""

        file_path = alerts_dir / file_name
        file_path.write_text(content, encoding="utf-8")
        return AlertArtifact(name=file_name, path=file_path, row_count=None)

    def _render_summary_text(self, summary: dict[str, Any]) -> str:
        """Render a compact human-readable alert summary."""

        return "\n".join(
            [
                f"Alerts for {summary['source_name']}",
                f"Generated at: {summary['generated_at']}",
                f"Latest successful run id: {summary['latest_successful_run_id']}",
                f"Previous successful run id: {summary['previous_successful_run_id']}",
                f"Baseline mode: {'yes' if summary['baseline_mode'] else 'no'}",
                f"Current catalog size: {summary['total_current_products']}",
                f"Price changes available: {summary['total_price_changes']}",
                f"Top price changes exported: {summary['top_price_changes_count']}",
                f"Price drops exported: {summary['price_drops_count']}",
                (
                    "Major increases exported "
                    f"(>= {summary['major_increase_threshold_pct']}%): "
                    f"{summary['major_increases_count']}"
                ),
                f"New products exported: {summary['new_products_count']}",
            ]
        )

    def _serialize_value(self, value: Any, *, for_csv: bool) -> Any:
        """Convert values into JSON/CSV-safe representations."""

        if value is None:
            return "" if for_csv else None
        return value

    def _as_decimal(self, value: Any) -> Decimal:
        """Safely normalize processed numeric values so sorting/filtering stays predictable."""

        if value in (None, ""):
            return Decimal("0")
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal("0")
