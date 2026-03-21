from __future__ import annotations

"""Read-only data access for the HTTP API."""

import csv
import json
from pathlib import Path
from typing import Any

from pricemonitor.config import AppSettings
from pricemonitor.storage.repositories import ScrapeRunRepository


class ReadApiService:
    """Read API-facing datasets from DB-backed run history and generated output files."""

    def __init__(
        self,
        *,
        settings: AppSettings,
        scrape_run_repo: ScrapeRunRepository,
    ) -> None:
        self.settings = settings
        self.scrape_run_repo = scrape_run_repo

    def list_sources(self) -> list[dict[str, Any]]:
        """Return configured sources in stable name order."""

        return [
            {
                "name": source_settings.name,
                "enabled": source_settings.enabled,
                "base_url": source_settings.base_url,
                "scraper": source_settings.scraper,
                "fetcher": source_settings.fetcher,
            }
            for _, source_settings in sorted(self.settings.sources.items())
        ]

    def list_runs(
        self,
        *,
        source_name: str | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Return recent runs as an API-friendly read model."""

        if source_name is not None:
            self._require_source_exists(source_name)

        runs = self.scrape_run_repo.list_recent(
            source_name=source_name,
            limit=limit + offset,
        )
        rows = [
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
                "insert_rate": self._ratio(scrape_run.records_inserted, scrape_run.records_fetched),
                # Today, inserted records are the validated records that made it into storage.
                "validity_rate": self._ratio(scrape_run.records_inserted, scrape_run.records_fetched),
                "error_message": scrape_run.error_message,
            }
            for scrape_run in runs
        ]
        return self._slice_rows(rows, limit=limit, offset=offset)

    def list_latest_products(
        self,
        *,
        source_name: str,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Read the latest-products export JSON for one source."""

        self._require_source_exists(source_name)
        rows = self._load_json_list(
            self.settings.exports_dir / source_name / "latest_products.json",
            hint=f"Run `pricemonitor export --source {source_name}` or `pricemonitor run --source {source_name}` first.",
        )
        return self._slice_rows(rows, limit=limit, offset=offset)

    def list_price_changes(
        self,
        *,
        source_name: str,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Read the price-changes export JSON for one source."""

        self._require_source_exists(source_name)
        rows = self._load_json_list(
            self.settings.exports_dir / source_name / "price_changes.json",
            hint=f"Run `pricemonitor export --source {source_name}` or `pricemonitor run --source {source_name}` first.",
        )
        return self._slice_rows(rows, limit=limit, offset=offset)

    def get_alert_summary(self, *, source_name: str) -> dict[str, Any]:
        """Read the alert summary JSON for one source."""

        self._require_source_exists(source_name)
        return self._load_json_object(
            self.settings.exports_dir / source_name / "alerts" / "alerts_summary.json",
            hint=f"Run `pricemonitor alert --source {source_name}` or `pricemonitor run --source {source_name}` first.",
        )

    def list_alert_top_price_changes(
        self,
        *,
        source_name: str,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Read the top-price-changes alert CSV for one source."""

        self._require_source_exists(source_name)
        rows = self._load_csv_rows(
            self.settings.exports_dir / source_name / "alerts" / "top_price_changes.csv",
            hint=f"Run `pricemonitor alert --source {source_name}` or `pricemonitor run --source {source_name}` first.",
        )
        return self._slice_rows(rows, limit=limit, offset=offset)

    def list_alert_new_products(
        self,
        *,
        source_name: str,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Read the new-products alert CSV for one source."""

        self._require_source_exists(source_name)
        rows = self._load_csv_rows(
            self.settings.exports_dir / source_name / "alerts" / "new_products.csv",
            hint=f"Run `pricemonitor alert --source {source_name}` or `pricemonitor run --source {source_name}` first.",
        )
        return self._slice_rows(rows, limit=limit, offset=offset)

    def _require_source_exists(self, source_name: str) -> None:
        """Raise a readable error for unknown source names."""

        if source_name not in self.settings.sources:
            raise ValueError(f"Unknown source: {source_name}")

    def _load_json_list(self, path: Path, *, hint: str) -> list[dict[str, Any]]:
        """Load a JSON array from disk and validate its shape."""

        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}. {hint}")

        rows = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError(f"Expected a JSON list in dataset: {path}")
        return rows

    def _load_json_object(self, path: Path, *, hint: str) -> dict[str, Any]:
        """Load a JSON object from disk and validate its shape."""

        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}. {hint}")

        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Expected a JSON object in dataset: {path}")
        return payload

    def _load_csv_rows(self, path: Path, *, hint: str) -> list[dict[str, Any]]:
        """Load a CSV dataset from disk as a list of dictionaries."""

        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}. {hint}")

        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def _slice_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Apply simple offset/limit pagination to already materialized rows."""

        return rows[offset : offset + limit]

    def _duration_seconds(self, scrape_run: Any) -> float | None:
        """Compute runtime only when the run has both endpoints."""

        if scrape_run.finished_at is None:
            return None
        return round((scrape_run.finished_at - scrape_run.started_at).total_seconds(), 2)

    def _ratio(self, numerator: int, denominator: int) -> float | None:
        """Return a rounded ratio, or None when the denominator is zero."""

        if denominator == 0:
            return None
        return round(numerator / denominator, 4)
