from __future__ import annotations

"""Thin wrappers that let Airflow call the existing pipeline modules."""

import os
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from pricemonitor.config import AppSettings, SourceSettings, load_settings
from pricemonitor.logging_config import configure_logging
from pricemonitor.pipelines.alert_run import run_alert_for_source
from pricemonitor.pipelines.process_run import run_process_for_source
from pricemonitor.pipelines.report_run import run_report_for_source
from pricemonitor.pipelines.scrape_run import run_scrape_for_source

DEFAULT_AIRFLOW_CONFIG_PATH = "/opt/airflow/project/configs/settings.yaml"


def get_airflow_config_path(config_path: str | None = None) -> str:
    """Resolve the config path for Airflow tasks from an explicit argument or env var."""

    return config_path or os.getenv("PRICEMONITOR_CONFIG", DEFAULT_AIRFLOW_CONFIG_PATH)


def list_enabled_sources(config_path: str | None = None) -> list[str]:
    """Return the enabled source names that Airflow DAGs should materialize as tasks."""

    settings = load_settings(get_airflow_config_path(config_path))
    return sorted(
        name
        for name, source_settings in settings.sources.items()
        if source_settings.enabled
    )


def run_scrape_task_for_source(
    *,
    source_name: str,
    config_path: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Execute the existing scrape pipeline for one source and return an XCom-safe summary."""

    settings = _load_runtime_settings(config_path)
    source_settings = _get_enabled_source_settings(settings, source_name)
    result = run_scrape_for_source(
        settings=settings,
        source_name=source_name,
        source_settings=source_settings,
        limit=limit,
    )
    return _json_safe(result)


def run_report_bundle_for_source(
    *,
    source_name: str,
    config_path: str | None = None,
    report_limit: int = 50,
    major_threshold_pct: Decimal | float = Decimal("15.00"),
) -> dict[str, Any]:
    """Refresh the processed, exported, and alert outputs for one source."""

    settings = _load_runtime_settings(config_path)
    _get_enabled_source_settings(settings, source_name)

    process_result = run_process_for_source(
        settings=settings,
        source_name=source_name,
        limit=report_limit,
    )
    export_result = run_report_for_source(
        settings=settings,
        source_name=source_name,
        limit=report_limit,
    )
    alert_result = run_alert_for_source(
        settings=settings,
        source_name=source_name,
        limit=report_limit,
        major_threshold_pct=major_threshold_pct,
    )

    return _json_safe(
        {
            "source_name": source_name,
            "process": process_result,
            "export": export_result,
            "alert": alert_result,
        }
    )


def run_end_to_end_for_source(
    *,
    source_name: str,
    config_path: str | None = None,
    scrape_limit: int | None = None,
    report_limit: int = 50,
    major_threshold_pct: Decimal | float = Decimal("15.00"),
) -> dict[str, Any]:
    """Run scrape, process, export, and alert for one source through existing modules."""

    settings = _load_runtime_settings(config_path)
    source_settings = _get_enabled_source_settings(settings, source_name)

    scrape_result = run_scrape_for_source(
        settings=settings,
        source_name=source_name,
        source_settings=source_settings,
        limit=scrape_limit,
    )
    process_result = run_process_for_source(
        settings=settings,
        source_name=source_name,
        limit=report_limit,
    )
    export_result = run_report_for_source(
        settings=settings,
        source_name=source_name,
        limit=report_limit,
    )
    alert_result = run_alert_for_source(
        settings=settings,
        source_name=source_name,
        limit=report_limit,
        major_threshold_pct=major_threshold_pct,
    )

    return _json_safe(
        {
            "source_name": source_name,
            "scrape": scrape_result,
            "process": process_result,
            "export": export_result,
            "alert": alert_result,
        }
    )


def _load_runtime_settings(config_path: str | None) -> AppSettings:
    """Load app settings and configure logging for code paths executed inside Airflow tasks."""

    settings = load_settings(get_airflow_config_path(config_path))
    configure_logging(settings.log_level, settings.log_file)
    return settings


def _get_enabled_source_settings(settings: AppSettings, source_name: str) -> SourceSettings:
    """Fail early if a DAG references an unknown or disabled source."""

    if source_name not in settings.sources:
        raise ValueError(f"Unknown source: {source_name}")

    source_settings = settings.sources[source_name]
    if not source_settings.enabled:
        raise ValueError(f"Source '{source_name}' is disabled.")

    return source_settings


def _json_safe(value: Any) -> Any:
    """Convert dataclasses and filesystem paths into XCom-friendly JSON values."""

    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    return value
