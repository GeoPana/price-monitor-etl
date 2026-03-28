from __future__ import annotations

"""Refresh processed, export, and alert outputs from stored DB state."""

from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import dag, task

from pricemonitor.pipelines.airflow_entrypoints import (
    get_airflow_config_path,
    list_enabled_sources,
    run_report_bundle_for_source,
)

CONFIG_PATH = get_airflow_config_path()
ENABLED_SOURCES = list_enabled_sources(CONFIG_PATH)
DEFAULT_ARGS = {
    "owner": "pricemonitor",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="pricemonitor_reports",
    description="Regenerate processed datasets, exports, and alerts for all enabled sources.",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 3, 23, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    tags=["pricemonitor", "reports"],
)
def pricemonitor_reports():
    """Build one reporting task per enabled source."""

    @task(execution_timeout=timedelta(minutes=45))
    def refresh_reports(source_name: str) -> dict[str, Any]:
        return run_report_bundle_for_source(
            source_name=source_name,
            config_path=CONFIG_PATH,
            report_limit=50,
            major_threshold_pct=15.0,
        )

    for source_name in ENABLED_SOURCES:
        refresh_reports.override(task_id=f"refresh_reports_{source_name}")(source_name)


pricemonitor_reports()
