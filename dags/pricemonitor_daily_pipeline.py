from __future__ import annotations

"""Daily end-to-end DAG that runs the full pipeline for each enabled source."""

from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import dag, task

from pricemonitor.pipelines.airflow_entrypoints import (
    get_airflow_config_path,
    list_enabled_sources,
    run_end_to_end_for_source,
)

CONFIG_PATH = get_airflow_config_path()
ENABLED_SOURCES = list_enabled_sources(CONFIG_PATH)
DEFAULT_ARGS = {
    "owner": "pricemonitor",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="pricemonitor_daily_pipeline",
    description="Run scrape, process, export, and alert once per day for all enabled sources.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 3, 23, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    max_active_runs=1,
    tags=["pricemonitor", "daily", "pipeline"],
)
def pricemonitor_daily_pipeline():
    """Build one full-pipeline task per enabled source."""

    @task(execution_timeout=timedelta(minutes=90))
    def run_pipeline(source_name: str) -> dict[str, Any]:
        # Keep the DAG task thin by delegating to reusable project entrypoints.
        return run_end_to_end_for_source(
            source_name=source_name,
            config_path=CONFIG_PATH,
            scrape_limit=None,
            report_limit=50,
            major_threshold_pct=15.0,
        )

    for source_name in ENABLED_SOURCES:
        run_pipeline.override(task_id=f"run_pipeline_{source_name}")(source_name)


pricemonitor_daily_pipeline()
