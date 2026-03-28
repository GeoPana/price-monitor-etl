from __future__ import annotations

"""Frequent scrape-only DAG that fans out one task per enabled source."""

from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import dag, task

from pricemonitor.pipelines.airflow_entrypoints import (
    get_airflow_config_path,
    list_enabled_sources,
    run_scrape_task_for_source,
)

CONFIG_PATH = get_airflow_config_path()
ENABLED_SOURCES = list_enabled_sources(CONFIG_PATH)
DEFAULT_ARGS = {
    "owner": "pricemonitor",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pricemonitor_source_scrape",
    description="Scrape each enabled source on a more frequent cadence.",
    schedule="15 0,12,18 * * *",
    start_date=pendulum.datetime(2026, 3, 23, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    tags=["pricemonitor", "scrape"],
)
def pricemonitor_source_scrape():
    """Build one scrape task per enabled source."""

    @task(execution_timeout=timedelta(minutes=45))
    def scrape_source(source_name: str) -> dict[str, Any]:
        return run_scrape_task_for_source(
            source_name=source_name,
            config_path=CONFIG_PATH,
            limit=None,
        )

    for source_name in ENABLED_SOURCES:
        scrape_source.override(task_id=f"scrape_{source_name}")(source_name)


pricemonitor_source_scrape()
