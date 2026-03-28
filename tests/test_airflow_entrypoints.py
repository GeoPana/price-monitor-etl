from __future__ import annotations

"""Tests for the thin Airflow entrypoint wrappers."""

from pathlib import Path

from pricemonitor.config import AppSettings, SourceSettings
from pricemonitor.pipelines.airflow_entrypoints import (
    list_enabled_sources,
    run_end_to_end_for_source,
    run_report_bundle_for_source,
)
from pricemonitor.pipelines.alert_run import AlertSourceResult
from pricemonitor.pipelines.process_run import ProcessSourceResult
from pricemonitor.pipelines.report_run import ReportSourceResult
from pricemonitor.pipelines.scrape_run import ScrapeSourceResult


def test_list_enabled_sources_filters_disabled_sources(monkeypatch) -> None:
    """Only enabled sources should be materialized into Airflow tasks."""

    settings = AppSettings(
        app_name="Price Monitor ETL",
        environment="test",
        log_level="INFO",
        database_url="sqlite:///test.db",
        log_file=Path("logs/test.log"),
        raw_dir=Path("data/raw"),
        processed_dir=Path("data/processed"),
        exports_dir=Path("data/exports"),
        logs_dir=Path("logs"),
        sources={
            "site_a": SourceSettings(
                name="site_a",
                enabled=True,
                base_url="https://example.com/site-a",
                scraper="site_a",
                fetcher="http",
            ),
            "site_b": SourceSettings(
                name="site_b",
                enabled=False,
                base_url="https://example.com/site-b",
                scraper="site_b",
                fetcher="browser",
            ),
        },
    )

    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints.load_settings",
        lambda config_path: settings,
    )

    assert list_enabled_sources("configs/settings.yaml") == ["site_a"]


def test_run_end_to_end_for_source_reuses_existing_pipeline_functions(monkeypatch) -> None:
    """The Airflow wrapper should delegate to the same pipeline modules the CLI uses."""

    settings = AppSettings(
        app_name="Price Monitor ETL",
        environment="test",
        log_level="INFO",
        database_url="sqlite:///test.db",
        log_file=Path("logs/test.log"),
        raw_dir=Path("data/raw"),
        processed_dir=Path("data/processed"),
        exports_dir=Path("data/exports"),
        logs_dir=Path("logs"),
        sources={
            "site_a": SourceSettings(
                name="site_a",
                enabled=True,
                base_url="https://example.com/site-a",
                scraper="site_a",
                fetcher="http",
            )
        },
    )
    calls: list[str] = []

    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints._load_runtime_settings",
        lambda config_path: settings,
    )
    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints.run_scrape_for_source",
        lambda **kwargs: calls.append("scrape")
        or ScrapeSourceResult(
            source_name="site_a",
            scrape_run_id=11,
            fetched_count=5,
            valid_count=5,
            invalid_count=0,
            inserted_count=5,
            archived_count=2,
            change_count=1,
        ),
    )
    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints.run_process_for_source",
        lambda **kwargs: calls.append("process")
        or ProcessSourceResult(
            source_name="site_a",
            latest_products_count=5,
            price_changes_count=1,
            run_summary_count=3,
            processed_dir=Path("data/processed/site_a"),
        ),
    )
    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints.run_report_for_source",
        lambda **kwargs: calls.append("export")
        or ReportSourceResult(
            source_name="site_a",
            latest_products_count=5,
            price_changes_count=1,
            run_summary_count=3,
            export_dir=Path("data/exports/site_a"),
        ),
    )
    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints.run_alert_for_source",
        lambda **kwargs: calls.append("alert")
        or AlertSourceResult(
            source_name="site_a",
            top_price_changes_count=1,
            price_drops_count=1,
            major_increases_count=0,
            new_products_count=1,
            alerts_dir=Path("data/exports/site_a/alerts"),
        ),
    )

    result = run_end_to_end_for_source(source_name="site_a", config_path="configs/settings.yaml")

    assert calls == ["scrape", "process", "export", "alert"]
    assert result["source_name"] == "site_a"
    assert result["scrape"]["scrape_run_id"] == 11
    assert Path(result["process"]["processed_dir"]) == Path("data/processed/site_a")
    assert Path(result["export"]["export_dir"]) == Path("data/exports/site_a")
    assert Path(result["alert"]["alerts_dir"]) == Path("data/exports/site_a/alerts")


def test_run_report_bundle_for_source_serializes_path_outputs(monkeypatch) -> None:
    """Reporting wrapper should return XCom-safe values instead of raw Path objects."""

    settings = AppSettings(
        app_name="Price Monitor ETL",
        environment="test",
        log_level="INFO",
        database_url="sqlite:///test.db",
        log_file=Path("logs/test.log"),
        raw_dir=Path("data/raw"),
        processed_dir=Path("data/processed"),
        exports_dir=Path("data/exports"),
        logs_dir=Path("logs"),
        sources={
            "site_a": SourceSettings(
                name="site_a",
                enabled=True,
                base_url="https://example.com/site-a",
                scraper="site_a",
                fetcher="http",
            )
        },
    )

    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints._load_runtime_settings",
        lambda config_path: settings,
    )
    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints.run_process_for_source",
        lambda **kwargs: ProcessSourceResult(
            source_name="site_a",
            latest_products_count=5,
            price_changes_count=1,
            run_summary_count=3,
            processed_dir=Path("data/processed/site_a"),
        ),
    )
    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints.run_report_for_source",
        lambda **kwargs: ReportSourceResult(
            source_name="site_a",
            latest_products_count=5,
            price_changes_count=1,
            run_summary_count=3,
            export_dir=Path("data/exports/site_a"),
        ),
    )
    monkeypatch.setattr(
        "pricemonitor.pipelines.airflow_entrypoints.run_alert_for_source",
        lambda **kwargs: AlertSourceResult(
            source_name="site_a",
            top_price_changes_count=1,
            price_drops_count=1,
            major_increases_count=0,
            new_products_count=1,
            alerts_dir=Path("data/exports/site_a/alerts"),
        ),
    )

    result = run_report_bundle_for_source(source_name="site_a", config_path="configs/settings.yaml")

    assert Path(result["process"]["processed_dir"]) == Path("data/processed/site_a")
    assert Path(result["export"]["export_dir"]) == Path("data/exports/site_a")
    assert Path(result["alert"]["alerts_dir"]) == Path("data/exports/site_a/alerts")
