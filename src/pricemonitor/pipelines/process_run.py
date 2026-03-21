from __future__ import annotations

"""Pipeline entry points for processed-data runs."""

import logging
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.exc import OperationalError

from pricemonitor.config import AppSettings, load_settings
from pricemonitor.logging_config import configure_logging
from pricemonitor.pipelines.common import format_db_operational_error, resolve_target_sources
from pricemonitor.services.process import ProcessService
from pricemonitor.storage.database import create_engine_from_url, create_session_factory
from pricemonitor.storage.repositories import (
    PriceChangeEventRepository,
    ProductSnapshotRepository,
    ScrapeRunRepository,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ProcessSourceResult:
    """Summary of one completed processed-data run."""

    source_name: str
    latest_products_count: int
    price_changes_count: int
    run_summary_count: int
    processed_dir: Path


def run_process_for_source(
    *,
    settings: AppSettings,
    source_name: str,
    limit: int,
) -> ProcessSourceResult:
    """Generate processed datasets for one source from stored DB records."""

    engine = create_engine_from_url(settings.database_url)
    try:
        session_factory = create_session_factory(engine)

        with session_factory() as session:
            process_service = ProcessService(
                processed_dir=settings.processed_dir,
                scrape_run_repo=ScrapeRunRepository(session),
                snapshot_repo=ProductSnapshotRepository(session),
                change_event_repo=PriceChangeEventRepository(session),
            )
            report = process_service.process_source_data(source_name, recent_limit=limit)
            counts = report.counts()

            logger.info(
                (
                    "Process completed for source=%s latest_products=%s "
                    "price_changes=%s run_summary=%s dir=%s"
                ),
                source_name,
                counts["latest_products_processed"],
                counts["price_changes_processed"],
                counts["run_summary_processed"],
                report.processed_dir,
            )
            print(
                f"Process completed for {source_name}: "
                f"latest_products={counts['latest_products_processed']} "
                f"price_changes={counts['price_changes_processed']} "
                f"run_summary={counts['run_summary_processed']} "
                f"dir={report.processed_dir}"
            )

            return ProcessSourceResult(
                source_name=source_name,
                latest_products_count=counts["latest_products_processed"],
                price_changes_count=counts["price_changes_processed"],
                run_summary_count=counts["run_summary_processed"],
                processed_dir=report.processed_dir,
            )
    except OperationalError as exc:
        raise SystemExit(format_db_operational_error(exc, settings.database_url)) from exc
    finally:
        engine.dispose()


def run_process_pipeline(config_path: str, source_name: str, limit: int) -> int:
    """Run processed-data pipelines for one source or all enabled sources."""

    settings = load_settings(config_path)
    configure_logging(settings.log_level, settings.log_file)

    target_sources = resolve_target_sources(settings, source_name)
    completed_sources: list[str] = []
    failed_sources: list[str] = []

    for resolved_source_name in target_sources:
        try:
            run_process_for_source(
                settings=settings,
                source_name=resolved_source_name,
                limit=limit,
            )
            completed_sources.append(resolved_source_name)
        except Exception:
            failed_sources.append(resolved_source_name)
            if source_name != "all":
                raise
            logger.exception(
                "Process pipeline failed for source=%s during all-source orchestration",
                resolved_source_name,
            )

    if source_name == "all":
        if failed_sources:
            print(
                "Process pipelines completed for successful sources: "
                f"{', '.join(completed_sources)}; failed: {', '.join(failed_sources)}"
            )
        else:
            print(f"All enabled process pipelines completed successfully: {', '.join(completed_sources)}")

    return 0 if not failed_sources else 1
