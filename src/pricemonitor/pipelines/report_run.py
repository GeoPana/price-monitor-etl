from __future__ import annotations

"""Pipeline entry points for reporting/export runs."""

import logging
from dataclasses import dataclass
from pathlib import Path

from pricemonitor.config import AppSettings, load_settings
from pricemonitor.logging_config import configure_logging
from pricemonitor.pipelines.common import resolve_target_sources
from pricemonitor.services.export import ExportService

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ReportSourceResult:
    """Summary of one completed reporting/export pipeline run."""

    source_name: str
    latest_products_count: int
    price_changes_count: int
    run_summary_count: int
    export_dir: Path


def run_report_for_source(
    *,
    settings: AppSettings,
    source_name: str,
    limit: int,
) -> ReportSourceResult:
    """Generate all business-facing exports for one source from processed datasets."""

    export_service = ExportService(
        processed_dir=settings.processed_dir,
        exports_dir=settings.exports_dir,
    )
    report = export_service.export_source_report(source_name, recent_limit=limit)
    counts = report.counts()

    logger.info(
        (
            "Report completed for source=%s latest_products=%s "
            "price_changes=%s run_summary=%s dir=%s"
        ),
        source_name,
        counts["latest_products"],
        counts["price_changes"],
        counts["run_summary"],
        report.export_dir,
    )
    print(
        f"Export completed for {source_name}: "
        f"latest_products={counts['latest_products']} "
        f"price_changes={counts['price_changes']} "
        f"run_summary={counts['run_summary']} "
        f"dir={report.export_dir}"
    )

    return ReportSourceResult(
        source_name=source_name,
        latest_products_count=counts["latest_products"],
        price_changes_count=counts["price_changes"],
        run_summary_count=counts["run_summary"],
        export_dir=report.export_dir,
    )


def run_report_pipeline(config_path: str, source_name: str, limit: int) -> int:
    """Run reporting/export pipelines for one source or all enabled sources."""

    settings = load_settings(config_path)
    configure_logging(settings.log_level, settings.log_file)

    target_sources = resolve_target_sources(settings, source_name)
    completed_sources: list[str] = []
    failed_sources: list[str] = []

    for resolved_source_name in target_sources:
        try:
            run_report_for_source(
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
                "Report pipeline failed for source=%s during all-source orchestration",
                resolved_source_name,
            )

    if source_name == "all":
        if failed_sources:
            print(
                "Report pipelines completed for successful sources: "
                f"{', '.join(completed_sources)}; failed: {', '.join(failed_sources)}"
            )
        else:
            print(f"All enabled report pipelines completed successfully: {', '.join(completed_sources)}")

    return 0 if not failed_sources else 1
