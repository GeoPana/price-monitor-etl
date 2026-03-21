from __future__ import annotations

"""Pipeline entry points for alert-generation runs."""

import logging
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from sqlalchemy.exc import OperationalError

from pricemonitor.config import AppSettings, load_settings
from pricemonitor.logging_config import configure_logging
from pricemonitor.pipelines.common import format_db_operational_error, resolve_target_sources
from pricemonitor.services.alert import AlertService
from pricemonitor.storage.database import create_engine_from_url, create_session_factory
from pricemonitor.storage.repositories import ProductSnapshotRepository, ScrapeRunRepository

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AlertSourceResult:
    """Summary of one completed alert pipeline run."""

    source_name: str
    top_price_changes_count: int
    price_drops_count: int
    major_increases_count: int
    new_products_count: int
    alerts_dir: Path


def run_alert_for_source(
    *,
    settings: AppSettings,
    source_name: str,
    limit: int,
    major_threshold_pct: Decimal | float = Decimal("15.00"),
) -> AlertSourceResult:
    """Generate alert artifacts for one source from processed data plus run comparison metadata."""

    threshold = Decimal(str(major_threshold_pct))
    engine = create_engine_from_url(settings.database_url)
    try:
        session_factory = create_session_factory(engine)

        with session_factory() as session:
            alert_service = AlertService(
                processed_dir=settings.processed_dir,
                exports_dir=settings.exports_dir,
                scrape_run_repo=ScrapeRunRepository(session),
                snapshot_repo=ProductSnapshotRepository(session),
            )
            report = alert_service.generate_source_alerts(
                source_name,
                recent_limit=limit,
                major_threshold_pct=threshold,
            )
            counts = report.counts()

            logger.info(
                (
                    "Alert completed for source=%s top_price_changes=%s "
                    "price_drops=%s major_increases=%s new_products=%s dir=%s"
                ),
                source_name,
                counts["top_price_changes"],
                counts["price_drops"],
                counts["major_increases"],
                counts["new_products"],
                report.alerts_dir,
            )
            print(
                f"Alert completed for {source_name}: "
                f"top_price_changes={counts['top_price_changes']} "
                f"price_drops={counts['price_drops']} "
                f"major_increases={counts['major_increases']} "
                f"new_products={counts['new_products']} "
                f"dir={report.alerts_dir}"
            )

            return AlertSourceResult(
                source_name=source_name,
                top_price_changes_count=counts["top_price_changes"],
                price_drops_count=counts["price_drops"],
                major_increases_count=counts["major_increases"],
                new_products_count=counts["new_products"],
                alerts_dir=report.alerts_dir,
            )
    except OperationalError as exc:
        raise SystemExit(format_db_operational_error(exc, settings.database_url)) from exc
    finally:
        engine.dispose()


def run_alert_pipeline(
    config_path: str,
    source_name: str,
    limit: int,
    major_threshold_pct: Decimal | float = Decimal("15.00"),
) -> int:
    """Run alert pipelines for one source or all enabled sources."""

    settings = load_settings(config_path)
    configure_logging(settings.log_level, settings.log_file)

    target_sources = resolve_target_sources(settings, source_name)
    completed_sources: list[str] = []
    failed_sources: list[str] = []

    for resolved_source_name in target_sources:
        try:
            run_alert_for_source(
                settings=settings,
                source_name=resolved_source_name,
                limit=limit,
                major_threshold_pct=major_threshold_pct,
            )
            completed_sources.append(resolved_source_name)
        except Exception:
            failed_sources.append(resolved_source_name)
            if source_name != "all":
                raise
            logger.exception(
                "Alert pipeline failed for source=%s during all-source orchestration",
                resolved_source_name,
            )

    if source_name == "all":
        if failed_sources:
            print(
                "Alert pipelines completed for successful sources: "
                f"{', '.join(completed_sources)}; failed: {', '.join(failed_sources)}"
            )
        else:
            print(f"All enabled alert pipelines completed successfully: {', '.join(completed_sources)}")

    return 0 if not failed_sources else 1
