from __future__ import annotations

"""Pipeline entry points for scrape runs."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.exc import OperationalError

from pricemonitor.config import AppSettings, SourceSettings, load_settings
from pricemonitor.logging_config import configure_logging
from pricemonitor.pipelines.common import format_db_operational_error, resolve_target_sources
from pricemonitor.scrapers.registry import get_scraper
from pricemonitor.services.change_detection import detect_price_changes
from pricemonitor.storage.database import create_engine_from_url, create_session_factory
from pricemonitor.storage.repositories import (
    PriceChangeEventRepository,
    ProductSnapshotRepository,
    RawPageArchiveRepository,
    ScrapeRunRepository,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ScrapeSourceResult:
    """Summary of one completed scrape pipeline run."""

    source_name: str
    scrape_run_id: int
    fetched_count: int
    valid_count: int
    invalid_count: int
    inserted_count: int
    archived_count: int
    change_count: int


def run_scrape_for_source(
    *,
    settings: AppSettings,
    source_name: str,
    source_settings: SourceSettings,
    limit: int | None,
) -> ScrapeSourceResult:
    """Run the full scrape pipeline for one source."""

    engine = create_engine_from_url(settings.database_url)
    try:
        session_factory = create_session_factory(engine)

        with session_factory() as session:
            scrape_run_repo = ScrapeRunRepository(session)
            snapshot_repo = ProductSnapshotRepository(session)
            change_event_repo = PriceChangeEventRepository(session)
            raw_archive_repo = RawPageArchiveRepository(settings.raw_dir)

            scrape_run = scrape_run_repo.create_scrape_run(source_name)
            # Commit immediately so downstream failures still have a persistent run id.
            session.commit()

            try:
                scraper = get_scraper(source_name, source_settings)
                products = scraper.scrape(limit=limit)

                scraper_stats = getattr(scraper, "last_scrape_stats", {})
                archived_pages = getattr(scraper, "last_archived_pages", [])

                fetched_count = scraper_stats.get("raw_records", len(products))
                valid_count = scraper_stats.get("valid_records", len(products))
                invalid_count = scraper_stats.get(
                    "invalid_records",
                    max(fetched_count - valid_count, 0),
                )

                archived_files = raw_archive_repo.archive_pages(
                    source_name=source_name,
                    scrape_run_id=scrape_run.id,
                    pages=archived_pages,
                )

                scraped_at = datetime.now(timezone.utc)
                inserted_count = snapshot_repo.insert_product_snapshots(
                    scrape_run_id=scrape_run.id,
                    source_name=source_name,
                    products=products,
                    scraped_at=scraped_at,
                )

                current_snapshots = snapshot_repo.list_for_scrape_run(scrape_run.id)
                previous_run = scrape_run_repo.get_previous_successful_run(
                    source_name=source_name,
                    before_scrape_run_id=scrape_run.id,
                )
                previous_snapshots = (
                    snapshot_repo.list_for_scrape_run(previous_run.id)
                    if previous_run is not None
                    else []
                )

                change_events = detect_price_changes(
                    source_name=source_name,
                    scrape_run_id=scrape_run.id,
                    current_snapshots=current_snapshots,
                    previous_snapshots=previous_snapshots,
                    changed_at=scraped_at,
                )
                change_count = change_event_repo.insert_price_change_events(change_events)

                scrape_run_repo.complete_scrape_run(
                    scrape_run.id,
                    records_fetched=fetched_count,
                    records_inserted=inserted_count,
                )
                session.commit()

                logger.info(
                    (
                        "Scrape completed for source=%s fetched=%s valid=%s "
                        "invalid=%s inserted=%s archived=%s changes=%s"
                    ),
                    source_name,
                    fetched_count,
                    valid_count,
                    invalid_count,
                    inserted_count,
                    len(archived_files),
                    change_count,
                )
                print(
                    f"Scrape completed for {source_name}: "
                    f"fetched={fetched_count} valid={valid_count} "
                    f"invalid={invalid_count} inserted={inserted_count} "
                    f"archived={len(archived_files)} changes={change_count}"
                )

                return ScrapeSourceResult(
                    source_name=source_name,
                    scrape_run_id=scrape_run.id,
                    fetched_count=fetched_count,
                    valid_count=valid_count,
                    invalid_count=invalid_count,
                    inserted_count=inserted_count,
                    archived_count=len(archived_files),
                    change_count=change_count,
                )
            except Exception as exc:
                session.rollback()
                scrape_run_repo.fail_scrape_run(
                    scrape_run.id,
                    error_message=str(exc),
                )
                session.commit()
                logger.exception(
                    "Scrape pipeline failed for source=%s scrape_run_id=%s",
                    source_name,
                    scrape_run.id,
                )
                raise
    except OperationalError as exc:
        raise SystemExit(format_db_operational_error(exc, settings.database_url)) from exc
    finally:
        engine.dispose()


def run_scrape_pipeline(config_path: str, source_name: str, limit: int | None) -> int:
    """Run scrape pipelines for one source or all enabled sources."""

    settings = load_settings(config_path)
    configure_logging(settings.log_level, settings.log_file)

    target_sources = resolve_target_sources(settings, source_name)
    completed_sources: list[str] = []
    failed_sources: list[str] = []

    for resolved_source_name in target_sources:
        try:
            run_scrape_for_source(
                settings=settings,
                source_name=resolved_source_name,
                source_settings=settings.sources[resolved_source_name],
                limit=limit,
            )
            completed_sources.append(resolved_source_name)
        except Exception:
            failed_sources.append(resolved_source_name)
            if source_name != "all":
                raise
            logger.exception(
                "Scrape pipeline failed for source=%s during all-source orchestration",
                resolved_source_name,
            )

    if source_name == "all":
        if failed_sources:
            print(
                "Scrape pipelines completed for successful sources: "
                f"{', '.join(completed_sources)}; failed: {', '.join(failed_sources)}"
            )
        else:
            print(f"All enabled scrape pipelines completed successfully: {', '.join(completed_sources)}")

    return 0 if not failed_sources else 1
