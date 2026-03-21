from __future__ import annotations

"""CLI entry points for database setup, configuration inspection, and scraping."""

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy.exc import OperationalError

from pricemonitor.config import AppSettings, SourceSettings, load_settings
from pricemonitor.logging_config import configure_logging
from pricemonitor.scrapers.registry import get_scraper
from pricemonitor.services.change_detection import detect_price_changes
from pricemonitor.services.export import ExportService
from pricemonitor.storage.database import create_engine_from_url, create_session_factory
from pricemonitor.storage.migrations import upgrade_to_head
from pricemonitor.storage.repositories import (
    ProductSnapshotRepository,
    RawPageArchiveRepository,
    PriceChangeEventRepository,
    ScrapeRunRepository,
)

logger = logging.getLogger(__name__)


def _format_db_operational_error(exc: OperationalError, database_url: str) -> str:
    """Turn low-level database errors into actionable CLI output."""

    lower_message = str(exc).lower()
    if "password authentication failed" in lower_message:
        return (
            "Database authentication failed for the configured DATABASE_URL.\n"
            f"Resolved URL: {database_url}\n"
            "If you are using docker-compose.yaml from this repo, the expected credentials are "
            "`postgres/postgres` on `localhost:5433`.\n"
            "If a Postgres container already existed with different credentials, recreate it with "
            "`docker compose down -v` and `docker compose up -d db`, or update DATABASE_URL to match "
            "the actual password."
        )

    return (
        "Database connection failed.\n"
        f"Resolved URL: {database_url}\n"
        f"Original error: {exc}"
    )


def _resolve_target_sources(settings: AppSettings, source_name: str) -> list[str]:
    """Resolve a single source or the set of all enabled sources."""

    if source_name == "all":
        enabled_sources = [
            name for name, source_settings in settings.sources.items() if source_settings.enabled
        ]
        if not enabled_sources:
            raise ValueError("No enabled sources found for --source all.")
        return enabled_sources

    if source_name not in settings.sources:
        raise ValueError(f"Unknown source: {source_name}")

    source_settings = settings.sources[source_name]
    if not source_settings.enabled:
        raise ValueError(f"Source '{source_name}' is disabled.")

    return [source_name]


def _run_single_source_scrape(
    *,
    settings: AppSettings,
    source_name: str,
    source_settings: SourceSettings,
    limit: int | None,
) -> None:
    """Run the full scrape/persist/archive flow for a single source."""

    try:
        engine = create_engine_from_url(settings.database_url)
        session_factory = create_session_factory(engine)

        with session_factory() as session:
            scrape_run_repo = ScrapeRunRepository(session)
            snapshot_repo = ProductSnapshotRepository(session)
            change_event_repo = PriceChangeEventRepository(session)
            raw_archive_repo = RawPageArchiveRepository(settings.raw_dir)

            scrape_run = scrape_run_repo.create_scrape_run(source_name)
            # Commit the run record first so downstream failures can still be linked to it.
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
            except Exception as exc:
                session.rollback()
                scrape_run_repo.fail_scrape_run(
                    scrape_run.id,
                    error_message=str(exc),
                )
                session.commit()
                logger.exception("Scrape failed for source=%s", source_name)
                raise
    except OperationalError as exc:
        raise SystemExit(_format_db_operational_error(exc, settings.database_url)) from exc


def _run_single_source_export(
    *,
    settings: AppSettings,
    source_name: str,
    limit: int,
) -> None:
    """Build CSV and JSON exports for one source from already-stored database data."""

    try:
        engine = create_engine_from_url(settings.database_url)
        session_factory = create_session_factory(engine)

        with session_factory() as session:
            export_service = ExportService(
                exports_dir=settings.exports_dir,
                scrape_run_repo=ScrapeRunRepository(session),
                snapshot_repo=ProductSnapshotRepository(session),
                change_event_repo=PriceChangeEventRepository(session),
            )
            report = export_service.export_source_report(source_name, recent_limit=limit)
            counts = report.counts()

            logger.info(
                (
                    "Export completed for source=%s latest_products=%s "
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
    except OperationalError as exc:
        raise SystemExit(_format_db_operational_error(exc, settings.database_url)) from exc
    

def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI parser."""

    parser = argparse.ArgumentParser(prog="pricemonitor", description="Price Monitor ETL CLI")
    parser.add_argument(
        "--config",
        default="configs/settings.yaml",
        help="Path to the main YAML configuration file.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Apply Alembic migrations up to head.")
    subparsers.add_parser("show-config", help="Print resolved application configuration.")

    scrape_parser = subparsers.add_parser("scrape", help="Run a scrape for a configured source or all enabled sources.")
    scrape_parser.add_argument("--source", required=True, help="Source name, e.g. site_a")
    scrape_parser.add_argument("--limit", type=int, default=None, help="Optional record limit")

    export_parser = subparsers.add_parser(
        "export",
        help="Write business-facing CSV and JSON exports for a source or all enabled sources.",
    )
    export_parser.add_argument("--source", required=True, help="Source name, e.g. site_a, or all")
    export_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max rows for price change and run summary exports.",
    )

    return parser


def handle_init_db(config_path: str) -> int:
    """Apply Alembic migrations to the configured database."""

    settings = load_settings(config_path)
    try:
        upgrade_to_head(config_path)
    except OperationalError as exc:
        raise SystemExit(_format_db_operational_error(exc, settings.database_url)) from exc
    print("Database schema is up to date.")
    return 0


def handle_show_config(config_path: str) -> int:
    """Print the resolved configuration for inspection and debugging."""

    settings = load_settings(config_path)
    print(json.dumps(settings.model_dump(mode="json"), indent=2))
    return 0


def handle_scrape(config_path: str, source_name: str, limit: int | None) -> int:
    """Run a scrape for a configured source or all enabled sources and persist the results."""

    settings = load_settings(config_path)
    configure_logging(settings.log_level, settings.log_file)

    target_sources = _resolve_target_sources(settings, source_name)
    exit_code = 0

    for resolved_source_name in target_sources:
        try:
            _run_single_source_scrape(
                settings=settings,
                source_name=resolved_source_name,
                source_settings=settings.sources[resolved_source_name],
                limit=limit,
            )
        except Exception:
            exit_code = 1
            if source_name != "all":
                raise
            logger.exception("Scrape failed for source=%s during all-source run", resolved_source_name)

    if source_name == "all" and exit_code == 0:
        print(f"All enabled scrapes completed successfully: {', '.join(target_sources)}")

    return exit_code


def handle_export(config_path: str, source_name: str, limit: int) -> int:
    """Generate business-facing CSV and JSON exports for one source or all enabled sources."""

    settings = load_settings(config_path)
    configure_logging(settings.log_level, settings.log_file)

    target_sources = _resolve_target_sources(settings, source_name)
    exit_code = 0

    for resolved_source_name in target_sources:
        try:
            _run_single_source_export(
                settings=settings,
                source_name=resolved_source_name,
                limit=limit,
            )
        except Exception:
            exit_code = 1
            if source_name != "all":
                raise
            logger.exception("Export failed for source=%s during all-source run", resolved_source_name)

    if source_name == "all" and exit_code == 0:
        print(f"All enabled exports completed successfully: {', '.join(target_sources)}")

    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch CLI subcommands."""

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init-db":
        settings = load_settings(args.config)
        configure_logging(settings.log_level, settings.log_file)
        return handle_init_db(args.config)

    if args.command == "show-config":
        return handle_show_config(args.config)

    if args.command == "scrape":
        return handle_scrape(args.config, args.source, args.limit)
    
    if args.command == "export":
        return handle_export(args.config, args.source, args.limit)

    parser.error(f"Unsupported command: {args.command}")
    return 2


def cli() -> None:
    """Console script entry point."""

    raise SystemExit(main())


if __name__ == "__main__":
    cli()
