from __future__ import annotations

"""CLI entry points for database setup, configuration inspection, and scraping."""

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy.exc import OperationalError

from pricemonitor.config import load_settings
from pricemonitor.logging_config import configure_logging
from pricemonitor.models.schemas import ScrapeRunCreate, ScrapeRunUpdate
from pricemonitor.scrapers.registry import get_scraper
from pricemonitor.storage.database import create_engine_from_url, create_session_factory, init_db
from pricemonitor.storage.repositories import ProductSnapshotRepository, ScrapeRunRepository

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


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI parser."""

    parser = argparse.ArgumentParser(prog="pricemonitor", description="Price Monitor ETL CLI")
    parser.add_argument(
        "--config",
        default="configs/settings.yaml",
        help="Path to the main YAML configuration file.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Create database tables.")
    subparsers.add_parser("show-config", help="Print resolved application configuration.")

    scrape_parser = subparsers.add_parser("scrape", help="Run a scrape for a specific source.")
    scrape_parser.add_argument("--source", required=True, help="Source name, e.g. site_a")
    scrape_parser.add_argument("--limit", type=int, default=None, help="Optional record limit")

    return parser


def handle_init_db(database_url: str) -> int:
    """Initialize the configured database schema."""

    try:
        engine = create_engine_from_url(database_url)
        init_db(engine)
    except OperationalError as exc:
        raise SystemExit(_format_db_operational_error(exc, database_url)) from exc
    print("Database initialized.")
    return 0


def handle_show_config(config_path: str) -> int:
    """Print the resolved configuration for inspection and debugging."""

    settings = load_settings(config_path)
    print(json.dumps(settings.model_dump(mode="json"), indent=2))
    return 0


def handle_scrape(config_path: str, source_name: str, limit: int | None) -> int:
    """Run a scrape for a configured source and persist its results."""

    settings = load_settings(config_path)
    configure_logging(settings.log_level, settings.log_file)

    if source_name not in settings.sources:
        raise ValueError(f"Unknown source: {source_name}")

    source_settings = settings.sources[source_name]
    if not source_settings.enabled:
        raise ValueError(f"Source '{source_name}' is disabled.")

    try:
        engine = create_engine_from_url(settings.database_url)
        session_factory = create_session_factory(engine)

        with session_factory() as session:
            scrape_run_repo = ScrapeRunRepository(session)
            snapshot_repo = ProductSnapshotRepository(session)

            scrape_run = scrape_run_repo.create(
                ScrapeRunCreate(
                    source_name=source_name,
                    started_at=datetime.now(timezone.utc),
                )
            )
            # Commit the run record first so downstream failures can still be linked to it.
            session.commit()

            try:
                scraper = get_scraper(source_name, source_settings)
                products = scraper.scrape(limit=limit)
                scraped_at = datetime.now(timezone.utc)

                snapshot_records = snapshot_repo.build_snapshot_records(
                    scrape_run_id=scrape_run.id,
                    source_name=source_name,
                    products=products,
                    scraped_at=scraped_at,
                )
                inserted_count = snapshot_repo.bulk_create(snapshot_records)

                scrape_run_repo.update(
                    scrape_run.id,
                    ScrapeRunUpdate(
                        status="succeeded",
                        finished_at=datetime.now(timezone.utc),
                        records_fetched=len(products),
                        records_inserted=inserted_count,
                    ),
                )
                session.commit()

                logger.info(
                    "Scrape completed for source=%s fetched=%s inserted=%s",
                    source_name,
                    len(products),
                    inserted_count,
                )
                print(
                    f"Scrape completed for {source_name}: "
                    f"fetched={len(products)} inserted={inserted_count}"
                )
                return 0
            except Exception as exc:
                session.rollback()
                scrape_run_repo.update(
                    scrape_run.id,
                    ScrapeRunUpdate(
                        status="failed",
                        finished_at=datetime.now(timezone.utc),
                        error_message=str(exc),
                    ),
                )
                session.commit()
                logger.exception("Scrape failed for source=%s", source_name)
                raise
    except OperationalError as exc:
        raise SystemExit(_format_db_operational_error(exc, settings.database_url)) from exc


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch CLI subcommands."""

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init-db":
        settings = load_settings(args.config)
        configure_logging(settings.log_level, settings.log_file)
        return handle_init_db(settings.database_url)

    if args.command == "show-config":
        return handle_show_config(args.config)

    if args.command == "scrape":
        return handle_scrape(args.config, args.source, args.limit)

    parser.error(f"Unsupported command: {args.command}")
    return 2


def cli() -> None:
    """Console script entry point."""

    raise SystemExit(main())


if __name__ == "__main__":
    cli()
