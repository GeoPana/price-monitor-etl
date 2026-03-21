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
from pricemonitor.pipelines.common import format_db_operational_error, resolve_target_sources
from pricemonitor.pipelines.report_run import run_report_for_source, run_report_pipeline
from pricemonitor.pipelines.scrape_run import run_scrape_for_source, run_scrape_pipeline
from pricemonitor.storage.migrations import upgrade_to_head

logger = logging.getLogger(__name__)
    

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

    run_parser = subparsers.add_parser(
        "run",
        help="Run the full end-to-end pipeline: scrape first, then export reports.",
    )
    run_parser.add_argument("--source", required=True, help="Source name, e.g. site_a, or all")
    run_parser.add_argument("--limit", type=int, default=None, help="Optional scrape record limit")
    run_parser.add_argument(
        "--report-limit",
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
        raise SystemExit(format_db_operational_error(exc, settings.database_url)) from exc
    print("Database schema is up to date.")
    return 0


def handle_show_config(config_path: str) -> int:
    """Print the resolved configuration for inspection and debugging."""

    settings = load_settings(config_path)
    print(json.dumps(settings.model_dump(mode="json"), indent=2))
    return 0


def handle_scrape(config_path: str, source_name: str, limit: int | None) -> int:
    """Run a scrape for a configured source or all enabled sources and persist the results."""

    return run_scrape_pipeline(config_path, source_name, limit)


def handle_export(config_path: str, source_name: str, limit: int) -> int:
    """Dispatch to the reporting/export pipeline module."""

    return run_report_pipeline(config_path, source_name, limit)


def handle_run(
    config_path: str,
    source_name: str,
    limit: int | None,
    report_limit: int,
) -> int:
    """Run the end-to-end pipeline for one source or all enabled sources."""

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
            run_report_for_source(
                settings=settings,
                source_name=resolved_source_name,
                limit=report_limit,
            )
            completed_sources.append(resolved_source_name)
        except Exception:
            failed_sources.append(resolved_source_name)
            if source_name != "all":
                raise
            logger.exception(
                "End-to-end pipeline failed for source=%s during all-source orchestration",
                resolved_source_name,
            )

    if source_name == "all":
        if failed_sources:
            print(
                "Pipeline runs completed for successful sources: "
                f"{', '.join(completed_sources)}; failed: {', '.join(failed_sources)}"
            )
        else:
            print(f"All enabled pipeline runs completed successfully: {', '.join(completed_sources)}")
    elif not failed_sources:
        print(f"Pipeline run completed for {source_name}.")

    return 0 if not failed_sources else 1


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

    if args.command == "run":
        return handle_run(args.config, args.source, args.limit, args.report_limit)
    
    parser.error(f"Unsupported command: {args.command}")
    return 2


def cli() -> None:
    """Console script entry point."""

    raise SystemExit(main())


if __name__ == "__main__":
    cli()
