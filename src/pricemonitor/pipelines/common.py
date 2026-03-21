from __future__ import annotations

"""Shared helpers for pipeline-style CLI flows."""

from sqlalchemy.exc import OperationalError

from pricemonitor.config import AppSettings


def format_db_operational_error(exc: OperationalError, database_url: str) -> str:
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


def resolve_target_sources(settings: AppSettings, source_name: str) -> list[str]:
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
