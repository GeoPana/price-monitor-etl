from __future__ import annotations

"""Configuration loading for the CLI and scraper runtime."""

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class SourceSettings(BaseModel):
    """Settings for a single configured source."""

    name: str
    enabled: bool = True
    base_url: str
    scraper: str
    fetcher: str = "http"
    timeout_seconds: int = 10
    sample_products: list[dict[str, Any]] = Field(default_factory=list)


class AppSettings(BaseModel):
    """Resolved application settings after YAML and env overrides are applied."""

    app_name: str
    environment: str
    log_level: str
    database_url: str
    log_file: Path
    raw_dir: Path
    processed_dir: Path
    exports_dir: Path
    logs_dir: Path
    sources: dict[str, SourceSettings]


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML file and require a mapping at the top level."""

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in YAML file: {path}")

    return data


def _resolve_path(root_dir: Path, value: str) -> Path:
    """Resolve config-relative filesystem paths against the repository root."""

    path = Path(value)
    if path.is_absolute():
        return path
    return (root_dir / path).resolve()


def load_settings(config_path: str | Path = "configs/settings.yaml") -> AppSettings:
    """Load application settings from YAML, source files, and optional env overrides."""

    config_path = Path(config_path).resolve()
    root_dir = config_path.parent.parent.resolve()

    env_file = root_dir / ".env"
    if env_file.exists():
        # Prefer the repo-local .env so CLI runs stay reproducible across shells.
        load_dotenv(env_file)

    base_config = _read_yaml(config_path)
    sources_dir = config_path.parent / "sources"

    sources: dict[str, SourceSettings] = {}
    if sources_dir.exists():
        # One file per source keeps source-specific configuration isolated.
        for source_file in sorted(sources_dir.glob("*.yaml")):
            source_settings = SourceSettings.model_validate(_read_yaml(source_file))
            sources[source_settings.name] = source_settings

    app_config = base_config.get("app", {})
    db_config = base_config.get("database", {})
    logging_config = base_config.get("logging", {})
    directories_config = base_config.get("directories", {})

    settings = AppSettings(
        app_name=app_config.get("name", "Price Monitor ETL"),
        environment=os.getenv("APP_ENV", app_config.get("environment", "development")),
        log_level=os.getenv("LOG_LEVEL", logging_config.get("level", "INFO")),
        database_url=os.getenv(
            "DATABASE_URL",
            db_config.get(
                "url",
                "postgresql+psycopg://postgres:postgres@localhost:5433/price_monitor",
            ),
        ),
        log_file=_resolve_path(root_dir, logging_config.get("file", "logs/pricemonitor.log")),
        raw_dir=_resolve_path(root_dir, directories_config.get("raw", "data/raw")),
        processed_dir=_resolve_path(root_dir, directories_config.get("processed", "data/processed")),
        exports_dir=_resolve_path(root_dir, directories_config.get("exports", "data/exports")),
        logs_dir=_resolve_path(root_dir, directories_config.get("logs", "logs")),
        sources=sources,
    )
    return settings
