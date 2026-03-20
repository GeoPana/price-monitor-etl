from __future__ import annotations

"""Programmatic Alembic helpers used by the CLI."""

from pathlib import Path

from alembic import command
from alembic.config import Config

DEFAULT_APP_CONFIG = Path("configs/settings.yaml")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _build_alembic_config(config_path: str | Path = DEFAULT_APP_CONFIG) -> Config:
    """Create an Alembic Config that points at this repo's migration environment."""

    project_root = _project_root()
    cfg = Config(str(project_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(project_root / "alembic"))
    cfg.attributes["app_config_path"] = str(Path(config_path).resolve())
    return cfg


def upgrade_to_head(config_path: str | Path = DEFAULT_APP_CONFIG) -> None:
    """Apply all pending migrations."""

    command.upgrade(_build_alembic_config(config_path), "head")


def stamp_head(config_path: str | Path = DEFAULT_APP_CONFIG) -> None:
    """Mark the current database as matching the latest revision without running DDL."""

    command.stamp(_build_alembic_config(config_path), "head")
