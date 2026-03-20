from __future__ import annotations

"""Alembic environment configuration for the project."""

from logging.config import fileConfig
from pathlib import Path

from alembic import context

import pricemonitor.models.db_models  # noqa: F401
from pricemonitor.config import load_settings
from pricemonitor.storage.database import Base, create_engine_from_url

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _get_app_config_path() -> Path:
    """Resolve the app config path from Alembic x-args or programmatic attributes."""

    x_args = context.get_x_argument(as_dictionary=True)
    if "app_config" in x_args:
        return Path(x_args["app_config"]).resolve()

    app_config_path = config.attributes.get("app_config_path")
    if app_config_path:
        return Path(str(app_config_path)).resolve()

    return (Path(__file__).resolve().parents[1] / "configs" / "settings.yaml").resolve()


def _get_database_url() -> str:
    """Load the same database URL used by the application itself."""

    settings = load_settings(_get_app_config_path())
    return settings.database_url


def run_migrations_offline() -> None:
    """Run migrations without creating an Engine."""

    url = _get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        render_as_batch=url.startswith("sqlite"),
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations using the application's engine settings."""

    connectable = create_engine_from_url(_get_database_url())

    try:
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
                render_as_batch=connection.dialect.name == "sqlite",
            )

            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
