from __future__ import annotations

"""FastAPI application factory for the read-only monitoring API."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from pricemonitor import __version__
from pricemonitor.api.routes.alerts import router as alerts_router
from pricemonitor.api.routes.health import router as health_router
from pricemonitor.api.routes.price_changes import router as price_changes_router
from pricemonitor.api.routes.products import router as products_router
from pricemonitor.api.routes.runs import router as runs_router
from pricemonitor.api.routes.sources import router as sources_router
from pricemonitor.config import load_settings
from pricemonitor.storage.database import create_engine_from_url, create_session_factory

_DEFAULT_CONFIG_ENV_VAR = "PRICEMONITOR_CONFIG"


def _resolve_config_path(config_path: str | Path | None = None) -> Path:
    """Resolve the config path from an explicit argument or environment variable."""

    if config_path is not None:
        return Path(config_path).resolve()

    return Path(
        os.getenv(_DEFAULT_CONFIG_ENV_VAR, "configs/settings.yaml")
    ).resolve()


def create_app(config_path: str | Path | None = None) -> FastAPI:
    """Create the FastAPI app using the same configuration source as the CLI."""

    resolved_config_path = _resolve_config_path(config_path)
    settings = load_settings(resolved_config_path)
    engine = create_engine_from_url(settings.database_url)
    session_factory = create_session_factory(engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.engine = engine
        app.state.session_factory = session_factory
        yield
        engine.dispose()

    app = FastAPI(
        title=f"{settings.app_name} API",
        version=__version__,
        description="Read-only API for querying latest monitoring outputs.",
        lifespan=lifespan,
    )

    app.include_router(health_router)
    app.include_router(sources_router)
    app.include_router(runs_router)
    app.include_router(products_router)
    app.include_router(price_changes_router)
    app.include_router(alerts_router)

    return app
