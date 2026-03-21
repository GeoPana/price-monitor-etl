from __future__ import annotations

"""FastAPI dependency helpers."""

from collections.abc import Iterator

from fastapi import Depends, Request
from sqlalchemy.orm import Session

from pricemonitor.api.read_service import ReadApiService
from pricemonitor.config import AppSettings
from pricemonitor.storage.repositories import ScrapeRunRepository


def get_settings(request: Request) -> AppSettings:
    """Expose app settings stored on FastAPI state."""

    return request.app.state.settings


def get_session(request: Request) -> Iterator[Session]:
    """Yield a SQLAlchemy session from the app-scoped session factory."""

    session_factory = request.app.state.session_factory
    with session_factory() as session:
        yield session


def get_read_api_service(
    settings: AppSettings = Depends(get_settings),
    session: Session = Depends(get_session),
) -> ReadApiService:
    """Build the read service used by API routes."""

    return ReadApiService(
        settings=settings,
        scrape_run_repo=ScrapeRunRepository(session),
    )
