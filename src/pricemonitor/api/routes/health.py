from __future__ import annotations

"""Health and service metadata endpoints."""

from fastapi import APIRouter, Depends

from pricemonitor import __version__
from pricemonitor.api.deps import get_settings
from pricemonitor.api.schemas import HealthResponse
from pricemonitor.config import AppSettings

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def get_health(settings: AppSettings = Depends(get_settings)) -> HealthResponse:
    """Return lightweight service health and configuration metadata."""

    return HealthResponse(
        status="ok",
        app_name=settings.app_name,
        environment=settings.environment,
        version=__version__,
        configured_sources=len(settings.sources),
    )
