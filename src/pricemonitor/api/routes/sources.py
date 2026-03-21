from __future__ import annotations

"""Configured source metadata endpoints."""

from fastapi import APIRouter, Depends

from pricemonitor.api.deps import get_read_api_service
from pricemonitor.api.read_service import ReadApiService
from pricemonitor.api.schemas import SourceResponse

router = APIRouter(tags=["sources"])


@router.get("/sources", response_model=list[SourceResponse])
def list_sources(service: ReadApiService = Depends(get_read_api_service)) -> list[SourceResponse]:
    """Return configured sources so clients know what can be queried."""

    return [SourceResponse.model_validate(row) for row in service.list_sources()]
