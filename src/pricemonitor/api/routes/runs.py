from __future__ import annotations

"""Recent run-history endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from pricemonitor.api.deps import get_read_api_service
from pricemonitor.api.read_service import ReadApiService
from pricemonitor.api.schemas import RunResponse

router = APIRouter(tags=["runs"])


@router.get("/runs", response_model=list[RunResponse])
def list_runs(
    source: str | None = Query(default=None, description="Optional source filter, e.g. site_a"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    service: ReadApiService = Depends(get_read_api_service),
) -> list[RunResponse]:
    """Return recent scrape/pipeline runs, optionally filtered by source."""

    try:
        rows = service.list_runs(
            source_name=source,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return [RunResponse.model_validate(row) for row in rows]
