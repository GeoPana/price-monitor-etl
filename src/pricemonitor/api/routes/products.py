from __future__ import annotations

"""Latest product endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from pricemonitor.api.deps import get_read_api_service
from pricemonitor.api.read_service import ReadApiService
from pricemonitor.api.schemas import LatestProductResponse

router = APIRouter(tags=["products"])


@router.get("/products/latest", response_model=list[LatestProductResponse])
def list_latest_products(
    source: str = Query(..., description="Source name, e.g. site_a"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    service: ReadApiService = Depends(get_read_api_service),
) -> list[LatestProductResponse]:
    """Return the latest exported product view for one source."""

    try:
        rows = service.list_latest_products(
            source_name=source,
            limit=limit,
            offset=offset,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return [LatestProductResponse.model_validate(row) for row in rows]
