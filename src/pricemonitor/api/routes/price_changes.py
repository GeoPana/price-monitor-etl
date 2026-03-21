from __future__ import annotations

"""Price-change endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from pricemonitor.api.deps import get_read_api_service
from pricemonitor.api.read_service import ReadApiService
from pricemonitor.api.schemas import PriceChangeResponse

router = APIRouter(tags=["price_changes"])


@router.get("/price-changes", response_model=list[PriceChangeResponse])
def list_price_changes(
    source: str = Query(..., description="Source name, e.g. site_a"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    service: ReadApiService = Depends(get_read_api_service),
) -> list[PriceChangeResponse]:
    """Return recent exported price-change rows for one source."""

    try:
        rows = service.list_price_changes(
            source_name=source,
            limit=limit,
            offset=offset,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return [PriceChangeResponse.model_validate(row) for row in rows]
