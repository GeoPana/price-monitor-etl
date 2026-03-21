from __future__ import annotations

"""Alert-report endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from pricemonitor.api.deps import get_read_api_service
from pricemonitor.api.read_service import ReadApiService
from pricemonitor.api.schemas import (
    AlertPriceChangeRowResponse,
    AlertSummaryResponse,
    NewProductAlertResponse,
)

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("/summary", response_model=AlertSummaryResponse)
def get_alert_summary(
    source: str = Query(..., description="Source name, e.g. site_a"),
    service: ReadApiService = Depends(get_read_api_service),
) -> AlertSummaryResponse:
    """Return the alert summary JSON for one source."""

    try:
        payload = service.get_alert_summary(source_name=source)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return AlertSummaryResponse.model_validate(payload)


@router.get("/top-price-changes", response_model=list[AlertPriceChangeRowResponse])
def list_top_price_changes(
    source: str = Query(..., description="Source name, e.g. site_a"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    service: ReadApiService = Depends(get_read_api_service),
) -> list[AlertPriceChangeRowResponse]:
    """Return ranked top-price-change alert rows for one source."""

    try:
        rows = service.list_alert_top_price_changes(
            source_name=source,
            limit=limit,
            offset=offset,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return [AlertPriceChangeRowResponse.model_validate(row) for row in rows]


@router.get("/new-products", response_model=list[NewProductAlertResponse])
def list_new_products(
    source: str = Query(..., description="Source name, e.g. site_a"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    service: ReadApiService = Depends(get_read_api_service),
) -> list[NewProductAlertResponse]:
    """Return newly seen products for one source from the generated alert CSV."""

    try:
        rows = service.list_alert_new_products(
            source_name=source,
            limit=limit,
            offset=offset,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return [NewProductAlertResponse.model_validate(row) for row in rows]
