from __future__ import annotations

"""Price-change detection between the current run and the previous successful run."""

import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Sequence

from pricemonitor.models.db_models import ProductSnapshot
from pricemonitor.models.schemas import PriceChangeEventCreate

logger = logging.getLogger(__name__)

_MONEY_QUANTIZE = Decimal("0.01")
_PERCENT_QUANTIZE = Decimal("0.01")
_HUNDRED = Decimal("100")


def detect_price_changes(
    *,
    source_name: str,
    scrape_run_id: int,
    current_snapshots: Sequence[ProductSnapshot],
    previous_snapshots: Sequence[ProductSnapshot],
    changed_at: datetime | None = None,
) -> list[PriceChangeEventCreate]:
    """Compare two runs and create change events only for products whose price actually moved."""

    if not previous_snapshots:
        logger.info("No previous successful run found for source=%s; skipping change detection.", source_name)
        return []

    previous_by_external_id = {snapshot.external_id: snapshot for snapshot in previous_snapshots}
    effective_changed_at = changed_at or datetime.now(timezone.utc)

    events: list[PriceChangeEventCreate] = []
    for current_snapshot in current_snapshots:
        previous_snapshot = previous_by_external_id.get(current_snapshot.external_id)
        if previous_snapshot is None:
            # New products are not price changes yet; they have no baseline.
            continue

        if previous_snapshot.currency != current_snapshot.currency:
            logger.warning(
                "Skipping change detection for source=%s external_id=%s due to currency mismatch (%s -> %s).",
                source_name,
                current_snapshot.external_id,
                previous_snapshot.currency,
                current_snapshot.currency,
            )
            continue

        previous_price = _effective_price(previous_snapshot)
        current_price = _effective_price(current_snapshot)
        if previous_price == current_price:
            continue

        absolute_difference = abs(current_price - previous_price).quantize(
            _MONEY_QUANTIZE,
            rounding=ROUND_HALF_UP,
        )
        percentage_difference = _compute_percentage_difference(
            previous_price=previous_price,
            absolute_difference=absolute_difference,
        )

        events.append(
            PriceChangeEventCreate(
                source_name=source_name,
                external_id=current_snapshot.external_id,
                scrape_run_id=scrape_run_id,
                previous_snapshot_id=previous_snapshot.id,
                current_snapshot_id=current_snapshot.id,
                product_name=current_snapshot.product_name,
                currency=current_snapshot.currency,
                previous_price=previous_price,
                current_price=current_price,
                absolute_difference=absolute_difference,
                percentage_difference=percentage_difference,
                changed_at=effective_changed_at,
            )
        )

    logger.info(
        "Detected %s price change events for source=%s.",
        len(events),
        source_name,
    )
    return events


def _effective_price(snapshot: ProductSnapshot) -> Decimal:
    """Treat sale price as the monitored price when it exists, otherwise fall back to listed price."""

    if snapshot.sale_price is not None:
        return snapshot.sale_price
    return snapshot.listed_price


def _compute_percentage_difference(
    *,
    previous_price: Decimal,
    absolute_difference: Decimal,
) -> Decimal | None:
    """Return a positive percentage movement relative to the previous price."""

    if previous_price == 0:
        return None

    return ((absolute_difference / previous_price) * _HUNDRED).quantize(
        _PERCENT_QUANTIZE,
        rounding=ROUND_HALF_UP,
    )
