from __future__ import annotations

"""Repository helpers that isolate SQLAlchemy persistence details."""

from sqlalchemy import Select, desc, select
from sqlalchemy.orm import Session

from pricemonitor.models.db_models import ProductSnapshot, ScrapeRun
from pricemonitor.models.schemas import ProductRecord, ProductSnapshotCreate, ScrapeRunCreate, ScrapeRunUpdate


class ScrapeRunRepository:
    """Persistence operations for scrape run lifecycle records."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def create(self, payload: ScrapeRunCreate) -> ScrapeRun:
        scrape_run = ScrapeRun(
            source_name=payload.source_name,
            status=payload.status,
            started_at=payload.started_at,
        )
        self.session.add(scrape_run)
        self.session.flush()
        return scrape_run

    def update(self, scrape_run_id: int, payload: ScrapeRunUpdate) -> ScrapeRun:
        scrape_run = self.session.get(ScrapeRun, scrape_run_id)
        if scrape_run is None:
            raise ValueError(f"Scrape run not found: {scrape_run_id}")

        scrape_run.status = payload.status
        scrape_run.finished_at = payload.finished_at
        scrape_run.records_fetched = payload.records_fetched
        scrape_run.records_inserted = payload.records_inserted
        scrape_run.error_message = payload.error_message
        self.session.flush()
        return scrape_run


class ProductSnapshotRepository:
    """Persistence operations for scraped product snapshots."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def bulk_create(self, records: list[ProductSnapshotCreate]) -> int:
        """Insert a batch of snapshots and retain the full record payload for traceability."""

        snapshots = [
            ProductSnapshot(
                scrape_run_id=record.scrape_run_id,
                source_name=record.source_name,
                external_id=record.external_id,
                product_name=record.product_name,
                brand=record.brand,
                category=record.category,
                product_url=record.product_url,
                currency=record.currency,
                listed_price=record.listed_price,
                sale_price=record.sale_price,
                availability=record.availability,
                payload=record.model_dump(mode="json"),
                scraped_at=record.scraped_at,
            )
            for record in records
        ]
        self.session.add_all(snapshots)
        self.session.flush()
        return len(snapshots)

    def list_latest_for_source(self, source_name: str) -> list[ProductSnapshot]:
        """Return snapshots for a source ordered from newest to oldest."""

        statement: Select[tuple[ProductSnapshot]] = (
            select(ProductSnapshot)
            .where(ProductSnapshot.source_name == source_name)
            .order_by(desc(ProductSnapshot.scraped_at))
        )
        return list(self.session.scalars(statement))

    def build_snapshot_records(
        self,
        scrape_run_id: int,
        source_name: str,
        products: list[ProductRecord],
        scraped_at,
    ) -> list[ProductSnapshotCreate]:
        """Project validated product records into snapshot create payloads."""

        return [
            ProductSnapshotCreate(
                scrape_run_id=scrape_run_id,
                source_name=source_name,
                scraped_at=scraped_at,
                **product.model_dump(),
            )
            for product in products
        ]
