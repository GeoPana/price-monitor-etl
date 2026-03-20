from __future__ import annotations

"""Repository helpers that isolate database writes, lifecycle updates, raw archives, and change events."""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from sqlalchemy import Select, desc, select
from sqlalchemy.orm import Session

from pricemonitor.models.db_models import ProductSnapshot, ScrapeRun, PriceChangeEvent
from pricemonitor.models.schemas import ArchivedPageRecord, ProductRecord, PriceChangeEventCreate


class ScrapeRunRepository:
    """Persistence operations for scrape run lifecycle records."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def create_scrape_run(
        self,
        source_name: str,
        *,
        started_at: datetime | None = None,
    ) -> ScrapeRun:
        """Create and flush a new running scrape record."""

        scrape_run = ScrapeRun(
            source_name=source_name,
            status="running",
            started_at=started_at or datetime.now(timezone.utc),
        )
        self.session.add(scrape_run)
        self.session.flush()
        return scrape_run

    def complete_scrape_run(
        self,
        scrape_run_id: int,
        *,
        records_fetched: int,
        records_inserted: int,
        finished_at: datetime | None = None,
    ) -> ScrapeRun:
        """Mark a scrape as successful and store its final counters."""

        scrape_run = self._require_run(scrape_run_id)
        scrape_run.status = "succeeded"
        scrape_run.finished_at = finished_at or datetime.now(timezone.utc)
        scrape_run.records_fetched = records_fetched
        scrape_run.records_inserted = records_inserted
        scrape_run.error_message = None
        self.session.flush()
        return scrape_run

    def fail_scrape_run(
        self,
        scrape_run_id: int,
        *,
        error_message: str,
        finished_at: datetime | None = None,
    ) -> ScrapeRun:
        """Mark a scrape as failed while preserving the original run record."""

        scrape_run = self._require_run(scrape_run_id)
        scrape_run.status = "failed"
        scrape_run.finished_at = finished_at or datetime.now(timezone.utc)
        scrape_run.error_message = error_message
        self.session.flush()
        return scrape_run   

    def get_by_id(self, scrape_run_id: int) -> ScrapeRun:
        """Fetch a scrape run or raise if it does not exist."""

        return self._require_run(scrape_run_id)
    
    def list_recent(self, *, source_name: str | None = None, limit: int = 10) -> list[ScrapeRun]:
        """Return recent scrape runs, optionally filtered by source."""

        statement: Select[tuple[ScrapeRun]] = select(ScrapeRun).order_by(desc(ScrapeRun.id))
        if source_name is not None:
            statement = statement.where(ScrapeRun.source_name == source_name)
        statement = statement.limit(limit)
        return list(self.session.scalars(statement))

    def get_previous_successful_run(
        self,
        *,
        source_name: str,
        before_scrape_run_id: int,
    ) -> ScrapeRun | None:
        """Return the latest successful run before the current one for the same source."""

        statement: Select[tuple[ScrapeRun]] = (
            select(ScrapeRun)
            .where(
                ScrapeRun.source_name == source_name,
                ScrapeRun.status == "succeeded",
                ScrapeRun.id < before_scrape_run_id,
            )
            .order_by(desc(ScrapeRun.id))
            .limit(1)
        )
        return self.session.scalar(statement)

    def _require_run(self, scrape_run_id: int) -> ScrapeRun:
        scrape_run = self.session.get(ScrapeRun, scrape_run_id)
        if scrape_run is None:
            raise ValueError(f"Scrape run not found: {scrape_run_id}")
        return scrape_run


class ProductSnapshotRepository:
    """Persistence operations for scraped product snapshots."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def insert_product_snapshots(
        self,
        *,
        scrape_run_id: int,
        source_name: str,
        products: list[ProductRecord],
        scraped_at: datetime,
    ) -> int:
        """Insert a batch of snapshots and retain the full record payload for traceability."""

        snapshots = [
            ProductSnapshot(
                scrape_run_id=scrape_run_id,
                source_name=source_name,
                external_id=product.external_id,
                product_name=product.product_name,
                brand=product.brand,
                category=product.category,
                product_url=product.product_url,
                currency=product.currency,
                listed_price=product.listed_price,
                sale_price=product.sale_price,
                availability=product.availability,
                payload=product.model_dump(mode="json"),
                scraped_at=scraped_at,
            )
            for product in products
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

    def list_latest_for_product(
        self,
        *,
        source_name: str,
        external_id: str,
        limit: int = 10,
    ) -> list[ProductSnapshot]:
        """Return the latest snapshots for a specific product within a source."""

        statement: Select[tuple[ProductSnapshot]] = (
            select(ProductSnapshot)
            .where(
                ProductSnapshot.source_name == source_name,
                ProductSnapshot.external_id == external_id,
            )
            .order_by(desc(ProductSnapshot.scraped_at))
            .limit(limit)
        )
        return list(self.session.scalars(statement))
    
    def list_for_scrape_run(self, scrape_run_id: int) -> list[ProductSnapshot]:
        """Return all snapshots inserted for a specific scrape run."""

        statement: Select[tuple[ProductSnapshot]] = (
            select(ProductSnapshot)
            .where(ProductSnapshot.scrape_run_id == scrape_run_id)
            .order_by(ProductSnapshot.id)
        )
        return list(self.session.scalars(statement))


class PriceChangeEventRepository:
    """Persistence operations for detected price-change events."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def insert_price_change_events(self, events: list[PriceChangeEventCreate]) -> int:
        """Insert the detected change events for a completed run."""

        if not events:
            return 0

        rows = [
            PriceChangeEvent(
                scrape_run_id=event.scrape_run_id,
                source_name=event.source_name,
                external_id=event.external_id,
                product_name=event.product_name,
                currency=event.currency,
                previous_snapshot_id=event.previous_snapshot_id,
                current_snapshot_id=event.current_snapshot_id,
                previous_price=event.previous_price,
                current_price=event.current_price,
                absolute_difference=event.absolute_difference,
                percentage_difference=event.percentage_difference,
                changed_at=event.changed_at,
            )
            for event in events
        ]
        self.session.add_all(rows)
        self.session.flush()
        return len(rows)

    def list_latest_for_source(self, source_name: str) -> list[PriceChangeEvent]:
        """Return recent change events for a source ordered from newest to oldest."""

        statement: Select[tuple[PriceChangeEvent]] = (
            select(PriceChangeEvent)
            .where(PriceChangeEvent.source_name == source_name)
            .order_by(desc(PriceChangeEvent.changed_at), desc(PriceChangeEvent.id))
        )
        return list(self.session.scalars(statement))


class RawPageArchiveRepository:
    """Filesystem archive support for raw fetched pages."""

    def __init__(self, raw_dir: Path) -> None:
        self.raw_dir = Path(raw_dir)

    def archive_pages(
        self,
        *,
        source_name: str,
        scrape_run_id: int,
        pages: list[ArchivedPageRecord],
    ) -> list[Path]:
        """Write raw HTML pages plus a manifest under the run-specific raw directory."""

        if not pages:
            return []

        run_dir = self.raw_dir / source_name / f"run_{scrape_run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

        written_paths: list[Path] = []
        manifest: list[dict[str, str | int]] = []

        for index, page in enumerate(pages, start=1):
            file_stem = self._build_file_stem(index=index, page=page)
            file_path = run_dir / f"{file_stem}.html"
            file_path.write_text(page.content, encoding="utf-8")

            written_paths.append(file_path)
            manifest.append(
                {
                    "index": index,
                    "page_type": page.page_type,
                    "page_url": page.page_url,
                    "file_name": file_path.name,
                }
            )

        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        return written_paths

    def _build_file_stem(self, *, index: int, page: ArchivedPageRecord) -> str:
        """Create stable, readable archive file names from page URLs."""

        parsed = urlsplit(page.page_url)
        slug_source = parsed.path.strip("/") or parsed.netloc or "page"
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", slug_source).strip("_").lower() or "page"
        return f"{index:03d}_{page.page_type}_{slug[:80]}"