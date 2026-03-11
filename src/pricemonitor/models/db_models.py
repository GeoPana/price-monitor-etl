from __future__ import annotations

"""SQLAlchemy ORM models for scrape runs and product snapshots."""

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, JSON, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pricemonitor.storage.database import Base


class ScrapeRun(Base):
    """Tracks the lifecycle and outcome of a single scrape execution."""

    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_name: Mapped[str] = mapped_column(String(100), index=True)
    status: Mapped[str] = mapped_column(String(20), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    records_fetched: Mapped[int] = mapped_column(default=0)
    records_inserted: Mapped[int] = mapped_column(default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    product_snapshots: Mapped[list["ProductSnapshot"]] = relationship(
        back_populates="scrape_run",
        cascade="all, delete-orphan",
    )


class ProductSnapshot(Base):
    """Stores a point-in-time view of a product observed during a scrape."""

    __tablename__ = "product_snapshots"
    __table_args__ = (
        # This supports the common lookup pattern of latest snapshots per product and source.
        Index("ix_product_snapshot_lookup", "source_name", "external_id", "scraped_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    scrape_run_id: Mapped[int] = mapped_column(ForeignKey("scrape_runs.id"), index=True)
    source_name: Mapped[str] = mapped_column(String(100), index=True)
    external_id: Mapped[str] = mapped_column(String(255), index=True)
    product_name: Mapped[str] = mapped_column(String(255))
    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category: Mapped[str | None] = mapped_column(String(255), nullable=True)
    product_url: Mapped[str] = mapped_column(Text)
    currency: Mapped[str] = mapped_column(String(3))
    listed_price: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    sale_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    availability: Mapped[str] = mapped_column(String(50), default="unknown")
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    scrape_run: Mapped[ScrapeRun] = relationship(back_populates="product_snapshots")
