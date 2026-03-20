"""Baseline schema captured at the end of Sprint 7.

This is the first Alembic-managed revision for the project.

For a database that was already created before Alembic was introduced and
already matches the Sprint 7 schema, do not run this revision against the
existing tables. Instead, mark it with:

    alembic -x app_config=configs/settings.yaml stamp head
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "20260320_0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scrape_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_name", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("records_fetched", sa.Integer(), nullable=False),
        sa.Column("records_inserted", sa.Integer(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_scrape_runs_source_name", "scrape_runs", ["source_name"], unique=False)
    op.create_index("ix_scrape_runs_status", "scrape_runs", ["status"], unique=False)

    op.create_table(
        "product_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scrape_run_id", sa.Integer(), nullable=False),
        sa.Column("source_name", sa.String(length=100), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("product_name", sa.String(length=255), nullable=False),
        sa.Column("brand", sa.String(length=255), nullable=True),
        sa.Column("category", sa.String(length=255), nullable=True),
        sa.Column("product_url", sa.Text(), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("listed_price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("sale_price", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("availability", sa.String(length=50), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["scrape_run_id"], ["scrape_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_product_snapshot_lookup", "product_snapshots", ["source_name", "external_id", "scraped_at"], unique=False)
    op.create_index("ix_product_snapshots_external_id", "product_snapshots", ["external_id"], unique=False)
    op.create_index("ix_product_snapshots_scrape_run_id", "product_snapshots", ["scrape_run_id"], unique=False)
    op.create_index("ix_product_snapshots_scraped_at", "product_snapshots", ["scraped_at"], unique=False)
    op.create_index("ix_product_snapshots_source_name", "product_snapshots", ["source_name"], unique=False)

    op.create_table(
        "price_change_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scrape_run_id", sa.Integer(), nullable=False),
        sa.Column("source_name", sa.String(length=100), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("product_name", sa.String(length=255), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("previous_snapshot_id", sa.Integer(), nullable=False),
        sa.Column("current_snapshot_id", sa.Integer(), nullable=False),
        sa.Column("previous_price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("current_price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("absolute_difference", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("percentage_difference", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(["current_snapshot_id"], ["product_snapshots.id"]),
        sa.ForeignKeyConstraint(["previous_snapshot_id"], ["product_snapshots.id"]),
        sa.ForeignKeyConstraint(["scrape_run_id"], ["scrape_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_price_change_event_lookup", "price_change_events", ["source_name", "external_id", "changed_at"], unique=False)
    op.create_index("ix_price_change_events_changed_at", "price_change_events", ["changed_at"], unique=False)
    op.create_index("ix_price_change_events_current_snapshot_id", "price_change_events", ["current_snapshot_id"], unique=False)
    op.create_index("ix_price_change_events_external_id", "price_change_events", ["external_id"], unique=False)
    op.create_index("ix_price_change_events_previous_snapshot_id", "price_change_events", ["previous_snapshot_id"], unique=False)
    op.create_index("ix_price_change_events_scrape_run_id", "price_change_events", ["scrape_run_id"], unique=False)
    op.create_index("ix_price_change_events_source_name", "price_change_events", ["source_name"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_price_change_events_source_name", table_name="price_change_events")
    op.drop_index("ix_price_change_events_scrape_run_id", table_name="price_change_events")
    op.drop_index("ix_price_change_events_previous_snapshot_id", table_name="price_change_events")
    op.drop_index("ix_price_change_events_external_id", table_name="price_change_events")
    op.drop_index("ix_price_change_events_current_snapshot_id", table_name="price_change_events")
    op.drop_index("ix_price_change_events_changed_at", table_name="price_change_events")
    op.drop_index("ix_price_change_event_lookup", table_name="price_change_events")
    op.drop_table("price_change_events")

    op.drop_index("ix_product_snapshots_source_name", table_name="product_snapshots")
    op.drop_index("ix_product_snapshots_scraped_at", table_name="product_snapshots")
    op.drop_index("ix_product_snapshots_scrape_run_id", table_name="product_snapshots")
    op.drop_index("ix_product_snapshots_external_id", table_name="product_snapshots")
    op.drop_index("ix_product_snapshot_lookup", table_name="product_snapshots")
    op.drop_table("product_snapshots")

    op.drop_index("ix_scrape_runs_status", table_name="scrape_runs")
    op.drop_index("ix_scrape_runs_source_name", table_name="scrape_runs")
    op.drop_table("scrape_runs")
