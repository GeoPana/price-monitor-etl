from __future__ import annotations

"""SQLAlchemy engine, session, and metadata helpers."""

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


def create_engine_from_url(database_url: str, echo: bool = False) -> Engine:
    """Create an engine with backend-specific connection options."""

    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        # SQLite needs this flag for the same-thread checks used in tests and CLI flows.
        connect_args["check_same_thread"] = False

    return create_engine(
        database_url,
        echo=echo,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Build the session factory used by repository code."""

    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)


def init_db(engine: Engine) -> None:
    """Import models and create all mapped tables."""

    from pricemonitor.models import db_models  # noqa: F401

    Base.metadata.create_all(engine)
