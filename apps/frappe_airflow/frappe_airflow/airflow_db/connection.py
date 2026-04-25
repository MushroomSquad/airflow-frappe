"""SQLAlchemy engine connecting to Airflow's PostgreSQL metadata database.

Reads AIRFLOW_DB_URL from environment.
Example: postgresql+psycopg2://airflow_ui:pass@airflow-postgres:5432/airflow
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

_engine = None
_SessionFactory = None


def _get_engine():
    global _engine, _SessionFactory
    if _engine is None:
        url = os.environ.get("AIRFLOW_DB_URL", "")
        if not url:
            raise RuntimeError("AIRFLOW_DB_URL environment variable is not set")
        _engine = create_engine(url, pool_pre_ping=True)
        _SessionFactory = sessionmaker(bind=_engine)
    return _engine


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session, commit on success, rollback on error."""
    engine = _get_engine()
    factory = sessionmaker(bind=engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_connection() -> bool:
    """Return True if Airflow DB is reachable."""
    try:
        with get_session() as s:
            s.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
