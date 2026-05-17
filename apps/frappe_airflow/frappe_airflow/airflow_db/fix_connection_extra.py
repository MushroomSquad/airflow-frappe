"""One-off repair: empty connection.extra breaks Airflow 3 Connections UI."""
from __future__ import annotations

from sqlalchemy import text

from frappe_airflow.airflow_db.connection import get_session


def run() -> int:
    """Set empty extra to NULL in Airflow metadata connection table."""
    with get_session() as s:
        result = s.execute(
            text(
                "UPDATE connection SET extra = NULL "
                "WHERE extra IS NOT NULL AND btrim(extra) = ''"
            )
        )
        return int(result.rowcount or 0)
