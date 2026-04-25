"""Read-only access to Airflow's `dag` table."""
from __future__ import annotations

from sqlalchemy import text

from frappe_airflow.airflow_db.connection import get_session


def list_dags(paused: bool | None = None) -> list[dict]:
    """Return list of DAGs, optionally filtered by paused state."""
    with get_session() as s:
        if paused is None:
            rows = s.execute(
                text(
                    "SELECT dag_id, is_paused, schedule_interval, last_parsed_time "
                    "FROM dag ORDER BY dag_id"
                )
            ).fetchall()
        else:
            rows = s.execute(
                text(
                    "SELECT dag_id, is_paused, schedule_interval, last_parsed_time "
                    "FROM dag WHERE is_paused = :paused ORDER BY dag_id"
                ),
                {"paused": paused},
            ).fetchall()
        return [
            {
                "dag_id": r.dag_id,
                "is_paused": bool(r.is_paused),
                "schedule_interval": r.schedule_interval or "",
                "last_parsed_time": str(r.last_parsed_time) if r.last_parsed_time else "",
            }
            for r in rows
        ]


def get_dag(dag_id: str) -> dict | None:
    with get_session() as s:
        row = s.execute(
            text(
                "SELECT dag_id, is_paused, schedule_interval, last_parsed_time "
                "FROM dag WHERE dag_id = :dag_id"
            ),
            {"dag_id": dag_id},
        ).fetchone()
        if row is None:
            return None
        return {
            "dag_id": row.dag_id,
            "is_paused": bool(row.is_paused),
            "schedule_interval": row.schedule_interval or "",
            "last_parsed_time": str(row.last_parsed_time) if row.last_parsed_time else "",
        }


def count_dags(paused: bool | None = None) -> int:
    with get_session() as s:
        if paused is None:
            return s.execute(text("SELECT COUNT(*) FROM dag")).scalar() or 0
        return s.execute(
            text("SELECT COUNT(*) FROM dag WHERE is_paused = :p"), {"p": paused}
        ).scalar() or 0
