"""Read-only access to Airflow's `dag` table."""
from __future__ import annotations

from sqlalchemy import text

from frappe_airflow.airflow_db.connection import get_session


def _get_dag_columns() -> set[str]:
    with get_session() as s:
        rows = s.execute(
            text(
                "SELECT column_name "
                "FROM information_schema.columns "
                "WHERE table_name = 'dag'"
            )
        ).fetchall()
    return {row.column_name for row in rows}


def _schedule_column(columns: set[str]) -> str | None:
    for candidate in ("schedule_interval", "timetable_summary", "timetable_description"):
        if candidate in columns:
            return candidate
    return None


def _last_parsed_column(columns: set[str]) -> str | None:
    return "last_parsed_time" if "last_parsed_time" in columns else None


def _build_select(columns: set[str], where_sql: str = "") -> str:
    schedule_col = _schedule_column(columns)
    last_parsed_col = _last_parsed_column(columns)
    schedule_expr = f"{schedule_col} AS schedule_value" if schedule_col else "'' AS schedule_value"
    parsed_expr = (
        f"{last_parsed_col} AS parsed_value" if last_parsed_col else "NULL AS parsed_value"
    )
    return (
        "SELECT dag_id, is_paused, "
        f"{schedule_expr}, {parsed_expr} "
        f"FROM dag {where_sql} ORDER BY dag_id"
    )


def list_dags(paused: bool | None = None) -> list[dict]:
    """Return list of DAGs, optionally filtered by paused state."""
    columns = _get_dag_columns()
    with get_session() as s:
        if paused is None:
            rows = s.execute(text(_build_select(columns))).fetchall()
        else:
            rows = s.execute(
                text(_build_select(columns, "WHERE is_paused = :paused")),
                {"paused": paused},
            ).fetchall()
        return [
            {
                "name": r.dag_id,
                "dag_id": r.dag_id,
                "is_paused": bool(r.is_paused),
                "schedule_interval": r.schedule_value or "",
                "last_parsed_time": str(r.parsed_value) if r.parsed_value else "",
            }
            for r in rows
        ]


def get_dag(dag_id: str) -> dict | None:
    columns = _get_dag_columns()
    with get_session() as s:
        row = s.execute(
            text(_build_select(columns, "WHERE dag_id = :dag_id")),
            {"dag_id": dag_id},
        ).fetchone()
        if row is None:
            return None
        return {
            "name": row.dag_id,
            "dag_id": row.dag_id,
            "is_paused": bool(row.is_paused),
            "schedule_interval": row.schedule_value or "",
            "last_parsed_time": str(row.parsed_value) if row.parsed_value else "",
        }


def count_dags(paused: bool | None = None) -> int:
    with get_session() as s:
        if paused is None:
            return s.execute(text("SELECT COUNT(*) FROM dag")).scalar() or 0
        return s.execute(
            text("SELECT COUNT(*) FROM dag WHERE is_paused = :p"), {"p": paused}
        ).scalar() or 0
