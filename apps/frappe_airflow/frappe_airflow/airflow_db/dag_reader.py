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


def _active_condition(columns: set[str]) -> str | None:
    """Return SQL fragment that filters to only live DAGs, or None if not applicable.

    Airflow keeps removed DAGs in the ``dag`` table indefinitely.
    - Airflow 3.x: ``is_stale = false`` (DAG file still exists / scheduler sees it).
    - Airflow 2.x: ``is_active = true``.
    """
    if "is_stale" in columns:
        return "is_stale = false"
    if "is_active" in columns:
        return "is_active = true"
    return None


def _build_select(columns: set[str], conditions: list[str] | None = None) -> str:
    """Build SELECT … FROM dag WHERE … ORDER BY dag_id.

    ``conditions`` is a list of bare SQL fragments joined with AND.
    The ``is_active`` filter is injected automatically when the column exists.
    """
    schedule_col = _schedule_column(columns)
    last_parsed_col = _last_parsed_column(columns)
    schedule_expr = f"{schedule_col} AS schedule_value" if schedule_col else "'' AS schedule_value"
    parsed_expr = (
        f"{last_parsed_col} AS parsed_value" if last_parsed_col else "NULL AS parsed_value"
    )

    all_conds: list[str] = []
    active_cond = _active_condition(columns)
    if active_cond:
        all_conds.append(active_cond)
    if conditions:
        all_conds.extend(conditions)

    where_clause = ("WHERE " + " AND ".join(all_conds)) if all_conds else ""
    return (
        "SELECT dag_id, is_paused, "
        f"{schedule_expr}, {parsed_expr} "
        f"FROM dag {where_clause} ORDER BY dag_id"
    )


def _row_to_dict(r) -> dict:
    return {
        "name": r.dag_id,
        "dag_id": r.dag_id,
        "is_paused": bool(r.is_paused),
        "schedule_interval": r.schedule_value or "",
        "last_parsed_time": str(r.parsed_value) if r.parsed_value else "",
    }


def list_dags(paused: bool | None = None) -> list[dict]:
    """Return list of active DAGs, optionally filtered by paused state."""
    columns = _get_dag_columns()
    extra = ["is_paused = :paused"] if paused is not None else []
    with get_session() as s:
        rows = s.execute(
            text(_build_select(columns, extra)),
            {"paused": paused} if paused is not None else {},
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def get_dag(dag_id: str) -> dict | None:
    """Return a single DAG by dag_id.

    Does not apply the is_active filter so the edit form works even for a
    temporarily deactivated DAG (e.g. mid-deployment).
    """
    columns = _get_dag_columns()
    schedule_col = _schedule_column(columns)
    last_parsed_col = _last_parsed_column(columns)
    schedule_expr = f"{schedule_col} AS schedule_value" if schedule_col else "'' AS schedule_value"
    parsed_expr = (
        f"{last_parsed_col} AS parsed_value" if last_parsed_col else "NULL AS parsed_value"
    )
    sql = text(
        f"SELECT dag_id, is_paused, {schedule_expr}, {parsed_expr} "
        "FROM dag WHERE dag_id = :dag_id"
    )
    with get_session() as s:
        row = s.execute(sql, {"dag_id": dag_id}).fetchone()
        return _row_to_dict(row) if row else None


def count_dags(paused: bool | None = None) -> int:
    """Count active DAGs."""
    columns = _get_dag_columns()
    active_cond = _active_condition(columns)
    with get_session() as s:
        if paused is None:
            where = f"WHERE {active_cond}" if active_cond else ""
        else:
            conds = ["is_paused = :p"]
            if active_cond:
                conds.append(active_cond)
            where = "WHERE " + " AND ".join(conds)
        return (
            s.execute(
                text(f"SELECT COUNT(*) FROM dag {where}"),
                {"p": paused} if paused is not None else {},
            ).scalar()
            or 0
        )


def set_dag_paused(dag_id: str, is_paused: bool) -> None:
    """Set the paused state of a DAG in Airflow's metadata DB."""
    with get_session() as s:
        s.execute(
            text("UPDATE dag SET is_paused = :paused WHERE dag_id = :dag_id"),
            {"paused": is_paused, "dag_id": dag_id},
        )
