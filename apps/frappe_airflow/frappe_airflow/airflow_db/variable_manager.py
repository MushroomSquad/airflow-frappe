"""CRUD operations on Airflow's `variable` table.

Variables for CLIENT_REGISTRY and client_config_{id} are stored as plain JSON
(is_encrypted=false). We never encrypt Variables — they contain structural config,
not secrets.
"""
from __future__ import annotations

from sqlalchemy import text

from frappe_airflow.airflow_db.connection import get_session


def get_variable(key: str) -> str | None:
    """Return variable value string, or None if not found."""
    with get_session() as s:
        row = s.execute(
            text("SELECT val FROM variable WHERE key = :key"),
            {"key": key},
        ).fetchone()
        return row.val if row else None


def set_variable(key: str, value: str, description: str = "") -> None:
    """Insert or update a variable. Value stored as plain text (not encrypted)."""
    with get_session() as s:
        existing = s.execute(
            text("SELECT key FROM variable WHERE key = :key"), {"key": key}
        ).fetchone()
        if existing:
            s.execute(
                text("UPDATE variable SET val=:val, description=:desc WHERE key=:key"),
                {"key": key, "val": value, "desc": description},
            )
        else:
            s.execute(
                text(
                    "INSERT INTO variable (key, val, description, is_encrypted) "
                    "VALUES (:key, :val, :desc, false)"
                ),
                {"key": key, "val": value, "desc": description},
            )


def delete_variable(key: str) -> None:
    with get_session() as s:
        s.execute(text("DELETE FROM variable WHERE key = :key"), {"key": key})


def list_variables(search: str | None = None) -> list[dict]:
    with get_session() as s:
        if search:
            rows = s.execute(
                text("SELECT key, description FROM variable WHERE key ILIKE :s ORDER BY key"),
                {"s": f"%{search}%"},
            ).fetchall()
        else:
            rows = s.execute(text("SELECT key, description FROM variable ORDER BY key")).fetchall()
        return [{"key": r.key, "description": r.description or ""} for r in rows]
