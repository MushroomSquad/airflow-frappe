"""CRUD operations on Airflow's `connection` table.

Passwords are Fernet-encrypted at rest. On read for list views, password is
never returned. On read for edit form, password is decrypted and returned.
On write, if password is blank/None, the existing encrypted value is preserved.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import text

from frappe_airflow.airflow_db.connection import get_session
from frappe_airflow.airflow_db.fernet import decrypt, encrypt


def _row_to_dict(row, include_password: bool = False) -> dict[str, Any]:
    d = {
        "conn_id": row.conn_id,
        "conn_type": row.conn_type or "",
        "description": row.description or "",
        "host": row.host or "",
        "schema": row.schema or "",
        "login": row.login or "",
        "port": row.port,
        "extra": row.extra or "",
        "is_encrypted": bool(row.is_encrypted),
    }
    if include_password:
        raw = row.password or ""
        if raw and row.is_encrypted:
            try:
                d["password"] = decrypt(raw)
            except Exception:
                d["password"] = ""
        else:
            d["password"] = raw
    return d


def list_connections(
    conn_type: str | None = None,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Return connections without passwords."""
    with get_session() as s:
        filters = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if conn_type:
            filters.append("conn_type = :conn_type")
            params["conn_type"] = conn_type
        elif conn_type is None:
            filters.append("conn_type != 'postgres'")
        if search:
            filters.append("conn_id ILIKE :search")
            params["search"] = f"%{search}%"
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        sql = text(
            "SELECT conn_id, conn_type, description, host, schema, login, port, extra, "
            f"is_encrypted FROM connection {where} ORDER BY conn_id LIMIT :limit OFFSET :offset"
        )
        rows = s.execute(sql, params).fetchall()
        return [
            {
                "name": r.conn_id,
                "conn_id": r.conn_id,
                "conn_type": r.conn_type or "",
                "description": r.description or "",
                "host": r.host or "",
                "login": r.login or "",
                "port": r.port,
                "extra": r.extra or "",
            }
            for r in rows
        ]


def list_marketplace_connections(limit: int = 500, offset: int = 0) -> list[dict]:
    """All non-database marketplace connections."""
    return list_connections(limit=limit, offset=offset)


def count_connections(conn_type: str | None = None, search: str | None = None) -> int:
    with get_session() as s:
        filters = []
        params: dict[str, Any] = {}
        if conn_type:
            filters.append("conn_type = :conn_type")
            params["conn_type"] = conn_type
        if search:
            filters.append("conn_id ILIKE :search")
            params["search"] = f"%{search}%"
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        sql = text(f"SELECT COUNT(*) FROM connection {where}")
        return s.execute(sql, params).scalar() or 0


def get_connection(conn_id: str) -> dict | None:
    """Return single connection with decrypted password."""
    with get_session() as s:
        sql = text(
            "SELECT conn_id, conn_type, description, host, schema, login, password, "
            "port, is_encrypted, extra FROM connection WHERE conn_id = :conn_id"
        )
        row = s.execute(sql, {"conn_id": conn_id}).fetchone()
        if row is None:
            return None
        return _row_to_dict(row, include_password=True)


def upsert_connection(data: dict) -> None:
    """Insert or update a connection. Password blank -> preserve existing encrypted value."""
    conn_id = data["conn_id"]
    new_password = data.get("password") or ""

    with get_session() as s:
        existing = s.execute(
            text("SELECT password, is_encrypted FROM connection WHERE conn_id = :id"),
            {"id": conn_id},
        ).fetchone()

        if new_password:
            encrypted_password = encrypt(new_password)
            is_enc = True
        elif existing:
            encrypted_password = existing.password or ""
            is_enc = bool(existing.is_encrypted)
        else:
            encrypted_password = ""
            is_enc = False

        extra = data.get("extra", "")
        if existing:
            s.execute(
                text(
                    "UPDATE connection SET conn_type=:conn_type, description=:description, "
                    "host=:host, schema=:schema, login=:login, password=:password, "
                    "port=:port, extra=:extra, is_encrypted=:is_encrypted WHERE conn_id=:conn_id"
                ),
                {
                    "conn_id": conn_id,
                    "conn_type": data.get("conn_type", ""),
                    "description": data.get("description", ""),
                    "host": data.get("host", ""),
                    "schema": data.get("schema", ""),
                    "login": data.get("login", ""),
                    "password": encrypted_password,
                    "port": data.get("port"),
                    "extra": extra,
                    "is_encrypted": is_enc,
                },
            )
        else:
            s.execute(
                text(
                    "INSERT INTO connection (conn_id, conn_type, description, host, schema, "
                    "login, password, port, extra, is_encrypted, is_extra_encrypted) "
                    "VALUES (:conn_id, :conn_type, :description, :host, :schema, "
                    ":login, :password, :port, :extra, :is_encrypted, false)"
                ),
                {
                    "conn_id": conn_id,
                    "conn_type": data.get("conn_type", ""),
                    "description": data.get("description", ""),
                    "host": data.get("host", ""),
                    "schema": data.get("schema", ""),
                    "login": data.get("login", ""),
                    "password": encrypted_password,
                    "port": data.get("port"),
                    "extra": extra,
                    "is_encrypted": is_enc,
                },
            )


def delete_connection(conn_id: str) -> None:
    with get_session() as s:
        s.execute(text("DELETE FROM connection WHERE conn_id = :id"), {"id": conn_id})
