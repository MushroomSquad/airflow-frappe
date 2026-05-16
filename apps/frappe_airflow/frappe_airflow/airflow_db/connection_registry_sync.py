"""Sync marketplace connections into CONNECTION_REGISTRY Airflow Variable."""
from __future__ import annotations

import json

from frappe_airflow.airflow_db.connection_manager import get_connection, list_marketplace_connections
from frappe_airflow.airflow_db.connection_meta import unpack_extra
from frappe_airflow.airflow_db.dag_platform import infer_connection_profile
from frappe_airflow.airflow_db.variable_manager import get_variable, set_variable

REGISTRY_KEY = "CONNECTION_REGISTRY"


def _entry_from_row(row: dict) -> dict:
    meta = unpack_extra(row.get("extra"))
    profile = infer_connection_profile(row["conn_id"], row.get("conn_type") or "", meta)
    if profile.get("is_companion"):
        return {}
    return {
        "platform": meta.get("platform") or profile.get("platform", ""),
        "slug": meta.get("slug") or profile.get("slug", ""),
        "display_name": meta.get("display_name", ""),
        "conn_type": profile.get("conn_type") or row.get("conn_type", ""),
        "target_db_connection": meta.get("target_db_connection", ""),
        "active": True,
    }


def build_connection_registry() -> dict:
    registry: dict = {}
    for row in list_marketplace_connections(limit=2000):
        entry = _entry_from_row(row)
        if entry:
            registry[row["conn_id"]] = entry
    return registry


def rebuild_connection_registry() -> None:
    set_variable(REGISTRY_KEY, json.dumps(build_connection_registry(), ensure_ascii=False), "")


def rebuild_connection_registry_entry(conn_id: str) -> None:
    registry = _load_registry()
    row = get_connection(conn_id)
    if not row:
        registry.pop(conn_id, None)
    else:
        entry = _entry_from_row(row)
        if entry:
            registry[conn_id] = entry
        else:
            registry.pop(conn_id, None)
    set_variable(REGISTRY_KEY, json.dumps(registry, ensure_ascii=False), "")


def remove_connection_registry_entry(conn_id: str) -> None:
    registry = _load_registry()
    registry.pop(conn_id, None)
    set_variable(REGISTRY_KEY, json.dumps(registry, ensure_ascii=False), "")


def _load_registry() -> dict:
    raw = get_variable(REGISTRY_KEY)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}
