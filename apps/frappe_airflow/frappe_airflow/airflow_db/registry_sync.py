"""Serialize AM Client + AM Cabinet records into CLIENT_REGISTRY Airflow Variable.

Called after every Client or Cabinet save. Rebuilds the full registry from scratch.
This is intentionally simple: no diffing, just full rebuild.
"""
from __future__ import annotations

import json

from frappe_airflow.airflow_db.variable_manager import set_variable

REGISTRY_KEY = "CLIENT_REGISTRY"


def build_registry(clients: list[dict]) -> dict:
    """Build CLIENT_REGISTRY dict from a list of client dicts.

    Each client dict has:
      id, display_name, db, cabinets: list[dict(slug, display_name, platform, active, dags, *_connection)]
    """
    registry: dict = {}
    for client in clients:
        entry: dict = {
            "display_name": client["display_name"],
            "db": client["db"],
        }
        for platform in ("wb", "oz", "ms", "ym"):
            cabs = [c for c in client.get("cabinets", []) if c["platform"] == platform]
            if cabs:
                entry[platform] = {
                    c["slug"]: _serialize_cabinet(c)
                    for c in cabs
                }
        registry[client["id"]] = entry
    return registry


def rebuild_client_registry(clients: list[dict]) -> None:
    """Serialize clients to JSON and write to CLIENT_REGISTRY Variable."""
    registry = build_registry(clients)
    set_variable(REGISTRY_KEY, json.dumps(registry, ensure_ascii=False), description="")


def _serialize_cabinet(cabinet: dict) -> dict:
    payload = {
        "display_name": cabinet["display_name"],
        "active": cabinet["active"],
        "dags": [d.strip() for d in cabinet.get("dags", "").split(",") if d.strip()],
    }

    connections = {
        "wb_token": cabinet.get("wb_token_connection") or "",
        "oz_seller": cabinet.get("oz_seller_connection") or "",
        "oz_performance": cabinet.get("oz_performance_connection") or "",
        "ms_token": cabinet.get("ms_token_connection") or "",
        "ym_token": cabinet.get("ym_token_connection") or "",
    }
    connections = {k: v for k, v in connections.items() if v}
    if connections:
        payload["connections"] = connections

    platform = cabinet.get("platform")
    if platform == "wb" and cabinet.get("wb_token_connection"):
        payload["token_conn_id"] = cabinet["wb_token_connection"]
    elif platform == "oz":
        if cabinet.get("oz_seller_connection"):
            payload["seller_conn_id"] = cabinet["oz_seller_connection"]
        if cabinet.get("oz_performance_connection"):
            payload["performance_conn_id"] = cabinet["oz_performance_connection"]
    elif platform == "ms" and cabinet.get("ms_token_connection"):
        payload["token_conn_id"] = cabinet["ms_token_connection"]
    elif platform == "ym" and cabinet.get("ym_token_connection"):
        payload["token_conn_id"] = cabinet["ym_token_connection"]

    return payload
