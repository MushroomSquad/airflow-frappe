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
      id, display_name, db, cabinets: list[dict(slug, display_name, platform, active, dags)]
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
                    c["slug"]: {
                        "display_name": c["display_name"],
                        "active": c["active"],
                        "dags": [d.strip() for d in c.get("dags", "").split(",") if d.strip()],
                    }
                    for c in cabs
                }
        registry[client["id"]] = entry
    return registry


def rebuild_client_registry(clients: list[dict]) -> None:
    """Serialize clients to JSON and write to CLIENT_REGISTRY Variable."""
    registry = build_registry(clients)
    set_variable(REGISTRY_KEY, json.dumps(registry, ensure_ascii=False), description="")
