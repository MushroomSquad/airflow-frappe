"""DEPRECATED: use connection_registry_sync + dag_registry_sync.

Kept for one-time migration script migrate_client_cabinet_to_connections.py.
"""
from __future__ import annotations

import json

from frappe_airflow.airflow_db.variable_manager import set_variable

REGISTRY_KEY = "CLIENT_REGISTRY"


def build_registry(clients: list[dict]) -> dict:
    """Build CLIENT_REGISTRY dict from legacy client list structure."""
    registry: dict = {}
    for client in clients:
        client_id = client["id"]
        registry[client_id] = {
            "display_name": client["display_name"],
            "db": client.get("db", ""),
        }
        for cab in client.get("cabinets", []):
            platform = cab["platform"]
            slug = cab["slug"]
            registry[client_id].setdefault(platform, {})[slug] = {
                "display_name": cab["display_name"],
                "active": cab.get("active", True),
                "dags": cab.get("dags", []),
            }
    return registry


def rebuild_client_registry(clients: list[dict]) -> None:
    """Serialize clients to JSON and write to CLIENT_REGISTRY Variable."""
    built = build_registry(clients)
    set_variable(REGISTRY_KEY, json.dumps(built, ensure_ascii=False), description="")
