"""Sync AM Client + marketplace connections into CLIENT_DIRECTORY Airflow Variable."""
from __future__ import annotations

import json

import frappe

from frappe_airflow.airflow_db.connection_manager import list_marketplace_connections
from frappe_airflow.airflow_db.connection_meta import unpack_extra
from frappe_airflow.airflow_db.dag_platform import infer_connection_profile
from frappe_airflow.airflow_db.variable_manager import set_variable

REGISTRY_KEY = "CLIENT_DIRECTORY"


def build_client_directory() -> dict:
    registry: dict[str, dict] = {}

    if frappe.db.exists("DocType", "AM Client"):
        for row in frappe.get_all("AM Client", fields=["name", "client_name"]):
            registry[row["name"]] = {
                "client_name": row["client_name"],
                "connections": [],
            }

    for row in list_marketplace_connections(limit=2000):
        meta = unpack_extra(row.get("extra"))
        profile = infer_connection_profile(row["conn_id"], row.get("conn_type") or "", meta)
        if profile.get("is_companion"):
            continue

        client = (meta.get("client") or "").strip()
        if not client:
            continue

        entry = registry.setdefault(
            client,
            {"client_name": client, "connections": []},
        )
        entry["connections"].append(row["conn_id"])

    for entry in registry.values():
        entry["connections"] = sorted(set(entry["connections"]))

    return registry


def rebuild_client_directory() -> None:
    set_variable(REGISTRY_KEY, json.dumps(build_client_directory(), ensure_ascii=False), "")
