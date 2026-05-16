"""Sync AM DAG Config into DAG_REGISTRY Airflow Variable."""
from __future__ import annotations

import json

import frappe

from frappe_airflow.airflow_db.dag_connection_sync import get_selected_connections
from frappe_airflow.airflow_db.variable_manager import get_variable, set_variable

REGISTRY_KEY = "DAG_REGISTRY"


def build_dag_registry() -> dict:
    registry: dict = {}
    for row in frappe.get_all("AM DAG Config", fields=["dag_id", "selected_connections"]):
        dag_id = row["dag_id"]
        registry[dag_id] = {
            "connections": get_selected_connections(dag_id),
        }
    return registry


def rebuild_dag_registry() -> None:
    set_variable(REGISTRY_KEY, json.dumps(build_dag_registry(), ensure_ascii=False), "")


def rebuild_dag_registry_entry(dag_id: str) -> None:
    registry = _load_registry()
    if frappe.db.exists("AM DAG Config", dag_id):
        registry[dag_id] = {"connections": get_selected_connections(dag_id)}
    else:
        registry.pop(dag_id, None)
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
