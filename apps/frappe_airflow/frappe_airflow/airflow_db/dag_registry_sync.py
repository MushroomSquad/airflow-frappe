"""Sync AM DAG Config into DAG_REGISTRY Airflow Variable."""
from __future__ import annotations

import json

import frappe

from frappe_airflow.airflow_db.dag_connection_sync import get_selected_connections
from frappe_airflow.airflow_db.variable_manager import get_variable, set_variable

REGISTRY_KEY = "DAG_REGISTRY"


def build_dag_registry() -> dict:
    # Cross-check against live Airflow DAGs so removed DAGs don't pollute DAG_REGISTRY.
    # Falls back to including all AM DAG Config entries if Airflow PG is unreachable.
    live_dag_ids: set[str] | None = None
    try:
        from frappe_airflow.airflow_db.dag_reader import list_dags

        live_dag_ids = {d["dag_id"] for d in list_dags()}
    except Exception:
        pass  # Airflow unreachable — include all (safe fallback)

    registry: dict = {}
    for row in frappe.get_all("AM DAG Config", fields=["dag_id"]):
        dag_id = row["dag_id"]
        if live_dag_ids is not None and dag_id not in live_dag_ids:
            continue  # DAG deleted from Airflow — skip orphan record
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
