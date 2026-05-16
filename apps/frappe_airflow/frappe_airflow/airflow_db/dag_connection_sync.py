"""Sync AM DAG Config selected_connections with Airflow DAG metadata."""
from __future__ import annotations

import json

import frappe

from frappe_airflow.airflow_db.dag_platform import conn_matches_dag, infer_dag_platform
from frappe_airflow.airflow_db.dag_reader import list_dags


def _parse_selected(raw: str | None) -> list[str]:
    if not raw:
        return []
    raw = raw.strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [str(item) for item in data if item]
        except json.JSONDecodeError:
            pass
    return [line.strip() for line in raw.splitlines() if line.strip()]


def _dump_selected(conn_ids: list[str]) -> str:
    return json.dumps(sorted(set(conn_ids)), ensure_ascii=False)


def get_selected_connections(dag_id: str) -> list[str]:
    if not frappe.db.exists("AM DAG Config", dag_id):
        return []
    raw = frappe.db.get_value("AM DAG Config", dag_id, "selected_connections") or ""
    return _parse_selected(raw)


def set_selected_connections(dag_id: str, conn_ids: list[str]) -> None:
    payload = _dump_selected(conn_ids)
    if frappe.db.exists("AM DAG Config", dag_id):
        frappe.db.set_value("AM DAG Config", dag_id, "selected_connections", payload)
    else:
        doc = frappe.get_doc({
            "doctype": "AM DAG Config",
            "dag_id": dag_id,
            "selected_connections": payload,
        })
        doc.insert(ignore_permissions=True)
    frappe.db.commit()
    _sync_dag_registry(dag_id)


def add_connection_to_dag_config(dag_id: str, conn_id: str) -> None:
    selected = get_selected_connections(dag_id)
    if conn_id in selected:
        return
    selected.append(conn_id)
    set_selected_connections(dag_id, selected)


def assign_connection_to_matching_dags(conn_id: str, conn_type: str) -> None:
    for dag in list_dags():
        dag_id = dag["dag_id"]
        platform = infer_dag_platform(dag_id)
        if conn_matches_dag(conn_type, platform, dag_id, conn_id=conn_id):
            add_connection_to_dag_config(dag_id, conn_id)


def _sync_dag_registry(dag_id: str) -> None:
    from frappe_airflow.airflow_db.dag_registry_sync import rebuild_dag_registry_entry

    rebuild_dag_registry_entry(dag_id)
