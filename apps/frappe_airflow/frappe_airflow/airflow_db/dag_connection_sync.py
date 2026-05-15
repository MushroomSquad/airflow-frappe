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


def set_selected_connections(dag_id: str, conn_ids: list[str], db_connection: str | None = None) -> None:
    payload = _dump_selected(conn_ids)
    if frappe.db.exists("AM DAG Config", dag_id):
        values = {"selected_connections": payload}
        if db_connection is not None:
            values["db_connection"] = db_connection
        frappe.db.set_value("AM DAG Config", dag_id, values)
        _sync_child_table(dag_id, conn_ids)
    else:
        doc = frappe.get_doc({
            "doctype": "AM DAG Config",
            "dag_id": dag_id,
            "db_connection": db_connection or "",
            "selected_connections": payload,
        })
        doc.insert(ignore_permissions=True)
    frappe.db.commit()


def add_connection_to_dag_config(dag_id: str, conn_id: str) -> None:
    selected = get_selected_connections(dag_id)
    if conn_id in selected:
        return
    selected.append(conn_id)
    db_connection = frappe.db.get_value("AM DAG Config", dag_id, "db_connection") if frappe.db.exists(
        "AM DAG Config", dag_id
    ) else ""
    set_selected_connections(dag_id, selected, db_connection=db_connection or "")


def assign_connection_to_matching_dags(conn_id: str, conn_type: str) -> None:
    for dag in list_dags():
        dag_id = dag["dag_id"]
        platform = infer_dag_platform(dag_id)
        if conn_matches_dag(conn_type, platform, dag_id, conn_id=conn_id):
            add_connection_to_dag_config(dag_id, conn_id)


def _sync_child_table(dag_id: str, conn_ids: list[str]) -> None:
    """Keep legacy child table in sync for any code still reading it."""
    if not frappe.db.exists("AM DAG Config", dag_id):
        return
    config = frappe.get_doc("AM DAG Config", dag_id)
    config.set("connections", [{"connection": conn_id} for conn_id in conn_ids])
    config.save(ignore_permissions=True)
